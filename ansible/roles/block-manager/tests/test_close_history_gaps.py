"""Renders tasks of close-history-gaps.yaml with ansible-playbook.

The tasks are taken from the role by name and run on localhost with faked
results of the commands they read (`gaps --json` of the target and of the
sources), so the Jinja of the source selection, the verdict and the report is
checked against real Ansible filters.

Run: python3 -m unittest discover -s ansible/roles/block-manager/tests
Needs ansible-core and PyYAML; skipped without ansible-playbook, unless
REQUIRE_ANSIBLE=1 is set (then a missing ansible-playbook fails). Set
ANSIBLE_PLAYBOOK to test another ansible-core; run it on one before 2.19 and
one from 2.19 on, which template `none` differently. The host's Ansible
setup is not read: every ANSIBLE_* variable is dropped, and ANSIBLE_CONFIG
points to an empty file.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROLE = Path(__file__).resolve().parents[1]
TASKS = yaml.safe_load((ROLE / "tasks/close-history-gaps.yaml").read_text())
DEFAULTS = yaml.safe_load((ROLE / "defaults/main/history_gaps.yaml").read_text())
PLAYBOOK = os.environ.get("ANSIBLE_PLAYBOOK") or shutil.which("ansible-playbook")
if not PLAYBOOK and os.environ.get("REQUIRE_ANSIBLE") == "1":
    raise RuntimeError("REQUIRE_ANSIBLE=1 and no ansible-playbook")

SELECTION = [
    "History gaps | stores of the sources",
    "History gaps | usable sources",
    "History gaps | skipped sources",
    "History gaps | failed sources",
    "History gaps | earliest source start",
]


def store(first, last, gaps=(), failed_checks=(), unprovable=0, missing_attestations=()):
    return {
        "first_height": first,
        "last_height": last,
        "gaps": list(gaps),
        "missing_attestations": list(missing_attestations),
        "failed_checks": list(failed_checks),
        "unprovable_events": {"heights": unprovable, "ranges": [], "total_ranges": 0},
    }


class Failed(str):
    """The stderr of a `gaps --json` that exits 2."""


def gaps_result(name, gaps):
    """The `gaps --json` task result of a source: `gaps` is its JSON, `None` for
    a source without a store, or `Failed(stderr)` for a `gaps` that exits 2."""
    if gaps is None:
        gaps = Failed("error: /workdir/data/bm-history-proofs.db: no such file")
    if isinstance(gaps, Failed):
        return {"item": name, "rc": 2, "stdout_lines": [], "stderr": str(gaps)}
    return {"item": name, "rc": 0, "stdout_lines": [json.dumps(gaps)], "stderr": ""}


def run(task_names, target, sources, expressions=None, collect_vars=None):
    """Runs the named tasks; returns {name: registered result} and the facts.

    `expressions` ({name: Jinja}) are templated after the tasks, with
    `collect_vars` as their task vars, and returned among the facts.
    """
    by_name = {task["name"]: task for task in TASKS}
    tasks = []
    for n, name in enumerate(task_names):
        task = dict(by_name[name], register=f"r{n}", ignore_errors=True)
        tasks.append(task)
    target_line = {"stdout_lines": [json.dumps(target)], "rc": 0}
    variables = dict(
        DEFAULTS,
        bm_history_sources=list(sources),
        history_gaps_now=target_line,
        history_gaps_final=target_line,
        history_source_probes={"results": [{"item": name, "rc": 0} for name in sources]},
        history_source_reads={"results": [gaps_result(n, g) for n, g in sources.items()]},
    )
    with tempfile.TemporaryDirectory() as temp:
        out = Path(temp) / "out.json"
        collected = {f"r{n}": f"{{{{ r{n} }}}}" for n in range(len(task_names))}
        facts = [
            "history_sources",
            "history_skipped_sources",
            "history_sources_first",
            "history_failed_sources",
        ]
        templated = {fact: f"{{{{ {fact} | default(none) }}}}" for fact in facts}
        templated.update(expressions or {})
        tasks.append({
            "name": "collect",
            "ansible.builtin.copy": {
                "dest": str(out),
                "content": "{{ {'results': results, 'facts': facts} | to_json }}",
            },
            "vars": dict(collect_vars or {}, results=collected, facts=templated),
        })
        config = Path(temp) / "ansible.cfg"
        config.write_text("")
        play = Path(temp) / "play.yaml"
        play.write_text(yaml.safe_dump([{
            "hosts": "localhost", "gather_facts": False, "vars": variables, "tasks": tasks,
        }]))
        done = subprocess.run(
            [PLAYBOOK, "-i", "localhost,", "-c", "local", str(play)],
            capture_output=True, text=True,
            env={key: value for key, value in os.environ.items() if not key.startswith("ANSIBLE_")}
            | {"ANSIBLE_CONFIG": str(config)},
        )
        if not out.exists():
            raise AssertionError(done.stdout + done.stderr)
        written = json.loads(out.read_text())
    results = {name: written["results"][f"r{n}"] for n, name in enumerate(task_names)}
    return results, written["facts"]


@unittest.skipUnless(PLAYBOOK, "needs ansible-playbook")
class SourceSelection(unittest.TestCase):
    def test_a_source_with_failed_checks_is_not_used_but_counts_in_the_verdict(self):
        target = store(100, 900)
        sources = {"bm2": store(100, 900), "bm3": store(50, 900, failed_checks=[[128, 128]])}
        verdict = "History gaps | stop when gaps remain"
        results, facts = run(SELECTION + [verdict], target, sources)
        self.assertEqual([s["name"] for s in facts["history_sources"]], ["bm2"])
        self.assertIn(
            "bm3: rows that failed their checks [[128, 128]], restore its store",
            facts["history_skipped_sources"],
        )
        self.assertEqual(int(facts["history_sources_first"]), 50)
        # The target starts later than bm3: only bm3 had that part.
        self.assertTrue(results[verdict].get("failed"), results[verdict])
        self.assertIn("bm3: rows that failed their checks", results[verdict]["msg"])

    def test_a_source_with_failed_checks_does_not_stop_a_target_that_needs_nothing(self):
        target = store(50, 900)
        sources = {"bm2": store(100, 900), "bm3": store(60, 900, failed_checks=[[128, 128]])}
        verdict = "History gaps | stop when gaps remain"
        results, facts = run(SELECTION + [verdict], target, sources)
        self.assertTrue(results[verdict].get("skipped"), results[verdict])
        self.assertEqual([s["name"] for s in facts["history_sources"]], ["bm2"])


    def test_a_target_behind_sources_that_all_have_failed_checks_stops(self):
        target = store(100, 900)
        sources = {
            "bm2": store(50, 900, failed_checks=[[128, 128]]),
            "bm3": store(60, 900, failed_checks=[[300, 300]]),
        }
        verdict = "History gaps | stop when gaps remain"
        results, facts = run(SELECTION + [verdict], target, sources)
        self.assertEqual(facts["history_sources"], [])
        self.assertTrue(results[verdict].get("failed"), results[verdict])
        self.assertIn("starts at 100, later than a source (50)", results[verdict]["msg"])

    def test_without_any_source_store_the_play_passes(self):
        # The first rollout: no source has a store yet.
        target = store(100, 900)
        sources = {"bm2": None}
        verdict = "History gaps | stop when gaps remain"
        failed_source = "History gaps | stop when a source failed"
        results, facts = run(SELECTION + [failed_source, verdict], target, sources)
        self.assertEqual(facts["history_sources"], [])
        self.assertEqual(facts["history_failed_sources"], [])
        self.assertTrue(results[failed_source].get("skipped"), results[failed_source])
        self.assertTrue(results[verdict].get("skipped"), results[verdict])
        self.assertIn("bm2: no history proofs store", facts["history_skipped_sources"])

    def test_only_a_source_whose_gaps_failed_for_another_reason_stops_the_play(self):
        target = store(1, 900)
        sources = {
            "bm2": store(1, 900),
            "bm3": Failed("error: unexpected argument 'history-proofs' found"),
            "bm4": Failed("error: /workdir/data/bm-history-proofs.db: database is locked"),
        }
        failed_source = "History gaps | stop when a source failed"
        results, facts = run(SELECTION + [failed_source], target, sources)
        self.assertEqual(facts["history_failed_sources"], ["bm4"])
        skipped = facts["history_skipped_sources"]
        self.assertIn("bm3: block-manager without history-proofs", skipped)
        self.assertIn(
            "bm4: gaps failed (rc 2): error: /workdir/data/bm-history-proofs.db: database is locked",
            skipped,
        )
        self.assertTrue(results[failed_source].get("failed"), results[failed_source])
        self.assertIn(
            """-e '{"bm_history_sources": ["bm2", "bm3"]}'""", results[failed_source]["msg"]
        )

    def test_the_copy_takes_from_usable_sources_only(self):
        copy = next(t for t in TASKS if t["name"] == "History gaps | close them from the sources one by one")
        self.assertEqual(copy["loop"], "{{ history_sources }}")

    def test_an_attestation_a_usable_source_has_is_copied_from_it(self):
        target = store(1, 5000, missing_attestations=[2048])
        sources = {"bm2": store(1, 5000), "bm3": store(1, 5000, missing_attestations=[2048])}
        for source, ranges in [("first", [[2048, 2048]]), ("last", [])]:
            _, facts = run(
                SELECTION, target, sources,
                expressions={"ranges": "{{ history_source_ranges }}"},
                collect_vars={"history_source": f"{{{{ history_sources | {source} }}}}"},
            )
            self.assertEqual(facts["ranges"], ranges, source)

    def test_an_attestation_no_source_has_is_reported_not_failed_on(self):
        target = store(1, 5000, missing_attestations=[2048])
        sources = {"bm2": store(1, 5000, missing_attestations=[2048])}
        verdict = "History gaps | stop when gaps remain"
        report = "History gaps | report"
        results, _ = run(SELECTION + [report, verdict], target, sources)
        self.assertTrue(results[verdict].get("skipped"), results[verdict])
        self.assertIn(
            "missing attestations no source read has (see sources not used; one left"
            " out with -e bm_history_sources is not read): [2048];",
            results[report]["msg"],
        )

    def test_an_attestation_only_a_source_with_failed_checks_has_stops_the_play(self):
        target = store(1, 5000, missing_attestations=[2048])
        sources = {
            "bm2": store(1, 5000, missing_attestations=[2048]),
            "bm3": store(1, 5000, failed_checks=[[128, 128]]),
        }
        verdict = "History gaps | stop when gaps remain"
        results, _ = run(SELECTION + [verdict], target, sources)
        self.assertTrue(results[verdict].get("failed"), results[verdict])
        self.assertIn("'sources': ['bm3']", results[verdict]["msg"])


@unittest.skipUnless(PLAYBOOK, "needs ansible-playbook")
class Report(unittest.TestCase):
    def test_report_shows_the_heights_with_an_unprovable_event(self):
        report = "History gaps | report"
        for unprovable in [0, 7]:
            target = store(1, 900, unprovable=unprovable)
            results, _ = run(SELECTION + [report], target, {"bm2": store(1, 900)})
            self.assertIn(
                f"heights with an unprovable event (an alert only): {unprovable};",
                results[report]["msg"],
            )

    def test_report_without_the_field_says_unknown(self):
        report = "History gaps | report"
        target = store(1, 900)
        del target["unprovable_events"]
        results, _ = run(SELECTION + [report], target, {"bm2": store(1, 900)})
        self.assertIn(
            "heights with an unprovable event (an alert only): unknown;",
            results[report]["msg"],
        )


if __name__ == "__main__":
    unittest.main()
