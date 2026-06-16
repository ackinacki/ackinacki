#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path


TOOLS_IMAGE_DEFAULT = "aerospike/aerospike-tools"
HELPER_IMAGE_DEFAULT = "python:3.12-slim"
TOOLS_MOUNT_DIR = "/data"
SCRIPT_MOUNT_PATH = "/work/aerospike_worker.py"


def die(message: str) -> None:
    print(f"Error: {message}", file=sys.stderr)
    raise SystemExit(1)


def run(
    cmd: list[str],
    *,
    capture: bool = False,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def command_exists(name: str) -> bool:
    return subprocess.call(
        ["bash", "-lc", f"command -v {name} >/dev/null 2>&1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) == 0


def require_docker() -> None:
    if not command_exists("docker"):
        die("docker is required")


def lower(value: str) -> str:
    return value.lower()


def detect_aerospike_container(explicit_container: str | None) -> str:
    if explicit_container:
        return explicit_container

    result = run(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"], capture=True)
    candidates: list[str] = []
    for raw_line in result.stdout.splitlines():
        if not raw_line.strip():
            continue
        try:
            name, image = raw_line.split("\t", 1)
        except ValueError:
            continue
        lname = lower(name)
        limage = lower(image)
        if "aerospike-tools" in limage:
            continue
        if "aerospike/aerospike-server" in limage or ("aerospike" in limage and "aerospike" in lname):
            candidates.append(name)

    if not candidates:
        die("No running Aerospike container detected; use --container")
    if len(candidates) > 1:
        joined = "\n  ".join(candidates)
        die(f"Detected multiple Aerospike containers:\n  {joined}\nSpecify --container explicitly")
    return candidates[0]


def inspect_network_names(container: str) -> list[str]:
    result = run(
        [
            "docker",
            "inspect",
            container,
            "--format",
            "{{range $k, $v := .NetworkSettings.Networks}}{{printf \"%s\\n\" $k}}{{end}}",
        ],
        capture=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def detect_network(container: str, explicit_network: str | None) -> str:
    if explicit_network:
        return explicit_network

    networks = inspect_network_names(container)
    if not networks:
        die(f"Container {container} has no Docker networks")
    if len(networks) == 1:
        return networks[0]

    filtered = [n for n in networks if n not in {"bridge", "host", "none", "ingress"}]
    if len(filtered) == 1:
        return filtered[0]

    preferred = []
    for network in filtered:
        lnet = lower(network)
        if any(token in lnet for token in ("telemetry", "monitor", "metric", "logging")):
            continue
        preferred.append(network)
    if len(preferred) == 1:
        return preferred[0]

    backendish = [n for n in preferred if ("backend" in lower(n) or "default" in lower(n))]
    if len(backendish) == 1:
        return backendish[0]

    joined = "\n  ".join(filtered)
    die(f"Container {container} is attached to multiple candidate networks:\n  {joined}\nSpecify --network explicitly")


def inspect_aliases(container: str, network: str) -> list[str]:
    result = run(
        [
            "docker",
            "inspect",
            container,
            "--format",
            "{{with index .NetworkSettings.Networks \"" + network + "\"}}{{range .Aliases}}{{printf \"%s\\n\" .}}{{end}}{{end}}",
        ],
        capture=True,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def detect_host(container: str, network: str, explicit_host: str | None) -> str:
    if explicit_host:
        return explicit_host

    aliases = inspect_aliases(container, network)
    if "aerospike" in aliases:
        return "aerospike"
    for alias in aliases:
        if alias != container and not (len(alias) == 12 and all(ch in "0123456789abcdef" for ch in alias)):
            return alias
    return container


def run_tools_raw(
    tools_image: str,
    network: str,
    args: list[str],
    *,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "run", "--rm", "--network", network, tools_image]
    cmd += args
    return run(cmd, capture=capture)


def detect_port(tools_image: str, network: str, host: str, explicit_port: str | None) -> str:
    if explicit_port:
        return explicit_port

    for candidate in ("3000", "4000"):
        try:
            run_tools_raw(
                tools_image,
                network,
                ["asinfo", "--no-config-file", "-h", host, "-p", candidate, "-v", "build"],
                capture=True,
            )
            return candidate
        except subprocess.CalledProcessError:
            continue
    die(f"Could not auto-detect Aerospike port for host '{host}'; use --port")


def build_cluster_args(args: argparse.Namespace, host: str, port: str) -> list[str]:
    cluster_args = ["--no-config-file", "-h", host, "-p", port]
    if args.user:
        cluster_args += ["-U", args.user]
        if args.password:
            cluster_args += ["-P", args.password]
        else:
            cluster_args += ["-P"]
    if args.auth:
        cluster_args += ["--auth", args.auth]
    if args.services_alternate:
        cluster_args += ["-S"]
    if args.tls_enable:
        cluster_args += ["--tls-enable"]
    if args.tls_name:
        cluster_args += ["--tls-name", args.tls_name]
    if args.tls_cafile:
        cluster_args += ["--tls-cafile", args.tls_cafile]
    if args.tls_capath:
        cluster_args += ["--tls-capath", args.tls_capath]
    return cluster_args


def list_sets(tools_image: str, network: str, cluster_args: list[str], namespace: str) -> list[str]:
    result = run_tools_raw(
        tools_image,
        network,
        ["asinfo", *cluster_args, "-v", f"sets/{namespace}"],
        capture=True,
    )
    raw = result.stdout.strip()
    if not raw:
        return []

    sets: list[str] = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        for part in entry.split(":"):
            if part.startswith("set="):
                set_name = part.split("=", 1)[1]
                if set_name:
                    sets.append(set_name)
                break
    return sets


def set_info(
    tools_image: str,
    network: str,
    cluster_args: list[str],
    namespace: str,
    set_name: str,
) -> str:
    result = run_tools_raw(
        tools_image,
        network,
        ["asinfo", *cluster_args, "-v", f"sets/{namespace}/{set_name}"],
        capture=True,
    )
    return result.stdout.strip()


def parse_set_info(raw: str) -> dict[str, str]:
    stats: dict[str, str] = {}
    for entry in raw.split(";"):
        for part in entry.split(":"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key:
                stats[key] = value
    return stats


def is_truncate_complete(raw: str) -> bool:
    if not raw:
        return True

    stats = parse_set_info(raw)
    if not stats:
        return True

    objects = stats.get("objects")
    truncating = stats.get("truncating", "false").lower()
    return objects == "0" and truncating == "false"


def wait_for_truncate_complete(
    tools_image: str,
    network: str,
    cluster_args: list[str],
    namespace: str,
    set_name: str,
    timeout_secs: float,
    interval_secs: float,
) -> None:
    started = time.monotonic()
    last_info = ""
    next_log_at = started

    while True:
        try:
            last_info = set_info(tools_image, network, cluster_args, namespace, set_name)
        except subprocess.CalledProcessError as error:
            last_info = (error.stdout or "").strip()
            if not last_info:
                last_info = (error.stderr or "").strip()

        if is_truncate_complete(last_info):
            elapsed = time.monotonic() - started
            print(f"Truncate complete: set={set_name} elapsed={elapsed:.1f}s stats={last_info or '(absent)'}")
            return

        now = time.monotonic()
        if now - started >= timeout_secs:
            die(
                f"Timed out after {timeout_secs:.1f}s waiting for truncate completion "
                f"for set '{set_name}'. Last stats: {last_info or '(empty)'}"
            )

        if now >= next_log_at:
            elapsed = now - started
            print(f"Waiting for truncate: set={set_name} elapsed={elapsed:.1f}s stats={last_info}")
            next_log_at = now + max(10.0, interval_secs)

        time.sleep(interval_secs)


def print_dry_run(cmd: list[str]) -> None:
    rendered = " ".join(subprocess.list2cmdline([part]) for part in cmd)
    print(f"[dry-run] {rendered}")


def run_tools(
    args: argparse.Namespace,
    network: str,
    tool_args: list[str],
) -> None:
    cmd = ["docker", "run", "--rm", "--network", network, args.tools_image, *tool_args]
    if args.dry_run:
        print_dry_run(cmd)
        return
    run(cmd)


def add_worker_connection_args(cmd: list[str], args: argparse.Namespace, host: str, port: str) -> None:
    cmd += ["--host", host, "--port", port]
    if args.user:
        cmd += ["--user", args.user]
    if args.password:
        cmd += ["--password", args.password]
    if args.auth:
        cmd += ["--auth", args.auth]
    if args.services_alternate:
        cmd += ["--services-alternate"]
    if args.tls_enable:
        cmd += ["--tls-enable"]
    if args.tls_name:
        cmd += ["--tls-name", args.tls_name]
    if args.tls_cafile:
        cmd += ["--tls-cafile", args.tls_cafile]
    if args.tls_capath:
        cmd += ["--tls-capath", args.tls_capath]


def run_worker(
    args: argparse.Namespace,
    network: str,
    mount_dir: Path,
    worker_args: list[str],
) -> None:
    script_path = Path(__file__).resolve()
    install_cmd = "python -m pip install --quiet aerospike"
    worker_cmd = "exec python " + " ".join(
        shlex.quote(part) for part in [SCRIPT_MOUNT_PATH, *worker_args]
    )
    shell_cmd = f"{install_cmd} && {worker_cmd}"

    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        f"{mount_dir}:{TOOLS_MOUNT_DIR}",
        "-v",
        f"{script_path}:{SCRIPT_MOUNT_PATH}:ro",
        args.helper_image,
        "sh",
        "-lc",
        shell_cmd,
    ]

    if args.dry_run:
        print_dry_run(cmd)
        return
    run(cmd)


def cmd_backup(args: argparse.Namespace) -> None:
    require_docker()

    output_file = Path(args.output_file).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    container = detect_aerospike_container(args.container)
    network = detect_network(container, args.network)
    host = detect_host(container, network, args.host)
    port = detect_port(args.tools_image, network, host, args.port)
    cluster_args = build_cluster_args(args, host, port)

    print(f"Using container: {container}")
    print(f"Using network:   {network}")
    print(f"Using host:      {host}")
    print(f"Using port:      {port}")
    print(f"Using namespace: {args.namespace}")
    print(f"Using output:    {output_file}")

    matched_sets = [set_name for set_name in list_sets(args.tools_image, network, cluster_args, args.namespace) if set_name.startswith(args.source_prefix)]
    if not matched_sets:
        die(f"No sets in namespace '{args.namespace}' matched prefix '{args.source_prefix}'")

    print("Sets to backup:")
    for set_name in matched_sets:
        print(f"  {set_name}")

    if output_file.exists() and not args.overwrite:
        die(f"Output file already exists: {output_file}; use --overwrite to replace it")

    worker_args = ["__backup_worker", "--namespace", args.namespace, "--output-file", f"{TOOLS_MOUNT_DIR}/{output_file.name}", "--fail-on-missing-key"]
    add_worker_connection_args(worker_args, args, host, port)
    for set_name in matched_sets:
        worker_args += ["--set", set_name]

    run_worker(args, network, output_file.parent, worker_args)


def cmd_restore(args: argparse.Namespace) -> None:
    require_docker()

    input_file = Path(args.input_file).resolve()
    if not input_file.is_file():
        die(f"Input file does not exist: {input_file}")

    container = detect_aerospike_container(args.container)
    network = detect_network(container, args.network)
    host = detect_host(container, network, args.host)
    port = detect_port(args.tools_image, network, host, args.port)

    print(f"Using container: {container}")
    print(f"Using network:   {network}")
    print(f"Using host:      {host}")
    print(f"Using port:      {port}")
    print(f"Using namespace: {args.namespace}")
    print(f"Using input:     {input_file}")
    print(f"Source prefix:   {args.source_prefix}")
    print(f"Dest prefix:     {args.dest_prefix}")
    print(f"Restore mode:    {args.mode}")

    worker_args = [
        "__restore_worker",
        "--namespace",
        args.namespace,
        "--input-file",
        f"{TOOLS_MOUNT_DIR}/{input_file.name}",
        "--source-prefix",
        args.source_prefix,
        "--dest-prefix",
        args.dest_prefix,
        "--mode",
        args.mode,
    ]
    add_worker_connection_args(worker_args, args, host, port)

    run_worker(args, network, input_file.parent, worker_args)


def cmd_truncate_prefix(args: argparse.Namespace) -> None:
    require_docker()
    if args.wait_timeout_secs <= 0:
        die("--wait-timeout-secs must be positive")
    if args.wait_interval_secs <= 0:
        die("--wait-interval-secs must be positive")

    container = detect_aerospike_container(args.container)
    network = detect_network(container, args.network)
    host = detect_host(container, network, args.host)
    port = detect_port(args.tools_image, network, host, args.port)
    cluster_args = build_cluster_args(args, host, port)

    print(f"Using container: {container}")
    print(f"Using network:   {network}")
    print(f"Using host:      {host}")
    print(f"Using port:      {port}")
    print(f"Using namespace: {args.namespace}")
    print(f"Using prefix:    {args.prefix}")
    print(f"Wait enabled:    {not args.no_wait}")
    if not args.no_wait:
        print(f"Wait timeout:    {args.wait_timeout_secs}s")
        print(f"Wait interval:   {args.wait_interval_secs}s")

    matched_sets = [
        set_name
        for set_name in list_sets(args.tools_image, network, cluster_args, args.namespace)
        if set_name.startswith(args.prefix)
    ]

    if not matched_sets:
        print("Sets to truncate:")
        print("  (none)")
        return

    print("Sets to truncate:")
    for set_name in matched_sets:
        print(f"  {set_name}")

    for set_name in matched_sets:
        truncate_args = [
            "asinfo",
            *cluster_args,
            "-v",
            f"truncate:namespace={args.namespace};set={set_name}",
        ]
        run_tools(args, network, truncate_args)
        if not args.no_wait and not args.dry_run:
            wait_for_truncate_complete(
                args.tools_image,
                network,
                cluster_args,
                args.namespace,
                set_name,
                args.wait_timeout_secs,
                args.wait_interval_secs,
            )


def cmd_digest_prefix(args: argparse.Namespace) -> None:
    require_docker()

    container = detect_aerospike_container(args.container)
    network = detect_network(container, args.network)
    host = detect_host(container, network, args.host)
    port = detect_port(args.tools_image, network, host, args.port)
    cluster_args = build_cluster_args(args, host, port)

    print(f"Using container: {container}")
    print(f"Using network:   {network}")
    print(f"Using host:      {host}")
    print(f"Using port:      {port}")
    print(f"Using namespace: {args.namespace}")
    print(f"Using prefix:    {args.prefix}")

    matched_sets = [
        set_name
        for set_name in list_sets(args.tools_image, network, cluster_args, args.namespace)
        if set_name.startswith(args.prefix)
    ]

    print("Sets to digest:")
    for set_name in matched_sets:
        print(f"  {set_name}")
    if not matched_sets:
        print("  (none)")
        empty_digest = hashlib.sha256().hexdigest()
        print(f"OVERALL prefix={args.prefix} records=0 digest={empty_digest}")
        return

    worker_args = ["__digest_worker", "--namespace", args.namespace, "--prefix", args.prefix]
    add_worker_connection_args(worker_args, args, host, port)
    for set_name in matched_sets:
        worker_args += ["--set", set_name]

    run_worker(args, network, Path.cwd(), worker_args)


def build_python_config(args: argparse.Namespace) -> dict[str, object]:
    config: dict[str, object] = {"hosts": [(args.host, int(args.port))]}
    if args.services_alternate:
        config["use_services_alternate"] = True
    if args.tls_enable:
        tls_config: dict[str, object] = {}
        if args.tls_name:
            tls_config["cafile"] = args.tls_cafile
        if args.tls_capath:
            tls_config["capath"] = args.tls_capath
        if args.tls_cafile:
            tls_config["cafile"] = args.tls_cafile
        config["tls"] = tls_config or {"enable": True}
    if args.auth:
        config["auth_mode"] = args.auth
    return config


def encode_value(value: object) -> object:
    if value is None:
        return {"t": "null"}
    if isinstance(value, bool):
        return {"t": "bool", "v": value}
    if isinstance(value, int) and not isinstance(value, bool):
        return {"t": "int", "v": value}
    if isinstance(value, float):
        return {"t": "float", "v": value}
    if isinstance(value, str):
        return {"t": "str", "v": value}
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {"t": "bytes", "v": base64.b64encode(raw).decode("ascii")}
    if isinstance(value, list):
        return {"t": "list", "v": [encode_value(item) for item in value]}
    if isinstance(value, tuple):
        return {"t": "tuple", "v": [encode_value(item) for item in value]}
    if isinstance(value, dict):
        return {
            "t": "map",
            "v": [
                [encode_value(key), encode_value(item)]
                for key, item in sorted(value.items(), key=lambda item: repr(item[0]))
            ],
        }
    raise TypeError(f"Unsupported value type: {type(value).__name__}")


def decode_value(value: object) -> object:
    if not isinstance(value, dict):
        raise TypeError(f"Malformed encoded value: {value!r}")
    value_type = value.get("t")
    if value_type == "null":
        return None
    if value_type in {"bool", "int", "float", "str"}:
        return value["v"]
    if value_type == "bytes":
        return base64.b64decode(value["v"])
    if value_type == "list":
        return [decode_value(item) for item in value["v"]]
    if value_type == "tuple":
        return tuple(decode_value(item) for item in value["v"])
    if value_type == "map":
        return {decode_value(key): decode_value(item) for key, item in value["v"]}
    raise TypeError(f"Unsupported encoded value type: {value_type!r}")


def connect_client(args: argparse.Namespace):
    import aerospike

    config = build_python_config(args)
    client = aerospike.Client(config)
    if args.user:
        return client.connect(args.user, args.password or "")
    return client.connect()


def record_payload(namespace: str, set_name: str, key: tuple[str, str, object], bins: dict[str, object]) -> dict[str, object]:
    user_key = key[2]
    if user_key is None:
        if len(key) < 4 or key[3] is None:
            raise RuntimeError(f"Missing stored user key and digest for {namespace}/{set_name}")
        key_payload = {
            "kind": "digest",
            "digest": encode_value(bytes(key[3])),
        }
    else:
        key_payload = {
            "kind": "user_key",
            "value": encode_value(user_key),
        }
    return {
        "namespace": namespace,
        "set": set_name,
        "key": key_payload,
        "bins": {name: encode_value(bins[name]) for name in sorted(bins)},
    }


def canonical_record_bytes(namespace: str, set_name: str, key: tuple[str, str, object], bins: dict[str, object]) -> bytes:
    payload = record_payload(namespace, set_name, key, bins)
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")


def cmd_backup_worker(args: argparse.Namespace) -> None:
    client = connect_client(args)
    output_file = Path(args.output_file)
    record_count = 0
    digest_only_count = 0

    try:
        with output_file.open("w", encoding="utf-8") as fh:
            for set_name in args.sets:
                scan = client.scan(args.namespace, set_name)

                def on_record(record: tuple[tuple[str, str, object], dict[str, object], dict[str, object]]) -> None:
                    nonlocal record_count
                    nonlocal digest_only_count

                    key, _meta, bins = record
                    if key[2] is None:
                        digest_only_count += 1
                    line = record_payload(args.namespace, set_name, key, bins)
                    fh.write(json.dumps(line, separators=(",", ":")))
                    fh.write("\n")
                    record_count += 1

                scan.foreach(on_record)
    finally:
        client.close()

    print(f"Backed up {record_count} records into {output_file} (digest_only={digest_only_count})")


def cmd_digest_worker(args: argparse.Namespace) -> None:
    client = connect_client(args)
    overall_record_digests: list[bytes] = []

    try:
        for set_name in sorted(args.sets):
            set_record_digests: list[bytes] = []
            scan = client.scan(args.namespace, set_name)

            def on_record(record: tuple[tuple[str, str, object], dict[str, object], dict[str, object]]) -> None:
                key, _meta, bins = record
                set_record_digests.append(
                    hashlib.sha256(canonical_record_bytes(args.namespace, set_name, key, bins)).digest()
                )

            scan.foreach(on_record)
            set_record_digests.sort()

            set_hasher = hashlib.sha256()
            for record_digest in set_record_digests:
                set_hasher.update(record_digest)
            set_digest = set_hasher.hexdigest()
            overall_record_digests.extend(set_record_digests)
            print(f"SET set={set_name} records={len(set_record_digests)} digest={set_digest}")
    finally:
        client.close()

    overall_record_digests.sort()
    overall_hasher = hashlib.sha256()
    for record_digest in overall_record_digests:
        overall_hasher.update(record_digest)
    print(f"OVERALL prefix={args.prefix} records={len(overall_record_digests)} digest={overall_hasher.hexdigest()}")


def cmd_restore_worker(args: argparse.Namespace) -> None:
    import aerospike

    client = connect_client(args)
    input_file = Path(args.input_file)
    restored = 0
    skipped = 0
    skipped_missing_user_key = 0

    policy: dict[str, object] = {}
    policy["key"] = aerospike.POLICY_KEY_SEND
    if args.mode == "replace":
        policy["exists"] = aerospike.POLICY_EXISTS_REPLACE
    elif args.mode == "update":
        policy["exists"] = aerospike.POLICY_EXISTS_IGNORE
    elif args.mode == "create_only":
        policy["exists"] = aerospike.POLICY_EXISTS_CREATE

    try:
        with input_file.open("r", encoding="utf-8") as fh:
            for line_number, raw_line in enumerate(fh, start=1):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue

                payload = json.loads(raw_line)
                source_set = payload["set"]
                if not source_set.startswith(args.source_prefix):
                    skipped += 1
                    continue

                dest_set = args.dest_prefix + source_set[len(args.source_prefix) :]
                key_payload = payload["key"]
                if key_payload["kind"] != "user_key":
                    skipped += 1
                    skipped_missing_user_key += 1
                    continue
                user_key = decode_value(key_payload["value"])
                if isinstance(user_key, bytes):
                    user_key = bytearray(user_key)
                bins = {name: decode_value(value) for name, value in payload["bins"].items()}
                key = (args.namespace, dest_set, user_key)
                try:
                    client.put(key, bins, policy=policy)
                except Exception as exc:  # noqa: BLE001
                    raise RuntimeError(f"Failed to restore line {line_number} into set {dest_set}: {exc}") from exc
                restored += 1
    finally:
        client.close()

    print(
        f"Restored {restored} records from {input_file}; "
        f"skipped {skipped} records (missing_user_key={skipped_missing_user_key})"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aerospike backup/restore helper with JSONL export/import")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def add_common(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--container")
        subparser.add_argument("--network")
        subparser.add_argument("--host")
        subparser.add_argument("--port")
        subparser.add_argument("--user")
        subparser.add_argument("--password")
        subparser.add_argument("--auth")
        subparser.add_argument("--services-alternate", action="store_true")
        subparser.add_argument("--tls-enable", action="store_true")
        subparser.add_argument("--tls-name")
        subparser.add_argument("--tls-cafile")
        subparser.add_argument("--tls-capath")
        subparser.add_argument("--tools-image", default=TOOLS_IMAGE_DEFAULT)
        subparser.add_argument("--helper-image", default=HELPER_IMAGE_DEFAULT)
        subparser.add_argument("--dry-run", action="store_true")

    backup = subparsers.add_parser("backup", help="Export all records from sets with a prefix into one JSONL file")
    backup.add_argument("--namespace", default="node")
    backup.add_argument("--source-prefix", required=True)
    backup.add_argument("--output-file", required=True)
    backup.add_argument("--overwrite", action="store_true")
    add_common(backup)
    backup.set_defaults(func=cmd_backup)

    restore = subparsers.add_parser("restore", help="Import records from one JSONL file and rewrite set prefixes")
    restore.add_argument("--namespace", default="node")
    restore.add_argument("--input-file", required=True)
    restore.add_argument("--source-prefix", required=True)
    restore.add_argument("--dest-prefix", required=True)
    restore.add_argument("--mode", choices=["create_only", "replace", "update"], default="update")
    add_common(restore)
    restore.set_defaults(func=cmd_restore)

    truncate = subparsers.add_parser("truncate-prefix", help="Truncate all sets in a namespace matching a prefix")
    truncate.add_argument("--namespace", default="node")
    truncate.add_argument("--prefix", required=True)
    truncate.add_argument("--wait-timeout-secs", type=float, default=1200.0)
    truncate.add_argument("--wait-interval-secs", type=float, default=1.0)
    truncate.add_argument("--no-wait", action="store_true")
    add_common(truncate)
    truncate.set_defaults(func=cmd_truncate_prefix)

    digest = subparsers.add_parser("digest-prefix", help="Compute order-independent digests for all sets matching a prefix")
    digest.add_argument("--namespace", default="node")
    digest.add_argument("--prefix", required=True)
    add_common(digest)
    digest.set_defaults(func=cmd_digest_prefix)

    worker_backup = subparsers.add_parser("__backup_worker", help=argparse.SUPPRESS)
    worker_backup.add_argument("--host", required=True)
    worker_backup.add_argument("--port", required=True)
    worker_backup.add_argument("--user")
    worker_backup.add_argument("--password")
    worker_backup.add_argument("--auth")
    worker_backup.add_argument("--services-alternate", action="store_true")
    worker_backup.add_argument("--tls-enable", action="store_true")
    worker_backup.add_argument("--tls-name")
    worker_backup.add_argument("--tls-cafile")
    worker_backup.add_argument("--tls-capath")
    worker_backup.add_argument("--namespace", required=True)
    worker_backup.add_argument("--output-file", required=True)
    worker_backup.add_argument("--set", dest="sets", action="append", required=True)
    worker_backup.add_argument("--fail-on-missing-key", action="store_true")
    worker_backup.set_defaults(func=cmd_backup_worker)

    worker_digest = subparsers.add_parser("__digest_worker", help=argparse.SUPPRESS)
    worker_digest.add_argument("--host", required=True)
    worker_digest.add_argument("--port", required=True)
    worker_digest.add_argument("--user")
    worker_digest.add_argument("--password")
    worker_digest.add_argument("--auth")
    worker_digest.add_argument("--services-alternate", action="store_true")
    worker_digest.add_argument("--tls-enable", action="store_true")
    worker_digest.add_argument("--tls-name")
    worker_digest.add_argument("--tls-cafile")
    worker_digest.add_argument("--tls-capath")
    worker_digest.add_argument("--namespace", required=True)
    worker_digest.add_argument("--prefix", required=True)
    worker_digest.add_argument("--set", dest="sets", action="append", required=True)
    worker_digest.set_defaults(func=cmd_digest_worker)

    worker_restore = subparsers.add_parser("__restore_worker", help=argparse.SUPPRESS)
    worker_restore.add_argument("--host", required=True)
    worker_restore.add_argument("--port", required=True)
    worker_restore.add_argument("--user")
    worker_restore.add_argument("--password")
    worker_restore.add_argument("--auth")
    worker_restore.add_argument("--services-alternate", action="store_true")
    worker_restore.add_argument("--tls-enable", action="store_true")
    worker_restore.add_argument("--tls-name")
    worker_restore.add_argument("--tls-cafile")
    worker_restore.add_argument("--tls-capath")
    worker_restore.add_argument("--namespace", required=True)
    worker_restore.add_argument("--input-file", required=True)
    worker_restore.add_argument("--source-prefix", required=True)
    worker_restore.add_argument("--dest-prefix", required=True)
    worker_restore.add_argument("--mode", choices=["create_only", "replace", "update"], default="update")
    worker_restore.set_defaults(func=cmd_restore_worker)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
