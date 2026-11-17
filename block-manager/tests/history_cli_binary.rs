// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

//! The `history-proofs` subcommands as the playbook runs them: through the
//! binary, reading stdout and stderr.

use std::path::Path;
use std::process::Command;
use std::process::Output;

use database::sqlite::history_proofs::HistoryProofsStore;
use database::sqlite::history_proofs::HistoryRow;
use testdir::testdir;

fn history_proofs(sqlite_path: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_block-manager"))
        .arg("history-proofs")
        .args(args)
        .arg("--sqlite-path")
        .arg(sqlite_path)
        .output()
        .unwrap()
}

/// stderr holds the command's own lines only: the playbook shows them as the
/// reasons behind an exit code, and the version banner is not one.
#[test]
fn history_proofs_stderr_has_no_banner() {
    let dir = testdir!();
    let output = Command::new(env!("CARGO_BIN_EXE_block-manager"))
        .args(["history-proofs", "gaps", "--json", "--sqlite-path"])
        .arg(&dir)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8(output.stderr).unwrap();
    let lines: Vec<&str> = stderr.lines().collect();
    assert_eq!(lines.len(), 1, "{stderr}");
    assert!(lines[0].starts_with("error: ") && lines[0].ends_with(": no such file"), "{stderr}");
}

/// With `--json`, as the playbook runs it, the problems go to stderr, and an
/// import of bad data exits 3 on every run until the store is restored.
#[test]
fn import_reports_bad_data_on_stderr_and_again_on_rerun() {
    let root = testdir!();
    let (source, target) = (root.join("source"), root.join("target"));
    let file = root.join("export.db");
    for dir in [&source, &target] {
        std::fs::create_dir_all(dir).unwrap();
    }
    HistoryProofsStore::open(&target).unwrap();
    // A layer boundary whose leaves are not the ones of its block id.
    let bad = HistoryRow {
        height: 128,
        block_id: [0x12; 32],
        envelope_hash: [0xe0; 32],
        tracked_root: [0; 32],
        tracked_hashes: None,
        history_proofs: Some([vec![1, 1], vec![0x61; 32]].concat()),
        block_merkle_leaves: Some(vec![0x42; 512]),
        gen_utime: Some(1_791_000_000),
    };
    HistoryProofsStore::open(&source).unwrap().insert_block(&bad, &[]).unwrap();
    let file_arg = file.to_str().unwrap();
    let export = history_proofs(
        &source,
        &["export", "--from-height", "128", "--to-height", "128", "--out", file_arg, "--json"],
    );
    assert_eq!(export.status.code(), Some(0), "{export:?}");

    let first = history_proofs(&target, &["import", "--from", file_arg, "--json"]);
    assert_eq!(first.status.code(), Some(3), "{first:?}");
    let stderr = String::from_utf8(first.stderr).unwrap();
    assert!(stderr.contains("check merkle_leaves failed at heights 128..=128: "), "{stderr}");

    let again = history_proofs(&target, &["import", "--from", file_arg, "--json"]);
    assert_eq!(again.status.code(), Some(3), "{again:?}");
    let stderr = String::from_utf8(again.stderr).unwrap();
    assert!(
        stderr.contains("rows of the range that failed their checks (1 heights): 128..=128"),
        "{stderr}"
    );
}

/// export prints at most 20 refused rows, then how many more.
#[test]
fn export_prints_a_bounded_list_of_refused_rows() {
    let root = testdir!();
    let source = root.join("source");
    std::fs::create_dir_all(&source).unwrap();
    {
        let mut store = HistoryProofsStore::open(&source).unwrap();
        for height in 1..=40u64 {
            let mut block_id = [0x34; 32];
            block_id[24..].copy_from_slice(&height.to_be_bytes());
            let row = HistoryRow {
                height,
                block_id,
                envelope_hash: [0xe0; 32],
                tracked_root: [0; 32],
                tracked_hashes: None,
                history_proofs: None,
                block_merkle_leaves: None,
                gen_utime: Some(1_791_000_000),
            };
            store.insert_block(&row, &[]).unwrap();
        }
        // Leaves off a layer boundary without a BK set update: refused.
        store
            .connection()
            .execute("UPDATE blocks SET block_merkle_leaves = zeroblob(512)", [])
            .unwrap();
    }
    let file = root.join("export.db");
    let output = history_proofs(
        &source,
        &[
            "export",
            "--from-height",
            "1",
            "--to-height",
            "40",
            "--out",
            file.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    let refused = stderr.lines().filter(|line| line.starts_with("row at height")).count();
    assert_eq!(refused, 20, "{stderr}");
    assert!(stderr.contains("... and 20 more rows not exported"), "{stderr}");
}

/// A wrong argument of the service still names the version; history-proofs
/// keeps its stderr to its own lines.
#[test]
fn banner_only_on_service_argument_errors() {
    let service = Command::new(env!("CARGO_BIN_EXE_block-manager"))
        .env("REST_API", "not an address")
        .output()
        .unwrap();
    assert_eq!(service.status.code(), Some(2), "{service:?}");
    let stderr = String::from_utf8(service.stderr).unwrap();
    assert!(stderr.starts_with("Starting Acki-Nacki Block Manager, version"), "{stderr}");
    assert!(stderr.contains("REST_API") || stderr.contains("--rest-api"), "{stderr}");

    let cli = history_proofs(&testdir!(), &["gaps", "--no-such-flag"]);
    assert_eq!(cli.status.code(), Some(2), "{cli:?}");
    let stderr = String::from_utf8(cli.stderr).unwrap();
    assert!(stderr.starts_with("error: "), "{stderr}");
}

#[test]
fn help_and_version_exit_0() {
    for args in [&["--help"][..], &["--version"], &["history-proofs", "gaps", "--help"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_block-manager")).args(args).output().unwrap();
        assert_eq!(output.status.code(), Some(0), "{args:?}: {output:?}");
        assert!(!output.stdout.is_empty(), "{args:?}");
    }
}

/// export names the rows it left out because they failed their checks on
/// the source; the playbook shows this stderr when the task fails.
#[test]
fn export_names_the_rows_that_failed_their_checks_on_stderr() {
    let root = testdir!();
    let source = root.join("source");
    std::fs::create_dir_all(&source).unwrap();
    {
        let mut store = HistoryProofsStore::open(&source).unwrap();
        for height in 1..=3u64 {
            let mut block_id = [0x56; 32];
            block_id[24..].copy_from_slice(&height.to_be_bytes());
            let row = HistoryRow {
                height,
                block_id,
                envelope_hash: [0xe0; 32],
                tracked_root: [0; 32],
                tracked_hashes: None,
                history_proofs: None,
                block_merkle_leaves: None,
                gen_utime: Some(1_791_000_000),
            };
            store.insert_block(&row, &[]).unwrap();
        }
        // The source's verifier failed the Merkle leaves of row 2.
        store
            .connection()
            .execute("INSERT INTO check_state (height, done, failed) VALUES (2, 2, 2)", [])
            .unwrap();
    }
    let file = root.join("export.db");
    let output = history_proofs(
        &source,
        &[
            "export",
            "--from-height",
            "1",
            "--to-height",
            "3",
            "--out",
            file.to_str().unwrap(),
            "--json",
        ],
    );
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(
            "rows not exported, they failed their checks on this block-manager (1 heights): 2..=2"
        ),
        "{stderr}"
    );
}
