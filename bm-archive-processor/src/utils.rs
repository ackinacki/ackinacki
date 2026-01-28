// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::path::Path;

/// Extract timestamp from file name: "bm-archive-TIMESTAMP.db"
pub fn parse_timestamp_from_filename(path: &Path) -> Option<i64> {
    let file = path.file_name()?.to_str()?;

    const PREFIX: &str = "bm-archive-";
    const SUFFIX: &str = ".db";
    if !file.starts_with(PREFIX) || !file.ends_with(SUFFIX) {
        return None;
    }
    let ts_str = &file[PREFIX.len()..file.len() - SUFFIX.len()];
    if ts_str.is_empty() || !ts_str.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    ts_str.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;
    use crate::config::AppConfig;
    use crate::domain::config::ProcessingRules;
    use crate::domain::grouping::group_by_timestamp;
    use crate::domain::traits::FileSystemClient;
    use crate::infra::file_storage::FileSystemClientImpl;
    use crate::Args;

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"").unwrap();
    }

    fn make_db_path(base: &Path, server: &str, ts: i64) -> PathBuf {
        base.join("incoming").join(server).join(format!("bm-archive-{ts}.db"))
    }

    fn build_tree(tmp: &TempDir, layout: &[(&str, &[i64])]) -> PathBuf {
        let root = tmp.path().to_path_buf();
        for (srv, tss) in layout {
            for ts in *tss {
                let p = make_db_path(&root, srv, *ts);
                touch(&p);
            }
        }
        root
    }

    #[test]
    fn test_parse_ok() {
        assert_eq!(
            parse_timestamp_from_filename(Path::new("bm-archive-1760158801.db")),
            Some(1760158801)
        );
    }

    #[test]
    fn test_parse_fail() {
        assert_eq!(parse_timestamp_from_filename(Path::new("x-176.db")), None);
        assert_eq!(parse_timestamp_from_filename(Path::new("bm-archive-foo.db")), None);
        assert_eq!(parse_timestamp_from_filename(Path::new("bm-archive-176.db-wrong")), None);
    }

    #[test]
    fn test_find_groups_full_match_require_all() {
        let tmp = TempDir::new().unwrap();
        let root = build_tree(
            &tmp,
            &[
                ("1", &[1759899601, 1759986001, 1760158802]),
                ("2", &[1759899601, 1759986001, 1760158801]),
                ("3", &[1759899601, 1759986002, 1760158801]),
            ],
        );

        let args = Args {
            root: root.clone(),
            incoming: "incoming".to_string(),
            daily: "daily".to_string(),
            processed: "processed".to_string(),
            compress: true,
            require_all_servers: true,
            full_db: PathBuf::from("./full.db"),
            bucket: None,
            skip_upload: true,
            dry_run: true,
        };

        let cfg = AppConfig::from_args(args);
        let fs_client = FileSystemClientImpl::new(cfg.incoming_dir.clone());
        let input_files = fs_client.get_arch_files().unwrap();
        let rules = ProcessingRules::builder().require_all_servers(cfg.require_all_servers).build();

        let groups =
            group_by_timestamp(input_files, rules.match_window_sec, rules.require_all_servers);
        println!("GROUPS: {groups:#?}");

        assert_eq!(groups.len(), 3);

        let anchors: Vec<i64> = groups.iter().map(|g| g.anchor_timestamp).collect();
        assert_eq!(anchors, vec![1759899601, 1759986001, 1760158801]);

        for group in &groups {
            assert_eq!(group.file_count(), 3);
        }

        // strict equal ts
        let g1_paths = &groups[0].paths();
        for srv in ["1", "2", "3"] {
            let expect = make_db_path(&root, srv, 1759899601);
            assert!(g1_paths.iter().any(|p| p == &expect), "missing {}", expect.display());
        }

        // 3rd (+1 sec) should be collected
        let g2_paths = &groups[1].paths();
        assert!(g2_paths.iter().any(|p| p == &make_db_path(&root, "1", 1759986001)));
        assert!(g2_paths.iter().any(|p| p == &make_db_path(&root, "2", 1759986001)));
        assert!(g2_paths.iter().any(|p| p == &make_db_path(&root, "3", 1759986002)));

        // 1st should be collected
        let g3_paths = &groups[2].paths();
        assert!(g3_paths.iter().any(|p| p == &make_db_path(&root, "1", 1760158802)));
        assert!(g3_paths.iter().any(|p| p == &make_db_path(&root, "2", 1760158801)));
        assert!(g3_paths.iter().any(|p| p == &make_db_path(&root, "3", 1760158801)));
    }

    #[test]
    fn test_find_groups_partial_when_not_require_all() {
        let tmp = TempDir::new().unwrap();

        let root = build_tree(
            &tmp,
            &[
                ("1", &[1759899601, 1759986001, 1760158802]),
                ("2", &[1759899601, 1759986001, 1760158801]),
                ("3", &[1759899601, /* skip: 1759986002 */ 1760158801]),
            ],
        );

        let args = Args {
            root: root.clone(),
            incoming: "incoming".to_string(),
            daily: "daily".to_string(),
            processed: "processed".to_string(),
            compress: true,
            require_all_servers: false,
            full_db: PathBuf::from("./full.db"),
            bucket: None,
            skip_upload: false,
            dry_run: false,
        };

        let cfg = AppConfig::from_args(args);

        let fs_client = FileSystemClientImpl::new(cfg.incoming_dir.clone());
        let input_files = fs_client.get_arch_files().unwrap();
        let rules = ProcessingRules::builder().require_all_servers(cfg.require_all_servers).build();

        let groups =
            group_by_timestamp(input_files, rules.match_window_sec, rules.require_all_servers);
        println!("GROUPS: {groups:#?}");

        assert!(groups.len() == 3);

        // check 2nd group
        let mid =
            groups.iter().find(|g| g.anchor_timestamp == 1759986001).expect("mid group missing");
        assert_eq!(mid.file_count(), 2);
        let mid_paths = mid.paths();
        assert!(mid_paths.iter().any(|p| p == &make_db_path(&root, "1", 1759986001)));
        assert!(mid_paths.iter().any(|p| p == &make_db_path(&root, "2", 1759986001)));
    }
}
