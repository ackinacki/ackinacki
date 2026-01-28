// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::path::PathBuf;

use crate::domain::models::ArchiveGroup;
use crate::domain::traits::AnchorTimestamp;

#[derive(Clone, Debug)]
pub struct ArchiveFile {
    pub _bm_id: String,
    pub ts: i64,
    pub path: PathBuf,
}

pub fn group_by_timestamp(
    input_files: BTreeMap<String, Vec<ArchiveFile>>,
    window_sec: i64,
    require_all_servers: bool,
) -> Vec<ArchiveGroup> {
    // sort by ts
    let sorted_per_server: Vec<Vec<ArchiveFile>> = input_files
        .into_values()
        .map(|mut files| {
            files.sort_unstable_by_key(|a| a.ts);
            files
        })
        .collect();

    // unique ts set
    let mut anchor_ts: BTreeSet<AnchorTimestamp> = BTreeSet::new();

    for files in &sorted_per_server {
        for a in files {
            anchor_ts.insert(a.ts);
        }
    }

    // dedup list
    let mut used_paths: HashSet<PathBuf> = HashSet::new();

    // grouped: ArchiveGroup instances
    let mut groups: Vec<ArchiveGroup> = Vec::new();

    // asc iter
    for anchor in anchor_ts {
        let mut picked: Vec<ArchiveFile> = Vec::new();

        for files in &sorted_per_server {
            if let Some(best) = pick_within_window(files, anchor, window_sec, &used_paths) {
                picked.push(best.clone());
            } else if require_all_servers {
                picked.clear();
                break;
            }
        }

        if !picked.is_empty() && (!require_all_servers || picked.len() == sorted_per_server.len()) {
            for p in &picked {
                used_paths.insert(p.path.clone());
            }
            groups.push(ArchiveGroup::new(anchor, picked));
        }
    }

    groups
}

// Selects a file with a timestamp within the window from the anchor, minimizing |ts - anchor|.
pub fn pick_within_window<'a>(
    files: &'a [ArchiveFile],
    anchor: i64,
    window_secs: i64,
    used_paths: &HashSet<PathBuf>,
) -> Option<&'a ArchiveFile> {
    let mut best: Option<&ArchiveFile> = None;
    let mut best_delta = i64::MAX;

    for a in files {
        if used_paths.contains(&a.path) {
            continue;
        }
        let d = (a.ts - anchor).abs();
        if d <= window_secs {
            if d < best_delta || (d == best_delta && best.map(|b| a.ts < b.ts).unwrap_or(true)) {
                best = Some(a);
                best_delta = d;
            }
        } else if a.ts > anchor + window_secs {
            // already sorted
            break;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::domain::config::ProcessingRules;

    fn af(ts: i64, name: &str) -> ArchiveFile {
        ArchiveFile { _bm_id: "".into(), ts, path: PathBuf::from(name) }
    }

    #[test]
    fn test_single_server_basic_grouping() {
        let mut map = BTreeMap::new();

        map.insert("srv1".into(), vec![af(1000, "a.db"), af(2000, "b.db"), af(3000, "c.db")]);

        let rules = ProcessingRules::builder().require_all_servers(false).build();
        let res = group_by_timestamp(map, rules.match_window_sec, rules.require_all_servers);

        assert_eq!(res.len(), 3);
        assert_eq!(res[0].anchor_timestamp, 1000);
        assert_eq!(res[0].file_count(), 1);
        assert_eq!(res[1].anchor_timestamp, 2000);
        assert_eq!(res[2].anchor_timestamp, 3000);
    }

    #[test]
    fn test_two_servers_matching_within_window() {
        let mut map = BTreeMap::new();

        map.insert("s1".into(), vec![af(1000, "s1a.db"), af(5000, "s1b.db")]);
        map.insert("s2".into(), vec![af(1100, "s2a.db"), af(4900, "s2b.db")]);

        let rules = ProcessingRules::builder().require_all_servers(true).build();
        let res = group_by_timestamp(map, rules.match_window_sec, rules.require_all_servers);

        assert_eq!(res.len(), 2);

        assert_eq!(res[0].anchor_timestamp, 1000);
        assert_eq!(res[0].file_count(), 2);

        assert_eq!(res[1].anchor_timestamp, 4900);
        assert_eq!(res[1].file_count(), 2);
    }

    #[test]
    fn test_require_all_servers_skips_groups() {
        let mut map = BTreeMap::new();

        map.insert("s1".into(), vec![af(1000, "s1a.db")]);
        map.insert("s2".into(), vec![]); // second server empty

        let rules = ProcessingRules::builder().require_all_servers(true).build();
        let res = group_by_timestamp(map, rules.match_window_sec, rules.require_all_servers);

        // No groups can be complete
        assert_eq!(res.len(), 0);
    }

    #[test]
    fn test_used_paths_are_not_reused() {
        let mut map = BTreeMap::new();

        map.insert("srv".into(), vec![af(1000, "a.db"), af(1001, "b.db"), af(1002, "c.db")]);

        let rules = ProcessingRules::builder().require_all_servers(false).build();
        let res = group_by_timestamp(map, rules.match_window_sec, rules.require_all_servers);

        // Three anchors → three groups → each file is used exactly once
        assert_eq!(res.len(), 3);

        assert_eq!(res[0].archive_files[0].path, PathBuf::from("a.db"));
        assert_eq!(res[1].archive_files[0].path, PathBuf::from("b.db"));
        assert_eq!(res[2].archive_files[0].path, PathBuf::from("c.db"));
    }

    #[test]
    fn test_pick_closest_timestamp() {
        let files = vec![
            af(1000, "a.db"),
            af(1100, "b.db"),
            af(900, "c.db"), // same distance as b (1001 anchor -> dist 101)
        ];

        let used = HashSet::new();

        // Anchor very close to 1000
        let best = pick_within_window(&files, 1001, 1000, &used).unwrap();
        // Closest is 1000 (distance 1)
        assert_eq!(best.path, PathBuf::from("a.db"));
    }

    #[test]
    fn test_pick_breaks_after_window() {
        let files = vec![
            af(1000, "a.db"),
            af(2000, "b.db"),
            af(9000, "c.db"), // > window, should stop here
        ];

        let used = HashSet::new();
        let best = pick_within_window(&files, 1000, 1500, &used);

        assert_eq!(best.unwrap().path, PathBuf::from("a.db"));
    }

    #[test]
    fn test_complex_realistic_case() {
        let mut map = BTreeMap::new();
        map.insert(
            "1".into(),
            vec![
                af(1000, "1/bm-1000.db"),
                af(1050, "1/bm-1050.db"),
                af(4620, "1/bm-4620.db"),
                af(4640, "1/bm-4640.db"),
                af(7202, "1/bm-7202.db"),
            ],
        );
        map.insert(
            "2".into(),
            vec![
                af(1000, "2/bm-1000.db"),
                af(1050, "2/bm-1050.db"),
                af(4620, "2/bm-4620.db"),
                af(4640, "2/bm-4640.db"),
                af(7202, "2/bm-7202.db"),
            ],
        );
        let rules = ProcessingRules::builder().require_all_servers(true).build();
        let res = group_by_timestamp(map, rules.match_window_sec, rules.require_all_servers);
        assert_eq!(res.len(), 5);

        assert_eq!(res[0].anchor_timestamp, 1000);
        assert_eq!(res[0].file_count(), 2);

        assert_eq!(res[1].anchor_timestamp, 1050);
        assert_eq!(res[1].file_count(), 2);

        assert_eq!(res[2].anchor_timestamp, 4620);
        assert_eq!(res[2].file_count(), 2);

        assert_eq!(res[3].anchor_timestamp, 4640);
        assert_eq!(res[3].file_count(), 2);

        assert_eq!(res[4].anchor_timestamp, 7202);
        assert_eq!(res[4].file_count(), 2);
    }
}
