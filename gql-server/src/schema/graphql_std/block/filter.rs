// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::InputObject;
use serde::Serialize;
use serde_with::with_prefix;

use crate::schema::graphql::filter::OptBooleanFilter;
use crate::schema::graphql::filter::OptFloatFilter;
use crate::schema::graphql::filter::OptIntFilter;
use crate::schema::graphql::filter::OptStringFilter;
use crate::schema::graphql::filter::WhereOp;

#[derive(InputObject, Debug, Serialize, Default)]
#[graphql(rename_fields = "snake_case")]
pub struct ExtBlkRefFilter {
    pub end_lt: OptStringFilter,
    pub file_hash: OptStringFilter,
    pub root_hash: OptStringFilter,
    pub seq_no: OptFloatFilter,
    #[graphql(name = "OR")]
    pub or: Option<Box<ExtBlkRefFilter>>,
}

#[derive(InputObject, Debug, Serialize, Default)]
#[graphql(rename_fields = "snake_case")]
pub struct BlockFilter {
    pub id: OptStringFilter,
    pub after_merge: OptBooleanFilter,
    pub after_split: OptBooleanFilter,
    pub flags: OptIntFilter,
    pub gen_utime: OptFloatFilter,
    pub key_block: OptBooleanFilter,
    #[serde(flatten, with = "prefix_prev_alt_ref")]
    pub prev_alt_ref: Option<ExtBlkRefFilter>,
    #[serde(flatten, with = "prefix_prev_ref")]
    pub prev_ref: Option<ExtBlkRefFilter>,
    pub shard: OptStringFilter,
    pub status: OptIntFilter,
    pub tr_count: OptIntFilter,
    pub workchain_id: OptIntFilter,
    #[graphql(name = "OR")]
    pub or: Option<Box<BlockFilter>>,
}

with_prefix!(prefix_prev_ref "prev_ref_");
with_prefix!(prefix_prev_alt_ref "prev_alt_ref_");

impl WhereOp for BlockFilter {}

#[cfg(test)]
pub mod tests {
    use crate::schema::graphql::block::filter::ExtBlkRefFilter;
    use crate::schema::graphql::block::BlockFilter;
    use crate::schema::graphql::filter::BooleanFilter;
    use crate::schema::graphql::filter::FloatFilter;
    use crate::schema::graphql::filter::IntFilter;
    use crate::schema::graphql::filter::StringFilter;
    use crate::schema::graphql::filter::WhereOp;

    #[test]
    fn test_block_filter_1() {
        let bf = BlockFilter {
            key_block: Some(BooleanFilter { eq: Some(true), ne: None }),
            workchain_id: Some(IntFilter { eq: Some(-1), ..Default::default() }),
            ..Default::default()
        };

        assert_eq!(bf.to_where().unwrap(), format!("WHERE key_block = true AND workchain_id = -1"));
    }

    #[test]
    fn test_block_filter_2() {
        let bf = BlockFilter { ..Default::default() };

        assert_eq!(bf.to_where(), None);
    }

    #[test]
    fn test_block_filter_3() {
        let bf = BlockFilter {
            prev_ref: Some(ExtBlkRefFilter {
                root_hash: Some(StringFilter {
                    eq: Some(
                        "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7".into(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            bf.to_where().unwrap(),
            "WHERE prev_ref_root_hash = \"75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7\"".to_owned()
        );
    }

    #[test]
    fn test_block_filter_4() {
        let bf = BlockFilter {
            prev_ref: Some(ExtBlkRefFilter {
                root_hash: Some(StringFilter {
                    eq: Some("ROOT_HASH".to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            or: Some(Box::new(BlockFilter {
                prev_alt_ref: Some(ExtBlkRefFilter {
                    root_hash: Some(StringFilter {
                        eq: Some("ALT_ROOT_HASH".to_owned()),
                        ..Default::default()
                    }),
                    seq_no: Some(FloatFilter { eq: Some(99f64), ..Default::default() }),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };

        assert_eq!(
            bf.to_where().unwrap(),
            format!(
                "WHERE prev_ref_root_hash = {:?} OR (prev_alt_ref_root_hash = {:?} AND prev_alt_ref_seq_no = {:?})",
                "ROOT_HASH", "ALT_ROOT_HASH", 99f64
            )
        );
    }
}
