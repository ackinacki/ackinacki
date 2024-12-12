// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use async_graphql::InputObject;
use serde::Serialize;
use serde_json::Value;

pub trait WhereOp {
    fn skip_nulls(obj: &mut Value) -> &mut Value {
        if obj.is_object() {
            let obj_mut = obj.as_object_mut().unwrap();
            for (k, mut v) in obj_mut.clone() {
                if v.is_null() {
                    obj_mut.shift_remove(&k);
                } else if v.is_object() {
                    obj_mut[&k] = Self::skip_nulls(&mut v).clone();
                }
            }
        }
        obj
    }

    fn into_str(pair: (String, Value)) -> Option<String> {
        let (field, value) = pair;

        let op = value
            .as_object()
            .unwrap()
            .iter()
            .map(|x| match x {
                (k, v) if k == "eq" => format!("{field} = {v}"),
                (k, v) if k == "ne" => format!("{field} <> {v}"),
                (k, v) if k == "gt" => format!("{field} > {v}"),
                (k, v) if k == "lt" => format!("{field} < {v}"),
                (k, v) if k == "ge" => format!("{field} >= {v}"),
                (k, v) if k == "le" => format!("{field} <= {v}"),
                (k, v) if k == "include" => format!(
                    "{field} IN ({})",
                    v.as_array()
                        .unwrap()
                        .iter()
                        .map(|x| format!("{x}"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                (k, v) if k == "notIn" => format!(
                    "{field} NOT IN ({})",
                    v.as_array()
                        .unwrap()
                        .iter()
                        .map(|x| format!("{x}"))
                        .collect::<Vec<_>>()
                        .join(",")
                ),
                (k, v) if v.is_object() => Self::into_str((k.to_string(), v.clone()))
                    .unwrap()
                    .strip_prefix("AND ")
                    .unwrap()
                    .to_string(),
                (k, v) => {
                    tracing::error!("unsupported filter: {k} => {v}");
                    unreachable!()
                }
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        // let op = ;
        Some(match field.as_str() {
            "or" => format!("OR ({op})"),
            _ => format!("AND {op}"),
        })
    }

    fn to_where(&self) -> Option<String>
    where
        Self: Serialize,
    {
        let mut json = serde_json::to_value(self).unwrap();
        let filtered = Self::skip_nulls(&mut json);
        let filtered = filtered
            .as_object()
            .unwrap()
            .iter()
            .map(|x| Filter::into_str((x.0.to_string(), x.1.clone())).unwrap())
            .collect::<Vec<_>>()
            .join(" ");

        if !filtered.is_empty() {
            Some(format!("WHERE {}", filtered.strip_prefix("AND ").unwrap()))
        } else {
            None
        }
    }
}

pub struct Filter;

impl WhereOp for Filter {}

#[allow(non_snake_case)]
#[derive(InputObject, Debug, Serialize)]
pub struct BooleanFilter {
    pub eq: Option<bool>,
    pub ne: Option<bool>,
}

pub type OptBooleanFilter = Option<BooleanFilter>;

#[allow(non_snake_case)]
#[derive(InputObject, Debug, Serialize, Default)]
pub struct FloatFilter {
    pub eq: Option<f64>,
    pub ne: Option<f64>,
    pub gt: Option<f64>,
    pub lt: Option<f64>,
    pub ge: Option<f64>,
    pub le: Option<f64>,
    #[graphql(name = "in")]
    pub include: Option<Vec<Option<f64>>>,
    pub notIn: Option<Vec<Option<f64>>>,
}

pub type OptFloatFilter = Option<FloatFilter>;

#[allow(non_snake_case)]
#[derive(InputObject, Debug, Serialize, Default)]
pub struct IntFilter {
    pub eq: Option<i32>,
    pub ne: Option<i32>,
    pub gt: Option<i32>,
    pub lt: Option<i32>,
    pub ge: Option<i32>,
    pub le: Option<i32>,
    #[graphql(name = "in")]
    pub include: Option<Vec<Option<i32>>>,
    pub notIn: Option<Vec<Option<i32>>>,
}

pub type OptIntFilter = Option<IntFilter>;

#[allow(non_snake_case)]
#[derive(InputObject, Debug, Serialize, Default)]
pub struct StringFilter {
    pub eq: Option<String>,
    pub ne: Option<String>,
    pub ge: Option<String>,
    pub le: Option<String>,
    #[graphql(name = "in")]
    pub include: Option<Vec<Option<String>>>,
    pub notIn: Option<Vec<Option<String>>>,
}

pub type OptStringFilter = Option<StringFilter>;

#[cfg(test)]
pub mod tests {
    use crate::schema::graphql::block::filter::ExtBlkRefFilter;
    use crate::schema::graphql::filter::Filter;
    use crate::schema::graphql::filter::StringFilter;
    use crate::schema::graphql::filter::WhereOp;
    use crate::schema::graphql::BlockFilter;
    use crate::schema::graphql::{self};

    #[test]
    fn test_boolean_filter() {
        let bf = serde_json::json!({ "after_split": { "eq": true } });
        assert_eq!(
            Filter::into_str(("after_split".to_owned(), bf)).unwrap(),
            "AND after_split = true".to_owned()
        );

        let bf = serde_json::json!({ "after_merge": { "ne": false } });
        assert_eq!(
            Filter::into_str(("after_merge".to_owned(), bf)).unwrap(),
            "AND after_merge <> false".to_owned()
        );
    }

    #[test]
    fn test_int_filter() {
        let int_f = serde_json::json!({ "global_id": { "eq": -1 } });
        assert_eq!(
            Filter::into_str(("global_id".to_owned(), int_f)).unwrap(),
            "AND global_id = -1".to_owned()
        );

        let int_f = serde_json::json!({ "global_id": { "notIn": [1, 2, 3] } });
        assert_eq!(
            Filter::into_str(("global_id".to_owned(), int_f)).unwrap(),
            "AND global_id NOT IN (1,2,3)".to_owned()
        );
    }

    #[test]
    fn test_string_filter() {
        let sf = serde_json::json!({ "id": { "eq": "6f451fa0..." } });
        assert_eq!(
            Filter::into_str(("id".to_owned(), sf)).unwrap(),
            "AND id = \"6f451fa0...\"".to_owned()
        );

        let sf = serde_json::json!({ "id": { "include": ["6f451fa0...", "d0d80836..."]} });
        assert_eq!(
            Filter::into_str(("id".to_owned(), sf)).unwrap(),
            "AND id IN (\"6f451fa0...\",\"d0d80836...\")".to_string()
        );
    }

    #[test]
    fn test_skip_nulls() {
        let bf = Some(BlockFilter {
            prev_ref: Some(ExtBlkRefFilter {
                root_hash: Some(StringFilter {
                    eq: Some(
                        "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                            .to_owned(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut value = serde_json::to_value(bf).unwrap();
        let res = <graphql::filter::Filter as WhereOp>::skip_nulls(&mut value);

        assert_eq!(
            *res,
            serde_json::json!({
                "prev_ref_root_hash": {
                    "eq": "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                }
            })
        );

        let bf = Some(BlockFilter {
            prev_ref: Some(ExtBlkRefFilter {
                root_hash: Some(StringFilter {
                    eq: Some(
                        "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                            .to_owned(),
                    ),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            or: Some(Box::new(BlockFilter {
                prev_alt_ref: Some(ExtBlkRefFilter {
                    end_lt: None,
                    file_hash: None,
                    root_hash: Some(StringFilter {
                        eq: Some(
                            "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                                .to_owned(),
                        ),
                        ..Default::default()
                    }),
                    seq_no: None,
                    or: None,
                }),
                ..Default::default()
            })),
            ..Default::default()
        });

        let mut value = serde_json::to_value(bf).unwrap();
        let res = <graphql::filter::Filter as WhereOp>::skip_nulls(&mut value);

        assert_eq!(
            *res,
            serde_json::json!({
                "prev_ref_root_hash": {
                    "eq": "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                },
                "or": {
                    "prev_alt_ref_root_hash": {
                        "eq": "75680317350e8e80f7cda49efee21543a7de5ea856f47052c7fdf6afe0b034b7"
                    }
                }
            })
        );

        let jo = res.as_object().unwrap().iter().collect::<Vec<_>>();
        println!("jo: {jo:?}");
    }
}
