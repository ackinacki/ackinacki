// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SqlProjection {
    columns: Vec<&'static str>,
}

pub fn normalize_graphql_field_name(name: &str) -> String {
    let mut normalized = String::with_capacity(name.len());
    for (index, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

impl SqlProjection {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub fn add(&mut self, column: &'static str) {
        if !self.columns.contains(&column) {
            self.columns.push(column);
        }
    }

    pub fn extend<const N: usize>(&mut self, columns: [&'static str; N]) {
        for column in columns {
            self.add(column);
        }
    }

    pub fn ensure_minimum<const N: usize>(&mut self, columns: [&'static str; N]) {
        if self.columns.is_empty() {
            self.extend(columns);
        }
    }

    pub fn columns(&self) -> &[&'static str] {
        &self.columns
    }

    pub fn select_list(&self) -> String {
        self.columns.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_graphql_field_name;
    use super::SqlProjection;

    #[test]
    fn add_deduplicates_and_preserves_order() {
        let mut projection = SqlProjection::new();
        projection.add("id");
        projection.add("chain_order");
        projection.add("id");

        assert_eq!(projection.select_list(), "id, chain_order");
    }

    #[test]
    fn extend_adds_each_column_once() {
        let mut projection = SqlProjection::new();
        projection.extend(["id", "status"]);
        projection.extend(["status", "seq_no"]);

        assert_eq!(projection.columns(), &["id", "status", "seq_no"]);
    }

    #[test]
    fn ensure_minimum_adds_only_when_empty() {
        let mut empty = SqlProjection::new();
        empty.ensure_minimum(["id"]);
        assert_eq!(empty.select_list(), "id");

        let mut non_empty = SqlProjection::new();
        non_empty.add("boc");
        non_empty.ensure_minimum(["id"]);
        assert_eq!(non_empty.select_list(), "boc");
    }

    #[test]
    fn normalize_graphql_name_keeps_snake_case() {
        assert_eq!(normalize_graphql_field_name("chain_order"), "chain_order");
    }

    #[test]
    fn normalize_graphql_name_converts_camel_case() {
        assert_eq!(normalize_graphql_field_name("chainOrder"), "chain_order");
        assert_eq!(normalize_graphql_field_name("bkSetUpdate"), "bk_set_update");
    }
}
