// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub enum PaginateDirection {
    Forward,
    Backward,
}

#[derive(Clone)]
pub struct PaginationArgs {
    pub first: Option<usize>,
    pub after: Option<String>,
    pub last: Option<usize>,
    pub before: Option<String>,
}

impl PaginationArgs {
    pub fn get_limit(&self) -> usize {
        1 + if let Some(first) = self.first {
            first
        } else if let Some(last) = self.last {
            last
        } else {
            crate::defaults::QUERY_BATCH_SIZE as usize
        }
    }

    pub fn shrink_portion<T>(&self, portion: &mut Vec<T>) {
        if portion.len() >= self.get_limit() {
            match self.get_direction() {
                PaginateDirection::Forward => portion.truncate(portion.len() - 1),
                PaginateDirection::Backward => {
                    portion.drain(0..1);
                }
            }
        }
    }

    pub fn get_direction(&self) -> PaginateDirection {
        if self.first.is_some() && self.after.is_some() && self.before.is_some() {
            PaginateDirection::Forward
        } else if self.last.is_some() || self.before.is_some() {
            PaginateDirection::Backward
        } else {
            PaginateDirection::Forward
        }
    }

    pub fn get_bound_markers(&self, num_nodes: usize) -> (bool, bool) {
        (self.has_previous_page(num_nodes), self.has_next_page(num_nodes))
    }

    pub fn has_next_page(&self, num_nodes: usize) -> bool {
        self.first.is_some_and(|first| num_nodes > first) || self.before.is_some()
    }

    pub fn has_previous_page(&self, num_nodes: usize) -> bool {
        self.last.is_some_and(|last| num_nodes > last) || self.after.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::PaginateDirection;
    use super::PaginationArgs;

    const AFTER: &str = "7698abb50000161ebb34601";
    const BEFORE: &str = "7698abb50000161ebb34605";

    fn make_args(
        first: Option<usize>,
        after: Option<&str>,
        last: Option<usize>,
        before: Option<&str>,
    ) -> PaginationArgs {
        PaginationArgs {
            first,
            after: after.map(str::to_owned),
            last,
            before: before.map(str::to_owned),
        }
    }

    #[test]
    fn get_direction_returns_forward_when_first_and_after() {
        let args = make_args(Some(2), Some(AFTER), None, None);
        assert!(matches!(args.get_direction(), PaginateDirection::Forward));
    }

    #[test]
    fn get_direction_returns_backward_when_last_and_before() {
        let args = make_args(None, None, Some(2), Some(BEFORE));
        assert!(matches!(args.get_direction(), PaginateDirection::Backward));
    }

    #[test]
    fn get_direction_returns_forward_when_first_and_after_and_before() {
        let args = make_args(Some(2), Some(AFTER), None, Some(BEFORE));
        assert!(matches!(args.get_direction(), PaginateDirection::Forward));
    }

    #[test]
    fn get_direction_returns_backward_when_last_and_after_and_before() {
        let args = make_args(None, Some(AFTER), Some(2), Some(BEFORE));
        assert!(matches!(args.get_direction(), PaginateDirection::Backward));
    }

    // --- has_previous_page / has_next_page ---

    #[test]
    fn has_previous_page_true_when_after_is_set() {
        let args = make_args(Some(3), Some(AFTER), None, None);
        // after cursor exists → items before it
        assert!(args.has_previous_page(3));
        assert!(args.has_previous_page(0));
    }

    #[test]
    fn has_previous_page_false_when_no_after_and_no_overflow() {
        let args = make_args(Some(3), None, None, None);
        assert!(!args.has_previous_page(3));
    }

    #[test]
    fn has_previous_page_true_on_backward_overflow() {
        let args = make_args(None, None, Some(3), None);
        // 4 rows returned for last=3 → overflow → previous page exists
        assert!(args.has_previous_page(4));
        assert!(!args.has_previous_page(3));
    }

    #[test]
    fn has_next_page_true_when_before_is_set() {
        let args = make_args(None, None, Some(3), Some(BEFORE));
        // before cursor exists → items after it
        assert!(args.has_next_page(3));
        assert!(args.has_next_page(0));
    }

    #[test]
    fn has_next_page_false_when_no_before_and_no_overflow() {
        let args = make_args(None, None, Some(3), None);
        assert!(!args.has_next_page(3));
    }

    #[test]
    fn has_next_page_true_on_forward_overflow() {
        let args = make_args(Some(3), None, None, None);
        // 4 rows returned for first=3 → overflow → next page exists
        assert!(args.has_next_page(4));
        assert!(!args.has_next_page(3));
    }

    #[test]
    fn window_query_both_pages_true() {
        let args = make_args(Some(10), Some(AFTER), None, Some(BEFORE));
        // Window between after and before, fewer items than first
        assert!(args.has_previous_page(3)); // after is set
        assert!(args.has_next_page(3)); // before is set
    }
}
