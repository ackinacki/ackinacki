// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
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
        if self.last.is_some() || self.before.is_some() {
            PaginateDirection::Backward
        } else {
            PaginateDirection::Forward
        }
    }

    pub fn get_bound_markers(&self, num_nodes: usize) -> (bool, bool) {
        (self.has_previous_page(num_nodes), self.has_next_page(num_nodes))
    }

    pub fn has_next_page(&self, num_nodes: usize) -> bool {
        if let Some(first) = self.first {
            num_nodes > first || self.before.is_some()
        } else {
            false
        }
    }

    pub fn has_previous_page(&self, num_nodes: usize) -> bool {
        if let Some(last) = self.last {
            num_nodes > last || self.after.is_some()
        } else {
            false
        }
    }
}
