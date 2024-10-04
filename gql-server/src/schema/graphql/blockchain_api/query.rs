// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pub enum PaginateDirection {
    Forward,
    Backward,
}

pub trait QueryArgs {
    fn get_direction(&self) -> PaginateDirection;
    fn get_limit(&self) -> usize;
}
