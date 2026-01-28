// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

/// Domain-specific configuration rules for archive processing.
///
/// This represents the business rules for how archives should be grouped and processed.
/// It's independent of CLI args, environment, or infrastructure concerns.
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, TypedBuilder)]
pub struct ProcessingRules {
    /// Whether all servers must be present in each archive group
    #[builder(default = false)]
    pub require_all_servers: bool,

    /// Time window in seconds for matching archive timestamps
    /// Archives within this window of anchor timestamp are grouped together
    #[builder(default = 3600)]
    pub match_window_sec: i64,

    /// Whether to compress processed files
    #[builder(default = false)]
    pub compress: bool,
}
