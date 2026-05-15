mod builder;
pub(crate) mod repository;
mod repository_inner;
#[cfg(test)]
mod tests;

pub use builder::ThreadAccountsStateBuilder;
pub use repository::ThreadAccountsRepository;
pub use repository::ThreadAccountsState;
pub use repository::ThreadAccountsStateDiff;

pub(crate) const DEFAULT_APPLY_TO_DURABLE: bool = true;
