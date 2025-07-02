// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

// Expectations:
// - It is allowed to pushback on incoming external messages.
// - External messages are stored per blockchain thread.

mod queue;
mod stamp;
mod thread_state;

pub use stamp::Stamp;
pub use thread_state::ExternalMessagesThreadState;
