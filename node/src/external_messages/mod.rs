// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

// Expectations:
// - It is allowed to pushback on incoming external messages.
// - External messages are stored per blockchain thread.

mod queue;
mod stamp;
mod state;
mod thread_state;

pub use queue::is_low_priority_external_message_function_id;
pub use queue::miner_message_function_id;
pub use queue::ExtMessageDst;
pub use queue::QueuedExtMessage;
pub use stamp::Stamp;
pub use state::ExtMessages;
pub use state::ExtMessagesLimits;
pub use state::ExtMessagesSelectionCursor;
pub use state::ExtMessagesSource;
pub use thread_state::ExternalMessagesThreadState;
