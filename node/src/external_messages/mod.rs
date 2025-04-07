// Expectations:
// - It is allowed to pushback on incoming external messages.
// - External messages are stored per blockchain thread.

mod progress;
mod queue;
mod thread_state;

pub use progress::Progress;
pub use thread_state::ExternalMessagesThreadState;
