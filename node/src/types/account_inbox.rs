use std::sync::Arc;

use crate::message::identifier::MessageIdentifier;
use crate::message::WrappedMessage;

pub type AccountInbox = account_inbox::range::MessagesRange<MessageIdentifier, Arc<WrappedMessage>>;
