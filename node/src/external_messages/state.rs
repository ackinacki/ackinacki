// 2022-2025 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::external_messages::QueuedExtMessage;

pub type ExtMessages = inbound_external_messages::Messages<QueuedExtMessage>;
pub type ExtMessagesLimits = inbound_external_messages::MessagesLimits;
pub type ExtMessagesSelectionCursor =
    inbound_external_messages::MessagesSelectionCursor<QueuedExtMessage>;
pub type ExtMessagesSource = Box<dyn inbound_external_messages::MessageScheduler<QueuedExtMessage>>;
