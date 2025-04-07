#[derive(Clone, Debug)]
pub enum LoadErr {
    NoState,
    DeserializationError,
}

pub trait DurableStorageRead<MessageKey, Message> {
    type LoadError: Into<LoadErr> + Clone;

    fn load_message(&self, key: &MessageKey) -> Result<Message, Self::LoadError>;
    fn next(&self, key: &MessageKey) -> Result<Option<MessageKey>, Self::LoadError>;
    fn remaining_messages(
        &self,
        starting_key: &MessageKey,
        limit: usize,
    ) -> Result<Vec<Message>, Self::LoadError>;
}
