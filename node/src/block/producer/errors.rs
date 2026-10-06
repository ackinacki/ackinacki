// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::fmt::Formatter;

use anyhow::anyhow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VerifyError {
    #[allow(unused)]
    DidNotProcessAllMessagesFromPreviousBlock,
    BlockHasMessagesWithEqualHash,
}

impl Display for VerifyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let error_description = match self {
            Self::DidNotProcessAllMessagesFromPreviousBlock => {
                "BP started processing new messages before it processed all messages from the previous state"
            }
            Self::BlockHasMessagesWithEqualHash => {
                "Block has messages with equal hashes"
            }
        };
        write!(f, "{error_description}")
    }
}

pub(crate) fn verify_error(error: VerifyError) -> anyhow::Error {
    anyhow!(error)
}

#[cfg(test)]
mod tests {
    use super::verify_error;
    use super::VerifyError;

    #[test]
    fn verify_errors_have_descriptions() {
        assert_eq!(
            VerifyError::DidNotProcessAllMessagesFromPreviousBlock.to_string(),
            "BP started processing new messages before it processed all messages from the previous state"
        );
        assert_eq!(
            verify_error(VerifyError::BlockHasMessagesWithEqualHash).to_string(),
            "Block has messages with equal hashes"
        );
    }
}
