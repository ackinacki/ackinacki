// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::fmt::Display;
use std::fmt::Formatter;

use anyhow::anyhow;

pub(crate) const BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK: u8 = 1;

#[derive(Debug)]
pub(crate) struct VerifyError {
    pub code: u8,
}

impl Display for VerifyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let error_description = match self.code {
            BP_DID_NOT_PROCESS_ALL_MESSAGES_FROM_PREVIOUS_BLOCK => {
                "BP started processing new messages before it processed all messages from the previous state"
            }
            _ => {
                unreachable!("Unknown verify error code")
            }
        };
        write!(f, "{}", error_description)
    }
}

impl VerifyError {
    pub(crate) fn new(code: u8) -> Self {
        Self { code }
    }
}

pub(crate) fn verify_error(code: u8) -> anyhow::Error {
    anyhow!(VerifyError::new(code))
}
