use std::fmt;

use node_types::BlockIdentifier;
use node_types::ThreadIdentifier;

use super::control::UpdatePhase;

#[derive(Debug)]
pub enum ArchiveStateError {
    NotReady { thread_id: ThreadIdentifier, phase: UpdatePhase },

    StaleUpdate { thread_id: ThreadIdentifier, expected: BlockIdentifier, actual: BlockIdentifier },

    ThreadAlreadyExists(ThreadIdentifier),

    NotUninitialized(ThreadIdentifier),

    Collapsed(ThreadIdentifier),

    ConcurrentModification(ThreadIdentifier),

    EpochChanged { expected: u64, actual: u64 },

    InconsistentUpdate,

    CorruptedControlRecord(ThreadIdentifier, String),

    Store(anyhow::Error),
}

impl fmt::Display for ArchiveStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotReady { thread_id, phase } => {
                write!(f, "Thread {thread_id} is not ready: phase is {phase:?}")
            }
            Self::StaleUpdate { thread_id, expected, actual } => {
                write!(
                    f,
                    "Stale update for thread {thread_id}: expected block {}, actual {}",
                    expected.to_hex_string(),
                    actual.to_hex_string()
                )
            }
            Self::ThreadAlreadyExists(tid) => write!(f, "Thread {tid} already exists"),
            Self::NotUninitialized(tid) => {
                write!(f, "Thread {tid} is not in Uninitialized phase")
            }
            Self::Collapsed(tid) => write!(f, "Thread {tid} is collapsed"),
            Self::ConcurrentModification(tid) => {
                write!(f, "Concurrent modification detected for thread {tid}")
            }
            Self::EpochChanged { expected, actual } => {
                write!(f, "Archive epoch changed: expected {expected}, actual {actual}")
            }
            Self::InconsistentUpdate => {
                write!(f, "Inconsistent update: routing not covered by any thread declaration")
            }
            Self::CorruptedControlRecord(tid, reason) => {
                write!(f, "Corrupted control record for thread {tid}: {reason}")
            }
            Self::Store(e) => write!(f, "Store error: {e}"),
        }
    }
}

impl std::error::Error for ArchiveStateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for ArchiveStateError {
    fn from(e: anyhow::Error) -> Self {
        Self::Store(e)
    }
}
