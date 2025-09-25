//! Note: this must not be neccessary in 99% cases.
//! The only usecase it is useful if you want to use ThreadIdentifier in a BTreeMap.

use super::ThreadIdentifier;

impl std::cmp::Ord for ThreadIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl std::cmp::PartialOrd for ThreadIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
