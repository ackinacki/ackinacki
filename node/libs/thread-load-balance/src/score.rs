#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Score(pub u32);

impl Score {
    pub fn is_none(&self) -> bool {
        self.0 == 0
    }
}
