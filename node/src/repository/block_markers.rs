use bitflags::bitflags;

bitflags! {
    pub struct Marker: u8 {
        const Empty = 0;
        const IsInvalidated = 1 << 7;
        const IsFinalized = 1;
        const HasLongEnoughDescendantsChain = 1 << 1;
        const HasDependenciesFinalized = 1 << 2;
        const HasFilledMinTimeRequirement = 1 << 3;
    }
}

impl Marker {
    pub fn is_ready_to_be_finalized(&self) -> bool {
        self.bits()
            == (Marker::HasLongEnoughDescendantsChain
                | Marker::HasDependenciesFinalized
                | Marker::HasFilledMinTimeRequirement)
                .bits()
    }
}

pub trait BlockMarkersRepository {}
