use serde::Deserialize;
// Re-export the macro so users only depend on this crate.
pub use versioned_struct_macro::versioned;

pub trait Transitioning: for<'a> Deserialize<'a> {
    type Old: for<'a> Deserialize<'a>;

    fn from(old: Self::Old) -> Self;

    /// Compatibility deserializer: try new, fall back to old.
    fn deserialize_data_compat(bytes: &[u8]) -> bincode::Result<Self> {
        // Try to read as the new format first.
        if let Ok(new) = bincode::deserialize::<Self>(bytes) {
            return Ok(new);
        }

        // If that fails (e.g. EOF because the old struct has fewer fields),
        // deserialize as the old struct and convert.
        let old: Self::Old = bincode::deserialize(bytes)?;
        Ok(Self::from(old))
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde::Serialize;

    use super::*;

    #[versioned]
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Data {
        pub f1: String,
        pub f2: u8,

        // only in DataOld
        #[legacy]
        pub f: i8,

        // only in Data
        #[future]
        pub f: u128,

        // only in Data
        #[future]
        pub ff: String,
    }

    impl Transitioning for Data {
        type Old = DataOld;

        fn from(old: Self::Old) -> Self {
            let f = if old.f > 0 { old.f as u128 } else { 0 };
            Self {
                f1: old.f1,
                f2: old.f2,
                // choose whatever default you want for the new field
                f,
                ff: "Hello".to_string(),
            }
        }
    }

    #[test]
    fn deserialize_old_bytes_into_new_data() {
        // Create an instance of the *old* struct
        let old = DataOld { f1: "hello".into(), f2: 42, f: -1 };
        println!("old: {old:?}");
        // Serialize as old format
        let bytes = bincode::serialize(&old).expect("serialize old");

        // Deserialize using the compat function into the *new* Data
        let new = Data::deserialize_data_compat(&bytes).expect("compat deserialize");

        println!("new: {new:?}");
        assert_eq!(new.f1, "hello");
        assert_eq!(new.f2, 42);
        assert_eq!(new.f, 0);
        assert_eq!(new.ff, "Hello");

        // Create an instance of the *old* struct
        let old = DataOld { f1: "hello".into(), f2: 42, f: 55 };
        println!("old: {old:?}");
        // Serialize as old format
        let bytes = bincode::serialize(&old).expect("serialize old");

        // Deserialize using the compat function into the *new* Data
        let new = Data::deserialize_data_compat(&bytes).expect("compat deserialize");

        println!("new: {new:?}");
        assert_eq!(new.f1, "hello");
        assert_eq!(new.f2, 42);
        assert_eq!(new.f, 55);
        assert_eq!(new.ff, "Hello");
    }
}
