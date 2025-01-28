#[cfg(feature = "misbehave")]
use std::fs::File;
#[cfg(feature = "misbehave")]
use std::io::Read;

#[cfg(feature = "misbehave")]
use serde::Deserialize;
#[cfg(feature = "misbehave")]
use serde::Serialize;

#[cfg(feature = "misbehave")]
#[derive(Debug, Deserialize, Serialize)]
pub struct MisbehaveRules {
    pub fork_test: ForkTest,
}

#[cfg(feature = "misbehave")]
#[derive(Debug, Deserialize, Serialize)]
pub struct ForkTest {
    pub from_seq: u32,
    pub to_seq: u32,
}

#[cfg(feature = "misbehave")]
pub fn misbehave_rules() -> anyhow::Result<Option<MisbehaveRules>> {
    if let Ok(file_path) = std::env::var("MISBEHAVE_RULES_PATH") {
        let mut file = File::open(&file_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let rules: MisbehaveRules = serde_yaml::from_str(&contents)?;
        Ok(Some(rules))
    } else {
        // MISBEHAVE_RULES_PATH variable is not set, misbehaving disabled
        Ok(None)
    }
}
