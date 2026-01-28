pub fn get_engine_version(_block_seq_no: u32) -> semver::Version {
    // TODO: take from node global config
    #[cfg(feature = "transitioning_node_version")]
    if todo!() {
        // this condition should be set for a particular transitioning version if required
        "X.X.X".parse().unwrap()
    }
    "1.0.3".parse().unwrap()
}
