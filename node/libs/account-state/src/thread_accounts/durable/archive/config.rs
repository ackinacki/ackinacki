pub struct ArchiveStoreConfig {
    /// Aerospike address (or None for in-memory).
    pub aerospike_address: Option<String>,
    /// Uniquely identifies this node archive (used as aerospike namespace).
    pub node_id: String,
    /// Number of parallel write threads (Rayon pool size). 0 = sequential.
    pub write_parallelism: usize,
}
