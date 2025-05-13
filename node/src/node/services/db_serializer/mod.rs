use std::sync::Arc;
use std::thread::JoinHandle;

use database::documents_db::DocumentsDb;
use tvm_block::ShardStateUnsplit;
use tvm_types::Cell;

use crate::bls::envelope::Envelope;
use crate::bls::GoshBLS;
use crate::database::write_to_db;
use crate::types::AckiNackiBlock;

pub struct DBSerializeService {
    handler: JoinHandle<anyhow::Result<()>>,
}

impl DBSerializeService {
    pub fn new(
        archive: Arc<dyn DocumentsDb>,
        receiver: std::sync::mpsc::Receiver<(
            Envelope<GoshBLS, AckiNackiBlock>,
            Option<Arc<ShardStateUnsplit>>,
            Option<Cell>,
        )>,
    ) -> anyhow::Result<Self> {
        let handler =
            std::thread::Builder::new().name("db_serializer".to_string()).spawn(move || loop {
                let (envelope, state, state_cell) = receiver.recv()?;
                if !cfg!(feature = "disable_db_write") {
                    write_to_db(archive.clone(), envelope, state, state_cell)?;
                }
            })?;
        Ok(Self { handler })
    }

    pub fn check(&self) {
        assert!(!self.handler.is_finished(), "DB serialization service should not stop");
    }
}
