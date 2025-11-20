mod block_producer;
pub mod memento;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use http_server::ExtMsgFeedbackList;
use network::channel::NetBroadcastSender;
use parking_lot::Mutex;
use telemetry_utils::instrumented_channel_ext::XInstrumentedSender;
use telemetry_utils::mpsc::InstrumentedSender;

use crate::block::producer::process::TVMBlockProducerProcess;
use crate::block::producer::producer_service::block_producer::BlockProducer;
use crate::bls::envelope::Envelope;
use crate::bls::gosh_bls::PubKey;
use crate::bls::gosh_bls::Secret;
use crate::bls::GoshBLS;
use crate::external_messages::ExternalMessagesThreadState;
use crate::helper::SHUTDOWN_FLAG;
use crate::node::associated_types::AckData;
use crate::node::associated_types::NackData;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::services::attestations_target::service::AttestationTargetsService;
use crate::node::shared_services::SharedServices;
use crate::node::NetworkMessage;
use crate::node::NodeIdentifier;
use crate::protocol::authority_switch::action_lock::BlockProducerCommand;
use crate::repository::optimistic_state::OptimisticStateSaveCommand;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockSeqNo;
use crate::types::CollectedAttestations;
use crate::types::RndSeed;
use crate::types::ThreadIdentifier;
use crate::utilities::guarded::AllowGuardedMut;

pub struct ProducerService {
    // Note: wrapped in option to take and trace the error
    handler: Option<JoinHandle<anyhow::Result<()>>>,
}

impl AllowGuardedMut for Option<BlockSeqNo> {}

impl ProducerService {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        self_addr: SocketAddr,
        thread_id: ThreadIdentifier,
        repository: RepositoryImpl,
        block_state_repository: BlockStateRepository,
        block_producer_control_rx: std::sync::mpsc::Receiver<BlockProducerCommand>,
        production_process: TVMBlockProducerProcess,
        feedback_sender: InstrumentedSender<ExtMsgFeedbackList>,
        received_acks: Arc<Mutex<Vec<Envelope<GoshBLS, AckData>>>>,
        received_nacks: Arc<Mutex<Vec<Envelope<GoshBLS, NackData>>>>,
        shared_services: SharedServices,
        bls_keys_map: Arc<Mutex<HashMap<PubKey, (Secret, RndSeed)>>>,
        last_block_attestations: Arc<Mutex<CollectedAttestations>>,
        attestations_target_service: AttestationTargetsService,
        self_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
        self_authority_tx: XInstrumentedSender<(NetworkMessage, SocketAddr)>,
        broadcast_tx: NetBroadcastSender<NodeIdentifier, NetworkMessage>,

        node_identifier: NodeIdentifier,
        production_timeout: Duration,
        save_state_frequency: u32,
        external_messages: ExternalMessagesThreadState,

        is_state_sync_requested: Arc<Mutex<Option<BlockSeqNo>>>,
        bp_production_count: Arc<AtomicI32>,
        save_optimistic_service_sender: InstrumentedSender<OptimisticStateSaveCommand>,
    ) -> anyhow::Result<Self> {
        let mut producer = BlockProducer::builder()
            .self_addr(self_addr)
            .node_identifier(node_identifier)
            .self_tx(self_tx)
            .self_authority_tx(self_authority_tx)
            .attestations_target_service(attestations_target_service)
            .production_timeout(production_timeout)
            .block_state_repository(block_state_repository)
            .shared_services(shared_services)
            .repository(repository)
            .last_block_attestations(last_block_attestations)
            .thread_id(thread_id)
            .broadcast_tx(broadcast_tx)
            .control_rx(block_producer_control_rx)
            .bls_keys_map(bls_keys_map)
            .production_process(production_process)
            .received_nacks(received_nacks)
            .received_acks(received_acks)
            .feedback_sender(feedback_sender)
            .save_state_frequency(save_state_frequency)
            .external_messages(external_messages)
            .is_state_sync_requested(is_state_sync_requested)
            .bp_production_count(bp_production_count)
            .save_optimistic_service_sender(save_optimistic_service_sender)
            .build();
        let handler =
            std::thread::Builder::new().name("ProducerService".to_string()).spawn(move || {
                tracing::info!("Starting producer service for {:?} ", thread_id);
                let res = producer.main_loop();
                tracing::info!("Producer service for {:?} finished: {res:?}", thread_id);
                res?;
                Ok(())
            })?;
        Ok(Self { handler: Some(handler) })
    }

    pub fn touch(&mut self) {
        if self.handler.as_ref().unwrap().is_finished() {
            let res = self.handler.take().unwrap().join().unwrap();
            if SHUTDOWN_FLAG.get() != Some(&true) {
                panic!("Producer service has finished with res: {res:?}");
            }
        }
    }
}
