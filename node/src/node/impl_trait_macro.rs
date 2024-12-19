// Note: this is a terrible solution for a terrible code.
// Must be fixed asap.

#[macro_export]
macro_rules! impl_node_trait {
    ($trait_name: ty,  $trait_impl: tt) => {


impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>

$trait_name for

$crate::node::Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TValidationProcess, TRepository, TAttestationProcessor, TRandomGenerator>
    where
        TBLSSignatureScheme: $crate::bls::BLSSignatureScheme<PubKey = $crate::bls::gosh_bls::PubKey> + Clone,
        <TBLSSignatureScheme as $crate::bls::BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        $crate::block::producer::process::BlockProducerProcess< Repository = TRepository>,
        TValidationProcess: $crate::block::keeper::process::BlockKeeperProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::types::AckiNackiBlock<TBLSSignatureScheme>>,

            OptimisticState = $crate::node::associated_types::OptimisticStateFor<TBlockProducerProcess>,
        >,
        TBlockProducerProcess: $crate::block::producer::process::BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::types::AckiNackiBlock<TBLSSignatureScheme>>,

        >,
        TRepository: $crate::repository::Repository<
            BLS = TBLSSignatureScheme,
            EnvelopeSignerIndex = $crate::node::SignerIndex,

            CandidateBlock = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::types::AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = $crate::node::associated_types::OptimisticStateFor<TBlockProducerProcess>,
            NodeIdentifier = $crate::node::NodeIdentifier,
            Attestation = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::node::associated_types::AttestationData>,
        >,
        <<TBlockProducerProcess as $crate::block::producer::process::BlockProducerProcess>::BlockProducer as $crate::node::BlockProducer>::Message: Into<
            <<TBlockProducerProcess as $crate::block::producer::process::BlockProducerProcess>::OptimisticState as $crate::repository::optimistic_state::OptimisticState>::Message,
        >,
        TStateSyncService: $crate::node::services::sync::StateSyncService<
            Repository = TRepository
        >,
        TAttestationProcessor: $crate::node::attestation_processor::AttestationProcessor<
            BlockAttestation = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::node::associated_types::AttestationData>,
            CandidateBlock = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::types::AckiNackiBlock<TBLSSignatureScheme>>,
        >,
        TRandomGenerator: rand::Rng,

    $trait_impl

}
}
