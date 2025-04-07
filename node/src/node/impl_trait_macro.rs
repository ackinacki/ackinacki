// Note: this is a terrible solution for a terrible code.
// Must be fixed asap.

#[macro_export]
macro_rules! impl_node_trait {
    ($trait_name: ty,  $trait_impl: tt) => {


impl<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TRandomGenerator>

$trait_name for

$crate::node::Node<TBLSSignatureScheme, TStateSyncService, TBlockProducerProcess, TRandomGenerator>
    where
        TBLSSignatureScheme: $crate::bls::BLSSignatureScheme<PubKey = $crate::bls::gosh_bls::PubKey> + Clone,
        <TBLSSignatureScheme as $crate::bls::BLSSignatureScheme>::PubKey: PartialEq,
        TBlockProducerProcess:
        $crate::block::producer::process::BlockProducerProcess<Repository = RepositoryImpl>,
        TBlockProducerProcess: $crate::block::producer::process::BlockProducerProcess<
            BLSSignatureScheme = TBLSSignatureScheme,
            CandidateBlock = $crate::bls::envelope::Envelope<TBLSSignatureScheme, $crate::types::AckiNackiBlock<TBLSSignatureScheme>>,
            OptimisticState = OptimisticStateImpl,
        >,
        <<TBlockProducerProcess as $crate::block::producer::process::BlockProducerProcess>::BlockProducer as $crate::node::BlockProducer>::Message: Into<
            <<TBlockProducerProcess as $crate::block::producer::process::BlockProducerProcess>::OptimisticState as $crate::repository::optimistic_state::OptimisticState>::Message,
        >,
        TStateSyncService: $crate::node::services::sync::StateSyncService<
            Repository = RepositoryImpl
        >,
        TRandomGenerator: rand::Rng,

    $trait_impl

}
}
