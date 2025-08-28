// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::associated_types::NackReason;
use crate::node::associated_types::NodeAssociatedTypes;
use crate::node::services::sync::StateSyncService;
use crate::node::Node;
use crate::repository::repository_impl::RepositoryImpl;
use crate::types::BlockIdentifier;
use crate::types::BlockSeqNo;
use crate::utilities::guarded::GuardedMut;

// const BROADCAST_ACK_BLOCK_DIFF: u32 = 10;

impl<TStateSyncService, TRandomGenerator> Node<TStateSyncService, TRandomGenerator>
where
    TStateSyncService: StateSyncService<Repository = RepositoryImpl>,
    TRandomGenerator: rand::Rng,
{
    pub(crate) fn on_ack(
        &mut self,
        ack: &<Self as NodeAssociatedTypes>::Ack,
    ) -> anyhow::Result<()> {
        tracing::trace!("on_ack {:?}", ack);
        let block_id: &BlockIdentifier = &ack.data().block_id;
        let block_seq_no = ack.data().block_seq_no;
        let Some(signatures_map) = self.get_block_keeper_pubkeys(block_id.clone()) else {
            tracing::trace!("ack can't be processed now, save to cache.");
            let acks = self.ack_cache.entry(block_seq_no).or_default();
            acks.push(ack.clone());
            return Ok(());
        };
        let is_valid = ack
            .verify_signatures(&signatures_map)
            .expect("Signatures verification should not crash.");
        if is_valid {
            let mut received_acks_in = self.received_acks.lock();
            received_acks_in.push(ack.clone());
            drop(received_acks_in);
        }
        Ok(())
    }

    pub(crate) fn on_nack(
        &mut self,
        nack: &<Self as NodeAssociatedTypes>::Nack,
    ) -> anyhow::Result<()> {
        // TODO: check signatures.
        match &nack.data().reason {
            NackReason::BadBlock { envelope } => {
                let block_state = self.block_state_repository.get(&envelope.data().identifier())?;
                block_state.guarded_mut(|e| {
                    e.add_suspicious(
                        nack.clone_signature_occurrences(),
                        nack.aggregated_signature().clone(),
                    )
                })?;
                self.validation_service.send((block_state, envelope.clone()));
            }
            _ => {
                tracing::warn!("Unimplemented. Should not be possible to reach.");
            }
        }
        tracing::trace!("on_nack {:?}", nack);
        // let block_id: &BlockIdentifier = &nack.data().block_id;
        // let block_seq_no = nack.data().block_seq_no;
        // self.block_state_repository.get(block_id)?.lock().add_suspicious()?;
        // let block = match self.repository.get_block_from_repo_or_archive(block_id) {
        // Err(e) => {
        // tracing::trace!("nack can't be processed now, save to cache. Error: {e:?}");
        // let nacks = self.nack_cache.entry(block_seq_no).or_default();
        // nacks.push(nack.clone());
        // return Ok(());
        // }
        // Ok(block) => block,
        // };
        // let thread_id = &self.get_block_thread_id(&block)?;
        // let last_fin_block = self.repository.select_thread_last_finalized_block(thread_id);
        // tracing::trace!("on_nack: last finalized block, {:?}", last_fin_block);
        // match last_fin_block {
        // Ok(Some((_, seq_no))) => {
        // if seq_no >= block_seq_no {
        // tracing::warn!("Received nack target block is older than the last finalized block for this thread");
        // return Ok(());
        // }
        // }
        // _ => return Ok(()),
        // }
        // let signatures_map = self.get_block_keeper_pubkeys(block.data().identifier()).unwrap();
        //
        // let is_valid_signatute = match nack.verify_signatures(&signatures_map) {
        // Ok(res) => res,
        // Err(e) => {
        // tracing::warn!("Failed to check Nack signatures: {e:?}");
        // return Ok(());
        // }
        // };
        // if !is_valid_signatute {
        // tracing::warn!("Invalid Nack signature");
        // return Ok(());
        // }
        //
        //
        // let Ok(is_valid_nack) = self.is_valid_nack(nack, thread_id, block.clone()) else {
        // tracing::warn!("Failed to validate Nack");
        // return Ok(());
        // };
        //
        // if is_valid_nack {
        // tracing::trace!("valid nack {:?}", nack);
        // let mut received_nacks_in = self.received_nacks.lock();
        // received_nacks_in.push(nack.clone());
        // drop(received_nacks_in);
        // let common_section = block.data().get_common_section();
        // if common_section.thread_id == self.thread_id && self.get_latest_block_producer() == common_section.producer_id {
        //     self.rotate_producer_group()?;
        // }
        // } else {
        // tracing::trace!("invalid nack {:?}", nack);
        // self.block_state_repository.get(block_id)?.lock().resolve_suspicious()?;
        // let parent_block_id = block.data().parent();
        // let bk_set = self.block_state_repository.get(&block_id.clone())?.lock().clone();
        // let bk_set = bk_set.bk_set().clone().unwrap();
        // if let Some(signer_index) =
        // self.get_node_signer_index_for_block_id(parent_block_id.clone())
        // {
        // if bk_set.contains_signer(&signer_index) {
        // tracing::trace!("Forward nack");
        // let new_nack_data =
        // NackReason::WrongNack { nack_data_envelope: Arc::new(nack.clone()) };
        // let new_nack = self.generate_nack(
        // nack.data().block_id.clone(),
        // nack.data().block_seq_no,
        // new_nack_data
        // )?.expect("Node has already checked that it has valid signer index, must not fail");
        // tracing::trace!("new nack {:?}", new_nack);
        // let mut received_nacks_in = self.received_nacks.lock();
        // received_nacks_in.push(new_nack);
        // drop(received_nacks_in);
        // let current_bp_id = self.get_latest_block_producer();
        // self.single_tx.send((current_bp_id, NetworkMessage::Nack((nack.clone(), *thread_id))))?;
        // TODO: handle situation when Nack was signed by current BP, rotate BP and broadcast nack in this case
        // }
        // }
        // }
        Ok(())
    }

    // pub(crate) fn _is_valid_nack(
    //     &mut self,
    //     nack: &<Self as NodeAssociatedTypes>::Nack,
    //     thread_id: &ThreadIdentifier,
    //     block: Arc<Envelope<GoshBLS, AckiNackiBlock>>,
    // ) -> anyhow::Result<bool> {
    //     match nack.data().clone().reason {
    //         // NackReason::SameHeightBlock{first_envelope, second_envelope} => {
    //         // self.is_valid_same_height_nack(block, first_envelope, second_envelope, thread_id)
    //         // },
    //         NackReason::BadBlock { envelope } => {
    //             self.is_valid_bad_block_nack(block, envelope, thread_id)
    //         }
    //         NackReason::WrongNack { nack_data_envelope: _ } => {
    //             tracing::trace!("WrongNack nack");
    //             Ok(false)
    //         }
    //     }
    // }

    // TODO: rework to check that blocks have the same parent id, were produced by the same node and have different hashes
    // fn _is_valid_same_height_nack(
    //     &mut self,
    //     block: Envelope<GoshBLS, AckiNackiBlock>,
    //     nack_first_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    //     nack_second_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    //     thread_id: &ThreadIdentifier,
    // ) -> anyhow::Result<bool> {
    //     tracing::trace!("SameHeightBlock nack");
    //     let nack_target_node_id = &nack_first_envelope.data().get_common_section().producer_id;
    //     let bk_set = match self.get_block_keeper_set_for_block_id(block.data().parent()).clone() {
    //         Some(value) => value,
    //         None => return Ok(false),
    //     };
    //     let data = match bk_set.get_by_node_id(nack_target_node_id) {
    //         Some(keeper_data) => keeper_data,
    //         None => return Ok(false),
    //     };
    //     let nack_key = data.pubkey.clone();
    //     if let Ok(thread1) = &self.get_block_thread_id(&nack_first_envelope) {
    //         if let Ok(thread2) = &self.get_block_thread_id(&nack_second_envelope) {
    //             if (thread_id != thread1) || (thread_id != thread2) {
    //                 return Ok(false);
    //             }
    //         } else {
    //             return Ok(false);
    //         }
    //     } else {
    //         return Ok(false);
    //     }
    //     let mut sign_producer_map = HashMap::new();
    //     sign_producer_map.insert(data.signer_index, nack_key.clone());
    //     if !nack_first_envelope
    //         .verify_signatures(&sign_producer_map)
    //         .expect("Signatures verification should not crash.")
    //     {
    //         return Ok(false);
    //     }
    //     if !nack_second_envelope
    //         .verify_signatures(&sign_producer_map)
    //         .expect("Signatures verification should not crash.")
    //     {
    //         return Ok(false);
    //     }
    //     if let Ok(res) = nack_first_envelope.data().check_hash() {
    //         if !res {
    //             return Ok(false);
    //         }
    //     } else {
    //         return Ok(false);
    //     }
    //     if let Ok(res) = nack_second_envelope.data().check_hash() {
    //         if !res {
    //             return Ok(false);
    //         }
    //     } else {
    //         return Ok(false);
    //     }
    //     if nack_first_envelope.data().get_hash() == nack_second_envelope.data().get_hash() {
    //         return Ok(false);
    //     }
    //     Ok(true)
    // }

    // fn _is_valid_bad_block_nack(
    //     &mut self,
    //     // TODO: check if block structure is the same. If there is a possibility of an attack with reused signatures!
    //     block: Arc<Envelope<GoshBLS, AckiNackiBlock>>,
    //     nack_envelope: Envelope<GoshBLS, AckiNackiBlock>,
    //     thread_id: &ThreadIdentifier,
    // ) -> anyhow::Result<bool> {
    //     tracing::trace!("BadBlock nack");
    //     let nack_target_node_id = &nack_envelope.data().get_common_section().producer_id;
    //     let block_nack = block.data().get_common_section().nacks.clone();
    //     let block_id = block.data().identifier();
    //     // TODO: check if black was already validated.
    //     // if self.blocks_state.get(block_id)?.
    //     let bk_set = match self.get_block_keeper_set_for_block_id(block.data().parent()).clone() {
    //         Some(value) => value,
    //         // Bug! It MUST NOT return false in case it couldn't find parent bkset. It can come later.
    //         None => return Ok(false),
    //     };
    //     let data = match bk_set.get_by_node_id(nack_target_node_id) {
    //         Some(keeper_data) => keeper_data,
    //         None => return Ok(false),
    //     };
    //     let nack_key = data.pubkey.clone();
    //     if let Ok(thread1) = &self.get_block_thread_id(&nack_envelope) {
    //         if thread_id != thread1 {
    //             return Ok(false);
    //         }
    //     } else {
    //         return Ok(false);
    //     }
    //     let mut sign_producer_map = HashMap::new();
    //     sign_producer_map.insert(data.signer_index, nack_key.clone());
    //     let is_valid_sig_block = nack_envelope
    //         .verify_signatures(&sign_producer_map)
    //         .expect("Signatures verification should not crash.");
    //     if (block.data().get_hash() == nack_envelope.data().get_hash()) || (!is_valid_sig_block) {
    //         return Ok(false);
    //     }
    //     let blockchain_config = load_blockchain_config(&self.config.local.blockchain_config_path)?;
    //     match self
    //         .repository
    //         .get_optimistic_state(&block_id, thread_id, None)
    //         .expect("Failed to get optimistic state of the previous block")
    //     {
    //         Some(mut state) => {
    //             let refs = self.shared_services.exec(|service| {
    //                 let mut refs = vec![];
    //                 for block_id in &nack_envelope.data().get_common_section().refs {
    //                     let state = service
    //                         .cross_thread_ref_data_service
    //                         .get_cross_thread_ref_data(block_id)
    //                         .expect("Failed to load ref state");
    //                     refs.push(state);
    //                 }
    //                 refs
    //             });
    //             if let Ok(res) = verify_block(
    //                 nack_envelope.data(),
    //                 Arc::new(blockchain_config),
    //                 &mut state,
    //                 self.config.clone(),
    //                 refs,
    //                 self.shared_services.clone(),
    //                 block_nack,
    //                 self.block_state_repository.clone(),
    //                 self.repository.accounts_repository().clone(),
    //                 self.metrics.clone(),
    //                 self.message_db.clone(),
    //             ) {
    //                 if res {
    //                     return Ok(false);
    //                 }
    //             }
    //         }
    //         None => {
    //             return Ok(false);
    //         }
    //     }
    //     Ok(true)
    // }

    pub(crate) fn _check_cached_acks_and_nacks(
        &mut self,
        last_processed_block: &<Self as NodeAssociatedTypes>::CandidateBlock,
    ) -> anyhow::Result<()> {
        // TODO: need to track thread id of the finalized block
        let block_seq_no = last_processed_block.data().seq_no();
        let block_id = last_processed_block.data().identifier();
        let cached_acks = self.ack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for ack in cached_acks {
            if ack.data().block_id == block_id {
                self.on_ack(&ack)?;
                break;
            }
        }
        let cached_nacks = self.nack_cache.get(&block_seq_no).cloned().unwrap_or_default();
        for nack in cached_nacks {
            if nack.data().block_id == block_id {
                self.on_nack(&nack)?;
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn _clear_old_acks_and_nacks(
        &mut self,
        finalized_block_seq_no: &BlockSeqNo,
    ) -> anyhow::Result<()> {
        let ack_keys: Vec<BlockSeqNo> = self.ack_cache.keys().cloned().collect();
        for key in ack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.ack_cache.remove(&key);
            }
        }
        let nack_keys: Vec<BlockSeqNo> = self.nack_cache.keys().cloned().collect();
        for key in nack_keys {
            if key <= *finalized_block_seq_no {
                let _ = self.nack_cache.remove(&key);
            }
        }
        let mut received_acks = self.received_acks.lock();
        let mut received_nacks = self.received_nacks.lock();
        received_acks.retain(|ack| ack.data().block_seq_no > *finalized_block_seq_no);
        received_nacks.retain(|nack| nack.data().block_seq_no > *finalized_block_seq_no);
        drop(received_acks);
        drop(received_nacks);
        Ok(())
    }
}
