use std::sync::OnceLock;

use parking_lot::Mutex;

use super::repository::BlockStateRepository;
use crate::bls::envelope::BLSSignedEnvelope;
use crate::node::AttestationData;
use crate::node::Envelope;
use crate::node::GoshBLS;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

static ADD_ATTN_MUTEX: OnceLock<Mutex<i32>> = OnceLock::new();

pub trait TryAddAttestation {
    fn try_add_attestation(
        &self,
        attestation: Envelope<GoshBLS, AttestationData>,
    ) -> anyhow::Result<bool>;
}

impl TryAddAttestation for BlockStateRepository {
    fn try_add_attestation(
        &self,
        attestation: Envelope<GoshBLS, AttestationData>,
    ) -> anyhow::Result<bool> {
        let guard = ADD_ATTN_MUTEX.get_or_init(|| Mutex::new(0));
        let block_identifier = attestation.data().block_id.clone();
        let block_state = self.get(&block_identifier)?;
        let (parent_block_identifier, thread_identifier) =
            block_state.guarded(|e| (e.parent_block_identifier().clone(), *e.thread_identifier()));
        anyhow::ensure!(parent_block_identifier.is_some());
        anyhow::ensure!(thread_identifier.is_some());
        let parent_block_identifier = parent_block_identifier.unwrap();
        let thread_identifier = thread_identifier.unwrap();
        let guarded = guard.lock();
        if block_state.guarded(|e| e.attestation.is_some()) {
            return Ok(false);
        }
        let parent_state = self.get(&parent_block_identifier)?;
        let children = parent_state.guarded(|e| {
            anyhow::ensure!(e.known_children().contains(&block_identifier));
            Ok(e.known_children().clone())
        })?;
        for child_block_identifier in children.iter() {
            let child = self.get(child_block_identifier)?;
            let (attestation, retracted, is_same_thread) = child.guarded(|e| {
                (
                    e.attestation().clone(),
                    e.retracted_attestation().clone(),
                    e.thread_identifier() == &Some(thread_identifier),
                )
            });
            if !is_same_thread {
                continue;
            }
            if attestation.is_some() && retracted.is_none() {
                return Ok(false);
            }
        }
        block_state.guarded_mut(|e| {
            e.attestation = Some(attestation);
            e.save()
        })?;
        drop(guarded);
        Ok(true)
    }
}
