use std::collections::HashMap;
use std::collections::HashSet;

use derive_getters::Getters;
use typed_builder::TypedBuilder;

use crate::bls::envelope::BLSSignedEnvelope;
use crate::bls::envelope::Envelope;
use crate::bls::BLSSignatureScheme;
use crate::bls::GoshBLS;
use crate::node::associated_types::AttestationData;
use crate::node::block_state::repository::BlockState;
use crate::node::block_state::repository::BlockStateRepository;
use crate::node::SignerIndex;
use crate::repository::repository_impl::RepositoryImpl;
use crate::repository::Repository;
use crate::types::AckiNackiBlock;
use crate::types::BlockIdentifier;
use crate::types::ForkResolution;
use crate::utilities::guarded::Guarded;

#[derive(TypedBuilder, Getters, Debug, Clone)]
pub struct Fork {
    pub parent_block_state: BlockState,
    // vector of children ids that create fork
    pub fork_block_ids: HashSet<BlockIdentifier>,
    // Map of attestations with target child from fork as a key
    pub fork_attestation_signers: HashMap<BlockIdentifier, HashSet<SignerIndex>>,
    pub fork_attestations: HashMap<BlockIdentifier, Envelope<GoshBLS, AttestationData>>,
    pub total_number_of_bks: Option<usize>,
}

// Note: this service needs fixes to work with threads merge
#[derive(TypedBuilder)]
pub struct ForkResolutionService {
    block_state_repository: BlockStateRepository,
    repository: RepositoryImpl,
    // map of unresolved forks with fork parent block id as a key
    #[builder(default)]
    unresolved_forks: HashMap<BlockIdentifier, Fork>,
    // map of resolved forks with fork winner id as a key
    #[builder(default)]
    resolved_forks: HashMap<BlockIdentifier, ForkResolution>,
}

impl ForkResolutionService {
    pub fn evaluate(&mut self, unprocessed_blocks: Vec<BlockState>) {
        let mut updated_forks = HashSet::new();
        for block_state in unprocessed_blocks {
            for fork in self.unresolved_forks.values_mut() {
                if fork.total_number_of_bks.is_none() {
                    fork.parent_block_state.guarded(|e| {
                        if e.bk_set().is_some() {
                            fork.total_number_of_bks = Some(e.get_descendant_bk_set().len());
                        }
                    });
                }
                if fork.total_number_of_bks.is_none() {
                    continue;
                }
                for block_id in &fork.fork_block_ids {
                    if let Some(signers) =
                        block_state.guarded(|e| e.verified_attestations_for(block_id))
                    {
                        tracing::trace!(
                            "ForkResolutionService: add signers for {block_id:?} {signers:?}"
                        );
                        let cur_signers =
                            fork.fork_attestation_signers.entry(block_id.clone()).or_default();
                        let initial_len = cur_signers.len();
                        cur_signers.extend(signers);
                        if cur_signers.len() != initial_len {
                            updated_forks
                                .insert(fork.parent_block_state.block_identifier().clone());
                            let candidate_block = self
                                .repository
                                .get_block(&block_state.guarded(|e| e.block_identifier().clone()))
                                .expect("should be able to read block")
                                .expect("block must exist");
                            let attestation = candidate_block
                                .data()
                                .get_common_section()
                                .block_attestations
                                .iter()
                                .find(|att| &att.data().block_id == block_id)
                                .cloned()
                                .expect("attestation must exist");
                            fork.fork_attestations
                                .entry(block_id.clone())
                                .and_modify(|cur_attestation| {
                                    let mut merged_signatures_occurences =
                                        cur_attestation.clone_signature_occurrences();
                                    let incoming_signature_occurrences =
                                        attestation.clone_signature_occurrences();
                                    for signer_index in incoming_signature_occurrences.keys() {
                                        let new_count = (*merged_signatures_occurences
                                            .get(signer_index)
                                            .unwrap_or(&0))
                                            + (*incoming_signature_occurrences
                                                .get(signer_index)
                                                .unwrap());
                                        merged_signatures_occurences
                                            .insert(*signer_index, new_count);
                                    }
                                    merged_signatures_occurences.retain(|_k, count| *count > 0);
                                    let new_signature = GoshBLS::merge(
                                        cur_attestation.aggregated_signature(),
                                        attestation.aggregated_signature(),
                                    )
                                    .expect("merge should not fail");
                                    *cur_attestation = Envelope::create(
                                        new_signature,
                                        merged_signatures_occurences,
                                        cur_attestation.data().clone(),
                                    );
                                })
                                .or_insert(attestation);
                        }
                    }
                }
            }
        }
        for parent_block_id in updated_forks {
            let fork = self.unresolved_forks.get(&parent_block_id).cloned().expect("must exist");
            if let Some(fork_resolve) = check_fork(&fork, &self.repository) {
                self.unresolved_forks.remove(&parent_block_id);
                self.resolved_forks.insert(parent_block_id, fork_resolve);
            }
        }
    }

    // Returns a ForkResolution IF this candidate is a winner in a fork.
    // In case of no fork or a lost fork this method returns None.
    pub fn resolve_fork(
        &self,
        candidate_id: BlockIdentifier,
        assume_extra_attestations: &[Envelope<GoshBLS, AttestationData>],
    ) -> Option<ForkResolution> {
        let block_state = self.block_state_repository.get(&candidate_id).expect("must exist");
        let parent_id = block_state
            .guarded(|e| e.parent_block_identifier().clone())
            .expect("parent must be set");
        if let Some(resolved_fork) = self.resolved_forks.get(&parent_id) {
            return if resolved_fork.winner() == &candidate_id {
                Some(resolved_fork.clone())
            } else {
                None
            };
        }
        if let Some(fork) = self.unresolved_forks.get(&candidate_id) {
            let mut fork = fork.clone();
            for block_id in fork.fork_block_ids.clone() {
                if let Some(attestation) =
                    assume_extra_attestations.iter().find(|e| e.data().block_id == block_id)
                {
                    let cur_signers =
                        fork.fork_attestation_signers.entry(block_id.clone()).or_default();
                    let new_signers: HashSet<SignerIndex> = HashSet::from_iter(
                        attestation.clone_signature_occurrences().keys().cloned(),
                    );
                    let cur_len = cur_signers.len();
                    cur_signers.extend(new_signers);
                    if cur_signers.len() > cur_len {
                        fork.fork_attestations
                            .entry(block_id.clone())
                            .and_modify(|cur_attestation| {
                                let mut merged_signatures_occurences =
                                    cur_attestation.clone_signature_occurrences();
                                let incoming_signature_occurrences =
                                    attestation.clone_signature_occurrences();
                                for signer_index in incoming_signature_occurrences.keys() {
                                    let new_count = (*merged_signatures_occurences
                                        .get(signer_index)
                                        .unwrap_or(&0))
                                        + (*incoming_signature_occurrences
                                            .get(signer_index)
                                            .unwrap());
                                    merged_signatures_occurences.insert(*signer_index, new_count);
                                }
                                merged_signatures_occurences.retain(|_k, count| *count > 0);
                                let new_signature = GoshBLS::merge(
                                    cur_attestation.aggregated_signature(),
                                    attestation.aggregated_signature(),
                                )
                                .expect("merge should not fail");
                                *cur_attestation = Envelope::create(
                                    new_signature,
                                    merged_signatures_occurences,
                                    cur_attestation.data().clone(),
                                );
                            })
                            .or_insert(attestation.clone());
                    }
                }
            }
            return check_fork(&fork, &self.repository);
        }
        None
    }

    pub fn found_fork(&mut self, parent_block_id: &BlockIdentifier) -> anyhow::Result<()> {
        let parent_block_state = self.block_state_repository.get(parent_block_id)?;
        let block_children = self
            .block_state_repository
            .get(parent_block_id)?
            .guarded(|e| e.known_children().clone());
        // TODO: on the moment of fork we can have parent block missing. need to save fork and wait for parent
        let total_number_of_bk = self.block_state_repository.get(parent_block_id)?.guarded(|e| {
            if e.bk_set().is_some() {
                Some(e.get_descendant_bk_set().len())
            } else {
                None
            }
        });
        self.unresolved_forks
            .entry(parent_block_id.clone())
            .and_modify(|fork| fork.fork_block_ids.extend(block_children.clone()))
            .or_insert({
                Fork::builder()
                    .parent_block_state(parent_block_state)
                    .fork_block_ids(block_children)
                    .fork_attestation_signers(HashMap::new())
                    .total_number_of_bks(total_number_of_bk)
                    .fork_attestations(HashMap::new())
                    .build()
            });
        Ok(())
    }
}

fn check_fork(fork: &Fork, repository_impl: &RepositoryImpl) -> Option<ForkResolution> {
    if let Some(leader) = find_fork_winner(fork) {
        let other_forks: Vec<Envelope<GoshBLS, AckiNackiBlock>> = fork
            .fork_block_ids
            .clone()
            .into_iter()
            .filter(|id| id != &leader)
            .map(|block_id| {
                repository_impl
                    .get_block(&block_id)
                    .expect("block must exist")
                    .expect("block must exist")
            })
            .collect();
        let lost_attestations: Vec<Envelope<GoshBLS, AttestationData>> = fork
            .fork_attestations
            .clone()
            .into_iter()
            .filter(|(id, _)| id != &leader)
            .map(|(_, attestation)| attestation.clone())
            .collect();
        Some(
            ForkResolution::builder()
                .parent_block_identifier(fork.parent_block_state.block_identifier().clone())
                .winner(leader.clone())
                .winner_attestations(
                    fork.fork_attestations
                        .get(&leader)
                        .cloned()
                        .expect("leader must have attestations"),
                )
                .other_forks(other_forks)
                .lost_attestations(lost_attestations)
                .nacked_forks(vec![])
                .build(),
        )
    } else {
        None
    }
}

fn find_fork_winner(fork: &Fork) -> Option<BlockIdentifier> {
    tracing::trace!("Check fork: {fork:?}");
    // 66% wins right now
    // if number of votes distributed below this threshold it
    // would be impossible to be certain in case of misbehaving nodes.
    // Assume 1/6 nodes may misbehave
    let Some(total_number_of_bks) = fork.total_number_of_bks() else {
        return None;
    };
    let necessary_signers_cnt = (total_number_of_bks * 2).div_ceil(3);

    let mut distributed_votes = HashSet::new();
    let mut misbehaving_check = HashMap::new();
    for (block_id, fork_signers) in &fork.fork_attestation_signers {
        let signers_cnt = fork_signers.len();
        if signers_cnt >= necessary_signers_cnt {
            tracing::trace!("Check fork: single leader {block_id:?}");
            return Some(block_id.clone());
        }
        for signer in fork_signers.iter() {
            misbehaving_check.entry(*signer).and_modify(|e| *e += 1).or_insert(1);
        }
        distributed_votes.extend(fork_signers.clone());
    }
    let misbehaving: HashSet<SignerIndex> = misbehaving_check
        .into_iter()
        .filter_map(|(k, v)| if v != 1 { Some(k) } else { None })
        .collect();
    if distributed_votes.len() < necessary_signers_cnt {
        return None;
    }
    let unknown = total_number_of_bks - distributed_votes.len();
    let (mut winners, top_score, lost_top_count) = fork.fork_attestation_signers.iter().fold(
        (vec![], 0usize, 0usize),
        |(mut winners, top_score, second_score), (id, signers)| {
            let counted_signers = signers.difference(&misbehaving).count();
            if counted_signers > top_score {
                return (vec![id.clone()], counted_signers, top_score);
            }
            if counted_signers == top_score {
                winners.push(id.clone());
            }
            if counted_signers > second_score {
                return (winners, top_score, counted_signers);
            }
            (winners, top_score, second_score)
        },
    );
    if winners.is_empty() {
        return None;
    }
    let potentially_misbehaving: usize =
        total_number_of_bks.div_ceil(6).saturating_sub(misbehaving.len());
    if winners.len() == 1
        && top_score > lost_top_count + usize::max(potentially_misbehaving, unknown)
    {
        // easy choice
        return Some(winners.remove(0));
    }
    // Critical last resort path
    if unknown > 0usize {
        return None;
    }
    winners.into_iter().max_by(BlockIdentifier::compare)
}
