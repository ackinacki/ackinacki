use std::sync::Arc;
use std::sync::Weak;

use telemetry_utils::mpsc::InstrumentedReceiver;
use weak_table::WeakKeyHashMap;

use super::Command;
use crate::node::block_state::block_state_inner::BlockStateInner;
use crate::types::BlockIdentifier;
use crate::utilities::guarded::Guarded;
use crate::utilities::guarded::GuardedMut;

pub(super) fn inner_loop(receiver_rx: InstrumentedReceiver<Command>) {
    // Note:
    // It is easy to make a map of block cross-thread-ref-data dependencies,
    // since block state will be in the WeakKeyHashMap as long as block state is needed.
    // That means once it's gone from tracking in the node for any reason we can remove it
    // from the table.
    //
    // The reverse table lookup is not easy though. The problem is that we have list of blocks
    // awaiting for some block. However it is most probably that that block is not in the system yet.
    // This means we can't use BlockState as a weak ref for this case.
    //
    // As a solution we can use Arc<BlockIdentifier> for dependencies and the reverse lookup.
    // However for this iteration we will scan entite dependencies list each update.

    let mut missing_dependencies =
        WeakKeyHashMap::<Weak<BlockStateInner>, Vec<BlockIdentifier>>::new();
    loop {
        match receiver_rx.recv() {
            Ok(Command::SetCrossThreadRefDataDependencies(block_state, mut dependencies)) => {
                dependencies.retain(|x| {
                    x.guarded(|e| e.has_cross_thread_ref_data_prepared() != &Some(true))
                });
                if dependencies.is_empty() {
                    block_state
                        .guarded_mut(|e| e.set_has_all_cross_thread_ref_data_available())
                        .unwrap();
                } else {
                    missing_dependencies.insert(
                        Arc::clone(&block_state),
                        dependencies.iter().map(|e| e.block_identifier().clone()).collect(),
                    );
                }
                missing_dependencies.remove_expired();
            }
            Ok(Command::NotifyCrossThreadRefDataPrepared(block_state)) => {
                missing_dependencies.remove_expired();
                let prepared_block_identifier = block_state.block_identifier().clone();
                let mut dependencies_fullfilled = vec![];
                for (block_state, dependencies) in missing_dependencies.iter_mut() {
                    dependencies.retain(|e| e != &prepared_block_identifier);
                    if dependencies.is_empty() {
                        dependencies_fullfilled.push(block_state.clone());
                    }
                }
                for block_state in dependencies_fullfilled.into_iter() {
                    block_state
                        .guarded_mut(|e| e.set_has_all_cross_thread_ref_data_available())
                        .unwrap();
                    missing_dependencies.remove(&block_state);
                }
            }
            Err(_) => {
                break;
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
// use super::*;
//
// #[test]
// fn ensure_weak_to_arc_works_as_expected() {
// let foo = Arc::<BlockIdentifier>::new(BlockIdentifier::default());
// let bar = Arc::downgrade(&foo);
// let baz = Weak::upgrade(&bar).unwrap();
// assert_eq!(2, Arc::strong_count(&foo));
// }
//
// #[test]
// fn ensure_different_new_arcs_have_separate_strong_count() {
// let block_identifier = BlockIdentifier::default();
// let foo = Arc::<BlockIdentifier>::new(block_identifier.clone());
// let bar = Arc::clone(&foo);
// let baz = Arc::<BlockIdentifier>::new(block_identifier);
// assert_eq!(2, Arc::strong_count(&foo));
// assert_eq!(1, Arc::strong_count(&baz));
// }
// }
