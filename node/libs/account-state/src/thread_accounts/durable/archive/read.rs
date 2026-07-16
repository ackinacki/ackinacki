#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use node_types::AccountRouting;
    use node_types::BlockIdentifier;
    use node_types::ThreadIdentifier;

    use crate::thread_accounts::durable::archive::config::ArchiveStoreConfig;
    use crate::thread_accounts::durable::archive::control::*;
    use crate::thread_accounts::durable::archive::store::ActiveArchiveUpdate;
    use crate::thread_accounts::durable::archive::update::AccumulatedUpdate;
    use crate::thread_accounts::durable::kv_store::in_memory::InMemoryKVStore;
    use crate::thread_accounts::durable::kv_store::KVStore;
    use crate::ArchiveOperation;
    use crate::ArchiveStateStore;
    use crate::ThreadAccount;

    fn test_kv() -> KVStore {
        KVStore::InMemory(InMemoryKVStore::new())
    }

    fn test_config() -> ArchiveStoreConfig {
        ArchiveStoreConfig { aerospike_address: None, node_id: String::new(), write_parallelism: 0 }
    }

    fn make_routing(seed: u8) -> AccountRouting {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        node_types::AccountIdentifier::new(bytes).redirect()
    }

    fn setup_idle_thread() -> (ArchiveStateStore, ThreadIdentifier, BlockIdentifier) {
        let kv = test_kv();
        let tid = ThreadIdentifier::default();
        let block_id = BlockIdentifier::default();

        let sys = SystemState { threads: HashSet::from([tid]), data_epoch: 0 };
        write_system_record(&kv, META_SET, &sys).unwrap();
        let ctrl = ThreadControlState::new_idle(tid, block_id);
        write_thread_control(&kv, META_SET, &ctrl, None).unwrap();

        let store = ArchiveStateStore::new(kv, test_config()).unwrap();
        (store, tid, block_id)
    }

    #[test]
    fn test_read_nonexistent_routing() {
        let (store, _, _) = setup_idle_thread();
        let routing = make_routing(1);
        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_after_write() {
        let (store, tid, block_0) = setup_idle_thread();
        let block_1 = BlockIdentifier::new([1u8; 32]);
        let routing = make_routing(1);

        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&update).unwrap();

        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_read_after_delete() {
        let (store, tid, block_0) = setup_idle_thread();
        let block_1 = BlockIdentifier::new([1u8; 32]);
        let block_2 = BlockIdentifier::new([2u8; 32]);
        let routing = make_routing(1);

        let mut u1 = AccumulatedUpdate::new();
        u1.transition_thread(tid, block_0, block_1);
        u1.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&u1).unwrap();

        let mut u2 = AccumulatedUpdate::new();
        u2.transition_thread(tid, block_1, block_2);
        u2.insert(routing, ArchiveOperation::Remove);
        store.apply_update(&u2).unwrap();

        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_sees_active_update() {
        let (store, tid, block_0) = setup_idle_thread();
        let block_1 = BlockIdentifier::new([1u8; 32]);
        let routing = make_routing(1);

        // Manually register an active update with a replace
        let mut update = AccumulatedUpdate::new();
        update.transition_thread(tid, block_0, block_1);
        update.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        let update_arc = Arc::new(ActiveArchiveUpdate::new(0, update));
        store.active_updates().write().push(update_arc.clone());

        // Read should see the active update's value
        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_some());

        // Deregister
        store.active_updates().write().retain(|u| !Arc::ptr_eq(u, &update_arc));

        // Now read returns None (not in store)
        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_read_sees_active_delete() {
        let (store, tid, block_0) = setup_idle_thread();
        let block_1 = BlockIdentifier::new([1u8; 32]);
        let block_2 = BlockIdentifier::new([2u8; 32]);
        let routing = make_routing(1);

        // Write the account normally
        let mut u1 = AccumulatedUpdate::new();
        u1.transition_thread(tid, block_0, block_1);
        u1.insert(routing, ArchiveOperation::UpdateOrInsert(ThreadAccount::default()));
        store.apply_update(&u1).unwrap();

        assert!(store.read_account_operation(&routing).unwrap().is_some());

        // Manually register an active update with a delete
        let mut u2 = AccumulatedUpdate::new();
        u2.transition_thread(tid, block_1, block_2);
        u2.insert(routing, ArchiveOperation::Remove);
        let update_arc = Arc::new(ActiveArchiveUpdate::new(0, u2));
        store.active_updates().write().push(update_arc.clone());

        // Read should see the delete (None)
        let result = store.read_account_operation(&routing).unwrap();
        assert!(result.is_none());

        // Deregister
        store.active_updates().write().retain(|u| !Arc::ptr_eq(u, &update_arc));

        // Now read sees the committed value again
        assert!(store.read_account_operation(&routing).unwrap().is_some());
    }
}
