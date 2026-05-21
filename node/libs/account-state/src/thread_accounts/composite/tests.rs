use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::ThreadIdentifier;
use node_types::TransactionHash;
use once_cell::sync::OnceCell;
use tracing_subscriber::EnvFilter;
use tvm_block::MsgAddressInt;
use tvm_block::ShardAccount;
use tvm_types::AccountId;

use crate::account::avm::AvmAccount;
use crate::account::avm::AvmAccountData;
use crate::account::avm::AvmAccountMetadata;
use crate::account::avm::AvmThreadAccount;
use crate::AccountHashMismatchError;
use crate::ArchiveOperation;
use crate::BlockAccountOperation;
use crate::DurableThreadAccountsStateDiff;
use crate::ThreadAccount;
use crate::ThreadAccountsRepository;
use crate::ThreadAccountsState;

fn new_state() -> ThreadAccountsState {
    ThreadAccountsRepository::new_state()
}

fn target_tmp() -> PathBuf {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());

    let path = PathBuf::from(target).join("test-accounts");

    std::fs::create_dir_all(&path).unwrap();

    path
}

fn assert_drained(repo: &ThreadAccountsRepository) {
    assert!(
        repo.flush_pending_and_wait_for_drain(Duration::from_secs(5)),
        "archive accumulator did not drain"
    );
}

fn new_u256(name: &str, seed: usize) -> [u8; 32] {
    let hasher = blake3::Hasher::new();
    let mut hasher = hasher;
    hasher.update(name.as_bytes());
    hasher.update(&(seed as u64).to_be_bytes());
    hasher.finalize().into()
}

fn new_acc(seed: usize) -> (AccountRouting, ThreadAccount) {
    let id = AccountIdentifier::new(new_u256("acc", seed));
    let routing = id.redirect();
    let tvm_acc = tvm_block::Account::with_address(
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap(),
    );
    let tvm_shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", seed).into(),
        seed as u64,
        Some(routing.dapp_id().as_array().into()),
    )
    .unwrap();
    let acc = ThreadAccount::from(tvm_shard_acc);
    (id.redirect(), acc)
}

#[test]
#[ignore]
fn test_fs_accounts_repo() -> anyhow::Result<()> {
    let root_path = target_tmp();
    std::fs::remove_dir_all(&root_path).ok();
    let start = Instant::now();
    let repo = ThreadAccountsRepository::builder(root_path).build()?;
    repo.ensure_thread(&ThreadIdentifier::default(), &BlockIdentifier::default(), true)?;
    let mut seed = 1;
    let mut prev_state = new_state();
    for block_index in 0..10000 {
        let block_id = BlockIdentifier::new(new_u256("block", block_index));
        let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &prev_state);
        let mut accounts = Vec::new();
        for _ in 0..1000 {
            let (routing, acc) = new_acc(seed);
            seed += 1;
            builder.insert_account(&routing, &acc);
            accounts.push((routing, acc));
        }
        let transition = builder.build(None)?;
        repo.finalize_thread_transition(
            &block_id,
            &ThreadIdentifier::default(),
            0,
            &transition.new_state,
            transition.account_operations,
        )?;
        assert_drained(&repo);
        repo.state_save(&block_id, &transition.new_state)?;
        for (routing, acc) in accounts {
            let acc1 = repo.state_account(&transition.new_state, &routing)?.unwrap();
            assert_eq!(acc.get_dapp_id(), acc1.get_dapp_id());
            assert_eq!(acc.vm_account()?.hash(), acc1.vm_account()?.hash());
        }
        prev_state = transition.new_state;
        if block_index % 10 == 0 {
            println!("Block {} done", block_index);
        }
    }
    println!("Repo filled in {:?}", start.elapsed());
    test_fs_accounts_repo_compact()?;
    Ok(())
}

#[test]
#[ignore]
fn test_fs_accounts_repo_compact() -> anyhow::Result<()> {
    let root_path = target_tmp();
    let start = Instant::now();
    let _repo = ThreadAccountsRepository::builder(root_path).build()?;
    println!("Repo loaded in {:?}", start.elapsed());
    Ok(())
}

// ==================== Test Helpers ====================

/// Creates a TVM account with a substantial data cell large enough to trigger
/// merkle update optimization (>= 256 bytes serialized).
fn new_large_acc(seed: usize, data_size: usize) -> (AccountRouting, ThreadAccount) {
    use tvm_block::*;
    use tvm_types::*;

    let id = AccountIdentifier::new(new_u256("large_acc", seed));
    let routing = id.redirect();
    let address =
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap();

    let data_cell = build_data_cell(seed, data_size);

    let mut code_builder = BuilderData::new();
    code_builder.append_raw(&[0xDE, 0xAD, 0xBE, 0xEF], 32).unwrap();
    let code_cell = code_builder.into_cell().unwrap();

    let state_init =
        StateInit { code: Some(code_cell), data: Some(data_cell), ..Default::default() };
    let storage = AccountStorage::active_by_init_code_hash(
        0,
        CurrencyCollection::with_grams(1_000_000_000),
        state_init,
        true,
    );
    let storage_stat = StorageInfo::with_values(0, None);
    let mut tvm_acc = Account::with_storage(&address, &storage_stat, &storage);
    tvm_acc.update_storage_stat().unwrap();

    let shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", seed).into(),
        seed as u64,
        Some(routing.dapp_id().as_array().into()),
    )
    .unwrap();

    let state_acc = ThreadAccount::from(shard_acc);
    assert!(
        state_acc.write_bytes().unwrap().len() >= 256,
        "Large account should be at least 256 bytes, got {}",
        state_acc.write_bytes().unwrap().len()
    );
    (routing, state_acc)
}

/// Builds a data cell chain of approximately `size` bytes using deterministic content.
fn build_data_cell(seed: usize, size: usize) -> tvm_types::Cell {
    use tvm_types::*;

    let chunk_size = 127; // max bytes per cell (~1023 bits)
    let num_cells = size.div_ceil(chunk_size);

    let mut prev_cell: Option<Cell> = None;
    for i in 0..num_cells {
        let offset = i * chunk_size;
        let this_chunk = (size - offset).min(chunk_size);
        let mut builder = BuilderData::new();
        let bytes: Vec<u8> = (0..this_chunk).map(|j| ((seed + offset + j) % 256) as u8).collect();
        builder.append_raw(&bytes, this_chunk * 8).unwrap();

        if let Some(prev) = prev_cell {
            builder.checked_append_reference(prev).unwrap();
        }

        prev_cell = Some(builder.into_cell().unwrap());
    }

    prev_cell.unwrap_or_default()
}

/// Creates a modified copy of a TVM account with a different balance.
/// This is the minimal change that produces a small merkle delta.
fn modify_acc_balance(acc: &ThreadAccount, new_balance: u64) -> ThreadAccount {
    use crate::VmAccount;

    let thread_account = acc.vm_account().unwrap();
    let mut tvm_account = tvm_block::Account::try_from(&thread_account).unwrap();
    tvm_account.set_balance(tvm_block::CurrencyCollection::with_grams(new_balance));

    let new_thread_account: VmAccount = tvm_account.try_into().unwrap();
    ThreadAccount::new(
        new_thread_account,
        acc.last_trans_hash(),
        acc.last_trans_lt() + 1,
        acc.get_dapp_id(),
    )
    .unwrap()
}

fn set_acc_dapp_id(acc: &ThreadAccount, dapp_id: DAppIdentifier) -> ThreadAccount {
    use crate::VmAccount;

    let thread_account = acc.vm_account().unwrap();
    let tvm_account = tvm_block::Account::try_from(&thread_account).unwrap();
    let new_thread_account: VmAccount = tvm_account.try_into().unwrap();
    ThreadAccount::new(
        new_thread_account,
        acc.last_trans_hash(),
        acc.last_trans_lt(),
        Some(dapp_id),
    )
    .unwrap()
}

/// Creates an AVM account for testing the AVM-skip guard.
fn new_avm_acc(seed: usize) -> (AccountRouting, ThreadAccount) {
    use node_types::AccountHash;

    let id = AccountIdentifier::new(new_u256("avm_acc", seed));
    let routing = id.redirect();
    let avm_account = AvmAccount::new(
        AccountHash::new(new_u256("avm_hash", seed)),
        AvmAccountMetadata { id, storage_used_bytes: 1024, ..Default::default() },
        None,
        None,
        Some(AvmAccountData { data: vec![0u8; 512] }),
    );
    let state_account = ThreadAccount::from_avm(AvmThreadAccount {
        vm_account: avm_account,
        last_trans_hash: TransactionHash::new(new_u256("avm_trans", seed)),
        last_trans_lt: seed as u64,
    });
    (routing, state_account)
}

/// Creates a fresh filesystem-backed repository in a temp directory.
/// Returns the repo and the TempDir (which keeps the directory alive).
fn setup_repo(apply_to_durable: bool) -> (ThreadAccountsRepository, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let repo = ThreadAccountsRepository::builder(dir.path())
        .set_apply_to_durable(apply_to_durable)
        .build()
        .unwrap();
    repo.ensure_thread(&ThreadIdentifier::default(), &BlockIdentifier::default(), true).unwrap();
    repo.start_archive_update_service().unwrap();
    (repo, dir)
}

/// Like setup_repo but with thread in Uninitialized phase — for repos that will import a snapshot.
fn setup_import_repo(apply_to_durable: bool) -> (ThreadAccountsRepository, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let repo = ThreadAccountsRepository::builder(dir.path())
        .set_apply_to_durable(apply_to_durable)
        .build()
        .unwrap();
    repo.ensure_thread(&ThreadIdentifier::default(), &BlockIdentifier::default(), true).unwrap();
    repo.reset_archive().unwrap();
    repo.start_archive_update_service().unwrap();
    (repo, dir)
}

// ==================== Tier 1: Low-Level Unit Tests ====================

#[test]
fn test_merkle_update_roundtrip_raw() {
    use tvm_block::Deserializable;
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    let (_, old_acc) = new_large_acc(1, 512);
    let new_acc = modify_acc_balance(&old_acc, 999_999);

    let old_bytes = old_acc.write_bytes().unwrap();
    let new_bytes = new_acc.write_bytes().unwrap();

    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();

    // Create merkle update
    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // Deserialize and apply
    let mu_cell2 = read_single_root_boc(&mu_bytes).unwrap();
    let mu2 = MerkleUpdate::construct_from_cell(mu_cell2).unwrap();
    let result_cell = mu2.apply_for(&old_cell).unwrap();

    // Reconstruct and compare
    let result_bytes = write_boc(&result_cell).unwrap();
    let result_acc = ThreadAccount::read_bytes(&result_bytes).unwrap();

    assert_eq!(
        result_acc.write_bytes().unwrap(),
        new_acc.write_bytes().unwrap(),
        "Merkle update roundtrip should produce identical account"
    );
    assert!(
        mu_bytes.len() < new_bytes.len(),
        "Merkle update ({} bytes) should be smaller than full state ({} bytes)",
        mu_bytes.len(),
        new_bytes.len()
    );
}

#[test]
fn test_merkle_update_size_smaller_than_full_state() {
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    let (_, old_acc) = new_large_acc(10, 1024);
    let new_acc = modify_acc_balance(&old_acc, 42);

    let old_bytes = old_acc.write_bytes().unwrap();
    let new_bytes = new_acc.write_bytes().unwrap();

    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();

    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // The merkle update should meet the MERKLE_UPDATE_SIZE_FACTOR = 2.0 threshold
    assert!(
        (mu_bytes.len() as f64 * 2.0) < new_bytes.len() as f64,
        "Merkle update ({} bytes * 2.0 = {}) should be less than full state ({} bytes)",
        mu_bytes.len(),
        mu_bytes.len() * 2,
        new_bytes.len()
    );
}

#[test]
fn test_merkle_update_large_change_not_smaller() {
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    let (_, old_acc) = new_large_acc(20, 512);
    let (_, new_acc) = new_large_acc(21, 512); // Completely different account

    let old_bytes = old_acc.write_bytes().unwrap();
    let new_bytes = new_acc.write_bytes().unwrap();

    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();

    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // For completely different accounts, the merkle update should NOT meet the 2x threshold
    let mu_with_factor = mu_bytes.len() as f64 * 2.0;
    let full_size = new_bytes.len() as f64;
    assert!(
        mu_with_factor >= full_size,
        "Merkle update ({} bytes * 2.0 = {:.0}) should NOT be less than full state ({} bytes) \
        for completely different accounts",
        mu_bytes.len(),
        mu_with_factor,
        new_bytes.len()
    );
}

// ==================== Tier 2: Builder/Repository Integration Tests ====================

#[test]
#[ignore]
fn test_builder_optimizes_large_account_update() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert a large account
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Modify the same account (balance change only)
    let modified_acc = modify_acc_balance(&large_acc, 999_999);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified_acc);
    let transition2 = builder2.build(None)?;

    // The diff should contain AccountMerkleUpdate for this routing
    let durable_accounts = &transition2.diff.durable.accounts;
    let update = durable_accounts.get(&routing);
    assert!(update.is_some(), "Diff should contain the modified account");
    let update = update.unwrap();
    assert!(
        matches!(update, BlockAccountOperation::AccountMerkleUpdate(_)),
        "Large account with small change should be optimized to account merkle update, got {:?}",
        std::mem::discriminant(update)
    );

    if let BlockAccountOperation::AccountMerkleUpdate(bytes) = update {
        assert!(!bytes.is_empty(), "Merkle update bytes should be non-empty");
    }

    // Verify the resulting state still has the correct account
    let retrieved = repo.state_account(&transition2.new_state, &routing)?.unwrap();
    assert_eq!(
        retrieved.write_bytes()?,
        modified_acc.write_bytes()?,
        "Account in new state should match the modified account"
    );

    Ok(())
}

#[test]
fn test_builder_skips_merkle_update_for_redirect_account() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 10_001));

    let (routing, large_acc) = new_large_acc(10_001, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    let redirect_acc = large_acc.with_redirect()?;
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &redirect_acc);
    let transition2 = builder2.build(None)?;

    let update = transition2.diff.durable.accounts.get(&routing).unwrap();
    assert!(
        matches!(update, BlockAccountOperation::UpdateOrInsert(account) if account.is_redirect()),
        "Redirect account should stay a full UpdateOrInsert, got {:?}",
        std::mem::discriminant(update)
    );

    Ok(())
}

#[test]
fn test_builder_skips_merkle_update_when_account_dapp_differs_from_routing() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 10_002));

    let (default_routing, large_acc) = new_large_acc(10_002, 1024);
    let dapp_a = DAppIdentifier::new(new_u256("dapp", 10_002));
    let dapp_b = DAppIdentifier::new(new_u256("dapp", 10_003));
    let routing = default_routing.account_id().routing(dapp_a);
    let large_acc = set_acc_dapp_id(&large_acc, dapp_a);

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    let modified_acc = set_acc_dapp_id(&modify_acc_balance(&large_acc, 999_999), dapp_b);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified_acc);
    let transition2 = builder2.build(None)?;

    let update = transition2.diff.durable.accounts.get(&routing).unwrap();
    assert!(
        matches!(update, BlockAccountOperation::UpdateOrInsert(_)),
        "Account with dapp id different from routing should stay full UpdateOrInsert, got {:?}",
        std::mem::discriminant(update)
    );

    Ok(())
}

#[test]
fn test_builder_allows_merkle_update_when_account_dapp_matches_routing() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 10_004));

    let (default_routing, large_acc) = new_large_acc(10_004, 1024);
    let dapp = DAppIdentifier::new(new_u256("dapp", 10_004));
    let routing = default_routing.account_id().routing(dapp);
    let large_acc = set_acc_dapp_id(&large_acc, dapp);

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    let modified_acc = set_acc_dapp_id(&modify_acc_balance(&large_acc, 999_999), dapp);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified_acc);
    let transition2 = builder2.build(None)?;

    let update = transition2.diff.durable.accounts.get(&routing).unwrap();
    assert!(
        matches!(update, BlockAccountOperation::AccountMerkleUpdate(_)),
        "Matching dapp id account should still be eligible for AccountMerkleUpdate, got {:?}",
        std::mem::discriminant(update)
    );

    Ok(())
}

#[test]
fn test_builder_keeps_small_account_as_update_or_insert() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 100));

    // Insert a small account (below the 256-byte threshold)
    let (routing, small_acc) = new_acc(1);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &small_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Modify the small account
    let modified = modify_acc_balance(&small_acc, 42);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified);
    let transition2 = builder2.build(None)?;

    // Should remain as UpdateOrInsert (too small for merkle optimization)
    let entry = transition2.diff.durable.accounts.get(&routing);
    assert!(entry.is_some(), "Diff should contain the modified account");
    let update = entry.unwrap();
    assert!(
        matches!(update, BlockAccountOperation::UpdateOrInsert(_)),
        "Small account should remain as UpdateOrInsert"
    );

    Ok(())
}

#[test]
fn test_builder_keeps_new_account_as_update_or_insert() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();

    // Insert a large account into an empty state (no old state to diff against)
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition = builder.build(None)?;

    // Should be UpdateOrInsert since there's no old account to create a delta from
    let entry = transition.diff.durable.accounts.get(&routing);
    assert!(entry.is_some(), "Diff should contain the new account");
    let update = entry.unwrap();
    assert!(
        matches!(update, BlockAccountOperation::UpdateOrInsert(_)),
        "New account (no prior state) should be UpdateOrInsert"
    );

    Ok(())
}

#[test]
fn test_builder_skips_avm_accounts() -> anyhow::Result<()> {
    // Test at the try_create_merkle_update level: AVM accounts should never
    // be converted to merkle updates. We verify by checking that an AVM account
    // always produces UpdateOrInsert in the diff.
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 200));

    let (routing, avm_acc) = new_avm_acc(1);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &avm_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Modify the AVM account
    let (_, modified_avm) = new_avm_acc(2);
    // Use the same routing but different account data
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified_avm);
    let transition2 = builder2.build(None)?;

    let entry = transition2.diff.durable.accounts.get(&routing);
    assert!(entry.is_some(), "Diff should contain the AVM account");
    let update = entry.unwrap();
    assert!(
        matches!(update, BlockAccountOperation::UpdateOrInsert(_)),
        "AVM accounts should never be converted to account merkle update"
    );

    Ok(())
}

#[test]
fn test_builder_preserves_remove_and_move_from_tvm() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 300));

    // Insert two large accounts
    let (routing1, large_acc1) = new_large_acc(1, 1024);
    let (routing2, large_acc2) = new_large_acc(2, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing1, &large_acc1);
    builder.insert_account(&routing2, &large_acc2);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Remove one, modify the other
    let modified2 = modify_acc_balance(&large_acc2, 42);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.remove_account(&routing1);
    builder2.insert_account(&routing2, &modified2);
    let transition2 = builder2.build(None)?;

    // routing1 should be Remove
    let entry1 = transition2.diff.durable.accounts.get(&routing1);
    assert!(entry1.is_some(), "Diff should contain the removed account");
    assert!(
        matches!(entry1.unwrap(), BlockAccountOperation::Remove),
        "Removed account should stay as Remove"
    );

    // // routing2 should be AccountMerkleUpdate (large account with small change)
    // let entry2 = transition2.diff.durable.accounts.iter().find(|(r, _)| r == &routing2);
    // assert!(entry2.is_some(), "Diff should contain the modified account");
    // assert!(
    //     matches!(entry2.unwrap().1, ThreadAccountDiff::AccountMerkleUpdate(_)),
    //     "Large modified account should be AccountMerkleUpdate"
    // );

    Ok(())
}

#[test]
#[ignore]
fn test_apply_merkle_update_diff_produces_correct_state() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 400));

    // Insert a large account
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Modify an account → build produces optimized diff with AccountMerkleUpdate
    let modified = modify_acc_balance(&large_acc, 777_777);
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing, &modified);
    let transition2 = builder2.build(None)?;

    // Verify the diff contains AccountMerkleUpdate
    let has_merkle_update = transition2
        .diff
        .durable
        .accounts
        .iter()
        .any(|(_, u)| matches!(u, BlockAccountOperation::AccountMerkleUpdate(_)));
    assert!(has_merkle_update, "Diff should contain at least one account merkle update");

    // Simulate receiver: apply the diff to the old state
    let transition =
        repo.state_apply_diff(&state1, transition2.diff, ThreadIdentifier::default(), 0)?;

    // Verify the applied state has the correct account
    let acc_from_new_state = repo.state_account(&transition2.new_state, &routing)?.unwrap();
    let acc_from_applied = repo.state_account(&transition.new_state, &routing)?.unwrap();

    assert_eq!(
        acc_from_applied.write_bytes()?,
        acc_from_new_state.write_bytes()?,
        "Account from applied diff should match account from producer's new state"
    );
    assert_eq!(
        acc_from_applied.write_bytes()?,
        modified.write_bytes()?,
        "Account from applied diff should match the modified account"
    );

    Ok(())
}

#[test]
#[ignore]
fn test_multiple_accounts_mixed_optimization() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 500));

    // Insert 2 large + 2 smalls accounts
    let (routing_l1, large1) = new_large_acc(1, 1024);
    let (routing_l2, large2) = new_large_acc(2, 1024);
    let (routing_s1, small1) = new_acc(101);
    let (routing_s2, small2) = new_acc(102);

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing_l1, &large1);
    builder.insert_account(&routing_l2, &large2);
    builder.insert_account(&routing_s1, &small1);
    builder.insert_account(&routing_s2, &small2);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Modify all 4 accounts (balance change)
    let mod_l1 = modify_acc_balance(&large1, 100);
    let mod_l2 = modify_acc_balance(&large2, 200);
    let mod_s1 = modify_acc_balance(&small1, 300);
    let mod_s2 = modify_acc_balance(&small2, 400);

    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.insert_account(&routing_l1, &mod_l1);
    builder2.insert_account(&routing_l2, &mod_l2);
    builder2.insert_account(&routing_s1, &mod_s1);
    builder2.insert_account(&routing_s2, &mod_s2);
    let transition2 = builder2.build(None)?;

    // Check optimization decisions
    for (routing, update) in &transition2.diff.durable.accounts {
        if routing == &routing_l1 || routing == &routing_l2 {
            assert!(
                matches!(update, BlockAccountOperation::AccountMerkleUpdate(_)),
                "Large account {:?} should be optimized to account merkle update",
                routing.account_id().to_hex_string()
            );
        } else if routing == &routing_s1 || routing == &routing_s2 {
            assert!(
                matches!(update, BlockAccountOperation::UpdateOrInsert(_)),
                "Small account {:?} should remain as UpdateOrInsert",
                routing.account_id().to_hex_string()
            );
        }
    }

    // Apply diff and verify all accounts are correct
    let applied =
        repo.state_apply_diff(&state1, transition2.diff, ThreadIdentifier::default(), 0)?;

    for (routing, expected) in [
        (&routing_l1, &mod_l1),
        (&routing_l2, &mod_l2),
        (&routing_s1, &mod_s1),
        (&routing_s2, &mod_s2),
    ] {
        let actual = repo.state_account(&applied.new_state, routing)?.unwrap();
        assert_eq!(
            actual.write_bytes()?,
            expected.write_bytes()?,
            "Account {} should match after diff application",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

// ==================== Tier 3: Serialization Tests ====================

#[test]
fn test_durable_diff_bincode_roundtrip_all_variants() {
    let (routing1, some_account) = new_acc(1);
    let (routing2, _) = new_acc(2);
    let (routing3, _) = new_acc(3);
    let (routing4, _) = new_acc(4);

    let diff = DurableThreadAccountsStateDiff {
        accounts: HashMap::from_iter([
            (routing1, BlockAccountOperation::UpdateOrInsert(some_account)),
            (routing2, BlockAccountOperation::Remove),
            (routing3, BlockAccountOperation::MoveFromTvm),
            (routing4, BlockAccountOperation::AccountMerkleUpdate(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ]),
    };

    let bytes = bincode::serialize(&diff).unwrap();
    let diff2: DurableThreadAccountsStateDiff = bincode::deserialize(&bytes).unwrap();

    assert!(diff == diff2, "Bincode roundtrip should produce identical diff");
}

#[test]
fn test_account_merkle_update_variant_bincode_roundtrip() {
    use tvm_block::Deserializable;
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    // Create a real merkle update
    let (_, old_acc) = new_large_acc(50, 512);
    let new_acc = modify_acc_balance(&old_acc, 12345);

    let old_bytes = old_acc.write_bytes().unwrap();
    let new_bytes = new_acc.write_bytes().unwrap();
    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();

    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // Wrap in ThreadAccountDiff and roundtrip via bincode
    let update = BlockAccountOperation::AccountMerkleUpdate(mu_bytes.clone());
    let serialized = bincode::serialize(&update).unwrap();
    let deserialized: BlockAccountOperation = bincode::deserialize(&serialized).unwrap();

    // Verify bytes match
    if let BlockAccountOperation::AccountMerkleUpdate(deserialized_bytes) = &deserialized {
        assert_eq!(
            deserialized_bytes, &mu_bytes,
            "Deserialized merkle update bytes should match original"
        );

        // Verify the deserialized bytes can still be applied
        let mu_cell2 = read_single_root_boc(deserialized_bytes).unwrap();
        let mu2 = MerkleUpdate::construct_from_cell(mu_cell2).unwrap();
        let result_cell = mu2.apply_for(&old_cell).unwrap();
        let result_bytes = write_boc(&result_cell).unwrap();
        let result_acc = ThreadAccount::read_bytes(&result_bytes).unwrap();

        assert_eq!(
            result_acc.write_bytes().unwrap(),
            new_acc.write_bytes().unwrap(),
            "Applied merkle update after bincode roundtrip should produce correct account"
        );
    } else {
        panic!("Expected AccountMerkleUpdate variant after deserialization");
    }
}

// ==================== Tier 4: TVM patch_account Tests ====================

#[test]
fn test_patch_account_applies_merkle_update() {
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_block::ShardStateUnsplit;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    // Create ShardAccounts and insert a large account
    let shard_state = ShardStateUnsplit::default();
    let mut accounts = shard_state.read_accounts().unwrap();

    let (routing, large_acc) = new_large_acc(60, 512);
    let shard_acc: ShardAccount = large_acc.clone().try_into().unwrap();
    accounts.insert(&routing.account_id().into(), &shard_acc).unwrap();

    // Create a modified account and merkle update
    let modified = modify_acc_balance(&large_acc, 555_555);
    let old_bytes = large_acc.write_bytes().unwrap();
    let new_bytes = modified.write_bytes().unwrap();
    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();
    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // Apply via patch_account
    crate::thread_accounts::tvm::patch_account(
        *routing.account_id(),
        BlockAccountOperation::AccountMerkleUpdate(mu_bytes),
        &mut accounts,
    )
    .unwrap();

    // Read back and verify
    let result_shard_acc = accounts.account(&routing.account_id().into()).unwrap().unwrap();
    let result_acc = ThreadAccount::from(result_shard_acc);
    assert_eq!(
        result_acc.write_bytes().unwrap(),
        modified.write_bytes().unwrap(),
        "patch_account with AccountMerkleUpdate should produce the modified account"
    );
}

#[test]
fn test_patch_account_merkle_update_nonexistent_account_errors() {
    use tvm_block::ShardStateUnsplit;

    let shard_state = ShardStateUnsplit::default();
    let mut accounts = shard_state.read_accounts().unwrap();

    let (routing, _) = new_large_acc(70, 512);
    let some_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];

    let result = crate::thread_accounts::tvm::patch_account(
        *routing.account_id(),
        BlockAccountOperation::AccountMerkleUpdate(some_bytes),
        &mut accounts,
    );

    assert!(result.is_err(), "Should error when applying merkle update to non-existent account");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("non-existent TVM account"),
        "Error should mention non-existent account, got: {err_msg}"
    );
}

// ==================== Tier 5: Durable Repository Tests ====================

#[test]
fn test_apply_account_merkle_update_from_durable_state() -> anyhow::Result<()> {
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;
    use tvm_types::write_boc;

    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 600));

    // Insert a large account into a durable state
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Manually create merkle update bytes
    let modified = modify_acc_balance(&large_acc, 888_888);
    let old_bytes = large_acc.write_bytes()?;
    let new_bytes = modified.write_bytes()?;
    let old_cell = read_single_root_boc(&old_bytes).unwrap();
    let new_cell = read_single_root_boc(&new_bytes).unwrap();
    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = write_boc(&mu_cell).unwrap();

    // Create a diff with AccountMerkleUpdate and apply it
    // We also need a valid TVM diff (identity since nothing changes in TVM)
    let tvm_cell = state1.tvm.shard_state.serialize().unwrap();
    let tvm_mu = MerkleUpdate::create(&tvm_cell, &tvm_cell).unwrap();

    let diff = crate::ThreadAccountsStateDiff {
        durable: DurableThreadAccountsStateDiff {
            accounts: HashMap::from_iter([(
                routing,
                BlockAccountOperation::AccountMerkleUpdate(mu_bytes),
            )]),
        },
        tvm: crate::thread_accounts::tvm::TvmThreadAccountsStateDiff { update: tvm_mu },
    };

    let applied = repo.state_apply_diff(&state1, diff, ThreadIdentifier::default(), 0)?;

    // Verify the account was correctly updated
    let applied_acc = repo.state_account(&applied.new_state, &routing)?.unwrap();
    assert_eq!(
        applied_acc.write_bytes()?,
        modified.write_bytes()?,
        "Applying AccountMerkleUpdate diff should produce the correct modified account"
    );

    Ok(())
}

#[test]
fn test_apply_account_merkle_update_nonexistent_account_errors() -> anyhow::Result<()> {
    use tvm_block::MerkleUpdate;
    use tvm_block::Serializable;

    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();

    // Create a valid-looking merkle update for an account that doesn't exist
    let (routing, large_acc) = new_large_acc(1, 512);
    let modified = modify_acc_balance(&large_acc, 42);
    let old_bytes = large_acc.write_bytes()?;
    let new_bytes = modified.write_bytes()?;
    let old_cell = tvm_types::read_single_root_boc(&old_bytes).unwrap();
    let new_cell = tvm_types::read_single_root_boc(&new_bytes).unwrap();
    let mu = MerkleUpdate::create(&old_cell, &new_cell).unwrap();
    let mu_cell = mu.serialize().unwrap();
    let mu_bytes = tvm_types::write_boc(&mu_cell).unwrap();

    // Apply to empty state — an account doesn't exist in durable or TVM
    let tvm_cell = state0.tvm.shard_state.serialize().unwrap();
    let tvm_mu = MerkleUpdate::create(&tvm_cell, &tvm_cell).unwrap();

    let diff = crate::ThreadAccountsStateDiff {
        durable: DurableThreadAccountsStateDiff {
            accounts: HashMap::from_iter([(
                routing,
                BlockAccountOperation::AccountMerkleUpdate(mu_bytes),
            )]),
        },
        tvm: crate::thread_accounts::tvm::TvmThreadAccountsStateDiff { update: tvm_mu },
    };

    let result = repo.state_apply_diff(&state0, diff, ThreadIdentifier::default(), 0);
    assert!(result.is_err(), "Should error when applying merkle update for non-existent account");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("non-existent account"),
        "Error should mention non-existent account, got: {err_msg}"
    );

    Ok(())
}

// ==================== Durable State Snapshot Tests ====================

fn new_acc_with_dapp(seed: usize, dapp_seed: usize) -> (AccountRouting, ThreadAccount) {
    let id = AccountIdentifier::new(new_u256("acc", seed));
    let dapp_id = DAppIdentifier::new(new_u256("dapp", dapp_seed));
    let routing = id.routing(dapp_id);
    let tvm_acc = tvm_block::Account::with_address(
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap(),
    );
    let tvm_shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", seed).into(),
        seed as u64,
        Some(dapp_id.as_array().into()),
    )
    .unwrap();
    let acc = ThreadAccount::from(tvm_shard_acc);
    (routing, acc)
}

#[test]
fn test_durable_snapshot_empty_state() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state = new_state();

    let snapshot = repo.export_durable_snapshot(&state)?;

    assert!(snapshot.accounts.is_empty(), "Empty state should have no accounts");

    let (repo2, _dir2) = setup_import_repo(true);
    let new_durable = repo2.import_durable_snapshot(
        snapshot,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;

    // Build a state from the imported durable
    let imported_state = ThreadAccountsState::from_parts(new_durable, state.tvm.clone());

    // State should be empty
    let (routing, _) = new_acc(1);
    assert!(
        repo2.state_account(&imported_state, &routing)?.is_none(),
        "Imported empty state should have no accounts"
    );

    Ok(())
}

#[test]
fn test_durable_snapshot_with_accounts() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert 10 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for i in 1..=10 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Export
    let snapshot = repo.export_durable_snapshot(&state1)?;
    assert!(!snapshot.accounts.is_empty(), "Snapshot should contain account data");

    // Import into a new repo
    let (repo2, _dir2) = setup_import_repo(true);
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));
    let new_durable = repo2.import_durable_snapshot(
        snapshot,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    let imported_state = ThreadAccountsState::from_parts(new_durable, state1.tvm.clone());
    repo2.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &imported_state,
        HashMap::new(),
    )?;
    assert_drained(&repo2);
    repo2.state_save(&block_id_2, &imported_state)?;

    // Verify all accounts
    for (routing, orig_acc) in &accounts {
        let imported_acc = repo2.state_account(&imported_state, routing)?;
        assert!(
            imported_acc.is_some(),
            "Account {} should exist after import",
            routing.account_id().to_hex_string()
        );
        let imported_acc = imported_acc.unwrap();
        assert_eq!(
            imported_acc.vm_account()?.hash(),
            orig_acc.vm_account()?.hash(),
            "Account hash should match for {}",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_preserves_dapp_structure() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Create accounts in 3 different dapp groups
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for dapp in 0..3 {
        for acc in 0..5 {
            let seed = dapp * 100 + acc + 1;
            let (routing, account) = new_acc_with_dapp(seed, dapp + 1);
            builder.insert_account(&routing, &account);
            accounts.push((routing, account));
        }
    }
    let tr1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &tr1.new_state,
        tr1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &tr1.new_state)?;

    // Export and import
    let snapshot = repo.export_durable_snapshot(&tr1.new_state)?;
    assert!(!snapshot.maps.is_empty(), "Should be not empty maps, got",);

    let (repo2, _dir2) = setup_import_repo(true);
    let new_durable = repo2.import_durable_snapshot(
        snapshot,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    let imported_state = ThreadAccountsState::from_parts(new_durable, tr1.new_state.tvm.clone());

    // Verify all accounts in correct dapp groups
    for (routing, orig_acc) in &accounts {
        let imported_acc = repo2.state_account(&imported_state, routing)?;
        assert!(
            imported_acc.is_some(),
            "Account {} in dapp {} should exist",
            routing.account_id().to_hex_string(),
            routing.dapp_id().to_hex_string()
        );
        assert_eq!(imported_acc.unwrap().vm_account()?.hash(), orig_acc.vm_account()?.hash(),);
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_bincode_roundtrip() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert 50 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for i in 1..=50 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let tr1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &tr1.new_state,
        tr1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &tr1.new_state)?;

    // Export, serialize, deserialize
    let snapshot = repo.export_durable_snapshot(&tr1.new_state)?;
    let bytes = bincode::serialize(&snapshot)?;
    let deserialized: crate::DurableStateSnapshot = bincode::deserialize(&bytes)?;

    // Import deserialized snapshot
    let (repo2, _dir2) = setup_import_repo(true);
    let new_durable = repo2.import_durable_snapshot(
        deserialized,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    let imported_state = ThreadAccountsState::from_parts(new_durable, tr1.new_state.tvm.clone());

    for (routing, orig_acc) in &accounts {
        let imported_acc = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported_acc.vm_account()?.hash(),
            orig_acc.vm_account()?.hash(),
            "Account {} should match after bincode roundtrip",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_after_updates() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert 20 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for i in 1..=20 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let tr1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &tr1.new_state,
        tr1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &tr1.new_state)?;

    // Update 5 accounts (seeds 1-5), remove 3 (seeds 6-8), add 5 new (seeds 21-25)
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &tr1.new_state);

    let mut updated_accounts = Vec::new();
    for i in 1..=5 {
        let modified = modify_acc_balance(&accounts[i - 1].1, 999_000 + i as u64);
        builder2.insert_account(&accounts[i - 1].0, &modified);
        updated_accounts.push((accounts[i - 1].0, modified));
    }

    let removed_routings: Vec<_> = (5..8).map(|i| accounts[i].0).collect();
    for routing in &removed_routings {
        builder2.remove_account(routing);
    }

    let mut new_accounts = Vec::new();
    for i in 21..=25 {
        let (routing, acc) = new_acc(i);
        builder2.insert_account(&routing, &acc);
        new_accounts.push((routing, acc));
    }

    let tr2 = builder2.build(None)?;
    repo.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &tr2.new_state,
        tr2.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_2, &tr2.new_state)?;

    // Export and import
    let snapshot = repo.export_durable_snapshot(&tr2.new_state)?;
    let (repo2, _dir2) = setup_import_repo(true);
    let new_durable = repo2.import_durable_snapshot(
        snapshot,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    let imported_state = ThreadAccountsState::from_parts(new_durable, tr2.new_state.tvm.clone());
    for (routing, info) in repo2.durable_map_iter(&imported_state.durable) {
        println!("{routing} {:?}", info);
    }

    // Verify updated accounts
    for (routing, expected_acc) in &updated_accounts {
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported.vm_account()?.hash(),
            expected_acc.vm_account()?.hash(),
            "Updated account should match"
        );
    }

    // Verify removed accounts are gone
    for routing in &removed_routings {
        assert!(
            repo2.state_account(&imported_state, routing)?.is_none(),
            "Removed account should not exist"
        );
    }

    // Verify new accounts
    for (routing, expected_acc) in &new_accounts {
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported.vm_account()?.hash(),
            expected_acc.vm_account()?.hash(),
            "New account should match"
        );
    }

    // Verify unchanged accounts (seeds 9-20)
    for (i, account) in accounts.iter().enumerate().take(20).skip(8) {
        let routing = &account.0;
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported.vm_account()?.hash(),
            account.1.vm_account()?.hash(),
            "Unchanged account {} should match",
            i + 1
        );
    }

    Ok(())
}

static LOG_INIT: OnceCell<()> = OnceCell::new();

pub fn init_logs() {
    LOG_INIT.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_test_writer()
            .init();
    });
}

#[test]
fn test_durable_snapshot_cross_repo_import() -> anyhow::Result<()> {
    init_logs();
    // Repo 1: has accounts 1-10
    let (repo1, _dir1) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    let mut accounts1 = Vec::new();
    let mut builder = repo1.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for i in 1..=10 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts1.push((routing, acc));
    }
    let tr1 = builder.build(None)?;
    repo1.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &tr1.new_state,
        tr1.account_operations,
    )?;
    assert_drained(&repo1);
    repo1.state_save(&block_id_1, &tr1.new_state)?;

    // Repo 2: has accounts 101-110
    let (repo2, _dir2) = setup_repo(true);
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));
    let block_id_3 = BlockIdentifier::new(new_u256("block", 3));

    let mut accounts2 = Vec::new();
    let mut builder2 = repo2.state_builder(&ThreadIdentifier::default(), 0, &new_state());
    for i in 101..=110 {
        let (routing, acc) = new_acc(i);
        builder2.insert_account(&routing, &acc);
        accounts2.push((routing, acc));
    }
    let tr2 = builder2.build(None)?;
    repo2.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &tr2.new_state,
        tr2.account_operations,
    )?;
    assert_drained(&repo2);
    repo2.state_save(&block_id_2, &tr2.new_state)?;

    // Export from repo1, import into repo2 (reset archive first to go back to Uninitialized)
    repo2.reset_archive()?;
    let snapshot = repo1.export_durable_snapshot(&tr1.new_state)?;
    let imported_durable = repo2.import_durable_snapshot(
        snapshot,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    let imported_state =
        ThreadAccountsState::from_parts(imported_durable, tr1.new_state.tvm.clone());
    repo2.finalize_thread_transition(
        &block_id_3,
        &ThreadIdentifier::default(),
        0,
        &imported_state,
        HashMap::new(),
    )?;
    assert_drained(&repo2);
    repo2.state_save(&block_id_3, &imported_state)?;

    // Verify imported accounts are accessible via block_id_3
    for (routing, orig_acc) in &accounts1 {
        let acc = repo2.state_account(&imported_state, routing)?;
        assert!(
            acc.is_some(),
            "Imported account {} should exist",
            routing.account_id().to_hex_string()
        );
        assert_eq!(acc.unwrap().vm_account()?.hash(), orig_acc.vm_account()?.hash());
    }

    // After `reset_archive`, repo2's PRE-RESET accounts are intentionally
    // unreachable — the archive is bound to the new data_epoch and the
    // old per-epoch sets are no longer referenced (and will be truncated).
    // The pre-reset state object (`tr2.new_state`) still has those
    // routings in its map, so `find_account_with_info` reaches its typed
    // hash-mismatch error. That error IS the correct outcome — it proves
    // the per-epoch isolation is doing its job. (Under the previous
    // buggy semantic, the old bytes were still served and the error
    // would never fire.)
    for (routing, orig_acc) in &accounts2 {
        let result = repo2.state_account(&tr2.new_state, routing);
        let err = result.unwrap_err();
        let mismatch = err.downcast_ref::<AccountHashMismatchError>().unwrap_or_else(|| {
            panic!(
                "Pre-reset account {} expected AccountHashMismatchError, got: {err:?}",
                routing.account_id().to_hex_string()
            )
        });
        assert_eq!(mismatch.routing, *routing);
        assert_eq!(mismatch.expected_hash, orig_acc.vm_account().unwrap().hash());

        let msg = err.to_string();
        assert!(
            msg.contains("exists in map") && msg.contains("no matching version"),
            "Pre-reset account {} expected the per-epoch-isolation error display, got: {msg}",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

#[test]
#[ignore]
fn test_durable_snapshot_with_large_accounts() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    for i in 1..=100 {
        let (routing, acc) = new_large_acc(i, 4096);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let tr1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &tr1.new_state,
        tr1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id, &tr1.new_state)?;

    let start = Instant::now();
    let snapshot = repo.export_durable_snapshot(&tr1.new_state)?;
    println!("Exported {} accounts in {:?}", accounts.len(), start.elapsed());

    let bytes = bincode::serialize(&snapshot)?;
    println!("Serialized durable snapshot: {} bytes", bytes.len());

    let deserialized: crate::DurableStateSnapshot = bincode::deserialize(&bytes)?;

    let (repo2, _dir2) = setup_repo(true);
    let start = Instant::now();
    let new_durable = repo2.import_durable_snapshot(
        deserialized,
        &ThreadIdentifier::default(),
        &BlockIdentifier::default(),
    )?;
    println!("Imported in {:?}", start.elapsed());

    let imported_state = ThreadAccountsState::from_parts(new_durable, tr1.new_state.tvm.clone());

    for (routing, orig_acc) in &accounts {
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(imported.vm_account()?.hash(), orig_acc.vm_account()?.hash());
    }

    Ok(())
}

// ==================== Durable Redirect Stub Tests ====================

#[test]
fn test_redirect_stub_created_for_dapp_account() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Insert an account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(1, 10);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    assert_ne!(dapp_id, account_id.redirect_dapp_id(), "dapp_id should differ from account_id");

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id, &state1)?;

    // The real account should be at (dapp_id, account_id)
    let real = repo.state_account(&state1, &routing)?;
    assert!(real.is_some(), "Real account should exist at (dapp_id, account_id)");
    let real = real.unwrap();
    assert!(!real.is_redirect(), "Real account should not be a redirect");
    assert_eq!(
        real.vm_account()?.hash(),
        acc.vm_account()?.hash(),
        "Real account data should match"
    );

    // Lookup at default routing (account_id, account_id) follows the redirect
    // and returns the real account
    let default_routing = account_id.redirect();
    let followed = repo.state_account(&state1, &default_routing)?;
    assert!(followed.is_some(), "Default routing lookup should find account via redirect");
    let followed = followed.unwrap();
    assert!(!followed.is_redirect(), "Followed redirect should return the real account");
    assert_eq!(
        followed.get_dapp_id(),
        Some(dapp_id),
        "Followed account should have the actual dapp_id"
    );
    assert_eq!(
        followed.vm_account()?.hash(),
        acc.vm_account()?.hash(),
        "Followed account data should match original"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_lookup_returns_redirect() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    let (routing, acc) = new_acc_with_dapp(2, 20);
    let account_id = *routing.account_id();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id, &state1)?;

    // Lookup via default routing follows the redirect and returns the real account
    let default_routing = account_id.redirect();
    let result = repo.state_account(&state1, &default_routing)?;
    assert!(result.is_some(), "Default routing lookup should find account via redirect");
    let followed = result.unwrap();
    assert!(
        !followed.is_redirect(),
        "Default routing should return the real account (via redirect)"
    );
    assert_eq!(
        followed.vm_account()?.hash(),
        acc.vm_account()?.hash(),
        "Account data via default routing should match original"
    );

    // Lookup via real routing returns the real account directly
    let result = repo.state_account(&state1, &routing)?;
    assert!(result.is_some(), "Real routing lookup should find the account");
    assert!(!result.unwrap().is_redirect(), "Real routing should return the actual account");

    Ok(())
}

#[test]
fn test_redirect_stub_is_materialized_in_archive() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    let (routing, acc) = new_acc_with_dapp(26, 260);
    let default_routing = routing.account_id().redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations,
    )?;
    assert_drained(&repo);

    let archived_redirect =
        repo.durable_map_repo().archive_account_for_test(&default_routing)?.unwrap();
    assert!(archived_redirect.is_redirect(), "Redirect route must be materialized in archive");
    assert_eq!(
        archived_redirect.get_dapp_id(),
        Some(*routing.dapp_id()),
        "Archived redirect must point to the real dapp"
    );

    let archived_real = repo.durable_map_repo().archive_account_for_test(&routing)?.unwrap();
    assert!(!archived_real.is_redirect(), "Real route must keep the real account body");
    assert_eq!(archived_real.vm_account()?.hash(), acc.vm_account()?.hash());

    Ok(())
}

#[test]
fn test_redirect_stub_removed_when_account_removed() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert an account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(3, 30);
    let account_id = *routing.account_id();
    let default_routing = account_id.redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Verify redirect exists
    assert!(repo.state_account(&state1, &default_routing)?.is_some(), "Redirect should exist");

    // Remove the account at its real routing
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.remove_account(&routing);
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &state2,
        transition2.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_2, &state2)?;

    // Both real account and redirect should be gone
    assert!(repo.state_account(&state2, &routing)?.is_none(), "Real account should be removed");
    assert!(
        repo.state_account(&state2, &default_routing)?.is_none(),
        "Redirect stub should also be removed"
    );
    assert!(
        repo.durable_map_repo().archive_account_for_test(&default_routing)?.is_none(),
        "Redirect archive entry should also be removed"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_for_move_from_tvm() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Step 1: Insert an account with dapp_id into TVM (apply_to_durable = false)
    let (routing, acc) = new_acc_with_dapp(4, 40);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    let default_routing = account_id.redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.set_apply_to_durable(false);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Account should be in TVM, not durable
    assert!(
        repo.state_account(&state1, &default_routing)?.is_some(),
        "Account should be findable in TVM via default routing"
    );

    // Step 2: Move from TVM to durable
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.set_apply_to_durable(true);
    builder2.move_from_tvm(1000)?;
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &state2,
        transition2.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_2, &state2)?;

    // The real account should be at (dapp_id, account_id) in durable
    let real = repo.state_account(&state2, &routing)?;
    assert!(real.is_some(), "Real account should exist in durable at (dapp_id, account_id)");
    assert!(!real.unwrap().is_redirect(), "Real account should not be a redirect");

    // Default routing follows redirect → returns the real account
    let followed = repo.state_account(&state2, &default_routing)?;
    assert!(followed.is_some(), "Default routing should find account via redirect");
    let followed = followed.unwrap();
    assert!(!followed.is_redirect(), "Followed redirect should return real account");
    assert_eq!(
        followed.get_dapp_id(),
        Some(dapp_id),
        "Followed account should have actual dapp_id"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_for_replace_with_redirect() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert an account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(5, 50);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    let default_routing = account_id.redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Replace the real account with a redirect at (dapp_id, account_id)
    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    builder2.replace_with_redirect(&routing)?;
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &state2,
        transition2.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_2, &state2)?;

    // The entry at (dapp_id, account_id) should be a redirect
    let entry_at_real = repo.state_account(&state2, &routing)?;
    assert!(entry_at_real.is_some(), "Entry at real routing should exist");
    assert!(entry_at_real.unwrap().is_redirect(), "Entry at real routing should be a redirect");

    // Default routing follows redirect → finds the cross-thread redirect at (dapp_id, account_id)
    let followed = repo.state_account(&state2, &default_routing)?;
    assert!(followed.is_some(), "Default routing should find entry via redirect");
    let followed = followed.unwrap();
    assert!(followed.is_redirect(), "Followed entry should be a cross-thread redirect");
    assert_eq!(
        followed.get_dapp_id(),
        Some(dapp_id),
        "Cross-thread redirect should point to actual dapp_id"
    );

    Ok(())
}

#[test]
fn test_no_redirect_stub_for_default_routing_account() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Insert an account with dapp_id == account_id (default routing)
    let (routing, acc) = new_acc(1);
    assert_eq!(
        *routing.dapp_id(),
        routing.account_id().redirect_dapp_id(),
        "This test requires dapp_id == account_id"
    );

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id, &state1)?;

    // Account should exist at its routing
    let result = repo.state_account(&state1, &routing)?;
    assert!(result.is_some(), "Account should exist");
    assert!(!result.unwrap().is_redirect(), "Account should not be a redirect");

    // The diff should contain exactly 1 entry (no extra redirect)
    assert_eq!(
        transition.diff.durable.accounts.len(),
        1,
        "No redirect stub needed when dapp_id == account_id"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_diff_contains_both_entries() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();

    // Insert an account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(6, 60);
    let account_id = *routing.account_id();
    let default_routing = account_id.redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;

    // The diff should contain 2 entries: real account + redirect stub
    let durable_accounts = &transition.diff.durable.accounts;
    assert_eq!(durable_accounts.len(), 2, "Diff should contain real account and redirect stub");

    // Find entries by routing
    let real_entry = durable_accounts.get(&routing);
    assert!(real_entry.is_some(), "Diff should contain entry at real routing");
    assert!(
        matches!(real_entry.unwrap(), BlockAccountOperation::UpdateOrInsert(_)),
        "Real entry should be UpdateOrInsert"
    );

    let redirect_entry = durable_accounts.get(&default_routing);
    assert!(redirect_entry.is_some(), "Diff should contain redirect entry at default routing");
    if let BlockAccountOperation::UpdateOrInsert(redirect_acc) = &redirect_entry.unwrap() {
        assert!(redirect_acc.is_redirect(), "Redirect entry should be a redirect");
    } else {
        panic!("Redirect entry should be UpdateOrInsert");
    }

    Ok(())
}

#[test]
fn test_redirect_stub_updated_when_account_dapp_changes() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert an account with dapp_id_1
    let id = AccountIdentifier::new(new_u256("acc", 7));
    let dapp_id_1 = DAppIdentifier::new(new_u256("dapp", 71));
    let routing1 = id.routing(dapp_id_1);
    let default_routing = id.redirect();

    let tvm_acc = tvm_block::Account::with_address(
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap(),
    );
    let shard_acc_1 = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", 7).into(),
        7,
        Some(dapp_id_1.as_array().into()),
    )
    .unwrap();
    let acc1 = ThreadAccount::from(shard_acc_1);

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing1, &acc1);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Verify default routing follows redirect → returns a real account with dapp_id_1
    let followed1 = repo.state_account(&state1, &default_routing)?.unwrap();
    assert!(!followed1.is_redirect(), "Should follow redirect to real account");
    assert_eq!(followed1.get_dapp_id(), Some(dapp_id_1));

    // Now change the account's dapp to dapp_id_2
    let dapp_id_2 = DAppIdentifier::new(new_u256("dapp", 72));
    let routing2 = id.routing(dapp_id_2);

    let shard_acc_2 = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", 8).into(),
        8,
        Some(dapp_id_2.as_array().into()),
    )
    .unwrap();
    let acc2 = ThreadAccount::from(shard_acc_2);

    let mut builder2 = repo.state_builder(&ThreadIdentifier::default(), 0, &state1);
    // Remove from old routing, insert at new routing
    builder2.remove_account(&routing1);
    builder2.insert_account(&routing2, &acc2);
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.finalize_thread_transition(
        &block_id_2,
        &ThreadIdentifier::default(),
        0,
        &state2,
        transition2.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_2, &state2)?;

    // Old routing should be removed
    assert!(repo.state_account(&state2, &routing1)?.is_none(), "Old routing should be empty");

    // New routing should have the account
    let real = repo.state_account(&state2, &routing2)?.unwrap();
    assert!(!real.is_redirect(), "New routing should have real account");

    // Default routing follows redirect → returns a real account with dapp_id_2
    let followed2 = repo.state_account(&state2, &default_routing)?;
    assert!(followed2.is_some(), "Default routing should find account via redirect");
    let followed2 = followed2.unwrap();
    assert!(!followed2.is_redirect(), "Should follow redirect to real account");
    assert_eq!(
        followed2.get_dapp_id(),
        Some(dapp_id_2),
        "Followed account should now have dapp_id_2"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_with_account_inserted_at_wrong_routing() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Create an account with dapp_id != account_id but insert at default routing
    // (simulates build_actions.rs using acc_id.dapp_originator())
    let id = AccountIdentifier::new(new_u256("acc", 8));
    let dapp_id = DAppIdentifier::new(new_u256("dapp", 80));
    let default_routing = id.redirect();
    let correct_routing = id.routing(dapp_id);

    let tvm_acc = tvm_block::Account::with_address(
        MsgAddressInt::with_standart(None, 0, AccountId::from_raw(id.as_slice().to_vec(), 256))
            .unwrap(),
    );
    let shard_acc = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", 8).into(),
        8,
        Some(dapp_id.as_array().into()),
    )
    .unwrap();
    let acc = ThreadAccount::from(shard_acc);
    assert_eq!(acc.get_dapp_id(), Some(dapp_id), "Account should have dapp_id set");

    // Insert at default routing (wrong — dapp_id != account_id)
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&default_routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id, &state1)?;

    // Account should be rerouted to the correct routing (dapp_id, account_id)
    let real = repo.state_account(&state1, &correct_routing)?;
    assert!(real.is_some(), "Account should exist at correct routing (dapp_id, account_id)");
    assert!(!real.unwrap().is_redirect(), "Account at correct routing should not be a redirect");

    // Default routing follows redirect → returns the real account
    let followed = repo.state_account(&state1, &default_routing)?;
    assert!(followed.is_some(), "Default routing should find account via redirect");
    let followed = followed.unwrap();
    assert!(!followed.is_redirect(), "Followed redirect should return real account");
    assert_eq!(
        followed.get_dapp_id(),
        Some(dapp_id),
        "Followed account should have actual dapp_id"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_diff_applied_correctly() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert an account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(9, 90);
    let account_id = *routing.account_id();
    let default_routing = account_id.redirect();

    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.finalize_thread_transition(
        &block_id_1,
        &ThreadIdentifier::default(),
        0,
        &state1,
        transition.account_operations.clone(),
    )?;
    assert_drained(&repo);
    repo.state_save(&block_id_1, &state1)?;

    // Apply the diff to the old state (simulates a receiving node)
    let applied =
        repo.state_apply_diff(&state0, transition.diff, ThreadIdentifier::default(), 0)?;

    // Real account should be accessible at the real routing
    let real_from_applied = repo.state_account(&applied.new_state, &routing)?;
    assert!(real_from_applied.is_some(), "Real account should exist in applied state");
    assert_eq!(
        real_from_applied.unwrap().vm_account()?.hash(),
        acc.vm_account()?.hash(),
        "Applied state should have correct account data"
    );

    // Default routing follows redirect → returns the real account
    let followed_from_applied = repo.state_account(&applied.new_state, &default_routing)?;
    assert!(followed_from_applied.is_some(), "Default routing should find account via redirect");
    let followed_from_applied = followed_from_applied.unwrap();
    assert!(!followed_from_applied.is_redirect(), "Followed redirect should return real account");
    assert_eq!(
        followed_from_applied.vm_account()?.hash(),
        acc.vm_account()?.hash(),
        "Followed account data should match original"
    );

    Ok(())
}

// ==================== Routing Lookup Stress Tests ====================

/// Lightweight AVM account with originator routing (dapp_id == account_id)
fn small_acc(seed: usize) -> (AccountRouting, ThreadAccount) {
    use node_types::AccountHash;
    let id = AccountIdentifier::new(new_u256("sm_acc", seed));
    let routing = id.redirect();
    let acc = ThreadAccount::from_avm(AvmThreadAccount {
        vm_account: AvmAccount::new(
            AccountHash::new(new_u256("sm_hash", seed)),
            AvmAccountMetadata { id, ..Default::default() },
            None,
            None,
            None,
        ),
        last_trans_hash: TransactionHash::new(new_u256("sm_tx", seed)),
        last_trans_lt: seed as u64,
    });
    (routing, acc)
}

/// Lightweight AVM account with separate dapp_id
fn small_acc_dapp(acc_seed: usize, dapp_seed: usize) -> (AccountRouting, ThreadAccount) {
    use node_types::AccountHash;
    let id = AccountIdentifier::new(new_u256("sm_acc", acc_seed));
    let dapp_id = DAppIdentifier::new(new_u256("sm_dapp", dapp_seed));
    let routing = id.routing(dapp_id);
    let acc = ThreadAccount::from_avm(AvmThreadAccount {
        vm_account: AvmAccount::new(
            AccountHash::new(new_u256("sm_hash", acc_seed)),
            AvmAccountMetadata { id, ..Default::default() },
            None,
            None,
            None,
        ),
        last_trans_hash: TransactionHash::new(new_u256("sm_tx", acc_seed)),
        last_trans_lt: acc_seed as u64,
    });
    (routing, acc)
}

/// Lightweight AVM account with explicit 32-byte key
fn small_acc_key(key: [u8; 32], seed: usize) -> (AccountRouting, ThreadAccount) {
    use node_types::AccountHash;
    let id = AccountIdentifier::new(key);
    let routing = id.redirect();
    let acc = ThreadAccount::from_avm(AvmThreadAccount {
        vm_account: AvmAccount::new(
            AccountHash::new(new_u256("sm_hash_k", seed)),
            AvmAccountMetadata { id, ..Default::default() },
            None,
            None,
            None,
        ),
        last_trans_hash: TransactionHash::new(new_u256("sm_tx_k", seed)),
        last_trans_lt: seed as u64,
    });
    (routing, acc)
}

fn insert_accounts(
    repo: &ThreadAccountsRepository,
    state: &ThreadAccountsState,
    accounts: &[(AccountRouting, ThreadAccount)],
) -> anyhow::Result<(ThreadAccountsState, HashMap<AccountRouting, ArchiveOperation>)> {
    let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, state);
    for (routing, acc) in accounts {
        builder.insert_account(routing, acc);
    }
    let tr = builder.build(None)?;
    Ok((tr.new_state, tr.account_operations))
}

fn verify_all(
    repo: &ThreadAccountsRepository,
    state: &ThreadAccountsState,
    accounts: &[(AccountRouting, ThreadAccount)],
    label: &str,
) {
    for (i, (routing, _)) in accounts.iter().enumerate() {
        let found = repo.state_account(state, routing).unwrap();
        assert!(
            found.is_some(),
            "{label}: account {i}/{} not found: dapp={}, acc={}",
            accounts.len(),
            routing.dapp_id().to_hex_string(),
            routing.account_id().to_hex_string(),
        );
    }
}

#[test]
fn stress_originator_routing() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let accounts: Vec<_> = (0..10_000).map(small_acc).collect();
    (state, _) = insert_accounts(&repo, &state, &accounts)?;
    verify_all(&repo, &state, &accounts, "originator");
    Ok(())
}

#[test]
fn stress_separate_dapps() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let mut accounts = Vec::new();
    for d in 0..200 {
        for a in 0..50 {
            accounts.push(small_acc_dapp(d * 1000 + a, d + 1));
        }
    }
    (state, _) = insert_accounts(&repo, &state, &accounts)?;
    verify_all(&repo, &state, &accounts, "separate_dapps");
    Ok(())
}

#[test]
fn stress_incremental_verify_each_step() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let mut all: Vec<(AccountRouting, ThreadAccount)> = Vec::new();
    for batch in 0..50 {
        let accounts: Vec<_> = (0..200)
            .map(|i| {
                let seed = batch * 200 + i;
                if i % 3 == 0 {
                    small_acc_dapp(seed, batch + 1)
                } else {
                    small_acc(seed)
                }
            })
            .collect();
        (state, _) = insert_accounts(&repo, &state, &accounts)?;
        all.extend(accounts);
        verify_all(&repo, &state, &all, &format!("batch_{batch}"));
    }
    Ok(())
}

#[test]
fn stress_save_load_verify() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut all: Vec<(AccountRouting, ThreadAccount)> = Vec::new();

    let p1: Vec<_> = (0..5000).map(small_acc).collect();
    let (state, account_operations) = insert_accounts(&repo, &new_state(), &p1)?;
    all.extend(p1);
    repo.finalize_thread_transition(
        &BlockIdentifier::new(new_u256("blk", 1)),
        &ThreadIdentifier::default(),
        0,
        &state,
        account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&BlockIdentifier::new(new_u256("blk", 1)), &state)?;
    verify_all(&repo, &state, &all, "after_save_1");

    let p2: Vec<_> = (0..2000).map(|i| small_acc_dapp(50000 + i, i % 100 + 1)).collect();
    let (state, account_operations) = insert_accounts(&repo, &state, &p2)?;
    all.extend(p2);
    verify_all(&repo, &state, &all, "after_phase2");

    repo.finalize_thread_transition(
        &BlockIdentifier::new(new_u256("blk", 2)),
        &ThreadIdentifier::default(),
        0,
        &state,
        account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&BlockIdentifier::new(new_u256("blk", 2)), &state)?;
    verify_all(&repo, &state, &all, "after_commit");
    Ok(())
}

#[test]
fn stress_insert_remove_reinsert() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();

    let accounts: Vec<_> = (0..5000).map(small_acc).collect();
    (state, _) = insert_accounts(&repo, &state, &accounts)?;

    {
        let mut builder = repo.state_builder(&ThreadIdentifier::default(), 0, &state);
        for (i, (routing, _)) in accounts.iter().enumerate() {
            if i % 3 == 0 {
                builder.remove_account(routing);
            }
        }
        state = builder.build(None)?.new_state;
    }

    for (i, (routing, _)) in accounts.iter().enumerate() {
        let found = repo.state_account(&state, routing)?;
        if i % 3 == 0 {
            assert!(found.is_none(), "Removed {i} should be None");
        } else {
            assert!(found.is_some(), "Kept {i} should exist");
        }
    }

    let reinserted: Vec<_> = accounts
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 == 0)
        .map(|(i, _)| small_acc(10000 + i))
        .collect();
    (state, _) = insert_accounts(&repo, &state, &reinserted)?;

    for (i, (routing, _)) in accounts.iter().enumerate() {
        if i % 3 != 0 {
            assert!(repo.state_account(&state, routing)?.is_some(), "Kept {i} gone");
        }
    }
    for (i, (routing, _)) in reinserted.iter().enumerate() {
        assert!(repo.state_account(&state, routing)?.is_some(), "Reinserted {i} not found");
    }
    Ok(())
}

#[test]
fn stress_high_nibble_keys() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let mut all = Vec::new();

    let bg: Vec<_> = (0..5000).map(small_acc).collect();
    all.extend(bg);

    // Keys with every possible first nibble pair for 0xe* and 0xf*
    for hi in 0x0eu8..=0x0f {
        for lo in 0x00u8..=0x0f {
            let prefix = (hi << 4) | lo;
            let mut key = new_u256("hn", prefix as usize);
            key[0] = prefix;
            all.push(small_acc_key(key, 9000 + prefix as usize));
        }
    }

    let account_operations;
    (state, account_operations) = insert_accounts(&repo, &state, &all)?;
    verify_all(&repo, &state, &all, "high_nibble");

    repo.finalize_thread_transition(
        &BlockIdentifier::new(new_u256("blk", 1)),
        &ThreadIdentifier::default(),
        0,
        &state,
        account_operations,
    )?;
    assert_drained(&repo);
    repo.state_save(&BlockIdentifier::new(new_u256("blk", 1)), &state)?;
    verify_all(&repo, &state, &all, "high_nibble_after_save");
    Ok(())
}

#[test]
fn stress_same_first_nibble() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let mut all = Vec::new();

    for i in 0..2000 {
        let mut key = new_u256("same_prefix", i);
        key[0] = 0xe1;
        all.push(small_acc_key(key, i));
    }

    (state, _) = insert_accounts(&repo, &state, &all)?;
    verify_all(&repo, &state, &all, "same_first_nibble");
    Ok(())
}

// #[test]
// fn stress_snapshot_roundtrip() -> anyhow::Result<()> {
//     let (repo, _dir) = setup_repo(true);
//     let mut state = new_state();
//     let mut all = Vec::new();
//     for i in 0..5000 {
//         if i % 4 == 0 { all.push(small_acc_dapp(i, i / 10 + 1)); }
//         else { all.push(small_acc(i)); }
//     }
//     state = insert_accounts(&repo, &state, &all)?;
//
//     let snapshot = repo.export_durable_snapshot(&state)?;
//     let (repo2, _dir2) = setup_repo(true);
//     let imported = repo2.import_durable_snapshot(snapshot, &ThreadIdentifier::default(), &BlockIdentifier::default())?;
//
//     assert_eq!(repo.state_hash(&state), repo2.state_hash(&imported));
//     verify_all(&repo2, &imported, &all, "snapshot_import");
//     Ok(())
// }

#[test]
fn stress_many_accounts_one_dapp() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let accounts: Vec<_> = (0..10_000).map(|i| small_acc_dapp(i, 1)).collect();
    (state, _) = insert_accounts(&repo, &state, &accounts)?;
    verify_all(&repo, &state, &accounts, "single_dapp_10k");
    Ok(())
}

#[test]
fn stress_one_account_per_dapp() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let accounts: Vec<_> = (0..10_000).map(|i| small_acc_dapp(i, i + 1)).collect();
    (state, _) = insert_accounts(&repo, &state, &accounts)?;
    verify_all(&repo, &state, &accounts, "10k_dapps_x_1");
    Ok(())
}

#[test]
fn stress_100k_routings() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let mut state = new_state();
    let mut all: Vec<(AccountRouting, ThreadAccount)> = Vec::new();

    // 100k accounts inserted in batches of 10k, verified after each batch
    for batch in 0..10 {
        let accounts: Vec<_> = (0..10_000)
            .map(|i| {
                let seed = batch * 10_000 + i;
                match seed % 5 {
                    0 => small_acc(seed),                      // originator
                    1 => small_acc_dapp(seed, seed / 100 + 1), // many dapps
                    2 => small_acc_dapp(seed, batch + 1),      // few dapps, many accounts
                    3 => {
                        // forced high nibble key
                        let mut key = new_u256("100k", seed);
                        key[0] = 0xe0 + (seed % 32) as u8;
                        small_acc_key(key, seed)
                    }
                    _ => small_acc(seed),
                }
            })
            .collect();
        (state, _) = insert_accounts(&repo, &state, &accounts)?;
        all.extend(accounts);
        verify_all(&repo, &state, &all, &format!("100k_batch_{batch}"));
        eprintln!("  batch {batch}: {} accounts verified", all.len());
    }

    Ok(())
}

// ==================== Snapshot pin protocol tests (spec/SNAPSHOT.md) ====================

/// Regression for the panic
///   "Account R exists in map (hash H_pin) but no matching version found in
///    pool, accumulator, or archive"
///
/// Before the snapshot pin protocol, the export iterated the live archive.
/// If a routing was updated between the snapshot block being finalized and
/// the export reading the archive, the snapshot shipped bytes hashing to
/// the *new* version while the snapshot map still recorded the *old* hash.
/// The receiver bailed on lookup.
///
/// The unfinalized accounts pool is drained at finalization bounds. Once
/// block 2 finalizes and overwrites archive routing bytes, block 1's
/// body is no longer reachable unless another operational state still
/// holds it above the finalized bound.
#[test]
fn test_export_fails_for_drained_finalized_body_after_archive_overwrite() -> anyhow::Result<()> {
    use crate::AnchorBlockRef;
    let (repo, _dir) = setup_repo(true);
    let thread_id = ThreadIdentifier::default();
    let mut state = new_state();

    // Block 1: insert account X with body v1.
    let (routing, acc_v1) = new_acc(7);
    let block1 = BlockIdentifier::new(new_u256("block", 1));
    let mut builder = repo.state_builder(&thread_id, 1, &state);
    builder.insert_account(&routing, &acc_v1);
    let transition1 = builder.build(None)?;
    let snapshot_state = transition1.new_state.clone();
    repo.finalize_thread_transition(
        &block1,
        &thread_id,
        1,
        &snapshot_state,
        transition1.account_operations,
    )?;
    assert_drained(&repo);
    state = transition1.new_state;

    // Block 2: update X to body v2 — this overwrites the archive's
    // routing record. Export from block 1 must fail after the block 1
    // operational version is drained at block 2 finalization.
    let (_, acc_v2) = new_acc(7777);
    let block2 = BlockIdentifier::new(new_u256("block", 2));
    let mut builder = repo.state_builder(&thread_id, 2, &state);
    builder.insert_account(&routing, &acc_v2);
    let transition2 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block2,
        &thread_id,
        2,
        &transition2.new_state,
        transition2.account_operations,
    )?;
    assert_drained(&repo);

    let mut snapshot = std::io::Cursor::new(Vec::new());
    let err = repo
        .durable_map_repo()
        .export_durable_snapshot_to_writer(&snapshot_state.durable, &mut snapshot)
        .expect_err("block 1 body must be drained after block 2 finalization");
    assert!(
        err.to_string().contains("not found in"),
        "unexpected export error after finalized body drain: {err:#}",
    );

    // Sanity: the AnchorBlockRef pin_set expansion still works as a
    // smoke test of the public API (no behavioural assertion — this
    // just exercises the type so it can't silently regress).
    let mut refs = std::collections::HashMap::new();
    refs.insert(ThreadIdentifier::default(), block1);
    let anchor = AnchorBlockRef {
        anchor_block_id: block1,
        anchor_thread_id: thread_id,
        cross_thread_refs: refs,
    };
    let set = anchor.pin_set();
    assert_eq!(set.len(), 1);
    assert_eq!(set.get(&thread_id), Some(&block1));

    Ok(())
}

#[test]
fn test_push_transition_does_not_preserve_imported_archive_only_version() -> anyhow::Result<()> {
    let (source_repo, _source_dir) = setup_repo(true);
    let thread_id = ThreadIdentifier::default();
    let source_state = new_state();

    let (routing, acc_v1) = new_acc(7001);
    let block1 = BlockIdentifier::new(new_u256("block", 7001));
    let mut builder = source_repo.state_builder(&thread_id, 1, &source_state);
    builder.insert_account(&routing, &acc_v1);
    let transition1 = builder.build(None)?;
    let snapshot_state = transition1.new_state.clone();
    source_repo.finalize_thread_transition(
        &block1,
        &thread_id,
        1,
        &snapshot_state,
        transition1.account_operations,
    )?;
    assert_drained(&source_repo);

    let mut snapshot = std::io::Cursor::new(Vec::new());
    source_repo
        .durable_map_repo()
        .export_durable_snapshot_to_writer(&snapshot_state.durable, &mut snapshot)?;
    let snapshot_bytes = snapshot.into_inner();

    let (import_repo, _import_dir) = setup_import_repo(true);
    let imported_durable = import_repo.durable_map_repo().import_durable_snapshot_from_reader(
        &mut std::io::Cursor::new(snapshot_bytes),
        &thread_id,
        &block1,
    )?;
    let imported_state =
        crate::ThreadAccountsState::from_parts(imported_durable, snapshot_state.tvm.clone());

    let (_, acc_v2) = new_acc(7002);
    let block2 = BlockIdentifier::new(new_u256("block", 7002));
    let mut builder = import_repo.state_builder(&thread_id, 2, &imported_state);
    builder.insert_account(&routing, &acc_v2);
    let transition2 = builder.build(None)?;
    import_repo.finalize_thread_transition(
        &block2,
        &thread_id,
        2,
        &transition2.new_state,
        transition2.account_operations,
    )?;
    assert_drained(&import_repo);

    let current =
        import_repo.state_account(&transition2.new_state, &routing)?.ok_or_else(|| {
            anyhow::anyhow!("updated account must remain readable after archive update")
        })?;
    assert_eq!(current.vm_account()?.hash(), acc_v2.vm_account()?.hash());

    let mut old_snapshot = std::io::Cursor::new(Vec::new());
    let err = import_repo
        .durable_map_repo()
        .export_durable_snapshot_to_writer(&imported_state.durable, &mut old_snapshot)
        .expect_err("archive-only v1 must not be preserved into the unfinalized pool");
    assert!(
        err.to_string().contains("not found in"),
        "unexpected export error after archive-only version overwrite: {err:#}",
    );

    Ok(())
}

/// Fix A: producer-side alignment fires when `request_snapshot_pin` is
/// called *before* the matching `push_transition`. The boundary batch
/// must reach the update loop without waiting for the next push or for
/// `is_full` — i.e. `acquire_snapshot_pin` must succeed promptly.
#[test]
fn test_pin_acquires_promptly_when_request_precedes_push() -> anyhow::Result<()> {
    use std::collections::HashMap;

    use crate::AnchorBlockRef;
    let (repo, _dir) = setup_repo(true);
    let thread_id = ThreadIdentifier::default();
    let mut state = new_state();

    // Get the thread to a known starting state with a single push.
    let (routing0, acc0) = new_acc(100);
    let block0 = BlockIdentifier::new(new_u256("block", 0));
    let mut builder = repo.state_builder(&thread_id, 0, &state);
    builder.insert_account(&routing0, &acc0);
    let transition0 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block0,
        &thread_id,
        0,
        &transition0.new_state,
        transition0.account_operations,
    )?;
    assert_drained(&repo);
    state = transition0.new_state;

    // Fix A path: request the pin BEFORE pushing the anchor's transition.
    let block1 = BlockIdentifier::new(new_u256("block", 1));
    let anchor = AnchorBlockRef {
        anchor_block_id: block1,
        anchor_thread_id: thread_id,
        cross_thread_refs: HashMap::new(),
    };
    let mut pin_request_guard = repo.request_snapshot_pin(anchor);

    // Now push the anchor. The alignment hook in `push_transition`
    // should detect the requested anchor matches and force-flush the
    // batch in the same critical section.
    let (routing1, acc1) = new_acc(200);
    let mut builder = repo.state_builder(&thread_id, 0, &state);
    builder.insert_account(&routing1, &acc1);
    let transition1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block1,
        &thread_id,
        0,
        &transition1.new_state,
        transition1.account_operations,
    )?;

    // Acquire with a tight timeout. If alignment fired correctly the
    // boundary batch is already in the channel and the update loop
    // should reach it within milliseconds. A failure here means the
    // batch is stuck in pending.
    let pin = repo.acquire_snapshot_pin(thread_id, block1, std::time::Duration::from_secs(2));
    assert!(pin.is_some(), "Fix A regression: acquire timed out — alignment did not fire");
    pin_request_guard.disarm();
    drop(pin);

    Ok(())
}

/// Fix B: producer-side alignment is missed because `request_snapshot_pin`
/// is called *after* the push; the anchor's transition is sitting in
/// pending. The late-flush hook in `request_snapshot_pin` must detect
/// this and ship the batch on the spot, so `acquire_snapshot_pin` still
/// succeeds promptly.
#[test]
fn test_pin_acquires_promptly_when_request_lands_after_push() -> anyhow::Result<()> {
    use std::collections::HashMap;

    use crate::AnchorBlockRef;
    let (repo, _dir) = setup_repo(true);
    let thread_id = ThreadIdentifier::default();
    let mut state = new_state();

    // Initial push so the thread is initialized.
    let (routing0, acc0) = new_acc(300);
    let block0 = BlockIdentifier::new(new_u256("block", 0));
    let mut builder = repo.state_builder(&thread_id, 0, &state);
    builder.insert_account(&routing0, &acc0);
    let transition0 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block0,
        &thread_id,
        0,
        &transition0.new_state,
        transition0.account_operations,
    )?;
    assert_drained(&repo);
    state = transition0.new_state;

    // Fix B path: push the anchor first, then request the pin.
    let block1 = BlockIdentifier::new(new_u256("block", 1));
    let (routing1, acc1) = new_acc(400);
    let mut builder = repo.state_builder(&thread_id, 0, &state);
    builder.insert_account(&routing1, &acc1);
    let transition1 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block1,
        &thread_id,
        0,
        &transition1.new_state,
        transition1.account_operations,
    )?;
    // The anchor's transition is now in pending. Without Fix B it
    // would sit there until the next push or until pending fills.
    let anchor = AnchorBlockRef {
        anchor_block_id: block1,
        anchor_thread_id: thread_id,
        cross_thread_refs: HashMap::new(),
    };
    let mut pin_request_guard = repo.request_snapshot_pin(anchor);

    // The late-flush hook should have shipped the batch synchronously.
    let pin = repo.acquire_snapshot_pin(thread_id, block1, std::time::Duration::from_secs(2));
    assert!(
        pin.is_some(),
        "Fix B regression: acquire timed out — late-flush did not ship the pending batch"
    );
    pin_request_guard.disarm();
    drop(pin);

    Ok(())
}

/// Stale network: after a block has been finalized and its batch fully
/// applied to the archive, no further `push_transition` calls are coming
/// in. A late `request_snapshot_pin` for that already-applied anchor must
/// still succeed promptly — there's no batch left to ship and no apply
/// for the update loop to drive a boundary signal off of, so the
/// shortcut in `request_snapshot_pin` has to detect this and transition
/// `Requested → BoundaryReached` directly.
#[test]
fn test_pin_acquires_when_anchor_already_applied_and_stream_is_stale() -> anyhow::Result<()> {
    use std::collections::HashMap;

    use crate::AnchorBlockRef;
    let (repo, _dir) = setup_repo(true);
    let thread_id = ThreadIdentifier::default();
    let mut state = new_state();

    // Finalize one block; let its batch apply to the archive.
    let (routing0, acc0) = new_acc(100);
    let block0 = BlockIdentifier::new(new_u256("block", 0));
    let mut builder = repo.state_builder(&thread_id, 0, &state);
    builder.insert_account(&routing0, &acc0);
    let transition0 = builder.build(None)?;
    repo.finalize_thread_transition(
        &block0,
        &thread_id,
        0,
        &transition0.new_state,
        transition0.account_operations,
    )?;
    assert_drained(&repo);
    state = transition0.new_state;
    let _ = state;

    // Stream is now stale — no more `finalize_thread_transition` calls.
    // Request a pin for the just-applied anchor. Without the shortcut,
    // the alignment hook never fires (no push), late-flush finds nothing
    // (pending is empty after drain), and the worker times out.
    let anchor = AnchorBlockRef {
        anchor_block_id: block0,
        anchor_thread_id: thread_id,
        cross_thread_refs: HashMap::new(),
    };
    let mut pin_request_guard = repo.request_snapshot_pin(anchor);

    // Tight timeout: the shortcut should have flipped the state to
    // BoundaryReached synchronously inside `request_snapshot_pin`.
    let pin = repo.acquire_snapshot_pin(thread_id, block0, std::time::Duration::from_millis(500));
    assert!(pin.is_some(), "stale-network regression: archive at anchor but pin not acquirable");
    pin_request_guard.disarm();
    drop(pin);

    Ok(())
}
