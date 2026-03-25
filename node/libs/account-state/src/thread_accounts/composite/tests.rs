use std::time::Instant;

use node_types::AccountIdentifier;
use node_types::AccountRouting;
use node_types::BlockIdentifier;
use node_types::DAppIdentifier;
use node_types::TransactionHash;
use tvm_block::MsgAddressInt;
use tvm_block::ShardAccount;
use tvm_types::AccountId;

use crate::account::avm::AvmAccount;
use crate::account::avm::AvmAccountData;
use crate::account::avm::AvmAccountMetadata;
use crate::account::avm::AvmStateAccount;
use crate::DurableThreadAccountsDiff;
use crate::FsCompositeThreadAccounts;
use crate::ThreadAccountUpdate;
use crate::ThreadAccounts;
use crate::ThreadAccountsBuilder;
use crate::ThreadAccountsRepository;
use crate::ThreadStateAccount;

fn target_tmp() -> std::path::PathBuf {
    let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());

    let path = std::path::PathBuf::from(target).join("test-accounts");

    std::fs::create_dir_all(&path).unwrap();

    path
}

fn new_u256(name: &str, seed: usize) -> [u8; 32] {
    let hasher = blake3::Hasher::new();
    let mut hasher = hasher;
    hasher.update(name.as_bytes());
    hasher.update(&(seed as u64).to_be_bytes());
    hasher.finalize().into()
}

fn new_acc(seed: usize) -> (AccountRouting, ThreadStateAccount) {
    let id = AccountIdentifier::new(new_u256("acc", seed));
    let routing = id.dapp_originator();
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
    let acc = ThreadStateAccount::from(tvm_shard_acc);
    (id.dapp_originator(), acc)
}

type Accounts = FsCompositeThreadAccounts;
type Repo = <Accounts as ThreadAccounts>::Repository;

#[test]
#[ignore]
fn test_fs_accounts_repo() -> anyhow::Result<()> {
    let root_path = target_tmp();
    std::fs::remove_dir_all(&root_path).ok();
    let start = Instant::now();
    let repo = Accounts::new_repository(root_path).build()?;
    let mut seed = 1;
    let mut prev_state = Repo::new_state();
    for block_index in 0..10000 {
        let block_id = BlockIdentifier::new(new_u256("block", block_index));
        let mut builder = repo.state_builder(&prev_state);
        let mut accounts = Vec::new();
        for _ in 0..1000 {
            let (routing, acc) = new_acc(seed);
            seed += 1;
            builder.insert_account(&routing, &acc);
            accounts.push((routing, acc));
        }
        let new_state = builder.build(None)?.new_state;
        repo.set_state(&block_id, &new_state)?;
        for (routing, acc) in accounts {
            let acc1 = repo.state_account(&new_state, &routing)?.unwrap();
            assert_eq!(acc.get_dapp_id(), acc1.get_dapp_id());
            assert_eq!(acc.account()?.hash(), acc1.account()?.hash());
        }
        prev_state = new_state;
        if block_index % 200 == 0 {
            repo.commit()?;
        }
        if block_index % 10 == 0 {
            println!("Block {} done", block_index);
        }
    }
    repo.commit()?;
    println!("Repo filled in {:?}", start.elapsed());
    test_fs_accounts_repo_compact()?;
    Ok(())
}

#[test]
#[ignore]
fn test_fs_accounts_repo_compact() -> anyhow::Result<()> {
    let root_path = target_tmp();
    let start = Instant::now();
    let _repo = Accounts::new_repository(root_path).build()?;
    println!("Repo loaded in {:?}", start.elapsed());
    Ok(())
}

// ==================== Test Helpers ====================

/// Creates a TVM account with a substantial data cell large enough to trigger
/// merkle update optimization (>= 256 bytes serialized).
fn new_large_acc(seed: usize, data_size: usize) -> (AccountRouting, ThreadStateAccount) {
    use tvm_block::*;
    use tvm_types::*;

    let id = AccountIdentifier::new(new_u256("large_acc", seed));
    let routing = id.dapp_originator();
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

    let state_acc = ThreadStateAccount::from(shard_acc);
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
fn modify_acc_balance(acc: &ThreadStateAccount, new_balance: u64) -> ThreadStateAccount {
    use crate::ThreadAccount;

    let thread_account = acc.account().unwrap();
    let mut tvm_account = tvm_block::Account::try_from(&thread_account).unwrap();
    tvm_account.set_balance(tvm_block::CurrencyCollection::with_grams(new_balance));

    let new_thread_account: ThreadAccount = tvm_account.try_into().unwrap();
    ThreadStateAccount::new(
        new_thread_account,
        acc.last_trans_hash(),
        acc.last_trans_lt() + 1,
        acc.get_dapp_id(),
    )
    .unwrap()
}

/// Creates an AVM account for testing the AVM-skip guard.
fn new_avm_acc(seed: usize) -> (AccountRouting, ThreadStateAccount) {
    use node_types::AccountHash;

    let id = AccountIdentifier::new(new_u256("avm_acc", seed));
    let routing = id.dapp_originator();
    let avm_account = AvmAccount::new(
        AccountHash::new(new_u256("avm_hash", seed)),
        AvmAccountMetadata { id, storage_used_bytes: 1024, ..Default::default() },
        None,
        None,
        Some(AvmAccountData { data: vec![0u8; 512] }),
    );
    let state_account = ThreadStateAccount::Avm(AvmStateAccount {
        account: avm_account,
        last_trans_hash: TransactionHash::new(new_u256("avm_trans", seed)),
        last_trans_lt: seed as u64,
        dapp_id: Some(*routing.dapp_id()),
    });
    (routing, state_account)
}

/// Creates a fresh filesystem-backed repository in a temp directory.
/// Returns the repo and the TempDir (which keeps the directory alive).
fn setup_repo(apply_to_durable: bool) -> (Repo, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let repo =
        Accounts::new_repository_with_apply_to_durable(dir.path().to_path_buf(), apply_to_durable)
            .build()
            .unwrap();
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
    let result_acc = ThreadStateAccount::read_bytes(&result_bytes).unwrap();

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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert a large account
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Modify the same account (balance change only)
    let modified_acc = modify_acc_balance(&large_acc, 999_999);
    let mut builder2 = repo.state_builder(&state1);
    builder2.insert_account(&routing, &modified_acc);
    let transition2 = builder2.build(None)?;

    // The diff should contain AccountMerkleUpdate for this routing
    let durable_accounts = &transition2.diff.durable.accounts;
    let entry = durable_accounts.iter().find(|(r, _)| r == &routing);
    assert!(entry.is_some(), "Diff should contain the modified account");
    let (_, update) = entry.unwrap();
    assert!(
        matches!(update, ThreadAccountUpdate::AccountMerkleUpdate(_)),
        "Large account with small change should be optimized to AccountMerkleUpdate, got {:?}",
        std::mem::discriminant(update)
    );

    if let ThreadAccountUpdate::AccountMerkleUpdate(bytes) = update {
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
fn test_builder_keeps_small_account_as_update_or_insert() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 100));

    // Insert a small account (below 256-byte threshold)
    let (routing, small_acc) = new_acc(1);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &small_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Modify the small account
    let modified = modify_acc_balance(&small_acc, 42);
    let mut builder2 = repo.state_builder(&state1);
    builder2.insert_account(&routing, &modified);
    let transition2 = builder2.build(None)?;

    // Should remain as UpdateOrInsert (too small for merkle optimization)
    let entry = transition2.diff.durable.accounts.iter().find(|(r, _)| r == &routing);
    assert!(entry.is_some(), "Diff should contain the modified account");
    let (_, update) = entry.unwrap();
    assert!(
        matches!(update, ThreadAccountUpdate::UpdateOrInsert(_)),
        "Small account should remain as UpdateOrInsert"
    );

    Ok(())
}

#[test]
fn test_builder_keeps_new_account_as_update_or_insert() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();

    // Insert a large account into an empty state (no old state to diff against)
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &large_acc);
    let transition = builder.build(None)?;

    // Should be UpdateOrInsert since there's no old account to create a delta from
    let entry = transition.diff.durable.accounts.iter().find(|(r, _)| r == &routing);
    assert!(entry.is_some(), "Diff should contain the new account");
    let (_, update) = entry.unwrap();
    assert!(
        matches!(update, ThreadAccountUpdate::UpdateOrInsert(_)),
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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 200));

    let (routing, avm_acc) = new_avm_acc(1);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &avm_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Modify the AVM account
    let (_, modified_avm) = new_avm_acc(2);
    // Use the same routing but different account data
    let mut builder2 = repo.state_builder(&state1);
    builder2.insert_account(&routing, &modified_avm);
    let transition2 = builder2.build(None)?;

    let entry = transition2.diff.durable.accounts.iter().find(|(r, _)| r == &routing);
    assert!(entry.is_some(), "Diff should contain the AVM account");
    let (_, update) = entry.unwrap();
    assert!(
        matches!(update, ThreadAccountUpdate::UpdateOrInsert(_)),
        "AVM accounts should never be converted to AccountMerkleUpdate"
    );

    Ok(())
}

#[test]
fn test_builder_preserves_remove_and_move_from_tvm() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 300));

    // Insert two large accounts
    let (routing1, large_acc1) = new_large_acc(1, 1024);
    let (routing2, large_acc2) = new_large_acc(2, 1024);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing1, &large_acc1);
    builder.insert_account(&routing2, &large_acc2);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Remove one, modify the other
    let modified2 = modify_acc_balance(&large_acc2, 42);
    let mut builder2 = repo.state_builder(&state1);
    builder2.remove_account(&routing1);
    builder2.insert_account(&routing2, &modified2);
    let transition2 = builder2.build(None)?;

    // routing1 should be Remove
    let entry1 = transition2.diff.durable.accounts.iter().find(|(r, _)| r == &routing1);
    assert!(entry1.is_some(), "Diff should contain the removed account");
    assert!(
        matches!(entry1.unwrap().1, ThreadAccountUpdate::Remove),
        "Removed account should stay as Remove"
    );

    // // routing2 should be AccountMerkleUpdate (large account with small change)
    // let entry2 = transition2.diff.durable.accounts.iter().find(|(r, _)| r == &routing2);
    // assert!(entry2.is_some(), "Diff should contain the modified account");
    // assert!(
    //     matches!(entry2.unwrap().1, ThreadAccountUpdate::AccountMerkleUpdate(_)),
    //     "Large modified account should be AccountMerkleUpdate"
    // );

    Ok(())
}

#[test]
#[ignore]
fn test_apply_merkle_update_diff_produces_correct_state() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 400));

    // Insert large account
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Modify account → build produces optimized diff with AccountMerkleUpdate
    let modified = modify_acc_balance(&large_acc, 777_777);
    let mut builder2 = repo.state_builder(&state1);
    builder2.insert_account(&routing, &modified);
    let transition2 = builder2.build(None)?;

    // Verify the diff contains AccountMerkleUpdate
    let has_merkle_update = transition2
        .diff
        .durable
        .accounts
        .iter()
        .any(|(_, u)| matches!(u, ThreadAccountUpdate::AccountMerkleUpdate(_)));
    assert!(has_merkle_update, "Diff should contain at least one AccountMerkleUpdate");

    // Simulate receiver: apply the diff to the old state
    let state_applied = repo.state_apply_diff(&state1, transition2.diff)?;

    // Verify the applied state has the correct account
    let acc_from_new_state = repo.state_account(&transition2.new_state, &routing)?.unwrap();
    let acc_from_applied = repo.state_account(&state_applied, &routing)?.unwrap();

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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 500));

    // Insert 2 large + 2 small accounts
    let (routing_l1, large1) = new_large_acc(1, 1024);
    let (routing_l2, large2) = new_large_acc(2, 1024);
    let (routing_s1, small1) = new_acc(101);
    let (routing_s2, small2) = new_acc(102);

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing_l1, &large1);
    builder.insert_account(&routing_l2, &large2);
    builder.insert_account(&routing_s1, &small1);
    builder.insert_account(&routing_s2, &small2);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Modify all 4 accounts (balance change)
    let mod_l1 = modify_acc_balance(&large1, 100);
    let mod_l2 = modify_acc_balance(&large2, 200);
    let mod_s1 = modify_acc_balance(&small1, 300);
    let mod_s2 = modify_acc_balance(&small2, 400);

    let mut builder2 = repo.state_builder(&state1);
    builder2.insert_account(&routing_l1, &mod_l1);
    builder2.insert_account(&routing_l2, &mod_l2);
    builder2.insert_account(&routing_s1, &mod_s1);
    builder2.insert_account(&routing_s2, &mod_s2);
    let transition2 = builder2.build(None)?;

    // Check optimization decisions
    for (routing, update) in &transition2.diff.durable.accounts {
        if routing == &routing_l1 || routing == &routing_l2 {
            assert!(
                matches!(update, ThreadAccountUpdate::AccountMerkleUpdate(_)),
                "Large account {:?} should be optimized to AccountMerkleUpdate",
                routing.account_id().to_hex_string()
            );
        } else if routing == &routing_s1 || routing == &routing_s2 {
            assert!(
                matches!(update, ThreadAccountUpdate::UpdateOrInsert(_)),
                "Small account {:?} should remain as UpdateOrInsert",
                routing.account_id().to_hex_string()
            );
        }
    }

    // Apply diff and verify all accounts are correct
    let state_applied = repo.state_apply_diff(&state1, transition2.diff)?;

    for (routing, expected) in [
        (&routing_l1, &mod_l1),
        (&routing_l2, &mod_l2),
        (&routing_s1, &mod_s1),
        (&routing_s2, &mod_s2),
    ] {
        let actual = repo.state_account(&state_applied, routing)?.unwrap();
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

    let diff = DurableThreadAccountsDiff {
        accounts: vec![
            (routing1, ThreadAccountUpdate::UpdateOrInsert(some_account)),
            (routing2, ThreadAccountUpdate::Remove),
            (routing3, ThreadAccountUpdate::MoveFromTvm),
            (routing4, ThreadAccountUpdate::AccountMerkleUpdate(vec![0xDE, 0xAD, 0xBE, 0xEF])),
        ],
    };

    let bytes = bincode::serialize(&diff).unwrap();
    let diff2: DurableThreadAccountsDiff = bincode::deserialize(&bytes).unwrap();

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

    // Wrap in ThreadAccountUpdate and roundtrip via bincode
    let update = ThreadAccountUpdate::AccountMerkleUpdate(mu_bytes.clone());
    let serialized = bincode::serialize(&update).unwrap();
    let deserialized: ThreadAccountUpdate = bincode::deserialize(&serialized).unwrap();

    // Verify bytes match
    if let ThreadAccountUpdate::AccountMerkleUpdate(deserialized_bytes) = &deserialized {
        assert_eq!(
            deserialized_bytes, &mu_bytes,
            "Deserialized merkle update bytes should match original"
        );

        // Verify the deserialized bytes can still be applied
        let mu_cell2 = read_single_root_boc(deserialized_bytes).unwrap();
        let mu2 = MerkleUpdate::construct_from_cell(mu_cell2).unwrap();
        let result_cell = mu2.apply_for(&old_cell).unwrap();
        let result_bytes = write_boc(&result_cell).unwrap();
        let result_acc = ThreadStateAccount::read_bytes(&result_bytes).unwrap();

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
    let shard_acc: tvm_block::ShardAccount = large_acc.clone().try_into().unwrap();
    accounts.insert(&routing.account_id().into(), &shard_acc).unwrap();

    // Create modified account and merkle update
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
        ThreadAccountUpdate::AccountMerkleUpdate(mu_bytes),
        &mut accounts,
    )
    .unwrap();

    // Read back and verify
    let result_shard_acc = accounts.account(&routing.account_id().into()).unwrap().unwrap();
    let result_acc = ThreadStateAccount::from(result_shard_acc);
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
        ThreadAccountUpdate::AccountMerkleUpdate(some_bytes),
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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 600));

    // Insert a large account into durable state
    let (routing, large_acc) = new_large_acc(1, 1024);
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &large_acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

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

    let diff = crate::CompositeThreadAccountsDiff {
        durable: DurableThreadAccountsDiff {
            accounts: vec![(routing, ThreadAccountUpdate::AccountMerkleUpdate(mu_bytes))],
        },
        tvm: crate::thread_accounts::tvm::TvmThreadStateDiff { update: tvm_mu },
    };

    let state_applied = repo.state_apply_diff(&state1, diff)?;

    // Verify the account was correctly updated
    let applied_acc = repo.state_account(&state_applied, &routing)?.unwrap();
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
    let state0 = Repo::new_state();

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

    // Apply to empty state — account doesn't exist in durable or TVM
    let tvm_cell = state0.tvm.shard_state.serialize().unwrap();
    let tvm_mu = MerkleUpdate::create(&tvm_cell, &tvm_cell).unwrap();

    let diff = crate::CompositeThreadAccountsDiff {
        durable: DurableThreadAccountsDiff {
            accounts: vec![(routing, ThreadAccountUpdate::AccountMerkleUpdate(mu_bytes))],
        },
        tvm: crate::thread_accounts::tvm::TvmThreadStateDiff { update: tvm_mu },
    };

    let result = repo.state_apply_diff(&state0, diff);
    assert!(result.is_err(), "Should error when applying merkle update for non-existent account");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("non-existent account"),
        "Error should mention non-existent account, got: {err_msg}"
    );

    Ok(())
}

// ==================== Durable State Snapshot Tests ====================

fn new_acc_with_dapp(seed: usize, dapp_seed: usize) -> (AccountRouting, ThreadStateAccount) {
    let id = AccountIdentifier::new(new_u256("acc", seed));
    let dapp_id = DAppIdentifier::new(new_u256("dapp", dapp_seed));
    let routing = id.routing_with(dapp_id);
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
    let acc = ThreadStateAccount::from(tvm_shard_acc);
    (routing, acc)
}

#[test]
fn test_durable_snapshot_empty_state() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state = Repo::new_state();

    let snapshot = repo.export_durable_snapshot(&state)?;

    assert!(snapshot.accounts.is_empty(), "Empty state should have no accounts");
    assert!(snapshot.dapp_accounts.is_empty(), "Empty state should have no dapp account maps");

    let (repo2, _dir2) = setup_repo(true);
    let new_durable = repo2.import_durable_snapshot(snapshot)?;

    // Build a state from the imported durable
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state.tvm.clone() };

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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert 10 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&state0);
    for i in 1..=10 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let state1 = builder.build(None)?.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Export
    let snapshot = repo.export_durable_snapshot(&state1)?;
    assert!(!snapshot.accounts.is_empty(), "Snapshot should contain account data");

    // Import into new repo
    let (repo2, _dir2) = setup_repo(true);
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));
    let new_durable = repo2.import_durable_snapshot(snapshot)?;
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state1.tvm.clone() };
    repo2.set_state(&block_id_2, &imported_state)?;

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
            imported_acc.account()?.hash(),
            orig_acc.account()?.hash(),
            "Account hash should match for {}",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_preserves_dapp_structure() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Create accounts in 3 different dapp groups
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&state0);
    for dapp in 0..3 {
        for acc in 0..5 {
            let seed = dapp * 100 + acc + 1;
            let (routing, account) = new_acc_with_dapp(seed, dapp + 1);
            builder.insert_account(&routing, &account);
            accounts.push((routing, account));
        }
    }
    let state1 = builder.build(None)?.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Export and import
    let snapshot = repo.export_durable_snapshot(&state1)?;
    assert!(
        snapshot.dapp_accounts.len() >= 3,
        "Should have at least 3 dapp account maps, got {}",
        snapshot.dapp_accounts.len()
    );

    let (repo2, _dir2) = setup_repo(true);
    let new_durable = repo2.import_durable_snapshot(snapshot)?;
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state1.tvm.clone() };

    // Verify all accounts in correct dapp groups
    for (routing, orig_acc) in &accounts {
        let imported_acc = repo2.state_account(&imported_state, routing)?;
        assert!(
            imported_acc.is_some(),
            "Account {} in dapp {} should exist",
            routing.account_id().to_hex_string(),
            routing.dapp_id().to_hex_string()
        );
        assert_eq!(imported_acc.unwrap().account()?.hash(), orig_acc.account()?.hash(),);
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_bincode_roundtrip() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert 50 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&state0);
    for i in 1..=50 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let state1 = builder.build(None)?.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Export, serialize, deserialize
    let snapshot = repo.export_durable_snapshot(&state1)?;
    let bytes = bincode::serialize(&snapshot).unwrap();
    let deserialized: crate::CompositeDurableStateSnapshot = bincode::deserialize(&bytes).unwrap();

    // Import deserialized snapshot
    let (repo2, _dir2) = setup_repo(true);
    let new_durable = repo2.import_durable_snapshot(deserialized)?;
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state1.tvm.clone() };

    for (routing, orig_acc) in &accounts {
        let imported_acc = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported_acc.account()?.hash(),
            orig_acc.account()?.hash(),
            "Account {} should match after bincode roundtrip",
            routing.account_id().to_hex_string()
        );
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_after_updates() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert 20 accounts
    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&state0);
    for i in 1..=20 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let state1 = builder.build(None)?.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Update 5 accounts (seeds 1-5), remove 3 (seeds 6-8), add 5 new (seeds 21-25)
    let mut builder2 = repo.state_builder(&state1);

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

    let state2 = builder2.build(None)?.new_state;
    repo.set_state(&block_id_2, &state2)?;
    repo.commit()?;

    // Export and import
    let snapshot = repo.export_durable_snapshot(&state2)?;
    let (repo2, _dir2) = setup_repo(true);
    let new_durable = repo2.import_durable_snapshot(snapshot)?;
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state2.tvm.clone() };

    // Verify updated accounts
    for (routing, expected_acc) in &updated_accounts {
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported.account()?.hash(),
            expected_acc.account()?.hash(),
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
            imported.account()?.hash(),
            expected_acc.account()?.hash(),
            "New account should match"
        );
    }

    // Verify unchanged accounts (seeds 9-20)
    for (i, account) in accounts.iter().enumerate().take(20).skip(8) {
        let routing = &account.0;
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(
            imported.account()?.hash(),
            account.1.account()?.hash(),
            "Unchanged account {} should match",
            i + 1
        );
    }

    Ok(())
}

#[test]
fn test_durable_snapshot_cross_repo_import() -> anyhow::Result<()> {
    // Repo 1: has accounts 1-10
    let (repo1, _dir1) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    let mut accounts1 = Vec::new();
    let mut builder = repo1.state_builder(&state0);
    for i in 1..=10 {
        let (routing, acc) = new_acc(i);
        builder.insert_account(&routing, &acc);
        accounts1.push((routing, acc));
    }
    let state1 = builder.build(None)?.new_state;
    repo1.set_state(&block_id_1, &state1)?;
    repo1.commit()?;

    // Repo 2: has accounts 101-110
    let (repo2, _dir2) = setup_repo(true);
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));
    let block_id_3 = BlockIdentifier::new(new_u256("block", 3));

    let mut accounts2 = Vec::new();
    let mut builder2 = repo2.state_builder(&Repo::new_state());
    for i in 101..=110 {
        let (routing, acc) = new_acc(i);
        builder2.insert_account(&routing, &acc);
        accounts2.push((routing, acc));
    }
    let state2 = builder2.build(None)?.new_state;
    repo2.set_state(&block_id_2, &state2)?;
    repo2.commit()?;

    // Export from repo1, import into repo2
    let snapshot = repo1.export_durable_snapshot(&state1)?;
    let imported_durable = repo2.import_durable_snapshot(snapshot)?;
    let imported_state =
        crate::CompositeThreadAccountsRef { durable: imported_durable, tvm: state1.tvm.clone() };
    repo2.set_state(&block_id_3, &imported_state)?;

    // Verify imported accounts are accessible via block_id_3
    for (routing, orig_acc) in &accounts1 {
        let acc = repo2.state_account(&imported_state, routing)?;
        assert!(
            acc.is_some(),
            "Imported account {} should exist",
            routing.account_id().to_hex_string()
        );
        assert_eq!(acc.unwrap().account()?.hash(), orig_acc.account()?.hash());
    }

    // Verify repo2's own accounts are still intact via state2
    for (routing, orig_acc) in &accounts2 {
        let acc = repo2.state_account(&state2, routing)?;
        assert!(
            acc.is_some(),
            "Repo2's own account {} should still exist",
            routing.account_id().to_hex_string()
        );
        assert_eq!(acc.unwrap().account()?.hash(), orig_acc.account()?.hash());
    }

    Ok(())
}

#[test]
#[ignore]
fn test_durable_snapshot_with_large_accounts() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    let mut accounts = Vec::new();
    let mut builder = repo.state_builder(&state0);
    for i in 1..=100 {
        let (routing, acc) = new_large_acc(i, 4096);
        builder.insert_account(&routing, &acc);
        accounts.push((routing, acc));
    }
    let state1 = builder.build(None)?.new_state;
    repo.set_state(&block_id, &state1)?;
    repo.commit()?;

    let start = Instant::now();
    let snapshot = repo.export_durable_snapshot(&state1)?;
    println!("Exported {} accounts in {:?}", accounts.len(), start.elapsed());

    let bytes = bincode::serialize(&snapshot).unwrap();
    println!("Serialized durable snapshot: {} bytes", bytes.len());

    let deserialized: crate::CompositeDurableStateSnapshot = bincode::deserialize(&bytes).unwrap();

    let (repo2, _dir2) = setup_repo(true);
    let start = Instant::now();
    let new_durable = repo2.import_durable_snapshot(deserialized)?;
    println!("Imported in {:?}", start.elapsed());

    let imported_state =
        crate::CompositeThreadAccountsRef { durable: new_durable, tvm: state1.tvm.clone() };

    for (routing, orig_acc) in &accounts {
        let imported = repo2.state_account(&imported_state, routing)?.unwrap();
        assert_eq!(imported.account()?.hash(), orig_acc.account()?.hash());
    }

    Ok(())
}

// ==================== Durable Redirect Stub Tests ====================

#[test]
fn test_redirect_stub_created_for_dapp_account() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Insert account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(1, 10);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    assert_ne!(dapp_id, account_id.use_as_dapp_id(), "dapp_id should differ from account_id");

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.set_state(&block_id, &state1)?;
    repo.commit()?;

    // Real account should be at (dapp_id, account_id)
    let real = repo.state_account(&state1, &routing)?;
    assert!(real.is_some(), "Real account should exist at (dapp_id, account_id)");
    let real = real.unwrap();
    assert!(!real.is_redirect(), "Real account should not be a redirect");
    assert_eq!(real.account()?.hash(), acc.account()?.hash(), "Real account data should match");

    // Lookup at default routing (account_id, account_id) follows the redirect
    // and returns the real account
    let default_routing = account_id.dapp_originator();
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
        followed.account()?.hash(),
        acc.account()?.hash(),
        "Followed account data should match original"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_lookup_returns_redirect() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    let (routing, acc) = new_acc_with_dapp(2, 20);
    let account_id = *routing.account_id();

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.set_state(&block_id, &state1)?;
    repo.commit()?;

    // Lookup via default routing follows the redirect and returns the real account
    let default_routing = account_id.dapp_originator();
    let result = repo.state_account(&state1, &default_routing)?;
    assert!(result.is_some(), "Default routing lookup should find account via redirect");
    let followed = result.unwrap();
    assert!(
        !followed.is_redirect(),
        "Default routing should return the real account (via redirect)"
    );
    assert_eq!(
        followed.account()?.hash(),
        acc.account()?.hash(),
        "Account data via default routing should match original"
    );

    // Lookup via real routing returns the real account directly
    let result = repo.state_account(&state1, &routing)?;
    assert!(result.is_some(), "Real routing lookup should find the account");
    assert!(!result.unwrap().is_redirect(), "Real routing should return the actual account");

    Ok(())
}

#[test]
fn test_redirect_stub_removed_when_account_removed() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(3, 30);
    let account_id = *routing.account_id();
    let default_routing = account_id.dapp_originator();

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Verify redirect exists
    assert!(repo.state_account(&state1, &default_routing)?.is_some(), "Redirect should exist");

    // Remove the account at its real routing
    let mut builder2 = repo.state_builder(&state1);
    builder2.remove_account(&routing);
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.set_state(&block_id_2, &state2)?;
    repo.commit()?;

    // Both real account and redirect should be gone
    assert!(repo.state_account(&state2, &routing)?.is_none(), "Real account should be removed");
    assert!(
        repo.state_account(&state2, &default_routing)?.is_none(),
        "Redirect stub should also be removed"
    );

    Ok(())
}

#[test]
fn test_redirect_stub_for_move_from_tvm() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Step 1: Insert account with dapp_id into TVM (apply_to_durable = false)
    let (routing, acc) = new_acc_with_dapp(4, 40);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    let default_routing = account_id.dapp_originator();

    let mut builder = repo.state_builder(&state0);
    builder.set_apply_to_durable(false);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Account should be in TVM, not durable
    assert!(
        repo.state_account(&state1, &default_routing)?.is_some(),
        "Account should be findable in TVM via default routing"
    );

    // Step 2: Move from TVM to durable
    let mut builder2 = repo.state_builder(&state1);
    builder2.set_apply_to_durable(true);
    builder2.move_from_tvm(1000)?;
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.set_state(&block_id_2, &state2)?;
    repo.commit()?;

    // Real account should be at (dapp_id, account_id) in durable
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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(5, 50);
    let account_id = *routing.account_id();
    let dapp_id = *routing.dapp_id();
    let default_routing = account_id.dapp_originator();

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Replace the real account with a redirect at (dapp_id, account_id)
    let mut builder2 = repo.state_builder(&state1);
    builder2.replace_with_redirect(&routing)?;
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.set_state(&block_id_2, &state2)?;
    repo.commit()?;

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
    let state0 = Repo::new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Insert account with dapp_id == account_id (default routing)
    let (routing, acc) = new_acc(1);
    assert_eq!(
        *routing.dapp_id(),
        routing.account_id().use_as_dapp_id(),
        "This test requires dapp_id == account_id"
    );

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.set_state(&block_id, &state1)?;
    repo.commit()?;

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
    let state0 = Repo::new_state();

    // Insert account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(6, 60);
    let account_id = *routing.account_id();
    let default_routing = account_id.dapp_originator();

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;

    // The diff should contain 2 entries: real account + redirect stub
    let durable_accounts = &transition.diff.durable.accounts;
    assert_eq!(durable_accounts.len(), 2, "Diff should contain real account and redirect stub");

    // Find entries by routing
    let real_entry = durable_accounts.iter().find(|(r, _)| r == &routing);
    assert!(real_entry.is_some(), "Diff should contain entry at real routing");
    assert!(
        matches!(real_entry.unwrap().1, ThreadAccountUpdate::UpdateOrInsert(_)),
        "Real entry should be UpdateOrInsert"
    );

    let redirect_entry = durable_accounts.iter().find(|(r, _)| r == &default_routing);
    assert!(redirect_entry.is_some(), "Diff should contain redirect entry at default routing");
    if let ThreadAccountUpdate::UpdateOrInsert(redirect_acc) = &redirect_entry.unwrap().1 {
        assert!(redirect_acc.is_redirect(), "Redirect entry should be a redirect");
    } else {
        panic!("Redirect entry should be UpdateOrInsert");
    }

    Ok(())
}

#[test]
fn test_redirect_stub_updated_when_account_dapp_changes() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo(true);
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));
    let block_id_2 = BlockIdentifier::new(new_u256("block", 2));

    // Insert account with dapp_id_1
    let id = AccountIdentifier::new(new_u256("acc", 7));
    let dapp_id_1 = DAppIdentifier::new(new_u256("dapp", 71));
    let routing1 = id.routing_with(dapp_id_1);
    let default_routing = id.dapp_originator();

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
    let acc1 = ThreadStateAccount::from(shard_acc_1);

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing1, &acc1);
    let transition1 = builder.build(None)?;
    let state1 = transition1.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Verify default routing follows redirect → returns real account with dapp_id_1
    let followed1 = repo.state_account(&state1, &default_routing)?.unwrap();
    assert!(!followed1.is_redirect(), "Should follow redirect to real account");
    assert_eq!(followed1.get_dapp_id(), Some(dapp_id_1));

    // Now change account's dapp to dapp_id_2
    let dapp_id_2 = DAppIdentifier::new(new_u256("dapp", 72));
    let routing2 = id.routing_with(dapp_id_2);

    let shard_acc_2 = ShardAccount::with_params(
        &tvm_acc,
        new_u256("trans", 8).into(),
        8,
        Some(dapp_id_2.as_array().into()),
    )
    .unwrap();
    let acc2 = ThreadStateAccount::from(shard_acc_2);

    let mut builder2 = repo.state_builder(&state1);
    // Remove from old routing, insert at new routing
    builder2.remove_account(&routing1);
    builder2.insert_account(&routing2, &acc2);
    let transition2 = builder2.build(None)?;
    let state2 = transition2.new_state;
    repo.set_state(&block_id_2, &state2)?;
    repo.commit()?;

    // Old routing should be removed
    assert!(repo.state_account(&state2, &routing1)?.is_none(), "Old routing should be empty");

    // New routing should have the account
    let real = repo.state_account(&state2, &routing2)?.unwrap();
    assert!(!real.is_redirect(), "New routing should have real account");

    // Default routing follows redirect → returns real account with dapp_id_2
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
    let state0 = Repo::new_state();
    let block_id = BlockIdentifier::new(new_u256("block", 1));

    // Create account with dapp_id != account_id but insert at default routing
    // (simulates build_actions.rs using acc_id.dapp_originator())
    let id = AccountIdentifier::new(new_u256("acc", 8));
    let dapp_id = DAppIdentifier::new(new_u256("dapp", 80));
    let default_routing = id.dapp_originator();
    let correct_routing = id.routing_with(dapp_id);

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
    let acc = ThreadStateAccount::from(shard_acc);
    assert_eq!(acc.get_dapp_id(), Some(dapp_id), "Account should have dapp_id set");

    // Insert at default routing (wrong — dapp_id != account_id)
    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&default_routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.set_state(&block_id, &state1)?;
    repo.commit()?;

    // Account should be rerouted to correct routing (dapp_id, account_id)
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
    let state0 = Repo::new_state();
    let block_id_1 = BlockIdentifier::new(new_u256("block", 1));

    // Insert account with dapp_id != account_id
    let (routing, acc) = new_acc_with_dapp(9, 90);
    let account_id = *routing.account_id();
    let default_routing = account_id.dapp_originator();

    let mut builder = repo.state_builder(&state0);
    builder.insert_account(&routing, &acc);
    let transition = builder.build(None)?;
    let state1 = transition.new_state;
    repo.set_state(&block_id_1, &state1)?;
    repo.commit()?;

    // Apply the diff to the old state (simulates a receiving node)
    let state_applied = repo.state_apply_diff(&state0, transition.diff)?;

    // Real account should be accessible at the real routing
    let real_from_applied = repo.state_account(&state_applied, &routing)?;
    assert!(real_from_applied.is_some(), "Real account should exist in applied state");
    assert_eq!(
        real_from_applied.unwrap().account()?.hash(),
        acc.account()?.hash(),
        "Applied state should have correct account data"
    );

    // Default routing follows redirect → returns the real account
    let followed_from_applied = repo.state_account(&state_applied, &default_routing)?;
    assert!(followed_from_applied.is_some(), "Default routing should find account via redirect");
    let followed_from_applied = followed_from_applied.unwrap();
    assert!(!followed_from_applied.is_redirect(), "Followed redirect should return real account");
    assert_eq!(
        followed_from_applied.account()?.hash(),
        acc.account()?.hash(),
        "Followed account data should match original"
    );

    Ok(())
}
