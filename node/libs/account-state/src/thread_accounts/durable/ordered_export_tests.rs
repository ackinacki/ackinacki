#[test]
fn partition_batch_get_does_not_use_unfinalized_versions() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo();
    let durable = repo.durable_map_repo();
    let (routing, account) = new_acc(1);
    let hash = account.account_hash();
    durable.accumulator.unfinalized_pool_insert(
        ThreadIdentifier::default(),
        1,
        &HashMap::from([(routing, ArchiveOperation::UpdateOrInsert(account.clone()))]),
    );
    assert!(durable.accumulator.unfinalized_pool_get(&hash).is_some());
    let map = ThreadAccountMap::new().update(&[(
        routing,
        Some((crate::AccountInfo::AccountHash(hash), crate::AccountCapacity::new(account.canonical_size()))),
    )]);
    let result = durable.read_partition_accounts_checked(
        &map,
        Default::default(),
        &|| false,
        |_, _| panic!("pool must not hide a missing pinned archive body"),
    );
    assert!(result.unwrap_err().to_string().contains("partition account missing"));
    Ok(())
}

#[test]
fn partition_batch_get_reads_only_selected_accounts_and_preserves_order() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo();
    let durable = repo.durable_map_repo();
    let accounts: Vec<_> = (0..24).map(|i| new_acc_with_dapp(i, i / 8)).collect();
    durable.archive.reset()?;
    durable.archive.thread_init_from_entries(
        &ThreadIdentifier::default(),
        BlockIdentifier::default(),
        accounts.iter().cloned().map(Ok),
    )?;
    let entries: Vec<_> = accounts
        .iter()
        .map(|(routing, account)| {
            (
                *routing,
                Some((
                    crate::AccountInfo::AccountHash(account.account_hash()),
                    crate::AccountCapacity::new(account.canonical_size()),
                )),
            )
        })
        .collect();
    let map = ThreadAccountMap::new().update(&entries);
    let parts = map.partition_by_capacity(map.capacity().bytes() / 4).unwrap();
    assert!(parts.len() > 1);
    for part in parts {
        let expected: Vec<_> = part.map.iter().map(|(routing, _)| routing).collect();
        let mut output = Vec::new();
        durable.read_partition_accounts_checked(
            &part.map,
            crate::PartitionReadLimits {
                accounts_per_batch: 2,
                in_flight_batches: 2,
                ..Default::default()
            },
            &|| false,
            |routing, account| {
                let source = accounts.iter().find(|(key, _)| *key == routing).unwrap();
                assert_eq!(account.canonical_bytes(), source.1.canonical_bytes());
                output.push(routing);
                Ok(())
            },
        )?;
        assert_eq!(output, expected);
    }
    Ok(())
}

#[test]
fn partition_batch_get_keeps_the_epoch_pinned_across_windows() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo();
    let durable = repo.durable_map_repo();
    let accounts: Vec<_> = (0..5).map(new_acc).collect();
    durable.archive.reset()?;
    durable.archive.thread_init_from_entries(
        &ThreadIdentifier::default(),
        BlockIdentifier::default(),
        accounts.iter().cloned().map(Ok),
    )?;
    let entries: Vec<_> = accounts
        .iter()
        .map(|(routing, account)| {
            (
                *routing,
                Some((
                    crate::AccountInfo::AccountHash(account.account_hash()),
                    crate::AccountCapacity::new(account.canonical_size()),
                )),
            )
        })
        .collect();
    let map = ThreadAccountMap::new().update(&entries);
    let mut count = 0;
    durable.read_partition_accounts_checked(
        &map,
        crate::PartitionReadLimits {
            accounts_per_batch: 1,
            in_flight_batches: 1,
            ..Default::default()
        },
        &|| false,
        |_, _| {
            if count == 0 {
                durable.archive.reset()?;
            }
            count += 1;
            Ok(())
        },
    )?;
    assert_eq!(count, 5);
    Ok(())
}

#[test]
fn partition_batch_get_rejects_missing_wrong_hash_and_wrong_size() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo();
    let durable = repo.durable_map_repo();
    let (routing, account) = new_acc(1);
    let (_, other) = new_acc(2);
    let size = account.canonical_size();
    let map = ThreadAccountMap::new().update(&[(
        routing,
        Some((
            crate::AccountInfo::AccountHash(account.account_hash()),
            crate::AccountCapacity::new(size),
        )),
    )]);
    assert!(durable
        .read_partition_accounts_checked(&map, Default::default(), &|| false, |_, _| panic!(
            "missing account must not be emitted"
        ))
        .is_err());
    durable.archive.reset()?;
    durable.archive.thread_init_from_entries(
        &ThreadIdentifier::default(),
        BlockIdentifier::default(),
        [Ok((routing, account.clone()))],
    )?;
    for (hash, capacity) in [(other.account_hash(), size), (account.account_hash(), size + 1)] {
        let invalid = ThreadAccountMap::new().update(&[(
            routing,
            Some((crate::AccountInfo::AccountHash(hash), crate::AccountCapacity::new(capacity))),
        )]);
        assert!(durable
            .read_partition_accounts_checked(
                &invalid,
                Default::default(),
                &|| false,
                |_, _| panic!("invalid account must not be emitted")
            )
            .is_err());
    }
    Ok(())
}

#[test]
fn partition_batch_get_restores_redirects_without_a_stored_body() -> anyhow::Result<()> {
    let (repo, _dir) = setup_repo();
    let (routing, _) = new_acc(1);
    let dapp = DAppIdentifier::new([17; 32]);
    let redirect = ThreadAccount::redirect(dapp);
    let map = ThreadAccountMap::new().update(&[(
        routing,
        Some((
            crate::AccountInfo::Redirect(dapp),
            crate::AccountCapacity::new(redirect.canonical_size()),
        )),
    )]);
    let mut emitted = 0;
    repo.durable_map_repo().read_partition_accounts_checked(
        &map,
        Default::default(),
        &|| false,
        |key, account| {
            assert_eq!(key, routing);
            assert_eq!(account.canonical_bytes(), redirect.canonical_bytes());
            emitted += 1;
            Ok(())
        },
    )?;
    assert_eq!(emitted, 1);
    Ok(())
}
