//! Legacy state wire compatibility, also used by immutable zerostates.
//! Snapshot account bodies are imported directly into
//! a staged archive epoch; local checkpoints never use this model.
use super::*;
use crate::multithreading::cross_thread_messaging::thread_references_state::ThreadReferencesState;
use crate::repository::optimistic_shard_state::OptimisticShardState;
use crate::types::thread_message_queue::legacy::Queue;
use crate::types::BlockInfo;
use crate::types::ThreadsTable;

pub(super) const MAGIC: &[u8; 7] = b"SNAP2V1";

#[derive(Serialize, Deserialize)]
pub(crate) struct State {
    block_seq_no: BlockSeqNo,
    block_id: BlockIdentifier,
    shard_state: OptimisticShardState,
    messages: Queue,
    high_priority_messages: Queue,
    block_info: BlockInfo,
    threads_table: ThreadsTable,
    thread_id: ThreadIdentifier,
    thread_refs_state: ThreadReferencesState,
    cropped: Option<(ThreadIdentifier, ThreadsTable)>,
    #[cfg(feature = "monitor-accounts-number")]
    accounts_number: u64,
}

pub(super) fn encode_state(state: OptimisticStateImpl) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&State::from(state))?)
}

impl From<OptimisticStateImpl> for State {
    fn from(state: OptimisticStateImpl) -> Self {
        Self {
            block_seq_no: state.block_seq_no,
            block_id: state.block_id,
            shard_state: state.shard_state,
            messages: Queue::from_current(&state.messages),
            high_priority_messages: Queue::from_current(&state.high_priority_messages),
            block_info: state.block_info,
            threads_table: state.threads_table,
            thread_id: state.thread_id,
            thread_refs_state: state.thread_refs_state,
            cropped: state.cropped,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number: state.accounts_number,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RefData {
    block_identifier: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    block_thread_identifier: ThreadIdentifier,
    outbound_messages: HashMap<
        AccountRouting,
        Vec<(crate::message::identifier::MessageIdentifier, Arc<WrappedMessage>)>,
    >,
    outbound_accounts: HashMap<
        AccountRouting,
        (
            Option<crate::types::account::WrappedAccount>,
            Option<crate::types::thread_message_queue::legacy::LegacyInbox>,
        ),
    >,
    threads_table: ThreadsTable,
    parent_block_identifier: BlockIdentifier,
    block_refs: Vec<BlockIdentifier>,
    is_tombstone: bool,
}

impl From<&CrossThreadRefData> for RefData {
    fn from(source: &CrossThreadRefData) -> Self {
        Self {
            block_identifier: *source.block_identifier(),
            block_seq_no: *source.block_seq_no(),
            block_thread_identifier: *source.block_thread_identifier(),
            outbound_messages: source.outbound_messages().clone(),
            outbound_accounts: source
                .outbound_accounts()
                .iter()
                .map(|(route, (account, inbox))| {
                    (*route, (account.clone(), inbox.as_ref().map(|inbox| inbox.messages.clone())))
                })
                .collect(),
            threads_table: source.threads_table().clone(),
            parent_block_identifier: *source.parent_block_identifier(),
            block_refs: source.block_refs().clone(),
            is_tombstone: *source.is_tombstone(),
        }
    }
}

impl RefData {
    fn into_current(
        self,
        groups: &[Vec<Arc<WrappedMessage>>],
    ) -> anyhow::Result<CrossThreadRefData> {
        let accounts = self
            .outbound_accounts
            .into_iter()
            .map(|(route, (account, inbox))| {
                let inbox = inbox
                    .map(|messages| {
                        let history = match messages.compacted_history() {
                            Some(range) => groups
                                .iter()
                                .find(|group| {
                                    group.first().map(|m| m.identifier())
                                        == Some(range.start().clone())
                                })
                                .ok_or_else(|| {
                                    anyhow::anyhow!("Missing legacy outbound inbox history")
                                })?
                                .as_slice(),
                            None => &[],
                        };
                        crate::types::thread_message_queue::legacy::inbox_from_legacy(
                            messages, history,
                        )
                    })
                    .transpose()?;
                Ok((route, (account, inbox)))
            })
            .collect::<anyhow::Result<HashMap<_, _>>>()?;
        Ok(CrossThreadRefData::builder()
            .block_identifier(self.block_identifier)
            .block_seq_no(self.block_seq_no)
            .block_thread_identifier(self.block_thread_identifier)
            .outbound_messages(self.outbound_messages)
            .outbound_accounts(accounts)
            .threads_table(self.threads_table)
            .parent_block_identifier(self.parent_block_identifier)
            .block_refs(self.block_refs)
            .is_tombstone(self.is_tombstone)
            .build())
    }
}

#[derive(Serialize, Deserialize)]
struct Ancestor {
    block_identifier: BlockIdentifier,
    block_seq_no: BlockSeqNo,
    thread_identifier: ThreadIdentifier,
    cross_thread_ref_data: RefData,
    bk_set: BlockKeeperSet,
    future_bk_set: BlockKeeperSet,
    envelope_hash: AckiNackiEnvelopeHash,
    parent_block_identifier: BlockIdentifier,
    block_version_state: BlockProtocolVersionState,
}

impl From<&AncestorBlockData> for Ancestor {
    fn from(source: &AncestorBlockData) -> Self {
        Self {
            block_identifier: source.block_identifier,
            block_seq_no: source.block_seq_no,
            thread_identifier: source.thread_identifier,
            cross_thread_ref_data: RefData::from(&source.cross_thread_ref_data),
            bk_set: source.bk_set.clone(),
            future_bk_set: source.future_bk_set.clone(),
            envelope_hash: source.envelope_hash.clone(),
            parent_block_identifier: source.parent_block_identifier,
            block_version_state: source.block_version_state.clone(),
        }
    }
}

// Structs and consecutive field writes have the same baseline bincode layout.
// Keep the common prefix fields listed once, in their original wire order.
macro_rules! prefix_fields {
    ($apply:ident) => {
        $apply!(
            db_messages,
            finalized_block,
            bk_set,
            descendant_bk_set,
            future_bk_set,
            descendant_future_bk_set,
            finalized_block_stats,
            attestation_target,
            producer_selector,
            block_height,
            prefinalization_proof,
            ancestor_blocks_finalization_checkpoints,
            finalizes_blocks,
            parent_ancestor_blocks_finalization_checkpoints,
            block_protocol_version_state,
            history_data_snapshot,
            history_cursor,
            durable_snapshot_header,
            finalization_chain
        )
    };
}

pub(super) fn encode_prefix(prefix: &StreamedThreadSnapshot) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bincode::serialize_into(&mut bytes, &prefix.optimistic_state)?;
    let ancestors: Vec<_> = prefix.ancestor_blocks_data.iter().map(Ancestor::from).collect();
    bincode::serialize_into(&mut bytes, &ancestors)?;
    macro_rules! write_fields { ($($field:ident),+) => { $(bincode::serialize_into(&mut bytes, &prefix.$field)?;)+ }; }
    prefix_fields!(write_fields);
    Ok(bytes)
}

pub(super) fn decode_prefix(bytes: &[u8]) -> anyhow::Result<StreamedThreadSnapshot> {
    use bincode::Options;
    // Envelope deserializers borrow byte slices; an io::Read deserializer
    // cannot supply those even when its underlying reader is a Cursor.
    let options = bincode::DefaultOptions::new().with_fixint_encoding();
    let mut reader = bincode::Deserializer::from_slice(bytes, options);
    let optimistic_state = Deserialize::deserialize(&mut reader)?;
    let ancestors: Vec<Ancestor> = Deserialize::deserialize(&mut reader)?;
    macro_rules! read_fields { ($($field:ident),+) => { StreamedThreadSnapshot {
        optimistic_state, ancestor_blocks_data: Vec::new(),
        $($field: Deserialize::deserialize(&mut reader)?,)+
    } }; }
    let mut prefix = prefix_fields!(read_fields);
    ensure!(u8::deserialize(&mut reader).is_err(), "Trailing legacy prefix bytes");
    prefix.ancestor_blocks_data = ancestors
        .into_iter()
        .map(|ancestor| {
            Ok(AncestorBlockData {
                block_identifier: ancestor.block_identifier,
                block_seq_no: ancestor.block_seq_no,
                thread_identifier: ancestor.thread_identifier,
                cross_thread_ref_data: ancestor
                    .cross_thread_ref_data
                    .into_current(&prefix.db_messages)?,
                bk_set: ancestor.bk_set,
                future_bk_set: ancestor.future_bk_set,
                envelope_hash: ancestor.envelope_hash,
                parent_block_identifier: ancestor.parent_block_identifier,
                block_version_state: ancestor.block_version_state,
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(prefix)
}

pub(super) fn convert_state(
    prefix: &mut StreamedThreadSnapshot,
) -> anyhow::Result<OptimisticStateImpl> {
    let source: State = bincode::deserialize(&prefix.optimistic_state)
        .context("Reading SNAP2V1 optimistic state")?;
    let mut groups = HashMap::new();
    for group in prefix.db_messages.iter().filter(|group| !group.is_empty()) {
        ensure!(
            groups.insert(group[0].identifier(), group.as_slice()).is_none(),
            "Duplicate legacy message range"
        );
    }
    let state = source.into_current(&mut groups)?;
    ensure!(groups.is_empty(), "Unreferenced legacy message ranges");
    ensure!(
        state.block_id == prefix.finalized_block.data().identifier(),
        "Legacy snapshot block ID mismatch"
    );
    ensure!(
        state.thread_id == *prefix.finalized_block.data().common_section().thread_id(),
        "Legacy snapshot thread ID mismatch"
    );
    validate_snapshot_inboxes(&state, &prefix.db_messages)?;
    prefix.optimistic_state = bincode::serialize(&state)?;
    Ok(state)
}

impl State {
    pub(crate) fn into_current(
        self,
        groups: &mut HashMap<MessageIdentifier, &[Arc<WrappedMessage>]>,
    ) -> anyhow::Result<OptimisticStateImpl> {
        let mut state = OptimisticStateImpl::zero();
        state.block_seq_no = self.block_seq_no;
        state.block_id = self.block_id;
        state.shard_state = self.shard_state;
        state.messages = self.messages.into_current(groups)?;
        state.high_priority_messages = self.high_priority_messages.into_current(groups)?;
        state.block_info = self.block_info;
        state.threads_table = self.threads_table;
        state.thread_id = self.thread_id;
        state.thread_refs_state = self.thread_refs_state;
        state.cropped = self.cropped;
        #[cfg(feature = "monitor-accounts-number")]
        {
            state.accounts_number = self.accounts_number;
        }
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::thread_message_queue::legacy::Dapp;

    #[test]
    fn snapshot_format_follows_block_protocol_not_running_binary() -> anyhow::Result<()> {
        use crate::config::GlobalConfig;
        use crate::versioning::ProtocolVersion;
        let old = ProtocolVersion::parse("test_1.0.7_0")?;
        let new = ProtocolVersion::parse("test_1.0.8_0")?;
        let old_config = GlobalConfig {
            engine_version: semver::Version::new(1, 0, 7),
            ..GlobalConfig::default()
        };
        let configs = ConfigRead::new(new.clone(), GlobalConfig::default())
            .with_retired(old.clone(), old_config);
        for version in [
            BlockProtocolVersionState::Current(old.clone()),
            BlockProtocolVersionState::TransitionLocked {
                from: old.clone(),
                to: new.clone(),
                milestone: BlockIdentifier::default(),
            },
            BlockProtocolVersionState::CompleteTransition { from: old, to: new.clone() },
        ] {
            assert_eq!(SnapshotVersion::for_block(&version, &configs)?, SnapshotVersion::Legacy);
        }
        assert_eq!(
            SnapshotVersion::for_block(&BlockProtocolVersionState::Current(new), &configs)?,
            SnapshotVersion::Current
        );
        assert!(SnapshotVersion::for_block(
            &BlockProtocolVersionState::Current(ProtocolVersion::parse("unknown")?),
            &configs
        )
        .is_err());
        Ok(())
    }

    #[test]
    fn legacy_prefix_converts_outbound_inboxes_and_queue_cursors() -> anyhow::Result<()> {
        let (mut prefix, legacy, _) = fixture()?;
        prefix.optimistic_state = bincode::serialize(&legacy)?;
        let state = convert_state(&mut prefix)?;
        let route = AccountRouting::default();
        let high = state.high_priority_messages.account_inbox_by_routing(&route).unwrap().clone();
        let reference = CrossThreadRefData::builder()
            .block_identifier(state.block_id)
            .block_seq_no(state.block_seq_no)
            .block_thread_identifier(state.thread_id)
            .outbound_messages(HashMap::new())
            .outbound_accounts(HashMap::from([(route, (None, Some(high.clone())))]))
            .threads_table(state.threads_table.clone())
            .parent_block_identifier(BlockIdentifier::default())
            .block_refs(Vec::new())
            .build();
        prefix.ancestor_blocks_data.push(AncestorBlockData {
            block_identifier: state.block_id,
            block_seq_no: state.block_seq_no,
            thread_identifier: state.thread_id,
            cross_thread_ref_data: reference,
            bk_set: BlockKeeperSet::new(),
            future_bk_set: BlockKeeperSet::new(),
            envelope_hash: AckiNackiEnvelopeHash([7; 32]),
            parent_block_identifier: BlockIdentifier::default(),
            block_version_state: BlockProtocolVersionState::none(),
        });
        let mut queues = state.messages.clone();
        // Keep a real key cursor through the legacy numeric representation.
        queues.advance_cursor(1);
        let cursor = queues.cursor();
        let source = Queue::from_current(&queues);
        let mut groups = prefix
            .db_messages
            .iter()
            .filter(|g| !g.is_empty())
            .map(|g| (g[0].identifier(), g.as_slice()))
            .collect();
        assert_eq!(source.into_current(&mut groups)?.cursor(), cursor);
        prefix.optimistic_state = encode_state(state)?;
        let encoded = encode_prefix(&prefix)?;
        let mut restored = decode_prefix(&encoded)?;
        convert_state(&mut restored)?;
        let actual = restored.ancestor_blocks_data[0]
            .cross_thread_ref_data
            .outbound_accounts()
            .get(&route)
            .unwrap()
            .1
            .as_ref()
            .unwrap();
        assert_eq!(actual.tail_sequence(), high.tail_sequence());
        assert_eq!(actual.commitment, high.commitment);
        assert_eq!(actual.canonical_size(), high.canonical_size());
        assert!(decode_prefix(&[encoded, vec![0]].concat()).is_err());
        Ok(())
    }

    fn legacy_queue(queue: &crate::types::thread_message_queue::ThreadMessageQueueState) -> Queue {
        let messages: BTreeMap<_, _> = queue
            .messages
            .iter()
            .map(|(id, dapp)| {
                let messages: BTreeMap<_, _> = dapp
                    .messages
                    .iter()
                    .map(|(route, inbox)| (route, inbox.messages.clone()))
                    .collect();
                (id, Dapp { order_set: messages.keys().copied().collect(), messages, cursor: 0 })
            })
            .collect();
        Queue { order_set: messages.keys().copied().collect(), messages, cursor: 0 }
    }

    fn fixture() -> anyhow::Result<(StreamedThreadSnapshot, State, Vec<u8>)> {
        let (mut state, bodies) = super::super::inbox_snapshot_tests::fixture();
        let mut snapshot = super::super::tests::make_test_snapshot();
        state.block_id = snapshot.finalized_block.data().identifier();
        snapshot.db_messages = bodies;
        let legacy = State {
            block_seq_no: state.block_seq_no,
            block_id: state.block_id,
            shard_state: state.shard_state,
            messages: legacy_queue(&state.messages),
            high_priority_messages: legacy_queue(&state.high_priority_messages),
            block_info: state.block_info,
            threads_table: state.threads_table,
            thread_id: state.thread_id,
            thread_refs_state: state.thread_refs_state,
            cropped: state.cropped,
            #[cfg(feature = "monitor-accounts-number")]
            accounts_number: state.accounts_number,
        };
        // Empty baseline durable map: u8 path length, 32-byte prefix, empty
        // node tag. The account-state tests cover nonempty maps and redirects.
        let mut durable = Vec::new();
        durable.extend(34u64.to_le_bytes());
        durable.extend([0; 34]);
        durable.extend(0u64.to_le_bytes());
        let prefix = StreamedThreadSnapshot::from_snapshot(&snapshot, durable.len() as u64);
        Ok((prefix, legacy, durable))
    }

    fn encode(
        mut prefix: StreamedThreadSnapshot,
        state: &State,
        durable: &[u8],
    ) -> anyhow::Result<Vec<u8>> {
        prefix.optimistic_state = bincode::serialize(state)?;
        let mut bytes = MAGIC.to_vec();
        bincode::serialize_into(
            &mut bytes,
            &StreamedThreadSnapshotHeader { prefix_len: bincode::serialized_size(&prefix)? },
        )?;
        bincode::serialize_into(&mut bytes, &prefix)?;
        bytes.extend(durable);
        Ok(bytes)
    }

    #[test]
    #[ignore = "requires main snapshot, prepared account subset and isolated Aerospike"]
    fn real_snapshot_subset_full_node_roundtrip() -> anyhow::Result<()> {
        ensure!(
            cfg!(feature = "disable_db_for_messages"),
            "Run with disable_db_for_messages (default features)"
        );
        let source_path = std::env::var("LOCAL_SNAPSHOT_PATH")
            .unwrap_or_else(|_| "/Volumes/x5/_/latest-main-snapshot".into());
        let subset_path = PathBuf::from(std::env::var("LOCAL_SNAPSHOT_SUBSET_OUTPUT")?);
        let limit: usize = std::env::var("LOCAL_SNAPSHOT_ACCOUNT_LIMIT")
            .unwrap_or_else(|_| "100000".into())
            .parse()?;
        let address = std::env::var("LOCAL_SNAPSHOT_AEROSPIKE_ADDRESS")?;
        let namespace = std::env::var("LOCAL_SNAPSHOT_AEROSPIKE_NAMESPACE")
            .unwrap_or_else(|_| "snapshot_import".into());
        let mut source_file = File::open(source_path)?;
        let mut compression = [0; 4];
        source_file.read_exact(&mut compression)?;
        ensure!(&compression == b"ZSTD", "Expected compressed source snapshot");
        let mut source = zstd::Decoder::new(source_file)?;
        let mut magic = [0; 7];
        source.read_exact(&mut magic)?;
        ensure!(&magic == MAGIC, "Expected SNAP2V1 source");
        let header: StreamedThreadSnapshotHeader = bincode::deserialize_from(&mut source)?;
        let mut prefix_bytes = vec![0; header.prefix_len as usize];
        source.read_exact(&mut prefix_bytes)?;
        let mut original = decode_prefix(&prefix_bytes)?;
        drop(source);
        // The source file can carry historical ranges separately. Materialize
        // them inline in this diagnostic fixture; never read or write message DB.
        let before = convert_state(&mut original.clone())?;
        let mut legacy: State = bincode::deserialize(&original.optimistic_state)?;
        let mut groups: HashMap<_, _> = original
            .db_messages
            .iter()
            .filter(|group| !group.is_empty())
            .map(|group| (group[0].identifier(), group))
            .collect();
        let history_bodies = original.db_messages.iter().map(Vec::len).sum::<usize>();
        for queue in [&mut legacy.messages, &mut legacy.high_priority_messages] {
            for dapp in queue.messages.values_mut() {
                for inbox in dapp.messages.values_mut() {
                    if let Some(range) = inbox.compacted_history() {
                        let history = groups.remove(range.start()).unwrap();
                        let mut tail: std::collections::VecDeque<_> = history
                            .iter()
                            .map(|message| (message.identifier(), message.clone()))
                            .collect();
                        tail.extend(inbox.tail_sequence().iter().cloned());
                        inbox.set_compacted_history(None);
                        inbox.set_tail_sequence(tail);
                    }
                }
            }
        }
        ensure!(groups.is_empty(), "Unreferenced source history");
        original.optimistic_state = bincode::serialize(&legacy)?;
        original.db_messages.clear();
        let after = convert_state(&mut original.clone())?;
        assert_eq!(before.messages.root_hash(), after.messages.root_hash());
        assert_eq!(
            before.high_priority_messages.root_hash(),
            after.high_priority_messages.root_hash()
        );
        eprintln!("INLINE_SOURCE history_bodies={history_bodies}");

        fn load_and_check(
            repo: &RepositoryImpl,
            expected: &OptimisticStateImpl,
            count: usize,
            groups: &[Vec<Arc<WrappedMessage>>],
        ) -> anyhow::Result<OptimisticStateImpl> {
            let state = repo
                .get_full_optimistic_state_no_cache(&expected.block_id, &expected.thread_id, None)?
                .ok_or_else(|| anyhow::anyhow!("Missing persisted state after snapshot import"))?;
            assert_eq!(bincode::serialize(&state)?, bincode::serialize(expected)?);
            assert_eq!(
                repo.thread_accounts_repository
                    .durable_map_iter(&state.shard_state.0.durable)
                    .count(),
                count
            );
            validate_snapshot_inboxes(&state, groups)?;
            assert_eq!(
                repo.select_thread_last_finalized_block(&expected.thread_id)?,
                Some((expected.block_id, expected.block_seq_no))
            );
            Ok(state.as_ref().clone())
        }

        let unique = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos();
        for synthetic_priorities in [false, true] {
            let started = Instant::now();
            let mut prefix = original.clone();
            if synthetic_priorities {
                // Keep the real primary metadata, but exercise nonempty normal
                // and high-priority queues even if main's snapshot has none.
                let mut queues = OptimisticStateImpl::zero();
                for (queue, lts) in
                    [(&mut queues.messages, 1..5), (&mut queues.high_priority_messages, 5..7)]
                {
                    let mut inbox = crate::types::AccountInbox::empty();
                    for lt in lts {
                        let header = tvm_block::InternalMessageHeader {
                            created_lt: lt,
                            ..Default::default()
                        };
                        let message = Arc::new(WrappedMessage::from_tvm(
                            &tvm_block::Message::with_int_header(header),
                        ));
                        inbox.commitment.add(&message.identifier());
                        inbox.add_messages(vec![(message.identifier(), message)])?;
                    }
                    queue.set_inbox(AccountRouting::default(), inbox);
                }
                let mut legacy: State = bincode::deserialize(&prefix.optimistic_state)?;
                legacy.messages = legacy_queue(&queues.messages);
                legacy.high_priority_messages = legacy_queue(&queues.high_priority_messages);
                prefix.optimistic_state = bincode::serialize(&legacy)?;
                prefix.db_messages.clear();
            }
            prefix.durable_snapshot_header.len = std::fs::metadata(&subset_path)?.len();
            let mut expected_prefix = prefix.clone();
            let expected = convert_state(&mut expected_prefix)?;
            let dir = tempfile::tempdir()?;
            let mut repositories = Vec::new();
            let mut repository_dirs = Vec::new();
            for index in 0..2 {
                let repo_dir = tempfile::tempdir_in(dir.path())?;
                let path = repo_dir.path().join("accounts");
                let accounts = account_state::ThreadAccountsRepository::builder(path)
                    .set_accounts_aerospike_store(
                        &address,
                        &namespace,
                        format!("full_{unique}_{}_{index}", usize::from(synthetic_priorities)),
                    )
                    .build()?;
                repositories.push(
                    super::super::tests::make_test_repository_with_accounts(&repo_dir, accounts)?.0,
                );
                repository_dirs.push(repo_dir);
            }
            let mut target = repositories.pop().unwrap();
            let mut repo = repositories.pop().unwrap();
            let mut legacy_header = MAGIC.to_vec();
            let prefix_bytes = encode_prefix(&prefix)?;
            bincode::serialize_into(
                &mut legacy_header,
                &StreamedThreadSnapshotHeader { prefix_len: prefix_bytes.len() as u64 },
            )?;
            legacy_header.extend(prefix_bytes);
            let mut legacy = Cursor::new(legacy_header).chain(File::open(&subset_path)?);
            let import_started = Instant::now();
            assert!(matches!(
                repo.set_state_from_snapshot_reader_checked(
                    &mut legacy,
                    &expected.thread_id,
                    Arc::new(Mutex::new(HashSet::new()))
                )?,
                SnapshotImportStatus::Imported
            ));
            let import_s = import_started.elapsed().as_secs_f64();
            let restored = load_and_check(&repo, &expected, limit, &prefix.db_messages)?;
            // Reexport primary and inline message bodies from the restored state.
            let export_started = Instant::now();
            let mut durable = Cursor::new(Vec::new());
            repo.thread_accounts_repository.export_snapshot_v1_to_writer_checked(
                &restored.shard_state.0,
                &mut durable,
                &|| false,
            )?;
            let mut exported_prefix = expected_prefix.clone();
            exported_prefix.optimistic_state = bincode::serialize(&restored)?;
            exported_prefix.db_messages.clear();
            exported_prefix.durable_snapshot_header.len = durable.get_ref().len() as u64;
            let snapshot = SnapshotFile {
                snapshot: exported_prefix.clone().into_snapshot(),
                format: SnapshotFormat::Streamed { durable_bytes: durable.into_inner() },
                compression: if synthetic_priorities {
                    SnapshotCompression::Plain
                } else {
                    SnapshotCompression::ZstdWithMagic
                },
            };
            let new_path = dir.path().join("new-snapshot");
            write_snapshot_file(&new_path, &snapshot)?;
            let decoded = read_snapshot_file(&new_path)?;
            // All primary fields and snapshot message groups survive the codec.
            let decoded_prefix = StreamedThreadSnapshot::from_snapshot(
                decoded.snapshot(),
                exported_prefix.durable_snapshot_header.len,
            );
            macro_rules! check_primary {
                ($($field:ident),+ $(,)?) => {$(
                    ensure!(bincode::serialize(&decoded_prefix.$field)? == bincode::serialize(&exported_prefix.$field)?,
                        "Primary field changed: {}", stringify!($field));
                )+};
            }
            check_primary!(
                optimistic_state,
                db_messages,
                finalized_block,
                finalized_block_stats,
                attestation_target,
                producer_selector,
                block_height,
                prefinalization_proof,
                finalizes_blocks,
                block_protocol_version_state,
                history_data_snapshot,
                history_cursor,
                durable_snapshot_header,
                finalization_chain
            );
            // HashMap serialization order is not part of primary semantics.
            macro_rules! check_equal {
                ($($field:ident),+ $(,)?) => {$(
                    ensure!(decoded_prefix.$field == exported_prefix.$field,
                        "Primary field changed: {}", stringify!($field));
                )+};
            }
            check_equal!(
                bk_set,
                descendant_bk_set,
                future_bk_set,
                descendant_future_bk_set,
                ancestor_blocks_finalization_checkpoints,
                parent_ancestor_blocks_finalization_checkpoints
            );
            ensure!(
                decoded_prefix.ancestor_blocks_data.len()
                    == exported_prefix.ancestor_blocks_data.len(),
                "Ancestor count changed"
            );
            for (actual, expected) in decoded_prefix
                .ancestor_blocks_data
                .iter()
                .zip(&exported_prefix.ancestor_blocks_data)
            {
                ensure!(
                    actual.bk_set == expected.bk_set
                        && actual.future_bk_set == expected.future_bk_set,
                    "Ancestor BK sets changed"
                );
                let a = &actual.cross_thread_ref_data;
                let b = &expected.cross_thread_ref_data;
                let a_messages: BTreeMap<_, _> = a.outbound_messages().iter().collect();
                let b_messages: BTreeMap<_, _> = b.outbound_messages().iter().collect();
                let a_accounts: BTreeMap<_, _> = a.outbound_accounts().iter().collect();
                let b_accounts: BTreeMap<_, _> = b.outbound_accounts().iter().collect();
                ensure!(
                    bincode::serialize(&a_messages)? == bincode::serialize(&b_messages)?
                        && bincode::serialize(&a_accounts)? == bincode::serialize(&b_accounts)?,
                    "Ancestor outbound content changed"
                );
                let mut a = actual.clone();
                let b = expected.clone();
                // Compare all remaining fields after replacing the independently
                // checked HashMaps with the same values (and iteration order).
                a.bk_set = b.bk_set.clone();
                a.future_bk_set = b.future_bk_set.clone();
                // Metadata is checked separately; the map payloads above are canonicalized.
                macro_rules! check_ref {
                    ($($field:ident),+ $(,)?) => {$(ensure!(bincode::serialize(a.cross_thread_ref_data.$field())? == bincode::serialize(b.cross_thread_ref_data.$field())?, "Ancestor reference changed: {}", stringify!($field));)+};
                }
                check_ref!(
                    block_identifier,
                    block_seq_no,
                    block_thread_identifier,
                    threads_table,
                    parent_block_identifier,
                    block_refs,
                    is_tombstone
                );
                a.cross_thread_ref_data = b.cross_thread_ref_data.clone();
                ensure!(
                    bincode::serialize(&a)? == bincode::serialize(&b)?,
                    "Ancestor metadata changed"
                );
            }
            let export_s = export_started.elapsed().as_secs_f64();

            let reimport_started = Instant::now();
            let mut file = File::open(new_path)?;
            let status = if synthetic_priorities {
                target.set_state_from_snapshot_reader_checked(
                    &mut file,
                    &expected.thread_id,
                    Arc::new(Mutex::new(HashSet::new())),
                )?
            } else {
                let mut envelope = [0; 4];
                file.read_exact(&mut envelope)?;
                assert_eq!(&envelope, b"ZSTD");
                let mut decoder = zstd::Decoder::new(file)?;
                target.set_state_from_snapshot_reader_checked(
                    &mut decoder,
                    &expected.thread_id,
                    Arc::new(Mutex::new(HashSet::new())),
                )?
            };
            assert!(matches!(status, SnapshotImportStatus::Imported));
            let final_state = load_and_check(&target, &expected, limit, &prefix.db_messages)?;
            assert_eq!(
                restored.shard_state.0.durable.map().hash(),
                final_state.shard_state.0.durable.map().hash()
            );
            assert_eq!(
                restored.shard_state.0.durable.map().capacity(),
                final_state.shard_state.0.durable.map().capacity()
            );
            for (index, (state, accounts)) in [
                (&restored, &repo.thread_accounts_repository),
                (&final_state, &target.thread_accounts_repository),
            ]
            .into_iter()
            .enumerate()
            {
                let checkpoint = dir.path().join(format!("checkpoint-{index}"));
                state.clone().save_to_file(&checkpoint, accounts)?;
                let loaded = OptimisticStateImpl::load_from_file(&checkpoint, accounts)?;
                assert_eq!(bincode::serialize(&loaded)?, bincode::serialize(state)?);
                assert_eq!(
                    loaded.shard_state.0.durable.map().hash(),
                    state.shard_state.0.durable.map().hash()
                );
            }
            let normal = expected
                .messages
                .messages
                .iter()
                .map(|(_, d)| d.messages.iter().count())
                .sum::<usize>();
            let high = expected
                .high_priority_messages
                .messages
                .iter()
                .map(|(_, d)| d.messages.iter().count())
                .sum::<usize>();
            eprintln!("FULL_NODE_VERIFIED synthetic_priorities={synthetic_priorities} accounts={limit} normal_inboxes={normal} high_inboxes={high} db_message_bodies={} import_s={import_s:.3} export_s={export_s:.3} reimport_and_check_s={:.3} total_s={:.3}",
                prefix.db_messages.iter().map(Vec::len).sum::<usize>(), reimport_started.elapsed().as_secs_f64(), started.elapsed().as_secs_f64());
        }
        Ok(())
    }

    #[test]
    fn legacy_snapshot_rebases_both_queues_and_preserves_legacy_wire_format() -> anyhow::Result<()>
    {
        let (prefix, state, durable) = fixture()?;
        let legacy = encode(prefix, &state, &durable)?;
        let header = read_snapshot_header_from_reader(&mut legacy.as_slice())?.unwrap();
        assert_eq!(header.block_id, state.block_id);
        for compressed in [false, true] {
            let dir = tempfile::tempdir()?;
            let (mut repository, _) = super::super::tests::make_test_repository(&dir)?;
            let status = if compressed {
                let bytes = zstd::encode_all(legacy.as_slice(), 1)?;
                let mut decoder = zstd::stream::read::Decoder::new(bytes.as_slice())?;
                repository.set_state_from_snapshot_reader_checked(
                    &mut decoder,
                    &state.thread_id,
                    Arc::new(Mutex::new(HashSet::new())),
                )?
            } else {
                repository.set_state_from_snapshot_reader_checked(
                    &mut legacy.as_slice(),
                    &state.thread_id,
                    Arc::new(Mutex::new(HashSet::new())),
                )?
            };
            assert!(matches!(status, SnapshotImportStatus::Imported));
            let restored = repository
                .thread_last_finalized_state
                .guarded(|states| states.get(&state.thread_id).cloned())
                .unwrap();
            let mut snapshot = super::super::tests::make_test_snapshot();
            snapshot.set_optimistic_state(restored.as_ref().clone())?;
            snapshot.db_messages = super::super::inbox_snapshot_tests::fixture().1;
            validate_snapshot_inboxes(&restored, snapshot.db_messages())?;
            let route = AccountRouting::default();
            for (source, target) in [
                (&state.messages, &restored.messages),
                (&state.high_priority_messages, &restored.high_priority_messages),
            ] {
                let source = source
                    .messages
                    .get(&node_types::DAppIdentifier::default())
                    .unwrap()
                    .messages
                    .get(&route)
                    .unwrap();
                let target = target.account_inbox_by_routing(&route).unwrap();
                assert_eq!(bincode::serialize(source)?, bincode::serialize(&target.messages)?);
                assert_eq!(
                    target.commitment.consumed,
                    crate::types::InboxCommitment::default().consumed
                );
            }
            let repo = &repository.thread_accounts_repository;
            let checkpoint = dir.path().join(format!("checkpoint-{compressed}"));
            restored.as_ref().clone().save_to_file(&checkpoint, repo)?;
            let reloaded = OptimisticStateImpl::load_from_file(&checkpoint, repo)?;
            assert_eq!(reloaded.messages.root_hash(), restored.messages.root_hash());
            assert_eq!(
                reloaded.high_priority_messages.root_hash(),
                restored.high_priority_messages.root_hash()
            );
            let mut durable = Cursor::new(Vec::new());
            repo.export_snapshot_v1_to_writer_checked(
                &restored.shard_state.0,
                &mut durable,
                &|| false,
            )?;
            let current = SnapshotFile {
                snapshot,
                format: SnapshotFormat::Streamed { durable_bytes: durable.into_inner() },
                compression: SnapshotCompression::Plain,
            };
            let mut output = Vec::new();
            write_snapshot_payload_to_writer(&current, &mut output)?;
            assert!(output.starts_with(MAGIC));
            read_thread_snapshot_from_payload(&output, SnapshotCompression::Plain)?;
        }
        assert!(read_thread_snapshot_from_payload(&legacy, SnapshotCompression::Plain).is_ok());
        Ok(())
    }

    #[test]
    fn legacy_snapshot_uses_the_staged_repository_import_path() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (prefix, state, durable) = fixture()?;
        let bytes = encode(prefix, &state, &durable)?;
        let (mut repository, _) = super::super::tests::make_test_repository(&dir)?;
        let controls = repository.thread_accounts_repository.get_archive_thread_control_states()?;
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(repository
            .set_state_from_snapshot_reader_checked(
                &mut trailing.as_slice(),
                &state.thread_id,
                Arc::new(Mutex::new(HashSet::new())),
            )
            .is_err());
        assert_eq!(
            repository.thread_accounts_repository.get_archive_thread_control_states()?,
            controls
        );
        // Retry on the same repository after the late error: the failed epoch
        // must not have been published and must not prevent a clean import.
        let status = repository.set_state_from_snapshot_reader_checked(
            &mut bytes.as_slice(),
            &state.thread_id,
            Arc::new(Mutex::new(HashSet::new())),
        )?;
        assert!(matches!(status, SnapshotImportStatus::Imported));
        let restored = repository
            .thread_last_finalized_state
            .guarded(|states| states.get(&state.thread_id).cloned())
            .unwrap();
        validate_snapshot_inboxes(&restored, &super::super::inbox_snapshot_tests::fixture().1)?;
        Ok(())
    }

    #[test]
    fn legacy_snapshot_rejects_inconsistent_queues_and_segments() -> anyhow::Result<()> {
        fn rejected(bytes: &[u8]) -> anyhow::Result<()> {
            let dir = tempfile::tempdir()?;
            let (mut repository, _) = super::super::tests::make_test_repository(&dir)?;
            let controls =
                repository.thread_accounts_repository.get_archive_thread_control_states()?;
            assert!(repository
                .set_state_from_snapshot_reader_checked(
                    &mut &*bytes,
                    &ThreadIdentifier::default(),
                    Arc::new(Mutex::new(HashSet::new())),
                )
                .is_err());
            assert_eq!(
                repository.thread_accounts_repository.get_archive_thread_control_states()?,
                controls
            );
            Ok(())
        }
        let (mut prefix, state, durable) = fixture()?;
        prefix.db_messages.clear();
        let missing = encode(prefix, &state, &durable)?;
        rejected(&missing)?;
        let (prefix, mut state, durable) = fixture()?;
        state.messages.cursor = 10;
        let bad_cursor = encode(prefix, &state, &durable)?;
        rejected(&bad_cursor)?;
        let (prefix, state, durable) = fixture()?;
        let bytes = encode(prefix, &state, &durable)?;
        rejected(&bytes[..bytes.len() - 1])?;
        let mut extra = bytes;
        extra.push(0);
        rejected(&extra)?;
        Ok(())
    }
}
