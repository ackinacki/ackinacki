use std::sync::Arc;
use std::sync::OnceLock;

use base64::Engine;
use node_types::AccountCodeHash;
use node_types::AccountDataHash;
use node_types::AccountHash;
use node_types::AccountIdentifier;
use node_types::DAppIdentifier;
use serde::Deserializer;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::Serializable;
use tvm_block::ShardAccount;
use tvm_types::read_single_root_boc;
use tvm_types::write_boc;
use tvm_types::Cell;

use crate::live_metrics::LiveThreadAccountCounter;

/// Magic written by the canonical TVM BOC writer (`0xb5ee9c72`, big-endian).
/// Synthetic test accounts must not start with this value, so they can be
/// distinguished from TVM accounts without decoding.
const TVM_BOC_MAGIC: [u8; 4] = [0xb5, 0xee, 0x9c, 0x72];

#[derive(Clone, Debug)]
// VM agnostic account repr.
pub struct ThreadAccount {
    inner: Arc<ThreadAccountInner>,
}

#[derive(Debug)]
struct ThreadAccountInner {
    canonical: OnceLock<Box<[u8]>>,
    decoded_tvm: OnceLock<ShardAccount>,
    live_counter: LiveThreadAccountCounter,
}

impl PartialEq for ThreadAccount {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
            || (self.canonical_bytes() == other.canonical_bytes()
                && self.inner.live_counter == other.inner.live_counter)
    }
}

impl Eq for ThreadAccount {}

impl Default for ThreadAccount {
    fn default() -> Self {
        Self::from_tvm(ShardAccount::default())
    }
}

impl ThreadAccount {
    pub fn new(
        account: VmAccount,
        last_trans_hash: node_types::TransactionHash,
        last_trans_lt: u64,
        dapp_id: Option<DAppIdentifier>,
    ) -> anyhow::Result<Self> {
        let mut shard = ShardAccount::with_params(
            &tvm_block::Account::default(),
            last_trans_hash.into(),
            last_trans_lt,
            dapp_id.map(From::from),
        )
        .map_err(|err| anyhow::anyhow!("{err}"))?;
        shard.set_account_cell(account.into()).map_err(|err| anyhow::anyhow!("{err}"))?;
        Self::try_from_tvm(shard)
    }

    pub fn vm_account(&self) -> anyhow::Result<VmAccount> {
        Ok(self.tvm_shard_account()?.account_cell().map_err(|err| anyhow::anyhow!("{err}"))?.into())
    }

    pub fn get_dapp_id(&self) -> Option<DAppIdentifier> {
        self.dapp_id()
    }

    pub fn last_trans_hash(&self) -> node_types::TransactionHash {
        self.tvm_shard_account().expect("TVM transaction metadata").last_trans_hash().into()
    }

    pub fn last_trans_lt(&self) -> u64 {
        self.tvm_shard_account().expect("TVM transaction metadata").last_trans_lt()
    }

    pub fn from_tvm(account: ShardAccount) -> Self {
        Self::try_from_tvm(account).expect("valid ShardAccount must have a canonical encoding")
    }

    pub fn try_from_tvm(account: ShardAccount) -> anyhow::Result<Self> {
        Ok(Self::from_decoded_tvm(account))
    }

    fn from_decoded_tvm(account: ShardAccount) -> Self {
        Self {
            inner: Arc::new(ThreadAccountInner {
                canonical: OnceLock::new(),
                decoded_tvm: OnceLock::from(account),
                live_counter: LiveThreadAccountCounter::new(),
            }),
        }
    }

    /// Store synthetic test account bytes with the account hash in the last 32 bytes.
    #[cfg(test)]
    pub(crate) fn from_synthetic_canonical(canonical: Box<[u8]>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !canonical.starts_with(&TVM_BOC_MAGIC),
            "synthetic account encoding collides with the TVM BOC magic"
        );
        anyhow::ensure!(canonical.len() >= 32, "synthetic account body must carry a 32-byte hash");
        Ok(Self::from_canonical_bytes(canonical))
    }

    /// Recreate a finalized account from authenticated protocol-canonical bytes.
    ///
    /// This deliberately performs no BOC or VM decoding. Untrusted ingress
    /// must use [`Self::validate_canonical_bytes`] instead.
    pub fn from_canonical_bytes(canonical: Box<[u8]>) -> Self {
        Self {
            inner: Arc::new(ThreadAccountInner {
                canonical: OnceLock::from(canonical),
                decoded_tvm: OnceLock::new(),
                live_counter: LiveThreadAccountCounter::new(),
            }),
        }
    }

    /// Validate TVM decoding at untrusted ingress while retaining the body.
    /// Trusted database reads use the lazy constructor instead.
    pub fn validate_tvm_bytes(canonical: Box<[u8]>) -> anyhow::Result<Self> {
        let decoded = ShardAccount::construct_from_bytes(&canonical)
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        let account = Self::from_canonical_bytes(canonical);
        let _ = account.inner.decoded_tvm.set(decoded);
        Ok(account)
    }

    /// Snapshot ingress: decode untrusted TVM bytes once, then encode the normalized representation
    /// once and retain both views. A redirect leaf commits only its target DApp, so discard any
    /// incidental transaction metadata rather than persisting an arbitrary redirect representation.
    pub(crate) fn recanonicalize_tvm_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut decoded =
            ShardAccount::construct_from_bytes(bytes).map_err(|err| anyhow::anyhow!("{err}"))?;
        if decoded.is_redirect() {
            let target = decoded
                .get_dapp_id()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("redirect without a DApp"))?;
            decoded = ShardAccount::with_redirect(Default::default(), 0, Some(target))
                .map_err(|err| anyhow::anyhow!("{err}"))?;
        }
        let root = decoded.serialize().map_err(|err| anyhow::anyhow!("{err}"))?;
        let canonical = write_boc(&root).map_err(|err| anyhow::anyhow!("{err}"))?;
        let account = Self::from_canonical_bytes(canonical.into_boxed_slice());
        let _ = account.inner.decoded_tvm.set(decoded);
        Ok(account)
    }

    /// Validate an untrusted canonical body against its separately supplied
    /// full account hash, retaining the original byte allocation on success.
    pub fn validate_canonical_bytes(
        canonical: Box<[u8]>,
        expected_hash: AccountHash,
    ) -> anyhow::Result<Self> {
        let decoded = if canonical.starts_with(&TVM_BOC_MAGIC) {
            let root = read_single_root_boc(&canonical).map_err(|err| anyhow::anyhow!("{err}"))?;
            anyhow::ensure!(
                root.repr_hash().as_slice() == expected_hash.as_slice(),
                "TVM canonical account hash does not match the expected full account hash"
            );
            let decoded = ShardAccount::construct_from_cell(root.clone())
                .map_err(|err| anyhow::anyhow!("{err}"))?;
            anyhow::ensure!(
                write_boc(&root).map_err(|err| anyhow::anyhow!("{err}"))?.as_slice()
                    == canonical.as_ref(),
                "TVM account body does not use the protocol-canonical BOC encoding"
            );
            decoded
        } else {
            anyhow::bail!(
                "canonical account validation only supports TVM accounts; synthetic test accounts are not accepted at untrusted ingress"
            );
        };
        let account = Self::from_canonical_bytes(canonical);
        let _ = account.inner.decoded_tvm.set(decoded);
        Ok(account)
    }

    /// Return canonical bytes, materializing the BOC once if only decoded TVM
    /// state was supplied. Errors in this validated in-memory state are fatal
    /// invariant violations; untrusted ingress is checked by fallible readers.
    pub fn canonical_bytes(&self) -> &[u8] {
        self.inner.canonical.get_or_init(|| {
            let decoded = self
                .inner
                .decoded_tvm
                .get()
                .expect("an account must have canonical bytes or decoded TVM state");
            let root = decoded
                .serialize()
                .expect("validated decoded TVM account must serialize to a cell");
            write_boc(&root)
                .expect("validated decoded TVM account must have a canonical BOC")
                .into_boxed_slice()
        })
    }

    pub fn canonical_size(&self) -> u64 {
        u64::try_from(self.canonical_bytes().len())
            .expect("canonical account size must fit the protocol u64 capacity")
    }

    /// Move canonical bytes out when uniquely owned; leave shared accounts intact.
    /// A uniquely owned decoded cache is released on the calling thread.
    pub fn into_canonical_bytes(self) -> Box<[u8]> {
        self.canonical_bytes();
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.canonical.into_inner().expect("canonical bytes initialized"),
            Err(inner) => inner.canonical.get().expect("canonical bytes initialized").clone(),
        }
    }

    /// TVM accounts use the complete `ShardAccount` root hash.
    /// Synthetic test accounts store their hash in the last 32 bytes.
    pub fn account_hash(&self) -> AccountHash {
        if !self.is_tvm() {
            let canonical = self.canonical_bytes();
            let suffix = canonical
                .get(canonical.len().saturating_sub(32)..)
                .expect("synthetic account body must carry a 32-byte hash");
            return AccountHash::new(
                suffix.try_into().expect("synthetic account body must carry a 32-byte hash"),
            );
        }
        self.tvm_shard_account()
            .expect("validated TVM account")
            .serialize()
            .expect("validated ShardAccount must serialize to a cell")
            .repr_hash()
            .into()
    }

    pub fn is_tvm(&self) -> bool {
        match self.inner.canonical.get() {
            Some(canonical) => canonical.starts_with(&TVM_BOC_MAGIC),
            None => self.inner.decoded_tvm.get().is_some(),
        }
    }

    /// Return the DApp recorded by a TVM account. Synthetic test accounts
    /// have no embedded routing metadata and return None.
    pub fn dapp_id(&self) -> Option<DAppIdentifier> {
        if !self.is_tvm() {
            return None;
        }
        self.tvm_shard_account()
            .expect("validated TVM account")
            .get_dapp_id()
            .cloned()
            .map(DAppIdentifier::from)
    }

    /// Whether this value is a synthetic TVM redirect body. Persistent redirect
    /// entries remain AccountInfo::Redirect; synthetic test accounts are not redirects.
    pub fn is_redirect(&self) -> bool {
        self.is_tvm() && self.tvm_shard_account().expect("validated TVM account").is_redirect()
    }

    /// Return the TVM representation, initializing it on demand and sharing
    /// the cached value across clones.
    pub fn tvm_shard_account(&self) -> anyhow::Result<&ShardAccount> {
        if !self.is_tvm() {
            anyhow::bail!("Can't decode non TVM state account as TVM")
        }
        if let Some(decoded) = self.inner.decoded_tvm.get() {
            return Ok(decoded);
        }
        Ok(self.inner.decoded_tvm.get_or_init(|| {
            let canonical = self
                .inner
                .canonical
                .get()
                .expect("an account without decoded TVM state must have canonical bytes");
            ShardAccount::construct_from_bytes(canonical)
                .expect("authenticated TVM canonical bytes must decode to a ShardAccount")
        }))
    }

    /// Return the inner TVM account without exposing its untyped storage cell.
    pub fn tvm_account(&self) -> anyhow::Result<tvm_block::Account> {
        self.tvm_shard_account()?
            .read_account()
            .map_err(|err| anyhow::anyhow!("{err}"))?
            .as_struct()
            .map_err(|err| anyhow::anyhow!("{err}"))
    }

    pub fn redirect(dapp_id: DAppIdentifier) -> Self {
        Self::from_tvm(
            ShardAccount::with_redirect(Default::default(), 0, Some(dapp_id.into()))
                .expect("Failed to create redirect account"),
        )
    }

    pub fn write_bytes(&self) -> anyhow::Result<Vec<u8>> {
        Ok(self.canonical_bytes().to_vec())
    }

    /// Retain trusted canonical bytes without decoding them.
    ///
    /// The Result return type is retained for API compatibility. Untrusted
    /// ingress with an expected hash uses [`Self::validate_canonical_bytes`].
    pub fn read_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(Self::from_canonical_bytes(Box::from(bytes)))
    }

    /// Owned-buffer form of [`Self::read_bytes`], retaining the allocation.
    pub fn read_owned_bytes(bytes: Vec<u8>) -> anyhow::Result<Self> {
        Ok(Self::from_canonical_bytes(bytes.into_boxed_slice()))
    }

    pub fn with_redirect(&self, redirect_dapp_id: DAppIdentifier) -> anyhow::Result<Self> {
        if !self.is_tvm() {
            return Ok(self.clone());
        }
        let shard_account = self.tvm_shard_account()?;
        if let Some(account_dapp_id) =
            shard_account.get_dapp_id().cloned().map(DAppIdentifier::from)
        {
            if account_dapp_id != redirect_dapp_id {
                tracing::error!(
                    target: "node",
                    "Redirect dapp id mismatch: account={account_dapp_id:?}, requested={redirect_dapp_id:?}"
                );
            }
        }
        Ok(Self::from_tvm(
            ShardAccount::with_redirect(
                shard_account.last_trans_hash().clone(),
                shard_account.last_trans_lt(),
                Some(redirect_dapp_id.into()),
            )
            .map_err(|err| anyhow::anyhow!("{err}"))?,
        ))
    }
}

impl From<ShardAccount> for ThreadAccount {
    fn from(value: ShardAccount) -> Self {
        Self::from_tvm(value)
    }
}

impl TryFrom<ThreadAccount> for ShardAccount {
    type Error = anyhow::Error;

    fn try_from(value: ThreadAccount) -> Result<Self, Self::Error> {
        value.tvm_shard_account().cloned()
    }
}

impl TryFrom<&ThreadAccount> for ShardAccount {
    type Error = anyhow::Error;

    fn try_from(value: &ThreadAccount) -> Result<Self, Self::Error> {
        value.tvm_shard_account().cloned()
    }
}

impl serde::Serialize for ThreadAccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Active-protocol compatibility: the legacy wire representation is
        // body-only. Epic 1 uses a separately versioned record carrying the
        // full hash rather than silently changing these bytes.
        serializer.serialize_bytes(self.canonical_bytes())
    }
}

impl<'de> serde::Deserialize<'de> for ThreadAccount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Preserve the body-only wire format without VM decoding. Content
        // validation belongs to the caller's explicit ingress validation.
        let canonical = Vec::<u8>::deserialize(deserializer)?;
        Ok(Self::from_canonical_bytes(canonical.into_boxed_slice()))
    }
}

/// Compatibility view of the inner TVM account cell. The complete version,
/// canonical bytes, and full account hash remain owned by ThreadAccount.
#[derive(Clone, Debug, Default)]
pub struct VmAccount(Cell);

impl VmAccount {
    pub fn read_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(Self(read_single_root_boc(bytes).map_err(|err| anyhow::anyhow!("{err}"))?))
    }

    pub fn read_base64(value: &str) -> anyhow::Result<Self> {
        Self::read_bytes(&base64::engine::general_purpose::STANDARD.decode(value)?)
    }

    pub fn write_bytes(&self) -> anyhow::Result<Vec<u8>> {
        write_boc(&self.0).map_err(|err| anyhow::anyhow!("{err}"))
    }

    pub fn id(&self) -> Option<AccountIdentifier> {
        tvm_block::Account::try_from(self).ok()?.get_id().map(AccountIdentifier::from)
    }

    /// Inner TVM cell hash used by transactions; not the full state-map hash.
    pub fn hash(&self) -> AccountHash {
        self.0.repr_hash().into()
    }

    pub fn data_hash(&self) -> anyhow::Result<Option<AccountDataHash>> {
        Ok(tvm_block::Account::try_from(self)?.get_data().map(|cell| cell.repr_hash().into()))
    }

    pub fn code_hash(&self) -> anyhow::Result<Option<AccountCodeHash>> {
        Ok(tvm_block::Account::try_from(self)?.get_code().map(|cell| cell.repr_hash().into()))
    }

    pub fn is_none(&self) -> bool {
        tvm_block::Account::try_from(self).is_ok_and(|account| account.is_none())
    }
}

impl From<Cell> for VmAccount {
    fn from(cell: Cell) -> Self {
        Self(cell)
    }
}

impl From<&Cell> for VmAccount {
    fn from(cell: &Cell) -> Self {
        Self(cell.clone())
    }
}

impl TryFrom<tvm_block::Account> for VmAccount {
    type Error = anyhow::Error;

    fn try_from(account: tvm_block::Account) -> anyhow::Result<Self> {
        Self::try_from(&account)
    }
}

impl TryFrom<&tvm_block::Account> for VmAccount {
    type Error = anyhow::Error;

    fn try_from(account: &tvm_block::Account) -> anyhow::Result<Self> {
        Ok(Self(account.serialize().map_err(|err| anyhow::anyhow!("{err}"))?))
    }
}

impl TryFrom<VmAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(account: VmAccount) -> anyhow::Result<Self> {
        Self::construct_from_cell(account.0).map_err(|err| anyhow::anyhow!("{err}"))
    }
}

impl TryFrom<&VmAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(account: &VmAccount) -> anyhow::Result<Self> {
        Self::try_from(account.clone())
    }
}

impl TryFrom<&mut VmAccount> for tvm_block::Account {
    type Error = anyhow::Error;

    fn try_from(account: &mut VmAccount) -> anyhow::Result<Self> {
        Self::try_from(&*account)
    }
}

impl From<VmAccount> for Cell {
    fn from(account: VmAccount) -> Self {
        account.0
    }
}

impl From<&VmAccount> for Cell {
    fn from(account: &VmAccount) -> Self {
        account.0.clone()
    }
}

impl From<&mut VmAccount> for Cell {
    fn from(account: &mut VmAccount) -> Self {
        account.0.clone()
    }
}

impl serde::Serialize for VmAccount {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.write_bytes().map_err(serde::ser::Error::custom)?)
    }
}

impl<'de> serde::Deserialize<'de> for VmAccount {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        Self::read_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tvm_block::Serializable;
    use tvm_types::read_single_root_boc;

    use super::ThreadAccount;
    use super::VmAccount;

    #[test]
    fn full_account_hash_distinguishes_versions_with_the_same_vm_hash() -> anyhow::Result<()> {
        let vm = VmAccount::try_from(&tvm_block::Account::default())?;
        let first = ThreadAccount::new(vm.clone(), Default::default(), 1, None)?;
        let second = ThreadAccount::new(vm.clone(), Default::default(), 2, None)?;
        assert_eq!(first.vm_account()?.hash(), vm.hash());
        assert_eq!(second.vm_account()?.hash(), vm.hash());
        assert_ne!(first.account_hash(), second.account_hash());
        assert!(first.inner.canonical.get().is_none());
        Ok(())
    }

    #[test]
    fn untrusted_body_validation_seeds_shared_decode() -> anyhow::Result<()> {
        assert!(ThreadAccount::validate_tvm_bytes(vec![0xb5, 0xee, 0x9c, 0x72].into()).is_err());
        let native = tvm_block::ShardAccount::default();
        let body = native.write_to_bytes()?.into_boxed_slice();
        let ptr = body.as_ptr();
        let account = ThreadAccount::validate_tvm_bytes(body)?;
        assert_eq!(account.canonical_bytes().as_ptr(), ptr);
        assert_eq!(account.tvm_shard_account()?, &native);
        let clone = account.clone();
        assert!(std::ptr::eq(account.tvm_shard_account()?, clone.tvm_shard_account()?));
        Ok(())
    }

    #[test]
    fn vm_account_compatibility_preserves_wire_and_full_thread_hash() -> anyhow::Result<()> {
        let inner = tvm_block::Account::default();
        let vm_account = VmAccount::try_from(&inner)?;
        let expected_bytes = inner.write_to_bytes()?;
        assert_eq!(vm_account.write_bytes()?, expected_bytes);
        assert_eq!(tvm_block::Account::try_from(VmAccount::read_bytes(&expected_bytes)?)?, inner);
        assert_eq!(bincode::serialize(&vm_account)?, bincode::serialize(&expected_bytes)?);
        let routing_dapp = node_types::DAppIdentifier::new([0x32; 32]);
        let thread =
            ThreadAccount::new(vm_account.clone(), Default::default(), 7, Some(routing_dapp))?;
        assert!(thread.inner.canonical.get().is_none());
        assert_eq!(thread.last_trans_lt(), 7);
        assert_eq!(thread.get_dapp_id(), Some(routing_dapp));
        assert_eq!(thread.vm_account()?.hash(), vm_account.hash());
        assert!(thread.inner.canonical.get().is_none());
        assert_ne!(thread.account_hash(), vm_account.hash());
        assert_eq!(thread.canonical_bytes(), thread.tvm_shard_account()?.write_to_bytes()?);
        Ok(())
    }

    #[test]
    fn consuming_canonical_moves_unique_bytes_and_preserves_shared_decode() -> anyhow::Result<()> {
        let account = ThreadAccount::default();
        let expected = account.canonical_bytes().to_vec();
        let allocation = account.canonical_bytes().as_ptr();
        let weak = Arc::downgrade(&account.inner);
        let bytes = account.into_canonical_bytes();
        assert_eq!(bytes.as_ref(), expected);
        assert_eq!(bytes.as_ptr(), allocation);
        assert!(weak.upgrade().is_none(), "decoded owner must be released with extraction");

        let account = ThreadAccount::default();
        let clone = account.clone();
        let bytes = clone.into_canonical_bytes();
        assert_eq!(bytes.as_ref(), account.canonical_bytes());
        assert!(account.inner.decoded_tvm.get().is_some());
        assert_ne!(bytes.as_ptr(), account.canonical_bytes().as_ptr());
        Ok(())
    }

    #[test]
    fn snapshot_ingress_normalizes_once_and_moves_the_canonical_allocation() -> anyhow::Result<()> {
        let decoded = tvm_block::ShardAccount::with_params(
            &tvm_block::Account::default(),
            [7; 32].into(),
            42,
            None,
        )?;
        let mut wire = Vec::new();
        tvm_types::BocWriter::with_root(&decoded.serialize()?)?
            .write_ex(&mut wire, true, true, None, None)?;
        let canonical = decoded.write_to_bytes()?;
        assert_ne!(wire, canonical);
        let account = ThreadAccount::recanonicalize_tvm_bytes(&wire)?;
        assert_eq!(account.canonical_bytes(), canonical);
        assert_eq!(account.inner.decoded_tvm.get(), Some(&decoded));
        let allocation = account.canonical_bytes().as_ptr();
        let bytes = account.into_canonical_bytes().into_vec();
        assert_eq!(bytes.as_ptr(), allocation);
        assert_eq!(bytes, canonical);
        assert!(ThreadAccount::recanonicalize_tvm_bytes(&[0xff]).is_err());
        Ok(())
    }

    #[test]
    fn snapshot_ingress_normalizes_redirect_metadata() -> anyhow::Result<()> {
        let target = node_types::DAppIdentifier::new([9; 32]);
        let decoded =
            tvm_block::ShardAccount::with_redirect([7; 32].into(), 42, Some(target.into()))?;
        let account = ThreadAccount::recanonicalize_tvm_bytes(&decoded.write_to_bytes()?)?;
        let expected = ThreadAccount::redirect(target);
        assert_eq!(account.canonical_bytes(), expected.canonical_bytes());
        assert_eq!(account.account_hash(), expected.account_hash());
        assert_eq!(account.last_trans_lt(), 0);
        Ok(())
    }

    #[test]
    fn finalized_tvm_account_keeps_canonical_bytes_hash_and_size() -> anyhow::Result<()> {
        let shard_account = tvm_block::ShardAccount::default();
        let expected_bytes = shard_account.write_to_bytes()?;
        let expected_hash = shard_account.serialize()?.repr_hash();

        let account = ThreadAccount::try_from_tvm(shard_account)?;

        assert_eq!(account.canonical_bytes(), expected_bytes);
        assert_eq!(account.canonical_size(), expected_bytes.len() as u64);
        assert_eq!(account.account_hash().as_slice(), expected_hash.as_slice());
        assert_eq!(account.tvm_shard_account()?.write_to_bytes()?, expected_bytes);
        Ok(())
    }

    #[test]
    fn full_tvm_hash_commits_shard_account_metadata() -> anyhow::Result<()> {
        let inner = tvm_block::Account::default();
        let first =
            tvm_block::ShardAccount::with_params(&inner, tvm_types::UInt256::default(), 1, None)?;
        let second =
            tvm_block::ShardAccount::with_params(&inner, tvm_types::UInt256::default(), 2, None)?;

        let first = ThreadAccount::try_from_tvm(first)?;
        let second = ThreadAccount::try_from_tvm(second)?;

        assert_eq!(
            first.tvm_shard_account()?.account_cell()?.repr_hash(),
            second.tvm_shard_account()?.account_cell()?.repr_hash()
        );
        assert_ne!(first.account_hash(), second.account_hash());
        Ok(())
    }

    #[test]
    fn canonical_tvm_ingress_retains_the_supplied_allocation() -> anyhow::Result<()> {
        let canonical: Box<[u8]> = tvm_block::ShardAccount::default().write_to_bytes()?.into();
        let expected_hash = read_single_root_boc(&canonical)?.repr_hash();

        let canonical_ptr = canonical.as_ptr();
        let account = ThreadAccount::from_canonical_bytes(canonical);

        assert_eq!(canonical_ptr, account.canonical_bytes().as_ptr());
        assert_eq!(account.account_hash().as_slice(), expected_hash.as_slice());
        Ok(())
    }

    #[test]
    fn untrusted_canonical_ingress_checks_the_supplied_hash() -> anyhow::Result<()> {
        let canonical: Box<[u8]> = tvm_block::ShardAccount::default().write_to_bytes()?.into();
        let expected_hash = read_single_root_boc(&canonical)?.repr_hash();

        let wrong_hash = node_types::AccountHash::new([0xA5; 32]);
        assert!(ThreadAccount::validate_canonical_bytes(canonical.clone(), wrong_hash).is_err());
        let canonical_ptr = canonical.as_ptr();
        let account =
            ThreadAccount::validate_canonical_bytes(canonical, expected_hash.clone().into())?;
        assert_eq!(canonical_ptr, account.canonical_bytes().as_ptr());

        Ok(())
    }

    #[test]
    fn tvm_hash_initializes_shared_decode_without_reencoding_boc() -> anyhow::Result<()> {
        let decoded = tvm_block::ShardAccount::default();
        let root = decoded.serialize()?;
        let bytes = tvm_types::write_boc(&root)?.into_boxed_slice();
        let bytes_ptr = bytes.as_ptr();
        let account = ThreadAccount::from_canonical_bytes(bytes);
        let clone = account.clone();
        assert!(account.inner.decoded_tvm.get().is_none());
        assert_eq!(account.account_hash().as_slice(), root.repr_hash().as_slice());
        assert_eq!(clone.account_hash(), account.account_hash());
        assert!(std::ptr::eq(account.tvm_shard_account()?, clone.tvm_shard_account()?));
        assert_eq!(bytes_ptr, clone.canonical_bytes().as_ptr());
        Ok(())
    }

    #[test]
    fn synthetic_canonical_requires_a_hash_suffix() {
        assert!(ThreadAccount::from_synthetic_canonical(vec![0x41; 31].into()).is_err());
        let account = ThreadAccount::from_synthetic_canonical(vec![0x41; 32].into()).unwrap();
        assert_eq!(account.account_hash(), node_types::AccountHash::new([0x41; 32]));
        assert!(account.inner.decoded_tvm.get().is_none());
    }

    #[test]
    fn redirect_serialization_remains_the_existing_raw_boc() -> anyhow::Result<()> {
        let shard_account = tvm_block::ShardAccount::with_redirect(
            tvm_types::UInt256::default(),
            0,
            Some(tvm_types::UInt256::default()),
        )?;
        let expected = shard_account.write_to_bytes()?;

        let account = ThreadAccount::try_from_tvm(shard_account)?;
        let decoded = account.tvm_shard_account()?;

        assert!(account.is_redirect());
        assert_eq!(account.dapp_id(), Some(node_types::DAppIdentifier::default()));
        assert!(account.inner.canonical.get().is_none());

        assert!(expected.starts_with(&super::TVM_BOC_MAGIC));
        assert_eq!(account.write_bytes()?, expected);
        assert!(decoded.is_redirect());
        assert_eq!(
            decoded.get_dapp_id().cloned().map(node_types::DAppIdentifier::from),
            Some(node_types::DAppIdentifier::default())
        );
        Ok(())
    }

    #[test]
    fn finalized_account_clones_share_canonical_bytes() -> anyhow::Result<()> {
        let account = ThreadAccount::try_from_tvm(tvm_block::ShardAccount::default())?;
        let clone = account.clone();

        assert!(Arc::ptr_eq(&account.inner, &clone.inner));
        assert_eq!(account.canonical_bytes().as_ptr(), clone.canonical_bytes().as_ptr());
        assert_eq!(std::mem::size_of::<ThreadAccount>(), std::mem::size_of::<usize>());
        assert_eq!(account.account_hash(), clone.account_hash());
        assert_eq!(account.canonical_size(), clone.canonical_size());
        Ok(())
    }

    #[test]
    fn decoded_tvm_cache_is_shared_without_affecting_equality_or_wire_bytes() -> anyhow::Result<()>
    {
        let decoded = tvm_block::ShardAccount::default();
        let populated = ThreadAccount::try_from_tvm(decoded.clone())?;
        assert_eq!(populated.inner.decoded_tvm.get(), Some(&decoded));
        let lazy = ThreadAccount::from_canonical_bytes(populated.canonical_bytes().into());
        let clone = lazy.clone();
        assert!(lazy.inner.decoded_tvm.get().is_none());
        assert!(Arc::ptr_eq(&lazy.inner, &clone.inner));
        assert_eq!(lazy, populated);
        let wire_before = bincode::serialize(&lazy)?;
        assert_eq!(clone.tvm_shard_account()?, &decoded);
        assert!(std::ptr::eq(lazy.tvm_shard_account()?, clone.tvm_shard_account()?,));
        assert_eq!(bincode::serialize(&lazy)?, wire_before);
        assert_eq!(lazy, populated);
        let restored: ThreadAccount = bincode::deserialize(&wire_before)?;
        assert!(restored.inner.decoded_tvm.get().is_none());
        assert_eq!(restored, populated);
        assert!(restored.inner.decoded_tvm.get().is_none());
        assert_eq!(restored.tvm_shard_account()?, &decoded);
        Ok(())
    }

    #[test]
    fn malformed_untrusted_tvm_is_rejected_before_lazy_initialization() {
        let malformed = Box::from(super::TVM_BOC_MAGIC.as_slice());
        assert!(ThreadAccount::validate_canonical_bytes(
            malformed,
            node_types::AccountHash::default(),
        )
        .is_err());
    }

    #[test]
    #[should_panic(expected = "authenticated TVM canonical bytes must decode")]
    fn invalid_authenticated_tvm_decode_is_an_invariant_violation() {
        let account =
            ThreadAccount::from_canonical_bytes(Box::from(super::TVM_BOC_MAGIC.as_slice()));
        let _ = account.tvm_shard_account();
    }

    #[test]
    fn body_only_readers_and_serde_leave_tvm_decoding_lazy() -> anyhow::Result<()> {
        let decoded = tvm_block::ShardAccount::default();
        let bytes = decoded.write_to_bytes()?;
        let borrowed = ThreadAccount::read_bytes(&bytes)?;
        let owned_bytes = bytes.clone();
        let owned_ptr = owned_bytes.as_ptr();
        let owned = ThreadAccount::read_owned_bytes(owned_bytes)?;
        assert_eq!(owned.canonical_bytes().as_ptr(), owned_ptr);
        let wire = bincode::serialize(&bytes)?;
        let restored: ThreadAccount = bincode::deserialize(&wire)?;
        for account in [borrowed, owned, restored] {
            assert!(account.inner.decoded_tvm.get().is_none());
            assert_eq!(account.canonical_bytes(), bytes);
            assert_eq!(bincode::serialize(&account)?, wire);
            assert!(account.inner.decoded_tvm.get().is_none());
            assert_eq!(account.tvm_shard_account()?, &decoded);
        }
        Ok(())
    }

    #[test]
    fn lazy_representations_are_shared_across_concurrent_clones() -> anyhow::Result<()> {
        let decoded = tvm_block::ShardAccount::default();
        let expected = decoded.write_to_bytes()?;
        let account = ThreadAccount::try_from_tvm(decoded.clone())?;
        assert!(account.inner.canonical.get().is_none());
        assert!(account.is_tvm());
        let _ = account.account_hash();
        assert!(!account.is_redirect());
        assert_eq!(account.dapp_id(), None);
        assert!(account.inner.canonical.get().is_none());
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let account = account.clone();
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        account.canonical_bytes().as_ptr() as usize
                    })
                })
                .collect();
            for handle in handles {
                assert_eq!(handle.join().unwrap(), account.canonical_bytes().as_ptr() as usize);
            }
        });
        assert_eq!(account.canonical_bytes(), expected);
        let lazy = ThreadAccount::from_canonical_bytes(expected.into_boxed_slice());
        assert!(lazy.inner.decoded_tvm.get().is_none());
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let lazy = lazy.clone();
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        lazy.tvm_shard_account().unwrap();
                        lazy.inner.decoded_tvm.get().unwrap() as *const tvm_block::ShardAccount
                            as usize
                    })
                })
                .collect();
            for handle in handles {
                assert_eq!(
                    handle.join().unwrap(),
                    lazy.inner.decoded_tvm.get().unwrap() as *const tvm_block::ShardAccount
                        as usize
                );
            }
        });
        assert_eq!(lazy.tvm_shard_account()?, &decoded);
        Ok(())
    }

    #[test]
    fn synthetic_account_round_trips_as_canonical_only() -> anyhow::Result<()> {
        let account_hash = node_types::AccountHash::new([0x5A; 32]);
        let expected: Box<[u8]> =
            [&[0x41, 0x56, 0x4d, 0x01, 0xAA, 0xBB][..], account_hash.as_slice()].concat().into();
        let account = ThreadAccount::from_synthetic_canonical(expected.clone())?;
        let restored = ThreadAccount::from_canonical_bytes(account.canonical_bytes().into());

        assert_eq!(account.canonical_bytes(), expected.as_ref());
        assert!(!account.canonical_bytes().starts_with(&super::TVM_BOC_MAGIC));
        assert_eq!(restored, account);
        assert_eq!(restored.account_hash(), account_hash);
        assert_eq!(restored.canonical_size(), restored.canonical_bytes().len() as u64);
        assert!(restored.tvm_shard_account().is_err());
        assert!(!restored.is_redirect());
        assert_eq!(restored.dapp_id(), None);
        assert!(restored.inner.decoded_tvm.get().is_none());
        Ok(())
    }
}
