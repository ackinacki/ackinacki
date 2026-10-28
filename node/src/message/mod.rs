// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::OnceLock;

use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use tvm_block::Deserializable;
use tvm_block::GetRepresentationHash;
use tvm_block::Serializable;
use tvm_types::UInt256;

pub mod identifier;
#[cfg(test)]
pub mod message_stub;

pub trait Message: Debug + Clone + Sync + Send + Serialize + for<'b> Deserialize<'b> {
    type AccountId;

    fn destination(&self) -> Self::AccountId;
}

/// Immutable message version with shared lazy canonical and TVM views.
/// Queue entries carry its identifier separately; no duplicate hash is stored.
#[derive(Clone)]
pub struct WrappedMessage {
    inner: Arc<WrappedMessageInner>,
}

struct WrappedMessageInner {
    canonical: OnceLock<Box<[u8]>>,
    decoded_tvm: OnceLock<tvm_block::Message>,
}

impl PartialEq for WrappedMessage {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner) || self.cmp(other) == Ordering::Equal
    }
}

impl Eq for WrappedMessage {}

impl WrappedMessage {
    pub fn identifier(&self) -> identifier::MessageIdentifier {
        identifier::MessageIdentifier::from(self.tvm_message().expect("validated TVM message"))
    }

    pub fn from_tvm(message: &tvm_block::Message) -> Self {
        Self::try_from_tvm(message).expect("valid TVM message must have a canonical BOC")
    }

    pub fn try_from_tvm(message: &tvm_block::Message) -> tvm_types::Result<Self> {
        Ok(Self {
            inner: Arc::new(WrappedMessageInner {
                canonical: OnceLock::new(),
                decoded_tvm: OnceLock::from(message.clone()),
            }),
        })
    }

    /// Retain trusted canonical bytes without decoding or reencoding them.
    pub fn from_canonical_bytes(canonical: Box<[u8]>) -> Self {
        Self {
            inner: Arc::new(WrappedMessageInner {
                canonical: OnceLock::from(canonical),
                decoded_tvm: OnceLock::new(),
            }),
        }
    }

    /// Validate an untrusted protocol-canonical TVM body, retaining both its
    /// original byte allocation and the decoded validation result on success.
    pub fn validate_canonical_bytes(canonical: Box<[u8]>) -> anyhow::Result<Self> {
        let decoded = tvm_block::Message::construct_from_bytes(&canonical)
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let expected = decoded.write_to_bytes().map_err(|error| anyhow::anyhow!("{error}"))?;
        anyhow::ensure!(expected.as_slice() == canonical.as_ref(), "Non-canonical message body");
        let message = Self::from_canonical_bytes(canonical);
        let _ = message.inner.decoded_tvm.set(decoded);
        Ok(message)
    }

    pub fn canonical_bytes(&self) -> &[u8] {
        self.inner.canonical.get_or_init(|| {
            self.inner
                .decoded_tvm
                .get()
                .expect("message must have canonical bytes or decoded TVM state")
                .write_to_bytes()
                .expect("validated TVM message must have a canonical BOC")
                .into_boxed_slice()
        })
    }

    pub fn canonical_size(&self) -> u64 {
        self.canonical_bytes().len().try_into().expect("message size must fit u64")
    }

    /// Borrow the cached TVM view. Trusted canonical input is decoded once
    /// across all clones; malformed trusted TVM state is an invariant violation.
    pub fn tvm_message(&self) -> tvm_types::Result<&tvm_block::Message> {
        if let Some(decoded) = self.inner.decoded_tvm.get() {
            return Ok(decoded);
        }
        let canonical = self
            .inner
            .canonical
            .get()
            .expect("message must have canonical bytes or decoded TVM state");
        if !canonical.starts_with(&[0xb5, 0xee, 0x9c, 0x72]) {
            return Err(tvm_types::error!("Can't decode non TVM message as TVM"));
        }
        Ok(self.inner.decoded_tvm.get_or_init(|| {
            tvm_block::Message::construct_from_bytes(canonical)
                .expect("authenticated TVM canonical bytes must decode to a Message")
        }))
    }
}

impl Debug for WrappedMessage {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}",
            self.tvm_message()
                .expect("validated TVM message")
                .hash()
                .expect("message must have hash")
                .to_hex_string()
        )
    }
}

// Preserve the previous wrapper's ordering for callers that compare messages.
// Queue order itself is defined by adjacent MessageIdentifier entries.
impl PartialOrd for WrappedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WrappedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        if Arc::ptr_eq(&self.inner, &other.inner) {
            return Ordering::Equal;
        }
        let left = self.tvm_message().expect("validated TVM message");
        let right = other.tvm_message().expect("validated TVM message");
        let hashes = || {
            left.hash()
                .expect("message must have hash")
                .cmp(&right.hash().expect("message must have hash"))
        };
        match (left.lt(), right.lt()) {
            (Some(a), Some(b)) => a.cmp(&b).then_with(hashes),
            _ => hashes(),
        }
    }
}

// Keep the old WrappedMessageData { data: Vec<u8> } envelope, also used by
// SerializedMessage in the adjacent message-archive branch.
#[derive(Serialize)]
struct MessageDataRef<'a> {
    data: &'a [u8],
}

#[derive(Deserialize)]
struct MessageData {
    data: Vec<u8>,
}

impl Serialize for WrappedMessage {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        MessageDataRef { data: self.canonical_bytes() }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WrappedMessage {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let data = MessageData::deserialize(deserializer)?;
        // Legacy readers accepted alternate valid BOC envelopes and emitted
        // the protocol writer's bytes on the next serialization. Normalize at
        // ingress once to preserve that behavior, then retain both views.
        let decoded = tvm_block::Message::construct_from_bytes(&data.data)
            .map_err(serde::de::Error::custom)?;
        let canonical = decoded.write_to_bytes().map_err(serde::de::Error::custom)?;
        let bytes = if canonical == data.data { data.data } else { canonical };
        let message = Self::from_canonical_bytes(bytes.into_boxed_slice());
        let _ = message.inner.decoded_tvm.set(decoded);
        Ok(message)
    }
}

impl Message for WrappedMessage {
    type AccountId = tvm_types::AccountId;

    fn destination(&self) -> Self::AccountId {
        self.tvm_message()
            .expect("validated TVM message")
            .int_dst_account_id()
            .unwrap_or(tvm_types::AccountId::from(UInt256::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Serialize;
    use tvm_block::GetRepresentationHash;
    use tvm_block::InternalMessageHeader;
    use tvm_block::Message;
    use tvm_block::Serializable;

    use super::WrappedMessage;
    use crate::message::identifier::MessageIdentifier;

    #[derive(Serialize)]
    struct LegacyMessageData {
        data: Vec<u8>,
    }

    fn message(lt: u64) -> Message {
        let header = InternalMessageHeader { created_lt: lt, ..Default::default() };
        Message::with_int_header(header)
    }

    #[test]
    fn canonical_body_and_identifier_match_native_tvm() {
        let tvm = message(123);
        let body = WrappedMessage::from_tvm(&tvm);
        let id = MessageIdentifier::from(&tvm);
        assert!(body.inner.canonical.get().is_none());
        assert_eq!(body.tvm_message().unwrap(), &tvm);
        assert_eq!(body.identifier(), id);
        let _ = format!("{body:?}");
        assert!(body.inner.canonical.get().is_none());
        assert_eq!(body.canonical_bytes(), tvm.write_to_bytes().unwrap());
        assert_eq!(body.canonical_size(), body.canonical_bytes().len() as u64);
        assert_eq!(id.inner().hash, tvm.hash().unwrap());
        assert_eq!(body.identifier(), id);
    }

    #[test]
    fn serde_matches_legacy_and_adjacent_serialized_message_layout() {
        let tvm = message(123);
        let legacy = LegacyMessageData { data: tvm.write_to_bytes().unwrap() };
        let body = WrappedMessage::from_tvm(&tvm);
        let wire = bincode::serialize(&legacy).unwrap();
        assert_eq!(bincode::serialize(&body).unwrap(), wire);
        assert_eq!(bincode::serialize(&Arc::new(body.clone())).unwrap(), wire);
        assert_eq!(serde_json::to_vec(&body).unwrap(), serde_json::to_vec(&legacy).unwrap());
        let restored: WrappedMessage = bincode::deserialize(&wire).unwrap();
        assert!(restored.inner.canonical.get().is_some());
        assert!(restored.inner.decoded_tvm.get().is_some());
        let canonical_ptr = restored.canonical_bytes().as_ptr();
        assert_eq!(restored, body);
        for _ in 0..3 {
            assert_eq!(bincode::serialize(&restored).unwrap(), wire);
            assert_eq!(restored.canonical_bytes().as_ptr(), canonical_ptr);
        }
    }

    #[test]
    fn canonical_ingress_and_clones_share_bytes_and_lazy_decoded_storage() {
        let canonical: Box<[u8]> = message(7).write_to_bytes().unwrap().into_boxed_slice();
        let ptr = canonical.as_ptr();
        let body = WrappedMessage::from_canonical_bytes(canonical);
        let clone = body.clone();
        assert!(Arc::ptr_eq(&body.inner, &clone.inner));
        assert!(body.inner.decoded_tvm.get().is_none());
        assert_eq!(body.canonical_bytes().as_ptr(), ptr);
        assert_eq!(clone.canonical_bytes().as_ptr(), ptr);
        assert_eq!(std::mem::size_of::<WrappedMessage>(), std::mem::size_of::<usize>());
        assert!(std::ptr::eq(body.tvm_message().unwrap(), clone.tvm_message().unwrap()));
        assert_eq!(body.canonical_bytes().as_ptr(), ptr);
    }

    #[test]
    fn lazy_views_initialize_once_across_concurrent_clones() {
        let tvm = message(9);
        let expected = tvm.write_to_bytes().unwrap();
        let decoded = WrappedMessage::from_tvm(&tvm);
        assert!(decoded.inner.canonical.get().is_none());
        let canonical = WrappedMessage::from_canonical_bytes(expected.clone().into_boxed_slice());
        assert!(canonical.inner.decoded_tvm.get().is_none());
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let decoded = decoded.clone();
                    let canonical = canonical.clone();
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        (
                            decoded.canonical_bytes().as_ptr() as usize,
                            canonical.tvm_message().unwrap() as *const Message as usize,
                        )
                    })
                })
                .collect();
            let mut handles = handles.into_iter();
            let expected_ptrs = handles.next().unwrap().join().unwrap();
            for handle in handles {
                assert_eq!(handle.join().unwrap(), expected_ptrs);
            }
        });
        assert_eq!(decoded.canonical_bytes(), expected);
        assert_eq!(canonical.tvm_message().unwrap(), &tvm);
    }

    #[test]
    #[should_panic(expected = "authenticated TVM canonical bytes must decode")]
    fn malformed_trusted_tvm_is_an_invariant_violation() {
        let message = WrappedMessage::from_canonical_bytes(vec![0xb5, 0xee, 0x9c, 0x72].into());
        let _ = message.tvm_message();
    }

    #[test]
    fn malformed_wire_body_returns_a_serde_error() {
        let wire = bincode::serialize(&LegacyMessageData { data: vec![0xff] }).unwrap();
        assert!(bincode::deserialize::<WrappedMessage>(&wire).is_err());
    }

    #[test]
    fn legacy_alternate_boc_is_normalized_once_at_ingress() {
        let tvm = message(7);
        let root = tvm.serialize().unwrap();
        let mut indexed = Vec::new();
        tvm_types::BocWriter::with_root(&root)
            .unwrap()
            .write_ex(&mut indexed, true, true, None, None)
            .unwrap();
        let canonical = tvm.write_to_bytes().unwrap();
        assert_ne!(indexed, canonical);
        let wire = bincode::serialize(&LegacyMessageData { data: indexed }).unwrap();
        let body: WrappedMessage = bincode::deserialize(&wire).unwrap();
        assert_eq!(body.canonical_bytes(), canonical);
        assert_eq!(body.tvm_message().unwrap(), &tvm);
        assert_eq!(
            bincode::serialize(&body).unwrap(),
            bincode::serialize(&LegacyMessageData { data: canonical }).unwrap()
        );
    }

    #[test]
    fn ordering_preserves_lt_then_hash() {
        let first = message(7);
        let second = message(8);
        assert!(WrappedMessage::from_tvm(&first) < WrappedMessage::from_tvm(&second));
        assert_eq!(WrappedMessage::from_tvm(&first), WrappedMessage::from_tvm(&first));
    }
}
