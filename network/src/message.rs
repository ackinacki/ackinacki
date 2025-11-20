use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::now_ms;

const MAX_UNCOMPRESSED_SIZE: usize = 1000;

#[derive(Debug, Clone)]
pub struct NetMessage {
    pub delivery_start_timestamp_ms: u64,
    pub id: String,
    pub label: String,
    pub compressed: bool,
    pub data: Arc<Vec<u8>>,
    pub last_sender_is_proxy: bool,
    pub received_at: u64,
    pub direct_receiver: Option<String>,
}

#[derive(Deserialize)]
struct NetMessageV0 {
    delivery_start_timestamp_ms: u64,
    id: String,
    label: String,
    compressed: bool,
    data: Arc<Vec<u8>>,
    last_sender_is_proxy: bool,
}

#[derive(Deserialize)]
struct NetMessageV1 {
    delivery_start_timestamp_ms: u64,
    id: String,
    label: String,
    compressed: bool,
    data: Arc<Vec<u8>>,
    last_sender_is_proxy: bool,
    direct_receiver: Option<String>,
}

#[derive(Serialize)]
struct NetMessageV1Ref<'a> {
    delivery_start_timestamp_ms: u64,
    id: &'a String,
    label: &'a String,
    compressed: bool,
    data: &'a Arc<Vec<u8>>,
    last_sender_is_proxy: bool,
    direct_receiver: &'a Option<String>,
}

impl<'a> From<&'a NetMessage> for NetMessageV1Ref<'a> {
    fn from(value: &'a NetMessage) -> Self {
        Self {
            delivery_start_timestamp_ms: value.delivery_start_timestamp_ms,
            id: &value.id,
            label: &value.label,
            compressed: value.compressed,
            data: &value.data,
            last_sender_is_proxy: value.last_sender_is_proxy,
            direct_receiver: &value.direct_receiver,
        }
    }
}

impl NetMessage {
    const V1_TAG: [u8; 8] = [97, 99, 107, 110, 99, 107, 0, 1];

    pub fn serialize(&self) -> anyhow::Result<Vec<u8>> {
        let v1 = NetMessageV1Ref::from(self);
        let mut buf = bincode::serialize(&v1)?;
        buf.extend(Self::V1_TAG);
        Ok(buf)
    }

    pub fn deserialize(bytes: &[u8]) -> anyhow::Result<Self> {
        let message = if bytes.ends_with(&Self::V1_TAG) {
            let v1 = bincode::deserialize::<NetMessageV1>(bytes)?;
            Self {
                delivery_start_timestamp_ms: v1.delivery_start_timestamp_ms,
                id: v1.id,
                label: v1.label,
                compressed: v1.compressed,
                data: v1.data,
                last_sender_is_proxy: v1.last_sender_is_proxy,
                received_at: 0,
                direct_receiver: v1.direct_receiver,
            }
        } else {
            let v0 = bincode::deserialize::<NetMessageV0>(bytes)?;
            Self {
                delivery_start_timestamp_ms: v0.delivery_start_timestamp_ms,
                id: v0.id,
                label: v0.label,
                compressed: v0.compressed,
                data: v0.data,
                last_sender_is_proxy: v0.last_sender_is_proxy,
                received_at: 0,
                direct_receiver: None,
            }
        };
        Ok(message)
    }

    pub fn transfer_size(msg: &NetMessage) -> u64 {
        let v1 = NetMessageV1Ref::from(msg);
        bincode::serialized_size(&v1)
            .unwrap_or_else(|_| (8 + msg.id.len() + msg.label.len() + msg.data.len() + 1) as u64)
    }

    pub fn delivery_duration_ms(&self) -> Result<u64, String> {
        let now = now_ms();
        if now >= self.delivery_start_timestamp_ms {
            Ok(now - self.delivery_start_timestamp_ms)
        } else if self.delivery_start_timestamp_ms - now < 5 {
            // 5ms difference in clock synchronization is acceptable
            Ok(0)
        } else {
            Err("System clock out of sync. Please check NTP or system time settings.".to_string())
        }
    }

    pub fn encode<Message: Debug + Serialize>(message: &Message) -> anyhow::Result<(Self, usize)> {
        let label = format!("{message:#?}");
        let start = Instant::now();
        let mut data = match bincode::serialize(message) {
            Ok(data) => data,
            Err(err) => {
                anyhow::bail!("Failed to serialize {label}: {err}");
            }
        };
        let serialize_time = start.elapsed().as_millis();
        let start = Instant::now();
        let uncompressed_size = data.len();
        let compressed = uncompressed_size > MAX_UNCOMPRESSED_SIZE;
        if compressed {
            data = match zstd::encode_all(data.as_slice(), zstd::DEFAULT_COMPRESSION_LEVEL) {
                Ok(compressed) => compressed,
                Err(err) => {
                    anyhow::bail!("Failed to compress {label}: {err}");
                }
            };
        }
        let compress_time = start.elapsed().as_millis();
        let compressed_size = data.len();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let id = now.as_nanos().to_string();
        if compress_time + serialize_time > 50 || compress_time > 10 {
            // TODO: metrics
            tracing::warn!(
                msg_id = id,
                msg_type = label,
                size = data.len(),
                uncompressed_size = uncompressed_size,
                compressed_size = compressed_size,
                compression = (100.0 * compressed_size as f64 / uncompressed_size as f64).round(),
                compress_time = compress_time,
                serialize_time = serialize_time,
                "Too long message serialization time {}",
                compress_time + serialize_time
            );
        }
        Ok((
            Self {
                delivery_start_timestamp_ms: now.as_millis() as u64,
                id,
                data: Arc::new(data),
                label,
                compressed,
                last_sender_is_proxy: false,
                received_at: u64::default(),
                direct_receiver: None,
            },
            uncompressed_size,
        ))
    }

    pub fn decode<Message: DeserializeOwned>(&self) -> anyhow::Result<(Message, usize, usize)> {
        let start = Instant::now();
        let hold_decompressed: Vec<u8>;
        let compressed_size = self.data.len();
        let data = if self.compressed {
            match zstd::decode_all(self.data.as_slice()) {
                Ok(decompressed) => {
                    hold_decompressed = decompressed;
                    hold_decompressed.as_slice()
                }
                Err(err) => {
                    anyhow::bail!("Failed to decompress {}: {err}", self.label);
                }
            }
        } else {
            self.data.as_slice()
        };
        let decompressed_size = data.len();
        let decompress_time = start.elapsed().as_millis() as usize;
        let start = Instant::now();
        let message = match bincode::deserialize::<Message>(data) {
            Ok(message) => message,
            Err(err) => {
                anyhow::bail!("Error deserializing {}: {}", self.label, err);
            }
        };
        let deserialize_time = start.elapsed().as_millis() as usize;
        if decompress_time + deserialize_time > 100 {
            // TODO: metrics
            tracing::warn!(
                msg_id = self.id,
                msg_type = self.label,
                decompressed_size = decompressed_size,
                compressed_size = compressed_size,
                compression = (100.0 * compressed_size as f64 / decompressed_size as f64).round(),
                decompress_time = decompress_time,
                deserialize_time = deserialize_time,
                "Too long message deserialization time {}",
                decompress_time + deserialize_time
            );
        }
        Ok((message, decompress_time, deserialize_time))
    }

    pub fn direct_receiver_peer_id<PeerId: FromStr>(&self) -> Option<PeerId> {
        self.direct_receiver.as_ref().and_then(|x| x.parse().ok())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Deserialize;
    use serde::Serialize;

    use crate::message::NetMessage;

    #[derive(Serialize, Deserialize)]
    struct NetMessageV0 {
        pub delivery_start_timestamp_ms: u64,
        pub id: String,
        pub label: String,
        pub compressed: bool,
        pub data: Arc<Vec<u8>>,
        pub last_sender_is_proxy: bool,
        #[serde(skip)]
        pub received_at: u64,
    }

    #[test]
    fn test_net_message_backward_compatibility() {
        let original_v1 = NetMessage {
            delivery_start_timestamp_ms: 1,
            id: "id1".to_string(),
            label: "label1".to_string(),
            compressed: false,
            data: Arc::new(vec![1, 2, 3]),
            last_sender_is_proxy: false,
            received_at: 2,
            direct_receiver: Some("recipient1".to_string()),
        };

        let buf_v1 = original_v1.serialize().unwrap();
        let v0 = bincode::deserialize::<NetMessageV0>(&buf_v1).unwrap();
        assert_eq!(v0.delivery_start_timestamp_ms, original_v1.delivery_start_timestamp_ms);
        assert_eq!(v0.id, original_v1.id);
        assert_eq!(v0.label, original_v1.label);
        assert_eq!(v0.compressed, original_v1.compressed);
        assert_eq!(v0.data, original_v1.data);
        assert_eq!(v0.last_sender_is_proxy, original_v1.last_sender_is_proxy);
        assert_eq!(0, v0.received_at);

        let v1 = NetMessage::deserialize(&buf_v1).unwrap();
        assert_eq!(v1.delivery_start_timestamp_ms, original_v1.delivery_start_timestamp_ms);
        assert_eq!(v1.id, original_v1.id);
        assert_eq!(v1.label, original_v1.label);
        assert_eq!(v1.compressed, original_v1.compressed);
        assert_eq!(v1.data, original_v1.data);
        assert_eq!(v1.last_sender_is_proxy, original_v1.last_sender_is_proxy);
        assert_eq!(0, v1.received_at);
        assert_eq!(v1.direct_receiver, original_v1.direct_receiver);

        let buf_v0 = bincode::serialize(&v0).unwrap();
        let v0 = bincode::deserialize::<NetMessageV0>(&buf_v0).unwrap();
        assert_eq!(v0.delivery_start_timestamp_ms, original_v1.delivery_start_timestamp_ms);
        assert_eq!(v0.id, original_v1.id);
        assert_eq!(v0.label, original_v1.label);
        assert_eq!(v0.compressed, original_v1.compressed);
        assert_eq!(v0.data, original_v1.data);
        assert_eq!(v0.last_sender_is_proxy, original_v1.last_sender_is_proxy);
        assert_eq!(0, v0.received_at);

        let v1 = NetMessage::deserialize(&buf_v0).unwrap();
        assert_eq!(v1.delivery_start_timestamp_ms, original_v1.delivery_start_timestamp_ms);
        assert_eq!(v1.id, original_v1.id);
        assert_eq!(v1.label, original_v1.label);
        assert_eq!(v1.compressed, original_v1.compressed);
        assert_eq!(v1.data, original_v1.data);
        assert_eq!(v1.last_sender_is_proxy, original_v1.last_sender_is_proxy);
        assert_eq!(0, v1.received_at);
        assert!(v1.direct_receiver.is_none());
    }
}
