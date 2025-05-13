use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use telemetry_utils::now_ms;

const MAX_UNCOMPRESSED_SIZE: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetMessage {
    pub delivery_start_timestamp_ms: u64,
    pub id: String,
    pub label: String,
    pub compressed: bool,
    pub data: Arc<Vec<u8>>,
    #[serde(skip)]
    pub received_at: u64,
}

impl NetMessage {
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
        let label = format!("{:#?}", message);
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
        if compress_time + serialize_time > 50 || compress_time > 5 {
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
                received_at: u64::default(),
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
}
