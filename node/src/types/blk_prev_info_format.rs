use serde::de::Error as DeserializationError;
use serde::ser::Error as SerializationError;
use tvm_block::Deserializable;
use tvm_block::Serializable;

pub struct BlkPrevInfoFormat;

impl serde_with::SerializeAs<tvm_block::BlkPrevInfo> for BlkPrevInfoFormat {
    fn serialize_as<S>(value: &tvm_block::BlkPrevInfo, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buffer: Vec<u8> = value
            .write_to_bytes()
            .map_err(|e| <S as serde::Serializer>::Error::custom(format!("{e}, ")))?;
        <Vec<u8> as serde::Serialize>::serialize(&buffer, serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, tvm_block::BlkPrevInfo> for BlkPrevInfoFormat {
    fn deserialize_as<D>(deserializer: D) -> Result<tvm_block::BlkPrevInfo, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buffer: Vec<u8> = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        let block_prev_info = tvm_block::BlkPrevInfo::construct_from_bytes(&buffer)
            .map_err(|e| <D as serde::Deserializer>::Error::custom(format!("{e}")))?;
        Ok(block_prev_info)
    }
}
