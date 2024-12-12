// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use serde::de::Error as DeserializationError;
use serde::ser::Error as SerializationError;

pub struct CellFormat;

impl serde_with::SerializeAs<tvm_types::Cell> for CellFormat {
    fn serialize_as<S>(value: &tvm_types::Cell, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let buffer: Vec<u8> = tvm_types::write_boc(value)
            .map_err(|e| <S as serde::Serializer>::Error::custom(format!("{}, ", e)))?;
        <Vec<u8> as serde::Serialize>::serialize(&buffer, serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, tvm_types::Cell> for CellFormat {
    fn deserialize_as<D>(deserializer: D) -> Result<tvm_types::Cell, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buffer: Vec<u8> = <Vec<u8> as serde::Deserialize>::deserialize(deserializer)?;
        let cell = tvm_types::read_single_root_boc(&buffer)
            .map_err(|e| <D as serde::Deserializer>::Error::custom(format!("{}", e)))?;
        Ok(cell)
    }
}
