use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use ed25519_dalek::SigningKey;
use serde_json::Value;
use tvm_abi::token::Detokenizer;
use tvm_abi::token::Tokenizer;
use tvm_block::MsgAddressInt;
use tvm_client::abi::DataLayout;
use tvm_client::abi::FunctionHeader;
use tvm_client::abi::MessageBodyType;
use tvm_client::encoding::decode_abi_number;
use tvm_types::Ed25519PrivateKey;
use tvm_types::SliceData;

use crate::contract::TvmContract;

static EMPTY_OBJECT: LazyLock<Value> = LazyLock::new(|| Value::Object(serde_json::Map::new()));

impl TvmContract {
    pub fn encode_function_call(
        &self,
        function: &str,
        header: Option<&Value>,
        parameters: Option<&Value>,
        internal: bool,
        sign_key: Option<&Ed25519PrivateKey>,
        address: Option<MsgAddressInt>,
    ) -> anyhow::Result<SliceData> {
        let function = self
            .abi
            .function(function)
            .map_err(|err| anyhow::anyhow!("Failed to encode function call: {err}"))?;

        let header = self.resolve_function_header(header, sign_key)?;
        let header_tokens = if let Some(header) = header {
            Tokenizer::tokenize_optional_params(function.header_params(), &header)
                .map_err(|err| anyhow::anyhow!("Failed to tokenize params: {err}"))?
        } else {
            HashMap::new()
        };
        let input_tokens = Tokenizer::tokenize_all_params(
            function.input_params(),
            parameters.unwrap_or_else(|| &EMPTY_OBJECT),
        )
        .map_err(|err| anyhow::anyhow!("Failed to tokenize params: {err}"))?;
        SliceData::load_builder(
            function
                .encode_input(&header_tokens, &input_tokens, internal, sign_key, address)
                .map_err(|err| anyhow::anyhow!("Failed to encode function call: {err}"))?,
        )
        .map_err(|err| anyhow::anyhow!("Failed to load builder: {err}"))
    }

    pub fn decode_function_body(
        &self,
        body: SliceData,
        is_internal: bool,
        allow_partial: bool,
        function_name: &str,
        data_layout: Option<DataLayout>,
    ) -> anyhow::Result<(MessageBodyType, Value, Option<FunctionHeader>)> {
        let variant = self.find_abi_function(function_name)?;
        let (msg_type, tokens, header) = match variant {
            AbiFunctionVariant::Function(function) => {
                let decode_output = || {
                    function
                        .decode_output(body.clone(), is_internal, allow_partial)
                        .map(|x| (MessageBodyType::Output, x, None))
                        .map_err(|err| anyhow::anyhow!("Invalid message for decode: {err}"))
                };
                let decode_input = || {
                    let decoded =
                        function
                            .decode_input(body.clone(), is_internal, allow_partial)
                            .map_err(|err| anyhow::anyhow!("Invalid message for decode: {err}"))?;
                    let (header, _, _) = tvm_abi::Function::decode_header(
                        self.abi.version(),
                        body.clone(),
                        self.abi.header(),
                        is_internal,
                    )
                    .map_err(|err| anyhow::anyhow!("Invalid message for decode: {err}"))?;
                    Ok((MessageBodyType::Input, decoded, FunctionHeader::from(&header)?))
                };

                match data_layout {
                    Some(DataLayout::Input) => decode_input(),
                    Some(DataLayout::Output) => decode_output(),
                    None => decode_output(),
                }
            }
            AbiFunctionVariant::Event(event) => {
                if is_internal {
                    return Err(anyhow::anyhow!(
                        "ABI event can be produced only in external outbound message",
                    ));
                }
                let decoded = event
                    .decode_input(body, allow_partial)
                    .map_err(|err| anyhow::anyhow!("Invalid message for decode: {err}"))?;
                Ok((MessageBodyType::Event, decoded, None))
            }
        }?;
        let decoded = Detokenizer::detokenize_to_json_value(&tokens)
            .map_err(|err| anyhow::anyhow!("Failed to detokenize value: {err}"))?;
        Ok((msg_type, decoded, header))
    }

    fn find_abi_function(&self, name: &str) -> anyhow::Result<AbiFunctionVariant<'_>> {
        if let Ok(function) = self.abi.function(name) {
            Ok(AbiFunctionVariant::Function(function))
        } else if let Ok(event) = self.abi.event(name) {
            Ok(AbiFunctionVariant::Event(event))
        } else {
            let function_id: u32 = decode_abi_number(name)?;
            if let Ok(function) = self
                .abi
                .function_by_id(function_id, true)
                .or_else(|_| self.abi.function_by_id(function_id, true))
            {
                Ok(AbiFunctionVariant::Function(function))
            } else if let Ok(event) = self.abi.event_by_id(function_id) {
                Ok(AbiFunctionVariant::Event(event))
            } else {
                Err(anyhow::anyhow!("Invalid function name: {name}"))
            }
        }
    }

    fn resolve_function_header(
        &self,
        header: Option<&Value>,
        sign_key: Option<&Ed25519PrivateKey>,
    ) -> anyhow::Result<Option<Value>> {
        let now =
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let required = |name: &str| self.abi.header().iter().any(|x| x.name == name);
        let mut updated_header = serde_json::Map::new();
        let mut should_set_time = required("time");
        let mut should_set_expire = required("expire");
        let mut should_set_pubkey = required("pubkey");
        if let Some(Value::Object(obj)) = &header {
            for (name, value) in obj {
                if name == "time" {
                    should_set_time = false;
                }
                if name == "expire" {
                    should_set_expire = false;
                }
                if name == "pubkey" {
                    should_set_pubkey = false;
                }
                updated_header.insert(name.clone(), value.clone());
            }
        }
        if should_set_time {
            updated_header.insert("time".to_string(), Value::Number(now.into()));
        }
        if should_set_expire {
            updated_header.insert("expire".to_string(), Value::Number((now / 1000 + 5u64).into()));
        }
        if should_set_pubkey {
            if let Some(sign_key) = sign_key {
                let pubkey =
                    hex::encode(SigningKey::from(sign_key.as_bytes()).verifying_key().as_bytes());
                updated_header.insert("pubkey".to_string(), Value::String(pubkey));
            }
        }
        Ok(if !updated_header.is_empty() { Some(Value::Object(updated_header)) } else { None })
    }
}

enum AbiFunctionVariant<'a> {
    Function(&'a tvm_abi::Function),
    Event(&'a tvm_abi::Event),
}
