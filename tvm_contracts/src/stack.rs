use std::ops::Deref;

use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use serde_json::Value;
use tvm_client::boc::internal::serialize_cell_to_base64;
use tvm_vm::stack::integer::IntegerData;
use tvm_vm::stack::StackItem;

enum ProcessingResult<'a> {
    Serialized(Value),
    Nested(Box<dyn Iterator<Item = &'a StackItem> + 'a>),
    // LevelUp,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum ComplexType {
    List(Vec<Value>),
    Cell(String),
    Builder(String),
    Slice(String),
    Continuation(String),
}

fn is_equal_type(left: &Value, right: &Value) -> bool {
    left["type"] == right["type"]
        && left.is_array() == right.is_array()
        && left.is_string() == right.is_string()
}

pub fn serialize_items<'a>(
    items: Box<dyn Iterator<Item = &'a StackItem> + 'a>,
    flatten_lists: bool,
) -> anyhow::Result<Value> {
    let mut stack = vec![(vec![], items)];
    let mut list_items: Option<Vec<Value>> = None;
    loop {
        let (mut vec, mut iter) = stack.pop().unwrap();
        let next = iter.next();
        if let Some(list) = list_items.take() {
            // list is ended if current tuple has next element
            // or it already contains more than one element
            // or element type in current tuple is not equal to list items type
            if next.is_some() || vec.len() != 1 || !is_equal_type(&vec[0], &list[0]) {
                vec.push(json!(ComplexType::List(list)));
            } else {
                list_items = Some(list);
            }
        }

        if let Some(item) = next {
            match process_item(item)? {
                ProcessingResult::Serialized(value) => {
                    vec.push(value);
                    stack.push((vec, iter));
                }
                ProcessingResult::Nested(nested_iter) => {
                    stack.push((vec, iter));
                    stack.push((vec![], nested_iter));
                }
            }
        } else if let Some((parent_vec, _)) = stack.last_mut() {
            // list starts from tuple with 2 elements: some value and null,
            // the value becomes the last list item
            if vec.len() == 2 && vec[1] == Value::Null && flatten_lists {
                vec.resize(1, Value::Null);
                list_items = Some(vec);
            } else if let Some(list) = list_items.take() {
                vec.extend(list);
                list_items = Some(vec);
            } else {
                parent_vec.push(Value::Array(vec));
            }
        } else {
            return Ok(Value::Array(vec));
        }
    }
}

fn serialize_integer_data(data: &IntegerData) -> String {
    let hex = data.to_str_radix(16);
    // all negative numbers and positive numbers less than u128::MAX are encoded as
    // decimal
    if hex.starts_with('-') || hex.len() <= 32 {
        data.to_str_radix(10)
    } else {
        // positive numbers between u128::MAX and u256::MAX are padded to 64 hex symbols
        if hex.len() <= 64 {
            format!("0x{hex:0>64}")
        } else {
            // positive numbers between u256::MAX and u512::MAX are padded to 128 symbols
            // positive numbers above u512::MAX are not padded
            format!("0x{hex:0>128}")
        }
    }
}

pub fn serialize_item(item: &StackItem) -> anyhow::Result<Value> {
    Ok(serialize_items(Box::new(vec![item].into_iter()), false)?[0].take())
}

fn process_item(item: &StackItem) -> anyhow::Result<ProcessingResult<'_>> {
    Ok(match item {
        StackItem::None => ProcessingResult::Serialized(Value::Null),
        StackItem::Integer(value) => {
            ProcessingResult::Serialized(Value::String(serialize_integer_data(value)))
        }
        StackItem::Tuple(items) => ProcessingResult::Nested(Box::new(items.iter())),
        StackItem::Builder(value) => {
            ProcessingResult::Serialized(json!(ComplexType::Builder(serialize_cell_to_base64(
                &value
                    .deref()
                    .clone()
                    .into_cell()
                    .map_err(|err| anyhow::anyhow!("Can not parse object: {err}"))?,
                "stack item `Builder`"
            )?)))
        }
        StackItem::Slice(value) => ProcessingResult::Serialized(json!(ComplexType::Slice(
            serialize_cell_to_base64(&value.clone().into_cell(), "stack item `Slice`")?
        ))),
        StackItem::Cell(value) => ProcessingResult::Serialized(json!(ComplexType::Cell(
            serialize_cell_to_base64(value, "stack item `Cell`")?
        ))),
        StackItem::Continuation(value) => ProcessingResult::Serialized(json!(
            ComplexType::Continuation(serialize_cell_to_base64(
                &value.code().clone().into_cell(),
                "stack item `Continuation`"
            )?)
        )),
    })
}
