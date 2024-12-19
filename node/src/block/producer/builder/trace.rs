// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use tvm_vm::executor::Engine;
use tvm_vm::executor::EngineTraceInfo;
use tvm_vm::executor::EngineTraceInfoType;
use tvm_vm::stack::Stack;
use tvm_vm::stack::StackItem;

use super::EngineTraceInfoData;

impl From<&EngineTraceInfo<'_>> for EngineTraceInfoData {
    fn from(info: &EngineTraceInfo) -> Self {
        let cmd_code_rem_bits = info.cmd_code.remaining_bits() as u32;
        let cmd_code_hex = info.cmd_code.to_hex_string();
        let cmd_code_cell_hash = info.cmd_code.cell().repr_hash().to_hex_string();
        let cmd_code_offset = info.cmd_code.pos() as u32;

        Self {
            info_type: format!("{:#?}", info.info_type),
            step: info.step,
            cmd_str: info.cmd_str.clone(),
            stack: info.stack.storage.iter().map(|s| s.to_string()).collect(),
            gas_used: info.gas_used.to_string(),
            gas_cmd: info.gas_cmd.to_string(),
            cmd_code_rem_bits,
            cmd_code_hex,
            cmd_code_cell_hash,
            cmd_code_offset,
        }
    }
}

#[allow(dead_code)]
pub fn simple_trace_callback(engine: &Engine, info: &EngineTraceInfo) {
    if info.info_type == EngineTraceInfoType::Dump {
        tracing::info!(target: "tvm_op", "{}", info.cmd_str);
    } else if info.info_type == EngineTraceInfoType::Start {
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            tracing::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            tracing::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            tracing::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    } else if info.info_type == EngineTraceInfoType::Exception {
        if engine.trace_bit(Engine::TRACE_CODE) {
            tracing::info!(target: "tvm", "{} ({}) BAD_CODE: {}\n", info.step, info.gas_cmd, info.cmd_str);
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            tracing::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            tracing::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            tracing::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    } else if info.has_cmd() {
        if engine.trace_bit(Engine::TRACE_CODE) {
            tracing::info!(target: "tvm", "{}\n", info.cmd_str);
        }
        if engine.trace_bit(Engine::TRACE_STACK) {
            tracing::info!(target: "tvm", " [ {} ] \n", dump_stack_result(info.stack));
        }
        if engine.trace_bit(Engine::TRACE_CTRLS) {
            tracing::trace!(target: "tvm", "{}", engine.dump_ctrls(true));
        }
        if engine.trace_bit(Engine::TRACE_GAS) {
            tracing::info!(target: "tvm", "gas - {}\n", info.gas_used);
        }
    }
}

#[allow(dead_code)]
fn dump_stack_result(stack: &Stack) -> String {
    // lazy_static::lazy_static!(
    //     static ref PREV_STACK: Mutex<Stack> = Mutex::new(Stack::new());
    // );
    // let mut prev_stack = PREV_STACK.lock().unwrap();
    let mut result = String::new();
    // let mut iter = prev_stack.iter();
    // let mut same = false;
    for item in stack.iter() {
        // if let Some(prev) = iter.next() {
        //     if prev == item {
        //         same = true;
        //         continue;
        //     }
        //     while iter.next().is_some() {}
        // }
        // if same {
        //     same = false;
        //     result = "--\"-- ".to_string();
        // }
        let string = match item {
            StackItem::None => "N".to_string(),
            StackItem::Integer(data) => match data.bitsize() {
                Ok(0..=230) => data.to_string(),
                Ok(l) if l > 230 => format!("0x{}", data.to_str_radix(16)),
                Ok(bitsize) => format!("I{}", bitsize),
                Err(err) => err.to_string(),
            },
            StackItem::Cell(data) => {
                format!("C{}-{}", data.bit_length(), data.references_count())
            }
            StackItem::Continuation(data) => format!("T{}", data.code().remaining_bits() / 8),
            StackItem::Builder(data) => {
                format!("B{}-{}", data.length_in_bits(), data.references().len())
            }
            StackItem::Slice(data) => {
                format!("S{}-{}", data.remaining_bits(), data.remaining_references())
            }
            StackItem::Tuple(data) => match data.len() {
                0 => "[]".to_string(),
                len => format!("[@{}]", len),
            },
        };
        result += &string;
        result += " ";
    }
    // *prev_stack = stack.clone();
    result
}
