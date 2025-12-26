use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde_json::Value;
use tvm_block::Account;
use tvm_block::CommonMsgInfo;
use tvm_block::ConfigParams;
use tvm_block::CurrencyCollection;
use tvm_block::Deserializable;
use tvm_block::ExternalInboundMessageHeader;
use tvm_block::Message;
use tvm_block::MsgAddressExt;
use tvm_block::MsgAddressInt;
use tvm_block::OutAction;
use tvm_block::OutActions;
use tvm_block::Serializable;
use tvm_client::encoding::slice_from_cell;
use tvm_executor::BlockchainConfig;
use tvm_types::Cell;
use tvm_types::HashmapType;
use tvm_types::SliceData;
use tvm_types::UInt256;
use tvm_vm::executor::gas::gas_state::Gas;
use tvm_vm::executor::BehaviorModifiers;
use tvm_vm::executor::Engine;
use tvm_vm::stack::integer::IntegerData;
use tvm_vm::stack::savelist::SaveList;
use tvm_vm::stack::Stack;
use tvm_vm::stack::StackItem;

use crate::stack;
use crate::TvmContract;

pub struct TvmExecutionOptions {
    pub blockchain_config: Arc<BlockchainConfig>,
    pub signature_id: i32,
    pub block_time: u32,
    pub block_lt: u64,
    pub transaction_lt: u64,
    pub behavior_modifiers: BehaviorModifiers,
}

impl Default for TvmExecutionOptions {
    fn default() -> Self {
        Self {
            transaction_lt: 1,
            block_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
                as u32,
            block_lt: 1,
            signature_id: 0,
            behavior_modifiers: BehaviorModifiers::default(),
            blockchain_config: Arc::new(BlockchainConfig::default()),
        }
    }
}

impl TvmContract {
    pub fn run_get(
        &self,
        account: &Account,
        function_name: &str,
        args: Option<&Value>,
    ) -> anyhow::Result<Value> {
        let dst =
            account.get_addr().ok_or_else(|| anyhow::anyhow!("Account has no address"))?.clone();
        let msg_body = self.encode_function_call(function_name, None, args, false, None, None)?;
        let msg_header = ExternalInboundMessageHeader {
            src: MsgAddressExt::with_extern(SliceData::from_raw(vec![0x55; 8], 64)).unwrap(),
            dst,
            import_fee: 0x1234u64.into(), // Taken from TVM-linker
        };
        let mut msg = Message::with_ext_in_header(msg_header);
        msg.set_body(msg_body);
        let options = TvmExecutionOptions { ..Default::default() };

        let (mut out_messages, _) = tvm_call_msg(account, false, &options, &msg)?;

        let decoded = self
            .decode_function_body(
                out_messages
                    .pop()
                    .ok_or_else(|| anyhow::anyhow!("Out message has no body"))?
                    .body()
                    .ok_or_else(|| anyhow::anyhow!("Out message has no body"))?,
                false,
                false,
                function_name,
                None,
            )?
            .1;

        Ok(decoded)
    }
}

pub fn tvm_get(
    account: &Account,
    function_name: &str,
    args: Vec<StackItem>,
) -> anyhow::Result<Vec<StackItem>> {
    let crc = tvm_crc16(function_name.as_bytes());
    let function_id = ((crc as u32) & 0xffff) | 0x10000;
    let mut stack_in = Stack::new();
    for arg in args {
        stack_in.push(arg);
    }

    stack_in.push(StackItem::Integer(Arc::new(IntegerData::from_u32(function_id))));

    let options = TvmExecutionOptions::default();
    let (engine, _) = tvm_call(&options, account, false, stack_in)?;
    Ok(engine.stack().storage.clone())
}

pub fn tvm_call_msg(
    account: &Account,
    return_updated_account: bool,
    options: &TvmExecutionOptions,
    msg: &Message,
) -> anyhow::Result<(Vec<Message>, Option<Account>)> {
    let msg_cell =
        msg.serialize().map_err(|err| anyhow::anyhow!("can not serialize message: {err}"))?;

    let mut stack = Stack::new();
    let balance = account.balance().map_or(0, |cc| cc.grams.as_u128());
    let function_selector = match msg.header() {
        CommonMsgInfo::IntMsgInfo(_) => tvm_vm::int!(0),
        CommonMsgInfo::ExtInMsgInfo(_) => tvm_vm::int!(-1),
        CommonMsgInfo::ExtOutMsgInfo(_) => return Err(anyhow::anyhow!("invalid message type")),
    };
    stack
        .push(tvm_vm::int!(balance)) // token balance of contract
        .push(tvm_vm::int!(0)) // token balance of msg
        .push(StackItem::Cell(msg_cell)) // message
        .push(StackItem::Slice(msg.body().unwrap_or_default())) // message body
        .push(function_selector); // function selector

    let (engine, updated_account) = tvm_call(options, account, return_updated_account, stack)?;

    // process out actions to get out messages
    let actions_cell = engine
        .get_actions()
        .as_cell()
        .map_err(|err| anyhow::anyhow!("can not get actions: {err}"))?
        .clone();
    let mut actions = OutActions::construct_from_cell(actions_cell)
        .map_err(|err| anyhow::anyhow!("can not parse actions: {err}"))?;

    let mut msgs = vec![];
    for action in actions.iter_mut() {
        if let OutAction::SendMsg { out_msg, .. } = std::mem::replace(action, OutAction::None) {
            msgs.push(out_msg);
        }
    }

    msgs.reverse();
    Ok((msgs, updated_account))
}

pub fn tvm_call(
    options: &TvmExecutionOptions,
    account: &Account,
    return_updated_account: bool,
    stack: Stack,
) -> anyhow::Result<(Engine, Option<Account>)> {
    let code = account.get_code().unwrap_or_default();
    let data = account.get_data().ok_or_else(|| anyhow::anyhow!("Account has no code"))?;
    let addr = account.get_addr().ok_or_else(|| anyhow::anyhow!("Account has no address"))?;
    let balance = account.balance().ok_or_else(|| anyhow::anyhow!("Account has no balance"))?;

    let mut ctrls = SaveList::new();
    ctrls
        .put(4, &mut StackItem::Cell(data))
        .map_err(|err| anyhow::anyhow!("can not put data to registers: {err}"))?;

    let mut sci = build_contract_info(
        options.blockchain_config.raw_config(),
        addr,
        balance,
        options.block_time,
        options.block_lt,
        options.transaction_lt,
        code.clone(),
        account.init_code_hash(),
    );
    sci.capabilities = options.blockchain_config.capabilites();
    ctrls
        .put(7, &mut sci.into_temp_data_item())
        .map_err(|err| anyhow::anyhow!("can not put SCI to registers: {err}"))?;

    let gas_limit = 1_000_000_000;
    let gas = Gas::new(gas_limit, 0, gas_limit, 10);

    let mut engine = Engine::with_capabilities(options.blockchain_config.capabilites()).setup(
        slice_from_cell(code)?,
        Some(ctrls),
        Some(stack),
        Some(gas),
    );

    engine.set_signature_id(options.signature_id);
    engine.modify_behavior(options.behavior_modifiers.clone());

    match engine.execute() {
        Err(err) => {
            let exception = tvm_vm::error::tvm_exception(err)
                .map_err(|err| anyhow::anyhow!("Unknown execution error: {err}"))?;
            let code = if let Some(code) = exception.custom_code() {
                code
            } else {
                !(exception.exception_code().unwrap_or(tvm_types::ExceptionCode::UnknownError)
                    as i32)
            };

            let exit_arg = stack::serialize_item(&exception.value)?;
            Err(anyhow::anyhow!("TVM execution failed: {exception} with code {code} and exit arg {exit_arg} at address {addr}"))
        }
        Ok(_) => match engine.get_committed_state().get_root() {
            StackItem::Cell(data) => {
                if return_updated_account {
                    let mut account = Account::new();
                    account.set_data(data.clone());
                    Ok((engine, Some(account)))
                } else {
                    Ok((engine, None))
                }
            }
            _ => Err(anyhow::anyhow!("invalid committed state")),
        },
    }
}

fn tvm_crc16(data: &[u8]) -> u16 {
    crc::Crc::<u16>::new(&crc::CRC_16_XMODEM).checksum(data)
}

#[allow(clippy::too_many_arguments)]
fn build_contract_info(
    config_params: &ConfigParams,
    address: &MsgAddressInt,
    balance: &CurrencyCollection,
    block_unixtime: u32,
    block_lt: u64,
    tr_lt: u64,
    code: Cell,
    init_code_hash: Option<&UInt256>,
) -> tvm_vm::SmartContractInfo {
    let mut info = tvm_vm::SmartContractInfo::with_myself(
        address.serialize().and_then(SliceData::load_cell).unwrap_or_default(),
    );
    info.block_lt = block_lt;
    info.trans_lt = tr_lt;
    info.unix_time = block_unixtime;
    info.balance = balance.clone();
    if let Some(data) = config_params.config_params.data() {
        info.config_params = Some(data.clone());
    }
    if let Some(hash) = init_code_hash {
        info.set_init_code_hash(hash.clone());
    }
    info.set_mycode(code);
    info
}
