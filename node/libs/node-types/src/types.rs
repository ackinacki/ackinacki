use serde::Deserialize;
use serde::Serialize;
use serde_with::serde_as;

use crate::u256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DAppIdentifierPath {
    pub prefix: DAppIdentifier,
    pub len: u8,
}

u256!(AccountIdentifier, "AccountIdentifier", ser = array);

impl AccountIdentifier {
    pub fn routing_with(self, dapp_id: DAppIdentifier) -> AccountRouting {
        AccountRouting::new(dapp_id, self)
    }

    pub fn dapp_originator(self) -> AccountRouting {
        AccountRouting::new(DAppIdentifier(self.0), self)
    }

    pub fn optional_dapp_originator(self, dapp: Option<DAppIdentifier>) -> AccountRouting {
        AccountRouting::new(dapp.unwrap_or(DAppIdentifier(self.0)), self)
    }

    pub fn use_as_dapp_id(&self) -> DAppIdentifier {
        DAppIdentifier(self.0)
    }
}

impl From<tvm_types::AccountId> for AccountIdentifier {
    fn from(value: tvm_types::AccountId) -> Self {
        let value_bytes = value.get_bytestring(0);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&value_bytes[..32]);
        Self(bytes)
    }
}

impl From<&tvm_types::AccountId> for AccountIdentifier {
    fn from(value: &tvm_types::AccountId) -> Self {
        let bytes = value.get_bytestring(0);
        let mut u256 = [0u8; 32];
        u256.copy_from_slice(&bytes[..32]);
        Self(u256)
    }
}

impl From<AccountIdentifier> for tvm_types::AccountId {
    fn from(value: AccountIdentifier) -> Self {
        Self::from(value.0)
    }
}

impl From<&AccountIdentifier> for tvm_types::AccountId {
    fn from(value: &AccountIdentifier) -> Self {
        Self::from(value.0)
    }
}

u256!(AccountHash, "AccountHash", ser = array);
u256!(AccountDataHash, "AccountDataHash", ser = array);
u256!(AccountCodeHash, "AccountCodeHash", ser = array);
u256!(DAppIdentifier, "DAppIdentifier", ser = array);
u256!(ThreadAccountsHash, "ThreadAccountsHash", ser = array);
u256!(TransactionHash, "TransactionHash", ser = array);
u256!(BlockIdentifier, "BlockIdentifier", ser = bytes);

impl BlockIdentifier {
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }

    // Note: the compare method is used in fork choice
    pub fn compare(a: &Self, b: &Self) -> std::cmp::Ordering {
        for i in 0..32 {
            match a.0[i].cmp(&b.0[i]) {
                std::cmp::Ordering::Less => {
                    return std::cmp::Ordering::Less;
                }
                std::cmp::Ordering::Greater => {
                    return std::cmp::Ordering::Greater;
                }
                _ => {}
            }
        }
        std::cmp::Ordering::Equal
    }

    pub fn as_rng_seed(&self) -> [u8; 32] {
        self.0
    }
}

impl From<&BlockIdentifier> for monotree::Hash {
    fn from(block_id: &BlockIdentifier) -> Self {
        block_id.0
    }
}

#[derive(
    Copy, Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd, Default, Serialize, Deserialize,
)]
pub struct AccountRouting {
    dapp_id: DAppIdentifier,
    account_id: AccountIdentifier,
}

impl AccountRouting {
    pub fn new(dapp_id: DAppIdentifier, account_id: AccountIdentifier) -> Self {
        Self { dapp_id, account_id }
    }

    pub fn is_dapp_originator(&self) -> bool {
        self.account_id.0 == self.dapp_id.0
    }

    pub fn dapp_id(&self) -> &DAppIdentifier {
        &self.dapp_id
    }

    pub fn set_dapp_id(&mut self, dapp_id: DAppIdentifier) {
        self.dapp_id = dapp_id;
    }

    pub fn account_id(&self) -> &AccountIdentifier {
        &self.account_id
    }

    pub fn get_bit(&self, index: usize) -> bool {
        if index < 256 {
            self.dapp_id.get_bit(index)
        } else {
            self.account_id.get_bit(index - 256)
        }
    }

    pub fn set_bit(&mut self, index: usize) {
        if index < 256 {
            self.dapp_id.set_bit(index);
        } else {
            self.account_id.set_bit(index - 256);
        }
    }
}

impl AccountRouting {
    pub fn to_hex_string(&self) -> String {
        format!("{}{}", hex::encode(self.dapp_id.0), hex::encode(self.account_id.0))
    }
}

impl std::fmt::Display for AccountRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.dapp_id.to_hex_string(), self.account_id.to_hex_string())
    }
}

impl std::str::FromStr for AccountRouting {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((dapp, account)) = s.split_once("::") {
            let account_id = AccountIdentifier::from_str(account)?;
            let dapp_id = if !dapp.is_empty() {
                DAppIdentifier::from_str(dapp)?
            } else {
                account_id.use_as_dapp_id()
            };
            Ok(Self::new(dapp_id, account_id))
        } else if s.len() == 128 && s.chars().all(|c| c.is_ascii_hexdigit()) {
            let (dapp, account) = s.split_at(64);
            let dapp_id = DAppIdentifier::from_str(dapp)?;
            let account_id = AccountIdentifier::from_str(account)?;
            Ok(Self::new(dapp_id, account_id))
        } else {
            Err(anyhow::anyhow!("Invalid account routing [{s}]"))
        }
    }
}

impl std::ops::BitAnd for &'_ AccountRouting {
    type Output = AccountRouting;

    fn bitand(self, rhs: Self) -> Self::Output {
        (&self.account_id & &rhs.account_id).routing_with(&self.dapp_id & &rhs.dapp_id)
    }
}

impl From<AccountRouting> for [[bool; 256]; 2] {
    fn from(val: AccountRouting) -> Self {
        let mut result = [[false; 256]; 2];
        let parts: [[u8; 32]; 2] = [val.dapp_id.0, val.account_id.0];
        for outer in 0..parts.len() {
            for inner in 0..parts[outer].len() {
                for shift in 0..8 {
                    let bits = 1 << shift;
                    result[outer][inner * 8 + (7 - shift)] = ((parts[outer][inner]) & bits) != 0;
                }
            }
        }
        result
    }
}

impl From<AccountRouting> for tvm_types::AccountId {
    fn from(value: AccountRouting) -> Self {
        Self::from(value.account_id())
    }
}

impl From<&AccountRouting> for tvm_types::AccountId {
    fn from(value: &AccountRouting) -> Self {
        Self::from(value.account_id())
    }
}

// Note:
// It must be possible to uniquely generate new thread id from any thread without
// collisions. Therefore, the u16 as an underlying type for thread identifier was changed.
// The new underlying type for the identifier is a block id and an u16. It will be generated
// by the block producer by taking the "current" block id after which the thread
// must be spawned and adding some local index in case of multiple threads have to
// be spawned simultaneously.

#[serde_as]
#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct ThreadIdentifier(#[serde_as(as = "serde_with::Bytes")] [u8; 34]);

impl Default for ThreadIdentifier {
    fn default() -> Self {
        Self([0; 34])
    }
}

impl ThreadIdentifier {
    pub fn new(block_id: &BlockIdentifier, id: u16) -> Self {
        let mut res = [0; 34];
        res[0] = ((id >> 8) & 0xFF) as u8;
        res[1] = (id & 0xFF) as u8;
        res[2..34].copy_from_slice(&block_id.0);
        Self(res)
    }

    // Note: not the best solution to have it. Yet it is a simple quick to implement
    // solution. Seems harmless to have.
    /// Checks if a particular block was the one where the thread was spawned.
    pub fn is_spawning_block(&self, block_id: &BlockIdentifier) -> bool {
        self.0[2..34] == block_id.0
    }

    pub fn spawning_block_id(&self) -> BlockIdentifier {
        let mut block_id_bytes = [0u8; 32];
        block_id_bytes.copy_from_slice(&self.0[2..34]);
        BlockIdentifier(block_id_bytes)
    }
}

impl From<[u8; 34]> for ThreadIdentifier {
    fn from(array: [u8; 34]) -> Self {
        Self(array)
    }
}

impl From<ThreadIdentifier> for [u8; 34] {
    fn from(value: ThreadIdentifier) -> Self {
        value.0
    }
}

impl TryFrom<String> for ThreadIdentifier {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match hex::decode(value) {
            Ok(array) => {
                let boxed_slice = array.into_boxed_slice();
                let boxed_array: Box<[u8; 34]> = match boxed_slice.try_into() {
                    Ok(array) => array,
                    Err(e) => anyhow::bail!("Expected a Vec of length 34 but it was {}", e.len()),
                };
                Ok(Self(*boxed_array))
            }
            Err(_) => anyhow::bail!("Failed to convert to ThreadIdentifier"),
        }
    }
}

impl std::fmt::LowerHex for ThreadIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", hex::encode(self.0))
    }
}

impl std::fmt::Display for ThreadIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "<T:{}>", hex::encode(self.0))
    }
}
impl std::fmt::Debug for ThreadIdentifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ThreadIdentifier<{}>", hex::encode(self.0))
    }
}

impl AsRef<[u8]> for ThreadIdentifier {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// Note:
// std::cmp::Ord notes:
// If you implement it manually, you should manually implement all four traits,
// based on the implementation of Ord
// And since there's a separate custom Ord, implementation here is an implementation for the Eq
// The same applies for Hash.
impl PartialEq for ThreadIdentifier {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for ThreadIdentifier {}

impl std::hash::Hash for ThreadIdentifier {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl Ord for ThreadIdentifier {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl PartialOrd for ThreadIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_must_set_bit_values() {
        let mut addr = AccountIdentifier::default();
        addr.set_bit(101);
        assert!(!addr.get_bit(0));
        assert!(!addr.get_bit(255));
        assert!(!addr.get_bit(100));
        assert!(addr.get_bit(101));
    }

    #[test]
    #[should_panic]
    fn it_must_panic_when_set_out_of_bounds() {
        let mut addr = AccountIdentifier::default();
        addr.set_bit(256);
    }

    #[test]
    #[should_panic]
    fn it_must_panic_when_read_out_of_bounds() {
        let addr = AccountIdentifier::default();
        let _ = addr.get_bit(256);
    }

    #[test]
    fn test_set_bit_value() {
        let mut address = AccountIdentifier::default();
        address.set_bit(255);
        assert!(address.get_bit(255));
        address.set_bit(254);
        assert!(address.get_bit(254));
        assert!(address.get_bit(255));
    }
}
