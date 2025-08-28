use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::LazyLock;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::Value;
use tvm_block::Account;
use tvm_contracts::TvmContract;
use tvm_types::UInt256;

use crate::parse_publisher_addr;
use crate::resolver::blockchain::AccountProvider;

fn get_u256(v: &Value, name: &str) -> anyhow::Result<UInt256> {
    get_optional_u256(v, name)?.ok_or_else(|| anyhow::anyhow!("{name} is not set"))
}

fn get_optional_u256(v: &Value, name: &str) -> anyhow::Result<Option<UInt256>> {
    let Some(field) = v.get(name) else { return Ok(None) };
    let s = field.as_str().ok_or_else(|| anyhow::anyhow!("{name} is not a string"))?;
    let u = UInt256::from_str(s).map_err(|err| anyhow::anyhow!("{name} is not a u256: {err}"))?;
    Ok(Some(u))
}

fn get_map<K, V: DeserializeOwned>(v: &Value, name: &str) -> anyhow::Result<HashMap<K, V>>
where
    K: FromStr + Eq + Hash,
    anyhow::Error: From<<K as FromStr>::Err>,
{
    let Some(field) = v.get(name) else { return Ok(HashMap::new()) };
    let m = field.as_object().ok_or_else(|| anyhow::anyhow!("{name} is not a map"))?;
    let m = m
        .into_iter()
        .map(|(k, v)| Ok::<_, anyhow::Error>((K::from_str(k)?, serde_json::from_value(v.clone())?)))
        .collect::<Result<_, _>>()?;
    Ok(m)
}

static ROOT: LazyLock<TvmContract> = LazyLock::new(|| {
    TvmContract::new(
        include_str!("../../../../contracts/bksystem/BlockKeeperContractRoot.abi.json"),
        include_bytes!("../../../../contracts/bksystem/BlockKeeperContractRoot.tvc"),
    )
});

pub struct Root(pub Account);
impl Root {
    pub fn get_epoch_code_hash(&self) -> anyhow::Result<UInt256> {
        let output = ROOT.run_get(&self.0, "getEpochCodeHash", None)?;
        get_u256(&output, "epochCodeHash")
    }
}

static BK: LazyLock<TvmContract> = LazyLock::new(|| {
    TvmContract::new(
        include_str!("../../../../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json"),
        include_bytes!("../../../../contracts/bksystem/AckiNackiBlockKeeperNodeWallet.tvc"),
    )
});

pub struct Bk(pub Account);
impl Bk {
    pub fn get_proxy_list_addr(&self) -> anyhow::Result<UInt256> {
        let output = BK.run_get(&self.0, "getProxyListAddr", None)?;
        get_u256(&output, "value0")
    }
}

static EPOCH: LazyLock<TvmContract> = LazyLock::new(|| {
    TvmContract::new(
        include_str!("../../../../contracts/bksystem/BlockKeeperEpochContract.abi.json"),
        include_bytes!("../../../../contracts/bksystem/BlockKeeperEpochContract.tvc"),
    )
});

pub struct Epoch(pub Account);

impl Epoch {
    pub fn get_owner_address(&self) -> anyhow::Result<UInt256> {
        let details = EPOCH.run_get(&self.0, "getDetails", None)?;
        get_u256(&details, "owner")
    }
}

static PROXY_LIST: LazyLock<TvmContract> = LazyLock::new(|| {
    TvmContract::new(
        include_str!("../../../../contracts/bksystem/BlockKeeperEpochProxyList.abi.json"),
        include_bytes!("../../../../contracts/bksystem/BlockKeeperEpochProxyList.tvc"),
    )
});

pub struct ProxyList(pub Account);

#[derive(Deserialize)]
struct ProxyListEntry {
    #[serde(deserialize_with = "crate::deserialize_publisher_addr")]
    pub addr: SocketAddr,
}

impl ProxyList {
    pub fn get_proxy_list(&self) -> anyhow::Result<(Option<SocketAddr>, HashSet<SocketAddr>)> {
        let details = PROXY_LIST.run_get(&self.0, "getDetails", None)?;
        let mut proxy_list = get_map::<u8, String>(&details, "ProxyList")?;
        if proxy_list.is_empty() {
            anyhow::bail!("Proxy list is empty");
        }
        let node_addr = proxy_list.remove(&0).map(parse_publisher_addr).transpose()?;
        let proxies = proxy_list
            .into_values()
            .map(|x| serde_json::from_str::<ProxyListEntry>(&x))
            .map(|x| x.map(|x| x.addr))
            .collect::<Result<_, _>>()?;
        Ok((node_addr, proxies))
    }
}
pub fn collect_bk_set<PeerId>(
    accounts: &impl AccountProvider,
    bk: &Bk,
    nodes: &mut HashMap<PeerId, (Option<SocketAddr>, HashSet<SocketAddr>)>,
) -> anyhow::Result<()>
where
    PeerId: Eq + Hash + From<UInt256>,
{
    let proxy_list_id = bk.get_proxy_list_addr()?;
    let Some(proxy_list_acc) = accounts.get_account(&proxy_list_id) else { return Ok(()) };
    let account_id =
        bk.0.get_id().ok_or_else(|| anyhow::anyhow!("Proxy list account has no id"))?;
    let account_id: UInt256 = account_id.try_into().map_err(|err| anyhow::anyhow!("{err}"))?;
    nodes.insert(account_id.into(), ProxyList(proxy_list_acc).get_proxy_list()?);
    Ok(())
}
