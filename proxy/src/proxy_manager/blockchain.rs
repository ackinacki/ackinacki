use std::io::Read;
use std::net::SocketAddr;

use network::parse_publisher_addr;

/// how to get proxy list from proxy list contract
/// Run getter getDetails in BlockKeeperEpochProxyList
/// tvm-cli run {contract_address} getDetails '{}' --abi BlockKeeperEpochProxyList.abi.json
/// how to get all proxy contracts by code hash
/// where to get code hash - Run getter getProxyListCodeHash in BlockKeeperContractRoot
/// tvm-cli run {contract_address} getProxyListCodeHash '{}' --abi BlockKeeperContractRoot.abi.json
/// how to query all contracts - Make a GraphQL query to search for contracts by hash.
/// query { accounts(filter: { code_hash: { eq: "{proxylistcodehash}" } }) { id } }
/// https://github.com/gosh-sh/acki-nacki/blob/dev/contracts/bksystem/BlockKeeperEpochProxyList.abi.json
/// https://github.com/gosh-sh/acki-nacki/blob/dev/contracts/bksystem/BlockKeeperContractRoot.abi.json
/// Fetches the list of proxy servers from the blockchain
pub async fn get_proxy_list() -> anyhow::Result<Vec<SocketAddr>> {
    // TODO: Implement actual blockchain interaction

    let mut file = std::fs::File::open("proxy_list.txt")?;
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    let v: Vec<SocketAddr> = buf.lines().map(parse_publisher_addr).collect::<Result<_, _>>()?;

    Ok(v)
}
