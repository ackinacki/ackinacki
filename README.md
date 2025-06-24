# Deployment Instructions

- [Deployment Instructions](#deployment-instructions)
- [Block Keeper](#block-keeper)
  - [System Requirements](#system-requirements)
  - [Block Keeper Wallet Deployment](#block-keeper-wallet-deployment)
    - [Prerequisites](#prerequisites)
    - [Configure tvm-cli](#configure-tvm-cli)
    - [Deploy](#deploy)
  - [Delegating License](#delegating-license)
  - [Block Keeper Deployment with Ansible](#block-keeper-deployment-with-ansible)
    - [Prerequisites](#prerequisites-1)
    - [Key Variables](#key-variables)
    - [Prepare Your Inventory](#prepare-your-inventory)
    - [Run Ansible Playbook](#run-ansible-playbook)
  - [Staking](#staking)
    - [Prerequisites](#prerequisites-2)
    - [Run the script](#run-the-script)
    - [Graceful Shutdown](#graceful-shutdown)
    - [Debug](#debug)
  - [Check node status](#check-node-status)
- [Block Manager](#block-manager)
  - [System requirements](#system-requirements-1)
  - [Deployment with Ansible](#deployment-with-ansible)
    - [Prerequisites](#prerequisites-3)
    - [Prepare Your Inventory](#prepare-your-inventory-1)
    - [Run Ansible Playbook](#run-ansible-playbook-1)
- [Proxy](#proxy)
  - [System requirements](#system-requirements-2)
  - [Deployment with Ansible](#deployment-with-ansible-1)
    - [Prerequisites](#prerequisites-4)
    - [Key Variables](#key-variables-1)
    - [Prepare Your Inventory](#prepare-your-inventory-2)
    - [Run Ansible Playbook](#run-ansible-playbook-2)

# Block Keeper

## System Requirements

| Component     | Requirements                                                                 |
| ------------- | -----------                                                                  |
| CPU (cores)   | 16 dedicated physical cores on a single CPU, or 16 vCPUs ≥ 2.4 GHz. Hyper-threading must be disabled.                                                                                      |
| RAM (GiB)     | 128 GB                                                                       |
| Storage       | 1 TB of high performance NVMe SSD (PCIe Gen3 with 4 lanes or better)         |
| Network       | The effective bandwidth may be limited to 1 Gbps full-duplex total traffic, meaning no more than 1 Gbps in each direction (ingress and egress). A 2.5 Gbps or better full-duplex network interface card (NIC) should be installed to support anticipated future load increases and avoid hardware replacement.                                                      |

**Steps**
1. Deploy the Block Keeper Wallet.
2. Create an Ansible inventory for Block Keeper node deployment.
3. Run the Ansible playbook using the inventory to deploy the Block Keeper node.
4. Delegate the license to the Block Keeper wallet.
5. Run the staking script.

## Block Keeper Wallet Deployment

### Prerequisites
- The **`tvm-cli`** and **`node-helper`** command-line tools must be installed.  
  Check out [this guide for installing CLI](https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet#create-a-wallet-1), and [download node-helper](https://github.com/ackinacki/ackinacki/releases).

- A deployed license contract with obtained license numbers.  
  Refer to the [Working with Licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses) section for details.

### Configure tvm-cli
For this example, we are using the Shellnet network.

To set the appropriate network, use the following command:
```bash
tvm-cli -g config --url shellnet.ackinacki.org/graphql
```

### Deploy
To deploy Block Keeper Wallet, use the shell script [`scripts/create_block_keeper_wallet.sh`](https://github.com/ackinacki/ackinacki/blob/main/scripts/create_block_keeper_wallet.sh).

To run the script, you need to provide the following arguments:

* `-m` – Path to the [Block Keeper Node Owners keys](https://docs.ackinacki.com/glossary#bk-node-owner-keys) file.
    * If the file exists, the script will use the existing keys;
    * If not, new keys will be generated and saved in this file.

* `-l` – A list of [License numbers](https://docs.ackinacki.com/glossary#license-number) to add to the [Block Keeper Wallet whitelist](https://docs.ackinacki.com/glossary#bk-wallet-whitelist). (Use `,` as a delimiter without spaces).
    * These license numbers must be obtained from the License Owners.

For example:
```bash
cd scripts
./create_block_keeper_wallet.sh -m ../bk_wallet/bk_wallet.keys.json -l 6,7
```

After the script completes successfully, make sure to save:
  * Your `Node ID`
  * The path to the BK Node Owner keys file

These will be required later when running the Ansible playbook.

## Delegating License

Before starting staking, a node must have at least one delegated license.
However, no more than ten licenses can be delegated to a single node.

Learn more about [working with licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses).

If the BK Node Owner is also a License Owner, they must use the `addBKWallet(uint256 pubkey)` method in the [`License`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.sol) contract to delegate their licenses to their node. 
(This must be done for each license contract).

Where:

* `pubkey` – the public key of the node owner, obtained in Step 4.

* [`License.abi.json`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.abi.json) – the ABI of the License contract.

* `License.keys.json` – the keys obtained by the License Owner during registration in the dashboard.

For example:

```bash
tvm-cli -j callx --addr 0:7f2f945faaae4cce286299afe74dac9460893dd5cba1ac273b9e91f55f1141ec --abi acki-nacki/contracts/bksystem/License.abi.json --keys license_onwer/license.keys.json --method addBKWallet '{"pubkey": "0xfa4edc8b63c4e66241a57c11e0a522769ca4a4f106692512fc92f2d658169bcc"}'
```

## Block Keeper Deployment with Ansible

### Prerequisites

- SSH access to servers.
- Docker with the compose plugin must be installed.

### Key Variables

`Node ID`: The unique identifier of the Block Keeper within the network. This is required to create a new Block Keeper.  
Use the value provided by the BK wallet deployment script.

`HOST_PRIVATE_IP`: Specify the private IP address of the host, for example, `127.0.0.1`.
Ensure that the ports do not overlap.

`PROXIES`: List the IP addresses of the Proxies that will be used to broadcast blocks across the network.
**If the Block Keeper will operate without a Proxy, this variable is not required.**

**Note**:  
**If you want to deploy a Proxy, refer to the [Proxy deployment guide](#proxy) or use an existing Proxy service.**
To use an existing Proxy, please contact the corresponding Proxy provider representative for details.

### Prepare Your Inventory

Here is a basic inventory for Shellnet node deployment:

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    #Path to store deployment files
    ROOT_DIR: /home/user/deployment
    #Path to store data
    MNT_DATA: /home/user/data
    BIND_PORT: 8500
    BIND_API_PORT: 8600
    BIND_MESSAGE_ROUTER_PORT: 8700
    BIND_GOSSIP_PORT: 10000
    BLOCK_MANAGER_PORT: 12000
    NODE_IMAGE: teamgosh/ackinacki-node:<latest-release-tag> # i.e. teamgosh/ackinacki-node:v0.3.3
    GQL_IMAGE: teamgosh/ackinacki-gql-server:<latest-release-tag> # i.e. teamgosh/ackinacki-gql-server:v0.3.3
    REVPROXY_IMAGE: teamgosh/ackinacki-nginx
    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"
    LOG_ROTATE_AMOUNT: 30
    LOG_ROTATE_SIZE: 1G
    STAKING_IMAGE: teamgosh/ackinacki-staking:<latest-release-tag>
    STAKING_TIME: 60
    TVM_ENDPOINT: shellnet.ackinacki.org
    NETWORK_NAME: shellnet
    GOSSIP_SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000
      - YOUR-NODE-ADDRESS:10000
    NODE_STORAGE_LIST:
      - http://shellnet0.ackinacki.org/storage/node/
      - http://shellnet1.ackinacki.org/storage/node/
      - http://shellnet2.ackinacki.org/storage/node/
      - http://shellnet3.ackinacki.org/storage/node/
      - http://shellnet4.ackinacki.org/storage/node/
      - http://YOUR-NODE-ADDRESS/storage/node/
    MASTER_KEY: PATH_TO_MASTER_KEY
    NODE_CONFIGS:
      - "zerostate"
      - "blockchain.conf.json"

block_keepers:
  hosts:

    YOUR-NODE-ADDRESS:
      NODE_ID: NODE_ID
      HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-ADDRESS
      HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-ADDRESS
      PROXIES: # delete this section if the BK will operate without a Proxy
        - PROXY_IP:8085
```


Ensure your configuration data is added to the inventory before running the playbook.

For testing, you can use an Ansible dry run:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

### Run Ansible Playbook

If everything looks good, run Ansible:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml
```

Upon completion of the script, BLS keys will be generated and saved in the file `block_keeper{{ NODE_ID }}_bls.keys.json` in the `{{ BK_DIR }}/bk-configs/` folder on the remote server, along with the node.

**`BLS keys`** - the keys used by Block Keeper (BK) to sign blocks. The keys have a lifespan of one Epoch. Each BK maintains a list of BLS public keys from other BKs (for the current Epoch), which are used to verify attestations on blocks.

Check the Docker containers.

```bash
docker ps
#OR
docker compose ps
```

Check node logs

```bash
tail -f $MNT_DATA/logs-block-keeper/node.log
```

## Staking
Staking is deployed as a Docker container using Docker Compose. Docker Compose, in turn, is deployed via Ansible using the `node-deployment.yaml` playbook.

The staking script performs the following tasks:
- Sends stakes
- Rotates Epochs
- Collects rewards

Staking runs as a background daemon inside a container, and its output is redirected to the `BK_LOGS_DIR`.
The staking container is part of the Docker Compose setup and runs alongside the Block Keeper node.

**Note:**  
Stakes and rewards are denominated in [NACKL](https://docs.ackinacki.com/glossary#nack) tokens.  
Rewards are distributed proportionally to the stake amount — the higher your stake, the greater your share of the rewards.  
As a Block Keeper with an active license, you can participate in staking under special conditions: if your BK wallet balance is below the [minimum stake](https://docs.ackinacki.com/glossary#minimal-stake), you can still place a stake as long as it is not less than your previous one.

To check the current minimum stake in the network, run the `getMinStakeNow` method of the system contract [BlockKeeperContractRoot](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/BlockKeeperContractRoot.sol) as follows:

  ```bash
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMinStakeNow {} --abi acki-nacki/contracts/bksystem/BlockKeeperContractRoot.abi.json
  ```

  The ABI file [BlockKeeperContractRoot.abi.json](https://raw.githubusercontent.com/ackinacki/ackinacki/fa3c2685c5efaaded16aa370066a39ea12d0f899/contracts/bksystem/BlockKeeperContractRoot.abi.json) is required to run this command.


### Prerequisites
- BK Node Owner keys file
- BLS keys file

The BLS keys file can be found in the `{{ BK_DIR }}/bk-configs/` directory, where `BK_DIR` is a variable defined in the Ansible inventory.
The BLS keys file format - `block_keeper{{ NODE_ID }}_bls.keys.json`

The staking script requires two parameters:
- **STAKING_IMAGE** – the Docker image used to run the staking script
- **STAKING_TIME (in seconds)** – defines the interval between staking script executions. Specify 120s for Shellnet. 
- **TVM_ENDPOINT** – TVM endpoint for connecting to enable staking

Specify above parameters in the inventory file.

**Note:**  
Make sure the BLS keys file has read and write permissions.

### Run the script
All neccessary keys are passed to staking container iside Docker compose file. 

```bash
docker compose up -d staking
```

### Graceful Shutdown
Staking supports a graceful shutdown process. During shutdown, any ongoing “continue staking” request will be canceled, and tokens will be returned to your wallet.

**Important:**
Before stopping staking, ensure there are no active operations such as processing Epoch or processing Cooler in log.
You can check this using the `tail` command:

```bash
tail -n 100 -f {{ BK_LOGS_DIR }}/staking.log
```

To gracefully stop staking, run:

```bash
docker compose down staking --timeout 60
```

After the shutdown, the log will contain the result of the “continue staking” request cancellation.

Example:
```
[2025-01-01T10:00:00+00:00] Signal has been recieved. Trying to shutdown gracefully
[2025-01-01T10:00:00+00:00] Current wallet balance - 843303030953
{
  "message_hash": "9748f89fd4e482ec9984e5af70176ee57c943746e8984baa6d299b992cbf92a4",
  "block_hash": "c292bc0ba0900224987b1dc7fb0ef058357a4c5d7fc317e99471398b92793baf",
  "tx_hash": "7cf5e9d9b085f57149211a6fa60591a8b25c7be7f9b2d5be8a89fc5e655d4edb",
  "return_value": null,
  "aborted": false,
  "exit_code": 0,
  "thread_id": "00000000000000000000000000000000000000000000000000000000000000000000",
  "producers": [
    "147.135.77.79:8600"
  ],
  "current_time": "1750428189973"
}
[2025-01-01T10:00:00+00:00] Balance after canceling continue staking - 854318121984
[2025-01-01T10:00:00+00:00] Exiting...
```

### Debug
To debug the staking script, enable command tracing by adding the `-x` flag:  

```bash
set -xeEuo pipefail
```
This will print each command as it executes, helping to identify issues during script execution.

## Check node status
To get the node's status by blocks, use the `node_sync_status.sh` script:

```bash
node_sync_status.sh path/to/log
```

# Block Manager

## System requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 4c/8t       | 32        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 8c/16t      | 64        | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

**Steps**

1. Create an Ansible inventory for Block Manager deployment.
2. Run the Ansible playbook against the inventory to deploy the Block Manager node.

## Deployment with Ansible

### Prerequisites

* A dedicated server for the Block Manager with SSH access.
* Docker with the Compose plugin installed.

### Prepare Your Inventory

Below is a basic inventory for Block Manager deployment. Make sure to include the `block-manager` host group.

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    # Path to store deployment files
    ROOT_DIR: /home/user/deployment
    # Path to store data
    MNT_DATA: /home/user/data
    BM_IMAGE: "teamgosh/ackinacki-block-manager"
    BM_DIR: "{{ ROOT_DIR }}/block-manager"
    BM_DATA_DIR: "{{ MNT_DATA }}/block-manager"
    BM_LOGS_DIR: "{{ MNT_DATA }}/logs-block-manager"
    THREAD_COUNT_SOFT_LIMIT: 4
    LOG_ROTATE_AMOUNT: 30
    LOG_ROTATE_SIZE: 1G

block_manager:
  hosts:

    YOUR-BM-HOST:
      # Node address to connect and download states for Block Manger
      HTTP_URL: NETWORK_NODE
      # Node streaming address for Block Manger
      NODE_IP: NODE_IP_ADDRESS
```

In case of testing, you can use dry run and check mode

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml --check --diff
```

### Run Ansible Playbook

If everything looks good, you can proceed with running Ansible.

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml
```

Check the running Docker containers using the following command:

```bash
docker ps
#OR
docker compose ps
```

To check the Block Manager logs, use the following command:

```bash
tail -f $MNT_DATA/logs-block-manager/block-manager.log
```

# Proxy

**Broadcast Proxy** is a specialized network service designed to optimize block exchange between participants in the Acki Nacki network. Its primary purpose is to reduce the overall network traffic between nodes operated by different Node Providers, while also improving the network’s scalability and stability.

## System requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage      | Network                                            |
| ------------- | ----------- | --------- | ------------ | -------------------------------------------------- |
| Recommended   | 8c/16t      | 32        | 500 GB NVMe  | 100 Gbit synchronous unmetered Internet connection |

**Important:**
To ensure stable operation, proxy servers must be deployed in pairs (one master and one failover).

## Deployment with Ansible

### Prerequisites
* A dedicated server for the Proxy.
* Docker with the Compose plugin installed.

The deployment uses Ansible roles and a playbook. There's no need to manually configure the proxy settings or the Docker Compose file — everything is handled through Ansible variables.

### Key Variables

`SEEDS:` A list of seed addresses for the Gossip protocol between the Proxy and Block Keeper nodes. This list must include at least three addresses.  

For example:  
```yaml
SEEDS:
  - BK_IP:10002
  - BK_IP:10000
  - BK_IP:10001
```

`NETWORK_NAME`: A string that defines the cluster ID in the Gossip network. It must match the NETWORK_NAME used by both the Proxy and Block Keeper nodes.  

For example:  
```yaml
NETWORK_NAME: shellnet
```

`PROXY_BIND`: The IP address on the host machine to bind the Proxy.

`PROXY_ADDR`: The address used by the Gossip protocol and Block Keepers to communicate with the Proxy.

**Note:**  
`PROXY_BIND` and `PROXY_ADDR` must be set to the same IP address.  

**Important:**  
You must add Proxy addresses to the Block Keeper configuration. Otherwise, the Block Keeper will not be able to connect to the Proxy.

Example Block Keeper host configuration:  
```yaml
block_keeper_host:
  NODE_ID: 99999
  PROXIES:
    - PROXY_IP:8085
```

### Prepare Your Inventory

Below is an example of a basic Ansible inventory for deploying a Proxy:  

```yaml
all:
  vars:
    PROXY_IMAGE: "docker/ackinacki-proxy:proxy_version"
    PROXY_PORT: 8085
    PROXY_DIR: /opt/depl/proxy
    NETWORK_NAME: shellnet

proxy:
  hosts:
    proxy:
      SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000
```

To test the deployment with a dry run:  

```bash
ansible-playbook -i test-inventory.yaml ansible/deploy-proxy.yml --check --diff
```

### Run Ansible Playbook

If everything looks correct, proceed with the actual deployment:  

```bash
ansible-playbook -i test-inventory.yaml ansible/deploy-proxy.yml
```

To view the Proxy logs:  

```bash
tail -f $PROXY_DIR/logs-proxy/proxy.log
```
