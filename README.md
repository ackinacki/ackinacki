# Acki Nacki Instructions

- [Acki Nacki Instructions](#acki-nacki-instructions)
- [Block Keeper System Requirements](#block-keeper-system-requirements)
- [(Current Phase) Joining the Acki Nacki Network from the Genesis Block](#current-phase-joining-the-acki-nacki-network-from-the-genesis-block)
  - [Stage 1. Preparation Checklist](#stage-1-preparation-checklist)
    - [For License Owner](#for-license-owner)
    - [For Node Owner](#for-node-owner)
  - [Stage 2. Zerostate Formation](#stage-2-zerostate-formation)
  - [Stage 3. Launch After the GOSH Team Signal](#stage-3-launch-after-the-gosh-team-signal)
- [(Next Phase) Joining the Acki Nacki Network After Launch](#next-phase-joining-the-acki-nacki-network-after-launch)
  - [Join As a License Owner](#join-as-a-license-owner)
  - [Join As a BK Node Owner](#join-as-a-bk-node-owner)
- [Block Keeper Documentation](#block-keeper-documentation)
  - [Block Keeper Wallet Deployment](#block-keeper-wallet-deployment)
    - [Prerequisites](#prerequisites)
    - [Configure tvm-cli](#configure-tvm-cli)
    - [Deploy](#deploy)
  - [Delegating License](#delegating-license)
  - [Block Keeper Deployment with Ansible](#block-keeper-deployment-with-ansible)
    - [Prerequisites](#prerequisites-1)
    - [Key Variables](#key-variables)
    - [Prepare Your Inventory](#prepare-your-inventory)
    - [Run the Ansible Playbook](#run-the-ansible-playbook)
    - [Run Multiple BK Nodes on a Single Server](#run-multiple-bk-nodes-on-a-single-server)
    - [Graceful Shutdown of a BK Node](#graceful-shutdown-of-a-bk-node)
  - [Running Staking with Ansible](#running-staking-with-ansible)
    - [Prerequisites](#prerequisites-2)
    - [Run the Script](#run-the-script)
    - [Graceful Shutdown for Staking](#graceful-shutdown-for-staking)
    - [Debug](#debug)
  - [Check Node Status](#check-node-status)
  - [Changing the IP Address of a BK Node](#changing-the-ip-address-of-a-bk-node)
    - [1. Calculate the Time You Have for Migration](#1-calculate-the-time-you-have-for-migration)
      - [1.1  Get the Current Epoch Contract Address](#11get-the-current-epoch-contract-address)
      - [1.2  Determine the Epoch Length](#12determine-the-epoch-length)
    - [2. Stop Staking (Graceful Shutdown)](#2-stop-staking-graceful-shutdown)
    - [3. Stop node (Graceful Shutdown)](#3-stop-node-graceful-shutdown)
    - [4. Deploy BK Software on the New Node](#3-deploy-bk-software-on-the-new-node)
    - [5. Start Staking on the New IP Address](#4-start-staking-on-the-new-ip-address)
    - [6. Check Node Status](#5-check-node-status)
- [Block Manager documentation](#block-manager-documentation)
  - [System Requirements](#system-requirements)
  - [Deployment with Ansible](#deployment-with-ansible)
    - [Prerequisites](#prerequisites-3)
    - [Generate Keys and Deploy Block Manager Wallet](#generate-keys-and-deploy-block-manager-wallet)
    - [Create an Ansible Inventory](#create-an-ansible-inventory)
    - [Test the Ansible Playbook](#test-the-ansible-playbook)
    - [Run the Ansible Playbook](#run-the-ansible-playbook-1)
    - [Verify the Deployment](#verify-the-deployment)
- [Proxy Documentation](#proxy-documentation)
  - [System Requirements](#system-requirements-1)
  - [Deployment with Ansible](#deployment-with-ansible-1)
    - [Prerequisites](#prerequisites-4)
    - [How Deployment Works](#how-deployment-works)
    - [Key Variables](#key-variables-1)
    - [Prepare Your Inventory](#prepare-your-inventory-2)
    - [Run the Ansible Playbook](#run-the-ansible-playbook-2)

# Block Keeper System Requirements

| Component     | Requirements                                                                 |
| ------------- | -----------                                                                  |
| CPU (cores)   | 16 dedicated physical cores on a single CPU, or 16 vCPUs ≥ 2.4 GHz. Hyper-threading must be disabled.                                                                                      |
| RAM (GiB)     | 128 GB                                                                       |
| Storage       | 1 TB of high performance NVMe SSD (PCIe Gen3 with 4 lanes or better)         |
| Network       | The effective bandwidth may be limited to 1 Gbps full-duplex total traffic, meaning no more than 1 Gbps in each direction (ingress and egress). A 2.5 Gbps or better full-duplex network interface card (NIC) should be installed to support anticipated future load increases and avoid hardware replacement.                                                      |

# (Current Phase) Joining the Acki Nacki Network from the Genesis Block

During this phase, information is being collected to form the Zerostate and nodes are being prepared for the network launch.
If you want to join the network right from the start, you need to take part in this phase.

## Stage 1. Preparation Checklist

**If you delegate license to your own nodes, you act simultaneously as a License Owner and Node Owner.**

### For License Owner

- ✅ You must delegate the licenses that you want to be included in the Zerostate. [See the instruction](https://docs.ackinacki.com/protocol-participation/license/license-delegation-guide#self-delegation-node-owners)

  ℹ️ **Note:**
  Even if you, as the License Owner, plan to delegate licenses to your own nodes, you still need to generate a `Node Provider Key Pair` ([using this instruction](https://github.com/ackinacki/acki-nacki-igniter/blob/main/README.md#generate-a-node-provider-key-pair)) for security reasons
  and complete the steps required for Node Owners (described below).

### For Node Owner

- ✅ At this stage, you are also acting as a Node Provider, so make sure to generate `Node Provider Key Pair` ([using this instruction](https://github.com/ackinacki/acki-nacki-igniter/blob/main/README.md#generate-a-node-provider-key-pair)) and send the public key to the License Owner for delegation.

- ✅ Generate `Node Owner and BLS keys` for each of your nodes.
[Instructions are here](https://github.com/ackinacki/acki-nacki-igniter/blob/main/docs/Keys_generation.md#generate-bk-node-owner-and-bls-keys).

- ✅ Collect all the required signatures to properly configure the Igniter DNSP client [using the instruction](https://github.com/ackinacki/acki-nacki-igniter/blob/main/docs/License_attachment.md#get-delegated-license-ids-and-signatures)

    - ✔️ obtain the `Delegation Signature`
    - ✔️ obtain the `License Proof Signature`
    - ✔️ generate the `Confirmation Signatures`

  🚨 **Important:**
  Do this for every delegated license on each node.

- ✅ Create a [`config.yaml`](https://github.com/gosh-sh/acki-nacki-igniter?tab=readme-ov-file#prepare-confirmation-signatures-and-create-configyaml) configuration file for running the Igniter.

- ✅ There is a `proxies` parameter in the config file. If you have more than 1 Node - to reduce network traffic - you will need to deploy at least 2 Proxy services for your nodes or use the existing ones. Do not start proxies now, but publish their IP addresses and certificates to config.yaml.

- ✅ Start the Igniter for each node.

[Full Igniter Instruction is here](https://github.com/ackinacki/acki-nacki-igniter/blob/main/README.md)

🚨 **Important:**
* The Igniter must be run for each node with the same server IP addresses and Proxy IP addresses where the BK will later be deployed.
* The Igniter must use the same keys and licenses that will be used for deploying the BK.

After starting the Igniter, you can verify your node data (licenses, BLS keys, Proxy IPs, BK public key, etc.) at:
`http://<your-node-IP>:10001`
(for example: http://94.156.178.1:10001)

[Example of how node information is displayed in DNSP](https://github.com/ackinacki/acki-nacki-igniter/blob/main/docs/gossip.jpg)

## Stage 2. Zerostate Formation

Data collected by the Igniters will be used to form the zerostate.

At network launch, the Zerostate will include:
* all BK wallets with their corresponding whitelists,
* all license contracts, and mappings to the contracts of the Node Owners’ wallets to which they were delegated.

## Stage 3. Launch After the GOSH Team Signal

The Gosh team will announce the exact network launch time in advance (including day, hour, minute).
So, either set up an automatic launch of the deployment scripts and staking on each server where the Igniter was running,
or launch them manually at the specified time
  - [BK deployment instructions ](#block-keeper-deployment-and-running-staking-with-ansible)
  - [Proxy deployment instructions](#proxy)

🚨 **Important:**
The BK and Proxy IP addresses must match those used when starting the Igniter.

You can change a node’s ip-adress after the network launch.
[see instruction](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#changing-the-ip-address-of-a-bk-node)

*A guide for Proxy IP migration will be added soon.*

# (Next Phase) Joining the Acki Nacki Network After Launch

In this phase, BK nodes that did not participate in the previous network launch phase will be able to join.

ℹ️ **Note:**
If you delegate license to your own node, you act simultaneously as a License Owner and a Node Owner.

**To join the network, follow these steps:**

## Join As a License Owner

**Step 1.**
After purchasing a license, register in the [dashboard](https://dashboard.ackinacki.com) using [this guide](#delegating-license) to get license details (number and contract address).

**Step 2.**
Provide your license number to the chosen Node Owner so they can add it to their BK wallet whitelist.

**Step 3.**
After confirmation from the Node Owner, delegate your license to the chosen BK using the BK Node Owner’s public key:
  * via the dashboard [see instruction](https://docs.ackinacki.com/protocol-participation/license/license-delegation-guide#delegation-via-acki-nacki-dashboard),
  or
  * manually via `tvm-cli` [see instruction](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#delegating-license).

Now wait for your BK to add your stake to the staking pool.

ℹ️ **Note:**
To check your rewards:
* Call `getDetails` in the BK wallet (see the balance field in the license mapping).
* You can also view your rewards using the [dashboard](https://dashboard.ackinacki.com).

🚨 **Important:**
Condition for receiving the maximum reward: continuous operation and not locking the license on the wallet.

## Join As a BK Node Owner

**Step 1.**
To deploy a BK wallet with a whitelist (license numbers received from License Owners), [follow this guide](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#block-keeper-wallet-deployment).

ℹ️ **Note:**
The whitelist can be updated at any time.

**Step 2.**
Share the BK wallet’s public key with the License Owner so they can delegate their licenses.

**Step 3.**
If you run multiple nodes and your network card is under 2 Gbps, set up a Proxy service [using the instruction](https://docs.ackinacki.com/protocol-participation/proxy-service) for stable operation, or use existing deployed proxies.

**Step 4.**
Start the node and staking:
After at least one license is delegated, the Node Owner deploys BK software and starts staking on the server [using the instruction](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#block-keeper-deployment-with-ansible).

**Step 5.**
[Check the node status](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#check-node-status).

**Now let’s go through all these steps in detail.**

# Block Keeper Documentation

## Block Keeper Wallet Deployment

### Prerequisites
- The [**`tvm-cli`**](https://github.com/tvmlabs/tvm-sdk/releases) command-line tool must be installed.

- A deployed license contract with obtained license numbers.
  Refer to the [Working with Licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses) section for details.

### Configure tvm-cli
For this example, we are using the `Shellnet` network.

To set the appropriate network, use the following command:
```bash
tvm-cli config -g --url shellnet.ackinacki.org/graphql
```

### Deploy
To deploy Block Keeper Wallet, use the shell script [`scripts/create_block_keeper_wallet.sh`](https://github.com/ackinacki/ackinacki/blob/main/scripts/create_block_keeper_wallet.sh).

To run the script, you need to provide the following arguments:

* `-nk` – Path to the [Block Keeper Node Owners keys](https://docs.ackinacki.com/glossary#bk-node-owner-keys) file.
    * If the file exists, the script will use the existing keys;
    * If not, new keys will be generated and saved in this file.

* `-l` – A list of [License numbers](https://docs.ackinacki.com/glossary#license-number) to add to the [Block Keeper Wallet whitelist](https://docs.ackinacki.com/glossary#bk-wallet-whitelist). (Use `,` as a delimiter without spaces).
    * These license numbers must be obtained from the License Owners.

For example:
```bash
cd scripts
./create_block_keeper_wallet.sh -nk ../bk_wallet/bk_wallet.keys.json -l 6,7
```

After the script completes successfully, make sure to save:
  * Your `Node ID`
  * The path to the BK Node Owner keys file

These will be required later when running the Ansible playbook.

ℹ️ **Note:**
The Node Owner can update the license whitelist at any time. However, the changes will only take effect at the beginning of the next Epoch.

To do this, call the `setLicenseWhiteList(mapping(uint256 => bool))` method in your Block Keeper Node Wallet contract, passing the license numbers received from the License Owners.

Where:
* `uint256 (key)` – the license number;
* `bool (value)` – set to `true` to **add** the license on the whitelist, or `false` to **remove** it.

Example command:
```shell
tvm-cli call <BK_NODE_WALLET_ADDR> setLicenseWhiteList \
  '{"whiteListLicense": {"1": true, "2": true, "5": true}}' \
  --abi contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json \
  --sign BK_NODE_OWNER_KEYS
```

## Delegating License

Before starting staking, a node must have at least one delegated license.
However, no more than 20 (twenty) licenses can be delegated to a single node.

Learn more about [working with licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses).

If the BK Node Owner is also a License Owner, they must use the `addBKWallet(uint256 pubkey)` method in the [`License`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.sol) contract to delegate their licenses to their node.
(This must be done for each license contract).

Where:
* `pubkey` – the public key of the node owner, obtained in Step 4.
* [`License.abi.json`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.abi.json) – the ABI of the License contract.
* `License.keys.json` – the keys obtained by the License Owner during registration in the dashboard.

For example:
```bash
tvm-cli -j callx --addr 0:7f2f945faaae4cce286299afe74dac9460893dd5cba1ac273b9e91f55f1141ec --abi contracts/bksystem/License.abi.json --keys license_onwer/license.keys.json --method addBKWallet '{"pubkey": "0xfa4edc8b63c4e66241a57c11e0a522769ca4a4f106692512fc92f2d658169bcc"}'
```

## Block Keeper Deployment with Ansible

### Prerequisites

- SSH access to servers.
- Docker with the compose plugin must be installed.

### Key Variables

`AUTH_TOKEN`: This token is used to authorize access to the BK API. You can specify any arbitrary string.

`Node ID`: The unique identifier of the Block Keeper within the network. It is required when creating a new Block Keeper. Use the value provided by the BK wallet deployment script.

`HOST_PUBLIC_IP`: The public IP address of the host. Make sure that the ports do not conflict with other services.

`HOST_PRIVATE_IP`: Specify the private IP address of the host, (e.g., `127.0.0.1`). Ensure that the ports do not overlap.

ℹ️ **Note**:
  **Providing Access Information to the Block Manager**
  To enable proper interaction with the Block Manager (BM), you must provide the BM Owner with the following information:
  * Authorization Token (`AUTH_TOKEN`) – the access token required for interacting with the BK API.
  * Node Public IP Address (`HOST_PUBLIC_IP`) – the public IP address of your node.

`PROXIES`: List the IP addresses of the Proxies that will be used to broadcast blocks across the network.
**If the Block Keeper will operate without a Proxy, this variable is not required.**

ℹ️ **Note**:
  **If you want to deploy a Proxy, refer to the [Proxy deployment guide](#proxy) or use an existing Proxy service.**
  To use an existing Proxy, please contact the corresponding Proxy provider representative for details.

### Prepare Your Inventory

Here is a basic inventory for `Shellnet` node deployment:

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    ROOT_DIR: /home/user/deployment # path to store deployment files
    MNT_DATA: /home/user/data       # path to store data
    BIND_PORT: 8500
    BIND_API_PORT: 8600
    BIND_MESSAGE_ROUTER_PORT: 8700
    BIND_GOSSIP_PORT: 10000
    BLOCK_MANAGER_PORT: 12000
    NODE_IMAGE: teamgosh/ackinacki-node:<latest-release-tag>      # i.e. teamgosh/ackinacki-node:v0.3.3
    GQL_IMAGE: teamgosh/ackinacki-gql-server:<latest-release-tag> # i.e. teamgosh/ackinacki-gql-server:v0.3.3
    REVPROXY_IMAGE: teamgosh/ackinacki-nginx
    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"
    LOG_ROTATE_AMOUNT: 30   # maximum number of log files to keep
    LOG_ROTATE_SIZE: 1G     # minimum size of the log file to rotate
    LOG_ROTATE_SPEC: "0 *"  # period of rotation in cron "minute hour" format
    STAKING_IMAGE: teamgosh/ackinacki-staking:<latest-release-tag>
    STAKING_TIME: 600
    TVM_ENDPOINT: shellnet.ackinacki.org
    NETWORK_NAME: shellnet
    THREAD_COUNT_SOFT_LIMIT: 4
    AUTH_TOKEN: my-secret-token    # the access token required for interacting with the BK API
    AEROSPIKE_PORT: 4000           # Aerospike DB listening port
    AEROSPIKE_FABRIC_PORT: 4001    # port for intra-cluster communication (migration, replication, etc.)
    AEROSPIKE_HEARTBEAT_PORT: 4002 # port used to maintain database cluster health (heartbeat)
    AEROSPIKE_IMAGE: "aerospike/aerospike-server:latest"
    NODE_GROUP_ID: ""
    OTEL_COLLECTOR: no
    OTEL_SERVICE_NAME: ""
    GOSSIP_SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000
      - YOUR-NODE-ADDRESS:10000
    NODE_OWNER_KEY: PATH_TO_NODE_OWNER_KEY
    NODE_CONFIGS:
      - "zerostate"
      - "blockchain.conf.json"

block_keepers:
  hosts:

    YOUR-NODE-ADDRESS:
      NODE_ID: NODE_ID
      HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-ADDRESS
      HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-ADDRESS
      PROXIES: # delete this section if the BK operates without a Proxy
        - PROXY_IP:8085
```

`LOG_ROTATE_SPEC` is the rotation schedule in cron "minute hour" format.

For example, default `"0 *"` value means that the log files will be rotated every hour on the 0th minute.

In case rotated log files are too large, you might want to decrease the period of rotation.

For example, `"*/5 *"` value means that the log files will be rotated every 5 minutes.


`NODE_GROUP_ID`, `OTEL_COLLECTOR` and `OTEL_SERVICE_NAME` are optional and related to node metrics integration.

They can be used to send metrics to the specified collector server. `NODE_GROUP_ID` should be used to identify the node group
and should be the same for all nodes in your deployment.


Ensure your configuration data is added to the inventory before running the playbook.

For testing, you can use an Ansible dry run:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

### Run the Ansible Playbook

If everything looks good, run Ansible:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml
```

Upon completion of the script, BLS keys will be generated and saved in the file `block_keeper{{ NODE_ID }}_bls.keys.json` in the `{{ BK_DIR }}/bk-configs/` folder on the remote server, along with the node.

**`BLS keys`** - the keys used by Block Keeper (BK) to sign blocks. The keys have a lifespan of one Epoch. Each BK maintains a list of BLS public keys from other BKs (for the current Epoch), which are used to verify attestations on blocks.

During the deployment of a BK node, the staking script will also be automatically started.

Check the Docker containers.

```bash
docker ps
#OR
docker compose ps
```

Verify that the containers using the specified images are in the UP status:
```
teamgosh/ackinacki-node
teamgosh/ackinacki-gql-server
teamgosh/ackinacki-nginx
teamgosh/ackinacki-staking
```

Check node logs

```bash
tail -f $MNT_DATA/logs-block-keeper/node.log
```

### Run Multiple BK Nodes on a Single Server

**Inportant:**
Make sure your server has enough resources to run multiple BK nodes.

To run multiple BK nodes on a single server, assign each BK node to a different ports and create a separate Docker Compose configuration for each one.
A single inventory file can be used.

Example inventory file for running two BK nodes on a single server (`Shellnet`):

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    ansible_host: SERVER_IP_TO_CONNECT
    NODE_IMAGE: teamgosh/ackinacki-node:<latest-release-tag>      # i.e. teamgosh/ackinacki-node:v0.3.3
    GQL_IMAGE: teamgosh/ackinacki-gql-server:<latest-release-tag> # i.e. teamgosh/ackinacki-gql-server:v0.3.3
    REVPROXY_IMAGE: teamgosh/ackinacki-nginx
    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"
    LOG_ROTATE_AMOUNT: 30
    LOG_ROTATE_SIZE: 1G
    STAKING_IMAGE: teamgosh/ackinacki-staking:<latest-release-tag>
    STAKING_TIME: 600
    TVM_ENDPOINT: shellnet.ackinacki.org
    NETWORK_NAME: shellnet
    THREAD_COUNT_SOFT_LIMIT: 4
    AUTH_TOKEN: my-secret-token    # the access token required for interacting with the BK API
    AEROSPIKE_PORT: 4000           # Aerospike DB listening port
    AEROSPIKE_FABRIC_PORT: 4001    # port for intra-cluster communication (migration, replication, etc.)
    AEROSPIKE_HEARTBEAT_PORT: 4002 # port used to maintain database cluster health (heartbeat)
    AEROSPIKE_IMAGE: "aerospike/aerospike-server:latest"
    NODE_GROUP_ID: ""
    OTEL_COLLECTOR: no
    OTEL_SERVICE_NAME: ""
    GOSSIP_SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000
      - YOUR-NODE-ADDRESS:10000
    NODE_CONFIGS:
      - "zerostate"
      - "blockchain.conf.json"

block_keepers:
  vars:
    HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-ADDRESS
    HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-ADDRESS
    PROXIES: # delete this section if the BK operates without a Proxy
      - PROXY_IP:8085
  hosts:
    YOUR-NODE-NAME:
      NODE_ID: NODE_ID
      ROOT_DIR: /home/user/deployment # path to store deployment files
      MNT_DATA: /home/user/data       # path to store data
      BIND_PORT: 8500
      BIND_API_PORT: 8600
      BIND_MESSAGE_ROUTER_PORT: 8700
      BIND_GOSSIP_PORT: 10000
      BLOCK_MANAGER_PORT: 12000
      NODE_OWNER_KEY: PATH_TO_NODE_OWNER_KEY

    YOUR-NODE-NAME-2:
      NODE_ID: SECOND_NODE_ID
      ROOT_DIR: /home/user/deployment-2 # path to store deployment files
      MNT_DATA: /home/user/data-2       # path to store data
      BIND_PORT: 8501
      BIND_API_PORT: 8601
      BIND_MESSAGE_ROUTER_PORT: 8701
      BIND_GOSSIP_PORT: 10001
      BLOCK_MANAGER_PORT: 12001
      NODE_OWNER_KEY: PATH_TO_NODE_OWNER_KEY_2
```

### Graceful Shutdown of a BK Node

To stop a Block Keeper (BK) node safely without data corruption, follow the steps below.

* Go to the Block Keeper directory

```bash
cd {{ ROOT_DIR }}/block-keeper
```

* To identify the service name of your node in Docker Compose, run:

```bash
docker compose ps
```

You should see log entries similar to:

```
NAME                    IMAGE                          SERVICE                 CREATED

block-keeper-node       teamgosh/ackinacki-node       node{{ NODE_ID }}       28 hours ago
```

* Make note of the `SERVICE` value (`node{{ NODE_ID }}` in the example).

* Gracefully stop the node process by sending SIGHUP to the service:

```bash
docker compose exec node{{ NODE_ID }} pkill node
```

* After shutdown, remove the container with the old configuration:

```
docker compose down node{{ NODE_ID }}
```

* Check the logs to ensure shutdown has completed.
You should see a message similar to:

```yaml
2025-08-21T11:21:08.761169Z TRACE ThreadId(01) node: Shutdown finished
```

and no new logs should appear afterward.

## Running Staking with Ansible
Staking is deployed as a Docker container using Docker Compose. Docker Compose, in turn, is deployed via Ansible using the `node-deployment.yaml` playbook.

The staking script performs the following tasks:
- Sends stakes
- Rotates Epochs
- Collects rewards

Staking runs as a background daemon inside a container, and its output is redirected to the `BK_LOGS_DIR`.
The staking container is part of the Docker Compose setup and runs alongside the Block Keeper node.

ℹ️ **Note:**
  Stakes and rewards are denominated in [NACKL](https://docs.ackinacki.com/glossary#nack) tokens.
  Node receives rewards per each License delegated to it.
  Maximum number of licenses delegated per node is 20.

  As a Block Keeper with an active license, you can participate in staking under special conditions: if your BK wallet balance is below the [minimum stake](https://docs.ackinacki.com/glossary#minimal-stake), you can still place a stake as long as you continue staking without interruptions and do not withdraw the received rewards.

  If your stake exceeds the [maximum stake](https://docs.ackinacki.com/glossary#maximum-stake), the excess amount will be automatically returned to your wallet.

  To check the current minimum and maximum stake in the network, run the following commands:

  ```bash
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMinStakeNow {} --abi contracts/bksystem/BlockKeeperContractRoot.abi.json
  ```

  ```bash
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMaxStakeNow {} --abi contracts/bksystem/BlockKeeperContractRoot.abi.json
  ```

  You will need the ABI file [BlockKeeperContractRoot.abi.json](https://raw.githubusercontent.com/ackinacki/ackinacki/fa3c2685c5efaaded16aa370066a39ea12d0f899/contracts/bksystem/BlockKeeperContractRoot.abi.json) to run these commands.


### Prerequisites
- BK Node Owner keys file
- BLS keys file

The BLS keys file can be found in the `{{ BK_DIR }}/bk-configs/` directory, where `BK_DIR` is a variable defined in the Ansible inventory.
The BLS keys file format - `block_keeper{{ NODE_ID }}_bls.keys.json`

The staking script requires the following parameters:
- **STAKING_IMAGE** – the Docker image used to run the staking script
- **STAKING_TIME (in seconds)** – defines the interval between staking script executions. Use 120 seconds for `Shellnet`.
- **TVM_ENDPOINT** – the TVM endpoint for connecting to enable staking

Specify these parameters in the inventory file.

ℹ️ **Note:**
  Ensure that the BLS keys file has both read and write permissions.

### Run the Script
All neccessary keys are passed to staking container iside Docker compose file.

```bash
docker compose up -d staking
```

### Graceful Shutdown for Staking
The staking process supports a graceful shutdown.

During shutdown:
* The continue staking option will be disabled;
* Any ongoing continue staking requests will be automatically canceled;
* The staked tokens from these canceled requests will be returned to your wallet.

1. **Disable upcoming stakes**

Disabling upcoming stakes ensures that no new Epoch will start while you shut down.

* Change to the Block Keeper directory:
```bash
cd {{ ROOT_DIR }}/block-keeper
```

* Send `SIGHUP` to the staking process:
```bash
sudo docker compose exec staking /bin/bash -c 'pkill --signal 1 -f staking.sh'
```

You should see log entries similar to:
```
[2025-01-01T00:00:00+00:00] SIGHUP signal has been recieved. Disabling continue staking
[2025-01-01T00:00:00+00:00] Active Stakes - "0xe5651dad30f31448443a894b81260d49caa13879ab9aee2dbe648fea3c85565c"
[2025-01-01T00:00:00+00:00] Stakes count - 1
[2025-01-01T00:00:00+00:00] Epoch in progress - "0xe5651dad30f31448443a894b81260d49caa13879ab9aee2dbe648fea3c85565c"
```

After that script will not send continue staking and you can shutdown staking service. During shutdown the script touches the active Epoch (if any) and returns any excess tokens.

ℹ️ **Note:**
  **Do not worry** if the log reports that an Epoch **“is being continued”**—this means it had already started before you sent the `SIGHUP` signal. There is **no need to send** `SIGHUP` again. Simply wait for the active epoch to complete, as described in the next step.

  Example:
  ```
  [2025-01-01T00:00:00+00:00] Epoch with address "0:4744f0af70056f31183c24c980fa624ef33ed031fbab51c2d20bb20000615c98" is being continued: true
  ```

2. **Pick the right moment to stop**

To avoid losing rewards and your reputation score when stopping staking, you must wait for the current epoch to end before stopping the staking service. Therefore, before shutting down the container, make sure the current Epoch is ready to be finished.

* Find `seqNoFinish` for the Epoch that is still active:

```bash
tvm-cli -j runx --abi contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json --addr <BK_NODE_WALLET_ADDR> -m getDetails| jq -r '.activeStakes[] | select(.status == "1") | .seqNoFinish'
```

You will need the ABI file [AckiNackiBlockKeeperNodeWallet.abi.json](https://raw.githubusercontent.com/ackinacki/ackinacki/refs/heads/main/contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json) to run this command.

ℹ️ **Note:**
  The address of the BK node wallet can be retrieved from the logs,
  or by calling the `getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey)` method in the `BlockKeeperContractRoot` system contract,  where
  `pubkey` - the public key of your BK node wallet.

  Example:
  ```bash
  tvm-cli -j runx --abi ../contracts/bksystem/BlockKeeperContractRoot.abi.json --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getAckiNackiBlockKeeperNodeWalletAddress '{"pubkey": "0x1093c528ac2976c6b7536ef25e1c126db9dc225f77cd596d2234613eb9cad9b9"}'
  ```

* Get the current network `seq_no`:
```bash
tvm-cli -j query-raw blocks seq_no --limit 1 --order '[{"path":"seq_no","direction":"DESC"}]' | jq '.[0].seq_no'
```
🚨 **Important:**
  **When the current `seq_no` is greater than `seqNoFinish`, the active Epoch is already complete and you can stop the container safely.**

3. **Additional method to check readiness for Shutdown**

You can also verify whether it's safe to stop staking by calling the `getDetails()` method on the Block Keeper wallet contract and inspecting the `activeStakes` field.

* If any stakes have status `0` or `1`, do not stop the staking process.
These statuses indicate that the stake is either in the Pre-Epoch phase (0) or the active Epoch (1).

* You can proceed with the shutdown only when all stakes have status `2` (i.e., the `Cooler` phase has started) or when the `activeStakes` list is empty.

To check the current state:
```bash
tvm-cli -j runx --abi contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json --addr <BK_NODE_WALLET_ADDR> -m getDetails
```

Example output when there are no active stakes:
```bash
{
  "pubkey": "0x15538499c83886367c07b0d30a8446bd6487da6c8a1da9bf5658ec01864a6fb3",
  "root": "0:7777777777777777777777777777777777777777777777777777777777777777",
  "balance": "0x000000000000000000000000000000000000000000000000000003fcd6cf278b",
  "activeStakes": {},
  "stakesCnt": "0",
  "licenses": {
    "0x0000000000000000000000000000000000000000000000000000000000000000": {
      "reputationTime": "129",
      "status": "0",
      "isPrivileged": false,
      "stakeController": null,
      "last_touch": "1751275067",
      "balance": "0",
      "lockStake": "0",
      "lockContinue": "0",
      "lockCooler": "0",
      "isLockToStake": false
}
```
4. **Stop the staking container**

To gracefully stop the staking container, run:
```bash
docker compose down staking --timeout 60
```

Where:
* `timeout` sets the graceful shutdown timeout in seconds.

Example shutdown log excerpt:
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

## Check Node Status
To get the node's status by blocks, use the `node_sync_status.sh` script:

```bash
node_sync_status.sh path/to/log
```

## Changing the IP Address of a BK Node
To move a Block Keeper (BK) node to a new IP address without losing your reputation score,
follow these rules:
* Wait until the current Epoch ends and do not apply for the next one.
* Start staking on the new node within half an Epoch after the current Epoch finishes.

Step‑by‑Step Guide:

### 1. Calculate the Time You Have for Migration
#### 1.1  Get the Current Epoch Contract Address

* Find the address of the currently Epoch:
```bash
tvm-cli -j runx --abi contracts/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json --addr <BK_NODE_WALLET_ADDR> -m getEpochAddress| jq -r -e '.epochAddress'
```

ℹ️ **Note:**
  The address of the BK node wallet can be retrieved from the logs,
  or by calling the `getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey)` method in the `BlockKeeperContractRoot` system contract,  where:
  * `pubkey` - the public key of your BK node wallet.

  Example:
  ```bash
  tvm-cli -j runx --abi ../contracts/bksystem/BlockKeeperContractRoot.abi.json --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getAckiNackiBlockKeeperNodeWalletAddress '{"pubkey": "0x1093c528ac2976c6b7536ef25e1c126db9dc225f77cd596d2234613eb9cad9b9"}'
  ```

#### 1.2  Determine the Epoch Length
Query `getDetails` on the Epoch contract to obtain its approximate duration in seconds:
```bash
tvm-cli -j run 0:6338d20a991247617c6beb9e22b0c36fa8f40d290c0e73f543e94f2ee43025fb \
--abi contracts/bksystem/BlockKeeperEpochContract.abi.json \
getDetails {} \
| jq -r '((.seqNoFinish - .seqNoStart) * 3 / 10 | tostring) + " seconds"'
```

🚨 **Important:**
  **You must launch staking on the new node in less than half of this time.**

### 2. Stop Staking (Graceful Shutdown)
To avoid losing rewards and reputation, wait for the current Epoch to end before stopping staking.
Use the procedure described in the [Graceful Shutdown for Staking](#graceful-shutdown-for-staking) section.

### 3. Stop Node (Graceful Shutdown)
Use the procedure described in the [Graceful Shutdown of a BK node](#graceful-shutdown-of-a-bk-node) section.

### 4. Deploy BK Software on the New Node

ℹ️ **Note:**
  You must reuse your existing key files (`BK Node Owner keys`, `BLS keys`) and the same `BK Wallet`.

Update the node’s IP address in your Ansible inventory.
Follow the instructions in [BK Deployment with Ansible](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#block-keeper-deployment-with-ansible).

Run the playbook:
```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml
```

### 5. Start Staking on the New IP Address
Follow the [staking launch guide](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#staking)

### 6. Check Node Status
Your BK node should now be operating on the new IP address. To get the node's status by blocks, use the `node_sync_status.sh` script:

```bash
node_sync_status.sh path/to/log
```

# Block Manager Documentation

## System Requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 4c/8t       | 32        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 8c/16t      | 64        | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

**Steps**

1. Identify the Block Keeper your Block Manager service will work with and request the following information from its owner:
  * Authorization Token (`BK_API_TOKEN`) – the access token required for interacting with the BK API.
  * BK Public IP Address (`NODE_IP`)
2. Deploy Block Manager Wallet
2. Prepare an Ansible inventory file for Block Manager deployment, specifying all required variables (see example below).
3. Run the Ansible playbook against your inventory to deploy the Block Manager node.

## Deployment with Ansible

### Prerequisites

* A dedicated server for the Block Manager with SSH access.
* Docker with the Compose plugin installed.

### Generate Keys and Deploy Block Manager Wallet

Before deploying a Block Manager, you must create two key pair files and then deploy the Block Manager wallet:
* One key pair will be used to operate the Block Manager wallet.
* The other will be used to sign external message authentication tokens.

```bash
tvm-cli genphrase --dump block_manager.keys.json
tvm-cli genphrase --dump block_manager_signing.keys.json
```

Specify these keys to your inventory file.
To deploy the Block Manager wallet, extract your keys from the files created above and run:

```bash
tvm-cli --abi ../contracts/bksystem/BlockManagerContractRoot.abi.json --addr 0:6666666666666666666666666666666666666666666666666666666666666666 -m deployAckiNackiBlockManagerNodeWallet '{"pubkey": "0xYOUR_PUB_KEY", "signerPubkey": "0xYOUR_SIGNING_KEY", "whiteListLicense": {"YOUR_LICENSE_NUMBER": true}}'
```

### Create an Ansible Inventory

Below is a sample inventory for Block Manager deployment.
Make sure to include the b`lock_manager` host group.

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    ROOT_DIR: /home/user/deployment # path to store deployment files
    MNT_DATA: /home/user/data       # path to store data
    BM_IMAGE: "teamgosh/ackinacki-block-manager:<latest-release-tag>" # i.e. teamgosh/ackinacki-block-manager:v0.3.3
    GQL_IMAGE: "teamgosh/ackinacki-gql-server:<latest-release-tag>"   # i.e. teamgosh/ackinacki-gql-server:v0.3.3
    NGINX_IMAGE: "teamgosh/ackinacki-nginx"
    BM_DIR: "{{ ROOT_DIR }}/block-manager"
    BM_DATA_DIR: "{{ MNT_DATA }}/block-manager"
    BM_LOGS_DIR: "{{ MNT_DATA }}/logs-block-manager"
    BM_WALLET_KEYS: block_manager.keys.json
    BM_SIGNER_KEYS: block_manager_signing.keys.json
    BK_API_TOKEN: my-secret-token # access token required for the BK API
    LOG_ROTATE_AMOUNT: 30
    LOG_ROTATE_SIZE: 1G

block_manager:
  hosts:

    YOUR-BM-HOST:
      NODE_IP: NODE_IP_ADDRESS         # BK node address for block streaming.
      HOST_PUBLIC_IP: HOST_IP_PUBLIC   # public IP for external access
      HOST_PRIVATE_IP: HOST_IP_PRIVATE # private IP for internal network access
```

### Test the Ansible Playbook

To validate your inventory and playbook syntax, use dry run and check mode:

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml --check --diff
```

### Run the Ansible Playbook

If validation passes, run the playbook to deploy the Block Manager:

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml
```

### Verify the Deployment

To check running Docker containers:

```bash
docker ps
#OR
docker compose ps
```

Ensure the following containers are running (status: UP):

```
teamgosh/ackinacki-block-manager
teamgosh/logrotate
teamgosh/ackinacki-nginx
teamgosh/ackinacki-gql-server
```

To follow Block Manager logs:

```bash
tail -f $MNT_DATA/logs-block-manager/block-manager.log
```

# Proxy Documentation

**Broadcast Proxy** is a specialized network service designed to optimize block exchange between participants in the Acki Nacki network. Its primary purpose is to reduce the overall network traffic between nodes operated by different Node Providers, while also improving the network’s scalability and stability.

Read more about [the motivation behind the Proxy here](https://docs.ackinacki.com/protocol-participation/proxy-service#why-everyone-should-connect-via-a-proxy).

## System Requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage      | Network                                            |
| ------------- | ----------- | --------- | ------------ | -------------------------------------------------- |
| Recommended   | 8c/16t      | 32        | 500 GB NVMe  | 1Gb *(multiply) the number of nodes behind Proxy. For example, for 100 nodes behind a proxy, the required bandwidth is 100 Gb. |

🚨 **Important:**
  To ensure stable operation, proxy servers must be deployed in pairs (one master and one failover).

## Deployment with Ansible

### Prerequisites
* A dedicated server for the Proxy.
* Docker with the Compose plugin installed.
* A file containing the Block Keeper Node Owner keys (required to sign the TLS certificate).
* Ansible for deployment

ℹ️ **Note:**
  The TLS certificate for the Proxy will only be accepted by other Block Keepers after the Block Keeper whose keys were used to generate this certificate has joined the BK‑set.

### How Deployment Works

* All deployment is automated with Ansible.
  You can learn [how to deploy the Proxy without Ansible here](https://github.com/ackinacki/ackinacki/blob/main/proxy/README.md).
* Docker Compose files and Proxy configuration are generated automatically.
* Since the QUIC+TLS protocol is used to communicate with the Proxy, the Proxy requires a TLS key and a TLS certificate.
  These will also be created automatically during deployment.
  For more details on [how the certificates are generated, see the link](https://github.com/ackinacki/ackinacki/tree/main/proxy#tls-key-and-certificate-generation).
* Place BK Node Owner keys to `ansible` folder
* You only need to prepare an Ansible inventory with the required variables ([see example below](#prepare-your-inventory-2)).

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

ℹ️ **Note:**
  `PROXY_BIND` and `PROXY_ADDR` must be set to the same IP address.

🚨 **Important:**
You must add Proxy addresses to the Block Keeper configuration. Otherwise, the Block Keeper will not be able to connect to the Proxy.

Example:
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

### Run the Ansible Playbook

If everything looks correct, proceed with the actual deployment:

```bash
ansible-playbook -i test-inventory.yaml ansible/deploy-proxy.yml
```

To view the Proxy logs:

```bash
tail -f $PROXY_DIR/logs-proxy/proxy.log
```
