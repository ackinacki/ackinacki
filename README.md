# Acki Nacki Instructions

- [Acki Nacki Instructions](#acki-nacki-instructions)
- [Block Keeper System Requirements](#block-keeper-system-requirements)
- [(Completed) Joining the Acki Nacki Network from the Genesis Block](#completed-joining-the-acki-nacki-network-from-the-genesis-block)
  - [Stage 1. Preparation Checklist](#stage-1-preparation-checklist)
    - [For License Owner](#for-license-owner)
    - [For Node Owner](#for-node-owner)
  - [Stage 2. Zerostate Formation](#stage-2-zerostate-formation)
  - [Stage 3. Launch After the GOSH Team Signal](#stage-3-launch-after-the-gosh-team-signal)
- [(Current Phase) Joining the Acki Nacki Network After Launch](#current-phase-joining-the-acki-nacki-network-after-launch)
  - [Join As a License Owner](#join-as-a-license-owner)
  - [Join As a BK Node Owner](#join-as-a-bk-node-owner)
- [Block Keeper Documentation](#block-keeper-documentation)
  - [Block Keeper Wallet Deployment](#block-keeper-wallet-deployment)
    - [Prerequisites](#prerequisites)
    - [Configure tvm-cli](#configure-tvm-cli)
    - [Deploy](#deploy)
    - [Updating the license whitelist in the BK Node Wallet](#updating-the-license-whitelist-in-the-bk-node-wallet)
  - [Delegating License](#delegating-license)
  - [Block Keeper Deployment with Ansible](#block-keeper-deployment-with-ansible)
    - [Prerequisites](#prerequisites-1)
    - [Key Variables](#key-variables)
    - [Prepare Your Inventory](#prepare-your-inventory)
    - [Run the Ansible Playbook](#run-the-ansible-playbook)
    - [Run Multiple BK Nodes on a Single Server](#run-multiple-bk-nodes-on-a-single-server)
  - [Block Keeper Node Management](#block-keeper-node-management)
    - [Graceful Shutdown of a BK Node](#graceful-shutdown-of-a-bk-node)
    - [Upgrading Block Keeper to a new image or configuration](#upgrading-block-keeper-to-a-new-image-or-configuration)
    - [Stopping node with data cleanup](#stopping-node-with-data-cleanup)
    - [Starting back a stopped node without configuration changes](#starting-back-a-stopped-node-without-configuration-changes)
  - [Running Staking with Ansible](#running-staking-with-ansible)
    - [Prerequisites](#prerequisites-2)
    - [Run the Script](#run-the-script)
    - [Graceful Shutdown for Staking](#graceful-shutdown-for-staking)
    - [Debug](#debug)
  - [Check Node Status](#check-node-status)
  - [How to Migrate BK to Another Server](#how-to-migrate-bk-to-another-server)
    - [1. Preparing the New Server](#1-preparing-the-new-server)
      - [1.1 Server Configuration](#11-server-configuration)
      - [1.2 Copying Keys](#12-copying-keys)
      - [1.3 Updating the Ansible Inventory File](#13-updating-the-ansible-inventory-file)
    - [2. Stopping the Old Server](#2-stopping-the-old-server)
      - [2.1 Proper Node Shutdown](#21-proper-node-shutdown)
      - [2.2 Stopping the Node](#22-stopping-the-node)
    - [3. Starting the Node on the New Server](#3-starting-the-node-on-the-new-server)
      - [3.1 Updating the Node IP address](#31-updating-the-node-ip-address)
      - [3.2 Restoring the Keys](#32-restoring-the-keys)
    - [4. Post-launch Checks](#6-post-launch-checks)
      - [4.1 Docker Container Check](#41-docker-container-check)
      - [4.2 Node Log Check](#42-node-log-check)
      - [4.3 Node Synchronization Status Check](#43-node-synchronization-status-check)
  - [Troubleshooting](#troubleshooting)
    - [Recovery from Corrupted BK State](#recovery-from-corrupted-bk-state)
    - [Error with sending continue stake request](#error-with-sending-continue-stake-request)
- [Block Manager Documentation](#block-manager-documentation)
  - [System Requirements](#system-requirements)
  - [Deployment with Ansible](#deployment-with-ansible)
    - [Prerequisites](#prerequisites-3)
    - [Generate Keys and Deploy the Block Manager Wallet](#generate-keys-and-deploy-the-block-manager-wallet)
      - [Manually (applies if you don’t have a license number)](#manually-applies-if-you-dont-have-a-license-number)
      - [Using the Script (if you already have a license number)](#using-the-script-if-you-already-have-a-license-number)
    - [Create an Ansible Inventory](#create-an-ansible-inventory)
    - [Test the Ansible Playbook](#test-the-ansible-playbook)
    - [Run the Ansible Playbook](#run-the-ansible-playbook-1)
    - [Verify the Deployment](#verify-the-deployment)
  - [Updating the Whitelist and Delegating the License to the BM Wallet](#updating-the-whitelist-and-delegating-the-license-to-the-bm-wallet)
- [Proxy Documentation](#proxy-documentation)
  - [System Requirements](#system-requirements-1)
  - [Deployment with Ansible](#deployment-with-ansible-1)
    - [Prerequisites](#prerequisites-4)
    - [How Deployment Works](#how-deployment-works)
    - [Key Variables](#key-variables-1)
    - [Prepare Your Inventory](#prepare-your-inventory-1)
    - [Run the Ansible Playbook](#run-the-ansible-playbook-2)
    - [Upgrading proxy configuration or image](#upgrading-proxy-configuration-or-image)

# Block Keeper System Requirements

| Component     | Requirements                                                                 |
| ------------- | -----------                                                                  |
| CPU (cores)   | 16 dedicated physical cores on a single CPU, or 16 vCPUs ≥ 2.4 GHz. Hyper-threading must be disabled.                                                                                      |
| RAM (GiB)     | 128 GB                                                                       |
| Storage       | 1 TB of high performance NVMe SSD (PCIe Gen3 with 4 lanes or better)         |
| Network       | The effective bandwidth may be limited to 1 Gbps full-duplex total traffic, meaning no more than 1 Gbps in each direction (ingress and egress). A 2.5 Gbps or better full-duplex network interface card (NIC) should be installed to support anticipated future load increases and avoid hardware replacement.                                                      |

# (Completed) Joining the Acki Nacki Network from the Genesis Block

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

The Gosh team has announced the exact network launch time ([day, hour, minute](https://t.me/ackinackinews/472)).
So, either set up an automatic launch of the deployment scripts and staking on each server where the Igniter was running,
or launch them manually at the specified time
  - [BK deployment instructions ](#block-keeper-deployment-and-running-staking-with-ansible)
  - [Proxy deployment instructions](#proxy)

🚨 **Important:**
The BK and Proxy IP addresses must match those used when starting the Igniter.

You can change a node’s ip-adress after the network launch.
[see instruction](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#changing-the-ip-address-of-a-bk-node)

*A guide for Proxy IP migration will be added soon.*

# (Current Phase) Joining the Acki Nacki Network After Launch

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

🚨 **Important:**
**If you delegate to your own BK node, keep in mind that you have only half an epoch to submit the stake.**

ℹ️ **Note:**
To check your rewards:
* Call `getDetails` in the BK wallet (see the balance field in the license mapping)  
[You can find more details here](https://docs.ackinacki.com/for-node-owners/protocol-participation/block-keeper/license/working-with-licenses)

* You can also view your rewards using the [dashboard](https://dashboard.ackinacki.com).

🚨 **Important:**
Condition for receiving the maximum reward: continuous operation and not locking the license on the wallet.

## Join As a BK Node Owner

**Step 1.**
To deploy a BK wallet with a whitelist (license numbers received from License Owners), [follow this guide](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#block-keeper-wallet-deployment).

ℹ️ **Note:**
The whitelist [can be updated](#updating-the-license-whitelist-in-the-bk-node-wallet) at any time.

**Step 2.**
Share the BK wallet’s public key with the License Owner so they can delegate their licenses.

**Step 3.**
If you run multiple nodes and your network card is under 2 Gbps, set up a Proxy service [using the instruction](https://docs.ackinacki.com/protocol-participation/proxy-service) for stable operation, or use existing deployed proxies.

**Step 4.**
Start the node and staking:
After at least one license is delegated, the Node Owner deploys BK software and starts staking on the server [using the instruction](https://github.com/ackinacki/ackinacki?tab=readme-ov-file#block-keeper-deployment-with-ansible).

🚨 **Important:**
After delegating the license to the BK wallet, you have **only half an epoch to submit the stake**.

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

### Updating the license whitelist in the BK Node Wallet

The Node Owner can update the license whitelist at any time. However, the changes will only take effect at the beginning of the next Epoch.

To do this, call the `setLicenseWhiteList(mapping(uint256 => bool))` method in your Block Keeper Node Wallet contract, passing the license numbers received from the License Owners.

Where:
* `uint256 (key)` – the license number;
* `bool (value)` – set to `true` to **add** the license on the whitelist, or `false` to **remove** it.

Example command:
```shell
tvm-cli call <BK_NODE_WALLET_ADDR> setLicenseWhiteList \
  '{"whiteListLicense": {"1": true, "2": true, "5": true}}' \
  --abi contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json \
  --sign BK_NODE_OWNER_KEYS
```

## Delegating License

Before starting staking, a node must have at least one delegated license.
However, no more than 20 (twenty) licenses can be delegated to a single node.

Learn more about [working with licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses).

If the BK Node Owner is also a License Owner, they must use the `addBKWallet(uint256 pubkey)` method in the [`License`](https://github.com/ackinacki/ackinacki/blob/main/contracts/0.79.3_compiled/bksystem/License.sol) contract to delegate their licenses to their node.
(This must be done for each license contract).

Where:
* `pubkey` – the public key of the BK node wallet.
* [`License.abi.json`](https://github.com/ackinacki/ackinacki/blob/main/contracts/0.79.3_compiled/bksystem/License.abi.json) – the ABI of the License contract.
* `License.keys.json` – the keys obtained by the License Owner during registration in the dashboard.

For example:
```bash
tvm-cli -j callx --addr 0:7f2f945faaae4cce286299afe74dac9460893dd5cba1ac273b9e91f55f1141ec --abi contracts/0.79.3_compiled/bksystem/License.abi.json --keys license_onwer/license.keys.json --method addBKWallet '{"pubkey": "0xfa4edc8b63c4e66241a57c11e0a522769ca4a4f106692512fc92f2d658169bcc"}'
```

🚨 **Important:**
**After delegating the license to the BK wallet, you have only half an epoch to submit the stake.**

## Block Keeper Deployment with Ansible

### Prerequisites

- SSH access to servers.
- Docker with the compose plugin must be installed.

### BK DNS Name Generation

To register a DNS name for your BK IP, you need to create an account on ZeroSSL
([https://zerossl.com/features/acme/](https://zerossl.com/features/acme/)).

After registration, generate the ACME credentials:

* EAB KID
* HMAC key

Add the obtained values to the inventory into the `CADDY_EAB` variable.

If you have multiple BKs on a single IP address, generate a dedicated DNS name for each BK.
It is recommended to add the **NODE_ID** prefix to the DNS name.

For example:  
```
x{{ (NODE_ID | string)[:6] }}.your-domain // your-bk-dns
```

### Prepare Your Inventory

Here is a basic inventory for `Shellnet` node deployment:

```yaml
all:
  vars:

    NODE_OWNER_KEY: PATH_TO_NODE_OWNER_KEY        # file name pattern "block_keeper{{ NODE_ID }}.keys.json"

    NODE_GROUP_ID: "your-group-id"
    OTEL_COLLECTOR: "your otel collector url"

    # the access token required for interacting with the BK API
    AUTH_TOKEN: my-secret-token    # replace it with some random string that only you know, required for direct SDK access

    ansible_port: 22
    ansible_user: ubuntu

    ROOT_DIR: /home/user/deployment   # path to store deployment files

    MNT_DATA: /home/user/data         # path to store node data and logs

    BK_FOLDER_NAME: "block-keeper"

    IMAGE_VERSION: "v0.13.10"   # Node patch version for the update
    NETWORK_NAME: "network-name"             # i.e. shellnet
    UPD_NETWORK_NAME: "network-name"    
    OTEL_SERVICE_NAME: "{{ NETWORK_NAME }}"

    TVM_ENDPOINT: "network-tvm-endpoint"     # shellnet network endpoint is "https://shellnet.ackinacki.org" shellnet.ackinacki.org

    BK_HOSTNAME: "your-bk-dns"     # specify in the following format x{{ (NODE_ID | string)[:6] }}.your-domain // your-bk-dns

    GOSSIP_SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000

    PROXIES: # delete this section if the BK operates without a Proxy
      - PROXY_IP:8085   # REPLACE WITH ACTUAL IP ADDRESS

    # Total usage may be higher if logs fill too quickly, but baseline is multiplying the two vars
    LOG_ROTATE_AMOUNT: 20   # maximum number of log files to keep
    LOG_ROTATE_SIZE: 5G     # minimum size of the log file to rotate
    LOG_ROTATE_SPEC: "*/2 *"  # period of rotation in cron "minute hour" format

    # can be changed if there are conflicting ports, but in general should be left 0
    PORT_OFFSET: 0

    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"

    NODE_CONFIGS:
      - "zerostate"
    
    AS_VERSION: "8.1.0.1"

    BIND_PORT: '{{ 8500 + PORT_OFFSET }}'                     # this port must be open
    BIND_API_PORT: '{{ 8600 + PORT_OFFSET }}'                 # this port must be open
    BIND_MESSAGE_ROUTER_PORT: '{{ 8700 + PORT_OFFSET }}'
    BIND_GOSSIP_PORT: '{{ 10100 + PORT_OFFSET }}'             # this port must be open
    BLOCK_MANAGER_PORT: '{{ 12000 + PORT_OFFSET }}'
    AEROSPIKE_PORT: '{{ 4000 + PORT_OFFSET * 10 }}'           # Aerospike DB listening port
    AEROSPIKE_FABRIC_PORT: '{{ 4001 + PORT_OFFSET * 10 }}'    # port for intra-cluster communication (migration, replication, etc.)
    AEROSPIKE_HEARTBEAT_PORT: '{{ 4002 + PORT_OFFSET * 10 }}' # port used to maintain database cluster health (heartbeat)

    NODE_IMAGE: teamgosh/ackinacki-node:<latest-release-tag>      # i.e. teamgosh/ackinacki-node:v0.3.3
    STAKING_IMAGE: teamgosh/ackinacki-staking:<latest-release-tag>
    AEROSPIKE_IMAGE: "aerospike/aerospike-server:latest"/AEROSPIKE_IMAGE: "aerospike/aerospike-server:{{ AS_VERSION }}"

    HOST_PRIVATE_IP: 127.0.0.1 # default, if no private IP

    EXT_MESSAGE_AUTH_REQUIRED: yes

    THREAD_COUNT_SOFT_LIMIT: 1
    THREAD_LOAD_THRESHOLD: 20000
    MIN_TIME_BETWEEN_STATE_PUBLISH_DIRECTIVES: 1200s
    NODE_JOINING_TIMEOUT: 600s
    STAKING_TIME: 420

    # Remove this variable from the inventory before upgrading nodes to avoid unnecessary steps.
    # However, it is required when starting new nodes without existing data,
    # especially if they are running behind your own proxy servers.
    BOOTSTRAP_BK_SET_URL: "BK_HOSTNAME:BIND_API_PORT/v2/bk_set_update"   # shellnet bootstrap url is "http://shellnet0.ackinacki.org:8600/v2/bk_set_update"

block_keepers:
  hosts:
    YOUR-NODE-ADDRESS:   # the host address where the deployment will be performed (domain or IP).
      NODE_ID: NODE_ID   # Node Owner wallet address (exactly 64 hexademical characters, without 0x or 0: prefix)
      HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-IP-ADDRESS
      HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-IP-ADDRESS  # remove if no private interface available

block_keeper_tls_proxy:
  vars:
    LOAD_CERT: false
    ENABLE_GEN_CERT: true
    ZEROSSL_EXCLUSIVE: yes
    CADDY_EAB: "<your kid> <your hmac key>" # <<<<< <<<<< Replace with actual values! <<<<< <<<<<
  hosts:
    YOUR-NODE-ADDRESS:
      HOST_PUBLIC_IP: "YOUR-NODE-PUBLIC-IP-ADDRESS" # BKs must be reachable via this IP and BK_PORT set below
      block_keepers:
        - x111111.mainbk.ackinacki.org: # first 6 characters of a node id of bk on the host
            BK_PORT: 8600               # BIND_API_PORT of that BK
```

### Key Variables

`NODE_ID`: The unique identifier of the Block Keeper within the network. It corresponds to the BK wallet address without the `0:` prefix. This identifier is required when creating a new Block Keeper.

`NODE_GROUP_ID` identifies the node group and should be the same across all nodes in your deployment.

`NODE_GROUP_ID`, `OTEL_COLLECTOR`, and `OTEL_SERVICE_NAME` are optional and related to node metrics integration.
They can be used to send metrics to a specified collector server. 

`AUTH_TOKEN`: This token is used to authorize access to the BK API. You can specify any arbitrary string.

`BK_HOSTNAME`: The DNS name of your BK node by which it should be accessible. It is also used to deploy the BK TLS proxy.
Specify in the following format:
```
x{{ (NODE_ID | string)[:6] }}.your-domain // your-bk-dns
```

`PROXIES`: List the IP addresses of the Proxies that will be used to broadcast blocks across the network.
**If the Block Keeper will operate without a Proxy, this variable is not required.**

ℹ️ **Note**:
  **If you want to deploy a Proxy, refer to the [Proxy deployment guide](#proxy) or use an existing Proxy service.**
  To use an existing Proxy, please contact the corresponding Proxy provider representative for details.

`LOG_ROTATE_SPEC` defines the log rotation schedule in cron format (`minute hour`).

For example, the default value `"0 *"` means that log files will be rotated every hour at the 0th minute.

If rotated log files are too large, you may want to shorten the rotation period.

For example, the value `"*/5 *"` means that log files will be rotated every 5 minutes.

`BOOTSTRAP_BK_SET_URL` should be set to the provided URL to correctly retrieve the initial BK set, ensuring that the node connects properly to the network and proxy server. When upgrading, this variable should be removed to avoid unnecessary logic execution.

`HOST_PUBLIC_IP`: The public IP address of the host. Make sure that the ports do not conflict with other services.

`HOST_PRIVATE_IP`: Specify the private IP address of the host, (e.g., `127.0.0.1`). Ensure that the ports do not overlap.

ℹ️ **Note**:
  **Providing Access Information to the Block Manager**
  To enable proper interaction with the Block Manager (BM), you must provide the BM Owner with the following information:
  * Authorization Token (`AUTH_TOKEN`) – the access token required for interacting with the BK API.
  * Node Public IP Address (`HOST_PUBLIC_IP`) – the public IP address of your node.

### Run the Ansible Playbook

Make sure to add your configuration data to the inventory before running the playbook.

For testing, you can use an Ansible dry run:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-deployment.yaml --check --diff
```

If everything looks good, run Ansible:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-deployment.yaml
```

Upon completion of the script, BLS keys will be generated and saved in the file `block_keeper{{ NODE_ID }}_bls.keys.json` in the `{{ BK_DIR }}/bk-configs/` folder on the remote server, along with the node.

**`BLS keys`** - the keys used by Block Keeper (BK) to sign blocks. The keys have a lifespan of one Epoch. New BLS keys are generated during restaking. Each BK maintains a list of BLS public keys from other BKs (for the current Epoch), which are used to verify attestations on blocks.

🚨 **Important:**
**Back up your BLS keys every time they are regenerated.**

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
teamgosh/ackinacki-staking
aerospike/aerospike-server
stakater/logrotate
```

Check node logs

```bash
tail -f $MNT_DATA/logs-block-keeper/node.log
```

### Run Multiple BK Nodes on a Single Server

**Important:**
Make sure your server has enough resources to run multiple BK nodes.

To run multiple BK nodes on a single server, assign each BK node to a different ports and create a separate Docker Compose configuration for each one.
A single inventory file can be used.

Example inventory file for running two BK nodes on a single server (`Shellnet`):

```yaml
all:
  vars:

    NODE_OWNER_KEY: PATH_TO_NODE_OWNER_KEY        # file name pattern "block_keeper{{ NODE_ID }}.keys.json"

    NODE_GROUP_ID: "your-group-id"
    OTEL_COLLECTOR: "your otel collector url"

    # the access token required for interacting with the BK API
    AUTH_TOKEN: my-secret-token    # replace it with some random string that only you know, required for direct SDK access

    ansible_port: 22
    ansible_user: ubuntu

    ROOT_DIR: /home/user/deployment   # path to store deployment files

    MNT_DATA: /home/user/data         # path to store node data and logs

    BK_FOLDER_NAME: "block-keeper-{{ (NODE_ID | string)[:6] }}"

    IMAGE_VERSION: "v0.13.10"   # Node patch version for the update
    NETWORK_NAME: "network-name"             # i.e. shellnet
    UPD_NETWORK_NAME: "network-name"    
    OTEL_SERVICE_NAME: "{{ NETWORK_NAME }}"

    TVM_ENDPOINT: "network-tvm-endpoint"     # shellnet network endpoint is "https://shellnet.ackinacki.org" shellnet.ackinacki.org

    BK_HOSTNAME: "your-bk-dns"     # specify in the following format x{{ (NODE_ID | string)[:6] }}.your-domain // your-bk-dns

    GOSSIP_SEEDS:
      - shellnet0.ackinacki.org:10000
      - shellnet1.ackinacki.org:10000
      - shellnet2.ackinacki.org:10000
      - shellnet3.ackinacki.org:10000
      - shellnet4.ackinacki.org:10000

    PROXIES: # delete this section if the BK operates without a Proxy
      - PROXY_IP:8085   # REPLACE WITH ACTUAL IP ADDRESS

    # Total usage may be higher if logs fill too quickly, but baseline is multiplying the two vars
    LOG_ROTATE_AMOUNT: 20   # maximum number of log files to keep
    LOG_ROTATE_SIZE: 5G     # minimum size of the log file to rotate
    LOG_ROTATE_SPEC: "*/2 *"  # period of rotation in cron "minute hour" format

    # can be changed if there are conflicting ports, but in general should be left 0
    PORT_OFFSET: 0 # for multinode deployment, change in specific host entry

    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"

    NODE_CONFIGS:
      - "zerostate"
    
    AS_VERSION: "8.1.0.1"

    BIND_PORT: '{{ 8500 + PORT_OFFSET }}'                     # this port must be open
    BIND_API_PORT: '{{ 8600 + PORT_OFFSET }}'                 # this port must be open
    BIND_MESSAGE_ROUTER_PORT: '{{ 8700 + PORT_OFFSET }}'
    BIND_GOSSIP_PORT: '{{ 10100 + PORT_OFFSET }}'             # this port must be open
    BLOCK_MANAGER_PORT: '{{ 12000 + PORT_OFFSET }}'
    AEROSPIKE_PORT: '{{ 4000 + PORT_OFFSET * 10 }}'           # Aerospike DB listening port
    AEROSPIKE_FABRIC_PORT: '{{ 4001 + PORT_OFFSET * 10 }}'    # port for intra-cluster communication (migration, replication, etc.)
    AEROSPIKE_HEARTBEAT_PORT: '{{ 4002 + PORT_OFFSET * 10 }}' # port used to maintain database cluster health (heartbeat)

    NODE_IMAGE: teamgosh/ackinacki-node:<latest-release-tag>      # i.e. teamgosh/ackinacki-node:v0.3.3
    STAKING_IMAGE: teamgosh/ackinacki-staking:<latest-release-tag>
    AEROSPIKE_IMAGE: "aerospike/aerospike-server:latest"/AEROSPIKE_IMAGE: "aerospike/aerospike-server:{{ AS_VERSION }}"

    HOST_PRIVATE_IP: 127.0.0.1 # default, if no private IP

    EXT_MESSAGE_AUTH_REQUIRED: yes

    THREAD_COUNT_SOFT_LIMIT: 1
    THREAD_LOAD_THRESHOLD: 20000
    MIN_TIME_BETWEEN_STATE_PUBLISH_DIRECTIVES: 1200s
    NODE_JOINING_TIMEOUT: 600s
    STAKING_TIME: 420

    # Remove this variable from the inventory before upgrading nodes to avoid unnecessary steps.
    # However, it is required when starting new nodes without existing data,
    # especially if they are running behind your own proxy servers.
    BOOTSTRAP_BK_SET_URL: "BK_HOSTNAME:BIND_API_PORT/v2/bk_set_update"   # shellnet bootstrap url is "http://shellnet0.ackinacki.org:8600/v2/bk_set_update"

block_keepers:
  hosts:

    YOUR-NODE-ADDRESS:   # the host address where the deployment will be performed (domain or IP).
      NODE_ID: NODE_ID   # Node Owner wallet address (exactly 64 hexademical characters, without 0x or 0: prefix)
      HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-IP-ADDRESS
      HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-IP-ADDRESS  # remove if no private interface available
    YOUR-NODE-ADDRESS-BK-2:   # MUST use BK_FOLDER_NAME for multinode deployment, see above
      ansible_host: SERVER_IP_TO_CONNECT
      NODE_ID: YET_ANOTHER_NODE_ID
      HOST_PUBLIC_IP: YOUR-NODE-PUBLIC-IP-ADDRESS
      HOST_PRIVATE_IP: YOUR-NODE-PRIVATE-IP-ADDRESS  # remove if no private interface available
      PORT_OFFSET: 1

block_keeper_tls_proxy:
  vars:
    LOAD_CERT: false
    ENABLE_GEN_CERT: true
    ZEROSSL_EXCLUSIVE: yes
    CADDY_EAB: "<your kid> <your hmac key>" # <<<<< <<<<< Replace with actual values! <<<<< <<<<<
  hosts:
    YOUR-NODE-ADDRESS:
      HOST_PUBLIC_IP: "YOUR-NODE-PUBLIC-IP-ADDRESS" # BKs must be reachable via this IP and BK_PORT set below
      block_keepers:
        - x111111.mainbk.ackinacki.org: # first 6 characters of a node id of BK on the host
            BK_PORT: 8600               # BIND_API_PORT of that BK
        - x222222.mainbk.ackinacki.org: # first 6 characters of a node id of BK-2 on the host
            BK_PORT: 8601               # BIND_API_PORT for BK-2
```

## Block Keeper Node Management

### Graceful Shutdown of a BK Node

To safely stop a Block Keeper (BK) node without risking data corruption, you can perform a graceful shutdown using the Ansible playbook.
This approach can also be used when you need to gracefully shut down multiple nodes at once.

Use the following command to do this:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-stopping.yml
```

Keep an eye on the output of the command to make sure that everything was successful and nothing timed out.

* Check the logs to ensure shutdown has completed.
You should see a message similar to:

```yaml
2025-08-21T11:21:08.761169Z TRACE ThreadId(01) monit: Shutdown finished
```

and no new logs should appear afterward.

### Upgrading Block Keeper to a new image or configuration

To upgrade the node or config vars to the new version, update the inventory correspondingly and use the following command:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-upgrading.yml
```

However, **it is strongly recommended** to check the output first:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-upgrading.yml --check --diff
```

Make sure that nothing in the docker compose or configuration files changed that was not intended to be changed. For example,
if you only upgrade the image, make sure that configuration variables have not changed.

### Stopping node with data cleanup

If you need to clean up a node database because of data corruption or if some upgrade explicitly requires it,
you can use the following command to stop the node and clean up the data:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-clean-data.yml
```

Afterward, you can use the [node upgrading script](#upgrading-block-keeper-to-a-new-image-or-configuration) or
[node startup script](#starting-back-a-stopped-node-without-configuration-changes) as necessary. If you have all
required keys on hand, then you can use [main BK deployment script](#run-the-ansible-playbook)
(using `--check --diff` first is recommended).

### Starting back a stopped node without configuration changes

If you stopped a node for some reason (with or without data clean), and want to just restart it without any configuration
changes (or if you do not have the necessary keys or other files on hand), you can use the following command:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-starting.yml
```

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
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMinStakeNow {} --abi contracts/0.79.3_compiled/bksystem/BlockKeeperContractRoot.abi.json
  ```

  ```bash
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMaxStakeNow {} --abi contracts/0.79.3_compiled/bksystem/BlockKeeperContractRoot.abi.json
  ```

  You will need the ABI file [BlockKeeperContractRoot.abi.json](https://raw.githubusercontent.com/ackinacki/ackinacki/fa3c2685c5efaaded16aa370066a39ea12d0f899/contracts/0.79.3_compiled/bksystem/BlockKeeperContractRoot.abi.json) to run these commands.


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
docker compose exec staking /bin/bash -c 'pkill --signal 1 -f staking.sh'
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
tvm-cli -j runx --abi contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json --addr <BK_NODE_WALLET_ADDR> -m getDetails| jq -r '.activeStakes[] | select(.status == "1") | .seqNoFinish'
```

You will need the ABI file [AckiNackiBlockKeeperNodeWallet.abi.json](https://raw.githubusercontent.com/ackinacki/ackinacki/refs/heads/main/contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json) to run this command.

ℹ️ **Note:**
  The address of the BK node wallet can be retrieved from the logs,
  or by calling the `getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey)` method in the `BlockKeeperContractRoot` system contract,  where
  `pubkey` - the public key of your BK node wallet.

  Example:
  ```bash
  tvm-cli -j runx --abi ../contracts/0.79.3_compiled/bksystem/BlockKeeperContractRoot.abi.json --addr 0:7777777777777777777777777777777777777777777777777777777777777777 -m getAckiNackiBlockKeeperNodeWalletAddress '{"pubkey": "0x1093c528ac2976c6b7536ef25e1c126db9dc225f77cd596d2234613eb9cad9b9"}'
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
tvm-cli -j runx --abi contracts/0.79.3_compiled/bksystem/AckiNackiBlockKeeperNodeWallet.abi.json --addr <BK_NODE_WALLET_ADDR> -m getDetails
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
      "isPrivileged": true,
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

## How to Migrate BK to Another Server

⚠️ Important conditions
* You must **reuse your existing key files** (`BK Node Owner keys` and `BLS keys`) and **the same `BK Wallet`**
* Use the **ZS version** that was used to deploy the mainnet
* **The migration must not cause the node to be out of staking for an extended period**  
  
  Therefore, before starting the migration:
  * configure the new server first; only when it is fully ready should you stop the old one and proceed with the migration;

  * It is recommended to start the migration after 25,000 blocks have passed since the beginning of the current Epoch (so that the Epoch has time to send a continue), giving you sufficient time to complete the process.
  
---
### 1. Preparing the New Server

#### 1.1 Server Configuration
* Provision a new server
* Install all required dependencies:  
  * Docker 
---
#### 1.2 Copying Keys

* Copy the BK Node Owner keys and BLS keys files from the old server to the new server, **place them into the appropriate directories using the same file names**.  
Alternatively, you can place the key files locally in the `ansible/files` directory.  
The paths to the key files can be found in the inventory file under the `NODE_OWNER_KEY` and `OTHER_KEYS` fields.

#### 1.3 Updating the Ansible Inventory File 

Ensure that all required configuration data is correctly specified in the Ansible inventory.

* Update the node’s IP address

> If the migration is performed without changing the IP address, make sure the inventory file matches the current configuration.

* If you are running multiple nodes on the same server, the `BK_DIR` folder name must be be unique for each node.  
For example:  
    ```
    BK_FOLDER_NAME: "block-keeper-{{ (NODE_ID | string)[:6] }}"
    ```

### 2. Stopping the Old Server

### 2.1 Proper Node Shutdown

📌 **Important:**
**The migration must not result in the node being removed from staking for an extended period.**  

Before stopping the node, make sure that the staking logs contain entries confirming that the current Epoch is in progress and has been continued:

```
There is active stake with epoch address "$EPOCH_ADDRESS"
```
```
Epoch with address "$EPOCH_ADDRESS" is being continued: true
```

### 2.2 Stopping the Node

📌 **Important:**  
**Proceed with stopping the old server only after the new server is fully prepared and the inventory file has been completed.**

On the **old server**, stop the BK node:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-stopping.yml
```

⏳ Wait until the node has fully stopped before continuing.

---
### 3. Starting the Node on the New Server

First, perform an **Ansible dry run**:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-deployment.yaml --check --diff
```

If everything looks correct, deploy the BK node:

```bash
ansible-playbook -i your-inventory.yaml ansible/node-deployment.yaml
```

⏳ Wait for:

* all Docker containers to start
* the node to fully synchronize with the network

---
### 4. Post-launch Checks

#### 4.1 Docker Container Check

```bash
docker ps
# OR
docker compose ps
```

Ensure that the all containers are in the **UP** state and are using the correct images:

* `teamgosh/ackinacki-node`
* `teamgosh/ackinacki-staking`
* `aerospike/aerospike-server`
* `stakater/logrotate`

---
#### 4.2 Node Log Check

```bash
tail -f $MNT_DATA/logs-block-keeper/node.log
```

Verify:

* there are no critical errors
* the synchronization process is progressing correctly
---
#### 4.3 Node Synchronization Status Check

To check the node’s block synchronization status, use:

```bash
node_sync_status.sh path/to/log
```
Additionally, ensure that:  
**the staking script runs without errors and reports the correct staking status**

## Troubleshooting

### Recovery from Corrupted BK State

Node may end up with a corrupted state after restart without graceful shutdown.
It may fail with a panic like this:
```
thread 'main' panicked at node/src/repository/repository_impl.rs:460:22:
Failed to get last finalized state: get_block_from_repo_or_archive: failed to load block: 87d520d75b051a37b7821c5e8236f65afb0371edec2f5afada2647445c870ea5
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

Follow these steps to restore the node (it will resync):

**Steps**

1) **Stop the BK node and clean up the data**
    ```bash
    ansible-playbook -i your-inventory.yaml ansible/node-clean-data.yml
    ```

2) **Restart services**

    ```bash
    ansible-playbook -i your-inventory.yaml ansible/node-starting.yml
    ```

3) **Verify service status**
Ensure that node, staking, and aerospike services are running without restarts or crashes:
    ```bash
    docker compose ps
    ```

    Ensure the following containers are running (status: UP):
    ```
    teamgosh/ackinacki-node
    teamgosh/ackinacki-gql-server
    teamgosh/ackinacki-staking
    aerospike/aerospike-server:latest
    ```

4) **Check the node logs**
Verify that the node logs are active and contain no errors:
    ```bash
    tail -f $MNT_DATA/logs-block-keeper/node.log
    ```
    
    Additionally, you can check the container logs:
    ```bash
    docker compose logs -f node{{ NODE_ID }}
    ```

5) **Check staking logs**
Review the staking logs. They must not contain errors and should display information about stakes, Epochs, and related activities:
    ```bash
    tail -f $MNT_DATA/logs-block-keeper/staking.log
    ```

    Additionally, you can check the container logs:
    ```bash
    docker compose logs -f staking
    ```

6) **Wait for synchronization**
It may take around 20+ minutes for synchronization. Once complete, the `seq_no` should begin to increase, indicating successful recovery.

    🚨 **Important:**
    If the node does not synchronize, it usually means there was an incorrect setup. Possible causes include:
    - Misconfigured config file
    - Incorrect or corrupted key files

### Error with sending continue stake request

If you notice an error like this in the logs:

```bash
[2025-09-25T03:58:04+00:00] Epoch with address "0:7f73cb8939cd302418b79b86e64837675c283af95c37aec9bff673011ce38fe1" is being continued: false [2025-09-25T03:59:05+00:00] Active Stakes - "0x5c5c7dc12c5cb21e6969a3d6c44173c682fd1e7dac8dcc9b1fb70ea64cf1678e" [2025-09-25T03:59:05+00:00] Stakes count - 1 [2025-09-25T03:59:05+00:00] Epoch in progress - "0x5c5c7dc12c5cb21e6969a3d6c44173c682fd1e7dac8dcc9b1fb70ea64cf1678e" [2025-09-25T03:59:06+00:00] There is active stake with epoch address "0:7f73cb8939cd302418b79b86e64837675c283af95c37aec9bff673011ce38fe1" [2025-09-25T03:59:06+00:00] Current epoch is not being continued. Sending continue stake... [2025-09-25T03:59:06+00:00] Trying signer index: 2577 [2025-09-25T03:59:07+00:00] Found proper signer index for continue: 2577 [2025-09-25T03:59:07+00:00] Sending continue stake - 10917648873105 { "Error": { "code": 621, "message": "Failed to execute the message. Error occurred during the compute phase.", "data": { "core_version": "2.22.3", "node_error": { "extensions": { "code": "TVM_ERROR", "message": "Failed to execute the message. Error occurred during the compute phase.", "details": { "producers": [ "94.156.233.53:8601" ], "message_hash": "2174189543433f1e1a2c77e078fc3cf53da8d99956fde1751eda1f941c65844f", "exit_code": 300, "current_time": "1758772748099", "thread_id": "00000000000000000000000000000000000000000000000000000000000000000000" } } }, "ext_message_token": null } } } [2025-09-25T03:59:08+00:00] Error with sending continue stake request. Go to the next step [2025-09-25T03:59:08+00:00] Epoch with address "0:7f73cb8939cd302418b79b86e64837675c283af95c37aec9bff673011ce38fe1" is being continued: false [2025-09-25T04:00:08+00:00] Active Stakes - "0x5c5c7dc12c5cb21e6969a3d6c44173c682fd1e7dac8dcc9b1fb70ea64cf1678e" [2025-09-25T04:00:08+00:00] Stakes count - 1 [2025-09-25T04:00:09+00:00] Epoch in progress - "0x5c5c7dc12c5cb21e6969a3d6c44173c682fd1e7dac8dcc9b1fb70ea64cf1678e" [2025-09-25T04:00:09+00:00] There is active stake with epoch address "0:7f73cb8939cd302418b79b86e64837675c283af95c37aec9bff673011ce38fe1" [2025-09-25T04:00:09+00:00] Current epoch is not being continued. Sending continue stake...
```

**This error occurs because messages are being sent to the wallet or license contract too frequently.**  
When messages are sent too often, the system doesn’t have enough time to process previous transactions, leading to a compute-phase execution error `TVM_ERROR, exit_code = 300`.

✅ What to Do

1. Do nothing if this error appears occasionally — it will stabilize on its own.
2. If the error occurs frequently:
  * Ensure your node or script is not sending “continue stake” too often.
  * Verify that the interval between messages is at least 200 blocks.
3. Once the interval is adjusted, the error will disappear automatically.

# Block Manager Documentation

## System Requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 4c/8t       | 32        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 8c/16t      | 64        | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

🚨 **Important:**
After purchasing a BM License, it must be delegated.

You can delegate it **to a Provider** via the [dashboard](https://dashboard.ackinacki.com/) by following [this instruction](https://docs.ackinacki.com/for-node-owners/protocol-participation/block-manager/licence/guide-to-bm-license-delegation).

Alternatively, you can delegate it **to your own BM wallet** if you plan to deploy the BM service yourself.  
In that case, you must first complete the BM License Pre-Deployment Verification using [this guide](https://docs.ackinacki.com/for-node-owners/network-participation/block-manager/licence/bm-license-pre-deployment-verification).
Once the verification is complete, you will receive the address of license contract and number, which are required for the delegation process.

**Steps for deploying the BM service:**

1. Identify the Block Keeper that your Block Manager service will work with, and request the following information from its owner:
  * Authorization Token (`BK_API_TOKEN`) – access token required for interacting with the BK API.
  * BK Public IP Address (`NODE_IP`)
  * The BK must also open a port for connections to the block streaming service
2. Deploy the Block Manager Wallet
3. Prepare an Ansible inventory file for the Block Manager deployment, specifying all required variables (see example below).
4. Run the Ansible playbook against your inventory to deploy the Block Manager node.

## Deployment with Ansible

### Prerequisites

* A dedicated server for the Block Manager with SSH access.
* Docker with the Compose plugin installed.

### Generate Keys and Deploy the Block Manager Wallet

#### Manually (applies if you don’t have a license number)

You can manually generate the keys and deploy the wallet:

```bash
tvm-cli genphrase --dump block_manager.keys.json
tvm-cli genphrase --dump block_manager_signing.keys.json
```

You will need to specify these keys in the inventory file.  
To deploy the Block Manager wallet, extract the keys from the files created above and run:

ℹ️ **Note:**  
if you don’t have a license number, leave the `whiteListLicense` field empty (`{}`).

```bash
tvm-cli --abi ../contracts/0.79.3_compiled/bksystem/BlockManagerContractRoot.abi.json --addr 0:6666666666666666666666666666666666666666666666666666666666666666 -m deployAckiNackiBlockManagerNodeWallet '{"pubkey": "0xYOUR_PUB_KEY", "signerPubkey": "0xYOUR_SIGNING_KEY", "whiteListLicense": {"YOUR_LICENSE_NUMBER": true}}'
```

#### Using the Script (if you already have a license number)

Before deploying a Block Manager, you need to create two key pairs:

* One key pair for operating the Block Manager wallet..
* Another for signing external message authentication tokens.

If you already have your license number, create a wallet by running:

```bash
create_block_manager_wallet.sh block_manager_wallet.keys.json block_manager_wallet_signing.keys.json 1 tvm-endpoint-address.org
```

### Create an Ansible Inventory

Below is a sample inventory for Block Manager deployment.
Make sure to include the `block_manager` host group.

```yaml
all:
  vars:
    ansible_port: 22
    ansible_user: ubuntu
    ROOT_DIR: /home/user/deployment # path to store deployment files
    MNT_DATA: /home/user/data       # path to store data
    BM_IMAGE: "teamgosh/ackinacki-block-manager:<latest-release-tag>" # i.e. teamgosh/ackinacki-block-manager:v0.3.3
    GQL_IMAGE: "teamgosh/ackinacki-gql-server:<latest-release-tag>"   # i.e. teamgosh/ackinacki-gql-server:v0.3.3
    NGINX_IMAGE: "teamgosh/ackinacki-nginx:v0.4.0"
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
ansible-playbook -i your-inventory.yaml ansible/block-manager-deployment.yaml --check --diff
```

### Run the Ansible Playbook

If validation passes, run the playbook to deploy the Block Manager:

```bash
ansible-playbook -i your-inventory.yaml ansible/block-manager-deployment.yaml
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

## Updating the Whitelist and Delegating the License to the BM Wallet

Before delegating, the license must be added to the BM wallet whitelist.  
To do this, run:

```bash
tvm-cli -j callx --addr BM_WALLET_ADDR --abi contracts/bksystem/AckiNackiBlockManagerNodeWallet.abi.json --keys block_manager.keys.json --method setLicenseWhiteList '{"whiteListLicense": {"YOUR_LICENSE_NUMBER": true}}'
```

To delegate the license to the BM wallet, run:

```bash
tvm-cli -j callx --addr LICENSE_ADDR --abi contracts/bksystem/LicenseBM.abi.json --keys BM_LICENSE_OWNER.keys.json --method addBMWallet '{"pubkey": "0xBM_WALLET_PUB_KEY"}'
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
When the Proxy starts, a certificate is created for communication with Block Keepers.
Specify the Block Keepers’ signing keys in your inventory. Block Keepers must be included in the Block Keeper set.

🚨 **Important:**
The Proxy certificate must be signed with the key of the BK located behind this Proxy.
Using a key from another BK may disrupt routing and lead to data loss.

### Prepare Your Inventory

Below is an example of a basic Ansible inventory for deploying a Proxy:

```yaml
all:
  vars:
    PROXY_IMAGE: "teamgosh/ackinacki-proxy:proxy_version"
    PROXY_PORT: 8085
    PROXY_DIR: /opt/depl/proxy
    NETWORK_NAME: shellnet
    PROXY_BK_ADDRS:
    - Your_first_BK_IP:8600
    - Your_second_BK_IP:8600
    - Your_third_BK_IP:8600
    SEEDS:
    - shellnet0.ackinacki.org:10000
    - shellnet1.ackinacki.org:10000
    - shellnet2.ackinacki.org:10000
    - shellnet3.ackinacki.org:10000
    - shellnet4.ackinacki.org:10000
    NODE_GROUP_ID: ""
    OTEL_COLLECTOR: no
    OTEL_SERVICE_NAME: ""

proxy:
  hosts:
    YOUR-PROXY-HOST:
      PROXY_ID: 1
      HOST_PUBLIC_IP: HOST_IP_PUBLIC
      # Add signing keys to proxy for generating certificates
      # Use several for reliability, but using different set for each proxy is recommended
      PROXY_SIGNING_KEYS:
      - "block-keeper-1.keys.json"
      - "block-keeper-2.keys.json"
      - "block-keeper-3.keys.json"
```

`PROXY_SIGNING_KEYS` list contains path to Block Keeper keys. Better to store them in the same folder with proxy deployment playbook. Also you can specify absolute path.

Note that the `PROXY_BK_ADDRS` variable must include at least several of your Block Keepers.
Actually, updating the BK set is not a computationally intensive operation, so it is recommended to include all your Block Keepers in the list.

To publish metrics to your collector, you should set the variables `OTEL_COLLECTOR`, `OTEL_SERVICE_NAME` and `NODE_GROUP_ID` to similar ones as in the Block Keeper deployment.
Also, in such case, make sure that each host has a unique `PROXY_ID` variable set.

To test the deployment with a dry run:

```bash
ansible-playbook -i your-inventory.yaml ansible/proxy-deployment.yaml --check --diff
```

### Run the Ansible Playbook

If everything looks correct, proceed with the actual deployment:

```bash
ansible-playbook -i your-inventory.yaml ansible/proxy-deployment.yaml
```

To view the Proxy logs:

```bash
tail -f $PROXY_DIR/logs-proxy/proxy.log
```

### Upgrading proxy configuration or image

Depending on whether you need to regenerate the proxy certificate (for example, if you changed the `PROXY_SIGNING_KEYS` variable),
you need to use either `ansible/proxy-deployment.yaml` or `ansible/proxy-upgrade.yaml` playbook.`

If you need to regenerate the proxy certificate, you need to run the `ansible/proxy-deployment.yaml` playbook.

If you need to upgrade the proxy image or other parameters without changing the keys (regenerating certificates), you need to run the `ansible/proxy-upgrade.yaml` playbook.

```bash
# This will regenerate the proxy certificate and requires BK keys
ansible-playbook -i your-inventory.yaml ansible/proxy-deployment.yaml

# This will stop, update compose and config, and restart proxy WITHOUT regenerating keys
ansible-playbook -i your-inventory.yaml ansible/proxy-upgrade.yaml
```
