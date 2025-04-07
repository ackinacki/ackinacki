# Deployment Instructions

- [Deployment Instructions](#deployment-instructions)
- [Block Keeper](#block-keeper)
  - [System Requirements](#system-requirements)
  - [Steps](#steps)
  - [Block Keeper Wallet Deployment](#block-keeper-wallet-deployment)
    - [Prerequisites](#prerequisites)
    - [Configure tvm-cli](#configure-tvm-cli)
    - [Deploy](#deploy)
  - [Delegating License](#delegating-license)
  - [Ansible](#ansible)
    - [Prerequisites](#prerequisites-1)
    - [Prepare Your Inventory](#prepare-your-inventory)
  - [Staking](#staking)
    - [Prerequisites](#prerequisites-2)
    - [Run the Script](#run-the-script)
  - [Node status](#Check-node-status)
- [Block Manager](#block-manager)
  - [System Requirements](#system-requirements-1)
  - [Steps](#steps-1)
  - [Ansible](#ansible-1)
    - [Prerequisites](#prerequisites-3)
    - [Prepare Your Inventory](#prepare-your-inventory-1)

# Block Keeper

## System Requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 8c/16t      | 64        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 12c/24t     | 128       | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

## Steps
1. Deploy the Block Keeper Wallet.
2. Create an Ansible inventory for Block Keeper node deployment.
3. Run the Ansible playbook against the inventory to deploy the Block Keeper node.
4. Delegate the license to the BK wallet.
5. Run the staking script.

## Block Keeper Wallet Deployment

### Prerequisites
- The `tvm-cli` command-line tool must be installed.
  See [this guide on how to install CLI](https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet#create-a-wallet-1).
- A deployed Multisig Wallet.
  See [this guide on deploying a Multisig Wallet](https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet).
- A deployed license contract with obtained license numbers.
  See [Working with Licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses)
- A sufficient balance of NACKL tokens in the Multisig Wallet, equal to at least two minimum stakes.

  * To find out the network's minimum stake, call the method `getMinStakeNow` of the system contract [BlockKeeperContractRoot](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/BlockKeeperContractRoot.sol) as follows:

  ```bash
  tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMinStakeNow {} --abi acki-nacki/contracts/bksystem/BlockKeeperContractRoot.abi.json
  ```
  [BlockKeeperContractRoot.abi.json](blob:https://github.com/ac93bdc5-7ad6-4c3f-8fe7-027652fc26e1)

  **Important!**
  For successful restaking, the BK wallet balance (in NACKLs) must contain an amount equal to double the minimum stake + 10%.

  * To receive test [NACKLs](https://docs.ackinacki.com/glossary#nackl) in the test network at shellnet.ackinacki.org, contact us via the [Telegram channel](https://t.me/+1tWNH2okaPthMWU0) and provide the address of your Multisig wallet.

### Configure tvm-cli
For this example, we are using the Shellnet network.

To set the appropriate network, use the following command:
```bash
tvm-cli -g config --url shellnet.ackinacki.org/graphql
```

### Deploy
To deploy Block Keeper Wallet, use the Shell script [`scripts/create_block_keeper_wallet.sh`](https://github.com/ackinacki/ackinacki/blob/main/scripts/create_block_keeper_wallet.sh).

To run the script, you need to provide the following arguments:

* `-w` – The Multisig wallet address from which NACKL tokens will be transferred.

* `-wk` – The path to the file containing the Multisig wallet keys.

* `-m` – The path to the [Block Keeper Node Owners keys](https://docs.ackinacki.com/glossary#bk-node-owner-keys) file.
    (If the file exists, the keys from it will be used;
    if not, new keys will be generated and saved in this file.)

* `-l` – A list of [License numbers](https://docs.ackinacki.com/glossary#license-number) to add to the [Block Keeper Wallet whitelist](https://docs.ackinacki.com/glossary#bk-wallet-whitelist). (Use `,` as a delimiter without spaces.) Obtain them from the License Owners.

For example:
```bash
cd scripts
./create_block_keeper_wallet.sh -w 0:c9f24d25753f3e319411e919946ee52c82632b18dd974d81be8c7ba4e212db2f -wk ../multisig_wallet/multisig.keys.json -m ../bk_wallet/bk_wallet.keys.json -l 6,7
```

After the script completes successfully, save your `Node ID` and the path to the BK Node Owner's keys file.

These will be required for the Ansible playbook.

## Delegating License

Before starting staking, a node must have at least one delegated license.
However, no more than five licenses can be delegated to a single node.

Learn more about [working with licenses](https://docs.ackinacki.com//protocol-participation/license/working-with-licenses).

If the BK Node Owner is also a License Owner, they must use the `addBKWallet(uint256 pubkey)` method in the [`License`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.sol) contract to delegate their licenses to their node. 
(This must be done for each license contract.)

Where:

* `pubkey` – the public key of the node owner, obtained in Step 4.

* [`License.abi.json`](https://github.com/ackinacki/ackinacki/blob/main/contracts/bksystem/License.abi.json) – the ABI of the License contract.

* `License.keys.json` – the keys obtained by the License Owner during registration in the dashboard.

For example:

```bash
tvm-cli -j callx --addr 0:7f2f945faaae4cce286299afe74dac9460893dd5cba1ac273b9e91f55f1141ec --abi acki-nacki/contracts/bksystem/License.abi.json --keys license_onwer/license.keys.json --method addBKWallet '{"pubkey": "0xfa4edc8b63c4e66241a57c11e0a522769ca4a4f106692512fc92f2d658169bcc"}'
```

## Ansible
### Prerequisites
- SSH access to servers.
- Docker with the compose plugin must be installed.

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
```

  **Node ID** is the identifier of the Block Keeper in the network. It is required to create a new Block Keeper.  
  *Use the one provided by the deploy BK wallet-script*

  **HOST_PRIVATE_IP** 
  You can specify, for example, `127.0.0.1`.
  *The ports must not overlap*


Ensure your configuration data is added to the inventory before running the playbook.

For testing, you can use an Ansible dry run:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

If everything looks good, run Ansible:

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml
```

Upon completion of the script, BLS keys will be generated and saved in the file `block_keeper{{ NODE_ID }}_bls.keys.json` in the `{{ BK_DIR }}/bk-configs/` folder on the remote server, along with the node.

**BLS keys** - the keys used by Block Keeper (BK) to sign blocks. The keys have a lifespan of one Epoch. Each BK maintains a list of BLS public keys from other BKs (for the current Epoch), which are used to verify attestations on blocks.

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
### Prerequisites
- BK Node Owner keys file
- BLS keys file

BLS keys file could be found at `{{ BK_DIR }}/bk-configs/` folder. BK_DIR is a variable from the ansible inventory
BLS keys file format - `block_keeper{{ NODE_ID }}_bls.keys.json`

### Run the script
```bash
staking.sh path/to/bk-node-owner-keys path/to/bls-keys-file
```

## Check node status
To get node status by blocks you can use `node_sync_status.sh`

```bash
node_sync_status.sh path/to/log
```

# Block Manager

## System requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 4c/8t      | 32        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 8c/16t     | 64       | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

## Steps 
1. Create an Ansible inventory for Block Manager deployment.
2. Run the Ansible playbook against the inventory to deploy the Block Manager node.

## Ansible
### Prerequisites
* A dedicated server for the Block Manager with SSH access.
* Docker with the Compose plugin installed.

### Prepare you inventory

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
