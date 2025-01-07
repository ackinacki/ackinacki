# Deployment instruction

- [Deployment instruction](#deployment-instruction)
- [Block Keeper](#block-keeper)
  - [System requirements](#system-requirements)
  - [Steps](#steps)
  - [Block Keeper Wallet deployment](#block-keeper-wallet-deployment)
    - [Prerequisites](#prerequisites)
    - [Configure tvm-cli](#configure-tvm-cli)
    - [Deploy](#deploy)
  - [Ansible](#ansible)
    - [Prerequisites](#prerequisites-1)
    - [Prepare you inventory](#prepare-you-inventory)
  - [Staking](#staking)
    - [Prerequisites](#prerequisites-2)
    - [Run the script](#run-the-script)
- [Block Manager](#block-manager)
  - [System requirements](#system-requirements-1)
  - [Steps](#steps-1)
  - [Ansible](#ansible-1)
    - [Prerequisites](#prerequisites-3)
    - [Prepare you inventory](#prepare-you-inventory-1)

# Block Keeper

## System requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 8c/16t      | 64        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 12c/24t     | 128       | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |


## Steps 
1. Deploy Block Keeper Wallet
2. Create ansible inventory for Block Keeper node deployment
3. Run ansible playbook against inventory to deploy Block Keeper node
4. Run staking script

## Block Keeper Wallet deployment

### Prerequisites
- `tvm-cli` command line tool installed.  
   See [this guide explaining how to install CLI](https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet)
- Deployed Multisig Wallet.  
  See [this guide explaining how to deploy Multisig Wallet](https://dev.ackinacki.com/how-to-deploy-a-multisig-wallet)
- Sufficient amount of NACKL tokens in the Multisig Wallet balance equal to 2 minimum stakes 

  * To find out the network's minimum stake, call the method in the root contract:

    ```bash
    tvm-cli -j run 0:7777777777777777777777777777777777777777777777777777777777777777 getMinStakeNow {} --abi acki-nacki/contracts/bksystem/BlockKeeperContractRoot.abi.json
    ```

  **Important!**
  For successful restaking, the balance must contain an amount equal to double the minimum stake + 10%.

  * To receive test NACKLs in the test network at shellnet.ackinacki.org, contact us via the [Telegram channel](https://t.me/+1tWNH2okaPthMWU0) and provide the address of your Sponsor Wallet.

### Configure tvm-cli
To set appropriate network use next command
```bash
tvm-cli config --url shellnet.ackinacki.org/graphql
```
In this case we are using Shellnet network

### Deploy 
To deploy Block Keeper Wallet use Shell script `scripts/create_block_keeper_wallet.sh`

This script uses 2 types of keys.

*These are all different keys that are generated in deployment scripts.
If you already have keys, specify the file path where they are stored in the script.
If such a file does not exist yet, it will be created.*

  * Master keys - the keys of the Node Owner. These are the primary keys for accessing the wallet. With this key, you can transfer tokens.

  * Service keys - auxiliary keys for accessing the wallet. Their functionality is similar to the master key, but tokens cannot be transferred with this key.

You have to provide necessary arguments:
- Sponsor Wallet address from where NACKL tokens will be sent
- Wallet keys file. File with the owner's keys of Sponsor Wallet
- Master keys file output path. Path to the file with keys for Block Keeper Wallet, which will be created
- Service keys file output path. Path to the file with Service Keys for Block Keeper Wallet, which will be created.

```bash
cd scripts
create_block_keeper_wallet.sh --help
```

After successful script completion save you `Node id` and `master key`- file path

It will be used in ansible playbook

## Ansible
### Prerequisites
- SSH access to servers
- Docker with compose plugin must be installed

### Prepare you inventory

Here is basic inventory for shellnet node deployment

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
    NODE_IMAGE: teamgosh/ackinacki-node
    GQL_IMAGE: teamgosh/ackinacki-gql-server
    REVPROXY_IMAGE: teamgosh/ackinacki-live
    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"
    LOG_ROTATE_AMOUNT: 30
    LOG_ROTATE_SIZE: 1G
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

Put your configuration data to inventory and be ready to run playbook

In case of testing you can use ansible-playbook dry run

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

If all is good you could run ansible

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml
```

Upon completion of the script, BLS keys will be generated and saved in the file `block_keeper{{ NODE_ID }}_bls.keys.json` in the `{{ BK_DIR }}/bk-configs/` folder on the remote server, along with the node.

**BLS keys** - the keys used by BK to sign blocks. The lifespan of the keys is one epoch. Each BK stores a list of BLS public keys of other BKs (for the current epoch), which they use to verify attestations on blocks.

Check docker containers

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
- Master keys file
- BLS keys file

BLS keys file could be found at `{{ BK_DIR }}/bk-configs/` folder. BK_DIR is a variable from the ansible inventory
BLS keys file format - `block_keeper{{ NODE_ID }}_bls.keys.json`

### Run the script
```bash
staking.sh path/to/master-keys-file path/to/bls-keys-file
```

# Block Manager

## System requirements

| Configuration | CPU (cores) | RAM (GiB) | Storage    | Network                                          |
| ------------- | ----------- | --------- | ---------- | ------------------------------------------------ |
| Minimum       | 4c/8t      | 32        | 1 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |
| Recommended   | 8c/16t     | 64       | 2 TB NVMe  | 1 Gbit synchronous unmetered Internet connection |

## Steps 
1. Create ansible inventory for Block Manager deployment
2. Run ansible playbook against inventory to deploy Block Manager node

## Ansible
### Prerequisites
- One server for Block-Manager only and SSH access to that server
- Docker with compose plugin must be installed

### Prepare you inventory

This is basic inventory for Block Manager deployment. Be sure to include `block-manager` host group.

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

In case of testing you can use dry run and check mode

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml --check --diff
```

If all is good you could run ansible

```bash
ansible-playbook -i test-inventory.yaml ansible/block-manager-deployment.yaml
```

Check docker containers

```bash
docker ps
#OR
docker compose ps
```

Check block-manager logs

```bash
tail -f $MNT_DATA/logs-block-manager/block-manager.log
```
