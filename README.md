# Block Keeper deployment instruction

- [Block Keeper deployment instruction](#block-keeper-deployment-instruction)
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



## Steps 
1. Deploy Block Keeper Wallet
2. Create ansible inventory for Block Keeper node deployment
3. Run ansible playbook against inventory to deploy Block Keeper node
4. Run staking script

## Block Keeper Wallet deployment

### Prerequisites
- `tvm-cli` command line tool installed
- Sponsor Wallet with enough NACKL tokens deployed
  
See [this guide explaining how to install CLI and deploy Sponsor Wallet](https://dev.ackinacki.com/how-to-deploy-a-sponsor-wallet#create-a-wallet)

### Configure tvm-cli
To set appropriate network use next command
```bash
tvm-cli config --url shellnet.ackinacki.org/graphql
```
In this case we are using Shellnet network

### Deploy 
To deploy Block Keeper Wallet use Shell script `scripts/create_block_keeper_wallet.sh`
You have to provide necessary arguments:
- Sponsor Wallet address from where NACKL tokens will be sent
- Wallet keys file. File with the owner's keys of Sponsor Wallet
- Master keys file output path. Path to the file with keys for Block Keeper Wallet, which will be created
- Service keys file output path. Path to the file with Service Keys for Block Keeper Wallet, which will be created

```bash
cd scripts
create_block_keeper_wallet --help
```

After successful script completion save you node id and master key file path
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
    NODE_IMAGE: teamgosh/ackinacki-node
    GQL_IMAGE: teamgosh/ackinacki-gql-server
    REVPROXY_IMAGE: teamgosh/ackinacki-live
    BK_DIR: "{{ ROOT_DIR }}/block-keeper"
    BK_DATA_DIR: "{{ MNT_DATA }}/block-keeper"
    BK_LOGS_DIR: "{{ MNT_DATA }}/logs-block-keeper"
    GOSSIP_SEEDS:
      - shellnet-0.ackinacki.org:10000
      - shellnet-1.ackinacki.org:10000
      - shellnet-2.ackinacki.org:10000
      - shellnet-3.ackinacki.org:10000
      - shellnet-4.ackinacki.org:10000
      - YOUR-NODE-ADDRESS:10000
    NODE_STORAGE_LIST:
      - http://shellnet-0.ackinacki.org/storage/node/
      - http://shellnet-1.ackinacki.org/storage/node/
      - http://shellnet-2.ackinacki.org/storage/node/
      - http://shellnet-3.ackinacki.org/storage/node/
      - http://shellnet-4.ackinacki.org/storage/node/
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

Put your configuration data to inventory and be ready to run playbook

In case of testing you can use ansible-playbook dry run

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

If all is good you could run ansible

```bash
ansible-playbook -i test-inventory.yaml ansible/node-deployment.yaml --check --diff
```

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
BLS keys file format - `bk{{ NODE_ID }}_bls.keys.json`

### Run the script
```bash
staking path/to/master-keys-file path/to/bls-keys-file
```
