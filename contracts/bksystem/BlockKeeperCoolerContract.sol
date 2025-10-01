/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./AckiNackiBlockKeeperNodeWallet.sol";


contract BlockKeeperCooler is Modifiers {
    string constant version = "1.0.0";

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint64 _seqNoFinish;
    address _owner;
    uint128 _stake;
    uint16 _signerIndex;
    LicenseStake[] _licenses;

    constructor (
        uint64 waitStep,
        address owner,
        address root,
        uint16 signerIndex,
        LicenseStake[] licenses,
        uint128 stake,
        bool isContinue,
        uint128 stakeContinue, 
        uint64 epochDurationContinue, 
        uint64 waitStepContinue, 
        bytes bls_pubkeyContinue, 
        uint16 signerIndexContinue, 
        LicenseStake[] licensesContinue, 
        optional(uint128) virtualStakeContinue, 
        uint128 reward_sum_continue,
        string myIp,
        uint32 unixtimeStart,
        uint128 sumReputationCoefContinue,
        optional(string) nodeVersionContinue,
        optional(uint8) slash_type
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address epoch) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == epoch, ERR_SENDER_NO_ALLOWED);
        _seqNoFinish = block.seqno + waitStep;
        _owner = owner;
        _stake = stake;
        _signerIndex = signerIndex;
        _licenses = licenses;
        AckiNackiBlockKeeperNodeWallet(_owner).updateLockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, _seqNoFinish, _licenses, isContinue, block.timestamp - unixtimeStart);
        if ((isContinue) && (!slash_type.hasValue())) {
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(stakeContinue);
            AckiNackiBlockKeeperNodeWallet(_owner).deployBlockKeeperContractContinueAfterDestroy{value: 0.1 vmshell, flag: 1, currencies: data_cur}(epochDurationContinue, waitStepContinue, bls_pubkeyContinue, _seqNoStart, signerIndexContinue, licensesContinue, _licenses, virtualStakeContinue, reward_sum_continue, myIp, sumReputationCoefContinue, block.timestamp - unixtimeStart, nodeVersionContinue);
        }
        if (slash_type.hasValue()) {
            this.slash{value: 0.1 vmshell, flag: 1}(slash_type.get(), true);
        }
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET) { return; }
        gosh.mintshellq(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET);
    }

    function slash(uint8 slash_type, bool isFromEpoch) public senderOfTwo(_owner, address(this)) accept {  
        ensureBalance();
        if (slash_type == FULL_STAKE_SLASH) {
            AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, 0, _licenses, isFromEpoch);
            BlockKeeperContractRoot(_root).coolerSlash{value: 0.1 vmshell, flag: 161, bounce: false}(_seqNoStart, _owner_pubkey);       
            return;
        }                
        uint128 reward = uint128(address(this).currencies[CURRENCIES_ID]) - _stake;
        uint128 slash_stake_reward = reward * slash_type / FULL_STAKE_PERCENT;
        uint128 slash_stake = 0;
        for (uint i = 0; i < _licenses.length; i++) {
            slash_stake = slashPartHelper(i, slash_type, slash_stake);   
        }
        _stake -= slash_stake;
        mapping(uint32 => varuint32) data_cur; 
        data_cur[CURRENCIES_ID] = varuint32(slash_stake + slash_stake_reward);
        AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, slash_stake, _licenses, isFromEpoch);    
        BlockKeeperContractRoot(_root).coolerSlash{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_seqNoStart, _owner_pubkey);       
    }

    function slashPartHelper(uint i, uint8 slash_type, uint128 slash_stake) private returns(uint128) {
        uint128 slash_value = _licenses[i].stake * slash_type / FULL_STAKE_PERCENT;
        _licenses[i].stake -= slash_value;
        slash_stake += slash_value;
        return slash_stake;
    }

    function touch() public view senderIs(_owner) accept {       
        ensureBalance();
        if (_seqNoFinish > block.seqno) { return; }
        AckiNackiBlockKeeperNodeWallet(_owner).unlockStakeCooler{value: 0.1 vmshell, flag: 161, bounce: false}(_seqNoStart, _licenses, _stake);
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root, 
        uint64 seqNoStart,
        uint64 seqNoFinish,
        address owner,
        uint16 signerIndex,
        varuint32 NACKLBalance) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner, _signerIndex, address(this).currencies[CURRENCIES_ID]);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperCooler");
    }    
}
