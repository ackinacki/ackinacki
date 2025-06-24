// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./AckiNackiBlockManagerNodeWallet.sol";
import "./BlockKeeperContractRoot.sol";

contract BlockManagerContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    address _licenseBMRoot;
    uint128 _numberOfActiveBlockManagers = 0;
    optional(address) _owner_wallet;
    address _giver;
    uint32 _networkStart;
    uint32 _epochDuration;
    uint32 _prevEpochDuration;
    uint32 _epochStart;
    uint32 _epochEnd;
    uint128 _numberOfActiveBlockManagersAtEpochStart = 0;
    uint128 _numberOfActiveBlockManagersAtPrevEpochStart = 0;
    uint128 _reward_adjustment = 0;
    uint128 _reward_adjustment_prev_epoch = 0;
    uint32 _reward_last_time = 0; 
    uint32 _min_reward_period = 520000;
    uint32 _reward_period = 520000;
    uint32 _calc_reward_num = 0;
    uint128 _reward_sum = 0;
    uint128 _reward_sum_prev_epoch = 0;
    


    constructor (
        address giver,
        address licenseBMRoot,
        uint32 epochDuration
    ) {
        _giver = giver;
        _licenseBMRoot = licenseBMRoot;
        _networkStart = block.timestamp;
        _epochStart = _networkStart;
        _reward_last_time = block.timestamp;
        _epochDuration = epochDuration;
        _epochEnd = _epochStart + _epochDuration;
        _reward_adjustment = gosh.calcbmrewardadj(_reward_sum, _reward_period, 10400000000000000000, uint128(0));
    } 

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg { 
        ensureBalance();
        _code[id] = code;
    }

    function calcRewardAdjustment() private returns (uint128) {
        if (_calc_reward_num == 0) {
            _reward_period = block.timestamp - _reward_last_time;
        } else {
            _reward_period = (_reward_period * _calc_reward_num + block.timestamp - _reward_last_time) / (_calc_reward_num + 1);
        }
        _reward_last_time = block.timestamp;
        _calc_reward_num += 1;
        _reward_adjustment = gosh.calcbmrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart));
    }

    function ensureBalance() private {
        if (block.timestamp >= _epochEnd) {
            _prevEpochDuration = block.timestamp - _epochStart;
            _epochStart = block.timestamp;
            _epochEnd = block.timestamp + _epochDuration;
            _numberOfActiveBlockManagersAtPrevEpochStart = _numberOfActiveBlockManagersAtEpochStart;
            _numberOfActiveBlockManagersAtEpochStart = _numberOfActiveBlockManagers;
            _reward_adjustment_prev_epoch = _reward_adjustment;
            _reward_sum_prev_epoch = _reward_sum;
        }
        calcRewardAdjustment();
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshell(ROOT_BALANCE);
    }

    function increaseBM(uint256 pubkey) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        _numberOfActiveBlockManagers += 1;
    }

    function deployAckiNackiBlockManagerNodeWallet(uint256 pubkey, mapping(uint256 => bool) whiteListLicense) public accept {
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeBlockManagerWalletStateInit(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockManagerNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_MANAGER_WALLET), wid: 0, flag: 1}(_code[m_LicenseBMCode], whiteListLicense, _licenseBMRoot, _networkStart);
    }

    function getReward(uint256 pubkey, uint32 rewarded, uint32 startBM, bool isEnd) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        if (isEnd) {
            _numberOfActiveBlockManagers -= 1;
        }
        if ((rewarded >= _epochStart) || (startBM + _prevEpochDuration >= _epochStart)) {
            return;
        }
        uint128 reward = gosh.calcbmreward(_numberOfActiveBlockManagersAtPrevEpochStart, _reward_sum_prev_epoch, _prevEpochDuration, _reward_adjustment_prev_epoch);
        _reward_sum += reward; 
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        gosh.mintecc(uint64(reward), CURRENCIES_ID);
        msg.sender.transfer({value: 0.1 ton, currencies: data_cur, flag: 1});  
    }
    
    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getAckiNackiBlockManagerNodeWalletAddress(uint256 pubkey) external view returns(address wallet){
        return BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode] ,address(this), pubkey);
    }

    function getCodes() external view returns(mapping(uint8 => TvmCell) code) {
        return _code;
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockManagerContractRoot");
    }   
}
