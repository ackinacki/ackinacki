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
    uint32 _networkStart;
    uint32 _epochDuration;
    uint64 _waitStep;
    uint32 _prevEpochDuration;
    uint32 _epochStart;
    uint32 _epochEnd;
    uint128 _numberOfActiveBlockManagersAtEpochStart = 0;
    uint128 _numberOfActiveBlockManagersAtPrevEpochStart = 0;
    uint128 _reward_adjustment = 0;
    uint128 _reward_adjustment_prev_epoch = 0;
    uint32 _reward_last_time = 0; 
    uint32 _min_reward_period = 432000;
    uint32 _reward_period = 432000;
    uint32 _calc_reward_num = 0;
    uint128 _reward_sum = 0;
    uint128 _slash_sum = 0;
    uint128 _reward_sum_prev_epoch = 0;
    bool _is_close_owner = false;
    uint8 _walletTouch = 200;

    constructor (
        address licenseBMRoot,
        uint32 epochDuration,
        uint64 waitStep,
        uint128 reward_adjustment,
        uint8 walletTouch
    ) {
        _licenseBMRoot = licenseBMRoot;
        _networkStart = block.timestamp;
        _epochStart = _networkStart;
        _reward_last_time = block.timestamp;
        _epochDuration = epochDuration;
        _epochEnd = _epochStart + _epochDuration;
        _reward_adjustment = reward_adjustment;
        _waitStep = waitStep;
        _walletTouch = walletTouch;
    } 

    function setConfig(uint32 epochDuration, uint64 waitStep, uint32 reward_period, uint32 min_reward_period, uint32 calc_reward_num, uint8 walletTouch) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _epochDuration = epochDuration;
        _waitStep = waitStep;
        _reward_period = reward_period;
        _min_reward_period = min_reward_period;
        _calc_reward_num = calc_reward_num;
        _walletTouch = walletTouch;
        _reward_adjustment = gosh.calcbmmvrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart), true);
    }

    function setConfigNode(uint32 epochDuration, uint64 waitStep, uint32 reward_period, uint32 min_reward_period, uint32 calc_reward_num, uint8 walletTouch) public senderIs(address(this)) accept {
        ensureBalance();
        _epochDuration = epochDuration;
        _waitStep = waitStep;
        _reward_period = reward_period;
        _min_reward_period = min_reward_period;
        _calc_reward_num = calc_reward_num;
        _walletTouch = walletTouch;
        _reward_adjustment = gosh.calcbmmvrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart), true);
    }

    function setOwner(address wallet) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept  {
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _owner_wallet = wallet;
    }

    function closeRoot() public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept  {
        _is_close_owner = true;
        ensureBalance();
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept  { 
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _code[id] = code;
    }

    function calcRewardAdjustment() private returns (uint128) {
        _reward_period = (_reward_period * _calc_reward_num + block.timestamp - _reward_last_time) / (_calc_reward_num + 1);
        _reward_last_time = block.timestamp;
        _calc_reward_num += 1;
        _reward_adjustment = gosh.calcbmmvrewardadj(_reward_sum, _reward_period, _reward_adjustment, uint128(block.timestamp - _networkStart), true);
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
        if (_reward_last_time + _min_reward_period < block.timestamp) {
            calcRewardAdjustment();
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
    }

    function increaseBM(uint256 pubkey) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        _numberOfActiveBlockManagers += 1;
    }

    function deployAckiNackiBlockManagerNodeWallet(uint256 pubkey, uint256 signerPubkey, mapping(uint256 => bool) whiteListLicense) public accept  {
        ensureBalance();
        require(whiteListLicense.keys().length <= MAX_LICENSE_NUMBER_WHITELIST_BM, ERR_TOO_MANY_LICENSES);
        TvmCell data = BlockKeeperLib.composeBlockManagerWalletStateInit(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockManagerNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_MANAGER_WALLET), wid: 0, flag: 1}(_code[m_LicenseBMCode], whiteListLicense, _licenseBMRoot, _networkStart, _waitStep, signerPubkey, _walletTouch);
    }

    function slashed(uint256 pubkey) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        _numberOfActiveBlockManagers -= 1;
        _slash_sum += uint128(msg.currencies[CURRENCIES_ID]);
    }

    function getReward(uint256 pubkey, uint256 sign_pubkey, address wallet_address, uint32 rewarded, uint32 startBM, bool isEnd) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        if (isEnd) {
            _numberOfActiveBlockManagers -= 1;
        }
        if ((rewarded >= _epochStart) || (startBM + _prevEpochDuration > _epochStart)) {
            AckiNackiBlockManagerNodeWallet(msg.sender).noRewards{value: 0.1 vmshell, flag: 1}(); 
            return;
        }
        TvmBuilder data;
        data.store(pubkey);
        data.store(sign_pubkey);
        data.store(wallet_address);
        uint128 reward = gosh.calcbmreward(data.toCell(), _numberOfActiveBlockManagersAtPrevEpochStart, _reward_sum_prev_epoch, _prevEpochDuration, _reward_adjustment_prev_epoch);
        _reward_sum += reward; 
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(reward);
        gosh.mintecc(uint64(reward), CURRENCIES_ID);
        AckiNackiBlockManagerNodeWallet(msg.sender).takeReward{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_walletTouch, _waitStep, _epochStart, _epochEnd);  
    }

    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getAckiNackiBlockManagerNodeWalletAddress(uint256 pubkey) external view returns(address wallet){
        return BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode] ,address(this), pubkey);
    }    

    function getAckiNackiBlockManagerNodeWalletCode () external view returns(TvmCell data){
        return BlockKeeperLib.buildBlockManagerWalletCode(_code[m_AckiNackiBlockManagerNodeWalletCode] ,address(this));
    }

    function getCodes() external view returns(mapping(uint8 => TvmCell) code) {
        return _code;
    }

    function getDetails() external view returns(
        uint128 reward_sum, 
        uint128 slash_sum, 
        uint128 numberOfActiveBlockManagers, 
        uint128 numberOfActiveBlockManagersAtEpochStart
    ){
        return (_reward_sum, _slash_sum, _numberOfActiveBlockManagers, _numberOfActiveBlockManagersAtEpochStart);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockManagerContractRoot");
    }
}
