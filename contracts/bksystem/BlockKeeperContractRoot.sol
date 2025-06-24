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
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperCoolerContract.sol";
import "./BLSKeyIndex.sol";
import "./SignerIndex.sol";

contract BlockKeeperContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint64 _epochDuration = 3000;
    uint64 _epochCliff = 1000;
    uint64 _waitStep = 1000;
    uint128 _minBlockKeepers = 10;
    address _giver;
    uint256 _totalStake = 0;
    address _licenseRoot;

    uint32 _networkStart;
    uint128 _numberOfActiveBlockKeepers = 0;
    uint128 _needNumberOfActiveBlockKeepers = 10000;

    uint32 _block_seqno = 0;
    uint128 _numberOfActiveBlockKeepersAtBlockStart = 0;

    uint128 _reward_adjustment = 0;
    uint32 _reward_last_time = 0; 
    uint32 _min_reward_period = 520000;
    uint32 _reward_period = 520000;
    uint32 _calc_reward_num = 0;

    uint128 _reputationCoefAvg = 0;
    uint128 _reward_sum = 0;
    uint128 _slash_sum = 0;

    optional(address) _owner_wallet;

    constructor (
        address giver,
        address licenseRoot
    ) {
        _giver = giver;
        _networkStart = block.timestamp;
        _licenseRoot = licenseRoot;
        uint32 time = block.timestamp - _networkStart;
        _reward_last_time = block.timestamp;
        _reward_adjustment = gosh.calcbkrewardadj(_reward_sum, MIN_REP_COEF, _reward_period, 10400000000000000000, uint128(time));
    }

    function ensureBalance() private {
        if ((_reward_last_time + _min_reward_period < block.timestamp) || (_calc_reward_num == 0)) {
            calcRewardAdjustment();
        }
        if (block.seqno != _block_seqno) {
            _block_seqno = block.seqno;
            _numberOfActiveBlockKeepersAtBlockStart = _numberOfActiveBlockKeepers;
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshell(ROOT_BALANCE);
    }

    function calcRewardAdjustment() private returns (uint128) {
        uint32 time = block.timestamp - _networkStart;
        if (_calc_reward_num == 0) {
            _reward_period = block.timestamp - _reward_last_time;
        } else {
            _reward_period = (_reward_period * _calc_reward_num + block.timestamp - _reward_last_time) / (_calc_reward_num + 1);
        }
        _reward_last_time = block.timestamp;
        _calc_reward_num += 1;
        if (_reputationCoefAvg == 0) {
            _reward_adjustment = gosh.calcbkrewardadj(_reward_sum, MIN_REP_COEF, _reward_period, _reward_adjustment, uint128(time));
        } else {
            _reward_adjustment = gosh.calcbkrewardadj(_reward_sum, _reputationCoefAvg, _reward_period, _reward_adjustment, uint128(time));
        }
    }

    function setOwner(address wallet) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) {
        ensureBalance();
        _owner_wallet = wallet;
    }

    function setConfig(uint64 epochDuration, uint64 epochCliff, uint64 waitStep, uint128 minBlockKeepers, uint32 reward_period, uint128 needNumberOfActiveBlockKeepers) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        ensureBalance();
        _epochDuration = epochDuration;
        _epochCliff = epochCliff;
        _waitStep = waitStep;
        _minBlockKeepers = minBlockKeepers;
        _reward_period = reward_period;
        _needNumberOfActiveBlockKeepers = needNumberOfActiveBlockKeepers;
    }

    function deployAckiNackiBlockKeeperNodeWallet(uint256 pubkey, mapping(uint256 => bool) whiteListLicense) public accept {
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeBlockKeeperWalletStateInit(_code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockKeeperNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_WALLET), wid: 0, flag: 1}(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperEpochCoolerCode], _code[m_BlockKeeperEpochProxyListCode], _code[m_BLSKeyCode], _code[m_SignerIndexCode], _code[m_LicenseCode], _epochDuration, _networkStart + UNLOCK_LICENSES, whiteListLicense, _licenseRoot, 0);
    }

    function decreaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake, uint128 rep_coef, bool is_slash, optional(uint128) virtualStake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        uint256 local_Stake = stake;
        if (virtualStake.hasValue()) {
            local_Stake = virtualStake.get();
        }
        if (_totalStake == local_Stake) {
            _reputationCoefAvg = MIN_REP_COEF;
        } else {
            _reputationCoefAvg = uint128(_reputationCoefAvg - rep_coef * local_Stake / (_totalStake - local_Stake));
        }
        _totalStake -= local_Stake;
        _numberOfActiveBlockKeepers -= 1;
        if (is_slash) {
            _slash_sum += uint128(stake);
        }
    }

    function decreaseStakes(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        _totalStake -= stake;
    }

    function increaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake, uint128 rep_coef, optional(uint128) virtualStake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        uint256 local_stake = stake;
        if (virtualStake.hasValue()) {
            local_stake = virtualStake.get();
        }
        _totalStake += local_stake;
        _numberOfActiveBlockKeepers += 1;
        if (_totalStake == 0) {
            _reputationCoefAvg = MIN_REP_COEF;
        } else {
            _reputationCoefAvg = uint128(_reputationCoefAvg * (_totalStake - local_stake) / _totalStake + rep_coef * local_stake / _totalStake);
        }
        BlockKeeperEpoch(msg.sender).setStake{value: 0.1 vmshell, flag: 1}(_totalStake, _numberOfActiveBlockKeepersAtBlockStart);
    }

    function receiveBlockKeeperRequestWithStakeFromWallet(uint256 pubkey, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, bool is_min, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        ensureBalance();
        optional(uint128) virtualStake;
        uint128 minStake = gosh.calcminstake(_reward_sum - _slash_sum, block.timestamp - _networkStart + uint128(_waitStep / 3), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        if (msg.currencies[CURRENCIES_ID] < minStake) {
            if (is_min == false) {
                AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}(licenses);
                return;
            } else {
                virtualStake = minStake;
            }
        }
        uint256 stake = msg.currencies[CURRENCIES_ID];
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender, pubkey, stake, signerIndex);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAccept{value: 0.1 vmshell, flag: 1}(pubkey,rep_coef, licenses, virtualStake, ProxyList, myIp);
    }

    function isBLSAccepted(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint16 signerIndex,uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {  
        ensureBalance();     
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}(licenses);
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey), pubkey, stake, bls_pubkey);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAccept{value: 0.1 vmshell, flag: 1}(pubkey, rep_coef, licenses, virtualStake, ProxyList, myIp);
    }

    function isSignerIndexAccepted(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {   
        ensureBalance();    
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}(licenses);
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 vmshell, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).deployPreEpochContract{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_epochDuration, _epochCliff, _waitStep, bls_pubkey, signerIndex, rep_coef, licenses, virtualStake, ProxyList, _reward_sum, myIp);
    }

    function receiveBlockKeeperRequestWithStakeFromWalletContinue(uint256 pubkey, bytes bls_pubkey, uint64 seqNoStartOld, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, bool is_min, mapping(uint8 => string) ProxyList) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        ensureBalance();
        uint128 minStake = gosh.calcminstake(_reward_sum - _slash_sum, block.timestamp - _networkStart + uint128(_waitStep / 3), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        optional(uint128) virtualStake;
        if (msg.currencies[CURRENCIES_ID] < minStake) {
            if (is_min == false) {
                AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}(licenses);
                return;
            } else {
                virtualStake = minStake;
            }
        }
        uint256 stake = msg.currencies[CURRENCIES_ID];
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender, pubkey, stake, signerIndex);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAcceptContinue{value: 0.1 vmshell, flag: 1}(pubkey, seqNoStartOld, rep_coef, licenses, virtualStake, ProxyList); 
    } 

    function isBLSAcceptedContinue(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {       
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1}(licenses);
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey), pubkey, stake, bls_pubkey);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAcceptContinue{value: 0.1 vmshell, flag: 1}(pubkey, seqNoStartOld, rep_coef, licenses, virtualStake, ProxyList);
    }

    function isSignerIndexContinue(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList) public senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {       
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1}(licenses);
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 vmshell, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).deployBlockKeeperContractContinue{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_epochDuration, _waitStep, seqNoStartOld, bls_pubkey, signerIndex, rep_coef, licenses, virtualStake, ProxyList, _reward_sum);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg { 
        ensureBalance();
        _code[id] = code;
    }

    function canDeleteEpoch(uint256 pubkey, uint64 seqNoStart, uint256 stake, uint128 reputationTime, uint256 totalStakeOld, uint128 numberOfActiveBlockKeepers, uint128 time, optional(uint128) virtualStake, uint128 reward_sum)  public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        if (_numberOfActiveBlockKeepers - 1 >= _minBlockKeepers) {
            uint128 reward = 0;
            uint128 reward_adjustment = _reward_adjustment;
            if (virtualStake.hasValue()) {
                reward = gosh.calcbkreward(reward_adjustment, numberOfActiveBlockKeepers, reward_sum, block.timestamp - time, uint128(totalStakeOld), uint128(virtualStake.get()), reputationTime);
            } else {
                reward = gosh.calcbkreward(reward_adjustment, numberOfActiveBlockKeepers, reward_sum, block.timestamp - time, uint128(totalStakeOld), uint128(stake), reputationTime);
            }
            _reward_sum += reward;
            _numberOfActiveBlockKeepers = _numberOfActiveBlockKeepers - 1;
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(reward);
            gosh.mintecc(uint64(reward), CURRENCIES_ID);
            BlockKeeperEpoch(msg.sender).canDelete{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET + 1 vmshell), currencies: data_cur, flag: 1}(reward);
        }
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg {
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private pure {
    }
    
    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getBlockKeeperCoolerAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], address(this), pubkey, seqNoStart);
    }

    function getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey) external view returns(address wallet){
        return BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey);
    }

    function getAckiNackiBlockKeeperNodeWalletCode () external view returns(TvmCell data){
        return BlockKeeperLib.buildBlockKeeperWalletCode(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this));
    }

    function getBlockKeeperEpochCode() external view returns(TvmCell epochCode){
        return BlockKeeperLib.buildBlockKeeperEpochCode(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this));
    }
    
    function getBlockKeeperEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address epochAddress){
        return BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart);
    }

    function getBlockKeeperPreEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address preEpochAddress) {
        return BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey, seqNoStart);
    }

    function getDetails() external view returns(uint128 minStake, uint128 numberOfActiveBlockKeepers) {
        return (gosh.calcminstake(_reward_sum - _slash_sum, block.timestamp - _networkStart + uint128(_waitStep / 3), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers), _numberOfActiveBlockKeepers);
    }

    function getEpochCodeHash() external view returns(uint256 epochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochCode(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this)));
    }

    function getPreEpochCodeHash() external view returns(uint256 preEpochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperPreEpochCode(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this)));
    }

    function getRewardOut(
        uint128 reward_adjustment,
        uint128 numberOfActiveBlockKeepers,
        uint128 stake,
        uint128 totalStake,
        uint128 reward_sum,
        uint128 timeepoch,
        uint128 reputationTime
        ) external pure returns(uint128 reward) {
        return gosh.calcbkreward(reward_adjustment, numberOfActiveBlockKeepers, reward_sum, timeepoch, totalStake, stake, reputationTime);
    }     

    function getProxyListCode() external view returns(TvmCell code) {
        return BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], address(this));
    }

    function getProxyListAddress(uint256 pubkey) external view returns(address proxyAddress) {
        return BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], pubkey, address(this));
    }

    function getRewardNow(
        ) external view returns(uint128 reward) {
        return gosh.calcbkreward(_reward_adjustment, _numberOfActiveBlockKeepers, _reward_sum, uint128(block.timestamp - _networkStart), uint128(_totalStake), uint128(_totalStake), uint128(0));
    }     

    function getMinStakeNow() external view returns(uint128 minstake) {
        return gosh.calcminstake(_reward_sum - _slash_sum, block.timestamp - _networkStart + uint128(_waitStep / 3), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
    }

    function getConfig() external view returns(uint64 epochDuration,uint64 epochCliff,uint64 waitStep) {
        return (_epochDuration, _epochCliff, _waitStep);
    }

    function getSignerIndexAddress(uint16 index) external view returns(address signerIndex) {
        return BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], index, address(this));
    }

    function getMinStakeOut(
        uint128 reward_sum,
        uint128 timeEpochStart,
        uint128 numberOfActiveBlockKeepersAtBlockStart,
        uint128 needNumberOfActiveBlockKeepers) external pure returns(uint128 minstake) {
        return gosh.calcminstake(reward_sum, timeEpochStart, numberOfActiveBlockKeepersAtBlockStart, needNumberOfActiveBlockKeepers);
    }

    function getBLSIndexAddress(bytes bls_key) external view returns(address blsAddress) {
        return BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, address(this));
    }

    function getCodes() external view returns(mapping(uint8 => TvmCell) code) {
        return _code;
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperContractRoot");
    }   
}
