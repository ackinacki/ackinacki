// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./AckiNackiBlockKeeperNodeWalletConfig.sol";
import "./AckiNackiBlockKeeperNodeWalletConfig.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperCoolerContract.sol";
import "./BLSKeyIndex.sol";
import "./SignerIndex.sol";

contract BlockKeeperContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint32 _epochDuration = 1000;
    uint64 _epochCliff = 1000;
    uint64 _waitStep = 1000;
    uint128 _minBlockKeepers = 10;
    address _giver;
    uint128 _num_wallets = 0;
    uint256 _totalStake = 0;

    uint32 _networkStart;
    uint128 _numberOfActiveBlockKeepers = 0;

    uint32 _block_seqno = 0;
    uint128 _numberOfActiveBlockKeepersAtBlockStart = 0;

    optional(address) _owner_wallet;

    constructor (
        address giver
    ) {
        _giver = giver;
        _networkStart = block.timestamp;
    }

    function getMoney() private {
        if (block.seqno != _block_seqno) {
            _block_seqno = block.seqno;
            _numberOfActiveBlockKeepersAtBlockStart = _numberOfActiveBlockKeepers;
        }
        if (address(this).balance > 1000000 vmshell) { return; }
        gosh.mintshell(1000000 vmshell);
    }

    function setOwner(address wallet) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) {
        _owner_wallet = wallet;
    }

    function setConfig(uint32 epochDuration, uint64 epochCliff, uint64 waitStep, uint128 minBlockKeepers) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        getMoney();
        _epochDuration = epochDuration;
        _epochCliff = epochCliff;
        _waitStep = waitStep;
        _minBlockKeepers = minBlockKeepers;
    }

    function deployAckiNackiBlockKeeperNodeWallet(uint256 pubkey) public accept {
        getMoney();
        TvmCell data = BlockKeeperLib.composeBlockKeeperWalletStateInit(_code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockKeeperNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_WALLET), wid: 0, flag: 1}(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperEpochCoolerCode], _code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletConfigCode], _code[m_BLSKeyCode], _code[m_SignerIndexCode], _num_wallets);
        _num_wallets += 1;
    }

    function deployAckiNackiBlockKeeperNodeWalletConfig(uint256 pubkey, uint128 num) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        TvmCell data = BlockKeeperLib.composeBlockKeeperWalletConfigStateInit(_code[m_AckiNackiBlockKeeperNodeWalletConfigCode], num);
        new AckiNackiBlockKeeperNodeWalletConfig {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_WALLET_CONFIG), wid: 0, flag: 1}(msg.sender);
    }

    function decreaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        getMoney();
        _totalStake -= stake;
        _numberOfActiveBlockKeepers -= 1;
    }

    function decreaseStakes(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        getMoney();
        _totalStake -= stake;
    }

    function increaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        getMoney();
        _totalStake += stake;
        _numberOfActiveBlockKeepers += 1;
        BlockKeeperEpoch(msg.sender).setStake{value: 0.1 vmshell, flag: 1}(_totalStake, _numberOfActiveBlockKeepersAtBlockStart);
    }

    function receiveBlockKeeperRequestWithStakeFromWallet(uint256 pubkey, bytes bls_pubkey, uint16 signerIndex, mapping(uint8 => string) ProxyList) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        if (msg.currencies[CURRENCIES_ID] < gosh.calcminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1)) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}();
            return;
        }
        uint256 stake = msg.currencies[CURRENCIES_ID];
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender, pubkey, stake, signerIndex);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAccept{value: 0.1 ton, flag: 1}(ProxyList);
    }

    function isBLSAccepted(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint16 signerIndex, mapping(uint8 => string) ProxyList) public view senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {       
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}();
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(msg.sender, pubkey, stake, bls_pubkey);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAccept{value: 0.1 ton, flag: 1}(ProxyList);
    }

    function isSignerIndexAccepted(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint16 signerIndex, mapping(uint8 => string) ProxyList) public view senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {       
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}();
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 ton, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).deployPreEpochContract{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_epochDuration, _epochCliff, _waitStep, bls_pubkey, signerIndex, ProxyList);
    }

    function receiveBlockKeeperRequestWithStakeFromWalletContinue(uint256 pubkey, bytes bls_pubkey, uint64 seqNoStartOld, uint16 signerIndex) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        if (msg.currencies[CURRENCIES_ID] < gosh.calcminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers)) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}();
            return;
        }
        uint256 stake = msg.currencies[CURRENCIES_ID];
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender, pubkey, stake, signerIndex);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAcceptContinue{value: 0.1 ton, flag: 1}(seqNoStartOld); 
    } 

    function isBLSAcceptedContinue(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex) public view senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {       
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}();
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(msg.sender, pubkey, stake, bls_pubkey);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAcceptContinue{value: 0.1 ton, flag: 1}(seqNoStartOld);
    }

    function isSignerIndexContinue(uint256 pubkey, bytes bls_pubkey, uint256 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex) public view senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {       
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}();
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 ton, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).deployBlockKeeperContractContinue{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_epochDuration, _waitStep, seqNoStartOld, bls_pubkey, signerIndex);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg { 
        _code[id] = code;
    }

    function canDeleteEpoch(uint256 pubkey, uint64 seqNoStart, uint256 stake, uint32 epochDuration, uint32 reputationTime, uint256 totalStakeOld, uint128 numberOfActiveBlockKeepers, uint128 time)  public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        if (_numberOfActiveBlockKeepers - 1 >= _minBlockKeepers) {
            uint256 reward = 0;
            reward = gosh.calcbkreward(numberOfActiveBlockKeepers, uint128(epochDuration), time, uint128(totalStakeOld), uint128(stake), uint128(reputationTime));
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

    function getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey);
    }

    function getAckiNackiBlockKeeperNodeWalletConfigAddress(uint128 num) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperWalletConfigAddress(_code[m_AckiNackiBlockKeeperNodeWalletConfigCode] , num);
    }
    
    function getBlockKeeperEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart);
    }

    function getBlockKeeperPreEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address) {
        return BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey, seqNoStart);
    }

    function getDetails() external view returns(uint128 minStake, uint128 numberOfActiveBlockKeepers, uint256 numWallets) {
        return (gosh.calcminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1), _numberOfActiveBlockKeepers, _num_wallets);
    }

    function getEpochCodeHash() external view returns(uint256 epochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochCode(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this)));
    }

    function getPreEpochCodeHash() external view returns(uint256 preEpochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperPreEpochCode(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this)));
    }

    function getRewardOut(
        uint128 numberOfActiveBlockKeepers,
        uint128 stake,
        uint128 totalStake,
        uint128 reputationTime,
        uint128 timenetwork,
        uint128 epochDuration
        ) external pure returns(uint128 reward) {
        return gosh.calcbkreward(numberOfActiveBlockKeepers, epochDuration, timenetwork, totalStake, stake, reputationTime);
    }     

    function getProxyListCode() external view returns(TvmCell) {
        return BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], address(this));
    }

    function getProxyListAddress(uint256 pubkey) external view returns(address) {
        return BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], pubkey, address(this));
    }

    function getRewardNow(
        ) external view returns(uint128 reward) {
        return gosh.calcbkreward(_numberOfActiveBlockKeepers, _epochDuration, uint128(block.timestamp - _networkStart), uint128(_totalStake), uint128(_totalStake), uint128(0));
    }     

    function getMinStakeNow() external view returns(uint128 minstake) {
        return gosh.calcminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1);
    }   

    function getMinStakeOut(
        uint128 numberOfActiveBlockKeepers1,
        uint128 numberOfActiveBlockKeepers2,
        uint128 timenetwork,
        uint128 epochDuration) external pure returns(uint128 minstake) {
        return gosh.calcminstake(epochDuration, timenetwork, numberOfActiveBlockKeepers1, numberOfActiveBlockKeepers2);
    }       

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperContractRoot");
    }   
}
