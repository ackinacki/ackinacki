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
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperCoolerContract.sol";
import "./BLSKeyIndex.sol";
import "./SignerIndex.sol";

contract BlockKeeperContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint64 _epochDuration = 259200;
    uint64 _epochCliff = 25920;
    uint64 _waitStep = 12960;
    uint128 _minBlockKeepers = 12;
    uint256 _totalStake = 0;
    address _licenseRoot;
    uint8 _walletTouch = 200;

    uint32 _networkStart;
    uint128 _numberOfActiveBlockKeepers = 0;

    uint32 _block_seqno = 0;
    uint128 _numberOfActiveBlockKeepersAtBlockStart = 0;
    uint128 _needNumberOfActiveBlockKeepers = 10000;
    bool _isNeedNumberOfActiveBlockKeepers = false;

    uint128 _reward_sum = 0;
    uint128 _slash_sum = 0;
    uint128 _nlinit = 5000;

    uint128 _mbkOld = 0;

    bool _is_close_owner = false;

    optional(address) _owner_wallet;

    uint256 _Gparam = 0;
    uint256 _REMparam = 0; 
    uint32 _Bparam = 0;

    constructor (
        address licenseRoot
    ) {
        _networkStart = block.timestamp;
        _licenseRoot = licenseRoot;
    }

    function ensureBalance() private {
        if (block.seqno != _block_seqno) {
            _block_seqno = block.seqno;
            _numberOfActiveBlockKeepersAtBlockStart = _numberOfActiveBlockKeepers;
        }
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshellq(ROOT_BALANCE);
    }

    function calcParams() private {
        if (block.seqno == 0) { return; }
        uint128 time = block.timestamp - _networkStart;
        uint128 mbk = gosh.calcmbk(time);
        if (mbk > _mbkOld) {
            _REMparam = _REMparam + (mbk - _mbkOld) * uint256(SCALEparam);
            _mbkOld = mbk;
        }
        if (_totalStake == 0) { return; }
        (uint256 resdiv, uint256 newREM) = math.divmod(_REMparam, _totalStake);
        _Gparam += resdiv;
        _REMparam = newREM;
    }


    function setOwner(address wallet) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _owner_wallet = wallet;
    }

    function closeRoot() public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        _is_close_owner = true;
        ensureBalance();
    }

    function setConfig(uint64 epochDuration, uint128 minBlockKeepers, bool isNeedNumberOfActiveBlockKeepers, uint128 needNumberOfActiveBlockKeepers, uint8 walletTouch, uint128 nlinit) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _epochDuration = epochDuration;
        _epochCliff = epochDuration / CONFIG_CLIFF_DENOMINATOR;
        _waitStep = epochDuration / CONFIG_WAIT_DENOMINATOR;
        _minBlockKeepers = minBlockKeepers;
        _isNeedNumberOfActiveBlockKeepers = isNeedNumberOfActiveBlockKeepers;
        _needNumberOfActiveBlockKeepers = needNumberOfActiveBlockKeepers;
        _walletTouch = walletTouch;
        _nlinit = nlinit;
    }

    function setConfigNode(uint64 epochDuration, uint128 minBlockKeepers, bool isNeedNumberOfActiveBlockKeepers, uint128 needNumberOfActiveBlockKeepers, uint8 walletTouch, uint128 nlinit) public senderIs(address(this)) accept {
        ensureBalance();
        _epochDuration = epochDuration;
        _epochCliff = epochDuration / CONFIG_CLIFF_DENOMINATOR;
        _waitStep = epochDuration / CONFIG_WAIT_DENOMINATOR;
        _minBlockKeepers = minBlockKeepers;
        _isNeedNumberOfActiveBlockKeepers = isNeedNumberOfActiveBlockKeepers;
        _needNumberOfActiveBlockKeepers = needNumberOfActiveBlockKeepers;
        _walletTouch = walletTouch;
        _nlinit = nlinit;
    }

    function deployAckiNackiBlockKeeperNodeWallet(uint256 pubkey, mapping(uint256 => bool) whiteListLicense) public accept {
        ensureBalance();
        require(whiteListLicense.keys().length <= MAX_LICENSE_NUMBER_WHITELIST_BK, ERR_TOO_MANY_LICENSES);
        TvmCell data = BlockKeeperLib.composeBlockKeeperWalletStateInit(_code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockKeeperNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_WALLET), wid: 0, flag: 1}(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperEpochCoolerCode], _code[m_BlockKeeperEpochProxyListCode], _code[m_BLSKeyCode], _code[m_SignerIndexCode], _code[m_LicenseCode], _epochDuration, _networkStart + UNLOCK_LICENSES, whiteListLicense, _licenseRoot, 0, _walletTouch);
    }

    function coolerSlash(uint64 seqNoStart, uint256 pubkey, uint128 licenseCount, bool isFull) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        _slash_sum += uint128(msg.currencies[CURRENCIES_ID]);
        if (isFull) {
            _nlinit -= licenseCount;
        }
    }

    function decreaseStakes(uint256 pubkey, uint64 seqNoStart, uint128 slash_stake, uint128 rep_coef) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        if (_totalStake != 0) {
            calcParams();
            _totalStake -= slash_stake * rep_coef;
        }
        _slash_sum += uint128(msg.currencies[CURRENCIES_ID]);

    }

    function increaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint128 stake, uint128 rep_coef, optional(uint128) virtualStake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        uint128 local_stake = stake;
        if (virtualStake.hasValue()) {
            local_stake = virtualStake.get();
        }
        calcParams();
        _totalStake += local_stake * rep_coef;
        _numberOfActiveBlockKeepers += 1;
        BlockKeeperEpoch(msg.sender).setStake{value: 0.1 vmshell, flag: 1}(_numberOfActiveBlockKeepersAtBlockStart, _Gparam);
    }

    function receiveBlockKeeperRequestWithStakeFromWallet(uint256 pubkey, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, bool is_min, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        ensureBalance();
        if (_epochDuration * CLOSE_EPOCH_NUMBER > block.seqno) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}(_epochDuration);
        }
        optional(uint128) virtualStake;
        uint128 diff = 0;
        if (_reward_sum > _slash_sum) {
            diff = _reward_sum - _slash_sum;
        }
        uint128 minStake = 0;
        if (_isNeedNumberOfActiveBlockKeepers) {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        } else {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _numberOfActiveBlockKeepersAtBlockStart);
        }
        uint128 stake = uint128(msg.currencies[CURRENCIES_ID]);
        if (stake < minStake) {
            if (is_min == false) {
                AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}(_epochDuration);
                return;
            } else {
                virtualStake = minStake;
            }
        }
        uint128 maxStake = 0;
        maxStake = calculateMaxStake(diff);
        if (minStake > maxStake) {
            maxStake = minStake;
        }
        if ((stake > maxStake) && (virtualStake.hasValue() == false)) {
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(stake - maxStake);
            msg.sender.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
            stake = maxStake;
        }
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAccept{value: 0.1 vmshell, flag: 1}(msg.sender, signerIndex, pubkey, rep_coef, stake, virtualStake, ProxyList, myIp);
    }

    function isBLSAccepted(address wallet, uint256 pubkey, bytes bls_pubkey, uint128 stake, bool isNotOk, uint16 signerIndex,uint128 rep_coef, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {  
        ensureBalance();     
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_epochDuration);
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(wallet);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAccept{value: 0.1 vmshell, flag: 1}(wallet, bls_pubkey, pubkey, rep_coef, stake, virtualStake, ProxyList, myIp);
    }

    function isSignerIndexAccepted(address wallet, uint256 pubkey, bytes bls_pubkey, uint128 stake, bool isNotOk, uint16 signerIndex, uint128 rep_coef, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp) public senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {   
        ensureBalance();    
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).stakeNotAccepted{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_epochDuration);
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 vmshell, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(wallet).deployPreEpochContract{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_epochDuration, _walletTouch, _epochCliff, _waitStep, bls_pubkey, signerIndex, rep_coef, virtualStake, ProxyList, _reward_sum, myIp);
    }

    function calculateMaxStake(uint128 diff) private view returns(uint128) {
        return (diff * KSMAX * MAX_LICENSE_NUMBER) / (KSMAX_DENOMINATOR * _nlinit);
    }

    function receiveBlockKeeperRequestWithStakeFromWalletContinue(uint256 pubkey, bytes bls_pubkey, uint64 seqNoStartOld, uint16 signerIndex, bool is_min, mapping(uint8 => string) ProxyList, uint64 seqNoFinish, uint128 sumReputationCoef) public senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        ensureBalance();
        uint128 stake = uint128(msg.currencies[CURRENCIES_ID]);
        uint128 diff = 0;
        if (_reward_sum > _slash_sum) {
            diff = _reward_sum - _slash_sum;
        }
        uint128 minStake;
        if (_isNeedNumberOfActiveBlockKeepers) {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128((seqNoFinish - block.seqno) * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        } else {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128((seqNoFinish - block.seqno) * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _numberOfActiveBlockKeepersAtBlockStart);
        }
        optional(uint128) virtualStake;
        if (stake < minStake) {
            if (is_min == false) {
                AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: msg.currencies, flag: 1}(_epochDuration);
                return;
            } else {
                virtualStake = minStake;
            }
        }
        uint128 maxStake = calculateMaxStake(diff);
        if (minStake > maxStake) {
            maxStake = minStake;
        }
        if ((stake > maxStake) && (virtualStake.hasValue() == false)){
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(stake - maxStake);
            msg.sender.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
            stake = maxStake;
        }
        TvmCell data = BlockKeeperLib.composeBLSKeyStateInit(_code[m_BLSKeyCode], bls_pubkey, address(this));
        new BLSKeyIndex {stateInit: data, value: varuint16(FEE_DEPLOY_BLS_KEY), wid: 0, flag: 1}(msg.sender);
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).isBLSKeyAcceptContinue{value: 0.1 vmshell, flag: 1}(msg.sender, signerIndex, pubkey, seqNoStartOld, stake, virtualStake, ProxyList, sumReputationCoef); 
    } 

    function isBLSAcceptedContinue(address wallet, uint256 pubkey, bytes bls_pubkey, uint128 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef) public senderIs(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))) accept {       
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(wallet).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_epochDuration);
            return;
        }
        TvmCell data = BlockKeeperLib.composeSignerIndexStateInit(_code[m_SignerIndexCode], signerIndex, address(this));
        new SignerIndex {stateInit: data, value: varuint16(FEE_DEPLOY_SIGNER_INDEX), wid: 0, flag: 1}(wallet);
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))).isSignerIndexAcceptContinue{value: 0.1 vmshell, flag: 1}(wallet, bls_pubkey, pubkey, seqNoStartOld, stake, virtualStake, ProxyList, sumReputationCoef);
    }

    function isSignerIndexContinue(address wallet, uint256 pubkey, bytes bls_pubkey, uint128 stake, bool isNotOk, uint64 seqNoStartOld, uint16 signerIndex, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef) public senderIs(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, address(this))) accept {       
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(stake);
        if (isNotOk == true) {
            AckiNackiBlockKeeperNodeWallet(wallet).stakeNotAcceptedContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1}(_epochDuration);
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, address(this))).destroyRoot{value: 0.1 vmshell, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)).deployBlockKeeperContractContinue{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 vmshell), currencies: data_cur, flag: 1}(_walletTouch, seqNoStartOld, bls_pubkey, signerIndex, virtualStake, ProxyList, sumReputationCoef);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept { 
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        ensureBalance();
        _code[id] = code;
    }

    function setNewCodeNode(uint8 id, TvmCell code) public senderIs(address(this)) accept { 
        ensureBalance();
        _code[id] = code;
    }

    function decreaseActiveBlockKeeper(
        uint256 pubkey,
        uint128 rep_coef,
        uint64 seqNoStart,
        uint128 stake,
        optional(uint128) virtualStake,
        uint128 reward_sum,
        uint256 gparam,
        bool isSlash
    ) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        ensureBalance();
        uint128 stakeToUse = stake;
        if (virtualStake.hasValue()) {
            stakeToUse = virtualStake.get();
        }
        if ((isSlash) || (_numberOfActiveBlockKeepers - 1 >= _minBlockKeepers)) {
            calcParams();
            _totalStake -= stakeToUse * rep_coef;
        }
        if (isSlash) {
            _numberOfActiveBlockKeepers -= 1;
            _slash_sum += uint128(stake); 
            return;
        }
        if (_numberOfActiveBlockKeepers - 1 >= _minBlockKeepers) {
            _numberOfActiveBlockKeepers -= 1;
            (uint128 reward, uint256 rewardRem) = calcBKReward(
                reward_sum,
                rep_coef,
                uint128(stakeToUse),
                gparam
            );
            _REMparam = _REMparam + rewardRem;
            _reward_sum += reward;
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(reward);
            gosh.mintecc(uint64(reward), CURRENCIES_ID);
            BlockKeeperEpoch(msg.sender).canDelete{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET + 1 vmshell), currencies: data_cur, flag: 1}(reward, _epochDuration, _waitStep, _reward_sum);
        } else {
            BlockKeeperEpoch(msg.sender).cantDelete{value: 0.1 vmshell, flag: 1}();
        }
    }

    function calcBKReward(
        uint128 reward_sum,
        uint128 rep_coef, 
        uint128 stake,
        uint256 gparam
        ) private view returns (uint128, uint256) {
        if (reward_sum > BK_TOTAL_SUPPLY) {
            return (0, 0);
        }
        (uint256 res, uint256 rewardRem)= math.divmod(uint256(stake) * uint256(rep_coef) * (_Gparam - gparam), SCALEparam);
        return (uint128(res), rewardRem);
    }


    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept {
        ensureBalance();
        require(_is_close_owner == false, ERR_SENDER_NO_ALLOWED);
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function updateCodeNode(TvmCell newcode, TvmCell cell) public senderIs(address(this)) accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private pure {
    }
    
    //Fallback/Receive
    receive() external {
        if (msg.currencies[CURRENCIES_ID] != 0) {
            tvm.accept();
            _slash_sum += uint128(msg.currencies[CURRENCIES_ID]);
        }
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
        uint128 diff = 0;
        if (_reward_sum > _slash_sum) {
            diff = _reward_sum - _slash_sum;
        }
        if (_isNeedNumberOfActiveBlockKeepers) {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        } else {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _numberOfActiveBlockKeepersAtBlockStart);
        }
        return (minStake, _numberOfActiveBlockKeepers);
    }

    function getEpochCodeHash() external view returns(uint256 epochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochCode(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this)));
    }

    function getPreEpochCodeHash() external view returns(uint256 preEpochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperPreEpochCode(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this)));
    }

    function getRewardOut(
        uint128 reward_sum,
        uint128 rep_coef,
        uint256 gparam,
        uint128 stake
        ) external view returns(uint128 reward) {
        (uint128 res, ) = calcBKReward(
            reward_sum,
            rep_coef,
            stake,
            gparam
        );
        return res;
    }     

    function getProxyListCode() external view returns(TvmCell code) {
        return BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], address(this));
    }

    function getProxyListAddress(uint256 pubkey) external view returns(address proxyAddress) {
        return BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], pubkey, address(this));
    }

    function getRewardNow(
        uint128 rep_coef,
        uint128 stake) external view returns(uint128 reward) {
        (uint128 res,) = calcBKReward(
            _reward_sum,
            rep_coef,
            stake,
            _Gparam
        );
        return res;
    }     

    function getMinStakeNow() external view returns(uint128 minstake) {
        uint128 diff = 0;
        if (_reward_sum > _slash_sum) {
            diff = _reward_sum - _slash_sum;
        }
        uint128 minStake;
        if (_isNeedNumberOfActiveBlockKeepers) {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _needNumberOfActiveBlockKeepers);
        } else {
            minStake = gosh.calcminstake(diff, block.timestamp - _networkStart + uint128(_epochCliff * BLOCKS_PER_SECOND_DENOMINATOR / BLOCKS_PER_SECOND), _numberOfActiveBlockKeepersAtBlockStart, _numberOfActiveBlockKeepersAtBlockStart);
        }
        return minStake;  
    }

    function getMaxStakeNow() external view returns(uint128 maxstake) {
        uint128 diff = 0;
        if (_reward_sum > _slash_sum) {
            diff = _reward_sum - _slash_sum;
        }
        return calculateMaxStake(diff);
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
