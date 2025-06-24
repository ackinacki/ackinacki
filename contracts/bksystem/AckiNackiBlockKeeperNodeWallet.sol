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
import "./BlockKeeperContractRoot.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BLSKeyIndex.sol";
import "./SignerIndex.sol";
import "./License.sol";

contract AckiNackiBlockKeeperNodeWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    mapping (uint256 => Stake) _activeStakes;
    uint8 _stakesCnt = 0;

    address _bk_root = address.makeAddrStd(0, 0);

    mapping(uint128 => LockStake) _lockmap;
    mapping(bytes => bool) _bls_keys;

    mapping(uint256 => LicenseData) _licenses;
    uint128 _licenses_count;

    uint128 _epochDuration;
    uint128 _last_stake = 0;

    uint32  _freeLicense;
    bool _isWaitStake = false;
    uint128 _balance = 0;
    mapping(uint256=>bool) _whiteListLicense;
    address _licenseRoot;

    constructor (
        TvmCell BlockKeeperPreEpochCode,
        TvmCell AckiNackiBlockKeeperNodeWalletCode,
        TvmCell BlockKeeperEpochCode,
        TvmCell BlockKeeperEpochCoolerCode,
        TvmCell BlockKeeperEpochProxyListCode,
        TvmCell BLSKeyCode,
        TvmCell SignerIndexCode,
        TvmCell LicenseCode,
        uint128 epochDuration,
        uint32 freeLicense,
        mapping(uint256=>bool) whiteListLicense,
        address licenseRoot,
        uint8 stakesCnt
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);
        _code[m_AckiNackiBlockKeeperNodeWalletCode] = AckiNackiBlockKeeperNodeWalletCode;
        _code[m_BlockKeeperPreEpochCode] = BlockKeeperPreEpochCode;
        _code[m_BlockKeeperEpochCode] = BlockKeeperEpochCode;
        _code[m_BlockKeeperEpochCoolerCode] = BlockKeeperEpochCoolerCode;
        _code[m_BlockKeeperEpochProxyListCode] = BlockKeeperEpochProxyListCode;
        _code[m_BLSKeyCode] = BLSKeyCode;
        _code[m_SignerIndexCode] = SignerIndexCode;
        _code[m_LicenseCode] = LicenseCode;
        _epochDuration = epochDuration;
        _freeLicense = freeLicense;
        _whiteListLicense = whiteListLicense;
        _licenseRoot = licenseRoot;
        _stakesCnt = stakesCnt;
    }

    function setLockToStake(uint256 license_number, bool lock) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        _licenses[license_number].isLockToStake = lock;
    }

    function removeLicense(uint256 license_number, address to) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST); 
        require(_licenses[license_number].status == LICENSE_REST, ERR_LICENSE_BUSY); 
        _licenses_count -= 1;
        _balance -= _licenses[license_number].balance;
        LicenseContract(msg.sender).deleteLicense{value: 0.1 vmshell, flag: 1}(_licenses[license_number].reputationTime, _licenses[license_number].isPriority, _licenses[license_number].last_touch);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = _licenses[license_number].balance;
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
        delete _licenses[license_number];
    }

    function setLicenseWhiteList(mapping(uint256 => bool) whiteListLicense) public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        ensureBalance();
        _whiteListLicense = whiteListLicense;
    }

    function addLicense(uint256 license_number, uint128 reputationTime, uint32 last_touch, bool isPriority) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        if ((_licenses_count >= MAX_LICENSE_NUMBER) || (_whiteListLicense[license_number] != true)) {
            LicenseContract(msg.sender).notAcceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
            return;
        }
        _licenses_count += 1;
        _licenses[license_number] = LicenseData(reputationTime, LICENSE_REST, isPriority, null, last_touch, 0, 0, 0, 0, false);
        LicenseContract(msg.sender).acceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey, last_touch);
    }

    function addBalance(uint256 license_number) public {
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        tvm.accept();
        _licenses[license_number].balance += uint128(msg.currencies[CURRENCIES_ID]);
        _balance += uint128(msg.currencies[CURRENCIES_ID]);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3);
    }

    function setLockStakeHelper(uint8 i, LicenseStake[] licenses) private {
        if (_licenses.exists(licenses[i].num)){
            _licenses[licenses[i].num].status = LICENSE_PRE_EPOCH;
            _licenses[licenses[i].num].stakeController = msg.sender;
            _licenses[licenses[i].num].last_touch = block.timestamp;
        }
    }

    function setLockStake(uint64 seqNoStart, uint256 stake, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            setLockStakeHelper(i, licenses);
        }
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, 0, bls_key, PRE_EPOCH_DEPLOYED, signerIndex);
        _bls_keys[bls_key] = true;
        _isWaitStake = false;
    }

    function deleteLockStakeHelper(uint8 i, LicenseStake[] licenses) private {
        if (_licenses.exists(licenses[i].num)){
            _licenses[licenses[i].num].status = LICENSE_REST;
            _licenses[licenses[i].num].stakeController = null;
            _licenses[licenses[i].num].lockStake = 0;
            _licenses[licenses[i].num].last_touch = block.timestamp;
        }
    }

    function deleteLockStake(uint64 seqNoStart, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
       require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deleteLockStakeHelper(i, licenses);
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[stakeHash];
        delete _bls_keys[bls_key];
        _stakesCnt -= 1;
    } 

    function slash(uint8 slash_type, bytes bls_key) internalMsg public view senderIs(address(this)) {
        require(slash_type < 100, ERR_WRONG_DATA);
        this.iterateStakes{value: 0.1 vmshell, flag: 1}(slash_type, bls_key, _activeStakes.min());
    }

    function iterateStakes(uint8 slash_type, bytes bls_key, optional(uint256, Stake) data) public view senderIs(address(this)) {
        if (data.hasValue() == false) {
            return;
        }  
        (uint256 key, Stake value) = data.get();
        if (value.bls_key == bls_key) {
            if (value.status == 1) {
                BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, value.seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type);
                return;
            } else {
                BlockKeeperCooler(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _activeStakes[key].seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type);
                return;
            }     
        }        
        this.iterateStakes{value: 0.1 vmshell, flag: 1}(slash_type, bls_key, _activeStakes.next(key));
    }

    function updateLockStakeHelper(uint8 i, LicenseStake[] licenses, bool is_continue) private {
        if (_licenses.exists(licenses[i].num)){
                _licenses[licenses[i].num].status = LICENSE_EPOCH;
                _licenses[licenses[i].num].stakeController = msg.sender;
                _licenses[licenses[i].num].last_touch = block.timestamp;
                if (is_continue) {
                    _licenses[licenses[i].num].lockStake = _licenses[licenses[i].num].lockContinue;
                    _licenses[licenses[i].num].lockContinue = 0;
                }
            }
    }

    function updateLockStake(uint64 seqNoStart, uint64 seqNoFinish, uint256 stake, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses, bool is_continue) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeHelper(i, licenses, is_continue);
        }
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, seqNoFinish, bls_key, EPOCH_DEPLOYED, signerIndex);
    }

    function clearLicenseStake(LicenseStake license, bool isContinue) private {
        if (isContinue) {
            _licenses[license.num].lockContinue = 0;
        } else {
            _licenses[license.num].lockStake = 0;
        }
    }

    function stakeNotAccepted(LicenseStake[] licenses) public senderIs(_root) accept {
        uint8 i = 0; 
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], false);
        }
        _stakesCnt -= 1;
        _isWaitStake = false;
    }

    function stakeNotAcceptedContinue(LicenseStake[] licenses) public senderIs(_root) accept {
        _isWaitStake = false;
        uint8 i = 0; 
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
    }

    function sendBlockKeeperRequestWithStakeHelperFirst(optional(uint256, LicenseData) data, uint128 sumBalance, bool is_ok, uint128 sumreputationCoef) private returns(optional(uint256, LicenseData), uint128, bool, uint128){
        (uint256 num, LicenseData value) = data.get();
        if (value.isLockToStake == false) {
            sumBalance += _licenses[num].balance - _licenses[num].lockCooler;
            if ((_licenses[num].last_touch + _epochDuration / 6 < block.timestamp) || (!_licenses[num].isPriority)) {
                is_ok = false;            
                _licenses[num].reputationTime = 0;
                _licenses[num].isPriority = false;
            }
            sumreputationCoef += gosh.calcrepcoef(value.reputationTime);
        }
        data = _licenses.next(num);
        return (data, sumBalance, is_ok, sumreputationCoef);
    }

    function sendBlockKeeperRequestWithStakeHelperSecond(optional(uint256, LicenseData) data, uint128 sumBalance, LicenseStake[] licenses, varuint32 stake) private returns(optional(uint256, LicenseData), LicenseStake[]){
        (uint256 num, LicenseData value) = data.get();
        if (value.isLockToStake == false) {
            uint128 lock = 0;
            if (sumBalance != 0) {
                lock = math.divc(uint128((_licenses[num].balance - value.lockCooler) * stake), sumBalance);
                _licenses[num].lockStake = lock;
            }
            licenses.push(LicenseStake(num, lock));
        }
        data = _licenses.next(num);
        return (data, licenses);
    }

    function sendBlockKeeperRequestWithStake(bytes bls_pubkey, varuint32 stake, uint16 signerIndex, mapping(uint8 => string) ProxyList, string myIp) public  onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        ensureBalance();
        require(bls_pubkey.length == 48, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 0, ERR_STAKE_EXIST);
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(_licenses_count > 0, ERR_TOO_LOW_LICENSES);
        require(signerIndex <= MAX_SIGNER_INDEX, ERR_WRONG_SIGNER_INDEX);
        _stakesCnt += 1;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        uint128 sumreputationCoef;
        bool is_ok = true;
        LicenseStake[] licenses;
        uint128 sumBalance = 0;
        optional(uint256, LicenseData) data = _licenses.min();
        uint8 i = 0; 
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeHelperFirst(data, sumBalance, is_ok, sumreputationCoef);
        }
        require(data.hasValue() == false, ERR_NOT_SUPPORT); 
        data = _licenses.min();
        i = 0;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeHelperSecond(data, sumBalance, licenses, stake);
        }
        require(data.hasValue() == false, ERR_NOT_SUPPORT); 
        if ((stake < sumBalance) || (_freeLicense < block.timestamp)) {
            is_ok = false;
        }
        _isWaitStake = true;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWallet{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, signerIndex, sumreputationCoef, licenses, is_ok, ProxyList, myIp);
    } 

    function sendBlockKeeperRequestWithCancelStakeContinue(uint64 seqNoStartOld) public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)).cancelContinueStake{value: 0.1 vmshell, flag: 1}();    
    } 

    function cancelContinueStakeHelper(uint8 i, LicenseStake[] licenses) private {
        if (_licenses.exists(licenses[i].num)){
            if (_licenses[licenses[i].num].status == LICENSE_CONTINUE) {
                _licenses[licenses[i].num].status = LICENSE_REST;
                _licenses[licenses[i].num].stakeController = null;
            }
            _licenses[licenses[i].num].lockContinue = 0;
        }
    }

    function cancelContinueStake(uint64 seqNoStartOld, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            cancelContinueStakeHelper(i, licenses);
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
    }

    function sendBlockKeeperRequestWithStakeContinueFirstHelper(optional(uint256, LicenseData) data, uint128 sumBalance, bool is_ok, uint128 sumreputationCoef) private view returns(optional(uint256, LicenseData), uint128, bool, uint128){
        (uint256 num, LicenseData value) = data.get();
        if (value.isLockToStake == false) {
            sumBalance += value.balance - value.lockStake - value.lockCooler;
            if (!_licenses[num].isPriority) {
                is_ok = false;
            }
            sumreputationCoef += gosh.calcrepcoef(value.reputationTime);
        }
        data = _licenses.next(num);
        return (data, sumBalance, is_ok, sumreputationCoef);
    }

    function sendBlockKeeperRequestWithStakeContinueSecondHelper(optional(uint256, LicenseData) data, LicenseStake[] licenses, uint128 sumBalance, varuint32 stake) private returns(optional(uint256, LicenseData), LicenseStake[]){
        (uint256 num, LicenseData value) = data.get();
        if (value.isLockToStake == false) {
            uint128 lock = 0;
            if (sumBalance != 0) {
                lock = math.divc(uint128((_licenses[num].balance - value.lockStake - value.lockCooler) * stake), sumBalance);
                _licenses[num].lockContinue = lock;
            }
            licenses.push(LicenseStake(num, lock));
        }
        data = _licenses.next(num);
        return (data, licenses);
    }

    function sendBlockKeeperRequestWithStakeContinue(bytes bls_pubkey, varuint32 stake, uint64 seqNoStartOld, uint16 signerIndex, mapping(uint8 => string) ProxyList) public onlyOwnerPubkey(_owner_pubkey) accept saveMsg {
        ensureBalance();
        require(_licenses_count > 0, ERR_TOO_LOW_LICENSES);
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(signerIndex <= MAX_SIGNER_INDEX, ERR_WRONG_SIGNER_INDEX);
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        require(bls_pubkey.length == 48, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 1, ERR_STAKE_EXIST);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        uint128 sumreputationCoef;
        LicenseStake[] licenses;
        bool is_ok = true;
        uint128 sumBalance = 0;
        optional(uint256, LicenseData) data = _licenses.min();
        uint8 i = 0; 
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        i += 1;
        if (data.hasValue()) {
            (data, sumBalance, is_ok, sumreputationCoef) = sendBlockKeeperRequestWithStakeContinueFirstHelper(data, sumBalance, is_ok, sumreputationCoef);
        }
        require(data.hasValue() == false, ERR_NOT_SUPPORT); 
        data = _licenses.min();
        i = 0;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        i += 1;
        if (data.hasValue()) {
            (data, licenses) = sendBlockKeeperRequestWithStakeContinueSecondHelper(data, licenses, sumBalance, stake);
        }
        require(data.hasValue() == false, ERR_NOT_SUPPORT); 
        if ((stake < sumBalance) || (_freeLicense < block.timestamp)) {
            is_ok = false;
        }
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWalletContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, seqNoStartOld, signerIndex, sumreputationCoef, licenses, is_ok, ProxyList);
    } 

    function deployPreEpochContract(uint64 epochDuration, uint64 epochCliff, uint64 waitStep, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 reward_sum, string myIp) public senderIs(_root) accept  {
        ensureBalance();
        uint64 seqNoStart = block.seqno + epochCliff;
        TvmCell data = BlockKeeperLib.composeBlockKeeperPreEpochStateInit(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        _isWaitStake = false;
        if ((_activeStakes.exists(stakeHash) == true) || (_stakesCnt > 1)) {
            _stakesCnt -= 1;
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            return;
        }
        new BlockKeeperPreEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST + 1 vmshell),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, signerIndex, rep_coef, licenses, virtualStake, ProxyList, reward_sum, myIp);
    }

    function deployBlockKeeperContractContinueHelper(uint8 i, LicenseStake[] licenses, address epoch_addr) private {
        if (_licenses.exists(licenses[i].num)){
            if (_licenses[licenses[i].num].status == LICENSE_REST) {
                _licenses[licenses[i].num].status = LICENSE_CONTINUE;
                _licenses[licenses[i].num].stakeController = epoch_addr;
                _licenses[licenses[i].num].last_touch = block.timestamp;
            }
            _licenses[licenses[i].num].lockContinue = 0;
        }
    }

    function deployBlockKeeperContractContinue(uint64 epochDuration, uint64 waitStep, uint64 seqNoStartold, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, LicenseStake[] licenses, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 reward_sum) public senderIs(_root) accept  {
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStartold);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        if (_activeStakes.exists(stakeHash) != true) {
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            return;
        }
        address epoch_addr = BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartold);
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            deployBlockKeeperContractContinueHelper(i, licenses, epoch_addr);
        }
        _isWaitStake = false;
        BlockKeeperEpoch(epoch_addr).continueStake{value: 0.1 vmshell, flag: 1, currencies: data_cur}(epochDuration, waitStep, bls_pubkey, signerIndex, rep_coef, licenses, virtualStake, ProxyList, reward_sum);    
    }

    function continueStakeNotAccept(uint64 seqNoStartOld, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        uint8 i = 0; 
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
        if (licenses.length > i) {
            clearLicenseStake(licenses[i], true);
        }
        i += 1;
    }

    function deployBlockKeeperContractContinueAfterDestroyHelper(uint8 i, LicenseStake[] licenses, mapping(uint256 => bool) licensesMap, uint128 addReputationTime,  uint128 reputationCoef) private returns(uint128){
        if (_licenses.exists(licenses[i].num)) {
            if (licensesMap.exists(licenses[i].num)) {
                uint128 diff = gosh.calcrepcoef(_licenses[licenses[i].num].reputationTime + addReputationTime) - gosh.calcrepcoef(_licenses[licenses[i].num].reputationTime);
                reputationCoef += diff;
            }
            _licenses[licenses[i].num].reputationTime += addReputationTime;
            _licenses[licenses[i].num] = LicenseData(_licenses[licenses[i].num].reputationTime, LICENSE_CONTINUE, _licenses[licenses[i].num].isPriority, null, block.timestamp, _licenses[licenses[i].num].balance, 0, _licenses[licenses[i].num].lockContinue, _licenses[licenses[i].num].lockCooler + licenses[i].stake, _licenses[licenses[i].num].isLockToStake);
        }
        return reputationCoef;
    }

    function deployBlockKeeperContractContinueAfterDestroy(uint64 epochDuration, uint64 waitStep, bytes bls_pubkey, uint64 seqNoStartOld, uint128 reputationCoef, uint16 signerIndex, LicenseStake[] licenses_continue, LicenseStake[] licenses, optional(uint128) virtualStake, uint128 addReputationTime, uint128 reward_sum, string myIp) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept  {
        ensureBalance();
        uint64 seqNoStart = block.seqno;
        mapping(uint256 => bool) licensesMap;
        uint8 i = 0;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i += 1;
        if (i + 1 <= licenses_continue.length) {
            licensesMap[licenses_continue[i].num] = true;
        }
        i = 0;
        if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        i += 1;if (i + 1 <= licenses.length) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(i, licenses, licensesMap, addReputationTime, reputationCoef);
        }
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart);
        new BlockKeeperEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: msg.currencies,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, true, reputationCoef, signerIndex, licenses, virtualStake, reward_sum, myIp);
    }

    function updateLockStakeCoolerHelper(uint8 i, LicenseStake[] licenses, bool isContinue) private {
        if (_licenses.exists(licenses[i].num)) {
            if (isContinue == false) {
                _licenses[licenses[i].num] = LicenseData(_licenses[licenses[i].num].reputationTime, LICENSE_REST, _licenses[licenses[i].num].isPriority, null, _licenses[licenses[i].num].last_touch, _licenses[licenses[i].num].balance, 0, _licenses[licenses[i].num].lockContinue, _licenses[licenses[i].num].lockCooler + licenses[i].stake, _licenses[licenses[i].num].isLockToStake);
            } else {
                _licenses[licenses[i].num] = LicenseData(_licenses[licenses[i].num].reputationTime, LICENSE_CONTINUE, _licenses[licenses[i].num].isPriority, null, _licenses[licenses[i].num].last_touch, _licenses[licenses[i].num].balance, 0, _licenses[licenses[i].num].lockContinue, _licenses[licenses[i].num].lockCooler + licenses[i].stake, _licenses[licenses[i].num].isLockToStake);
            }
        }

    }

    function updateLockStakeCooler(uint64 seqNoStart, uint64 seqNoFinish, LicenseStake[] licenses, uint128 epochDuration, bool isContinue) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        _epochDuration = epochDuration;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        i += 1;
        if (i + 1 <= licenses.length) {
            updateLockStakeCoolerHelper(i, licenses, isContinue);
        } 
        if (isContinue == false) {       
            _stakesCnt -= 1;
        }
        _activeStakes[hash].status = COOLER_DEPLOYED;
        _activeStakes[hash].seqNoFinish = seqNoFinish;
    } 

    function unlockStakeCoolerHelper(uint8 i, LicenseStake[] licenses, uint128 sum, uint128 reward) public {
        if (_licenses.exists(licenses[i].num)) {
            uint128 value = 0;
            if (sum == 0) {
                value = reward / uint128(licenses.length);
            } else {
                value = math.divr(reward * licenses[i].stake, sum);
            }
            _licenses[licenses[i].num].lockCooler -= licenses[i].stake;
            _balance += value - licenses[i].stake;
        }   
    }

    function unlockStakeCooler(uint64 seqNoStart, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        delete _bls_keys[_activeStakes[hash].bls_key];
        uint128 reward = 0;
        if (uint128(msg.currencies[CURRENCIES_ID]) > _last_stake) {
            reward = uint128(msg.currencies[CURRENCIES_ID]) - _last_stake;
        }
        _last_stake = uint128(msg.currencies[CURRENCIES_ID]);
        uint128 sum = 0;
       require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            sum += licenses[i].stake;
        }
        i = 0;
        if (i + 1 <= licenses.length) {  
            unlockStakeCoolerHelper(i, licenses, sum, reward);   
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);     
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward); 
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            unlockStakeCoolerHelper(i, licenses, sum, reward);      
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[hash];
    } 

    function slashCoolerFirstHelper(uint8 i, LicenseStake[] licenses) private {
        if (_licenses.exists(licenses[i].num)) {
            if (_licenses[licenses[i].num].status == LICENSE_PRE_EPOCH) {
                uint128 diff = gosh.calcrepcoef(_licenses[licenses[i].num].reputationTime) - gosh.calcrepcoef(0);
                BlockKeeperPreEpoch(_licenses[licenses[i].num].stakeController.get()).changeReputation{value: 0.1 vmshell, flag: 1}(false, diff);
            } else 
            if (_licenses[licenses[i].num].status == LICENSE_EPOCH) {
                uint128 diff = gosh.calcrepcoef(_licenses[licenses[i].num].reputationTime) - gosh.calcrepcoef(0);
                BlockKeeperEpoch(_licenses[licenses[i].num].stakeController.get()).changeReputation{value: 0.1 vmshell, flag: 1}(false, diff, licenses[i].num);
            } else 
            if (_licenses[licenses[i].num].status == LICENSE_CONTINUE) {
                uint128 diff = gosh.calcrepcoef(_licenses[licenses[i].num].reputationTime) - gosh.calcrepcoef(0);
                BlockKeeperEpoch(_licenses[licenses[i].num].stakeController.get()).changeReputationContinue{value: 0.1 vmshell, flag: 1}(diff);
            } 
            _licenses[licenses[i].num].reputationTime = 0;
            _licenses[licenses[i].num].balance -= licenses[i].stake;
            _licenses[licenses[i].num].lockCooler -= licenses[i].stake;
            _licenses[licenses[i].num].isPriority = false;
        }  
    }

    function slashCoolerSecondHelper(uint8 i, LicenseStake[] licenses, uint8 slash_type) private {
        if (_licenses.exists(licenses[i].num)) {
            _licenses[licenses[i].num].reputationTime = 0;
            _licenses[licenses[i].num].balance -= licenses[i].stake * slash_type / FULL_STAKE_PERCENT;
        }
    } 

    function slashCoolerFull(uint64 seqNoStart, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        delete _bls_keys[_activeStakes[stakeHash].bls_key];
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[stakeHash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[stakeHash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[stakeHash];
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);        
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);    
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);       
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerFirstHelper(i, licenses);          
        }
        _last_stake = 0;
    }

    function slashCoolerPart(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {     
            slashCoolerSecondHelper(i, licenses, slash_type);      
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);       
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashCoolerSecondHelper(i, licenses, slash_type);         
        }
        _last_stake -= _last_stake * slash_type / FULL_STAKE_PERCENT;   
        _activeStakes[stakeHash].stake -= slash_stake; 
    }

    function slashCooler(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();        
        if (slash_type == FULL_STAKE_SLASH) {
            slashCoolerFull(seqNoStart, licenses);
        }
        else {
            slashCoolerPart(seqNoStart, slash_type, slash_stake, licenses);
        }
    }

    function slashStakeFirstHelper(uint8 i, LicenseStake[] licenses) private {
        if (_licenses.exists(licenses[i].num)) {
            _licenses[licenses[i].num].reputationTime = 0;
            _licenses[licenses[i].num].balance -= licenses[i].stake;
            _licenses[licenses[i].num].lockStake = 0;
            _licenses[licenses[i].num].isPriority = false;
        }          
    }

    function slashStakeSecondHelper(uint8 i, LicenseStake[] licenses, uint8 slash_type) private {
        if (_licenses.exists(licenses[i].num)) {
            _licenses[licenses[i].num].balance -= licenses[i].stake * slash_type / FULL_STAKE_PERCENT;
            _licenses[licenses[i].num].lockStake -= licenses[i].stake * slash_type / FULL_STAKE_PERCENT;
        }     
    }

    function slashStakeFull(uint64 seqNoStart, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        if (_activeStakes[hash].status == EPOCH_DEPLOYED) {
            _stakesCnt -= 1;
        }
        delete _bls_keys[_activeStakes[hash].bls_key];
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[hash];
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {  
            slashStakeFirstHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);  
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);       
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeFirstHelper(i, licenses);         
        }
        _last_stake = 0;
    }

    function slashStakePart(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        uint8 i = 0;
        if (i + 1 <= licenses.length) {        
            slashStakeSecondHelper(i, licenses, slash_type);
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);   
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);       
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);     
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        i += 1;
        if (i + 1 <= licenses.length) {
            slashStakeSecondHelper(i, licenses, slash_type);           
        }
        _last_stake -= _last_stake * slash_type / FULL_STAKE_PERCENT;
        _activeStakes[hash].stake -= slash_stake;
    }

    function slashStake(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        if (slash_type == FULL_STAKE_SLASH) {
            slashStakeFull(seqNoStart, licenses);
        }
        else {
            slashStakePart(seqNoStart, slash_type, slash_stake, licenses);
        }  
    } 

    function withdrawToken(uint256 license_number, address to, varuint32 value) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        require(value + _licenses[license_number].lockStake + _licenses[license_number].lockContinue + _licenses[license_number].lockCooler <= _licenses[license_number].balance, ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        _licenses[license_number].balance -= uint128(value);
        _licenses[license_number].isPriority = false;
        _balance -= uint128(value);
        to.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
    }

    function withdrawWalletTokenHelper(optional(uint256, LicenseData) data, uint128 sumBalance) private view returns(optional(uint256, LicenseData), uint128){
        (uint256 num, LicenseData value) = data.get();
        if (value.isLockToStake == false) {
            sumBalance += value.balance;
        }
        data = _licenses.next(num);
        return (data, sumBalance);
    }

    function withdrawWalletToken(address to, varuint32 value) public view onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        uint128 sumBalance = 0;
        optional(uint256, LicenseData) data = _licenses.min();
        uint8 i = 0; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        i += 1; 
        if (data.hasValue()) {
            (data, sumBalance) = withdrawWalletTokenHelper(data, sumBalance);
        }
        require(value + sumBalance <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data_cur, flag: 1});
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root,
        uint256 balance,
        mapping (uint256 => Stake) activeStakes,
        uint8 stakesCnt,
        mapping(uint256 => LicenseData) licenses
    ) {
        return  (_owner_pubkey, _root, address(this).currencies[CURRENCIES_ID], _activeStakes, _stakesCnt, _licenses);
    }

    function getEpochAddress() external view returns(optional(address) epochAddress) {
        optional(address) epochAddressOpt = null;
        for ((, Stake stake) : _activeStakes) {
            if (stake.status == 1) {
                epochAddressOpt = BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, stake.seqNoStart);
                break;
            }
        }
        return epochAddressOpt;
    }

    function getProxyListAddr() external view returns(address) {
        return BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root);
    }

    function getProxyListCodeHash() external view returns(uint256) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _root));
    }

    function getProxyListCode() external view returns(TvmCell) {
        return BlockKeeperLib.buildBlockKeeperEpochProxyListCode(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _root);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockKeeperNodeWallet");
    }
}
