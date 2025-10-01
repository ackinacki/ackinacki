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
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperCoolerContract.sol";
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
    bool _isContinue = false;

    mapping(uint256 => LicenseData) _licenses;
    uint128 _licenses_count;

    uint128 _epochDuration;

    uint32  _freeLicense;
    bool _isWaitStake = false;
    uint128 _balance = 0;
    mapping(uint256=>bool) _whiteListLicense;
    address _licenseRoot;
    bool _isSlashing = false;
    uint64 _walletLastTouch;
    uint256 _signing_pubkey;
    uint8 _walletTouch;

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
        uint8 stakesCnt,
        uint8 walletTouch
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
        _walletTouch = walletTouch;
        _signing_pubkey = _owner_pubkey;
    }

    function setSigningPubkey(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        _signing_pubkey = pubkey;
    }

    function checkCooler() private view returns(bool result) {
        result = false;
        for (Stake value : _activeStakes.values()) {
            if (value.status == COOLER_DEPLOYED) {
                result = true;
                if (value.seqNoFinish <= block.seqno) {
                    BlockKeeperCooler(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, value.seqNoStart)).touch{value: 0.1 vmshell, flag: 1}();
                }
            }
        }
    }

    function setLockToStake(uint256 license_number, bool lock) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        _licenses[license_number].isLockToStake = lock;
    }

    function setLockToStakeByWallet(uint256 license_number, bool lock) public  onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        _licenses[license_number].isLockToStakeByWallet = lock;
    }

    function removeLicense(uint256 license_number) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE); 
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST); 
        require(_licenses[license_number].status == LICENSE_REST, ERR_LICENSE_BUSY); 
        require(_licenses[license_number].coolerCount == 0, ERR_LICENSE_BUSY); 
        require(_licenses[license_number].lockStake + _licenses[license_number].lockContinue + _licenses[license_number].lockCooler == 0, ERR_LICENSE_BUSY); 
        require(_licenses[license_number].isLockBecauseOfSlashing == false); 
        _licenses_count -= 1;
        _balance -= _licenses[license_number].balance;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = _licenses[license_number].balance;       
        LicenseContract(msg.sender).deleteWallet{value: 0.1 vmshell, currencies: data, flag: 1}(_licenses[license_number].reputationTime, _licenses[license_number].isPrivileged, _licenses[license_number].last_touch);
        delete _licenses[license_number];
    }

    function setLicenseWhiteList(mapping(uint256 => bool) whiteListLicense) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        require(whiteListLicense.keys().length <= MAX_LICENSE_NUMBER_WHITELIST_BK, ERR_TOO_MANY_LICENSES);
        _whiteListLicense = whiteListLicense;
    }

    function addLicense(uint256 license_number, uint128 reputationTime, uint64 last_touch, bool isPrivileged) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        if ((_isWaitStake == true) || (_licenses_count >= MAX_LICENSE_NUMBER) || (_whiteListLicense[license_number] != true) || (_licenses.exists(license_number))) {
            LicenseContract(msg.sender).notAcceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey);
            return;
        }

        _licenses_count += 1;
        _licenses[license_number] = LicenseData(reputationTime, LICENSE_REST, isPrivileged, null, last_touch, 0, 0, 0, 0, false, 0, false, false);
        LicenseContract(msg.sender).acceptLicense{value: 0.1 vmshell, flag: 1}(_owner_pubkey, last_touch);
    }

    function addBalance(uint256 license_number) public {
        ensureBalance();
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        require(uint128(msg.currencies[CURRENCIES_ID]) > 0, ERR_LOW_VALUE);
        require(_isWaitStake == false, ERR_WAIT_STAKE); 
        tvm.accept();

        _licenses[license_number].balance += uint128(msg.currencies[CURRENCIES_ID]);
        _balance += uint128(msg.currencies[CURRENCIES_ID]);
    }

    function deleteLicense(uint256 license_number) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE); 
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST); 
        require(_licenses[license_number].status == LICENSE_REST, ERR_LICENSE_BUSY); 
        require(_licenses[license_number].coolerCount == 0, ERR_LICENSE_BUSY); 
        require(_licenses[license_number].isLockBecauseOfSlashing == true); 
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        _licenses_count -= 1;
        _balance -= _licenses[license_number].balance;
        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = _licenses[license_number].balance;       
        LicenseContract(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)).destroyLicense{value: 0.1 vmshell, currencies: data, flag: 1}();
        delete _licenses[license_number];
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3) { return; }
        gosh.mintshellq(FEE_DEPLOY_BLOCK_KEEPER_WALLET * 3);
    }

    function setLockStakeHelper(LicenseStake value) private {
        if (_licenses.exists(value.num)){
            _licenses[value.num].status = LICENSE_PRE_EPOCH;
            _licenses[value.num].stakeController = msg.sender;
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
        for (LicenseStake value: licenses) {
            setLockStakeHelper(value);
        }
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, 0, bls_key, PRE_EPOCH_DEPLOYED, signerIndex);
        _isWaitStake = false;
    }

    function deleteLockStakeHelper(LicenseStake value) private {
        if (_licenses.exists(value.num)){
            _licenses[value.num].status = LICENSE_REST;
            _licenses[value.num].stakeController = null;
            _licenses[value.num].lockStake = 0;
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
        for (LicenseStake value: licenses) {
            deleteLockStakeHelper(value);
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[stakeHash];
        _stakesCnt -= 1;
    }

    function slash(uint8 slash_type, bytes bls_key) public senderIs(address(this)) {
        ensureBalance();
        require(slash_type < FULL_STAKE_PERCENT, ERR_WRONG_DATA);
        for (Stake value : _activeStakes.values()) {
            if (value.bls_key == bls_key) {
                if (value.status == EPOCH_DEPLOYED) {
                    BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, value.seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type);
                    _isSlashing = true;
                    return;
                } 
                if (value.status == COOLER_DEPLOYED) {
                    BlockKeeperCooler(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, value.seqNoStart)).slash{value: 0.1 vmshell, flag: 1}(slash_type, false);
                    return;
                }
            } 
        }
    }

    function updateLockStakeHelper(LicenseStake value) private {
        if (_licenses.exists(value.num)){
            _licenses[value.num].stakeController = msg.sender;
            _licenses[value.num].last_touch = block.seqno;
            if (_licenses[value.num].lockContinue != 0) {
                _licenses[value.num].lockStake = _licenses[value.num].lockContinue;
                _licenses[value.num].lockContinue = 0;
            }
            _licenses[value.num].status = LICENSE_EPOCH;
        }
    }

    function updateLockStake(uint64 seqNoStart, uint64 seqNoFinish, uint256 stake, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            updateLockStakeHelper(value);
        }
        _activeStakes[stakeHash] = Stake(stake, seqNoStart, seqNoFinish, bls_key, EPOCH_DEPLOYED, signerIndex);
    }

    function stakeNotAccepted(uint64 epochDuration) public senderIs(_root) accept {
        ensureBalance();
        _stakesCnt -= 1;
        _isWaitStake = false;
        _epochDuration = epochDuration;
    }

    function stakeNotAcceptedContinue(uint64 epochDuration) public senderIs(_root) accept {
        ensureBalance();
        _isWaitStake = false;
        _epochDuration = epochDuration;
    }

    function sendBlockKeeperRequestWithStakeHelperFirst(uint256 num, uint128 sumBalance, bool is_ok, uint128 sumreputationCoef, uint8 num_licenses) private returns(uint128, bool, uint128, uint8){
        if ((_licenses[num].isLockToStake == false) && (_licenses[num].isLockToStakeByWallet == false) && (_licenses[num].isLockBecauseOfSlashing == false)) {
            if (_licenses[num].lockCooler < _licenses[num].balance) {
                sumBalance += _licenses[num].balance - _licenses[num].lockCooler;
            }
            if (_licenses[num].last_touch + _epochDuration / EPOCH_DENOMINATOR < block.seqno) {
                _licenses[num].reputationTime = 0;
                _licenses[num].isPrivileged = false;
            }
            if (!_licenses[num].isPrivileged) {
                is_ok = false;
            }
            sumreputationCoef += gosh.calcrepcoef(_licenses[num].reputationTime);
            num_licenses += 1;
        }
        return (sumBalance, is_ok, sumreputationCoef, num_licenses);
    }

    function tryCooler() public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        bool isCooler = checkCooler();
        isCooler;
    }

    function sendBlockKeeperRequestWithStake(bytes bls_pubkey, varuint32 stake, uint16 signerIndex, mapping(uint8 => string) ProxyList, string myIp, optional(string) nodeVersion) public  onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(bls_pubkey.length == BLS_PUBKEY_LENGTH, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 0, ERR_STAKE_EXIST);
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(_licenses_count > 0, ERR_TOO_LOW_LICENSES);
        require(signerIndex <= MAX_SIGNER_INDEX, ERR_WRONG_SIGNER_INDEX);
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        bool isCooler = checkCooler();
        if (isCooler) {
            return;
        }
        if (_isSlashing) {
            return;
        }
        _stakesCnt += 1;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        uint128 sumreputationCoef;
        bool is_ok = true;
        uint128 sumBalance = 0;
        uint8 num_licenses = 0;
        for ((uint256 num,) : _licenses) {
            (sumBalance, is_ok, sumreputationCoef, num_licenses) = sendBlockKeeperRequestWithStakeHelperFirst(num, sumBalance, is_ok, sumreputationCoef, num_licenses);
        }
        require(num_licenses > 0, ERR_TOO_LOW_LICENSES);
        require(stake <= sumBalance / 2, ERR_LOW_VALUE);
        _isWaitStake = true;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWallet{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, signerIndex, sumreputationCoef, is_ok, ProxyList, myIp, nodeVersion);
    } 

    function sendBlockKeeperRequestWithCancelStakeContinue(uint64 seqNoStartOld) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        require(_isContinue == true, ERR_EPOCH_NOT_CONTINUE);
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)).cancelContinueStake{value: 0.1 vmshell, flag: 1}();    
    }

    function cancelContinueStakeHelper(LicenseStake value) private {
        if (_licenses.exists(value.num)){
            if (_licenses[value.num].status == LICENSE_CONTINUE) {
                _licenses[value.num].status = LICENSE_REST;
                _licenses[value.num].stakeController = null;
            }
            _licenses[value.num].lockContinue = 0;
        }
    }

    function cancelContinueStake(uint64 seqNoStartOld, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        ensureBalance();
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        _isContinue = false;
        for (LicenseStake value: licenses) {
            cancelContinueStakeHelper(value);
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
    }

    function sendBlockKeeperRequestWithStakeContinueFirstHelper(uint128 sumreputationCoef, uint256 num, uint128 sumBalance, bool is_ok, uint8 num_licenses) private returns(uint128, uint128, bool, uint8){
        if ((_licenses[num].isLockToStake == false) && (_licenses[num].isLockToStakeByWallet == false) && (_licenses[num].isLockBecauseOfSlashing == false)) {
            uint128 value = _licenses[num].lockStake + _licenses[num].lockCooler;
            if (value < _licenses[num].balance) {
                sumBalance += _licenses[num].balance - value;
            }
            if ((_licenses[num].last_touch + _epochDuration / EPOCH_DENOMINATOR < block.seqno) && (_licenses[num].status != LICENSE_EPOCH) && (_licenses[num].status != LICENSE_CONTINUE)) {
                _licenses[num].reputationTime = 0;
                _licenses[num].isPrivileged = false;
            }
            if (!_licenses[num].isPrivileged) {
                is_ok = false;
            }
            sumreputationCoef += gosh.calcrepcoef(_licenses[num].reputationTime);
            num_licenses += 1;
        }
        return (sumreputationCoef, sumBalance, is_ok, num_licenses);
    }

    function sendBlockKeeperRequestWithStakeContinue(bytes bls_pubkey, varuint32 stake, uint64 seqNoStartOld, uint16 signerIndex, mapping(uint8 => string) ProxyList, optional(string) nodeVersion) public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(_licenses_count > 0, ERR_TOO_LOW_LICENSES);
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        require(signerIndex <= MAX_SIGNER_INDEX, ERR_WRONG_SIGNER_INDEX);
        require(_isContinue == false, ERR_EPOCH_ALREADY_CONTINUE);
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        bool isCooler = checkCooler();
        if (isCooler) {
            return;
        }
        if (_isSlashing) {
            return;
        }
        TvmBuilder b;
        b.store(seqNoStartOld);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        require(_activeStakes.exists(stakeHash) == true, ERR_STAKE_DIDNT_EXIST);
        require(_activeStakes[stakeHash].status == EPOCH_DEPLOYED, ERR_STAKE_STATUS_NOT_EPOCH);
        require(_activeStakes[stakeHash].seqNoFinish > block.seqno + _epochDuration / EPOCH_DENOMINATOR_CONTINUE, ERR_TOO_LATE);
        require(bls_pubkey.length == BLS_PUBKEY_LENGTH, ERR_WRONG_BLS_PUBKEY);
        require(stake <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_stakesCnt == 1, ERR_STAKE_EXIST);

        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = stake;
        bool is_ok = true;
        uint128 sumBalance = 0;
        uint128 sumreputationCoef = 0;
        uint8 num_licenses = 0;
        for ((uint256 num,) : _licenses) {
            (sumreputationCoef, sumBalance, is_ok, num_licenses) = sendBlockKeeperRequestWithStakeContinueFirstHelper(sumreputationCoef, num, sumBalance, is_ok, num_licenses);
        }
        if ((stake < sumBalance) || (_freeLicense < block.timestamp)) {
            is_ok = false;
        }
        require(num_licenses > 0, ERR_TOO_LOW_LICENSES);
        require(stake <= sumBalance, ERR_LOW_VALUE);
        _isWaitStake = true;
        BlockKeeperContractRoot(_root).receiveBlockKeeperRequestWithStakeFromWalletContinue{value: 0.1 vmshell, currencies: data_cur, flag: 1} (_owner_pubkey, bls_pubkey, seqNoStartOld, signerIndex, is_ok, ProxyList, _activeStakes[stakeHash].seqNoFinish, sumreputationCoef, nodeVersion);
    } 

    function calcSumBalanceHelper(uint256 num, uint128 sumBalance) private view returns(uint128){
        if ((_licenses[num].isLockToStake == false) && (_licenses[num].isLockToStakeByWallet == false) && (_licenses[num].isLockBecauseOfSlashing == false)) {
            if (_licenses[num].lockCooler + _licenses[num].lockContinue + _licenses[num].lockStake < _licenses[num].balance) {
                sumBalance += _licenses[num].balance - _licenses[num].lockCooler - _licenses[num].lockContinue - _licenses[num].lockStake;
            }
        }
        return sumBalance;
    }

    function deployPreEpochHelper(uint256 num, uint128 sumBalance, LicenseStake[] licenses, varuint32 stake) private returns(LicenseStake[]){
        if ((_licenses[num].isLockToStake == false) && (_licenses[num].isLockToStakeByWallet == false) && (_licenses[num].isLockBecauseOfSlashing == false)) {
            uint128 lock = 0;
            _licenses[num].lockStake = 0;
            if (sumBalance != 0) {
                uint128 value = 0;
                if (_licenses[num].lockCooler < _licenses[num].balance) {
                    value = _licenses[num].balance - _licenses[num].lockCooler;
                    lock = math.divc(uint128(value * stake), sumBalance);
                }
                _licenses[num].lockStake = math.min(lock, value);
            }
            licenses.push(LicenseStake(num, _licenses[num].lockStake));
        }
        return licenses;
    }

    function deployPreEpochContract(uint64 epochDuration, uint8 walletTouch, uint64 epochCliff, uint64 waitStep, bytes bls_pubkey, uint16 signerIndex, uint128 rep_coef, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 reward_sum, string myIp, optional(string) nodeVersion) public senderIs(_root) accept  {
        ensureBalance();       
        if (_isSlashing == true) {
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            _stakesCnt -= 1;
            _isWaitStake = false;
            return;
        }
        uint128 stake = uint128(msg.currencies[CURRENCIES_ID]);
        LicenseStake[] licenses;
        uint128 licenseSumBalance = 0;
        for ((uint256 num,) : _licenses) {
            licenseSumBalance = calcSumBalanceHelper(num, licenseSumBalance);        
        }
        for ((uint256 num,) : _licenses) {
            licenses = deployPreEpochHelper(num, licenseSumBalance, licenses, stake);        
        }
        _epochDuration = epochDuration;
        _walletTouch = walletTouch;
        uint64 seqNoStart = block.seqno + epochCliff;
        TvmCell data = BlockKeeperLib.composeBlockKeeperPreEpochStateInit(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, seqNoStart);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        new BlockKeeperPreEpoch {
            stateInit: data,
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST + 1 vmshell),
            currencies: data_cur,
            wid: 0, 
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, signerIndex, rep_coef, licenses, virtualStake, ProxyList, reward_sum, myIp, epochCliff, nodeVersion);
    }

    function deployBlockKeeperContractContinueHelper(uint256 num, LicenseStake[] licenses, uint128 sumBalance, varuint32 stake, address epoch_addr) private returns(LicenseStake[]){
        if ((_licenses[num].isLockToStake == false) && (_licenses[num].isLockToStakeByWallet == false) && (_licenses[num].isLockBecauseOfSlashing == false)) {
            uint128 lock = 0;
            _licenses[num].lockContinue = 0;
            if (sumBalance != 0) {
                uint128 value = _licenses[num].lockStake + _licenses[num].lockCooler;
                if (value < _licenses[num].balance) {
                    value = _licenses[num].balance - value;
                    lock = math.divc(uint128(value * stake), sumBalance);
                } else {
                    value = 0;
                }
                _licenses[num].lockContinue = math.min(lock, value);
            }
            licenses.push(LicenseStake(num, _licenses[num].lockContinue));
            if (_licenses[num].status == LICENSE_REST) {
                _licenses[num].status = LICENSE_CONTINUE;
                _licenses[num].stakeController = epoch_addr;
            }
        }
        return licenses;
    }

    function deployBlockKeeperContractContinue(uint8 walletTouch, uint64 seqNoStartold, bytes bls_pubkey, uint16 signerIndex, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef, optional(string) nodeVersion) public senderIs(_root) accept  {
        ensureBalance();
        _walletTouch = walletTouch;
        uint128 stake = uint128(msg.currencies[CURRENCIES_ID]);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = msg.currencies[CURRENCIES_ID];
        TvmBuilder b;
        b.store(seqNoStartold);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        if ((_activeStakes.exists(stakeHash) != true) || (_activeStakes[stakeHash].status != EPOCH_DEPLOYED) || (_isSlashing)) {
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            return;
        } 
        LicenseStake[] licenses;
        address epoch_addr = BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartold);
        uint128 sumBalance = 0;
        for ((uint256 num,) : _licenses) {
            sumBalance = calcSumBalanceHelper(num, sumBalance);
        }
        for ((uint256 num,) : _licenses) {
            licenses = deployBlockKeeperContractContinueHelper(num, licenses, sumBalance, stake, epoch_addr);
        }
        if (licenses.length == 0) {
            BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
            return;
        }
        BlockKeeperEpoch(epoch_addr).continueStake{value: 0.1 vmshell, flag: 1, currencies: data_cur}(bls_pubkey, signerIndex, licenses, virtualStake, ProxyList, sumReputationCoef, nodeVersion);    
    }

    function continueStakeNotAccept(uint64 seqNoStartOld, bytes bls_key, uint16 signerIndex, LicenseStake[] licenses) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        ensureBalance();
        _isWaitStake = false;
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        for (LicenseStake value: licenses) {
            if (_licenses.exists(value.num)) {
                _licenses[value.num].lockContinue = 0;  
                if (_licenses[value.num].status == LICENSE_CONTINUE) {
                    _licenses[value.num].status = LICENSE_REST;
                    _licenses[value.num].stakeController = null;
                }
            }
        }
    }

    function continueStakeAccept(uint64 seqNoStartOld) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept {
        ensureBalance();
        _isWaitStake = false;
        _isContinue = true;
    }

    function deployBlockKeeperContractContinueAfterDestroyHelper(LicenseStake value, mapping(uint256 => bool) licensesMap, uint128 reputationCoef, uint128 delta_time) private view returns(uint128){
        if (_licenses.exists(value.num)) {
            if (licensesMap.exists(value.num)) {
                uint128 previous = 0;
                if (delta_time < _licenses[value.num].reputationTime) {
                    previous = _licenses[value.num].reputationTime - delta_time;
                }
                reputationCoef += gosh.calcrepcoef(_licenses[value.num].reputationTime) - gosh.calcrepcoef(previous);
            }
        }
        return reputationCoef;
    }

    function deployBlockKeeperContractContinueAfterDestroy(uint64 epochDuration, uint64 waitStep, bytes bls_pubkey, uint64 seqNoStartOld, uint16 signerIndex, LicenseStake[] licenses_continue, LicenseStake[] licenses, optional(uint128) virtualStake, uint128 reward_sum, string myIp, uint128 reputationCoef, uint128 delta_time, optional(string) nodeVersion) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStartOld)) accept  {
        ensureBalance();
        uint64 seqNoStart = block.seqno;
        mapping(uint256 => bool) licensesMap;
        _epochDuration = epochDuration;
        for (LicenseStake value: licenses_continue) {
            licensesMap[value.num] = true;
        }
        for (LicenseStake value: licenses) {
            reputationCoef = deployBlockKeeperContractContinueAfterDestroyHelper(value, licensesMap, reputationCoef, delta_time);
        }
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart);
        _isContinue = false;
        new BlockKeeperEpoch {
            stateInit: data,
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: msg.currencies,
            wid: 0,
            flag: 1
        } (waitStep, epochDuration, bls_pubkey, _code, reputationCoef, signerIndex, licenses_continue, virtualStake, reward_sum, myIp, true, null, nodeVersion);
    }

    function updateLockStakeCoolerHelper(LicenseStake value, uint32 addReputationTime) private {
        if (_licenses.exists(value.num)) {
            _licenses[value.num].reputationTime += addReputationTime;
            _licenses[value.num].status = LICENSE_REST;
            _licenses[value.num].stakeController = null;
            _licenses[value.num].last_touch = block.seqno;
            _licenses[value.num].lockCooler += _licenses[value.num].lockStake;
            _licenses[value.num].lockStake = 0;
            _licenses[value.num].coolerCount += 1;
        }
    }

    function updateLockStakeCooler(uint64 seqNoStart, uint64 seqNoFinish, LicenseStake[] licenses, bool isContinue, uint32 addReputationTime) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            updateLockStakeCoolerHelper(value, addReputationTime);
        }
        if (isContinue == false) {       
            _stakesCnt -= 1;
        }
        _activeStakes[hash].status = COOLER_DEPLOYED;
        _activeStakes[hash].seqNoFinish = seqNoFinish;
    }

    function unlockStakeCoolerHelper(LicenseStake value, uint256 length, uint128 reward) private {
        if (_licenses.exists(value.num)) {
            uint128 lic_reward = 0;
            lic_reward = reward / uint128(length);
            _licenses[value.num].lockCooler -= value.stake;
            _licenses[value.num].balance += lic_reward;
            _licenses[value.num].coolerCount -= 1;
            _balance += lic_reward;
        }   
    }

    function unlockStakeCooler(uint64 seqNoStart, LicenseStake[] licenses, uint128 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        uint128 reward = 0;
        if (uint128(msg.currencies[CURRENCIES_ID]) > stake) {
            reward = uint128(msg.currencies[CURRENCIES_ID]) - stake;
        }
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            unlockStakeCoolerHelper(value, licenses.length, reward);   
        }
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[hash];
    }

    function slashCoolerFirstHelper(LicenseStake value, bool is_preepoch_cancel, address epochcontinue) private returns(bool, address) {
        if (_licenses.exists(value.num)) {
            if (is_preepoch_cancel == false) {
                if (_licenses[value.num].status == LICENSE_PRE_EPOCH) {
                    BlockKeeperPreEpoch(_licenses[value.num].stakeController.get()).cancelPreEpoch{value: 0.1 vmshell, flag: 1}();
                    is_preepoch_cancel = true;
                } else 
                if (_licenses[value.num].status == LICENSE_CONTINUE) {
                    epochcontinue = _licenses[value.num].stakeController.get();
                }
            }
            _licenses[value.num].reputationTime = 0;
            _balance -= value.stake;
            _licenses[value.num].balance -= value.stake;
            _licenses[value.num].lockCooler -= value.stake;
            _licenses[value.num].isPrivileged = false;
            _licenses[value.num].coolerCount -= 1;
            _licenses[value.num].isLockBecauseOfSlashing = true;
        }
        return (is_preepoch_cancel, epochcontinue);
    }

    function slashCoolerSecondHelper(LicenseStake value, uint8 slash_type) private {
        if (_licenses.exists(value.num)) {
            uint128 slash_value = value.stake * slash_type / FULL_STAKE_PERCENT;
            _licenses[value.num].balance -= slash_value;
            _licenses[value.num].lockCooler -= slash_value;
            _balance -= slash_value;
        }
    }

    function slashCoolerFull(uint64 seqNoStart, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        delete b;
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[stakeHash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[stakeHash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[stakeHash];
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        bool is_preepoch_cancel;
        address epochcontinue;
        for (LicenseStake value: licenses) {
            (is_preepoch_cancel, epochcontinue) = slashCoolerFirstHelper(value, is_preepoch_cancel, epochcontinue);   
        }
        if (epochcontinue != address.makeAddrStd(0, 0)) {
            BlockKeeperEpoch(epochcontinue).cancelContinueStake{value: 0.1 vmshell, flag: 1}();
        }
    }

    function slashCoolerPart(uint64 seqNoStart, uint8 slash_type, uint128 slash_stake, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 stakeHash = tvm.hash(b.toCell());
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            slashCoolerSecondHelper(value, slash_type);   
        }
        _activeStakes[stakeHash].stake -= slash_stake; 
    }

    function slashCooler(uint64 seqNoStart, uint8 slash_type, uint128 slash_stake, LicenseStake[] licenses, bool isFromEpoch) public senderIs(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();        
        if (isFromEpoch) {
            _isSlashing = false;
        }
        if (slash_type == FULL_STAKE_SLASH) {
            slashCoolerFull(seqNoStart, licenses);
        } else {
            slashCoolerPart(seqNoStart, slash_type, slash_stake, licenses);
        }
    }

    function slashStakeFirstHelper(LicenseStake value) private {
        if (_licenses.exists(value.num)) {
            _licenses[value.num].reputationTime = 0;
            _licenses[value.num].balance -= value.stake;
            _licenses[value.num].lockStake = 0;
            _licenses[value.num].isPrivileged = false;
            _licenses[value.num].stakeController = null;
            _licenses[value.num].status = LICENSE_REST;
            _licenses[value.num].isLockBecauseOfSlashing = true;
            _balance -= value.stake;
        }          
    }

    function slashStakeSecondHelper(LicenseStake value, uint8 slash_type) private {
        if (_licenses.exists(value.num)) {
            uint128 slash_value = value.stake * slash_type / FULL_STAKE_PERCENT;
            _licenses[value.num].balance -= slash_value;
            _licenses[value.num].lockStake -= slash_value;
            _balance -= slash_value;
        }     
    }

    function slashStakeFull(uint64 seqNoStart, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        _stakesCnt -= 1;
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], _activeStakes[hash].bls_key, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], _activeStakes[hash].signerIndex, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        delete _activeStakes[hash];
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            slashStakeFirstHelper(value);   
        }
    }

    function slashStakePart(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses) private {
        TvmBuilder b;
        b.store(seqNoStart);
        uint256 hash = tvm.hash(b.toCell());
        delete b;
        require(licenses.length <= MAX_LICENSE_NUMBER, ERR_NOT_SUPPORT);
        require(licenses.length > 0, ERR_NOT_SUPPORT);
        for (LicenseStake value: licenses) {
            slashStakeSecondHelper(value, slash_type);   
        }
        _activeStakes[hash].stake -= slash_stake;
    }

    function slashStake(uint64 seqNoStart, uint8 slash_type, uint256 slash_stake, LicenseStake[] licenses, bool isContinue, bytes bls_key_continue, uint16 signerIndex_continue, LicenseStake[] licenses_continue) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, seqNoStart)) accept {
        ensureBalance();
        if (slash_type == FULL_STAKE_SLASH) {
            slashStakeFull(seqNoStart, licenses);
        }
        else {
            slashStakePart(seqNoStart, slash_type, slash_stake, licenses);
        }  
        if (isContinue == false) { return; }
        _isContinue = false;
        for (LicenseStake value: licenses_continue) {
            cancelContinueStakeHelper(value);
        }
        _isSlashing = false;
        BLSKeyIndex(BlockKeeperLib.calculateBLSKeyAddress(_code[m_BLSKeyCode], bls_key_continue, _root)).destroy{value: 0.1 vmshell, flag: 1}();
        SignerIndex(BlockKeeperLib.calculateSignerIndexAddress(_code[m_SignerIndexCode], signerIndex_continue, _root)).destroy{value: 0.1 vmshell, flag: 1}();
    } 

    function withdrawToken(uint256 license_number, varuint32 value) public senderIs(BlockKeeperLib.calculateLicenseAddress(_code[m_LicenseCode], license_number, _licenseRoot)) accept {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE); 
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);
        require(_licenses.exists(license_number), ERR_LICENSE_NOT_EXIST);
        require(value + _licenses[license_number].lockStake + _licenses[license_number].lockContinue + _licenses[license_number].lockCooler <= _licenses[license_number].balance, ERR_LOW_VALUE);

        mapping(uint32 => varuint32) data;
        data[CURRENCIES_ID] = value;
        _licenses[license_number].balance -= uint128(value);
        _licenses[license_number].isPrivileged = false;
        _balance -= uint128(value);
        msg.sender.transfer({value: 0.1 vmshell, currencies: data, flag: 1});
    }

    function withdrawWalletTokenHelper(LicenseData value, uint128 sumLock) private pure returns(uint128){
        sumLock += value.lockStake + value.lockContinue + value.lockCooler;
        return sumLock;
    }

    function withdrawWalletToken(address to, varuint32 value) public onlyOwnerPubkey(_owner_pubkey) accept  {
        ensureBalance();
        require(_isWaitStake == false, ERR_WAIT_STAKE); 
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        uint128 sumLock = 0;
        for ((, LicenseData data): _licenses) {
            sumLock = withdrawWalletTokenHelper(data, sumLock);        
        }
        require(value + _balance <= address(this).currencies[CURRENCIES_ID] + sumLock, ERR_LOW_VALUE);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = value;
        to.transfer({value: 0.1 vmshell, currencies: data_cur, flag: 1});
    }

    function resetWallet() public onlyOwnerPubkey(_owner_pubkey) accept {
        ensureBalance();
        require(block.seqno > _walletLastTouch + _walletTouch, ERR_WALLET_BUSY);
        _walletLastTouch = block.seqno;
        require(_isWaitStake == false, ERR_WAIT_STAKE);
        for ((, LicenseData value): _licenses) {
            if ((value.status == PRE_EPOCH_DEPLOYED) && (value.stakeController.hasValue())) {
                BlockKeeperPreEpoch(value.stakeController.get()).cancelPreEpoch{value: 0.1 vmshell, flag: 1}();
                return;
            }   
            if ((value.status == EPOCH_DEPLOYED) && (value.stakeController.hasValue())) {
                BlockKeeperEpoch(value.stakeController.get()).slash{value: 0.1 vmshell, flag: 1}(FULL_STAKE_SLASH);
                return;
            }      
        }
    }

    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        uint256 signerPubkey,
        address root,
        uint256 balance,
        mapping (uint256 => Stake) activeStakes,
        uint8 stakesCnt,
        mapping(uint256 => LicenseData) licenses,
        uint128 epochDuration,
        mapping(uint256=>bool) whiteListLicense
    ) {
        return  (_owner_pubkey, _signing_pubkey, _root, address(this).currencies[CURRENCIES_ID], _activeStakes, _stakesCnt, _licenses, _epochDuration, _whiteListLicense);
    }

    function getEpochAddress() external view returns(optional(address) epochAddress) {
        optional(address) epochAddressOpt = null;
        for ((, Stake stake) : _activeStakes) {
            if (stake.status == EPOCH_DEPLOYED) {
                epochAddressOpt = BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, stake.seqNoStart);
                break;
            }
        }
        return epochAddressOpt;
    }

    function getSumBalanceForStake() external view returns(uint128 sumBalance) {
        sumBalance = 0;
        for ((uint256 num,) : _licenses) {
            sumBalance = calcSumBalanceHelper(num, sumBalance);        
        }
        return sumBalance;
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
