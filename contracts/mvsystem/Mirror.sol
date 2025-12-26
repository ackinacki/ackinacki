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

import "./libraries/VerifiersLib.sol";
import "./MobileVerifiersContractRoot.sol";
import "./modifiers/modifiers.sol";
import "./Indexer.sol";
import "./Mvmultifactor.sol";
import "./PopitGame.sol";
import "./PopCoinRoot.sol";
import "./Miner.sol";

contract Mirror is Modifiers {
    string constant version = "1.0.0";

    address _root;
    address _minerRoot;
    uint128 _index;
    uint256 _rootPubkey;

    bool _isIndexUpdated = false;
    mapping(uint8 => TvmCell) _code;

    event MultifactorDeployed(uint256 pubkey, address multifactor);
    event PopitGameDeployed(address multifactor);
    event PopCoinRootDeployed(string name, address popitGameOwner);
    event MinerDeployed(address multifactor);

    constructor (
        address root,
        address minerRoot,
        uint128 index,
        uint256 rootPubkey
    ) {
        require(index >= 0, ERR_WRONG_DATA);
        require(index < MAX_MIRROR_INDEX, ERR_WRONG_DATA);
        _root = root;
        _index = index;
        _rootPubkey = rootPubkey;
        _minerRoot = minerRoot;
    }

    function setIsIndexUpdated() public senderIs(address(this)) accept {
        _isIndexUpdated = false;
    }

    function ensureBalance() private {
        if (_isIndexUpdated == false) {
            uint256 addr = address(this).value & 0xFFF;
            (, uint256 modulus) = math.divmod(addr, MAX_MIRROR_INDEX);
            if (modulus == 0) {
                modulus = MAX_MIRROR_INDEX;
            }
            _index = uint128(modulus) - 1;
            _isIndexUpdated = true;
        }
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function setNewIndex(uint128 index) public senderIs(address(this)) accept {
        ensureBalance();
        _index = index;
    }

    function setNewCode(uint8 id, TvmCell code) public senderIs(address(this)) accept {
        ensureBalance();
        _code[id] = code;
    }

    function destroyNode() public senderIs(address(this)) accept {
        selfdestruct(address(this));
    }

    function setNewPubkey(uint256 pubkey) public senderIs(address(this)) accept {
        _rootPubkey = pubkey;
    }

    function deployPopitGame(uint256 pubkey) public senderIs(VerifiersLib.calculateMultifactorAddress(_code[m_MvMultifactor], pubkey, _root)) accept {
        ensureBalance();
        TvmCell data = VerifiersLib.composePopitGameStateInit(_code[m_PopitGame], _root, msg.sender);
        mapping(uint8 => TvmCell) code;
        code[m_PopCoinRoot] = _code[m_PopCoinRoot];
        code[m_PopitGame] = _code[m_PopitGame];
        code[m_PopCoinWallet] = _code[m_PopCoinWallet];
        code[m_Boost] = _code[m_Boost];
        new PopitGame {stateInit: data, value: varuint16(FEE_DEPLOY_POPIT_GAME_WALLET), wid: 0, flag: 1}(code, _rootPubkey, _index);
        address addrExtern = address.makeAddrExtern(PopitGameDeployedEmit, bitCntAddress);
        emit PopitGameDeployed{dest: addrExtern}(msg.sender);
    }

    function deployMultifactor(
        string name,
        string zkid,
        bytes proof,
        uint256 epk,
        bytes epk_sig,
        uint64 epk_expire_at,
        bytes jwk_modulus,
        bytes kid,
        uint64 jwk_modulus_expire_at,
        uint8 index_mod_4,
        string iss_base_64,
        string provider,
        string header_base_64,
        uint256 pub_recovery_key,
        bytes pub_recovery_key_sig,
        uint256 jwk_update_key,
        bytes jwk_update_key_sig,
        mapping(uint256 => bytes) root_provider_certificates) public accept {
        ensureBalance();
        uint256 owner_pubkey = msg.pubkey();
        (, uint256 modulus) = math.divmod(owner_pubkey, MAX_MIRROR_INDEX);
        require(modulus == _index, ERR_WRONG_MIRROR_INDEX);
        require(VerifiersLib.checkName(name), ERR_WRONG_NAME);
        require(owner_pubkey != 0 && pub_recovery_key != 0 && epk != 0 && jwk_update_key != 0, ERR_ZERO_PUBKEY);
        require(pub_recovery_key != owner_pubkey && pub_recovery_key != epk && pub_recovery_key != jwk_update_key && owner_pubkey != epk && owner_pubkey != jwk_update_key && epk != jwk_update_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(pub_recovery_key, TvmSlice(pub_recovery_key_sig), pub_recovery_key), ERR_INVALID_SIGNATURE);
        require(tvm.checkSign(epk, TvmSlice(epk_sig), epk), ERR_INVALID_SIGNATURE);
        require(tvm.checkSign(jwk_update_key, TvmSlice(jwk_update_key_sig), jwk_update_key), ERR_INVALID_SIGNATURE);
        require(uint64(block.timestamp + MIN_EPK_LIFE_TIME) < epk_expire_at, ERR_FACTOR_EXPIRED);
        require(uint64(block.timestamp + MIN_JWK_LIFE_TIME) < jwk_modulus_expire_at, ERR_JWK_EXPIRED);
        require(epk_expire_at < uint64(block.timestamp + MAX_EPK_LIFE_TIME), ERR_FACTOR_TIMESTAMP_TOO_BIG);
        require(jwk_modulus_expire_at < uint64(block.timestamp + MAX_JWK_LIFE_TIME), ERR_JWK_TIMESTAMP_TOO_BIG);
        bytes ph = gosh.poseidon(index_mod_4, epk_expire_at, epk, jwk_modulus, iss_base_64, header_base_64, zkid);
        require(gosh.vergrth16(proof, ph), ERR_INVALID_PROOF);
        require(provider.byteLength() < MAX_LEN, ERR_BAD_LEN);
        TvmCell data = VerifiersLib.composeIndexerStateInit(_code[m_Indexer], name);
        address wallet = VerifiersLib.calculateMultifactorAddress(_code[m_MvMultifactor], owner_pubkey, _root);
        address indexer = new NameIndex {stateInit: data, value: varuint16(FEE_DEPLOY_INDEXER), flag: 1}(wallet, _rootPubkey, _index, _root);
        NameIndex(indexer).isOwner{value: 0.1 vmshell, flag: 1}(
            wallet,
            zkid,
            proof,
            epk,
            epk_sig,
            epk_expire_at,
            jwk_modulus,
            kid,
            jwk_modulus_expire_at,
            index_mod_4,
            iss_base_64,
            provider,
            header_base_64,
            pub_recovery_key,
            pub_recovery_key_sig,
            jwk_update_key,
            jwk_update_key_sig,
            root_provider_certificates,
            owner_pubkey,
            _index
        );
    }

    function isDeployMultifactor(
        string name,
        bool ready,
        string zkid,
        bytes proof,
        uint256 epk,
        bytes epk_sig,
        uint64 epk_expire_at,
        bytes jwk_modulus,
        bytes kid,
        uint64 jwk_modulus_expire_at,
        uint8 index_mod_4,
        string iss_base_64,
        string provider,
        string header_base_64,
        uint256 pub_recovery_key,
        bytes pub_recovery_key_sig,
        uint256 jwk_update_key,
        bytes jwk_update_key_sig,
        mapping(uint256 => bytes) root_provider_certificates,
        uint256 owner_pubkey) public senderIs(VerifiersLib.calculateIndexerAddress(_code[m_Indexer], name)) accept {
        ensureBalance();
        if (!ready) { return; }
        TvmCell data = VerifiersLib.composeMultifactorStateInit(_code[m_MvMultifactor], owner_pubkey, _root);
        address multifactor = new Multifactor {stateInit: data, value: varuint16(FEE_DEPLOY_MULTIFACTOR), wid: 0, flag: 1}(name, zkid, proof, epk, epk_sig, epk_expire_at, jwk_modulus, kid, jwk_modulus_expire_at, index_mod_4, iss_base_64, provider, header_base_64, pub_recovery_key, pub_recovery_key_sig, jwk_update_key, jwk_update_key_sig, root_provider_certificates, _index);
        address addrExtern = address.makeAddrExtern(MultifactorDeployedEmit, bitCntAddress);
        emit MultifactorDeployed{dest: addrExtern}(owner_pubkey, multifactor);
    }

    function updateWhiteList(uint256 pubkey, uint8 index, string name) public accept {
        pubkey;
        ensureBalance();
        optional(address) new_addr;
        if (index == m_Boost) {
            address popitGameAddress = VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], _root, msg.sender);
            new_addr = VerifiersLib.calculateBoostAddress(_code[m_Boost], popitGameAddress, _root);
        }
        if (index == m_PopCoinWallet) {
            new_addr = VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, msg.sender);
        }
        if (index == m_PopitGame) {
            new_addr = VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], _root, msg.sender);
        }
        if (index == m_Mirror) {
            require(msg.sender != address.makeAddrStd(0, 0), ERR_INVALID_SENDER);
            (, uint256 modulus) = math.divmod(msg.sender.value, MAX_MIRROR_INDEX);
            require(modulus == _index, ERR_WRONG_MIRROR_INDEX);
            new_addr = address(this);
        }
        if (index == m_Miner) {
            new_addr = VerifiersLib.calculateMinerGameAddress(_code[m_Miner], msg.sender);
        }
        if (new_addr.hasValue()) {
            Multifactor(msg.sender).setWhiteList{value: 0.1 vmshell, flag: 1}(new_addr.get(), _index);
        }
    }

    function deployPopCoinRoot(
        string name,
        uint16 maxPopitIndex,
        mapping(uint16 => PopitMedia) popits_media,
        string description,
        bool isPublic,
        address popitGameOwner
    ) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        TvmCell data = VerifiersLib.composePopCoinRootStateInit(_code[m_PopCoinRoot], _root, name);
        address newroot = VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], _root, name);
        (, uint256 modulus) = math.divmod(newroot.value, MAX_MIRROR_INDEX);
        require(modulus == _index, ERR_WRONG_MIRROR_INDEX);
        new PopCoinRoot {stateInit: data, value: varuint16(FEE_DEPLOY_POP_COIN_ROOT), wid: 0, flag: 1}(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), maxPopitIndex, popits_media, description, _rootPubkey, isPublic, _index, popitGameOwner);
        address addrExtern = address.makeAddrExtern(PopCoinRootDeployedEmit, bitCntAddress);
        emit PopCoinRootDeployed{dest: addrExtern}(name, popitGameOwner);
    }

    function deployMiner() public accept {
        ensureBalance();
        address multifactor = msg.sender;
        require(multifactor != address.makeAddrStd(0, 0), ERR_INVALID_SENDER);
        (, uint256 modulus) = math.divmod(multifactor.value, MAX_MIRROR_INDEX);
        require(modulus == _index, ERR_WRONG_MIRROR_INDEX);
        address popitGame = VerifiersLib.calculatePopitGameAddress(_code[m_PopitGame], _root, msg.sender);
        TvmCell data = VerifiersLib.composeMinerStateInit(_code[m_Miner], multifactor);
        address boost = VerifiersLib.calculateBoostAddress(_code[m_Boost], popitGame, _root);
        new Miner {stateInit: data, value: varuint16(FEE_DEPLOY_MINER), wid: 0, flag: 1}(_index, popitGame, _minerRoot, boost, _root);
        address addrExtern = address.makeAddrExtern(MinerDeployedEmit, bitCntAddress);
        emit MinerDeployed{dest: addrExtern}(msg.sender);
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private {
        cell;
        tvm.accept();
        tvm.resetStorage();
    }

    //Getters
    function getMinerAddress(address multifactor) external view returns(address miner) {
        return VerifiersLib.calculateMinerGameAddress(_code[m_Miner], multifactor);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "MobileVerifiersContractRootMirror");
    }
}
