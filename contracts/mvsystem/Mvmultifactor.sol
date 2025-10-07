// SPDX-License-Identifier: GPL-3.0-or-later

pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./MobileVerifiersContractRoot.sol";
import "./Mirror.sol";

contract Multifactor is Modifiers {

    struct Transaction {
        // Transaction Id.
        uint64 id;
        // Ephemeral Public key of custodian queued transaction.
        uint256 creator;
        // Destination address of gram transfer.
        address dest;
        // Amount of nanograms to transfer.
        uint128 value;
        // Amount of ECC token to transfer.
        mapping(uint32 => varuint32) cc;
        // Flags for sending internal message (see SENDRAWMSG in TVM spec).
        uint16 sendFlags;
        // Payload used as body of outbound internal message.
        TvmCell payload;
        // Bounce flag for header of outbound internal message.
        bool bounce;
    }

    struct JWKData {
        bytes modulus;
        uint64 modulus_expire_at;
    }

    /*
     *  Constants
     */
    string constant version = "1.0.1";

    /*
     *  Send flags
     */
    // Forward fees for message will be paid from contract balance.
    uint8 constant FLAG_PAY_FWD_FEE_FROM_BALANCE = 1;
    // Tells node to send all remaining balance.
    uint8 constant FLAG_SEND_ALL_REMAINING = 128;

    mapping(uint256 => uint256) public _factors_ordered_by_timestamp;// key -->  64 most significant bits: expiration timestamp |  192 less significant bits of epk, value --> epk
    uint8 public _factors_len;
    
    address _root;
    string public _name;
    uint256 static _owner_pubkey; //seed phrase

    optional(uint256, uint64) _candidate_new_owner_pubkey_and_expiration;

    uint256 public _pub_recovery_key;
    uint256 public _jwk_update_key;

    mapping(uint256 => bytes) public _root_provider_certificates; //cert serial number -> cert bytes, related to provider (Google etc), so it may be updated after _zkid was updated, 

    mapping(uint256 => JWKData) public _jwk_modulus_data;
    uint8 public _jwk_modulus_data_len;
    optional(uint256, JWKData) _start_point_jwk;

    string public _zkid;
    uint8 public _index_mod_4; // is constant for the same provider and _zkid
    string public _iss_base_64; // is constant for the same provider and _zkid
    bytes public _lv_provider_bytes;

    bool public _use_security_card = false;
    mapping(uint256 => bool) public _m_security_cards; // security_card pub_key -> 
    uint8 public _m_security_cards_len = 0;
    
    // Dictionary of queued transactions waiting confirmations.
    mapping(uint64 => Transaction) public _m_transactions;
    uint8 public _m_transactions_len = 0;

    uint128 public _min_value = 0;
    uint256 public _max_cleanup_txns = 10;

    bool public _force_remove_oldest;
    mapping(address => bool) _whiteListOfAddress;
    uint32 public _verification_key_index = 1;

    constructor (
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
        mapping(uint256 => bytes) root_provider_certificates,
        uint128 index
    ) {
        require(index >= 0, ERR_WRONG_DATA);
        require(index < MAX_MIRROR_INDEX, ERR_WRONG_DATA);
        require(_owner_pubkey != 0 && pub_recovery_key != 0 && epk != 0 && jwk_update_key != 0, ERR_ZERO_PUBKEY); 
        require(pub_recovery_key != _owner_pubkey && pub_recovery_key != epk && pub_recovery_key != jwk_update_key && _owner_pubkey != epk && _owner_pubkey != jwk_update_key && epk != jwk_update_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(pub_recovery_key, TvmSlice(pub_recovery_key_sig), pub_recovery_key), ERR_INVALID_SIGNATURE);
        require(tvm.checkSign(epk, TvmSlice(epk_sig), epk), ERR_INVALID_SIGNATURE);
        require(tvm.checkSign(jwk_update_key, TvmSlice(jwk_update_key_sig), jwk_update_key), ERR_INVALID_SIGNATURE);
        require(uint64(block.timestamp + MIN_EPK_LIFE_TIME) < epk_expire_at, ERR_FACTOR_EXPIRED); 
        require(uint64(block.timestamp + MIN_JWK_LIFE_TIME) < jwk_modulus_expire_at, ERR_JWK_EXPIRED);
        require(epk_expire_at < uint64(block.timestamp + MAX_EPK_LIFE_TIME), ERR_FACTOR_TIMESTAMP_TOO_BIG);
        require(jwk_modulus_expire_at < uint64(block.timestamp + MAX_JWK_LIFE_TIME), ERR_JWK_TIMESTAMP_TOO_BIG);
        //TODO: should we control validate TLS data for jwk_modulus (and jwk_modulus_expire_at) to fully check wallet setup or this is too cumbersome?
        bytes ph = gosh.poseidon(index_mod_4, epk_expire_at, epk, jwk_modulus, iss_base_64, header_base_64, zkid);
        require(gosh.vergrth16(proof, ph), ERR_INVALID_PROOF);
        require(provider.byteLength() < MAX_LEN, ERR_BAD_LEN);
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (address root) = abi.decode(data, (address));
        _root = root;
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        if (msg.sender != _root) {
            require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        }
        tvm.accept();
        _zkid = zkid;
        _index_mod_4 = index_mod_4;
        _iss_base_64 = iss_base_64;
        _pub_recovery_key = pub_recovery_key;
        _factors_ordered_by_timestamp[generateIdBasedOnTimestampAndUintData(epk_expire_at, epk)] = epk;
        _factors_len = 1;
        _jwk_modulus_data[tvm.hash(kid)] = JWKData(jwk_modulus, jwk_modulus_expire_at);
        _jwk_modulus_data_len = 1;
        _root_provider_certificates = root_provider_certificates;
        _force_remove_oldest = false;
        _name = name;
        _whiteListOfAddress[_root] = true;
        _jwk_update_key = jwk_update_key;
        _lv_provider_bytes = bytes(bytes1(uint8(provider.byteLength())));
        bytes tail = provider;
        _lv_provider_bytes.append(tail);
        _start_point_jwk = _jwk_modulus_data.min();
        Mirror(msg.sender).deployPopitGame{value: 0.1 vmshell}(_owner_pubkey);
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function cleanWhiteList(uint32 epk_expire_at) public {
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        delete _whiteListOfAddress;
        _whiteListOfAddress[_root] = true;
    }

    function updateWhiteList(uint32 epk_expire_at, uint8 index, string name, uint128 indexMirror) public view {
        uint256 addrValue = BASE_PART * SHIFT + indexMirror + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        Mirror(expectedAddress).updateWhiteList{value: 0.1 vmshell, flag: 1}(_owner_pubkey, index, name);
    }

    function setWhiteList(address new_addr, uint128 index) public accept {
        require(index >= 0, ERR_WRONG_DATA);
        require(index < MAX_MIRROR_INDEX, ERR_WRONG_DATA);
        if (_whiteListOfAddress.values().length >= 20) { return; }
        ensureBalance();
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        _whiteListOfAddress[new_addr] = true;
    }

    function generateIdBasedOnTimestampAndUintData(uint64 expire_at, uint256 data_) inline private pure returns (uint256) {
        uint256 suffix = (data_ & 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF); //first  192 less significant bits of data_
        return (uint256(expire_at) << 192) | suffix;
    }

    /** Functions to add check and delete JWK keys  */
    bytes public _wasm_hash = hex"b8891b913656ae35d9ffff371f0f03e4f1f869d0e17556a8c273750313884b0a";
    function addJwkModulus(uint256 root_cert_sn, bytes lv_kid, bytes tls_data) public onlyOwnerPubkey(_jwk_update_key) returns (bool success) {
        require(_root_provider_certificates.exists(root_cert_sn), ERR_CERT_NOT_FOUND);
        ensureBalance();
        bytes stamp = bytes(bytes4(block.timestamp & 0xFFFFFFFF));
        bytes stamp_lv_provider_bytes = stamp;
        stamp_lv_provider_bytes.append(_lv_provider_bytes);
        TvmCell wasm_result_cell = gosh.runwasmconcatmultiarg(abi.encode(_wasm_hash), 
        abi.encode(tls_data), 
        abi.encode(_root_provider_certificates[root_cert_sn]), 
        abi.encode(lv_kid), 
        abi.encode(stamp_lv_provider_bytes), 
        abi.encode(WASM_FUNCTION), abi.encode(WASM_MODULE), abi.encode(WASM_BINARY));
        bytes wasm_result = abi.decode(wasm_result_cell, bytes);
        if (wasm_result.length == 3 && wasm_result[0] == 0x00){
            uint16 wasm_err_code = 0;
            wasm_err_code |= uint16(uint8(wasm_result[2])) << 0; 
            wasm_err_code |= uint16(uint8(wasm_result[1])) << 8;
            require(false, wasm_err_code);
        }
        require(wasm_result.length > 9 && wasm_result[0] == 0x01, ERR_TLS_DATA);
        uint64 jwk_modulus_expire_at_new = 0;
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[8])) << 0;  
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[7])) << 8;  
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[6])) << 16; 
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[5])) << 24; 
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[4])) << 32; 
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[3])) << 40; 
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[2])) << 48; 
        jwk_modulus_expire_at_new |= uint64(uint8(wasm_result[1])) << 56; 
        bytes kid = lv_kid[1:];
        uint jwk_hash = tvm.hash(kid);
        bool permitted = (jwk_modulus_expire_at_new > uint64(block.timestamp + MIN_JWK_LIFE_TIME )) && ((!_jwk_modulus_data.exists(jwk_hash)) || (_jwk_modulus_data[jwk_hash].modulus_expire_at < uint64(block.timestamp + MIN_JWK_LIFE_TIME ))); 
        require(permitted, ERR_INVALID_JWK); 
        tvm.accept(); 
        uint8 num_iter = MAX_NUM_OF_JWK; 
        if (_jwk_modulus_data_len < MAX_NUM_OF_JWK) {
            num_iter = _jwk_modulus_data_len;
        }
        cleanExpiredJwks(num_iter);
        if (_jwk_modulus_data_len == MAX_NUM_OF_JWK) {
            return false;
        }
        bytes jwk_modulus = wasm_result[9:];
        if (!_jwk_modulus_data.exists(jwk_hash)) {
            _jwk_modulus_data_len = _jwk_modulus_data_len + 1;
        }
        _jwk_modulus_data[jwk_hash] = JWKData(jwk_modulus, jwk_modulus_expire_at_new);
		if (_start_point_jwk.hasValue()) {
            (uint256 hash_, ) = _start_point_jwk.get();
            if (hash_ == jwk_hash) {
                _start_point_jwk = _jwk_modulus_data.min();
            }
        }
        return true;
    }

    function deleteJwkModulusByUpdateJwkKey(bytes kid) public onlyOwnerPubkey(_jwk_update_key) {
        ensureBalance();
        uint256 jwk_hash = tvm.hash(kid);
        require(_jwk_modulus_data.exists(jwk_hash), ERR_JWK_NOT_FOUND);
        tvm.accept();
        delete _jwk_modulus_data[jwk_hash];
        _jwk_modulus_data_len = _jwk_modulus_data_len - 1;
        _start_point_jwk = _jwk_modulus_data.min();
    }

    function cleanExpiredJwks(uint8 num_iter) inline private {
        if (!_start_point_jwk.hasValue()) {
            _start_point_jwk = _jwk_modulus_data.min();
        }
        optional(uint256, JWKData) pair = _start_point_jwk;
        uint8 iter = 0;
        while(pair.hasValue() && iter < num_iter) {
            (uint256 hash_, JWKData data_) = pair.get();
            if ((block.timestamp > data_.modulus_expire_at) && _jwk_modulus_data.exists(hash_)) {
                delete _jwk_modulus_data[hash_];
                _jwk_modulus_data_len = _jwk_modulus_data_len - 1;
            }
            pair = _jwk_modulus_data.next(hash_);
            if (!pair.hasValue()) {
                pair = _jwk_modulus_data.min();
            }
            iter++;
        }
        _start_point_jwk = pair;
    }

    function cleanAllExpiredJwks(uint64 epk_expire_at) public {
        ensureBalance();
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        uint8 num_iter = MAX_NUM_OF_JWK;
        if (_jwk_modulus_data_len < MAX_NUM_OF_JWK) {
            num_iter = _jwk_modulus_data_len;
        }
        cleanExpiredJwks(num_iter);
    }

    function deleteJwkModulusByFactor(uint64 epk_expire_at, bytes kid) public {
        ensureBalance();
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        uint256 hash_ = tvm.hash(kid);
        require(_jwk_modulus_data.exists(hash_), ERR_JWK_NOT_FOUND);
        tvm.accept();
        delete _jwk_modulus_data[hash_];
        _jwk_modulus_data_len = _jwk_modulus_data_len - 1;
        _start_point_jwk = _jwk_modulus_data.min();
    }


    /** Functions to add and clear zkp factors  */

    function addZKPfactor(
        bytes proof,
        uint256 epk,
        bytes kid,
        string header_base_64,
        uint64 epk_expire_at
    ) public returns (bool success)
    {
        require(epk != 0, ERR_ZERO_PUBKEY);
        require(msg.pubkey() == epk, ERR_INVALID_SIGNATURE);
        require(_pub_recovery_key != epk && _owner_pubkey != epk && _jwk_update_key != epk, ERR_REPEATING_KEY);
        require(uint64(block.timestamp + MIN_EPK_LIFE_TIME) < epk_expire_at, ERR_FACTOR_EXPIRED);
        ensureBalance();
        require(epk_expire_at < uint64(block.timestamp + MAX_EPK_LIFE_TIME), ERR_FACTOR_TIMESTAMP_TOO_BIG);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, epk);
        require(!_factors_ordered_by_timestamp.exists(key), ERR_REPEATING_KEY);
        uint256 jwk_hash = tvm.hash(kid);
        require(_jwk_modulus_data.exists(jwk_hash), ERR_JWK_NOT_FOUND);
        require(uint64(block.timestamp + MIN_JWK_LIFE_TIME) < _jwk_modulus_data[jwk_hash].modulus_expire_at, ERR_JWK_EXPIRED);
        bytes ph = gosh.poseidon(_index_mod_4, epk_expire_at, epk, _jwk_modulus_data[jwk_hash].modulus, _iss_base_64, header_base_64, _zkid);
        require(gosh.vergrth16(proof, ph), ERR_INVALID_PROOF);
        tvm.accept();
        uint8 num_iter = NUMBER_OF_FACTORS_TO_CLEAR;
        if (_factors_len < NUMBER_OF_FACTORS_TO_CLEAR) {
            num_iter = _factors_len;
        }
        cleanExpiredZKPFactors(num_iter);
        if (_factors_len == MAX_NUM_OF_FACTORS && _force_remove_oldest) {
            cleanOldestZKPFactor();
        }
        if (_factors_len == MAX_NUM_OF_FACTORS) {
            return false; 
        }
        _factors_ordered_by_timestamp[key] = epk;
        _factors_len = _factors_len + 1;
        return true; 
    }

    function deleteZKPfactorByItself(uint64 epk_expire_at) public {
        ensureBalance();
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        delete _factors_ordered_by_timestamp[key];
        _factors_len = _factors_len - 1;
    }

    function cleanExpiredZKPFactors(uint8 num_iter) inline private {
        optional(uint256, uint256) pair = _factors_ordered_by_timestamp.min();
        uint8 iter = 0;
        while(pair.hasValue() && iter < num_iter) {
            (uint256 key, ) = pair.get();
            uint64 epk_expire_at = uint64(key >> 192);
            if (block.timestamp >= epk_expire_at) {
                delete _factors_ordered_by_timestamp[key];
                _factors_len = _factors_len - 1;
                pair = _factors_ordered_by_timestamp.next(key);
                iter++;
            }
            else {
                break;
            }
        }
    }

    function cleanOldestZKPFactor() inline private {
        optional(uint256, uint256) pair = _factors_ordered_by_timestamp.min();
        if (pair.hasValue()) {
            (uint256 key, ) = pair.get();
            delete _factors_ordered_by_timestamp[key];
            _factors_len = _factors_len - 1;
        }
    }

    function cleanAllExpiredZKPFactors(uint64 epk_expire_at) public {
        ensureBalance();
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        cleanExpiredZKPFactors(MAX_NUM_OF_FACTORS);
    }


    /** Functions to change/delete keys, jwks and zkp factors via master owner pubkey */

    function setWasmHash(bytes wasm_hash) public onlyOwnerPubkey(_owner_pubkey)  {
        tvm.accept();
        ensureBalance();
        _wasm_hash = wasm_hash;
    }

	function setForceRemoveOldest(bool flag) public onlyOwnerPubkey(_owner_pubkey) {
        tvm.accept();
        ensureBalance();
        _force_remove_oldest = flag;
    }

    function addRootProviderCertificate(uint256 sn, bytes root_provider_certificate) public onlyOwnerPubkey(_owner_pubkey) {
        require(!_root_provider_certificates.exists(sn), ERR_REPEATING_CERT);
        tvm.accept();
        ensureBalance();
        _root_provider_certificates[sn] = root_provider_certificate;
    }

    function deleteRootProviderCertificate(uint256 sn) public onlyOwnerPubkey(_owner_pubkey) {
        require(_root_provider_certificates.exists(sn), ERR_CERT_NOT_FOUND);
        tvm.accept();
        ensureBalance();
        delete _root_provider_certificates[sn];
    }

    function cleanRootProviderCertificates() public onlyOwnerPubkey(_owner_pubkey) {
        tvm.accept();
        ensureBalance();
        delete _root_provider_certificates;
    }
    
    function cleanAllJwks() public onlyOwnerPubkey(_owner_pubkey) {
        ensureBalance();
        tvm.accept();
        delete _jwk_modulus_data;
        _jwk_modulus_data_len = 0;
        _start_point_jwk = null;
    }

    function cleanAllZKPFactors() public onlyOwnerPubkey(_owner_pubkey) {
        ensureBalance();
        tvm.accept();
        delete _factors_ordered_by_timestamp;
        _factors_len = 0;
    }

    function updateRecoveryPhrase(uint256 new_pub_recovery_key, bytes new_pub_recovery_key_sig) public onlyOwnerPubkey(_owner_pubkey) {
        ensureBalance();
        require(new_pub_recovery_key != 0, ERR_ZERO_PUBKEY);
        require(_pub_recovery_key != new_pub_recovery_key && new_pub_recovery_key != _owner_pubkey && new_pub_recovery_key != _jwk_update_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(new_pub_recovery_key, TvmSlice(new_pub_recovery_key_sig),new_pub_recovery_key), ERR_INVALID_SIGNATURE);
        tvm.accept();
        _pub_recovery_key = new_pub_recovery_key;
    }

    function updateJwkUpdateKey(uint256 new_jwk_update_key, bytes new_jwk_update_key_sig) public onlyOwnerPubkey(_owner_pubkey) {
        ensureBalance();
        require(new_jwk_update_key != 0, ERR_ZERO_PUBKEY);
        require(new_jwk_update_key != _jwk_update_key && new_jwk_update_key != _owner_pubkey && new_jwk_update_key != _pub_recovery_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(new_jwk_update_key, TvmSlice(new_jwk_update_key_sig), new_jwk_update_key), ERR_INVALID_SIGNATURE);
        tvm.accept();
        _jwk_update_key = new_jwk_update_key;
    }

    function updateZkid(
        string zkid, 
        bytes proof,
        uint256 epk, 
        bytes epk_sig,
        bytes kid,
        bytes jwk_modulus,
        uint64 jwk_modulus_expire_at,
        uint8 index_mod_4, 
        string iss_base_64, 
        string header_base_64,
        uint64 epk_expire_at,
        uint256 jwk_update_key,
        bytes jwk_update_key_sig,
        string provider,
        mapping(uint256 => bytes) root_provider_certificates
    ) public onlyOwnerPubkey(_owner_pubkey) {
        require(epk != 0 && jwk_update_key != 0, ERR_ZERO_PUBKEY);
        require(_pub_recovery_key != epk && _owner_pubkey != epk && _pub_recovery_key != jwk_update_key && _owner_pubkey != jwk_update_key &&  epk != jwk_update_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(epk, TvmSlice(epk_sig), epk), ERR_INVALID_SIGNATURE);
        require(tvm.checkSign(jwk_update_key, TvmSlice(jwk_update_key_sig), jwk_update_key), ERR_INVALID_SIGNATURE);
        require(uint64(block.timestamp + MIN_EPK_LIFE_TIME) < epk_expire_at, ERR_FACTOR_EXPIRED); 
        require(uint64(block.timestamp + MIN_JWK_LIFE_TIME) < jwk_modulus_expire_at, ERR_JWK_EXPIRED);
        require(epk_expire_at < uint64(block.timestamp + MAX_EPK_LIFE_TIME), ERR_FACTOR_TIMESTAMP_TOO_BIG);
        require(jwk_modulus_expire_at < uint64(block.timestamp + MAX_JWK_LIFE_TIME), ERR_JWK_TIMESTAMP_TOO_BIG);
        //TODO: should we control validate TLS data for jwk_modulus (and jwk_modulus_expire_at) to fully check wallet setup or this is too cumbersome?
        ensureBalance();
        bytes ph = gosh.poseidon(index_mod_4, epk_expire_at, epk, jwk_modulus, iss_base_64, header_base_64, zkid);
        require(gosh.vergrth16(proof, ph), ERR_INVALID_PROOF);
        require(provider.byteLength() < MAX_LEN, ERR_BAD_LEN);
        tvm.accept();
        delete _root_provider_certificates;
        _root_provider_certificates = root_provider_certificates;
        _zkid = zkid;
        _index_mod_4 = index_mod_4;
        _iss_base_64 = iss_base_64;
        delete _factors_ordered_by_timestamp;
        _factors_ordered_by_timestamp[generateIdBasedOnTimestampAndUintData(epk_expire_at, epk)] = epk;
        _factors_len = 1;
        delete _jwk_modulus_data;
        _jwk_modulus_data[tvm.hash(kid)] = JWKData(jwk_modulus, jwk_modulus_expire_at);
        _jwk_modulus_data_len = 1;
        _lv_provider_bytes = bytes(bytes1(uint8(provider.byteLength())));
        bytes tail = provider;
        _lv_provider_bytes.append(tail);
        _jwk_update_key = jwk_update_key;
        _start_point_jwk = _jwk_modulus_data.min();
    }

    function updateSeedPhrase(uint256 new_owner_pubkey, bytes new_owner_pubkey_sig) public onlyOwnerPubkey(_owner_pubkey) {
        require(new_owner_pubkey != 0, ERR_ZERO_PUBKEY);
        require(new_owner_pubkey != _jwk_update_key && new_owner_pubkey != _owner_pubkey && new_owner_pubkey != _pub_recovery_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(new_owner_pubkey, TvmSlice(new_owner_pubkey_sig), new_owner_pubkey), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        _owner_pubkey = new_owner_pubkey;
    }

    function deleteJwkModulus(bytes kid) public onlyOwnerPubkey(_owner_pubkey) {
        uint256 jwk_hash = tvm.hash(kid);
        require(_jwk_modulus_data.exists(jwk_hash), ERR_JWK_NOT_FOUND);
        tvm.accept();
        ensureBalance();
        delete _jwk_modulus_data[jwk_hash];
        _jwk_modulus_data_len = _jwk_modulus_data_len - 1;
        _start_point_jwk = _jwk_modulus_data.min();
    }

   
    function deleteZKPfactor(uint64 epk_expire_at, uint256 epk) public onlyOwnerPubkey(_owner_pubkey) {
        uint256 key  = generateIdBasedOnTimestampAndUintData(epk_expire_at, epk);
        require(_factors_ordered_by_timestamp.exists(key), ERR_FACTOR_NOT_FOUND);
        tvm.accept();
        ensureBalance();
        delete _factors_ordered_by_timestamp[key];
        _factors_len = _factors_len - 1;
    }

    function deleteZKPfactor_(uint256 epk) public onlyOwnerPubkey(_owner_pubkey) {
        tvm.accept();
        ensureBalance();
        optional(uint256, uint256) pair = _factors_ordered_by_timestamp.min();
        while(pair.hasValue()) {
            (uint256 key, uint256 _epk) = pair.get();
            if (epk == _epk) {
                delete _factors_ordered_by_timestamp[key];
                _factors_len = _factors_len - 1;
                break;
            }
            else {
                pair = _factors_ordered_by_timestamp.next(key);
            }
        }
    }

    /** Function to maintain master owner pubkey based on zkp factors and recovery key*/

    function changeSeedPhrase (
       uint64 epk_expire_at, uint256 new_owner_pubkey, bytes new_owner_pubkey_sig) public {
        require(new_owner_pubkey != 0, ERR_ZERO_PUBKEY);
        require(new_owner_pubkey != _jwk_update_key && new_owner_pubkey != _owner_pubkey && new_owner_pubkey != _pub_recovery_key, ERR_REPEATING_KEY);
        require(tvm.checkSign(new_owner_pubkey, TvmSlice(new_owner_pubkey_sig), new_owner_pubkey), ERR_INVALID_SIGNATURE);   
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        require(!_candidate_new_owner_pubkey_and_expiration.hasValue(), ERR_SEED_PHRASE_NEW_CANDIDATE_EXISTS);
        tvm.accept();
        ensureBalance();
        _candidate_new_owner_pubkey_and_expiration = (new_owner_pubkey, epk_expire_at);
    }


    function acceptCandidateSeedPhrase(uint256 new_owner_pubkey) public onlyOwnerPubkey(_pub_recovery_key)  {
        require(_candidate_new_owner_pubkey_and_expiration.hasValue(), ERR_SEED_PHRASE_NEW_CANDIDATE_NOT_FOUND);
        (uint256 new_owner_pubkey_, uint64 epk_expire_at_) = _candidate_new_owner_pubkey_and_expiration.get();
        require(new_owner_pubkey_ == new_owner_pubkey, ERR_SEED_PHRASE_NEW_CANDIDATE_NOT_FOUND);
        require(block.timestamp < epk_expire_at_, ERR_FACTOR_EXPIRED);
        tvm.accept();
        ensureBalance();
        _owner_pubkey = new_owner_pubkey_;
        _candidate_new_owner_pubkey_and_expiration = null;
    }

    function deleteCandidateSeedPhrase(uint64 epk_expire_at) public {
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        _candidate_new_owner_pubkey_and_expiration = null;
    }

    /** Security Card keys maintaining functionality */

    function addSecurityCard(uint256 pubkey, bytes pubkey_sig) public onlyOwnerPubkey(_owner_pubkey) {
        require(_m_security_cards_len < MAX_CARDS, ERR_TOO_MUCH_CARDS_ADDED);
        require(!_m_security_cards.exists(pubkey), ERR_CARD_EXISTS);
        require(pubkey != 0, ERR_ZERO_PUBKEY);
        require(tvm.checkSign(pubkey, TvmSlice(pubkey_sig), pubkey), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        _m_security_cards[pubkey] = true;
        _m_security_cards_len =  _m_security_cards_len + 1;
        _use_security_card = true;    
    }

    function turnOffSecurityCards() public onlyOwnerPubkey(_owner_pubkey) {
        require(_use_security_card, ERR_CARD_IS_TURNED_OFF);
        tvm.accept();
        ensureBalance();
        _use_security_card = false;
    }

    function turnOnSecurityCards() public onlyOwnerPubkey(_owner_pubkey) {
        require(!_m_security_cards.empty(), ERR_NO_CARDS);
        require(!_use_security_card, ERR_CARD_IS_TURNED_ON);
        tvm.accept();
        ensureBalance();
        _use_security_card = true;
    }

    function deleteSecurityCard(uint256 pubkey) public onlyOwnerPubkey(_owner_pubkey) {    
        require(_m_security_cards.exists(pubkey), ERR_CARD_NOT_FOUND);
        tvm.accept();
        ensureBalance();
        delete _m_security_cards[pubkey];
        _m_security_cards_len = _m_security_cards_len - 1;
        if (_m_security_cards.empty()) {
            _use_security_card = false;
        }
    }

    function deleteAllSecurityCards() public onlyOwnerPubkey(_owner_pubkey) {
        require(!_m_security_cards.empty(), ERR_NO_CARDS);
        tvm.accept();
        ensureBalance();
        _use_security_card = false;
        delete _m_security_cards;
        _m_security_cards_len  = 0;
    }

    /** Value Transfer/Exchange functionality */

    function sendTransaction(
        uint64 epk_expire_at,
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        uint8 flags,
        TvmCell payload) public view returns(address)
    {
        require(!_use_security_card, ERR_CARD_IS_TURNED_ON);
        require(value >= _min_value, ERR_TOO_SMALL_VALUE);
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        if (payload != TvmCell()) {
            require(_whiteListOfAddress[dest] == true, ERR_WRONG_ADDRESS);
        }
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        tvm.accept();
        ensureBalance();
        dest.transfer(varuint16(value), bounce, flags, payload, cc);
        return dest;
    }

    function submitTransaction(
        uint64 epk_expire_at,
        address dest,
        uint128 value,
        mapping(uint32 => varuint32) cc,
        bool bounce,
        bool allBalance,
        TvmCell payload)
    public returns (uint64 transId)
    {
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        require(value >= _min_value, ERR_TOO_SMALL_VALUE);
        if (payload != TvmCell()) {
            require(_whiteListOfAddress[dest] == true, ERR_WRONG_ADDRESS);
        }
        removeExpiredTransactions();
        require(_m_transactions_len < MAX_QUEUED_REQUESTS, ERR_TRX_WAITLIST_OVERFLOWED);
        tvm.accept(); //TODO: check what if remove this
        ensureBalance();
        uint8 flags = getSendFlags(allBalance);        
        if (_use_security_card == false) {
            dest.transfer(varuint16(value), bounce, flags, payload, cc);
            return 0;
        } else {
            uint64 transactionId = generateTrxId();
            Transaction txn = Transaction(transactionId, msg.pubkey(), dest, value, cc, flags, payload, bounce);
            _m_transactions[transactionId] = txn;
            _m_transactions_len = _m_transactions_len + 1;
            return transactionId;
        }
    }

    
    function confirmTransaction(uint64 transactionId) public {
        require(_use_security_card, ERR_CARD_IS_TURNED_OFF);     
        require(_m_security_cards.exists(msg.pubkey()), ERR_INVALID_SIGNATURE);
        removeExpiredTransactions();
        optional(Transaction) otxn = _m_transactions.fetch(transactionId);        
        require(otxn.hasValue(), ERR_TRX_NOT_FOUND);
        tvm.accept();
        ensureBalance();
        uint64 marker = getExpirationBound();
        bool needCleanup = transactionId <= marker;
        if (!needCleanup) {
            Transaction txn = otxn.get();
            txn.dest.transfer(varuint16(txn.value), txn.bounce, txn.sendFlags, txn.payload, txn.cc);   
        }
        delete _m_transactions[transactionId];
        _m_transactions_len = _m_transactions_len - 1;
    }

    /// @dev Removes expired transactions from storage.
    function removeExpiredTransactions() inline private {
        uint64 marker = getExpirationBound();
        optional(uint64, Transaction) otxn = _m_transactions.min();
        if (!otxn.hasValue()) { return; }
        (uint64 trId, Transaction txn) = otxn.get();
        bool needCleanup = trId <= marker;
        if (!needCleanup) { return; }

        tvm.accept();
        uint i = 0;
        while (needCleanup && i < _max_cleanup_txns) {
            // transaction is expired, remove it
            i++;
            delete _m_transactions[trId];
            _m_transactions_len = _m_transactions_len - 1;
            otxn = _m_transactions.next(trId);
            if (!otxn.hasValue()) {
                needCleanup = false;
            } else {
                (trId, txn) = otxn.get();
                needCleanup = trId <= marker;
            }
        }        
        tvm.commit();
    }

    /*** Auxiliary functions */

    function setMaxCleanupTxns(uint64 epk_expire_at, uint value) public { 
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE);
        require(value > 0 && value <= MAX_QUEUED_REQUESTS, ERR_MAX_CLEANUP_TXNS_INVALID);
        tvm.accept();
        ensureBalance();
        _max_cleanup_txns = value;
    }

    function setMinValue(uint64 epk_expire_at, uint128 value) public {
        require(block.timestamp < epk_expire_at, ERR_FACTOR_EXPIRED);
        uint256 key = generateIdBasedOnTimestampAndUintData(epk_expire_at, msg.pubkey());
        require(_factors_ordered_by_timestamp.exists(key) && _factors_ordered_by_timestamp[key] == msg.pubkey(), ERR_INVALID_SIGNATURE); 
        tvm.accept();
        ensureBalance();
        _min_value = value;
    }
    
    /// @dev Generates new id for object.
    function generateTrxId() inline private pure returns (uint64) {
        return (uint64(block.timestamp) << 32) | (tx.logicaltime & 0xFFFFFFFF);
    }

    /// @dev Returns timestamp after which transactions are treated as expired.
    function getExpirationBound() inline private pure returns (uint64) {
        return (uint64(block.timestamp) - EXPIRATION_TIME) << 32;
    }

    /// @dev Returns transfer flags according to input value and `allBalance` flag.
    function getSendFlags(bool allBalance) inline private pure returns (uint8) {        
        uint8 flags = FLAG_PAY_FWD_FEE_FROM_BALANCE;
        if (allBalance) {
            flags = FLAG_SEND_ALL_REMAINING;
        }
        return flags;
    }

    /*** Getters */

    /// @dev Get-method that returns transaction info by id.
    /// @return trans Transaction structure.
    /// Throws exception if transaction does not exist.
    function getTransaction(uint64 transactionId) public view
        returns (Transaction trans) {
        optional(Transaction) txn = _m_transactions.fetch(transactionId);
        require(txn.hasValue(), ERR_TRX_NOT_FOUND);
        trans = txn.get();
    }

    /// @dev Get-method that returns array of pending transactions.
    /// Returns not expired transactions only.
    /// @return transactions Array of queued transactions.
    function getTransactions() public view returns (Transaction[] transactions) {
        uint64 bound = getExpirationBound();
        optional(uint64, Transaction) otxn = _m_transactions.min();
        while (otxn.hasValue()) {
            // returns only not expired transactions
            (uint64 id, Transaction txn) = otxn.get();
            if (id > bound) {
                transactions.push(txn);
            }
            otxn = _m_transactions.next(id);
        }
    }

    /// @dev Get-method that returns submitted transaction ids.
    /// @return ids Array of transaction ids.
    function getTransactionIds() public view returns (uint64[] ids) {
        uint64 trId = 0;
        optional(uint64, Transaction) otxn = _m_transactions.min();
        while (otxn.hasValue()) {
            (trId, ) = otxn.get();
            ids.push(trId);
            otxn = _m_transactions.next(trId);
        }
    }

    function getZKPEphemeralPublicKeys() public view returns (uint256[]) {
        return _factors_ordered_by_timestamp.values();
    }
    
    function getTimeStamp() external pure returns (uint32) {
        return block.timestamp;
    }

    function getSecurityCardKeys() public view returns (uint256[] sc_keys) {
        return _m_security_cards.keys();
    }

    function get_epk_expire_at(uint256 epk) public view returns (uint64) {
        optional(uint256, uint256) pair = _factors_ordered_by_timestamp.min();
        while(pair.hasValue()) {
            (uint256 key, uint256 epk_) = pair.get();
            if (epk == epk_) {
                uint64 epk_expire_at = uint64(key >> 192);
                return epk_expire_at;
            }
            else {
                pair = _factors_ordered_by_timestamp.next(key);
            }
        }
        return 0;
    } 

    function getVersion() external pure returns(string, string) {
        return (version, "Multifactor");
    } 
}