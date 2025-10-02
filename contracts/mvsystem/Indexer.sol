// SPDX-License-Identifier: GPL-3.0-or-later

pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./MobileVerifiersContractRoot.sol";
import "./Mirror.sol";

contract NameIndex is Modifiers {
    string constant version = "1.0.0";

    string static _name;
    address _wallet;
    address _root;
    uint256 _rootPubkey;

    constructor (
        address wallet,
        uint256 rootPubkey,
        uint128 index,
        address root
    ) accept {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib) = abi.decode(data, (string));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        _root = root;
        _wallet = wallet;
        _rootPubkey = rootPubkey;        
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        if (msg.sender != _root) {
            require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        }
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function isOwner(
        address wallet,
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
        uint256 owner_pubkey,
        uint128 index) public view accept {
        require(index >= 0, ERR_WRONG_DATA);
        require(index < MAX_MIRROR_INDEX, ERR_WRONG_DATA);
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        ensureBalance();
        bool ready = _wallet == wallet;
        Mirror(msg.sender).isDeployMultifactor{value: 0.1 vmshell, flag: 1}(
            _name, 
            ready,
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
            owner_pubkey
        );
    }

    function isOwnerRoot(
        address wallet,
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
        uint256 owner_pubkey,
        address mirror) public onlyOwnerPubkey(_rootPubkey) accept {
        ensureBalance();
        _wallet = wallet;
        Mirror(mirror).isDeployMultifactor{value: 0.1 vmshell, flag: 1}(
            _name, 
            true,
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
            owner_pubkey
        );
    }

    function setNewWalletRoot(address wallet) public onlyOwnerPubkey(_rootPubkey) accept {
        _wallet = wallet;
    }

    function setNewWallet(address wallet) public senderIs(_wallet) accept {
        _wallet = wallet;
    }

    function destroyNode() public senderIs(address(this)) accept {
        selfdestruct(address(this));
    }

    function getDetails() external view returns(string name, address wallet) {
        return (_name, _wallet);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "Indexer");
    } 
}