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

import "./AuthServiceRoot.sol";

/// @title AuthProfile
/// @notice Personal authentication profile.
///         Address is deterministic: uniquely derived from (root, hash(description)).
///         Destroyed when receiving an internal message from the multifactor address.
contract AuthProfile {

    string constant version = "1.0.0";

    uint16 constant ERR_INVALID_SENDER = 101;

    uint constant bitCntAddress = 256;

    event ContextAdded(TvmCell context);

    /// @notice Address of the AuthServiceRoot that owns this profile.
    ///         Stored in varInit — part of address derivation.
    address static _root;

    /// @notice Hash of the profile description; stored in varInit — part of address derivation.
    uint256 static _descriptionHash;

    /// @notice Hash of the owner's public key
    uint256 _pubkeyHash;

    /// @notice Hash of the multifactor address
    uint256 _multifactorHash;

    /// @notice Human-readable description of this profile
    string _description;

    /// @notice Deploy constructor; called exclusively by AuthServiceRoot
    /// @param pubkeyHash      Hash of the owner's public key
    /// @param multifactorHash Hash of the multifactor address
    /// @param description     Profile description
    constructor(uint256 pubkeyHash, uint256 multifactorHash, string description) public {
        require(msg.sender == _root, ERR_INVALID_SENDER);
        tvm.accept();
        _pubkeyHash = pubkeyHash;
        _multifactorHash = multifactorHash;
        _description = description;

        AuthServiceRoot(_root).onProfileDeployed{value: 0.05 vmshell, flag: 1}(_descriptionHash, _multifactorHash, description);
    }

    /// @notice Destroys this profile if the sender's address hash matches _multifactorHash
    receive() external {
        TvmBuilder b;
        b.store(msg.sender);
        if (tvm.hash(b.toCell()) == _multifactorHash) {
            selfdestruct(_root);
        }
    }

    /// @notice Emits ContextAdded event to the profile's own external address; owner-only
    function addContext(TvmCell context) public {
        TvmBuilder b;
        b.store(msg.pubkey());
        require(tvm.hash(b.toCell()) == _pubkeyHash, ERR_INVALID_SENDER);
        tvm.accept();
        address addrExtern = address.makeAddrExtern(address(this).value, bitCntAddress);
        emit ContextAdded{dest: addrExtern}(context);
    }

    /// @notice Returns profile details
    function getDetails() external view returns (
        string  description,
        uint256 descriptionHash,
        uint256 pubkeyHash,
        uint256 multifactorHash,
        address root
    ) {
        description     = _description;
        descriptionHash = _descriptionHash;
        pubkeyHash      = _pubkeyHash;
        multifactorHash = _multifactorHash;
        root            = _root;
    }

    /// @notice Returns contract version
    function getVersion() external pure returns (string, string) {
        return (version, "AuthProfile");
    }
}
