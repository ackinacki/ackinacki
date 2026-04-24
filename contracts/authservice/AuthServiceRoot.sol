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

import "./AuthProfile.sol";

/// @title AuthServiceRoot
/// @notice Root contract that deploys AuthProfile contracts on request.
///         Emits AuthProfileDeployed event to an external address derived
///         from the multifactor address hash.
///         Can be deployed at any address; AuthProfile addresses are derived
///         from (root, hash(description)) so they are globally unique.
contract AuthServiceRoot {

    string constant version = "1.0.0";

    uint16 constant ERR_INVALID_SENDER = 101;

    /// @notice Minimum native balance required for contract operation
    uint64 constant MIN_BALANCE = 100 vmshell;

    /// @notice Bit count for external event address
    uint constant bitCntAddress = 256;

    /// @notice Stored code of AuthProfile contract
    TvmCell _profileCode;

    /// @notice Root owner public key
    uint256 _ownerPubkey;

    /// @notice Emitted from Root when a new AuthProfile is successfully deployed.
    ///         Fired upon receiving the onProfileDeployed callback from AuthProfile.
    ///         Event is sent to address.makeAddrExtern(multifactorHash, 256).
    /// @param profile          Address of the newly deployed AuthProfile
    /// @param multifactorHash  Hash of the multifactor address
    /// @param description      Profile description
    event AuthProfileDeployed(address profile, uint256 multifactorHash, string description);

    /// @notice Root constructor; sets owner to the deployer's pubkey.
    ///         Used for direct (non-zerostate) deployments only.
    constructor() public {
        tvm.accept();
        _ownerPubkey = msg.pubkey();
    }

    /// @notice Ensures minimal native balance for Root operations
    function ensureBalance() private {
        if (address(this).balance > MIN_BALANCE) return;
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Sets (or updates) the AuthProfile contract code; owner-only
    /// @param code  New AuthProfile TVC code cell
    function setProfileCode(TvmCell code) public {
        require(msg.pubkey() == _ownerPubkey, ERR_INVALID_SENDER);
        tvm.accept();
        _profileCode = code;
    }

    /// @notice Deploys a new AuthProfile for the given pubkey hash and multifactor hash.
    ///         Address is deterministic: unique per hash(description).
    /// @param pubkeyHash      Hash of the owner's public key
    /// @param multifactorHash Hash of the multifactor address
    /// @param description     Human-readable profile description (determines address)
    function deployProfile(uint256 pubkeyHash, uint256 multifactorHash, string description) public {
        tvm.accept();
        ensureBalance();

        uint256 descriptionHash = _hashDescription(description);
        TvmCell stateInit = _buildProfileStateInit(descriptionHash);

        new AuthProfile{
            stateInit: stateInit,
            value: 5 vmshell,
            flag: 1
        }(pubkeyHash, multifactorHash, description);
    }

    /// @notice Callback invoked by the newly deployed AuthProfile.
    ///         Verifies the sender is the expected profile address,
    ///         then emits AuthProfileDeployed to address.makeAddrExtern(multifactorHash, 256).
    /// @param descriptionHash  Hash of the profile description (used to verify sender)
    /// @param multifactorHash  Hash of the multifactor address (used as event address)
    /// @param description      Profile description
    function onProfileDeployed(uint256 descriptionHash, uint256 multifactorHash, string description) public {
        address expectedProfile = address(tvm.hash(_buildProfileStateInit(descriptionHash)));
        require(msg.sender == expectedProfile, ERR_INVALID_SENDER);

        address addrExtern = address.makeAddrExtern(multifactorHash, bitCntAddress);
        emit AuthProfileDeployed{dest: addrExtern}(msg.sender, multifactorHash, description);
    }

    /// @notice Returns the deterministic address of an AuthProfile by description
    /// @param description  Profile description
    /// @return profile  Deterministic AuthProfile contract address
    function getProfileAddress(string description) external view returns (address profile) {
        return address(tvm.hash(_buildProfileStateInit(_hashDescription(description))));
    }

    /// @notice Computes the pubkey hash expected by deployProfile
    /// @param pubkey  Public key (uint256)
    /// @return hash   tvm.hash of a cell containing the pubkey
    function hashPubkey(uint256 pubkey) external pure returns (uint256 hash) {
        TvmBuilder b;
        b.store(pubkey);
        return tvm.hash(b.toCell());
    }

    /// @notice Computes the multifactor address hash expected by deployProfile
    /// @param multifactor  Multifactor contract address
    /// @return hash        tvm.hash of a cell containing the address
    function hashMultifactor(address multifactor) external pure returns (uint256 hash) {
        TvmBuilder b;
        b.store(multifactor);
        return tvm.hash(b.toCell());
    }

    /// @notice Returns root version
    function getVersion() external pure returns (string, string) {
        return (version, "AuthServiceRoot");
    }

    // ─── Upgradeable ─────────────────────────────────────────────────────────

    /// @notice Upgrades the root contract code; owner-only.
    ///         After upgrade onCodeUpgrade is called on the new code.
    /// @param newcode  New contract code cell
    /// @param cell     Initialisation data passed to onCodeUpgrade
    function updateCode(TvmCell newcode, TvmCell cell) public {
        require(msg.pubkey() == _ownerPubkey, ERR_INVALID_SENDER);
        tvm.accept();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    /// @notice Called by UpdateZeroContract (or updateCode) after code swap.
    ///         Resets storage and decodes initial state from cell:
    ///         cell = abi.encode(profileCode, ownerPubkey).
    ///         Encoded by Giver.getDataForAuthService().
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        tvm.resetStorage();
        (_profileCode, _ownerPubkey) = abi.decode(cell, (TvmCell, uint256));
    }

    // ─── Internal helpers ────────────────────────────────────────────────────

    /// @dev Computes tvm.hash of a description string
    function _hashDescription(string description) private pure returns (uint256) {
        TvmBuilder b;
        b.store(description);
        return tvm.hash(b.toCell());
    }

    /// @dev Builds AuthProfile stateInit cell from descriptionHash
    function _buildProfileStateInit(uint256 descriptionHash) private view returns (TvmCell) {
        return abi.encodeStateInit({
            contr: AuthProfile,
            varInit: {
                _root: address(this),
                _descriptionHash: descriptionHash
            },
            code: _profileCode
        });
    }
}
