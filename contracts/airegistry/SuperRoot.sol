pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./RootModel.sol";

/// @title SuperRoot
/// @notice Global registry for all RootModel contracts.
///         Two supported deploy paths:
///           - **Zerostate**: pre-placed at a chosen vanity address (e.g.,
///             `0:a11a...a11a`) via `zerostate-helper add contract`.
///           - **External message**: deployed via constructor at the
///             deterministic `tvm.hash(stateInit)` address (one canonical
///             address per code version since there are no static fields).
///         The RootModel code is passed at construction and updated only via
///         `updateCode` (owner-gated, in place) on a version bump; RootModel
///         addresses derived by this SuperRoot are stable between such updates.
///         Children bind to this specific SuperRoot instance
///         via their `_superRootAddress` static, which is mixed into the
///         derivation here.
contract SuperRoot is AiRegistryModifiers {
    string constant version = "4.0.27";

    /// @notice Canonical code hash of the child RootModel. The constructor
    ///         rejects any caller-supplied code whose `tvm.hash` does not
    ///         match this constant — this locks the registry to one
    ///         specific RootModel bytecode per code version. To bump versions,
    ///         rebuild RootModel, recompute the hash below, recompile
    ///         SuperRoot, then `updateCode` the live SuperRoot in place
    ///         (passing the new RootModel code) — no redeploy / no address change.
    /// @dev    Hash is for the child *code* cell (not stateInit), i.e.
    ///         the value returned by `tvm.decodeStateInit(tvc).code` then
    ///         `tvm.hash`. It is the same as the `code_hash` field
    ///         printed by `tvm-cli decode stateinit --tvc <file>`.
    uint256 constant ROOT_MODEL_CODE_HASH = 0x0a6fe90e89faa99bdd4286965ec75e5085d7c0f365b8c2e3e1467cf584d359bc;

    event RootRegistered(address rootAddress);

    uint256 _ownerPubkey;
    TvmCell _rootModelCode;

    constructor(uint256 pubkey, TvmCell rootModelCode) accept {
        require(tvm.hash(rootModelCode) == ROOT_MODEL_CODE_HASH, ERR_BAD_CODE_HASH);
        _ownerPubkey = pubkey;
        _rootModelCode = rootModelCode;
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    // ========================================================
    // Owner pubkey rotation (code is upgradable in place via updateCode below)
    // ========================================================

    function setPubkey(uint256 pubkey) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        _ownerPubkey = pubkey;
    }

    // ========================================================
    // Code upgrade — keep the SuperRoot at a FIXED address across versions
    // ========================================================

    /// @notice Owner-gated in-place code swap. Lets the SuperRoot live at ONE
    ///         address forever (deployed once, code updated per version) instead
    ///         of rotating its code-derived address on every version bump. On a
    ///         version bump the child RootModel changes too, so pass its new code
    ///         as `newRootModelCode` (the new SuperRoot code carries the matching
    ///         `ROOT_MODEL_CODE_HASH`, re-checked in `onCodeUpgrade`); pass an
    ///         empty cell to keep the current RootModel code.
    function updateCode(TvmCell newcode, TvmCell newRootModelCode) public onlyOwnerPubkey(_ownerPubkey) accept {
        ensureBalance();
        TvmCell migrationCell = abi.encode(_ownerPubkey, _rootModelCode, newRootModelCode);
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(migrationCell);
    }

    /// @notice Re-init from the migration cell after `updateCode` (and the only
    ///         place storage is rebuilt post-swap). `newRootModelCode` empty ⇒
    ///         keep the old RootModel code; else adopt it. Either way the stored
    ///         code must hash to the new `ROOT_MODEL_CODE_HASH` pin.
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        tvm.resetStorage();
        (uint256 pubkey, TvmCell oldRootModelCode, TvmCell newRootModelCode)
            = abi.decode(cell, (uint256, TvmCell, TvmCell));
        _ownerPubkey = pubkey;
        _rootModelCode = newRootModelCode.toSlice().empty() ? oldRootModelCode : newRootModelCode;
        require(tvm.hash(_rootModelCode) == ROOT_MODEL_CODE_HASH, ERR_BAD_CODE_HASH);
    }

    // ========================================================
    // Address derivation
    // ========================================================

    function _calculateRootModelAddress(uint256 ownerPubkey) private view returns (address) {
        TvmCell s = abi.encodeStateInit({
            code: _rootModelCode,
            contr: RootModel,
            pubkey: ownerPubkey,
            varInit: {
                _ownerPubkey: ownerPubkey,
                _superRootAddress: address(this)
            }
        });
        return address.makeAddrStd(0, tvm.hash(s));
    }

    // ========================================================
    // Registration entry points (called by child contracts)
    // ========================================================

    function registerRoot(uint256 ownerPubkey) public {
        ensureBalance();
        address expected = _calculateRootModelAddress(ownerPubkey);
        require(msg.sender == expected, ERR_INVALID_SENDER);
        tvm.accept();

        address regExtern = address.makeAddrExtern(RootRegisteredEmit, bitCntAddress);
        emit RootRegistered{dest: regExtern}(expected);
    }

    // ========================================================
    // Getters
    // ========================================================

    function getRootModelAddress(uint256 ownerPubkey) external view returns (address) {
        return _calculateRootModelAddress(ownerPubkey);
    }

    function getOwnerPubkey() external view returns (uint256) { return _ownerPubkey; }

    function getVersion() external pure returns (string, string) {
        return (version, "SuperRoot");
    }
}
