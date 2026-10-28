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
    string constant version = "4.0.36";

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
    uint256 constant ROOT_MODEL_CODE_HASH = 0xe92a14cb9c5ac757e16be2f453d5c3a25e7bec90044a1389b97414d1b785cac8;

    /// @notice Native value carried by the deploy message that creates a RootModel.
    /// @dev    A STARTING FIGURE, NOT A MEASUREMENT — and it is marked as such until a run
    ///         says otherwise. The child accepts on its constructor's first line, so most of
    ///         what it does is billed to itself; what this has to cover is delivery and the
    ///         part before that accept.
    ///
    ///         5 vmshell, and generous on purpose: BOTH ends of this message mint their own gas.
    ///         This contract does, and the child does too now that an internal `new` puts it in
    ///         this contract's configured dapp — under the old external deploy it landed in a
    ///         dapp with no configuration and its `ensureBalance` was dead code. So the figure
    ///         is not rationing anything scarce.
    ///
    ///         A REPEATED call credits this amount to whoever already occupies the address —
    ///         measured, not assumed, and recorded in `test_superroot_deploys_root`. That was
    ///         once treated as a leak worth a `mapping(uint256 => bool)` guard, and the guard
    ///         was removed again on the arithmetic: both ends of this message mint their own
    ///         gas from their dapp's configuration, so a repeat makes one free-gas contract
    ///         print for another and nothing is lost. The map's cost was not free — a record
    ///         per model owner, forever, in a contract at a fixed address, plus a migration
    ///         through `resetStorage` that would silently drop it and reopen the entry anyway.
    varuint16 constant ROOT_MODEL_DEPLOY_VALUE = 5 vmshell;

    event RootRegistered(address rootAddress);

    uint256 _ownerPubkey;
    TvmCell _rootModelCode;

    constructor(uint256 pubkey, TvmCell rootModelCode) accept {
        require(tvm.hash(rootModelCode) == ROOT_MODEL_CODE_HASH, ERR_BAD_CODE_HASH);
        // The third door into the same field, guarded like the other two. `onlyOwnerPubkey(0)`
        // admits every unsigned message, so a zero key here would hand `updateCode` and `setPubkey`
        // to anyone. External deploy through this constructor is a supported path — the contract
        // header says so and the history has such deploys — so "we bring it up from a stub" does
        // not make the entrance unreachable.
        require(pubkey != 0, ERR_NOT_OWNER);
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
        // Same guard as on the upgrade path, and for the same reason: `onlyOwnerPubkey(0)` admits
        // every unsigned message, so rotating the key to zero would hand `updateCode` and this
        // very setter to anyone. Rotation is the other door into the field and needs the check
        // just as much as `onCodeUpgrade` does.
        require(pubkey != 0, ERR_NOT_OWNER);
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
        // A zero key makes `onlyOwnerPubkey` admit every unsigned message, so `updateCode` would
        // no longer be owner-gated. Checked here rather than in the constructor: an account
        // upgraded from a stub never runs one.
        require(pubkey != 0, ERR_NOT_OWNER);
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

    /// @notice Deploy the RootModel for `ownerPubkey`. Anyone may call this.
    /// @dev    WHY THE SUPER ROOT DEPLOYS IT, AND WHY THAT FIXES MORE THAN IT LOOKS. A RootModel
    ///         used to be deployed by its owner as an external signed message, which put it in a
    ///         dapp of its own with no configuration. `gosh.mintshellq` draws on a dapp's
    ///         configuration, so `RootModel.ensureBalance()` was dead code from the day it was
    ///         written — the same trap already written out at length in `TokenContract.sol`. An
    ///         internal `new` from here puts the root in THIS contract's dapp, which is configured,
    ///         and the identical untouched line starts working.
    ///
    ///         OPEN ON PURPOSE. The address is derived from `ownerPubkey` alone, so a caller cannot
    ///         aim the deploy anywhere except that key's canonical address, and cannot choose what
    ///         is deployed there — the code comes from `_rootModelCode`, pinned and re-checked on
    ///         every upgrade. There is nothing to gate: the worst a stranger can do is pay to bring
    ///         somebody's root into existence at the address it was always going to have.
    ///
    ///         CALLING IT TWICE IS A NO-OP, and that matters more than the first call. An open
    ///         entry will be called again — by a retry, a race, or somebody idly poking it. A
    ///         `new` at an address that already holds an active account does not overwrite it and
    ///         does not revert this transaction; the deploy message simply fails to instantiate at
    ///         the far end. `bounce: false` so that failure does not come back as a bounce this
    ///         contract would have to handle. The `RootModelDeployed` event therefore reports an
    ///         ATTEMPT at an address, not proof of a fresh contract — the chain is what says which.
    /// @dev    NOTHING COMES FROM THE CALLER EXCEPT A PUBLIC KEY, and that is what makes an open
    ///         entry safe here rather than a matter of trust. Two earlier shapes of this took a
    ///         `TvmCell tokenContractCode`: first from the caller — which let anyone burn this
    ///         contract's value on a cell the CHILD would reject, after the `bounce: false`
    ///         message had already gone — then from storage behind an owner-only setter. Both
    ///         were answering a question that does not exist: `RootModel` validates that cell
    ///         against its own pinned hash and then THROWS IT AWAY. It derives deal addresses
    ///         from the pin, never from the cell. The check only ever proved the deployer held
    ///         a correct copy, which mattered while a human deployed it and means nothing now.
    /// @dev    `view` although it deploys: `new` writes nothing in THIS contract's storage, it
    ///         emits a message, and the compiler classifies it accordingly. The function it
    ///         replaces, `registerRoot`, carried the same modifier for the same reason.
    function deployRootModel(uint256 ownerPubkey) public view {
        tvm.accept();
        ensureBalance();
        address target = _calculateRootModelAddress(ownerPubkey);
        new RootModel{
            stateInit: abi.encodeStateInit({
                code: _rootModelCode,
                contr: RootModel,
                pubkey: ownerPubkey,
                varInit: {
                    _ownerPubkey: ownerPubkey,
                    _superRootAddress: address(this)
                }
            }),
            value: ROOT_MODEL_DEPLOY_VALUE,
            flag: 1,
            bounce: false
        }();

        address regExtern = address.makeAddrExtern(RootRegisteredEmit, bitCntAddress);
        emit RootRegistered{dest: regExtern}(target);
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
