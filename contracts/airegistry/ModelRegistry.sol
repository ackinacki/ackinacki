pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

// The book type is imported ONLY so `abi.encodeStateInit` reproduces the exact
// InferenceOrderBook init-data layout for address derivation. Together with the
// pinned code hash/depth below this PINS the registry to a specific
// InferenceOrderBook version — the derived address is valid only for books
// deployed by that SAME version (the init-data cell is sensitive to the book's
// full variable layout, not just its `_modelHash` static — verified on-chain).
// The pin is cascade-updated (like NOTE_CODE_HASH) on every book layout/version
// bump: bump IOB_CODE_HASH + IOB_CODE_DEPTH and `updateCode`. Nothing else to do —
// the address is derived on read, so a pin change is picked up automatically.
import "./InferenceOrderBook.sol";

/// @title Canonical Model Registry
/// @notice Owner-curated set of canonical model names. A name is either present
///         (registered) or not — `mapping(nameHash => bool)`. The order book
///         address is DERIVED on read from `sha256(name)` + the pinned book code
///         hash/depth (mirroring DexLib.computeInferenceOrderBookAddress); it is
///         NOT stored, so an IOB pin bump needs no re-cache.
/// @dev    Owner == the account's key (`tvm.pubkey()`), set in the zerostate to
///         the SuperRoot key. Only the owner may mutate or `updateCode`.
contract ModelRegistry {

    /// @notice Contract semantic version (kept in lockstep with the airegistry stack).
    string constant version = "4.0.27";

    // ── pinned InferenceOrderBook code (cascade-updated on version bump) ──
    /// @dev InferenceOrderBook 4.0.20 (tickSize=1M) code hash + depth. One model ⇒ one book.
    uint256 constant IOB_CODE_HASH  = 0x6749dae6b943dbbbeee4268186924da750f027ff721f9c1e936b0b1081e9b522;
    uint16  constant IOB_CODE_DEPTH = 33;

    /// @notice Self-top-up floor (mirrors the airegistry stack).
    uint64 constant MIN_BALANCE = 100 vmshell;

    // ── errors ────────────────────────────────────────────────
    uint16 constant ERR_NOT_OWNER  = 100; // caller is not the owner key
    uint16 constant ERR_NO_PUBKEY  = 101; // deployed without a pubkey
    uint16 constant ERR_NOT_MODEL  = 102; // model is not registered

    /// @notice nameHash => registered.  nameHash = tvm.hash(bytes(name)).
    mapping(uint256 => bool) _models;

    /// @dev Only the owner key may mutate. `accept` so the owner pays gas.
    modifier onlyOwner() {
        require(msg.pubkey() == tvm.pubkey(), ERR_NOT_OWNER);
        tvm.accept();
        _;
    }

    constructor() {
        require(tvm.pubkey() != 0, ERR_NO_PUBKEY);
        tvm.accept();
    }

    /// @dev Keep the account funded for its own gas/outgoing messages.
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    function _key(string name) private pure returns (uint256) {
        return tvm.hash(bytes(name));
    }

    /// @dev Derive the book address from the pinned code hash/depth + model hash.
    ///      Builds the init-data cell via `contr: InferenceOrderBook` (empty
    ///      code) then hashes the StateInit from (codeHash, dataHash, depths) —
    ///      identical to DexLib's hash-based derivations.
    function _orderBook(uint256 modelHash) private pure returns (address) {
        TvmCell dummy;
        TvmCell si = abi.encodeStateInit({
            contr: InferenceOrderBook,
            varInit: { _modelHash: modelHash },
            code: dummy
        });
        TvmSlice s = si.toSlice();
        s.skip(5);       // StateInit header: split_depth/special/code?/data?/library?
        s.loadRef();     // skip the (empty) code ref
        TvmCell data = s.loadRef();
        return address.makeAddrStd(
            0, abi.stateInitHash(IOB_CODE_HASH, tvm.hash(data), IOB_CODE_DEPTH, data.depth())
        );
    }

    // ── mutators (owner only) ─────────────────────────────────

    /// @notice Register a single canonical model name.
    function setModel(string canonicalName) public onlyOwner {
        ensureBalance();
        if (!canonicalName.empty()) {
            _models[_key(canonicalName)] = true;
        }
    }

    /// @notice Register many canonical names in one call (bulk seed, e.g. 100 at
    ///         a time) — merged into the existing set. Empty strings are skipped.
    function setModels(string[] names) public onlyOwner {
        ensureBalance();
        for (uint32 i = 0; i < names.length; i++) {
            if (!names[i].empty()) {
                _models[_key(names[i])] = true;
            }
        }
    }

    /// @notice Unregister a canonical model name.
    function removeModel(string canonicalName) public onlyOwner {
        ensureBalance();
        delete _models[_key(canonicalName)];
    }

    /// @notice Upgrade this contract's code in place (owner-gated). The fixed
    ///         address is preserved. `onCodeUpgrade` WIPES the whole model set —
    ///         a clean slate on upgrade.
    function updateCode(TvmCell newcode) public onlyOwner {
        ensureBalance();
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade();
    }

    /// @dev Runs once, right after the code swap. Wipes all registered models
    ///     (single source of truth is re-seeded fresh after an upgrade).
    function onCodeUpgrade() private {
        delete _models;
    }

    // ── getters ───────────────────────────────────────────────

    /// @notice Order book address of a REGISTERED model (reverts `ERR_NOT_MODEL`
    ///         if the name is not registered). Derived on the fly — no cache.
    function orderBookOf(string canonicalName) public view returns (address) {
        require(_models[_key(canonicalName)], ERR_NOT_MODEL);
        return _orderBook(sha256(canonicalName));
    }

    /// @notice Whether a name is registered.
    function has(string canonicalName) external view returns (bool) {
        return _models[_key(canonicalName)];
    }

    /// @notice sha256(canonicalName) — the on-chain authoritative model id.
    function modelHashOf(string canonicalName) external pure returns (uint256) {
        return sha256(canonicalName);
    }

    /// @notice Number of registered models.
    function count() external view returns (uint32 n) {
        for ((, bool v) : _models) {
            v;
            n++;
        }
    }

    /// @notice The pinned book code hash + depth used for derivation.
    function inferenceOrderBookCode() external pure returns (uint256 codeHash, uint16 codeDepth) {
        return (IOB_CODE_HASH, IOB_CODE_DEPTH);
    }
}
