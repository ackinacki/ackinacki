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
    string constant version = "4.0.36";

    // ── pinned InferenceOrderBook code (cascade-updated on version bump) ──
    /// @dev InferenceOrderBook code hash + depth, cascade-updated with every rebuild. NO
    ///      GENERATION NUMBER HERE ON PURPOSE: this comment carried "4.0.32" while the value
    ///      beneath it was already 4.0.33, because the cascade rewrites the literal and cannot
    ///      rewrite prose. A version in a comment beside a generated constant is a second copy
    ///      of a fact that has one owner, and it goes stale the first time the owner moves.
    ///      The book's only static is the model hash, so one model ⇒ one book.
    uint256 constant IOB_CODE_HASH  = 0xe97227c5d1a8fff171e0c5a1f6aa3e063f663bfcb5c86757392aef82a8775954;
    uint16  constant IOB_CODE_DEPTH = 31;

    /// @notice Self-top-up floor (mirrors the airegistry stack).
    uint64 constant MIN_BALANCE = 100 vmshell;

    // ── errors ────────────────────────────────────────────────
    uint16 constant ERR_NOT_OWNER  = 100; // caller is not the owner key
    uint16 constant ERR_NO_PUBKEY  = 101; // deployed without a pubkey
    uint16 constant ERR_NOT_MODEL  = 102; // model is not registered
    uint16 constant ERR_NAME_TOO_LONG = 103; // model name > 127 bytes (see MODEL_NAME_MAX)

    // A canonical model name MUST fit one cell (<=127 bytes), the same bound the
    // InferenceOrderBook / TokenContract constructors enforce. The book address is
    // derived from `sha256(name)`, and `sha256`/SHA256U hashes only the FIRST cell —
    // so a longer name is truncated to its first 127 bytes for the id while its entry
    // key here is `tvm.hash(bytes(name))` over the FULL name. Without this bound two
    // distinct long names sharing the first 127 bytes would be two registry entries
    // but resolve to ONE order book (merged liquidity/oracle). Enforcing the bound on
    // every write keeps both hashes injective over the name space → one entry ↔ one book.
    uint16 constant MODEL_NAME_MAX = 127;

    /// @notice nameHash => registered.  nameHash = tvm.hash(bytes(name)).
    // Value is the NAME, not a flag. A hash cannot be read back into the string it came from, so
    // a `bool` map could only ever answer "is this name registered?" for a name you already had to
    // guess. With the name stored, the registry can be read: `modelNameOf` returns what it was
    // seeded with. Costs ~31 bytes a row against ~1 bit — see `modelNameOf`.
    mapping(uint256 => string) _models;

    /// @notice Live cardinality of `_models`, maintained incrementally on every insert/remove so
    ///         `count()` is O(1). A full-map scan would blow the get-method gas budget at the
    ///         production scale (~8558+ entries).
    uint32 _count;

    /// @notice Key allowed to mutate the set and upgrade the code. Held in storage rather than
    ///         read from `tvm.pubkey()` so an upgrade can hand the registry to a different key:
    ///         `tvm.pubkey()` belongs to the stateInit and a code swap cannot change it, which
    ///         would otherwise pin ownership to whoever deployed the account. Seeded from
    ///         `tvm.pubkey()` at construction and by an upgrade that carries no new key, so the
    ///         deploy-time owner stays the owner unless one is explicitly supplied.
    uint256 _ownerPubkey;

    /// @dev Only the owner key may mutate. `accept` so the owner pays gas.
    modifier onlyOwner() {
        require(msg.pubkey() == _ownerPubkey, ERR_NOT_OWNER);
        tvm.accept();
        _;
    }

    constructor() {
        require(tvm.pubkey() != 0, ERR_NO_PUBKEY);
        tvm.accept();
        _ownerPubkey = tvm.pubkey();
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
            require(canonicalName.byteLength() <= MODEL_NAME_MAX, ERR_NAME_TOO_LONG);
            uint256 k = _key(canonicalName);
            if (_models[k].empty()) { _count++; }   // count only NEW keys → incremental
            _models[k] = canonicalName;
        }
    }

    /// @notice Register many canonical names in one call (bulk seed, e.g. 100 at
    ///         a time) — merged into the existing set. Empty strings are skipped.
    function setModels(string[] names) public onlyOwner {
        ensureBalance();
        for (uint32 i = 0; i < names.length; i++) {
            // Skip empty AND over-long (>127) names rather than revert the whole batch —
            // an over-long name would alias another entry's order book (see MODEL_NAME_MAX).
            if (!names[i].empty() && names[i].byteLength() <= MODEL_NAME_MAX) {
                uint256 k = _key(names[i]);
                if (_models[k].empty()) { _count++; }
                _models[k] = names[i];
            }
        }
    }

    /// @notice Unregister a canonical model name.
    function removeModel(string canonicalName) public onlyOwner {
        ensureBalance();
        uint256 k = _key(canonicalName);
        if (!_models[k].empty()) { delete _models[k]; _count--; }
    }

    /// @notice Upgrade this contract's code in place (owner-gated). The fixed
    ///         address is preserved. `onCodeUpgrade` WIPES the whole model set —
    ///         a clean slate on upgrade.
    function updateCode(TvmCell newcode) public onlyOwner {
        ensureBalance();
        // Carry the current owner across explicitly: `onCodeUpgrade` resets storage, and an
        // empty cell there means "no owner supplied" and falls back to the stateInit key.
        TvmCell migration = abi.encode(_ownerPubkey);
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(migration);
    }

    /// @dev Runs once, right after the code swap, and wipes the whole model set — the single
    ///     source of truth is re-seeded fresh afterwards.
    ///
    ///     The `TvmCell` parameter serves two purposes. It makes this entry point match the one
    ///     every other root exposes, so a caller that swaps the code and then invokes
    ///     `onCodeUpgrade(TvmCell)` — the zerostate premine stub does exactly that — resolves
    ///     the same function id and lands here; without the parameter that call resolves to no
    ///     function at all. And when the cell is non-empty it carries the pubkey to install as
    ///     the new owner, which is the only way to re-key the registry: ownership lives in
    ///     `_ownerPubkey` precisely because the stateInit key cannot be reassigned.
    ///
    ///     An empty cell keeps the current owner, so `updateCode` above stays a pure code swap.
    ///     Decode the cell BEFORE resetting storage and read nothing else from the old state:
    ///     the previous code may have had a different variable layout — the premine stub has no
    ///     `_ownerPubkey` at all — so the incoming cell is the only trustworthy input here.
    function onCodeUpgrade(TvmCell cell) private {
        tvm.accept();
        uint256 incomingOwner = cell.toSlice().empty() ? 0 : abi.decode(cell, uint256);
        tvm.resetStorage();
        // No key supplied: fall back to the account's stateInit key, which is the owner a
        // freshly constructed registry would have.
        _ownerPubkey = incomingOwner == 0 ? tvm.pubkey() : incomingOwner;
        require(_ownerPubkey != 0, ERR_NO_PUBKEY);
    }

    /// @dev Entry point for an upgrade issued by code that calls the NO-ARGUMENT form. The
    ///     function id is derived from the signature and the call is emitted by the OLD code
    ///     but executed in the new one, so both forms have to exist here for an upgrade to be
    ///     accepted from either kind of predecessor: the premine stub calls
    ///     `onCodeUpgrade(TvmCell)`, while the generation that declared `onCodeUpgrade()`
    ///     calls this one. No cell means no owner was supplied, so ownership falls back to the
    ///     account's stateInit key — which is exactly who owns that earlier generation.
    function onCodeUpgrade() private {
        TvmBuilder empty;
        onCodeUpgrade(empty.toCell());
    }

    /// @notice Key currently allowed to mutate the set and upgrade the code.
    function owner() external view returns (uint256) {
        return _ownerPubkey;
    }

    // ── getters ───────────────────────────────────────────────

    /// @notice Order book address of a REGISTERED model (reverts `ERR_NOT_MODEL`
    ///         if the name is not registered). Derived on the fly — no cache.
    function orderBookOf(string canonicalName) public view returns (address) {
        require(!_models[_key(canonicalName)].empty(), ERR_NOT_MODEL);
        return _orderBook(sha256(canonicalName));
    }

    /// @notice The name this registry holds under `key`, or empty if none. `key` is what `_key`
    ///         produces — `tvm.hash(bytes(name))` — NOT the model hash.
    /// @dev    THE TWO HASHES ARE DIFFERENT AND THE DIFFERENCE IS INVISIBLE. The map is keyed by
    ///         `tvm.hash`; the ORDER BOOK is addressed by `sha256(name)`. This getter first took a
    ///         `modelHash` and looked it up directly, which found nothing for every name ever
    ///         seeded — and found it QUIETLY, returning an empty string exactly as it would for a
    ///         name that is genuinely absent. `has` and `orderBookOf` went on working because they
    ///         call `_key` themselves, so nothing else showed the fault.
    function modelNameOf(uint256 key) external view returns (string) {
        return _models[key];
    }

    /// @notice The key `modelNameOf` wants for a given name — so a caller never has to know which
    ///         of the two hashes this map uses.
    function keyOf(string canonicalName) external pure returns (uint256) {
        return _key(canonicalName);
    }

    /// @notice Whether a name is registered.
    function has(string canonicalName) external view returns (bool) {
        return !_models[_key(canonicalName)].empty();
    }

    /// @notice sha256(canonicalName) — the on-chain authoritative model id.
    function modelHashOf(string canonicalName) external pure returns (uint256) {
        return sha256(canonicalName);
    }

    /// @notice Number of registered models.
    function count() external view returns (uint32) {
        return _count;   // O(1) — no full-map scan (the set is unbounded at ~8558+ entries)
    }

    /// @notice The pinned book code hash + depth used for derivation.
    function inferenceOrderBookCode() external pure returns (uint256 codeHash, uint16 codeDepth) {
        return (IOB_CODE_HASH, IOB_CODE_DEPTH);
    }

    /// @notice Contract version, in the same shape the four sibling contracts expose it.
    /// @dev    This registry was the only one of the five airegistry contracts without it: the
    ///         `version` constant was declared and read by nothing, so the registry's version was
    ///         the one thing in the stack that could not be checked against a running chain.
    function getVersion() external pure returns (string, string) {
        return (version, "ModelRegistry");
    }
}
