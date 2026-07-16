pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./interfaces.sol";
import "../dex/libraries/DexLib.sol";
import "../dex/RootPN.sol";
import "./InferenceOrderBook.sol";

/// @title TokenContract (streaming deal / `token_contract` per spec §3-4)
/// @notice One streaming inference deal between one seller (owner) and one
///         buyer. Payment is in ECC[2] SHELL, escrowed in this contract;
///         identities/locks ride on PrivateNotes (model B).
///
///         PROBE TICK (spec §3.1.2). The first tick of every stream is a
///         *probe*: it is FROZEN from the buyer's escrow and NOT prepaid to the
///         seller, and the seller posts `SELLER_PROBE_COMMISSION` (≈ the
///         platform fee on one tick, `_probeCommission()`). While in `Probe`:
///           - silence through `SETTLE_WINDOW` (advance) → probe accepted: the
///             probe tick is finalized to the seller, the commission is returned
///             to the seller, the platform fee is taken by-fact, and the deal
///             enters `Streaming` with the standard two-tick invariant (§3.2);
///           - buyer `stop()` → BURN BOTH: the buyer's probe tick AND the
///             seller's commission go to `gosh.burnecc`; nothing to either side
///             (scam revenue = 0, §3.1.2/§5.4). Remaining deposit refunds the buyer;
///           - seller no-show (`reclaimOnTimeout`, `STREAM_TIMEOUT`) → the buyer
///             reclaims the probe tick in full (pays nothing) and the commission
///             is returned to the seller; NO burn (no-show is not slashed, §9.1);
///           - dispute → both notes lock; on `DISPUTE_WINDOW` timeout it reduces
///             to the probe rule (burn both, §4.2), not the standard split.
///         Burn happens ONLY on an active buyer stop, never on a seller no-show.
///
///         After the probe is accepted (`_probeAccepted`), at any open moment
///         exactly one tick is prepaid to the seller (delivered, awaiting
///         finalization) and exactly one tick is frozen as buffer (spec §3.2);
///         lifecycle is the standard split (§4.1) untouched.
///
///         Timing windows and the platform fee are PROTOCOL CONSTANTS
///         (`SETTLE_WINDOW`/`STREAM_TIMEOUT`/`DISPUTE_WINDOW`/`PLATFORM_FEE_BPS`
///         in modifiers.sol), not per-deal parameters.
///
///         Lifecycle:
///         1. `fund()`/`fundFromOrderBook()` — buyer escrows SHELL; the buyer
///                             note pubkey is recorded (spec §2.3/§3.1.1).
///         1b.`fundProbeCommission()` — seller posts SELLER_PROBE_COMMISSION.
///         2. `open(cipher)` — seller posts the endpoint encrypted to the
///                             buyer's pubkey, freezes the probe tick, locks the
///                             buyer note; state = `Probe`.
///         3. `advance()`    — seller-driven: after `SETTLE_WINDOW` of buyer
///                             silence, accept the probe (Probe→Streaming) or
///                             finalize the prepaid tick (Streaming).
///         4a.`stop()`       — buyer exit: Probe → burn both; Streaming → §4.1.
///         4b.`dispute()`    — buyer contests; both notes lock;
///                             `resolveDisputeTimeout()`/`releaseDispute()`.
///         4c.`reclaimOnTimeout()` — seller no-show after `STREAM_TIMEOUT`.
///         5. `withdrawShell`/`destroy` — seller pulls finalized SHELL (§3.5).
contract TokenContract is AiRegistryModifiers {
    string constant version = "4.0.27";

    // Canonical AI SuperRoot account id (workchain 0) — same anchor IOB/PN pin. Used ONLY as the
    // fixed sink for `cleanupUnopened`'s residual-native sweep (so a permissionless caller cannot
    // route the leftover gas to an arbitrary address). On shellnet the SuperRoot is now ADDRESS-STABLE
    // across versions (`SuperRoot.updateCode` swaps code in place, no rotation), so this is a fixed
    // literal — no genaddr recompute, no pin cycle. LOCAL/MAINNET build: the vanity 0:0c0c… SuperRoot.
    uint256 constant SUPER_ROOT_ADDR = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;

    // Canonical PrivateNote code hash/depth. `postFromNote` proves the note-driven
    // caller is a genuine PrivateNote (pinned code) for the supplied deposit id. Only
    // the canonical RootPN can deploy a canonical-code note, and it always bakes its
    // real InferenceOrderBook code into that note, so a passing caller's supplied
    // book hash is authoritative — the TC needs no RootPN round-trip. The note does
    // NOT pin the TC code (RootPN bakes it into the note at deploy), so this pin is
    // one-way (TC->note) and the build stays cycle-free. Re-pin when PrivateNote is rebuilt.
    uint256 constant PRIVATE_NOTE_CODE_HASH  = 0x5f78299c6438d3e156042ef1ed9fcd70064a3f34221b2c051c99567c9f21ef2e;
    uint16  constant PRIVATE_NOTE_CODE_DEPTH = 19;

    // Native value attached to THIS contract's cross-dapp messages (register / stream-lock /
    // payout). Tunable; recipients self-fund via `accept`/`ensureBalance`, so this
    // only needs to cover what a non-accepting hop requires. (TC/RM-local — NOT the shared
    // REGISTER_FORWARD_VALUE the IOB also uses for its SHELL handover.)
    varuint16 constant DAPP_MSG_VALUE = 0.01 vmshell;

    event ContractDeployed(address self);
    event StreamFunded(address buyer, uint128 deposit);
    event ProbeCommissionFunded(uint128 amount);
    event StreamOpened(address buyer, uint128 pricePerTick);
    event ProbeAccepted(address buyer, uint128 toSeller, uint128 commissionReturned);
    event ProbeBurned(address buyer, uint128 burnedProbe, uint128 burnedCommission, uint128 refundToBuyer);
    event TickFinalized(uint128 finalizedOwed, uint128 deposit);
    event StreamStopped(address buyer, uint128 toSeller, uint128 refundToBuyer);
    event StreamDisputed(address buyer, uint64 at);
    event DisputeResolved(uint128 toSeller, uint128 refundToBuyer, bool released);
    event StreamReclaimed(address buyer, uint128 refundToBuyer);
    event ShellWithdrawn(address recipient, uint128 amount);
    event ContractDestroyed(address self);

    // Static (part of stateInit, contribute to address derivation).
    uint256 static _sellerPubkey;
    address static _rootModelAddress;
    uint64 static _nonce;

    // Canonical InferenceOrderBook code hash/depth, delivered by the seller's
    // canonical PrivateNote via `postFromNote` (runtime, NOT a pinned constant → no
    // IOB<->TC cycle). Authenticates the `fundFromOrderBook` caller AND lets the TC
    // derive the book address to post its own sell offer. 0 until the note posts.
    uint256 _iobHash;
    uint16  _iobDepth;
    // Seller note confirmed this TC is its deal (`postFromNote`, msg.sender ==
    // _sellerNote). A TC nobody confirmed never gets the book hash → never trades.
    bool    _noteAuthorized;
    // A TC is a one-shot deal: at most one LIVE resting sell offer (was enforced by
    // the IOB `_sellTcInUse` map; now the TC itself is the single source). Cleared
    // on cancel (`onSellClosed`) so the seller can re-list on the same live TC.
    bool    _offerPosted;
    // Seller wind-down intent (`close`): when a live offer must be cancelled first,
    // the note's cancel fires `onSellClosed`, which then self-destructs (the offer
    // is provably off the book by then, so no resting offer outlives the TC).
    // `_closePayout` is where the residual native gas is swept.
    bool    _closing;
    address _closePayout;

    // Immutable deal config (constructor).
    string  _modelName;
    uint256 _modelHash;       // sha256(_modelName), verified in the ctor — on-chain authoritative id
    // tokens per tick — FIXED protocol constant (no longer a per-deal param).
    uint128 constant TICK_SIZE = 1_000_000;
    uint128 _pricePerTick;    // SHELL per tick (P)
    uint128 _maxTicks;        // upper bound on ticks this deal serves

    // Deal state.
    address _buyer;           // buyer note address (funder; payouts/locks)
    uint256 _buyerPubkey;     // buyer note pubkey (gateway auth, spec §3.1.1)
    // Direct (OTC, off-book) fund() is DISABLED by default: the seller must first
    // sanction a specific buyer note via `authorizeDirectFund`, so only that note can
    // bind as `_buyer` on this TC. Auth is by the caller BEING the sanctioned note
    // (address auth) — fund() carries ECC SHELL so it is an internal message and
    // msg.pubkey() is 0, i.e. signature-pubkey auth is unavailable; the sanctioned
    // buyer pubkey is recorded here for the §3.1.1 gateway.
    address _authorizedBuyer;
    uint256 _authorizedBuyerPubkey;
    address _sellerNote;      // seller note address (dispute lock)
    bytes   _endpointCipher;  // endpoint encrypted to the buyer's pubkey

    bool    _funded;
    bool    _opened;
    // Latch: open() was ever called. cleanupUnopened is a permissionless no-show
    // recovery for a funded-but-NEVER-opened deal. The latch scopes it to that case
    // only: after a normal open+close the `!_opened` guard is true again (stop/reclaim
    // leave _opened=false), and this latch keeps cleanupUnopened from running once the
    // seller has real `_finalizedOwed` earnings to withdraw.
    bool    _everOpened;
    bool    _probeAccepted;   // false = Probe (first tick), true = Streaming (§3.1.2)
    bool    _disputed;

    bool    _sellerProbeFunded; // seller posted SELLER_PROBE_COMMISSION
    uint128 _sellerProbeLocked; // SHELL held as the seller's probe commission

    uint128 _deposit;         // SHELL available for future ticks (value + reserved fee)
    uint128 _prepaid;         // SHELL: the delivered, not-yet-finalized tick (value P); 0 in Probe
    uint128 _frozen;          // SHELL: buffer tick in Streaming, or the probe tick in Probe (value P)
    uint128 _finalizedOwed;   // SHELL finalized to the seller (withdrawable; incl. rebate / returned commission)
    uint128 _feeAccrued;      // SHELL fee charged by-fact on finalized ticks (§5.1)
    uint128 _ticksFinalized;  // count of finalized ticks (n for rebate §5.3)
    bool    _everDisputed;    // a dispute ever opened → no rebate (§5.3)
    uint64  _fundedTime;      // when funded (MATCH_OPEN_TIMEOUT cleanup, §2.1)
    uint64  _prepaidTime;     // when `_prepaid`/probe was set (settle window)
    uint64  _lastAdvance;     // last seller activity (stream timeout)
    uint64  _disputeTime;     // when the dispute opened
    uint64  _settleWindow;    // per-deal advance window W = f(pricePerTick), §9.1
    uint64  _streamTimeout;   // per-deal reclaim window = W + grace, §9.1

    constructor(
        string  modelName,
        uint256 modelHash,
        uint128 pricePerTick,
        uint128 maxTicks,
        address sellerNote
    ) {
        // Deployer authentication: the deal TC is deployed off-chain by the seller as an EXTERNAL
        // message signed with the seller key (`pubkey == _sellerPubkey` in the stateInit). Gate the
        // constructor to that key BEFORE accept, so nobody else can occupy this canonical (sellerPubkey,
        // nonce) address with foreign deal terms (price/model/maxTicks/sellerNote are ctor args, not part
        // of the address). Without this, a third party could front-run the deploy and the seller's
        // `postSellOffer` would then rest an offer carrying injected terms in the seller's name.
        require(msg.pubkey() == _sellerPubkey, ERR_INVALID_SENDER);
        tvm.accept();
        require(pricePerTick > 0, ERR_BAD_PARAM);
        require(maxTicks >= 2, ERR_BAD_PARAM);
        // maxTicks*(price + fee) must fit uint128 so every downstream unit×ticks total (the
        // deposit bound here and the order-book fill cost) stays overflow-free.
        require(uint256(maxTicks) * uint256(pricePerTick + _fee(pricePerTick)) <= uint256(type(uint128).max), ERR_OVERFLOW);
        // On-chain authoritative model id: same single-cell sha256 invariant as the order book.
        // Binds this deal contract's modelHash to the verified `producer--model--version` preimage
        // (so an indexer reading the TC alone gets the genuine model name, not a free-text label).
        require(modelName.byteLength() <= 127, ERR_BAD_PARAM);
        require(sha256(modelName) == modelHash, ERR_BAD_PARAM);

        _modelName    = modelName;
        _modelHash    = modelHash;
        _pricePerTick = pricePerTick;
        _maxTicks     = maxTicks;
        _sellerNote   = sellerNote;

        // Per-deal advance window scaled by tick price (caps idle drain to the
        // slope), clamped to [SETTLE_WINDOW, STREAM_WINDOW_MAX]; the reclaim
        // window is W + grace so reclaim is always strictly after advance (§9.1).
        uint64 w = uint64(uint256(pricePerTick) * uint256(STREAM_WINDOW_SECS_PER_SHELL) / uint256(SHELL_UNIT));
        if (w < SETTLE_WINDOW) { w = SETTLE_WINDOW; }
        if (w > STREAM_WINDOW_MAX) { w = STREAM_WINDOW_MAX; }
        _settleWindow  = w;
        _streamTimeout = w + STREAM_TIMEOUT_GRACE;

        ensureBalance();

        address selfExtern = address.makeAddrExtern(ContractDeployedEmit, bitCntAddress);
        emit ContractDeployed{dest: selfExtern}(address(this));

        IRootModelRegistry(_rootModelAddress).registerTokenContract{value: DAPP_MSG_VALUE, flag: 1}(_sellerPubkey, _nonce);
        // NOTE: the InferenceOrderBook hash is delivered later by the seller's
        // canonical PrivateNote (`postFromNote`), so a TC nobody confirmed never activates.
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    function _payShell(address to, uint128 amount) private pure {
        if (amount == 0) { return; }
        mapping(uint32 => varuint32) ecc;
        ecc[SHELL_ECC_ID] = varuint32(amount);
        to.transfer({value: DAPP_MSG_VALUE, bounce: false, flag: 1, currencies: ecc});
    }

    /// @notice Burn SHELL via gosh.burnecc (spec §5.4). uint64-bounded like _settleFees.
    function _burnShell(uint128 amount) private pure {
        // gosh.burnecc takes uint64; an amount above uint64.max (an expensive deal's
        // probe tick / commission) is burned in up to 10 uint64-sized chunks rather
        // than one truncated call. The 10-chunk bound keeps the burn within the
        // action/gas limit so the close cannot brick; 10 * uint64.max is far above
        // any real deal's SHELL, so a genuine burn always completes within it.
        uint128 cap = uint128(type(uint64).max);
        for (uint8 i = 0; i < 10 && amount > 0; i++) {
            uint128 chunk = amount > cap ? cap : amount;
            gosh.burnecc(uint64(chunk), SHELL_ECC_ID);
            amount -= chunk;
        }
    }

    /// @notice Platform fee (2.5%, PLATFORM_FEE_BPS) of `amount` (spec §5.1).
    function _fee(uint128 amount) private pure returns (uint128) {
        return uint128(uint256(amount) * uint256(PLATFORM_FEE_BPS) / uint256(BPS_DENOMINATOR));
    }

    /// @notice Seller probe commission (spec §3.1.2/§9.2): a percent of the tick
    ///         price P (`SELLER_PROBE_COMMISSION_BPS`), on the order of the
    ///         platform fee on a single tick. Returned to the seller on probe
    ///         acceptance / no-show; burned with the probe tick on a probe stop.
    function _probeCommission() private view returns (uint128) {
        return uint128(uint256(_pricePerTick) * uint256(SELLER_PROBE_COMMISSION_BPS) / uint256(BPS_DENOMINATOR));
    }

    /// @notice Seller rebate (§5.3) for `n` cleanly-finalized ticks at price P:
    ///         rate = min(REBATE_MAX_BPS, REBATE_SLOPE_BPS·n) bps; rebate =
    ///         rate/10000 · n · P. Always < the fee charged on n ticks (rate <
    ///         PLATFORM_FEE_BPS by construction), so net burn stays positive.
    function _rebate(uint128 n) private view returns (uint128) {
        uint256 rateBps = uint256(REBATE_SLOPE_BPS) * uint256(n);
        if (rateBps > uint256(REBATE_MAX_BPS)) { rateBps = uint256(REBATE_MAX_BPS); }
        return uint128(rateBps * uint256(n) * uint256(_pricePerTick) / uint256(BPS_DENOMINATOR));
    }

    /// @notice Settle accrued fees at close: pay the seller a rebate (only on a
    ///         clean, never-disputed close, §5.3) and burn the net (§5.4).
    function _settleFees(bool clean) private {
        uint128 rebate = 0;
        if (clean && !_everDisputed) {
            rebate = _rebate(_ticksFinalized);
            if (rebate > _feeAccrued) { rebate = _feeAccrued; }   // safety (never triggers by construction)
        }
        uint128 netBurn = _feeAccrued - rebate;
        if (rebate > 0) { _finalizedOwed += rebate; }             // seller withdraws it
        _burnShell(netBurn);
        _feeAccrued = 0;
    }

    // ========================================================
    // 1. Fund — buyer escrows SHELL, locks the deal
    // ========================================================

    function _recordFunding(address buyer, uint256 buyerPubkey, uint128 paid) private {
        // Buyer-side, by-fact fee (§5.1): the escrow covers per-tick (P + fee).
        // Must cover >= 2 full ticks (probe + at least one streaming tick) and <= maxTicks.
        uint128 unit = _pricePerTick + _fee(_pricePerTick);
        require(paid >= 2 * unit, ERR_INSUFFICIENT_DEPOSIT);
        require(uint256(paid) <= uint256(_maxTicks) * uint256(unit), ERR_OVERFLOW);
        _buyer       = buyer;
        _buyerPubkey = buyerPubkey;
        _deposit     = paid;
        _funded      = true;
        // A match-fill consumes the offer (IOB removed it from the book before this
        // callback); a direct fund() requires !_offerPosted. Either way there is no
        // longer a live offer -> clear the latch so destroy()/withdrawShell judge by
        // the REAL offer state, not the `_funded` proxy. Also blocks re-list.
        _offerPosted = false;
        _fundedTime  = uint64(block.timestamp);
        emit StreamFunded{dest: address.makeAddrExtern(StreamFundedEmit, bitCntAddress)}(buyer, paid);
    }

    /// @notice Buyer sends ECC[2] SHELL to escrow the deal (direct path) and
    ///         records the buyer note pubkey for gateway auth (spec §3.1.1).
    function fund() public {
        ensureBalance();
        require(!_funded, ERR_ALREADY_FUNDED);
        // Mutually exclusive with a live offer: a TC resting on the book funds ONLY
        // via the order-book match — never a direct deposit that would coexist with
        // the offer, so a TC is sold exactly once.
        require(!_offerPosted, ERR_OFFER_LIVE);
        // Only the seller-sanctioned buyer note. Default none -> direct fund()
        // disabled, all funding flows through fundFromOrderBook.
        require(_authorizedBuyer != address(0) && msg.sender == _authorizedBuyer, ERR_INVALID_SENDER);
        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.exists(SHELL_ECC_ID), ERR_NO_SHELL);
        tvm.accept();
        _recordFunding(msg.sender, _authorizedBuyerPubkey, uint128(currencies[SHELL_ECC_ID]));
    }

    /// @notice Seller sanctions ONE specific buyer note (address + gateway pubkey) for
    ///         the direct off-book fund() path. Default: none -> direct fund() is
    ///         disabled and all funding flows through the order-book match. The address
    ///         is the auth handle; the pubkey is recorded as `_buyerPubkey` on fund()
    ///         for the §3.1.1 gateway. Not while already funded.
    function authorizeDirectFund(address buyerNote, uint256 buyerPubkey) public onlyOwnerPubkey(_sellerPubkey) accept {
        require(!_funded, ERR_ALREADY_FUNDED);
        ensureBalance();
        _authorizedBuyer       = buyerNote;
        _authorizedBuyerPubkey = buyerPubkey;
    }

    /// @notice Order-book handover (spec §2.3): the InferenceOrderBook forwards
    ///         the matched SHELL, binds the buyer note (not msg.sender), and
    ///         records the buyer note pubkey it held in the book — the buyer's
    ///         PrivateNote forwards its `_ephemeralPubkey` when ordering, the OB
    ///         threads it through the match (§3.1.1, gateway auth).
    function fundFromOrderBook(address buyerNote, uint256 buyerPubkey) public {
        ensureBalance();
        mapping(uint32 => varuint32) currencies = msg.currencies;
        if (!currencies.exists(SHELL_ECC_ID)) { return; }
        tvm.accept();
        uint128 paid = uint128(currencies[SHELL_ECC_ID]);
        // The book forwards bounce:false, so this path never reverts: on any non-fundable fill —
        // already funded (nonce reuse), under 2 ticks, or over maxTicks — it accepts and refunds
        // the buyer note IN FULL rather than reverting, so the buyer's SHELL is always returned.
        // Authenticate the caller as the canonical InferenceOrderBook for this
        // model (hash delivered by RootPN), so only a real match binds `buyerNote`
        // as `_buyer`. `_iobHash == 0` = RootPN reply not arrived yet: also a
        // non-fundable case → refund + retry (same accept-then-refund pattern, so the
        // buyer's SHELL is never held on a revert).
        uint128 unit = _pricePerTick + _fee(_pricePerTick);
        if (_iobHash == 0
            || msg.sender != DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash)
            || _funded || paid < 2 * unit || uint256(paid) > uint256(_maxTicks) * uint256(unit)) {
            _payShell(buyerNote, paid);
            return;
        }
        _recordFunding(buyerNote, buyerPubkey, paid);
    }

    /// @notice Note-driven, single-call sell-offer post (spec §2.3). The seller's
    ///         canonical PrivateNote calls this after deriving THIS TC locally; it
    ///         delivers the canonical InferenceOrderBook code hash/depth (baked into
    ///         the note by RootPN at deploy), so the TC needs no RootPN round-trip.
    ///         Auth is dual: `msg.sender == _sellerNote` AND `_sellerNote` is the
    ///         canonical PrivateNote for the supplied `depositIdentifierHash` (pinned
    ///         PrivateNote code). Only the canonical RootPN can deploy a canonical-code
    ///         note, and it always bakes its real book code, so a passing caller's
    ///         `iobHash` is authoritative — no forged book. Stores the hashes (enabling
    ///         `fundFromOrderBook`) and, on the first post, places the resting ask
    ///         itself (`msg.sender == TC` at the book). Re-run after a cancel re-posts.
    /// @param iobHash Canonical InferenceOrderBook code hash the note holds.
    /// @param iobDepth Canonical InferenceOrderBook code depth the note holds.
    /// @param depositIdentifierHash The calling note's deposit-identifier static.
    /// @param flags Order flags (POST_ONLY / IOC / FOK / MARKET) forwarded to the OB.
    function postFromNote(uint256 iobHash, uint16 iobDepth, uint256 depositIdentifierHash, uint8 flags) public {
        require(msg.sender == _sellerNote, ERR_INVALID_SENDER);
        require(_sellerNote == DexLib.computeCanonicalNoteAddressFromHash(
            PRIVATE_NOTE_CODE_HASH, PRIVATE_NOTE_CODE_DEPTH, depositIdentifierHash), ERR_BAD_PARAM);
        tvm.accept();
        ensureBalance();
        _noteAuthorized = true;
        _iobHash  = iobHash;
        _iobDepth = iobDepth;
        if (_offerPosted || _funded) { return; }  // already resting / one-shot funded → no re-post
        _offerPosted = true;
        address orderBook = DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash);
        InferenceOrderBook(orderBook).placeSellOffer{value: 1 vmshell, flag: 1, bounce: false}(
            _pricePerTick, _maxTicks, flags, _sellerPubkey, _nonce, _sellerNote);
    }

    /// @notice Seller posts THIS deal's resting sell offer to the
    ///         InferenceOrderBook. Only a note-confirmed, hash-activated TC can
    ///         post — so an offer on the book always implies a live, seller-owned
    ///         TC, and a match always forwards the buyer's SHELL to a real deal
    ///         contract. At most one LIVE offer per TC: a cancel frees the latch
    ///         (`onSellClosed`) so the seller can re-list on the same TC; a fill
    ///         locks the TC to its buyer (one-shot `_funded` deal). The TC (not the
    ///         note) posts, so the IOB no longer needs its per-TC `_sellTcInUse` map.
    /// @param flags Order flags (POST_ONLY / IOC / FOK / MARKET) forwarded to the OB.
    function placeSellOffer(uint8 flags) public onlyOwnerPubkey(_sellerPubkey) accept {
        require(_noteAuthorized, ERR_NOT_INITIALIZED);
        require(_iobHash != 0, ERR_NOT_INITIALIZED);
        require(!_offerPosted, ERR_ALREADY_REGISTERED);
        require(!_funded, ERR_ALREADY_FUNDED);   // a funded TC (one-shot deal) cannot post an offer
        ensureBalance();
        _offerPosted = true;
        address orderBook = DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash);
        InferenceOrderBook(orderBook).placeSellOffer{value: 1 vmshell, flag: 1, bounce: false}(
            _pricePerTick, _maxTicks, flags, _sellerPubkey, _nonce, _sellerNote);
    }

    /// @notice The canonical InferenceOrderBook calls this when this TC's resting
    ///         sell offer is removed WITHOUT a fill (the seller cancelled it).
    ///         Clears the `_offerPosted` latch so the seller can post a fresh offer
    ///         on the SAME live TC without redeploying — a cancel does not destroy
    ///         the TC. Only clears while unfunded: a filled TC is a committed
    ///         one-shot deal (`_funded` latch), never re-offered. Guarded to the
    ///         canonical book (derived from the RootPN-supplied `_iobHash`); the
    ///         book forwards bounce:false, so a stale/foreign caller is ignored
    ///         (accept-then-noop, mirroring `fundFromOrderBook`), never reverted.
    function onSellClosed() public {
        ensureBalance();
        if (_iobHash == 0
            || msg.sender != DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash)) {
            return;
        }
        tvm.accept();
        if (_funded) { return; }        // a fill won the race → deal is live, keep the TC
        _offerPosted = false;           // offer is off the book → re-list-able
        if (_closing) {
            // Seller flagged wind-down (`close`): the offer is now provably off the
            // book (the IOB removed it before this callback), so self-destruct is
            // safe — no resting offer can outlive the TC.
            emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
            selfdestruct(_closePayout);
        }
    }

    /// @notice Seller wind-down of an UNFUNDED deal (no buyer yet). If a sell offer
    ///         is still resting, flags intent (`_closing`) and remembers the payout:
    ///         the note then cancels the offer (`cancelInferenceOrder`) and the
    ///         resulting `onSellClosed` self-destructs — the offer is provably off
    ///         the book by then, so no resting offer outlives the TC. If no offer
    ///         rests, self-destructs immediately. A FUNDED deal has the buyer's SHELL
    ///         inside and must close via `stop`/`withdrawShell`.
    function close(address payoutAddress) public onlyOwnerPubkey(_sellerPubkey) accept {
        require(!_funded, ERR_ALREADY_FUNDED);   // unfunded ⟹ not opened, not disputed
        ensureBalance();
        if (_offerPosted) {
            _closing     = true;
            _closePayout = payoutAddress;
            return;
        }
        emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
        selfdestruct(payoutAddress);
    }

    // ========================================================
    // 1b. Probe commission — seller posts SELLER_PROBE_COMMISSION (spec §3.1.2)
    // ========================================================

    /// @notice Seller posts the probe commission in SHELL before `open()`.
    ///         Held in the contract: returned to the seller on probe acceptance
    ///         or seller no-show, burned with the probe tick on a probe stop.
    ///         Sent as an internal ECC[2] message (open() is external and cannot
    ///         carry value); the buyer's note is never touched.
    function fundProbeCommission() public {
        ensureBalance();
        require(!_opened, ERR_ALREADY_OPEN);
        require(!_sellerProbeFunded, ERR_PROBE_ALREADY_FUNDED);
        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.exists(SHELL_ECC_ID), ERR_NO_SHELL);
        uint128 amount = uint128(currencies[SHELL_ECC_ID]);
        uint128 need = _probeCommission();
        require(amount >= need, ERR_INSUFFICIENT_DEPOSIT);
        tvm.accept();

        _sellerProbeLocked = need;
        _sellerProbeFunded = true;

        // Refund any excess SHELL above the required commission to the sender.
        uint128 excess = amount - need;
        if (excess > 0) { _payShell(msg.sender, excess); }

        emit ProbeCommissionFunded{dest: address.makeAddrExtern(ProbeCommissionFundedEmit, bitCntAddress)}(need);
    }

    // ========================================================
    // 2. Open — seller posts encrypted endpoint, freezes the probe tick
    // ========================================================

    /// @notice Seller-only. Posts the endpoint ciphertext (encrypted to the
    ///         buyer's pubkey), freezes ONE tick as the probe (spec §3.1.2: not
    ///         prepaid to the seller), and locks the buyer note. No platform fee
    ///         yet — it is taken by-fact when the probe is accepted (§5.1).
    function open(bytes endpointCipher) public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        require(_funded, ERR_NOT_FUNDED);
        require(!_opened, ERR_ALREADY_OPEN);
        require(_sellerProbeFunded, ERR_PROBE_NOT_FUNDED);
        require(_deposit >= _pricePerTick, ERR_INSUFFICIENT_DEPOSIT);

        _endpointCipher = endpointCipher;

        // Probe tick: frozen from the buyer's escrow, NOT prepaid to the seller.
        _prepaid       = 0;
        _frozen        = _pricePerTick;
        _deposit      -= _pricePerTick;
        _probeAccepted = false;
        _prepaidTime   = uint64(block.timestamp);
        _lastAdvance   = uint64(block.timestamp);
        _opened        = true;
        _everOpened    = true;   // permanent latch: scopes cleanupUnopened to the never-opened case

        IStreamNote(_buyer).streamLock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);

        emit StreamOpened{dest: address.makeAddrExtern(StreamOpenedEmit, bitCntAddress)}(_buyer, _pricePerTick);
    }

    // ========================================================
    // 3. Advance — accept the probe or finalize an accepted tick
    // ========================================================

    /// @notice Seller-only. After `SETTLE_WINDOW` of buyer silence:
    ///         - in `Probe`: accept the probe (§3.1.2) — finalize the probe tick
    ///           to the seller, return the commission, charge the fee by-fact,
    ///           and enter `Streaming` with the two-tick invariant (§3.2);
    ///         - in `Streaming`: finalize the prepaid tick and roll the invariant
    ///           (silence = consent, §3.3).
    function advance() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        require(_opened, ERR_NOT_OPEN);
        require(!_disputed, ERR_DISPUTED);
        // Probe phase: fixed short PROBE_WINDOW; streaming: per-deal _settleWindow.
        uint64 advanceWindow = _probeAccepted ? _settleWindow : PROBE_WINDOW;
        require(uint64(block.timestamp) >= _prepaidTime + advanceWindow, ERR_SETTLE_WINDOW_OPEN);

        uint128 fee = _fee(_pricePerTick);

        if (!_probeAccepted) {
            // Probe accepted: the probe tick (held in _frozen) → seller, fee by-fact.
            _finalizedOwed  += _frozen;
            _feeAccrued     += fee;
            _ticksFinalized += 1;
            _deposit        -= fee;
            _frozen          = 0;

            // Return the seller's probe commission (no burn — probe accepted).
            uint128 commission = _sellerProbeLocked;
            _sellerProbeLocked = 0;
            _finalizedOwed    += commission;

            _probeAccepted = true;

            // Establish the two-tick invariant from the remaining deposit:
            // prepay the next tick forward + freeze a buffer, as the deposit allows.
            // Reserve the prepaid tick's by-fact fee: prepay only when the deposit
            // covers the tick value PLUS its later finalize fee, so a deposit in
            // [P, P+fee) does not prepay a tick it can't finalize.
            if (_deposit >= _pricePerTick + fee) {
                _prepaid  = _pricePerTick;
                _deposit -= _pricePerTick;
            } else {
                _prepaid = 0;
            }
            // Buffer reserves 2*fee: once BOTH ticks are live (_prepaid + _frozen) TWO
            // by-fact fees are still owed from _deposit, so the buffer guard leaves one
            // fee per live tick (e.g. paid = 3P+2f freezes the buffer).
            if (_deposit >= _pricePerTick + 2 * fee) {
                _frozen   = _pricePerTick;
                _deposit -= _pricePerTick;
            } else {
                _frozen = 0;
            }
            _prepaidTime = uint64(block.timestamp);
            _lastAdvance = uint64(block.timestamp);

            emit ProbeAccepted{dest: address.makeAddrExtern(ProbeAcceptedEmit, bitCntAddress)}(_buyer, _finalizedOwed, commission);
            return;
        }

        // Streaming: finalize the delivered tick (P → seller, fee by-fact, §5.1).
        // A stream that exhausted its deposit rolls to _prepaid == 0 (no delivered
        // tick). Only a live prepaid tick can be finalized, so advance requires
        // _prepaid > 0 and rejects otherwise (nothing to advance).
        require(_prepaid > 0, ERR_NO_PREPAID_TICK);
        _finalizedOwed  += _prepaid;
        _feeAccrued     += fee;
        _ticksFinalized += 1;
        _deposit        -= fee;

        _prepaid     = _frozen;
        _prepaidTime = uint64(block.timestamp);

        // Refill the buffer only if the deposit covers the buffer tick's value AND the
        // by-fact fees still owed by BOTH now-live ticks: the rolled _prepaid (old
        // buffer) and this refilled _frozen each finalize later, so reserve one fee per
        // live tick (2*fee total).
        if (_deposit >= _pricePerTick + 2 * fee) {
            _frozen   = _pricePerTick;
            _deposit -= _pricePerTick;
        } else {
            _frozen = 0;
        }
        _lastAdvance = uint64(block.timestamp);

        emit TickFinalized{dest: address.makeAddrExtern(TickFinalizedEmit, bitCntAddress)}(_finalizedOwed, _deposit);
    }

    // ========================================================
    // 4. Shared streaming close (spec §4.1 under the optimistic §3.3 rule)
    // ========================================================

    /// @notice Window-gated close of the streaming phase, shared by `stop()` and
    ///         `resolveDisputeTimeout()` so the split is IDENTICAL on every streaming close
    ///         (directive 92). The current prepaid tick is finalized to the seller ONLY if its
    ///         acceptance window (`_settleWindow`) has elapsed (silence = consent, §3.3); otherwise it
    ///         is not accepted and refunds to the buyer (no fee charged for it). On the dispute-timeout
    ///         path this prevents an overpay when `_settleWindow > DISPUTE_WINDOW` (the timeout can fire
    ///         before the window elapses). Updates the finalized/fee/tick counters and zeroes the escrow
    ///         buckets; the caller does its own `_settleFees(clean)` + unlock + payout.
    function _settleStreamingClose() private returns (uint128 toSeller, uint128 refundB) {
        bool tickAccepted = _prepaid > 0 && uint64(block.timestamp) >= _prepaidTime + _settleWindow;
        if (tickAccepted) {
            uint128 fee = _fee(_pricePerTick);
            _finalizedOwed  += _prepaid;
            _feeAccrued     += fee;
            _ticksFinalized += 1;
            _deposit        -= fee;            // fee by-fact, only for the kept tick (§5.1)
            toSeller = _prepaid;
            refundB  = _frozen + _deposit;
        } else {
            // Window still open (or nothing prepaid) → buyer keeps the unaccepted tick.
            toSeller = 0;
            refundB  = _prepaid + _frozen + _deposit;
        }
        _prepaid = 0; _frozen = 0; _deposit = 0;
    }

    // ========================================================
    // 4a. Stop — buyer exit (probe burn §3.1.2 / standard split §4.1)
    // ========================================================

    function stop() public {
        ensureBalance();
        require(_opened, ERR_NOT_OPEN);
        require(msg.sender == _buyer, ERR_NOT_BUYER);
        require(!_disputed, ERR_DISPUTED);
        tvm.accept();

        if (!_probeAccepted) {
            // Stop on the probe (§3.1.2): BURN BOTH the buyer's probe tick and the
            // seller's commission — nothing to either side. Remaining deposit (no
            // fee was charged on the probe) refunds the buyer.
            uint128 burnedProbe      = _frozen;
            uint128 burnedCommission = _sellerProbeLocked;
            uint128 refund           = _deposit;
            _frozen = 0; _sellerProbeLocked = 0; _deposit = 0;
            _opened = false;

            _burnShell(burnedProbe);
            _burnShell(burnedCommission);

            IStreamNote(_buyer).streamUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
            _payShell(_buyer, refund);

            emit ProbeBurned{dest: address.makeAddrExtern(ProbeBurnedEmit, bitCntAddress)}(_buyer, burnedProbe, burnedCommission, refund);
            return;
        }

        // Standard split (§4.1) under the optimistic rule (§3.3) — the shared close window-gates the
        // current tick (same gate as advance()) and is reused by resolveDisputeTimeout (directive 92).
        (uint128 toSeller, uint128 refundB) = _settleStreamingClose();
        _opened = false;

        _settleFees(true);   // clean amicable close → rebate to seller, burn net (§5.3/§5.4)

        IStreamNote(_buyer).streamUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
        _payShell(_buyer, refundB);

        emit StreamStopped{dest: address.makeAddrExtern(StreamStoppedEmit, bitCntAddress)}(_buyer, toSeller, refundB);
    }

    // ========================================================
    // 4b. Dispute — symmetric, both notes lock (spec §4.2)
    // ========================================================

    function dispute() public {
        ensureBalance();
        require(_opened, ERR_NOT_OPEN);
        require(msg.sender == _buyer, ERR_NOT_BUYER);
        require(!_disputed, ERR_DISPUTED);
        tvm.accept();

        _disputed     = true;
        _everDisputed = true;   // a dispute ever opened → seller forfeits rebate (§5.3)
        _disputeTime  = uint64(block.timestamp);

        // Lock only the buyer note. The seller's deal collateral (probe
        // commission) is held in THIS TC, not on the seller note, so the seller
        // note has nothing at risk to freeze. `_sellerNote` is a raw, unverified
        // ctor arg, so it is never locked; only `_buyer` — bound by
        // fund()/fundFromOrderBook — is locked here.
        IStreamNote(_buyer).streamDisputeLock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);

        emit StreamDisputed{dest: address.makeAddrExtern(StreamDisputedEmit, bitCntAddress)}(_buyer, _disputeTime);
    }

    /// @notice Seller concedes. In `Probe`: the probe tick goes back to the buyer
    ///         and the commission is returned to the seller (a seller concession
    ///         is not a buyer stop → no burn, §3.1.2). In `Streaming`: contested
    ///         ticks + deposit refund to the buyer.
    function releaseDispute() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        require(_disputed, ERR_NOT_DISPUTED);

        if (!_probeAccepted) {
            uint128 commission = _sellerProbeLocked;
            _sellerProbeLocked = 0;
            _finalizedOwed    += commission;          // commission returned to seller

            uint128 refund = _frozen + _deposit;      // probe tick back to the buyer
            _frozen = 0; _deposit = 0;
            _disputed = false; _opened = false;

            _settleFees(false);   // no fee accrued on the probe; clears state safely

            _unlockBoth();
            _payShell(_buyer, refund);

            emit DisputeResolved{dest: address.makeAddrExtern(DisputeResolvedEmit, bitCntAddress)}(0, refund, true);
            return;
        }

        uint128 refundB = _prepaid + _frozen + _deposit;
        _prepaid = 0; _frozen = 0; _deposit = 0;
        _disputed = false; _opened = false;

        _settleFees(false);   // disputed → no rebate, burn accrued fees (§5.3)

        _unlockBoth();
        _payShell(_buyer, refundB);

        emit DisputeResolved{dest: address.makeAddrExtern(DisputeResolvedEmit, bitCntAddress)}(0, refundB, true);
    }

    /// @notice Anyone, after `DISPUTE_WINDOW`. In `Probe`: reduces to the probe
    ///         rule — BURN BOTH (§3.1.2/§4.2), an unaccepted probe has no value
    ///         to either side. In `Streaming`: fall back to the standard split.
    function resolveDisputeTimeout() public {
        ensureBalance();
        require(_disputed, ERR_NOT_DISPUTED);
        require(uint64(block.timestamp) >= _disputeTime + DISPUTE_WINDOW, ERR_DISPUTE_WINDOW_OPEN);
        tvm.accept();

        if (!_probeAccepted) {
            // Probe rule: burn the probe tick and the commission; remaining
            // deposit refunds the buyer. No tick finalized, no fee.
            uint128 burnedProbe      = _frozen;
            uint128 burnedCommission = _sellerProbeLocked;
            uint128 refund           = _deposit;
            _frozen = 0; _sellerProbeLocked = 0; _deposit = 0;
            _disputed = false; _opened = false;

            _burnShell(burnedProbe);
            _burnShell(burnedCommission);

            _unlockBoth();
            _payShell(_buyer, refund);

            emit ProbeBurned{dest: address.makeAddrExtern(ProbeBurnedEmit, bitCntAddress)}(_buyer, burnedProbe, burnedCommission, refund);
            return;
        }

        // Standard split — the SAME window-gated streaming close as stop() (directive 92): the disputed
        // tick goes to the seller ONLY if its acceptance window has elapsed by the timeout, else it
        // refunds to the buyer (no overpay when _settleWindow > DISPUTE_WINDOW). Disputed → no rebate.
        (uint128 toSeller, uint128 refundB) = _settleStreamingClose();
        _disputed = false; _opened = false;

        _settleFees(false);   // disputed → no rebate, burn net (§5.3)

        _unlockBoth();
        _payShell(_buyer, refundB);

        emit DisputeResolved{dest: address.makeAddrExtern(DisputeResolvedEmit, bitCntAddress)}(toSeller, refundB, false);
    }

    function _unlockBoth() private view {
        // Only the buyer note is locked now (see dispute()), so only it is unlocked.
        IStreamNote(_buyer).streamDisputeUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
        IStreamNote(_buyer).streamUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
    }

    // ========================================================
    // 4c. Reclaim on seller no-show (spec §3.4 / §3.1.2)
    // ========================================================

    function reclaimOnTimeout() public {
        ensureBalance();
        require(_opened, ERR_NOT_OPEN);
        require(msg.sender == _buyer, ERR_NOT_BUYER);
        require(!_disputed, ERR_DISPUTED);
        require(uint64(block.timestamp) >= _lastAdvance + _streamTimeout, ERR_STREAM_TIMEOUT_OPEN);
        tvm.accept();

        if (!_probeAccepted) {
            // Seller no-show on the probe (§3.1.2/§3.4): the buyer reclaims the
            // probe tick in full (pays nothing), the commission is returned to
            // the seller. NO burn — a no-show is not slashed (§9.1).
            uint128 commission = _sellerProbeLocked;
            _sellerProbeLocked = 0;
            _finalizedOwed    += commission;

            uint128 refund = _frozen + _deposit;
            _frozen = 0; _deposit = 0;
            _opened = false;

            IStreamNote(_buyer).streamUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
            _payShell(_buyer, refund);

            emit StreamReclaimed{dest: address.makeAddrExtern(StreamReclaimedEmit, bitCntAddress)}(_buyer, refund);
            return;
        }

        // Seller delivered the prepaid tick before vanishing → P + fee finalized;
        // buyer reclaims the buffer + remaining deposit. No rebate (abandoned).
        // Guard _prepaid>0: only a delivered (prepaid) tick is finalized here; a
        // stream that exhausted its deposit has none, so it is skipped. The buyer
        // must still exit on timeout, so this path never reverts.
        if (_prepaid > 0) {
            uint128 fee = _fee(_pricePerTick);
            _finalizedOwed  += _prepaid;
            _feeAccrued     += fee;
            _ticksFinalized += 1;
            _deposit        -= fee;
        }

        uint128 refundB = _frozen + _deposit;
        _prepaid = 0; _frozen = 0; _deposit = 0;
        _opened  = false;

        _settleFees(false);   // seller abandoned → no rebate, burn net

        IStreamNote(_buyer).streamUnlock{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(_sellerPubkey, _nonce);
        _payShell(_buyer, refundB);

        emit StreamReclaimed{dest: address.makeAddrExtern(StreamReclaimedEmit, bitCntAddress)}(_buyer, refundB);
    }

    // ========================================================
    // 4d. Cleanup — seller funded-but-never-opened (no-show, spec §2.1)
    // ========================================================

    /// @notice Anyone, after `MATCH_OPEN_TIMEOUT` with no open(): refund the
    ///         buyer's full deposit and return any posted probe commission to
    ///         the seller (nothing delivered → no fee, no penalty, §2.1), then
    ///         self-destruct the dead deal.
    /// @dev    Permissionless (no-show recovery), so the payout is NOT caller-chosen: the buyer's
    ///         deposit + seller commission (ECC SHELL) are refunded to their fixed notes FIRST, then
    ///         the residual native gas is swept to the canonical SuperRoot (a fixed protocol sink),
    ///         never to an arbitrary caller-supplied address.
    function cleanupUnopened() public {
        ensureBalance();
        require(_funded, ERR_NOT_FUNDED);
        require(!_opened, ERR_ALREADY_OPEN);
        // Never-opened only: after a real open+close the earnings live in
        // `_finalizedOwed` for withdrawShell, so this permissionless recovery is scoped
        // to the never-opened case. A funded deal that genuinely never opened has
        // _everOpened=false.
        require(!_everOpened, ERR_ALREADY_OPEN);
        require(uint64(block.timestamp) >= _fundedTime + MATCH_OPEN_TIMEOUT, ERR_STREAM_TIMEOUT_OPEN);
        tvm.accept();

        uint128 refund     = _deposit;
        uint128 commission = _sellerProbeLocked;
        _deposit = 0; _sellerProbeLocked = 0; _funded = false; _sellerProbeFunded = false;

        _payShell(_buyer, refund);
        _payShell(_sellerNote, commission);   // return the seller's probe commission

        emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
        selfdestruct(address.makeAddrStd(0, SUPER_ROOT_ADDR));   // residual native → fixed SuperRoot, not caller
    }

    // ========================================================
    // 5. Seller withdraw + destroy
    // ========================================================

    function withdrawShell(uint128 amount, address recipient) public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        require(amount > 0, ERR_ZERO_AMOUNT);
        require(amount <= _finalizedOwed, ERR_INSUFFICIENT_TOKENS);
        uint128 balance = uint128(address(this).currencies[SHELL_ECC_ID]);
        require(amount <= balance, ERR_INSUFFICIENT_TOKENS);

        _finalizedOwed -= amount;
        _payShell(recipient, amount);

        emit ShellWithdrawn{dest: address.makeAddrExtern(ShellWithdrawnEmit, bitCntAddress)}(recipient, amount);

        // Auto-cleanup on the funded happy path: a funded deal whose stream has
        // closed (`!_opened`, buyer already refunded by `stop`) and whose seller
        // just drained the last of `_finalizedOwed` has nothing left to do → sweep
        // the residual to the recipient. A funded TC's sell offer was consumed on
        // the fill (removed from the book), so no resting offer remains and this is
        // race-free.
        if (_funded && !_opened && !_disputed && _finalizedOwed == 0 && !_offerPosted) {
            emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
            selfdestruct(recipient);
        }
    }

    function destroy(address payoutAddress) public onlyOwnerPubkey(_sellerPubkey) accept {
        require(!_opened, ERR_STILL_OPEN);
        require(!_disputed, ERR_DISPUTED);
        // Emergency manual close. Blocked while a LIVE sell offer still rests on the
        // book, so the TC is never destroyed out from under a resting offer that a
        // later match could fund. `_offerPosted` tracks the REAL offer state (cleared
        // on match-fill in _recordFunding), not the `_funded` proxy, so this single
        // check is exact. Cancel the offer first (via the note, or use `close`).
        require(!_offerPosted, ERR_OFFER_LIVE);
        // Never selfdestruct over a live buyer deposit: a matched-but-unopened deal
        // (_funded && !_opened) still holds the buyer's escrowed SHELL, which selfdestruct would
        // sweep to the seller-chosen payoutAddress. Refund the buyer (and return the seller's probe
        // commission) first, mirroring cleanupUnopened, so the sweep only takes residual native gas.
        if (_funded) {
            uint128 refund     = _deposit;
            uint128 commission = _sellerProbeLocked;
            _deposit = 0; _sellerProbeLocked = 0; _funded = false; _sellerProbeFunded = false;
            _payShell(_buyer, refund);
            _payShell(_sellerNote, commission);
        }
        emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
        selfdestruct(payoutAddress);
    }

    // ========================================================
    // Getters
    // ========================================================

    function getState() external view returns (
        bool funded, bool opened, bool probeAccepted, bool disputed,
        uint128 deposit, uint128 prepaid, uint128 frozen, uint128 finalizedOwed,
        uint64 prepaidTime, uint64 lastAdvance, uint64 disputeTime, uint64 fundedTime
    ) {
        return (_funded, _opened, _probeAccepted, _disputed, _deposit, _prepaid, _frozen,
                _finalizedOwed, _prepaidTime, _lastAdvance, _disputeTime, _fundedTime);
    }

    /// @notice Offer state: `offerPosted` = the TC has a live resting sell offer on
    ///         the order book right now; `closing` = a seller wind-down is in progress
    ///         (the offer is being cancelled before self-destruct). Lets a client read
    ///         whether this TC is actively selling.
    function getOffer() external view returns (bool offerPosted, bool closing) {
        return (_offerPosted, _closing);
    }

    /// @notice Probe state (spec §3.1.2): whether the seller posted the
    ///         commission and the SHELL amount currently locked as it.
    function getProbe() external view returns (bool probeFunded, uint128 probeLocked, uint128 probeCommission) {
        return (_sellerProbeFunded, _sellerProbeLocked, _probeCommission());
    }

    function getConfig() external view returns (
        uint16 platformFeeBps, uint64 settleWindow, uint64 streamTimeout, uint64 disputeWindow
    ) {
        return (PLATFORM_FEE_BPS, _settleWindow, _streamTimeout, DISPUTE_WINDOW);
    }

    /// @notice Fee state (spec §5): accrued fee, finalized-tick count (rebate n),
    ///         whether a dispute ever opened, and the rebate config.
    function getFees() external view returns (
        uint128 feeAccrued, uint128 ticksFinalized, bool everDisputed,
        uint16 rebateMaxBps, uint16 rebateSlopeBps
    ) {
        return (_feeAccrued, _ticksFinalized, _everDisputed, REBATE_MAX_BPS, REBATE_SLOPE_BPS);
    }

    function getDeal() external view returns (uint128 tickSize, uint128 pricePerTick, uint128 maxTicks) {
        return (TICK_SIZE, _pricePerTick, _maxTicks);
    }

    function getParties() external view returns (address buyer, address sellerNote) {
        return (_buyer, _sellerNote);
    }

    /// @notice Buyer note pubkey recorded at the match (spec §3.1.1): the gateway
    ///         verifies the buyer's challenge signature against this.
    function getBuyerPubkey() external view returns (uint256) { return _buyerPubkey; }

    function getEndpointCipher() external view returns (bytes) { return _endpointCipher; }

    function getModelName() external view returns (string) { return _modelName; }
    function getModelHash() external view returns (uint256) { return _modelHash; }

    function getShellBalance() external view returns (uint128) {
        return uint128(address(this).currencies[SHELL_ECC_ID]);
    }

    function getSeller() external view returns (uint256 sellerPubkey, address rootModelAddress, uint64 nonce) {
        return (_sellerPubkey, _rootModelAddress, _nonce);
    }

    function getVersion() external pure returns (string, string) {
        return (version, "TokenContract");
    }
}
