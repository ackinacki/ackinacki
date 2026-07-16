pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./interfaces.sol";
// Deploy guard only: the real PrivateNote type gives the stateInit data layout so
// we can recompute a note's address from its pinned CODE HASH.
import "../dex/PrivateNote.sol";
// Sell-offer guard: the TokenContract type gives the stateInit data layout so we can
// recompute a deal contract's address from its pinned CODE HASH (placeSellOffer).
import "./TokenContract.sol";
// Sell-offer guard (cont.): the RootModel type gives the stateInit data layout so we can
// recompute the seller's RootModel address (the TC's `_rootModelAddress`) from its pinned
// CODE HASH + the canonical SuperRoot — see `_rootModelAddr` / `_tokenContractAddr`.
import "./RootModel.sol";

/// @notice Handover: the matched seller's `token_contract` receives the deal's
///         SHELL and binds the buyer note (spec §2.3).
interface ITokenContractDeal {
    function fundFromOrderBook(address buyerNote, uint256 buyerPubkey) external;
    // Called when the TC's resting sell offer is cancelled (removed WITHOUT a
    // fill) so the TC can clear its `_offerPosted` latch and re-list.
    function onSellClosed() external;
}

/// @notice Async pull sink (spec §6.2): the weekly median is handed back here.
interface IWeeklyMedianSink {
    function onWeeklyMedian(uint256 eventId, uint256 oracleListHash, uint32 tokenType, uint256 price) external;
}

/// @notice Owner-facing confirmation mirrors pushed into a PrivateNote so the order owner can read
///         just its own note's ext-out and learn the deal `tokenContract`. The note authenticates the
///         caller as the canonical book for `_modelHash` (pinned IOB code).
interface IPrivateNote {
    function onInferencePlaced(uint256 modelHash, address tokenContract, uint128 orderId, bool isBuy, uint256 price, uint128 ticks) external;
    function onInferenceFilled(uint256 modelHash, address tokenContract, uint128 orderId, uint128 ticks, uint256 clearingPrice, bool isBuy) external;
}

/// @title InferenceOrderBook (spec §2 + §8) — full price→time CLOB with a queued,
///        resumable matching engine (ported from dex/OrderBook.sol).
/// @notice Both sides rest (SELL offer = one-deal slot; BUY = §8.1 limit/subscription
///         with SHELL budget held here). Order entry goes through a circular QUEUE:
///         each op is enqueued and `processHead` drains it one-at-a-time, self-calling
///         across txs. Matching that crosses more than `MAX_MATCHES_PER_CALL` resting
///         orders YIELDS a continuation cursor on the head entry and resumes next tx —
///         so large orders fully take and FOK never partial-fills. The queue serializes
///         all ops, so liquidity can't shift under an in-flight taker.
///
///         On each fill the matched SHELL (trade·(clearing+fee)) is forwarded to the
///         SELL side's token_contract with the BUY side's note bound (§2.3) → the deal
///         streams off-book (§3). The book also keeps a daily-VWAP / weekly-median
///         reference price (§6/§7).
///
///         Dropped vs dex OrderBook (irrelevant to inference): outcomes, epochs,
///         quote/base + collateral, event-resolution shutdown, and PN callbacks
///         (inference order entry is fire-and-forget; the note tracks via getters).
contract InferenceOrderBook is AiRegistryModifiers {
    string constant version = "4.0.27";

    // ⚠ Re-pin whenever dex/PrivateNote is recompiled (note↔OB layout coupling:
    //   the note bakes this book's state layout via `new InferenceOrderBook`, so any
    //   OB layout change forces a note rebuild → new note hash → re-pin → OB rebuild).
    uint256 constant NOTE_CODE_HASH  = 0x5f78299c6438d3e156042ef1ed9fcd70064a3f34221b2c051c99567c9f21ef2e;
    uint16  constant NOTE_CODE_DEPTH = 19;

    // Canonical inference TokenContract (deal contract) code. placeSellOffer verifies
    // the sell offer's `tokenContract` derives from this pinned code + the seller's
    // statics — else a fill would route the BUYER's SHELL to a fake (the IOB is the
    // contract that forwards SHELL on a fill, so the check must live HERE, not only in
    // the note: placeSellOffer is public and a direct call would bypass a note check).
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xa2c32147ed9bedec588e81ad2f55300e0640635428254b710964f38331c84f45;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 10;

    // Canonical RootModel code. The seller's per-deal TokenContract is bound to its RootModel
    // (its `_rootModelAddress` static is the seller's RootModel, NOT address(0)). To verify a
    // TokenContract the IOB first recomputes the seller's RootModel address from this pinned code
    // hash + the canonical SuperRoot, then derives the TC address from it (see _tokenContractAddr).
    // Re-pin whenever airegistry/RootModel is recompiled.
    uint256 constant ROOT_MODEL_CODE_HASH  = 0x0a6fe90e89faa99bdd4286965ec75e5085d7c0f365b8c2e3e1467cf584d359bc;
    uint16  constant ROOT_MODEL_CODE_DEPTH = 8;

    // Canonical AI SuperRoot account id (workchain 0). Every RootModel registers under it via its
    // `_superRootAddress` static, so it is the anchor for the RootModel-address derivation. Must
    // match the live SuperRoot the sellers' RootModels were deployed under; re-pin if the SuperRoot
    // is redeployed at a different address.
    // FIXED SuperRoot at the vanity 0:0c0c… address on LOCAL, SHELLNET and MAINNET — the zerostate
    // force-places the SuperRoot here (removed from PremineAddresses), and the address is stable
    // across contract changes and versions. shellnet == local, no per-version / code-derived
    // rotation. (See dexdo-specs/shellnet-update.md.)
    uint256 constant SUPER_ROOT_ADDR = 0x0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c;

    // Local errors (NOT in shared AiRegistryErrors — avoids rippling RootModel/TC/SuperRoot pins).
    uint16 constant ERR_NOT_DEPLOYER_NOTE = 333;
    uint16 constant ERR_NO_LIQUIDITY      = 334;
    uint16 constant ERR_BAD_FLAGS         = 335;
    uint16 constant ERR_EXPIRED           = 336;
    uint16 constant ERR_FOK_UNFILLED      = 337;
    uint16 constant ERR_NOT_SUB           = 338;
    uint16 constant ERR_NOTHING_TO_CLAIM  = 339;
    uint16 constant ERR_QUEUE_FULL        = 340;
    uint16 constant ERR_NOT_SELF          = 341;
    uint16 constant ERR_BAD_TOKEN_CONTRACT = 342;
    uint16 constant ERR_NAME_TOO_LONG      = 343;
    uint16 constant ERR_BAD_MODEL_NAME     = 344;

    // Order-type flags (spec §8 / dex parity).
    uint8 constant FLAG_IOC       = 0x01;
    uint8 constant FLAG_FOK       = 0x02;
    uint8 constant FLAG_MARKET    = 0x04;
    uint8 constant FLAG_POST_ONLY = 0x08;
    uint8 constant TAKER_FLAGS    = 0x07;

    // Gas / walk caps.
    uint8  constant MAX_MATCHES_PER_CALL = 30;
    uint8  constant MAX_CANCEL_PER_CALL  = 30;
    uint8  constant MAX_PRECHECK_LEVELS  = 40;

    // Queue (circular).
    uint8 constant QENTRY_PLACE      = 1;
    uint8 constant QENTRY_CANCEL     = 2;
    uint8 constant QENTRY_CANCEL_ALL = 3;
    uint8 constant QUEUE_CAPACITY    = 100;
    uint8 constant QUEUE_PLACE_LIMIT = 90;

    // §8 subscription.
    uint64 constant SUB_PERIOD    = 2_419_200;   // ≈ month (4 weeks)
    uint8  constant SUB_CYCLES    = 4;
    uint64 constant SUB_CYCLE_LEN = 604_800;     // 1 week

    // Reference-price stats.
    uint64  constant SECS_PER_DAY  = 86400;
    uint128 constant MIN_LIQUIDITY = 1;

    // Static — address derivation: one book per model.
    uint256 static _modelHash;

    // On-chain authoritative model id `producer--model--version`: the ctor requires
    // `sha256(_modelName) == _modelHash`, so this string is the genuine preimage of the
    // address-defining hash (split on `--` for the three fields). Capped at one cell (<=127 B).
    string _modelName;

    // ── Order book ──
    struct Order {
        address note;           // owner note; cancel auth + handover
        uint256 buyerPubkey;    // BUY: buyer note pubkey (gateway auth, §3.1.1); SELL: 0
        address tokenContract;  // SELL: seller's deal contract; BUY: 0
        uint256 price;          // price per tick P
        uint128 amount;         // remaining ticks
        uint128 initialAmount;
        uint128 filledAccum;
        uint128 escrow;         // BUY: SHELL budget held; SELL: 0
        uint64  deadline;       // BUY GTD (0 = GTC)
        uint64  ts;
        uint8   flags;
        bool    isBuy;
        uint128 nextAtPrice;
        uint128 prevAtPrice;
        uint128 nextInOwner;
        uint128 prevInOwner;
    }
    mapping(uint128 => Order) _orders;
    uint128 _nextOrderId;
    uint128 _orderCount;

    // One resting SELL per deal TokenContract is now enforced by the TC itself
    // (`TokenContract._offerPosted`), because the TC posts its own offer. A TC is a
    // single one-shot deal, so it posts at most once (re-listing needs a new TC).

    struct PriceLevel { uint128 firstOrderId; uint128 lastOrderId; uint128 totalAmount; }
    mapping(bool => mapping(uint256 => PriceLevel)) _levels;   // isBuy → price → level

    mapping(address => uint128) _ownerHead;
    mapping(address => uint128) _ownerTail;

    uint128 _executedNotional;
    uint128 _executedTicks;
    uint64  _matchSeq;

    // ── Queue ──
    struct QueueEntry {
        uint8   entryType;
        address owner;          // the note (msg.sender at entry)
        uint256 buyerPubkey;    // BUY: buyer note pubkey (§3.1.1); else 0
        bool    isBuy;
        uint8   flags;
        uint256 price;          // stored price (market: 0 sell / max buy)
        uint128 amount;         // original ticks
        uint128 escrow;         // BUY budget received; SELL 0
        address tokenContract;  // SELL TC
        uint64  deadline;
        uint128 targetOrderId;  // CANCEL target
        // Match continuation cursor (taker crossing > one tx of liquidity):
        uint128 contOrderId;    // assigned on first run; 0 = not started
        uint128 contRemaining;
        uint128 contLeftover;
        uint128 contScanOrder;  // resting maker to resume the scan from (0 = from best)
    }
    mapping(uint8 => QueueEntry) _queue;
    uint8 _queueHead;
    uint8 _queueTail;
    uint8 _queueSize;

    // ── Reference-price stats ──
    struct DayData { uint64 day; uint256 priceVolSum; uint256 volSum; }
    mapping(uint8 => DayData) _daily;
    uint8 _curSlot;

    // ── §8 subscription state (keyed by resting BUY order id) ──
    struct Sub {
        uint64  periodStart;
        uint8   curCycle;
        uint128 cycleBudget;
        uint128 cycleSpent;
        bool    autoRenew;
        bool    exists;
    }
    mapping(uint128 => Sub) _subs;
    mapping(uint128 => mapping(uint8 => uint128)) _forfeitPool;
    mapping(uint128 => mapping(uint8 => uint128)) _cycleFundedTicks;
    mapping(uint128 => mapping(uint8 => mapping(address => uint128))) _cycleSellerTicks;

    address _deployerNote;

    event InferenceOrderPlaced(uint128 orderId, bool isBuy, uint256 price, uint128 ticks, address note, address tokenContract, uint64 deadline);
    event InferenceOrderCancelled(uint128 orderId, uint128 refunded, address note);
    event InferenceFilled(uint128 makerId, uint128 takerId, uint128 ticks, uint256 clearingPrice, address sellerTC, address buyerNote, address sellerNote);
    event InferenceExecuted(uint128 ticks, uint256 clearingPrice, uint128 cost);
    event InferenceRefunded(address note, uint128 amount);
    event InferenceSubscriptionPlaced(uint128 orderId, address buyerNote, uint128 maxPrice, uint128 ticks, uint128 cycleBudget, bool autoRenew);
    event InferenceCycleForfeited(uint128 orderId, uint8 cycle, uint128 forfeited, uint128 fundedTicks);
    event InferenceForfeitClaimed(uint128 orderId, uint8 cycle, address sellerNote, uint128 amount);
    /// @notice Emitted once at book deploy — lets an indexer map `modelHash` → the verified model name
    ///         (and which note opened the market). `modelName` is the genuine sha256 preimage.
    event InferenceOrderBookDeployed(address note, uint256 modelHash, string modelName);

    // ========================================================
    // Deploy guard (spec §2/§8)
    // ========================================================

    function _noteAddrFromHash(uint256 depositHash) private returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            contr: PrivateNote, code: dummyCode,
            varInit: { _depositIdentifierHash: depositHash }
        });
        TvmSlice s = si.toSlice();
        s.skip(5);
        s.loadRef();
        TvmCell dataCell = s.loadRef();
        return address.makeAddrStd(
            0, abi.stateInitHash(NOTE_CODE_HASH, tvm.hash(dataCell), NOTE_CODE_DEPTH, dataCell.depth()));
    }

    /// @notice Deterministic RootModel address for `ownerPubkey` from its pinned code hash/depth +
    ///         the canonical SuperRoot. Mirrors `SuperRoot._calculateRootModelAddress` (varInit
    ///         `{_ownerPubkey, _superRootAddress}`, tvm pubkey = ownerPubkey). The seller's RootModel
    ///         owner pubkey IS the seller pubkey (one RootModel per seller key).
    function _rootModelAddr(uint256 ownerPubkey) private returns (address) {
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            code: dummyCode, contr: RootModel, pubkey: ownerPubkey,
            varInit: {
                _ownerPubkey: ownerPubkey,
                _superRootAddress: address.makeAddrStd(0, SUPER_ROOT_ADDR)
            }
        });
        TvmSlice s = si.toSlice();
        s.skip(5);
        s.loadRef();
        TvmCell dataCell = s.loadRef();
        return address.makeAddrStd(
            0, abi.stateInitHash(ROOT_MODEL_CODE_HASH, tvm.hash(dataCell), ROOT_MODEL_CODE_DEPTH, dataCell.depth()));
    }

    /// @notice Deterministic inference TokenContract address from its pinned code hash/depth + the
    ///         seller's statics. `_rootModelAddress` is the seller's REAL RootModel (derived via
    ///         `_rootModelAddr`), matching `RootModel._calculateTokenContractAddress` and the
    ///         on-chain deploy — NOT address(0). A fake/foreign deal contract can't pass the
    ///         placeSellOffer check, so a fill never routes the buyer's SHELL off a canonical TC.
    function _tokenContractAddr(uint256 sellerPubkey, uint64 nonce) private returns (address) {
        address rootModel = _rootModelAddr(sellerPubkey);
        TvmCell dummyCode;
        TvmCell si = abi.encodeStateInit({
            code: dummyCode, contr: TokenContract, pubkey: sellerPubkey,
            varInit: { _sellerPubkey: sellerPubkey, _rootModelAddress: rootModel, _nonce: nonce }
        });
        TvmSlice s = si.toSlice();
        s.skip(5);
        s.loadRef();
        TvmCell dataCell = s.loadRef();
        return address.makeAddrStd(
            0, abi.stateInitHash(TOKEN_CONTRACT_CODE_HASH, tvm.hash(dataCell), TOKEN_CONTRACT_CODE_DEPTH, dataCell.depth()));
    }

    constructor(uint256 depositHash, string modelName) {
        require(msg.sender == _noteAddrFromHash(depositHash), ERR_NOT_DEPLOYER_NOTE);
        // On-chain authoritative model name: it must be the preimage of the address-defining
        // modelHash. `sha256`/SHA256U hashes a single cell, so the id must fit one (<=127 bytes) —
        // a longer name would hash only its first cell and never match `_modelHash`.
        require(modelName.byteLength() <= 127, ERR_NAME_TOO_LONG);
        require(sha256(modelName) == _modelHash, ERR_BAD_MODEL_NAME);
        tvm.accept();
        _deployerNote = msg.sender;
        _modelName = modelName;
        emit InferenceOrderBookDeployed{dest: address.makeAddrExtern(InferenceOBDeployedEmit, bitCntAddress)}(
            msg.sender, _modelHash, modelName);
        _nextOrderId = 1;
        ensureBalance();
    }

    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    function _payShell(address to, uint128 amount) private pure {
        if (amount == 0) { return; }
        mapping(uint32 => varuint32) ecc;
        ecc[SHELL_ECC_ID] = varuint32(amount);
        to.transfer({value: 1 vmshell, bounce: false, flag: 1, currencies: ecc});
    }

    function _tickFee(uint256 p) private pure returns (uint256) {
        return (p * uint256(PLATFORM_FEE_BPS)) / uint256(BPS_DENOMINATOR);
    }
    function _unit(uint256 p) private pure returns (uint256) { return p + _tickFee(p); }

    // ========================================================
    // Level navigation
    // ========================================================

    function _bestOpposite(bool takerIsBuy) private view returns (optional(uint256, PriceLevel)) {
        return takerIsBuy ? _levels[false].min() : _levels[true].max();
    }
    function _nextOpposite(bool takerIsBuy, uint256 cur) private view returns (optional(uint256, PriceLevel)) {
        return takerIsBuy ? _levels[false].next(cur) : _levels[true].prev(cur);
    }
    function _crosses(bool takerIsBuy, uint256 takerPrice, bool isMarket, uint256 makerPrice) private pure returns (bool) {
        if (isMarket) { return true; }
        return takerIsBuy ? (takerPrice >= makerPrice) : (makerPrice >= takerPrice);
    }

    // ========================================================
    // Book mutation
    // ========================================================

    function _insertIntoBook(uint128 orderId, Order o) private {
        uint128 oTail = _ownerTail[o.note];
        PriceLevel level = _levels[o.isBuy][o.price];
        uint128 priceTail = level.lastOrderId;

        o.nextAtPrice = 0; o.prevAtPrice = priceTail; o.nextInOwner = 0; o.prevInOwner = oTail;
        _orders[orderId] = o;

        if (priceTail == 0) {
            _levels[o.isBuy][o.price] = PriceLevel({firstOrderId: orderId, lastOrderId: orderId, totalAmount: o.amount});
        } else {
            _orders[priceTail].nextAtPrice = orderId;
            level.lastOrderId = orderId;
            level.totalAmount += o.amount;
            _levels[o.isBuy][o.price] = level;
        }
        if (oTail == 0) { _ownerHead[o.note] = orderId; } else { _orders[oTail].nextInOwner = orderId; }
        _ownerTail[o.note] = orderId;
        _orderCount++;
    }

    function _removeFromBook(uint128 orderId) private {
        Order o = _orders[orderId];
        // Idempotency guard: a removed/empty slot has amount 0. Prevents a double
        // _removeFromBook from underflowing _orderCount below (see OrderBook).
        if (o.amount == 0) { return; }
        // Drop any §8 subscription metadata for this order on EVERY removal path — fill, expire, AND
        // cancel/cancelAll — so `getSubscription(orderId).exists` never outlives the resting order.
        // No-op for non-sub orders.
        delete _subs[orderId];
        uint128 prevP = o.prevAtPrice;
        uint128 nextP = o.nextAtPrice;
        PriceLevel level = _levels[o.isBuy][o.price];
        if (prevP == 0) { level.firstOrderId = nextP; } else { _orders[prevP].nextAtPrice = nextP; }
        if (nextP == 0) { level.lastOrderId = prevP; } else { _orders[nextP].prevAtPrice = prevP; }
        if (level.totalAmount >= o.amount) { level.totalAmount -= o.amount; } else { level.totalAmount = 0; }
        if (level.firstOrderId == 0) { delete _levels[o.isBuy][o.price]; } else { _levels[o.isBuy][o.price] = level; }

        uint128 oPrev = o.prevInOwner;
        uint128 oNext = o.nextInOwner;
        if (oPrev == 0) { _ownerHead[o.note] = oNext; } else { _orders[oPrev].nextInOwner = oNext; }
        if (oNext == 0) { _ownerTail[o.note] = oPrev; } else { _orders[oNext].prevInOwner = oPrev; }

        delete _orders[orderId];
        _orderCount--;
    }

    // ========================================================
    // Fill settlement (spec §2.3 → §3)
    // ========================================================

    function _settleFill(uint128 makerId, uint128 takerId, address buyerNote, address sellerNote, uint256 buyerPubkey, address sellerTC, uint128 trade, uint256 clearing, bool takerIsBuy) private returns (uint128) {
        uint128 cost = uint128(uint256(trade) * _unit(clearing));
        mapping(uint32 => varuint32) ecc;
        ecc[SHELL_ECC_ID] = varuint32(cost);
        ITokenContractDeal(sellerTC).fundFromOrderBook{
            value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false, currencies: ecc
        }(buyerNote, buyerPubkey);
        _executedNotional += uint128(uint256(trade) * clearing);
        _executedTicks    += trade;
        _matchSeq += 1;
        _recordTrade(uint128(clearing), trade);
        emit InferenceFilled{dest: address.makeAddrExtern(MatchedEmit, bitCntAddress)}(makerId, takerId, trade, clearing, sellerTC, buyerNote, sellerNote);
        emit InferenceExecuted{dest: address.makeAddrExtern(ExecutedEmit, bitCntAddress)}(trade, clearing, cost);

        // Owner-facing confirmation mirrors: push the deal `sellerTC` into each side's note so the
        // owner reads only its note's ext-out. Each side gets ITS own order id (maker/taker depends
        // on which side is the taker). bounce:false — the mirror is best-effort and never blocks the fill.
        uint128 buyerOrderId  = takerIsBuy ? takerId : makerId;
        uint128 sellerOrderId = takerIsBuy ? makerId : takerId;
        IPrivateNote(buyerNote).onInferenceFilled{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, sellerTC, buyerOrderId, trade, clearing, true);
        IPrivateNote(sellerNote).onInferenceFilled{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, sellerTC, sellerOrderId, trade, clearing, false);
        return cost;
    }

    /// @notice FOK pre-check: enough crossing liquidity (and, for a taker BUY, enough
    ///         escrow to pay for it) to fully fill `need`. The escrow bound makes a
    ///         FLAG_MARKET|FLAG_FOK buy check its budget, not just raw volume, so it fills
    ///         all-or-nothing.
    /// @dev    LIMITATION: this checks volume/cost by price level and does NOT simulate the
    ///         per-order min-fill=2 rule. So a FOK can pass here yet fill short in `_match`
    ///         when the only path leaves a sub-2 remainder: (i) a taker BUY with an odd
    ///         `need` against 2-tick lots; (ii) a taker SELL, whose one-maker-stop caps it at
    ///         a single bid regardless of summed volume. In both, funds stay safe — the
    ///         filled part funds real deals and the sub-2 shard is refunded — only the FOK
    ///         all-or-nothing atomicity is approximate. A precise check needs a per-order
    ///         match simulation.
    function _enoughLiquidity(bool takerIsBuy, uint256 takerPrice, bool isMarket, uint128 need, uint128 escrow) private view returns (bool) {
        uint128 accum = 0;
        uint256 cost = 0;
        uint8 walked = 0;
        optional(uint256, PriceLevel) it = _bestOpposite(takerIsBuy);
        while (it.hasValue()) {
            (uint256 lp, PriceLevel lvl) = it.get();
            if (!_crosses(takerIsBuy, takerPrice, isMarket, lp)) { break; }
            uint128 take = lvl.totalAmount;
            if (accum + take > need) { take = need - accum; }
            if (takerIsBuy) { cost += uint256(take) * _unit(lp); }   // price each crossing tick
            accum += take;
            if (accum >= need) { break; }
            walked++;
            if (walked >= MAX_PRECHECK_LEVELS) { break; }
            it = _nextOpposite(takerIsBuy, lp);
        }
        if (accum < need) { return false; }
        // A taker BUY must AFFORD the fill, not just find the volume: the running cost of
        // `need` ticks must fit the escrow, so a FLAG_MARKET|FLAG_FOK buy (no price cap)
        // fills all-or-nothing. (SELL takers carry no escrow → skip.)
        if (takerIsBuy && cost > uint256(escrow)) { return false; }
        return true;
    }

    // ========================================================
    // Matching engine (best-first, price→time, bounded; resumes via the queue)
    // ========================================================

    /// @return remaining unfilled ticks, leftoverEscrow, capped (true = hit the
    ///         per-tx cap with crossing liquidity left → caller continues).
    function _match(
        uint128 takerId, bool takerIsBuy, uint256 takerPrice, bool isMarket,
        address takerNote, address takerTC, uint256 takerBuyerPubkey, uint128 amount, uint128 buyEscrow,
        uint128 resumeFrom
    ) private returns (uint128 remaining, uint128 leftoverEscrow, bool capped, uint128 nextResume) {
        remaining = amount;
        leftoverEscrow = buyEscrow;
        capped = false;
        nextResume = 0;
        uint8 matches = 0;

        // Resume the scan from `resumeFrom` — a maker skipped on a prior tx — so a taker
        // crossing many un-fillable makers advances instead of re-scanning the same head
        // each continuation. The book is frozen while a continuation holds the queue head,
        // so the saved position stays valid; if that order was removed since, fall back to
        // the best level.
        uint256 startLevel = 0;
        uint128 startOrder = 0;
        if (resumeFrom != 0 && _orders[resumeFrom].amount != 0 && _orders[resumeFrom].isBuy != takerIsBuy) {
            startLevel = _orders[resumeFrom].price;
            startOrder = resumeFrom;
        }
        optional(uint256, PriceLevel) it;
        if (startOrder != 0) { it.set(startLevel, _levels[!takerIsBuy][startLevel]); }
        else { it = _bestOpposite(takerIsBuy); }
        while (it.hasValue() && remaining > 0) {
            (uint256 lp, ) = it.get();
            if (!_crosses(takerIsBuy, takerPrice, isMarket, lp)) { break; }

            uint128 cur = (lp == startLevel && startOrder != 0) ? startOrder : _levels[!takerIsBuy][lp].firstOrderId;
            startOrder = 0;   // the resume position applies only to the first scanned level
            while (cur != 0 && remaining > 0) {
                if (matches >= MAX_MATCHES_PER_CALL) { return (remaining, leftoverEscrow, true, cur); }
                Order mk = _orders[cur];
                uint128 nextOrd = mk.nextAtPrice;
                // Clearing = the SELLER's ask, both directions (Variant 2):
                //  - taker BUY: lp = the maker SELL's ask (already the seller's price);
                //  - taker SELL: takerPrice = the taker SELL's ask (_pricePerTick), NOT the
                //    maker BID's lp. This caps the fund at maxTicks*unit(ask) so the TC is
                //    never over-funded; the bid-ask spread stays in the maker buyer's escrow
                //    and refunds via the residual path. SELLs are limit-only (no market), so
                //    takerPrice > 0 here.
                uint256 clearing = takerIsBuy ? lp : takerPrice;

                // §8 subscription maker (resting buy): roll cycles + cap by cycle budget.
                bool makerSub = (!takerIsBuy) && _subs[cur].exists;
                if (makerSub) {
                    _subTouch(cur);
                    if (!_subs[cur].exists) { cur = nextOrd; continue; }   // expired
                    mk = _orders[cur];
                    nextOrd = mk.nextAtPrice;
                }

                // GTD limit BUY resting past its deadline: refund the buyer's escrow and remove it
                // before it can settle as live liquidity. Subscriptions roll/expire via _subTouch
                // above; a plain GTD bid enforces its deadline here at match time. Skip taker BUY
                // (mk = a SELL offer; deadline only on buys).
                if (!takerIsBuy && _isExpiredGtdBid(mk.deadline, makerSub)) {
                    _refundAndRemove(cur);
                    cur = nextOrd;
                    continue;
                }

                uint128 trade = remaining < mk.amount ? remaining : mk.amount;

                address buyerNote;
                address sellerNote;
                address sellerTC;
                uint256 buyerPubkey;
                if (takerIsBuy) { buyerNote = takerNote; sellerNote = mk.note;  buyerPubkey = takerBuyerPubkey; sellerTC = mk.tokenContract; }
                else            { buyerNote = mk.note;   sellerNote = takerNote; buyerPubkey = mk.buyerPubkey;   sellerTC = takerTC; }

                uint256 unit = _unit(clearing);
                uint128 budget = takerIsBuy ? leftoverEscrow : mk.escrow;
                if (makerSub) {
                    Sub s = _subs[cur];
                    uint128 cycRem = s.cycleBudget > s.cycleSpent ? s.cycleBudget - s.cycleSpent : 0;
                    if (cycRem < budget) { budget = cycRem; }
                }
                uint128 afford = unit > 0 ? uint128(uint256(budget) / unit) : trade;
                if (afford < trade) { trade = afford; }
                // Min-fill = 2 ticks: a deal needs probe + >=1 stream tick, and
                // fundFromOrderBook rejects a sub-2 fund. A 0/1-tick fill is not a settleable
                // deal. trade==0 is un-tradeable; trade==1 is a sub-2 dust remainder.
                if (trade < 2) {
                    // Taker BUY can't take >=2 from the best SELL (remaining<2, or escrow
                    // affords <2 and pricier levels afford even less) -> stop; _finalizeTaker
                    // refunds the sub-2 remainder rather than resting it as dust.
                    if (takerIsBuy) { return (remaining, leftoverEscrow, false, 0); }
                    // Count the skip toward MAX_MATCHES_PER_CALL so a taker-SELL scans a
                    // bounded number of levels per tx. On the cap it returns `capped` and
                    // resumes; the removals below shrink the book so the resume makes progress
                    // (no re-scan of the same dust).
                    matches++;
                    if (makerSub) { cur = nextOrd; continue; }   // subscription rolls to next cycle
                    // Plain bid offering < 2 ticks (0 = un-tradeable, or a 1-tick dust remainder
                    // left by a partial fill): refund the owner IN FULL and remove it. Removing —
                    // rather than skipping — keeps the scan bounded and stops dust from
                    // accumulating in the book.
                    _refundAndRemove(cur);
                    cur = nextOrd;
                    continue;
                }

                uint128 cost = _settleFill(takerIsBuy ? cur : takerId, takerIsBuy ? takerId : cur, buyerNote, sellerNote, buyerPubkey, sellerTC, trade, clearing, takerIsBuy);

                if (takerIsBuy) { leftoverEscrow -= cost; } else { _orders[cur].escrow = mk.escrow - cost; }

                if (makerSub) {
                    _subs[cur].cycleSpent += cost;
                    uint8 cy = _subs[cur].curCycle;
                    _cycleFundedTicks[cur][cy] += trade;
                    _cycleSellerTicks[cur][cy][takerNote] += trade;   // takerNote = seller note
                }

                _orders[cur].filledAccum += trade;
                // SELL offer = one-deal slot → consumed on match (taker BUY), even
                // on partial. BUY maker (taker SELL) is reduced (spans deals).
                if (takerIsBuy) {
                    _removeFromBook(cur);                       // maker SELL: no buyer escrow to return
                } else if (mk.amount == trade) {
                    // Fully-filled maker BUY: the residual escrow (over-fund + clearing-remainder
                    // = mk.escrow - cost, set above) is returned to the buyer.
                    if (makerSub) {
                        // §8.3/§8.4: an early full-fill forfeits ONLY the CURRENT cycle's own
                        // unspent (cycleBudget - cycleSpent) to that cycle's sellers (claimForfeit); the
                        // FUTURE cycles' budget (weeks not yet served) refunds to the BUYER, so each
                        // cycle's sellers earn only from the cycle they served. Read escrow/note/sub
                        // before _removeFromBook deletes the order.
                        uint128 resid = _orders[cur].escrow;
                        address bnote = _orders[cur].note;
                        Sub s = _subs[cur];
                        if (resid > 0) {
                            uint128 cycUnspent = s.cycleBudget > s.cycleSpent ? s.cycleBudget - s.cycleSpent : 0;
                            if (cycUnspent > resid) { cycUnspent = resid; }   // resid caps the forfeit
                            uint128 refundFuture = resid - cycUnspent;
                            _orders[cur].escrow = 0;
                            if (cycUnspent > 0) {
                                if (_cycleFundedTicks[cur][s.curCycle] == 0) {
                                    // No seller served this cycle, so a forfeit pool would be
                                    // unclaimable — return this cycle's unspent to the buyer instead.
                                    refundFuture += cycUnspent;
                                } else {
                                    _forfeitPool[cur][s.curCycle] += cycUnspent;
                                    emit InferenceCycleForfeited{dest: address.makeAddrExtern(CycleForfeitedEmit, bitCntAddress)}(cur, s.curCycle, cycUnspent, _cycleFundedTicks[cur][s.curCycle]);
                                }
                            }
                            if (refundFuture > 0) {
                                _payShell(bnote, refundFuture);
                                emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(bnote, refundFuture);
                            }
                        }
                        _removeFromBook(cur);   // _subs[cur] dropped inside _removeFromBook
                    } else {
                        _refundAndRemove(cur);                  // limit BUY: residual escrow back to the buyer
                    }
                } else {
                    _orders[cur].amount = mk.amount - trade;
                    _levels[!takerIsBuy][lp].totalAmount -= trade;
                }

                remaining -= trade;
                matches++;
                cur = nextOrd;

                // Taker SELL is itself one deal → one fill, then consumed.
                if (!takerIsBuy) { remaining = 0; break; }
            }
            it = _nextOpposite(takerIsBuy, lp);
        }
        return (remaining, leftoverEscrow, false, 0);
    }

    /// @notice A resting order is an expired plain GTD bid when it is not a
    ///         subscription and its non-zero deadline has passed. Subscriptions
    ///         roll/expire via `_subTouch`, so they are excluded. Single source
    ///         of the expiry rule shared by the match loop and the pre-check purge.
    function _isExpiredGtdBid(uint64 deadline, bool isSub) private view returns (bool) {
        return !isSub && deadline != 0 && block.timestamp >= deadline;
    }

    function _refundAndRemove(uint128 orderId) private {
        Order o = _orders[orderId];
        uint128 refund = o.escrow;
        _removeFromBook(orderId);
        if (refund > 0) { _payShell(o.note, refund); emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(o.note, refund); }
    }

    /// @notice Refund + remove expired GTD BUYs a SELL taker at `takerPrice` crosses, mirroring the
    ///         match-time expiry sweep (§ `_match` deadline branch) so POST_ONLY/FOK prechecks see
    ///         only live liquidity. Subscriptions roll/expire via `_subTouch`, so they are skipped.
    ///         Bounded by `MAX_PRECHECK_LEVELS`.
    function _purgeExpiredBids(uint256 takerPrice, bool isMarket) private {
        optional(uint256, PriceLevel) it = _bestOpposite(false);   // best resting BUY
        uint8 walked = 0;
        uint8 purged = 0;
        while (it.hasValue()) {
            (uint256 lp, ) = it.get();
            if (!_crosses(false, takerPrice, isMarket, lp)) { break; }
            optional(uint256, PriceLevel) nxt = _nextOpposite(false, lp);   // capture before mutation
            uint128 cur = _levels[true][lp].firstOrderId;
            while (cur != 0) {
                uint128 nextP = _orders[cur].nextAtPrice;
                if (_isExpiredGtdBid(_orders[cur].deadline, _subs[cur].exists)) {
                    _refundAndRemove(cur);
                    // Per-call cap: each purge is a refund + emit (an output action), so a
                    // single SELL placement purges a bounded number of expired bids. Any
                    // leftover expired bids are purged on a later placement or lazily by the
                    // match loop's own deadline check.
                    purged++;
                    if (purged >= MAX_MATCHES_PER_CALL) { return; }
                }
                cur = nextP;
            }
            walked++;
            if (walked >= MAX_PRECHECK_LEVELS) { break; }
            it = nxt;
        }
    }

    /// @notice Rest leftover (limit) or refund (taker-only / market) after a match completes.
    function _finalizeTaker(
        uint128 orderId, uint256 buyerPubkey, bool isBuy, uint256 storedPrice, address note, address tc,
        uint128 remaining, uint128 leftover, uint128 initialAmount, uint8 flags, uint64 deadline
    ) private {
        bool takerOnly = (flags & FLAG_MARKET) != 0 || (flags & (FLAG_IOC | FLAG_FOK)) != 0;
        if (isBuy) {
            // remaining < 2: a 1-tick remainder can never fund a deal (min-fill skips it),
            // so resting it would only leave unfillable dust. Refund the leftover escrow
            // instead of inserting it — same as the fully-filled/taker-only case.
            if (remaining < 2 || takerOnly) {
                if (leftover > 0) { _payShell(note, leftover); emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(note, leftover); }
                return;
            }
            _insertIntoBook(orderId, Order({
                note: note, buyerPubkey: buyerPubkey, tokenContract: address(0), price: storedPrice,
                amount: remaining, initialAmount: initialAmount, filledAccum: initialAmount - remaining,
                escrow: leftover, deadline: deadline, ts: uint64(block.timestamp),
                flags: flags, isBuy: true,
                nextAtPrice: 0, prevAtPrice: 0, nextInOwner: 0, prevInOwner: 0
            }));
        } else {
            if (remaining > 0 && !takerOnly) {
                _insertIntoBook(orderId, Order({
                    note: note, buyerPubkey: 0, tokenContract: tc, price: storedPrice,
                    amount: remaining, initialAmount: initialAmount, filledAccum: initialAmount - remaining,
                    escrow: 0, deadline: 0, ts: uint64(block.timestamp),
                    flags: flags, isBuy: false,
                    nextAtPrice: 0, prevAtPrice: 0, nextInOwner: 0, prevInOwner: 0
                }));
            } else if (remaining > 0 && tc != address(0)) {
                // Taker-only SELL (IOC/FOK) that did NOT rest and was NOT funded:
                // remaining>0 means it never matched a buyer — a filled taker-SELL leaves
                // remaining==0 via one-maker-stop, and its latch was cleared in _recordFunding.
                // This offer never rests and gets no other callback, so notify the TC here
                // (onSellClosed frees the `_offerPosted` latch) to keep it usable.
                ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
            }
        }
    }

    // ========================================================
    // Queue (circular) — ported from dex OrderBook
    // ========================================================

    function _allocSlot() private returns (uint8 slot) {
        slot = _queueTail;
        _queueTail = uint8((uint32(_queueTail) + 1) % uint32(QUEUE_CAPACITY));
        _queueSize++;
    }

    function _advanceHead() private {
        delete _queue[_queueHead];
        _queueHead = uint8((uint32(_queueHead) + 1) % uint32(QUEUE_CAPACITY));
        _queueSize--;
    }

    function _enqueuePlace(address owner, uint256 buyerPubkey, bool isBuy, uint8 flags, uint256 price, uint128 amount, uint128 escrow, address tc, uint64 deadline) private {
        require(_queueSize < QUEUE_PLACE_LIMIT, ERR_QUEUE_FULL);
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_PLACE, owner: owner, buyerPubkey: buyerPubkey, isBuy: isBuy, flags: flags, price: price,
            amount: amount, escrow: escrow, tokenContract: tc, deadline: deadline,
            targetOrderId: 0, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
    }

    function _enqueueCancel(address owner, uint128 targetOrderId) private {
        require(_queueSize < QUEUE_CAPACITY, ERR_QUEUE_FULL);
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL, owner: owner, buyerPubkey: 0, isBuy: false, flags: 0, price: 0,
            amount: 0, escrow: 0, tokenContract: address(0), deadline: 0,
            targetOrderId: targetOrderId, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
    }

    function _enqueueCancelAll(address owner) private {
        require(_queueSize < QUEUE_CAPACITY, ERR_QUEUE_FULL);
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL_ALL, owner: owner, buyerPubkey: 0, isBuy: false, flags: 0, price: 0,
            amount: 0, escrow: 0, tokenContract: address(0), deadline: 0,
            targetOrderId: 0, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
    }

    function _selfCallProcessHead() private pure {
        InferenceOrderBook(address(this)).processHead{value: 3 vmshell, flag: 1, bounce: false}();
    }

    /// @notice Public drain entry: anyone can poke (and the engine self-calls it
    ///         to continue an in-flight match across txs).
    function processHead() public {
        tvm.accept();
        ensureBalance();
        _processHeadCore();
    }

    function _processHeadCore() private {
        if (_queueSize == 0) { return; }
        QueueEntry e = _queue[_queueHead];
        bool keepHead = false;

        if (e.entryType == QENTRY_CANCEL) {
            _doCancel(e.owner, e.targetOrderId);
        } else if (e.entryType == QENTRY_CANCEL_ALL) {
            uint8 cancelled = _doCancelAll(e.owner);
            keepHead = (cancelled >= MAX_CANCEL_PER_CALL);
        } else {
            keepHead = _doPlaceHead();
        }

        if (!keepHead) { _advanceHead(); }
        if (_queueSize > 0) { _selfCallProcessHead(); }
    }

    /// @notice Process (or resume) the head PLACE entry. Returns true to keep the
    ///         head (a match continuation is needed — resumes next tx).
    function _doPlaceHead() private returns (bool) {
        QueueEntry e = _queue[_queueHead];
        bool firstRun = (e.contOrderId == 0);
        uint128 orderId = firstRun ? _nextOrderId++ : e.contOrderId;
        bool isMarket = (e.flags & FLAG_MARKET) != 0;

        if (firstRun) {
            // One resting SELL per deal TokenContract is now enforced by the TC
            // itself (`_offerPosted`), since the TC posts its own offer — no
            // per-TC map here.
            // Purge expired GTD bids that a SELL taker crosses BEFORE the prechecks, so POST_ONLY
            // crossing and FOK `_enoughLiquidity` see the same live liquidity `_match` would (it
            // lazily refunds expired bids). This keeps the prechecks consistent with the match:
            // POST_ONLY tests only live liquidity, and FOK counts only fillable volume.
            // (Only BUYs carry a deadline → purge for taker SELLs.)
            if (!e.isBuy) { _purgeExpiredBids(e.price, isMarket); }

            // POST_ONLY: reject if it would cross.
            if ((e.flags & FLAG_POST_ONLY) != 0) {
                optional(uint256, PriceLevel) best = _bestOpposite(e.isBuy);
                if (best.hasValue()) {
                    (uint256 bp, ) = best.get();
                    if (_crosses(e.isBuy, e.price, false, bp)) {
                        if (e.isBuy && e.escrow > 0) { _payShell(e.owner, e.escrow); }
                        else if (!e.isBuy && e.tokenContract != address(0)) {
                            // SELL rejected before resting -> notify the TC (onSellClosed frees
                            // the `_offerPosted` latch) so it stays usable.
                            ITokenContractDeal(e.tokenContract).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
                        }
                        return false;
                    }
                }
            }
            // FOK: all-or-nothing pre-check (bounded level walk).
            if ((e.flags & FLAG_FOK) != 0 && !_enoughLiquidity(e.isBuy, e.price, isMarket, e.amount, e.escrow)) {
                if (e.isBuy && e.escrow > 0) { _payShell(e.owner, e.escrow); }
                else if (!e.isBuy && e.tokenContract != address(0)) {
                    ITokenContractDeal(e.tokenContract).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();  // free the TC's offer latch
                }
                return false;
            }

            // Mirror the placement ONLY after the rejection checks pass — a crossing POST_ONLY or
            // under-liquid FOK returns above without inserting/filling, so it must NOT emit
            // InferenceOrderPlaced / onInferencePlaced (else clients would track an order that
            // never rested).
            emit InferenceOrderPlaced{dest: address.makeAddrExtern(OfferPlacedEmit, bitCntAddress)}(
                orderId, e.isBuy, e.price, e.amount, e.owner, e.tokenContract, e.deadline);
            IPrivateNote(e.owner).onInferencePlaced{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
                _modelHash, e.tokenContract, orderId, e.isBuy, e.price, e.amount);
        }

        uint128 inAmount   = firstRun ? e.amount  : e.contRemaining;
        uint128 inEscrow   = firstRun ? e.escrow  : e.contLeftover;
        uint128 resumeFrom = firstRun ? 0 : e.contScanOrder;
        (uint128 remaining, uint128 leftover, bool capped, uint128 nextResume) =
            _match(orderId, e.isBuy, e.price, isMarket, e.owner, e.tokenContract, e.buyerPubkey, inAmount, inEscrow, resumeFrom);

        if (capped) {
            // Taker crossed > one tx of liquidity → persist cursor + scan position, resume
            // next tx from where it stopped so an un-fillable head is not re-scanned.
            e.contOrderId = orderId;
            e.contRemaining = remaining;
            e.contLeftover = leftover;
            e.contScanOrder = nextResume;
            _queue[_queueHead] = e;
            return true;
        }
        _finalizeTaker(orderId, e.buyerPubkey, e.isBuy, e.price, e.owner, e.tokenContract, remaining, leftover, e.amount, e.flags, e.deadline);
        return false;
    }

    function _doCancel(address owner, uint128 orderId) private {
        Order o = _orders[orderId];
        if (o.amount == 0 && o.note == address(0)) { return; }
        if (o.note != owner) { return; }
        uint128 refund = o.escrow;
        // A cancelled SELL is removed WITHOUT a fill → free the TC's `_offerPosted`
        // latch so the seller can re-list on the same (still-live) TC. Read the TC
        // BEFORE `_removeFromBook` deletes the order.
        bool    freeTc = !o.isBuy && o.tokenContract != address(0);
        address tc     = o.tokenContract;
        _removeFromBook(orderId);
        emit InferenceOrderCancelled{dest: address.makeAddrExtern(OfferCancelledEmit, bitCntAddress)}(orderId, refund, owner);
        _payShell(owner, refund);
        if (freeTc) { ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(); }
    }

    function _doCancelAll(address owner) private returns (uint8 cancelled) {
        cancelled = 0;
        uint128 cur = _ownerHead[owner];
        while (cur != 0 && cancelled < MAX_CANCEL_PER_CALL) {
            Order o = _orders[cur];
            uint128 next = o.nextInOwner;
            uint128 refund = o.escrow;
            bool    freeTc = !o.isBuy && o.tokenContract != address(0);
            address tc     = o.tokenContract;
            _removeFromBook(cur);
            emit InferenceOrderCancelled{dest: address.makeAddrExtern(OfferCancelledEmit, bitCntAddress)}(cur, refund, owner);
            if (refund > 0) { _payShell(owner, refund); }
            if (freeTc) { ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(); }
            cur = next;
            cancelled++;
        }
    }

    // ========================================================
    // Public order entry (enqueue + drain)
    // ========================================================

    /// @dev The deal TokenContract posts its OWN offer now (not the seller note),
    ///      so `msg.sender` IS the deal contract. Requiring `msg.sender` to be the
    ///      canonical TC for (sellerPubkey, nonce) proves the TC is deployed AND
    ///      note-confirmed (only a confirmed TC can reach `TokenContract.placeSellOffer`)
    ///      → every resting offer maps to a live, canonical TC, so a match always funds
    ///      a real deal contract. `ownerNote` is the seller note (resting-order owner,
    ///      for onInferencePlaced/handover). A TC is one-shot (enforces one offer
    ///      itself), so no `_sellTcInUse` map.
    function placeSellOffer(uint128 pricePerTick, uint128 maxTicks, uint8 flags, uint256 sellerPubkey, uint64 nonce, address ownerNote) public {
        ensureBalance();
        // Auth BEFORE accept: a non-canonical sender is rejected without charging the
        // contract. A real TC always passes this, so a genuine offer never reverts here.
        require(msg.sender == _tokenContractAddr(sellerPubkey, nonce), ERR_BAD_TOKEN_CONTRACT);
        tvm.accept();
        // Param / capacity checks AFTER accept: the TC latched `_offerPosted` optimistically
        // and forwarded bounce:false. On ANY non-resting outcome, notify the TC (onSellClosed
        // frees the latch) and return rather than revert, so the TC stays usable (re-list /
        // close / destroy all remain available).
        //  - deal serves >= 2 ticks (fundFromOrderBook floor);
        //  - a SELL is a fixed-price limit, never a market order (market clearing = 0);
        //  - price > 0; flag combos consistent (POST_ONLY xor taker; IOC xor FOK);
        //  - the placement queue has room (else _enqueuePlace rejects with ERR_QUEUE_FULL).
        bool bad = maxTicks < 2
            || pricePerTick == 0
            || (flags & FLAG_MARKET) != 0
            || ((flags & FLAG_POST_ONLY) != 0 && (flags & TAKER_FLAGS) != 0)
            || ((flags & FLAG_IOC) != 0 && (flags & FLAG_FOK) != 0)
            // maxTicks*(price + fee) must fit uint128 so the fill cost never overflows the cast.
            || uint256(maxTicks) * _unit(pricePerTick) > uint256(type(uint128).max);
        if (bad || _queueSize >= QUEUE_PLACE_LIMIT) {
            ITokenContractDeal(msg.sender).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
            return;
        }
        _enqueuePlace(ownerNote, 0, false, flags, pricePerTick, maxTicks, 0, msg.sender, 0);
        _processHeadCore();
    }

    function placeBuyOrder(uint128 maxPricePerTick, uint128 ticks, uint8 flags, uint64 deadline, uint256 buyerPubkey) public {
        ensureBalance();
        // A deal serves >= 2 ticks (probe + >=1 stream); a 1-tick buy can never fund a
        // deal (fundFromOrderBook rejects < 2), so it is rejected up front.
        require(ticks >= 2, ERR_BAD_PARAM);
        require((flags & FLAG_POST_ONLY) == 0 || (flags & TAKER_FLAGS) == 0, ERR_BAD_FLAGS);
        require((flags & FLAG_IOC) == 0 || (flags & FLAG_FOK) == 0, ERR_BAD_FLAGS);
        require(deadline == 0 || deadline > block.timestamp, ERR_EXPIRED);
        bool isMarket = (flags & FLAG_MARKET) != 0;
        require(isMarket || maxPricePerTick > 0, ERR_BAD_PARAM);

        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.exists(SHELL_ECC_ID), ERR_NO_SHELL);
        uint128 escrow = uint128(currencies[SHELL_ECC_ID]);
        if (!isMarket) { require(escrow >= uint128(uint256(ticks) * _unit(maxPricePerTick)), ERR_INSUFFICIENT_DEPOSIT); }
        tvm.accept();
        _enqueuePlace(msg.sender, buyerPubkey, true, flags, isMarket ? type(uint256).max : maxPricePerTick, ticks, escrow, address(0), deadline);
        _processHeadCore();
    }

    function cancelOrder(uint128 orderId) public {
        ensureBalance();
        tvm.accept();
        _enqueueCancel(msg.sender, orderId);
        _processHeadCore();
    }

    function cancelAllOrders() public {
        ensureBalance();
        tvm.accept();
        _enqueueCancelAll(msg.sender);
        _processHeadCore();
    }

    // ========================================================
    // §8 subscription (semantic order)
    // ========================================================

    /// @notice §8 subscription: a resting limit buy whose budget is throttled into
    ///         SUB_CYCLES weekly cycles; unspent per-cycle budget is forfeited (not
    ///         rolled) to the sellers it funded that cycle, pro-rata by funded ticks.
    /// @dev Rests as a standing bid (filled by incoming sells); does not take on
    ///      placement. Renewal = client re-places (§8.2); `autoRenew` is a hint.
    function placeSubscription(uint128 maxPricePerTick, uint128 ticks, bool autoRenew, uint256 buyerPubkey) public {
        ensureBalance();
        require(ticks > 0 && maxPricePerTick > 0, ERR_BAD_PARAM);
        mapping(uint32 => varuint32) currencies = msg.currencies;
        require(currencies.exists(SHELL_ECC_ID), ERR_NO_SHELL);
        uint128 escrow = uint128(currencies[SHELL_ECC_ID]);
        require(escrow >= uint128(uint256(ticks) * _unit(maxPricePerTick)), ERR_INSUFFICIENT_DEPOSIT);
        // Each of the SUB_CYCLES cycles must be able to fund a real deal (min-fill = 2 ticks):
        // the per-cycle budget (escrow / SUB_CYCLES) covers at least two ticks at the ceiling price.
        require(escrow / uint128(SUB_CYCLES) >= 2 * _unit(maxPricePerTick), ERR_INSUFFICIENT_DEPOSIT);
        tvm.accept();

        address buyerNote = msg.sender;
        uint128 orderId = _nextOrderId++;
        uint64  dl = uint64(block.timestamp) + SUB_PERIOD;
        uint128 cycleBudget = escrow / uint128(SUB_CYCLES);

        _insertIntoBook(orderId, Order({
            note: buyerNote, buyerPubkey: buyerPubkey, tokenContract: address(0), price: maxPricePerTick,
            amount: ticks, initialAmount: ticks, filledAccum: 0,
            escrow: escrow, deadline: dl, ts: uint64(block.timestamp),
            flags: 0, isBuy: true,
            nextAtPrice: 0, prevAtPrice: 0, nextInOwner: 0, prevInOwner: 0
        }));
        _subs[orderId] = Sub({
            periodStart: uint64(block.timestamp), curCycle: 0,
            cycleBudget: cycleBudget, cycleSpent: 0, autoRenew: autoRenew, exists: true
        });
        emit InferenceSubscriptionPlaced{dest: address.makeAddrExtern(SubscriptionPlacedEmit, bitCntAddress)}(orderId, buyerNote, maxPricePerTick, ticks, cycleBudget, autoRenew);
    }

    function _subTouch(uint128 orderId) private {
        Sub s = _subs[orderId];
        if (!s.exists) { return; }
        while (block.timestamp >= s.periodStart + (uint64(s.curCycle) + 1) * SUB_CYCLE_LEN) {
            uint128 unspent = s.cycleBudget > s.cycleSpent ? s.cycleBudget - s.cycleSpent : 0;
            if (unspent > 0) {
                Order o = _orders[orderId];
                if (o.escrow < unspent) { unspent = o.escrow; }
                _orders[orderId].escrow = o.escrow - unspent;
                if (_cycleFundedTicks[orderId][s.curCycle] == 0) {
                    // No seller served this cycle, so a forfeit pool would be unclaimable —
                    // return this cycle's unspent to the buyer instead.
                    _payShell(o.note, unspent);
                    emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(o.note, unspent);
                } else {
                    _forfeitPool[orderId][s.curCycle] += unspent;
                    emit InferenceCycleForfeited{dest: address.makeAddrExtern(CycleForfeitedEmit, bitCntAddress)}(orderId, s.curCycle, unspent, _cycleFundedTicks[orderId][s.curCycle]);
                }
            }
            s.cycleSpent = 0;
            s.curCycle += 1;
            if (s.curCycle >= SUB_CYCLES) { _subs[orderId] = s; _expireSub(orderId); return; }
        }
        _subs[orderId] = s;
    }

    function _expireSub(uint128 orderId) private {
        Order o = _orders[orderId];
        uint128 refund = o.escrow;
        _removeFromBook(orderId);   // _subs[orderId] dropped inside _removeFromBook
        if (refund > 0) { _payShell(o.note, refund); emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(o.note, refund); }
    }

    function pokeSubscription(uint128 orderId) public {
        ensureBalance();
        require(_subs[orderId].exists, ERR_NOT_SUB);
        tvm.accept();
        _subTouch(orderId);
    }

    function claimForfeit(uint128 orderId, uint8 cycle) public {
        ensureBalance();
        address seller = msg.sender;
        uint128 pool  = _forfeitPool[orderId][cycle];
        uint128 total = _cycleFundedTicks[orderId][cycle];
        uint128 mine  = _cycleSellerTicks[orderId][cycle][seller];
        require(pool > 0 && total > 0 && mine > 0, ERR_NOTHING_TO_CLAIM);
        tvm.accept();
        uint128 share = uint128(uint256(pool) * uint256(mine) / uint256(total));
        delete _cycleSellerTicks[orderId][cycle][seller];
        _forfeitPool[orderId][cycle]      = pool - share;
        _cycleFundedTicks[orderId][cycle] = total - mine;
        _payShell(seller, share);
        emit InferenceForfeitClaimed{dest: address.makeAddrExtern(ForfeitClaimedEmit, bitCntAddress)}(orderId, cycle, seller, share);
    }

    // ========================================================
    // Reference-price stats (spec §6.2 / §7)
    // ========================================================

    function _recordTrade(uint128 price, uint128 ticks) private {
        uint64 day = uint64(block.timestamp) / SECS_PER_DAY;
        DayData cur = _daily[_curSlot];
        if (cur.day != day) {
            if (cur.day != 0 || cur.volSum != 0) { _curSlot = uint8((_curSlot + 1) % 8); }
            _daily[_curSlot] = DayData({day: day, priceVolSum: 0, volSum: 0});
        }
        _daily[_curSlot].priceVolSum += uint256(price) * uint256(ticks);
        _daily[_curSlot].volSum      += uint256(ticks);
    }

    function _weeklyMedian() private view returns (uint256) {
        uint64 nowDay = uint64(block.timestamp) / SECS_PER_DAY;
        uint64 minDay = nowDay >= 6 ? nowDay - 6 : 0;
        uint256[] vwaps;
        uint256 totalVol = 0;
        for (uint8 i = 0; i < 8; i++) {
            DayData d = _daily[i];
            if (d.volSum == 0) { continue; }
            if (d.day < minDay || d.day > nowDay) { continue; }
            vwaps.push(d.priceVolSum / d.volSum);
            totalVol += d.volSum;
        }
        require(totalVol >= MIN_LIQUIDITY, ERR_NO_LIQUIDITY);
        uint n = vwaps.length;
        for (uint a = 0; a < n; a++) {
            for (uint b = a + 1; b < n; b++) {
                if (vwaps[b] < vwaps[a]) { uint256 t = vwaps[a]; vwaps[a] = vwaps[b]; vwaps[b] = t; }
            }
        }
        if (n % 2 == 1) { return vwaps[n / 2]; }
        return (vwaps[n / 2 - 1] + vwaps[n / 2]) / 2;
    }

    function getWeeklyMedianPrice() external view returns (uint256) { return _weeklyMedian(); }

    function requestWeeklyMedian(uint256 eventId, uint256 oracleListHash, uint32 tokenType) public {
        ensureBalance();
        uint256 price = _weeklyMedian();
        IWeeklyMedianSink(msg.sender).onWeeklyMedian{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            eventId, oracleListHash, tokenType, price);
    }

    // ========================================================
    // Getters
    // ========================================================

    function getOrder(uint128 id) external view returns (
        address note, address tokenContract, uint256 price, uint128 amount,
        uint128 escrow, uint64 deadline, uint8 flags, bool isBuy, uint64 ts
    ) {
        Order o = _orders[id];
        return (o.note, o.tokenContract, o.price, o.amount, o.escrow, o.deadline, o.flags, o.isBuy, o.ts);
    }

    function getBestBidAsk() external view returns (bool hasBid, uint256 bid, bool hasAsk, uint256 ask) {
        optional(uint256, PriceLevel) b = _levels[true].max();
        optional(uint256, PriceLevel) a = _levels[false].min();
        if (b.hasValue()) { (bid, ) = b.get(); hasBid = true; }
        if (a.hasValue()) { (ask, ) = a.get(); hasAsk = true; }
    }

    function getStats() external view returns (uint128 nextOrderId, uint128 orderCount, uint128 executedNotional, uint128 executedTicks) {
        return (_nextOrderId, _orderCount, _executedNotional, _executedTicks);
    }

    function getQueueSize() external view returns (uint8) { return _queueSize; }

    function getSubscription(uint128 orderId) external view returns (
        bool exists, uint64 periodStart, uint8 curCycle, uint128 cycleBudget, uint128 cycleSpent, bool autoRenew
    ) {
        Sub s = _subs[orderId];
        return (s.exists, s.periodStart, s.curCycle, s.cycleBudget, s.cycleSpent, s.autoRenew);
    }

    function getForfeit(uint128 orderId, uint8 cycle) external view returns (uint128 pool, uint128 fundedTicks) {
        return (_forfeitPool[orderId][cycle], _cycleFundedTicks[orderId][cycle]);
    }

    function getParams() external view returns (uint256 modelHash, uint16 platformFeeBps) {
        return (_modelHash, PLATFORM_FEE_BPS);
    }

    /// @notice The verified canonical model id `producer--model--version` (sha256 == _modelHash).
    function getModelName() external view returns (string) { return _modelName; }

    function getVersion() external pure returns (string, string) {
        return (version, "InferenceOrderBook");
    }
}
