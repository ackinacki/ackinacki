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
// CODE HASH + the canonical SuperRoot — see `DexLib.computeRootModelAddressFromHash` and
// the note's `_tokenContractAddr`. (The note's own `_rootModelAddr` wrapper is gone: the
// super root deploys RootModels now, so a note has no reason to derive that address.)
import "./RootModel.sol";

/// @notice Handover: the matched seller's `token_contract` receives the deal's
///         SHELL and binds the buyer note (spec §2.3).
interface ITokenContractDeal {
    /// @dev `paid` LEADS the parameter list so it survives a bounce — see `onBounce`. Do not
    ///      reorder: an address in front is 267 bits and would push it past the 256-bit window.
    function fundFromOrderBook(uint128 paid, address buyerNote, uint256 buyerPubkey, uint8 dealFlags) external;
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
    function onInferencePlaced(uint256 modelHash, address tokenContract, uint128 orderId, uint64 clientOrderId, bool isBuy, uint256 price, uint128 ticks) external;
    function onInferenceFilled(uint256 modelHash, address tokenContract, uint128 orderId, uint64 clientOrderId, uint128 ticks, uint256 clearingPrice, uint128 spent, bool isBuy) external;
    /// @notice The third outcome family (task Q): the book took the request, refused it, and gave
    ///         the money back — without a bounce and without ever creating an order.
    /// @dev    ONE MESSAGE, BOTH HALVES. The refund used to travel as an ordinary `creditFromBook`
    ///         while the reason went out as an event to a watcher, so the note received money and
    ///         could not say what for: no order id had ever been assigned, and the amount alone
    ///         does not identify a request. Splitting them also left a window in which the note
    ///         had cleared its pending record and the credit had not yet arrived.
    function onInferenceRejected(uint256 modelHash, uint64 clientOrderId, uint8 reason, uint128 refunded) external;
    /// @dev Sent from the book's ONE removal point, so cancel, expiry and fill all reach it.
    function onInferenceOrderRemoved(uint256 modelHash, uint128 orderId, uint8 cause, uint128 refunded) external;
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
    string constant version = "4.0.36";

    // ⚠ Re-pin whenever dex/PrivateNote is recompiled (note↔OB layout coupling:
    //   the note bakes this book's state layout via `new InferenceOrderBook`, so any
    //   OB layout change forces a note rebuild → new note hash → re-pin → OB rebuild).
    uint256 constant NOTE_CODE_HASH  = 0xacf19e140b58469a50165bcbda88cca952b2036678f1f5823b6a6bebd3fc32b1;
    uint16  constant NOTE_CODE_DEPTH = 20;

    // Canonical inference TokenContract (deal contract) code. placeSellOffer verifies
    // the sell offer's `tokenContract` derives from this pinned code + the seller's
    // statics — else a fill would route the BUYER's SHELL to a fake (the IOB is the
    // contract that forwards SHELL on a fill, so the check must live HERE, not only in
    // the note: placeSellOffer is public and a direct call would bypass a note check).
    uint256 constant TOKEN_CONTRACT_CODE_HASH  = 0xee4105b4800d852dde1a86cec4e270ecfa2ae0e199f05a46823aed792933e711;
    uint16  constant TOKEN_CONTRACT_CODE_DEPTH = 18;

    // Canonical RootModel code. The seller's per-deal TokenContract is bound to its RootModel
    // (its `_rootModelAddress` static is the seller's RootModel, NOT address(0)). To verify a
    // TokenContract the IOB first recomputes the seller's RootModel address from this pinned code
    // hash + the canonical SuperRoot, then derives the TC address from it (see _tokenContractAddr).
    // Re-pin whenever airegistry/RootModel is recompiled.
    uint256 constant ROOT_MODEL_CODE_HASH  = 0xe92a14cb9c5ac757e16be2f453d5c3a25e7bec90044a1389b97414d1b785cac8;
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
    //
    // THESE NUMBERS MUST NOT APPEAR IN THE SHARED TABLE. This contract INHERITS that table, so a
    // number declared in both resolves here to the local name and, everywhere a human looks it up,
    // to the shared one. The code still behaves correctly; the DIAGNOSIS breaks, which is worse
    // than a wrong number because it fails in the direction of confident wrong answers. Exit 333
    // used to read as `ERR_BOND_ALREADY_FUNDED` ("the bond is already funded") when the book meant
    // `ERR_NOT_DEPLOYER_NOTE`, and exit 336 as `ERR_OFFER_LIVE` when the book meant `ERR_EXPIRED`.
    // Both cost real debugging time, so they moved to 345/346 — free in BOTH tables. Check any new
    // number against both with `lint_error_code_collisions.py`.
    uint16 constant ERR_NOT_DEPLOYER_NOTE = 345;
    uint16 constant ERR_NO_LIQUIDITY      = 334;
    uint16 constant ERR_BAD_FLAGS         = 335;
    uint16 constant ERR_EXPIRED           = 346;
    uint16 constant ERR_QUEUE_FULL        = 340;
    uint16 constant ERR_BAD_TOKEN_CONTRACT = 342;
    uint16 constant ERR_NAME_TOO_LONG      = 343;
    uint16 constant ERR_BAD_MODEL_NAME     = 344;

    // Order-type flags (spec §8 / dex parity): per-order execution instructions, not
    // required to be symmetric between the two sides of a match (a taker IOC vs a
    // resting maker, etc.).
    uint8 constant FLAG_IOC       = 0x01;
    uint8 constant FLAG_FOK       = 0x02;
    uint8 constant FLAG_MARKET    = 0x04;
    uint8 constant FLAG_POST_ONLY = 0x08;
    uint8 constant TAKER_FLAGS    = 0x07;
    // ALL-OR-NONE: the order settles ONLY in full and ONLY against a SINGLE counterparty.
    // Partial fills are forbidden and so is assembling the quantity from several makers —
    // a maker that cannot cover the whole remaining amount is skipped, not partially taken.
    //
    // NOT the same as FLAG_FOK. FOK demands immediate full execution or cancellation; an AON
    // order may REST in the book and wait for a counterparty large enough to take it whole.
    // AON + IOC together give "all of it, from one maker, right now, else drop" (FOK from a
    // single counterparty); AON + POST_ONLY rests an all-or-none quote.
    //
    // A subscription is always AON: it is one buyer against one seller for the whole volume.
    uint8 constant FLAG_AON       = 0x20;
    // Capability flags: stable properties a buyer and a seller must AGREE on to match
    // (a buyer requiring the capability only fills against a maker that offers it, and
    // vice versa). Agreement is decided by `_dealCompatible`, never by order-type bits.
    // FLAG_TEE = the seller runs its inference inside a Trusted Execution Environment
    // (self-asserted; the contract stores the claim, it cannot verify a real TEE).
    // FLAG_TEE is a capability/requirement bit, ASYMMETRIC by order side: on a SELL it means
    // "seller offers a TEE endpoint", on a BUY it means "buyer requires a TEE endpoint". It is
    // orthogonal to the order-type bits (kept OUT of TAKER_FLAGS) and may combine with any of
    // them. Self-asserted — the book compares only the declared bits, never off-chain evidence.
    // Compatibility (see `_dealCompatible`): a BUY that requires TEE fills only a SELL that
    // offers it; every other combination is compatible. TEE attestation itself is ordinary
    // endpoint verification off-chain; a mismatch is handled by the existing dispute flow.
    uint8 constant FLAG_TEE       = 0x10;
    // SUBSCRIPTION: this BUY is a time-based subscription rather than a one-shot purchase.
    // Like FLAG_TEE it is a property of the DEAL, not an execution instruction: it survives the
    // match and is handed to the TokenContract, which switches its whole settlement branch on it
    // (weekly take-or-pay vs pay-per-consumed-token). Always accompanied by FLAG_AON — a
    // subscription is one seller for the full volume. The term itself does not travel with the
    // order: it is always one month, derived from this flag alone.
    uint8 constant FLAG_SUBSCRIPTION = 0x40;
    // The slice of `flags` that describes the DEAL (not the execution) and therefore travels to
    // the TokenContract on a fill. Execution bits (IOC/FOK/MARKET/POST_ONLY/AON) are consumed by
    // the book and never leave it.
    uint8 constant DEAL_FLAGS_MASK = 0x50;   // FLAG_TEE | FLAG_SUBSCRIPTION
    // Union of every supported flag bit; any bit outside this mask is rejected at the
    // order boundary so an unknown or mistyped bit can never rest or fill.
    uint8 constant SUPPORTED_FLAGS = 0x7F;

    // Gas / walk caps.
    uint8  constant MAX_MATCHES_PER_CALL = 30;
    uint8  constant MAX_CANCEL_PER_CALL  = 30;
    uint8  constant MAX_PRECHECK_LEVELS  = 40;
    // Per-call cap on TOTAL makers EXAMINED in a level-walk (purge / POST_ONLY precheck). The
    // resting book is unbounded and MAX_PRECHECK_LEVELS caps only price LEVELS, not makers within
    // a level — so without this a single packed level could exhaust the tx gas budget.
    uint16 constant MAX_SCAN_PER_CALL    = 100;
    // The POST_ONLY precheck gets a LARGER walk budget than the matching engine, because the two
    // walks cost different things: the precheck only reads orders and follows pointers, while a
    // matching pass that examines the same maker may also settle it — messages, escrow moves, book
    // writes. Exhausting the precheck budget answers conservatively (reject a POST_ONLY that may
    // not actually cross), and a deep run of incompatible or AON-mismatched makers is exactly when
    // that happens, so the budget is set where such a run is implausible rather than merely rare.
    uint16 constant MAX_PRECHECK_SCAN    = 400;
    // Ceiling on the value one queue-head transaction can send out. Derived, not guessed:
    // MAX_MATCHES_PER_CALL fills x 4 valued messages x REGISTER_FORWARD_VALUE, plus room for the
    // taker's own finalisation (placement mirror, offer-latch release, escrow refund) and the
    // self-call that continues the queue.
    //
    // THE FOURTH MESSAGE IS THE REMOVAL MIRROR. Three of them belong to the fill itself — the
    // handover to the deal and one confirmation to each side's note. The fourth comes from
    // `_removeFromBook`, the single point where an order ceases to exist: it tells the owner's
    // note, and a taker-BUY removes the maker on EVERY match, partial ones included. So a deep
    // buy pays it thirty times over, and it is not optional — the note's outstanding-order guard
    // is what would rest forever without it.
    //
    // Keep in step with MAX_MATCHES_PER_CALL and REGISTER_FORWARD_VALUE — raising either without
    // raising this leaves the deep-match path unfunded, and an under-funded action phase stops the
    // queue rather than one order. That is not hypothetical: this comment said THREE while the
    // code sent four, and the derivation went on looking correct while the budget silently no
    // longer covered it.
    uint64  constant MATCH_TX_BUDGET     = 1000 vmshell;

    // Queue (circular).
    // Why an order left the book. The note mirrors this to its owner, who otherwise sees one
    // undifferentiated "gone" for five different endings and cannot tell a cancel he asked for
    // from an expiry he did not, nor learn what came back.
    uint8 constant REMOVED_FILLED    = 1;   // consumed by a match
    uint8 constant REMOVED_CANCELLED = 2;   // the owner asked
    uint8 constant REMOVED_EXPIRED   = 3;   // the deadline passed
    uint8 constant REMOVED_DUST      = 4;   // below the tradeable minimum, refunded in full
    uint8 constant REMOVED_REJECTED  = 5;   // a cancel was refused; NOTHING was removed

    uint8 constant QENTRY_PLACE      = 1;
    uint8 constant QENTRY_CANCEL     = 2;
    uint8 constant QENTRY_CANCEL_ALL = 3;
    // Permissionless "remove if expired" — serialized through the same queue as place/cancel so it
    // cannot mutate the book between a taker's capped match-continuation txs.
    uint8 constant QENTRY_EXPIRE     = 4;
    uint8 constant QUEUE_CAPACITY    = 100;
    uint8 constant QUEUE_PLACE_LIMIT = 90;

    // InferenceOrderCancelRejected.reason — a cancel with no order or a foreign owner.
    uint8 constant CANCEL_REJ_NOT_FOUND = 0;
    uint8 constant CANCEL_REJ_NOT_OWNER = 1;
    // The queue was full when the cancel arrived. UNLIKE THE OTHER TWO, THE ORDER IS STILL ALIVE:
    // this reason says "ask again", not "it is gone", and the note must keep its record. Telling
    // the two apart is the whole reason a reason code exists here.
    uint8 constant CANCEL_REJ_QUEUE_FULL = 2;

    // InferenceOrderRejected.reason — a submission that never became an order, so it has no id to
    // report and no `InferenceOrderPlaced` was ever emitted for it.
    uint8 constant PLACE_REJ_POST_ONLY = 0;
    uint8 constant PLACE_REJ_FOK       = 1;
    uint8 constant PLACE_REJ_EXPIRED   = 2;
    // NOT A REJECTION. The request was ACCEPTED and has simply run out of road: a taker-only order
    // that met what liquidity there was, or a remainder too small to rest. The note is told through
    // the same entry point because it needs the same two things — the money and the number — but
    // the reason is distinct, because "we would not take this" and "we took it and it is finished"
    // are different facts to whoever reads the outcome.
    uint8 constant PLACE_END_TAKER     = 3;

    // Reference-price stats.
    uint64  constant SECS_PER_DAY  = 86400;
    uint128 constant MIN_LIQUIDITY = 1;
    // Minimum price step: order prices are quoted in whole SHELL (1e9 base units), so a price
    // must be a positive multiple of PRICE_STEP. Rejects sub-SHELL dust granularity.
    uint128 constant PRICE_STEP = 1_000_000_000;

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
        // The number the NOTE gave this request before it was sent (task Q). The book does not
        // interpret it and never generates one — it carries it so every answer can name which
        // request it answers. `uint64` and not wider on purpose: it has to survive a bounce, where
        // only the leading bits of the body come back.
        uint64  clientOrderId;
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

    // Head and tail of the level's FIFO. No running total is kept: nothing reads one, and a level
    // is always walked order by order anyway (AON, deal-shape and escrow checks are per-maker), so
    // a total would be a slot to write on every insert, removal and partial fill and never a
    // question anyone asks.
    struct PriceLevel { uint128 firstOrderId; uint128 lastOrderId; }
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
        uint64  clientOrderId;  // the note's own number for this request (task Q); 0 on non-place
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
    // Per-TC cumulative finalized-tick high-water mark. `reportFinalized` records only the
    // NEW delta into the reference-price stats, so a re-sent / bounced report never
    // double-counts (exactly-once, same shape as the OEL count) and only irreversibly
    // served ticks ever move the median.
    mapping(address => uint128) _finalizedSeen;

    address _deployerNote;

    // THE BOOK HOLDS NO MONEY AT ALL (generation 4.0.33) — not for a moment, and there is
    // deliberately no balance variable here.
    //
    // It used to have custody: escrow arrived as ECC[2] and sat on this account between placement
    // and fill. It no longer does. The book is a matcher and a registry of orders; the money lives
    // as records on the note and on the deal, and every figure the book moves is one it was already
    // recording anyway — `e.escrow`, per order. An aggregate balance would be nothing but the sum
    // of those records: a second source of truth for the same money, and the only thing a second
    // source of truth reliably does is drift from the first.
    //
    // So the escrow is charged where it is owned — `leftoverEscrow -= cost`, `_orders[cur].escrow =
    // mk.escrow - cost` — and the book passes the number on. Two things follow, and both are load-
    // bearing: no outgoing message from this contract may attach `currencies`, and no incoming one
    // may be read for them.
    //
    // WHAT THAT COSTS, STATED PLAINLY. Under currency an escrow proved itself — ECC on a message
    // cannot be invented, so `placeBuyOrder` could believe `msg.currencies` without asking who
    // called. A figure carries no such proof, so the derivation of the caller's note address now
    // does the work the currency used to do. That check is not a formality here; it is the entire
    // reason an `escrow` argument is not free money.

    /// @notice Buyer note behind each dispatched-but-unconfirmed handover, keyed by the DEAL.
    /// @dev    Not custody — the book holds nothing. It is the one fact a bounce cannot carry: the
    ///         bounced window fits the function id and `paid` and nothing more, so if the handover
    ///         comes back there would otherwise be no way to say whose escrow it was. Keyed by deal
    ///         because one match walk dispatches to many of them; cleared as soon as the handover
    ///         is answered either way.
    mapping(address => address) _handoverBuyer;

    // There is NO ledger of undelivered refunds here, and no `retryCredit` to drain one. Both
    // existed briefly and were removed: they answered a failure mode this rewrite had invented for
    // itself by letting a note refuse an incoming credit. A note cannot refuse — see
    // `PrivateNote.creditFromBook` — so the refusal, the ledger and the retry all go together.
    //
    // The wider rule they violated: NOTHING IN THIS SYSTEM RETRIES. A movement that did not go
    // through did not go through. A ledger of owed-but-unsent figures is a debt the contract
    // promises to settle later, and nothing here settles debts.

    event InferenceOrderPlaced(uint128 orderId, bool isBuy, uint256 price, uint128 ticks, address note, address tokenContract, uint64 deadline, uint8 flags);
    event InferenceOrderCancelled(uint128 orderId, uint128 refunded, address note);
    event InferenceOrderCancelRejected(uint128 orderId, uint8 reason, address note);
    event InferenceFilled(uint128 makerId, uint128 takerId, uint128 ticks, uint256 clearingPrice, address sellerTC, address buyerNote, address sellerNote);
    event InferenceExecuted(uint128 ticks, uint256 clearingPrice, uint128 cost);
    /// @notice An order that already carried an id is gone from the book, and `amount` is what came
    ///         back with it (zero on an ask, which holds no escrow). Together with
    ///         `InferenceOrderCancelled`, `InferenceOrderExpired` and `InferenceFilled` this closes
    ///         the set: every `InferenceOrderPlaced{orderId: N}` is followed by at least one event
    ///         carrying N, so an order never disappears unannounced. An expiring bid emits this
    ///         alongside `InferenceOrderExpired` — the refund and the reason are separate facts.
    event InferenceRefunded(uint128 orderId, address note, uint128 amount);
    /// @notice A submission refused before it became an order — a crossing POST_ONLY, an
    ///         under-liquid FOK, or a deadline that lapsed while it waited in the queue. No id was
    ///         ever assigned and no placement was announced, so the client matches this to its
    ///         request by `note`; `refund` is the escrow handed straight back (zero on an ask,
    ///         whose deal TC is released instead and named in `tokenContract`).
    event InferenceOrderRejected(uint8 reason, address note, address tokenContract, uint128 refund);
    /// @notice A resting order was removed because its deadline passed (§2.1.1). `isBuy` tells the
    ///         side; `tokenContract` is the freed deal TC on the ask side, `address(0)` on a bid.
    event InferenceOrderExpired(uint128 orderId, bool isBuy, address note, address tokenContract);
    /// @notice Emitted once at book deploy — lets an indexer map `modelHash` → the verified model name
    ///         (and which note opened the market). `modelName` is the genuine sha256 preimage.
    event InferenceOrderBookDeployed(address note, uint256 modelHash, string modelName);

    // ========================================================
    // Deploy guard (spec §2/§8)
    // ========================================================

    function _noteAddrFromHash(uint256 depositHash) private pure returns (address) {
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
    function _rootModelAddr(uint256 ownerPubkey) private pure returns (address) {
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
    function _tokenContractAddr(uint256 sellerPubkey, uint64 nonce) private pure returns (address) {
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

    /// @notice Top the balance up to cover the WORST transaction the queue head can turn into,
    ///         rather than to the idle floor.
    ///
    ///         A queue-head pass is the one place where this contract's outbound value is not
    ///         proportional to what came in. An order arrives carrying a vmshell or two, and
    ///         settling it can send three valued messages per fill — the deal funding plus a
    ///         confirmation mirror to each side's note — up to `MAX_MATCHES_PER_CALL` times, with
    ///         the taker's own finalisation and the self-call on top. Spend therefore outruns
    ///         income structurally, and the balance lives near the floor, which is precisely where
    ///         a deep match is not funded.
    ///
    ///         Running short in the ACTION phase does not drop a message — it aborts the whole
    ///         transaction. The head is not consumed, and the retry aborts identically, so the
    ///         queue stops for everyone rather than for one order. Funding the ceiling up front is
    ///         what keeps a busy book from stalling on its own success.
    function ensureMatchBudget() private pure {
        if (address(this).balance > MATCH_TX_BUDGET) { return; }
        gosh.mintshellq(MATCH_TX_BUDGET);
    }

    /// @notice Pass `amount` on to the note `to` (generation 4.0.33) — refunds, leftovers, cancels.
    /// @dev    The book subtracts nothing of its own here, because it owns nothing: the figure was
    ///         charged against the ORDER (`e.escrow`) by the caller, and this hands the same figure
    ///         to the note that owned that order. One record down, one record up.
    ///
    ///         Every `to` on this path is a note the book learned from an order it authenticated at
    ///         placement, so it is not naming arbitrary addresses; the note checks anyway, deriving
    ///         this book from `_modelHash` — one book per model — and rejecting anything else.
    ///
    ///         `bounce: true` and a pending record, for the same reason as the handover below: a
    ///         credit that never lands must not evaporate a buyer's refund.
    /// @dev `view`, not the default: since the book stopped holding money this touches no state of
    ///      its own — it reads `_modelHash` and passes a figure on. That the compiler can say so is
    ///      itself the check that the custody removal is complete on this path.
    /// @notice Answer a placement that was refused without ever becoming an order (task Q).
    /// @dev    THE THIRD OUTCOME FAMILY, and the one that had no answer at all. A POST_ONLY that
    ///         would cross, an FOK that cannot fill whole, a request whose deadline had already
    ///         passed — the book returns normally in each case, so there is no bounce; and no order
    ///         id was ever assigned, so nothing in the refund identified the request. The note saw
    ///         a credit appear and had to guess what it was for.
    ///
    ///         ONE MESSAGE CARRIES BOTH the refund and the number, and that is deliberate. Sending
    ///         the money as an ordinary credit and the reason as a second message would leave a
    ///         window where the note has cleared its pending record and has not yet been paid —
    ///         which is exactly the state the pending record exists to forbid.
    ///
    ///         `bounce: false`, like every other mirror: the note is derived, present, and has no
    ///         branch that can refuse this.
    function _rejectToNote(address note, uint64 clientOrderId, uint8 reason, uint128 refunded) private view {
        IPrivateNote(note).onInferenceRejected{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, clientOrderId, reason, refunded);
    }

    function _payShell(address to, uint128 amount) private view {
        if (amount == 0) { return; }
        IPrivateBalanceFromBook(to).creditFromBook{value: 1 vmshell, bounce: true, flag: 1}(
            amount, _modelHash);
    }

    /// @notice Recover a figure whose credit never landed (generation 4.0.33).
    /// @dev    The book has NO BALANCE to restore into — it holds nothing — so a bounce cannot be
    ///         answered by putting money back on a total. It has to be re-aimed at the party the
    ///         figure belongs to, and that means knowing WHO, which the bounce itself cannot say:
    ///         the window carries the leading bits of the body, and `paid` (128) already fills it
    ///         alongside the function id (32); a note address is 267 bits and would not fit beside
    ///         them however the parameters are ordered. So `msg.sender` answers "who bounced" and
    ///         `_inFlight` answers "on whose behalf" — a record of dispatched-but-unconfirmed
    ///         figures, not custody of money.
    ///
    ///         It is keyed by counterparty rather than held in one slot BECAUSE ONE MATCH WALK
    ///         HANDS OFF TO MANY DEALS — up to `MAX_MATCHES_PER_CALL` — so a single pending field,
    ///         the shape a note can afford for its one-at-a-time transfer, could not say which of
    ///         them failed.
    onBounce(TvmSlice body) external {
        // A bounce arrives with whatever value survived the failed hop, which can be very little,
        // and the handover branch below SENDS — it re-aims the escrow at the buyer. The book lives
        // in a configured dapp and can mint, so it tops itself up rather than letting a buyer's
        // refund depend on how much gas happened to come back. (`PrivateNote.onBounce` does the
        // same; the deal's cannot, and that is called out where it is written.)
        ensureBalance();
        uint32 functionId = body.load(uint32);
        // There is NO branch for a bounced `creditFromBook`, and that is the design, not an
        // omission: a note cannot refuse money, so a refund it was sent is a refund it took. The
        // branch that stood here recorded such refusals into a `_strandedRefund` ledger a
        // `retryCredit` could re-send — a cure for an illness this rewrite briefly invented by
        // guarding the note's credit entries. With the guards gone the illness is gone, and a
        // retry ledger would only be a place for debts to accumulate that nothing ever settles.
        // Nothing in this system retries: a movement that did not go through did not go through.
        if (functionId == abi.functionId(ITokenContractDeal.fundFromOrderBook)) {
            // The deal refused the handover or does not exist. The escrow belongs to the BUYER, not
            // to the deal that failed, which is why the buyer's note was recorded at dispatch: it
            // cannot be read back out of the bounce. This is a REDIRECT, not a retry — the figure
            // goes to a different party than the one that turned it down.
            uint128 paid = body.load(uint128);
            address buyerNote = _handoverBuyer[msg.sender];
            delete _handoverBuyer[msg.sender];
            if (buyerNote.value != 0) { _payShell(buyerNote, paid); }
        }
    }

    /// @notice The deal confirms it credited a handover; the pending record is dropped.
    /// @dev    Without this the `_handoverBuyer` entry would outlive the fill it was written for —
    ///         one row per funded deal, never cleared, in a book that funds deals for a living.
    ///         It also makes the record mean what its name says: entries that remain are handovers
    ///         still unanswered, so a stuck one is visible instead of buried among successes.
    ///
    ///         Deleting on the DEAL'S OWN say-so is safe because deletion can only lose the record
    ///         of a handover that succeeded: a deal that lies here erases its own claim to a refund
    ///         it never received, which harms nobody but itself. Deriving the caller canonically
    ///         would prove it is a real deal and still not prove it is the one that was paid, so
    ///         the entry's own key does that job — a deal with no entry deletes nothing.
    function onHandoverAccepted() public {
        ensureBalance();
        delete _handoverBuyer[msg.sender];
    }


    /// @notice Buyer note behind an unanswered handover to `deal`, or `addr_none` if there is none.
    function getHandoverBuyer(address deal) external view returns (address) { return _handoverBuyer[deal]; }

    function _tickFee(uint256 p) private pure returns (uint256) {
        return (p * uint256(PLATFORM_FEE_BPS)) / uint256(BPS_DENOMINATOR);
    }
    function _unit(uint256 p) private pure returns (uint256) { return p + _tickFee(p); }

    /// @notice Both capability rules a pair must satisfy to settle, in one place so `_match` and
    ///         the two prechecks cannot drift apart. Applied everywhere executable liquidity is
    ///         inspected, so all three classify a given maker identically. The prechecks still part
    ///         ways with `_match` once their walk budgets run out: only `_match` resumes across
    ///         transactions, so each precheck then answers in its own conservative direction —
    ///         reject the placement rather than let it fill past what the walk inspected.
    ///
    ///         TEE is ASYMMETRIC: a BUY that REQUIRES it fills only a SELL that OFFERS it; the
    ///         other three combinations are fine.
    ///
    ///         SUBSCRIPTION is SYMMETRIC — a pairing, not a requirement: a subscription bid matches
    ///         only a seller who declared he takes subscriptions, and such a seller matches ONLY
    ///         subscriptions. A weekly take-or-pay obligation is a different product from a one-shot
    ///         purchase, so neither side may be dragged into the other's shape by a mere price cross.
    function _dealCompatible(bool takerIsBuy, uint8 takerFlags, uint8 makerFlags) private pure returns (bool) {
        uint8 buyFlags  = takerIsBuy ? takerFlags : makerFlags;
        uint8 sellFlags = takerIsBuy ? makerFlags : takerFlags;
        if ((buyFlags & FLAG_TEE) != 0 && (sellFlags & FLAG_TEE) == 0) { return false; }
        return (buyFlags & FLAG_SUBSCRIPTION) == (sellFlags & FLAG_SUBSCRIPTION);
    }

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
            _levels[o.isBuy][o.price] = PriceLevel({firstOrderId: orderId, lastOrderId: orderId});
        } else {
            _orders[priceTail].nextAtPrice = orderId;
            level.lastOrderId = orderId;
            _levels[o.isBuy][o.price] = level;
        }
        if (oTail == 0) { _ownerHead[o.note] = orderId; } else { _orders[oTail].nextInOwner = orderId; }
        _ownerTail[o.note] = orderId;
        _orderCount++;

        // TELL THE OWNER'S NOTE THE ORDER IS RESTING. This is the only place in the book where an
        // order STARTS existing, exactly as `_removeFromBook` is the only place one stops — so the
        // note's outstanding-order record is written and cleared from a matched pair of points, and
        // an order can neither be recorded without resting nor rest without being recorded.
        //
        // IT USED TO BE SENT AT PLACEMENT, BEFORE MATCHING, AND THAT WAS THE BUG. A taker that
        // filled completely — every IOC/FOK/MARKET, and any limit order the book could satisfy —
        // was announced as resting and then never inserted, so nothing ever removed it. The note
        // kept the record forever, `_restingInf` never emptied, and `withdrawTokens` refused for
        // good: a guard written against withdrawing TOO EARLY turned into withdrawing NEVER.
        //
        // The client event `InferenceOrderPlaced` deliberately stays at the placement site. The two
        // signals mean different things and now say so: the event means ACCEPTED, this mirror means
        // RESTING. Conflating them is what produced the defect.
        //
        // `bounce: false`, like the removal mirror: a note that cannot take this must not be able
        // to stall an insertion, which happens inside the match walk and would stall it for both
        // sides.
        IPrivateNote(o.note).onInferencePlaced{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, o.tokenContract, orderId, o.clientOrderId, o.isBuy, o.price, o.amount);
    }

    function _removeFromBook(uint128 orderId, uint8 cause, uint128 refunded) private {
        Order o = _orders[orderId];
        // Idempotency guard: a removed/empty slot has amount 0. Prevents a double
        // _removeFromBook from underflowing _orderCount below (see OrderBook).
        if (o.amount == 0) { return; }
        uint128 prevP = o.prevAtPrice;
        uint128 nextP = o.nextAtPrice;
        PriceLevel level = _levels[o.isBuy][o.price];
        if (prevP == 0) { level.firstOrderId = nextP; } else { _orders[prevP].nextAtPrice = nextP; }
        if (nextP == 0) { level.lastOrderId = prevP; } else { _orders[nextP].prevAtPrice = prevP; }
        if (level.firstOrderId == 0) { delete _levels[o.isBuy][o.price]; } else { _levels[o.isBuy][o.price] = level; }

        uint128 oPrev = o.prevInOwner;
        uint128 oNext = o.nextInOwner;
        if (oPrev == 0) { _ownerHead[o.note] = oNext; } else { _orders[oPrev].nextInOwner = oNext; }
        if (oNext == 0) { _ownerTail[o.note] = oPrev; } else { _orders[oNext].prevInOwner = oPrev; }

        // TELL THE OWNER'S NOTE THE ORDER IS GONE. This is the only place in the book where an
        // order ceases to exist — cancel, expiry and fill all arrive here — so one send covers all
        // three reasons, and no fourth reason can be added without passing through this line.
        //
        // Without it the note would never learn that a resting order was removed: a cancel or an
        // expiry refunds through `creditFromBook`, which carries an amount and a model hash and no
        // identifier at all, so the record written at placement would rest forever and the note
        // would become permanently unwithdrawable — a worse failure than the one being fixed.
        //
        // `bounce: false` and deliberately non-blocking: a note that cannot take this message must
        // not be able to stall a removal, because removals happen inside the match walk and would
        // stall it for BOTH sides. The cost of that choice is stated at the note's counters — a
        // mirror that never arrives leaves the guard silent rather than broken.
        IPrivateNote(o.note).onInferenceOrderRemoved{
            value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false
        }(_modelHash, orderId, cause, refunded);

        delete _orders[orderId];
        _orderCount--;
    }

    // ========================================================
    // Fill settlement (spec §2.3 → §3)
    // ========================================================

        /// @param dealFlags The DEAL slice of the buyer's order flags (`DEAL_FLAGS_MASK`: TEE +
    ///        SUBSCRIPTION). Execution bits stay in the book; these describe what was bought and
    ///        select the TokenContract's settlement branch.
    function _settleFill(uint128 makerId, uint128 takerId, address buyerNote, address sellerNote, uint256 buyerPubkey, address sellerTC, uint128 trade, uint256 clearing, bool takerIsBuy, uint8 dealFlags, uint64 takerClientId) private returns (uint128) {
        uint128 cost = uint128(uint256(trade) * _unit(clearing));
        // A subscription carries the buyer's own `2P` on top of the escrow it spends. The book does
        // not hold it — it forwards deposit and bond together and the TC separates them — but the
        // book is where the money is, so the amount is settled here, at the CLEARING price the deal
        // is actually struck at. `placeBuyOrder` made room for it against the limit price, and
        // clearing never exceeds that, so the escrow always covers this.
        uint128 bond = (dealFlags & FLAG_SUBSCRIPTION) != 0 ? uint128(2 * clearing) : 0;
        uint128 debit = cost + bond;
        // The handover (generation 4.0.33). What used to ride as ECC[2] on this message is now the
        // leading argument. The book takes nothing off itself — it never held this; the figure was
        // charged against the buyer's order — and the deal believes the number only because it
        // re-derives this book from `_modelHash` before crediting anything.
        //
        // `bounce: true`, where the currency version could afford `bounce: false`. Then, a refused
        // fill left the SHELL sitting on the deal's account and the deal refunded it onward, so the
        // money was somewhere real either way. Now a refusal means the buyer's order was debited for
        // a credit that never happened, and the bounce is the only thing that says so. The deal
        // still refunds non-fundable fills itself rather than reverting; the bounce covers the
        // narrower case where it never got that far — a call arriving before RootPN told it which
        // book to trust, or a deal that no longer exists.
        //
        // The buyer is recorded FIRST, before the call: the escrow is his, and if this comes back
        // the bounce can carry the figure but not the name.
        _handoverBuyer[sellerTC] = buyerNote;
        ITokenContractDeal(sellerTC).fundFromOrderBook{
            value: REGISTER_FORWARD_VALUE, flag: 1, bounce: true
        }(debit, buyerNote, buyerPubkey, dealFlags);
        _executedNotional += uint128(uint256(trade) * clearing);
        _executedTicks    += trade;
        _matchSeq += 1;
        // Reference-price volume is NOT recorded here: a match only reserves ticks that
        // can still be unwound (funded-but-never-opened refunds the buyer in full via the
        // TC's cleanupUnopened), so counting it would let reserved-then-refunded volume
        // move the median for ~gas. Volume is recorded only from the TC's authenticated
        // cumulative finalized-tick reports (`reportFinalized`), i.e. ticks actually served
        // with payment + platform fee already irreversible.
        emit InferenceFilled{dest: address.makeAddrExtern(MatchedEmit, bitCntAddress)}(makerId, takerId, trade, clearing, sellerTC, buyerNote, sellerNote);
        emit InferenceExecuted{dest: address.makeAddrExtern(ExecutedEmit, bitCntAddress)}(trade, clearing, cost);

        // Owner-facing confirmation mirrors: push the deal `sellerTC` into each side's note so the
        // owner reads only its note's ext-out. Each side gets ITS own order id (maker/taker depends
        // on which side is the taker). bounce:false — the mirror is best-effort and never blocks the fill.
        uint128 buyerOrderId  = takerIsBuy ? takerId : makerId;
        uint128 sellerOrderId = takerIsBuy ? makerId : takerId;
        // EACH SIDE GETS ITS OWN CLIENT NUMBER, and only one of the two is at hand. The MAKER is
        // resting, so its number is in storage under its id; the TAKER may never have been
        // inserted, so its number arrives as an argument. Reading both from storage would work for
        // the maker and silently yield zero for the taker — the case this whole task is about.
        uint64 makerClientId  = _orders[makerId].clientOrderId;
        uint64 buyerClientId  = takerIsBuy ? takerClientId : makerClientId;
        uint64 sellerClientId = takerIsBuy ? makerClientId : takerClientId;
        // `debit` — WHAT ACTUALLY LEFT THE ESCROW, not `trade * clearing`. The two differ by the
        // subscription bond, and the note subtracts this figure from its in-flight record, so a
        // recomputed approximation would leave that record permanently short or permanently over.
        // The seller side carries it too and ignores it: a sell has no client record to reduce.
        IPrivateNote(buyerNote).onInferenceFilled{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, sellerTC, buyerOrderId, buyerClientId, trade, clearing, debit, true);
        IPrivateNote(sellerNote).onInferenceFilled{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(
            _modelHash, sellerTC, sellerOrderId, sellerClientId, trade, clearing, debit, false);
        // The BUYER's escrow is debited by what actually left the book, bond included — the caller
        // subtracts this from `leftoverEscrow` or the maker's stored escrow, and an amount short of
        // what was sent would leave the book crediting escrow it no longer holds.
        return debit;
    }

    /// @notice Precise FOK pre-check: read-only simulate the exact `_match` fill and report
    ///         whether the taker FULLY fills `need`, so FLAG_FOK is truly all-or-nothing.
    /// @dev    Mirrors the rules that decide whether a maker can be TAKEN and for how much —
    ///         per-order min-fill = 2, the taker-SELL one-deal-slot (one maker, then stop),
    ///         escrow affordability against the running `leftEscrow`, the expired-GTD skip and
    ///         the §8 subscription cycle budget — instead of summing raw level totals.
    ///         Fail-closed: a maker examined past MAX_MATCHES_PER_CALL (a fill `_match` could
    ///         not settle in one atomic tx) or a walk past MAX_PRECHECK_LEVELS returns false, so
    ///         a FOK that cannot complete in a single transaction is rejected rather than allowed
    ///         to partially fill.
    ///
    ///         What it deliberately does NOT re-derive is the subscription buyer's `2P` bond, and
    ///         the reason is where the invariant lives rather than an omission here. Coverage of
    ///         `cost + bond` is established once, at the boundary, by the entry guard in
    ///         `placeBuyOrder` — `escrow >= ticks * unit(limit) + 2 * limit` — so an order that
    ///         reaches this walk is already funded for the bond at its own limit, and a fill can
    ///         only clear at or better than that limit. `SUBSCRIPTION` implies `AON`, so there is
    ///         no partial fill for a bond to be apportioned across, and the amount this simulation
    ///         is asked about is the whole order or nothing. Re-subtracting the bond from
    ///         `leftEscrow` here would therefore charge it twice against the same guarantee and
    ///         reject fills the guard has already funded. The rule is the general one: an invariant
    ///         proven at the edge is not re-proven in the loop, and a doc comment claiming the loop
    ///         mirrors EVERY rule promises more than the function needs to do.
    function _fokFullyFillable(bool takerIsBuy, uint256 takerPrice, bool isMarket, uint128 need, uint128 escrow, uint8 takerFlags) private view returns (bool) {
        uint128 remaining = need;
        uint128 leftEscrow = escrow;
        uint8 matches = 0;
        uint8 walked = 0;
        uint16 scanned = 0;
        optional(uint256, PriceLevel) it = _bestOpposite(takerIsBuy);
        while (it.hasValue() && remaining > 0) {
            (uint256 lp, ) = it.get();
            if (!_crosses(takerIsBuy, takerPrice, isMarket, lp)) { break; }
            // Clearing = the seller's ask both directions (Variant 2), constant per level.
            uint256 unit = _unit(takerIsBuy ? lp : takerPrice);
            uint128 cur = _levels[!takerIsBuy][lp].firstOrderId;
            while (cur != 0 && remaining > 0) {
                // Budgeted exactly as `_match` budgets itself, because a divergence here is a
                // false answer: examining a maker costs the SCAN budget, and only makers that
                // cost `_match` real work — a fill, or a removal it performs on the way — cost the
                // MATCH budget. Charging every examined maker to the match budget rejected FOKs
                // after thirty skipped makers that `_match` would have walked past for free and
                // filled atomically. The escrow comes back on a rejection, so this was liveness
                // rather than money, but the answer was still wrong.
                if (matches >= MAX_MATCHES_PER_CALL || scanned >= MAX_SCAN_PER_CALL) { return false; }
                scanned++;
                Order mk = _orders[cur];
                uint128 nextOrd = mk.nextAtPrice;
                // TEE-incompatible: `_match` skips this maker, so the FOK simulation must too —
                // otherwise mixed TEE/non-TEE volume could pass FOK then fail/partial for lack
                // of compatible volume.
                if (!_dealCompatible(takerIsBuy, takerFlags, mk.flags)) { cur = nextOrd; continue; }

                // Expired maker (EITHER side — both now carry deadlines): `_match` drops it
                // instead of settling it, so the FOK simulation must skip it too. The drop is a
                // book write and a message there, charged to the match budget — mirrored here.
                if (_isExpired(mk.deadline)) { matches++; cur = nextOrd; continue; }

                uint128 budget = takerIsBuy ? leftEscrow : mk.escrow;

                uint128 trade = remaining < mk.amount ? remaining : mk.amount;
                uint128 afford = unit > 0 ? uint128(uint256(budget) / unit) : trade;
                if (afford < trade) { trade = afford; }

                // AON on either side — mirror of `_match`, so the FOK simulation counts only
                // volume `_match` would actually settle (see the AON branch there).
                if (((takerFlags & FLAG_AON) != 0 && trade < remaining)
                 || ((mk.flags   & FLAG_AON) != 0 && trade < mk.amount)) { cur = nextOrd; continue; }

                if (trade < 2) {
                    // BUY: cannot take >= 2 from the best crossing SELL, and pricier levels
                    // afford even less → it can never complete. SELL: this bid is un-fillable
                    // (dust / no budget); `_match` REMOVES it, which is work — charged as such.
                    if (takerIsBuy) { return false; }
                    matches++;
                    cur = nextOrd;
                    continue;
                }

                remaining -= trade;
                matches++;                              // a settled fill, as in `_match`
                if (takerIsBuy) { leftEscrow -= uint128(uint256(trade) * unit); }
                // Taker SELL is one deal → one fill then stop: it fully fills only if this
                // single maker covered the whole `need`.
                if (!takerIsBuy) { return remaining == 0; }
                cur = nextOrd;
            }
            walked++;
            if (walked >= MAX_PRECHECK_LEVELS) { return false; }
            it = _nextOpposite(takerIsBuy, lp);
        }
        return remaining == 0;
    }

    /// @notice POST_ONLY cross test against the same executable maker set `_match` and
    ///         `_fokFullyFillable` use: walk opposite levels best-first, skip makers `_match`
    ///         would not settle — expired makers on EITHER side — and report whether the FIRST
    ///         genuinely executable maker crosses `takerPrice`.
    ///         Both walk budgets — MAX_PRECHECK_SCAN per maker and MAX_PRECHECK_LEVELS per level —
    ///         fail closed: returning "no cross" lets `_doPlaceHead` call `_match`, which neither
    ///         budget binds (its own per-tx cap resumes across txs via the queue cursor), so it
    ///         could settle a maker deeper than this walk reached and violate POST_ONLY.
    function _executableCrosses(bool takerIsBuy, uint256 takerPrice, uint8 takerFlags, uint128 takerAmount) private view returns (bool) {
        uint8 walked = 0;
        uint16 scanned = 0;
        optional(uint256, PriceLevel) it = _bestOpposite(takerIsBuy);
        while (it.hasValue()) {
            (uint256 lp, ) = it.get();
            if (!_crosses(takerIsBuy, takerPrice, false, lp)) { return false; }   // ordered levels → no worse level crosses
            uint128 cur = _levels[!takerIsBuy][lp].firstOrderId;
            while (cur != 0) {
                // Bound the walk (the resting book is unbounded). Exhaustion must fail closed:
                // `_doPlaceHead` interprets false as safe to call `_match`, which can continue past
                // this cap and consume a deeper crossing maker. Counted BEFORE the TEE skip below
                // so incompatible makers also consume the budget.
                scanned++;
                if (scanned >= MAX_PRECHECK_SCAN) { return true; }
                Order mk = _orders[cur];
                // TEE-incompatible: `_match` never settles this pair, so POST_ONLY does not cross
                // it — an incompatible best quote must not reject an otherwise-valid POST_ONLY.
                if (!_dealCompatible(takerIsBuy, takerFlags, mk.flags)) { cur = mk.nextAtPrice; continue; }
                // Expired maker (either side) is non-executable — `_match` drops it, never settles
                // it, so it must not make an otherwise-valid POST_ONLY reject.
                if (_isExpired(mk.deadline)) { cur = mk.nextAtPrice; continue; }
                // AON on either side: `_match` skips a counterparty that cannot take/give the FULL
                // amount, so such a maker is not a cross either. Size only — escrow affordability is
                // deliberately NOT modelled here: ignoring it can only make this walk report a cross
                // `_match` would skip, i.e. reject a POST_ONLY that could have rested. That is the
                // fail-closed direction; the opposite (resting an order that then crosses) is not.
                if (((takerFlags & FLAG_AON) != 0 && mk.amount < takerAmount)
                 || ((mk.flags   & FLAG_AON) != 0 && takerAmount < mk.amount)) { cur = mk.nextAtPrice; continue; }
                // Below the minimum fill: `_match` cannot trade a maker of under two ticks and
                // removes it as dust instead, so it is not a cross either. Unlike the escrow
                // approximation above, leaving this out does not merely err on the safe side — a
                // POST_ONLY rejection returns without ever calling `_match`, so nothing sweeps the
                // dust and the level stays unusable for as long as the maker rests. A bid carries
                // no mandatory deadline, so that can be indefinitely.
                if (mk.amount < 2) { cur = mk.nextAtPrice; continue; }
                return true;                            // a real crossing maker → POST_ONLY must reject
            }
            walked++;
            // Level budget exhausted: fail closed for the same reason as the per-maker cap above.
            // `_match` is not bound by this budget and resumes across txs, so answering "no cross"
            // here could still end in a fill on a level this walk never reached.
            if (walked >= MAX_PRECHECK_LEVELS) { return true; }
            it = _nextOpposite(takerIsBuy, lp);
        }
        return false;
    }

    // ========================================================
    // Matching engine (best-first, price→time, bounded; resumes via the queue)
    // ========================================================

    /// @return remaining unfilled ticks, leftoverEscrow, capped (true = hit the
    ///         per-tx cap with crossing liquidity left → caller continues).
    function _match(
        uint128 takerId, bool takerIsBuy, uint256 takerPrice, bool isMarket,
        address takerNote, address takerTC, uint256 takerBuyerPubkey, uint128 amount, uint128 buyEscrow,
        uint128 resumeFrom, uint8 takerFlags, uint64 takerClientId
    ) private returns (uint128 remaining, uint128 leftoverEscrow, bool capped, uint128 nextResume) {
        remaining = amount;
        leftoverEscrow = buyEscrow;
        capped = false;
        nextResume = 0;
        // TWO budgets, because a maker costs one of two very different things. SETTLING one builds
        // outbound messages and rewrites the book, so those are capped tight (MAX_MATCHES_PER_CALL).
        // EXAMINING one that turns out to be unusable — wrong deal shape, AON that does not fit,
        // dust — is a read and a pointer step, so those get the much larger walk budget
        // (MAX_SCAN_PER_CALL). Charging a skip as if it were a fill made a run of incompatible
        // makers stretch a single order across many transactions for no work done.
        uint8  matches = 0;
        uint16 scanned = 0;

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
                if (matches >= MAX_MATCHES_PER_CALL || scanned >= MAX_SCAN_PER_CALL) {
                    return (remaining, leftoverEscrow, true, cur);
                }
                scanned++;
                Order mk = _orders[cur];
                uint128 nextOrd = mk.nextAtPrice;
                // Deal-shape filter (TEE requirement, SUBSCRIPTION pairing). An incompatible maker
                // is SKIPPED — it stays resting untouched in its queue position for a counterparty
                // that can use it — and costs only the walk budget, since nothing is settled or
                // written. Mirrored in _fokFullyFillable / _executableCrosses so the prechecks make
                // the same decisions.
                if (!_dealCompatible(takerIsBuy, takerFlags, mk.flags)) { cur = nextOrd; continue; }
                // Clearing = the SELLER's ask, both directions (Variant 2):
                //  - taker BUY: lp = the maker SELL's ask (already the seller's price);
                //  - taker SELL: takerPrice = the taker SELL's ask (_pricePerTick), NOT the
                //    maker BID's lp. This caps the fund at maxTicks*unit(ask) so the TC is
                //    never over-funded; the bid-ask spread stays in the maker buyer's escrow
                //    and refunds via the residual path. SELLs are limit-only (no market), so
                //    takerPrice > 0 here.
                uint256 clearing = takerIsBuy ? lp : takerPrice;

                // Maker resting past its deadline: drop it inline before it can settle as live
                // liquidity, and emit `InferenceOrderExpired` (via the removal wrappers).
                // SIDE-NEUTRAL — both sides carry deadlines now: a GTD limit BUY refunds the
                // buyer's escrow, a SELL offer frees its deal TC's latch (it holds no escrow).
                // Each drop is ONE output action, so count it toward MAX_MATCHES_PER_CALL: a run
                // of expired makers stays bounded per tx and resumes via the queue (the cap check
                // above returns `capped` with the cursor).
                if (_isExpired(mk.deadline)) {
                    matches++;
                    if (takerIsBuy) { _removeExpiredSell(cur); } else { _removeExpiredBid(cur); }
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
                uint128 afford = unit > 0 ? uint128(uint256(budget) / unit) : trade;
                if (afford < trade) { trade = afford; }

                // ALL-OR-NONE, both directions. Checked AFTER the escrow cap, because a fill the
                // budget cannot cover is not a fill at all:
                //  - an AON TAKER must leave with `remaining == 0` from ONE maker, so a maker that
                //    cannot cover the whole remainder is skipped rather than partially taken;
                //  - an AON MAKER settles only whole, so a taker that cannot absorb its full
                //    `amount` must leave it untouched.
                // A skip settles nothing and writes nothing, so it costs the walk budget only; the
                // run of too-small counterparties stays bounded and resumes via the queue cursor.
                if (((takerFlags & FLAG_AON) != 0 && trade < remaining)
                 || ((mk.flags   & FLAG_AON) != 0 && trade < mk.amount)) {
                    cur = nextOrd;
                    continue;
                }
                // Min-fill = 2 ticks: a deal needs probe + >=1 stream tick, and
                // fundFromOrderBook rejects a sub-2 fund. A 0/1-tick fill is not a settleable
                // deal. trade==0 is un-tradeable; trade==1 is a sub-2 dust remainder.
                if (trade < 2) {
                    // Taker BUY can't take >=2 from the best SELL (remaining<2, or escrow
                    // affords <2 and pricier levels afford even less) -> stop; _finalizeTaker
                    // refunds the sub-2 remainder rather than resting it as dust.
                    if (takerIsBuy) { return (remaining, leftoverEscrow, false, 0); }
                    // This one DOES settle: the dust bid is refunded and removed, which is an
                    // outbound message and a book write, so it is charged to the match budget like
                    // a fill. On the cap it returns `capped` and resumes; the removals shrink the
                    // book, so the resume makes progress (no re-scan of the same dust).
                    matches++;
                    // Bid offering < 2 ticks (0 = un-tradeable, or a 1-tick dust remainder
                    // left by a partial fill): refund the owner IN FULL and remove it. Removing —
                    // rather than skipping — keeps the scan bounded and stops dust from
                    // accumulating in the book.
                    _refundAndRemove(cur, REMOVED_DUST);
                    cur = nextOrd;
                    continue;
                }

                // The deal descriptors are always the BUYER's: a subscription is bought, not sold.
                uint8 buyFlags    = takerIsBuy ? takerFlags    : mk.flags;
                // MAKER IS THE RESTING ORDER AND TAKER IS THE INCOMING ONE, always. Which side
                // buys does not enter into it, and selecting them by `takerIsBuy` here was the
                // defect: on a taker SELL it handed `makerId` the incoming id and `takerId` the
                // resting one. `_settleFill` then swaps by the same flag to decide which id
                // belongs to the buyer — correctly — so the two swaps composed and every note
                // received its COUNTERPARTY's order id, in the mirror and in the event alike.
                // The swap inside `_settleFill` is right and stays; it was being fed already
                // reversed, and fixing both would reverse a third time.
                uint128 cost = _settleFill(cur, takerId, buyerNote, sellerNote, buyerPubkey, sellerTC, trade, clearing, takerIsBuy, buyFlags & DEAL_FLAGS_MASK, takerClientId);

                if (takerIsBuy) { leftoverEscrow -= cost; } else { _orders[cur].escrow = mk.escrow - cost; }

                _orders[cur].filledAccum += trade;
                // SELL offer = one-deal slot → consumed on match (taker BUY), even
                // on partial. BUY maker (taker SELL) is reduced (spans deals).
                if (takerIsBuy) {
                    _removeFromBook(cur, REMOVED_FILLED, 0);    // maker SELL: no buyer escrow to return
                } else if (mk.amount == trade) {
                    // Fully-filled maker BUY: the residual escrow (over-fund + clearing-remainder)
                    // returns to the buyer.
                    _refundAndRemove(cur, REMOVED_FILLED);
                } else {
                    _orders[cur].amount = mk.amount - trade;
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

    /// @notice A resting order is expired when its non-zero deadline has passed. Deadline-based, so
    ///         it is SIDE-NEUTRAL: it governs GTD bids AND (mandatory-deadline) SELL asks alike.
    ///         Single source of the expiry rule shared by the match loop, the view prechecks and
    ///         the permissionless `expireOrder`.
    function _isExpired(uint64 deadline) private pure returns (bool) {
        return deadline != 0 && block.timestamp >= deadline;
    }

    function _refundAndRemove(uint128 orderId, uint8 cause) private {
        Order o = _orders[orderId];
        uint128 refund = o.escrow;
        // The note is told the SAME figure this function is about to pay out, from the same read.
        _removeFromBook(orderId, cause, refund);
        if (refund > 0) { _payShell(o.note, refund); emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(orderId, o.note, refund); }
    }

    /// @notice Expiry-remove a resting BID: refund the buyer's escrow AND emit
    ///         `InferenceOrderExpired`, so an expired order is observably gone rather than
    ///         vanishing silently. `_refundAndRemove` is shared with dust / fully-filled removals
    ///         (which are NOT expiries), so the event lives in this wrapper — the single bid-side
    ///         expiry entry point (match sweep + `_doExpire`).
    function _removeExpiredBid(uint128 orderId) private {
        address note = _orders[orderId].note;
        _refundAndRemove(orderId, REMOVED_EXPIRED);
        emit InferenceOrderExpired{dest: address.makeAddrExtern(OrderExpiredEmit, bitCntAddress)}(orderId, true, note, address(0));
    }

    /// @notice Expiry-remove a resting SELL: an ask holds NO buyer escrow (the seller bond is
    ///         funded only after a match), so removal frees its deal TC's `_offerPosted` latch via
    ///         `onSellClosed` (keeping the TC re-listable) — not a refund. The single ask-side
    ///         expiry entry point (match sweep + `_doExpire`).
    function _removeExpiredSell(uint128 orderId) private {
        Order o = _orders[orderId];
        address tc = o.tokenContract;
        _removeFromBook(orderId, REMOVED_EXPIRED, 0);   // an ask holds no escrow
        if (tc != address(0)) {
            ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
        }
        emit InferenceOrderExpired{dest: address.makeAddrExtern(OrderExpiredEmit, bitCntAddress)}(orderId, false, o.note, tc);
    }

    // Expiry cleanup is NOT a separate book pass. Expired makers are removed inline by whatever
    // scan already runs: `_match` drops them as it crosses (charged to its own per-tx budget), the
    // view prechecks skip them, and orders NO taker ever crosses — which no scan would ever reach —
    // are removed by the permissionless `expireOrder`. A `_purgeExpired*` re-walk would burn the
    // scan budget on LIVE makers to find the dead ones, so it is deliberately gone.

    /// @notice Rest leftover (limit) or refund (taker-only / market) after a match completes.
    function _finalizeTaker(
        uint128 orderId, uint256 buyerPubkey, bool isBuy, uint256 storedPrice, address note, address tc,
        uint128 remaining, uint128 leftover, uint128 initialAmount, uint8 flags, uint64 deadline,
        uint64 clientOrderId
    ) private {
        bool takerOnly = (flags & FLAG_MARKET) != 0 || (flags & (FLAG_IOC | FLAG_FOK)) != 0;
        if (isBuy) {
            // remaining < 2: a 1-tick remainder can never fund a deal (min-fill skips it),
            // so resting it would only leave unfillable dust. Refund the leftover escrow
            // instead of inserting it — same as the fully-filled/taker-only case.
            if (remaining < 2 || takerOnly) {
                // Terminal for `orderId` whether or not anything came back: the placement was
                // announced with this id, so its disappearance has to be announced with it too.
                emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(orderId, note, leftover);
                // AND ANNOUNCED TO THE NOTE, not only to the watcher. The event goes off-chain; the
                // note's in-flight record lives here and is what refuses a withdrawal. Until this
                // line the record simply stayed — the request had been accepted, so no rejection
                // fired, and it had not rested, so no removal fired either.
                //
                // One message carries the money and the number, exactly as the three refusals do.
                // No condition on whether anything filled: after the fill mirror learned to
                // SUBTRACT, the record already holds precisely what is left, so the same call is
                // correct for a partial fill and for no fill at all.
                _rejectToNote(note, clientOrderId, PLACE_END_TAKER, leftover);
                return;
            }
            _insertIntoBook(orderId, Order({
                note: note, buyerPubkey: buyerPubkey, tokenContract: address(0), price: storedPrice,
                amount: remaining, initialAmount: initialAmount, filledAccum: initialAmount - remaining,
                escrow: leftover, deadline: deadline, clientOrderId: clientOrderId, ts: uint64(block.timestamp),
                flags: flags, isBuy: true,
                nextAtPrice: 0, prevAtPrice: 0, nextInOwner: 0, prevInOwner: 0
            }));
        } else {
            if (remaining > 0 && !takerOnly) {
                _insertIntoBook(orderId, Order({
                    note: note, buyerPubkey: 0, tokenContract: tc, price: storedPrice,
                    amount: remaining, initialAmount: initialAmount, filledAccum: initialAmount - remaining,
                    // A SELL carries a MANDATORY absolute deadline (set at placement, §2.1.1);
                    // carry it onto the resting remainder so the ask stays expirable.
                    escrow: 0, deadline: deadline, clientOrderId: clientOrderId, ts: uint64(block.timestamp),
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
                // An ask holds no escrow, so the refund is zero — but the id was announced at
                // placement and this is where it stops existing, so it is announced closing too.
                emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(orderId, note, 0);
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

    function _enqueuePlace(address owner, uint256 buyerPubkey, bool isBuy, uint8 flags, uint256 price, uint128 amount, uint128 escrow, address tc, uint64 deadline, uint64 clientOrderId) private {
        require(_queueSize < QUEUE_PLACE_LIMIT, ERR_QUEUE_FULL);
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_PLACE, owner: owner, buyerPubkey: buyerPubkey, isBuy: isBuy, flags: flags, price: price,
            amount: amount, escrow: escrow, tokenContract: tc, deadline: deadline,
            clientOrderId: clientOrderId, targetOrderId: 0, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
    }

    /// @dev Returns false instead of reverting when the queue is full. A revert here unwound the
    ///      whole transaction, so the owner's note saw a BOUNCE — and a bounce carries no reason,
    ///      only the fact of failure. The note read that as "the cancel did not happen" in the one
    ///      way that matters and dropped its record of a LIVE order. Refusing softly keeps the
    ///      decision where the data is: the book knows why, and says so.
    function _enqueueCancel(address owner, uint128 targetOrderId) private returns (bool) {
        if (_queueSize >= QUEUE_CAPACITY) { return false; }
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL, owner: owner, buyerPubkey: 0, isBuy: false, flags: 0, price: 0,
            amount: 0, escrow: 0, tokenContract: address(0), deadline: 0,
            clientOrderId: 0, targetOrderId: targetOrderId, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
        return true;
    }

    /// @dev Same soft refusal as `_enqueueCancel`, same reason.
    function _enqueueCancelAll(address owner) private returns (bool) {
        if (_queueSize >= QUEUE_CAPACITY) { return false; }
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_CANCEL_ALL, owner: owner, buyerPubkey: 0, isBuy: false, flags: 0, price: 0,
            amount: 0, escrow: 0, tokenContract: address(0), deadline: 0,
            clientOrderId: 0, targetOrderId: 0, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
        });
        return true;
    }

    // Subscription place is a book insertion → gated by QUEUE_PLACE_LIMIT like a normal place.
    /// @notice Enqueue a permissionless expiry request. Routed through the queue like `cancelOrder`
    ///         so it serialises AFTER any in-flight match rather than mutating the book mid-
    ///         continuation (which would invalidate the frozen cursor `_match` resumes from).
    /// @dev Bounded by the PLACE limit, not the full capacity. The gap between the two exists so
    ///      an owner can always cancel and get his escrow out of a book that is otherwise full, and
    ///      `expireOrder` is permissionless — letting it draw from that gap hands a stranger the
    ///      reserve kept for the owner. It is not self-limiting either: `_processHeadCore` keeps
    ///      the head when a placement needs a match continuation or a cancel-all hit its per-call
    ///      cap, so during those states each call adds an entry and removes none.
    function _enqueueExpire(uint128 targetOrderId) private {
        require(_queueSize < QUEUE_PLACE_LIMIT, ERR_QUEUE_FULL);
        uint8 slot = _allocSlot();
        _queue[slot] = QueueEntry({
            entryType: QENTRY_EXPIRE, owner: address(0), buyerPubkey: 0, isBuy: false, flags: 0, price: 0,
            amount: 0, escrow: 0, tokenContract: address(0), deadline: 0,
            clientOrderId: 0, targetOrderId: targetOrderId, contOrderId: 0, contRemaining: 0, contLeftover: 0, contScanOrder: 0
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
        // Fund the ceiling BEFORE the head runs: every branch below can fan out to the per-call
        // cap, and the shortfall would surface in the action phase, where it is no longer
        // recoverable.
        ensureMatchBudget();
        QueueEntry e = _queue[_queueHead];
        bool keepHead = false;

        if (e.entryType == QENTRY_CANCEL) {
            _doCancel(e.owner, e.targetOrderId);
        } else if (e.entryType == QENTRY_CANCEL_ALL) {
            uint8 cancelled = _doCancelAll(e.owner);
            keepHead = (cancelled >= MAX_CANCEL_PER_CALL);
        } else if (e.entryType == QENTRY_EXPIRE) {
            _doExpire(e.targetOrderId);
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

        // Deadline: the ingress check only holds at SUBMIT. It can lapse while an order waits its
        // turn in the pending queue, or (for a BUY) across a multi-tx match continuation. Re-check
        // on every (re)entry so an order queued before its deadline never TAKES liquidity after it,
        // mirroring the maker-side `_isExpired` sweep. SIDE-NEUTRAL now that a SELL also carries a
        // mandatory deadline: a lapsed BUY refunds its remaining escrow, a lapsed SELL frees its
        // deal TC's offer latch (it holds no escrow). A BUY GTC (deadline == 0) never drops here.
        //
        // Two different terminal events, because the two cases are not the same thing to a client.
        // On a continuation the order already has an id and an announced placement, so it closes
        // with `InferenceRefunded` carrying that id. On the first run it never became an order at
        // all — no id, no placement — so it is a rejected submission.
        if (e.deadline != 0 && e.deadline <= block.timestamp) {
            uint128 refund = 0;
            if (e.isBuy) {
                refund = firstRun ? e.escrow : e.contLeftover;
            } else if (e.tokenContract != address(0)) {
                ITokenContractDeal(e.tokenContract).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
            }
            if (firstRun) {
                emit InferenceOrderRejected{dest: address.makeAddrExtern(OrderRejectedEmit, bitCntAddress)}(
                    PLACE_REJ_EXPIRED, e.owner, e.tokenContract, refund);
                // Money and reason in one message, named by the number the note itself chose.
                _rejectToNote(e.owner, e.clientOrderId, PLACE_REJ_EXPIRED, refund);
            } else {
                emit InferenceRefunded{dest: address.makeAddrExtern(BuyUnmatchedEmit, bitCntAddress)}(
                    e.contOrderId, e.owner, refund);
                // THE SAME TWO THINGS THE FIRST-RUN BRANCH DOES, and until now this one did
                // neither. A continuation whose deadline passed emitted an event and returned:
                // the leftover escrow was never paid back and the note was never told, so the
                // money was gone and the in-flight record stayed forever.
                //
                // `e.clientOrderId` survives into the continuation because the queue entry is
                // rewritten whole, not rebuilt — the number the note chose is still there.
                _rejectToNote(e.owner, e.clientOrderId, PLACE_REJ_EXPIRED, refund);
            }
            return false;
        }

        // Taken from the counter only once the order is CERTAIN to exist — after the rejection
        // checks below, not before them. A number spent on a submission that returns unplaced
        // leaves a hole in the sequence, and a client reading the log has no way to tell a hole
        // from an order whose events it missed.
        uint128 orderId = firstRun ? 0 : e.contOrderId;
        bool isMarket = (e.flags & FLAG_MARKET) != 0;
        uint8 takerFlags = e.flags;   // full taker flags — `_dealCompatible` reads the deal bits by side

        if (firstRun) {
            // One resting SELL per deal TokenContract is now enforced by the TC
            // itself (`_offerPosted`), since the TC posts its own offer — no
            // per-TC map here.
            // Purge expired GTD bids that a SELL taker crosses BEFORE the prechecks, so POST_ONLY
            // crossing and FOK `_fokFullyFillable` see the same live liquidity `_match` would (it
            // lazily refunds expired bids). This keeps the prechecks consistent with the match:
            // POST_ONLY tests only live liquidity, and FOK counts only fillable volume.
            // (Only BUYs carry a deadline → purge for taker SELLs.)
            // No pre-pass to drop expired makers: the view prechecks below already SKIP them (so
            // they test the same live liquidity `_match` settles), `_match` removes them inline as
            // it crosses, and non-crossing expired orders are reaped by `expireOrder`.

            // POST_ONLY: reject only if it would cross a GENUINELY executable maker. Testing the
            // raw best level would falsely reject when that level is all expired GTD / cycle-
            // AON-incompatible sizes; `_executableCrosses` skips those, mirroring `_match`.
            if ((e.flags & FLAG_POST_ONLY) != 0 && _executableCrosses(e.isBuy, e.price, takerFlags, e.amount)) {
                uint128 back = 0;
                if (e.isBuy && e.escrow > 0) { back = e.escrow; }
                else if (!e.isBuy && e.tokenContract != address(0)) {
                    // SELL rejected before resting -> notify the TC (onSellClosed frees
                    // the `_offerPosted` latch) so it stays usable.
                    ITokenContractDeal(e.tokenContract).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
                }
                emit InferenceOrderRejected{dest: address.makeAddrExtern(OrderRejectedEmit, bitCntAddress)}(
                    PLACE_REJ_POST_ONLY, e.owner, e.tokenContract, back);
                // Money and reason together, named by the note's own number.
                _rejectToNote(e.owner, e.clientOrderId, PLACE_REJ_POST_ONLY, back);
                return false;
            }
            // FOK: precise all-or-nothing pre-check (per-order simulation of `_match`).
            if ((e.flags & FLAG_FOK) != 0 && !_fokFullyFillable(e.isBuy, e.price, isMarket, e.amount, e.escrow, takerFlags)) {
                uint128 back = 0;
                if (e.isBuy && e.escrow > 0) { back = e.escrow; }
                else if (!e.isBuy && e.tokenContract != address(0)) {
                    ITokenContractDeal(e.tokenContract).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();  // free the TC's offer latch
                }
                emit InferenceOrderRejected{dest: address.makeAddrExtern(OrderRejectedEmit, bitCntAddress)}(
                    PLACE_REJ_FOK, e.owner, e.tokenContract, back);
                // Money and reason together, named by the note's own number.
                _rejectToNote(e.owner, e.clientOrderId, PLACE_REJ_FOK, back);
                return false;
            }

            // Announce ACCEPTANCE only after the rejection checks pass — a crossing POST_ONLY or
            // under-liquid FOK returns above without inserting or filling, so it must not be
            // announced at all. The id is taken here for the same reason.
            //
            // THE CLIENT EVENT ONLY. The note's mirror used to go out here too, and that was
            // wrong: at this point nobody knows yet whether the order will rest or fill on the
            // spot. It now goes from `_insertIntoBook`, which is where resting actually happens —
            // see the note there for what the old placement got wrong. "Accepted" and "resting"
            // are different facts, and after this they are announced from different places.
            orderId = _nextOrderId++;
            emit InferenceOrderPlaced{dest: address.makeAddrExtern(OfferPlacedEmit, bitCntAddress)}(
                orderId, e.isBuy, e.price, e.amount, e.owner, e.tokenContract, e.deadline, e.flags);
        }

        uint128 inAmount   = firstRun ? e.amount  : e.contRemaining;
        uint128 inEscrow   = firstRun ? e.escrow  : e.contLeftover;
        uint128 resumeFrom = firstRun ? 0 : e.contScanOrder;
        (uint128 remaining, uint128 leftover, bool capped, uint128 nextResume) =
            _match(orderId, e.isBuy, e.price, isMarket, e.owner, e.tokenContract, e.buyerPubkey, inAmount, inEscrow, resumeFrom, takerFlags, e.clientOrderId);

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
        _finalizeTaker(orderId, e.buyerPubkey, e.isBuy, e.price, e.owner, e.tokenContract, remaining, leftover, e.amount, e.flags, e.deadline, e.clientOrderId);
        return false;
    }

    function _doCancel(address owner, uint128 orderId) private {
        Order o = _orders[orderId];
        // Give every cancel a terminal outcome the OWNER'S NOTE can act on, not just an event the
        // client can read. The event goes to a watcher off-chain; the note's record lives on-chain
        // and is what refuses a withdrawal. Announcing only to the watcher left the note holding a
        // resting-order record for an order the book had already forgotten, and nothing else would
        // ever clear it — the removal mirror fires from `_removeFromBook`, and neither branch below
        // reaches it.
        //
        // BOTH branches send it, and the second is the less obvious one. "Not yours" still means
        // "not in your books": the caller asked to cancel this id, so its note believes it owns
        // one, and that belief is exactly what has to go.
        if (o.amount == 0 && o.note == address(0)) {
            emit InferenceOrderCancelRejected{dest: address.makeAddrExtern(OfferCancelRejectedEmit, bitCntAddress)}(orderId, CANCEL_REJ_NOT_FOUND, owner);
            IPrivateNote(owner).onInferenceOrderRemoved{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(_modelHash, orderId, REMOVED_REJECTED, 0);
            return;
        }
        if (o.note != owner) {
            emit InferenceOrderCancelRejected{dest: address.makeAddrExtern(OfferCancelRejectedEmit, bitCntAddress)}(orderId, CANCEL_REJ_NOT_OWNER, owner);
            IPrivateNote(owner).onInferenceOrderRemoved{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(_modelHash, orderId, REMOVED_REJECTED, 0);
            return;
        }
        // Limit or subscription: the full remaining escrow returns to the buyer on cancel
        // (unused subscription budget is refunded, not forfeited).
        uint128 refund = o.escrow;
        // A cancelled SELL is removed WITHOUT a fill → free the TC's `_offerPosted`
        // latch so the seller can re-list on the same (still-live) TC. Read the TC
        // BEFORE `_removeFromBook` deletes the order.
        bool    freeTc = !o.isBuy && o.tokenContract != address(0);
        address tc     = o.tokenContract;
        _removeFromBook(orderId, REMOVED_CANCELLED, refund);
        emit InferenceOrderCancelled{dest: address.makeAddrExtern(OfferCancelledEmit, bitCntAddress)}(orderId, refund, owner);
        _payShell(owner, refund);
        if (freeTc) { ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(); }
    }

    function _doCancelAll(address owner) private returns (uint8 cancelled) {
        cancelled = 0;
        uint128 cur = _ownerHead[owner];
        while (cur != 0 && cancelled < MAX_CANCEL_PER_CALL) {
            Order o = _orders[cur];
            uint128 next = o.nextInOwner;   // captured before any settle/removal below
            uint128 refund = o.escrow;
            bool    freeTc = !o.isBuy && o.tokenContract != address(0);
            address tc     = o.tokenContract;
            _removeFromBook(cur, REMOVED_CANCELLED, refund);
            emit InferenceOrderCancelled{dest: address.makeAddrExtern(OfferCancelledEmit, bitCntAddress)}(cur, refund, owner);
            if (refund > 0) { _payShell(owner, refund); }
            if (freeTc) { ITokenContractDeal(tc).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}(); }
            cur = next;
            cancelled++;
        }
    }

    /// @notice Remove `orderId` ONLY if its deadline has genuinely passed — the work behind the
    ///         permissionless `expireOrder`. O(1), no book walk. Idempotent: an order that is
    ///         already gone is a silent no-op, so a keeper can spam it and racing callers are
    ///         harmless. A still-LIVE order is refused — this is not a back-door cancel; only the
    ///         owner cancels a live order (`cancelOrder`). The removal wrappers refund the bid /
    ///         free the SELL's TC latch AND emit `InferenceOrderExpired`.
    function _doExpire(uint128 orderId) private {
        Order o = _orders[orderId];
        if (o.amount == 0 && o.note == address(0)) { return; }   // already gone → no-op
        if (!_isExpired(o.deadline)) { return; }                 // still live → refuse
        if (o.isBuy) { _removeExpiredBid(orderId); } else { _removeExpiredSell(orderId); }
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
    /// @param deadline ABSOLUTE expiry (unix seconds), MANDATORY for a SELL (spec §2.1.1). Already
    ///        anchored and capped (`ttl <= MAX_SELL_TTL`) by the seller's PrivateNote — the ONLY
    ///        path to this function — so it is NOT re-checked or re-anchored here: time may have
    ///        passed reaching the book, and re-validating a 1h bound against a moved clock would be
    ///        wrong. `deadline != 0` holds by construction (the note rejects `ttl == 0`), so a SELL
    ///        never rests as GTC; an already-past deadline is not a hard error — the queued-deadline
    ///        recheck in `_doPlaceHead` drops it with `onSellClosed`.
    function placeSellOffer(uint128 pricePerTick, uint128 maxTicks, uint8 flags, uint256 sellerPubkey, uint64 nonce, address ownerNote, uint64 deadline) public {
        // ACCEPT FIRST, and only in the two entries the deal calls. Everything before
        // `tvm.accept()` is charged to the INCOMING message, so whatever ran ahead of it set the
        // floor on what a caller had to attach. Measured rather than assumed, and the answer was
        // not the one expected: `ensureBalance()` costs more than deriving an address, and below
        // the floor a call did not fail its guard — it ran out of gas before reaching one.
        //
        // THIS ALSO PUTS ACCEPT ABOVE THE SENDER GUARD, which is a change and not a saving:
        // an unauthorised call used to be turned away before `accept` and cost this contract
        // nothing. It now pays the compute for any message that arrives, including one it is
        // about to reject.
        tvm.accept();
        ensureBalance();
        // Auth. This used to sit above `accept` and the comment here used to say so — that a
        // non-canonical sender was turned away without charging the contract. That is no longer
        // true and the sentence had to go with the line: the guard is unchanged, but it now runs
        // on this contract's gas. A real TC always passes it, so a genuine offer never reverts.
        require(msg.sender == _tokenContractAddr(sellerPubkey, nonce), ERR_BAD_TOKEN_CONTRACT);
        // Param / capacity checks AFTER accept: the TC latched `_offerPosted` optimistically
        // and forwarded bounce:false. On ANY non-resting outcome, notify the TC (onSellClosed
        // frees the latch) and return rather than revert, so the TC stays usable (re-list /
        // close / destroy all remain available).
        //  - deal serves >= 2 ticks (fundFromOrderBook floor);
        //  - a SELL is a fixed-price limit, never a market order (market clearing = 0);
        //  - price > 0; flag combos consistent (POST_ONLY xor taker; IOC xor FOK);
        //  - only supported flag bits set (no unknown/out-of-mask bits);
        //  - the placement queue has room (else _enqueuePlace rejects with ERR_QUEUE_FULL).
        bool bad = maxTicks < 2
            || pricePerTick == 0
            || pricePerTick % PRICE_STEP != 0          // price must be a whole multiple of 1 SHELL
            || (flags & FLAG_MARKET) != 0
            || ((flags & FLAG_POST_ONLY) != 0 && (flags & TAKER_FLAGS) != 0)
            || ((flags & FLAG_IOC) != 0 && (flags & FLAG_FOK) != 0)
            || (flags & ~SUPPORTED_FLAGS) != 0
            // A SUBSCRIPTION ask implies AON, exactly as a subscription bid does. Weekly
            // settlement is meaningful only for a volume that belongs to ONE deal: without AON a
            // smaller taker consumes part of the ask, the order leaves the book, and the
            // TokenContract latches one-shot — the seller is left needing a fresh contract, a
            // fresh nonce and a fresh bond for a subscription he never got to serve. The bid side
            // has required this since the flag existed; the ask side did not, and the flags travel
            // through `postSellOffer` and `postFromNote` untouched, so nothing upstream caught it.
            || ((flags & FLAG_SUBSCRIPTION) != 0 && (flags & FLAG_AON) == 0)
            // maxTicks*(price + fee) must fit uint128 so the fill cost never overflows the cast.
            || uint256(maxTicks) * _unit(pricePerTick) > uint256(type(uint128).max)
            // A SELL must carry an expiry. The bound itself is the note's job (see @param), but a
            // zero deadline would rest as GTC and is rejected here as a malformed offer.
            || deadline == 0;
        if (bad || _queueSize >= QUEUE_PLACE_LIMIT) {
            ITokenContractDeal(msg.sender).onSellClosed{value: REGISTER_FORWARD_VALUE, flag: 1, bounce: false}();
            return;
        }
        _enqueuePlace(ownerNote, 0, false, flags, pricePerTick, maxTicks, 0, msg.sender, deadline, 0);
        _processHeadCore();
    }

    /// @notice A subscription is this same call with `FLAG_SUBSCRIPTION` set; the term is fixed at
    ///         one month (`SUB_WEEKS`), so the order carries no duration of its own.
    /// @param escrow The buyer's SHELL, as a FIGURE (generation 4.0.33) — subtracted from the
    ///        calling note's `_balance[CURRENCIES_ID_SHELL]` before it called.
    /// @param depositHash The caller's `_depositIdentifierHash`, used to DERIVE the address a real
    ///        note with that hash would have. This is not book-keeping: under currency the escrow
    ///        proved itself, because ECC on a message cannot be forged, and this call did not care
    ///        who sent it. A figure proves nothing, so the derivation below replaces the proof the
    ///        currency used to carry. Claiming someone else's hash only derives an address that is
    ///        not the caller, so it buys nothing.
    /// @dev THE CLIENT NUMBER LEADS THE LIST, and the reason is the bounced window. A bounce
    ///      returns only the leading bits of the body, 256 guaranteed, and the note has to know
    ///      WHICH request came back: function id (32) + `clientOrderId` (64) = 96 bits, with room
    ///      to spare. `escrow` no longer has to travel there at all — the note recorded the figure
    ///      under this number before sending, so the number alone is enough to undo it. That is
    ///      also why the number is `uint64` and not wider.
    function placeBuyOrder(uint64 clientOrderId, uint128 escrow, uint128 maxPricePerTick, uint128 ticks, uint8 flags, uint64 deadline, uint256 buyerPubkey, uint256 depositHash) public {
        ensureBalance();
        require(msg.sender == _noteAddrFromHash(depositHash), ERR_INVALID_SENDER);
        // A deal serves >= 2 ticks (probe + >=1 stream); a 1-tick buy can never fund a
        // deal (fundFromOrderBook rejects < 2), so it is rejected up front.
        require(ticks >= 2, ERR_BAD_PARAM);
        require((flags & FLAG_POST_ONLY) == 0 || (flags & TAKER_FLAGS) == 0, ERR_BAD_FLAGS);
        require((flags & FLAG_IOC) == 0 || (flags & FLAG_FOK) == 0, ERR_BAD_FLAGS);
        require((flags & ~SUPPORTED_FLAGS) == 0, ERR_BAD_FLAGS);
        // A subscription is one buyer against ONE seller for the WHOLE volume, so FLAG_SUBSCRIPTION
        // implies FLAG_AON: weekly settlement has a meaning only for a volume that belongs to a
        // single deal. The term is fixed at one month, so the rest is arithmetic — the volume
        // divides into its weeks, and a month could physically deliver it at the one-tick-per-
        // minute ceiling (MIN_SECONDS_PER_TICK). An order nobody could serve does not rest.
        bool isSub = (flags & FLAG_SUBSCRIPTION) != 0;
        require(!isSub || (flags & FLAG_AON) != 0, ERR_BAD_FLAGS);
        require(!isSub || ticks % uint128(SUB_WEEKS) == 0, ERR_BAD_PARAM);
        require(!isSub || ticks <= uint128(SUB_WEEKS) * SUB_TICKS_PER_WEEK, ERR_BAD_PARAM);
        require(deadline == 0 || deadline > block.timestamp, ERR_EXPIRED);
        bool isMarket = (flags & FLAG_MARKET) != 0;
        // A subscription carries a bond of `2P` beside its escrow, and a market order has no price
        // to size one against — it is priced only when it clears, which is after the escrow is
        // committed. So a month is bought at a limit or not at all. That also matches what the
        // order means: a term commitment at a price nobody stated is not something to rest.
        require(!isSub || !isMarket, ERR_BAD_FLAGS);
        // Limit price must be a positive whole multiple of 1 SHELL (market buys carry no price).
        require(isMarket || (maxPricePerTick > 0 && maxPricePerTick % PRICE_STEP == 0), ERR_BAD_PARAM);

        require(escrow > 0, ERR_NO_SHELL);
        // Compare in uint256: escrow (uint128) must cover the full ticks*(price+fee)
        // product. This also caps the required cost at uint128, so the fill cost
        // never overflows the downstream cast (mirrors the placeSellOffer bound).
        // A subscription must also cover its `2P` bond, sized at the LIMIT price: the deal clears at
        // or below it, so room made here is room enough whatever it clears at. The bond is not
        // volume — the TC sets it aside and derives the ticks from what is left — so it is required
        // on top of the full tick cost, never out of it.
        if (!isMarket) {
            require(uint256(escrow) >= uint256(ticks) * _unit(maxPricePerTick)
                + (isSub ? 2 * uint256(maxPricePerTick) : 0), ERR_INSUFFICIENT_DEPOSIT);
        }
        tvm.accept();
        _enqueuePlace(msg.sender, buyerPubkey, true, flags, isMarket ? type(uint256).max : maxPricePerTick, ticks, escrow, address(0), deadline, clientOrderId);
        _processHeadCore();
    }

    /// @dev A FULL QUEUE IS AN ANSWER, NOT A FAILURE. It used to revert, which reached the note as
    ///      a bounce; a bounce says only "the call did not go through", so the note undid its own
    ///      record — of an order that is still resting in the book. The record is what refuses a
    ///      withdrawal, so the owner lost the guard on a live order and kept the order.
    ///
    ///      Now the book answers with `CANCEL_REJ_QUEUE_FULL` and touches nothing. NO REMOVAL
    ///      MIRROR HERE, deliberately: the other two rejections mean the order is gone and the
    ///      note's record must go with it, this one means the opposite.
    function cancelOrder(uint128 orderId) public {
        ensureBalance();
        tvm.accept();
        if (!_enqueueCancel(msg.sender, orderId)) {
            emit InferenceOrderCancelRejected{dest: address.makeAddrExtern(OfferCancelRejectedEmit, bitCntAddress)}(
                orderId, CANCEL_REJ_QUEUE_FULL, msg.sender);
            return;
        }
        _processHeadCore();
    }

    /// @dev Same soft refusal as `cancelOrder`. The id reported is 0 — there is no single order
    ///      this request was about, and inventing one would name an order the caller never asked
    ///      for.
    function cancelAllOrders() public {
        ensureBalance();
        tvm.accept();
        if (!_enqueueCancelAll(msg.sender)) {
            emit InferenceOrderCancelRejected{dest: address.makeAddrExtern(OfferCancelRejectedEmit, bitCntAddress)}(
                0, CANCEL_REJ_QUEUE_FULL, msg.sender);
            return;
        }
        _processHeadCore();
    }

    /// @notice Permissionless expiry: drop `orderId` IFF its deadline has passed. Anyone — a keeper,
    ///         the counterparty, or the order's own deal TC — may call it, so a resting order truly
    ///         expires on time even when NO taker ever crosses it. That is the gap a lazy match-time
    ///         sweep cannot close: an order nobody crosses is never scanned, so it would rest
    ///         forever and (on the ask side) keep its TC's `_offerPosted` latch stuck.
    ///         Idempotent — a gone or still-live order is a silent no-op (see `_doExpire`), so it is
    ///         safe to spam and safe to race.
    function expireOrder(uint128 orderId) public {
        ensureBalance();
        tvm.accept();
        _enqueueExpire(orderId);
        _processHeadCore();
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

    /// @notice The canonical deal TokenContract reports its CUMULATIVE finalized-tick count
    ///         (ticks served with payment + platform fee already irreversible). Only the new
    ///         delta over what this TC has reported before is recorded into the reference-price
    ///         VWAP/median, so reserved-but-refundable matches never move it and a re-sent /
    ///         bounced report never double-counts. Auth = the caller IS the canonical TC for
    ///         `(sellerPubkey, nonce)` (same derivation `placeSellOffer` / `fundFromOrderBook`
    ///         bind), so no other account can inject reference-price volume. The TC forwards
    ///         bounce:false, so a stale/foreign caller is accept-then-noop, never a revert.
    function reportFinalized(uint256 sellerPubkey, uint64 nonce, uint128 pricePerTick, uint128 cumulativeFinalized) public {
        // ACCEPT FIRST, and only in the two entries the deal calls. Everything before
        // `tvm.accept()` is charged to the INCOMING message, so whatever ran ahead of it set the
        // floor on what a caller had to attach. Measured rather than assumed, and the answer was
        // not the one expected: `ensureBalance()` costs more than deriving an address, and below
        // the floor a call did not fail its guard — it ran out of gas before reaching one.
        tvm.accept();
        ensureBalance();
        if (msg.sender != _tokenContractAddr(sellerPubkey, nonce)) { return; }
        uint128 seen = _finalizedSeen[msg.sender];
        if (cumulativeFinalized <= seen) { return; }          // monotonic: never below the high-water mark
        _finalizedSeen[msg.sender] = cumulativeFinalized;
        _recordTrade(pricePerTick, cumulativeFinalized - seen);
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

    /// @notice Answers a weekly-median query by calling the asker back (spec §6.2).
    ///
    /// @dev Accepts, so the book carries the query rather than leaning on whatever value it
    ///      arrived with. The caller is not identified: an OracleEventList is addressed by
    ///      `(oracle, index)`, neither of which appears in this signature, and the binding between
    ///      a list and this book is recorded on the list's side, after the book already exists —
    ///      so there is nothing here to derive a sender from. The figure itself is public anyway,
    ///      the same one `getWeeklyMedianPrice` returns.
    function requestWeeklyMedian(uint256 eventId, uint256 oracleListHash, uint32 tokenType) public view {
        tvm.accept();
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


    function getParams() external view returns (uint256 modelHash, uint16 platformFeeBps) {
        return (_modelHash, PLATFORM_FEE_BPS);
    }

    /// @notice The verified canonical model id `producer--model--version` (sha256 == _modelHash).
    function getModelName() external view returns (string) { return _modelName; }

    function getVersion() external pure returns (string, string) {
        return (version, "InferenceOrderBook");
    }
}
