pragma gosh-solidity >=0.76.1;

import "./errors.sol";

abstract contract AiRegistryModifiers is AiRegistryErrors {
    uint64 constant MIN_BALANCE = 100 vmshell;

    // ECC currency id used for payments and fee burn.
    uint32 constant SHELL_ECC_ID = 2;

    uint16 constant BPS_DENOMINATOR = 10_000;

    // ── Protocol-wide constants (were per-ДОБ params; fixed by design) ──────
    // Platform fee: 2.5% (spec §5.1 PLATFORM_FEE_BPS), buyer-side, BY-FACT (on
    // delivered ticks), net-of-rebate burned (§5.4). NOT charged upfront.
    uint16 constant PLATFORM_FEE_BPS = 250;       // 2.5%
    // Seller rebate (§5.3): rate = min(REBATE_MAX_BPS, REBATE_SLOPE_BPS * n) bps, paid only on a
    // clean (never-disputed) close. REBATE_MAX_BPS is strictly below PLATFORM_FEE_BPS, so the net
    // burn stays positive however many ticks a deal delivers.
    uint16 constant REBATE_MAX_BPS   = 200;       // 2.0% cap
    uint16 constant REBATE_SLOPE_BPS = 4;         // bps per tick (cap at 50 ticks)
    // Probe phase (spec §3.1.2): the buyer's window to try the service before anything can be
    // claimed. Deliberately short — a buyer who finds a dead endpoint should stop and burn, not
    // wait out a long silence.
    uint64  constant PROBE_WINDOW    = 180;        // probe acceptance window (s)
    uint64  constant DISPUTE_WINDOW  = 600;        // dispute -> resolution timeout (s)
    uint64  constant MATCH_OPEN_TIMEOUT = 600;     // funded-but-unopened cleanup (no-show, §2.1)

    // ── Subscription / consumption accounting ───────────────────────────────────────────────
    uint64  constant SUB_WEEK_LEN = 604_800;       // one subscription week (s)
    // A subscription is ALWAYS one month — four weekly settlement periods. The term is not a
    // parameter: a single fixed length means the order carries no duration to validate, both
    // sides of a match agree on the term by construction, and there is exactly one shape of
    // subscription on the book instead of one per week count.
    uint8   constant SUB_WEEKS    = 4;             // one month, in weekly periods

    // Physical floor on generation: the fastest model in existence produces TICK_SIZE (1M) tokens
    // in ~58.8 s, so ONE TICK PER MINUTE is the ceiling nothing can beat. A claim of `d` ticks is
    // rejected unless `d * MIN_SECONDS_PER_TICK <= elapsed` — the seller cannot assert more output
    // than any hardware could have produced in the time that passed. Waiting longer widens THIS
    // bound and nothing else: a claim is also capped at `MAX_CLAIM_DELTA` — one tick per call,
    // whatever the elapsed time — so silence never buys the right to assert a batch. At these
    // values the per-call cap is the binding one, and a caller written against the elapsed-time
    // bound alone will have its claim rejected. Revisit if a materially faster model ships.
    uint64  constant MIN_SECONDS_PER_TICK = 60;    // (s per tick)
    // Promotion window for the pending claim. EXACTLY ONE claim may be pending, so the wait is one
    // tick-time: after it, an unchallenged claim becomes final even though no further claim
    // arrived. Without this the LAST claim of a deal could never be promoted — there is no
    // successor to promote it — and would be unpayable.
    //
    // Equal to `MIN_CLAIM_INTERVAL` on purpose, and the equality is what makes ONE pending slot
    // enough: a claim cannot arrive sooner than the interval, and by then the window on the
    // previous one has closed, so the slot the newcomer needs is always free. The seller therefore
    // runs at most ONE tick ahead of what is trusted, and the amount a dispute can be about is a
    // single tick rather than a quantity the seller can grow by claiming quickly.
    uint64  constant CLAIM_PROMOTE_WINDOW = MIN_SECONDS_PER_TICK;       // 60 s
    // Floor on the gap between two claims, and the partner of the window above: the claim carries
    // its own timestamp and is promoted on its own window, so the buyer's contest time is a full
    // window on every claim regardless of how fast the seller claims. Equal to the window, which
    // keeps the claimable rate at the physical ceiling — one tick per minute — while leaving the
    // single pending slot free whenever the next claim is entitled to arrive.
    uint64  constant MIN_CLAIM_INTERVAL = MIN_SECONDS_PER_TICK;         // 60 s
    // Ticks one week can physically deliver at that rate cap. A subscription may not buy more
    // volume than its own term could ever produce, so the whole-term ceiling is SUB_WEEKS of these
    // — derived from the cap rather than written down, so the two can never drift apart.
    uint128 constant SUB_TICKS_PER_WEEK = uint128(SUB_WEEK_LEN / MIN_SECONDS_PER_TICK);   // 10 080

    // Forwarded value for child -> parent registration messages.
    varuint16 constant REGISTER_FORWARD_VALUE = 5 vmshell;

    // ── Per-entry gas charge (measured, #950) ───────────────────────────────────────────────
    // Every call into a deal burns SHELL ECC for the work it causes: the caller attaches the
    // amount below, the entry burns exactly that, and the fee lands on the sender under flag 1.
    // The deal no longer relies on a funder's one-time deposit to stay alive — it lives in the
    // ordinary dapp now, so `ensureBalance` covers the native side and these charge the caller
    // for what the call costs the network.
    //
    // The figures come from the measured campaign in #950, rounded UP to the next 0.005: the
    // deployment run cost 0.23684 vmshell across eleven transactions, and each entry's share was
    // measured on its own transaction rather than divided out of the total. The 4.0.35 correction
    // from live shellnet is folded in — the claim shelf settled at ~0.0124 once the two-slot
    // conveyor removed two state variables, and 0.015 is that number rounded up.
    //
    // Rounding UP per entry is deliberate and does NOT sum to the deal's lifetime figure: adding
    // the rounded rows gives 0.260 against a measured 0.23684, because every row carries its own
    // margin. Sizing a whole deal is a separate question, answered by the formula in #950
    // (~0.215 constant + 0.013 per tick); these constants price ONE call.
    uint64 constant GAS_DEPLOY          = 0.100 vmshell;  // measured 0.09705 — the code-carrying message
    uint64 constant GAS_TERMINAL        = 0.045 vmshell;  // measured 0.04262 (stop); every wind-down path
    uint64 constant GAS_POST_FROM_NOTE  = 0.025 vmshell;  // measured 0.02344
    uint64 constant GAS_FUND_FROM_BOOK  = 0.020 vmshell;  // measured 0.01865
    uint64 constant GAS_OPEN            = 0.015 vmshell;  // measured 0.01174
    uint64 constant GAS_ACCEPT_PROBE    = 0.015 vmshell;  // measured 0.01324
    uint64 constant GAS_CLAIM           = 0.015 vmshell;  // shellnet 4.0.35 shelf 0.0124
    uint64 constant GAS_FUND_DEAL       = 0.010 vmshell;  // measured 0.00685
    // Entries the #950 run never exercised, priced by the closest measured neighbour rather than
    // left unpriced: the accounting entries do a claim's work (`GAS_CLAIM`), and the ones that only
    // record or read take the floor the campaign's cheapest row established (0.001 -> 0.005).
    uint64 constant GAS_SETTLE          = 0.015 vmshell;  // finalize / settleWeek — claim-shaped work
    uint64 constant GAS_LIGHT           = 0.005 vmshell;  // onSellClosed / touchDeal / withdrawShell

    // External address constants for directed events (off-chain subscribers).
    uint constant bitCntAddress = 256;
    uint128 constant RootRegisteredEmit          = 700;
    uint128 constant TokenContractRegisteredEmit = 702;
    uint128 constant ContractDeployedEmit        = 703;
    uint128 constant ContractDestroyedEmit       = 709;
    uint128 constant ShellWithdrawnEmit          = 710;
    // Streaming deal (spec §3-4)
    uint128 constant StreamFundedEmit            = 720;
    uint128 constant StreamOpenedEmit            = 721;
    uint128 constant TickFinalizedEmit           = 722;
    uint128 constant StreamStoppedEmit           = 723;
    uint128 constant StreamDisputedEmit          = 724;
    uint128 constant DisputeResolvedEmit         = 725;
    // Probe / seller bond (spec §3.1.2 / §4.2)
    uint128 constant SellerBondFundedEmit        = 727;
    uint128 constant ProbeAcceptedEmit           = 728;
    uint128 constant ProbeBurnedEmit             = 729;
    /// @notice External event id for `TokenContract.TicksClaimed`.
    /// @dev    It used to share `TickFinalizedEmit` (722) with `TickFinalized`, and this is the
    ///         quietest of the three collisions: both bodies are `(uint128, uint128)`, so a
    ///         positional decode SUCCEEDS and hands back two plausible numbers that mean different
    ///         things — `TicksClaimed(trusted, claimed)` counts ticks, `TickFinalized(finalizedOwed,
    ///         deposit)` counts money. The other two collisions break loudly; this one only ever
    ///         produced a wrong figure in somebody's report.
    uint128 constant TicksClaimedEmit            = 730;
    /// @notice External event id for `TokenContract.EndpointSet` — the endpoint ciphertext CHANGED.
    /// @dev    Its own id, not a seat borrowed from `StreamOpenedEmit`. The endpoint is now written
    ///         from two places (`open`, and `fundDeal` when the seller sends it with the bond), and
    ///         only one of them opens the stream — so a buyer subscribed to the opening would hear
    ///         nothing about the other. A shared id would have been worse than silence: the two
    ///         bodies differ, so the decode would fail on whichever arrived unexpected, and a
    ///         listener cannot tell a failed decode from an event that was never sent.
    uint128 constant EndpointSetEmit              = 731;
    /// @notice External event id for `TokenContract.ContractDeployed` — a DEAL was born.
    /// @dev    It shared `ContractDeployedEmit` (703) with `RootModel.ContractDeployed` until now,
    ///         and the two events are identical in name and in body, so nothing ever failed: a
    ///         listener waiting for deals decoded root-model births just as successfully and
    ///         counted them as deals. Silent by construction — the only thing separating them was
    ///         the message's `src`, which a subscriber filtering by address never looks at.
    ///         A deal's birth now sits with the deal's other events (720+) instead of in the
    ///         registry's range, which is where it belonged from the start.
    uint128 constant DealDeployedEmit             = 732;
    /// @notice External event id for `TokenContract.BuyerBondFunded` — the buyer posted his `2P`.
    /// @dev    Its own id rather than a seat beside `SellerBondFundedEmit` (727). The two events
    ///         carry the same body — one `uint128` — so a shared address would decode cleanly on
    ///         either and a listener counting seller bonds would count buyer bonds as well. That is
    ///         the quiet collision class, and it is the reason this range is audited by
    ///         `verify_event_addresses.py` rather than by eye.
    uint128 constant BuyerBondFundedEmit          = 733;
    // InferenceOrderBook (spec §2 + §8) — dedicated 1000+ range (separate from registry/streaming/oracle 700s)
    uint128 constant OfferPlacedEmit             = 1000;
    uint128 constant OfferCancelledEmit          = 1001;
    uint128 constant BuyUnmatchedEmit            = 1002;
    uint128 constant MatchedEmit                 = 1003;
    uint128 constant ExecutedEmit                = 1004;
    // (736-738 were InferenceOracle — folded into InferenceOrderBook's
    //  daily-VWAP/weekly-median reference price; standalone oracle removed.)
    // InferenceOrderBook §8 — continue the 1000+ range
    // (1005-1007 were the weekly-cycle subscription events; a subscription is an ordinary order
    //  now, so it reports through the same OfferPlaced / Matched / OrderCancelled events.)
    uint128 constant InferenceOBDeployedEmit     = 1008;
    uint128 constant OfferCancelRejectedEmit     = 1009;
    uint128 constant OrderExpiredEmit            = 1010;
    /// @notice External event id for `InferenceOrderBook.InferenceOrderRejected`.
    /// @dev    It shared `OfferCancelRejectedEmit` (1009) with `InferenceOrderCancelRejected`.
    ///         Two refusals, but of different things and with different bodies —
    ///         `(uint8, address, address, uint128)` against `(uint128, uint8, address)`. 1009 stays
    ///         with the cancel rejection, whose name it matches; the placement rejection gets 1011.
    uint128 constant OrderRejectedEmit           = 1011;

    modifier accept() {
        tvm.accept();
        _;
    }

    modifier onlyOwnerPubkey(uint256 ownerPubkey) {
        require(msg.pubkey() == ownerPubkey, ERR_NOT_OWNER);
        _;
    }

    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }
}
