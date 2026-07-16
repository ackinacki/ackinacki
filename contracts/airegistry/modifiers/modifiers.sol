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
    // Seller rebate (§5.3, positive anti-scam): rate = min(REBATE_MAX_BPS,
    // REBATE_SLOPE_BPS * n) bps; paid only on clean (non-disputed) close;
    // REBATE_MAX_BPS strictly < PLATFORM_FEE_BPS so net burn > 0 always.
    uint16 constant REBATE_MAX_BPS   = 200;       // 2.0% cap
    uint16 constant REBATE_SLOPE_BPS = 4;         // bps per tick (cap at 50 ticks)
    // Seller probe commission (spec §3.1.2/§9.2): a percent of the tick price P,
    // on the order of the platform fee on one tick. Returned to the seller on
    // probe acceptance / no-show; burned with the probe tick on a probe stop.
    uint16 constant SELLER_PROBE_COMMISSION_BPS = 250;   // 2.5% of P
    // Streaming-deal timing (spec §9.1). The advance window is PER-DEAL, scaled by
    // tick price so an idle stream drains at most ~0.1 SHELL/min (slope), capped:
    //   W = clamp(pricePerTick * STREAM_WINDOW_SECS_PER_SHELL / SHELL_UNIT,
    //             SETTLE_WINDOW, STREAM_WINDOW_MAX)
    // computed once in the TokenContract ctor (-> _settleWindow); the reclaim
    // window is W + grace (-> _streamTimeout). The probe phase uses a fixed short
    // PROBE_WINDOW (a scammed buyer should stop()+burn, not wait a long W).
    uint64  constant SETTLE_WINDOW   = 180;        // dynamic advance-window FLOOR (s)
    uint64  constant PROBE_WINDOW    = 180;        // fixed probe-phase advance window (s)
    uint64  constant STREAM_WINDOW_MAX = 3600;     // W_MAX hard cap (1h)
    uint64  constant STREAM_WINDOW_SECS_PER_SHELL = 600;  // slope = 0.1 SHELL/min
    uint128 constant SHELL_UNIT      = 1_000_000_000;     // minimal units per 1 SHELL
    uint64  constant STREAM_TIMEOUT_GRACE = 300;   // reclaim = advance W + grace (s)
    uint64  constant DISPUTE_WINDOW  = 600;        // dispute -> split timeout (s)
    uint64  constant MATCH_OPEN_TIMEOUT = 600;     // funded-but-unopened cleanup (no-show, §2.1)

    // Forwarded value for child -> parent registration messages.
    varuint16 constant REGISTER_FORWARD_VALUE = 5 vmshell;

    // External address constants for directed events (off-chain subscribers).
    uint constant bitCntAddress = 256;
    uint128 constant RootRegisteredEmit          = 700;
    uint128 constant TokenContractRegisteredEmit = 702;
    uint128 constant ContractDeployedEmit        = 703;
    uint128 constant TokensPurchasedEmit         = 705;
    uint128 constant TokensConsumedEmit          = 706;
    uint128 constant FeeBurnedEmit               = 707;
    uint128 constant TokensReplenishedEmit       = 708;
    uint128 constant ContractDestroyedEmit       = 709;
    uint128 constant ShellWithdrawnEmit          = 710;
    uint128 constant ReservationCancelledEmit    = 712;
    uint128 constant AvailableReducedEmit        = 714;
    // Streaming deal (spec §3-4)
    uint128 constant StreamFundedEmit            = 720;
    uint128 constant StreamOpenedEmit            = 721;
    uint128 constant TickFinalizedEmit           = 722;
    uint128 constant StreamStoppedEmit           = 723;
    uint128 constant StreamDisputedEmit          = 724;
    uint128 constant DisputeResolvedEmit         = 725;
    uint128 constant StreamReclaimedEmit         = 726;
    // Probe tick (spec §3.1.2)
    uint128 constant ProbeCommissionFundedEmit   = 727;
    uint128 constant ProbeAcceptedEmit           = 728;
    uint128 constant ProbeBurnedEmit             = 729;
    // InferenceOrderBook (spec §2 + §8) — dedicated 1000+ range (separate from registry/streaming/oracle 700s)
    uint128 constant OfferPlacedEmit             = 1000;
    uint128 constant OfferCancelledEmit          = 1001;
    uint128 constant BuyUnmatchedEmit            = 1002;
    uint128 constant MatchedEmit                 = 1003;
    uint128 constant ExecutedEmit                = 1004;
    uint128 constant StreamClosedEmit            = 735;
    // (736-738 were InferenceOracle — folded into InferenceOrderBook's
    //  daily-VWAP/weekly-median reference price; standalone oracle removed.)
    // InferenceOrderBook §8 — continue the 1000+ range
    uint128 constant SubscriptionPlacedEmit      = 1005;
    uint128 constant CycleForfeitedEmit          = 1006;
    uint128 constant ForfeitClaimedEmit          = 1007;
    uint128 constant InferenceOBDeployedEmit     = 1008;

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
