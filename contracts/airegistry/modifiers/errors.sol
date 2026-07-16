pragma gosh-solidity >=0.76.1;

abstract contract AiRegistryErrors {
    uint16 constant ERR_NOT_OWNER             = 301;
    uint16 constant ERR_INVALID_SENDER        = 302;
    uint16 constant ERR_ZERO_AMOUNT           = 303;
    uint16 constant ERR_ALREADY_REGISTERED    = 304;
    uint16 constant ERR_NOT_INITIALIZED       = 305;
    uint16 constant ERR_INSUFFICIENT_TOKENS   = 306;
    uint16 constant ERR_CONTRACT_LOCKED       = 307;
    uint16 constant ERR_NOT_RESERVED          = 308;
    uint16 constant ERR_RESERVATION_OVERFLOW  = 309;
    uint16 constant ERR_NOT_EMPTY             = 310;
    uint16 constant ERR_NO_SHELL              = 311;
    uint16 constant ERR_BAD_FEE_BPS           = 312;
    uint16 constant ERR_BAD_PARAM             = 313;
    uint16 constant ERR_OVERFLOW              = 314;
    uint16 constant ERR_FIRST_BATCH_LIMIT     = 315;
    uint16 constant ERR_BAD_CODE_HASH         = 316;
    uint16 constant ERR_SINGLE_SESSION_REQUIRED = 317;
    // Streaming deal (spec §3-4)
    uint16 constant ERR_NOT_FUNDED            = 318;
    uint16 constant ERR_ALREADY_FUNDED        = 319;
    uint16 constant ERR_NOT_OPEN              = 320;
    uint16 constant ERR_ALREADY_OPEN          = 321;
    uint16 constant ERR_NOT_BUYER             = 322;
    uint16 constant ERR_SETTLE_WINDOW_OPEN    = 323;
    uint16 constant ERR_DISPUTED              = 324;
    uint16 constant ERR_NOT_DISPUTED          = 325;
    uint16 constant ERR_DISPUTE_WINDOW_OPEN   = 326;
    uint16 constant ERR_STREAM_TIMEOUT_OPEN   = 327;
    uint16 constant ERR_INSUFFICIENT_DEPOSIT  = 328;
    uint16 constant ERR_STILL_OPEN            = 329;
    // (330 ERR_ALREADY_SET / 331 ERR_LOW_LIQUIDITY were the standalone
    //  InferenceOracle — removed; reference price lives in InferenceOrderBook.)
    // Probe tick (spec §3.1.2)
    uint16 constant ERR_PROBE_NOT_FUNDED      = 332;  // open() before the seller funded the probe commission
    uint16 constant ERR_PROBE_ALREADY_FUNDED  = 333;  // fundProbeCommission() called twice
    uint16 constant ERR_NOT_PROBE             = 334;  // op requires the Probe state (probe not yet accepted)
    uint16 constant ERR_ALREADY_STREAMING     = 335;  // probe already accepted, Probe-only op rejected
    uint16 constant ERR_OFFER_LIVE            = 336;  // destroy blocked: a live sell offer still rests on the book
    uint16 constant ERR_NO_PREPAID_TICK       = 337;  // advance() in streaming with no delivered tick to finalize
}
