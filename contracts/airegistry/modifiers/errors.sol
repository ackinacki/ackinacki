pragma gosh-solidity >=0.76.1;

abstract contract AiRegistryErrors {
    uint16 constant ERR_NOT_OWNER             = 301;
    uint16 constant ERR_INVALID_SENDER        = 302;
    uint16 constant ERR_ZERO_AMOUNT           = 303;
    uint16 constant ERR_ALREADY_REGISTERED    = 304;
    uint16 constant ERR_INSUFFICIENT_TOKENS   = 306;
    uint16 constant ERR_NO_SHELL              = 311;
    uint16 constant ERR_BAD_PARAM             = 313;
    uint16 constant ERR_OVERFLOW              = 314;
    uint16 constant ERR_BAD_CODE_HASH         = 316;
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
    uint16 constant ERR_BOND_NOT_FUNDED      = 332;  // open() before the seller funded the mirror bond
    uint16 constant ERR_BOND_ALREADY_FUNDED  = 333;  // fundDeal() called twice
    // 334 and 335 were ERR_NOT_PROBE / ERR_ALREADY_STREAMING, both dead — declared, never raised.
    // They are gone rather than kept "for tidiness": InferenceOrderBook declares its OWN 334 and
    // 335 (ERR_NO_LIQUIDITY, ERR_BAD_FLAGS) and inherits these, so a dead constant here did not sit
    // quietly — it made a live exit code ambiguous, and a search by number answered with the
    // meaning that never happens. That cost real diagnostic time on an exit 334.
    uint16 constant ERR_OFFER_LIVE            = 336;  // destroy blocked: a live sell offer still rests on the book
}
