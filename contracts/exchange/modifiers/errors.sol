pragma gosh-solidity >=0.76.1;

abstract contract USDCBridgeErrors {
    uint16 constant ERR_ZERO_AMOUNT = 204;
    uint16 constant ERR_INVALID_SENDER = 207;
    uint16 constant ERR_NOT_OWNER = 209;
    uint16 constant ERR_NOT_WHOLE_USDC = 213;
    uint16 constant ERR_OVERFLOW = 214;
    uint16 constant ERR_INVALID_NONCE = 215;
    uint16 constant ERR_NO_ECC = 216;
    uint16 constant ERR_MULTIPLE_ECC = 217;
    uint16 constant ERR_RECIPIENT_TOO_LONG = 218;
    uint16 constant ERR_HASH_MISMATCH = 219;
    uint16 constant ERR_INVALID_ZKPROOF = 220;
    uint16 constant ERR_UNSUPPORTED_TOKEN = 221;
}
