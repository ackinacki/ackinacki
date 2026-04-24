pragma gosh-solidity >=0.76.1;

abstract contract ExchangeErrors {
    uint16 constant ERR_ZERO_AMOUNT = 204;
    uint16 constant ERR_INVALID_SENDER = 207;
    uint16 constant ERR_NOT_OWNER = 209;
    uint16 constant ERR_NOT_WHOLE_USDC = 213;
    uint16 constant ERR_OVERFLOW = 214;
    uint16 constant ERR_INVALID_NONCE = 215;
}
