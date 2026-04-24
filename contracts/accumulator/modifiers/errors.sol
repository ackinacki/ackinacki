pragma gosh-solidity >=0.76.1;

abstract contract AccumulatorErrors {
    uint16 constant ERR_INVALID_DENOM = 200;
    uint16 constant ERR_WRONG_SHELL_AMOUNT = 201;
    uint16 constant ERR_WRONG_USDC_AMOUNT = 202;
    uint16 constant ERR_NOT_WHOLE_USDC = 203;
    uint16 constant ERR_ZERO_AMOUNT = 204;
    uint16 constant ERR_ORDER_NOT_SOLD = 205;
    uint16 constant ERR_NO_OWED = 206;
    uint16 constant ERR_INVALID_SENDER = 207;
    uint16 constant ERR_ALREADY_CLAIMED = 208;
    uint16 constant ERR_NOT_OWNER = 209;
    uint16 constant ERR_INSUFFICIENT_REDEEMABLE = 210;
    uint16 constant ERR_WRONG_CODE = 211;
    uint16 constant ERR_WRONG_ADDRESS = 212;
    uint16 constant ERR_MULTIPLE_CURRENCIES = 213;
    uint16 constant ERR_OVERFLOW = 214;
}
