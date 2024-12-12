// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;

abstract contract Errors {
    string constant versionErrors = "1.0.0";
    
    uint16 constant ERR_MESSAGE_IS_EXIST = 300;
    uint16 constant ERR_MESSAGE_WITH_HUGE_EXPIREAT = 301;
    uint16 constant ERR_MESSAGE_EXPIRED = 302;
    uint16 constant ERR_NOT_OWNER = 303;
    uint16 constant ERR_LOW_BALANCE = 304;
    uint16 constant ERR_INVALID_SENDER = 305;
    uint16 constant ERR_LOW_VALUE = 306;
    uint16 constant ERR_WRONG_CUSTODIAN = 307;
    uint16 constant ERR_CANDIDATE_EXIST = 308;
}
