// SPDX-License-Identifier: GPL-3.0-or-later
pragma gosh-solidity >=0.76.1;

abstract contract Errors {
    string constant versionErrors = "1.0.0";

    /** Public keys general errors */
    uint16 constant ERR_ZERO_PUBKEY  = 101;
    uint16 constant ERR_REPEATING_KEY  = 102;
    uint16 constant ERR_BAD_LEN = 103;

    /** Incorrect Values errors */
    uint16 constant ERR_TOO_SMALL_VALUE = 200;
    uint16 constant ERR_LOW_BALANCE = 201;

    /** TRX list errors */
    uint16 constant ERR_TRX_NOT_FOUND = 300;
    uint16 constant ERR_TRX_WAITLIST_OVERFLOWED = 301;
    uint16 constant ERR_MAX_CLEANUP_TXNS_INVALID = 302;
    
    /** Replay protection errors */
    uint16 constant ERR_MESSAGE_IS_EXIST = 400;
    uint16 constant ERR_MESSAGE_WITH_HUGE_EXPIREAT = 401;
    uint16 constant ERR_MESSAGE_EXPIRED = 402;
    
    /** Message signer and Groth16 related errors */
    uint16 constant ERR_NOT_OWNER = 500; 
    uint16 constant ERR_INVALID_SIGNATURE = 501;
    uint16 constant ERR_FACTOR_EXPIRED = 502;
    uint16 constant ERR_INVALID_SENDER_ADDR = 503;
    uint16 constant ERR_INVALID_PROOF = 504;
    uint16 constant ERR_FACTOR_NOT_FOUND = 506;
    uint16 constant ERR_JWK_EXPIRED = 507;
    uint16 constant ERR_JWK_NOT_FOUND = 508;
    uint16 constant ERR_INVALID_JWK = 509;
    uint16 constant ERR_TLS_DATA = 510;
    uint16 constant ERR_FACTOR_TIMESTAMP_TOO_BIG = 511;
    uint16 constant ERR_REPEATING_CERT = 512;
    uint16 constant ERR_CERT_NOT_FOUND = 513;
    uint16 constant ERR_JWK_TIMESTAMP_TOO_BIG = 514;

    /** Wallet Access Recovery related errors */
    uint16 constant ERR_SEED_PHRASE_NEW_CANDIDATE_EXISTS = 600;
    uint16 constant ERR_SEED_PHRASE_NEW_CANDIDATE_NOT_FOUND = 601;
    
    /** Security card errors */
    uint16 constant ERR_CARD_IS_TURNED_ON = 700;
    uint16 constant ERR_NO_CARDS = 701;
    uint16 constant ERR_TOO_MUCH_CARDS_ADDED = 702;
    uint16 constant ERR_CARD_EXISTS = 703;
    uint16 constant ERR_CARD_IS_TURNED_OFF = 704;
    uint16 constant ERR_CARD_NOT_FOUND = 705;    
}
