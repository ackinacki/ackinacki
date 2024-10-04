// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;

import "./replayprotection.sol";
import "./structs/structs.sol";

abstract contract Modifiers is ReplayProtection {   
    string constant versionModifiers = "1.0.0";
    
    //TvmCell constants
    uint8 constant m_ConfigCode = 1;
    
    //Deploy constants
    uint128 constant FEE_DEPLOY_CONFIG = 15 ton;

    uint32 constant CURRENCIES_ID = 1;
    uint32 constant CURRENCIES_ID_SHELL = 2;
            
    modifier onlyOwnerPubkeyOptional(optional(uint256) rootpubkey) {
        require(rootpubkey.hasValue() == true, ERR_NOT_OWNER);
        require(msg.pubkey() == rootpubkey.get(), ERR_NOT_OWNER);
        _;
    }

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }
    
    modifier onlyOwnerAddress(address addr) {
        require(msg.sender == addr, ERR_NOT_OWNER);
        _;
    }
    
    modifier minValue(uint128 val) {
        require(msg.value >= val, ERR_LOW_VALUE);
        _;
    }
    
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER);
        _;
    }
    
    modifier minBalance(uint128 val) {
        require(address(this).balance > val + 1 ton, ERR_LOW_BALANCE);
        _;
    }
}
