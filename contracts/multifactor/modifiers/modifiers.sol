/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;

//import "./replayprotection.sol";
//import "./structs/structs.sol";

import "./errors.sol";

abstract contract Modifiers /*is ReplayProtection */ is Errors  {   
    string constant versionModifiers = "1.0.0";

    modifier onlyOwnerPubkey(uint256 rootpubkey) {
        require(msg.pubkey() == rootpubkey, ERR_NOT_OWNER);
        _;
    }
    
    modifier senderIs(address sender) {
        require(msg.sender == sender, ERR_INVALID_SENDER_ADDR);
        _;
    }
}
