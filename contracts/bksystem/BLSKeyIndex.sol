/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";

contract BLSKeyIndex is Modifiers {
    string constant version = "1.0.0";
    address static _root;
    bytes static _bls;
    address _wallet;
    bool _ready;

    constructor (
        address wallet
    ) senderIs(_root) accept {
        _ready = false;
        _wallet = wallet;
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLS_KEY) { 
            return; 
        }
        gosh.mintshellq(FEE_DEPLOY_BLS_KEY);
    }

    function isBLSKeyAccept(address wallet, uint16 signerIndex, uint256 pubkey, uint128 rep_coef, uint128 stake, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp) public senderIs(_root) accept {
        ensureBalance();
        if (wallet == _wallet) { 
            BlockKeeperContractRoot(_root).isBLSAccepted{value: 0.1 vmshell, flag: 1}(wallet, pubkey, _bls, stake, _ready, signerIndex, rep_coef, virtualStake, ProxyList, myIp);
            _ready = true;
        } else {
            BlockKeeperContractRoot(_root).isBLSAccepted{value: 0.1 vmshell, flag: 1}(wallet, pubkey, _bls, stake, true, signerIndex, rep_coef, virtualStake, ProxyList, myIp);
        }
    }

    function isBLSKeyAcceptContinue(address wallet, uint16 signerIndex, uint256 pubkey, uint64 seqNoStartOld, uint128 stake, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef) public senderIs(_root) accept {
        ensureBalance();
        if (wallet == _wallet) { 
            BlockKeeperContractRoot(_root).isBLSAcceptedContinue{value: 0.1 vmshell, flag: 1}(wallet, pubkey, _bls, stake, _ready, seqNoStartOld, signerIndex, virtualStake, ProxyList, sumReputationCoef);
            _ready = true;
        } else {          
            BlockKeeperContractRoot(_root).isBLSAcceptedContinue{value: 0.1 vmshell, flag: 1}(wallet, pubkey, _bls, stake, true, seqNoStartOld, signerIndex, virtualStake, ProxyList, sumReputationCoef);
        }
    }

    function destroyRoot() public senderIs(_root) accept {
        ensureBalance();
        selfdestruct(_root);
    }

    function destroy() public senderIs(_wallet) accept {
        ensureBalance();
        selfdestruct(_root);
    }

    function getReadyStatus() external view returns(bool ready) {
        return _ready;
    }
  
    function getVersion() external pure returns(string, string) {
        return (version, "BLSKey");
    }
}
