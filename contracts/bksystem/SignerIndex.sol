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

contract SignerIndex is Modifiers {
    string constant version = "1.0.0";
    uint16 static _signerIndex;
    address static _root;
    address _wallet;
    bool _ready;

    constructor (
        address wallet
    ) senderIs(_root) accept {
        _ready = false;
        _wallet = wallet;
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_SIGNER_INDEX) { return; }
        gosh.mintshellq(FEE_DEPLOY_SIGNER_INDEX);
    }

    function isSignerIndexAccept(address wallet, bytes blsKey, uint256 pubkey, uint128 rep_coef, uint128 stake, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, string myIp, optional(string) nodeVersion) public senderIs(_root) accept {
        ensureBalance();
        if (_wallet == wallet) {
            BlockKeeperContractRoot(_root).isSignerIndexAccepted{value: 0.1 vmshell, flag: 1}(wallet, pubkey, blsKey, stake, _ready, _signerIndex, rep_coef, virtualStake, ProxyList, myIp, nodeVersion);
            _ready = true;
        } else {
            BlockKeeperContractRoot(_root).isSignerIndexAccepted{value: 0.1 vmshell, flag: 1}(wallet, pubkey, blsKey, stake, true, _signerIndex, rep_coef, virtualStake, ProxyList, myIp, nodeVersion);
        }
    }

    function isSignerIndexAcceptContinue(address wallet, bytes blsKey, uint256 pubkey, uint64 seqNoStartOld, uint128 stake, optional(uint128) virtualStake, mapping(uint8 => string) ProxyList, uint128 sumReputationCoef, optional(string) nodeVersion) public senderIs(_root) accept {
        ensureBalance();
        if (_wallet == wallet) {
            BlockKeeperContractRoot(_root).isSignerIndexContinue{value: 0.1 vmshell, flag: 1}(wallet, pubkey, blsKey, stake, _ready, seqNoStartOld, _signerIndex, virtualStake, ProxyList, sumReputationCoef, nodeVersion);
            _ready = true;
        } else {
            BlockKeeperContractRoot(_root).isSignerIndexContinue{value: 0.1 vmshell, flag: 1}(wallet, pubkey, blsKey, stake, true, seqNoStartOld, _signerIndex, virtualStake, ProxyList, sumReputationCoef, nodeVersion);
        }
    }

    function destroy() public senderIs(_wallet) accept {
        ensureBalance();
        selfdestruct(_root);
    }

    function getReadyStatus() external view returns(bool ready) {
        return _ready;
    }
 
    function getVersion() external pure returns(string, string) {
        return (version, "SignerIndex");
    }
}
