// SPDX-License-Identifier: GPL-3.0-or-later
/*
 * GOSH contracts
 *
 * Copyright (C) 2022 Serhii Horielyshev, GOSH pubkey 0xd060e0375b470815ea99d6bb2890a2a726c5b0579b83c742f5bb70e10a771a04
 */
pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";

contract BLSKeyIndex is Modifiers {
    string constant version = "1.0.0";
    bytes static _bls;
    address static _root;
    address _wallet;
    uint256 _wallet_pubkey;
    uint256 _stake;
    uint16 _signerIndex;
    bool _ready;

    constructor (
        address wallet,
        uint256 pubkey,
        uint256 stake,
        uint16 signerIndex
    ) accept {
        getMoney();
        _ready = false;
        _wallet = wallet;
        _wallet_pubkey = pubkey;
        _stake = stake;
        _signerIndex = signerIndex;
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLS_KEY) { return; }
        gosh.mintshell(FEE_DEPLOY_BLS_KEY);
    }

    function isBLSKeyAccept(mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        getMoney();
        BlockKeeperContractRoot(_root).isBLSAccepted{value: 0.1 vmshell}(_wallet_pubkey, _bls, _stake, _ready, _signerIndex, ProxyList);
        _ready = true;
    }

    function isBLSKeyAcceptContinue(uint64 seqNoStartOld, mapping(uint8 => string) ProxyList) public senderIs(_root) accept {
        getMoney();
        BlockKeeperContractRoot(_root).isBLSAcceptedContinue{value: 0.1 vmshell}(_wallet_pubkey, _bls, _stake, _ready, seqNoStartOld, _signerIndex, ProxyList);
        _ready = true;
    }

    function destroyRoot() public senderIs(_root) accept {
        getMoney();
        selfdestruct(_root);
    }

    function destroy() public senderIs(_wallet) accept {
        getMoney();
        selfdestruct(_root);
    }
    
    function getVersion() external pure returns(string, string) {
        return (version, "BLSKey");
    }
}
