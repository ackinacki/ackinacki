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
import "./libraries/DappLib.sol";
import "DappConfig.sol";

contract DappRoot is Modifiers {
    string constant version = "1.0.0";
    
    mapping(uint8 => TvmCell) _code;
    address _owner;
    

    constructor (
    ) {
        _owner = address.makeAddrStd(0, 0);
        gosh.mintshell(100000 vmshell);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerPubkey(tvm.pubkey()) accept saveMsg { 
        getMoney();
        _code[id] = code;
    }

    function getMoney() private pure {
        if (address(this).balance > 100000 vmshell) { return; }
        gosh.mintshell(100000 vmshell);
    }

    function deployNewConfigCustom(
        uint256 dapp_id
    ) public view accept {
        getMoney();
        CreditConfig info = CreditConfig(
            false,
            0
        );
        TvmCell data = DappLib.composeDappConfigStateInit(_code[m_ConfigCode], dapp_id);
        new DappConfig {stateInit: data, value: varuint16(FEE_DEPLOY_CONFIG), wid: 0, flag: 1}(dapp_id, info);
    }

    function deployNewConfig(
        uint256 dapp_id,
        bool is_unlimit,
        int128 available_balance
    ) public view onlyOwnerPubkey(tvm.pubkey()) accept {
        getMoney();
        CreditConfig info = CreditConfig(
            is_unlimit,
            available_balance
        );
        TvmCell data = DappLib.composeDappConfigStateInit(_code[m_ConfigCode], dapp_id);
        new DappConfig {stateInit: data, value: varuint16(FEE_DEPLOY_CONFIG), wid: 0, flag: 1}(dapp_id, info);
    }

    function getConfigAddr(uint256 dapp_id) external view returns(address config) {
        return DappLib.calculateDappConfigAddress(_code[m_ConfigCode], dapp_id);
    }            
}
