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

contract AckiNackiBlockKeeperNodeWalletConfig is Modifiers {
    string constant version = "1.0.0";
    address _root = address.makeAddrStd(0,0x7777777777777777777777777777777777777777777777777777777777777777);
    uint128 _node_id;
    address _wallet_addr;

    constructor (
        address wallet
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (uint128 node_id) = abi.decode(data, (uint128));
        require(msg.sender == _root, ERR_SENDER_NO_ALLOWED);
        _wallet_addr = wallet;
        _node_id = node_id;
    }

        //Getters
    function getDetails() external view returns(
        uint256 node_id,
        address wallet_addr
    ) {
        return  (_node_id, _wallet_addr);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "AckiNackiBlockKeeperNodeWalletConfig");
    }
}
