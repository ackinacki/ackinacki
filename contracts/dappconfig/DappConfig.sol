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

contract DappConfig is Modifiers {
    string constant version = "1.0.0";

    CreditConfig _data;
    address _owner;
    address _voter;
    uint256 _dapp_id;

    constructor (
        uint256 dapp_id,
        CreditConfig data
    ) {
        dapp_id;
        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (uint256 dapp_id_salt) = abi.decode(salt, (uint256));
        _owner = address(0x9999999999999999999999999999999999999999999999999999999999999999);
        _dapp_id = dapp_id_salt;
        _voter = address.makeAddrStd(0, 0);
        require(msg.sender == _owner, ERR_INVALID_SENDER);
        _data = data;
    }

    function setNewConfig(
        uint128 minted
    ) public internalMsg senderIs(address(this)) functionID(5) {
        _data.available_balance -= int128(minted);
    }

    receive() external {
        tvm.accept();
        _data.available_balance += int128(msg.currencies[CURRENCIES_ID_SHELL]);
    }

    function getDetails() external view returns(uint256 dapp_id, CreditConfig data) {
        return (_dapp_id, _data);
    }          
}
