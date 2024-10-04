// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
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
        CreditConfig data
    ) {
        TvmCell salt = abi.codeSalt(tvm.code()).get();
        (uint256 dapp_id) = abi.decode(salt, (uint256));
        _owner = address(0x9999999999999999999999999999999999999999999999999999999999999999);
        _dapp_id = dapp_id;
        _voter = address.makeAddrStd(0, 0);
        require(msg.sender == _owner, ERR_INVALID_SENDER);
        mintshell(1000 ton);
        _data = data;
    }

    function getMoney() private pure {
        if (address(this).balance > 1000 ton) { return; }
        mintshell(1000 ton);
    }

    function setNewConfig(
        bool is_unlimit,
        int128 available_credit,
	    uint128 credit_per_block,
	    uint128 available_credit_max_value,
        uint128 start_block_seqno,
	    uint128 end_block_seqno
    ) public senderIs(_voter) functionID(5) {
        getMoney();
        _data.is_unlimit = is_unlimit;
        _data.available_credit = available_credit;
        _data.credit_per_block = credit_per_block;
        _data.available_credit_max_value = available_credit_max_value;
        _data.start_block_seqno = start_block_seqno;
        _data.end_block_seqno = end_block_seqno;
        _data.last_updated_seqno = block.seqno;
    }

    function getDetails() external view returns(uint256 dapp_id, CreditConfig data) {
        return (_dapp_id, _data);
    }          
}
