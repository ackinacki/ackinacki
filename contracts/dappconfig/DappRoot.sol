// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
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
        mintshell(100000 ton);
    }

    function setNewCode(uint8 id, TvmCell code) public senderIs(_owner) accept saveMsg { 
        getMoney();
        _code[id] = code;
    }

    function getMoney() private pure {
        if (address(this).balance > 100000 ton) { return; }
        mintshell(100000 ton);
    }

    function deployNewConfig(
        uint256 dapp_id,
        bool is_unlimit,
        int128 available_credit,
	    uint128 credit_per_block,
	    uint128 available_credit_max_value,
        uint128 start_block_seqno,
	    uint128 end_block_seqno
    ) public view onlyOwnerPubkey(tvm.pubkey()) accept {
        getMoney();
        CreditConfig info = CreditConfig(
            is_unlimit,
            available_credit,
            credit_per_block,
            available_credit_max_value,
            start_block_seqno,
            end_block_seqno,
            block.seqno,
            uint128(0)
        );
        TvmCell data = DappLib.composeDappConfigStateInit(_code[m_ConfigCode], dapp_id);
        mint(1000 ton, CURRENCIES_ID_SHELL);
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID_SHELL] = varuint32(1000 ton);
        new DappConfig {stateInit: data, value: varuint16(FEE_DEPLOY_CONFIG), currencies: data_cur, wid: 0, flag: 1}(info);
    }

    function getConfigAddr(uint256 dapp_id) external view returns(address config) {
        return DappLib.calculateDappConfigAddress(_code[m_ConfigCode], dapp_id);
    }            
}
