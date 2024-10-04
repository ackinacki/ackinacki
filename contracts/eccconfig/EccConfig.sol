// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";

contract EccConfig is Modifiers {
    string constant version = "1.0.0";

    mapping(uint32 => EccData) _data;
    address _owner;

    constructor (
    ) {
        _owner = address.makeAddrStd(0, 0);
        mintshell(100000 ton);
    }

    function getMoney() private pure {
        if (address(this).balance > 100000 ton) { return; }
        mintshell(100000 ton);
    }

    function setNewToken(EccToken token, optional(address) to) public senderIs(_owner) {
        getMoney();
        require(_data.exists(token.key) == false, ERR_WRONG_KEY);
        EccData data;
        data.data = token;
        data.time = block.timestamp;
        _data[token.key] = data;
        mint(token.baseMinted, token.key);
        if (to.hasValue()) {
            mapping(uint32 => varuint32) data_cur;
            data_cur[token.key] = varuint32(token.baseMinted);
            to.get().transfer({value: 0.1 ton, flag: 1, currencies: data_cur});
        }
    }

    function getDetails() external view returns(mapping(uint32 => EccData) data) {
        return _data;
    }          
}
