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
import "./AckiNackiBlockKeeperNodeWallet.sol";


contract BlockKeeperCooler is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint64 _seqNoFinish;
    address _owner;
    bytes _bls_pubkey;
    uint256 _walletId;
    uint256 _stake;
    uint16 _signerIndex;

    constructor (
        uint64 waitStep,
        address owner,
        address root,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint256 walletId,
        uint16 signerIndex
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address epoch) = abi.decode(data, (string, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == epoch, ERR_SENDER_NO_ALLOWED);
        _seqNoFinish = block.seqno + waitStep;
        _owner = owner;
        _bls_pubkey = bls_pubkey;
        _code = code;
        _walletId = walletId;
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).updateLockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, block.timestamp);
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET);
    }

    function slash(uint8 slash_type) public senderIs(_owner) accept {  
        getMoney();
        if (slash_type == FULL_STAKE_SLASH) {
            AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, 0);
            selfdestruct(_root);
            return;
        }                
        uint256 slash_stake = _stake * PART_STAKE_PERCENT_0 / 100;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(slash_stake);
        _root.transfer({value: 0.1 ton, currencies: data_cur, flag: 1});
        _stake -= slash_stake;
        AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, slash_stake);
    }

    function touch() public saveMsg {       
        if (_seqNoFinish <= block.seqno) { tvm.accept(); }
        else { return; }    
        getMoney();
        AckiNackiBlockKeeperNodeWallet(_owner).unlockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart);
        selfdestruct(_owner);
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root, 
        uint64 seqNoStart,
        uint64 seqNoFinish,
        address owner,
        uint256 walletId,
        uint16 signerIndex) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner, _walletId, _signerIndex);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperCooler");
    }    
}
