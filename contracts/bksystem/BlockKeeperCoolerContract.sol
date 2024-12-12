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

    constructor (
        uint64 waitStep,
        address owner,
        address root,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint256 walletId
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
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).updateLockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, block.timestamp);
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET);
    }

    function slash() public senderIs(_owner) accept {  
        getMoney();
        AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart);
        destroy(_root);
    }

    function touch() public saveMsg {       
        if (_seqNoFinish <= block.seqno) { tvm.accept(); }
        else { return; }    
        getMoney();
        if (address(this).balance < 0.2 vmshell) { return; }
        AckiNackiBlockKeeperNodeWallet(_owner).unlockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart);
        destroy(_owner);
    }

    function destroy(address to) private accept {
        selfdestruct(to);
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
        uint256 walletId) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner, _walletId);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperCooler");
    }    
}
