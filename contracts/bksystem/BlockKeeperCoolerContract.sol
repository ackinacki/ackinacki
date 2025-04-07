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
    uint256 _stake;
    uint16 _signerIndex;
    LicenseStake[] _licenses;

    constructor (
        uint64 waitStep,
        address owner,
        address root,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint16 signerIndex,
        LicenseStake[] licenses,
        uint128 epochDuration,
        bool is_continue
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
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        _licenses = licenses;
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).updateLockStakeCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, block.timestamp, _licenses, epochDuration, is_continue);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET);
    }

    function slash(uint8 slash_type) public senderIs(_owner) accept {  
        ensureBalance();
        if (slash_type == FULL_STAKE_SLASH) {
            AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, 0, _licenses);
            selfdestruct(_root);
            return;
        }                
        uint256 slash_stake = _stake * slash_type / 100;
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(slash_stake);
        _root.transfer({value: 0.1 vmshell, currencies: data_cur, flag: 1});
        _stake -= slash_stake;
        AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 vmshell, flag: 1}(_seqNoStart, slash_type, slash_stake, _licenses);            
    }

    function touch() public saveMsg {       
        if (_seqNoFinish <= block.seqno) { tvm.accept(); }
        else { return; }    
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = address(this).currencies[CURRENCIES_ID];
        AckiNackiBlockKeeperNodeWallet(_owner).unlockStakeCooler{value: 0.1 vmshell, flag: 1, currencies: data_cur}(_seqNoStart, _licenses);
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
        uint16 signerIndex) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner, _signerIndex);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperCooler");
    }    
}
