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
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperEpochProxyList.sol";

contract BlockKeeperPreEpoch is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint64 _seqNoDestruct;
    uint32 _epochDuration;
    uint64 _waitStep;
    address _owner;
    bytes _bls_pubkey;
    varuint32 _stake;
    uint16 _signerIndex;
    uint128 _sumReputationCoef;
    LicenseStake[] _licenses;
    address _wallet;
    optional(uint128) _virtualStake;

    constructor (
        uint64 waitStep,
        uint32 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint16 signerIndex, 
        uint128 rep_coef,
        LicenseStake[] licenses,
        optional(uint128) virtualStake,
        mapping(uint8 => string) ProxyList
    ) {
        _code = code;
        TvmBuilder b;
        b.store(_code[m_AckiNackiBlockKeeperNodeWalletCode]);
        uint256 hashwallet = tvm.hash(b.toCell());
        delete b;
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, uint256 hashwalletsalt, address root) = abi.decode(data, (string, uint256, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        require(hashwallet == hashwalletsalt, ERR_SENDER_NO_ALLOWED);
        _root = root;
        require(msg.sender == BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] , _root, _owner_pubkey), ERR_SENDER_NO_ALLOWED);
        _owner = msg.sender;
        _waitStep = waitStep;
        _bls_pubkey = bls_pubkey;
        _epochDuration = epochDuration;
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        _sumReputationCoef = rep_coef;
        _licenses = licenses;
        _virtualStake = virtualStake;
        _seqNoDestruct = _seqNoStart * 2 - block.seqno + 1;
        ensureBalance();
        _wallet = msg.sender;
        AckiNackiBlockKeeperNodeWallet(_wallet).setLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _stake, _bls_pubkey, _signerIndex, licenses);
        new BlockKeeperEpochProxyList {
                stateInit: BlockKeeperLib.composeBlockKeeperEpochProxyListStateInit(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root), 
                value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST),
                wid: 0, 
                flag: 1
        } (_code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _seqNoStart, ProxyList);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST + 1 vmshell) { return; }
        gosh.mintshell(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST + 1 vmshell);
    }

    function changeReputation(bool is_inc, uint128 value) public senderIs(_wallet) {
        if (is_inc) {
            _sumReputationCoef += value;
        } else {
            _sumReputationCoef -= value;
        }
    }

    function touch() public saveMsg {       
        if (_seqNoStart <= block.seqno) { tvm.accept(); }
        else { return; } 
        if (_seqNoDestruct < block.seqno) { 
            AckiNackiBlockKeeperNodeWallet(_wallet).deleteLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _bls_pubkey, _signerIndex, _licenses);
            selfdestruct(_wallet);
            return;
        }
        ensureBalance();
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = _stake;
        
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart);
        address epoch = new BlockKeeperEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: data_cur,
            wid: 0, 
            flag: 1
        } (_waitStep, _epochDuration, _bls_pubkey, _code, false, _sumReputationCoef, _signerIndex, _licenses, _virtualStake);
        selfdestruct(epoch);
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        uint256 pubkey,
        address root, 
        uint64 seqNoStart,
        address owner) 
    {
        return  (_owner_pubkey, _root, _seqNoStart, _owner);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperPreEpoch");
    }
}
