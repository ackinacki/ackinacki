/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
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
    uint64 _epochDuration;
    uint64 _waitStep;
    bytes _bls_pubkey;
    varuint32 _stake;
    uint16 _signerIndex;
    uint128 _sumReputationCoef;
    LicenseStake[] _licenses;
    address _wallet;
    optional(uint128) _virtualStake;
    uint128 _reward_sum;
    string _myIp;
    bool isDestroy = false;

    constructor (
        uint64 waitStep,
        uint64 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint16 signerIndex, 
        uint128 rep_coef,
        LicenseStake[] licenses,
        optional(uint128) virtualStake,
        mapping(uint8 => string) ProxyList,
        uint128 reward_sum,
        string myIp,
        uint64 epochCliff
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
        _waitStep = waitStep;
        _bls_pubkey = bls_pubkey;
        _epochDuration = epochDuration;
        _stake = msg.currencies[CURRENCIES_ID];
        _signerIndex = signerIndex;
        _sumReputationCoef = rep_coef;
        _licenses = licenses;
        _virtualStake = virtualStake;
        _seqNoDestruct = _seqNoStart + uint64(PRE_EPOCH_DESTRUCT_MULT)*epochCliff;
        _reward_sum = reward_sum;
        _wallet = msg.sender;
        _myIp = myIp;
        mapping(uint8 => TvmCell) code_for_proxy;
        code_for_proxy[m_AckiNackiBlockKeeperNodeWalletCode] = _code[m_AckiNackiBlockKeeperNodeWalletCode];
        code_for_proxy[m_BlockKeeperPreEpochCode] = _code[m_BlockKeeperPreEpochCode];
        code_for_proxy[m_BlockKeeperEpochCode] = _code[m_BlockKeeperEpochCode];
        AckiNackiBlockKeeperNodeWallet(_wallet).setLockStake{value: 0.1 vmshell, flag: 1}(_seqNoStart, _stake, _bls_pubkey, _signerIndex, licenses);
        new BlockKeeperEpochProxyList {
                stateInit: BlockKeeperLib.composeBlockKeeperEpochProxyListStateInit(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root), 
                value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PROXY_LIST),
                wid: 0, 
                flag: 1
        } (code_for_proxy, _seqNoStart, ProxyList);
    }

    function ensureBalance() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 1 vmshell) { return; }
        gosh.mintshellq(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 1 vmshell);
    }

    function destroy(bool isProxyDelete) public view senderIs(address(this)) accept {
        if (isProxyDelete) {
            BlockKeeperEpochProxyList(BlockKeeperLib.calculateBlockKeeperEpochProxyListAddress(_code[m_BlockKeeperEpochProxyListCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperPreEpochCode], _owner_pubkey, _root)).destroy{value: 0.1 vmshell, flag: 1}(_seqNoStart);
        }
        AckiNackiBlockKeeperNodeWallet(_wallet).deleteLockStake{value: 0.1 vmshell, flag: 161, bounce: false}(_seqNoStart, _bls_pubkey, _signerIndex, _licenses);
    }
        
    function touch() public pure accept { 
        ensureBalance();      
        this.touchIn{value: 0.1 vmshell, flag: 1}();
    }

    function touchIn() public senderIs(address(this)) accept { 
        ensureBalance();      
        if (_seqNoStart > block.seqno) { return; } 
        if (isDestroy) { return; }
        isDestroy = true;
        if (_seqNoDestruct < block.seqno) { 
            this.destroy{value: 0.1 vmshell, flag: 1}(false);
            return;
        }
                
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart);
        new BlockKeeperEpoch {
            stateInit: data, 
            value: 0.1 vmshell,
            wid: 0, 
            flag: 161, 
            bounce: false
        } (_waitStep, _epochDuration, _bls_pubkey, _code, _sumReputationCoef, _signerIndex, _licenses, _virtualStake, _reward_sum, _myIp, false, null);
    }

    function cancelPreEpoch() public senderIs(_wallet) accept {     
        ensureBalance();  
        if (isDestroy) { return; }
        isDestroy = true;
        this.destroy{value: 0.1 vmshell, flag: 1}(true);
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
        return  (_owner_pubkey, _root, _seqNoStart, _wallet);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperPreEpoch");
    }
}
