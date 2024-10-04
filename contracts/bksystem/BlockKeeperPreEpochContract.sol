// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./BlockKeeperContractRoot.sol";
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperSlashContract.sol";

contract BlockKeeperPreEpoch is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint32 _epochDuration;
    uint64 _waitStep;
    address _owner;
    bytes _bls_pubkey;
    varuint32 _stake;
    uint256 _walletId;

    constructor (
        uint64 waitStep,
        uint32 epochDuration,
        bytes bls_pubkey,
        mapping(uint8 => TvmCell) code,
        uint256 walletId
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
        _walletId = walletId;
        AckiNackiBlockKeeperNodeWallet(msg.sender).setLockStake{value: 0.1 ton, flag: 1}(_seqNoStart, _stake);
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET) { return; }
        mintshell(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET);
    }

    function touch() public saveMsg {       
        if (_seqNoStart <= block.seqno) { tvm.accept(); }
        else { return; } 
        getMoney();
        if (address(this).balance < FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET + FEE_DEPLOY_BLOCK_KEEPER_SLASH + 1e9) { return; }
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = _stake;
        TvmCell dataslash = BlockKeeperLib.composeBlockKeeperSlashStateInit(_code[m_BlockKeeperSlashCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart);
        new BlockKeeperSlashContract {
            stateInit: dataslash, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_SLASH),
            wid: 0, 
            flag: 1
        } (_code, _walletId);
        
        TvmCell data = BlockKeeperLib.composeBlockKeeperEpochStateInit(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart);
        address epoch = new BlockKeeperEpoch {
            stateInit: data, 
            value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_WALLET),
            currencies: data_cur,
            wid: 0, 
            flag: 1
        } (_waitStep, _epochDuration, _bls_pubkey, _code, false, _walletId, 0);
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
        address owner,
        uint256 walletId) 
    {
        return  (_owner_pubkey, _root, _seqNoStart, _owner, _walletId);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperPreEpoch");
    }
}
