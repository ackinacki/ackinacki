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
import "./BlockKeeperSlashContract.sol";


contract BlockKeeperCooler is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    uint64 _seqNoFinish;
    address _owner;
    bytes _bls_pubkey;
    mapping(uint256 => bool) _slashMember;
    uint128 _slashed = 0;
    uint256 _walletId;

    constructor (
        uint64 waitStep,
        address owner,
        address root,
        bytes bls_pubkey,
        mapping(uint256 => bool) slashMember,
        uint128 slashed,
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
        _slashMember = slashMember;
        _slashed = slashed;
        _code = code;
        _walletId = walletId;
        AckiNackiBlockKeeperNodeWallet(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey)).updateLockStakeCooler{value: 0.1 ton, flag: 1}(_seqNoStart);
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET) { return; }
        mintshell(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET);
    }

    function slash() public senderIs(BlockKeeperLib.calculateBlockKeeperSlashAddress(_code[m_BlockKeeperSlashCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart)) accept {  
        getMoney();
        AckiNackiBlockKeeperNodeWallet(_owner).slashCooler{value: 0.1 ton, flag: 1}(_seqNoStart);
        destroy(_root);
    }

    function touch() public saveMsg {       
        if (_seqNoFinish <= block.seqno) { tvm.accept(); }
        else { return; }    
        getMoney();
        if (address(this).balance < 0.2 ton) { return; }
        AckiNackiBlockKeeperNodeWallet(_owner).unlockStakeCooler{value: 0.1 ton, flag: 1}(_seqNoStart);
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
        mapping(uint256 => bool) slashMember,
        uint256 walletId) 
    {
        return (_owner_pubkey, _root, _seqNoStart, _seqNoFinish, _owner, _slashMember, _walletId);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperCooler");
    }    
}
