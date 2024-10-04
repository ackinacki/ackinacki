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
import "./BlockKeeperCoolerContract.sol";
import "./BlockKeeperEpochContract.sol";

contract BlockKeeperSlashContract is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    uint256 static _owner_pubkey;
    address _root; 
    uint64 static _seqNoStart;
    mapping(uint256 => bool) _slashMember;
    uint128 _precision = 1e12;
    uint128 _isSlash = 0;
    uint256 _walletId;

    constructor (
        mapping(uint8 => TvmCell) code,
        uint256 walletId
    ) {
        _code = code;
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, uint256 hashwalletsalt, uint256 hashpreepochsalt, address root) = abi.decode(data, (string, uint256, uint256, address));
        require(BlockKeeperLib.versionLib == lib, ERR_SENDER_NO_ALLOWED);
        _root = root;
        _walletId = walletId;
        TvmBuilder b;
        b.store(_code[m_AckiNackiBlockKeeperNodeWalletCode]);
        uint256 hashwallet = tvm.hash(b.toCell());
        delete b;
        require(hashwallet == hashwalletsalt, ERR_SENDER_NO_ALLOWED);
        b.store(_code[m_BlockKeeperPreEpochCode]);
        uint256 hashpreepoch = tvm.hash(b.toCell());
        delete b;
        require(hashpreepoch == hashpreepochsalt, ERR_SENDER_NO_ALLOWED);
        require(msg.sender == BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey, _seqNoStart), ERR_SENDER_NO_ALLOWED);
    }

    function getMoney() private pure {
        if (address(this).balance > FEE_DEPLOY_BLOCK_KEEPER_SLASH) { return; }
        mintshell(FEE_DEPLOY_BLOCK_KEEPER_SLASH);
    }

    function getRequestToSlashBlockKeeper(uint256 pubkey, uint64 seqNoStart, uint128 numberOfActiveBlockKeepers) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, pubkey, seqNoStart)) {
        getMoney();
        if (_slashMember[pubkey] == true) {
            return;
        }
        _slashMember[pubkey] = true;
        _isSlash += _precision / numberOfActiveBlockKeepers;
        if (_isSlash > _precision * 66 / 100) {
            slash();
        }
    }

    function slash() private {
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _root, _owner_pubkey, _seqNoStart)).slash{value: 0.1 ton, flag: 1}();
        BlockKeeperCooler(BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], _root, _owner_pubkey, _seqNoStart)).slash{value: 0.1 ton, flag: 1}();
        selfdestruct(_root);
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
        return (_owner_pubkey, _root, _seqNoStart, BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], _root, _owner_pubkey));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperSlashContract");
    }  
}
