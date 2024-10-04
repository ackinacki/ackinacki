// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >=0.66.0;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/BlockKeeperLib.sol";
import "./AckiNackiBlockKeeperNodeWallet.sol";
import "./BlockKeeperPreEpochContract.sol";
import "./BlockKeeperEpochContract.sol";
import "./BlockKeeperCoolerContract.sol";

contract BlockKeeperContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    uint32 _epochDuration = 1000;
    uint64 _epochCliff = 1000;
    uint64 _waitStep = 1000;
    uint128 _minBlockKeepers = 10;
    address _giver;
    uint256 _num_wallets = 0;
    uint256 _totalStake = 0;

    uint32 _networkStart;
    uint128 _numberOfActiveBlockKeepers = 0;

    constructor (
        address giver
    ) {
        _giver = giver;
        _networkStart = block.timestamp;
    }

    function getMoney() private pure {
        if (address(this).balance > 1000000 ton) { return; }
        mintshell(1000000 ton);
    }

    function setConfig(uint32 epochDuration, uint64 epochCliff, uint64 waitStep, uint128 minBlockKeepers) public onlyOwnerPubkey(tvm.pubkey()) accept {
        getMoney();
        _epochDuration = epochDuration;
        _epochCliff = epochCliff;
        _waitStep = waitStep;
        _minBlockKeepers = minBlockKeepers;
    }

    function deployAckiNackiBlockKeeperNodeWallet(uint256 pubkey) public accept {
        getMoney();
        TvmCell data = BlockKeeperLib.composeBlockKeeperWalletStateInit(_code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockKeeperNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_WALLET), wid: 0, flag: 1}(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperEpochCode], _code[m_BlockKeeperEpochCoolerCode], _code[m_BlockKeeperSlashCode], _num_wallets);
        _num_wallets += 1;
    }

    function increaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        getMoney();
        _totalStake += stake;
        _numberOfActiveBlockKeepers += 1;
        BlockKeeperEpoch(msg.sender).setStake{value: 0.1 ton, flag: 1}(_totalStake, _numberOfActiveBlockKeepers);
    }

    function decreaseActiveBlockKeeperNumber(uint256 pubkey, uint64 seqNoStart, uint256 stake) public senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        getMoney();
        _totalStake -= stake;
        _numberOfActiveBlockKeepers -= 1;
    }

    function receiveBlockKeeperRequestWithStakeFromWallet(uint256 pubkey, bytes bls_pubkey) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        if (msg.currencies[CURRENCIES_ID] < getminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1)) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 ton, currencies: msg.currencies, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(msg.sender).deployPreEpochContract{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 ton), currencies: msg.currencies, flag: 1}(_epochDuration, _epochCliff, _waitStep, bls_pubkey);
    }

    function receiveBlockKeeperRequestWithStakeFromWalletContinue(uint256 pubkey, bytes bls_pubkey, uint64 seqNoStartOld) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey)) accept {
        if (msg.currencies[CURRENCIES_ID] < getminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers)) {
            AckiNackiBlockKeeperNodeWallet(msg.sender).stakeNotAccepted{value: 0.1 ton, currencies: msg.currencies, flag: 1}();
            return;
        }
        AckiNackiBlockKeeperNodeWallet(msg.sender).deployBlockKeeperContractContinue{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_PRE_EPOCHE_WALLET + 0.5 ton), currencies: msg.currencies, flag: 1}(_epochDuration, _waitStep, seqNoStartOld, bls_pubkey);
    } 

    function setNewCode(uint8 id, TvmCell code) public onlyOwner accept saveMsg { 
        _code[id] = code;
    }

    function canDeleteEpoch(uint256 pubkey, uint64 seqNoStart, uint256 stake, uint32 epochDuration, uint32 reputationTime, uint256 totalStakeOld, uint128 numberOfActiveBlockKeepers, uint128 time)  public view senderIs(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)) accept {
        if (_numberOfActiveBlockKeepers - 1 >= _minBlockKeepers) {
            uint256 reward = 0;
            reward = getreward(numberOfActiveBlockKeepers, uint128(epochDuration), time, uint128(totalStakeOld), uint128(stake), uint128(reputationTime));
            mapping(uint32 => varuint32) data_cur;
            data_cur[CURRENCIES_ID] = varuint32(reward);
            mint(uint64(reward), CURRENCIES_ID);
            BlockKeeperEpoch(msg.sender).canDelete{value: varuint16(FEE_DEPLOY_BLOCK_KEEPER_EPOCHE_COOLER_WALLET + 1 ton), currencies: data_cur, flag: 1}(reward);
        }
    }

    function sendRequestToSlashBlockKeeper(uint256 slashpubkey, uint64 slashSeqNoStart, uint64 seqNoStart, uint256 pubkey) public view senderIs(BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey)) accept {
        BlockKeeperEpoch(BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart)).sendRequestToSlashBlockKeeper{value: 1 ton, flag: 1}(slashpubkey, slashSeqNoStart, _numberOfActiveBlockKeepers);
    } 

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwner accept saveMsg {
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade(cell);
    }

    function onCodeUpgrade(TvmCell cell) private pure {
    }
    
    //Fallback/Receive
    receive() external {
    }


    //Getters
    function getBlockKeeperCoolerAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperCoolerEpochAddress(_code[m_BlockKeeperEpochCoolerCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], _code[m_BlockKeeperEpochCode], address(this), pubkey, seqNoStart);
    }

    function getAckiNackiBlockKeeperNodeWalletAddress(uint256 pubkey) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperWalletAddress(_code[m_AckiNackiBlockKeeperNodeWalletCode] ,address(this), pubkey);
    }
    
    function getBlockKeeperEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address){
        return BlockKeeperLib.calculateBlockKeeperEpochAddress(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this), pubkey, seqNoStart);
    }

    function getBlockKeeperPreEpochAddress(uint256 pubkey, uint64 seqNoStart) external view returns(address) {
        return BlockKeeperLib.calculateBlockKeeperPreEpochAddress(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this), pubkey, seqNoStart);
    }

    function getDetails() external view returns(uint128 minStake, uint128 numberOfActiveBlockKeepers, uint256 numWallets) {
        return (getminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1), _numberOfActiveBlockKeepers, _num_wallets);
    }

    function getEpochCodeHash() external view returns(uint256 epochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperEpochCode(_code[m_BlockKeeperEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this)));
    }

    function getPreEpochCodeHash() external view returns(uint256 preEpochCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperPreEpochCode(_code[m_BlockKeeperPreEpochCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], address(this)));
    }

    function getSlashCodeHash() external view returns(uint256 slashCodeHash) {
        return tvm.hash(BlockKeeperLib.buildBlockKeeperSlashCode(_code[m_BlockKeeperSlashCode], _code[m_AckiNackiBlockKeeperNodeWalletCode], _code[m_BlockKeeperPreEpochCode], address(this)));
    }

    function getRewardOut(
        uint128 numberOfActiveBlockKeepers,
        uint128 stake,
        uint128 totalStake,
        uint128 reputationTime,
        uint128 timenetwork,
        uint128 epochDuration
        ) external pure returns(uint128 reward) {
        return getreward(numberOfActiveBlockKeepers, epochDuration, timenetwork, totalStake, stake, reputationTime);
    }     

    function getRewardNow(
        ) external view returns(uint128 reward) {
        return getreward(_numberOfActiveBlockKeepers, _epochDuration, uint128(block.timestamp - _networkStart), uint128(_totalStake), uint128(_totalStake), uint128(0));
    }     

    function getMinStakeNow() external view returns(uint128 minstake) {
        return getminstake(uint128(_epochDuration), uint128(block.timestamp - _networkStart), _numberOfActiveBlockKeepers, _numberOfActiveBlockKeepers + 1);
    }      

    function getVersion() external pure returns(string, string) {
        return (version, "BlockKeeperContractRoot");
    }   
}
