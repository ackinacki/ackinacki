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
import "./libraries/VerifiersLib.sol";
import "./PopCoinRoot.sol";
import "./Game.sol";
import "./PopitGame.sol";
import "./MobileVerifiersContractRoot.sol";

contract PopCoinWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    address _popcoinroot;
    string static _name;
    address static _owner;
    address _root;
    uint128 _value;
    mapping(uint256 => uint128) _popits_candidate;
    address _popitGame;

    bool _isReady;
    bool _isPlay;
    address _game;
    uint256 _root_pubkey;
    uint32 _start_time;

    constructor (
        TvmCell PopitGameCode,
        TvmCell GameCode,
        uint128 value,
        address popcoinroot,
        uint256 root_pubkey
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root, uint256 hash) = abi.decode(data, (string, address, uint256));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        require(hash == tvm.hash(PopitGameCode), ERR_INVALID_SENDER);
        _root = root;
        _popcoinroot = popcoinroot;
        _code[m_Game] = GameCode;
        require(msg.sender == VerifiersLib.calculatePopitGameAddress(PopitGameCode, _root, _owner), ERR_INVALID_SENDER);
        _value = value;
        _popitGame = msg.sender;
        _root_pubkey = root_pubkey;
        _start_time = block.timestamp;
        if (_value == 0) {
            PopCoinRoot(_popcoinroot).isReady{value: 0.1 vmshell, flag: 1}(_owner, _value, _popitGame);
            return;
        }
        PopCoinRoot(_popcoinroot).mintValue{value: 0.1 vmshell, flag: 1}(_owner, _value);
        PopitGame(_popitGame).popCoinWalletDeployed{value: 0.1 vmshell, flag: 1}(_name);
    }

    function deleteCandidate(uint256 index) public senderIs(_owner) accept {
        ensureBalance();
        delete _popits_candidate[index];
    }

    function addValue(uint256 id, uint128 value) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_popits_candidate.exists(id) == false, ERR_ALREADY_EXIST);
        PopCoinRoot(_popcoinroot).mintValuePopit{value: 0.1 vmshell, flag: 1}(_owner, id, value);
        _popits_candidate[id] = value;
    }

    function addValueOld(uint128 value) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        PopCoinRoot(_popcoinroot).mintValueOld{value: 0.1 vmshell, flag: 1}(_owner, value);
        _value += value;
    }

    function activatePopit(uint256 id, uint16 indexRoot) public view senderIs(_owner) accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        require(_popits_candidate.exists(id) == true, ERR_NOT_EXIST);
        PopCoinRoot(_popcoinroot).isReadyPopit{value: 0.1 vmshell, flag: 1}(_owner, indexRoot, id, _popits_candidate[id], _popitGame);
    }

    function setReadyPopit(uint256 index) public senderIs(_popcoinroot) accept {
        ensureBalance();
        _value += _popits_candidate[index];
        delete _popits_candidate[index];
    }


    function updateGameCode() public view senderIs(_owner) accept {
        require(_isPlay == false, ERR_ALREADY_PLAY);
        ensureBalance();
        PopitGame(_popitGame).updateGameCodeForWallet{value: 0.1 vmshell, flag: 1}(_name);
    }

    function setGameCode(TvmCell GameCode) public senderIs(_popitGame) accept {
        require(_isPlay == false, ERR_ALREADY_PLAY);
        ensureBalance();
        _code[m_Game] = GameCode;
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function activate() public view senderIs(_owner) accept {
        ensureBalance();
        if (_isReady == true) { return; }
        PopCoinRoot(_popcoinroot).isReady{value: 0.1 vmshell, flag: 1}(_owner, _value, _popitGame);
    }

    function setReady() public senderIs(_popcoinroot) accept {
        ensureBalance();
        _isReady = true;
    }
    
    function deployGame() public view senderIs(_owner) accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        require(_isPlay == false, ERR_ALREADY_PLAY);
        PopitGame(_popitGame).deployGame{value: 0.1 vmshell, flag: 1}(_name);
    }

    function deployGameFinal(uint64 mbiCur) public senderIs(_popitGame) accept {
        ensureBalance();
        _isPlay = true;
        TvmCell data = VerifiersLib.composeGameStateInit(_code[m_Game], _root, _owner, _popcoinroot, address(this));
        _game = new Game {stateInit: data, value: varuint16(FEE_DEPLOY_GAME), wid: 0, flag: 1}(_code[m_Game], mbiCur, _name);
    }

    function gameDestroyed() public senderIs(_game) accept {
        ensureBalance();
        _isPlay = false;
        PopitGame(_popitGame).gameDestroyed{value: 0.1 vmshell, flag: 1}(_name);
    }

    function destroy() public senderIs(_owner) {
        require(_isPlay == false, ERR_ALREADY_PLAY);
        selfdestruct(_popitGame);
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getDetails() external view returns(
        address popcoinroot,
        string name,
        address owner,
        uint128 value,
        bool isReady,
        bool isPlay,
        address gameAddress,
        mapping(uint256 => uint128) popits_candidate,
        uint64 start_time
    ) {
        return  (_popcoinroot, _name, _owner, _value, _isReady, _isPlay, _game, _popits_candidate, _start_time);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "PopCoinWallet");
    }
}
