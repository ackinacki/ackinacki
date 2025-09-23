/*
 * Copyright (c) GOSH Technology Ltd. All rights reserved.
 * 
 * Acki Nacki and GOSH are either registered trademarks or trademarks of GOSH
 * 
 * Licensed under the ANNL. See License.txt in the project root for license information.
*/
pragma gosh-solidity >=0.76.1;
pragma ignoreIntOverflow;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./libraries/VerifiersLib.sol";
import "./PopCoinRoot.sol";
import "./PopitGame.sol";
import "./MobileVerifiersContractRoot.sol";

contract PopCoinWallet is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    address _popcoinroot;
    string static _name;
    address static _owner;
    address _root;
    uint64 _value;
    mapping(uint256 => uint64) _popits_candidate;
    mapping(uint256 => uint64) _popits_mbi;
    address _popitGame;

    bool _isReady;
    uint256 _root_pubkey;
    uint64 _mbiCurBase;

    constructor (
        TvmCell PopitGameCode,
        uint64 value,
        address popcoinroot,
        uint256 root_pubkey,
        uint64 mbiCur
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root, uint256 hash) = abi.decode(data, (string, address, uint256));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        require(hash == tvm.hash(PopitGameCode), ERR_INVALID_SENDER);
        _root = root;
        _popcoinroot = popcoinroot;
        _mbiCurBase = mbiCur;
        require(msg.sender == VerifiersLib.calculatePopitGameAddress(PopitGameCode, _root, _owner), ERR_INVALID_SENDER);
        _value = value;
        _popitGame = msg.sender;
        _root_pubkey = root_pubkey;
        if (_value == 0) {
            PopCoinRoot(_popcoinroot).isReady{value: 0.1 vmshell, flag: 1}(_owner, _value, _popitGame, mbiCur);
            return;
        }
        PopCoinRoot(_popcoinroot).mintValue{value: 0.1 vmshell, flag: 1}(_owner, _value, mbiCur);
        PopitGame(_popitGame).popCoinWalletDeployed{value: 0.1 vmshell, flag: 1}(_name);
    }

    function setNewOwner(address owner) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _owner = owner;
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _code[id] = code;
    }

    function deleteCandidate(uint256 index) public senderIs(_owner) accept {
        ensureBalance();
        delete _popits_candidate[index];
        delete _popits_mbi[index];
    }

    function addValue(uint256 id, uint64 value, uint64 mbiCur) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_popits_candidate.exists(id) == false, ERR_ALREADY_EXIST);
        _popits_candidate[id] = value;
        _popits_mbi[id] = mbiCur;
        PopitGame(_popitGame).addValuePopit{value: 0.1 vmshell, flag: 1}(_name, id, value);
    }

    function addValuePopitGame(uint256 id, uint64 value, uint64 mbiCur) public senderIs(_popitGame) accept {
        ensureBalance();
        if (mbiCur == 0) { 
            delete _popits_candidate[id];
            delete _popits_mbi[id];
            return;
        }
        PopCoinRoot(_popcoinroot).mintValuePopit{value: 0.1 vmshell, flag: 1}(_owner, id, value, mbiCur);
    }

    function addValueOld(uint64 value) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        PopCoinRoot(_popcoinroot).mintValueOld{value: 0.1 vmshell, flag: 1}(_owner, value);
        _value += value;
    }

    function activatePopit(uint256 id, uint16 indexRoot) public view senderIs(_owner) accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        require(_popits_candidate.exists(id) == true, ERR_NOT_EXIST);
        PopCoinRoot(_popcoinroot).isReadyPopit{value: 0.1 vmshell, flag: 1}(_owner, indexRoot, id, _popits_candidate[id], _popitGame, _popits_mbi[id]);
    }

    function setReadyPopit(uint256 index) public senderIs(_popcoinroot) accept {
        ensureBalance();
        _value += _popits_candidate[index];
        delete _popits_candidate[index];
        delete _popits_mbi[index];
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function activate() public view senderIs(_owner) accept {
        ensureBalance();
        if (_isReady == true) { return; }
        PopCoinRoot(_popcoinroot).isReady{value: 0.1 vmshell, flag: 1}(_owner, _value, _popitGame, _mbiCurBase);
    }

    function setReady() public senderIs(_popcoinroot) accept {
        ensureBalance();
        _isReady = true;
    }

    function destroy() public senderIs(_owner) accept {
        ensureBalance();
        selfdestruct(_popitGame);
    }

    function destroyRoot() public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        selfdestruct(_popitGame);
    }

    function updateCode(TvmCell newcode, TvmCell cell) public view onlyOwnerPubkey(_root_pubkey) accept  {
        ensureBalance();
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
    function getDetails() external view returns(
        address popcoinroot,
        string name,
        address owner,
        uint64 value,
        bool isReady,
        mapping(uint256 => uint64) popits_candidate,
        mapping(uint256 => uint64) popits_mbi
    ) {
        return  (_popcoinroot, _name, _owner, _value, _isReady, _popits_candidate, _popits_mbi);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "PopCoinWallet");
    }
}
