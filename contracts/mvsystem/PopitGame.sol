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
import "./PopCoinWallet.sol";
import "./MobileVerifiersContractRoot.sol";
import "./Boost.sol";

contract PopitGame is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    address static _owner;
    uint64 _mbiCur;
    address _root;
    uint32 _startTime;
    uint256 _root_pubkey;
    address _boost;
    uint128 _rewards;
    event PopCoinRootReceived(string name);
    event PopCoinWalletReceived(string name);

    constructor (
        mapping(uint8 => TvmCell) code,
        uint256 root_pubkey,
        uint128 index
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        _root = root;
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        tvm.accept();
        _startTime = block.timestamp;
        _code = code;
        _root_pubkey = root_pubkey;
        data = VerifiersLib.composeBoostStateInit(_code[m_Boost], address(this), _root);
        _boost = new Boost {stateInit: data, value: varuint16(FEE_DEPLOY_BOOST), wid: 0, flag: 1}(_owner, _root_pubkey);
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _code[id] = code;
    }

    function setNewOwner(address owner) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _owner = owner;
    }

    function addValuePopit(string name, uint256 id, uint64 value) public view senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, _owner)) accept {
        PopCoinWallet(msg.sender).addValuePopitGame{value: 0.1 vmshell, flag: 1}(id, value, _mbiCur);
    }

    function popCoinRootDeployed(string name) public view senderIs(VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], _root, name)) accept {
        ensureBalance();
        address addrExtern = address.makeAddrExtern(0, 0);
        emit PopCoinRootReceived{dest: addrExtern}(name);
    }

    function popCoinWalletDeployed(string name) public view senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, _owner)) accept {
        ensureBalance();
        address addrExtern = address.makeAddrExtern(0, 0);
        emit PopCoinWalletReceived{dest: addrExtern}(name);
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function setMbiCur(uint64 mbiCur) public senderIs(_boost) accept {
        ensureBalance();
        _mbiCur = mbiCur;
    }

    function deployPopCoinWallet(string name, uint64 value) public view onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_mbiCur != 0, ERR_WRONG_DATA);
        TvmCell data = VerifiersLib.composePopCoinWalletStateInit(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, _owner);
        new PopCoinWallet {stateInit: data, value: varuint16(FEE_DEPLOY_POP_COIN_WALLET), wid: 0, flag: 1}(_code[m_PopitGame], value, VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], _root, name), _root_pubkey, _mbiCur);
    }

    function deployPopCoinWalletOldTransfer(string name, uint64 value) public view onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        TvmCell data = VerifiersLib.composePopCoinWalletStateInit(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, _owner);
        new PopCoinWallet {stateInit: data, value: varuint16(FEE_DEPLOY_POP_COIN_WALLET), wid: 0, flag: 1}(_code[m_PopitGame], value, VerifiersLib.calculatePopCoinRootAddress(_code[m_PopCoinRoot], _root, name), _root_pubkey, _mbiCur);
    }

    function withdraw(uint128 value, address to) public senderIs(_owner) {
        ensureBalance();
        require(value <= address(this).currencies[CURRENCIES_ID], ERR_LOW_VALUE);   
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(value);
        to.transfer({value: 0.1 vmshell, currencies: data_cur, flag: 1});
        _mbiCur = 0;
        Boost(_boost).deleteMbiCur{value: 0.1 vmshell, flag: 1}();
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
        ensureBalance();
        tvm.accept();
        if (msg.sender != _owner) {
            _rewards += uint128(msg.currencies[CURRENCIES_ID]);
        }
    }

    //Getters
    function getPopCoinWalletAddress(string name) external view returns(address popCoinWalletAddress) {
        return VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], tvm.hash(_code[m_PopitGame]), _root, name, _owner);
    }

    function getDetails() external view returns(
        address owner,
        address root,
        uint32 startTime,
        uint64 mbiCur,
        address boost,
        uint128 rewards,
        uint128 minstake
    ) {
        return  (_owner, _root, _startTime, _mbiCur, _boost, _rewards, gosh.calcminstakebm(_rewards, block.timestamp - _startTime));
    }

    function getVersion() external pure returns(string, string) {
        return (version, "PopitGame");
    }
}
