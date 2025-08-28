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
import "./PopCoinWallet.sol";
import "./PopitGame.sol";
import "./MobileVerifiersContractRoot.sol";

contract PopCoinRoot is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;
    bool _isReady;
    uint256 _popitgamehash;
    address _root;
    string static _name;
    bool _isPublic;
    uint128 _totalSupply;
    uint16 _maxPopitIndex = 0;
    mapping(uint16 => Popit) _popits_value;
    mapping(uint16 => PopitMedia) _popits_media;
    mapping(uint256 => PopitCandidateWithMedia) _popits_candidate;
    uint256 _root_pubkey;
    string _description;
    uint128 _rewards;
    uint128 _basicValue;
    address _popitGameOwner;

    constructor (
        TvmCell PopCoinWalletCode,
        uint256 popitgamehash,
        uint16 maxPopitIndex, //Size of popits_media - 1
        mapping(uint16 => PopitMedia) popits_media,
        string description,
        uint256 root_pubkey,
        bool isPublic,
        uint128 index,
        address popitGameOwner
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        _root = root;
        uint256 addrValue = BASE_PART * SHIFT + index + 1;
        address expectedAddress = address.makeAddrStd(0, addrValue);
        if (msg.sender != _root) {
            require(msg.sender == expectedAddress, ERR_INVALID_SENDER);
        }
        _code[m_PopCoinWallet] = PopCoinWalletCode;
        _popitgamehash = popitgamehash;
        _popits_media = popits_media;
        _root_pubkey = root_pubkey;
        _isPublic = isPublic;
        _description = description;
        _maxPopitIndex = maxPopitIndex;
        _popitGameOwner = popitGameOwner;
        PopitGame(_popitGameOwner).popCoinRootDeployed{value: 0.1 vmshell, flag: 1}(_name);
    }

    function setIsPublic(bool isPublic) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _isPublic = isPublic;
    }

    function setPopitMedia(uint16 index, PopitMedia data) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        _popits_media[index] = data;
    }

    function addNewPopit(
        string media,
        optional(uint32) protopopit
    ) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        newPopit(media, protopopit);
    }

    function addNewPopitPublic(
        string media,
        optional(uint32) protopopit
    ) public accept {
        ensureBalance();
        require(_isReady == true, ERR_NOT_READY);
        require(_isPublic == true, ERR_NOT_PUBLIC);
        newPopit(media, protopopit);
    }

    function newPopit(
        string media,
        optional(uint32) protopopit
    ) private {
        TvmBuilder b;
        b.store(media);
        b.store(protopopit);
        b.store(block.timestamp);
        uint256 id = tvm.hash(b.toCell());
        require(_popits_candidate.exists(id) == false, ERR_ALREADY_EXIST);
        _popits_candidate[id] = PopitCandidateWithMedia(0, media, protopopit, block.timestamp);
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function activate(bool isOld) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        _isReady = true;
        _basicValue = _totalSupply;
        if (!isOld) {
            MobileVerifiersContractRoot(_root).sendTapRewards{value: 0.1 vmshell, flag: 1}(_name);
        }
    }

    function activatePopit(uint256 id, optional(string) media) public view onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        require(_popits_candidate.exists(id) == true, ERR_NOT_READY);
        MobileVerifiersContractRoot(_root).sendTapRewardsPopit{value: 0.1 vmshell, flag: 1}(_name, id, media);
    }

    function deleteCandidate(uint256 id) public onlyOwnerPubkey(_root_pubkey) accept {
        ensureBalance();
        delete _popits_candidate[id];
    }

    function getTapReward(uint256 id, optional(string) media) public senderIs(_root) accept {
        ensureBalance();
        _maxPopitIndex += 1;
        uint128 reward = uint128(msg.currencies[CURRENCIES_ID]);
        if (media.hasValue()) {
            _popits_media[_maxPopitIndex] = PopitMedia(media.get(), id, _popits_candidate[id].protopopit);
        } else {
            _popits_media[_maxPopitIndex] = PopitMedia(_popits_candidate[id].media, id, _popits_candidate[id].protopopit);
        }
        if (reward != 0) {
            _popits_value[_maxPopitIndex] = Popit(reward, _popits_candidate[id].value, _popits_candidate[id].value);
        }
        _totalSupply += _popits_candidate[id].value;
        delete _popits_candidate[id];
    }

    function destroy() public view onlyOwnerPubkey(_root_pubkey) accept {
        require(_isReady == false, ERR_ALREADY_READY);
        MobileVerifiersContractRoot(_root).popCoinRootDestroyed{value: 0.1 vmshell, flag: 161, bounce: false}(_name);
    }

    function mintValue(address owner, uint128 value) public senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], _popitgamehash, _root, _name, owner)) accept {
        ensureBalance();
        _totalSupply += value;
        if (_isReady == true) {
            PopCoinWallet(msg.sender).setReady{value: 0.1 vmshell, flag: 1}();
        }
    }

    function mintValuePopit(address owner, uint256 dataId, uint128 dataValue) public senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], _popitgamehash, _root, _name, owner)) accept {
        ensureBalance();
        require(_popits_candidate.exists(dataId) == true, ERR_NOT_READY);
        _popits_candidate[dataId].value += dataValue;
    }

    function mintValueOld(address owner, uint128 value) public senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], _popitgamehash, _root, _name, owner)) accept {
        ensureBalance();
        _totalSupply += value;
    }
    
    function isReady(address owner, uint128 value, address popitGameAddress) public view senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], _popitgamehash, _root, _name, owner)) accept {
        ensureBalance();
        if (_isReady == false) { return; }
        PopCoinWallet(msg.sender).setReady{value: 0.1 vmshell, flag: 1}();
        if (_basicValue == 0) {
            return;
        }
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_rewards * value / _basicValue);
        popitGameAddress.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
    }

    function isReadyPopit(address owner, uint16 indexRoot, uint256 candidateId, uint128 candidateValue, address popitGameAddress) public senderIs(VerifiersLib.calculatePopCoinWalletAddress(_code[m_PopCoinWallet], _popitgamehash, _root, _name, owner)) accept {
        ensureBalance();
        if (_popits_media[indexRoot].id != candidateId) { return; }
        PopCoinWallet(msg.sender).setReadyPopit{value: 0.1 vmshell, flag: 1}(candidateId);
        if (_popits_value[indexRoot].leftValue == 0) { return; }
        mapping(uint32 => varuint32) data_cur;
        data_cur[CURRENCIES_ID] = varuint32(_popits_value[indexRoot].rewards * candidateValue / _popits_value[indexRoot].value);
        popitGameAddress.transfer({value: 0.1 vmshell, flag: 1, currencies: data_cur});
        _popits_value[indexRoot].leftValue -= candidateValue;
        if (_popits_value[indexRoot].leftValue == 0) {
            delete _popits_value[indexRoot];
        }
    }
    
    //Fallback/Receive
    receive() external {
        ensureBalance();
        tvm.accept();
        if ((msg.sender == _root) && (_rewards == 0)) {
            _rewards += uint128(msg.currencies[CURRENCIES_ID]);
        }
    }

    //Getters
    function getDetails() external view returns(
        address root,
        string name,
        uint128 totalSupply,
        uint16 maxPopitIndex,
        mapping(uint16 => Popit) popits_value,
        mapping(uint16 => PopitMedia) popits_media,
        uint128 rewards,
        mapping(uint256 => PopitCandidateWithMedia) popits_candidate,
        bool isReadyStatus,
        address popitGameOwner,
        string description
    ) {
        return (_root, _name, _totalSupply, _maxPopitIndex, _popits_value, _popits_media, _rewards, _popits_candidate, _isReady, _popitGameOwner, _description);
    }

    function getVersion() external pure returns(string, string) {
        return (version, "PopCoinRoot");
    }
}
