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
import "./Game.sol";
import "./MobileVerifiersContractRoot.sol";

contract Game is Modifiers {
    string constant version = "1.0.0";
    mapping(uint8 => TvmCell) _code;

    address _root;
    address static _owner;
    address static _popCoinWallet;
    optional(bytes) permanent_popits;
    optional(bytes) deck;
    optional(bytes) deck_mod;
    uint256 rnd_seed;
    int64 game_balance;
    uint64 _height = 1;
    optional(uint8) hand_size;
    address static _popcoinroot_address;
    optional(address) _attack_sender;
    optional(bytes) _attack_popits;
    optional(bytes) _attack_rewards;
    TvmCell _gameCode;
    uint32 _timeStart;
    uint64 _height_start = 1;
    uint64 _height_max_prev_prev = 1;
    uint64 _height_max_prev = 1;
    uint64 _height_max = 1;
    uint64 _diff_height = 0;
    bool _rewarded = true;

    uint64 _mbiCur;
    uint64 _mbiPrev;
    uint64 _mbiPrevPrev;
    string _name;

    constructor (
        TvmCell gameCode,
        uint64 mbiCur,
        string name
    ) {
        TvmCell data = abi.codeSalt(tvm.code()).get();
        (string lib, address root) = abi.decode(data, (string, address));
        require(VerifiersLib.versionLib == lib, ERR_INVALID_SENDER);
        _root = root;
        require(msg.sender == _popCoinWallet, ERR_INVALID_SENDER);
        rnd_seed = rnd.next();
        _gameCode = gameCode;
        _timeStart = block.timestamp;
        _mbiCur = mbiCur;
        _mbiPrev = mbiCur;
        _mbiPrevPrev = mbiCur;
        _name = name;
    }

    function ensureBalance() private pure {
        if (address(this).balance > CONTRACT_BALANCE) { return; }
        gosh.mintshellq(CONTRACT_BALANCE);
    }

    function getReward() public view senderIs(_owner) accept {
        if (!_rewarded) {
            MobileVerifiersContractRoot(_root).sendRewards{value: 0.1 vmshell, flag: 1}(_owner, _popCoinWallet, _popcoinroot_address, _mbiPrev, _mbiPrevPrev, _height_max_prev, _height_max_prev_prev);
        }
    }

    function updateMbiCur(uint64 mbiCur) public senderIs(_popCoinWallet) {
        _mbiCur = mbiCur;
    }

    function destroy() private {
        PopCoinWallet(_popCoinWallet).gameDestroyed{value: 0.1 vmshell, flag: 1}();
        selfdestruct(_popCoinWallet);
    }

    function setAttack(address owner, bytes attack_popits, address popcoinwallet) public senderIs(VerifiersLib.calculateGameAddress(_gameCode, _root, owner, _popcoinroot_address, popcoinwallet)) accept {
        ensureBalance();
        if (_attack_sender.hasValue()) { return; }
        _attack_sender = msg.sender;
        _attack_popits = attack_popits;
    }

    function play(
        bytes hand,
        bytes layout,
        bytes owned_popits,
        bytes shop_history,
        bytes discards,
        uint64 rerolls,
        optional(address) attack_recipient_owner,
        optional(address) attack_recipient_popcoin_wallet,
        optional(bytes) attack_popits) public senderIs(_owner) accept {
        rerolls;
        discards;
        shop_history;
        owned_popits;
        layout;
        hand;
        ensureBalance();
        if (block.timestamp > RewardPeriod + _timeStart) {
            destroy();
            return;
        }
        if ((block.timestamp > _timeStart) && (_height_start == 0)) {
            _height_start = _height;
            _rewarded = false;
            _height_start = _height;
            _height_max_prev_prev = _height_max_prev;
            _height_max_prev = _height_max;
            _height_max = 1;
            _mbiPrevPrev = _mbiPrev;
            _mbiPrev = _mbiCur;
        }
        if ((attack_recipient_owner.hasValue()) && (attack_recipient_popcoin_wallet.hasValue())) {
            Game(VerifiersLib.calculateGameAddress(_gameCode, _root, attack_recipient_owner.get(), _popcoinroot_address, attack_recipient_popcoin_wallet.get())).setAttack{value: 0.1 vmshell, flag: 1}(_owner, attack_popits.get(), _popCoinWallet);
        }
        uint64 height = _height;
        //WASM
        if (_height > _height_max) {
            _height_max = _height;
        }
        if ((_height > height) && (_height_start != 0)) {
            _diff_height += 1;
        }
        if (_diff_height >= 5) {
            if (block.timestamp > _timeStart) {
                _timeStart += RewardPeriod;
                _height_start = 0;
                _diff_height = 0;
                MobileVerifiersContractRoot(_root).addGameNumber{value: 0.1 vmshell, flag: 1}(_owner, _popcoinroot_address, _popCoinWallet);
            }
        }
    }
    
    //Fallback/Receive
    receive() external {
    }

    //Getters
    function getVersion() external pure returns(string, string) {
        return (version, "Game");
    }
}
