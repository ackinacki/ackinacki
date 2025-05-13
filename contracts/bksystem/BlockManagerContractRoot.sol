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
import "./libraries/BlockKeeperLib.sol";
import "./AckiNackiBlockManagerNodeWallet.sol";
import "./BlockKeeperContractRoot.sol";

contract BlockManagerContractRoot is Modifiers {
    string constant version = "1.0.0";

    mapping(uint8 => TvmCell) _code;
    address _licenseBMRoot;
    address _BKRoot;
    uint128 _numberOfActiveBlockManagers = 0;
    optional(address) _owner_wallet;
    optional(uint32) _bm_start;
    uint32 _bm_end = 0;
    address _giver;

    constructor (
        address giver,
        address licenseBMRoot,
        address bkroot
    ) {
        _giver = giver;
        _licenseBMRoot = licenseBMRoot;
        _BKRoot = bkroot;
    }

    function setNewCode(uint8 id, TvmCell code) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg { 
        ensureBalance();
        _code[id] = code;
    }

    function ensureBalance() private pure {
        if (address(this).balance > ROOT_BALANCE) { return; }
        gosh.mintshell(ROOT_BALANCE);
    }

    function increaseBM(uint256 pubkey) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        _numberOfActiveBlockManagers += 1;
    }

    function deployAckiNackiBlockManagerNodeWallet(uint256 pubkey, mapping(uint256 => bool) whiteListLicense) public view accept {
        ensureBalance();
        TvmCell data = BlockKeeperLib.composeBlockManagerWalletStateInit(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey);
        new AckiNackiBlockManagerNodeWallet {stateInit: data, value: varuint16(FEE_DEPLOY_BLOCK_MANAGER_WALLET), wid: 0, flag: 1}(_code[m_LicenseBMCode], whiteListLicense, _licenseBMRoot);
    }

    function getReward(uint256 pubkey, uint32 startBM, uint32 stopBM) public senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        ensureBalance();
        uint32 time = stopBM - startBM;
        time;
        ///// UPDATE REWARD CODE
        uint128 reward = 5;

        BlockKeeperContractRoot(_BKRoot).sendReward{value: 0.1 ton, flag: 1}(msg.sender, reward);
        _numberOfActiveBlockManagers -= 1;
    }

    function tryUpdateCode(uint256 pubkey, uint256 old_hash) public view senderIs(BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode], address(this), pubkey)) accept {
        old_hash;
        return;
    }

    function updateCode(TvmCell newcode, TvmCell cell) public onlyOwnerWallet(_owner_wallet, tvm.pubkey()) accept saveMsg {
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
    function getAckiNackiBlockManagerNodeWalletAddress(uint256 pubkey) external view returns(address wallet){
        return BlockKeeperLib.calculateBlockManagerWalletAddress(_code[m_AckiNackiBlockManagerNodeWalletCode] ,address(this), pubkey);
    }

    function getCodes() external view returns(mapping(uint8 => TvmCell) code) {
        return _code;
    }

    function getVersion() external pure returns(string, string) {
        return (version, "BlockManagerContractRoot");
    }   
}
