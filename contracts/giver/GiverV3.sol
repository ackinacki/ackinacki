pragma gosh-solidity >= 0.76.1;
pragma AbiHeader expire;

abstract contract Upgradable {
    /*
     * Set code
     */

    function upgrade(TvmCell newcode) public virtual {
        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
        tvm.commit();
        tvm.setcode(newcode);
        tvm.setCurrentCode(newcode);
        onCodeUpgrade();
    }

    function onCodeUpgrade() internal virtual;
}

contract GiverV3 is Upgradable {

    uint8 constant MAX_CLEANUP_MSGS = 30;
    mapping(uint256 => uint32) m_messages;

    modifier acceptOnlyOwner {
        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
        _;
    }

    event SentCurrency(address dst, varuint16 value, mapping(uint32 => varuint32));
    event SentCurrencyWithFlag(address dst, varuint16 value, mapping(uint32 => varuint32), uint8 flag);

    constructor () {}

    /*
     * Publics
     */

    /// @notice Allows to accept simple transfers.
    receive() external {}

    /// @notice Transfers grams to other contracts.
    function sendTransaction(address dest, varuint16 value, bool bounce) public pure {
        tvm.accept();
        dest.transfer(value, bounce, 3);
    }

    function sendCurrency(address dest, varuint16 value, mapping(uint32 => varuint32) ecc) public pure {
        tvm.accept();
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: 1, currencies: ecc});
        emit SentCurrency(dest, value, ecc);
    }

    function sendWithBody(address dest, varuint16 value, bool bounce, uint8 flag, TvmCell body) public acceptOnlyOwner {
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: bounce, flag: flag, body: body});
    }

    function _mintEccIfNeeded(mapping(uint32 => varuint32) ecc) private pure {
        for (uint32 id = 1; id <= 3; id++) {
            if (ecc.exists(id)) {
                if (address(this).currencies[id] < ecc[id]) {
                    gosh.mintecc(uint64(ecc[id]) - uint64(address(this).currencies[id]), id);
                }
            }
        }
    }

    function sendCurrencyWithFlag(address dest, varuint16 value, mapping(uint32 => varuint32) ecc, uint8 flag) public pure {
        tvm.accept();
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: flag, currencies: ecc});
        emit SentCurrencyWithFlag(dest, value, ecc, flag);
    }

    function sendCurrencyWithBody(address dest, varuint16 value, mapping(uint32 => varuint32) ecc, uint8 flag, TvmCell body) public pure {
        tvm.accept();
        _mintEccIfNeeded(ecc);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: flag, currencies: ecc, body: body});
    }

    function sendFreeToken(address dest) public pure {
        tvm.accept();
        varuint32 drop = 50 vmshell;
        uint64 value = 10 vmshell;
        mapping(uint32 => varuint32) data_cur;
        data_cur[2] = drop;
        gosh.mintecc(uint64(drop), 2);
        if (address(this).balance <= value + 1000 vmshell) {
            gosh.mintshellq(uint64(value + 1000 vmshell - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: 1, currencies: data_cur});
    }

    /*
     * Get methods
     */
    struct Message {
        uint256 hash;
        uint32 expireAt;
    }
    function getMessages() public view returns (Message[] messages) {
        for ((uint256 msgHash, uint32 expireAt) : m_messages) {
            messages.push(Message(msgHash, expireAt));
        }
    }
    
    function getData(string name, uint128 decimals, TvmCell walletCode, TvmCell transactionCode, uint256 pubkey, bool mintDisabled, address initialSupplyToOwner, uint128 initialSupply) public view returns (TvmCell) {
        return abi.encode(name, decimals, walletCode, transactionCode, pubkey, mintDisabled, initialSupplyToOwner, initialSupply);
    }

    function getAccumulatorData(TvmCell sellOrderCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(sellOrderCode, pubkey);
    }

    function getExchangeData(uint256 pubkey, address usdcWallet, uint128 totalMinted, uint64 mintNonce, uint64 mintAccumulatorNonce) public view returns (TvmCell) {
        return abi.encode(pubkey, usdcWallet, totalMinted, mintNonce, mintAccumulatorNonce);
    }

    function getDataForAuthService(TvmCell profileCode, uint256 pubkey) public view returns (TvmCell) {
        return abi.encode(profileCode, pubkey);
    }

    function onCodeUpgrade() internal override {}
}
