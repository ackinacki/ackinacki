// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

pragma ever-solidity >= 0.61.2;
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

function mint(uint64 stake, uint32 key) assembly pure {
    ".blob xC726"
}

function mintshell(uint64 value) assembly pure {
    ".blob xC728"
}

contract GiverV3 is Upgradable {

    uint8 constant MAX_CLEANUP_MSGS = 30;
    mapping(uint256 => uint32) m_messages;

    modifier acceptOnlyOwner {
        require(msg.pubkey() == tvm.pubkey(), 101);
        tvm.accept();
        _;
    }

    constructor () {}

    /*
     * Publics
     */

    /// @notice Allows to accept simple transfers.
    receive() external {}

    /// @notice Transfers grams to other contracts.
    function sendTransaction(address dest, varuint16 value, bool bounce) public {
        dest.transfer(value, bounce, 3);
        gc();
    }

    function sendCurrency(address dest, varuint16 value, mapping(uint32 => varuint32) ecc) public {
        if (ecc.exists(1)) {
            if (address(this).currencies[1] < ecc[1]) {
                mint(uint64(ecc[1]) - uint64(address(this).currencies[1]), 1);
            }
        }
        if (ecc.exists(2)) {
            if (address(this).currencies[2] < ecc[2]) {
                mint(uint64(ecc[2]) - uint64(address(this).currencies[2]), 2);
            }
        }
        if (address(this).balance <= value + 1000 ton) {
            mintshell(uint64(value + 1000 ton - address(this).balance));
        }
        dest.transfer({value: value, bounce: false, flag: 3, currencies: ecc});
        gc();
    }

    /*
     * Privates
     */

    /// @notice Function with predefined name called after signature check. Used to
    /// implement custom replay protection with parallel access.
    function afterSignatureCheck(TvmSlice body, TvmCell) private inline
    returns (TvmSlice)
    {
        // owner check
        require(msg.pubkey() == tvm.pubkey(), 101);
        uint256 bodyHash = tvm.hash(body);
        // load and drop message timestamp (uint64)
        (, uint32 expireAt) = body.load(uint64, uint32);
        require(expireAt > block.timestamp, 57);
        require(!m_messages.exists(bodyHash), 102);

        tvm.accept();
        m_messages[bodyHash] = expireAt;

        return body;
    }

    /// @notice Allows to delete expired messages from dict.
    function gc() private inline {
        uint counter = 0;
        for ((uint256 bodyHash, uint32 expireAt) : m_messages) {
            if (counter >= MAX_CLEANUP_MSGS) {
                break;
            }
            counter++;
            if (expireAt <= block.timestamp) {
                delete m_messages[bodyHash];
            }
        }
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

    function onCodeUpgrade() internal override {}
}
