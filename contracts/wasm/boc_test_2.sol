pragma ton-solidity >=0.39.0;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

contract LargestCellTest {
    struct Bits {
        uint256 a1;
        uint256 a2;
        uint256 a3;
        uint128 a4;
        uint64 a5;
        uint32 a6;
        uint16 a7;
        uint8 a8;
        bool a9;
        bool a10;
        bool a11;
        bool a12;
    }
    TvmCell public data;
    uint16 c;
    constructor(uint64 value) public {
        // Call the VM command to convert SHELL tokens to VMSHELL tokens to pay the transaction fee.
        gosh.cnvrtshellq(value);
        // Ensure that the contract's public key is set.
        require(tvm.pubkey() != 0, 101);
        c = 10; 
        tvm.accept();
        LargestCellTest(this).add{value: 0, flag: 128, bounce: false}();
    }

    function runAdd() public {
        

        for (uint i = 0; i < 2; i++) {
            LargestCellTest(this).add();
            tvm.accept();
        }
    }

    function add() public {
        TvmCell tmp = data;

        for (uint i = 0; i < 1; i++) {
            TvmCell d = tmp;
            TvmBuilder new0;
            new0.store(Bits(1 + c + i, 2 + c + i, 3 + c + i, 4 + uint128(i + c), 1 + uint64(c + i), 2 + uint32(c + i), 3 + uint16(c + i), 0, false, false, false, false));
            TvmBuilder new1;
            new1.store(Bits(2 + c + i, 3 + c + i, 4 + c + i, 5 + uint128(c + i), 1 + uint64(c + i), 2 + uint32(c + i), 3 + uint16(c + i), 0, false, false, false, false));
            TvmBuilder new2;
            new2.store(Bits(3 + c + i, 4 + c + i, 5 + c + i, 6 + uint128(c + i), 1 + uint64(c + i), 2 + uint32(c + i), 3 + uint16(c + i), 0, false, false, false, false));
            TvmBuilder new3;
            new3.store(Bits(4 + c + i, 5 + c + i, 6 + c + i, 7 + uint128(c + i), 1 + uint64(c + i), 2 + uint32(c + i), 3 + uint16(c + i), 0, false, false, false, false));
            new0.store(d);
            new0.storeRef(new1);
            new0.storeRef(new2);
            new0.storeRef(new3);
            tmp = new0.toCell();
            data = tmp;
        }
        //data = tmp;
        c++;
        tvm.accept();
        LargestCellTest(this).add{value: 0, flag: 128, bounce: false}();
    }

    function get() public view responsible returns (TvmCell) {
        return{value: 0, flag: 64, bounce: false} data;
    }
}


