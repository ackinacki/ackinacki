pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

interface IHelloWorld {
    function touch() external;
}


// This is class that describes you smart contract.
contract helloWorld {
    // Contract can have an instance variables.
    // In this example instance variable `timestamp` is used to store the time of `constructor` or `touch`
    // function call
    uint32 public timestamp;

    // The contract can have a `constructor` – a function that is called when the contract is deployed to the blockchain.
    // Parameter `value` represents the number of SHELL tokens to be converted to VMSHELL to pay the transaction fee.
    // In this example, the constructor stores the current timestamp in an instance variable.
    // All contracts need to call `tvm.accept()` for a successful deployment.
    constructor(uint64 value) {
        // Call the VM command to convert SHELL tokens to VMSHELL tokens to pay the transaction fee.
        gosh.cnvrtshellq(value);

        // Ensure that the contract's public key is set.
        require(tvm.pubkey() != 0, 101);

        // The current smart contract agrees to buy some gas to complete the
        // current transaction. This action is required to process external
        // messages, which carry no value (and therefore no gas).
        tvm.accept();

        // Set the instance variable to the current block timestamp.
        timestamp = block.timestamp;
    }

    // Converts SHELL to VMSHELL for payment of transaction fees
    // Parameter `value`- the amount of SHELL tokens that will be exchanged 1-to-1 into VMSHELL tokens.
    function exchangeToken(uint64 value) public pure {
        tvm.accept();
        getTokens();
        gosh.cnvrtshellq(value);
    }

    // Returns a static message, "helloWorld".
    // This function serves as a basic example of returning a fixed string in Solidity.
    function renderHelloWorld () public pure returns (string) {
        return 'helloWorld';
    }

    // Returns a static message, "helloWorld".
    // This function serves as a basic example of returning a fixed string in Solidity.
    bytes public wasmResSaved;
    function runWasm (bytes wasmBinary, string wasmModule, string wasmFunction, bytes wasmArgs, bytes wasmHash) public returns (bytes) {
        
        getTokens();
        // uint8[] wasmBinaryArr = Convert.FromBase64String(wasmBinary);
        // uint8[] wasmArgsArr = Convert.FromBase64String(wasmArgs);
        // TODO: somehow pack everythign into cells
        //                                              [1,2]
        TvmCell wasmResultCell = gosh.runwasm(abi.encode(wasmHash), abi.encode(wasmArgs), abi.encode(wasmFunction), abi.encode(wasmModule), abi.encode(wasmBinary));
        tvm.commit();
        tvm.log("result acquired");
        // TvmCell eoncoded = abi.encode(wasmArgs);
        // bytes decoded = abi.decode(eoncoded, (string));
        bytes wasmResult = abi.decode(wasmResultCell, bytes);
        // uint8[3]
        //              cell      string        string      cell    
        wasmResSaved = wasmResult;
        tvm.accept(); 
        return wasmResult;
        
    }
    function runWasmConcatMultiarg (bytes wasmBinary, string wasmModule, string wasmFunction, bytes wasmArgs, bytes wasmArgs2, bytes wasmArgs3, bytes wasmArgs4, bytes wasmHash) public returns (bytes) {
        
        getTokens();
        // uint8[] wasmBinaryArr = Convert.FromBase64String(wasmBinary);
        // uint8[] wasmArgsArr = Convert.FromBase64String(wasmArgs);
        // TODO: somehow pack everythign into cells
        //                                              [1,2]
        TvmCell wasmResultCell = gosh.runwasmconcatmultiarg(abi.encode(wasmHash), abi.encode(wasmArgs4), abi.encode(wasmArgs3), abi.encode(wasmArgs2), abi.encode(wasmArgs), abi.encode(wasmFunction), abi.encode(wasmModule), abi.encode(wasmBinary));
        tvm.commit();
        tvm.log("result acquired");
        // TvmCell eoncoded = abi.encode(wasmArgs);
        // bytes decoded = abi.decode(eoncoded, (string));
        bytes wasmResult = abi.decode(wasmResultCell, bytes);
        // uint8[3]
        //              cell      string        string      cell    
        wasmResSaved = wasmResult;
        tvm.accept(); 
        return wasmResult;
        
    }

    // function rejoinChainOfCells (TvmCell input) public pure returns (uint8[]) {
    //     // TODO
    //     //uint8[] dataVec = input;
    // }

    // function splitToChainOfCells (uint8[] byteArr) public pure returns (TvmCell) {
    //     // TODO
    // }

    // pub(super) fn rejoin_chain_of_cells(input: &Cell) -> Result<Vec<u8>, failure::Error> {
    //     let mut data_vec = input.data().to_vec();
    //     let mut cur_cell: Cell = input.clone();
    //     while cur_cell.reference(0).is_ok() {
    //         let old_len = data_vec.len();
    //         cur_cell = cur_cell.reference(0)?;
    //         data_vec.append(&mut cur_cell.data().to_vec());

    //         assert!(data_vec.len() - old_len == cur_cell.data().len());
    //     }
    //     Ok(data_vec)
    // }

    // Updates the `timestamp` variable with the current blockchain time.
    // We will use this function to modify the data in the contract.
    // Сalled by an external message.
    function touch() external {
        // Informs the TVM that we accept this message.
        tvm.accept();
        getTokens();
        // Update the timestamp variable with the current block timestamp.
        timestamp = block.timestamp;
    }

    // Used to call the touch method of a contract via an internal message.
    // Parameter 'addr' - the address of the contract where the 'touch' will be invoked.
    function callExtTouch(address addr) public view {
        // Each function that accepts an external message must check that
        // the message is correctly signed.
        require(msg.pubkey() == tvm.pubkey(), 102);
        tvm.accept();
        getTokens();
        IHelloWorld(addr).touch();
    }

    // Sends VMSHELL to another contract with the same Dapp ID.
    // Parameter `dest` - the target address within the same Dapp ID to receive the transfer.
    // Parameter `value`- the amount of VMSHELL tokens to transfer.
    // Parameter `bounce` - Bounce flag. Set true if need to transfer funds to existing account;
    // set false to create new account.
    function sendVMShell(address dest, uint128 amount, bool bounce) public view {
        require(msg.pubkey() == tvm.pubkey(), 102);
        tvm.accept();
        getTokens();
        // Enables a transfer with arbitrary settings
        dest.transfer(varuint16(amount), bounce, 0);
    }

    // Allows transferring SHELL tokens within the same Dapp ID and to other Dapp IDs.
    // Parameter `dest` - the target address to receive the transfer.
    // Parameter `value`- the amount of SHELL tokens to transfer.
    function sendShell(address dest, uint128 value) public view {
        require(msg.pubkey() == tvm.pubkey(), 102);
        tvm.accept();
        getTokens();

        TvmCell payload;
        mapping(uint32 => varuint32) cc;
        cc[2] = varuint32(value);
        // Executes transfer to target address
        dest.transfer(0, true, 1, payload, cc);
    }

    // Deploys a new contract within its Dapp.
    // The address of the new contract is calculated as a hash of its initial state.
    // The owner's public key is part of the initial state.
    // Parameter `stateInit` - the contract code plus data.
    // Parameter `initialBalance` - the amount of funds to transfer. 
    // Parameter `payload` - a tree of cells used as the body of the outbound internal message.
    function deployNewContract(
        TvmCell stateInit,
        uint128 initialBalance,
        TvmCell payload
    )
        public pure
    {
        // Runtime function to deploy contract with prepared msg body for constructor call.
        tvm.accept();
        getTokens();
        address addr = address.makeAddrStd(0, tvm.hash(stateInit));
        addr.transfer({stateInit: stateInit, body: payload, value: varuint16(initialBalance)});
    }
    
    // Checks the contract balance
    // and if it is below the specified limit, mints VMSHELL.
    // The amounts are specified in nanotokens.
    // Used to enable automatic balance replenishment.
    function getTokens() private pure {
        if (address(this).balance > 100000000000) {     // 100 VMSHELL
            return; 
        }
        gosh.mintshellq(100000000000);                   // 100 VMSHELL
    }

    function create_cell() private pure returns (TvmCell) {
        return create_deep_cell(700);
    }

    function create_big_cell2(uint256 iterations) public pure returns (TvmCell) {
        tvm.accept();
        TvmBuilder b;
        uint256 n = 100;
        b.store(n);
        TvmCell c = b.toCell();

        for (uint256 i = 0; i < iterations; i++) {
            TvmBuilder b2;
            b2.storeRef(c);
            /* if (i % 2 == 0) b2.storeRef(c); */
            b2.storeRef(c);
            /* 
            b2.storeRef(c);
            b2.storeRef(c); */
            c = b2.toCell();
        }
        return c;
    }

    function create_big_cell(uint256 iterations) public pure returns (TvmCell) {
        tvm.accept();
        TvmBuilder b;
        uint256 n = 100;
        b.store(n);
        TvmCell c = b.toCell();

        for (uint256 i = 0; i < iterations; i++) {
            TvmBuilder b2;
            b2.storeRef(c);
            b2.storeRef(c);
            b2.storeRef(c);
            b2.storeRef(c);
            c = b2.toCell();
        }
        return c;
    }

    function create_deep_cell(uint256 iterations) public pure returns (TvmCell) {
        tvm.accept();
        TvmBuilder b1;
        uint256 n = 100;
        b1.store(n);
        TvmCell c = b1.toCell();
        for (uint256 i = 0; i < iterations; i++) {
            TvmBuilder b2;
            b2.storeRef(c);
            c = b2.toCell();
        }
        return c;
    }
}

