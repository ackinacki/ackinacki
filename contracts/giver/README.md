**GiverV3** – A system contract used on the [test network](https://shellnet.ackinacki.org) as a faucet.

- **Code Hash**: `56df20d56a1352e6076ee0010e71f166c68169aa1821bcad056c9e14fcb79f4c` (compiled with `sold` v0.76.0)

### Building GiverV3 with `sold`

`sold` is an all-in-one compiler and linker for the TVM Solidity language, packaged into a single binary.

To build `sold` manually, follow [this guide](https://github.com/gosh-sh/TVM-Solidity-Compiler?tab=readme-ov-file#build-and-install), or download prebuilt binaries [here](https://github.com/gosh-sh/TVM-Solidity-Compiler/releases).

To compile the GiverV3 contract:

```bash
sold GiverV3.sol
```

