> **⚠️ RECOMMENDED VERSION ⚠️**
>
> **This is the latest, recommended version of the multisig wallet, providing the complete set of lifecycle events. Use this version for all new deployments.**
>
> **The previous version ([`../updatecustodianmultisigwallet`](../updatecustodianmultisigwallet)) is deprecated and should not be used.**

**UpdateCustodianMultisigWallet_v2** (v2.4.0) – A multisignature wallet with support for upgrade and SHELL exchange features.

This version additionally emits lifecycle events (`TransactionSent`, `TransactionSubmitted`,
`TransactionConfirmed`, `WalletSetup`, `CustodiansUpdated`, `FundsReceived`, `UnknownCall`,
`RequestsDropped`) to hardcoded external destinations. A deployment reports its initial
custodian set through `WalletSetup` only; `CustodiansUpdated` marks an actual change.
Code upgrades require multisig confirmation via `submitUpdateCode` / `confirmUpdateCode`
(reqConfirmsData custodian confirmations).

A queued upgrade is observable before it is confirmed: `CodeUpdateSubmitted`,
`CodeUpdateConfirmed` and `CodeUpdateApplied` identify the pending code by hash, and
`getUpdateCode` returns a queued entry including the code cell itself. Each of the four
queues can be read the same three ways — by id, as a list and as a list of ids; the
code-update list identifies each pending code by hash instead of carrying the cells, and
the listing forms return only requests that have not expired.

The wallet manages its own gas: when its vmshell balance drops below `minBalance`,
`ensureBalance` (run at the start of every operation, and once from the constructor)
converts SHELL up to `targetBalance`. Auto top-up is disabled while `minBalance` is 0.
The initial config is set at construction (the `minBalance` / `targetBalance` constructor
arguments) and changed later under multisig via `submitConfigUpdate` / `confirmConfigUpdate`
(reqConfirmsData confirmations), observable through `ConfigUpdateSubmitted` /
`ConfigUpdateConfirmed` / `ConfigUpdateApplied` and readable via `getBalanceConfig`;
it survives custodian changes.

Changing the custodian set clears the transfer, data-update, code-update and config-update
queues: pending requests are discarded and have to be submitted again under the new set.
What was discarded is reported by `RequestsDropped`, which counts the requests per queue; the
data update being applied is executed rather than discarded and is not counted. The balance
config itself is an operator setting and is NOT reset by a custodian change.

Every send is reported under its own `transactionId`, whether or not it was queued, and a
send-all transfer reports the balance being swept rather than the unused value argument.
A message carrying a body the wallet does not implement reaches `fallback` and is reported
as `UnknownCall` with its sender and value; `ExecutionFailure` carries the leading function
id of a bounced body. The cleanup budget set through `setMaxCleanupOperations` takes any
positive value and keeps it across custodian changes.

- **Code Hash (sha256)**: `cfcaac10d43c8dc062298cb48df097be67cddec52b9cfd558309a7549f01c1f1` (compiled with `sold` 0.81.0 (commit.c5780830))

### Building `UpdateCustodianMultisigWallet_v2` with `sold`

`sold` is an all-in-one compiler and linker for the TVM Solidity language, available as a single binary.

To manually build `sold`, follow [this guide](https://github.com/gosh-sh/TVM-Solidity-Compiler?tab=readme-ov-file#build-and-install), or download the binaries directly from [here](https://github.com/gosh-sh/TVM-Solidity-Compiler/releases).

To compile the `UpdateCustodianMultisigWallet_v2` contract:

```bash
sold --tvm-version gosh  UpdateCustodianMultisigWallet_v2.sol
```

