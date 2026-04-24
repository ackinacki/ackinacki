# AuthService

On-chain authentication service for Acki Nacki.

## Architecture

```
AuthServiceRoot
    │
    │  deployProfile(pubkeyHash, multifactorHash, description)
    ▼
AuthProfile  (address = hash(stateInit(root, descriptionHash)))
    │
    │  onProfileDeployed(descriptionHash, multifactorHash, description)  ← callback
    ▼
AuthServiceRoot  →  emit AuthProfileDeployed{dest: makeAddrExtern(multifactorHash, 256)}(profile, multifactorHash, description)
```

### AuthServiceRoot

- Deployed in zerostate via `UpdateZeroContract` at `0:0404...0404`, or manually at any address.
- Stores `AuthProfile` contract code (set at deploy time via `setProfileCode` or `onCodeUpgrade`).
- `deployProfile` accepts **pubkey hash**, **multifactor hash**, and a **description**.
- Profile address is deterministic: derived from `(root, hash(description))`.
- On callback from the freshly deployed profile, emits `AuthProfileDeployed` to `address.makeAddrExtern(multifactorHash, 256)`.
- Upgradeable: `updateCode(newcode, cell)` swaps code and calls `onCodeUpgrade`.

### AuthProfile

- Address is **deterministic**: derived from `stateInit` containing `_root` and `_descriptionHash = hash(description)`.
- `_root` is a `static` field (part of varInit) — the root can be deployed at any address.
- Constructor is callable only from `_root` (the root deploys it).
- `addContext(cell)` emits `ContextAdded` event to the profile's own external address — owner-only.
- `receive()` — destroys the profile if `tvm.hash(msg.sender) == _multifactorHash` (internal message from the multifactor address).

## Methods

### AuthServiceRoot

| Method | Access | Description |
|--------|--------|-------------|
| `deployProfile(pubkeyHash, multifactorHash, description)` | external (open) | Deploys a new AuthProfile. Address is derived from `description`. |
| `onProfileDeployed(descriptionHash, multifactorHash, description)` | internal callback | Called by AuthProfile on deploy; emits `AuthProfileDeployed`. |
| `getProfileAddress(description)` | view | Returns the deterministic address for the given description. |
| `hashPubkey(pubkey)` | pure | Computes `tvm.hash` of a pubkey — the hash expected by `deployProfile`. |
| `hashMultifactor(multifactor)` | pure | Computes `tvm.hash` of a multifactor address — the hash expected by `deployProfile`. |
| `setProfileCode(code)` | owner-only | Updates stored AuthProfile code. |
| `updateCode(newcode, cell)` | owner-only | Upgrades AuthServiceRoot code. |
| `getVersion()` | pure | Returns `("1.0.0", "AuthServiceRoot")`. |

### AuthProfile

| Method | Access | Description |
|--------|--------|-------------|
| `addContext(context)` | owner key only | Emits `ContextAdded(context)` to `makeAddrExtern(address(this), 256)`. |
| `receive()` | internal | If `tvm.hash(msg.sender) == _multifactorHash`, destroys the profile via `selfdestruct`. |
| `getDetails()` | view | Returns `description`, `descriptionHash`, `pubkeyHash`, `multifactorHash`, `root`. |
| `getVersion()` | pure | Returns `("1.0.0", "AuthProfile")`. |

## Events

```solidity
// AuthServiceRoot — emitted to address.makeAddrExtern(multifactorHash, 256)
event AuthProfileDeployed(address profile, uint256 multifactorHash, string description);

// AuthProfile — emitted to address.makeAddrExtern(address(this).value, 256)
event ContextAdded(TvmCell context);
```

## Build

```bash
cd contracts/authservice && make
cp contracts/authservice/*.tvc contracts/authservice/*.abi.json \
   contracts/0.79.3_compiled/authservice/
```

## Zerostate integration

AuthServiceRoot is automatically deployed in `generate_zerostate.py`:

1. A premine stub (UpdateZeroContract) is placed at `AUTH_SERVICE_ROOT_ADDRESS` by `deploy_premine_stubs()`.
2. In `deploy_updated_premine_stubs()`:
   - `Giver.getDataForAuthService(profileCode, pubkey)` encodes the init cell.
   - `updateCode` is called to swap in AuthServiceRoot code; `onCodeUpgrade` resets storage and decodes `(_profileCode, _ownerPubkey)` from the cell.
3. The result is added to the zerostate via `zerostate-helper`.

Owner key: `./config/AuthServiceRoot.keys.json` (generated alongside other node keys).

## Manual deployment (shellnet / any live network)

```bash
NETWORK=http://shellnet.ackinacki.org:8600
GIVER=0:1111111111111111111111111111111111111111111111111111111111111111
GIVER_ABI=./contracts/giver/GiverV3.abi.json
GIVER_KEYS=./tests/GiverV3.keys.json
ROOT_ABI=./contracts/0.79.3_compiled/authservice/AuthServiceRoot.abi.json
ROOT_TVC=./contracts/0.79.3_compiled/authservice/AuthServiceRoot.tvc
PROFILE_TVC=./contracts/0.79.3_compiled/authservice/AuthProfile.tvc

# 1. Generate keys
tvm-cli -j getkeypair -o root.keys.json

# 2. Compute deploy address and bake key into TVC copy
cp $ROOT_TVC /tmp/AuthServiceRoot_deploy.tvc
tvm-cli -j genaddr /tmp/AuthServiceRoot_deploy.tvc --abi $ROOT_ABI \
  --setkey root.keys.json --save
# → prints raw_address, save it as ROOT_ADDR

# 3. Fund the address (native + ECC[2]) from the Giver.
#    Before deploying any profile, the root must have enough balance
#    to cover ensureBalance() (100 vmshell) + 5 vmshell per deployProfile call.
tvm-cli -j callx --abi $GIVER_ABI --addr $GIVER --keys $GIVER_KEYS \
  -m sendCurrencyWithFlag \
  '{"dest":"<ROOT_ADDR>","value":"10000000000000","ecc":{"2":"10000000000000"},"flag":"16"}'

# 4. Deploy constructor
tvm-cli -j deploy /tmp/AuthServiceRoot_deploy.tvc '{}' \
  --abi $ROOT_ABI --sign root.keys.json

# 5. Set AuthProfile code
PROFILE_CODE=$(tvm-cli -j decode stateinit --tvc $PROFILE_TVC | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['code'])")
tvm-cli -j callx --abi $ROOT_ABI --addr <ROOT_ADDR> --keys root.keys.json \
  -m setProfileCode "{\"code\":\"$PROFILE_CODE\"}"
```

> **Note:** The Giver requires its own keys (`tests/GiverV3.keys.json`). `flag: 16` ensures the transfer goes through even if the destination is not yet initialised.

### Updating code on a live root

```bash
# Encode init cell via Giver helper (locally with tvm-debugger)
PROFILE_CODE=$(tvm-cli -j decode stateinit --tvc $PROFILE_TVC | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['code'])")
ROOT_CODE=$(tvm-cli -j decode stateinit --tvc $ROOT_TVC | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['code'])")

# Call updateCode on the live root
tvm-cli -j callx --addr <ROOT_ADDR> --abi $ROOT_ABI --keys root.keys.json \
  -m updateCode '{"newcode":"<ROOT_CODE>","cell":"<CELL>"}'
```

`cell` is produced by `Giver.getDataForAuthService(profileCode, pubkey)` = `abi.encode(profileCode, ownerPubkey)`.

## Shellnet deployment (current)

- **AuthServiceRoot**: `0:d6054a384e148b7dac122acf24ec7f218b44826a8a68bb085f2ba371b59ff6a8`
- Owner keys: `tests/authservice/shellnet_root3.keys.json`

## Local tests

```bash
# 1. Regenerate zerostate (if contracts changed)
DISABLE_MV=true make generate_zerostate

# 2. Start network
make run

# 3. Run tests
NETWORK=localhost python3 tests/authservice/test.py

# 4. Stop network
make kill
```

Test phases:
1. **Verify** — AuthServiceRoot is active at the fixed address.
2. **Deploy** — Compute `hashPubkey` + `hashMultifactor`, get `getProfileAddress(description)`, call `deployProfile(pubkeyHash, multifactorHash, description)`.
3. **Verify details** — Check description, descriptionHash, pubkeyHash, multifactorHash, root.
4. **addContext** — Stranger call rejected (exit_code 101); owner call succeeds.
5. **Multifactor destroy** — Send internal message from multifactor address to profile; profile is destroyed via `receive()`.
6. **Non-multifactor message** — Send from a different address; profile must remain active.

## Address derivation

```python
descriptionHash = tvm.hash(encode(description))   # TvmBuilder b; b.store(description); hash(b.toCell())
stateInit = abi.encodeStateInit(
    contr=AuthProfile,
    varInit={_root: address(this), _descriptionHash: descriptionHash},
    code=profileCode
)
address = address(tvm.hash(stateInit))
```

Both `getProfileAddress(description)` and `deployProfile` use the same derivation, so the address is always predictable before deployment.

## Auth mechanisms

### addContext — owner pubkey auth

```solidity
TvmBuilder b;
b.store(msg.pubkey());
require(tvm.hash(b.toCell()) == _pubkeyHash, ERR_INVALID_SENDER);
```

The caller must sign the external message with the private key whose `tvm.hash(pubkey)` matches the `pubkeyHash` passed at deploy time.

### receive — multifactor destroy

```solidity
receive() external {
    TvmBuilder b;
    b.store(msg.sender);
    if (tvm.hash(b.toCell()) == _multifactorHash) {
        selfdestruct(_root);
    }
}
```

Only an internal message from the address whose `tvm.hash(address)` matches `_multifactorHash` triggers `selfdestruct`. All other senders are silently ignored.
