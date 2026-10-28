pragma gosh-solidity >=0.76.1;
pragma AbiHeader expire;
pragma AbiHeader pubkey;

import "./modifiers/modifiers.sol";
import "./interfaces.sol";
import "../dex/libraries/DexLib.sol";
import "../dex/RootPN.sol";
import "./InferenceOrderBook.sol";

/// @title TokenContract (streaming deal / `token_contract` per spec §3-4)
/// @notice One streaming inference deal between one seller (owner) and one
///         buyer. Payment is in ECC[2] SHELL, escrowed in this contract;
///         identities/locks ride on PrivateNotes (model B).
///
///         CONSUMPTION MODEL. The first tick is a PROBE: frozen at `open()`, owed to nobody, and
///         nothing is claimable until `acceptProbe` takes it (§3.1.2). Past that the escrow stays
///         whole until the seller CLAIMS consumption (`claimTokens`, cumulative from zero — the
///         probe is its first tick), and the seller posts the mirror `SELLER_BOND` (2P,
///         `_bondAmount()`, §4.2) before `open()`. There is no prepaid/frozen buffer beyond the
///         probe: a claim earns money by outliving its own window, not by being paid in advance.
///         A THREE-DEEP pipeline carries the claim, all in cumulative TOKENS so remainders survive
///         (56 then 33 is 89, nothing lost to rounding): one FINAL figure, which is the only one
///         money is computed from, and two PENDING claims. A fresh claim advances the pipeline by
///         one; `finalize()` drains it once `CLAIM_PROMOTE_WINDOW` passes with no dispute — which
///         is what makes the LAST claim payable, since nothing supersedes it. The seller may thus
///         run up to two ticks ahead of what he can be paid for: exactly the old two-tick exposure
///         bound, which is why the bond stays 2P mirroring it.
///
///         Claims are bounded twice: `MIN_CLAIM_INTERVAL` between them, and `MIN_SECONDS_PER_TICK`
///         on how much output the elapsed time can justify — one tick per minute, the ceiling no
///         model on the market can beat (fastest known: 1M tokens in ~58.8 s).
///
///         ONE contract, two settlements, selected by the deal flags the book hands down at match:
///           - ORDINARY deal (`subWeeks == 0`): pay by FACT — trusted ticks x price, the rest of
///             the escrow returns to the buyer. Time is irrelevant.
///           - SUBSCRIPTION (`subWeeks > 0`): TAKE-OR-PAY — each week boundary credits the whole
///             weekly quota regardless of consumption (`settleWeek`), because what was bought is
///             reserved availability. Consumption tracking exists for the dispute path only.
///
///         DISPUTE (§4.2) reaches ONLY the contested delta `claimed - trusted`; everything trusted
///         is beyond reach, exactly as a finalized tick was. What it reaches is DESTROYED, not
///         handed over — on the concession branch as much as on the timeout. The buyer gets back
///         the weeks the term never started and nothing more, which is what `stop()` returns too,
///         so disputing never refunds him more than simply ending the deal; it costs him `D` on
///         top, staked from the bond he posted beside the escrow. What it buys him is that the
///         seller collects only for what his claims prove. The timeout differs in one thing: it
///         burns an equal `D` from the seller's bond as well, which is what keeps agreeing his
///         cheaper branch. Notes are never locked — all economics live here.
///
///         Lifecycle:
///         1. `fund()`/`fundFromOrderBook()` — buyer escrows SHELL; the buyer
///                             note pubkey is recorded (spec §2.3/§3.1.1).
///         1b.`fundDeal(amount)` — the one funding door: the seller's note sends gas as
///             attached ECC[2] and the `SELLER_BOND` mirror bond (2P) as a figure.
///         2. `open(cipher)` — seller posts the endpoint encrypted to the
///                             buyer's pubkey and freezes the probe tick;
///                             state = `Probe`. The note is NOT locked.
///         3. `claimTokens()` — seller reports cumulative consumption (rate-limited).
///         3c.`finalize()`   — permissionless: promote pending claims after the window.
///         3b.`settleWeek()` — subscription only: permissionless weekly take-or-pay credit.
///         4a.`stop()`       — buyer exit; settles trusted ticks, refunds the rest. Also the
///                             seller-no-show path: silence leaves his last claim unpromoted.
///         4b.`dispute()`    — buyer contests the delta;
///                             `releaseDispute()` / `resolveDisputeTimeout()`.
///         5. `withdrawShell`/`destroy` — seller pulls finalized SHELL (§3.5).
contract TokenContract is AiRegistryModifiers {
    string constant version = "4.0.36";

    // `SUPER_ROOT_ADDR` used to live here. Its ONLY use was as the fixed sink for
    // `cleanupUnopened`'s residual-native sweep — a permissionless call, so the leftover gas went to
    // a constant rather than anywhere a caller could name. That sweep now goes to `_sellerNote`, and
    // the guard turned out to be unnecessary: `_sellerNote` is stored state, verified against the
    // canonical note derivation when it is bound, so a caller cannot aim it either. With the last
    // use gone the constant was dead, and a dead address constant is the same trap as a dead error
    // code (#831) — it reads like something the contract still relies on.


    // Canonical PrivateNote code hash/depth. `postFromNote` proves the note-driven
    // caller is a genuine PrivateNote (pinned code) for the supplied deposit id. Only
    // the canonical RootPN can deploy a canonical-code note, and it always bakes its
    // real InferenceOrderBook code into that note, so a passing caller's supplied
    // book hash is authoritative — the TC needs no RootPN round-trip. The note does
    // NOT pin the TC code (RootPN bakes it into the note at deploy), so this pin is
    // one-way (TC->note) and the build stays cycle-free. Re-pin when PrivateNote is rebuilt.
    uint256 constant PRIVATE_NOTE_CODE_HASH  = 0xacf19e140b58469a50165bcbda88cca952b2036678f1f5823b6a6bebd3fc32b1;
    uint16  constant PRIVATE_NOTE_CODE_DEPTH = 20;

    // Native value attached to THIS contract's messages (register / stream-lock / payout).
    // Tunable; recipients that live in a configured dapp self-fund via `accept` + their own mint,
    // so this only needs to cover what a non-accepting hop requires. Since 4.0.36 the deal itself
    // is one of those: deployed by its seller's note, it lands in that note's dapp and mints its
    // own native floor in `ensureBalance`. What it cannot mint is ECC[2], which is what every
    // entry burns — that reserve comes from the deploy and from the two funding calls, one per
    // party. (TC/RM-local — NOT the shared REGISTER_FORWARD_VALUE the IOB also uses for its
    // SHELL handover.)
    /// @notice RootPN, the custodian a write-off is reported to.
    /// @dev    Declared here rather than inherited: a deal extends the airegistry modifiers, not
    ///         the dex ones where the dex-side literal lives. Used for one thing — telling the
    ///         holder of the coins that a figure behind them will never be redeemed.
    address constant ROOT_PN_ADDR = address.makeAddrStd(0, 0x1010101010101010101010101010101010101010101010101010101010101010);

    varuint16 constant DAPP_MSG_VALUE = 0.01 vmshell;

    event ContractDeployed(address self);
    event StreamFunded(address buyer, uint128 deposit);
    event SellerBondFunded(uint128 amount);
    event BuyerBondFunded(uint128 amount);
    event StreamOpened(address buyer, uint128 pricePerTick);
    /// @notice The endpoint ciphertext was written. Carries the ciphertext itself, so a buyer who
    ///         hears it does not have to come back for `getEndpointCipher()` — the value is public
    ///         either way, encrypted to his key, so putting it in the event reveals nothing new.
    event EndpointSet(bytes endpointCipher);
    event TickFinalized(uint128 finalizedOwed, uint128 deposit);
    event TicksClaimed(uint128 trusted, uint128 claimed);
    event StreamStopped(address buyer, uint128 toSeller, uint128 refundToBuyer);
    event ProbeAccepted(address buyer, uint128 toSeller, uint128 bondReturned);
    event ProbeBurned(address buyer, uint128 burnedProbe, uint128 burnedBond, uint128 refundToBuyer);
    event StreamDisputed(address buyer, uint64 at);
    event DisputeResolved(uint128 toSeller, uint128 refundToBuyer, bool released);
    // `StreamReclaimed(address buyer, uint128 refundToBuyer)` stood here and was never emitted —
    // no `emit`, and no external-address constant to emit it through, so nothing could have. It is
    // removed rather than left as documentation: an event in the ABI is a promise to indexers that
    // something can be observed, and this one promised an outcome that never occurs. Measured on
    // both builds: dropping it changes neither the code hash nor the depth (the TVC is byte-
    // identical), only the ABI's event count.
    event ShellWithdrawn(address recipient, uint128 amount);
    event ContractDestroyed(address self);

    // Static (part of stateInit, contribute to address derivation).
    uint256 static _sellerPubkey;
    address static _rootModelAddress;
    uint64 static _nonce;

    // Canonical InferenceOrderBook code hash/depth, delivered by the seller's
    // canonical PrivateNote via `postFromNote` (runtime, NOT a pinned constant → no
    // IOB<->TC cycle). Authenticates the `fundFromOrderBook` caller AND lets the TC
    // derive the book address to post its own sell offer. 0 until the note posts.
    uint256 _iobHash;
    uint16  _iobDepth;
    // Seller note confirmed this TC is its deal (`postFromNote`, msg.sender ==
    // _sellerNote). A TC nobody confirmed never gets the book hash → never trades.
    bool    _noteAuthorized;
    // A TC is a one-shot deal: at most one LIVE resting sell offer (was enforced by
    // the IOB `_sellTcInUse` map; now the TC itself is the single source). Cleared
    // on cancel (`onSellClosed`) so the seller can re-list on the same live TC.
    bool    _offerPosted;

    // Immutable deal config (constructor).
    string  _modelName;
    uint256 _modelHash;       // sha256(_modelName), verified in the ctor — on-chain authoritative id
    // tokens per tick — FIXED protocol constant (no longer a per-deal param).
    uint128 constant TICK_SIZE = 1_000_000;
    // Ceiling on what ONE claim may add. A claim states a tick of work, so a tick is what it may
    // state; a longer silence is claimed as a sequence of claims, not as a single large one.
    // This is what bounds the disputable amount: the contested value can never exceed one tick,
    // whatever the rate allowance accumulated to, so the seller bond covers it by construction.
    // It costs no throughput — MIN_CLAIM_INTERVAL is one minute and a tick takes a minute to
    // produce, so a seller running at the physical ceiling claims exactly one tick per claim.
    uint128 constant MAX_CLAIM_DELTA = TICK_SIZE;
    uint128 _pricePerTick;    // SHELL per tick (P)
    uint128 _maxTicks;        // upper bound on ticks this deal serves

    // Deal state.
    address _buyer;           // buyer note address (funder; payouts/locks)
    uint256 _buyerPubkey;     // buyer note pubkey (gateway auth, spec §3.1.1)
    // `_buyer` is bound once, by the book, on the match — there is no other funding door and no
    // way for a caller to name itself the buyer. The pubkey beside it is the buyer note's, threaded
    // through the match for the §3.1.1 gateway.
    address _sellerNote;      // seller note address (dispute lock)
    bytes   _endpointCipher;  // endpoint encrypted to the buyer's pubkey

    bool    _funded;
    bool    _opened;
    // Latch: open() was ever called. cleanupUnopened is a permissionless no-show
    // recovery for a funded-but-NEVER-opened deal. The latch scopes it to that case
    // only: after a normal open+close the `!_opened` guard is true again (`stop` leaves
    // _opened=false), and this latch keeps cleanupUnopened from running once the
    // seller has real `_finalizedOwed` earnings to withdraw.
    bool    _everOpened;
    bool    _disputed;

    // ── Probe tick (spec §3.1.2) ────────────────────────────────────────────────────────────
    // The FIRST tick of every deal is a trial. It is frozen out of the buyer's escrow at `open()`
    // and paid to nobody until the buyer has had PROBE_WINDOW to try the endpoint. Nothing may be
    // claimed before it is accepted, so a seller who never delivers cannot reach the escrow at all,
    // and a buyer who finds nothing there stops and burns it — together with a mirror tick of the
    // seller's bond, which is what makes the revenue of a first-tick scam exactly zero.
    bool    _probeAccepted;   // false = Probe (nothing claimable yet), true = the stream is live
    uint128 _probeTick;       // SHELL held as the probe (value P) while unaccepted
    uint64  _probeTime;       // when the probe was frozen (its acceptance window)

    bool    _sellerBondFunded; // seller posted the mirror bond (SELLER_BOND = 2P)
    // Buyer posted his own `2P`. SEPARATE FROM `_funded`, and the separation is the point: the
    // escrow arrives from the BOOK at the match, the bond arrives from the buyer's NOTE afterwards,
    // and only then is `open()` allowed. The bond cannot ride the escrow because its size is
    // `2 * clearing` and the clearing price does not exist until the match has happened.
    bool    _buyerBondFunded;
    uint128 _sellerBond;       // SHELL held as the seller's mirror collateral (up to 2P), §4.2

    /// @notice The buyer's own `2P`, held OUTSIDE the escrow — a subscription only. It is what `D`
    ///         is taken from, and holding it apart from `_deposit` is the whole point: the escrow
    ///         is spent down week by week, so a stake taken from it costs the buyer only as much
    ///         as he still expected back. Whole weeks refund, so in weeks one through three that
    ///         is the full `D`; in the LAST week nothing is coming back and the same `D` would be
    ///         drawn from money already destined to burn, making the dispute free exactly where
    ///         take-or-pay has the most left to lose. Kept separate, it bites the same everywhere
    ///         in the term, and it returns untouched when no dispute happens.
    ///
    ///         An ordinary deal has no weekly clock and no take-or-pay, so it needs none of this
    ///         and posts nothing: its stake comes from the escrow as before.
    uint128 _buyerBond;

    /// @notice The deal's SHELL, held as a NUMBER (generation 4.0.33).
    /// @dev    This is the private analogue of ECC[2] — same currency, different place: a record
    ///         in this contract instead of physical currency on the account. It is the private
    ///         `_balance[CURRENCIES_ID_SHELL]` a PrivateNote keeps, narrowed to a scalar because a
    ///         deal only ever handles the one type.
    ///
    ///         `_balance` is the TOTAL; `_deposit`, `_finalizedOwed`, `_buyerBond` and
    ///         `_sellerBond` are earmarks WITHIN it, which is why they are tracked separately and
    ///         why none of them is read as a balance. Every figure that enters does so from an
    ///         authenticated peer, and every figure that leaves is subtracted here before the peer
    ///         is asked to add it — so across any pair, what one side took off its record the other
    ///         side put on. Nothing is minted here and nothing is destroyed by transfer; a burn is
    ///         a subtraction from this number and nothing else.
    uint128 _balance;

    uint128 _deposit;         // SHELL available for future ticks (value + reserved fee)
    uint128 _finalizedOwed;   // SHELL finalized to the seller (withdrawable; incl. rebate / returned bond)
    uint128 _feeAccrued;      // SHELL fee charged by-fact on finalized ticks (§5.1)
    uint128 _ticksFinalized;  // count of finalized ticks (n for rebate §5.3)
    bool    _everDisputed;    // a dispute ever opened → no rebate (§5.3)
    uint64  _fundedTime;      // when funded (MATCH_OPEN_TIMEOUT cleanup, §2.1)
    uint64  _disputeTime;     // when the dispute opened

    // ── Deal shape, set once at funding from the BUY side of the match ──────────────────────
    // The book hands down the DEAL slice of the buyer's order flags (TEE + SUBSCRIPTION), and the
    // shape follows from them alone — a subscription is always one month, so nothing about the
    // term travels with the fill. ONE contract serves both kinds of deal: an ordinary purchase is
    // simply a subscription with zero weeks, so there is no second per-deal contract, no second
    // pin, and no branch that only one of them exercises.
    /// @dev Mirror of the book's deal-flag bit; contract-local constants are not reachable across
    ///      contracts, so it is restated here — keep in sync if the bit is ever renumbered.
    uint8   constant FLAG_SUBSCRIPTION = 0x40;
    // A RECORD of what was bought, not a gate. Deal-shape compatibility is decided in the book by
    // `_dealCompatible` before any SHELL moves, so by the time a fill reaches this contract the
    // pairing is already settled and nothing here re-checks it. Kept so an off-chain reader — the
    // gateway deciding whether to serve a TEE endpoint, an indexer classifying deals — can see the
    // shape without reconstructing the match.
    uint8   _dealFlags;       // DEAL_FLAGS_MASK slice: TEE | SUBSCRIPTION
    uint8   _subWeeks;        // SUB_WEEKS on a subscription; 0 = ordinary deal
    uint8   _weekIndex;       // weeks already settled (0 .. _subWeeks)
    uint128 _tokensPerWeek;   // weekly quota in tokens; ordinary deal = the whole funded volume
    uint128 _fundedTokens;    // total tokens the buyer paid for (cap on any claim)
    uint128 _tokensPaid;      // tokens already paid for (basis for the next payout delta)
    uint64  _periodStart;     // when the weekly clock started (probe acceptance)
    uint128 _weekBaseTokens;  // cumulative consumption at the start of the current week

    // ── Consumption accounting (seller-claimed, buyer-contestable) ─────────────────────────
    // The seller periodically claims the CUMULATIVE tokens the buyer consumed. Two accumulators:
    // the older value nobody complained about is TRUSTED and irrevocably the seller's; the newest
    // claim is CONTESTED and is what a dispute is fought over. A claim becomes trusted once its own
    // CLAIM_PROMOTE_WINDOW has passed with no complaint — measured per claim, from the moment it
    // landed, so the seller cannot shorten anyone's contest time by claiming faster.
    // Two-deep claim pipeline, both CUMULATIVE TOKEN counts (not ticks: remainders must survive,
    // so 56 then 33 is 89 and nothing is lost to rounding).
    //   _tokensFinal  — promoted, irrevocably the seller's, the only figure money is computed from
    //   _tokensPend   — the one pending claim, landed at `_lastClaimTime`
    //
    // ONE PENDING SLOT, AND THAT IS THE POINT RATHER THAN A SAVING. It used to be two, which let
    // the seller stand two ticks ahead of what was trusted; a dispute was then about a quantity he
    // could grow by claiming quickly, and "file two claims and go quiet" was a shape the contract
    // permitted. With `CLAIM_PROMOTE_WINDOW == MIN_CLAIM_INTERVAL`, the next claim may not arrive
    // before the current one has matured, so at most ONE tick is ever unsettled. The old scheme
    // needed a defensive check that the constants still added up; this one does not need the check
    // because there is nothing left to add up.
    uint128 _tokensFinal;
    uint128 _tokensPend;
    uint64  _lastClaimTime;   // when _tokensPend landed: min-interval, rate cap, its own window

    constructor(
        string  modelName,
        uint256 modelHash,
        uint128 pricePerTick,
        uint128 maxTicks,
        address sellerNote,
        uint256 depositIdentifierHash
    ) {
        // DEPLOYED BY THE SELLER'S CANONICAL PrivateNote (generation 4.0.36), as an INTERNAL
        // message. It used to be an external message signed with the seller key, gated on
        // `msg.pubkey() == _sellerPubkey` — a check that says nothing here, because an internal
        // message carries no pubkey at all and would pass it with a zero.
        //
        // The replacement is the check `postFromNote` already made, moved to the only moment that
        // settles the question: the sender must BE the canonical note for `depositIdentifierHash`,
        // derived from the pinned PrivateNote code. Deal terms (price/model/maxTicks) are ctor
        // arguments and not part of the address, so without this a third party could occupy this
        // canonical (sellerPubkey, nonce) address with foreign terms and the seller's offer would
        // then rest in his name carrying them.
        //
        // Note the ORDER: authenticate before `accept`, so a stranger's deploy attempt is refused
        // at the door and paid for by whoever sent it.
        require(msg.sender == sellerNote, ERR_INVALID_SENDER);
        require(sellerNote == DexLib.computeCanonicalNoteAddressFromHash(
            PRIVATE_NOTE_CODE_HASH, PRIVATE_NOTE_CODE_DEPTH, depositIdentifierHash), ERR_INVALID_SENDER);
        tvm.accept();
        // The deal is in a configured dapp from its first block now, so it can hold its own floor
        // instead of depending on the funder to have attached enough.
        ensureBalance();
        _chargeGas(GAS_DEPLOY);
        require(pricePerTick > 0, ERR_BAD_PARAM);
        require(maxTicks >= 2, ERR_BAD_PARAM);
        // maxTicks*(price + fee) must fit uint128 so every downstream unit×ticks total (the
        // deposit bound here and the order-book fill cost) stays overflow-free.
        require(uint256(maxTicks) * uint256(pricePerTick + _fee(pricePerTick)) <= uint256(type(uint128).max), ERR_OVERFLOW);
        // On-chain authoritative model id: same single-cell sha256 invariant as the order book.
        // Binds this deal contract's modelHash to the verified `producer--model--version` preimage
        // (so an indexer reading the TC alone gets the genuine model name, not a free-text label).
        require(modelName.byteLength() <= 127, ERR_BAD_PARAM);
        require(sha256(modelName) == modelHash, ERR_BAD_PARAM);

        _modelName    = modelName;
        _modelHash    = modelHash;
        _pricePerTick = pricePerTick;
        _maxTicks     = maxTicks;
        _sellerNote   = sellerNote;
        // AN ADDRESS FIELD THAT IS ONLY EVER READ MUST STILL BE WRITTEN ONCE (#941). A declared and
        // unassigned `address` is `addr_none`, and `addr_none` is not `address(0)`: the first has a
        // one-element representation, the second is `addr_std(0, 0)` with four. `.value` compiles to
        // PARSEMSGADDR and then takes the FOURTH element, so reading `.value` on an untouched field
        // is an out-of-range access — `exit_code 5`, not a false, and no `require` in sight.
        //
        // `_buyer` is written in exactly one place, `_recordFunding`, so it holds a real address
        // only once a deal has been funded. Every close ends in `_die`, and `_die` opens with
        // `if (_buyer.value != 0)`. A deal nobody ever bought therefore could not be closed by any
        // path at all — the seller's bond stayed inside a contract that refused to die.
        //
        // This assignment is the whole fix: it makes the field `addr_std(0, 0)`, whose `.value` is a
        // readable zero, so the guard answers false and the close proceeds. `_sellerNote` never had
        // the problem because the constructor above always sets it.
        _buyer        = address(0);


        // ITS OWN ADDRESS, not the one `RootModel` announces its birth on. Both contracts emit an
        // event called `ContractDeployed` with the same body — `(address self)` — and both used to
        // send it to `ContractDeployedEmit`. That is the quiet kind of collision: the decode
        // SUCCEEDS on either message and yields a plausible address, so a listener waiting for a
        // deal to appear is handed root models as well and cannot tell them apart except by the
        // message's `src`. A deal and a registry root are not the same event and no longer share a
        // channel; the deal's belongs with the deal's other events, not in the registry's range.
        address selfExtern = address.makeAddrExtern(DealDeployedEmit, bitCntAddress);
        emit ContractDeployed{dest: selfExtern}(address(this));

        // `bounce: false`, STATED rather than left to the default. This was the deal's only
        // bounceable send — the flag was simply absent, which means TRUE — while the other seven
        // outgoing calls all say `false` explicitly and the contract carries no `onBounce` at all.
        // So a refused registration produced a bounce nobody handled: the value came back, no
        // state was corrected, and the deal went on believing it was registered.
        //
        // Saying `false` does NOT make that silence go away — it makes it uniform and deliberate.
        // The one condition that can refuse this call is a `TOKEN_CONTRACT_CODE_HASH` in the root
        // that disagrees with the deal's real code, i.e. a pin the cascade did not update; the
        // deal cannot learn about it either way, since there was never a handler to tell.
        IRootModelRegistry(_rootModelAddress).registerTokenContract{
            value: DAPP_MSG_VALUE, flag: 1, bounce: false
        }(_sellerPubkey, _nonce);
        // NOTE: the InferenceOrderBook hash is delivered later by the seller's
        // canonical PrivateNote (`postFromNote`), so a TC nobody confirmed never activates.
    }

    // A DEAL FUNDS ITS OWN GAS AGAIN (generation 4.0.36), because it finally lives somewhere that
    // lets it. The floor below is the same three lines every other contract in this tree carries.
    //
    // It was removed in 4.0.33 for a reason that has now expired. A mint draws on the DAPP CONFIG
    // of the dapp the contract lives in; a TokenContract was deployed by an EXTERNAL message into
    // its OWN dapp, and that dapp has no config, so there was nothing for it to draw on. The deal
    // is deployed by the seller's PrivateNote now and lands in the note's dapp — configured, like
    // everything else — so the mint has a config behind it and the floor holds.
    //
    // What this replaces is the funder's one-time deposit. `fundDeal` used to arrive with ECC[2]
    // under flag 17 (16 = convert the sent token, 1 = fees on the sender), and the conversion WAS
    // the deal's whole gas supply: one call, two effects — gas on the account, money on `_balance`.
    // Undershoot it and the deal stopped forever, which is what made sizing it a published formula
    // rather than a detail. Now the native side is self-served and the ECC stays ECC, held as the
    // reserve every entry burns its charge from (see `_chargeGas`).
    function ensureBalance() private pure {
        if (address(this).balance > MIN_BALANCE) { return; }
        gosh.mintshellq(MIN_BALANCE);
    }

    /// @notice Burn this entry's measured SHELL charge (#950 table, constants in modifiers).
    /// @dev    The charge is a BURN, not a payment: nobody is credited, which is what makes calling
    ///         a deal cost something rather than move something. It comes out of the ECC reserve the
    ///         funder put in, so the caller of an external entry (`claimTokens` and the other
    ///         key-signed ones) spends the deal's reserve rather than attaching currency — an
    ///         external message cannot carry any. Internal callers attach theirs on the message.
    ///
    ///         Sized so the reserve covers a whole deal: ~0.215 constant + 0.013 per tick (#950).
    ///         A reserve that runs dry does NOT strand the deal the way an empty native balance
    ///         once did — `ensureBalance` keeps it answering — so an exhausted reserve is a refused
    ///         burn on one entry, not a contract that stops existing.
    function _chargeGas(uint64 amount) private pure {
        gosh.burnecc(amount, SHELL_ECC_ID);
    }

    // EVERY `selfdestruct` IN THIS FILE DISPOSES OF `_balance` ON ITS OWN LINE, in the function
    // that destructs. There was a `_sweepBalance(to)` helper here and it is gone on purpose: a
    // reader looking at a wind-down has to see where the money went without following a call, and
    // an irreversible operation should not hide its disposal behind a name.
    //
    // WHY THE DISPOSAL HAS TO BE WRITTEN AT ALL (generation 4.0.33). `selfdestruct` used to be a
    // sweep for free: the deal's SHELL was ECC on the account, so destroying the account carried
    // whatever was left to the payout address, and a wind-down could end on `selfdestruct` without
    // saying anything about money. `_balance` is a variable. It does not travel. Destroying the
    // contract with a non-zero `_balance` pays nobody — it annihilates the figure, silently, with
    // no failed call and nothing in a log.
    //
    // A residual is not hypothetical, though the way it arises has narrowed. Payouts no longer come
    // home — `bounce: false`, and there is no handler to catch them — so the old source is gone.
    // What remains is arithmetic: rounding on the fee split, a figure zeroed in one earmark and
    // handed on through `_balance`, an amount below what any path pays out. Small, and none of it
    // owned by anyone the contract can name, which is exactly why the sweep is explicit.
    //
    // The rule each site follows: THE RESIDUAL GOES WHERE THE DESTRUCT'S OWN PAYEE GOES, because
    // that is what residual ECC did by itself. `cleanupUnopened` is the single exception and says
    // so at length: it pays TWO parties, so a residual there has no identifiable owner.


    /// @notice A note asks whether this deal is still here and still hers.
    /// @dev    THE ANSWER IS THE ABSENCE OF A BOUNCE, and this method's whole job is to produce a
    ///         bounce in exactly the cases where it should. It checks the sender and does nothing
    ///         else — no state, no payment, no event. Anything more would give it a second way to
    ///         fail, and a failure here is read as "the deal is gone".
    ///
    ///         Three outcomes, not two:
    ///           alive and the caller is one of its notes -> no bounce; the record stands, and the
    ///                                                       deal will announce its own close later
    ///           this deal does not exist                 -> bounce; the record is cleared
    ///           alive, but the caller is a stranger      -> bounce; the record is cleared, and
    ///                                                       rightly: that note was tracking
    ///                                                       something that was never hers
    ///
    ///         The third case is why this REVERTS rather than returning quietly. A deal that never
    ///         bounces can only ever report its own death, so a note holding a stale record — one
    ///         belonging to no deal of hers — would have nothing that could clear it and would be
    ///         unwithdrawable forever. Reverting is what makes the signal answer both kinds of
    ///         wrong instead of one.
    ///
    ///         BOTH notes must pass. A deal is tracked by the buyer and the seller alike, and a
    ///         check admitting only one would tell the other that a live deal is dead.
    function touchDeal() public view {
        require(msg.sender == _buyer || msg.sender == _sellerNote, ERR_INVALID_SENDER);
    }

    /// @notice The ONE way this contract ceases to exist.
    /// @dev    There were five `selfdestruct` calls and no helper. Five places that must not forget
    ///         the same thing are five chances to forget it, and task E adds a thing that must not
    ///         be forgotten: both notes have to be told, or their `_liveDeals` record rests forever
    ///         and the owner can never withdraw. That is a worse failure than the one the record
    ///         exists to prevent, so the announcement cannot live at each call site.
    ///
    ///         TWO MESSAGES, NO PARAMETERS. A deal announcing its own death is `msg.sender` on the
    ///         receiving side, and a note keys `_liveDeals` by address — so the sender authenticates
    ///         itself and identifies itself in one, and nothing has to be passed or compared.
    ///
    ///         `bounce: false`: the notes may be gone, and a deal that is destroying itself has
    ///         nowhere to receive a bounce. This is the last thing it ever does.
    /// @notice The one way a deal ends after it has owed the seller anything (task O).
    /// @dev    PAY, THEN DIE — never leave a debt behind a living contract.
    ///
    ///         Every close used to credit `_finalizedOwed` and return, leaving the deal alive until
    ///         the seller came to collect. `stop()` is called by the BUYER, and both of its branches
    ///         create that debt: the buyer's own call left a contract standing that only the seller
    ///         could clear, and while it stood, the buyer's note counted it among its live deals and
    ///         refused to withdraw. He locked himself out, and the key was held by the party with no
    ///         reason to hurry.
    ///
    ///         The residual figure goes the same way in the same breath. Everything owed to anyone
    ///         was settled by the caller before reaching here, so whatever is left is arithmetic
    ///         remainder, and it follows the destruct's payee like the native gas always did.
    ///
    ///         `withdrawShell` is untouched and still needed: a subscription accrues `owed` at every
    ///         weekly boundary, and the seller draws it MID-LIFE, long before any of this runs.
    function _payOwedAndDie() private {
        uint128 owed = _finalizedOwed;
        _finalizedOwed = 0;
        if (owed > 0) { _payShell(_sellerNote, owed); }
        if (_balance > 0) { _payShell(_sellerNote, _balance); }
        _die(_sellerNote);
    }

    function _die(address payoutAddress) private {
        if (_buyer.value != 0) {
            IInferenceNoteMirror(_buyer).onDealClosed{value: DAPP_MSG_VALUE, flag: 1, bounce: false}();
        }
        if (_sellerNote.value != 0) {
            IInferenceNoteMirror(_sellerNote).onDealClosed{value: DAPP_MSG_VALUE, flag: 1, bounce: false}();
        }
        emit ContractDestroyed{dest: address.makeAddrExtern(ContractDestroyedEmit, bitCntAddress)}(address(this));
        selfdestruct(payoutAddress);
    }

    /// @notice Pay `amount` of the deal's SHELL to the note (generation 4.0.33).
    /// @dev    No currency moves. This subtracts the figure from `_balance` and asks the receiver
    ///         to add the same figure to its own record — the two halves of one transfer, so the
    ///         pair conserves: what left here arrived there, and nothing was created on the way.
    ///         The receiver authenticates this contract by re-deriving its canonical address from
    ///         `_sellerPubkey`/`_nonce` (both are static, so the derivation cannot be spoofed by a
    ///         contract that is not the real deal), which is what stops an unknown contract from
    ///         handing a note a number and having it stick.
    ///
    ///         `bounce: false`, AND THE REASON IS THAT THE RECEIVER IS NO LONGER ARBITRARY. Every
    ///         payout now goes to a note that is party to this deal — `_sellerNote` or `_buyer` —
    ///         and a note's credit entry does nothing but check its caller. It has no branch that
    ///         can refuse a legitimate deal, so a bounce could only ever mean the note is gone,
    ///         which cannot happen while the deal it owns is alive.
    ///
    ///         The `onBounce` handler this used to need is gone with it, and so is the `purpose`
    ///         parameter that existed solely to tell that handler which earmark to restore. Both
    ///         were the cost of paying an address the seller could name; that address is gone too
    ///         (see `withdrawShell`, `close`, `destroy`).
    ///
    ///         The subtraction still happens BEFORE the call, and now it is unconditional: there is
    ///         no path that puts the figure back, so a credit that lands must never be spendable
    ///         here as well.
    function _payShell(address to, uint128 amount) private {
        if (amount == 0) { return; }
        _balance -= amount;
        IPrivateBalance(to).creditFromDeal{value: DAPP_MSG_VALUE, bounce: false, flag: 1}(
            amount, _sellerPubkey, _nonce);
    }

    /// @notice Burn SHELL (spec §5.4) — generation 4.0.33.
    /// @dev    A burn is now a SUBTRACTION and nothing else. With the balance held as a number,
    ///         destroying value means the number goes down: there is no currency to hand to
    ///         `gosh.burnecc`, and calling it would destroy ECC this contract no longer holds.
    ///         The old uint64 chunking existed only because that builtin took a uint64; a
    ///         subtraction has no such bound, so the loop is gone and a burn of any size is exact
    ///         in one step. Burnt SHELL is not credited to anyone — that is precisely what makes
    ///         it a burn rather than a transfer, and it is the one place a pair does NOT balance
    ///         by design.
    function _burnShell(uint128 amount) private {
        if (amount == 0) { return; }
        _balance -= amount;
        // AND TELL THE CUSTODIAN. This contract destroys nothing — it holds no currency, which is
        // the whole of this generation — so what happens above is a figure going down. The coins
        // behind it sit in RootPN, whose `_deployedValues` counts claims that have not been
        // redeemed. A written-off figure will never be redeemed, so unless the root is told, that
        // ledger keeps counting an obligation nobody will ever present.
        //
        // Today the consequence is only that the pool stays OVER-collateralised, which is safe. It
        // stops being safe the moment that accumulation is itself destroyed: the pool would then be
        // short against honest withdrawals, silently.
        //
        // One-way, and losing it degrades in the safe direction — no report means no accounting,
        // which is exactly today's behaviour. `sellerPubkey` and `nonce` are this deal's statics,
        // so the root re-derives this address from its own baked codes and admits nothing else.
        IWriteOffSink(ROOT_PN_ADDR).reportDealWriteOff{
            value: DAPP_MSG_VALUE, flag: 1, bounce: false
        }(_sellerPubkey, _nonce, amount);
    }

    // NO `onBounce`. The deal makes no bounceable call: every payout goes to a note that is party
    // to this deal, under `bounce: false`, and the one-way report to the custodian is `bounce:
    // false` too. A handler here would be code that can never run, and the honest way to say that
    // is not to write it.
    //
    // The handler that used to live here existed for one address — the recipient `withdrawShell`
    // let the seller name. That parameter is gone, and with it the only payout that could
    // realistically fail. Its `purpose` argument, the bounced-window budget it rode in, and the
    // `PAY_GENERAL` / `PAY_OWED` earmarks all belonged to the same machinery and are gone as well.

    /// @notice Platform fee (2.5%, PLATFORM_FEE_BPS) of `amount` (spec §5.1).
    function _fee(uint128 amount) private pure returns (uint128) {
        return uint128(uint256(amount) * uint256(PLATFORM_FEE_BPS) / uint256(BPS_DENOMINATOR));
    }

    /// @notice Seller mirror bond (spec §4.2): two ticks, for every deal shape. A dispute is about
    ///         CLAIMS — the unfinalized ones — and `MAX_CLAIM_DELTA` caps a claim at one tick, so
    ///         two ticks cover the whole pipeline (both pending slots) with room to spare. Nothing
    ///         about the subscription clock enters here: weekly settlement is not disputable
    ///         volume, it is the price of a reservation, and it never becomes the subject of a
    ///         burn.
    ///         Held in this TC until close, returned on a clean exit or a no-show, and burned
    ///         mark-for-mark against the buyer's burned `D` on either dispute resolution. The
    ///         platform fee (§5.1) is separate and not part of this bond.
    function _bondAmount() private view returns (uint128) {
        return 2 * _pricePerTick;
    }

    /// @notice Seller rebate (§5.3) for `n` cleanly-finalized ticks at price P:
    ///         rate = min(REBATE_MAX_BPS, REBATE_SLOPE_BPS·n) bps; rebate =
    ///         rate/10000 · n · P. Always < the fee charged on n ticks (rate <
    ///         PLATFORM_FEE_BPS by construction), so net burn stays positive.
    function _rebate(uint128 n) private view returns (uint128) {
        uint256 rateBps = uint256(REBATE_SLOPE_BPS) * uint256(n);
        if (rateBps > uint256(REBATE_MAX_BPS)) { rateBps = uint256(REBATE_MAX_BPS); }
        return uint128(rateBps * uint256(n) * uint256(_pricePerTick) / uint256(BPS_DENOMINATOR));
    }

    /// @notice Settle accrued fees at close: pay the seller a rebate (only on a
    ///         clean, never-disputed close, §5.3) and burn the net (§5.4).
    function _settleFees(bool clean) private {
        uint128 rebate = 0;
        if (clean && !_everDisputed) {
            rebate = _rebate(_ticksFinalized);
            if (rebate > _feeAccrued) { rebate = _feeAccrued; }   // safety (never triggers by construction)
        }
        uint128 netBurn = _feeAccrued - rebate;
        if (rebate > 0) { _finalizedOwed += rebate; }             // seller withdraws it
        _burnShell(netBurn);
        _feeAccrued = 0;
        // Every streaming close (stop / dispute-resolve) routes
        // through here after `_ticksFinalized` is final, so this is the single point that
        // publishes the deal's finalized volume to the reference-price median.
        _reportFinalized();
    }

    /// @notice Publish this deal's CUMULATIVE finalized-tick count to the canonical
    ///         InferenceOrderBook, which records only the new delta into the reference-price
    ///         VWAP/median — so a match that is later refunded (never served) contributes no
    ///         volume, killing the reserved-volume oracle manipulation. Cumulative + book-side
    ///         delta is exactly-once, so extra calls are harmless; a deal that finalized nothing
    ///         reports nothing. bounce:false — best-effort, never blocks the close it rides on.
    function _reportFinalized() private view {
        // Every deal is an order-book match — `_recordFunding` is reachable only from
        // `fundFromOrderBook` — so being funded IS being funded from the book, and the separate
        // latch that used to gate this was unreachable. What remains to check is that there is
        // volume to report and a book to report it to.
        if (_ticksFinalized == 0 || _iobHash == 0) { return; }
        address orderBook = DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash);
        // 0.01, LIKE EVERY OTHER OUTGOING CALL THIS CONTRACT MAKES. The 1 vmshell that stood here was
        // a hundred times the rest of them together and nothing derived it. Measured floors, on a
        // local chain: this entry ran out of gas below 0.02 while the book still did its own
        // `ensureBalance()` before accepting. Moving that accept ahead is what makes 0.01 enough,
        // so the two changes belong together — this figure alone would break the call.
        InferenceOrderBook(orderBook).reportFinalized{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(
            _sellerPubkey, _nonce, _pricePerTick, _ticksFinalized);
    }

    // ========================================================
    // 1. Fund — buyer escrows SHELL, locks the deal
    // ========================================================

    /// @notice Record the deal shape handed down by the book at match time. An ordinary deal is
    ///         the degenerate case: zero weeks, and the whole funded volume as the single "quota",
    ///         so every downstream branch reads the same two fields instead of a separate type.
    function _recordDealShape(uint8 dealFlags, uint128 ticks) private {
        // The term is not carried by the order: a subscription is ALWAYS one month, so the flag
        // alone determines the shape. `_subWeeks == 0` is the ordinary deal — the whole volume as
        // a single "quota" and no weekly clock.
        uint8 subWeeks = (dealFlags & FLAG_SUBSCRIPTION) != 0 ? SUB_WEEKS : 0;
        _dealFlags     = dealFlags;
        _subWeeks      = subWeeks;
        _weekIndex     = 0;
        _periodStart   = uint64(block.timestamp);
        uint128 tokens = ticks * TICK_SIZE;
        _tokensPerWeek = subWeeks == 0 ? tokens : tokens / uint128(subWeeks);
        _fundedTokens  = tokens;
        _lastClaimTime = uint64(block.timestamp);
    }

    function _recordFunding(address buyer, uint256 buyerPubkey, uint128 paid) private {
        // Buyer-side, by-fact fee (§5.1): the escrow covers per-tick (P + fee).
        // Must cover >= 2 full ticks (probe + at least one streaming tick) and <= maxTicks.
        uint128 unit = _pricePerTick + _fee(_pricePerTick);
        require(paid >= 2 * unit, ERR_INSUFFICIENT_DEPOSIT);
        require(uint256(paid) <= uint256(_maxTicks) * uint256(unit), ERR_OVERFLOW);
        _buyer       = buyer;
        _buyerPubkey = buyerPubkey;
        _deposit     = paid;
        _funded      = true;
        // The match-fill consumed the offer — the book removed it before this callback — so no
        // live offer remains. Clear the latch, so `destroy`/`withdrawShell` judge by the REAL offer
        // state rather than by `_funded` as a proxy. It also blocks a re-list on a filled deal.
        _offerPosted = false;
        _fundedTime  = uint64(block.timestamp);
        emit StreamFunded{dest: address.makeAddrExtern(StreamFundedEmit, bitCntAddress)}(buyer, paid);
    }

    // The direct off-book path (`fund()` + `authorizeDirectFund`) is gone. Every deal is now born
    // from a book match, so the book is the single place where price, volume, deal shape and the
    // AON/subscription pairing are validated — a second funding door would bypass all of it.

    /// @notice Order-book handover (spec §2.3): the InferenceOrderBook forwards
    ///         the matched SHELL, binds the buyer note (not msg.sender), and
    ///         records the buyer note pubkey it held in the book — the buyer's
    ///         PrivateNote forwards its `_ephemeralPubkey` when ordering, the OB
    ///         threads it through the match (§3.1.1, gateway auth).
    /// @param dealFlags DEAL slice of the buyer's order flags (`DEAL_FLAGS_MASK`: TEE |
    ///        SUBSCRIPTION). Execution bits never leave the book; these describe what was bought.
    ///        The subscription term is fixed at one month, so nothing about duration is passed
    ///        here; the book guarantees a subscription's volume divides evenly into its weeks, so
    ///        the weekly quota derived below is exact.
    /// @param paid The matched SHELL, as a FIGURE (generation 4.0.33). The book subtracted it from
    ///        its own record before calling; this credits the same figure here. Because it is now
    ///        an argument rather than currency riding on the message, the sender check below is the
    ///        ONLY thing standing between this deal and a fabricated balance — see the guard.
    ///        It leads the parameter list so the BOOK can recover it if this call bounces (its
    ///        `onBounce` reads the leading 256 bits); reordering it would strand the figure.
    /// @dev   Every path out of here answers the book — credit or refund, this call confirms with
    ///        `onHandoverAccepted` so the book can drop the pending record it wrote before sending.
    ///        The one path that does NOT confirm is the revert below, and that is the point: a
    ///        revert bounces, and the book restores from the record instead.
    function fundFromOrderBook(uint128 paid, address buyerNote, uint256 buyerPubkey, uint8 dealFlags) public {
        if (paid == 0) { return; }
        // Authenticate the caller as the canonical InferenceOrderBook for this model (hash
        // delivered by RootPN) BEFORE accepting, so this contract pays only for messages a real
        // match produced — and, since 4.0.33, so a figure only enters this deal from a contract
        // whose address derives from code the root vouched for. An arbitrary contract calling this
        // with any `paid` it likes is turned away here and credits nothing.
        //
        // `_iobHash == 0` means the RootPN reply has not arrived, so the canonical book cannot be
        // derived and the caller cannot be authenticated either way. That case REVERTS rather than
        // returning: the real book has already taken the figure off its own record, and a bounce is
        // the only thing that puts it back. Returning quietly would destroy it.
        require(_iobHash != 0, ERR_INVALID_SENDER);
        // A caller that fails the derivation subtracted nothing anywhere — there is no figure to
        // return — so this stays a silent return, and stays cheap for the spammer to be refused.
        if (msg.sender != DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash)) {
            return;
        }
        tvm.accept();
        ensureBalance();
        // NOT CHARGED, and it must stay that way. This is the BOOK telling the deal what
        // happened to its offer, not a party doing something: nobody here chose to call, and
        // the book attaches no ECC anywhere. A charge would make clearing `_offerPosted` depend
        // on the reserve, and `burnecc` fails in the ACTION phase — the whole transaction
        // reverts, the latch stays set, and the book has already removed the ask. Reordering
        // the statements cannot help: a revert takes the clearing with it.
        // Credited BEFORE the non-fundable branch below, which refunds through `_payShell` and
        // therefore subtracts it again. Both halves go through the balance, so the pair with the
        // book conserves whichever way this call ends.
        _balance += paid;
        // Answer the book now, not per branch: past this point the figure is on `_balance` and this
        // call will not revert, so the handover HAS been accepted whether the deal keeps the money
        // or refunds it below. Confirming here rather than in each branch is what keeps the two
        // outcomes from disagreeing about whether the book still has a claim outstanding.
        IOrderBookHandover(msg.sender).onHandoverAccepted{
            value: DAPP_MSG_VALUE, flag: 1, bounce: false
        }();
        // A subscription arrives with the buyer's `2P` riding on top of the escrow — the book
        // forwards deposit + bond as one transfer and this is where the two part company. It never
        // counts as volume: the ticks below are derived from what is left after the bond is set
        // aside, so a subscription buys exactly the volume it ordered.
        uint128 bond = (dealFlags & FLAG_SUBSCRIPTION) != 0 ? _bondAmount() : 0;
        // The book forwards bounce:false, so this path never reverts: on any non-fundable fill —
        // already funded (nonce reuse), under 2 ticks, over maxTicks, or a subscription that did
        // not carry its bond — it accepts and refunds the buyer note IN FULL rather than reverting,
        // so the buyer's SHELL is always returned.
        uint128 unit = _pricePerTick + _fee(_pricePerTick);
        if (_funded || paid < bond || paid - bond < 2 * unit
            || uint256(paid - bond) > uint256(_maxTicks) * uint256(unit)) {
            _payShell(buyerNote, paid);
            return;
        }
        _buyerBond = bond;
        // ONE FLAG, TWO SOURCES. `open()` asks a single question — is the buyer's bond in? — and
        // the two deal types answer it from different places. A SUBSCRIPTION's bond arrives inside
        // this very transfer, carved out above, so it is already in by the time this call returns
        // and there is no second message to wait for. An ORDINARY deal's arrives later, from the
        // buyer's note, and sets the same flag in `fundBuyerBond`.
        //
        // Written here rather than as a second condition in `open()` on purpose: a check that says
        // "either the buyer's bond is funded OR this is a subscription" would pass a subscription
        // whose carve-out silently produced nothing, which is the case worth catching.
        if (bond > 0) { _buyerBondFunded = true; }
        _recordFunding(buyerNote, buyerPubkey, paid - bond);
        // The funded volume in ticks: the escrow is fee-inclusive, so divide by the same unit the
        // bound above used. Exact by construction — the book funds whole ticks.
        _recordDealShape(dealFlags, (paid - bond) / unit);
    }

    /// @notice Note-driven, single-call sell-offer post (spec §2.3). The seller's
    ///         canonical PrivateNote calls this after deriving THIS TC locally; it
    ///         delivers the canonical InferenceOrderBook code hash/depth (baked into
    ///         the note by RootPN at deploy), so the TC needs no RootPN round-trip.
    ///         Auth is dual: `msg.sender == _sellerNote` AND `_sellerNote` is the
    ///         canonical PrivateNote for the supplied `depositIdentifierHash` (pinned
    ///         PrivateNote code). Only the canonical RootPN can deploy a canonical-code
    ///         note, and it always bakes its real book code, so a passing caller's
    ///         `iobHash` is authoritative and always names the canonical book. Stores it (enabling
    ///         `fundFromOrderBook`) and, on the first post, places the resting ask
    ///         itself (`msg.sender == TC` at the book). Re-run after a cancel re-posts.
    /// @param iobHash Canonical InferenceOrderBook code hash the note holds.
    /// @param iobDepth Canonical InferenceOrderBook code depth the note holds.
    /// @param depositIdentifierHash The calling note's deposit-identifier static.
    /// @param flags Order flags (POST_ONLY / IOC / FOK / MARKET) forwarded to the OB.
    /// @param deadline ABSOLUTE expiry (unix seconds) the seller note already anchored and capped
    ///        (`PrivateNote.postSellOffer`, `ttl <= MAX_SELL_TTL`). The note is the ONLY path that
    ///        reaches the book — this is the single place a TC posts its ask, and there is no
    ///        direct seller-key post path — so the deadline arrives pre-validated and is forwarded
    ///        as-is: the book neither re-checks the 1h bound (its clock has moved on) nor re-anchors.
    ///
    ///        A note-confirmed, hash-activated TC posts (`msg.sender == TC` at the book), so an
    ///        offer on the book always implies a live, seller-owned TC and a match always forwards
    ///        the buyer's SHELL to a real deal contract. At most one LIVE offer per TC: a cancel
    ///        frees the latch (`onSellClosed`) so the seller can re-list on the SAME live TC; a
    ///        fill locks the TC to its buyer (one-shot `_funded`).
    function postFromNote(uint256 iobHash, uint16 iobDepth, uint256 depositIdentifierHash, uint8 flags, uint64 deadline) public {
        require(msg.sender == _sellerNote, ERR_INVALID_SENDER);
        require(_sellerNote == DexLib.computeCanonicalNoteAddressFromHash(
            PRIVATE_NOTE_CODE_HASH, PRIVATE_NOTE_CODE_DEPTH, depositIdentifierHash), ERR_BAD_PARAM);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_POST_FROM_NOTE);
        _noteAuthorized = true;
        _iobHash  = iobHash;
        _iobDepth = iobDepth;
        if (_offerPosted || _funded) { return; }  // already resting / one-shot funded → no re-post
        _offerPosted = true;
        address orderBook = DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash);
        // 0.01. This one measured a floor of 0.01 even before the book's accept moved, so the margin
        // here comes from the change rather than from the figure.
        InferenceOrderBook(orderBook).placeSellOffer{value: DAPP_MSG_VALUE, flag: 1, bounce: false}(
            _pricePerTick, _maxTicks, flags, _sellerPubkey, _nonce, _sellerNote, deadline);
    }

    /// @notice The canonical InferenceOrderBook calls this when this TC's resting
    ///         sell offer is removed WITHOUT a fill (the seller cancelled it).
    ///         Clears the `_offerPosted` latch so the seller can post a fresh offer
    ///         on the SAME live TC without redeploying — a cancel does not destroy
    ///         the TC. Only clears while unfunded: a filled TC is a committed
    ///         one-shot deal (`_funded` latch), never re-offered. Guarded to the
    ///         canonical book (derived from the RootPN-supplied `_iobHash`); the
    ///         book forwards bounce:false, so a stale/foreign caller is ignored
    ///         (accept-then-noop, mirroring `fundFromOrderBook`), never reverted.
    function onSellClosed() public {
        if (_iobHash == 0
            || msg.sender != DexLib.computeInferenceOrderBookAddressFromHash(_iobHash, _iobDepth, _modelHash)) {
            return;
        }
        tvm.accept();
        ensureBalance();
        // NOT CHARGED — same reason as `fundFromOrderBook`, and worse here: the book sends this
        // with `bounce: false`, so a reverted transaction is never retried and the latch would
        // stay set for good, leaving the deal unable to re-list, close or be destroyed.
        if (_funded) { return; }        // a fill won the race → deal is live, keep the TC
        // Offer is off the book → re-list-able. AND THAT IS ALL THIS DOES NOW.
        //
        // It used to also destroy the deal, when `close()` had flagged wind-down beforehand: the
        // cancel landing here was the second half of a two-step close. That step is gone with the
        // flag — `close()` now refuses while an offer rests, so the seller cancels FIRST and calls
        // `close()` afterwards, and the destruct happens where he asked for it rather than as a
        // side effect of a callback from the book.
        //
        // The deal therefore SURVIVES a cancel, which is the same thing it already did for every
        // cancel that was not a wind-down. One behaviour instead of two, decided by the seller's
        // own call instead of by a flag he set earlier and could not see.
        _offerPosted = false;
    }

    /// @notice Return the posted seller bond to the note that posted it. The bond is collateral,
    ///         not contract balance, so any path that destroys the deal hands it back before
    ///         `selfdestruct` takes what is left. Every wind-down needs this, not only the funded
    ///         ones: `fundDeal` accepts the bond from the moment the deal exists, since `2P`
    ///         is known from the constructor and bonding before offering is the stronger order —
    ///         so a deal that never matched can still be holding one.
    function _returnBond() private {
        uint128 bondBack = _sellerBond;
        if (bondBack == 0) { return; }
        _sellerBond = 0; _sellerBondFunded = false;
        _payShell(_sellerNote, bondBack);
    }

    /// @notice Seller wind-down of an UNFUNDED deal (no buyer yet). REFUSED while a sell offer is
    ///         still resting: cancel the offer first, then close. A FUNDED deal has the buyer's
    ///         SHELL inside and must close via `stop`/`withdrawShell`.
    ///
    ///         It used to accept in that state and merely flag intent, leaving the note to cancel
    ///         the offer and the resulting `onSellClosed` to self-destruct. Nothing obliged the
    ///         second step, and nothing reported that it was owed: the call SUCCEEDED, the ask
    ///         stayed in the book and stayed EXECUTABLE, and a seller who believed he had wound
    ///         down could still be handed a live deal. The two-step was the design; the silence
    ///         about step two was the defect.
    ///
    ///         Refusing costs the seller nothing he was not already going to spend — he had to
    ///         cancel the offer either way — and it makes this door agree with `destroy()`, which
    ///         has always refused on the same state with this same error. Two wind-down entries
    ///         behaving oppositely on one condition is what produced the false success.
    ///
    ///         What it does NOT do is prevent a fill: the ask is live until the cancel lands,
    ///         whichever order the seller works in. This only stops him from being told otherwise.
    /// @dev THE PAYEE IS NOT AN ARGUMENT. It is `_sellerNote`, a static of this deal, and that is
    ///      the whole point: an address the caller names is an address nobody validated, and it was
    ///      the reason this contract carried a bounce handler, a `purpose` earmark and a stored
    ///      `_closePayout`. All three are gone with it. Model: `resolveDisputeTimeout`.
    function close() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        require(!_funded, ERR_ALREADY_FUNDED);   // unfunded ⟹ not opened, not disputed
        // Same condition and same code as `destroy()`. `_offerPosted` tracks the REAL offer state:
        // it is cleared by `onSellClosed`, which the book sends on every way an ask leaves it —
        // cancel, expiry, and rejection before resting — so this refusal cannot outlast the offer.
        require(!_offerPosted, ERR_OFFER_LIVE);
        // Return the bond through the canonical path before the sweep. A bond CAN exist here: it
        // is postable from the moment the deal is constructed, since `2P` is known from the
        // constructor and bonding before offering is the stronger order. Leaving it to
        // `selfdestruct` would send it elsewhere than `_sellerNote`, and a sweep is not a payment:
        // it carries no figure, so the note would have nothing to credit the bond against.
        _returnBond();
        // Anything still on `_balance` goes the same way as everything else here.
        if (_balance > 0) { _payShell(_sellerNote, _balance); }
        // Residual native gas unchanged; only the money needed a line of its own.
        _die(_sellerNote);
    }

    // ========================================================
    // 1b. Seller bond — seller posts the mirror collateral 2P (spec §4.2)
    // ========================================================

    /// @notice THE way money enters a deal from the seller side (generation 4.0.33). One call,
    ///         two effects: the ECC[2] attached to it became this contract's gas on arrival
    ///         (flag 17, and a deal has no other source of gas at all), and `amount` is the
    ///         figure credited to `_balance`.
    ///
    ///         The figure is believed BECAUSE OF THE SENDER CHECK BELOW, and for no other reason.
    ///         `amount` is just a number on a message; what makes it real is that it came from
    ///         `_sellerNote` — an address this deal already proved is the canonical PrivateNote for
    ///         this seller — and that the note subtracted the same number from its own record
    ///         before sending. A contract that is not that note cannot make this credit happen, so
    ///         a balance cannot be conjured by calling here; and because the note's subtraction and
    ///         this addition are the two halves of one message, the pair conserves.
    ///
    ///         What arrives is the mirror bond (`2P`, §4.2): held until close, returned to the note
    ///         on any clean exit / concession / no-show, burned mark-for-mark against the buyer's
    ///         burned `D` on a dispute that reaches timeout with no concession. Nothing about the
    ///         money says "bond" — the figure is a figure. It is this function's checks that make
    ///         it one, which is why there is one funding door and not one per purpose.
    /// @param amount The bond, as a figure, already subtracted from the note's
    ///        `_balance[CURRENCIES_ID_SHELL]`.
    /// @param endpointCipher OPTIONAL, and it does exactly one thing: writes the endpoint and says
    ///        so. It does NOT open the deal. The seller's note is the only sender here, so the
    ///        endpoint can travel with the bond and the seller never needs an operational wallet to
    ///        put it on chain — but starting the stream stays `open()`'s job alone, because opening
    ///        also freezes the probe tick out of an escrow that does not exist until the match.
    function fundDeal(uint128 amount, optional(bytes) endpointCipher) public {
        require(msg.sender == _sellerNote, ERR_INVALID_SENDER);
        require(!_opened, ERR_ALREADY_OPEN);
        require(!_sellerBondFunded, ERR_BOND_ALREADY_FUNDED);
        require(amount > 0, ERR_NO_SHELL);
        _balance += amount;
        // Postable BEFORE the match. `_pricePerTick` is a constructor argument, so `2P` is known
        // from the moment this deal exists — there is nothing about a match the bond has to wait
        // for. Allowing it early is the stronger order: the seller bonds, then offers, and a fill
        // can never land on a deal that has no collateral behind it. It is also what the
        // note-funded flow needs, where the seller prepares the whole deal from the note without
        // an operational wallet.
        //
        // A bond posted on a deal that never matches is not stranded: `close` winds an unfunded
        // deal down (deferring to `onSellClosed` when an offer still rests), and `destroy` covers
        // the same ground once nothing rests. Undersized bonds still revert, and the note sends
        // this call with bounce:true, so the SHELL returns rather than settling here unaccounted.
        uint128 need = _bondAmount();
        require(amount >= need, ERR_INSUFFICIENT_DEPOSIT);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_FUND_DEAL);

        _sellerBond = need;
        _sellerBondFunded = true;

        // Refund any excess SHELL above the required bond to the sender.
        uint128 excess = amount - need;
        if (excess > 0) { _payShell(msg.sender, excess); }

        emit SellerBondFunded{dest: address.makeAddrExtern(SellerBondFundedEmit, bitCntAddress)}(need);

        // Written AFTER the bond is on the books, so the two never disagree about what happened:
        // the bond is the reason this call exists and the endpoint rides along with it.
        if (endpointCipher.hasValue()) {
            _endpointCipher = endpointCipher.get();
            emit EndpointSet{dest: address.makeAddrExtern(EndpointSetEmit, bitCntAddress)}(_endpointCipher);
        }
    }

    /// @notice The BUYER's mirror of `fundDeal`: his note posts his own `2P` bond. Symmetric with
    ///         the seller's by construction — same size, same "excess comes back", same one-shot
    ///         latch — and the order the two arrive in does not matter, because `open()` waits for
    ///         both rather than for a sequence.
    ///
    /// @dev    WHY THE BUYER CANNOT SEND IT WITH THE ESCROW. The bond is `2 * clearing`, and the
    ///         clearing price is not known until the match: the buyer states a LIMIT, the book
    ///         fills at or below it, and only then is there a figure to take twice. The seller has
    ///         no such problem — he names his own price, so his bond is sizeable from the moment
    ///         the deal exists. Hence one bond before the match and one after it, which looks
    ///         asymmetric and is the only arrangement that can be symmetric in the amount.
    ///
    ///         The native value attached under flag 17 is NOT this figure and is not returned by
    ///         it: it becomes the deal's own gas. A deal is SELF-ROOTED — native does not cross
    ///         into its dapp, so it pays for every transaction out of its own balance, and until
    ///         now that balance came only from the seller. The buyer was having his calls paid for
    ///         by the counterparty. The money that matters here travels as `amount`, a figure on
    ///         the call, exactly as it does in `fundDeal`.
    function fundBuyerBond(uint128 amount) public {
        // The buyer of record, not "whoever claims to be": `_buyer` is written by
        // `fundFromOrderBook` from what the book handed over, and nothing else writes it.
        require(msg.sender == _buyer, ERR_INVALID_SENDER);
        require(!_opened, ERR_ALREADY_OPEN);
        require(!_buyerBondFunded, ERR_BOND_ALREADY_FUNDED);
        require(amount > 0, ERR_NO_SHELL);
        _balance += amount;
        uint128 need = _bondAmount();
        require(amount >= need, ERR_INSUFFICIENT_DEPOSIT);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_FUND_DEAL);

        _buyerBond = need;
        _buyerBondFunded = true;

        // Excess back to the sender, as on the seller's side. The note sends `2 * clearing` and the
        // deal computes `2 * _pricePerTick` from its own state; the two agree, but the deal trusts
        // its own figure and returns the difference rather than keeping whatever arrived.
        uint128 excess = amount - need;
        if (excess > 0) { _payShell(msg.sender, excess); }

        emit BuyerBondFunded{dest: address.makeAddrExtern(BuyerBondFundedEmit, bitCntAddress)}(need);
    }

    // ========================================================
    // 2. Open — seller posts encrypted endpoint, freezes the probe tick
    // ========================================================

    /// @notice Seller-only. Posts the endpoint ciphertext (encrypted to the buyer's pubkey) and
    ///         freezes ONE tick as the probe (spec §3.1.2: not prepaid to the seller). The buyer's
    ///         note is NOT locked — the seller's risk is the per-deal mirror bond (§4.2), see the
    ///         note below. No platform fee yet either: it is taken by-fact when the probe is
    ///         accepted (§5.1).
    function open(bytes endpointCipher) public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_OPEN);
        require(_funded, ERR_NOT_FUNDED);
        require(!_opened, ERR_ALREADY_OPEN);
        // BOTH bonds, and the order they arrived in does not matter. The seller's can be posted
        // from the moment the deal exists; the buyer's cannot exist before the match priced it. So
        // this waits for a pair, not for a sequence.
        require(_sellerBondFunded, ERR_BOND_NOT_FUNDED);
        require(_buyerBondFunded, ERR_BOND_NOT_FUNDED);
        require(_deposit >= _pricePerTick, ERR_INSUFFICIENT_DEPOSIT);

        // AN EMPTY ARGUMENT NO LONGER WIPES WHAT IS THERE. Until `fundDeal` learned to carry the
        // endpoint, this line was the only writer and an empty `open` merely left the field as
        // empty as it already was — nothing could be lost because nothing could have arrived
        // first. Now something can: a seller who sent the endpoint with his bond and then opened
        // with an empty argument would erase it here, start the probe clock, and leave the buyer
        // with nothing to connect to and a tick of his own bond to lose over it. Assigning only a
        // non-empty value refuses no call that works today; it just stops one from destroying the
        // value the other path put there.
        if (!endpointCipher.empty()) {
            _endpointCipher = endpointCipher;
        }
        // Announced from BOTH places that write the field, not only from the new one. A buyer who
        // learned to listen for this would otherwise hear it on a deal whose seller sent the
        // endpoint with the bond and hear nothing on a deal whose seller sent it at the open —
        // the same fact, reported or not depending on which call the seller happened to use.
        emit EndpointSet{dest: address.makeAddrExtern(EndpointSetEmit, bitCntAddress)}(_endpointCipher);

        // Freeze the probe tick out of the escrow: held by the contract, owed to nobody yet.
        _probeTick     = _pricePerTick;
        _deposit      -= _pricePerTick;
        _probeAccepted = false;
        _probeTime     = uint64(block.timestamp);

        // Claim anchors start at the open; they only begin to matter once the probe is accepted.
        _lastClaimTime = uint64(block.timestamp);
        _opened        = true;
        _everOpened    = true;   // permanent latch: scopes cleanupUnopened to the never-opened case

        // No note lock (§4.2): the buyer's at-risk SHELL is the deposit held in THIS TC,
        // not the note, so nothing on the note needs freezing while the stream runs.

        emit StreamOpened{dest: address.makeAddrExtern(StreamOpenedEmit, bitCntAddress)}(_buyer, _pricePerTick);
    }

    // ========================================================
    // 3. Consumption claims (seller-driven, buyer-contestable)
    // ========================================================

    /// @notice Seller-only. After PROBE_WINDOW of buyer silence the trial tick is his: silence on
    ///         a live endpoint is consent (§3.1.2). The tick is credited, its fee taken by-fact,
    ///         and only now does the deal become claimable. The seller bond STAYS locked — from
    ///         here on it mirrors the claim pipeline, not the probe.
    function acceptProbe() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_ACCEPT_PROBE);
        require(_opened, ERR_NOT_OPEN);
        require(!_disputed, ERR_DISPUTED);
        require(!_probeAccepted, ERR_ALREADY_REGISTERED);
        require(uint64(block.timestamp) >= _probeTime + PROBE_WINDOW, ERR_SETTLE_WINDOW_OPEN);

        uint128 fee = _fee(_pricePerTick);
        if (fee > _deposit) { fee = _deposit; }
        _finalizedOwed  += _probeTick;
        _feeAccrued     += fee;
        _deposit        -= fee;
        _probeTick       = 0;
        _probeAccepted   = true;
        // The subscription clock starts HERE, not at the match. A week is a week of served
        // capacity, and until the trial tick is accepted the seller has served nothing — billing
        // from the fill would let the weekly quota accrue against a deal that never proved itself.
        _periodStart     = uint64(block.timestamp);

        // ONE frame of reference for the whole deal: a claim is the CUMULATIVE consumption since
        // the stream opened, and the probe is its FIRST tick — not something claimed on top of.
        // A seller who has served two ticks claims two, probe included; `_claimCap` and the
        // exhaustion check in `finalize` measure against the same total. What the probe changes is
        // only that its tick is already PAID for, which is what `_tokensPaid` records here, so the
        // next settlement charges the buyer for consumption beyond it and not for it twice.
        _tokensPaid      = TICK_SIZE;
        // Seed the pipeline with the probe as its first, already-trusted claim. Leaving it at zero
        // while `_tokensPaid` stands at a tick would force the seller's next claim to re-state the
        // probe before it could state anything new: a claim for two ticks would read as a delta of
        // two and be refused, so he would have to spend a whole interval claiming a tick that was
        // already paid for.
        _tokensFinal     = TICK_SIZE;
        _tokensPend      = TICK_SIZE;
        _weekBaseTokens  = 0;            // week one counts the probe against its own quota
        _ticksFinalized  = 1;
        _lastClaimTime   = uint64(block.timestamp);

        emit ProbeAccepted{dest: address.makeAddrExtern(ProbeAcceptedEmit, bitCntAddress)}(
            _buyer, _pricePerTick, 0);
    }

    /// @notice Ceiling on the cumulative claim right now. An ordinary deal exposes the whole funded
    ///         volume from the start. A subscription exposes ONE weekly quota above what the
    ///         previous weeks already consumed: the volume is a per-week allowance, it does not
    ///         roll forward, and what is not drawn is forfeited at the boundary (paid for
    ///         regardless — take-or-pay). Exact at the end: the book guarantees the volume divides
    ///         evenly by `_subWeeks`, so the final cap is `_fundedTokens`.
    ///
    /// @dev    KNOWN COUPLING: the ceiling moves when a week is BOOKED, not when the clock passes
    ///         a boundary — `_weekBaseTokens` is advanced by `_chargeWeeksThrough`, which runs from
    ///         `settleWeek` and from the exits. There is no way around it without giving up the
    ///         no-roll-forward rule: pricing the next quota needs to know what consumption stood at
    ///         when the week turned, and nothing records that until the boundary is settled.
    ///         Consequence for a seller: after a boundary, claiming into the new quota waits for
    ///         someone to call the permissionless `settleWeek`. No money is at stake — the exits
    ///         charge every unsettled week regardless — but a client that assumes the ceiling
    ///         follows the clock will sit on week one's quota for the whole term.
    function _claimCap() private view returns (uint128) {
        if (!_isSubscription()) { return _fundedTokens; }
        // Past the final boundary the term sells no further capacity, so the ceiling stops at what
        // has already been stated and the deal admits no new claim. Without this the quota formula
        // below keeps offering `_weekBaseTokens + _tokensPerWeek` after `_weekIndex` has reached
        // `_subWeeks`, which is capacity for a week the buyer never bought: the fifth week of a
        // four-week term. `_fundedTokens` bounds the total, so nothing beyond the paid escrow was
        // ever reachable, but every admitted claim rewrites `_lastClaimTime`, and `settleWeek`
        // closes only once `_lastClaimTime + CLAIM_PROMOTE_WINDOW` has passed — so post-term claims
        // defer the close, hold `_opened`, keep the buyer's bond posted, and each one raises
        // `_ticksFinalized`, which is what `_reportFinalized` publishes to the book as authoritative
        // served volume. Stating the ceiling as the recorded cumulative ends all four at the source.
        //
        // Deliberately `_tokensPend` and not zero: a repeat of the same figure must stay the no-op
        // it already is (`claimTokens` returns early on `cumulativeTokens == _tokensPend`, before
        // `_lastClaimTime` is touched), while anything HIGHER is refused. A zero ceiling would turn
        // that harmless repeat into a revert and change a settled behaviour this fix has no business
        // touching.
        if (_weekIndex >= _subWeeks) { return _tokensPend; }
        // One quota per week, measured from what the PREVIOUS weeks already consumed rather than
        // from the start of the term. A cumulative ceiling would let an unused week roll forward
        // and be spent on top of the next one's allowance; the quota is reserved capacity for its
        // own week, and what is not drawn in it is forfeited — paid for regardless, take-or-pay.
        uint128 cap = _weekBaseTokens + _tokensPerWeek;
        return cap > _fundedTokens ? _fundedTokens : cap;
    }

    /// @notice Seller claims the CUMULATIVE consumption of this deal, in ticks.
    ///
    ///         Two accumulators, one promotion rule: a claim becomes trusted by outliving its own
    ///         CLAIM_PROMOTE_WINDOW without a complaint, and by nothing else. A claim landing here
    ///         promotes the previous one only in the sense that it cannot arrive until that window
    ///         has already closed — the arrival is not what earns the trust, the silence is.
    ///
    ///         A seller who stops claiming therefore does NOT freeze his last figure in the
    ///         contested state: it matures on its own clock, and `finalize` lets anyone make that
    ///         official. This is what makes the LAST claim of a deal payable, since no successor
    ///         is ever coming for it.
    ///
    ///         Three independent bounds, each answering a different question:
    ///           - `MIN_CLAIM_INTERVAL` (60 s) — HOW OFTEN a claim may be made;
    ///           - `MAX_CLAIM_DELTA` (one tick) — HOW MUCH one claim may add, whatever the silence
    ///             before it. This is the bound the seller bond mirrors: the contested value is one
    ///             claim's delta, so capping the delta caps the dispute;
    ///           - `MIN_SECONDS_PER_TICK` — the physical rate: no model produces a tick in under a
    ///             minute, so no claim may assert one in less.
    ///         At the physical ceiling the three agree exactly — a tick a minute, one tick a claim.
    function claimTokens(uint128 cumulativeTokens) public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_CLAIM);
        require(_opened, ERR_NOT_OPEN);
        require(!_disputed, ERR_DISPUTED);
        // Nothing is claimable until the trial tick has been accepted: the buyer's first minutes
        // buy him a look at the service, not an obligation.
        require(_probeAccepted, ERR_NOT_OPEN);
        require(cumulativeTokens >= _tokensPend, ERR_BAD_PARAM);      // cumulative, never decreasing
        // Re-sending the figure already on record changes nothing and must LEAVE nothing changed —
        // decided HERE, before the weekly books are touched below. A retry that happens to land
        // after a boundary would otherwise settle a week on its way to doing nothing.
        if (cumulativeTokens == _tokensPend) { return; }
        // Bring the weekly books up to the CLOCK before measuring the ceiling against them.
        // `_claimCap` reads `_weekBaseTokens`, and that snapshot only moves when weeks are charged
        // — which `settleWeek` does permissionlessly but not automatically. Between a boundary and
        // whenever someone gets round to calling it, the ceiling would still be the previous
        // week's, so the allowance a week did not draw could be spent during the next one; the
        // snapshot taken afterwards would then sit on that raised figure and open a fresh full
        // quota on top of it. Charging here settles nothing new — it is the same call `settleWeek`
        // would make, just at the moment the answer is actually needed.
        _settleBoundaries(_weeksElapsed());
        require(cumulativeTokens <= _claimCap(), ERR_BAD_PARAM);       // never beyond what is available yet

        uint64 elapsed = uint64(block.timestamp) - _lastClaimTime;
        require(elapsed >= MIN_CLAIM_INTERVAL, ERR_SETTLE_WINDOW_OPEN);
        uint128 delta = cumulativeTokens - _tokensPend;
        require(delta <= MAX_CLAIM_DELTA, ERR_BAD_PARAM);
        // Physical ceiling: TICK_SIZE tokens need at least MIN_SECONDS_PER_TICK seconds.
        require(uint256(delta) * uint256(MIN_SECONDS_PER_TICK) <= uint256(elapsed) * uint256(TICK_SIZE), ERR_BAD_PARAM);

        // Promote what has served its window, then take the new claim. ONE pending slot is enough
        // because `MIN_CLAIM_INTERVAL` (required above) and `CLAIM_PROMOTE_WINDOW` (checked inside
        // `_promoteDue`) are the same number, and both comparisons are `>=` against the same
        // `_lastClaimTime` — so they are literally the same condition. A claim entitled to arrive
        // at all arrives no earlier than the moment the previous one matured, and the promotion on
        // the next line therefore always frees the slot this one is about to take.
        //
        // That is a relationship between two constants, and constants get retuned. Rather than
        // leave it as an assumption in a comment, check it: after promoting, the pending slot must
        // hold exactly what is now trusted, which is what "the slot was released" means. If the
        // window were ever raised above the interval, this refuses the claim instead of
        // overwriting one that has neither been trusted nor contested — the overwrite would be
        // silent, and it would erase the buyer's unexpired right to contest it.
        _promoteDue();
        require(_tokensPend == _tokensFinal, ERR_SETTLE_WINDOW_OPEN);
        _tokensPend    = cumulativeTokens;
        _lastClaimTime = uint64(block.timestamp);
        emit TicksClaimed{dest: address.makeAddrExtern(TicksClaimedEmit, bitCntAddress)}(_tokensFinal, _tokensPend);
    }

    /// @notice The pending claim becomes final — nobody contested it, since an open dispute blocks
    ///         every path that calls this.
    /// @dev    `_lastClaimTime` is deliberately LEFT ALONE. It dates the claim now held in
    ///         `_tokensPend`, and after promotion that claim is still the one the slot holds — it
    ///         has merely also become trusted. Moving the clock here would re-open a window on a
    ///         figure nobody may contest any more, and `claimTokens` reads the same field to
    ///         enforce the minimum interval, so a nudge would hand the seller a free early claim.
    function _promote() private {
        if (_tokensPend > _tokensFinal) { _tokensFinal = _tokensPend; }
    }

    /// @notice Promote the pending claim if it has served its CLAIM_PROMOTE_WINDOW without a
    ///         complaint, and nothing else. It is judged against ITS OWN landing time, so the buyer
    ///         gets the full window on every claim regardless of how fast the seller claims — the
    ///         guarantee is a property of the claim, not of the gap between claims.
    ///
    ///         Every terminal path promotes through here, so closing a deal never advances the
    ///         pipeline further than simply waiting would have.
    function _promoteDue() private {
        if (block.timestamp >= _lastClaimTime + CLAIM_PROMOTE_WINDOW) { _promote(); }
    }

    /// @notice Permissionless. Promotes the pending claims once `CLAIM_PROMOTE_WINDOW` has passed
    ///         with no dispute. This is what makes the LAST claim of a deal payable at all: nothing
    ///         supersedes it, so without a window it would stay pending forever. Also settles and
    ///         closes when the funded volume is exhausted.
    function finalize() public {
        require(_opened && !_disputed, ERR_NOT_OPEN);
        // Permissionless and pays its own way from the contract balance, so it must move the
        // trusted figure: there has to be a claim that is both unpromoted AND past its window.
        // Both halves are required — a call that only satisfies one of them would burn the
        // contract's gas to change nothing, which anybody could repeat at the deal's expense.
        require(_tokensPend > _tokensFinal
                && block.timestamp >= _lastClaimTime + CLAIM_PROMOTE_WINDOW, ERR_SETTLE_WINDOW_OPEN);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_SETTLE);
        _promoteDue();
        if (!_isSubscription() && _tokensFinal >= _fundedTokens) { _payFinalAndClose(); }
    }

    /// @notice Value of `n` tokens and its by-fact platform fee. Computed from the CUMULATIVE
    ///         total by the callers, never per-delta, so integer division never loses a remainder.
    function _valueOf(uint128 n) private view returns (uint128 pay, uint128 fee) {
        pay = uint128(uint256(n) * uint256(_pricePerTick) / uint256(TICK_SIZE));
        fee = _fee(pay);
    }

    // ========================================================
    // 3b. Weekly settlement (subscription only, take-or-pay)
    // ========================================================

    /// @notice Weeks of the term fully ELAPSED at `ts` — the boundaries a settlement may cross.
    /// @dev Takes the instant explicitly because a dispute is settled as of the moment it was
    ///      RAISED, not the moment someone got around to resolving it. Resolution arrives at least
    ///      DISPUTE_WINDOW later and may be deferred indefinitely on the timeout branch, so a
    ///      present-time reading would hand the seller every boundary that drifted past in the
    ///      meantime — including the one a buyer disputed seconds before it landed.
    function _weeksElapsedAt(uint64 ts) private view returns (uint8) {
        uint256 e = uint256((ts - _periodStart) / SUB_WEEK_LEN);
        if (e > uint256(_subWeeks)) { e = uint256(_subWeeks); }
        return uint8(e);
    }

    function _weeksElapsed() private view returns (uint8) {
        return _weeksElapsedAt(uint64(block.timestamp));
    }

    /// @notice Weeks the seller had BEGUN reserving for at `ts` — the elapsed ones plus the one in
    ///         progress.
    function _weeksStartedAt(uint64 ts) private view returns (uint8) {
        uint8 e = _weeksElapsedAt(ts);
        return e < _subWeeks ? e + 1 : e;
    }

    function _weeksStarted() private view returns (uint8) {
        return _weeksStartedAt(uint64(block.timestamp));
    }

    /// @notice Charge every started-but-unsettled week up to `target` at the FULL weekly quota and
    ///         mark them settled. No-op on an ordinary deal.
    ///
    ///         Take-or-pay is a promise about every week the seller reserved, so it is collected
    ///         on EVERY path that ends a subscription and covers EVERY unsettled boundary — the
    ///         result cannot depend on how often a keeper called `settleWeek`, nor on which exit
    ///         the parties happened to take.
    ///
    ///         `pay + fee` is clamped as ONE sum so the subtraction below always has room.
    ///         Bounded by `_subWeeks` (a month), so the loop is at most four iterations.
    function _chargeWeeksThrough(uint8 target) private {
        // Nothing is owed before the trial tick is accepted, on ANY path. `_periodStart` is set at
        // funding and only re-anchored by `acceptProbe`, so a subscription left sitting on its
        // probe still accumulates elapsed weeks — and a terminal path reading that clock would bill
        // them. `settleWeek` refuses for the same reason; this covers the exits it does not.
        if (!_isSubscription() || !_probeAccepted) { return; }
        while (_weekIndex < target) {
            // Charge UP TO the cumulative total the term owes after this week, not a flat quota on
            // top of whatever was paid before. The two differ exactly once: the accepted probe has
            // already paid a tick of week one, and adding a whole quota over it would bill that
            // tick twice — visible as a real overcharge on an early cancel, and as an underpaid
            // final week when the deposit clamp catches up with it at full term.
            uint128 target_ = uint128(uint256(_weekIndex + 1) * uint256(_tokensPerWeek));
            uint128 due = target_ > _tokensPaid ? target_ - _tokensPaid : 0;
            (uint128 pay, uint128 fee) = _valueOf(due);
            if (pay + fee > _deposit) {
                uint128 room = _deposit;
                fee = room < fee ? room : fee;
                pay = room - fee;
            }
            _deposit       -= pay + fee;
            _finalizedOwed += pay;
            _feeAccrued    += fee;
            _tokensPaid     = target_;
            _weekIndex     += 1;
            // A new week starts from what has been consumed so far, so its allowance is its own.
            _weekBaseTokens = _tokensFinal > _tokensPend ? _tokensFinal : _tokensPend;
        }
    }


    /// @notice Cross every week boundary the clock has passed — the WHOLE settlement, not just its
    ///         bookkeeping half: charge the weeks, announce it, and publish the volume the term has
    ///         delivered so far.
    ///
    /// @dev Kept as one operation because the two halves used to live in different callers. When a
    ///      boundary was crossed by `claimTokens` bringing the books up to the clock, the week was
    ///      charged but nothing announced it and no volume reached the reference price; the next
    ///      `settleWeek` then saw `due == _weekIndex`, had nothing to do, and never reached the
    ///      reporting half either. The week went by silently. Whoever crosses a boundary now
    ///      finishes crossing it.
    ///
    ///      `reportFinalized` is cumulative against a high-water mark on the book's side, so
    ///      reporting on every boundary records only the delta. The figure is `_tokensFinal` —
    ///      work actually served — not the quota billed: a week reserved and left undrawn adds
    ///      nothing to the public price.
    /// @return crossed whether anything was settled at all.
    function _settleBoundaries(uint8 due) private returns (bool crossed) {
        if (due <= _weekIndex) { return false; }
        _chargeWeeksThrough(due);
        emit TickFinalized{dest: address.makeAddrExtern(TickFinalizedEmit, bitCntAddress)}(_finalizedOwed, _deposit);
        _recordDelivered();
        _reportFinalized();
        return true;
    }

    /// @notice Permissionless. Credits the seller every week boundary that has passed since the
    ///         last settlement — the WHOLE week each time, independently of consumption, because a
    ///         subscription buys reserved availability, not delivered volume. Consumption tracking
    ///         exists for the dispute path only. A call that crosses no new boundary is refused
    ///         rather than accepted, so the function only ever pays for real settlement work.
    function settleWeek() public {
        require(_isSubscription(), ERR_BAD_PARAM);
        require(_opened && !_disputed, ERR_NOT_OPEN);
        // Same gate as `claimTokens`: nothing is owed before the probe is accepted. Both payment
        // paths must agree on that, or the take-or-pay one becomes a way around the trial.
        require(_probeAccepted, ERR_NOT_OPEN);
        uint8 due = _weeksElapsed();
        // A crossed boundary to settle, OR a term that is over and a deal still open — the second
        // is how the close below is retried once the last claim has stopped being contestable.
        require(due > _weekIndex || _weekIndex >= _subWeeks, ERR_SETTLE_WINDOW_OPEN);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_SETTLE);

        // Past the end of the term the `require` above still admits this call — that is how the
        // deferred close is retried — and those retries must not each pay for another report, so
        // the whole settlement is skipped when no boundary is crossed.
        _settleBoundaries(due);

        // Final week settled: nothing left to reserve, so the deal is over — but closing it is not
        // unconditional. Closing clears `_opened`, and `dispute()` needs `_opened`, so a close that
        // lands while a claim is still inside its promotion window would take the buyer's right to
        // contest that claim away mid-window and return the bond that backs the argument. The term
        // ending is not the seller's cue to escape a claim he filed seconds earlier.
        //
        // So the deal waits: the weeks are settled either way, and the close happens on a later
        // call, once nothing is contestable. That call is permissionless like this one, and the
        // require above admits it.
        if (_weekIndex >= _subWeeks
            && block.timestamp >= _lastClaimTime + CLAIM_PROMOTE_WINDOW) {
            _promoteDue();          // every terminal path promotes what waiting would have promoted
            _payFinalAndClose();
        }
    }

    function _isSubscription() private view returns (bool) {
        return _subWeeks != 0;
    }

    /// @notice Credit everything final-but-unpaid, then close cleanly.
    /// @dev    A SUBSCRIPTION has no final consumption top-up, and that is the canon rather than a
    ///         simplification. §8.2/§8.3: outside a dispute the seller is credited the full
    ///         `weekRevenue` for every week that was charged, whatever the buyer actually drew, and
    ///         "учёт потребления существует только для пути диспута". The week IS the payment, so
    ///         paying a consumption remainder on top of it would pay for the same service twice on
    ///         the buyer's `stop()` — where the started week is charged in full (§8.1) — and would
    ///         hand the seller a started week he is not entitled to on `sellerStop`, where §8.1 says
    ///         plainly that he does not take it. Both exits therefore add nothing here; what he
    ///         keeps is exactly the weeks that were charged, and the disputed week is settled by
    ///         claims on its own path.
    ///
    ///         An ordinary deal is the opposite case and keeps the old behaviour: it has no weekly
    ///         clock, nothing is charged in advance, and this remainder is the whole bill.
    function _payFinalAndClose() private {
        uint128 owed = (!_isSubscription() && _tokensFinal > _tokensPaid)
            ? _tokensFinal - _tokensPaid : 0;
        (uint128 pay, uint128 fee) = _valueOf(owed);
        if (pay + fee > _deposit) {
            uint128 room = _deposit;
            fee = room < fee ? room : fee;
            pay = room - fee;
        }
        _deposit       -= pay + fee;
        _finalizedOwed += pay;
        _feeAccrued    += fee;
        // The money mark moves only where money moved. On a subscription `owed` is zero by the rule
        // above, and advancing `_tokensPaid` to a consumption figure nobody paid for would leave the
        // final state asserting a payment that never happened — harmless here, since `_closeClean`
        // follows immediately, and wrong in the only way that matters: a field read afterwards would
        // describe something the contract did not do.
        if (owed > 0 && _tokensFinal > _tokensPaid) { _tokensPaid = _tokensFinal; }
        _closeClean();
    }

    /// @notice An unaccepted probe belongs to the buyer. Every close that is not the buyer walking
    ///         away from the trial returns it to the escrow, so it refunds with the rest instead of
    ///         being stranded in the contract. Burning it is `stop()`'s business alone.
    function _releaseProbe() private {
        if (_probeAccepted || _probeTick == 0) { return; }
        _deposit  += _probeTick;
        _probeTick = 0;
    }

    /// @notice The buyer's bond is collateral against a dispute, never payment. Every close folds
    ///         whatever survives back into the escrow so it refunds with the rest — called AFTER
    ///         the seller has been paid on each path, so it can never reach him, and after the
    ///         stake has been taken on the dispute paths, so the part `D` consumed is already gone.
    function _releaseBuyerBond() private {
        if (_buyerBond == 0) { return; }
        _deposit  += _buyerBond;
        _buyerBond = 0;
    }

    /// @notice Delivered volume, in ticks: the trusted consumption itself. Reference-price volume
    ///         (§7) and the seller rebate (§5.3) both measure WORK DONE, so a take-or-pay week the
    ///         buyer reserved but never drew contributes nothing to either. Recorded on every
    ///         terminal path, so a subscription that runs its full term is rewarded exactly like
    ///         one that was ended early.
    function _recordDelivered() private {
        // Claims are cumulative FROM ZERO, so the probe tick is already inside `_tokensFinal` once
        // anything has been claimed at all — it is the first tick of the same count, not a separate
        // one. It only needs supplying when the seller never claimed: an accepted probe is a
        // delivered tick even if no claim ever followed it.
        uint128 delivered = _tokensFinal;
        if (_probeAccepted && delivered < TICK_SIZE) { delivered = TICK_SIZE; }
        _ticksFinalized = delivered / TICK_SIZE;
    }

    /// @notice Terminal, no dispute ever opened: pay the rebate (§5.3), burn the net fee (§5.4),
    ///         return the bond, refund whatever the buyer did not spend.
    function _closeClean() private {
        _releaseProbe();
        // No dispute ever reached a resolution on this path, so the bond comes back whole. Folded
        // in here, after `_payFinalAndClose` has already clamped the seller's payment against the
        // escrow — the bond is never available to pay him.
        _releaseBuyerBond();
        _recordDelivered();
        uint128 refund = _deposit;
        _deposit = 0;
        _finalizedOwed += _sellerBond; _sellerBond = 0;
        _opened = false;
        _settleFees(true);
        if (refund > 0) { _payShell(_buyer, refund); }
        emit StreamStopped{dest: address.makeAddrExtern(StreamStoppedEmit, bitCntAddress)}(_buyer, _finalizedOwed, refund);
        // The event carries `_finalizedOwed` because it reports what the seller EARNED; the payment
        // of it happens on the next line, so the figure is still readable when the event is built.
        _payOwedAndDie();
    }

    // ========================================================
    // 4. Exits
    // ========================================================
    //
    // WHAT EACH EXIT COSTS THE BUYER — the invariant every change here must preserve.
    //
    //   probe phase (trial tick not accepted):
    //     stop()            probe tick burned, mirror tick burned from the bond
    //     sellerStop()      mirror tick burned from the bond; the trial tick goes back to the buyer
    //     dispute -> either D = one tick (the floor), burned; timeout burns the mirror too
    //
    //   after the probe:
    //     stop()            weeks through the one IN PROGRESS + trusted claims
    //     sellerStop()      weeks that ENDED only + trusted claims   (the seller quit mid-week)
    //     dispute -> either the buyer pays what stop() would cost him, PLUS D. The seller collects
    //                       only what his claims prove for the disputed week; the unearned rest of
    //                       it is burned, not refunded, and the timeout burns his D as well.
    //
    // The rule: no door out is cheaper than `stop()`. A dispute takes no evidence and its moment
    // is freely chosen, so the instant it settles for less than the ordinary exit it stops being
    // about the service and becomes the way to shed the bill — `D` is capped at 2P by spec while a
    // weekly quota runs to thousands of ticks, so "charge fewer weeks on the dispute path" is never
    // a small difference. `sellerStop` is the one asymmetry, and it runs the other way: the seller
    // forgoes the week he walked out of.
    //
    // The rule binds `sellerStop` on the trial too, which is why leaving during it has a price at
    // all. A price only one door charges is not a price: whoever finds the free door takes it, and
    // pre-probe there is no started week for `sellerStop` to forgo, so its own asymmetry has
    // nothing to bite on yet. The seller therefore burns a tick of bond whichever side ends the
    // trial. The trial tick itself does NOT follow him: it is the buyer's until acceptance, so it
    // burns only when the buyer is the one walking away from a service he asked to try, and
    // returns with the escrow when the seller is. Destroying it on the seller's exit would hand
    // him a grief worth exactly what it cost him — he opens the stream, leaves at once, and the
    // buyer's tick goes with him.
    //
    // ========================================================

    /// @notice Buyer ends the deal. An ordinary deal settles by FACT — the seller keeps the ticks
    ///         that were claimed and left uncontested, the rest of the escrow returns. A
    ///         subscription cancels instead of settling by fact: every week already STARTED is paid
    ///         IN FULL (take-or-pay — the seller reserved that capacity and cannot re-sell it
    ///         retroactively), and only whole UNSTARTED weeks refund. Cancelling buys back the
    ///         future, never the week already under way.
    ///         The contested tail (claimed but still inside its promotion window) is NOT paid on
    ///         this path — the buyer walking away is precisely the statement that it is disputed.
    /// @notice Scheme E penalty: taken from the bonds and BURNED, never handed to the counterparty.
    ///
    /// @dev    THE BURN IS THE POINT, NOT A SIMPLIFICATION. Giving the forfeit to the other side
    ///         would give that side an INTEREST IN THE OUTCOME: a seller would profit from the
    ///         buyer stopping, a buyer from the seller conceding. Destroying it leaves each party
    ///         paying only for its own act and gaining nothing from the other's loss. "Give it to
    ///         the injured party, that is fairer" will sound reasonable to whoever reads this next;
    ///         it is precisely what turns a penalty into a prize.
    ///
    ///         Clamped to what each holds; burned in one call, since `_burnShell` also reports the
    ///         write-off to the root's ledger.
    function _forfeitBonds(uint128 fromBuyer, uint128 fromSeller) private {
        uint128 b = fromBuyer  <= _buyerBond  ? fromBuyer  : _buyerBond;
        uint128 s = fromSeller <= _sellerBond ? fromSeller : _sellerBond;
        _buyerBond  -= b;
        _sellerBond -= s;
        _burnShell(b + s);
    }

    function stop() public {
        require(_opened, ERR_NOT_OPEN);
        require(msg.sender == _buyer, ERR_NOT_BUYER);
        require(!_disputed, ERR_DISPUTED);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_TERMINAL);

        // Still on the probe: the buyer tried the service and is walking away from it. The trial
        // tick is destroyed together with a mirror tick of the seller's bond — neither side keeps
        // it, so a seller who set out to take the first tick and disappear collects nothing and
        // pays a tick for the attempt. The rest of the bond goes back, the rest of the escrow
        // returns to the buyer, and no fee is charged on a tick nobody was paid for.
        if (!_probeAccepted) {
            uint128 bondBurn = _probeTick <= _sellerBond ? _probeTick : _sellerBond;
            uint128 burned   = _probeTick + bondBurn;
            _releaseBuyerBond();     // no dispute happened → the buyer's own bond refunds with the escrow
            uint128 refund   = _deposit;
            _sellerBond -= bondBurn;
            _finalizedOwed += _sellerBond; _sellerBond = 0;
            _probeTick = 0; _deposit = 0;
            _opened = false;
            _burnShell(burned);
            if (refund > 0) { _payShell(_buyer, refund); }
            emit ProbeBurned{dest: address.makeAddrExtern(ProbeBurnedEmit, bitCntAddress)}(
                _buyer, _pricePerTick, bondBurn, refund);
            // THE BRANCH THAT MADE THE CASE FOR TASK O. It is reached by the BUYER walking out of
            // an unaccepted trial, it hands the seller back the rest of his bond as a debt, and it
            // used to stop right here — the buyer's own call leaving a live contract that only the
            // seller could clear, and his note refusing to withdraw until he did.
            _payOwedAndDie();
            return;
        }

        _promoteDue();
        // Scheme E (ordinary): leaving early does not shed the pending tick — the buyer pays it
        // and the seller is paid it. Plus one tick of his own bond, burned. Total `N·P + P`.
        // Before this, walking out inside the window cost the buyer nothing (#979).
        if (!_isSubscription()) {
            if (_tokensPend > _tokensFinal) { _tokensFinal = _tokensPend; }
            _forfeitBonds(_pricePerTick, 0);
        }
        _chargeWeeksThrough(_weeksStarted());   // the week in progress is owed in full
        _payFinalAndClose();
    }

    /// @notice Seller gives up the deal (hardware died, model pulled). Whole weeks he did serve are
    ///         still owed to him, but the week he walks out of is NOT: he stopped reserving it
    ///         halfway, so take-or-pay has nothing to protect and the buyer keeps that money.
    ///         He forfeits the pending tail exactly like the buyer would, so quitting never pays
    ///         better than delivering.
    ///         Leaving during the trial is the same statement made earlier, and it is priced the
    ///         same way `stop()` prices it — one tick out of the bond. The week he walks out of is
    ///         what he normally forgoes, and before the probe there is no started week to forgo, so
    ///         without this the earliest possible walk-out would be the only free one.
    function sellerStop() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        require(_opened, ERR_NOT_OPEN);
        require(!_disputed, ERR_DISPUTED);
        // The trial tick itself is untouched: it is the buyer's until acceptance, and he is not the
        // one ending this. `_releaseProbe` inside the close returns it to the escrow to refund with
        // the rest, so the walk-out costs the seller a tick and costs the buyer nothing.
        if (!_probeAccepted) {
            uint128 bondBurn = _pricePerTick <= _sellerBond ? _pricePerTick : _sellerBond;
            _sellerBond -= bondBurn;
            _burnShell(bondBurn);
        }
        _promoteDue();
        // Scheme E (ordinary), and deliberately not the buyer's mirror: he LOSES the pending tick
        // rather than being paid it — quitting never pays better than delivering. `_promoteDue` ran
        // first, so only the still-contestable one is dropped. Plus one tick of his bond, burned.
        // `_probeAccepted` GATES THIS, and the gate is the whole correction. The branch above
        // already prices a walk-out during the trial at one tick; adding scheme E's forfeit on top
        // took the seller's entire 2P for leaving at the earliest possible moment — the cheapest
        // exit priced as the dearest. Scheme E describes the running stream (N−1 trusted, one
        // pending); before acceptance there is no pending claim to lose and the trial rule stands
        // on its own.
        if (_probeAccepted && !_isSubscription()) {
            _tokensPend = _tokensFinal;
            _forfeitBonds(0, _pricePerTick);
        }
        _chargeWeeksThrough(_weeksElapsed());   // only the weeks he saw through to the end
        _payFinalAndClose();
    }

    // ========================================================
    // 4b. Dispute — the buyer's standing right to challenge the deal (spec §4.2, §8.4)
    // ========================================================

    /// @notice Buyer challenges the deal. No precondition beyond owning it: the complaint may be
    ///         about claims that overstate consumption, or about a service that is simply not there
    ///         — and the second case leaves no on-chain trace to require, since a seller who
    ///         delivers nothing also claims nothing. Making a claim mandatory would put exactly
    ///         that case out of reach.
    ///
    ///         It is not free, and not only for the seller. Raising a dispute forfeits the value of
    ///         every claim not yet paid for, trusted ones included — the buyer is saying that
    ///         consumption did not happen, and he gives up the money whichever way it resolves.
    ///         Terminal: the deal does not resume.
    function dispute() public {
        require(_opened, ERR_NOT_OPEN);
        require(msg.sender == _buyer, ERR_NOT_BUYER);
        require(!_disputed, ERR_DISPUTED);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        _disputed     = true;
        _everDisputed = true;
        _disputeTime  = uint64(block.timestamp);
        emit StreamDisputed{dest: address.makeAddrExtern(StreamDisputedEmit, bitCntAddress)}(_buyer, _disputeTime);
    }

    /// @notice The amount at stake in a dispute, `D` — ONE figure, spent identically by both sides.
    ///
    ///         The spec fixes its shape: `0 < D <= 2P`, and a timeout burns an EQUAL `D` from each
    ///         party while the unused part of the bond returns. Two properties matter and neither
    ///         is optional. It is bounded, so a long-running deal with many honest claims behind it
    ///         cannot put the buyer's whole consumption on the fire against a fixed bond. And it is
    ///         non-zero even when nothing was claimed at all — that is exactly the no-service case
    ///         a dispute has to reach, and if it cost the buyer nothing there, opening one against
    ///         an honest seller would be free.
    ///
    ///         Clamped to what each side actually holds, so neither can be charged more than it
    ///         brought to the deal.
    function _disputeStake() private view returns (uint128) {
        // Order matters, and it is: value, then the FLOOR, then the cap, then the real-holdings
        // clamps last. The floor is `d < P -> d = P`, not `d == 0 -> d = P`. A substitution at zero
        // only catches the case where nothing at all was claimed; any value strictly between zero
        // and `P` passed straight through it, so a dispute over a fraction of a tick cost a fraction
        // of a tick — and the property being defended is not "a dispute is not free" but "a dispute
        // costs at least a tick", which is what makes opening one against an honest seller a real
        // decision rather than a cheap one. The clamps stay last because they answer a different
        // question: not what the stake should be, but how much of it each side can actually cover.
        uint128 d = _claimedUnpaidValue();
        if (d < _pricePerTick) { d = _pricePerTick; }   // floor: a dispute always costs a tick
        uint128 cap = _bondAmount();
        if (d > cap) { d = cap; }
        // Clamped to what each side can actually put up — a dispute with a stake on one side only
        // would be free to raise and costly to receive.
        //
        // A subscription stakes its own posted bond, NOT the escrow. The escrow is spent down week
        // by week and what remains of it is already promised back to the buyer, so a stake drawn
        // from there costs him only as much as he still expected to see again — the full `D` early
        // in the term, and nothing at all in the final week, where the residue burns anyway. The
        // bond does not move with the weeks, so the price of a dispute is the same on the first day
        // and the last. An ordinary deal has no weekly clock, nothing burns unearned, and the
        // escrow is the only thing the buyer has staked: it is read there as before.
        uint128 mine = _isSubscription() ? _buyerBond : _deposit;
        if (d > mine)        { d = mine; }
        if (d > _sellerBond) { d = _sellerBond; }
        return d;
    }

    /// @notice Value of everything CLAIMED and not yet paid for — the raw figure `_disputeStake`
    ///         bounds. Cumulative by construction, which is why it is capped before use.
    ///
    ///         This is what keeps the dispute from being the cheap way out. On `stop()` the buyer
    ///         pays for the trusted claims and the fresh tail comes back to him, so if a dispute
    ///         only touched that tail it would cost him no more than leaving quietly — and he would
    ///         always dispute. Charging him the whole claimed amount makes raising a dispute an act
    ///         with a price: it says the consumption behind those claims was not delivered, and
    ///         someone who says that gives up the money either way.
    ///
    ///         The platform fee is part of that price. `stop()` takes `pay + fee` out of the escrow
    ///         for the same ticks, so a dispute that forfeited only `pay` would still be the
    ///         cheaper door — by the fee, on every deal — and cheaper is all it takes for the
    ///         buyer to prefer it. Both exits have to cost the same, or the choice stops being
    ///         about whether the work was delivered.
    ///         The base is the one `_settleTrustedAndClose` uses, and for the same reason: on a
    ///         subscription past its first charged boundary the money mark `_tokensPaid` has already
    ///         moved to cover whole weeks whether or not they were consumed, so measuring unpaid
    ///         claims from it understates them by everything the take-or-pay charge absorbed. The
    ///         consumption mark `_weekBaseTokens` is what the claims are actually above. Before the
    ///         first boundary there is no such mark — it is still zero while the probe tick has been
    ///         paid for at `acceptProbe` — so the paid mark is correct there, and it is what an
    ///         ordinary deal uses throughout, having no weeks at all.
    function _claimedUnpaidValue() private view returns (uint128) {
        uint128 base = (_isSubscription() && _weekIndex > 0) ? _weekBaseTokens : _tokensPaid;
        uint128 tokens = _tokensPend > base ? _tokensPend - base : 0;
        (uint128 pay, uint128 fee) = _valueOf(tokens);
        return pay + fee;
    }

    /// @notice Resolve the pipeline the way a dispute leaves it: promote the SUPERSEDED claim and
    ///         drop only the newest one.
    ///
    ///         Trusted consumption survives a dispute. A claim that outlived its own contest window
    ///         unchallenged is the seller's, and an ordinary deal simply defers paying for it until
    ///         close — so wiping the pipeline back to `_tokensPaid` would hand the buyer every tick
    ///         he consumed and let the bounded stake stand in for the bill. A dispute reaches the
    ///         claim it was raised against and no further back.
    function _voidClaims() private {
        // This is `_promoteDue` evaluated at the instant the dispute was raised: the pending claim
        // is judged against ITS OWN landing time, and it is the seller's if it had already outlived
        // CLAIM_PROMOTE_WINDOW by then. A claim earns trust by surviving its window and by nothing
        // else — being replaced by a later claim is no longer even a distinct event, since the
        // replacement cannot arrive until the window has closed.
        //
        // `finalize` is permissionless, so a matured claim is one anybody could already have
        // promoted; whether someone bothered to spend the gas is not something the buyer gets to
        // dispute away.
        //
        // Measured against `_disputeTime`, not `block.timestamp`: a resolution arrives at least
        // DISPUTE_WINDOW after the dispute opened, so by then every window has expired and a
        // present-time check would pass unconditionally. The dispute freezes the picture at the
        // moment it was raised.
        if (_tokensPend > _tokensFinal
            && _lastClaimTime + CLAIM_PROMOTE_WINDOW <= _disputeTime) {
            _tokensFinal = _tokensPend;
        }
        _tokensPend = _tokensFinal;
    }

    /// @notice Seller agrees. He collects nothing for the disputed period — neither the claims nor
    ///         the running week — but his bond comes back whole, so agreeing costs him only what he
    ///         was arguing for. That is what keeps the cheap resolution available and the expensive
    ///         one rare.
    function releaseDispute() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        require(_disputed, ERR_NOT_DISPUTED);
        // The SAME `D` the timeout would have used, taken from the buyer alone: whoever opened the
        // dispute pays for it either way, so it is never free to raise. The seller's bond comes
        // back untouched — that is the whole of what agreeing buys him, and it is why agreeing
        // stays his cheaper branch. The claimed consumption is destroyed rather than handed over,
        // so inflating a claim and then agreeing to it earns nothing.
        // COMPLETED weeks only, exactly as `settleWeek` and `sellerStop` count them. The week the
        // dispute happened in is not settled by take-or-pay at all: it is settled by the claim
        // record in `_settleTrustedAndClose`, so the seller is paid for what he can show he
        // delivered and nothing more. Charging it here would pay him in full for precisely the
        // period whose delivery is being disputed, and it would empty the escrow that `D` is
        // taken from — on the last week of a term that leaves nothing to stake on either side.
        //
        // The unearned part of that week is destroyed rather than returned, on this branch and on
        // the timeout alike; see `_settleTrustedAndClose`. So disputing never refunds the buyer
        // more than ending the deal would — it costs him `D` more — and what it takes from the
        // seller is precisely the part of the week he cannot show he served.
        //
        // Counted at `_disputeTime`. The deal is terminal from the instant the dispute is raised,
        // so boundaries that drift past while it sits unresolved are not weeks the seller reserved
        // anything for — a buyer disputing a second before a boundary would otherwise be billed
        // for the whole week that started a second later.
        _chargeWeeksThrough(_weeksElapsedAt(_disputeTime));
        // Measured HERE, before `_voidClaims` collapses the pipeline it reads — but taken inside
        // the terminal, after the seller has been paid for what his claims prove.
        // Scheme E (ordinary): one tick from EACH side, pending dropped by `_voidClaims`.
        // Scheme E (ordinary): the pending tick is DROPPED — not charged to the buyer, not credited
        // to the seller — and its escrow refunds with the rest. One tick from EACH bond.
        // A subscription keeps its own `D`, taken from the buyer alone, as before.
        bool sub = _isSubscription();
        _voidClaims();
        _disputed = false;
        if (sub) { _settleTrustedAndClose(_disputeStake(), 0); }
        else     { _settleTrustedAndClose(_pricePerTick, _pricePerTick); }
    }

    /// @notice Permissionless after `DISPUTE_WINDOW` with nobody agreeing. Everything claimed is
    ///         burned out of the buyer's escrow, and the seller's whole bond is burned on top —
    ///         refusing to settle is what the bond is there for. An unresolved dispute is nobody's
    ///         win: the money leaves the system rather than landing on either side.
    function resolveDisputeTimeout() public {
        require(_disputed, ERR_NOT_DISPUTED);
        require(block.timestamp >= _disputeTime + DISPUTE_WINDOW, ERR_DISPUTE_WINDOW_OPEN);
        tvm.accept();
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        // Nobody agreed, so nobody is believed: an EQUAL `D` is destroyed on each side (§4.2). The
        // seller's bond is not forfeited wholesale — whatever `D` does not consume returns to him
        // through the settlement below, which is what makes refusing to settle a bounded cost
        // rather than a total one.
        // Completed weeks only, counted at `_disputeTime`; see releaseDispute. This branch is the
        // one that can be resolved arbitrarily late, so reading the clock here would be the
        // difference between one week and the whole remaining term.
        _chargeWeeksThrough(_weeksElapsedAt(_disputeTime));
        // As on the concession branch: measured before `_voidClaims`, applied after the payout.
        if (_isSubscription()) {
            uint128 d = _disputeStake();
            _voidClaims();
            _disputed = false;
            _settleTrustedAndClose(d, d);
            return;
        }
        // Scheme E (ordinary): NOBODY AGREED, so the pending tick is PAID and DESTROYED — the
        // buyer's escrow loses it, the seller does not gain it. Measured before `_voidClaims` drops
        // the record. Two ticks from EACH bond, so refusing costs strictly more than conceding.
        uint128 burnPend = 0;
        if (_tokensPend > _tokensFinal) {
            (uint128 pp, uint128 pf) = _valueOf(_tokensPend - _tokensFinal);
            burnPend = pp + pf;
        }
        _voidClaims();
        _disputed = false;
        if (burnPend > _deposit) { burnPend = _deposit; }
        _deposit -= burnPend;
        _burnShell(burnPend);
        _settleTrustedAndClose(2 * _pricePerTick, 2 * _pricePerTick);
    }

    /// @notice Shared terminal: credit whatever is trusted but unpaid, THEN take the stake, then
    ///         close. `pay + fee` is clamped as ONE sum against the deposit — clamping only `pay`
    ///         lets the subsequent `pay + fee` subtraction underflow and revert permanently.
    ///
    /// @dev Only COMPLETED weeks are charged; the week the dispute was raised in is settled from
    ///      the claim record instead. The order below is the substance, not housekeeping: the
    ///      seller is paid for what his matured claims prove BEFORE `stake` is taken, so a buyer's
    ///      dispute is never funded out of revenue the seller has already demonstrated. Taking the
    ///      stake first did exactly that once the escrow ran thin — on the last week of a term
    ///      there is no future escrow left, so the stake came straight out of proven earnings.
    ///
    /// @param sellerStake  what the SELLER forfeits: zero when he conceded, non-zero when nobody
    ///               than from the buyer alone. The unearned part of the disputed week burns on
    ///               either branch, so this flag decides only whether the seller's bond pays too —
    ///               which is what leaves agreeing his cheaper move and keeps the timeout rare.
    function _settleTrustedAndClose(uint128 buyerStake, uint128 sellerStake) private {
        _releaseProbe();
        // Idempotent: the resolvers charge the completed weeks before taking the stake, so this is
        // a no-op on that path. It stays as the single place a settlement guarantees the finished
        // weeks are booked before anything closes. Both callers are dispute resolutions, so the
        // count is taken at `_disputeTime` like everything else on this path.
        _chargeWeeksThrough(_weeksElapsedAt(_disputeTime));
        // A subscription week stands alone: the dispute is about the ticks of the week it was
        // raised in, so the seller is paid from where that week STARTED. `_weekBaseTokens` is the
        // consumption at the last boundary; `_tokensPaid` is money, and the two part company the
        // moment a week is under-consumed — take-or-pay charges the whole quota while consumption
        // lags it. Measuring from the money watermark would net an earlier week's reserved-but-
        // unused capacity against this week's real delivery and pay the seller nothing for ticks
        // he demonstrably served.
        //
        // Until the FIRST week has been charged there is no such boundary: `_weekBaseTokens` is
        // still zero while the probe tick has already been paid for at `acceptProbe`, which is
        // what `_tokensPaid` records. Measuring from zero there would pay that tick a second time.
        // So the base is the paid mark until a week has actually completed — which is also what an
        // ordinary deal uses throughout, having no weeks and no boundary at all.
        uint128 base = (_isSubscription() && _weekIndex > 0) ? _weekBaseTokens : _tokensPaid;
        uint128 owed = _tokensFinal > base ? _tokensFinal - base : 0;
        (uint128 pay, uint128 fee) = _valueOf(owed);
        if (pay + fee > _deposit) {
            uint128 room = _deposit;
            fee = room < fee ? room : fee;
            pay = room - fee;
        }
        _deposit        -= pay + fee;
        _finalizedOwed  += pay;
        _feeAccrued     += fee;
        _recordDelivered();
        if (_tokensFinal > _tokensPaid) { _tokensPaid = _tokensFinal; }

        // THE FORFEITS, taken only after the payout above — the seller is paid for what his
        // matured claims prove before anything is taken from either side. Each side's amount is
        // stated by the CALLER: `timedOut` used to mean "the seller pays too", and a conceding
        // seller had to be passed `true` to get a symmetric forfeit — a flag lying about its branch.
        uint128 db = buyerStake > _buyerBond ? _buyerBond : buyerStake;
        uint128 ds = sellerStake > _sellerBond ? _sellerBond : sellerStake;
        _buyerBond  -= db;
        _sellerBond -= ds;
        {
            _burnShell(db + ds);
        }

        // WHAT IS LEFT. Take-or-pay does not extend to the week a dispute was raised in: it is a
        // promise about reserved capacity, and the dispute is the buyer saying the capacity was not
        // there. Completed weeks stand; that one is settled from the claim record above, and the
        // part the seller could not show he served is unearned.
        //
        // That unearned part is DESTROYED, on both branches alike. Only the weeks the term never
        // reached come back, which is exactly what `stop()` returns as well — so the dispute never
        // buys the buyer a cheaper exit than simply ending the deal, it costs him `D` on top of it.
        // What it does buy him is that the seller does not collect for a week he cannot show he
        // served: on `stop()` that week is paid in full under take-or-pay, here it is paid only up
        // to the claim record. The price to the seller is therefore exactly how far he fell short —
        // nothing at all if he served the week out, the whole week if he served none of it — while
        // the buyer's own bill is the same either way, which is what keeps the dispute a statement
        // about delivery rather than a way to shed the bill.
        //
        // Refunding it instead would make the dispute the cheapest door in the contract, and one
        // available with no precondition: cancelling the running week would cost `D`, capped at 2P
        // by spec, against a weekly quota that runs to thousands of ticks.
        uint128 refund = _deposit;
        // `_probeAccepted` gates this exactly as it gates `_chargeWeeksThrough`, and for the same
        // reason. `_periodStart` is set at funding and RE-ANCHORED at `acceptProbe`, so until the
        // trial tick is taken the clock still runs from the match — a term that never began would
        // otherwise be counted as started, its weeks would look consumed, and after `_subWeeks` of
        // the seller simply not accepting there would be no unstarted week left to refund. The
        // buyer's whole deposit would burn over a service that never opened.
        if (_isSubscription() && _probeAccepted) {
            uint8 started = _weeksStartedAt(_disputeTime);
            uint8 unstarted = _subWeeks > started ? _subWeeks - started : 0;
            (uint128 rPay, uint128 rFee) =
                _valueOf(uint128(uint256(unstarted) * uint256(_tokensPerWeek)));
            uint128 refundable = rPay + rFee;
            if (refundable < refund) {
                _burnShell(refund - refundable);
                refund = refundable;
            }
        }
        // Whatever `D` did not consume goes home with the refund — the bond is collateral, and only
        // the staked part of it was ever at risk. Added AFTER the burn above so it is measured
        // against the escrow alone: the bond is not weekly money and must not be mistaken for the
        // unearned part of a week.
        refund += _buyerBond; _buyerBond = 0;
        _deposit = 0;
        _finalizedOwed += _sellerBond; _sellerBond = 0;
        _opened = false;
        _settleFees(false);     // a dispute ever opened → no rebate (§5.3)
        if (refund > 0) { _payShell(_buyer, refund); }
        emit DisputeResolved{dest: address.makeAddrExtern(DisputeResolvedEmit, bitCntAddress)}(_finalizedOwed, refund, sellerStake == 0);
        // Pay what the resolution earned him and end. A resolved dispute is as terminal as an
        // ordinary close, and leaving the seller to come and collect would keep BOTH notes holding
        // a live deal — including the buyer's, who is the one who opened the dispute.
        _payOwedAndDie();
    }

    /// @notice Anyone, after `MATCH_OPEN_TIMEOUT` with no open(): refund the
    ///         buyer's full deposit and return any posted seller bond to
    ///         the seller (nothing delivered → no fee, no penalty, §2.1), then
    ///         self-destruct the dead deal.
    /// @dev    Permissionless (no-show recovery), so the payout is NOT caller-chosen: the buyer's
    ///         deposit and the seller bond go to their fixed notes FIRST, then the residual native
    ///         gas is swept to `_sellerNote` — stored state checked against the canonical note
    ///         derivation, never an address the caller supplies. Any residual FIGURE is zeroed
    ///         rather than swept, because after paying two parties nothing can say whose it is.
    function cleanupUnopened() public {
        require(_funded, ERR_NOT_FUNDED);
        require(!_opened, ERR_ALREADY_OPEN);
        // Never-opened only: after a real open+close the earnings live in
        // `_finalizedOwed` for withdrawShell, so this permissionless recovery is scoped
        // to the never-opened case. A funded deal that genuinely never opened has
        // _everOpened=false.
        require(!_everOpened, ERR_ALREADY_OPEN);
        // Two ways in. The timeout is the no-show recovery this call was written for. The other is
        // the buyer's note saying, at match time, that it CANNOT post the bond — there is nothing
        // to wait for then: without the bond `open` can never succeed, so the deal is already dead
        // and the timeout would only delay returning the seller's money.
        require(msg.sender == _buyer
                || uint64(block.timestamp) >= _fundedTime + MATCH_OPEN_TIMEOUT, ERR_STREAM_TIMEOUT_OPEN);
        tvm.accept();
        ensureBalance();
        // NOTHING IS TAKEN FROM THE RESERVE HERE. This exit delivers nothing and penalises nobody;
        // charging it would take the last of a reserve the seller funded in order to hand his own
        // money back to him. What IS burned is whatever the caller attached — the charge rides with
        // the call, and a caller who attached nothing simply gets the deal wound down for free
        // rather than not at all.
        uint128 attached = uint128(msg.currencies[SHELL_ECC_ID]);
        if (attached > 0) { gosh.burnecc(uint64(attached), SHELL_ECC_ID); }

        _releaseBuyerBond();        // never opened → no dispute was possible; the bond refunds whole
        uint128 refund = _deposit;
        uint128 bondBack = _sellerBond;
        _deposit = 0; _sellerBond = 0; _funded = false; _sellerBondFunded = false; _buyerBondFunded = false;
        // The resting offer goes with the funding it was matched against: leaving it set would
        // block both ways out of the state this call creates — `postFromNote` returns early on
        // `_offerPosted`, and `destroy` refuses on it.
        _offerPosted = false;
        // The endpoint ciphertext goes with the cycle it belonged to. It was encrypted FOR THE
        // BUYER being refunded here, so leaving it in storage means the next buyer of this same
        // deal is handed a stream he cannot decrypt — and `open` emits it without complaint if
        // the seller passes an empty value, because an empty optional means "unchanged".
        delete _endpointCipher;

        _payShell(_buyer, refund);
        _payShell(_sellerNote, bondBack);   // return the seller's bond (no-show, not slashed)

        // THE ONE EXIT THAT ZEROES ITS RESIDUAL INSTEAD OF PAYING IT ONWARD, and the reason is not
        // the one an earlier draft of this comment gave. It said the payee was SuperRoot and a
        // SuperRoot cannot hold a figure; the payee is `_sellerNote` now and could hold one
        // perfectly well, so that argument is gone and this is the argument that survives:
        //
        // THIS EXIT PAYS TWO PARTIES, so the owner of a residual is unknowable. Both `_payShell`
        // calls above zero their earmark on the way out and hand the figure on through `_balance`,
        // so anything still sitting here afterwards came from one of the two and left no record of
        // which. A remainder is therefore either the buyer's refund or the seller's bond, with
        // nothing in the contract able to say which. Handing it to the destruct's payee would give
        // the seller's note money
        // that may be the buyer's; handing it to the buyer would do the mirror wrong. Zeroing takes
        // it from nobody in particular, which is the only disposal that cannot rob a specific party.
        //
        // Zeroing is also a real disposal rather than a loss dressed up as one: the ECC these
        // figures stand for never left RootPN's custody, a deal only ever held a number against it,
        // so subtracting the number is the whole of what "the money leaves here" can mean.
        //
        // In the ordinary flow this zeroes nothing: the two payouts above cleared the deposit and
        // the bond, so `_balance` is zero and only a bounced credit could leave a remainder.
        if (_balance > 0) { _burnShell(_balance); }
        // BOTH NOTES MUST STOP COUNTING THIS DEAL AS LIVE. `onInferenceFilled` set
        // `_liveDeals[deal]` on the buyer's note AND on the seller's — the book reports a fill
        // to both — and `withdrawTokens` refuses while that map is non-empty. Until this call
        // the clearing came free with `_die`; now that the deal survives, it has to be said out
        // loud, or a fully refunded buyer and a seller who got his bond back both sit unable to
        // withdraw until someone destroys a deal that is no longer in anyone's way.
        if (_buyer.value != 0) {
            IInferenceNoteMirror(_buyer).onDealClosed{value: DAPP_MSG_VALUE, flag: 1, bounce: false}();
        }
        if (_sellerNote.value != 0) {
            IInferenceNoteMirror(_sellerNote).onDealClosed{value: DAPP_MSG_VALUE, flag: 1, bounce: false}();
        }
        // Residual NATIVE gas goes to the seller's note, which is where the deal's gas came from in
        // the note-funded flow — `fundDeal` sends it from there. This used to be the fixed SuperRoot
        // sink, guarding against an aimable payee on a call anyone may make after the timeout; that
        // guard is not needed, because `_sellerNote` is STORED STATE verified against the canonical
        // note derivation when it is bound, not something the caller supplies. Whoever triggers the
        // cleanup still cannot point it at himself.
        //
        // GAS ONLY. The money was zeroed above, and no figure is ever sent to a root.
        // THE DEAL SURVIVES THIS CALL. A no-show costs the seller the match, not the contract:
        // the state above is back to what it was before funding, so the same deal can take a new
        // offer through `postFromNote` without paying GAS_DEPLOY again — the largest single charge
        // in the table — for a deploy that would change nothing but the address.
        //
        // Winding it up for good is a separate, deliberate call: `destroy`, made by the seller when
        // the deal is idle and unfunded. Splitting the two also removes the path where one no-show
        // paid two terminal charges, one here and one on the destroy that followed.
    }

    // ========================================================
    // 5. Seller withdraw + destroy
    // ========================================================

    /// @dev NO RECIPIENT ARGUMENT. The seller is paid at `_sellerNote` and nowhere else. Naming a
    ///      recipient was the single reason this contract needed a bounce handler at all — it was
    ///      the one payout aimed at an address nothing had validated. With the address fixed to a
    ///      static, the payout cannot miss, and the handler, the `purpose` earmark and the restore
    ///      logic all went with it.
    function withdrawShell(uint128 amount) public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_LIGHT);
        require(amount > 0, ERR_ZERO_AMOUNT);
        require(amount <= _finalizedOwed, ERR_INSUFFICIENT_TOKENS);
        // The deal's own record, not the account's currency (generation 4.0.33). The check still
        // earns its place next to the `_finalizedOwed` bound above: that one says how much the
        // seller is OWED, this one says how much the deal actually HOLDS, and the two are written
        // by different paths — a bound on one is not a bound on the other.
        require(amount <= _balance, ERR_INSUFFICIENT_TOKENS);

        _finalizedOwed -= amount;
        _payShell(_sellerNote, amount);

        emit ShellWithdrawn{dest: address.makeAddrExtern(ShellWithdrawnEmit, bitCntAddress)}(_sellerNote, amount);

        // Auto-cleanup on the funded happy path: a funded deal whose stream has
        // closed (`!_opened`, buyer already refunded by `stop`) and whose seller
        // just drained the last of `_finalizedOwed` has nothing left to do → sweep
        // the residual to the recipient. A funded TC's sell offer was consumed on
        // the fill (removed from the book), so no resting offer remains and this is
        // race-free.
        if (_funded && !_opened && !_disputed && _finalizedOwed == 0 && !_offerPosted) {
            // The residual figure follows the destruct's payee, as everywhere else.
            if (_balance > 0) { _payShell(_sellerNote, _balance); }
            // Residual native gas unchanged; the seller's money went to the note above.
            _die(_sellerNote);
        }
    }

    /// @dev NO PAYEE ARGUMENT — `_sellerNote`, like every other exit. See `close`.
    function destroy() public onlyOwnerPubkey(_sellerPubkey) accept {
        ensureBalance();
        _chargeGas(GAS_TERMINAL);
        require(!_opened, ERR_STILL_OPEN);
        require(!_disputed, ERR_DISPUTED);
        // Emergency manual close. Blocked while a LIVE sell offer still rests on the
        // book, so the TC is never destroyed out from under a resting offer that a
        // later match could fund. `_offerPosted` tracks the REAL offer state (cleared
        // on match-fill in _recordFunding), not the `_funded` proxy, so this single
        // check is exact. Cancel the offer first (via the note, or use `close`).
        require(!_offerPosted, ERR_OFFER_LIVE);
        // Never selfdestruct over a live buyer deposit: a matched-but-unopened deal
        // (_funded && !_opened) still holds the buyer's escrowed SHELL, which selfdestruct would
        // sweep away from him. Refund the buyer (and return the seller's
        // bond) first, mirroring cleanupUnopened, so the sweep only takes residual native gas.
        if (_funded) {
            _releaseProbe();
            _releaseBuyerBond();
            uint128 refund = _deposit;
            _deposit = 0; _funded = false;
            _payShell(_buyer, refund);
        }
        _returnBond();
        // Same rule as every other exit: the destruct's payee also takes the residual figure,
        // because that is what the destruct did with residual ECC before the money became a
        // record. Everything owed was just paid above — the buyer's deposit, the probe, both
        // bonds — so a non-zero remainder here is arithmetic left over from those, small and
        // owned by nobody the contract can name.
        if (_balance > 0) { _payShell(_sellerNote, _balance); }
        // Residual native gas unchanged; only the money needed a line of its own.
        _die(_sellerNote);
    }

    // ========================================================
    // Getters
    // ========================================================

    /// @notice `tokensPending` is the single contestable claim, `lastClaimTime` is when it landed,
    ///         and the buyer needs both: the window he has to act inside runs from the second and
    ///         closes CLAIM_PROMOTE_WINDOW later.
    ///
    /// @dev    `tokensSuperseded` and `prevClaimTime` ARE GONE, not renamed and not filled with a
    ///         stand-in. They described the middle stage of a three-deep pipeline that no longer
    ///         exists; returning `tokensFinal` under the old name would keep every caller compiling
    ///         while telling them a claim is superseded when nothing can be. A reader that breaks
    ///         is told the truth once; a reader that keeps working is told a falsehood forever.
    function getState() external view returns (
        bool funded, bool opened, bool probeAccepted, bool disputed,
        uint128 deposit, uint128 probeTick, uint128 finalizedOwed,
        uint128 tokensFinal, uint128 tokensPending,
        uint64 probeTime, uint64 lastClaimTime,
        uint64 disputeTime, uint64 fundedTime
    ) {
        return (_funded, _opened, _probeAccepted, _disputed, _deposit, _probeTick, _finalizedOwed,
                _tokensFinal, _tokensPend,
                _probeTime, _lastClaimTime, _disputeTime, _fundedTime);
    }

    /// @notice Subscription shape: `subWeeks == 0` is an ordinary deal (whole volume, no clock).
    /// @dev `weekBaseTokens` is reported because the claim ceiling cannot be derived without it:
    ///      inside the term `_claimCap()` is `_weekBaseTokens + _tokensPerWeek` capped by
    ///      `fundedTokens`, and `_weekBaseTokens` tracks CONSUMPTION at the last week boundary while
    ///      `tokensPaid` tracks MONEY. The two part company whenever a week was under-consumed, so a
    ///      client that restarts and reads only `tokensPaid` cannot tell how much the seller may
    ///      still claim this week.
    ///      Once `weekIndex` has reached `subWeeks` the formula no longer applies: the term sells no
    ///      further capacity, so the ceiling is the cumulative already stated and no higher claim is
    ///      admitted. A client reading `weekBaseTokens` past the end of the term and expecting one
    ///      more quota would be reading a week that was never bought.
    function getSubscription() external view returns (
        uint8 dealFlags, uint8 subWeeks, uint8 weekIndex, uint128 tokensPerWeek,
        uint128 fundedTokens, uint128 tokensPaid, uint64 periodStart, uint128 weekBaseTokens
    ) {
        return (_dealFlags, _subWeeks, _weekIndex, _tokensPerWeek, _fundedTokens, _tokensPaid,
                _periodStart, _weekBaseTokens);
    }

    /// @notice Offer state: `offerPosted` = the TC has a live resting sell offer on the order book
    ///         right now. Lets a client read whether this TC is actively selling.
    ///
    /// @dev    `closing` IS GONE from this tuple, and deliberately not replaced by a constant
    ///         `false` under the same name. It reported a wind-down-in-progress, a state that no
    ///         longer exists: `close()` refuses while an offer rests instead of remembering an
    ///         intention. Leaving the name in place would keep every caller compiling while
    ///         telling them, forever, that no wind-down is pending — which would be true by
    ///         accident and unfalsifiable. A reader that breaks is told the truth once.
    function getOffer() external view returns (bool offerPosted) {
        return _offerPosted;
    }

    /// @notice Seller bond state (spec §4.2): whether the seller posted the mirror
    ///         bond, the SHELL amount currently held as it, and the required bond (2P).
    function getSellerBond() external view returns (bool bondFunded, uint128 bondHeld, uint128 bondRequired) {
        return (_sellerBondFunded, _sellerBond, _bondAmount());
    }

    /// @notice Buyer bond (§4.2, subscription only): the `2P` posted with the escrow at funding and
    ///         held apart from it, which is what `D` is staked from. `bondRequired` is zero on an
    ///         ordinary deal, where the stake comes from the escrow and nothing is posted.
    function getBuyerBond() external view returns (uint128 bondHeld, uint128 bondRequired) {
        return (_buyerBond, _isSubscription() ? _bondAmount() : 0);
    }

    function getConfig() external pure returns (
        uint16 platformFeeBps, uint64 minClaimInterval, uint64 minSecondsPerTick, uint64 disputeWindow
    ) {
        return (PLATFORM_FEE_BPS, MIN_CLAIM_INTERVAL, MIN_SECONDS_PER_TICK, DISPUTE_WINDOW);
    }

    /// @notice Fee state (spec §5): accrued fee, finalized-tick count (rebate n),
    ///         whether a dispute ever opened, and the rebate config.
    function getFees() external view returns (
        uint128 feeAccrued, uint128 ticksFinalized, bool everDisputed,
        uint16 rebateMaxBps, uint16 rebateSlopeBps
    ) {
        return (_feeAccrued, _ticksFinalized, _everDisputed, REBATE_MAX_BPS, REBATE_SLOPE_BPS);
    }

    function getDeal() external view returns (uint128 tickSize, uint128 pricePerTick, uint128 maxTicks) {
        return (TICK_SIZE, _pricePerTick, _maxTicks);
    }

    function getParties() external view returns (address buyer, address sellerNote) {
        return (_buyer, _sellerNote);
    }

    /// @notice Buyer note pubkey recorded at the match (spec §3.1.1): the gateway
    ///         verifies the buyer's challenge signature against this.
    function getBuyerPubkey() external view returns (uint256) { return _buyerPubkey; }

    function getEndpointCipher() external view returns (bytes) { return _endpointCipher; }

    function getModelName() external view returns (string) { return _modelName; }
    function getModelHash() external view returns (uint256) { return _modelHash; }

    /// @notice The deal's SHELL (generation 4.0.33): the private record, not the account's ECC.
    /// @dev    Reads `_balance`. Off-chain callers that used to compare this against
    ///         `ecc_balance[2]` will now see them diverge, and that is the point — the account's
    ///         ECC is gas, this is the money.
    function getShellBalance() external view returns (uint128) {
        return _balance;
    }

    function getSeller() external view returns (uint256 sellerPubkey, address rootModelAddress, uint64 nonce) {
        return (_sellerPubkey, _rootModelAddress, _nonce);
    }

    function getVersion() external pure returns (string, string) {
        return (version, "TokenContract");
    }
}
