pragma gosh-solidity >=0.76.1;

interface IRootModelRegistry {
    function registerTokenContract(uint256 sellerPubkey, uint64 nonce) external;
}

/// @notice Receiver of a private-balance credit (§5, generation 4.0.33).
/// @dev    SHELL is held as a NUMBER — `_balance[CURRENCIES_ID_SHELL]` on a note,
///         `_balance` on a deal — not as ECC on the account. Value therefore moves by
///         message, not by `currencies:`: the payer subtracts from its own record and
///         calls this, and the receiver adds the same figure. The receiver MUST
///         authenticate `msg.sender` by re-deriving the payer's canonical address from
///         `sellerPubkey`/`nonce` before crediting, so an arbitrary contract cannot
///         conjure a balance by simply calling this entry point.
///
///         SENT `bounce: false`, AND THAT IS WHY THERE IS NO `purpose`. This call used to
///         carry an 8-bit earmark whose only reader was the payer's own `onBounce`: it said
///         which counter to restore if the credit came home. The deal no longer pays anyone
///         but the notes party to it — `_sellerNote` and `_buyer`, both statics — and a note's
///         credit entry has no branch that can refuse a legitimate deal. A payout that cannot
///         miss needs no return path, so the handler, the earmark and the bit budget they
///         lived inside are all gone together.
///
///         What that budget bought is worth remembering rather than restoring: a bounced body
///         preserves only its leading bits, 256 guaranteed, so anything the payer had to
///         recover had to sit at the front. If a bounceable call is ever added here, that
///         constraint comes back with it and `amount` goes first again.
interface IPrivateBalance {
    function creditFromDeal(uint128 amount, uint256 sellerPubkey, uint64 nonce) external;
}

/// @notice The same movement, paid by an InferenceOrderBook instead of a deal.
/// @dev    A separate entry point because AUTHENTICATION DIFFERS, and authentication is the only
///         thing making either credit real. A deal is derived from `(sellerPubkey, nonce)`; a book
///         is derived from `modelHash` alone — one book per model. Folding both into one function
///         would mean a receiver that cannot tell which derivation to run, i.e. one that accepts
///         whichever the caller claims — and a check the caller chooses is not a check.
///
///         No `purpose` here. A book keeps its earmarks per ORDER (`e.escrow`), and by the time it
///         pays, the order it paid from is already cancelled or filled; there is no counter for a
///         bounce to restore, only the balance. Bounced-window budget: 32 + 128 = 160 bits.
interface IPrivateBalanceFromBook {
    function creditFromBook(uint128 amount, uint256 modelHash) external;
}

/// @notice A deal telling its book that a handover landed.
/// @dev    The book writes down whose escrow a handover carried BEFORE dispatching it, because a
///         bounce can return the figure but not the name. This is what lets it forget: the record
///         exists to answer a failure, so it should not outlive a success.
interface IOrderBookHandover {
    function onHandoverAccepted() external;
}

/// @notice A note's view of the deals and orders it still has outstanding (task E).
/// @dev    Both entry points are told by a contract announcing something about ITSELF, so
///         `msg.sender` is both the authentication and the key — nothing is passed and nothing is
///         compared. `onDealClosed` carries no parameters at all for that reason.
interface IInferenceNoteMirror {
    function onDealClosed() external;
}

/// @notice The custodian being told that a deal wrote a figure off its own record.
/// @dev    The deal burns nothing — it holds no currency. Word size is not its problem either: the
///         root holds the coins, so the root deals in whatever width they need.
interface IWriteOffSink {
    function reportDealWriteOff(uint256 sellerPubkey, uint64 nonce, uint128 amount) external;
}
