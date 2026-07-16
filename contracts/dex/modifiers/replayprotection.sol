pragma gosh-solidity >=0.76.1;

import "./errors.sol";

/// @title ReplayProtection — external message anti-replay for DEX contracts.
/// @notice Copied/adapted from contracts/multifactor/modifiers/replayprotection.sol.
///         Intended for mix-in into contracts that accept user-signed external messages
///         (those with `pragma AbiHeader time; AbiHeader expire; AbiHeader pubkey`).
///         Call sites mark each user-facing external method with the `saveMsg` modifier.
///         Internal-only methods (where `afterSignatureCheck` never fires) must NOT use
///         `saveMsg` — the hook only populates `lastMessage` for ext messages; a save
///         after an internal call would write `messages[0][0]`.
abstract contract ReplayProtection is Errors {
    string constant versionRP = "6.2.0";

    /// @notice Seen external messages, keyed by (expireAt, messageHash).
    ///         Entries are garbage-collected opportunistically via `gc()`.
    mapping(uint32 => mapping(uint256 => bool)) messages;

    /// @notice Bound on per-call cleanup iterations to keep `saveMsg` gas-bounded.
    uint8 constant MAX_CLEANUP_ITERATIONS = 20;

    /// @notice Info captured by `afterSignatureCheck`; consumed by `_saveMsg`.
    struct MessageInfo {
        uint256 messageHash;
        uint32  expireAt;
    }
    MessageInfo lastMessage;

    /// @notice Modifier to be placed on every user-signed public method.
    ///         Records the current message hash (set by afterSignatureCheck)
    ///         so a retransmission with the same hash is rejected.
    modifier saveMsg() {
        _saveMsg();
        tvm.commit();
        _;
    }

    function _saveMsg() inline internal {
        gc();
        messages[lastMessage.expireAt][lastMessage.messageHash] = true;
    }

    /// @notice Compiler-invoked hook after ext message signature verification.
    ///         Parses timestamp(64) + expireAt(32) from the body head,
    ///         validates the expireAt window, rejects duplicates.
    function afterSignatureCheck(TvmSlice body, TvmCell message) private inline returns (TvmSlice) {
        body.load(uint64);
        uint32 expireAt = body.load(uint32);
        require(expireAt > block.timestamp, ERR_MESSAGE_EXPIRED);
        require(expireAt < block.timestamp + 5 minutes, ERR_MESSAGE_WITH_HUGE_EXPIREAT);

        uint messageHash = tvm.hash(message);
        optional(mapping(uint256 => bool)) m = messages.fetch(expireAt);
        require(!m.hasValue() || !m.get()[messageHash], ERR_MESSAGE_IS_EXIST);
        lastMessage = MessageInfo({messageHash: messageHash, expireAt: expireAt});
        return body;
    }

    /// @notice Garbage-collect expired message buckets, capped at MAX_CLEANUP_ITERATIONS.
    function gc() private {
        uint counter = 0;
        for ((uint32 expireAt, mapping(uint256 => bool) m) : messages) {
            m; // suppress unused warning
            if (counter >= MAX_CLEANUP_ITERATIONS) {
                break;
            }
            counter++;
            if (expireAt <= block.timestamp) {
                delete messages[expireAt];
            } else {
                break;
            }
        }
    }
}
