#pragma once
// Completion.h — F8c: the result of an asynchronous client operation, without
// futures or promises (spec 2026-10-01-f8c-no-futures-design.md §2.1).
//
// Every asynchronous operation takes an app-chosen Token and a target:
//   * a Completions& queue (rune/client/Completions.h) that the owning app
//     thread polls, or
//   * a CompletionHandler {fn, ctx}: a plain function pointer called on the
//     duty thread.
// Exactly-once rule: an operation is either rejected synchronously (an OpResult
// other than Accepted — no completion ever follows) or accepted, in which case
// exactly ONE Completion is delivered for its token.
//
// No std::function, no virtual call, no allocation on any of these types.

#include <cstdint>
#include <span>
#include <type_traits>

namespace rune::client {

/// App-chosen operation id.  Only the low 48 bits are carried (the producer's
/// 64-bit ack slot uses the rest as a tag — §2.4).
using Token = uint64_t;
inline constexpr Token kMaxToken = (Token{1} << 48) - 1;

enum class OpKind : uint8_t {
    Send = 1, Tier2Batch, Flush,                                         // producer
    Connect, Ping, DefineTopic, GetTopicSchema,                          // client control
    CommitSync,                                                          // group consumer
    InitTransactions, SendOffsets, CommitTransaction, AbortTransaction,  // txn
    SubscribeBound,                                                      // optional (§2.6)
    ListTopics,                                                          // client control (C3: listTopics())
    BatchAck,       ///< producer per-batch acknowledgement (C6: Producer::batchAcksTo; see batchAckOf())
    ProducerClose,  ///< producer begin-close (C6a: Producer::close(target, token))
    TxnClose,       ///< C7: TransactionalProducer::close(target, token)
    ClientDrain,    ///< C7: Client::close()'s producer drain (internal)
    AlterTopic,     ///< Task 18: Client::alterTopic() / alterTopicAndWait()
    CreateView,     ///< Task 21: Client::createView() / createViewAndWait()
    GetViewRow,     ///< Task 21: Client::getViewRow() / getViewRowAndWait()
    // Catalog history (Task 5): the remaining DDL ops (wire 74-77, answered by DdlResponse 78).
    DropTopic,         ///< Client::dropTopic() / dropTopicAndWait()
    DropView,          ///< Client::dropView() / dropViewAndWait()
    AlterView,         ///< Client::alterView() / alterViewAndWait()
    AlterTopicConfig,  ///< Client::alterTopicConfig() / alterTopicConfigAndWait()
};

enum class OpStatus : uint8_t {
    Ok = 0,
    BrokerError,     ///< errorCode = broker ErrorCode; payload = message text (truncated)
    Timeout,         ///< the op's own duty-side deadline expired
    Closed,          ///< Client closed or the owning handle closed before the result
    NotConnected,    ///< control ops: the seed can never reconnect (never connected, Rejected, Closed, or a
                     ///< stopped failover).  While it can, an op waits for it inside its own deadline
                     ///< (ddlTimeoutMs / pingTimeoutMs -> Timeout).  Other ops: not Connected when processed.
    Superseded,      ///< commitSync replaced by a newer commitSync
    Rejoined,        ///< group generation changed under a commitSync
    SessionLost,     ///< session dropped after the request was sent (no answer possible)
    Fenced,          ///< txn: ProducerFenced / InvalidProducerEpoch
    DeliveryFailed,  ///< producer: delivery timeout / redirect budget / deferred-buffer full
    ResultTooLarge,  ///< polled form only: variable result > the reservation (value = real size)
    InvalidArgument, ///< the duty thread refused the op as malformed before sending it (payload = why)
};

/// Authentication (codes 44..47, client version 9): the name of an auth-related Completion::errorCode when
/// status == BrokerError, else nullptr.  AuthenticationFailed ends the session (a connect fails with it, every
/// pending op too); NotAuthorized fails exactly that op and is never re-sent; SessionRevoked completes
/// everything in flight and is terminal (no reconnect); PasswordChangeRequired fails exactly that op.
[[nodiscard]] constexpr const char* authErrorName(uint16_t code) noexcept {
    switch (code) {
    case 44: return "AuthenticationFailed";
    case 45: return "NotAuthorized";
    case 46: return "SessionRevoked";
    case 47: return "PasswordChangeRequired";
    default: return nullptr;
    }
}

/// One completion record: 40 bytes, trivially copyable.  A variable payload
/// (ping echo, schema descriptor blob, error text) follows it inline.
struct Completion {
    Token    token = 0;          ///< as passed at issue
    int64_t  value = 0;          ///< Send/Tier2: baseOffset (-1 if none); Connect: sessionId;
                                 ///< DefineTopic/GetTopicSchema/AlterTopic: topicId; ResultTooLarge: real size
                                 ///< CreateView: viewTopicId; GetViewRow: rowLen (informational; the payload's
                                 ///< own length is ground truth)
    int64_t  aux   = 0;          ///< Connect refused AuthenticationFailed: the broker's retryAfterMs (rate-limited login; 0 = none)
                                 ///< GetTopicSchema/DefineTopic: version; AlterTopic: newVersion; Tier2: recordCount
                                 ///< CreateView: outputFingerprint (bit-cast uint64_t)
    OpKind   op{};
    OpStatus status = OpStatus::Ok;
    uint16_t errorCode = 0;      ///< broker ErrorCode when status == BrokerError; for GetViewRow carries the
                                 ///< ViewRowResponse::Status (1=AbsentKey,2=NotReady,3=SchemaMismatch,4=UnknownView)
    uint32_t payloadLen = 0;     ///< bytes of inline payload
    uint32_t reservedBytes = 0;  ///< credit returned to the Completions on consume (§2.2)
    uint32_t reserved0 = 0;      ///< BatchAck: producerEpoch; GetTopicSchema: the topic's partitionCount
                                 ///< (Phase 2b Part C; 0 = a pre-2b broker that does not report it);
                                 ///< DDL ops: the generated requestId's lo (0 = the caller's explicit
                                 ///< requestId — Client::requestIdOf)
};
static_assert(sizeof(Completion) == 40);

/// One BatchAck credit unit: the reservation of one producer completion record
/// (CompletionRing::completionRecordBytes(0); asserted in Completions.h).  A
/// BatchAck returns reservedBytes = units * kBatchAckUnitBytes — the units are
/// derived from it, so reserved0 is free to carry the batch's producerEpoch.
inline constexpr uint32_t kBatchAckUnitBytes = 48;
[[nodiscard]] constexpr uint32_t batchAckUnits(const Completion& c) noexcept {
    return c.reservedBytes / kBatchAckUnitBytes;
}
static_assert(std::is_trivially_copyable_v<Completion>);

/// Callback target: a plain function pointer + context.  Called on the duty
/// thread (the Thread-mode runner, or the Invoker owner inside doWork()) under
/// the client's callback try/catch; it must not block or wait.  `ctx` must stay
/// valid until its op's completion has been delivered, including a `Closed`
/// delivered by Client::close().
struct CompletionHandler {
    void (*fn)(void* ctx, const Completion&, std::span<const uint8_t> payload) noexcept = nullptr;
    void* ctx = nullptr;

    /// Binds a member function `void C::m(const Completion&, std::span<const uint8_t>)`
    /// without allocating.
    template <auto Member, class C>
    static CompletionHandler of(C& obj) noexcept {
        return CompletionHandler{
            [](void* c, const Completion& done, std::span<const uint8_t> payload) noexcept {
                (static_cast<C*>(c)->*Member)(done, payload);
            },
            &obj};
    }

    void operator()(const Completion& c, std::span<const uint8_t> payload) const noexcept {
        fn(ctx, c, payload);
    }
};

/// A registered Completions' reference in the Client's duty-side registry
/// (spec §2.2): a 10-bit slot index and a 5-bit generation.  A completion
/// addressed to a released (and possibly reused) slot fails the generation
/// check and is dropped, never misdelivered.  kNoCqRef: not registered.
using CqRef = uint16_t;
inline constexpr CqRef    kNoCqRef        = 0xFFFF;
inline constexpr uint32_t kCqRefIndexBits = 10;
inline constexpr uint32_t kMaxCqs         = 1u << kCqRefIndexBits;  // registry capacity
/// 5-bit generation.  "Dropped, never misdelivered" does NOT rest on its
/// width: a released slot is quarantined until every pending op that targets
/// it has completed (the duty side counts them), so a slot is never recycled
/// while a stale completion for it can still be produced.  The generation is a
/// backstop (and distinguishes successive owners for debugging).
inline constexpr uint32_t kCqRefGenMask   = 0x1F;
[[nodiscard]] constexpr CqRef makeCqRef(uint32_t index, uint32_t gen) noexcept {
    return static_cast<CqRef>((index & (kMaxCqs - 1)) | ((gen & kCqRefGenMask) << kCqRefIndexBits));
}
[[nodiscard]] constexpr uint32_t cqRefIndex(CqRef r) noexcept { return r & (kMaxCqs - 1); }
[[nodiscard]] constexpr uint32_t cqRefGen(CqRef r) noexcept { return (r >> kCqRefIndexBits) & kCqRefGenMask; }

/// Synchronous result of ISSUING an op: only Accepted is ever followed by a
/// completion.
enum class OpResult : uint8_t {
    Accepted,        ///< exactly one completion will follow
    Closed,          ///< Client (or this handle / queue) closed
    QueueFull,       ///< the target Completions has no credit; poll() then retry
    BackPressured,   ///< command / producer ring full (non-blocking try* forms only)
    InvalidArgument, ///< invalid topic handle, token > kMaxToken, value too large, ...
    WrongThread,     ///< Completions used off its owner thread, or an Invoker op off the owner
};

} // namespace rune::client
