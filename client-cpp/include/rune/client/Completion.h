#pragma once
// Completion.h — F8c: the result of an asynchronous client operation, without
// std::future / std::promise (spec 2026-10-01-f8c-no-futures-design.md §2.1).
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
};

enum class OpStatus : uint8_t {
    Ok = 0,
    BrokerError,     ///< errorCode = broker ErrorCode; payload = message text (truncated)
    Timeout,         ///< the op's own duty-side deadline expired
    Closed,          ///< Client closed or the owning handle closed before the result
    NotConnected,    ///< session not Connected when the duty thread processed it
    Superseded,      ///< commitSync replaced by a newer commitSync
    Rejoined,        ///< group generation changed under a commitSync
    SessionLost,     ///< session dropped after the request was sent (no answer possible)
    Fenced,          ///< txn: ProducerFenced / InvalidProducerEpoch
    DeliveryFailed,  ///< producer: delivery timeout / redirect budget / deferred-buffer full
    ResultTooLarge,  ///< polled form only: variable result > the reservation (value = real size)
};

/// One completion record: 40 bytes, trivially copyable.  A variable payload
/// (ping echo, schema descriptor blob, error text) follows it inline.
struct Completion {
    Token    token = 0;          ///< as passed at issue
    int64_t  value = 0;          ///< Send/Tier2: baseOffset (-1 if none); Connect: sessionId;
                                 ///< DefineTopic/GetTopicSchema: topicId; ResultTooLarge: real size
    int64_t  aux   = 0;          ///< GetTopicSchema/DefineTopic: version; Tier2: recordCount
    OpKind   op{};
    OpStatus status = OpStatus::Ok;
    uint16_t errorCode = 0;      ///< broker ErrorCode when status == BrokerError
    uint32_t payloadLen = 0;     ///< bytes of inline payload
    uint32_t reservedBytes = 0;  ///< credit returned to the Completions on consume (§2.2)
    uint32_t reserved0 = 0;
};
static_assert(sizeof(Completion) == 40);
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
