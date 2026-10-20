// include/rune/client/producer/TxnTypes.h - Phase 2b F: the vocabulary shared by the producer's coordinator
// link and (later tasks) its identity / transaction machines.
//
// Task 2 content: the coordinator correlation-id space, the request kinds, a decoded coordinator answer,
// the error verdict table's result type and the plain-function sink the link offers frames through.
#pragma once

#include <rune/client/producer/InFlightRing.h>

#include <cstddef>
#include <cstdint>
#include <span>

namespace rune::client::producer {

namespace coordcorr {
/// Coordinator request ids: `kTag | ordinal << 40 | slot << 32 | seq`.
///
/// Tag choice (differs from the plan's 1 << 59, which is the Client's own counter tag - every control op,
/// ping and DDL draw `kClientCorrIdTag | n` from it, so the two spaces
/// overlapped for n < 2^32): bit 58 is free, and `isCoord` masks bits 58..62 so a Client counter id (bit 59)
/// is never mistaken for a coordinator id.
inline constexpr int64_t kClientCounterTag = int64_t{1} << 59;   // Client::kClientCorrIdTag (static_asserted there)
inline constexpr int64_t kTag              = int64_t{1} << 58;
inline constexpr int64_t kFetchTag         = int64_t{1} << 61;   // GroupConsumerHub::kFetchCorrTag
inline constexpr int64_t kOffsetFetchTag   = int64_t{1} << 60;   // GroupConsumerHub::kOffsetFetchCorrTag
static_assert((kTag & (batchcorr::kTag | kFetchTag | kOffsetFetchTag | kClientCounterTag)) == 0,
              "coordinator ids must not collide with batch / fetch / offset-fetch / Client-counter tags");
inline constexpr int64_t kTagMask = int64_t{0x1F} << 58;

[[nodiscard]] constexpr int64_t make(uint32_t ordinal, uint8_t slot, uint32_t seq) noexcept {
    return kTag | (static_cast<int64_t>(ordinal & batchcorr::kMaxOrdinal) << 40) |
           (static_cast<int64_t>(slot) << 32) | static_cast<int64_t>(seq);
}
[[nodiscard]] constexpr bool isCoord(int64_t c) noexcept { return c > 0 && (c & kTagMask) == kTag; }
[[nodiscard]] constexpr uint32_t ordinal(int64_t c) noexcept {
    return static_cast<uint32_t>(c >> 40) & batchcorr::kMaxOrdinal;
}
[[nodiscard]] constexpr uint8_t slot(int64_t c) noexcept { return static_cast<uint8_t>(c >> 32); }
}  // namespace coordcorr

enum class ReqKind : uint8_t { InitProducerId, AddPartitions, AddOffsets, TxnOffsetCommit, EndTxn };

// ---- Phase 2b F Task 5: the transaction machine's per-state facts (the old TxnSm booleans as enums) ----

/// B-1 (Kafka transactionStarted): did the coordinator see this transaction?  No = nothing was ever added (it
/// ends locally, no EndTxn); Maybe = a leg was sent but never answered (m15: abort sends EndTxn(abort) and a 19
/// then means "nothing open"); Yes = an AddPartitions / AddOffsets was acknowledged OK.
enum class TxnStarted : uint8_t { No, Maybe, Yes };
/// B-3 (KIP-360): a sequence gap (a lost batch) makes the abort bump the epoch (InitProducerId after
/// EndTxn(abort)) and restart every sequence.
enum class AbortKind : uint8_t { Plain, BumpEpoch };

/// Coordinator answer, decoded (the link never interprets kind-specific bodies beyond errorCode).
/// `anyKind`: an ErrorResponse - it matches whichever request owns `corrId`.
struct CoordAnswer {
    ReqKind                  kind;
    int64_t                  corrId;
    int16_t                  errorCode;
    std::span<const uint8_t> frame;
    bool                     anyKind = false;
};

/// What an answer's (kind, errorCode) means (FD5 / FD6).  The link retries Retry / Rediscover itself and
/// hands every other verdict to the caller.
enum class Verdict : uint8_t {
    Ok,
    Retry,        ///< 27 ConcurrentTransactions (any kind); EndTxn: any code that is not final
    Rediscover,   ///< 14 NotCoordinator: wait for the coordinator move, then re-send
    Fenced,       ///< 17 / 18: FatalError(Fenced)
    Fatal,        ///< FatalError(BrokerError ec): InitProducerId any other code, 45 on InitProducerId / EndTxn, EndTxn 19
    Abortable,    ///< AbortableError(BrokerError ec): AddPartitions / AddOffsets / TxnOffsetCommit failures, 45 there
    Reject,       ///< 39 InvalidRequest: never retried; BrokerError(39) (AbortableError inside a transaction)
};

/// Function pointer + context (no std::function): offer one frame to the coordinator session.
struct CoordinatorSink {
    enum class Offer : uint8_t { Accepted, Retry, Fatal };
    Offer (*offer)(void* ctx, const uint8_t* buf, std::size_t len) noexcept = nullptr;
    int64_t (*sessionId)(void* ctx) noexcept = nullptr;   // 0 = not connected
    void* ctx = nullptr;
    /// NotCoordinator was answered: ask for a fresh cluster view (the Client sends a MetadataRequest).  Called by
    /// the link at most once per coordinator generation (until onCoordinatorMoved), on the duty thread.  May be null.
    void (*rediscover)(void* ctx) noexcept = nullptr;
};

}  // namespace rune::client::producer
