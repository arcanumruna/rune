// include/rune/client/producer/CoordinatorLink.h - Phase 2b F (FD5 / FD6): a producer's coordinator request
// slots.
//
// Owned by the producer's duty thread (shared-nothing: no atomics, no locks).  A fixed pool of
// kMaxCoordRequests slots with preallocated kCoordFrameCap-byte frames; each slot is a per-state variant
// (Free / Unsent / InFlight / Backoff / AwaitCoordinator) holding only what that state needs.  Correlation
// ids are coordcorr::make(ordinal, slot, seq): the Client routes an answer to its producer by the ordinal.
// Retries run on the duty clock (wall time), never in duty cycles.  ZERO allocation after construction.
#pragma once

#include <rune/client/Completion.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/producer/TxnTypes.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <variant>

namespace rune::client::producer {

class CoordinatorLink {
public:
    static constexpr std::size_t kMaxCoordRequests = 8;
    static constexpr std::size_t kCoordFrameCap    = 4096;
    static constexpr int64_t     kBackoffBaseNs    = 1'000'000;     // 1 ms
    static constexpr int64_t     kBackoffCapNs     = 100'000'000;   // 100 ms
    static constexpr std::size_t kHdrSessionOff    = 8;             // MessageHeader.sessionId
    static constexpr std::size_t kHdrCorrOff       = 16;            // MessageHeader.correlationId

    struct Free {};
    struct Building {};   ///< acquired, the caller is encoding; submit() makes it Unsent
    struct Unsent {};
    struct InFlight { int64_t sessionId = 0; int64_t answerByNs = 0; };
    struct Backoff  { int64_t untilNs = 0; };
    struct AwaitCoordinator { uint64_t gen = 0; };
    using SlotState = std::variant<Free, Building, Unsent, InFlight, Backoff, AwaitCoordinator>;
    struct Slot {
        SlotState state = Free{};
        ReqKind   kind  = ReqKind::InitProducerId;
        int64_t   corrId = 0;
        uint32_t  attempt = 0;
        int64_t   deadlineNs = 0;   ///< absolute duty-clock expiry, > 0
        uint16_t  frameLen = 0;
        std::array<uint8_t, kCoordFrameCap> frame{};
    };

    /// `seqBase`: the ordinal's carried-forward request sequence (ProducerHub keeps it per ordinal, like the
    /// batch sequences): ids never repeat across incarnations of an ordinal, so a late answer to a closed
    /// producer's request can never match the new one's.  The 32-bit sequence wraps only after 2^32 requests
    /// on one ordinal (~50 days at one per millisecond), far past any answer's lifetime.
    CoordinatorLink(uint32_t ordinal, CoordinatorSink sink, uint32_t seqBase = 0) noexcept
        : ordinal_(ordinal), seq_(seqBase), sink_(sink) {}

    /// A free slot, or nullptr (the caller retries next pass).  The slot carries its correlation id
    /// (`corrId`); the caller encodes the request into slot->frame with that id and sets frameLen, then
    /// submit() makes it Unsent.  ZERO allocation.  deadlineNs MUST be > 0 (absolute duty clock): every slot, in every state, expires by it.
    /// A non-positive deadline is refused (nullptr; asserts in debug).
    [[nodiscard]] Slot* acquire(ReqKind kind, int64_t deadlineNs) noexcept;
    /// Unsent: pump() offers it.  Stamps the frame's header correlation id.
    void submit(Slot& s) noexcept;

    /// One duty pass: offer Unsent / due Backoff / AwaitCoordinator-after-move slots; expire deadlines.
    /// `onExpired(slot)` reports a slot whose deadline passed (the caller completes its op Timeout); the
    /// link releases it afterwards, so a deadline fires exactly once and a late answer finds no slot.
    /// Returns the work done (offers + expiries).
    template <class OnExpired>
    int pump(int64_t nowNs, OnExpired&& onExpired) {
        if (live_ == 0) return 0;   // idle: touches no slot memory
        ++scans_;
        int work = 0;
        for (Slot& s : slots_) {
            if (std::holds_alternative<Free>(s.state) || std::holds_alternative<Building>(s.state)) continue;
            if (nowNs >= s.deadlineNs) {
                onExpired(s);
                release(s);
                ++work;
                continue;
            }
            work += offerIfDue_(s, nowNs);
        }
        return work;
    }

    /// An answer for one of our ids: classify (FD5 / FD6), retry in place or hand it to the caller.
    /// Returns the slot to the caller (then release()) unless the answer was a retriable one or matches
    /// no live slot (late / duplicate).
    [[nodiscard]] Slot* onAnswer(const CoordAnswer& a, int64_t nowNs) noexcept;
    /// Idempotent.
    void release(Slot& s) noexcept;
    /// Every slot back to Free (a terminal failure: fenced / revoked / closeImmediately - nothing is ever
    /// offered or re-sent again for them).  Idempotent.
    void releaseAll() noexcept {
        for (Slot& s : slots_) release(s);
    }
    void onCoordinatorMoved() noexcept { ++gen_; }   ///< AwaitCoordinator slots become due
    void onSessionChanged() noexcept;                ///< InFlight -> Unsent (same corrId)
    [[nodiscard]] std::size_t inUse() const noexcept { return live_; }
    /// Next sequence to be handed out minus one: ProducerHub carries it to the ordinal's next incarnation.
    [[nodiscard]] uint32_t seq() const noexcept { return seq_; }
    /// Test hook: pump passes that scanned the slots (an idle link never does).
    [[nodiscard]] uint64_t scans() const noexcept { return scans_; }
    [[nodiscard]] uint32_t ordinal() const noexcept { return ordinal_; }
    /// The coordinator session exists (the sink reports a non-zero id): a request offered now is sent.
    [[nodiscard]] bool connected() const noexcept {
        return sink_.sessionId != nullptr && sink_.sessionId(sink_.ctx) != 0;
    }

    /// The verdict table (FD5 / FD6; Kafka parity, TransactionalProducerImpl::classify_ plus auth).
    [[nodiscard]] static Verdict classify(ReqKind kind, int16_t errorCode) noexcept;
    /// min(1 ms << (attempt - 1), 100 ms); attempt 0 -> 0.
    [[nodiscard]] static int64_t backoffNs(uint32_t attempt) noexcept;

private:
    int offerIfDue_(Slot& s, int64_t nowNs) noexcept;
    bool offer_(Slot& s) noexcept;

    std::array<Slot, kMaxCoordRequests> slots_{};
    uint32_t        ordinal_;
    uint32_t        seq_;
    uint32_t        live_ = 0;   // slots not Free (Building included)
    uint64_t        scans_ = 0;
    uint64_t        gen_ = 0;
    CoordinatorSink sink_;
};

// ---- Phase 2b F (FD4): the producer identity machine, one state per phase, each holding only its data ----
// (Declared here, not in TxnTypes.h: IdRequested names CoordinatorLink::Slot.)

/// FD4: a bare ProducerImpl with no link wired (unit rigs, benches) starts IdAssigned with this id.
inline constexpr int64_t kRigProducerId = 0x7E57'0000'0000'0001LL;

struct IdUnassigned {};
struct IdRequested { CoordinatorLink::Slot* slot = nullptr; };
struct IdAssigned  { int64_t producerId = -1; int16_t epoch = 0; };
struct IdFailed    { OpStatus status = OpStatus::BrokerError; uint16_t errorCode = 0; };
using ProducerIdentity = std::variant<IdUnassigned, IdRequested, IdAssigned, IdFailed>;
inline constexpr std::size_t kIdAssignedIndex = 2;   // index of IdAssigned in ProducerIdentity (static_asserted below)
static_assert(std::is_same_v<std::variant_alternative_t<kIdAssignedIndex, ProducerIdentity>, IdAssigned>);

// ---- Phase 2b F Task 5: the transaction machine, one state per phase, each holding only its data ----
// (Declared here, not in TxnTypes.h, for the same reason: the states name CoordinatorLink::Slot.)  Owned by
// ProducerImpl's duty side; the alternative index IS the TxnState value (static_asserted in ProducerImpl.h).
// Today's TxnSm booleans became states / enums: started + maybeStarted -> TxnStarted, endSent -> EndSent,
// bumpEpoch -> AbortKind, ended -> Aborting{started = No, phase = Bumping} (a retried abort re-sends only the
// bump), opId == 0 (in doubt) -> InDoubt.

/// One pending lifecycle op: where its single completion goes and when it times out.  An EMPTY target (no
/// handler, no queue) is the close's own abort: its outcome is the close's, never delivered on its own.
struct TxnOp {
    detail::OpTarget target{};
    Token            token      = 0;
    int64_t          deadlineNs = 0;   ///< absolute duty clock (transactionOpTimeoutMs)
};
/// m5 / m13: calls that joined a pending commit / abort complete with it (one EndTxn).  Fixed capacity (zero
/// allocation): a call beyond it completes BrokerError(19).
inline constexpr std::size_t kMaxTxnJoiners = 4;
struct TxnJoiners {
    std::array<TxnOp, kMaxTxnJoiners> ops{};
    uint8_t                           n = 0;
};

struct AwaitData {};                                          ///< EndTxn waits for every record of the txn to be acked
struct EndSent  { CoordinatorLink::Slot* slot = nullptr; };   ///< EndTxn on the link
struct InDoubt  {};                                           ///< the op timed out: the app retries the SAME direction (B-2)
struct Bumping  { CoordinatorLink::Slot* slot = nullptr; };   ///< InitProducerId after EndTxn(abort) (B-3); nullptr = not yet sent
using EndPhase = std::variant<AwaitData, EndSent, InDoubt, Bumping>;

struct Uninitialized {};
struct Initializing   { TxnOp op; CoordinatorLink::Slot* slot = nullptr; };   ///< slot nullptr = not yet sent
struct Ready          {};
struct InTransaction  { TxnStarted started = TxnStarted::No; };
struct Committing     { TxnOp op; EndPhase phase = AwaitData{}; TxnStarted started = TxnStarted::No; TxnJoiners joined{}; };
struct Aborting       {
    TxnOp      op;
    EndPhase   phase   = AwaitData{};
    TxnStarted started = TxnStarted::No;
    AbortKind  kind    = AbortKind::Plain;
    TxnJoiners joined{};
};
struct AbortableError {
    OpStatus   status    = OpStatus::BrokerError;
    uint16_t   errorCode = 0;
    TxnStarted started   = TxnStarted::No;
    AbortKind  kind      = AbortKind::Plain;
};
struct FatalError     { OpStatus status = OpStatus::Fenced; uint16_t errorCode = 0; };
using TxnMachine = std::variant<Uninitialized, Initializing, Ready, InTransaction, Committing, Aborting,
                                AbortableError, FatalError>;

}  // namespace rune::client::producer
