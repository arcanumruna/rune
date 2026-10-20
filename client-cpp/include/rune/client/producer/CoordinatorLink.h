// include/rune/client/producer/CoordinatorLink.h - Phase 2b F (FD5 / FD6): a producer's coordinator request
// slots.
//
// Owned by the producer's duty thread (shared-nothing: no atomics, no locks).  A fixed pool of
// kMaxCoordRequests slots with preallocated kCoordFrameCap-byte frames; each slot is a per-state variant
// (Free / Unsent / InFlight / Backoff / AwaitCoordinator) holding only what that state needs.  Correlation
// ids are coordcorr::make(ordinal, slot, seq): the Client routes an answer to its producer by the ordinal.
// Retries run on the duty clock (wall time), never in duty cycles.  ZERO allocation after construction.
#pragma once

#include <rune/client/producer/TxnTypes.h>

#include <array>
#include <cstddef>
#include <cstdint>
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
    void onCoordinatorMoved() noexcept { ++gen_; }   ///< AwaitCoordinator slots become due
    void onSessionChanged() noexcept;                ///< InFlight -> Unsent (same corrId)
    [[nodiscard]] std::size_t inUse() const noexcept { return live_; }
    /// Next sequence to be handed out minus one: ProducerHub carries it to the ordinal's next incarnation.
    [[nodiscard]] uint32_t seq() const noexcept { return seq_; }
    /// Test hook: pump passes that scanned the slots (an idle link never does).
    [[nodiscard]] uint64_t scans() const noexcept { return scans_; }
    [[nodiscard]] uint32_t ordinal() const noexcept { return ordinal_; }

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

}  // namespace rune::client::producer
