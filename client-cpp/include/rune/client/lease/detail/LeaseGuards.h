// include/rune/client/lease/detail/LeaseGuards.h - the produce deadline guard of a LeaseSession (Task 15; spec 12.2, R5).
//
// DUTY-THREAD OWNED (shared-nothing): the session registers a block of `capacity` guard slots (GuardReserve), arms a slot at
// each keepalive answer (GuardArm: the instant at which the lease becomes AT RISK), and binds producers to a slot through the
// PRODUCER's ring (kMsgGuardBind, FIFO with that producer's writes).  When the duty cycle passes an Armed slot's deadline it
// FAILS every record the bound producers accepted and not yet acknowledged (DeliveryFailed, errorCode LeaseAtRisk), and the
// slot is Tripped: bound producers append and send nothing until a GuardArm re-arms it (Cleared).  The session learns of it
// through OpKind::LeaseGuard completions into its own queue.
//
// Event credit (no completion is ever lost): the session reserves a pool of P = 2*Capacity + 8 records for its whole life.
// The duty side delivers a guard event only while posted - acked < P; the session acks the number of events it consumed
// (GuardAck, at most one per doWork).  Edges that find the pool exhausted wait in a per-slot pending queue and COALESCE:
// a slot keeps at most two pending edges (alternating Tripped / Rearmed), and a third replaces the pair with itself
// (T,R,T,R delivers as T,R with the latest armSeq: the final state is never lost, an intermediate Entered/Cleared pair may be).
#pragma once

#include <rune/client/Completion.h>
#include <rune/client/detail/OpTarget.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace rune::client::producer {
class ProducerImpl;
}

namespace rune::client::detail {

inline constexpr uint32_t kMaxGuardProducers = 8;

/// OpKind::LeaseGuard completion: value = slot, aux = gen, token = the session id, reserved0 = event | armSeq << 8.
enum class GuardEvent : uint8_t { Reserved = 1, ReserveFailed = 2, Tripped = 3, Rearmed = 4, Ended = 5, BindRefused = 6 };

[[nodiscard]] constexpr uint32_t guardEventWord(GuardEvent e, uint32_t armSeq) noexcept {
    return static_cast<uint32_t>(e) | (armSeq << 8);
}

struct GuardEntry {
    enum class State : uint8_t { Free, Armed, Tripped, Ended };
    uint32_t                gen      = 0;
    State                   state    = State::Free;
    uint32_t                armSeq   = 0;
    int64_t                 atRiskNs = 0;
    producer::ProducerImpl* producers[kMaxGuardProducers]{};
    uint8_t                 nProducers = 0;
    // pending (undelivered) edges, oldest first
    GuardEvent              edge[2]{};
    uint32_t                edgeSeq[2]{};
    uint8_t                 nEdge     = 0;
    bool                    pendEnded = false;
    int32_t                 next      = -1;     ///< intrusive pending list
    bool                    listed    = false;
};

class LeaseGuards {
public:
    /// Cold: sizes the session table (ClientConfig::maxLeaseSessions) and sets the delivery sink.
    void configure(uint32_t maxSessions, CompletionSink sink);

    /// GuardReserve.  `target` is the session queue's pinned target.  Delivers Reserved / ReserveFailed.
    void reserve(uint32_t sid, uint32_t capacity, uint32_t pool, const OpTarget& target);
    void arm(uint32_t sid, uint32_t slot, uint32_t gen, uint32_t armSeq, int64_t atRiskNs, int64_t nowNs);
    void end(uint32_t sid, uint32_t slot, uint32_t gen);
    /// GuardRelease: frees the block; returns its pinned target (kNoCqRef cqRef when there was none).
    OpTarget release(uint32_t sid);
    void ack(uint32_t sid, uint64_t consumedTotal);
    /// Attaches `p` (kMsgGuardBind, duty thread); a refusal kills the producer's guard (blocked for good) and posts BindRefused.
    void bind(uint32_t sid, uint32_t slot, uint32_t gen, producer::ProducerImpl& p);
    /// The producer is going away (duty thread).
    void unbindProducer(producer::ProducerImpl& p) noexcept;
    /// Duty cycle, after the agent drain: trips every Armed slot whose deadline passed.  One compare when none is due.
    int trip(int64_t nowNs);

    [[nodiscard]] bool has(uint32_t sid) const noexcept { return find_(sid) != nullptr; }
    [[nodiscard]] uint64_t testPosted(uint32_t sid) const noexcept;
    [[nodiscard]] uint64_t testAcked(uint32_t sid) const noexcept;
    [[nodiscard]] int testState(uint32_t sid, uint32_t slot) const noexcept;
    [[nodiscard]] uint64_t droppedBindRefused() const noexcept { return droppedBindRefused_; }

private:
    struct Block {
        bool                        used = false;
        uint32_t                    sid = 0;
        uint32_t                    capacity = 0;
        uint32_t                    pool = 0;
        OpTarget                    target{};
        std::unique_ptr<GuardEntry[]> entries;
        uint64_t                    posted = 0;
        uint64_t                    acked  = 0;
        int32_t                     head = -1, tail = -1;
    };
    [[nodiscard]] Block* find_(uint32_t sid) noexcept;
    [[nodiscard]] const Block* find_(uint32_t sid) const noexcept;
    void deliver_(Block& b, GuardEvent ev, uint32_t slot, uint32_t gen, uint32_t seq) noexcept;
    void pushEdge_(Block& b, uint32_t slot, GuardEvent ev, uint32_t seq) noexcept;
    void enlist_(Block& b, uint32_t slot) noexcept;
    void flush_(Block& b) noexcept;
    void failProducers_(GuardEntry& e) noexcept;
    void recomputeMin_() noexcept;

    std::vector<Block> blocks_;
    CompletionSink     sink_{};
    int64_t            minArmedNs_ = INT64_MAX;
    uint64_t           droppedBindRefused_ = 0;
};

}  // namespace rune::client::detail
