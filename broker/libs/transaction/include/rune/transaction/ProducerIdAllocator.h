#pragma once
// ---------------------------------------------------------------------------
// ProducerIdAllocator.h — Monotonic producerId allocator.
//
// Namespace: rune::transaction
// C++23. NO locks. Single-writer.
//
// ## Overview
//
// Allocates monotonically increasing producerIds in blocks. Each allocation
// returns the next unique ProducerId (int64_t). A brand-new non-transactional
// producer always gets a fresh PID.
//
// This is a pure in-memory allocator; persistence/recovery of the "next PID"
// across crashes is the responsibility of P5-3's transaction log. The allocator
// always starts at 0 (for tests) or at a recovered watermark (for production).
//
// Block allocation:
//   The allocator reserves ids in blocks of kBlockSize. When the current block
//   is exhausted, the "watermark" (highest allocated block ceiling) would be
//   persisted by P5-3 before more ids are handed out. The abstraction here is
//   purely deterministic — tests always get monotonic ids from 0 upward.
//
// ## Single-writer, no locks, no hot-path allocation.
// ---------------------------------------------------------------------------

#include <rune/transaction/Types.h>

#include <cassert>
#include <cstdint>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// ProducerIdAllocator
// ---------------------------------------------------------------------------
class ProducerIdAllocator {
public:
    /// Block size: number of ids reserved per "generation".
    /// In a full system the transaction log would persist the block ceiling
    /// before any ids in the block are used. Default 1000 matches common usage.
    static constexpr ProducerId kBlockSize = 1000;

    /// Sentinel returned by allocate() when the producerId space is exhausted
    /// (next_ has reached INT64_MAX). Callers must treat this as an allocation
    /// failure — NEVER wrap to a negative producerId (Kafka treats producerId as
    /// a non-negative int64; a negative value would collide with the -1 "no PID"
    /// sentinel and corrupt dedup keying).
    static constexpr ProducerId kExhausted = -1;

    /// F15 I7: sentinel returned by allocate() in GATED mode when the next id
    /// lies at or above the last COMMITTED block ceiling (the next block's
    /// ceiling record is not yet majority-durable in __transaction_state).
    /// Retriable: the caller answers ConcurrentTransactions (27).
    static constexpr ProducerId kBlockNotCommitted = -2;

    /// Construct with a starting base (defaults to 0; P5-3 passes recovered watermark).
    explicit ProducerIdAllocator(ProducerId startingBase = 0) noexcept
        : next_(startingBase)
        , blockCeiling_(startingBase)  // allocate on first call
    {}

    // Non-copyable (holds monotonic counter state).
    ProducerIdAllocator(const ProducerIdAllocator&)            = delete;
    ProducerIdAllocator& operator=(const ProducerIdAllocator&) = delete;

    // Movable (for P5-3 recovery).
    ProducerIdAllocator(ProducerIdAllocator&&)                 = default;
    ProducerIdAllocator& operator=(ProducerIdAllocator&&)      = default;

    // -----------------------------------------------------------------------
    // allocate() — returns the next unique producerId.
    //
    // Deterministic and monotonically increasing. No hot-path allocation.
    // In production, the caller (TransactionCoordinator) ensures the block
    // ceiling is persisted before handing out ids — that's P5-3's job.
    // -----------------------------------------------------------------------
    [[nodiscard]] ProducerId allocate() noexcept {
        // Exhaustion guard (Part B #15): never wrap past INT64_MAX into negative.
        if (next_ == INT64_MAX) {
            return kExhausted;
        }
        // F15 I7 (Kafka ProducerIdManager): in gated mode an id is handed out
        // only from a block whose ceiling record is committed.
        if (gated_ && next_ >= committedLimit_) {
            return kBlockNotCommitted;
        }
        if (next_ >= blockCeiling_) {
            // Replenish from the next block, clamping the ceiling at INT64_MAX so
            // next_ + kBlockSize cannot overflow to a negative value.
            if (next_ > INT64_MAX - kBlockSize) {
                blockCeiling_ = INT64_MAX;
            } else {
                blockCeiling_ = next_ + kBlockSize;
            }
        }
        return next_++;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // reseedAtLeast() — advance the allocator so the NEXT allocation is strictly
    // greater than @p maxSeen.
    //
    // A1 fix: after replaying the durable TransactionLog, the coordinator must
    // reseed the allocator past the highest recovered producerId; otherwise the
    // default allocator (starting at 0) would re-hand-out producerIds that are
    // still live in the recovered metadata → two sessions share a PID → silent
    // EOS break (ProducerStateManager dedup/LSO is keyed by producerId).
    //
    // Idempotent and monotone: never moves the allocator backwards. Guards
    // against INT64_MAX overflow (leaves next_ at INT64_MAX rather than wrapping
    // negative).
    // -----------------------------------------------------------------------
    void reseedAtLeast(ProducerId maxSeen) noexcept {
        // We want next_ > maxSeen, i.e. next_ >= maxSeen + 1.
        if (maxSeen == INT64_MAX) {
            // Cannot represent maxSeen+1; park at INT64_MAX (allocate() then
            // detects exhaustion — see the overflow guard there).
            if (next_ < INT64_MAX) next_ = INT64_MAX;
        } else {
            ProducerId target = maxSeen + 1;
            if (next_ < target) next_ = target;
        }
        // Re-arm the block ceiling so the next allocate() replenishes cleanly.
        if (blockCeiling_ < next_) blockCeiling_ = next_;
    }

    // -----------------------------------------------------------------------
    // F15 I7 — persisted pid block ceiling (Kafka ProducerIdManager).
    //
    // commitLimit(): switch to GATED mode and raise the committed ceiling: ids
    // in [nextId(), limit) may be handed out because a ceiling record >= limit
    // is majority-durable in __transaction_state.  Monotone (never lowers).
    // Ungated allocators (store-less rigs, the disk TransactionLog coordinator)
    // keep the original unbounded behaviour.
    // -----------------------------------------------------------------------
    void commitLimit(ProducerId limit) noexcept {
        gated_ = true;
        if (limit > committedLimit_) committedLimit_ = limit;
    }

    /// Re-arm the gate at a coordinator LOAD: nothing may be handed out until a
    /// ceiling written by THIS load commits (a limit committed by an earlier
    /// load of this node is never reused, even though the reseed would already
    /// place nextId() above it).
    void resetGate() noexcept {
        gated_          = true;
        committedLimit_ = 0;
    }

    /// True once commitLimit() has been called (allocation is ceiling-gated).
    [[nodiscard]] bool gated() const noexcept { return gated_; }

    /// The last committed ceiling (exclusive) — meaningful only when gated().
    [[nodiscard]] ProducerId committedLimit() const noexcept { return committedLimit_; }

    /// The next id that will be allocated (monotonically increasing).
    [[nodiscard]] ProducerId nextId() const noexcept { return next_; }

    /// The current block ceiling (ids in [startOfBlock, blockCeiling_) are
    /// allocated from the current block). P5-3 persists this.
    [[nodiscard]] ProducerId blockCeiling() const noexcept { return blockCeiling_; }

private:
    ProducerId next_{0};
    ProducerId blockCeiling_{0};
    bool       gated_{false};
    ProducerId committedLimit_{0};
};

} // namespace rune::transaction
