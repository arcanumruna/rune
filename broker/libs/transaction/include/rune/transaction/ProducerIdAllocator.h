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
        if (next_ >= blockCeiling_) {
            // Replenish from the next block.
            blockCeiling_ = next_ + kBlockSize;
        }
        return next_++;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// The next id that will be allocated (monotonically increasing).
    [[nodiscard]] ProducerId nextId() const noexcept { return next_; }

    /// The current block ceiling (ids in [startOfBlock, blockCeiling_) are
    /// allocated from the current block). P5-3 persists this.
    [[nodiscard]] ProducerId blockCeiling() const noexcept { return blockCeiling_; }

private:
    ProducerId next_{0};
    ProducerId blockCeiling_{0};
};

} // namespace rune::transaction
