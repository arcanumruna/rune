#pragma once
// ---------------------------------------------------------------------------
// ProducerIdRecovery.h — recoverProducerIdWatermark: reseed a
// ProducerIdAllocator from the highest producerId found in a partition log.
//
// Namespace: rune::transaction
// C++23.  Header-only template.
//
// ## Purpose
//
// ProducerIdAllocator is purely in-memory.  On a fresh start (failover or
// restart) it resets to 0 and would re-issue PIDs that already appear in the
// durable log → two live producer sessions share a PID → silent EOS break
// (ProducerStateManager dedup / LSO is keyed by producerId).
//
// recoverProducerIdWatermark() fixes this by scanning the committed portion of
// the log (up to the high-watermark) for the maximum producerId ever written,
// then calling ProducerIdAllocator::reseedAtLeast(maxSeen) so the next
// allocation is strictly greater than any already-used PID.
//
// ## Template parameter
//
// LogT: concrete log type — rune::storage::Log or rune::storage::InMemoryLog.
// NO ILog / virtual dispatch (de-virtualized seam, Milestone D Phase 2).
// Mirror pattern from ProducerStateManager::rebuildFromLog<LogT>.
// ---------------------------------------------------------------------------

#include <rune/transaction/ProducerIdAllocator.h>
#include <rune/protocol/ProducerIdBinding.h>
#include <rune/storage/ProducerIdScan.h>

namespace rune::transaction {

// ---------------------------------------------------------------------------
// recoverProducerIdWatermark<LogT>
//
// Scans [logStartOffset, highWatermark) in `log`, finds the maximum
// producerId, then reseeds `alloc` so the next allocation is strictly above
// it.  If the log contains no idempotent batches the allocator is left
// unchanged.
//
// @param alloc         The allocator to reseed (in-place, non-const).
// @param log           Concrete log (Log or InMemoryLog).
// @param highWatermark Committed HW (exclusive).  Must equal replica HW, NOT LEO.
// ---------------------------------------------------------------------------
template<class LogT>
void recoverProducerIdWatermark(ProducerIdAllocator& alloc,
                                LogT&                log,
                                int64_t              highWatermark) noexcept
{
    const ProducerId maxSeen = rune::storage::scanMaxProducerId(log, highWatermark);
    if (maxSeen >= 0) {
        alloc.reseedAtLeast(rune::protocol::producerIdCounter(maxSeen));   // #122: counter units
    }
}

} // namespace rune::transaction
