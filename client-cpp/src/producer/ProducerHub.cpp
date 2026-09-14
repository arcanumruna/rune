// src/producer/ProducerHub.cpp
//
// CB-2 — ProducerHub non-inline method implementations.
//
// DR-BUG-1 fix: all hot-path methods load the snapshot_ atomically with
// acquire semantics once per call, then iterate the immutable list.  The
// snapshot is never mutated in place; add() publishes a new snapshot with
// release.  This makes every hot-path call lock-free and allocation-free
// while still being safe for concurrent add() from the app thread.

#include <rune/client/producer/ProducerHub.h>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// add() — cold path (app thread, serialised by addMtx_)
//
// Allocates one new ImplList vector (copy of current + new entry), then
// publishes it atomically with release.  Any hot-path call that had already
// loaded the old snapshot continues safely on the immutable old list; the
// next hot-path call will see the new snapshot via acquire.
// ---------------------------------------------------------------------------

void ProducerHub::add(std::shared_ptr<ProducerImpl> impl)
{
    std::lock_guard<std::mutex> lk(addMtx_);
    // Load the current snapshot (may be concurrently read on runner thread,
    // but we hold addMtx_ so no concurrent add() is possible).
    const ImplSnapshot current =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);

    // Build a new vector with the extra entry.
    auto next = std::make_shared<ImplList>(*current);
    next->push_back(std::move(impl));

    // Publish the new snapshot with release so the runner thread's acquire
    // load in any hot-path method sees all writes to *next.
    std::atomic_store_explicit(&snapshot_,
                               ImplSnapshot(std::move(next)),
                               std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Hot-path helpers — load snapshot once per call, iterate immutably.
// Zero locks, zero per-call allocations.
// ---------------------------------------------------------------------------

int ProducerHub::drainAndFlush(int64_t nowNs,
                                int64_t sessionId,
                                std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    // Single acquire load — no lock, no allocation.
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);

    int work = 0;
    for (auto& impl : *snap) {
        // Drain + flush in a tight loop: drain one message at a time so that
        // flush boundaries (kMsgFlush or batchBytes triggers) are honoured
        // between individual records.  This ensures that a flush() call between
        // two sends produces two separate batches (the expected user-facing
        // semantic).  We cap the loop at 256 iterations to bound CPU.
        static constexpr int kMaxInnerIters = 256;
        for (int iter = 0; iter < kMaxInnerIters; ++iter) {
            const int drained = impl->drainRing(nowNs);
            const int flushed = impl->flushBatches(nowNs, sessionId, sendFn);
            work += drained + flushed;
            if (drained == 0) break; // no more messages in the ring
        }
    }
    return work;
}

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset) {
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->onProduceResponse(correlationId, baseOffset);
    }
}

void ProducerHub::failAll(const std::string& reason) {
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->failAllPending(reason);
    }
}

bool ProducerHub::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    for (auto& impl : *snap) {
        if (impl->retryRedirectedProduce(corrId, sendFn)) {
            return true;
        }
    }
    return false;
}

int ProducerHub::resendAllRedirected(
    int64_t newSessionId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    int resent = 0;
    for (auto& impl : *snap) {
        resent += impl->resendAllRedirected(newSessionId, sendFn);
    }
    return resent;
}

bool ProducerHub::hasInFlight() const noexcept {
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    for (const auto& impl : *snap) {
        if (impl->hasInFlight()) return true;
    }
    return false;
}

void ProducerHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    const ImplSnapshot snap =
        std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->sweepTimedOut(now);
    }
}

} // namespace rune::client::producer
