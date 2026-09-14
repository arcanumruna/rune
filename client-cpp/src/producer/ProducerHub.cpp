// src/producer/ProducerHub.cpp
//
// CB-2 — ProducerHub non-inline method implementations.
//
// DR-BUG-1 fix (wait-free raw-pointer snapshot):
//   All hot-path methods load snapshot_ once with std::memory_order_acquire —
//   a genuinely wait-free, lock-free, refcount-free atomic pointer load —
//   then iterate the immutable ImplList.  The list is never mutated once
//   published; add() publishes a NEW ImplList with memory_order_release and
//   retains the old pointer in owned_ so in-flight readers remain safe.
//
//   Hot-path cost per cycle: one atomic pointer load (MOV + LDAR on AArch64,
//   MOV on x86-64 with implicit acquire).  Zero spinlocks, zero refcount
//   operations, zero per-cycle heap allocation.

#include <rune/client/producer/ProducerHub.h>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// add() — cold path (app thread, serialised by addMtx_)
//
// 1. Load the current live pointer (safe: we hold addMtx_ so no concurrent
//    add(); the runner thread may concurrently read snapshot_ but only reads
//    the ImplList, never writes it).
// 2. Build a new ImplList (copy of current + new entry).
// 3. Stash the new raw pointer in owned_ BEFORE publishing, so it is always
//    reclaimed at destruction even if destruction races construction (safe
//    because owned_ is only touched under addMtx_ during add()).
// 4. Publish with release so the runner thread's acquire load sees all writes
//    to *next.
// ---------------------------------------------------------------------------

void ProducerHub::add(std::shared_ptr<ProducerImpl> impl)
{
    std::lock_guard<std::mutex> lk(addMtx_);

    // Load the current live pointer (relaxed is fine here: we hold addMtx_
    // so no other add() can modify snapshot_; the runner thread only reads).
    const ImplList* current = snapshot_.load(std::memory_order_relaxed);

    // Build a new list with the extra entry.
    auto* next = new ImplList(*current);
    next->push_back(std::move(impl));

    // Retain ownership before publishing (so destructor always sees it).
    owned_.push_back(next);

    // Publish with release: runner thread's acquire load will see *next fully.
    snapshot_.store(next, std::memory_order_release);
}

// ---------------------------------------------------------------------------
// Hot-path helpers — single wait-free acquire load per call, then iterate.
// Zero locks, zero refcount operations, zero per-call allocations.
// ---------------------------------------------------------------------------

int ProducerHub::drainAndFlush(int64_t nowNs,
                                int64_t sessionId,
                                std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    // Single wait-free acquire load — no spinlock, no refcount inc/dec.
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);

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
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->onProduceResponse(correlationId, baseOffset);
    }
}

void ProducerHub::failAll(const std::string& reason) {
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->failAllPending(reason);
    }
}

bool ProducerHub::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(const uint8_t*, std::size_t)> sendFn)
{
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
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
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
    int resent = 0;
    for (auto& impl : *snap) {
        resent += impl->resendAllRedirected(newSessionId, sendFn);
    }
    return resent;
}

bool ProducerHub::hasInFlight() const noexcept {
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
    for (const auto& impl : *snap) {
        if (impl->hasInFlight()) return true;
    }
    return false;
}

void ProducerHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    const ImplList* snap = snapshot_.load(std::memory_order_acquire);
    for (auto& impl : *snap) {
        impl->sweepTimedOut(now);
    }
}

} // namespace rune::client::producer
