// include/rune/client/producer/ProducerHub.h
//
// CB-2 — ProducerHub: single point that ClientAgent calls on the agent thread
// for all producer duty-cycle work.
//
// Design:
//   ProducerHub holds a list of ProducerImpl instances created by Client::producer().
//   On each doWork cycle the agent calls:
//     hub.drainAndFlush(nowNs, sessionId, sendFn)
//   When a ProduceResponse arrives (dispatchEgressFrame), the agent calls:
//     hub.onProduceResponse(correlationId, baseOffset, topicId, partition)
//
// Threading model (DR-BUG-1 fix — wait-free raw-pointer snapshot):
//   Client::producer() may be called from the APP thread AFTER the runner
//   thread has started.  To avoid a data race on the producer list while
//   keeping the hot path genuinely wait-free (no spinlock, no refcount churn),
//   the list is published behind an atomic raw pointer + retire-list:
//
//     add() — COLD PATH (app thread, serialised by addMtx_):
//       Allocates a new ImplList (copy of current + new impl), publishes it
//       with a release store into snapshot_, and moves the previous raw
//       pointer into owned_ (a vector of unique_ptrs under addMtx_).
//       Any in-flight hot-path reader that already loaded the old pointer
//       iterates an immutable ImplList that remains alive until destruction.
//       Producers are never removed and add() is rare, so owned_ is bounded
//       (at most N entries for N registered producers) and trivially small.
//       All lists — including the current live one — are freed at ~ProducerHub.
//
//     Hot iterators (drainAndFlush, onProduceResponse, failAll,
//       sweepTimedOut, hasInFlight, resendAllRedirected,
//       retryRedirectedProduce, empty) — HOT PATH (runner/agent thread):
//       ONE snapshot_.load(std::memory_order_acquire) — a genuinely lock-free,
//       refcount-free pointer load — then iterate *ptr.  Because the ImplList
//       is never mutated once published and its lifetime extends until
//       ~ProducerHub, no hazard-pointer or refcount is needed.
//
//   Result: the hot path is WAIT-FREE — a single atomic pointer load, zero
//   refcount operations, zero spinlocks, zero per-cycle heap allocation.
//   The cold path (add) does one allocation of a new ImplList — only at
//   producer-registration time, which is infrequent.
//
//   std::atomic<const ImplList*> is always lock-free (pointer width is natively
//   supported by all hardware Rune targets).

#pragma once

#include <rune/client/producer/ProducerImpl.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

namespace rune::client::producer {

// Alias for the immutable producer list.
using ImplList = std::vector<std::shared_ptr<ProducerImpl>>;

class ProducerHub {
public:
    ProducerHub() {
        // Publish a valid empty list so hot-path loads are never null.
        auto* empty = new ImplList{};
        snapshot_.store(empty, std::memory_order_relaxed);
        owned_.emplace_back(empty);
    }

    // Non-copyable, non-movable.
    ProducerHub(const ProducerHub&) = delete;
    ProducerHub& operator=(const ProducerHub&) = delete;
    ProducerHub(ProducerHub&&) = delete;
    ProducerHub& operator=(ProducerHub&&) = delete;

    ~ProducerHub() {
        // owned_ holds unique_ptrs for ALL ImplList allocations (including the
        // live one).  Release them here.  The raw pointer in snapshot_ is not
        // separately freed — it is always one of the owned_ entries.
        // No lock needed: destruction must happen after all threads have stopped.
        for (auto* p : owned_) delete p;
        owned_.clear();
    }

    // ---- Registration (may be called from any thread after agent start()) ---
    //
    // Cold path: acquires addMtx_, allocates a new ImplList (copy of current +
    // impl), publishes it with memory_order_release, and retains the old pointer
    // in owned_ for safe deferred freeing at destruction.

    void add(std::shared_ptr<ProducerImpl> impl);

    // ---- Agent-thread duty cycle (implemented in ProducerHub.cpp) -----------

    /// Drain all producer rings and flush ready batches.
    /// sendFn: the agent's transport send function.
    /// Returns total work count (drained + flushed).
    int drainAndFlush(int64_t nowNs,
                      int64_t sessionId,
                      std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// Route a ProduceResponse to all producers (the one that owns the
    /// correlationId will resolve the ack; others ignore it).
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Fail all pending acks on close.
    void failAll(const std::string& reason);

    /// Fail a single pending ack identified by correlationId.
    /// Used by the Redirect retarget-failure path to fail ONLY the produce that
    /// triggered the redirect, leaving all other pending produces intact so the
    /// client session can continue serving other partitions.
    void failPending(int64_t corrId, const std::string& reason);

    /// Sweep timed-out acks (called from Client::sweepTimeouts).
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    bool empty() const noexcept {
        // Single wait-free acquire load — no lock, no refcount.
        return snapshot_.load(std::memory_order_acquire)->empty();
    }

    /// CB-5 B: attempt to resend a redirected Produce identified by correlationId.
    /// Returns true if one producer handled it (resent or failed).
    bool retryRedirectedProduce(int64_t corrId,
                                std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: resend every in-flight Produce across all producers after a
    /// leader redirect + reconnect, re-stamped with the new sessionId.
    /// Returns the total number of frames resent.
    int resendAllRedirected(int64_t newSessionId,
                            std::function<bool(const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: true if any producer has an in-flight (unacked) batch.
    [[nodiscard]] bool hasInFlight() const noexcept;

    /// MD1 review IMPORTANT-1: find the (topicId, partition) for an in-flight
    /// corrId across all producers.  Returns true and sets the out params if
    /// found; false if unknown.  Agent-thread only (redirect handler).
    [[nodiscard]] bool partitionForCorrId(int64_t corrId,
                                          int32_t& topicId,
                                          int32_t& partition) const noexcept;

    /// MD1 review IMPORTANT-2: return the (topicId, partition) of the first
    /// partition with pending or in-flight produce work across all producers.
    /// Returns true and sets the out params if any exists; false otherwise.
    /// Agent-thread only (produce drain routing).
    [[nodiscard]] bool firstPendingPartition(int32_t& topicId,
                                             int32_t& partition) const noexcept;


private:
    // ---- Wait-free snapshot pointer ------------------------------------------
    // Published by add() with release; loaded by hot-path methods with acquire.
    // std::atomic<const ImplList*> is lock-free on all platforms (pointer load
    // is a native instruction — no spinlock, no refcount).
    // Never null: constructor stores a valid empty ImplList before any thread
    // can call hot-path methods.
    std::atomic<const ImplList*> snapshot_{nullptr};

    // ---- Retire list + live owner (cold path only, under addMtx_) ------------
    // Holds owning raw pointers to every ImplList ever allocated, including the
    // currently live one.  add() appends the newly allocated list here before
    // publishing it.  Freed at destruction — safe because all threads are joined
    // before ~ProducerHub runs.
    // Using raw pointers (not unique_ptr) to keep the vector trivially movable
    // and avoid any potential overhead in the destructor loop.
    std::vector<const ImplList*> owned_;   // guarded by addMtx_ during add()

    // ---- Cold-path mutex (serialises concurrent add() calls) ---------------
    // Only acquired by add(); never held during hot-path iteration.
    std::mutex addMtx_;
};

} // namespace rune::client::producer
