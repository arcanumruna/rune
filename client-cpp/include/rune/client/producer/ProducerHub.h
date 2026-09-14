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
// Threading model (DR-BUG-1 fix — lock-free copy-on-write snapshot):
//   Client::producer() may be called from the APP thread AFTER the runner
//   thread has started.  To avoid a data race on the producer list, the list
//   is published behind an atomically-swapped shared_ptr snapshot:
//
//     add() — COLD PATH (app thread):
//       Acquires addMtx_, builds a new vector (copy of current + new impl),
//       and publishes it with a release store into snapshot_.  This is the
//       only place that writes snapshot_.
//
//     Hot iterators (drainAndFlush, onProduceResponse, failAll,
//       sweepTimedOut, hasInFlight, resendAllRedirected,
//       retryRedirectedProduce) — HOT PATH (runner/agent thread):
//       Load snapshot_ once with acquire at the start of each call (one
//       atomic load per cycle — no lock, no per-cycle heap allocation).
//       Then iterate the immutable snapshot.  Because the snapshot is a
//       shared_ptr<const vector<...>>, the vector is never mutated once
//       published; any concurrent add() creates a NEW vector rather than
//       modifying the existing one.
//
//   Result: the hot path holds NO mutex and performs NO heap allocation per
//   cycle.  The cold path (add) does one allocation of a new vector — only
//   at producer-registration time, which is infrequent.

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

// Alias for the immutable producer list snapshot.
using ImplList = std::vector<std::shared_ptr<ProducerImpl>>;
using ImplSnapshot = std::shared_ptr<const ImplList>;

class ProducerHub {
public:
    ProducerHub()
        : snapshot_(std::make_shared<const ImplList>())
    {}

    // Non-copyable, non-movable.
    ProducerHub(const ProducerHub&) = delete;
    ProducerHub& operator=(const ProducerHub&) = delete;
    ProducerHub(ProducerHub&&) = delete;
    ProducerHub& operator=(ProducerHub&&) = delete;

    // ---- Registration (may be called from any thread after agent start()) ---
    //
    // Cold path: acquires addMtx_, allocates a new vector (old + impl),
    // and publishes it with memory_order_release so the runner thread's
    // acquire load in the hot path sees the new entry.

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

    /// Sweep timed-out acks (called from Client::sweepTimeouts).
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    bool empty() const noexcept {
        return std::atomic_load_explicit(&snapshot_, std::memory_order_acquire)->empty();
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


private:
    // ---- Lock-free snapshot -----------------------------------------------
    // Published by add() with release; loaded by hot-path methods with acquire.
    // std::atomic_load/store on shared_ptr is standard (C++20 std::atomic<shared_ptr>
    // is preferred but shared_ptr atomic free functions are available in C++11+).
    // We use the free-function forms for broadest compiler support.
    std::shared_ptr<const ImplList> snapshot_;

    // ---- Cold-path mutex (serialises concurrent add() calls) ---------------
    // Only acquired by add(); never held during hot-path iteration.
    std::mutex addMtx_;
};

} // namespace rune::client::producer
