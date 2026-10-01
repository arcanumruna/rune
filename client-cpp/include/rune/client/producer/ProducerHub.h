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
//     hub.onProduceResponse(correlationId, baseOffset)  (O(1) by batch-id ordinal)
//
// Threading model (shared-nothing, single-owner — NO atomics, NO locks):
//   The producer list is owned SOLELY by the runner (agent) thread.  Every
//   method below runs on that thread: the duty-cycle iterators (drainAndFlush,
//   onProduceResponse, sweepTimedOut, hasInFlight, resend*, partitionForCorrId,
//   firstPendingPartition, updateIngressRoute) AND registration (add).
//
//   Client::producer() is called from an APP thread, but it does NOT touch this
//   list directly.  Instead it hands the new ProducerImpl to the runner thread
//   via the agent's M2O command ring (kMsgRegisterProducer) — the same
//   ring-buffer mechanism used for every other app→runner event.  The runner
//   drains that command and calls add() itself.  The list is therefore a plain
//   std::vector mutated by exactly one thread: no atomic snapshot, no RCU
//   retire-list, no registration mutex.
//
//   Shutdown: failAll()/~ProducerHub run on the app thread ONLY after
//   agent_.stop() has joined the runner thread, so they are single-threaded too.
//
//   Cross-thread handoff is a ring buffer, per the shared-nothing law — there is
//   no shared mutable state and no atomics anywhere in this component.

#pragma once

#include <rune/client/producer/ProducerImpl.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace rune::client::producer {

// Alias for the producer list (runner-thread-owned; plain vector).
using ImplList = std::vector<std::shared_ptr<ProducerImpl>>;

class ProducerHub {
public:
    ProducerHub() = default;

    // Non-copyable, non-movable.
    ProducerHub(const ProducerHub&) = delete;
    ProducerHub& operator=(const ProducerHub&) = delete;
    ProducerHub(ProducerHub&&) = delete;
    ProducerHub& operator=(ProducerHub&&) = delete;

    ~ProducerHub() = default;  // vector frees the shared_ptrs; runs after join.

    // ---- Registration — RUNNER-THREAD ONLY -----------------------------------
    //
    // Called by the runner thread when it drains a kMsgRegisterProducer command
    // from the agent ring (Client::producer() submits it from the app thread).
    // A plain push_back on the single-owner vector — no lock, no atomic.  Assigns
    // the producer's ordinal (its index; encoded into its batch correlation ids).
    // Throws std::length_error past batchcorr::kMaxOrdinal + 1 producers.

    void add(std::shared_ptr<ProducerImpl> impl);

    // ---- Agent-thread duty cycle (implemented in ProducerHub.cpp) -----------

    /// Drain all producer rings and flush ready batches through `sink` (a plain
    /// call; see ProducerImpl::flushBatches for the Sink concept).
    template <class Sink>
    int drainAndFlush(int64_t nowNs, int64_t sessionId, const Sink& sink) {
        int work = 0;
        for (auto& impl : producers_) {
            // fix1 minor 9: publish the consumer-liveness heartbeat ONCE per
            // producer per cycle (not once per drainRing() call below, which can
            // run up to 256x/cycle — same relaxed store either way, just cheaper).
            impl->publishHeartbeat(nowNs);
            // Drain ONE message then flush, so flush() boundaries between sends are
            // honoured; bounded at 256 records per producer per cycle.
            static constexpr int kMaxInnerIters = 256;
            for (int iter = 0; iter < kMaxInnerIters; ++iter) {
                const int drained = impl->drainRing(nowNs);
                const int flushed = impl->flushBatches(nowNs, sessionId, sink);
                work += drained + flushed;
                if (drained == 0) break;
            }
            work += impl->retransmitStalled(nowNs, sink);
        }
        return work;
    }

    /// Route a ProduceResponse to the ONE producer named by the batch id's
    /// ordinal (O(1)); non-batch ids (transactional / control) are ignored.
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Fail all pending acks on close.
    void failAll(const std::string& reason);

    /// fix1 C2/I3: mark every registered producer's ring closed. Called by
    /// Client::closeImpl_ while the Client (and its pump-hook contexts) are
    /// still fully valid, before the transport is torn down — a Producer that
    /// outlives this Client then gets SendResult::Closed from trySubmit() and
    /// never touches the (by-then-dangling) pump hook.
    void closeAllRings() noexcept {
        for (auto& impl : producers_) impl->markRingClosed();
    }

    /// Fail every record of the in-flight batch with this wire correlationId
    /// (forwards to ProducerImpl::failBatch; non-batch ids are ignored).
    /// Used by the Redirect retarget-failure path to fail ONLY the produce that
    /// triggered the redirect, leaving all other pending produces intact so the
    /// client session can continue serving other partitions.
    void failPending(int64_t corrId, const std::string& reason);

    /// Sweep timed-out acks (called from Client::sweepTimeouts).
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    bool empty() const noexcept {
        // Runner-thread-only read of the single-owner vector — no lock, no atomic.
        return producers_.empty();
    }
    std::size_t size() const noexcept { return producers_.size(); }

    /// CB-5 B: attempt to resend a redirected Produce identified by correlationId.
    /// Returns true if one producer handled it (resent or failed).
    bool retryRedirectedProduce(int64_t corrId,
                                std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: resend every in-flight Produce across all producers after a
    /// leader redirect + reconnect, re-stamped with the new sessionId.
    /// Returns the total number of frames resent.
    int resendAllRedirected(int64_t newSessionId,
                            std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn);

    /// Task 1.4 (redirect convergence): re-offer every in-flight frame across all
    /// producers through its partition-leader route (multi-node).  No global
    /// re-stamp, no redirect-budget consumption (the agent send path re-stamps the
    /// per-leader sessionId).  Returns the total number of frames offered.
    int resendAllViaRoute(std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn);

    /// CB-5 B: true if any producer has an in-flight (unacked) batch.
    [[nodiscard]] bool hasInFlight() const noexcept;

    /// MD1 review IMPORTANT-1: find the (topicId, partition) for an in-flight
    /// corrId across all producers.  Returns true and sets the out params if
    /// found; false if unknown.  Agent-thread only (redirect handler).
    [[nodiscard]] bool partitionForCorrId(int64_t corrId,
                                          int32_t& topicId,
                                          int32_t& partition) noexcept;

    /// MD1 review IMPORTANT-2: return the (topicId, partition) of the first
    /// partition with pending or in-flight produce work across all producers.
    /// Returns true and sets the out params if any exists; false otherwise.
    /// Agent-thread only (produce drain routing).
    [[nodiscard]] bool firstPendingPartition(int32_t& topicId,
                                             int32_t& partition) const noexcept;

    /// E5.1: Update the per-worker ingressStreamId for a (topicId, partition)
    /// across ALL registered producers.  Called from Client::onMetadataResponse
    /// on the agent thread.
    void updateIngressRoute(int32_t topicId, int32_t partition,
                            int32_t ingressStreamId) noexcept;


private:
    // ---- Single-owner producer list (runner thread only) ---------------------
    // Mutated by add() and iterated by every duty-cycle method — all on the one
    // runner thread.  No atomic, no lock: the shared-nothing law delivers
    // registration via the agent ring (kMsgRegisterProducer), so this vector is
    // never touched concurrently.  Shutdown paths run after the runner is joined.
    ImplList producers_;
};

} // namespace rune::client::producer
