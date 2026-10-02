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

// Alias for the producer list (runner-thread-owned; plain vector).  F8c C7:
// raw pointers to the impls the duty side holds its share of (§3.3); a slot
// is the producer's ordinal, nullptr once its producer was released (removed).
using ImplList = std::vector<ProducerImpl*>;

class ProducerHub {
public:
    ProducerHub() = default;

    // Non-copyable, non-movable.
    ProducerHub(const ProducerHub&) = delete;
    ProducerHub& operator=(const ProducerHub&) = delete;
    ProducerHub(ProducerHub&&) = delete;
    ProducerHub& operator=(ProducerHub&&) = delete;

    ~ProducerHub() = default;  // holds nothing after handBackAll() (Client close).

    // ---- Registration — RUNNER-THREAD ONLY -----------------------------------
    //
    // Called by the runner thread when it drains a kMsgRegisterProducer command
    // from the agent ring (Client::producer() submits it from the app thread).
    // A plain push_back on the single-owner vector — no lock, no atomic.  Assigns
    // the producer's ordinal (its index; encoded into its batch correlation ids).
    // Throws std::length_error past batchcorr::kMaxOrdinal + 1 producers.

    void add(ProducerImpl* impl);

    /// F8c C7: forget a released, quiescent producer (its slot becomes a
    /// tombstone; the ordinal is reused only once every fresh one is taken,
    /// oldest first).  The caller detaches the duty share.
    void remove(ProducerImpl* impl) noexcept;

    /// Client close (after closeFinalAll): every producer still held is
    /// handed back through `detach(impl)`; the hub holds nothing afterwards.
    template <class Detach>
    void handBackAll(Detach&& detach) noexcept {
        ImplList all;
        all.swap(producers_);
        active_.clear();
        for (ProducerImpl* impl : all) {
            if (impl != nullptr) detach(impl);
        }
    }

    /// The producers held (live, non-tombstone slots) — any order.
    template <class F>
    void forEach(F&& f) {
        for (std::size_t i = 0; i < active_.size(); ++i) {
            if (active_[i] != nullptr) f(*active_[i]);
        }
    }

    // ---- Agent-thread duty cycle (implemented in ProducerHub.cpp) -----------

    /// Drain all producer rings and flush ready batches through `sink` (a plain
    /// call; see ProducerImpl::flushBatches for the Sink concept).
    template <class Sink>
    int drainAndFlush(int64_t nowNs, int64_t sessionId, const Sink& sink) {
        int work = 0;
        for (ProducerImpl* impl : active_) {
            if (impl == nullptr) [[unlikely]] continue;  // F8c C7: a released slot
            // fix1 minor 9: publish the consumer-liveness heartbeat ONCE per
            // producer per cycle (not once per drainRing() call below, which can
            // run up to 256x/cycle — same relaxed store either way, just cheaper).
            impl->publishHeartbeat(nowNs);
            // F12: drain a whole pass, then flush ONCE (flushBatches walks only
            // the dirty partitions).  The old loop drained ONE message and then
            // flushed — a walk over EVERY partition per drained record (256 x
            // partitions per producer per cycle).  The drain pass itself stops at
            // the boundaries that need a flush in between (a flush() token; a
            // record whose builder is full / whose Tier-2 batch is queued ahead):
            // flush there, then drain on.  Bounded at kMaxDrainPerCycle messages
            // per producer per cycle (as before); a Direct producer's drain is a
            // no-op, so it gets exactly one flush pass.
            static constexpr int kMaxDrainPerCycle = 256;
            int budget = kMaxDrainPerCycle;
            for (int pass = 0; pass < kMaxDrainPerCycle; ++pass) {
                const int drained = impl->drainRing(nowNs, budget);
                const int flushed = impl->flushBatches(nowNs, sessionId, sink);
                work += drained + flushed;
                budget -= drained;
                const ProducerImpl::DrainStop stop = impl->lastDrainStop();
                if (budget <= 0 || stop == ProducerImpl::DrainStop::Drained) break;
                // Blocked and the flush freed nothing: draining again is futile
                // this cycle (window full / no session / transport back-pressure).
                if (stop == ProducerImpl::DrainStop::Blocked && flushed == 0) break;
            }
            work += impl->retransmitStalled(nowNs, sink);
            // F8c C6a: a closing producer's begin-close completion / deadline.
            if (!impl->lifeOpen()) [[unlikely]] work += impl->closeDuty(nowNs);
        }
        return work;
    }

    /// Route a ProduceResponse to the ONE producer named by the batch id's
    /// ordinal (O(1)); non-batch ids (transactional / control) are ignored.
    void onProduceResponse(int64_t correlationId, int64_t baseOffset);

    /// Fail every pending ack with `st` (redirect-reconnect failure:
    /// SessionLost; the producers stay open).
    void failAll(OpStatus st);

    /// Client close (after closeAllRings): every producer completes everything
    /// it accepted with `st` exactly once, draining its closed ring to the
    /// frozen tail (bounded by the close-drain unblock rule).
    void closeFinalAll(OpStatus st, int stalledYieldsBeforeUnblock);

    /// T15b: true when any producer's intake is closed (begin-close draining,
    /// closed, or discarding).  Duty thread; O(producers).
    [[nodiscard]] bool anyClosing() const noexcept {
        for (const ProducerImpl* impl : active_) {
            if (impl != nullptr && !impl->lifeOpen()) return true;
        }
        return false;
    }

    /// F8c C6a: the duty-thread failure tail-close of every producer (T15:
    /// the Aeron client closed under a shard).  Idempotent.
    void failCloseAll(OpStatus st) {
        for (ProducerImpl* impl : active_) {
            if (impl != nullptr) impl->failClose(st);
        }
    }

    /// fix1 C2/I3: mark every registered producer's ring closed. Called by
    /// Client::closeImpl_ while the Client (and its pump-hook contexts) are
    /// still fully valid, before the transport is torn down — a Producer that
    /// outlives this Client then gets SendResult::Closed from trySubmit() and
    /// never touches the (by-then-dangling) pump hook.
    void closeAllRings() noexcept {
        for (ProducerImpl* impl : active_) {
            if (impl != nullptr) impl->markRingClosed();
        }
    }

    /// Fail every record of the in-flight batch with this wire correlationId
    /// (DeliveryFailed; forwards to ProducerImpl::failBatch; non-batch ids are
    /// ignored).  Used by the Redirect retarget-failure path to fail ONLY the
    /// produce that triggered the redirect, leaving all other pending produces
    /// intact so the client session can continue serving other partitions.
    void failPending(int64_t corrId);

    /// Sweep timed-out acks (called from Client::sweepTimeouts).
    void sweepTimedOut(const std::chrono::steady_clock::time_point& now);

    bool empty() const noexcept {
        // Runner-thread-only read of the single-owner vector — no lock, no atomic.
        return active_.empty();
    }
    /// Producers held (released + forgotten ones excluded).
    std::size_t size() const noexcept { return active_.size(); }

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
    ImplList producers_;                  // by ordinal (ProduceResponse routing); nullptr = released
    ImplList active_;                     // m2: the live producers, dense (every duty walk)
    std::vector<uint32_t> freeOrdinals_;  // F8c C7: released ordinals, FIFO (reused once fresh run out)
    std::size_t freeHead_ = 0;            // next freeOrdinals_ entry to reuse
    std::vector<uint64_t> nextBase_;      // per ordinal: the next incarnation's sequence base (F2)
};

} // namespace rune::client::producer
