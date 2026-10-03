#pragma once
// ---------------------------------------------------------------------------
// WorkerEgress.h — per-worker Aeron dynamic-MDC egress publication (read-rework
// T3/T4).
//
// Gives one worker its OWN egress ExclusivePublication so each worker pushes its
// owned partitions' Records directly to consumers, in parallel, with NO gateway
// funnel.  The worker thread is the SOLE writer of this publication (shared-
// nothing): the control thread never touches it.  Consumers self-register via
// DYNAMIC MDC (validated in MdcEgressSpikeTest) — the driver auto-adds them on
// connect and auto-removes on liveness timeout, so the publisher never manages
// the destination list (no cross-thread destination state).
//
// ## Channel
//   aeron:udp?control-mode=dynamic|control=<controlEndpoint>|fc=min
//   where controlEndpoint = <broker-advertised-host>:<CTRL_BASE + workerId>.
//   Clients subscribe to that control address (control-mode=dynamic) and are
//   auto-added as MDC destinations.
//
// ## Design (mirrors WorkerIngress)
//   - Resolves the publication via a bounded addExclusivePublication +
//     findExclusivePublication resolve via AeronPump (until Aeron answers — AeronPump::kUntilAnswered, bounded by Aeron's driverTimeoutMs; 1 ms
//     sleep with the default pump, invoker pumping otherwise),
//     the same pattern used across the gateway/ingress.
//   - Non-throwing construction; check resolved() after construction.
//   - SINGLE-OWNER / SINGLE-THREAD: one WorkerEgress per worker, written only on
//     that worker's thread.  No locks, no atomics.
//   - Zero-copy send: tryClaim for frames <= maxPayloadLength (one copy into the
//     term buffer), offer() (auto-fragmented) for larger frames — same policy as
//     the gateway egress path.
// ---------------------------------------------------------------------------

#include <rune/aeronutil/AeronPump.h>

#include <Aeron.h>
#include <spdlog/spdlog.h>
#include <ExclusivePublication.h>
#include <concurrent/AtomicBuffer.h>
#include <concurrent/logbuffer/BufferClaim.h>
#include <util/Index.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace rune::node {

class WorkerEgress {
public:
    // -----------------------------------------------------------------------
    // Construction — resolves a dynamic-MDC ExclusivePublication on
    // (controlEndpoint, streamId).  Non-throwing; check resolved().
    // controlEndpoint is a host:port the broker binds for MDC control (where
    // subscribers connect); it is embedded in the channel's control= field.
    // -----------------------------------------------------------------------
    WorkerEgress(std::shared_ptr<aeron::Aeron> aeron,
                 std::string                   controlEndpoint,
                 int32_t                       streamId,
                 rune::aeronutil::AeronPump    pump   = {},
                 std::chrono::milliseconds     budget = rune::aeronutil::AeronPump::kUntilAnswered) noexcept
        : aeron_(std::move(aeron))
        , controlEndpoint_(std::move(controlEndpoint))
        , streamId_(streamId)
    {
        if (!aeron_) return;  // null aeron_ → unresolved (unit tests w/o driver)

        channel_ = "aeron:udp?control-mode=dynamic|control=" + controlEndpoint_;

        // A driver registration error (bad channel, ...) makes add/find THROW;
        // this ctor is noexcept, so it becomes a logged resolve failure.
        bool failed = false;
        try {
            const int64_t regId = aeron_->addExclusivePublication(channel_, streamId_);
            pub_ = pump.resolve([&]() -> std::shared_ptr<aeron::ExclusivePublication> {
                try {
                    return aeron_->findExclusivePublication(regId);
                } catch (const std::exception& e) {
                    spdlog::error("WorkerEgress: publication {} stream {} failed: {}", channel_, streamId_, e.what());
                    failed = true;
                    return nullptr;
                }
            }, budget, &failed);
        } catch (const std::exception& e) {
            spdlog::error("WorkerEgress: addExclusivePublication {} stream {} failed: {}", channel_, streamId_, e.what());
        }
        // pub_ == nullptr → resolved() returns false; caller handles.
    }

    // Non-copyable, non-movable (single-owner resource handle).
    WorkerEgress(const WorkerEgress&)            = delete;
    WorkerEgress& operator=(const WorkerEgress&) = delete;
    WorkerEgress(WorkerEgress&&)                 = delete;
    WorkerEgress& operator=(WorkerEgress&&)      = delete;
    ~WorkerEgress() = default;

    [[nodiscard]] bool     resolved() const noexcept { return pub_ != nullptr; }
    // Raw pub accessor — used by the worker's push loop to send to the shared MDC
    // pub via the common sendEncodedToPub_ helper (broadcast/hybrid fallback).
    [[nodiscard]] aeron::ExclusivePublication* pub() const noexcept { return pub_.get(); }
    [[nodiscard]] int32_t  streamId() const noexcept { return streamId_; }
    [[nodiscard]] const std::string& controlEndpoint() const noexcept { return controlEndpoint_; }
    [[nodiscard]] const std::string& channel() const noexcept { return channel_; }

    // True once at least one MDC destination (consumer) is connected.
    [[nodiscard]] bool isConnected() const noexcept {
        return pub_ && pub_->isConnected();
    }

    // Max single-fragment payload (frames <= this go via tryClaim).
    [[nodiscard]] std::int32_t maxPayloadLength() const noexcept {
        return pub_ ? pub_->maxPayloadLength() : 0;
    }

    // -----------------------------------------------------------------------
    // send — publish one encoded frame [data, data+len).  MUST be called only
    // on the owning worker thread.  Returns the Aeron publication result:
    //   > 0  : accepted (new position)
    //   <= 0 : back-pressure / not-connected / error (caller does not advance
    //          the cursor and retries next cycle).
    // Zero extra copy beyond the single term-buffer write (tryClaim) or Aeron's
    // fragmentation buffer (offer for > MTU).
    // -----------------------------------------------------------------------
    int64_t send(const uint8_t* data, std::size_t len) {
        if (!pub_) [[unlikely]] return -1;
        auto* mutableBuf = const_cast<uint8_t*>(data);
        aeron::concurrent::AtomicBuffer buf(mutableBuf, static_cast<aeron::util::index_t>(len));
        if (len <= static_cast<std::size_t>(pub_->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            int64_t pos = pub_->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                claim.buffer().putBytes(claim.offset(), buf, 0,
                                        static_cast<aeron::util::index_t>(len));
                claim.commit();
            }
            return pos;
        }
        // Larger than one MTU fragment: offer() fragments it; the client's egress
        // FragmentAssembler reassembles.
        return pub_->offer(buf, 0, static_cast<aeron::util::index_t>(len));
    }

private:

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::string                                  controlEndpoint_;
    int32_t                                      streamId_;
    std::string                                  channel_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
};

} // namespace rune::node
