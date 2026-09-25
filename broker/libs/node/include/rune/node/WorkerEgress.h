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
//     findExclusivePublication loop (kResolveMaxMs=5000 / kResolveSliceMs=1),
//     the same pattern used across the gateway/ingress.
//   - Non-throwing construction; check resolved() after construction.
//   - SINGLE-OWNER / SINGLE-THREAD: one WorkerEgress per worker, written only on
//     that worker's thread.  No locks, no atomics.
//   - Zero-copy send: tryClaim for frames <= maxPayloadLength (one copy into the
//     term buffer), offer() (auto-fragmented) for larger frames — same policy as
//     the gateway egress path.
// ---------------------------------------------------------------------------

#include <Aeron.h>
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
                 int32_t                       streamId) noexcept
        : aeron_(std::move(aeron))
        , controlEndpoint_(std::move(controlEndpoint))
        , streamId_(streamId)
    {
        if (!aeron_) return;  // null aeron_ → unresolved (unit tests w/o driver)

        channel_ = "aeron:udp?control-mode=dynamic|control=" + controlEndpoint_ + "|fc=min";

        int64_t regId = aeron_->addExclusivePublication(channel_, streamId_);
        using clock = std::chrono::steady_clock;
        auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
        while (clock::now() < deadline) {
            pub_ = aeron_->findExclusivePublication(regId);
            if (pub_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
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
    static constexpr int kResolveMaxMs   = 5000;
    static constexpr int kResolveSliceMs = 1;

    std::shared_ptr<aeron::Aeron>                aeron_;
    std::string                                  controlEndpoint_;
    int32_t                                      streamId_;
    std::string                                  channel_;
    std::shared_ptr<aeron::ExclusivePublication> pub_;
};

} // namespace rune::node
