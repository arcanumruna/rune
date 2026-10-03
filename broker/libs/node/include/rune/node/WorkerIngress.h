#pragma once
// ---------------------------------------------------------------------------
// WorkerIngress.h — per-worker Aeron subscription + zero-copy poll (E5.1.2a).
//
// Gives one worker its own Aeron subscription on a distinct stream id so
// workers poll the wire in parallel (per-worker ingress, Milestone E5a).
//
// ## Design
//   - Subscribes to (channel, streamId) via a bounded resolve loop that mirrors
//     ClientGateway.cpp:85-99 exactly: addSubscription + findSubscription loop,
//     kResolveMaxMs=5000 ms; via AeronPump (1 ms sleep with the default pump,
//     invoker pumping of the shard's own driver + conductor otherwise, §3.6).
//   - Reassembles multi-fragment messages (produces can exceed a single Aeron
//     MTU) with rune::aeronutil::FragmentAssembler — same as the gateway;
//     buffers of lost images are freed by an ImageLossWatch after each poll.
//   - poll<Handler>(fragmentLimit, handler) is TEMPLATED on Handler and the
//     assembler delegate is a template argument, all inlined into Aeron's
//     templated Subscription/Image poll: no std::function, no trampoline.
//   - ZERO-COPY: the handler receives a pointer into Aeron's term buffer valid
//     ONLY for the duration of the handler call.  Do NOT store the pointer.
//   - SINGLE-OWNER / SINGLE-THREAD: one WorkerIngress per worker, polled only
//     on that worker's thread.  No locks, no atomics.
//
// ## Handler signature
//   handler(const uint8_t* data, std::size_t len,
//           aeron::concurrent::logbuffer::Header& hdr)
//
// ## Failure model
//   Construction is non-throwing.  If the subscription is not resolved within
//   the bounded window, resolved() returns false and poll() is a no-op.
//   The caller must check resolved() after construction.
// ---------------------------------------------------------------------------

#include <rune/aeronutil/AeronPump.h>

#include <Aeron.h>
#include <spdlog/spdlog.h>
#include <Subscription.h>
#include <rune/aeronutil/FragmentAssembler.h>
#include <concurrent/AtomicBuffer.h>
#include <util/Index.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

namespace rune::node {

// ---------------------------------------------------------------------------
// WorkerIngress
// ---------------------------------------------------------------------------
class WorkerIngress {
public:
    // -----------------------------------------------------------------------
    // Construction
    //
    // Resolves an Aeron subscription on (channel, streamId) with a bounded
    // resolve loop that mirrors ClientGateway.cpp:85-99.
    // Non-throwing. Check resolved() after construction.
    // -----------------------------------------------------------------------
    WorkerIngress(std::shared_ptr<aeron::Aeron> aeron,
                  std::string                   channel,
                  int32_t                       streamId,
                  rune::aeronutil::AeronPump    pump   = {},
                  std::chrono::milliseconds     budget = rune::aeronutil::AeronPump::kUntilAnswered) noexcept
        : aeron_(std::move(aeron))
        , channel_(std::move(channel))
        , streamId_(streamId)
    {
        if (!aeron_) return;  // null aeron_ → unresolved

        // Add, then resolve (spec §3.6): the default pump sleeps 1 ms between
        // probes (legacy ClientGateway.cpp:85-99 behaviour); an invoker pump
        // drives this shard's own driver + conductor instead of sleeping.
        // A driver registration error (bad channel, ...) makes add/find THROW;
        // this ctor is noexcept, so it becomes a logged resolve failure.
        bool failed = false;
        try {
            const int64_t regId = aeron_->addSubscription(channel_, streamId_);
            sub_ = pump.resolve([&]() -> std::shared_ptr<aeron::Subscription> {
                try {
                    return aeron_->findSubscription(regId);
                } catch (const std::exception& e) {
                    spdlog::error("WorkerIngress: subscription {} stream {} failed: {}", channel_, streamId_, e.what());
                    failed = true;
                    return nullptr;
                }
            }, budget, &failed);
        } catch (const std::exception& e) {
            spdlog::error("WorkerIngress: addSubscription {} stream {} failed: {}", channel_, streamId_, e.what());
        }
        // sub_ == nullptr → resolved() returns false; caller handles.
    }

    // Non-copyable, non-movable (FragmentAssembler lambda captures `this`).
    WorkerIngress(const WorkerIngress&)            = delete;
    WorkerIngress& operator=(const WorkerIngress&) = delete;
    WorkerIngress(WorkerIngress&&)                 = delete;
    WorkerIngress& operator=(WorkerIngress&&)      = delete;

    ~WorkerIngress() = default;

    // -----------------------------------------------------------------------
    // resolved — true once the Subscription is live.
    // -----------------------------------------------------------------------
    [[nodiscard]] bool resolved() const noexcept { return sub_ != nullptr; }

    // -----------------------------------------------------------------------
    // streamId — the stream this ingress is subscribed to.
    // -----------------------------------------------------------------------
    [[nodiscard]] int32_t streamId() const noexcept { return streamId_; }

    // -----------------------------------------------------------------------
    // poll — drive the subscription; up to fragmentLimit reassembled messages.
    //
    // Handler signature:
    //   void handler(const uint8_t* data, std::size_t len,
    //                aeron::Header& hdr)
    //
    // LIFETIME CONTRACT: data is a zero-copy view into Aeron's term buffer,
    // valid ONLY for the duration of the handler invocation.  Never store it.
    //
    // Returns the number of fragments handled (add to doWork work count).
    // Returns 0 if not resolved().
    //
    // No std::function anywhere (Task 11 fix 2: rune templated assembler).
    // -----------------------------------------------------------------------
    template <class Handler>
    int poll(int fragmentLimit, Handler&& handler) {
        if (!sub_) [[unlikely]] return 0;

        // Task 11 fix 2 (B3): templated assembler + the caller's handler, all
        // inlined into Aeron's templated Subscription/Image poll — no
        // std::function, no trampoline.
        const int n = sub_->poll(
            [this, &handler](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                             aeron::util::index_t l, aeron::Header& h) {
                assembler_.onFragment(
                    [&handler](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                               aeron::util::index_t length, aeron::Header& header) {
                        handler(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                                static_cast<std::size_t>(length), header);
                    },
                    b, o, l, h);
            },
            fragmentLimit);
        imageWatch_.afterPoll(*sub_, assembler_);   // free buffers of lost images (m-c)
        return n;
    }

    // -----------------------------------------------------------------------
    // controlledPoll — as poll(), but the handler returns aeron::Controlled-
    // PollAction (Task 13 fix round 1, G-A6 per-client pause).  ABORT leaves
    // the message (its END fragment) on ITS image — one client publication —
    // and Aeron moves on to the other images.  Use ONE poll flavour per
    // WorkerIngress (separate reassembly state; a shard uses only this one).
    // -----------------------------------------------------------------------
    template <class Handler>
    int controlledPoll(int fragmentLimit, Handler&& handler) {
        if (!sub_) [[unlikely]] return 0;
        const int n = sub_->controlledPoll(
            [this, &handler](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                             aeron::util::index_t l, aeron::Header& h) {
                return controlledAssembler_.onFragment(
                    [&handler](aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                               aeron::util::index_t length, aeron::Header& header) {
                        return handler(reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                                       static_cast<std::size_t>(length), header);
                    },
                    b, o, l, h);
            },
            fragmentLimit);
        controlledImageWatch_.afterPoll(*sub_, controlledAssembler_);
        return n;
    }

private:
    // Mirror the exact constants from ClientGateway.cpp:49-50.
    static constexpr int kResolveMaxMs   = 5000;  // 5 s

    std::shared_ptr<aeron::Aeron>          aeron_;
    std::string                            channel_;
    int32_t                                streamId_;
    std::shared_ptr<aeron::Subscription>   sub_;

    rune::aeronutil::FragmentAssembler     assembler_;
    rune::aeronutil::ImageLossWatch        imageWatch_;
    rune::aeronutil::ControlledFragmentAssembler controlledAssembler_;   // controlledPoll only
    rune::aeronutil::ImageLossWatch              controlledImageWatch_;
};

} // namespace rune::node
