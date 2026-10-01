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
//   - Wraps an aeron::FragmentAssembler (member) to reassemble multi-fragment
//     messages (produces can exceed a single Aeron MTU) — same as the gateway.
//   - poll<Handler>(fragmentLimit, handler) is TEMPLATED on Handler so OUR
//     call site stays concrete and inlined.
//   - The only std::function / type-erasure crossing is Aeron's own
//     fragment_handler_t at the Subscription::poll boundary.  We bridge from
//     the fixed assembler callback to the concrete Handler via a raw-function-
//     pointer + void* trampoline (zero overhead, zero additional std::function
//     of our own).
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
                  std::chrono::milliseconds     budget = std::chrono::milliseconds{kResolveMaxMs}) noexcept
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
        return sub_->poll(
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
    }

private:
    // Mirror the exact constants from ClientGateway.cpp:49-50.
    static constexpr int kResolveMaxMs   = 5000;  // 5 s

    std::shared_ptr<aeron::Aeron>          aeron_;
    std::string                            channel_;
    int32_t                                streamId_;
    std::shared_ptr<aeron::Subscription>   sub_;

    rune::aeronutil::FragmentAssembler     assembler_;
};

} // namespace rune::node
