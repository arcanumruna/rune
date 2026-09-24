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
//     kResolveMaxMs=5000 ms / kResolveSliceMs=1 ms slices.
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

#include <Aeron.h>
#include <Subscription.h>
#include <FragmentAssembler.h>
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
                  int32_t                       streamId) noexcept
        : aeron_(std::move(aeron))
        , channel_(std::move(channel))
        , streamId_(streamId)
        // The assembler's fixed callback dispatches to the per-poll trampoline
        // via raw void* + function pointer.  No std::function of our own.
        , assembler_([this](aeron::concurrent::AtomicBuffer& buffer,
                            aeron::util::index_t             offset,
                            aeron::util::index_t             length,
                            aeron::Header&                   header) {
              // Invoke trampoline: trampoline_(ctx_, data, len, hdr).
              // ctx_/trampoline_ are set for the duration of each poll() call.
              trampoline_(ctx_,
                          reinterpret_cast<const uint8_t*>(buffer.buffer() + offset),
                          static_cast<std::size_t>(length),
                          header);
          })
    {
        if (!aeron_) return;  // null aeron_ → unresolved

        // Mirror ClientGateway.cpp:85-99 EXACTLY.
        int64_t regId = aeron_->addSubscription(channel_, streamId_);

        using clock = std::chrono::steady_clock;
        auto deadline = clock::now() + std::chrono::milliseconds{kResolveMaxMs};
        while (clock::now() < deadline) {
            sub_ = aeron_->findSubscription(regId);
            if (sub_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds{kResolveSliceMs});
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
    // No std::function of our own.  The only type-erasure is Aeron's own
    // fragment_handler_t at the Subscription::poll boundary.
    // -----------------------------------------------------------------------
    template <class Handler>
    int poll(int fragmentLimit, Handler&& handler) {
        if (!sub_) [[unlikely]] return 0;

        // Install the concrete handler as a raw trampoline for this call.
        // poll() is synchronous and single-threaded — the raw pointer is
        // valid for the entire duration of sub_->poll(...).
        using H = std::remove_reference_t<Handler>;
        ctx_        = static_cast<void*>(std::addressof(handler));
        trampoline_ = [](void* ctx,
                         const uint8_t*   data,
                         std::size_t      len,
                         aeron::Header&   hdr) {
            (*static_cast<H*>(ctx))(data, len, hdr);
        };

        int result = sub_->poll(assembler_.handler(), fragmentLimit);

        // Clear trampoline to prevent accidental use after the call stack is gone.
        ctx_        = nullptr;
        trampoline_ = nullptr;
        return result;
    }

private:
    // Mirror the exact constants from ClientGateway.cpp:49-50.
    static constexpr int kResolveMaxMs   = 5000;  // 5 s
    static constexpr int kResolveSliceMs = 1;

    // Raw-pointer trampoline: set for the duration of each poll() call.
    // No std::function, no heap allocation, no virtual call of our own.
    using TrampolineFn = void(*)(void* ctx,
                                 const uint8_t* data,
                                 std::size_t    len,
                                 aeron::Header& hdr);

    std::shared_ptr<aeron::Aeron>          aeron_;
    std::string                            channel_;
    int32_t                                streamId_;
    std::shared_ptr<aeron::Subscription>   sub_;

    // Per-poll trampoline state (valid only during sub_->poll()).
    void*          ctx_        = nullptr;
    TrampolineFn   trampoline_ = nullptr;

    aeron::FragmentAssembler               assembler_;
};

} // namespace rune::node
