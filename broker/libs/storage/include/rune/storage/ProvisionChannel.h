#pragma once
// ProvisionChannel — the worker → segment-provisioner channel (D19 L3).
//
// A provisioned Log posts its demands (Threshold / Adopted / Hold), its read-map
// growth requests and its retired read maps here.  Concrete and inline: the
// owner's roll path posts with a direct call (ring offer + doorbell), never an
// indirect one.  ONE channel per provisioner thread, shared by every worker
// (M2O ring); the provisioner polls ring() and parks on doorbell().
//
// Shared-nothing: the ring and the doorbell are the only cross-thread state
// (ring-implementation internals); no lock, no allocation after construction.
#include <rune/consensus/ControlRing.h>
#include <rune/consensus/detail/RingDoorbell.h>

#include <cstdint>

namespace rune::storage {

class ProvisionChannel {
public:
    /// Record types: a = Log* (demand / grow) or MappedFile* (dispose).
    static constexpr int32_t kDemandMsgType = 1;    ///< b = Log::ProvisionDemand
    static constexpr int32_t kGrowMsgType = 2;      ///< b = Log packed read-map request
    static constexpr int32_t kDisposeMsgType = 3;   ///< b = 0
    using Ring = rune::consensus::ControlRing<64 * 1024>;   // ~2.7k demands

    ProvisionChannel() {
        const auto w = ring_.watchInfo();
        (void)bell_.addWatch(rune::consensus::detail::RingWatch{w.buffer, w.length, w.capacity,
                                                                  w.headPositionIndex});
    }
    ProvisionChannel(const ProvisionChannel&)            = delete;
    ProvisionChannel& operator=(const ProvisionChannel&) = delete;

    /// Any worker thread.  False when the ring is full: the caller re-posts
    /// later (a demand is never lost).
    bool post(int32_t type, int64_t a, int64_t b) noexcept {
        if (interceptFn_ != nullptr) [[unlikely]] return interceptFn_(interceptCtx_, type, a, b);
        if (!ring_.offer(type, a, b)) return false;
        bell_.ringIfParked();
        return true;
    }

    /// The provisioner thread polls this ring.
    [[nodiscard]] Ring& ring() noexcept { return ring_; }
    /// The provisioner's idle strategy parks on this doorbell.
    [[nodiscard]] rune::consensus::detail::RingDoorbell& doorbell() noexcept { return bell_; }

    /// Read-map growth / disposal is served (false while a test intercepts).
    [[nodiscard]] bool servesReadMaps() const noexcept { return interceptFn_ == nullptr; }

    /// Tests: every post goes to `fn` instead of the ring (its result is the
    /// post's).  No read-map growth is requested while intercepted.
    using InterceptFn = bool (*)(void* ctx, int32_t type, int64_t a, int64_t b) noexcept;
    void interceptForTest(InterceptFn fn, void* ctx) noexcept {
        interceptFn_  = fn;
        interceptCtx_ = ctx;
    }

private:
    Ring                                   ring_;
    rune::consensus::detail::RingDoorbell  bell_;
    InterceptFn                            interceptFn_  = nullptr;
    void*                                  interceptCtx_ = nullptr;
};

} // namespace rune::storage
