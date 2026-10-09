#pragma once
// ---------------------------------------------------------------------------
// Throttler.h — CP-8: Token-bucket I/O throttler for the compaction cleaner.
//
// Bounds aggregate cleaner read+write bytes/sec to `ioMaxBytesPerSecond`.
// Kafka equivalent: Throttler (kafka.utils.Throttler) with a token-bucket
// model.  Default: unlimited (-1), matching Kafka's default.
//
// Thread-safety: a single Throttler is shared across cleaner threads.  Because
// Rune's default is 1 cleaner thread and the agent is single-threaded, the
// simplest implementation tracks time and accumulated bytes in non-atomic
// fields and relies on single-threaded cleaner use.  For multi-cleaner (N>1)
// support, promote period tracking to atomics (out-of-scope for CP-8).
//
// HARD INVARIANT: maybeThrottle() MUST only be called on the CLEANER THREAD —
// never on the append/duty-cycle thread.  The append thread must not be blocked
// by I/O throttling.
// ---------------------------------------------------------------------------

#include <algorithm>
#include <cstdint>
#include <thread>
#include <chrono>

namespace rune::storage {

/// Fix round 6: a cleaner-thread callback run at every pass segment / throttle
/// boundary — the agent drains OTHER logs' posted file operations there, so an
/// owner waiting on them (FollowerSync::TruncatePending) never waits a whole
/// (possibly throttled) compaction pass of an unrelated log.  No allocation.
struct BoundaryHook {
    void (*fn)(void*) = nullptr;
    void* ctx = nullptr;
    void operator()() const { if (fn != nullptr) fn(ctx); }
};

class Throttler {
public:
    /// Construct a throttler.
    /// @param maxBytesPerSec  Maximum bytes/sec allowed.  -1 = unlimited.
    explicit Throttler(int64_t maxBytesPerSec = -1) noexcept
        : maxBytesPerSec_(maxBytesPerSec)
        , windowStart_(std::chrono::steady_clock::now())
    {}

    /// Called by the cleaner thread after reading/writing `bytes` bytes.
    /// Sleeps the calling thread (cleaner-only!) if the accumulated rate
    /// exceeds maxBytesPerSec_.  No-op if unlimited.
    ///
    /// MUST NOT be called on the append/duty-cycle thread.
    void maybeThrottle(int64_t bytes) noexcept {
        if (maxBytesPerSec_ <= 0) return; // unlimited — fast path
        bytesInWindow_ += bytes;

        const auto now = std::chrono::steady_clock::now();
        const auto elapsed = std::chrono::duration<double>(now - windowStart_).count();

        // rate > max, without dividing: a burst within one clock tick of the
        // window start (elapsed == 0 on a coarse clock) used to return here
        // unthrottled (Throttler.BurstOverLimitSleeps flaked at 0 ms).
        if (static_cast<double>(bytesInWindow_) > static_cast<double>(maxBytesPerSec_) * std::max(elapsed, 0.0)) {
            // Compute the sleep needed to bring the average rate down to the limit.
            // targetDuration = bytesInWindow / maxBytesPerSec
            // sleepMs = (targetDuration - elapsed) * 1000
            const double targetSec = static_cast<double>(bytesInWindow_)
                                     / static_cast<double>(maxBytesPerSec_);
            const double sleepSec = targetSec - elapsed;
            if (sleepSec > 0.0) {
                const int64_t sleepMs = static_cast<int64_t>(sleepSec * 1000.0);
                // Fix round 6: sleep in slices, running the boundary hook before
                // each, so a long throttle sleep never delays other logs' file ops.
                for (int64_t left = sleepMs; left > 0; left -= kSliceMs) {
                    hook_();
                    std::this_thread::sleep_for(std::chrono::milliseconds(left < kSliceMs ? left : kSliceMs));
                }
            }
            // Reset the window after sleeping.
            bytesInWindow_ = 0;
            windowStart_   = std::chrono::steady_clock::now();
        }

        // Reset window every second to avoid unbounded accumulation.
        if (elapsed >= 1.0) {
            bytesInWindow_ = 0;
            windowStart_   = now;
        }
    }

    int64_t maxBytesPerSec() const noexcept { return maxBytesPerSec_; }
    /// Fix round 6: run `hook` before every throttle sleep slice (<= kSliceMs).
    void setBoundaryHook(BoundaryHook hook) noexcept { hook_ = hook; }
    static constexpr int64_t kSliceMs = 10;

private:
    int64_t maxBytesPerSec_;
    int64_t bytesInWindow_ = 0;
    std::chrono::steady_clock::time_point windowStart_;
    BoundaryHook hook_{};
};

} // namespace rune::storage
