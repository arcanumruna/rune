#pragma once
// ParkUntilWorkIdle.h — A7: a backoff idle strategy that actually idles (findings §4.12).
//
// Measured (k3s-node-01, 3-node acks=all 1 KiB, 20k rec/s): every shard worker at ~100% CPU, ~28k passes/s,
// ~7 syscalls per pass (a recvmmsg per driver socket + an epoll_wait), ~35 µs per pass, 20-60 parks/s.  The
// Aeron backoff (10 spins, 20 yields, then park) counts IDLE PASSES, and Aeron sizes it for passes that cost
// nanoseconds (shared-memory polls).  A shard pass pumps an invoker media driver, so one "spin" is a full
// syscall-heavy pass: 30 passes ~= 1 ms of polling before the first park.  Any datagram inside that window
// resets the backoff — at 20k rec/s one always does, so the worker never parks.  Idle cluster: 39% CPU per
// worker for the same reason (heartbeats/status messages every few ms reset it).
//
// Park-until-work: park in ppoll() on the invoker driver's transport-poller epoll fds (DriverWakeFds.h) —
// woken by the next datagram, bounded by the park period (the doubling 1 µs .. maxPark of the backoff, which
// keeps today's bound for work that does not arrive on a socket: rings from other threads, timers).  With
// readiness to wake on, the spin / yield passes buy nothing, so the derived shape parks after the FIRST empty
// pass (NodeConfig::shardIdleShape).  Without fds (non-Linux, threaded driver) it parks with a plain sleep.
//
// One idle() call = exactly one action, in order: `maxSpins` immediate re-runs (no-op), `maxYields`
// sched_yields, then parks (period doubling minPark -> maxPark).  idle(work > 0) / reset() return to the start.  Never busy-spins:
// the spin/yield phases are bounded and every later empty pass parks.
#include <rune/consensus/IdleStrategy.h>
#include <rune/node/IdleShape.h>

#include <poll.h>
#if defined(__linux__)
#include <time.h>
#endif

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <thread>

namespace rune::node {

class ParkUntilWorkIdle final : public rune::consensus::IdleStrategy {
public:
    static constexpr int kMaxWakeFds = 8;
    enum class Phase : uint8_t { NotIdle, Spinning, Yielding, Parking };

    /// `fds` (may be null when n == 0): readiness fds to park on, owned by the caller and valid for this
    /// strategy's lifetime.  At most kMaxWakeFds are used; negative fds are skipped.
    ParkUntilWorkIdle(IdleShape shape, const int* fds, int n) noexcept : shape_(sanitize_(shape)) {
        for (int i = 0; i < n && nfds_ < kMaxWakeFds; ++i) {
            if (fds[i] < 0) continue;
            pfds_[static_cast<std::size_t>(nfds_)] = pollfd{fds[i], POLLIN, 0};
            ++nfds_;
        }
        reset();
    }

    void idle(int workCount) override {
        if (workCount > 0) {
            reset();
            return;
        }
        if (spins_ < shape_.maxSpins) {
            ++spins_;
            phase_ = Phase::Spinning;
            // No pause instruction: a spin here means "run the next pass now" (a pass costs
            // microseconds; a cpu pause would add nothing but an atomics-law exception).
        } else if (yields_ < shape_.maxYields) {
            ++yields_;
            phase_ = Phase::Yielding;
            std::this_thread::yield();
        } else {
            phase_ = Phase::Parking;
            park_(parkNs_);
            parkNs_ = std::min(parkNs_ * 2, shape_.maxParkNs);
        }
    }

    void reset() override {
        phase_  = Phase::NotIdle;
        spins_  = 0;
        yields_ = 0;
        parkNs_ = shape_.minParkNs;
    }

    [[nodiscard]] Phase            phase() const noexcept { return phase_; }
    [[nodiscard]] int              wakeFdCount() const noexcept { return nfds_; }
    [[nodiscard]] const IdleShape& shape() const noexcept { return shape_; }
    /// The period the NEXT park will use.
    [[nodiscard]] int64_t nextParkNs() const noexcept { return parkNs_; }
    /// Owner-thread tallies (diagnostics / tests): parks taken, and parks ended early by readiness.
    [[nodiscard]] uint64_t parks() const noexcept { return parks_; }
    [[nodiscard]] uint64_t readinessWakes() const noexcept { return wakes_; }

private:
    static IdleShape sanitize_(IdleShape s) noexcept {
        s.maxSpins  = std::max<int32_t>(s.maxSpins, 0);
        s.maxYields = std::max<int32_t>(s.maxYields, 0);
        s.minParkNs = std::max<int64_t>(s.minParkNs, 1);
        s.maxParkNs = std::max<int64_t>(s.maxParkNs, s.minParkNs);
        return s;
    }

    void park_(int64_t ns) noexcept {
        ++parks_;
        if (nfds_ == 0) {
            std::this_thread::sleep_for(std::chrono::nanoseconds{ns});
            return;
        }
#if defined(__linux__)
        const timespec ts{static_cast<time_t>(ns / 1'000'000'000), static_cast<long>(ns % 1'000'000'000)};
        const int r = ::ppoll(pfds_.data(), static_cast<nfds_t>(nfds_), &ts, nullptr);
#else
        // No ppoll: millisecond timeout, rounded up (production never gets here — wake fds are Linux-only).
        const int r = ::poll(pfds_.data(), static_cast<nfds_t>(nfds_), static_cast<int>((ns + 999'999) / 1'000'000));
#endif
        if (r > 0) ++wakes_;
    }

    IdleShape                          shape_;
    std::array<pollfd, kMaxWakeFds>    pfds_{};
    int                                nfds_   = 0;
    Phase                              phase_  = Phase::NotIdle;
    int32_t                            spins_  = 0;
    int32_t                            yields_ = 0;
    int64_t                            parkNs_ = 0;
    uint64_t                           parks_  = 0;
    uint64_t                           wakes_  = 0;
};

} // namespace rune::node
