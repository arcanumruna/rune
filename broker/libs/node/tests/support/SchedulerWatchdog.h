#pragma once
// SchedulerWatchdog.h — test support (Task 23 fix round 3, controller ruling on concern 1).
//
// A harness-side stall signal: one thread of the TEST process sleeps kTickNs in a loop and records the largest
// overshoot of a sleep (how long the scheduler kept it off the CPU past its deadline).  Process-wide starvation - a
// SIGSTOP, an overloaded host - hits every thread of the process, including the media-driver threads that carry the
// consensus heartbeats, which the brokers' own admin / control pass gaps do not see.
//
// No atomics and no locks: the thread waits in poll() on a pipe with a kTickNs timeout; stop() writes one byte to the
// pipe (the kernel pipe is the only cross-thread channel) and joins; the result is handed over by the join
// (happens-before), so maxOvershootNs() / maxOvershootEndingSince() are read only after stop().  Test-only; never linked
// into the product.
//
// A stall takes effect LATER than it ends (a heartbeat timeout, then fencing, then an election), so the cell asks for the
// largest overshoot whose interval [end - overshoot, end] overlaps a window that reaches back before its evidence
// (maxOvershootOverlapping): every overshoot of at least kRecordNs is kept with its end time in a fixed, preallocated ring
// (the oldest overwritten).  Times are nowNs(): CLOCK_MONOTONIC_RAW, the brokers' MonotonicNanoClock, so they compare
// with the evidence stamps the brokers record (Task 23 fix round 4, R3-M1).
#include <poll.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <system_error>
#include <thread>

namespace rune::node::testing {

class SchedulerWatchdog {
public:
    static constexpr int64_t kTickNs = 5'000'000;     // 5 ms
    static constexpr int64_t kRecordNs = 10'000'000;  // overshoots kept with their end time (>= 10 ms)
    static constexpr std::size_t kRing = 4096;
    /// CLOCK_MONOTONIC_RAW ns - the same clock as rune::consensus::MonotonicNanoClock (the brokers' pass clocks).
    static int64_t nowNs() noexcept {
        timespec ts{};
        ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
        return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
    }

    SchedulerWatchdog() = default;
    SchedulerWatchdog(const SchedulerWatchdog&) = delete;
    SchedulerWatchdog& operator=(const SchedulerWatchdog&) = delete;
    ~SchedulerWatchdog() { stop(); }

    /// Starts the thread (a running watchdog is stopped first; its result is discarded).
    void start() {
        stop();
        if (::pipe(fds_) != 0) throw std::runtime_error("SchedulerWatchdog: pipe() failed");
        maxOvershootNs_ = 0;
        count_ = 0;
        try {
            if (failNextStartForTest) { failNextStartForTest = false; throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again)); }
            thread_ = std::thread([this] { run_(); });
        } catch (...) {   // R3-M2: never leak the pipe when the thread cannot start
            ::close(fds_[0]);
            ::close(fds_[1]);
            fds_[0] = fds_[1] = -1;
            throw;
        }
    }
    /// Test seam (R3-M2): the next start() fails as a std::thread constructor would (after pipe()).
    inline static bool failNextStartForTest = false;
    /// Stops and joins the thread; afterwards maxOvershootNs() holds the run's largest overshoot.  Idempotent.
    void stop() {
        if (!thread_.joinable()) return;
        const char b = 1;
        (void)!::write(fds_[1], &b, 1);
        thread_.join();
        ::close(fds_[0]);
        ::close(fds_[1]);
        fds_[0] = fds_[1] = -1;
    }
    /// The largest overshoot of one kTickNs sleep (ns) of the last run.  Read only after stop() (the join hands it over).
    [[nodiscard]] int64_t maxOvershootNs() const noexcept { return maxOvershootNs_; }
    /// The largest recorded overshoot (>= kRecordNs, else 0) whose interval [end - overshoot, end] overlaps [fromNs, toNs]
    /// (nowNs() times).  Read only after stop().
    [[nodiscard]] int64_t maxOvershootOverlapping(int64_t fromNs, int64_t toNs) const noexcept {
        int64_t m = 0;
        const std::size_t n = count_ < kRing ? count_ : kRing;
        for (std::size_t i = 0; i < n; ++i) {
            const Stall& st = ring_[i];
            if (st.endNs >= fromNs && st.endNs - st.overshootNs <= toNs && st.overshootNs > m) m = st.overshootNs;
        }
        return m;
    }

private:
    void run_() {   // the watchdog thread: owns maxOvershootNs_ / ring_ / count_ until the join
        pollfd p{fds_[0], POLLIN, 0};
        constexpr int kTickMs = static_cast<int>(kTickNs / 1'000'000);
        for (;;) {
            const int64_t t0 = nowNs();
            const int r = ::poll(&p, 1, kTickMs);
            if (r > 0) return;   // stop()
            const int64_t t1 = nowNs();
            const int64_t over = t1 - t0 - kTickNs;
            if (over > maxOvershootNs_) maxOvershootNs_ = over;
            if (over >= kRecordNs) ring_[count_++ % kRing] = Stall{t1, over};
        }
    }

    std::thread thread_;
    int         fds_[2] = {-1, -1};
    struct Stall { int64_t endNs = 0; int64_t overshootNs = 0; };
    int64_t                    maxOvershootNs_ = 0;
    std::array<Stall, kRing>   ring_{};   // preallocated; no allocation while running
    std::size_t                count_ = 0;
};

} // namespace rune::node::testing
