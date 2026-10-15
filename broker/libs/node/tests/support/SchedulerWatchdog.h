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
// (happens-before), so maxOvershootNs() is read only after stop().  Test-only; never linked into the product.
#include <poll.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>

namespace rune::node::testing {

class SchedulerWatchdog {
public:
    static constexpr int64_t kTickNs = 5'000'000;   // 5 ms

    SchedulerWatchdog() = default;
    SchedulerWatchdog(const SchedulerWatchdog&) = delete;
    SchedulerWatchdog& operator=(const SchedulerWatchdog&) = delete;
    ~SchedulerWatchdog() { stop(); }

    /// Starts the thread (a running watchdog is stopped first; its result is discarded).
    void start() {
        stop();
        if (::pipe(fds_) != 0) throw std::runtime_error("SchedulerWatchdog: pipe() failed");
        maxOvershootNs_ = 0;
        thread_ = std::thread([this] { run_(); });
    }
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

private:
    void run_() {   // the watchdog thread: owns maxOvershootNs_ until the join
        using Clock = std::chrono::steady_clock;
        pollfd p{fds_[0], POLLIN, 0};
        constexpr int kTickMs = static_cast<int>(kTickNs / 1'000'000);
        for (;;) {
            const auto t0 = Clock::now();
            const int r = ::poll(&p, 1, kTickMs);
            if (r > 0) return;   // stop()
            const int64_t slept = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count();
            if (slept - kTickNs > maxOvershootNs_) maxOvershootNs_ = slept - kTickNs;
        }
    }

    std::thread thread_;
    int         fds_[2] = {-1, -1};
    int64_t     maxOvershootNs_ = 0;
};

} // namespace rune::node::testing
