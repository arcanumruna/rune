// examples/bench/BenchIdle.h — A7 (rune-broker findings §4.12): the invoker bench loop's idle.
//
// Measured before A7: the open-loop shard loop never idled — ~311k recvmmsg/s + ~58k epoll_wait/s on its
// thread, ~80-100% of a core at 20k rec/s, CPU the brokers it measures need on a 4-core box.  Park: when a
// pass sent nothing, did no work and polled no ack, ppoll() on the shard driver's wake fds until a datagram
// arrives (an ack / status message), the next scheduled send is due, or maxPark passes.  The thread's timer
// slack is cut to 1 us so the wake for the next scheduled send is on time (latency is measured from it).
// RUNE_BENCH_IDLE=spin restores the old loop (the A/B off arm).
#pragma once

#include <poll.h>
#if defined(__linux__)
#include <sys/prctl.h>
#include <time.h>
#endif

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

namespace rune::bench {

enum class BenchIdle : uint8_t { Park, Spin };

/// RUNE_BENCH_IDLE: "spin" (any case) = the old spin loop; anything else (unset) = park.
inline BenchIdle parseBenchIdle(const char* v) {
    if (v == nullptr) return BenchIdle::Park;
    std::string s(v);
    for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s == "spin" ? BenchIdle::Spin : BenchIdle::Park;
}

inline constexpr int64_t kNoNextSend    = INT64_MAX;
inline constexpr int64_t kBenchMaxParkNs = 200'000;   // = the broker's default RUNE_IDLE_MAX_PARK_US

/// How long an idle pass may park: until the next scheduled send (never past it), at most maxParkNs;
/// 0 when a send is already due.
[[nodiscard]] inline int64_t benchParkTimeoutNs(int64_t nextDueNs, int64_t nowNs, int64_t maxParkNs) noexcept {
    if (nextDueNs == kNoNextSend) return maxParkNs;
    if (nextDueNs <= nowNs) return 0;
    return std::min(nextDueNs - nowNs, maxParkNs);
}

/// The calling thread's park primitive: ppoll on the wake fds (Linux), else a plain sleep.
class BenchParker {
public:
    BenchParker(const int* fds, int n) noexcept {
        for (int i = 0; i < n && n_ < kMax; ++i) pfds_[n_++] = pollfd{fds[i], POLLIN, 0};
#if defined(__linux__)
        (void)::prctl(PR_SET_TIMERSLACK, 1'000UL, 0, 0, 0);   // 1 us: wake for the next send on time
#endif
    }
    void park(int64_t ns) noexcept {
        if (ns <= 0) return;
        ++parks_;
#if defined(__linux__)
        if (n_ > 0) {
            const timespec ts{static_cast<time_t>(ns / 1'000'000'000), static_cast<long>(ns % 1'000'000'000)};
            (void)::ppoll(pfds_, static_cast<nfds_t>(n_), &ts, nullptr);
            return;
        }
#endif
        std::this_thread::sleep_for(std::chrono::nanoseconds{ns});
    }
    [[nodiscard]] int      wakeFdCount() const noexcept { return n_; }
    [[nodiscard]] uint64_t parks() const noexcept { return parks_; }

private:
    static constexpr int kMax = 8;
    pollfd   pfds_[kMax]{};
    int      n_     = 0;
    uint64_t parks_ = 0;
};

} // namespace rune::bench
