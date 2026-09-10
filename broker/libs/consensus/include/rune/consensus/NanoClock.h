#pragma once
// NanoClock.h — Injectable monotonic nanosecond clock.
//
// All time on the duty-cycle hot path comes through nowNs() so that tests can
// inject a deterministic manual clock without touching system calls.
//
// Production:  MonotonicNanoClock  (clock_gettime CLOCK_MONOTONIC_RAW)
// Tests:       ManualNanoClock     (advance() driven by test loop)

#include <cstdint>
#include <functional>
#include <time.h>   // clock_gettime, CLOCK_MONOTONIC_RAW

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Abstract interface
// ---------------------------------------------------------------------------
struct NanoClock {
    virtual ~NanoClock() = default;
    virtual int64_t nowNs() const = 0;
};

// ---------------------------------------------------------------------------
// MonotonicNanoClock — wall-clock implementation using CLOCK_MONOTONIC_RAW.
// Suitable for production use.
// ---------------------------------------------------------------------------
class MonotonicNanoClock final : public NanoClock {
public:
    int64_t nowNs() const override {
        struct timespec ts{};
        ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
        return static_cast<int64_t>(ts.tv_sec) * INT64_C(1'000'000'000) +
               static_cast<int64_t>(ts.tv_nsec);
    }
};

// ---------------------------------------------------------------------------
// ManualNanoClock — injected clock for deterministic tests.
// Tests call advance(ns) to move time forward; doWork(nowNs()) sees that value.
// ---------------------------------------------------------------------------
class ManualNanoClock final : public NanoClock {
public:
    explicit ManualNanoClock(int64_t initialNs = 0) : now_{initialNs} {}

    int64_t nowNs() const override { return now_; }

    void set(int64_t ns) { now_ = ns; }
    void advance(int64_t deltaNs) { now_ += deltaNs; }

private:
    int64_t now_{0};
};

// ---------------------------------------------------------------------------
// FuncNanoClock — wraps a std::function<int64_t()> for convenience.
// ---------------------------------------------------------------------------
class FuncNanoClock final : public NanoClock {
public:
    explicit FuncNanoClock(std::function<int64_t()> fn) : fn_{std::move(fn)} {}
    int64_t nowNs() const override { return fn_(); }
private:
    std::function<int64_t()> fn_;
};

} // namespace rune::consensus
