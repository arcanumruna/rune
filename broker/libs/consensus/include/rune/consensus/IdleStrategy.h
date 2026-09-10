#pragma once
// IdleStrategy.h — Backoff-idle abstraction for the AgentRunner duty-cycle loop.
//
// Agrona/Aeron semantics:
//   idle(workCount): if workCount > 0 → reset (work was done); else → back off.
//   reset()        : called when work resumes after a period of idleness.
//
// Wraps aeron::concurrent::BackoffIdleStrategy from
//   <concurrent/BackOffIdleStrategy.h>  (confirmed present in conan package).
//
// BusySpinIdleStrategy is provided as an explicit low-latency option but MUST
// NOT be used as the default; it burns a core when idle.

#include <concurrent/BackOffIdleStrategy.h>  // aeron::concurrent::BackoffIdleStrategy
#include <chrono>

namespace rune::consensus {

// ---------------------------------------------------------------------------
// Abstract interface (Agrona-style)
// ---------------------------------------------------------------------------
struct IdleStrategy {
    virtual ~IdleStrategy() = default;
    /// Called after every doWork(): back off when workCount==0, reset on work.
    virtual void idle(int workCount) = 0;
    /// Reset the back-off state (called implicitly by idle(workCount>0)).
    virtual void reset() = 0;
};

// ---------------------------------------------------------------------------
// BackoffIdleStrategy — wraps aeron::concurrent::BackoffIdleStrategy
// Progression: spin → yield → nanosleep (doubling up to 1 ms)
// ---------------------------------------------------------------------------
class BackoffIdleStrategy final : public IdleStrategy {
public:
    /// Defaults match Agrona's defaults (10 spins, 20 yields, 1µs–1ms park).
    explicit BackoffIdleStrategy(
        int64_t maxSpins  = 10,
        int64_t maxYields = 20,
        std::chrono::nanoseconds minPark = std::chrono::microseconds{1},
        std::chrono::nanoseconds maxPark = std::chrono::milliseconds{1})
        : inner_{maxSpins, maxYields, minPark, maxPark}
    {}

    void idle(int workCount) override { inner_.idle(workCount); }
    void reset()             override { inner_.reset(); }

private:
    aeron::concurrent::BackoffIdleStrategy inner_;
};

// ---------------------------------------------------------------------------
// BusySpinIdleStrategy — explicit low-latency option; NEVER the default.
// Calls cpu_pause() every cycle regardless of workCount.
// ---------------------------------------------------------------------------
class BusySpinIdleStrategy final : public IdleStrategy {
public:
    void idle(int /*workCount*/) override {
        // cpu_pause (PAUSE instruction on x86) to be a polite busy-spin.
        // Does NOT yield or sleep — this is the explicit low-latency option.
#if defined(__x86_64__) || defined(__i386__)
        __asm__ volatile("pause" ::: "memory");
#elif defined(__aarch64__) || defined(__arm__)
        __asm__ volatile("yield" ::: "memory");
#else
        // fallback: compiler barrier
        __asm__ volatile("" ::: "memory");
#endif
    }
    void reset() override {}
};

} // namespace rune::consensus
