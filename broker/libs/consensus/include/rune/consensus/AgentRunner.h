#pragma once
// AgentRunner.h — One-thread runner for an Agent with backoff idle strategy.
//
// Faithful C++ port of io.aeron.concurrent.AgentRunner.
// Spawns exactly ONE std::thread; calls:
//
//     while (running_) {
//         int w = agent_.doWork(clock_.nowNs());
//         idle_.idle(w);      // ← backoff when w==0; NEVER a bare busy-spin
//     }
//
// start()  — spawn the thread (idempotent: no-op if already started)
// stop()   — set stop flag; does NOT join
// join()   — wait for the thread to exit (must call stop() first or the thread
//            will run indefinitely)
//
// Source reference: AgentRunner.java (Agrona)

#include <rune/consensus/NanoClock.h>
#include <rune/consensus/IdleStrategy.h>

#include <atomic>
#include <thread>
#include <memory>
#include <cassert>

#if defined(__APPLE__) || defined(__linux__)
#  include <pthread.h>
#endif

namespace rune::consensus {

// Name the CALLING thread so `ps -M`, `top -H`, Instruments, and perf show a
// human-readable label instead of an anonymous TID.  macOS: pthread_setname_np
// takes only the name and always targets the current thread.  Linux: caps the
// name at 15 chars + NUL, so it is truncated here.  No-op elsewhere.
inline void setCurrentThreadName(const char* name) noexcept {
    if (!name) return;
#if defined(__APPLE__)
    ::pthread_setname_np(name);
#elif defined(__linux__)
    char buf[16];
    std::size_t i = 0;
    for (; name[i] && i < sizeof(buf) - 1; ++i) buf[i] = name[i];
    buf[i] = '\0';
    ::pthread_setname_np(::pthread_self(), buf);
#else
    (void)name;
#endif
}

// Forward declaration (AgentRunner depends on Agent; Agent is defined in
// ConsensusAgent.h but the interface itself can be declared here to keep
// headers lean).
struct Agent;

class AgentRunner {
public:
    // @param agent    The agent to drive (must outlive AgentRunner).
    // @param idle     IdleStrategy to use (must outlive AgentRunner).
    // @param clock    NanoClock providing nowNs() (must outlive AgentRunner).
    AgentRunner(Agent&       agent,
                IdleStrategy& idle,
                NanoClock&   clock) noexcept
        : agent_{agent}, idle_{idle}, clock_{clock}
    {}

    ~AgentRunner() {
        stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    AgentRunner(const AgentRunner&)            = delete;
    AgentRunner& operator=(const AgentRunner&) = delete;

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// Spawn the agent thread.  Idempotent: subsequent calls are no-ops.
    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true,
                std::memory_order_acq_rel, std::memory_order_relaxed)) {
            return; // already started
        }
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this]() { run(); });
    }

    /// Signal the agent thread to stop.  Does NOT join.
    void stop() noexcept {
        running_.store(false, std::memory_order_release);
    }

    /// Block until the agent thread exits.  Must call stop() first or this
    /// blocks forever.
    void join() {
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    /// Convenience: stop() + join().
    void stopAndJoin() {
        stop();
        join();
    }

    bool isRunning() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

private:
    void run() {
        setCurrentThreadName(agent_.name());
        idle_.reset();
        while (running_.load(std::memory_order_acquire)) {
            const int64_t now = clock_.nowNs();
            const int     w   = agent_.doWork(now);
            idle_.idle(w); // backs off when w==0; never a bare busy-spin
        }
    }

    Agent&        agent_;
    IdleStrategy& idle_;
    NanoClock&    clock_;

    std::atomic<bool> started_{false};
    std::atomic<bool> running_{false};
    std::thread       thread_;
};

} // namespace rune::consensus
