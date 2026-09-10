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

namespace rune::consensus {

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
