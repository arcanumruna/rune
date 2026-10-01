#pragma once
// AgentRunner.h — One-thread runner for an Agent with backoff idle strategy.
//
// Faithful C++ port of io.aeron.concurrent.AgentRunner.
// Spawns exactly ONE std::thread; calls:
//
//     for (;;) {
//         int w = agent_.doWork(clock_.nowNs());
//         if (stop command drained from the control ring) break;
//         idle_.idle(w);      // ← backoff when w==0; NEVER a bare busy-spin
//     }
//
// start()  — spawn the thread (idempotent: no-op if already started)
// stop()   — post a Stop COMMAND on the runner's control ring; does NOT join
// join()   — wait for the thread to exit (must call stop() first or the thread
//            will run indefinitely)
//
// F9a (no atomics): Agrona's AgentRunner uses a volatile `isRunning` flag.  We
// deliberately do NOT: the controlling thread sends Stop over a ControlRing
// (rune::consensus::ControlRing — the memory ordering lives inside the ring
// implementation) and the agent thread drains it every duty cycle, after
// doWork() and before idling (plus once before the first doWork()).  join()
// provides the happens-before for everything read after the thread exits.
//
// Thread ownership:
//   - started_/stopSent_/running_ : CONTROLLER-owned (the thread that calls
//     start()/stop()/join()/isRunning()).  The controller role may move between
//     threads only across a happens-before edge (e.g. a std::thread start or
//     join — ShardModeTest stops a node from a helper thread it then joins).
//   - stopRing_                   : producer = controller, consumer = agent thread.
//   - everything inside run()     : agent-thread-owned.
//
// Source reference: AgentRunner.java (Agrona)

#include <rune/consensus/NanoClock.h>
#include <rune/consensus/IdleStrategy.h>
#include <rune/consensus/ControlRing.h>

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
        if (started_) return; // already started (controller-owned flag)
        started_ = true;
        running_ = true;
        thread_  = std::thread([this]() { run(); });
    }

    /// Ask the agent thread to stop: posts ONE Stop command on the control ring.
    /// Does NOT join.  Idempotent; a stop() before start() is a no-op (matching
    /// the old flag semantics, where start() re-armed the flag).
    void stop() noexcept {
        if (!started_ || stopSent_) {
            running_ = false;
            return;
        }
        // The ring is empty except for this one command (stopSent_ guards
        // repeats), so the offer cannot fail on capacity.
        stopSent_ = stopRing_.offer(kStopCmd);
        running_  = false;
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

    /// CONTROLLER view: true between start() and stop().
    bool isRunning() const noexcept { return running_; }

private:
    static constexpr std::int32_t kStopCmd = 1;

    /// Agent thread: drain the control ring; true once Stop has been seen.
    bool stopRequested_() noexcept {
        bool stop = false;
        stopRing_.poll([&stop](std::int32_t type, std::int64_t, std::int64_t) noexcept {
            if (type == kStopCmd) stop = true;
        });
        return stop;
    }

    void run() {
        setCurrentThreadName(agent_.name());
        idle_.reset();
        if (stopRequested_()) return;          // stopped before the first cycle
        for (;;) {
            const int64_t now = clock_.nowNs();
            const int     w   = agent_.doWork(now);
            if (stopRequested_()) break;       // checked BEFORE idling
            idle_.idle(w); // backs off when w==0; never a bare busy-spin
        }
    }

    Agent&        agent_;
    IdleStrategy& idle_;
    NanoClock&    clock_;

    // Controller-owned (see header comment).
    bool started_  = false;
    bool stopSent_ = false;
    bool running_  = false;
    // controller → agent thread.  One command ever travels on it.
    ControlRing<256> stopRing_;
    std::thread      thread_;
};

} // namespace rune::consensus
