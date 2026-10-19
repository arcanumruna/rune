#pragma once
// SignalControl.h — F9a: process signals as RING EVENTS (no atomics, no
// async-signal-handler globals).
//
//   1. blockControlSignals() — called by main() BEFORE any thread exists, so the
//      mask (SIGINT, SIGTERM, SIGUSR1, and the internal wake SIGUSR2) is
//      inherited by every thread: no thread ever runs an async signal handler.
//   2. SignalThread — ONE dedicated thread loops sigwait() (POSIX; portable to
//      macOS — no signalfd) and posts events onto the main CONTROL ring:
//        SIGINT / SIGTERM → kStopEvent
//        SIGUSR1          → kFrDumpEvent   (dump the flight-recorder report)
//        SIGHUP           → kTlsReloadEvent (Task 9: RUNE_TLS cert/key renewal —
//                           main() forwards this to BrokerNode::requestTlsCertReload(),
//                           which itself only offers a trigger onto a ring the
//                           gateway's OWN duty-cycle thread polls and acts on —
//                           the signal path never touches gateway state directly)
//      The halt path (BrokerNode fail-stop, on the runtime thread) posts
//      kHaltEvent{exitCode} on the SAME ring via postHalt() — the exit code
//      travels in the message, never through a global.
//   3. main() is the ring's ONE consumer (its 100 ms poll loop).
//
// Shutdown of the signal thread: the destructor posts Quit on the thread's own
// command ring, then wakes sigwait() with a THREAD-directed SIGUSR2; the thread
// drains its command ring on every wake and exits on Quit (an external SIGUSR2
// without a Quit command is ignored).  A Quit observed while a real signal woke
// the thread wins: shutdown is already in progress.

#include <rune/consensus/ControlRing.h>

#include <cerrno>
#include <csignal>
#include <cstdint>
#include <thread>

#include <pthread.h>
#include <signal.h>

namespace rune::node {

/// Main control ring: signal thread + runtime (halt) → main thread.
using ControlEventRing = rune::consensus::ControlRing<1024>;

inline constexpr std::int32_t kStopEvent      = 1;
inline constexpr std::int32_t kFrDumpEvent    = 2;
inline constexpr std::int32_t kHaltEvent      = 3;   ///< a = exit code
inline constexpr std::int32_t kTlsReloadEvent = 4;   ///< Task 9: SIGHUP — reload cert/key

/// The signals the control plane owns (blocked everywhere, consumed by sigwait).
inline sigset_t controlSignalSet() noexcept {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGUSR1);
    sigaddset(&set, SIGHUP);    // Task 9: RUNE_TLS cert/key renewal (reload, not stop)
    sigaddset(&set, SIGUSR2);   // internal: wakes the signal thread for shutdown
    return set;
}

/// Block the control signals in the CALLING thread (inherited by threads it
/// creates afterwards).  main() calls this first.  Returns 0 or an errno.
///
/// First resets each control signal's disposition to SIG_DFL: a shell starts
/// background jobs with SIGINT ignored, and an IGNORED signal is discarded at
/// generation even while blocked — sigwait() would never see it.  (The old
/// std::signal(SIGINT, handler) overrode an inherited SIG_IGN the same way.)
/// SIG_DFL is never acted on: the signals stay blocked in every thread.
inline int blockControlSignals() noexcept {
    const sigset_t set = controlSignalSet();
    if (const int rc = ::pthread_sigmask(SIG_BLOCK, &set, nullptr); rc != 0) return rc;
    struct sigaction dfl {};
    dfl.sa_handler = SIG_DFL;
    sigemptyset(&dfl.sa_mask);
    for (const int sig : {SIGINT, SIGTERM, SIGUSR1, SIGHUP, SIGUSR2}) {
        if (::sigaction(sig, &dfl, nullptr) != 0) return errno;
    }
    return 0;
}

/// Post a halt (fail-stop) request with its exit code.  Any thread.  The ring
/// has room for dozens of events and halt fires once; a full ring is retried
/// briefly (bounded) rather than silently losing the exit code.
inline bool postHalt(ControlEventRing& ring, int exitCode) noexcept {
    for (int i = 0; i < 1000; ++i) {
        if (ring.offer(kHaltEvent, exitCode)) return true;
        std::this_thread::yield();
    }
    return false;
}

class SignalThread {
public:
    /// Precondition: blockControlSignals() was called by this thread (or the
    /// mask is otherwise blocked) BEFORE construction.
    explicit SignalThread(ControlEventRing& events)
        : events_(events), thread_([this] { run_(); }) {}

    ~SignalThread() {
        (void)cmds_.offer(kQuitCmd);          // one command, otherwise-empty ring
        ::pthread_kill(thread_.native_handle(), SIGUSR2);   // wake sigwait()
        thread_.join();
    }

    SignalThread(const SignalThread&)            = delete;
    SignalThread& operator=(const SignalThread&) = delete;

    /// Test hook: the signal thread's handle (thread-directed pthread_kill).
    [[nodiscard]] pthread_t nativeHandle() noexcept { return thread_.native_handle(); }

private:
    static constexpr std::int32_t kQuitCmd = 1;

    void run_() noexcept {
        const sigset_t set = controlSignalSet();
        for (;;) {
            int sig = 0;
            if (::sigwait(&set, &sig) != 0) continue;
            // Drain the command ring on EVERY wake (macOS sigwait can report a
            // spurious sig 0, so do not key the Quit check on SIGUSR2 alone).
            bool quit = false;
            cmds_.poll([&quit](std::int32_t type, std::int64_t, std::int64_t) noexcept {
                if (type == kQuitCmd) quit = true;
            });
            if (quit) return;
            if (sig == SIGUSR2) continue;   // wake only (an external SIGUSR2 is ignored)
            std::int32_t ev = 0;
            if (sig == SIGUSR1) ev = kFrDumpEvent;
            else if (sig == SIGHUP) ev = kTlsReloadEvent;   // Task 9: cert/key renewal
            else if (sig == SIGINT || sig == SIGTERM) ev = kStopEvent;
            else continue;   // spurious wake (macOS sigwait may report sig 0): ignore
            // Full ring: main is not draining (wedged) — a repeated signal will
            // retry; never block the signal thread.
            (void)events_.offer(ev, sig);
        }
    }

    ControlEventRing&                  events_;
    rune::consensus::ControlRing<256>  cmds_;     // owner (dtor caller) → signal thread
    std::thread                        thread_;   // last: starts after cmds_ exists
};

} // namespace rune::node
