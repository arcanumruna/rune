// include/rune/client/Client.h
//
// CC-6 — Public Client API: connect()/ping() futures + lifecycle callbacks.
// Namespace: rune::client
//
// Threading model:
//   - The Client owns a background runner thread that loops agent_.doWork()
//     under the configured IdleStrategy (BackoffIdleStrategy by default for
//     CPU-friendly behaviour; YieldingIdleStrategy for LowLatency mode which
//     trades a CPU core for sub-µs reaction time).
//     The app just calls connect()/ping() and awaits the returned futures.
//   - connect() / ping() are called from the APP thread.  They submit via the
//     agent's thread-safe ring and register promises in cross-thread maps.
//   - The egress observer (set on the agent BEFORE start) runs on the RUNNER
//     (agent) thread and resolves/rejects promises.
//   - Callbacks (onConnected / onDisconnected / onError) also fire on the
//     runner thread.  Document this to callers: do not block inside them.
//   - Cross-thread structures (pendingConnect_ + pendingPings_) are guarded by
//     a single std::mutex (control-plane, NOT the agent hot path).
//   - correlationId generation uses an atomic counter.
//   - Futures NEVER hang: the runner does a periodic timeout sweep that fails
//     any pending-connect older than connectTimeoutMs and any pending-ping
//     older than pingTimeoutMs.
//
// Usage contract:
//   1. Construct Client(config).
//      The constructor wires the egress observer, starts the agent, and
//      launches the background runner.  No separate start() call needed.
//   2. (Optional) Register callbacks BEFORE calling connect():
//        client.onConnected([](int64_t sid){ ... });
//        client.onDisconnected([](){ ... });
//        client.onError([](const std::string& msg){ ... });
//   3. co_await / .get() on the future returned by connect().
//   4. Send pings:
//        auto f = client.ping({'h','i',0,0xFF});
//        auto echo = f.get();
//   5. Call close() when done, or rely on the dtor (both are idempotent).

#pragma once

#include <rune/client/agent/ClientAgent.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// Bytes alias
// ---------------------------------------------------------------------------
using Bytes = std::vector<uint8_t>;

// ---------------------------------------------------------------------------
// IdleMode — controls the runner-thread idle strategy.
//
// LowLatency: YieldingIdleStrategy — on each idle cycle the runner calls
//   std::this_thread::yield() instead of parking.  This keeps the runner
//   hot (no OS park latency) so it reacts to a submitted command / inbound
//   frame in ~µs.  Cost: one CPU core pegged at ~100% while the runner is
//   alive, even when idle.  Choose for latency-sensitive paths (game clients,
//   real-time systems).
//
// Balanced (default): BackoffIdleStrategy — spins a few cycles → yields →
//   parks up to 1 ms.  CPU-friendly for idle clients that do not need
//   sub-millisecond reaction time.
// ---------------------------------------------------------------------------
enum class IdleMode { Balanced, LowLatency };

// ---------------------------------------------------------------------------
// ClientConfig
// ---------------------------------------------------------------------------
struct ClientConfig {
    std::string gatewayChannel   = "aeron:ipc";
    int32_t     gatewayStreamId  = 1;
    std::string egressChannel    = "aeron:ipc";
    int32_t     egressStreamId   = 2;
    std::string dataDir;

    // High-level timeouts (milliseconds).
    int64_t connectTimeoutMs = 5000;
    int64_t pingTimeoutMs    = 5000;

    // Keepalive interval passthrough (nanoseconds, 0 = use agent default).
    int64_t keepaliveIntervalNs = 0;

    // Runner-thread idle strategy.
    // Balanced (default) parks up to 1 ms between doWork() calls — CPU-friendly.
    // LowLatency uses a yielding idle — no park, sub-µs reaction time, 1 CPU core cost.
    IdleMode idleMode = IdleMode::Balanced;

    // CB-2: Static topic configuration for the producer.
    // Mirrors the broker's RUNE_TOPICS environment variable.
    // TODO(dynamic): fetch topic metadata from the broker on connect.
    std::vector<TopicSpec> topics;
};

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------
class Client {
public:
    // -------------------------------------------------------------------------
    // Construction / destruction
    // -------------------------------------------------------------------------

    /// Construct and start the client.
    ///
    /// Sets the egress observer BEFORE starting the agent (required by CC-5
    /// contract), starts the agent, and launches the background runner thread.
    ///
    /// Callbacks (onConnected / onDisconnected / onError) MUST be registered
    /// before calling connect(); they may be set up to this point safely from
    /// the constructing thread before connect() is called.
    explicit Client(ClientConfig cfg);

    /// Idempotent.  Calls close() which stops the runner, stops the agent,
    /// fails any still-pending futures, and fires onDisconnected.
    ~Client();

    // Non-copyable, non-movable.
    Client(const Client&)            = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&)                 = delete;
    Client& operator=(Client&&)      = delete;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// Stop the runner (signal + join), stop the agent, fail any still-pending
    /// futures with an error, and fire onDisconnected.
    /// Idempotent: safe to call multiple times or after the dtor.
    void close();

    // -------------------------------------------------------------------------
    // Callback registration — MUST be called before connect()  (FIX F4)
    //
    // These setters are SETUP-PHASE ONLY: they are single-threaded and must be
    // called BEFORE the first connect().  The callback std::functions are read
    // on the runner thread once connect() traffic can arrive; setting them after
    // connect() would be an unsynchronised data race on the std::function.  The
    // contract is now ENFORCED: each setter asserts (debug) and no-ops with a
    // one-shot error (release) if called after connect() — see Client.cpp.  The
    // first connect() publishes a happens-before (release) that the runner
    // thread acquires, so callbacks set before connect() are safely visible.
    //
    // onConnected/onError fire on the runner/agent thread. onDisconnected fires
    // on the thread that calls close() (or the dtor's thread). Do NOT block
    // inside a callback, and do NOT call close() from within a callback that runs
    // on the runner thread (onConnected/onError) — that self-joins the runner
    // (UB). If a callback must close, hand the close() off to another thread.
    // -------------------------------------------------------------------------

    void onConnected(std::function<void(int64_t sessionId)> cb);
    void onDisconnected(std::function<void()> cb);
    void onError(std::function<void(const std::string&)> cb);

    // -------------------------------------------------------------------------
    // State observer
    // -------------------------------------------------------------------------

    /// Thread-safe (atomic snapshot from the agent).
    [[nodiscard]] session::SessionState state() const noexcept;

    // -------------------------------------------------------------------------
    // API (called from the APP thread)
    // -------------------------------------------------------------------------

    /// Submit a connect request.
    ///
    /// Returns a future that:
    ///   - resolves (set_value) when the agent receives ConnectResponse{OK};
    ///     onConnected(sessionId) fires immediately after;
    ///   - rejects (set_exception) on ConnectResponse/ErrorResponse{error} or
    ///     when connectTimeoutMs elapses.
    std::future<void> connect();

    /// Encode and submit a Ping, returning a future for the echo bytes.
    ///
    /// Requires an established session (state() == Connected).  If the session
    /// is not Connected the future is rejected immediately with a clear error
    /// (Milestone-A decision: reject rather than queue).
    ///
    /// The future resolves with the echo Bytes when the gateway's Pong arrives
    /// with a matching correlationId.  Binary-safe (NUL bytes, 0xFF, empty).
    ///
    /// Rejected with a timeout error if pingTimeoutMs elapses without a Pong.
    std::future<Bytes> ping(Bytes payload);

    // ---- CB-2: Producer API --------------------------------------------------

    /// Look up a topic by name and return its handle (topicId + partitionCount).
    /// Returns an invalid handle (topicId == -1) if the topic is not found in
    /// the static ClientConfig::topics list.
    /// TODO(dynamic): fetch topic metadata from the broker on connect.
    [[nodiscard]] TopicHandle topic(const std::string& name) const noexcept;

    /// Create a Producer with the given config.
    /// The producer uses the same underlying agent ring and session as the Client.
    /// MUST be called before connect() — producers are registered in the hub
    /// during the setup phase.  (Calling after start() is a data race on the hub.)
    Producer producer(ProducerConfig cfg = {});

private:
    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    struct PendingConnect {
        std::promise<void>                          promise;
        std::chrono::steady_clock::time_point       deadline;
    };

    struct PendingPing {
        std::promise<Bytes>                         promise;
        std::chrono::steady_clock::time_point       deadline;
    };

    // -------------------------------------------------------------------------
    // Runner
    // -------------------------------------------------------------------------

    /// Background thread body: loops doWork() under the configured IdleStrategy
    /// (YieldingIdleStrategy for LowLatency, BackoffIdleStrategy for Balanced)
    /// until stopped_, performing a periodic timeout sweep.
    void runLoop();

    /// Check pending-connect / pending-ping timeouts.  Called from the runner.
    void sweepTimeouts();

    // -------------------------------------------------------------------------
    // Egress observer (runs on the runner/agent thread)
    // -------------------------------------------------------------------------

    void onEgressFrame(codec::TemplateId tid,
                       const uint8_t*    buf,
                       std::size_t       len);

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    static int64_t nowNs() noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
    }

    /// Fail all pending futures with the given message.
    /// Caller must NOT hold mtx_ (acquires it internally).
    void failAllPending(const std::string& reason);

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    ClientConfig cfg_;

    // ---- Agent (runner-thread owned after start; app uses submit* + snapshot) -
    agent::ClientAgent agent_;

    // ---- Runner thread -------------------------------------------------------
    std::atomic<bool> stopped_   {false};
    std::thread       runnerThread_;

    // ---- Callback-contract guard (FIX F4) ------------------------------------
    // Set (release) by the first connect() on the app thread.  Once true, the
    // callback setters refuse to mutate the std::functions (which the runner
    // thread may now read), preventing an unsynchronised data race.  The
    // release store here + acquire load on the runner thread establish the
    // happens-before that publishes the pre-connect() callbacks.
    std::atomic<bool> connectCalled_ {false};

    // ---- Cross-thread promise maps (guarded by mtx_) -------------------------
    // pendingConnect_: at most one in-flight connect at a time.
    // pendingPings_:   map from correlationId → PendingPing.
    std::mutex                              mtx_;
    std::unique_ptr<PendingConnect>         pendingConnect_;
    std::map<int64_t, PendingPing>          pendingPings_;

    // ---- Monotonic correlationId counter (atomic; incremented by app thread) -
    std::atomic<int64_t> nextCorrId_ {1};

    // ---- Callbacks (set before connect(); read on runner thread after that) --
    std::function<void(int64_t)>      cbConnected_;
    std::function<void()>             cbDisconnected_;
    std::function<void(const std::string&)> cbError_;

    // ---- Timeout sweep cadence -----------------------------------------------
    // Sweep every ~100 ms (50 iterations × 2 ms sleep inside BackoffIdleStrategy
    // max-park period is 1 ms, so we sweep roughly every N doWork calls).
    static constexpr int64_t kSweepIntervalMs = 100;
    std::chrono::steady_clock::time_point nextSweep_;

    // ---- Flag: did we already fire onDisconnected on close()? ----------------
    std::atomic<bool> disconnectedFired_ {false};

    // ---- CB-2: ProducerHub + topic registry ----------------------------------
    // Hub is owned by Client and wired into the agent before start().
    producer::ProducerHub producerHub_;
};

} // namespace rune::client
