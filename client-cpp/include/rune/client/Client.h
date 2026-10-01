// include/rune/client/Client.h
//
// CC-6 — Public Client API: connect()/ping() futures + lifecycle callbacks.
// Namespace: rune::client
//
// Threading model:
//   - Task 12: RunnerMode::Invoker (opt-in) runs NO threads — the constructing
//     thread calls Client::doWork() (one dutyCycle) in its own loop; everything
//     below that says "runner thread" then means that owner thread.
//   - The Client owns a background runner thread that loops dutyCycle()
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
#include <rune/client/metadata/MetadataCache.h>
#include <rune/client/metadata/PartitionRouter.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/TransactionalProducer.h>
#include <rune/client/producer/TransactionalProducerImpl.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/TypedTopic.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// Bytes alias
// ---------------------------------------------------------------------------
using Bytes = std::vector<uint8_t>;

// ---------------------------------------------------------------------------
// Schema DDL result types (Task 10)
// ---------------------------------------------------------------------------
struct DefineTopicResult {
    int32_t  topicId   = -1;
    uint16_t version   = 0;
    uint16_t errorCode = 0;
};

struct TopicSchemaResult {
    int32_t              topicId   = -1;
    uint16_t             version   = 0;
    std::vector<uint8_t> descriptorBlob;
    uint16_t             errorCode = 0;
};

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
//
// BusySpin: NEVER parks or yields — a tight doWork loop with a cpu_pause() on
//   idle cycles.  Lowest possible reaction time (sub-µs) → the client side never
//   adds park latency to a produce/ack or consume round-trip (needed for a
//   sub-100µs p99 target).  Cost: pegs one CPU core continuously; use only on a
//   dedicated core.  LowLatency still parks 1–100µs, so its p99 floor is ~100µs.
// ---------------------------------------------------------------------------
enum class IdleMode { Balanced, LowLatency, BusySpin };

// ---------------------------------------------------------------------------
// RunnerMode — who runs the client duty cycle (spec §4.1).
//   Thread  (default): a background runner thread under the IdleMode strategy.
//   Invoker (opt-in):  no threads at all — the embedded driver and the Aeron
//                      client conductor are invoker-driven and the constructing
//                      thread calls Client::doWork() in its own loop.
// ---------------------------------------------------------------------------
enum class RunnerMode : uint8_t { Thread, Invoker };

// Injected monotonic clock (ns), sampled once per duty cycle.  nullptr = steady clock.
using NanoClock = int64_t (*)() noexcept;

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
    int64_t connectTimeoutMs   = 5000;
    int64_t pingTimeoutMs      = 5000;
    int64_t produceTimeoutMs   = 5000; // max wait for a ProduceResponse ack

    // Aeron term buffer length (bytes) for the embedded driver's log buffers.
    // Power of two in [64 KiB, 1 GiB].  Default 4 MiB; raise (e.g. 32 MiB) for
    // deep in-flight on high-throughput small-record producing.
    std::size_t termBufferBytes = 4u * 1024 * 1024;

    // Explicit Aeron knobs (spec §4.1/§4.3).  Env overrides (RUNE_CLIENT_AERON_MTU,
    // RUNE_AERON_SOCKET_BUFFER_BYTES, RUNE_CLIENT_AERON_DRIVER_IDLE) win; a
    // malformed env value is logged once at construction and not applied.
    // mtuBytes: Aeron's default 1408 (fits a 1500-byte path unfragmented).  8192
    // (transport::kJumboMtuLength) is OPT-IN for paths that carry jumbo datagrams
    // end to end (loopback, jumbo-frame LANs) — never the default.
    // socketBufferBytes: 0 = OS default; otherwise >= Aeron's 128 KiB receive
    // window (the driver refuses a smaller SO_RCVBUF; validated at start).
    std::size_t mtuBytes          = 1408;
    std::size_t socketBufferBytes = 2u * 1024 * 1024;  // SO_SNDBUF and SO_RCVBUF

    // M2 FIX [CB-5]: subscribe-request timeout — if a SubscribeResponse does
    // not arrive within this window the pending subscribe is failed (active
    // cleared) so the app is not left polling subscriptionId() forever.
    int64_t subscribeTimeoutMs = 10000; // max wait for a SubscribeResponse

    // Keepalive interval passthrough (nanoseconds, 0 = use agent default).
    int64_t keepaliveIntervalNs = 0;

    // Runner-thread idle strategy.
    // Balanced (default) parks up to 1 ms between doWork() calls — CPU-friendly.
    // LowLatency uses a yielding idle — no park, sub-µs reaction time, 1 CPU core cost.
    IdleMode idleMode = IdleMode::Balanced;

    RunnerMode runnerMode = RunnerMode::Thread;
    // Thread mode: pin the runner thread to this CPU (Linux; -1 = do not pin).
    int32_t    runnerCore = -1;
    // Duty-cycle clock (tests inject; nullptr = std::chrono::steady_clock).
    NanoClock  nanoClock  = nullptr;
    // Aeron client liveness: in Invoker mode doWork() must run at least this often.
    int64_t    aeronClientLivenessTimeoutNs = 10'000'000'000LL;
    // T15 D1: run the embedded driver + Aeron conductor on the duty thread even in
    // RunnerMode::Thread (DriverMode::Invoker: no driver thread, no conductor
    // thread — the runner pumps both).  Default false: a plain Client keeps its
    // driver and conductor threads.  Shard clients force true (shardClientConfig).
    bool       driverOnDutyThread = false;
    // T15 R1: sessions never give up once Live (SessionConfig::
    // reconnectForeverAfterLive).  Default false = today's behaviour; shard
    // clients force true.
    bool       reconnectForeverAfterLive = false;

    // CB-2: Static topic configuration for the producer.
    // Mirrors the broker's RUNE_TOPICS environment variable.
    // TODO(dynamic): fetch topic metadata from the broker on connect.
    std::vector<TopicSpec> topics;
};

/// Maps a ClientConfig to the agent/transport configuration (applies env overrides).
[[nodiscard]] agent::AgentConfig toAgentConfig(const ClientConfig& c);

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

    /// Invoker mode: one duty cycle (driver + conductor, commands, egress, sessions,
    /// ProduceResponses, producers, periodic sweep).  Returns the work count
    /// (0 = idle; the caller applies its own idle strategy).  Call only from the
    /// constructing thread.  Re-entry from a callback fired inside the duty cycle
    /// returns 0 without running a nested cycle.  Thread mode: throws logic_error.
    int doWork();

    /// True once the Aeron client closed itself (invoker owner stalled past
    /// aeronClientLivenessTimeoutNs, or the driver timed out).
    [[nodiscard]] bool transportClosed() const noexcept { return agent_.transportClosed(); }

    /// Test hook (T15a): the transport, for its thread-model observers
    /// (usesDriverThread / usesConductorInvoker / aeronMediaDriverTimeoutMs, and
    /// callerPumpViolations once the runner is joined).
    [[nodiscard]] const transport::AeronTransport& testTransport() const noexcept {
        return agent_.testTransport();
    }

    /// Test hook: transactional producers adopted by the duty-cycle thread.
    [[nodiscard]] std::size_t testTxnProducerCount() const noexcept { return txnProducers_.size(); }
    /// Test hook: producers adopted by the duty-cycle thread.
    [[nodiscard]] std::size_t testProducerCount() const noexcept { return producerHub_.size(); }
    /// Test hook: fill the agent command ring so the next registration finds it full.
    void testFillCommandRing() { agent_.testFillCommandRing(); }
    /// fix5 Important-2(c) test hooks: orphan a claim on the agent command ring
    /// (claimed, never committed), lower the close-drain unblock threshold, and
    /// read how often close()'s drain had to unblock.
    aeron::util::index_t testClaimCommandRingWithoutCommitting(std::size_t payloadLen) {
        return agent_.testClaimNoopWithoutCommitting(payloadLen);
    }
    void testSetCloseDrainUnblockAfterYields(int n) noexcept {
        agent_.testSetCloseDrainUnblockAfterYields(n);
    }
    [[nodiscard]] int testCloseDrainUnblockCalls() const noexcept {
        return agent_.testCloseDrainUnblockCalls();
    }
    /// fix5 (txn symmetry) test hook: called inside submitRegistration_ AFTER its
    /// up-front stopped_ fast-fail check and BEFORE the registration is written
    /// to the ring — the exact window the post-submit compare_exchange re-check
    /// in producer()/transactionalProducer() exists for. A test calls close()
    /// from here to force "close() won the race" deterministically. Cold path
    /// (registration only); empty by default.
    void testSetBeforeRegistrationSubmitHook(std::function<void()> fn) {
        testBeforeRegistrationSubmitHook_ = std::move(fn);
    }
    /// Task 14 fix1 I2 test hook (owner thread, Invoker): fn runs at the top of
    /// every dutyCycle(), inside its in-cycle guard (nullptr = none). A throwing
    /// fn simulates a duty cycle that throws.
    void testSetDutyCycleHook(void (*fn)()) noexcept { testDutyCycleHook_ = fn; }
    /// Test hook (duty-cycle thread only): install a partition route as metadata would.
    void testSetRoute(int32_t topicId, int32_t partition, const std::string& endpoint) {
        agent_.setRoute(topicId, partition, endpoint);
    }
    /// fix1 C1 test hook: total user-callback exceptions caught (never let a
    /// throwing callback escape a noexcept frame / a runner thread and
    /// terminate the process). Sums this Client's own onConnected/onError/
    /// onDisconnected catches with the subscriber and group-consumer hubs'.
    /// (ProducerImpl's own PendingAck/batchAck callback catches — fix2 minor —
    /// are counted separately per producer; see ProducerImpl::testCallbackExceptionCount().)
    ///
    /// fix2 minor-5: backed by PLAIN counters (no atomics — shared-nothing
    /// rule), so this is only safe to call: in Invoker mode, from the owner
    /// thread (the only thread that ever writes them); in Thread mode, AFTER
    /// close() has returned (which joins the runner first — no writer is left
    /// running). Calling it from another thread while a Thread-mode Client is
    /// still live is a data race.
    [[nodiscard]] uint64_t testCallbackExceptionCount() const noexcept;

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

    // ---- Task 10: Schema DDL round-trips ------------------------------------

    /// Submit a DefineTopic request and return a future for the result.
    ///
    /// Requires an established session (state() == Connected).  If the session
    /// is not Connected the future is rejected immediately.
    ///
    /// The future resolves when DefineTopicResponse arrives with a matching
    /// correlationId.  errorCode==0 means success; non-zero carries an ErrorCode.
    ///
    /// Rejected with a timeout error if pingTimeoutMs elapses without a response.
    std::future<DefineTopicResult> defineTopic(std::string            name,
                                               uint32_t               partitions,
                                               std::string            keyField,
                                               bool                   validate,
                                               std::span<const uint8_t> descriptorBlob);

    /// Submit a GetTopicSchema request and return a future for the result.
    ///
    /// Requires an established session (state() == Connected).  Rejected
    /// immediately if the session is not Connected.
    ///
    /// The future resolves when TopicSchemaResponse arrives; descriptorBlob is
    /// owned (copied from the Aeron buffer).  errorCode==UnknownTopic(7) if the
    /// topic does not exist.
    ///
    /// Rejected with a timeout error if pingTimeoutMs elapses without a response.
    std::future<TopicSchemaResult> getTopicSchema(std::string name);

    // ---- Task 9 (CLI): list topics known to this client ----------------------

    /// Return a snapshot of all topics that were successfully defined through
    /// this client instance (errorCode==0 DefineTopicResponse).
    ///
    /// Returns a vector of (name, topicId) pairs in insertion order.
    /// Thread-safe: takes a lock-guarded snapshot.
    [[nodiscard]] std::vector<std::pair<std::string, int32_t>> listTopics();

    // ---- Task 11: Typed topic API --------------------------------------------

    /// Open a typed topic, verifying the broker's schema fingerprint matches T.
    ///
    /// Chains on getTopicSchema(name):
    ///   - If errorCode != OK: the returned future is rejected.
    ///   - If schemaMatches(T::kFingerprint, blob) is false: rejected with
    ///     "generated type is stale — rerun rune schema generate".
    ///   - Otherwise: populates TopicSchemaCache and resolves with
    ///     TypedTopic<T>(producer(topicId), topicId).
    ///
    /// The fingerprint check is the EXACT-fingerprint gate: the generated struct
    /// and the broker's stored descriptor must be byte-identical in structure.
    ///
    /// Requires an established session (same as getTopicSchema).
    /// The continuation runs on a detached thread (std::async(std::launch::async)).
    template <class T>
    [[nodiscard]] std::future<TypedTopic<T>> openTopic(std::string name) {
        // Obtain the schema from the broker.
        auto schemaFut = getTopicSchema(name);

        // Chain a continuation: runs on a separate thread, awaits schemaFut.
        // RULE #1 (shared-nothing): no shared mutable state is introduced here.
        // `this` is accessed only to call producer() which uses the agent ring
        // (thread-safe) and to call topicSchemaCacheStore() (mutex-guarded below).
        return std::async(std::launch::async,
            [this, schemaFut = std::move(schemaFut)]() mutable -> TypedTopic<T> {
                // Wait for the schema response (may throw on error/timeout).
                TopicSchemaResult r = schemaFut.get();  // throws if rejected

                // Check broker-side error code.
                if (r.errorCode != 0) {
                    throw std::runtime_error(
                        "openTopic: broker returned errorCode " +
                        std::to_string(r.errorCode));
                }

                // Exact-fingerprint gate.
                if (!schemaMatches(T::kFingerprint,
                                   std::span<const uint8_t>(r.descriptorBlob))) {
                    throw std::runtime_error(
                        "openTopic: generated type is stale — rerun rune schema generate");
                }

                // Populate the per-topic schema metadata cache (guarded by mtx_
                // so it is safe to call from this async thread).
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    topicSchemaCache_.put(r.topicId,
                        metadata::TopicSchemaEntry{r.version, /*validate=*/false, /*keyField=*/""});
                }

                // Create a Producer for this typed topic.
                Producer p = producer();
                return TypedTopic<T>(std::move(p), r.topicId);
            });
    }

    // ---- CB-4: Subscribe API -------------------------------------------------

    /// Subscribe to a topic partition using a Key (partition resolved via FNV-1a).
    ///
    /// Returns a Subscription handle immediately.  Records begin flowing once
    /// the broker acknowledges (SubscribeResponse) — the Subscription does NOT
    /// block for that ack.
    ///
    /// Threading:
    ///   - subscribe() is called on the APP thread.
    ///   - The handler fires on the RUNNER (agent) thread.  Must NOT block.
    ///   - The record spans (key, value) are zero-copy views into the Aeron
    ///     buffer, valid ONLY during the handler call.  Copy data to retain it.
    ///   - unsubscribe() / Subscription dtor are safe from any thread.
    ///   - Do NOT call unsubscribe() from inside the handler.
    ///
    /// Demand / back-pressure: default is broker-paced push (the broker pauses
    /// when egress back-pressure applies, per GB-3).
    /// TODO(CB-5): request(n) demand for Reactive-Streams style flow control.
    Subscription subscribe(TopicHandle       topic,
                           const Key&        key,
                           SubscribeOptions  opts,
                           RecordHandler     handler);

    /// Subscribe overload taking an explicit partition number (no key required).
    Subscription subscribe(TopicHandle       topic,
                           int32_t           partition,
                           SubscribeOptions  opts,
                           RecordHandler     handler);

    // ---- CB-2: Producer API --------------------------------------------------

    /// Look up a topic by name and return its handle (topicId + partitionCount).
    /// Returns an invalid handle (topicId == -1) if the topic is not found in
    /// the static ClientConfig::topics list.
    /// TODO(dynamic): fetch topic metadata from the broker on connect.
    [[nodiscard]] TopicHandle topic(const std::string& name) const noexcept;

    /// Create a Producer with the given config.
    /// The producer uses the same underlying agent ring and session as the Client.
    /// May be called before or after connect(); registration is submitted through
    /// the agent command ring and is thread-safe.
    ///
    /// Wires the producer's pump hook (ProducerImpl::setPumpFn) to this Client,
    /// in both runner modes (fix1 C1): a back-pressured blocking send() either
    /// drives the duty cycle itself (Invoker owner thread, outside a callback),
    /// fails fast instead of recursively re-entering the agent's own poll (any
    /// duty-cycle thread already mid-cycle, either mode), or falls back to its
    /// heartbeat-bounded wait (a genuinely separate thread) — see
    /// ProducerImpl::sendBlocking / PumpResult.
    Producer producer(ProducerConfig cfg = {});

    /// Test hook: like producer(), but with an explicit ring capacity (small ring
    /// to exercise back-pressure without filling the real default).
    Producer testProducerWithRingCap(ProducerConfig cfg, int32_t ringCap);

    // ---- TX-5: Transactional Producer API -----------------------------------

    /// Create a TransactionalProducer with the given transactional id.
    /// MUST be called before connect() (setup phase).
    /// The returned handle's initTransactions() / beginTransaction() / send() /
    /// commitTransaction() / abortTransaction() lifecycle is fully thread-safe
    /// via the agent duty cycle.
    TransactionalProducer transactionalProducer(const std::string& transactionalId,
                                                ProducerConfig cfg = {});

    // ---- CG-5: Consumer-group API -------------------------------------------

    /// Join a consumer group and begin receiving pushed Records for assigned
    /// partitions.
    ///
    /// Returns a GroupConsumer handle.  The handle fires onAssigned/onRevoked
    /// callbacks on the runner thread as the group coordinator assigns or
    /// revokes partitions.  Records for assigned partitions are pushed via the
    /// broker's server-side subscription and routed through onRecords.
    ///
    /// Threading:
    ///   - subscribeGroup() is called on the APP thread (setup phase, before
    ///     connect() or right after — same contract as producer()).
    ///   - onAssigned / onRevoked / onRecords callbacks fire on the RUNNER thread.
    ///   - commit() / commitSync() are wait-free from the APP thread.
    ///   - close() on the handle is safe from any thread.
    ///
    /// topicNames are resolved against ClientConfig::topics.  Topics not found
    /// in the static list are silently skipped (topicId lookup fails).
    GroupConsumer subscribeGroup(std::string                groupId,
                                 std::vector<std::string>   topicNames,
                                 GroupConfig                cfg = {});

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

    // Task 10: schema DDL pending ops (guarded by mtx_, same as pendingPings_).
    struct PendingDefine {
        std::promise<DefineTopicResult>             promise;
        std::chrono::steady_clock::time_point       deadline;
        std::string                                 topicName;  // saved for the knownTopics registry
    };

    struct PendingGet {
        std::promise<TopicSchemaResult>             promise;
        std::chrono::steady_clock::time_point       deadline;
    };

    // -------------------------------------------------------------------------
    // Runner
    // -------------------------------------------------------------------------

    /// Teardown shared by close() and ~Client (no contract checks; idempotent).
    void closeImpl_();

    /// Background thread body: loops doWork() under the configured IdleStrategy
    /// (YieldingIdleStrategy for LowLatency, BackoffIdleStrategy for Balanced)
    /// until stopped_, performing a periodic timeout sweep.
    void runLoop();

    /// Check pending-connect / pending-ping timeouts.  Called from the duty cycle
    /// every kSweepIntervalMs of duty-cycle time.
    void sweepTimeouts(int64_t nowNs);

    /// One duty cycle: agent -> ProduceResponses (plain calls into the hub) ->
    /// producer drain/flush through AgentProduceSink -> periodic sweep.  Shared by
    /// the runner thread (Thread mode) and Client::doWork() (Invoker mode).
    int  dutyCycle(int64_t nowNs);
    int  producerDuty_(int64_t nowNs);
    /// Route the agent's buffered ProduceResponses into the hub (plain calls) and
    /// clear the buffer.  Run by dutyCycle() and at the top of every resend path so
    /// an ack that arrived earlier in the same poll is applied before a resend.
    int  routeBufferedAcks_();
    [[nodiscard]] int64_t now() const noexcept {
        return cfg_.nanoClock != nullptr ? cfg_.nanoClock() : nowNs();
    }

    /// Shared implementation of producer() / testProducerWithRingCap().
    /// direct = Task 14 Direct ingress (Invoker producer(): owner-thread appends,
    /// no ring hop); false = Ring ingress (Thread mode, and the ring test seam).
    Producer producerWithRingCap_(ProducerConfig cfg, int32_t ringCap, bool direct);

    /// fix1 C1: run a user callback, catching and counting/logging any escaping
    /// exception instead of letting it unwind into a noexcept frame
    /// (ClientAgent::doWork / Client::pumpTrampoline_) or off the end of the
    /// runner thread's body — either of which terminates the process. A user
    /// callback that needs confirmation of acceptance should use trySend, not
    /// the throwing blocking send(), from inside a callback (see Producer.h).
    template <class Fn>
    void invokeUserCallback_(const char* name, Fn&& fn) noexcept {
        try {
            fn();
        } catch (const std::exception& e) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: user callback '%s' threw: %s (caught; the duty cycle continues)\n",
                name, e.what());
        } catch (...) {
            ++callbackExceptions_;
            std::fprintf(stderr,
                "rune-client: user callback '%s' threw a non-std::exception "
                "(caught; the duty cycle continues)\n", name);
        }
    }

    /// fix1 C1: pump hook bound to every ProducerImpl via setPumpFn(), in BOTH
    /// runner modes. On the duty-cycle thread (Invoker owner, or the Thread-mode
    /// runner) already inside a cycle: DeclinedInCycle (pumping would recursively
    /// re-enter the agent's own poll). On the Invoker owner thread, NOT inside a
    /// cycle: Pumped (runs one dutyCycle()). Anything else (Thread mode's
    /// ordinary app thread, or a foreign thread touching an Invoker producer off
    /// its owner thread): NotOwner — a genuinely separate thread may still drain
    /// the ring in real time, so the caller falls back to its heartbeat-bounded
    /// wait instead.
    static producer::PumpResult pumpTrampoline_(void* ctx);  // fix1 I2: may throw (dutyCycle)

    /// Setup-phase ring submit, bounded (5 s).  Invoker mode runs the duty cycle
    /// between attempts — this thread IS the ring's consumer; thread mode parks
    /// 100 µs.  On timeout runs `onFail` and throws.
    template <class Submit, class OnFail>
    void submitRegistration_(Submit&& submit, OnFail&& onFail) {
        // After close() no duty cycle will ever adopt the holder (and Thread mode
        // would park 5 s on a ring nobody drains): fail fast, nothing leaks.
        // A fast-fail only: correctness of the registration-vs-close race does
        // NOT rest on this load (a load creates no happens-before edge from the
        // later ring commit to close()'s drain). It rests on the post-submit
        // seq_cst compare_exchange in producerWithRingCap_ (fix3 C1; the full
        // argument is there and at closeImpl_'s exchange).
        if (stopped_.load(std::memory_order_seq_cst)) {
            onFail();
            throw std::logic_error("client closed");
        }
        if (testBeforeRegistrationSubmitHook_) [[unlikely]] testBeforeRegistrationSubmitHook_();
        if (submit()) return;
        // Ring full.  Pumping is only safe on the duty-cycle thread OUTSIDE a cycle:
        // from a duty-cycle callback it would re-enter the agent's own poll, and in
        // Invoker mode a non-owner thread must never run the duty cycle.  Thread
        // mode: an app thread parks while the runner drains; the runner itself
        // (callback) cannot wait for itself.  inDutyCycle_ is read only on the
        // duty-cycle thread (no cross-thread read).
        const bool onDutyThread = std::this_thread::get_id() == dutyThreadId_;
        if ((onDutyThread && inDutyCycle_) ||
            (cfg_.runnerMode == RunnerMode::Invoker && !onDutyThread)) {
            onFail();
            throw std::logic_error("ring full: cannot pump from callback/non-owner thread");
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!submit()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                onFail();
                throw std::runtime_error("Client: command ring full — registration timed out");
            }
            if (cfg_.runnerMode == RunnerMode::Invoker) {
                (void)dutyCycle(now());
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds{100});
            }
        }
    }

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

    /// Internal subscribe implementation shared by Key and partition overloads.
    Subscription subscribeImpl(TopicHandle              topic,
                                int32_t                  partition,
                                uint8_t                  partitionKnown,
                                std::span<const uint8_t> keyBytes,
                                SubscribeOptions         opts,
                                RecordHandler            handler);

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    ClientConfig cfg_;

    // ---- Agent (runner-thread owned after start; app uses submit* + snapshot) -
    agent::ClientAgent agent_;

    // ---- Runner thread -------------------------------------------------------
    std::atomic<bool> stopped_   {false};
    // fix5: see testSetBeforeRegistrationSubmitHook(). Empty by default.
    std::function<void()> testBeforeRegistrationSubmitHook_;
    // Task 14 fix1 I2 test hook: called at the top of every dutyCycle() (inside
    // the in-cycle guard). Plain function pointer, duty-cycle thread only.
    void (*testDutyCycleHook_)() = nullptr;
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
    // pendingDefine_:  map from correlationId → PendingDefine (Task 10).
    // pendingGet_:     map from correlationId → PendingGet    (Task 10).
    std::mutex                              mtx_;
    std::unique_ptr<PendingConnect>         pendingConnect_;
    std::map<int64_t, PendingPing>          pendingPings_;
    std::map<int64_t, PendingDefine>        pendingDefine_;
    std::map<int64_t, PendingGet>           pendingGet_;

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
    int64_t         nextSweepNs_ = 0;       // duty-cycle thread only
    bool            inDutyCycle_ = false;   // duty-cycle thread only
    // fix2 minor-5: count of user-callback exceptions caught at this Client's
    // own cbConnected_/cbError_/cbDisconnected_ call sites. PLAIN uint64_t, no
    // atomic (shared-nothing rule — single-writer, like nextSweepNs_/
    // inDutyCycle_ above): written ONLY on the duty-cycle thread while the
    // Client is live, and from closeImpl_'s thread strictly AFTER the runner
    // join (Thread mode) / on the owner thread (Invoker mode) — never
    // concurrently with the duty-cycle thread's own writes. See
    // testCallbackExceptionCount()'s doc for the read-side contract this
    // implies for callers (a test thread may NOT poll this while the Client is
    // still running in Thread mode).
    uint64_t callbackExceptions_ = 0;
    std::thread::id ownerThread_;           // constructing thread (Invoker owner)
    std::thread::id dutyThreadId_;          // thread running dutyCycle(): owner or runner
    // Built ONCE: the txn producers' send paths (no per-cycle std::function build).
    std::function<bool(int32_t, const uint8_t*, std::size_t)> txnSendFn_;
    std::function<bool(int32_t, const uint8_t*, std::size_t)> txnControlSendFn_;

    // ---- Flag: did we already fire onDisconnected on close()? ----------------
    std::atomic<bool> disconnectedFired_ {false};

    // ---- CB-2: ProducerHub + topic registry ----------------------------------
    // Hub is owned by Client and wired into the agent before start().
    producer::ProducerHub producerHub_;

    // ---- TX-5: Transactional producers ---------------------------------------
    // Duty-cycle-thread owned (registered through the agent ring, like producers).
    // Created at setup time (before connect()); rarely more than one per client.
    std::vector<std::shared_ptr<producer::TransactionalProducerImpl>> txnProducers_;

    // ---- CB-4: SubscriberHub -------------------------------------------------
    // Owned by Client.  subscribe() / onEgressFrame() / close() all access it.
    subscriber::SubscriberHub subscriberHub_;

    // ---- CG-5: GroupConsumerHub ---------------------------------------------
    // Owned by Client.  subscribeGroup() / onEgressFrame() / close() access it.
    group::GroupConsumerHub groupConsumerHub_;

    // ---- CB-5 B: data-op leader-redirect state -------------------------------
    // Task 1.1: the redirect latch (redirectPending) and the current-leader
    // scalars (currentLeaderChannel / currentLeaderNode) MOVED into the agent's
    // endpoint-keyed Session (agent_.seedSession()).  They are agent-thread-only
    // plain data (single-owner → no atomic, per the shared-nothing law).  The
    // Client reaches them through the seed session so that, in Task 1.3, each
    // node's redirect state lives with its own SessionMachine.
    //
    // Accessors (agent/runner-thread only) — thin helpers so the redirect/route
    // paths read/write the seed session's fields without repeating the deref.
    [[nodiscard]] bool& redirectPending() noexcept {
        return agent_.seedSession().redirectPending;
    }
    [[nodiscard]] std::string& currentLeaderChannel() noexcept {
        return agent_.seedSession().currentLeaderChannel;
    }
    [[nodiscard]] int32_t& currentLeaderNode() noexcept {
        return agent_.seedSession().currentLeaderNode;
    }

    // ---- MD1 Task 10: Metadata cache + partition router ----------------------
    // MetadataCache is populated from MetadataResponse frames (on connect and
    // after a zero-RTT reroute).  PartitionRouter maps key → partition → node.
    // Both are agent-thread-only (runner thread); no mutex required.
    metadata::MetadataCache  metadataCache_;
    metadata::PartitionRouter partitionRouter_;

    // ---- Task 11: per-topic schema metadata (populated by openTopic<T>) ------
    // Guarded by mtx_ (the openTopic async continuation writes it from an app
    // thread; any future reader must also hold mtx_).
    metadata::TopicSchemaCache topicSchemaCache_;

    // ---- Task 9 (CLI): names of topics successfully defined via defineTopic() -
    // Populated in onEgressFrame when DefineTopicResponse{OK} resolves a pending
    // define op.  Guarded by mtx_.  Ordered by insertion to preserve apply order.
    std::vector<std::pair<std::string, int32_t>> knownTopics_;

    // correlationId for the most recently sent MetadataRequest (0 = none).
    int64_t pendingMetadataCorrId_ {0};

    // MD1 review IMPORTANT-2 (proactive per-partition produce routing): the
    // current-leader channel/node scalars now live in agent_.seedSession()
    // (see currentLeaderChannel()/currentLeaderNode() above) — Task 1.1.

    // ---- MD1 review IMPORTANT-3: background metadata refresh w/ backoff -------
    // When no-progress is detected (unknown partition leader / repeated
    // redirects) a metadata refresh is scheduled with EXPONENTIAL backoff so the
    // client cannot storm the broker.  metadataBackoffNs_ is the wait before the
    // NEXT scheduled refresh; it doubles per schedule up to the cap and resets on
    // a successful MetadataResponse.  nextMetadataRefreshAt_ == time_point::max()
    // means "no refresh armed".  Agent-thread-only.
    static constexpr int64_t kMetadataBackoffInitNs = 100'000'000LL;    // 100 ms
    static constexpr int64_t kMetadataBackoffMaxNs  = 30'000'000'000LL; // 30 s
    int64_t metadataBackoffNs_ {kMetadataBackoffInitNs};
    std::chrono::steady_clock::time_point nextMetadataRefreshAt_ {
        std::chrono::steady_clock::time_point::max()};  // max() = not armed

    // sendMetadataRequest() — encodes and submits a MetadataRequest for all
    // topics to the agent ring.  Called on the runner thread after
    // ConnectResponse{OK} and by the backoff refresh sweep.
    void sendMetadataRequest(int64_t sessionId) noexcept;

    // scheduleMetadataRefresh() — arm a background metadata refresh with
    // exponential backoff (IMPORTANT-3).  Called on no-progress.  The backoff
    // doubles each call (capped) so the client can't storm.
    void scheduleMetadataRefresh() noexcept;

    // tickMetadataRefresh() — fire an armed refresh if its backoff window has
    // elapsed and the session is Connected.  Called from sweepTimeouts().
    void tickMetadataRefresh() noexcept;

    // Task 1.3: produce routing moved to the agent's flush→sendFn boundary
    // (ClientAgent partition routing table + per-leader sessions).  The old
    // single-transport routeProduceToLeader() retarget model is DELETED.

    // onMetadataResponse() — updates the cache from a decoded MetadataResponse.
    // Called on the runner thread via the metadataResponseFn_ hook.
    void onMetadataResponse(const codec::MetadataResponseDecoded& resp) noexcept;

public:
    // ---- MD1 review: test-only inspection hooks (agent-thread state) ----------
    // These expose the routing decision so tests can assert a keyed produce
    // resolves to the correct partition leader's endpoint via cached metadata,
    // and that a redirect updates the cache with zero full-refresh round-trip.
    // NOT part of the production API surface; safe to call only when the runner
    // is quiescent (tests pump deterministically).
    [[nodiscard]] std::string testRouteEndpoint(int32_t topicId,
                                                const Key& key) const {
        return partitionRouter_.routeKey(topicId, key).channel;
    }
    [[nodiscard]] int32_t testLeaderFor(int32_t topicId, int32_t partition) const {
        return metadataCache_.leaderFor(topicId, partition);
    }
    [[nodiscard]] int32_t testEpochFor(int32_t topicId, int32_t partition) const {
        return metadataCache_.epochFor(topicId, partition);
    }
    [[nodiscard]] std::string testCurrentLeaderChannel() const {
        return agent_.seedSession().currentLeaderChannel;
    }
};

} // namespace rune::client
