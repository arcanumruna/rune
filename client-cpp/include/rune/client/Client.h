// include/rune/client/Client.h
//
// CC-6 — Public Client API: control ops with completion tokens + lifecycle callbacks.
// Namespace: rune::client
//
// Threading model (F8c: no futures, no atomics, no mutex):
//   - Task 12: RunnerMode::Invoker (opt-in) runs NO threads — the constructing
//     thread calls Client::doWork() (one dutyCycle) in its own loop; everything
//     below that says "duty thread" then means that owner thread.
//   - RunnerMode::Thread (default): a background runner thread loops dutyCycle()
//     under the configured IdleStrategy.
//   - App threads never touch duty-owned state.  Every request (control op,
//     registration, callback setter, completion-queue add / release) is a
//     command on the agent's M2O command ring; the duty thread owns the pending
//     tables, the correlation-id counter and the session.  Results come back
//     as completions (rune/client/Completion.h): into a Completions queue the
//     issuing thread polls, or to a CompletionHandler on the duty thread.
//   - Exactly-once: an op either returns a non-Accepted OpResult (no
//     completion ever) or completes exactly once — with its result, its own
//     Timeout (duty-clock deadline), a broker error, NotConnected, SessionLost,
//     Superseded, or Closed (Client close completes every outstanding token).
//   - *AndWait() are cold-path blocking conveniences: pump-until-done on the
//     Invoker owner, an idle-strategy poll on a Thread-mode app thread; they
//     throw on any non-Ok outcome.  Never call them from a duty-cycle callback.
//   - EXCEPT the catalog reads (catalogHistory / catalogAsOf / catalogDiff /
//     catalogReader()): the Client's CatalogReader belongs to the thread that
//     CONSTRUCTED the Client, in every runner mode — any other thread gets
//     std::logic_error (its cache is owner-thread state, shared with no one).
//     Another app thread reads the catalog through its OWN reader:
//     `rune::client::CatalogReader mine(client);` constructed on that thread.
//   - Callbacks (onConnected / onError) fire on the duty thread, onDisconnected
//     on the thread that runs close().  Do not block inside them.
//
// Usage contract:
//   1. Construct Client(config) — wires the agent, starts the runner (Thread).
//   2. (Optional) callbacks: client.onConnected(...) etc. (ring-ordered before
//      any later command, so set them before connect to see its outcome).
//   3. client.connectAndWait();   or   client.connect(cq, token) + cq.poll(...)
//   4. auto echo = client.pingAndWait(bytes);
//   5. close() when done, or rely on the dtor (both are idempotent).

#pragma once

#include <rune/client/agent/ClientAgent.h>
#include <rune/client/session/SessionMachine.h>
#include <rune/client/codec/Codec.h>
#include <rune/client/metadata/MetadataCache.h>
#include <rune/client/metadata/PartitionRouter.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/subscriber/SubscriberHub.h>
#include <rune/client/group/GroupConsumer.h>
#include <rune/client/group/GroupConsumerHub.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/Completion.h>
#include <rune/client/Completions.h>
#include <rune/client/detail/CoordRoute.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/detail/StatusRing.h>
#include <rune/client/DutyView.h>
#include <rune/client/shard/ShardState.h>
#include <rune/client/catalog/CatalogReader.h>
#include <rune/client/detail/SecureZero.h>
#include <rune/client/security/SecurityDdl.h>
#include <rune/client/security/SecurityReader.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/NamespaceMessages.h>
#include <rune/client/NamespaceLimitFields.h>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <cstdint>
#include <cstdio>
#include <array>
#include <exception>
#include <expected>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// Bytes alias
// ---------------------------------------------------------------------------
using Bytes = std::vector<uint8_t>;

// ---------------------------------------------------------------------------
// Schema DDL result types (Task 10)
// ---------------------------------------------------------------------------
// Topic persistence for defineTopic (Phase 2b E).  The DefineTopic wire byte;
// values are APPEND-ONLY.
//   Disk      - durable log (default; what the overloads without it send).
//   Transient - replicated to the ISR before ack, never written to disk.  The
//               topic's metadata survives a broker restart, its data does not
//               (a restarted node rejoins empty; after a full-cluster restart
//               the topic is empty).
enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

/// Result of DefineTopic. errorCode: 0 OK; TopicExists=25 (the name - or a name
/// differing from it only in letter case - is taken); InvalidSchema=23 (bad
/// descriptor or partitions < 1); InvalidTopicName=40 (empty, > 249 bytes, "."/"..",
/// a byte outside [A-Za-z0-9._-], or the reserved "__" prefix).
struct DefineTopicResult {
    int32_t  topicId   = -1;
    uint16_t version   = 0;
    uint16_t errorCode = 0;
    std::string detail;   ///< catalog history: the broker's reason (an ErrorResponse message); "" on OK
    /// The DDL's requestId (generated, or DdlOptions::requestId): re-issuing
    /// with DdlOptions::requestId = this is idempotent (the leader dedupes).
    rune::protocol::RequestId requestId{};
};

// ---------------------------------------------------------------------------
// Catalog history (Task 5): DDL options, topic creation options, DDL result.
// ---------------------------------------------------------------------------

/// Carried by every DDL in the DDL extension (client version 8).
struct DdlOptions {
    std::string statement;                  ///< original DDL text (QL/CLI fill it; "" for API-issued)
    std::string clientName;                 ///< "" = ClientConfig::clientName
    /// {0,0} = the client generates one (its random instance id + a counter);
    /// reused verbatim on every resend of this DDL (F14).
    rune::protocol::RequestId requestId{};
    struct ReplayedFrom { std::string clusterId; int64_t offset = -1; std::string principal; };
    /// `rune catalog replay` only (Task 15) -> DdlExtension.hasReplayedFrom.
    std::optional<ReplayedFrom> replayedFrom;
};

/// DefineTopic's catalog-history fields (F32: TopicConfig is the ONE definition,
/// rune/protocol/CatalogTypes.h).  replicationFactor 0 = the cluster default.
struct TopicOptions {
    int16_t                     replicationFactor = 0;
    rune::protocol::TopicConfig config{};
};

/// Result of DropTopic / DropView / AlterView / AlterTopicConfig (DdlResponse 78).
/// errorCode 0 OK, else a broker ErrorCode (e.g. UnknownTopic 7, InvalidRequest 39
/// with detail); NotLeader (2) only once the retry budget ran out, Redirect (3)
/// only after more than Client::kMaxDdlRedirects hops.
struct DdlResult {
    uint16_t    errorCode  = 0;
    uint64_t    objectId   = 0;
    uint32_t    newVersion = 0;
    std::string detail;
    rune::protocol::RequestId requestId{};   ///< as DefineTopicResult::requestId
};

struct TopicSchemaResult {
    int32_t              topicId   = -1;
    uint16_t             version   = 0;
    std::vector<uint8_t> descriptorBlob;
    uint16_t             errorCode = 0;
    /// Phase 2b Part C: the topic's partition count as reported by the broker
    /// (TopicSchemaResponse trailing field); <= 0 = not reported (pre-2b broker)
    /// — typed topics then route over 1 partition (typedPartitionCount).
    int32_t              partitionCount = 0;
};

// ---------------------------------------------------------------------------
// Schema DDL result type (Task 18: Client::alterTopic / alterTopicAndWait)
// ---------------------------------------------------------------------------
struct AlterTopicResult {
    int32_t  topicId    = -1;
    uint16_t newVersion = 0;
    uint16_t errorCode  = 0;
    std::string detail;   ///< catalog history: the broker's reason; "" on OK
    rune::protocol::RequestId requestId{};   ///< as DefineTopicResult::requestId
};

// ---------------------------------------------------------------------------
// View-engine result types (Task 21: Client::createView / Client::getViewRow)
// ---------------------------------------------------------------------------

/// Result of CreateView (wire 70/71). errorCode is a broker ErrorCode (e.g.
/// ViewExists=32, ViewSourceNotFound=33, ViewFieldUnresolved=34, TopicExists=25,
/// InvalidTopicName=40 (same name rule as defineTopic), ViewSourceIsView=41 (a source is
/// another view's output - not supported until Phase 3),
/// ViewSourceTransient=37: a view that reads a transient topic must itself be
/// transient).
struct CreateViewResult {
    int32_t  viewTopicId       = -1;
    uint64_t outputFingerprint = 0;
    uint16_t errorCode         = 0;
    std::string detail;   ///< catalog history: the broker's reason; "" on OK
    rune::protocol::RequestId requestId{};   ///< as DefineTopicResult::requestId
};

/// Result of GetViewRow (wire 72/73). `status` is ViewRowResponse::Status
/// (0=OK, 1=AbsentKey, 2=NotReady, 3=SchemaMismatch, 4=UnknownView,
/// 5=InvalidRequest — the broker could not decode the request, non-retriable,
/// 6=Busy — the owning worker was full, the read was not performed, retry,
/// 7=RowTooLarge — the row exceeds the broker's egress frame, non-retriable,
/// 45=NotAuthorized — the session holds no READ on the view (authentication, client version 9; the same value as
/// ErrorCode::NotAuthorized; non-retriable, answered for a missing and a hidden view alike); matching
/// rune::protocol::client::ViewRowResponse's enum values exactly — duplicated
/// here as plain uint16_t constants so Client.h need not include
/// DataMessages.h). `rowBytes`'s OWN size is ground truth for the row length
/// (rowLen on the wire is informational only); empty unless status == kOk.
/// Decode positionally with rune::client::view::readSlot (ViewRowCodec.h)
/// against a RowLayout built from the view's own authored output field types.
struct ViewRowResult {
    static constexpr uint16_t kOk              = 0;
    static constexpr uint16_t kAbsentKey       = 1;
    static constexpr uint16_t kNotReady        = 2;
    static constexpr uint16_t kSchemaMismatch  = 3;
    static constexpr uint16_t kUnknownView     = 4;
    static constexpr uint16_t kInvalidRequest  = 5;
    static constexpr uint16_t kBusy            = 6;
    static constexpr uint16_t kRowTooLarge     = 7;   ///< the row exceeds the broker's egress frame (no bytes)
    static constexpr uint16_t kNotAuthorized   = 45;  ///< the session may not READ the view (no bytes; never retried)
    /// Client-side only: the view's source-partition leader was not reached within
    /// the retry budget; never sent by a broker.  NB: a GetViewRow's deadline is
    /// pingTimeoutMs (not ddlTimeoutMs); when that is shorter than ddlRetryBudget
    /// (the defaults: 5 s < 10 s) a read that is never routed ends as a Timeout
    /// (the *AndWait form throws), not as kNotLeader.
    static constexpr uint16_t kNotLeader       = 8;

    uint16_t              status = kUnknownView;
    std::vector<uint8_t>  rowBytes;
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
    // Stale-session threshold passthrough (SessionConfig::responseTimeoutNs;
    // nanoseconds, 0 = use the agent default).
    int64_t responseTimeoutNs = 0;

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
    // T15b: shard status (ClientShard sets it; a plain Client keeps false = no
    // cost beyond one predictable branch per duty cycle).  The duty thread
    // derives this Client's ShardStatus every cycle (shard/ShardState.h — plain
    // code, no indirect call) and publishes it on change into a StatusRing the
    // owner / control thread drains with pollShardStatus().
    bool     shardStatus = false;
    uint32_t shardIndex  = 0;

    // CB-2: Static topic configuration for the producer.
    // The client's static topic table (env RUNE_CLIENT_TOPIC_TABLE in the examples); broker topics are created by DDL.
    // TODO(dynamic): fetch topic metadata from the broker on connect.
    std::vector<TopicSpec> topics;

    // ---- Task 9 (RUNE_TLS enablement) — client<->broker TLS/AEAD public config.
    // Mapped 1:1 into session::SessionConfig by toAgentConfig() below. Every
    // field defaults to today's plaintext behaviour (offerCrypto=false ->
    // byte-identical ConnectRequest, pre-Task-5 shape).
    //
    // offerCrypto: true -> connect() offers a v(crypto) ConnectRequest
    // (cipherList/handshakeBytes, plus the reserved — always empty — authToken) and
    // drives the TLS 1.3 handshake.
    bool offerCrypto = false;
    // tlsTrustPem: nullopt = trust the system store (production default);
    // "" = trust nothing (deliberately untrusted — tests only); non-empty =
    // a PEM CA bundle to trust instead of the system store.
    std::optional<std::string> tlsTrustPem;
    // tlsExpectHost: the name checked against the broker certificate's SAN (a DNS
    // name, or an IP literal against its iPAddress SANs).  "" (the default) = each
    // session checks the host of ITS endpoint (the gateway channel for the first,
    // the advertised endpoint for every other broker; a pre-TLS redirect never moves
    // the name).  Issue #115: offerCrypto with the system store (tlsTrustPem unset),
    // no tlsExpectHost and a gateway channel naming no host throws
    // std::invalid_argument - the system store with no name check would accept any
    // public certificate for any name.
    std::string tlsExpectHost;
    // (authToken, removed — fix-on-sight, authentication Task 7: it was documented as validated
    // "by the broker's existing auth path", but no such path existed.  The wire field stays
    // reserved and is always sent empty; see `username` / `password` below for real credentials.)
    // ---- Authentication (spec §5, D5): SCRAM-SHA-512 inside the session.
    // username / password: the principal this Client logs in as, once per broker session (every
    // session of a multi-node client logs in).  "" = no credentials: the broker answers
    // AuthenticationFailed (44) — the client never short-circuits, the broker enforces.  The
    // password is never logged and never sent (only a proof derived from it).
    std::string username;
    std::string password;
    // passwordFile: a file holding the password (read ONCE at construction; trailing newline
    // stripped) — keeps it off the command line and out of the environment.  Setting both
    // `password` and `passwordFile` throws std::invalid_argument; an unreadable file throws
    // std::runtime_error.
    std::string passwordFile;
    // allowUnauthenticatedBroker (review B I3): a Client WITH credentials refuses a broker that skips the login
    // (dev mode RUNE_AUTH=disabled, a pre-authentication broker, a cluster with no security admin yet) with
    // AuthenticationFailed — a rogue endpoint must not receive a credentialed client's data.  Set true to accept
    // such a broker anyway (authStatus() then reports Anonymous).  A Client without credentials is unaffected.
    bool allowUnauthenticatedBroker = false;
    // securityBootstrapOnly (issue #121): this Client exists only for bootstrapSecurity() and has no credentials.  On a
    // cluster whose bootstrap window was reopened (its last admin disabled or dropped; the broker answers
    // MustAuthenticate because its other users still log in) it skips the login instead of failing it.  Ignored with
    // credentials.  The CLI's `rune security bootstrap` sets it.
    bool securityBootstrapOnly = false;
    // minPasswordLength / scramIterations: how this client builds a NEW verifier (ALTER USER ...
    // SET PASSWORD, the CLI's bootstrap) — the broker never sees a password, so the minimum is the
    // client's rule (D9).  Not used by the login itself, which follows the broker's salt and count.
    uint32_t minPasswordLength = 12;
    uint32_t scramIterations   = 4096;
    // requireCrypto (downgrade safety, spec §11 require_tls's client-side
    // mirror): when true, a session never proceeds without TLS — a ConnectResponse
    // that negotiates NO cipher (a MITM that stripped the crypto offer) fails the
    // connect attempt rather than silently going plaintext.  Issue #14: such a
    // refusal is unauthenticated, so it is retried (connect budget, then Backoff /
    // Disconnected with lastConnectError HandshakeFailed), never terminal.
    // Meaningless unless offerCrypto is also true.  Default false; ALWAYS on for a
    // credentialed client (username, or securityBootstrapOnly) unless
    // insecurePlaintextAuth.
    bool requireCrypto = false;
    // insecurePlaintextAuth (issue #14, TESTS ONLY): let a credentialed client log in over a plaintext session.
    // Default false: a Client with credentials (username / password / passwordFile) or securityBootstrapOnly
    // requires TLS — construction throws std::invalid_argument without offerCrypto, and requireCrypto is forced on
    // — so a password proof, a verifier or the bootstrap material never travel in clear (issue #113).  An
    // authenticating broker refuses plaintext sessions anyway (issue #10).
    bool insecurePlaintextAuth = false;

    // ---- Catalog history (Task 5) — DDL.
    // clientName: carried in every DDL's extension (DdlOptions::clientName
    // overrides it per call); the catalog event's principal records it.
    std::string clientName = "rune-client";
    // ddlRetryBudget: how long a DDL answered NotLeader (a catalog leader change
    // / a catching-up leader) is retried, with a 100 ms backoff doubling to 1 s,
    // before it fails explicitly (BrokerError NotLeader).  Bounded by the DDL's
    // own deadline (ddlTimeoutMs) too.
    std::chrono::milliseconds ddlRetryBudget{10'000};
    // ddlTimeoutMs: the deadline of EVERY DDL (DefineTopic, AlterTopic,
    // CreateView, DropTopic, DropView, AlterView, AlterTopicConfig) — the whole
    // op, its Redirect hops and NotLeader retries included; no answer by then ->
    // Timeout.  Independent of pingTimeoutMs: a catalog leader holds a
    // CreateTopic OK until the topic serves (up to 5 s).
    int64_t ddlTimeoutMs = 15'000;

    // DR client bug 2: the deadline of Client::close()'s drain phase (every
    // producer flushed, every transactional producer closed).  At the deadline
    // close() tears down anyway: everything still pending completes exactly
    // once (Closed) and close() returns — it never waits forever on a peer or a
    // command-ring drain.  <= 0 = the default, 65 s: above the DEFAULT
    // deliveryTimeoutMs (30 s) and transactionOpTimeoutMs (60 s), so a normal
    // drain is never cut short.  A producer configured with a larger
    // deliveryTimeoutMs / transactionOpTimeoutMs has its drain cut at
    // closeTimeoutMs — raise this to match.  Values above one year are clamped
    // (never "forever"); closeImmediately() is the no-drain form.
    int64_t closeTimeoutMs = 65'000;
    // traceDdlRetries: record every FOLLOWED Redirect and every scheduled NotLeader
    // retry of a DDL (`rune ql --verbose`); read them with Client::takeDdlRetryNotes().
    // Requires RunnerMode::Invoker (the constructor throws std::invalid_argument otherwise).
    // Default false: nothing is recorded, nothing allocates.
    bool traceDdlRetries = false;
    // TEST ONLY (namespaces Task 11): > 0 = the ConnectRequest.clientVersion this client announces instead of
    // protocol::client::kClientVersion (e.g. 13, to prove a pre-namespace client is refused the namespace ops).
    // 0 (the default) = the current version.  Never set it in production code.
    int32_t testAnnounceClientVersion = 0;
};

/// Maps a ClientConfig to the agent/transport configuration (applies env overrides).
[[nodiscard]] agent::AgentConfig toAgentConfig(const ClientConfig& c);

/// Authentication (spec §5): who this Client is to its seed broker right now.
///   Authenticated          the seed session is Connected and the broker authenticated `user`.
///   PasswordChangeRequired Connected with a one-time (bootstrap) password: the broker answers every
///                          request except a password change with PasswordChangeRequired (47).
///   Anonymous              no login took place: the seed is not Connected, or the broker runs without
///                          authentication (brokerMode DevDisabled / InProcess).
struct AuthStatus {
    enum class Kind : uint8_t { Authenticated, PasswordChangeRequired, Anonymous } kind = Kind::Anonymous;
    std::string                      user;          ///< ClientConfig::username when authenticated, else ""
    rune::protocol::client::AuthMode brokerMode = rune::protocol::client::AuthMode::InProcess;
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

    /// = closeImmediately() (user decision 2: a destructor never waits for
    /// the broker).  Idempotent.  Call close() first when delivery matters.
    ~Client();

    // Non-copyable, non-movable.
    Client(const Client&)            = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&)                 = delete;
    Client& operator=(Client&&)      = delete;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// F8c C7 (user decision 2): drain, then tear down.  Every producer's
    /// intake closes and it drains — its ring and builders flushed, every
    /// accepted record acknowledged (or failed by the broker / its delivery
    /// timeout: each producer's drain is bounded by deliveryTimeoutMs); every
    /// transactional producer closes (an open broker transaction is aborted
    /// first, bounded by transactionOpTimeoutMs) — then closeImmediately().
    /// Blocking like the *AndWait sugar: the Invoker owner pumps doWork(), a
    /// Thread-mode app thread idle-polls; from a duty-cycle callback (or a
    /// non-owner Invoker thread): std::logic_error.  Subscriptions and group
    /// consumers are not drained (they are finished by the teardown).
    /// Idempotent: safe to call multiple times or after the dtor.
    void close();

    /// Tear down now: stop the runner (close the command ring + join), stop
    /// the agent, complete every outstanding op Closed — including every
    /// record a producer accepted but has not had acknowledged (each
    /// registered Completions then finishes) — hand every still-held handle
    /// impl back to its handle, and fire onDisconnected.  Never waits for the
    /// broker.  Idempotent; the destructor does this.
    void closeImmediately();

    /// Invoker mode: one duty cycle (driver + conductor, commands, egress, sessions,
    /// ProduceResponses, producers, periodic sweep).  Returns the work count
    /// (0 = idle; the caller applies its own idle strategy).  Call only from the
    /// constructing thread.  Re-entry from a callback fired inside the duty cycle
    /// returns 0 without running a nested cycle.  Thread mode: throws logic_error.
    int doWork();

    /// A7 park-until-work (Invoker owner): the embedded invoker driver's socket-readiness fds — park in
    /// ppoll() on them when doWork() returned 0 instead of spinning; a datagram (an ack, a status message)
    /// wakes the owner.  0 fds in Thread driver mode or off Linux.  Valid until close.
    [[nodiscard]] int wakeFds(int* out, int max) const noexcept { return agent_.wakeFds(out, max); }

    /// True once the Aeron client closed itself (invoker owner stalled past
    /// aeronClientLivenessTimeoutNs, or the driver timed out).
    [[nodiscard]] bool transportClosed() const noexcept { return agent_.transportClosed(); }

    /// T15b test seam (any thread): the transport fault path, as if the Aeron
    /// client had closed itself — delivered through the command ring and
    /// applied by the duty thread on its next cycle.  The Client then fails for
    /// good (see dutyCycle: producers' intakes tail-closed, every accepted
    /// record completed Closed exactly once, no further I/O).
    void testInjectTransportFault() { (void)agent_.submitTestFault(); }
    /// T15b test hook: duty cycles run in failed mode (Invoker owner, or after
    /// close() in Thread mode — a plain duty-thread counter).
    [[nodiscard]] uint64_t testFailedDutyCycles() const noexcept { return failedDutyCycles_; }

    // ---- T15b: shard status (ClientConfig::shardStatus) -------------------------
    /// The ring's one consumer (the Invoker owner / an Agent control thread):
    /// h(const shard::ShardStatus&) for every status the duty thread published
    /// since the last call, in order.  0 when shardStatus is off.
    template <class H>
    int pollShardStatus(H&& h) {
        return shardRing_ ? shardRing_->drainAll(std::forward<H>(h)) : 0;
    }
    /// The duty side's current status (duty thread / Invoker owner only).
    [[nodiscard]] const shard::ShardStatus& shardStatusDuty() const noexcept { return shardDuty_; }
    /// After close() / closeImmediately() returned (no duty thread left): publish
    /// the final Closed status and finish the ring.  Idempotent.
    void publishShardClosed() noexcept;

    /// Test hook (T15a): the transport, for its thread-model observers
    /// (usesDriverThread / usesConductorInvoker / aeronMediaDriverTimeoutMs, and
    /// callerPumpViolations once the runner is joined).
    [[nodiscard]] const transport::AeronTransport& testTransport() const noexcept {
        return agent_.testTransport();
    }

    /// F8c C7 test hooks (duty thread / owner in Invoker mode, or after close):
    /// handle releases applied but not yet freed by the duty side; subscription
    /// / group-consumer impls the hubs still hold.
    [[nodiscard]] std::size_t testPendingReleases() const noexcept { return releases_.size(); }
    [[nodiscard]] std::size_t testSubscriptionImplsHeld() const noexcept { return subscriberHub_.ownedCount(); }
    [[nodiscard]] std::size_t testGroupImplsHeld() const noexcept { return groupConsumerHub_.ownedCount(); }
    /// Handle impls close() handed back (adopted registrations included) —
    /// read after close().
    [[nodiscard]] uint64_t testImplsHandedBack() const noexcept { return implsHandedBack_; }
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
    /// up-front closed fast-fail check and BEFORE the registration is written
    /// to the ring — the exact window the post-submit compare_exchange re-check
    /// in producer() exists for. A test calls close()
    /// from here to force "close() won the race" deterministically. Cold path
    /// (registration only); empty by default.
    void testSetBeforeRegistrationSubmitHook(std::function<void()> fn) {
        testBeforeRegistrationSubmitHook_ = std::move(fn);
    }
    /// Task 14 fix1 I2 test hook (owner thread, Invoker): fn runs at the top of
    /// every dutyCycle(), inside its in-cycle guard (nullptr = none). A throwing
    /// fn simulates a duty cycle that throws.
    void testSetDutyCycleHook(void (*fn)()) noexcept { testDutyCycleHook_ = fn; }
    /// F8a fix2 test hooks (duty thread / owner in Invoker mode only).
    [[nodiscard]] uint64_t testIngressOfferRefused() const noexcept { return agent_.ingressOfferRefused(); }
    [[nodiscard]] uint64_t testStaleFramesDropped() const noexcept { return agent_.staleFramesDropped(); }
    /// Fix round 5 (N4-I1) test hook: final-leg HandshakeFailed verdicts the agent applied.
    [[nodiscard]] uint64_t testHandshakeFinalLegFailures() const noexcept { return agent_.handshakeFinalLegFailures(); }
    [[nodiscard]] uint64_t testStaleSessionNotices() const noexcept { return agent_.staleSessionNotices(); }
    /// Test hook (duty-cycle thread only): the seed session's current broker session id (0 = none).
    [[nodiscard]] int64_t testSeedSessionId() const noexcept { return agent_.seedSession().machine.sessionId(); }
    [[nodiscard]] std::size_t testPendingUnsubscribes() const noexcept { return subscriberHub_.pendingUnsubscribes(); }
    [[nodiscard]] uint64_t testIngressFatalDropped() const noexcept { return agent_.ingressFatalDropped(); }
    /// DR client bug 2: ring-drained frames dropped after the transport refused
    /// them at the command ring's head for the response timeout.
    [[nodiscard]] uint64_t testSendHeadExpired() const noexcept { return agent_.sendHeadExpired(); }
    /// DR client bug 2 test seam (duty thread / Invoker owner): a MetadataRequest
    /// through the command ring (kMsgSend), exactly as the connect path sends one.
    void testSubmitMetadataRequest() noexcept { sendMetadataRequest(agent_.sessionId()); }
    [[nodiscard]] bool testGroupHubEmpty() const noexcept { return groupConsumerHub_.empty(); }
    [[nodiscard]] std::size_t testUnsentSubscribes() const noexcept { return subscriberHub_.unsentSubscribes(); }
    [[nodiscard]] std::size_t testMaxFrameLength() const noexcept { return agent_.maxFrameLength(); }
    /// fix4 test seam (Invoker, owner thread only): cap the max frame length.
    void testCapMaxFrameLength(std::size_t cap) noexcept { agent_.testCapMaxFrameLength(cap); }
    /// F8c fix round 1 test seams (set before the threads they affect run):
    /// make a claim from testClaimCommandRingWithoutCommitting() header-less;
    /// make close()'s drain unable to unblock (forces the fix5 abandonment);
    /// cap the command ring's message length (0 = uncapped); read its tail.
    void testZeroCommandRingClaimHeader(aeron::util::index_t claimIndex) noexcept {
        agent_.testZeroClaimHeader(claimIndex);
    }
    void testDisableCloseDrainUnblock() noexcept { agent_.testDisableCloseDrainUnblock(); }
    void testCapCommandMsgLength(std::size_t cap) noexcept { agent_.testCapCommandMsgLength(cap); }
    [[nodiscard]] std::int64_t testCommandRingTail() const noexcept { return agent_.commandRingTail(); }
    void testForceIngressOffers(transport::AeronTransport::OfferResult r, int n) noexcept {
        agent_.testForceIngressOffers(r, n);
    }
    /// F8c C7 F1 test seam (duty thread / Invoker owner): put a Ping frame of
    /// the current seed session on the command ring as a kMsgSend — with
    /// testForceIngressOffers(Retry, ...) it ABORTs and stays at the ring head.
    bool testSubmitSessionPing() {
        uint8_t buf[64]{};
        const std::size_t n = codec::encodePing(buf, sizeof buf, "x", agent_.stateAndSessionId().sessionId, 1);
        return n != 0 && agent_.submitSend(buf, n);
    }
    /// F8a fix1 test hook: called on the submitting thread each time a
    /// lifecycle hub command (unsubscribe / GroupConsumer close) finds the
    /// command ring full and waits for duty-thread progress.  Set before use.
    void testSetHubCommandRingFullHook(std::function<void()> fn) {
        testHubCommandRingFullHook_ = std::move(fn);
    }
    /// Test hook (duty-cycle thread only): install a partition route as metadata would.
    void testSetRoute(int32_t topicId, int32_t partition, const std::string& endpoint) {
        agent_.setRoute(topicId, partition, endpoint);
    }
    /// Task 16 test hook (duty-cycle thread only): the channel Produce frames of
    /// (topicId, partition) are published on — the owning worker's ingress for a
    /// shard-mode broker, the node for a routed legacy one; "" = the default path.
    [[nodiscard]] std::string testDataChannelFor(int32_t topicId, int32_t partition) const {
        return agent_.dataChannelFor(topicId, partition);
    }
    /// Task 16 fix round 1 test hooks (duty-cycle thread only).
    [[nodiscard]] int  testMetadataResponses() const noexcept { return metadataResponses_; }
    [[nodiscard]] bool testRouteParked(int32_t topicId, int32_t partition) const noexcept {
        return producerHub_.routeParked(topicId, partition);
    }
    [[nodiscard]] bool testPublicationOpen(const std::string& channel, int32_t streamId) const noexcept {
        return agent_.testPublicationOpen(channel, streamId);
    }
    [[nodiscard]] std::size_t testSessionCount() const noexcept { return agent_.sessionCount(); }
    /// D4: the coordinator route (empty = the seed), its session index, and how
    /// many coordinator moves / fallbacks were followed (duty thread / owner).
    [[nodiscard]] const std::string& testCoordinatorChannel() const noexcept {
        static const std::string kNone;   // the seed (empty, as before)
        const auto* rm = std::get_if<detail::CoordRemote>(&coordRoute_);
        return rm != nullptr ? rm->channel : kNone;
    }
    [[nodiscard]] uint32_t testCoordinatorSessionIndex() const noexcept { return coordIdx(); }
    [[nodiscard]] uint64_t testCoordinatorRedirects() const noexcept { return coordinatorRedirects_; }
    [[nodiscard]] uint64_t testCoordinatorFallbacks() const noexcept { return coordinatorFallbacks_; }
    /// DR client bug 1 (duty thread / after close): seed failovers started (each a
    /// move of the seed to another broker), and the MetadataRequests sent on a
    /// non-seed session because the seed was not Connected.
    [[nodiscard]] uint64_t testSeedFailovers() const noexcept { return seedFailovers_; }
    /// Fix round 2 (I-1): control ops waiting for a Connected seed (duty thread / Invoker owner).
    [[nodiscard]] uint32_t testCtlAwaitingSeed() const noexcept { return ctlAwaitingSeed_; }
    /// Fix round 1 (I2): failover moves that issued their own Connect (never, with reconnectForeverAfterLive).
    [[nodiscard]] uint64_t testSeedFailoverConnects() const noexcept { return seedFailoverConnects_; }
    /// DR client bug 2 (after close): close() drains cut short by closeTimeoutMs.
    [[nodiscard]] uint64_t testCloseDeadlineHits() const noexcept { return closeDeadlineHits_; }
    [[nodiscard]] uint64_t testMetadataViaOtherSession() const noexcept { return metadataViaOtherSession_; }
    [[nodiscard]] const std::string& testSeedChannel() const noexcept { return agent_.seedChannel(); }
    /// Fix round 1 test seam (duty thread / Invoker owner): an urgent metadata ask.
    void testRequestMetadataNow() noexcept { lastUrgentMetadataNs_ = INT64_MIN; requestMetadataNow_(); }
    /// Test hook (duty-cycle thread only): agent session `i`'s state.
    [[nodiscard]] session::SessionState testSessionStateAt(uint32_t i) const noexcept { return agent_.sessionStateAt(i); }
    /// Review B I5 test seam (Invoker owner only): the seed's link dropped — Connected -> Resuming -> a resume ConnectRequest.
    void testForceResume() { agent_.seedSession().machine.onTransportDisconnected(now()); }
    [[nodiscard]] uint64_t testSessionConnectFailuresAt(uint32_t i) const noexcept { return agent_.sessionConnectFailuresAt(i); }
    /// Test hook (duty-cycle thread only): the cached advertised ingress stream (0 = none).
    [[nodiscard]] int32_t testIngressStreamIdFor(int32_t topicId, int32_t partition) const {
        return metadataCache_.ingressStreamIdFor(topicId, partition);
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
    // Callback registration
    //
    // F8c C3: each setter is a command for the duty thread (the Invoker owner
    // outside a duty cycle installs it directly), so it never races the duty
    // thread reading the callback, from any thread, at any time.  Ring order:
    // a callback set before connect() is installed before that connect is
    // processed.  onConnected/onError fire on the duty thread; onDisconnected
    // on the thread that runs close() (or the dtor).  Do NOT block inside a
    // callback, and do NOT call close() from one that runs on the duty thread.
    // -------------------------------------------------------------------------

    void onConnected(std::function<void(int64_t sessionId)> cb);
    void onDisconnected(std::function<void()> cb);
    void onError(std::function<void(const std::string&)> cb);

    // -------------------------------------------------------------------------
    // State observer
    // -------------------------------------------------------------------------

    /// The seed session's state.  F8c C3: the duty thread publishes it into a
    /// StatusRing (one consumer: the owner); no atomic snapshot.  Callable on
    /// the owner (constructing) thread or on the duty thread (a callback);
    /// any other thread gets std::logic_error.  Eventually consistent; but a
    /// connect completion (and onConnected) is always delivered AFTER the
    /// Connected state was published, so state() == Connected once
    /// connectAndWait() returns.
    [[nodiscard]] session::SessionState state() const;

    /// Authentication: the seed session's login state (see AuthStatus).  Same threading rule as state().
    [[nodiscard]] AuthStatus authStatus() const;

    /// The broker error code that ended the seed session for good (SessionRevoked 46, AuthenticationFailed 44, ...), or 0
    /// while the session is not Rejected.  The duty thread only (Invoker owner / a callback); any other thread gets 0.
    [[nodiscard]] int16_t sessionRejectCode() const noexcept;

    // -------------------------------------------------------------------------
    // Control ops (F8c C3) — completion tokens, no futures
    //
    // Each takes a CompletionTarget (a Completions& registered with THIS
    // Client and owned by the calling thread, or a CompletionHandler called on
    // the duty thread) and an app-chosen Token (<= kMaxToken).  The return is
    // synchronous: only Accepted is followed by exactly one Completion.
    //   InvalidArgument — bad token / target / frame larger than the transport
    //                     can carry / payload larger than one CQ record;
    //   WrongThread     — Invoker mode off the owner thread, or a Completions
    //                     used off its owner thread;
    //   QueueFull       — the Completions has no credit (poll it, retry);
    //   BackPressured   — the agent command ring is full (retry);
    //   Closed          — the Client (or the queue) is closed.
    // Completion fields per op (status Ok unless noted):
    //   Connect        value = sessionId.  A second connect while one is
    //                  pending completes the older one Superseded.
    //   Ping           payload = the echo bytes.
    //   DefineTopic    value = topicId, aux = version; a broker errorCode in the
    //                  response -> BrokerError (errorCode, value/aux kept).
    //   GetTopicSchema value = topicId, aux = version, payload = descriptor
    //                  blob (polled form: up to maxResultBytes, else
    //                  ResultTooLarge with value = the real size),
    //                  reserved0 = the topic's partitionCount (Phase 2b; <= 0
    //                  = not reported, e.g. a pre-2b broker).
    //   ListTopics     payload = encodeTopicList() of the topics defined
    //                  through this Client (decodeTopicList()).
    // A not-Connected session is decided on the duty thread: NotConnected.
    // ErrorResponse{corrId} -> BrokerError (errorCode, payload = text); a
    // session-fatal one (SessionExpired / UnknownSession) -> SessionLost for
    // every pending op.  Deadlines: connectTimeoutMs / pingTimeoutMs of duty
    // time -> Timeout.  close() -> Closed.
    // -------------------------------------------------------------------------

    OpResult connect(CompletionTarget target, Token token);
    OpResult ping(std::span<const uint8_t> payload, CompletionTarget target, Token token);
    OpResult defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                         bool validate, std::span<const uint8_t> descriptorBlob,
                         CompletionTarget target, Token token);
    /// Phase 2b E: as above, with an explicit Persistence (Transient = a
    /// transient topic).  The overload without it is Persistence::Disk.
    OpResult defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                         bool validate, std::span<const uint8_t> descriptorBlob,
                         Persistence persistence, CompletionTarget target, Token token);
    OpResult getTopicSchema(std::string_view name, CompletionTarget target, Token token);
    /// As above, selecting a specific schema fingerprint (0 = latest; same as
    /// the 3-arg overload).  Task 18.
    OpResult getTopicSchema(std::string_view name, uint64_t requestedFingerprint,
                            CompletionTarget target, Token token);
    /// Task 18: propose an AlterTopic (expectedVersion is the optimistic-
    /// concurrency gate — stale -> SchemaConflict; incompatible ops ->
    /// SchemaIncompatible; naming a VIEW's output topic -> ViewNotAlterable=38).
    /// Completion: value = topicId, aux = newVersion; a
    /// broker errorCode in the response -> BrokerError (errorCode, value/aux kept).
    OpResult alterTopic(std::string_view name, uint16_t expectedVersion,
                        std::span<const uint8_t> opsBlob, CompletionTarget target, Token token);
    OpResult listTopics(CompletionTarget target, Token token);

    /// Task 21: CreateView (wire 70) — authors a materialized view over an
    /// already-defined source topic. `planBlob` is the serialized authored
    /// ViewPlan (rune::client::view::serializeAuthored, ViewPlanAuthor.h) —
    /// the single source's topicId/keyField, output fields and sizing knobs.
    /// Completion: value = viewTopicId, aux = outputFingerprint (bit-cast
    /// uint64_t); a broker errorCode (e.g. ViewExists) -> BrokerError.
    /// Phase 1 has NO open-by-name wire: the caller must remember the
    /// returned viewTopicId/outputFingerprint for every later getViewRow().
    OpResult createView(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                        std::span<const uint8_t> planBlob, CompletionTarget target, Token token);
    /// Task 21: GetViewRow (wire 72) — a point lookup into a materialized
    /// view's output by its folded uint64 key. `requestedFingerprint` pins
    /// the caller to the output shape it was given at CreateView time (0 =
    /// latest); a drifted fingerprint is rejected with SchemaMismatch BEFORE
    /// any row bytes are read. Completion: value = rowLen (informational),
    /// errorCode = ViewRowResponse::Status (0=OK..4=UnknownView; status != OK
    /// -> BrokerError), payload = rowBytes (empty unless status == OK).
    OpResult getViewRow(int32_t viewTopicId, uint64_t keyU64, uint64_t requestedFingerprint,
                        CompletionTarget target, Token token);

    // ---- Catalog history (Task 5): DDL with the DDL extension ---------------
    // Every DDL (the overloads above included, with default DdlOptions) carries
    // the DDL extension: statement, clientName, a 128-bit requestId (F14) and
    // optionally replayedFrom.  Every DDL handles every answer explicitly:
    //   OK / a specific code      -> its completion (BrokerError for a code);
    //   ErrorResponse{Redirect, endpoint} -> re-sent, the SAME frame and
    //       requestId, on that node's own session (the seed never moves); more
    //       than kMaxDdlRedirects hops -> BrokerError errorCode 3, payload
    //       "redirected more than 3 times";
    //   NotLeader (ErrorResponse or a response's errorCode 2) -> re-sent after a
    //       100 ms backoff doubling to 1 s while inside ClientConfig::
    //       ddlRetryBudget (and the DDL deadline), then BrokerError errorCode 2;
    //   ErrorResponse{InvalidRequest | other} -> BrokerError, payload = its message;
    //   no answer by the DDL deadline (ClientConfig::ddlTimeoutMs) -> Timeout.
    // Completion of the four new ops: value = objectId, aux = newVersion,
    // payload = detail (or the ErrorResponse message).
    static constexpr int kMaxDdlRedirects = 3;
    /// F14: this Client's random 64-bit instance id — the hi word of every
    /// requestId it generates (immutable; any thread).
    [[nodiscard]] uint64_t instanceId() const noexcept { return clientInstanceId_; }
    /// The requestId a DDL completion's request carried when the client
    /// generated it: {instanceId(), reserved0} (lo = the duty thread's 32-bit
    /// counter, never 0).  none() for a non-DDL completion or an explicit
    /// DdlOptions::requestId (the caller already has it).  Retrying a DDL with
    /// DdlOptions::requestId = this is idempotent (the leader dedupes).
    [[nodiscard]] rune::protocol::RequestId requestIdOf(const Completion& c) const noexcept {
        return isDdlOp(c.op) && c.reserved0 != 0 ? rune::protocol::RequestId{clientInstanceId_, c.reserved0}
                                                 : rune::protocol::RequestId{};
    }

    /// One followed Redirect / scheduled NotLeader retry of a DDL (ClientConfig::traceDdlRetries).
    struct DdlRetryNote {
        enum class Kind : uint8_t { Redirect, NotLeader } kind = Kind::Redirect;
        int         redirects = 0;     ///< Redirect: the hop count so far (<= kMaxDdlRedirects)
        int64_t     backoffMs = 0;     ///< NotLeader: the wait before the resend
        std::string text;              ///< Redirect: the leader endpoint; NotLeader: the broker's detail
    };
    static constexpr std::size_t kMaxDdlRetryNotes = 1024;
    /// Moves the recorded notes (oldest first) into `out` (appended) and returns how
    /// many further notes were dropped past kMaxDdlRetryNotes since the last take.
    /// Invoker owner thread only (std::logic_error otherwise).
    std::size_t takeDdlRetryNotes(std::vector<DdlRetryNote>& out);

    OpResult dropTopic(std::string_view name, const DdlOptions& ddl, CompletionTarget target, Token token);
    OpResult dropView(std::string_view name, const DdlOptions& ddl, CompletionTarget target, Token token);
    /// Ruling 4: replaces the view's plan and output under the same objectId
    /// (a fresh sink topic).  expectedVersion = the view object's version.
    OpResult alterView(std::string_view name, std::string_view keyField, uint8_t persistence,
                       uint32_t expectedVersion, std::span<const uint8_t> planBlob, const DdlOptions& ddl,
                       CompletionTarget target, Token token);
    /// Ruling 5: changes = (ConfigKey, value) pairs (retention / compaction keys only).
    OpResult alterTopicConfig(std::string_view name, uint32_t expectedVersion,
                              std::span<const rune::protocol::client::ConfigChangeWire> changes,
                              const DdlOptions& ddl, CompletionTarget target, Token token);
    /// DefineTopic with the catalog-history fields: an empty descriptorBlob is an
    /// UNTYPED topic; TopicOptions carries replicationFactor + the topic config.
    OpResult defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                         std::span<const uint8_t> descriptorBlob, bool validate, Persistence persistence,
                         const TopicOptions& topic, const DdlOptions& ddl, CompletionTarget target, Token token);
    OpResult alterTopic(std::string_view name, uint16_t expectedVersion, std::span<const uint8_t> opsBlob,
                        const DdlOptions& ddl, CompletionTarget target, Token token);
    OpResult createView(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                        std::span<const uint8_t> planBlob, const DdlOptions& ddl, CompletionTarget target,
                        Token token);

    /// Decoded ListTopics payload: (name, topicId) in definition order.
    [[nodiscard]] static std::vector<std::pair<std::string, int32_t>>
    decodeTopicList(std::span<const uint8_t> payload);

    // ---- *AndWait: cold-path blocking conveniences ---------------------------
    // Each builds a short-lived Completions, issues the op and waits (Invoker
    // owner: pumps doWork(); Thread-mode app thread: idle-strategy poll).
    // Throws std::runtime_error on any non-Ok outcome or wait timeout (the op
    // itself still completes later and is dropped with the queue), and
    // std::logic_error from a duty-cycle callback / non-owner Invoker thread.

    static constexpr std::chrono::nanoseconds kDefaultWaitTimeout = std::chrono::seconds(60);

    void  connectAndWait(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    Bytes pingAndWait(std::span<const uint8_t> payload,
                      std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// A broker errorCode is returned in the result (not thrown), as before.
    DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions,
                                         std::string_view keyField, bool validate,
                                         std::span<const uint8_t> descriptorBlob,
                                         std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// Phase 2b E: defineTopic(..., persistence), blocking.  Same result contract.
    DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions,
                                         std::string_view keyField, bool validate,
                                         std::span<const uint8_t> descriptorBlob,
                                         Persistence persistence,
                                         std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// A broker errorCode (e.g. UnknownTopic) is returned in the result.
    TopicSchemaResult getTopicSchemaAndWait(std::string_view name,
                                            std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// As above, selecting a specific schema fingerprint (0 = latest).  Task 18.
    TopicSchemaResult getTopicSchemaAndWait(std::string_view name, uint64_t requestedFingerprint,
                                            std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// Task 18: alterTopic(), blocking.  A broker errorCode (e.g.
    /// SchemaIncompatible / SchemaConflict / ViewNotAlterable=38) is returned in the result (not thrown).
    AlterTopicResult alterTopicAndWait(std::string_view name, uint16_t expectedVersion,
                                       std::span<const uint8_t> opsBlob,
                                       std::chrono::nanoseconds timeout = kDefaultWaitTimeout);

    /// Task 21: createView(), blocking. A broker errorCode (e.g. ViewExists,
    /// ViewSourceNotFound, ViewFieldUnresolved, ViewSourceTransient=37 - a view
    /// over a transient topic must itself be transient) is returned in the result
    /// (not thrown).
    CreateViewResult createViewAndWait(std::string_view viewName, std::string_view keyField,
                                       uint8_t persistence, std::span<const uint8_t> planBlob,
                                       std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
    /// Routing (Task 22 fix round 1): only the source partition's leader serves a row; a Redirect / NotLeader is followed
    /// within min(pingTimeoutMs, ddlRetryBudget) and ends as ViewRowResult::kNotLeader, or as a Timeout (throws) when
    /// pingTimeoutMs is the shorter of the two (the defaults).
    /// Task 21: getViewRow(), blocking. A non-OK ViewRowResponse status (e.g.
    /// AbsentKey, NotReady, NotAuthorized) is returned in the result (not thrown) — only a
    /// transport-level failure (Timeout, NotConnected, SessionLost, Closed)
    /// throws.
    ViewRowResult getViewRowAndWait(int32_t viewTopicId, uint64_t keyU64, uint64_t requestedFingerprint,
                                    std::chrono::nanoseconds timeout = kDefaultWaitTimeout);

    // ---- Catalog history (Task 5): blocking DDL conveniences ----------------
    // Pump-until-done wrappers over the token forms above.  A broker errorCode
    // (incl. NotLeader past the budget / Redirect past kMaxDdlRedirects) is
    // returned in the result with its detail; Timeout / NotConnected /
    // SessionLost / Closed throw (as every *AndWait).
    DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions, std::string_view keyField,
                                         std::span<const uint8_t> descriptorBlob, bool validate,
                                         Persistence persistence, const TopicOptions& topic,
                                         const DdlOptions& ddl, std::chrono::milliseconds timeout);
    AlterTopicResult alterTopicAndWait(std::string_view name, uint16_t expectedVersion,
                                       std::span<const uint8_t> opsBlob, const DdlOptions& ddl,
                                       std::chrono::milliseconds timeout);
    CreateViewResult createViewAndWait(std::string_view viewName, std::string_view keyField, uint8_t persistence,
                                       std::span<const uint8_t> planBlob, const DdlOptions& ddl,
                                       std::chrono::milliseconds timeout);
    DdlResult dropTopicAndWait(std::string_view name, const DdlOptions& ddl, std::chrono::milliseconds timeout);
    DdlResult dropViewAndWait(std::string_view name, const DdlOptions& ddl, std::chrono::milliseconds timeout);
    DdlResult alterViewAndWait(std::string_view name, std::string_view keyField, uint8_t persistence,
                               uint32_t expectedVersion, std::span<const uint8_t> planBlob, const DdlOptions& ddl,
                               std::chrono::milliseconds timeout);
    DdlResult alterTopicConfigAndWait(std::string_view name, uint32_t expectedVersion,
                                      std::span<const rune::protocol::client::ConfigChangeWire> changes,
                                      const DdlOptions& ddl, std::chrono::milliseconds timeout);

    // ---- Catalog history (Task 13, spec §7): reads of `__catalog` ----------------
    // Each call refreshes this Client's CatalogReader (catalogReader()): it reads
    // the committed tail from its cache end (from 0 the first time) on the seed
    // broker — any broker serves it — until the caught-up marker, replaying every
    // event with the shared verifying library, then answers from the cached,
    // replayed prefix with that library's history / stateAsOf / diff.  The cache
    // is (prefix, the broker's history epoch): a stream whose epoch differs (a
    // truncation below a served high watermark, another broker) drops it and
    // re-reads from 0 — a stale history is never served.  Blocking like the
    // *AndWait sugar (the Invoker owner pumps doWork()); callable ONLY from the
    // Client's constructing thread (the reader's owner, bound in the
    // constructor): any other thread gets std::logic_error, in every runner mode
    // (another thread constructs its own CatalogReader).  Never a hang: no marker
    // before `timeout` is CatalogReadError::Kind::Timeout (a long-leaderless
    // catalog withholds the marker); a fingerprint / decode / gap failure is
    // Kind::Replay naming its offset (the verified prefix stays cached).
    /// SHOW HISTORY: the matching events, in offset order.
    CatalogResult<std::vector<rune::catalog::RecordedEvent>> catalogHistory(const rune::catalog::HistoryFilter& filter,
                                                                            std::chrono::milliseconds timeout);
    /// AS OF: the catalog state at an offset / time / object version.
    CatalogResult<rune::catalog::CatalogState> catalogAsOf(const rune::catalog::AsOf& at,
                                                           std::chrono::milliseconds timeout);
    /// DIFF: every object that differs between the states at `a` and `b`.
    CatalogResult<std::vector<rune::catalog::DiffEntry>> catalogDiff(const rune::catalog::AsOf& a,
                                                                     const rune::catalog::AsOf& b,
                                                                     std::chrono::milliseconds timeout);
    /// The Client's own reader (built in the constructor; owned by the Client's
    /// constructing thread) — e.g. for its non-blocking beginRefresh / pollRefresh.
    /// Any other thread: std::logic_error (every runner mode).
    CatalogReader& catalogReader();

    // ---- Authentication (Task 8): security DDL and reads of `__security` ----------------------------------
    /// Every security DDL.  The VERIFIER is built here from the plan's password (D9: at least
    /// ClientConfig::minPasswordLength bytes, else std::invalid_argument "password must be at least N characters" —
    /// nothing is sent; iterations = ClientConfig::scramIterations), and only the plan's REDACTED statement is sent
    /// (D10; DdlOptions::statement is ignored).  Redirect to the `__security` leader and NotLeader retries behave
    /// exactly like catalog DDL (same budget, the same request id on every resend).  The plan (by value) holds the
    /// plaintext password: it is wiped after the frame is built.
    OpResult securityDdl(rune::ql::SecurityPlan plan, const DdlOptions& ddl, CompletionTarget target, Token token);
    SecurityDdlResult securityDdlAndWait(rune::ql::SecurityPlan plan, const DdlOptions& ddl,
                                         std::chrono::milliseconds timeout);
    /// D4: the one-time bootstrap of a fresh cluster — `adminName` + a one-time password + the CLUSTER KEY FILE's
    /// bytes (the MAC under the key proves possession; the library never prints the password).
    OpResult bootstrapSecurity(std::string_view adminName, std::string_view oneTimePassword,
                               std::span<const uint8_t> clusterKeyFileContents, CompletionTarget target, Token token);
    SecurityDdlResult bootstrapSecurityAndWait(std::string_view adminName, std::string_view oneTimePassword,
                                               std::span<const uint8_t> clusterKeyFileContents,
                                               std::chrono::milliseconds timeout);

    // Reads of `__security` (ADMIN CLUSTER only — any other principal gets Kind::Rejected, brokerErrorCode 45):
    // the catalog reads' contract (owner thread only, a cached verified prefix + history epoch, Timeout / Replay /
    // Closed / Rejected errors), on the shared EventLogStream.
    template <class T> using SecurityResult = std::expected<T, CatalogReadError>;
    SecurityResult<std::vector<rune::security::RecordedEvent>> securityHistory(const rune::security::HistoryFilter& filter,
                                                                                std::chrono::milliseconds timeout);
    SecurityResult<rune::security::GrantsView> securityGrantsFor(rune::security::GranteeKind kind, std::string_view name,
                                                                  std::optional<rune::security::AsOf> at,
                                                                  std::chrono::milliseconds timeout);
    SecurityResult<std::vector<rune::security::UserRow>> securityUsers(std::chrono::milliseconds timeout);
    SecurityResult<std::vector<rune::security::RoleRow>> securityRoles(std::chrono::milliseconds timeout);
    SecurityReader& securityReader();

    // ---- Namespaces (Task 11, spec §10 / D13): namespace DDL (wire 81 -> DdlResponse 78) and ListNamespaces
    // (82 -> 83).  Client version 14; the broker answers an older session InvalidRequest (39).  Namespace DDL needs
    // ADMIN ON CLUSTER (else 45).  Redirect / NotLeader retries behave exactly like catalog DDL.
    /// The DDL itself: `kind` Create (fields not in `presentMask` take the broker's current defaults) or SetLimits
    /// (the full record: pass kNamespaceFullMask).  Completes Ok (value = nsId, aux = new version) or BrokerError
    /// (errorCode, payload = detail).
    OpResult namespaceDdl(rune::protocol::client::NamespaceDdlKind kind, std::string_view name,
                          const std::array<uint8_t, rune::protocol::client::kNamespacePresentMaskBytes>& presentMask,
                          const rune::protocol::NamespaceLimits& limits, const DdlOptions& ddl,
                          CompletionTarget target, Token token);
    /// One page of namespaces with nsId > afterNsId (at most `max`; the broker caps it): completes Ok with the raw
    /// ListNamespacesResponse frame as payload (decode with rune::protocol::client::ListNamespacesResponse::decode;
    /// reserve 64 KiB), or BrokerError.  Only the session's own namespace unless it is a cluster admin.
    OpResult listNamespaces(uint32_t afterNsId, uint16_t max, CompletionTarget target, Token token);

    /// Blocking forms (any thread but the runner).  A broker refusal is the error (DdlResult: errorCode +
    /// detail); a transport failure (Timeout / NotConnected / SessionLost / Closed) throws as every *AndWait does.
    template <class T> using NamespaceResult = std::expected<T, DdlResult>;
    /// Creates `name`; returns its nsId.
    NamespaceResult<uint32_t> createNamespace(std::string_view name, const NamespaceLimitsPatch& limits = {},
                                              std::chrono::milliseconds timeout = std::chrono::seconds(30));
    /// Replaces the namespace's whole limits record with `full`.
    NamespaceResult<void> setNamespaceLimits(std::string_view name, const rune::protocol::NamespaceLimits& full,
                                             std::chrono::milliseconds timeout = std::chrono::seconds(30));
    /// Every namespace this session may see, in nsId order (pages internally).
    NamespaceResult<std::vector<rune::protocol::client::NamespaceInfo>>
    listNamespaces(std::chrono::milliseconds timeout = std::chrono::seconds(30));

    /// Topics successfully defined through this Client (Task 9 CLI), in
    /// definition order.  Duty-owned registry, read through a ListTopics op.
    [[nodiscard]] std::vector<std::pair<std::string, int32_t>>
    listTopics(std::chrono::nanoseconds timeout = kDefaultWaitTimeout);

    /// Open a typed topic: getTopicSchemaAndWait(name, T::kFingerprint) — the
    /// VERSION-BINDING gate (Task 19).  The broker (Task 13) searches the
    /// topic's schema history for the version whose fingerprint equals
    /// T::kFingerprint and, on a match, returns THAT version's materialized
    /// descriptor blob — not necessarily the latest.  This is what lets a
    /// client compiled against an older schema version keep working after the
    /// topic has evolved: it binds to (and every send() stamps) the version it
    /// was actually compiled against, instead of whatever is latest.
    ///
    /// A broker errorCode (no version has that fingerprint — UnknownSchema or
    /// equivalent) throws the same "stale generated type" error as an exact
    /// descriptor mismatch did before Task 18 added fingerprint selection.
    ///
    /// Belt-and-suspenders (consistency with TypedTopic::fromSchema, which
    /// still performs this check on its own completion path): even on a
    /// broker Ok, re-verify client-side that the returned descriptor's
    /// fingerprint actually equals T::kFingerprint before binding.  This is
    /// a cold path (one-time at open) — not a zero-alloc/hot-path concern —
    /// so a broker mis-selection can never silently bind a typed client to
    /// the wrong shape.
    ///
    /// No thread is spawned (the old async-task continuation is gone); the
    /// per-topic schema cache is written by the duty thread when it handles
    /// the response.
    template <class T>
    [[nodiscard]] TypedTopic<T> openTopicAndWait(std::string_view name,
                                                 std::chrono::nanoseconds timeout = kDefaultWaitTimeout) {
        TopicSchemaResult r = getTopicSchemaAndWait(name, T::kFingerprint, timeout);
        if (r.errorCode != 0) {
            throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
        }
        if (!schemaMatches(T::kFingerprint, std::span<const uint8_t>(r.descriptorBlob))) {
            throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
        }
        // Phase 2b Part C: route over the topic's partitions (broker-reported;
        // a pre-2b broker reports 0 -> 1, the pre-2b partition-0 behaviour).
        return TypedTopic<T>(producer(), TopicHandle{r.topicId, typedPartitionCount(r.partitionCount)},
                             r.version);
    }

    /// #418: the run-time lineage of the generated type T for topic `name` (T::Lineage), built from the topic's
    /// evolving descriptor (GetTopicSchema with kSchemaLineageFingerprint).  A T decodes records of versions
    /// 1..T::kVersion on its own; a record written at a LATER version (the topic evolved after T was generated)
    /// needs this:  `auto lin = client.lineageAndWait<T>("chars");  rec.decode(bytes, lin);`  (or T::Decoder(bytes,
    /// lin)).  Cold: once per topic, then reuse it (re-fetch after the topic evolves again: a record of a version
    /// the lineage does not know decodes false).  Throws std::runtime_error when the broker answers an error, and
    /// std::invalid_argument when the topic's schema is not T's.
    template <class T>
    [[nodiscard]] typename T::Lineage lineageAndWait(std::string_view name,
                                                     std::chrono::nanoseconds timeout = kDefaultWaitTimeout) {
        TopicSchemaResult r = getTopicSchemaAndWait(name, rune::protocol::client::kSchemaLineageFingerprint, timeout);
        if (r.errorCode != 0) {
            throw std::runtime_error("lineage: the broker did not serve the schema lineage of '" + std::string(name) +
                                     "' (error " + std::to_string(r.errorCode) + ")");
        }
        return T::lineage(rune::codec::deserialize(std::span<const uint8_t>(r.descriptorBlob)));
    }

    /// Phase 2b F (FD7): the same version-binding open, but the typed topic BORROWS `shared` instead of owning
    /// a producer of its own - its sends and claims go through that producer, inside its transaction when one
    /// is open.  `shared` must outlive the returned topic.
    template <class T>
    [[nodiscard]] TypedTopic<T, ProducerRef> openTopicAndWait(std::string_view name, Producer& shared,
                                                              std::chrono::nanoseconds timeout = kDefaultWaitTimeout) {
        TopicSchemaResult r = getTopicSchemaAndWait(name, T::kFingerprint, timeout);
        if (r.errorCode != 0 || !schemaMatches(T::kFingerprint, std::span<const uint8_t>(r.descriptorBlob))) {
            throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
        }
        return TypedTopic<T, ProducerRef>(ProducerRef(shared),
                                          TopicHandle{r.topicId, typedPartitionCount(r.partitionCount)}, r.version);
    }

    /// Test hooks (duty thread only, or after close): completions addressed to
    /// a released / reused registry slot and dropped by the generation check;
    /// the CqRef a Completions was registered under (its own field).
    [[nodiscard]] uint64_t testStaleCqDrops() const noexcept { return staleCqDrops_; }
    /// Test hook (duty thread only): deliver one Ok completion for `token` to
    /// registry reference `ref` — as an op that targeted it would.
    void testDeliverToCqRef(CqRef ref, Token token, uint32_t reservedBytes);
    /// Test hook (duty thread only, or after close): registered queues.
    [[nodiscard]] std::size_t testRegisteredCqs() const noexcept;
    /// Task 5 re-review F4 test seam (the ISSUING thread, before the op): the next
    /// DDL issued with a generated requestId gets a stamp offset `delta` bytes
    /// past its frame — as if the issue-time decode check had been bypassed —
    /// so the duty thread's own guard is exercised (it fails the op
    /// InvalidArgument, never sends {0,0}).
    void testSkewNextDdlRequestIdAt(uint32_t delta) noexcept { testSkewDdlRequestIdAt_ = delta; }
    /// Task 5 fix round 1: pending DDLs waiting for a resend (pump work); duty thread.
    [[nodiscard]] int testDdlAwaitingResend() const noexcept { return ddlAwaitingResend_; }
    /// Task 5 fix round 1 (M3): queue releases still deferred behind a ring fence.
    [[nodiscard]] std::size_t testDeferredReleases() const noexcept { return deferredReleases_.size(); }
    /// Test hook (duty thread only): pending control ops (connect included).
    [[nodiscard]] std::size_t testPendingControlOps() const noexcept {
        return pendingCtl_.size() + (hasPendingConnect_ ? 1u : 0u);
    }
    // ---- CB-4: Subscribe API -------------------------------------------------

    /// Subscribe to a topic partition using a Key (partition resolved via FNV-1a).
    ///
    /// Plain subscriptions are sent on the seed session.  While a consumer group
    /// (subscribeGroup) is active on this Client, that session is the group
    /// coordinator's and never follows a redirect: a plain subscription to a
    /// partition another node leads is then finished with
    /// Subscription::failReason() == SubscriptionFailReason::
    /// LeaderElsewhereWhileGroupActive (re-subscribing cannot succeed; use a
    /// separate Client for it).
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
    ///   - unsubscribe() / the Subscription dtor (owning thread, or from inside
    ///     the handler) have guaranteed delivery; wait for finished() to know no
    ///     handler call can run any more (then position() is final).
    ///   - After close(): returns an already-finished Subscription (active() and
    ///     finished() report it at once; nothing is submitted).
    ///   - Throws std::length_error (nothing submitted) if the Subscribe frame
    ///     can never be carried by the transport (over its max message length).
    ///     A Subscribe the transport later rejects permanently (Fatal) finishes
    ///     the Subscription instead (finished() turns true, never bound).
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

    /// Every partition of the topic (spec §3): `topic.partitionCount` subscriptions, partition 0..N-1, each with
    /// its own copy of `opts` (a filter included) and of `handler`; Record::partition says which partition a
    /// record came from.  The handlers run on the runner thread, one partition's records in order, partitions
    /// interleaved.  Throws std::invalid_argument when partitionCount < 1 (nothing submitted).
    /// The set covers `topic.partitionCount` AS OF THIS CALL: partitions added to the topic later are not
    /// subscribed (re-subscribe with a fresh handle), and a stale, larger count fails the extra partitions
    /// individually (RejectedByBroker).
    ///
    /// Filters: a SubscribeOptions::filter applies to every partition.  subscribe(topic, key, ...) is
    /// PARTITION-WIDE: the key only picks the partition, it does not select records — to read one entity's
    /// records, combine the key (its partition) with a filter on the entity's field:
    ///   client.subscribe(topic, entityKey, SubscribeOptions{}.filter(EntityEvent::Filter{...entity field...}), h);
    SubscriptionSet subscribe(TopicHandle       topic,
                              AllPartitions,
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
    ///   - subscribeGroup() and the handle's callback setters are called on the
    ///     APP thread in the setup phase, BEFORE connect() (the setters travel
    ///     to the duty thread as commands, in ring order ahead of the connect).
    ///   - onAssigned / onRevoked / onRecords callbacks fire on the RUNNER thread.
    ///   - commit() / commitSync() / close() on the handle are commands for
    ///     the duty thread (agent command ring; F8a — no shared state).
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

    friend class Completions;
    template <class P> friend class detail::EventLogStream;   // refresh(): the runner mode, doWork(), checkBlockingCaller_

    /// A completion's destination as the duty thread holds it (shared with
    /// the group / txn libraries since F8c C4).
    using DutyTarget = detail::OpTarget;

    // ---- Catalog history (Task 5): a pending DDL's state — one explicit state
    // per phase, each holding exactly its variables (hard rule 3, F29).
    // `redirects` counts the Redirect hops followed (> kMaxDdlRedirects fails);
    // budgetEndNs = min(DDL deadline, first NotLeader / session loss + ddlRetryBudget).
    /// The session an in-flight DDL was offered on (agent session index) and
    /// its connect episode then — a different episode (left Connected, a
    /// reconnect, a new id) = that session was lost under the DDL.
    struct DdlOfferedOn      { uint32_t sessionIndex = 0; uint64_t sessionEpoch = 0; };
    /// Its first offer was refused by the seed's transport: re-offered every cycle.
    struct DdlUnsentOnSeed   {};
    /// In flight; no NotLeader seen yet.
    struct DdlSent           { int redirects = 0; DdlOfferedOn on{}; };
    /// In flight again after a NotLeader / a lost session.
    struct DdlNotLeaderSeen  { int redirects; int64_t budgetEndNs; int64_t backoffNs; DdlOfferedOn on; };
    /// Why a DDL waits for its resend: a NotLeader answer, or its session was
    /// lost before any answer (Task 5 re-review F1: past the budget the first
    /// ends BrokerError NotLeader, the second OpStatus::SessionLost).
    enum class DdlResendCause : uint8_t { NotLeader, SessionLost };
    /// Not in flight: re-sent at resendAtNs (if refused then, every cycle after, until budgetEndNs).
    struct DdlAwaitingResend {
        int redirects; int64_t budgetEndNs; int64_t backoffNs; int64_t resendAtNs;
        DdlResendCause cause = DdlResendCause::NotLeader;
    };
    /// Redirected (no NotLeader seen) to a node whose session cannot take the
    /// frame yet (still connecting): re-offered every cycle until it does.
    struct DdlHeldForNode    { int redirects; };
    using DdlRetry = std::variant<DdlUnsentOnSeed, DdlSent, DdlNotLeaderSeen, DdlAwaitingResend, DdlHeldForNode>;
    /// Everything only a DDL has (review M1: one variant on PendingCtl).
    struct DdlPending {
        std::vector<uint8_t> frame;          // the encoded request, re-sent verbatim (stable requestId)
        std::string          endpoint;       // the node it goes to ("" = the seed); a Redirect moves it
        uint32_t             requestLo = 0;  // the generated requestId's lo (0 = an explicit requestId)
        DdlRetry             retry{};
    };
    /// The DDL ops (the ones that carry the DDL extension, plus the bootstrap).
    [[nodiscard]] static constexpr bool isDdlOp(OpKind op) noexcept {
        return op == OpKind::DefineTopic || op == OpKind::AlterTopic || op == OpKind::CreateView ||
               op == OpKind::DropTopic || op == OpKind::DropView || op == OpKind::AlterView ||
               op == OpKind::AlterTopicConfig || op == OpKind::SecurityDdl || op == OpKind::SecurityBootstrap ||
               op == OpKind::NamespaceDdl;
    }
    /// Task 22 fix round 1 (C1): the ops answered by a Redirect / NotLeader and so
    /// re-sent on the named node's session within the retry budget: the DDLs and
    /// GetViewRow (a view row is served only by its source partition's leader).
    [[nodiscard]] static constexpr bool isRoutedCtlOp(OpKind op) noexcept {
        return isDdlOp(op) || op == OpKind::GetViewRow;
    }
    /// A DDL that is not in flight (its frame waits for the resend pump).
    [[nodiscard]] static bool ddlWaitsForResend(const DdlRetry& r) noexcept {
        return std::holds_alternative<DdlUnsentOnSeed>(r) || std::holds_alternative<DdlAwaitingResend>(r) ||
               std::holds_alternative<DdlHeldForNode>(r);
    }

    /// One pending control op (duty-thread owned; cold path).
    struct PendingCtl {
        int64_t              corrId     = 0;  // 0 for connect
        int64_t              deadlineNs = 0;  // duty clock
        Token                token      = 0;
        OpKind               op         = OpKind::Ping;
        DutyTarget           target{};
        std::string          topicName;       // DefineTopic: for listTopics()
        std::vector<uint8_t> unsentFrame;     // non-DDL: transport said Retry: re-offered each cycle
        /// Fix round 2 (I-1): a non-routed op (Ping / GetTopicSchema) issued, or left unsent, while the
        /// seed was not Connected — its frame waits (on no session) for a Connected seed, inside the op's
        /// own deadline; it never reached a broker.
        struct CtlAwaitingSeed { std::vector<uint8_t> frame; };
        std::variant<std::monostate, DdlPending, CtlAwaitingSeed> ddl;   // catalog history: a DDL's own state
        detail::SecretString newPassword;   // review B I4: a self SET PASSWORD's plaintext until its answer (wiped always)
    };

    /// Header of a kMsgControlOp ring record (followed by the op's frame).
    struct ControlOpHeader {
        CompletionHandler handler{};
        Token             token         = 0;
        uint32_t          reservedBytes = 0;
        CqRef             cqRef         = kNoCqRef;
        OpKind            op            = OpKind::Ping;
        uint8_t           pad           = 0;
        // Catalog history (F14): offset of the DDL extension's requestId in the
        // frame (encoder extOffset() + extRequestIdOffset(), verified by a
        // decode at issue), stamped by the duty thread (instance id + its
        // counter); 0 = the frame carries an explicit requestId (or is not a DDL).
        uint32_t          requestIdAt   = 0;
        uint16_t          secretLen     = 0;   // review B I4: trailing bytes of the record's frame area that are a secret, not wire
    };

    /// A handler completion deferred while close() runs (delivered after every
    /// queue is handed back, so a handler may destroy a Completions).
    struct DeferredHandler {
        CompletionHandler    handler{};
        Completion           c{};
        std::vector<uint8_t> payload;
    };

    /// (state, sessionId) of the seed session as published to app threads.
    struct SeedStatus {
        int64_t sessionId = 0;
        uint8_t state     = 0;
        uint8_t authMode  = static_cast<uint8_t>(rune::protocol::client::AuthMode::InProcess);   // brokerAuthMode()
        uint8_t passwordChangeRequired = 0;                                                      // one-time password
        uint8_t authenticated = 0;                                                               // review B I3: an earned session
        uint8_t pad[4]{};
    };

    // -------------------------------------------------------------------------
    // Runner
    // -------------------------------------------------------------------------

    /// Teardown shared by close() and ~Client (no contract checks; idempotent).
    void closeImpl_(bool drainDeadlineHit = false);
    /// The contract checks of close() / closeImmediately().
    void checkCloseCaller_(const char* what) const;

    // ---- F8c C7: handle release (spec §3.3) -----------------------------------
    struct PendingRelease {
        detail::HandleKind kind  = detail::HandleKind::Producer;
        void*              impl  = nullptr;
        std::int64_t       fence = 0;  // command-ring tail at a DIRECT release (0: via the ring)
    };
    /// HandleRelease from a handle (the hub-command sink's entry for it).
    detail::HubSubmit submitRelease_(const detail::HubCommand& cmd);
    /// Duty side: the handle's destructor semantics (unless `closing`: the
    /// close path finishes everything itself), then queue the release.
    void applyRelease_(const detail::HubCommand& cmd, std::int64_t fence, bool closing);
    /// End of a duty cycle (never inside a hub walk): forget every released
    /// impl that is quiescent and past its fence, and detach the duty share
    /// (the last of the two sides frees it).
    int  reapReleases_() noexcept;
    /// Client close (after every hub finished): hand every still-held impl back.
    void handBackAll_() noexcept;
    /// F8c C7 fix round 2 (F1): a release that found the command ring full is
    /// ORPHANED (no command, no wait); the duty side discovers and applies it.
    void orphanRelease_(const detail::HubCommand& cmd) noexcept;
    void noteOrphan_(detail::HandleKind kind, void* impl, std::int64_t fence);
    void scanOrphans_();
    int  applyDueOrphans_();

    // ---- F8c C7: Client::close()'s drain ----------------------------------------
    OpResult issueDrain_(Completions& q, Token token);
    void applyClientDrain_(const detail::HubCommand& cmd);
    /// Begin-close every open producer / transactional producer; complete the
    /// drain waiters once all settled.  Every duty cycle while one is pending.
    int  checkDrain_();
    static void drainPartDone_(void* ctx, const Completion& c, std::span<const uint8_t> payload) noexcept;

    // ---- F8c C2: completion-queue registry (duty-thread owned) --------------
    void registerCq_(Completions& q);
    /// DR client bug 2: registration bounded by `deadline` (close()'s own queue).
    /// InDoubt = the CqAdd is still unread in the command ring at the deadline:
    /// the caller keeps `q` alive across closeImpl_() (which applies the CqAdd
    /// and hands the queue back).  Throws as the unbounded form does.
    enum class CqRegistration : uint8_t { Registered, BornFinished, InDoubt };
    CqRegistration registerCq_(Completions& q, std::chrono::steady_clock::time_point deadline);
    static constexpr int64_t kDefaultCloseTimeoutMs = 65'000;
    static constexpr int64_t kMaxCloseTimeoutMs     = 365LL * 24 * 3600 * 1000;   // fix round 1 (I5): one year
    uint64_t closeDeadlineHits_ = 0;   ///< close() drains cut short by closeTimeoutMs (test hook)
    void releaseCq_(Completions& q) noexcept;
    void applyCqAdd_(detail::CompletionRing* ring);
    void applyCqRelease_(CqRef ref, detail::CompletionRing* ring, uint64_t prodIssued);
    /// F8c C6: a slot whose last pin / producer completion is gone returns to
    /// the free ring.
    void maybeRecycleCqSlot_(uint32_t idx) noexcept;
    /// F8c C6: producer::ProducerImpl::Hooks — duty-side delivery of one
    /// producer completion to registry slot `ref` (generation check + the
    /// producer-op accounting above), and the begin-close / closeImmediately
    /// issue path.
    static void producerCqDeliverTrampoline_(void* ctx, CqRef ref, const Completion& c) noexcept;
    void producerCqDeliver_(CqRef ref, const Completion& c) noexcept;
    static OpResult issueProducerCloseTrampoline_(void* ctx, producer::ProducerImpl& impl,
                                                  const CompletionTarget* target, Token token);
    OpResult issueProducerClose_(producer::ProducerImpl& impl, const CompletionTarget* target, Token token);
    void applyProducerCommand_(const detail::HubCommand& cmd);
    /// close(): hand every registered queue back (producer-close; §3.3 (b)).
    void closeCqRegistry_();
    /// I3: count / uncount a pending op's target slot; recycle a free slot.
    static void pinTargetTrampoline_(void* ctx, detail::OpTarget& t) noexcept;
    void pinCqSlot_(DutyTarget& t) noexcept;
    void freeCqSlot_(uint32_t idx) noexcept;
    /// I3': apply deferred releases whose fence the drain has passed (all: close).
    int applyDeferredReleases_(bool all) noexcept;
    /// Deliver one completion to its target (duty thread / closing thread).
    void deliver_(const DutyTarget& t, Completion c, std::span<const uint8_t> payload,
                  detail::CompletionRing::Overflow ov = detail::CompletionRing::Overflow::Truncate);
    /// F8c C4: detail::CompletionSink bound to the group hub / txn producers.
    static void deliverTrampoline_(void* ctx, const detail::OpTarget& t, const Completion& c,
                                   std::span<const uint8_t> payload) noexcept;
    /// F8c C4: detail::TargetedOpSink bound to handles that issue token ops
    /// (GroupConsumer::commitSync, the txn ops): validate + reserve, encode the
    /// target into the command, apply directly on the duty thread or submit.
    static OpResult targetedOpTrampoline_(void* ctx, detail::HubCommand& cmd,
                                          const CompletionTarget& target, Token token, OpKind op);
    OpResult issueTargeted_(detail::HubCommand& cmd, const CompletionTarget& target, Token token, OpKind op,
                            bool viaRing = false);
    /// F8c C4: detail::AndWaitSink — the handles' *AndWait sugar.
    static Completion andWaitTrampoline_(void* ctx, const char* what, std::chrono::nanoseconds timeout,
                                         detail::AndWaitIssue issue, void* issueCtx, std::string* text);
    /// I3: pin the completion target a command carries (re-encoded counted).
    void pinCommandTarget_(detail::HubCommand& cmd) noexcept;
    /// D4: a follower named the group / txn coordinator (ErrorResponse{
    /// NotCoordinator, endpoint}) — the coordinator route follows it (the seed
    /// session never moves).  No-op when it names the current coordinator.
    void onCoordinatorRedirect_(const std::string& endpoint);
    /// D4: tell the group hub the COORDINATOR session's (connected, sessionId,
    /// episode) if it changed; a coordinator session that (re)connected joins the
    /// members there; one that gave up falls back to the seed.  Called from
    /// syncSeedSession_ (every transition point) and before the group's session
    /// watch.  Duty thread only; O(1) when unchanged.
    void syncCoordinatorSession_() noexcept;
    /// D4: the coordinator session's broker id (0 while not connected / known).
    [[nodiscard]] int64_t coordSessionId_() const noexcept {
        return agent_.sessionMachineAt(coordIdx()).sessionId();
    }
    // ---- D4 coordinator route (duty thread only) ----------------------------
    // Kafka's coordinator connection: empty = the seed (byte-identical to the
    // pre-D4 client until a NotCoordinator answer names another node).
    detail::CoordRoute coordRoute_;   // #362: CoordSeed, or CoordRemote{session index, channel, failBase, downSince}
    [[nodiscard]] uint32_t coordIdx() const noexcept { return detail::coordIdxOf(coordRoute_); }
    // A coordinator that stays down past kCoordDownFallbackNs is given up like an exhausted connect budget (back to
    // the seed, which names the CURRENT coordinator): a crashed coordinator node's session otherwise retries for
    // minutes and every txn / group op waits on it.
    static constexpr int64_t kCoordDownFallbackNs = 5'000'000'000LL;
    uint64_t    coordinatorRedirects_ = 0;   // coordinator moves followed
    uint64_t    coordinatorFallbacks_ = 0;   // coordinator sessions given up (back to the seed)
    // r2 LOW-3: a coordinator channel whose session was Rejected / Closed, and
    // the metadataResponses_ count then (re-followed only after fresh metadata).
    std::optional<detail::RejectedCoord> coordRejected_;
    // The last coordinator-session tuple the group hub was told about.
    uint32_t              lastCoordIdx_     = 0;
    session::SessionState lastCoordState_   = session::SessionState::Disconnected;
    int64_t               lastCoordSid_     = 0;
    int64_t               lastCoordEpisode_ = 0;
    /// Every group frame (JoinGroup / SyncGroup / Heartbeat / OffsetFetch):
    /// the seed's ring path, or the coordinator session (built once).
    group::SendFn coordSendFn_;
    /// Phase 2b F: the CoordinatorSink every producer's CoordinatorLink offers through.
    [[nodiscard]] static producer::CoordinatorSink::Offer coordOfferTrampoline_(void* ctx, const uint8_t* buf,
                                                                                std::size_t len) noexcept;
    [[nodiscard]] static int64_t coordSessionIdTrampoline_(void* ctx) noexcept;
    /// F14: NotCoordinator -> send a MetadataRequest (duty thread).
    [[nodiscard]] static bool rediscoverTrampoline_(void* ctx) noexcept;

    // ---- F8c C3: control ops (duty-thread owned pending table) --------------
    /// `secret` (review B I4): bytes carried to the duty thread AFTER the wire frame in the same record, handed to the
    /// op's PendingCtl::newPassword (a self SET PASSWORD's plaintext, installed on OK) — never sent, wiped on any outcome.
    OpResult issueControl_(OpKind op, CompletionTarget target, Token token,
                           std::span<const uint8_t> frame, uint32_t payloadHint, uint32_t requestIdAt = 0,
                           std::span<const uint8_t> secret = {});
    // ---- Catalog history (Task 5): DDL issue + answers (duty thread, cold) ----
    /// The DDL extension of `ddl` (views into `ddl` / cfg_; requestId as given).
    [[nodiscard]] rune::protocol::client::DdlExtension ddlExt_(const DdlOptions& ddl) const noexcept;
    /// Issue an encoded DDL frame whose extension `ext` starts at `extOffset`
    /// (the encoder's extOffset(), review M2): the duty thread stamps a
    /// generated requestId there when ext.requestId is none() — the offset is
    /// verified against a decode of the frame first.
    OpResult issueDdl_(OpKind op, CompletionTarget target, Token token, std::span<const uint8_t> frame,
                       const rune::protocol::client::DdlExtension& ext, std::size_t extOffset,
                       std::span<const uint8_t> secret = {});
    /// Review B I4: the session user's own SET PASSWORD was applied — every session logs in with the new password
    /// from now on, the salted-password cache is cleared, a one-time session becomes a normal one.
    void installSelfPassword_(std::string_view newPassword) noexcept;
    /// ErrorResponse{Redirect, endpoint} for pendingCtl_[i] (a DDL).
    void onDdlRedirect_(std::size_t i, std::string_view endpoint, int64_t nowNs);
    /// NotLeader (ErrorResponse or a response's errorCode 2) for pendingCtl_[i] (a DDL).
    void onDdlNotLeader_(std::size_t i, std::string_view detail, int64_t nowNs);
    /// Offer d.frame to d.endpoint's node (the seed when ""); on Accepted, `on`
    /// names the session it went out on.
    transport::AeronTransport::OfferResult offerDdl_(DdlPending& d, int64_t nowNs, DdlOfferedOn& on);
    /// Move d to `next`, keeping ddlAwaitingResend_ exact.
    void setDdlRetry_(DdlPending& d, DdlRetry next) noexcept;
    /// ddlPending_ / ddlAwaitingResend_ for an entry entering (+1) / leaving (-1) the table.
    void countDdl_(const PendingCtl& p, int sign) noexcept;
    /// A seed-session loss: every pending NON-DDL control op ends SessionLost (a
    /// DDL follows its own session: the pump re-sends it, same requestId).
    void failControlOnSeedLoss_(uint16_t errorCode, std::span<const uint8_t> text);
    /// Complete pendingCtl_[i] (a DDL) as a terminal failure with `code` / `detail`.
    void failDdl_(std::size_t i, uint16_t code, std::string_view detail, int64_t value = 0, int64_t aux = 0);
    /// Duty cycle while any DDL is pending: an in-flight DDL whose session was
    /// lost moves to a resend toward the seed (same frame, same requestId,
    /// inside the NotLeader budget); every DDL whose resend is due (backoff
    /// elapsed, held for a connecting node, unsent on the seed) is re-offered;
    /// one refused past its budget fails explicitly.
    int driveDdl_(int64_t nowNs);
    void applyControlOpRecord_(const uint8_t* rec, std::size_t len);
    void applyControlOp_(const ControlOpHeader& h, std::span<const uint8_t> frame);
    void completeCtl_(PendingCtl& p, OpStatus st, int64_t value = 0, int64_t aux = 0,
                      uint16_t errorCode = 0, std::span<const uint8_t> payload = {},
                      detail::CompletionRing::Overflow ov = detail::CompletionRing::Overflow::Truncate,
                      uint32_t reserved0 = 0);
    /// Remove pendingCtl_[i] (swap-remove) and return it (delivery happens
    /// after removal: a handler may issue a new op).
    PendingCtl takeCtl_(std::size_t i);
    [[nodiscard]] std::size_t findCtl_(int64_t corrId) const noexcept;
    int  retryUnsentControl_();
    void sweepControl_(int64_t nowNs);
    void failAllControl_(OpStatus st);
    /// Authentication (44 / 46): every pending control op (and a pending connect) completes
    /// BrokerError `errorCode` with `text` — the session is Rejected, nothing more can be answered.
    void failAllControlWithCode_(uint16_t errorCode, std::string_view text);
    /// Authentication: an ErrorResponse carrying codes 44..47 — true when it concerned a subscription, a
    /// group fetch, a produce batch or the whole session and is fully handled here (see Client.cpp).
    bool onAuthErrorResponse_(const codec::ErrorResponseView& e);
    /// Authentication (D26): the seed session was revoked — every in-flight request and subscription
    /// completes SessionRevoked; the Rejected seed then fails the Client for good (no reconnect).
    void onSeedRevoked_(std::string_view text);
    void completeConnect_(OpStatus st, int64_t value, uint16_t errorCode, std::string_view text, int64_t aux = 0);
    /// Fix round 4: the seed's Connected edge — every connect-time action, once.
    void onSeedConnected_(int64_t sessionId, int32_t brokerVersion);
    /// A blocking call (`what`) must not run where it could never complete:
    /// Invoker mode — only the owner thread, outside a duty-cycle callback;
    /// Thread mode — never on the runner thread.  std::logic_error otherwise.
    void checkBlockingCaller_(const char* what) const;
    /// Issue-and-wait shared by the *AndWait wrappers (see their doc).
    template <class Issue, class OnDone>
    void andWait_(const char* what, std::chrono::nanoseconds timeout, uint32_t maxResultBytes,
                  Issue&& issue, OnDone&& onDone);
    void throwForStatus_(const char* what, const Completion& c, std::span<const uint8_t> payload) const;
    /// F8c C3: publish (state, sessionId) to app threads on change (duty).
    void publishSeedStatus_(session::SessionState st, int64_t sid) noexcept;
    /// F8c C3: install a callback set through a ClientSetCallback command.
    void applySetCallback_(int which, void* holder);
    template <class Fn>
    void setCallback_(int which, Fn cb);

    /// Background thread body: loops doWork() under the configured IdleStrategy
    /// (YieldingIdleStrategy for LowLatency, BackoffIdleStrategy for Balanced)
    /// until the command ring is closed, performing a periodic timeout sweep.
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

    /// F8a: HubCommandSink bound to every Subscription / GroupConsumer handle
    /// (ctx = this).  Submits the command on the agent ring with
    /// submitRegistration_'s bounded back-pressure, plus the same post-submit
    /// close() re-check as producer() (see detail::HubSubmit).
    static detail::HubSubmit hubCommandTrampoline_(void* ctx, const detail::HubCommand& cmd);
    /// F8a fix3: the error for a failed submitSend (oversize vs ring full).
    [[nodiscard]] std::exception_ptr submitError_(const char* what, std::size_t frameLen) const;
    /// F8a fix2: duty-thread direct send on the default ingress publication.
    static detail::SendResult directSendTrampoline_(void* ctx, const uint8_t* buf, std::size_t len);
    /// D4: the group hub's direct sends (OffsetCommit / LeaveGroup) to the coordinator.
    static detail::SendResult coordDirectSendTrampoline_(void* ctx, const uint8_t* buf, std::size_t len);
    /// Group fetch routing (GroupConsumerHub::FetchRouter, ctx = this): a
    /// member's per-partition Subscribe goes to the partition leader's session.
    static uint32_t fetchLeaderTrampoline_(void* ctx, int32_t topicId, int32_t partition) noexcept;
    /// E2 final fix wave (I2): a fresh correlationId for the subscriber hub
    /// (a moved subscription re-armed after its node session was lost).
    static int64_t nextCorrTrampoline_(void* ctx) noexcept;
    /// E2 final fix wave (I2): a moved plain subscription answered Redirect
    /// follows it PER PARTITION — the partition's route moves to `endpoint` and
    /// the re-armed Subscribe goes to that node's own session; the seed is
    /// never retargeted.  False when `corrId` is not a pending moved one.
    bool followMovedSubscriptionRedirect_(int64_t corrId, std::string_view endpoint) noexcept;
    static int64_t fetchSessionIdTrampoline_(void* ctx, uint32_t session) noexcept;
    static uint64_t fetchEpochTrampoline_(void* ctx, uint32_t session) noexcept;
    static void fetchReviveTrampoline_(void* ctx, uint32_t session) noexcept;
    static int32_t fetchBrokerVersionTrampoline_(void* ctx, uint32_t session) noexcept;
    /// #290: a node session's reject code (46 / 44 ...) while it is Rejected, else 0 (0 for the seed, whose
    /// rejection ends the Client's work on its own path).
    static int16_t nodeRejectCodeTrampoline_(void* ctx, uint32_t session) noexcept;
    static uint32_t nodeSessionIndexOfTrampoline_(void* ctx, int64_t sid) noexcept;   // #446
    [[nodiscard]] int16_t nodeRejectCode_(uint32_t session) const noexcept;
    static int32_t subscribeBrokerVersionTrampoline_(void* ctx, uint32_t session) noexcept;   // Phase 2c: 0 unless Connected
    /// M1 (duty thread, while a group consumer exists): a leader-node session a
    /// group partition subscribes through was lost (left Connected) or became
    /// terminally Disconnected — ask for metadata (the leader may have moved),
    /// re-drive a Disconnected one's connect, re-route.  Event-driven: state
    /// transitions of the agent's sessions, no clock.
    int watchGroupSessions_(int64_t nowNs);
    /// #514: a leader-node session carrying a homed subscription or a group fetch that has been silent for more
    /// than two keepalive intervals keeps the backoff-paced metadata refresh armed (a BOUND reader arms nothing
    /// else): fresh metadata moves it through driveHomed / group reconcileFetch before the session is stale.
    int watchSilentLeaders_(int64_t nowNs);
    int watchProduceRoutes_(int64_t nowNs) noexcept;
    // ---- DR client bug 1: seed failover (duty thread) -------------------------
    // The seed's broker died: its session went silent (stale loss) or it gave up
    // reconnecting there.  Every seed-bound path (metadata, DDL, new Subscribes,
    // GetViewRow, the default group / txn coordinator) would otherwise wait on a
    // dead broker for good.  The seed moves to another broker — first one a live
    // session is Connected to, then the metadata's brokers, then the bootstrap —
    // one candidate per attempt, each bounded by kSeedFailoverAttemptMaxNs (or the
    // connect timeout, if shorter).  Per-state data (hard rule 3):
    struct SeedSteady {};                  ///< Connected, never Live, closed: nothing to do
    struct SeedFailingOver {
        uint32_t attempt           = 0;    ///< candidates tried this episode
        int64_t  attemptDeadlineNs = 0;    ///< the current candidate's connect deadline (duty clock)
        int64_t  notBeforeNs       = 0;    ///< the next attempt never starts earlier (pacing)
        int64_t  gapNs             = kSeedFailoverMinGapNs;  ///< fix round 1 (I2): doubles per attempt
        int      backoffAttemptMoved = -1; ///< I2 (reconnectForever): the Backoff entry already re-pointed
        int32_t  lastTriedNode = INT32_MIN; ///< fix round 2 (I-3): the cyclic cursor (INT32_MAX = the bootstrap)
    };
    /// Fix round 1 (I2): a non-retriable connect answer ended the episode — the
    /// seed stays down (explicit) until it is Live again.
    struct SeedFailoverStopped {};
    std::variant<SeedSteady, SeedFailingOver, SeedFailoverStopped> seedFailover_;
    uint64_t seedFailoverLossesSeen_ = 0;  ///< the seed machine's staleSessionLosses() last seen
    uint64_t seedFailovers_          = 0;  ///< moves started (test hook)
    uint64_t seedFailoverConnects_   = 0;  ///< fix round 1 (I2): moves that connected by themselves
    uint64_t metadataViaOtherSession_ = 0; ///< MetadataRequests sent on a non-seed session
    std::vector<std::pair<int32_t, std::string>> seedCandidates_;  ///< (nodeId, channel); cold scratch (capacity persists)
    static constexpr int64_t kSeedFailoverAttemptMaxNs = 10'000'000'000LL;  // 10 s per candidate
    static constexpr int64_t kSeedFailoverMinGapNs     = 100'000'000LL;     // first gap 100 ms ...
    static constexpr int64_t kSeedFailoverMaxGapNs     = 10'000'000'000LL;  // ... doubling, capped at 10 s
    uint64_t seedMovesSeen_ = 0;           ///< fix round 1 (C1): agent_.seedMoves() last seen
    uint64_t seedConnectEpochSeen_ = 0;    ///< fix round 2 (C-1): agent_.connectedEpoch(0) last seen
    int64_t  metadataAnswerByNs_ = 0;      ///< fix round 1 (M2): an unanswered MetadataRequest's due time (0 = none)
    void noteMetadataSent_() noexcept;
    bool offerMetadataOnNode_(uint32_t index) noexcept;
    /// Fix round 1 (I4): one MetadataRequest on EVERY Connected session (route
    /// failures); returns how many were sent.
    int  sendMetadataRequestEverywhere_() noexcept;
    /// Fix round 1 (I1): re-offer every in-flight batch on its current route.
    void resendAllInFlightViaRoute_() noexcept;
    int  driveSeedFailover_(int64_t nowNs) noexcept;
    bool startSeedFailoverAttempt_(SeedFailingOver& f, int64_t nowNs, bool connectNow = true) noexcept;
    /// A MetadataRequest on the seed when it is Connected, else on any Connected
    /// node session (the new leader is learnt from any live broker).  False when
    /// no session is Connected (the caller arms the backoff refresh).
    bool sendMetadataRequestAnywhere_() noexcept;
    std::vector<uint64_t> routeConnectFailuresSeen_;  // DR-7: per session, last seen connectFailures()
    std::vector<uint8_t>  routeSessionUp_;            // DR-7 review M1: per session, Connected last pass
    uint64_t              heldOffersSeen_ = 0;        // DR-7: last seen agent_.heldOffers()
    // DR-15: Subscribes whose Redirect named an unreachable node — re-sent (only
    // they) after a fresh metadata answer and a doubling backoff.
    int retryRedirectedSubscribes_(int64_t nowNs);
    // E2 residual round 5 (R3): the follow of a NEW plain Subscribe's Redirect.
    enum class RedirectFollow : uint8_t { PerPartition, Hold, BySeed };
    [[nodiscard]] RedirectFollow redirectFollowFor_(const std::string& endpoint) const;
    void redirectBySeed_(int64_t corrId, const std::string& newEndpoint);
    int  releaseHeldRedirectFollows_();
    struct HeldRedirect { int64_t corrId; std::string endpoint; int metaSeen; };
    std::vector<HeldRedirect> heldRedirectFollows_;   // duty thread; cold
    uint64_t redirectFollowsHeld_ = 0;
    uint64_t staleSubscribeRedirects_ = 0;   // R4: Redirects from a session the Subscribe is not outstanding on
public:
    [[nodiscard]] uint64_t testStaleSubscribeRedirects() const noexcept { return staleSubscribeRedirects_; }
    [[nodiscard]] uint64_t testRedirectFollowsHeld() const noexcept { return redirectFollowsHeld_; }
    /// PR #441 F4: re-sends of ONE pending Subscribe answered RecordEgressUnavailable before it fails explicitly.
    static constexpr int kMaxEgressUnavailableRetriesForTest = 3;
private:
    std::vector<int64_t>  redirectedSubscribes_;
    int                   redirectMetaSeen_ = 0;
    int64_t               nextRedirectedResendNs_ = 0;
    static constexpr int64_t kRedirectedResendInitNs = 100'000'000LL;     // 100 ms
    static constexpr int64_t kRedirectedResendMaxNs  = 2'000'000'000LL;   // 2 s
    static constexpr int kMaxEgressUnavailableRetries = kMaxEgressUnavailableRetriesForTest;
    int64_t               redirectedResendBackoffNs_ = kRedirectedResendInitNs;
    uint64_t              unreachableRedirects_ = 0;
    uint64_t groupUpMask_   = 0;  // leader sessions (bit i-1 = session i) Connected last pass
    uint64_t groupDiscMask_ = 0;  // ... terminally Disconnected last pass
    uint64_t groupRejMask_  = 0;  // #290: ... Rejected last pass (a login refused there is never "lost")
    int64_t  nextSilentCheckNs_  = 0;   // #514: watchSilentLeaders_ runs once per silentCheckPeriodNs_ (duty clock)
    int64_t  silentCheckPeriodNs_ = 0;  // #514: a quarter of the configured keepalive interval (set at construction)
    static detail::SendResult fetchOfferTrampoline_(void* ctx, uint32_t session, const uint8_t* buf,
                                                    std::size_t len) noexcept;
    /// A Redirect / NotLeader answered a group fetch Subscribe (corrId tagged
    /// GroupConsumerHub::isFetchCorrId): re-route that partition, re-subscribe.
    void onGroupFetchRefused_(int64_t corrId, bool redirect, std::string_view endpoint) noexcept;
    /// T15b: the failure path (duty thread).  Latched once, when the Aeron
    /// client closed (or the seed session was Rejected, a terminal answer):
    /// every producer's intake is tail-closed and every accepted record
    /// completes Closed exactly once; pending control ops complete Closed; the
    /// agent stops all I/O; a Thread-mode runner switches to a parked loop.
    void enterFailed_();
    /// One duty cycle once failed: the command ring (no I/O), the producers'
    /// discard of their closed rings, and the close / release machinery.
    int  failedDutyCycle_(int64_t nowNs);
    /// T15b: derive this cycle's ShardStatus; publish it on change.
    void publishShardStatus_() noexcept;
    bool transportFailed_ = false;  // duty thread only
    uint64_t failedDutyCycles_ = 0;  // duty thread only (test hook)
    // T15b shard status (cfg_.shardStatus): ring created in the ctor before the
    // runner starts; duty-side state below is the duty thread's.
    std::unique_ptr<detail::StatusRing<shard::ShardStatus, 64 * 1024>> shardRing_;
    shard::ShardStatus shardDuty_{};
    shard::ShardStatus shardPublished_{};
    bool               shardDirty_  = false;
    bool               shardClosed_ = false;  // the closing thread
    /// F8a fix2: last observed seed-session state (duty thread only).
    session::SessionState lastSeedState_ = session::SessionState::Disconnected;
    // fix4 M5: the last (state, sessionId, connect episode) of the seed session
    // the hubs were told about (duty thread only).
    int64_t lastSeedSessionId_ = 0;
    int64_t lastSeedEpisode_   = 0;
    /// No-hang fix round 4 (item 6): the seed machine's staleSessionLosses() the
    /// duty thread last acted on (duty thread only).
    uint64_t lastSeedStaleLosses_ = 0;
    /// fix4 I-1/M5: tell both hubs the seed session's current (connected,
    /// sessionId, episode) if it changed since the last call.  Called AT the
    /// transition points: the ConnectResponse / ErrorResponse handlers (before
    /// any resend or callback of that poll), before applying any hub command,
    /// and once per duty cycle (step 3c).  Duty thread only; O(1) when unchanged.
    void syncSeedSession_() noexcept;
    /// F8a: apply one hub command on the duty thread (ring drain, close drain,
    /// or directly when a handle is used on the duty thread).
    void applyHubCommand_(const detail::HubCommand& cmd);
    /// fix3 I3: the duty-thread DIRECT application (acts on the impl).
    void applyHubCommandDirect_(const detail::HubCommand& cmd);
    /// The thread whose handle calls take the duty path: the Thread-mode runner;
    /// none in Invoker mode (the owner is both app and duty thread).
    [[nodiscard]] std::thread::id handleDutyThread_() const noexcept {
        return cfg_.runnerMode == RunnerMode::Thread ? dutyThreadId_ : std::thread::id{};
    }

    /// Setup-phase ring submit, bounded (5 s).  Invoker mode runs the duty cycle
    /// between attempts — this thread IS the ring's consumer; thread mode parks
    /// 100 µs.  On timeout runs `onFail` and throws.  Returns true when the
    /// command was accepted, false when the ring closed under it (close()
    /// raced this call: the command was NOT written — the caller still owns
    /// its payload).  Throws logic_error("client closed") (after onFail) if
    /// the Client was already closed on entry.
    template <class Submit, class OnFail>
    bool submitRegistration_(Submit&& submit, OnFail&& onFail) {
        // After close() no duty cycle will ever adopt the holder (and Thread mode
        // would park 5 s on a ring nobody drains): fail fast, nothing leaks.
        // A fast-fail only: correctness of the registration-vs-close race rests
        // on the ring itself (F8c C2) — the closed bit is in the tail word the
        // claim CASes, so the submit below is either before the close (drained
        // by close()) or refused as Closed (never written).
        using S = agent::ClientAgent::Submit;
        if (agent_.commandRingClosed()) {
            onFail();
            throw std::logic_error("client closed");
        }
        if (testBeforeRegistrationSubmitHook_) [[unlikely]] testBeforeRegistrationSubmitHook_();
        S r = submit();
        if (r == S::Accepted) return true;
        if (r == S::Closed) return false;  // F8c C2: linearizable — never written
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
        for (;;) {
            r = submit();
            if (r == S::Accepted) return true;
            if (r == S::Closed) return false;
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

    /// Internal subscribe implementation shared by Key and partition overloads.
    Subscription subscribeImpl(TopicHandle              topic,
                                int32_t                  partition,
                                uint8_t                  partitionKnown,
                                std::span<const uint8_t> keyBytes,
                                SubscribeOptions&        opts,   // the caller's own copy: callbacks are moved out
                                RecordHandler            handler);

    // -------------------------------------------------------------------------
    // Members
    // -------------------------------------------------------------------------

    ClientConfig cfg_;

    // ---- Agent (runner-thread owned after start; app uses submit* + snapshot) -
    agent::ClientAgent agent_;

    // ---- Runner thread -------------------------------------------------------
    // F8c C2/C3: no stopped_ flag.  "Closed" IS the agent command ring's closed
    // bit (ring internals, linearizable with every submit): the runner loops
    // until it sees it, close() sets it once (its first caller does the work).
    // fix5: see testSetBeforeRegistrationSubmitHook(). Empty by default.
    std::function<void()> testBeforeRegistrationSubmitHook_;
    // Task 14 fix1 I2 test hook: called at the top of every dutyCycle() (inside
    // the in-cycle guard). Plain function pointer, duty-cycle thread only.
    void (*testDutyCycleHook_)() = nullptr;
    std::function<void()> testHubCommandRingFullHook_;  // F8a fix1; empty by default
    std::thread       runnerThread_;

    // ---- F8c C3: control ops (duty-thread owned; PendingX maps + mtx_ gone) --
    // Correlation-id spaces (disjoint; an answer is matched by its id alone):
    //   session i's ConnectRequests   i * ClientAgent::kSessionCorrIdStride + n (< 2^50)
    //   keepalive Pings               negative (-(session index + 1))
    //   the Client's counter (below)  kClientCorrIdTag | n — every control op
    //                                 (ping / DDL / schema / view / metadata),
    //                                 Subscribe / Unsubscribe, txn, OffsetCommit
    //   group OffsetFetch             bit 60 (GroupConsumerHub::kOffsetFetchCorrTag)
    //   group fetch Subscribes        bit 61 (GroupConsumerHub::kFetchCorrTag)
    //   producer batches              bit 62 (producer::batchcorr::kTag)
    //   producer coordinator requests bit 58 (producer::coordcorr::kTag; bit 59 is the counter above)
    // Task 5 fix round 0 (C): the counter used to start at 1 — the seed's
    // connect ids (session 0) count from 1 too, so the n-th connect and the
    // n-th control op shared an id and an answer to one was taken for the other.
    static constexpr int64_t kClientCorrIdTag = int64_t{1} << 59;
    static_assert(kClientCorrIdTag == producer::coordcorr::kClientCounterTag,
                  "the coordinator id tag (bit 58) is chosen disjoint from this counter's");
    int64_t                 nextCorrId_ = kClientCorrIdTag | 1;   // duty-owned correlation counter
    std::vector<PendingCtl> pendingCtl_;               // ping / define / get, by corrId (linear: cold)
    // Catalog history (F14): this Client's random 64-bit instance id (drawn once
    // at construction, never 0; immutable) and the duty thread's DDL counter.
    static uint64_t         drawClientInstanceId_();   // std::random_device, 2 x 32 bits, redrawn while 0
    const uint64_t          clientInstanceId_ = drawClientInstanceId_();
    uint32_t                requestCounter_ = 0;       // duty thread only: the requestId's lo (32-bit, never 0)
    int                     ddlPending_ = 0;           // pendingCtl_ entries that are DDLs or routed control ops (GetViewRow)
    int                     ddlAwaitingResend_ = 0;    // pending DDLs in a ddlWaitsForResend() state
    std::vector<DdlRetryNote> ddlRetryNotes_;          // duty thread; filled only under cfg_.traceDdlRetries
    std::size_t             ddlRetryNotesDropped_ = 0;
    void noteDdlRetry_(DdlRetryNote note);
    PendingCtl              pendingConnect_;           // at most one connect
    bool                    hasPendingConnect_ = false;
    uint32_t                unsentCtl_ = 0;            // entries holding an unsent frame
    uint32_t                ctlAwaitingSeed_ = 0;      // fix round 2 (I-1): entries in CtlAwaitingSeed
    /// Fix round 2 (I-1): no attempt to Connect the seed is pending or will start without app action
    /// (never Live, Rejected, Closed, or a stopped failover) — a control op ends NotConnected only then.
    [[nodiscard]] bool seedCannotReconnect_() const noexcept;
    int driveAwaitingSeed_();
    std::vector<uint8_t>    ctlScratch_;               // frame patch buffer (reused)
    // closing_: written by the closing thread after the runner is joined (or
    // on the Invoker owner); read only on the duty / closing thread.
    bool                    closing_ = false;
    std::vector<DeferredHandler> closeDeferred_;       // closing thread only

    // ---- F8c C2: completion-queue registry (duty-thread owned) --------------
    struct CqSlot {
        detail::CompletionRing* ring = nullptr;
        uint8_t                 gen  = 0;
        bool                    quarantined = false;  // released, still targeted (I3)
        uint32_t                targets     = 0;      // pending ops that target this slot
        // F8c C6: producer completions (per-record / Tier-2 acks, BatchAck credit
        // units) delivered to THIS generation, and — once released — how many
        // its owner issued.  Producer ops travel the producer ring, not the
        // command ring, so they cannot be pinned before the release: the slot
        // stays quarantined until prodDelivered == prodIssued.
        uint64_t                prodDelivered = 0;
        uint64_t                prodIssued    = 0;  // valid while quarantined
    };
    /// I3': an Invoker-owner release fenced on the command ring tail.
    struct DeferredRelease {
        detail::CompletionRing* ring  = nullptr;
        std::int64_t            fence = 0;
        CqRef                   ref   = kNoCqRef;
        uint64_t                prodIssued = 0;  // C6: the owner's producer-op count
    };
    std::vector<DeferredRelease> deferredReleases_;  // duty-thread owned
    std::vector<CqSlot>   cqSlots_;
    std::vector<uint16_t> cqFree_;          // FIFO ring of released slots (I3)
    uint32_t              cqFreeHead_  = 0;
    uint32_t              cqFreeCount_ = 0;
    uint64_t              staleCqDrops_ = 0;

    // ---- F8c C3: seed-session status for app threads (duty -> owner ring) ---
    mutable detail::StatusRing<SeedStatus> seedStatus_;  // consumer side drained by const state()
    bool       seedStatusDirty_ = false;  // duty: a newer status than the ring holds
    SeedStatus seedStatusLast_{};         // duty: last status computed
    mutable SeedStatus seedStatusCache_{};  // owner: latest drained

    // ---- Callbacks (duty-thread owned; installed via ClientSetCallback) ------
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
    // Thread running dutyCycle(): the owner (Invoker), or the runner (Thread),
    // which writes its OWN id as its first action and only then publishes the
    // start latch (F8c C8, spec §3.4); the constructor returns only after
    // reading that record, so every other reader is ordered after the write.
    std::thread::id dutyThreadId_;
    // ---- CB-2: ProducerHub + topic registry ----------------------------------
    // Hub is owned by Client and wired into the agent before start().
    producer::ProducerHub producerHub_;

    std::vector<PendingRelease> releases_;  // duty-thread owned (C7)
    std::vector<PendingRelease> orphans_;   // duty-thread owned (C7 F1): orphaned, not yet applied
    uint64_t implsHandedBack_ = 0;          // closing thread (C7 test hook)
    struct DrainWaiter {
        DutyTarget target{};
        Token      token = 0;
    };
    std::vector<DrainWaiter> drainWaiters_;  // duty-thread owned (C7): Client::close() drains

    // ---- CB-4: SubscriberHub -------------------------------------------------
    // Duty-thread owned (F8a): subscribe()/unsubscribe() reach it via the agent ring.
    subscriber::SubscriberHub subscriberHub_;

    // ---- CG-5: GroupConsumerHub ---------------------------------------------
    // Duty-thread owned (F8a): subscribeGroup() registers through the agent ring.
    group::GroupConsumerHub groupConsumerHub_;
    // subscribeGroup()'s one-group-per-Client guard (app side; setup phase —
    // the hub itself is duty-thread owned and is never read by an app thread).
    bool groupSubscribed_ = false;

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

    // ---- Task 11: per-topic schema metadata ---------------------------------
    // F8c C3: duty-thread owned — written when a TopicSchemaResponse{OK}
    // completes a getTopicSchema (no app-thread writer, no mutex).
    metadata::TopicSchemaCache topicSchemaCache_;

    // ---- Task 9 (CLI): names of topics successfully defined via defineTopic() -
    // Duty-thread owned: appended when DefineTopicResponse{OK} completes a
    // define; read by the ListTopics op.  Insertion order = apply order.
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
    bool sendMetadataRequest(int64_t sessionId) noexcept;   // false: not submitted (fix round 1 M2)

    // scheduleMetadataRefresh() — arm a background metadata refresh with
    // exponential backoff (IMPORTANT-3).  Called on no-progress.  The backoff
    // doubles each call (capped) so the client can't storm.
    void scheduleMetadataRefresh() noexcept;

    // tickMetadataRefresh() — fire an armed refresh if its backoff window has
    // elapsed and the session is Connected.  Called from sweepTimeouts().
    void tickMetadataRefresh() noexcept;
    // Task 16 fix round 1: request metadata now (owner moved), rate-limited.
    void requestMetadataNow_() noexcept;
    static constexpr int64_t kUrgentMetadataMinGapNs = 100'000'000LL;  // 100 ms (duty clock)
    int64_t lastUrgentMetadataNs_ = INT64_MIN;
    int     metadataResponses_    = 0;  // MetadataResponses applied (test hook)

    // Catalog history (Task 13): built eagerly in the constructor, so its owner thread
    // is the Client's constructing thread and there is no lazy-creation race
    // (never touched by the duty thread — its callbacks only write the reader's
    // own ring).  Declared late: destroyed early, after ~Client's close.
    const std::unique_ptr<CatalogReader> catalogReader_;
    const std::unique_ptr<SecurityReader> securityReader_;   // authentication Task 8 (same owner-thread rule)
    uint32_t testSkewDdlRequestIdAt_ = 0;   // F4 test seam: issuing thread only (testSkewNextDdlRequestIdAt)

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
    // NOT part of the production API surface.  They read duty-owned state, so
    // they may be called only on the duty thread (the Invoker owner between
    // doWork() calls, or a duty-cycle callback), or once a Thread-mode runner
    // has been joined (after close()).  F8c fix round 1: debug builds abort on
    // any other caller, turning a would-be TSan-only data race (a test thread
    // reading while the runner writes) into a deterministic failure.
    [[nodiscard]] std::string testRouteEndpoint(int32_t topicId,
                                                const Key& key) const {
        testHookOwnerCheck_();
        return partitionRouter_.routeKey(topicId, key).channel;
    }
    [[nodiscard]] int32_t testLeaderFor(int32_t topicId, int32_t partition) const {
        testHookOwnerCheck_();
        return metadataCache_.leaderFor(topicId, partition);
    }
    [[nodiscard]] int32_t testEpochFor(int32_t topicId, int32_t partition) const {
        testHookOwnerCheck_();
        return metadataCache_.epochFor(topicId, partition);
    }
    void testHookOwnerCheck_() const noexcept {
#ifndef NDEBUG
        const bool onDuty = std::this_thread::get_id() == dutyThreadId_;
        const bool runnerGone =
            cfg_.runnerMode == RunnerMode::Thread && !runnerThread_.joinable();
        if (!onDuty && !runnerGone) {
            std::fprintf(stderr,
                         "rune-client: FATAL: duty-owned test hook read off the duty thread "
                         "while the runner is live (data race); aborting\n");
            std::fflush(stderr);
            std::abort();
        }
#endif
    }
    /// F8c C8: the duty-thread id.  Any thread, once the constructor returned
    /// (the start latch orders the runner's write before the return).
    [[nodiscard]] std::thread::id testDutyThreadId() const noexcept { return dutyThreadId_; }
    /// F8c C8: the runner's id (Thread mode) — the constructing/closing thread only.
    [[nodiscard]] std::thread::id testRunnerThreadId() const noexcept { return runnerThread_.get_id(); }
    [[nodiscard]] std::string testCurrentLeaderChannel() const {
        return agent_.seedSession().currentLeaderChannel;
    }
};

} // namespace rune::client
