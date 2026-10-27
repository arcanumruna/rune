#pragma once
// ShardTestClient.h — a minimal REAL wire client for shard-mode integration
// tests: its own (threaded) embedded driver + Aeron client, raw Rune protocol
// frames over UDP.  Every wait is a bounded poll of a real condition (hard
// iteration cap), never a sleep-for-correctness.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <Aeron.h>
#include <FragmentAssembler.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <future>
#include <map>
#include <set>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace rune::node::test {

namespace pc = rune::protocol::client;

/// The test client's DefineTopic config: topics roll at 1 MiB, not the 64 MiB default.  The log preallocates
/// (fallocate) its active segment to segmentRollBytes, so a stress run creating a few hundred topics in one process
/// otherwise fills the disk (~65 MB per topic).  A test that asserts on rolling or segment sizes passes its own config.
inline constexpr int64_t kTestTopicSegmentRollBytes = 1LL << 20;
inline rune::protocol::TopicConfigView boundedTestTopicConfig() {
    rune::protocol::TopicConfigView c;
    c.segmentRollBytes = kTestTopicSegmentRollBytes;
    return c;
}

inline uint16_t freeUdpPort() {
    return rune::aeronutil::testing::allocFreeUdpPort();   // D8: never an ephemeral port
}

/// n disjoint blocks of `width` consecutive free UDP ports (per-worker base ranges), each reserved for this
/// process by the collision-proof allocator (FreePort.h) — every port of a block, not just its first.
inline std::vector<uint16_t> freeUdpPortBlocks(int n, int width) {
    std::vector<uint16_t> out;
    for (int i = 0; i < n; ++i)
        out.push_back(rune::aeronutil::testing::allocFreeUdpPortRange(static_cast<uint32_t>(width)));
    return out;
}

struct PartitionInfo {
    int32_t topicId = 0, partition = 0, leaderId = -1, ingressStreamId = 0;
    std::string ingressEndpoint;
    int32_t egressControlPort = 0;   ///< per-worker MDC egress control (leader host)
};

inline std::vector<uint8_t> makeBatch(int records) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < records; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}

class ShardTestClient {
public:
    static constexpr int32_t kEgressStream = 4242;
    static constexpr int     kMaxIters     = 50'000;   // × 100 µs ⇒ 5 s hard cap per wait

    /// `termBufferBytes`: the client driver's term — it bounds the client's own
    /// publications (a Produce frame must fit term / 8).
    /// `legacyTransport`: the pre-2026-10-02 client transport (1408 MTU, 2 MiB
    /// socket buffers, sparse terms) instead of the host-derived one.
    /// `mode`: Invoker = the rune-client-cpp RunnerMode::Invoker shape — the
    /// client's driver AND conductor run only inside this client's calls (every
    /// wait pumps them); a test that stops calling the client stops its driver
    /// (no status messages to the broker).  Construct, use and destroy it on ONE
    /// thread (the driver's pump owner).
    explicit ShardTestClient(const std::filesystem::path& aeronDir, int32_t termBufferBytes = 1 << 20,
                             bool legacyTransport = false, DriverMode mode = DriverMode::Threaded,
                             int32_t rcvWindowBytes = 128 * 1024)
        : driver_(cfgFor(aeronDir, termBufferBytes, legacyTransport, rcvWindowBytes), mode),
          invoker_(mode == DriverMode::Invoker)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir.string());
        ctx.useConductorAgentInvoker(invoker_);
        aeron_ = aeron::Aeron::connect(ctx);
        egressChannel_ = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
        const int64_t reg = aeron_->addSubscription(egressChannel_, kEgressStream);
        for (int i = 0; i < kMaxIters && !egress_; ++i) { egress_ = aeron_->findSubscription(reg); pause_(); }
    }
    ~ShardTestClient() { pubs_.clear(); exclusivePubs_.clear(); egress_.reset(); aeron_.reset(); }

    /// Keep every session this client connected alive like a real client does
    /// (rune-client-cpp SessionMachine: a Keepalive every 1 s) while it waits.
    /// Off by default: tests that model a SILENT client (paused, no keepalive)
    /// rely on the gateway's lease.
    void keepSessionsAlive(bool on) noexcept { keepalive_ = on; }

    /// F-A: how long THIS client has sent nothing on `sid` (since its connect / its last offered Keepalive);
    /// max() if this client does not keep `sid`.  A harness-observed cause, never product evidence.
    [[nodiscard]] std::chrono::nanoseconds silentFor(int64_t sid) const {
        for (const auto& cs : sessions_)
            if (cs.sessionId == sid) return std::chrono::steady_clock::now() - cs.lastSent;
        return std::chrono::nanoseconds::max();
    }
    /// F-A: the broker told this client that `sid` expired (SessionExpired / UnknownSession).
    [[nodiscard]] bool sessionExpired(int64_t sid) const { return expired_.count(sid) != 0; }
    /// F-A: stop keeping `sid` alive (the caller reconnects / forgot it).
    void dropSession(int64_t sid) {
        std::erase_if(sessions_, [sid](const ConnectedSession& cs) { return cs.sessionId == sid; });
        expired_.erase(sid);
    }

    /// Run a blocking call (a node's controlledShutdown / stop) on another
    /// thread while THIS thread keeps pumping the client (keepalives, egress):
    /// a real client's runtime keeps running while the broker does work.
    template <class F> auto whilePumping(F&& f) {
        auto fut = std::async(std::launch::async, std::forward<F>(f));
        while (fut.wait_for(std::chrono::seconds{0}) != std::future_status::ready) { poll_(); pause_(); }
        return fut.get();
    }

    const std::string& egressChannel() const noexcept { return egressChannel_; }
    /// Advertise the reply channel under `host` (the subscription stays bound on
    /// 127.0.0.1) — a client that sends a HOSTNAME in its ConnectRequest.
    void advertiseEgressHost(const std::string& host) {
        const auto colon = egressChannel_.rfind(':');
        egressChannel_ = "aeron:udp?endpoint=" + host + egressChannel_.substr(colon);
    }

    /// Aeron image session id of the last ConnectResponse / of the ack for `corr` (0 = none).
    int32_t connectImageSessionId() const noexcept { return connectImage_; }
    int32_t ackImageSessionId(int64_t corr) const {
        auto it = ackImages_.find(corr);
        return it == ackImages_.end() ? 0 : it->second;
    }

    /// `deadline`: a caller with its own deadline (TestCluster) bounds the send + answer waits by it instead
    /// of the per-wait 5 s cap (a dead gateway costs the remaining budget, not 2 x ~6 s).
    int64_t connect(const std::string& gatewayChannel, int32_t stream,
                    std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max()) {
        connect_.reset();                                  // each connect waits for ITS response
        pc::ConnectRequest req;
        req.correlationId  = ++corr_;
        req.egressStreamId = kEgressStream;
        req.clientVersion  = clientVersion_;
        req.egressChannel  = egressChannel_;
        req.sessionId      = connectHint_;   // authentication Task 11: a resume hint (0 = none)
        std::vector<uint8_t> buf(pc::MessageHeader::ENCODED_SIZE + pc::ConnectRequest::BODY_FIXED + 2 +
                                 req.egressChannel.size());
        const std::size_t len = req.encode(buf.data());
        if (!send(gatewayChannel, stream, buf.data(), len, deadline)) return -1;
        for (int i = 0; i < kMaxIters && !connect_ && std::chrono::steady_clock::now() < deadline; ++i) { poll_(); pause_(); }
        if (!connect_ || connect_->code != 0) return -1;
        sessions_.push_back({gatewayChannel, stream, connect_->assignedSessionId, std::chrono::steady_clock::now()});
        return connect_->assignedSessionId;
    }

    bool produce(const std::string& channel, int32_t stream, int64_t sessionId, int64_t corr,
                 int32_t topicId, int32_t partition, uint8_t acks, const std::vector<uint8_t>& batch) {
        pc::Produce msg;
        msg.sessionId = sessionId; msg.correlationId = corr;
        msg.topicId = topicId; msg.partition = partition; msg.partitionKnown = 1;
        msg.acks = acks; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        msg.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(msg.encodedSize());
        const std::size_t encodedLen = msg.encode(buf.data());
        return send(channel, stream, buf.data(), encodedLen);
    }

    /// An idempotent produce (F15 S5 e2e): producer id / epoch / sequence on the wire.
    bool produceIdempotent(const std::string& channel, int32_t stream, int64_t sessionId, int64_t corr,
                           int32_t topicId, int32_t partition, uint8_t acks, int64_t producerId,
                           int16_t producerEpoch, int32_t baseSequence, const std::vector<uint8_t>& batch) {
        pc::Produce msg;
        msg.sessionId = sessionId; msg.correlationId = corr;
        msg.topicId = topicId; msg.partition = partition; msg.partitionKnown = 1;
        msg.acks = acks; msg.producerId = producerId; msg.producerEpoch = producerEpoch;
        msg.baseSequence = baseSequence;
        msg.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(msg.encodedSize());
        const std::size_t encodedLen = msg.encode(buf.data());
        return send(channel, stream, buf.data(), encodedLen);
    }

    std::optional<pc::ProduceResponse> awaitProduceResponse(int64_t corr) {
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = acks_.find(corr); it != acks_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }

    /// The ack already received for `corr` (no polling; nullopt = none yet).
    std::optional<pc::ProduceResponse> ackFor(int64_t corr) const {
        if (auto it = acks_.find(corr); it != acks_.end()) return it->second;
        return std::nullopt;
    }

    /// Produce until an OK ack (retries NotLeader while leadership settles).  Returns the ack.
    std::optional<pc::ProduceResponse> produceUntilOk(const std::string& channel, int32_t stream,
                                                      int64_t sessionId, int32_t topicId,
                                                      int32_t partition, uint8_t acks,
                                                      const std::vector<uint8_t>& batch,
                                                      int maxAttempts = 100) {
        for (int a = 0; a < maxAttempts; ++a) {
            const int64_t c = ++corr_;
            if (!produce(channel, stream, sessionId, c, topicId, partition, acks, batch)) continue;
            auto ack = awaitProduceResponse(c);
            if (ack && ack->errorCode == 0) return ack;
            std::this_thread::sleep_for(std::chrono::milliseconds{20});   // leadership settling
        }
        return std::nullopt;
    }

    std::vector<PartitionInfo> metadata(const std::string& gatewayChannel, int32_t stream, int64_t sessionId) {
        pc::MetadataRequest req;
        req.sessionId = sessionId; req.correlationId = ++corr_;
        std::vector<uint8_t> buf(req.encodedSize());
        const std::size_t encodedLen = req.encode(buf.data());
        metadata_.reset();
        if (!send(gatewayChannel, stream, buf.data(), encodedLen)) return {};
        for (int i = 0; i < kMaxIters && !metadata_; ++i) { poll_(); pause_(); }
        return metadata_.value_or(std::vector<PartitionInfo>{});
    }

    /// Push subscription (read_uncommitted) from `fromOffset`; pushed Records and
    /// any ErrorResponse are captured (records(), errors()).
    bool subscribe(const std::string& gatewayChannel, int32_t stream, int64_t sessionId,
                   int32_t topicId, int32_t partition, int64_t fromOffset) {
        pc::Subscribe s;
        s.sessionId = sessionId; s.correlationId = ++corr_;
        s.topicId = topicId; s.partition = partition; s.partitionKnown = 1;
        s.fromOffset = fromOffset; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        const std::size_t len = s.encode(buf.data());
        return send(gatewayChannel, stream, buf.data(), len);
    }
    /// Push subscription with an explicit isolation (0 = read_uncommitted,
    /// 1 = read_committed); returns its correlationId (0 = not sent).
    int64_t subscribeIsolated(const std::string& gatewayChannel, int32_t stream, int64_t sessionId,
                              int32_t topicId, int32_t partition, int64_t fromOffset, uint8_t isolation) {
        pc::Subscribe s;
        s.sessionId = sessionId; s.correlationId = ++corr_;
        s.topicId = topicId; s.partition = partition; s.partitionKnown = 1;
        s.fromOffset = fromOffset; s.isolation = isolation;
        std::vector<uint8_t> buf(s.encodedSize());
        const std::size_t len = s.encode(buf.data());
        return send(gatewayChannel, stream, buf.data(), len) ? s.correlationId : 0;
    }
    /// E2 round 6: a RESUMED Subscribe claiming data lineage `claim` (8 LE bytes
    /// in key, partitionKnown = 1); returns its correlationId (0 = not sent).
    int64_t subscribeClaiming(const std::string& gatewayChannel, int32_t stream, int64_t sessionId, int32_t topicId,
                              int32_t partition, int64_t fromOffset, uint64_t claim) {
        uint8_t key[8];
        for (int i = 0; i < 8; ++i) key[i] = static_cast<uint8_t>(claim >> (8 * i));
        pc::Subscribe s;
        s.sessionId = sessionId; s.correlationId = ++corr_;
        s.topicId = topicId; s.partition = partition; s.partitionKnown = 1;
        s.fromOffset = fromOffset; s.isolation = 0;
        s.key = std::span<const uint8_t>(key, 8);
        std::vector<uint8_t> buf(s.encodedSize());
        const std::size_t len = s.encode(buf.data());
        return send(gatewayChannel, stream, buf.data(), len) ? s.correlationId : 0;
    }
    struct PushedRecords { int64_t subscriptionId; int64_t baseOffset; std::vector<uint8_t> bytes; uint64_t seq = 0; };
    struct ErrorSeen     { int64_t correlationId; int16_t code; std::string message; int64_t sessionId = 0; uint64_t seq = 0; };
    /// Poll the egress until `pred()` holds (bounded: kMaxIters × 100 µs).
    template <class Pred> bool pollUntil(Pred pred) {
        for (int i = 0; i < kMaxIters; ++i) { poll_(); if (pred()) return true; pause_(); }
        return pred();
    }
    /// Poll the egress until `pred()` holds or `budget` elapses — for a retry loop whose every attempt
    /// re-sends its stimulus (e.g. produce until a Latest subscription delivers): a short per-attempt window,
    /// never pollUntil's whole 5 s cap spent on an attempt that cannot succeed.
    template <class Pred> bool pollFor(std::chrono::milliseconds budget, Pred pred) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while (std::chrono::steady_clock::now() < deadline) { poll_(); if (pred()) return true; pause_(); }
        poll_();
        return pred();
    }
    const std::vector<PushedRecords>& records() const noexcept { return records_; }
    /// Forget the pushed records and error frames seen so far (a long-lived client of a many-cell matrix).
    void clearObserved() { records_.clear(); errors_.clear(); }
    const std::vector<ErrorSeen>&     errors()  const noexcept { return errors_; }

    using Clock = std::chrono::steady_clock;

    /// Await the ProduceResponse for `corr` until `deadline` (nullopt = no response).
    std::optional<pc::ProduceResponse> awaitProduceResponseUntil(int64_t corr, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = acks_.find(corr); it != acks_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }

    /// Send one DefineTopic and await ITS response until `deadline` (nullopt = no response).
    std::optional<pc::DefineTopicResponse> defineTopicUntil(const std::string& gatewayChannel, int32_t stream,
                                                            int64_t sessionId, std::string_view name,
                                                            uint32_t partitions, bool validate,
                                                            const std::vector<uint8_t>& descriptor,
                                                            Clock::time_point deadline,
                                                            uint8_t persistence = 0) {
        pc::DefineTopic m;
        m.sessionId = sessionId;
        m.topicName = name; m.partitionCount = partitions; m.validate = validate;
        m.persistence = persistence;
        m.descriptorBlob = descriptor.data();
        m.descriptorLen  = static_cast<uint32_t>(descriptor.size());
        // Catalog history Task 9: a v8 DDL (the extension carries a requestId, stable
        // across the resends below) — a broker that is not (yet) the catalog leader
        // answers the retriable NotLeader, resent here within the deadline exactly
        // as the client does; a retry of a committed DDL gets its original result.
        m.hasExt = true;
        m.ext.clientName = "shard-test-client";
        m.ext.requestId  = nextRequestId_();
        while (Clock::now() < deadline) {
            m.correlationId = ++corr_;
            std::vector<uint8_t> buf(m.encodedSize());
            const std::size_t len = m.encode(buf.data());
            const int64_t c = m.correlationId;
            if (!send(gatewayChannel, stream, buf.data(), len)) return std::nullopt;
            std::optional<pc::DefineTopicResponse> got;
            while (!got && Clock::now() < deadline) {
                poll_();
                if (auto it = defines_.find(c); it != defines_.end()) got = it->second;
                else pause_();
            }
            if (!got || got->errorCode != static_cast<uint16_t>(pc::ErrorCode::NotLeader)) return got;
            pause_();
        }
        return std::nullopt;
    }

    int64_t nextCorrelationId() noexcept { return ++corr_; }
    /// The correlation id of the request this client sent last (the helpers that hide theirs).
    [[nodiscard]] int64_t lastCorrelationId() const noexcept { return corr_; }
    /// Authentication Task 17: how many non-Records frames arrived for `corr` (an exactly-one-answer check).
    [[nodiscard]] std::size_t framesFor(int64_t corr) const { auto it = frameCount_.find(corr); return it == frameCount_.end() ? 0 : it->second; }
    /// Issue #278: Records frames seen for a subscription id no SubscribeResponse{OK} to this client ever named (a
    /// subscription the broker registered although it refused it).
    [[nodiscard]] std::size_t strayRecords() const {
        std::size_t n = 0;
        for (const PushedRecords& p : records_) n += knowsSubscriptionId(p.subscriptionId) ? 0 : 1;
        return n;
    }
    /// Forget the last ConnectResponse, then wait for the next one (a machine that sent its own ConnectRequest through
    /// send()): nullopt if none by `deadline`.
    void forgetConnectResponse() noexcept { connect_.reset(); }
    std::optional<pc::ConnectResponse> awaitConnectResponse(Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            poll_();
            if (connect_) return connect_;
            pause_();
        }
        return connect_;
    }
    /// The sessionId of later ConnectRequests (the resume hint of a reconnecting client; 0 = a fresh connect).
    void setConnectHint(int64_t sessionId) noexcept { connectHint_ = sessionId; }
    /// Authentication Task 12: keep the raw bytes of every answer frame by correlation id (not Records), so a test can
    /// compare two answers byte for byte and decode response types this client has no helper for.  Off by default.
    void captureRawAnswers(bool on = true) noexcept { captureRaw_ = on; }
    std::optional<std::vector<uint8_t>> awaitRaw(int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = raw_.find(corr); it != raw_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// Authentication Task 11: the raw bytes of the AuthServerFirst (13) / AuthServerFinal (15) frames by correlation id.
    /// The first of the two to arrive for `corr` (a rate-limited or malformed AuthClientFirst is answered by a final
    /// straight away).  nullopt: none by `deadline`.
    struct AuthFrame { uint16_t templateId = 0; std::vector<uint8_t> bytes; };
    std::optional<AuthFrame> awaitAuthFrame(int64_t corr, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            poll_();
            if (auto a = authFrameFor_(corr)) return a;
            pause_();
        }
        return authFrameFor_(corr);
    }
    /// The AuthServerFinal (15) for `corr` specifically (after an AuthServerFirst was seen).
    std::optional<AuthFrame> awaitAuthFinal(int64_t corr, Clock::time_point deadline) {
        const auto key = std::make_pair(static_cast<uint16_t>(pc::TemplateId::AuthServerFinal), corr);
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = authFrames_.find(key); it != authFrames_.end()) return AuthFrame{key.first, it->second};
            pause_();
        }
        return std::nullopt;
    }
    /// A session-level Ping (a request the broker answers with a Pong, or UnknownSession for a dead session): the
    /// ErrorResponse code answered for it, nullopt if none by `deadline` (a Pong or silence).
    std::optional<int16_t> pingError(const std::string& ch, int32_t stream, int64_t sid, Clock::time_point deadline) {
        pc::Ping p; p.sessionId = sid; p.correlationId = ++corr_;
        uint8_t buf[64];
        const std::size_t len = p.encode(buf);
        if (!send(ch, stream, buf, len)) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            for (const ErrorSeen& e : errors_) if (e.correlationId == p.correlationId) return e.code;
            pause_();
        }
        return std::nullopt;
    }
    /// The last ConnectResponse this client decoded (authentication tests read authMode / authState from it).
    [[nodiscard]] const std::optional<pc::ConnectResponse>& lastConnectResponse() const noexcept { return connect_; }

    // ── E2 final fix wave (C1 multi-node): consumer-group frames ──────────────
    /// The ConnectRequest clientVersion of later connects (group frames need >= 3).
    void setClientVersion(int32_t v) noexcept { clientVersion_ = v; }
    struct JoinAnswer { int16_t errorCode = -1; std::string memberId; int32_t generation = -1; };
    /// `memberId` empty: a new member; else a re-join of that member.
    std::optional<JoinAnswer> joinGroup(const std::string& gw, int32_t stream, int64_t sid, std::string_view group,
                                        std::string_view memberId = {}, std::vector<int32_t> topicIds = {}) {
        pc::JoinGroup m;
        m.sessionId = sid; m.correlationId = ++corr_; m.sessionTimeoutMs = 30'000;
        m.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
        m.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::vector<uint8_t> buf(512);
        const std::size_t len = m.encode(buf.data(), std::span<const int32_t>(topicIds));
        const int64_t c = m.correlationId;
        if (!send(gw, stream, buf.data(), len)) return std::nullopt;
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = joins_.find(c); it != joins_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// Heartbeat of a member: its result code (OK once the group is Stable at
    /// `generation`; RebalanceInProgress while it rebalances).
    std::optional<int16_t> heartbeat(const std::string& gw, int32_t stream, int64_t sid, std::string_view group,
                                     const std::string& member, int32_t generation) {
        pc::Heartbeat m;
        m.sessionId = sid; m.correlationId = ++corr_; m.generationId = generation;
        m.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
        m.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(member.data()), member.size());
        std::vector<uint8_t> buf(512);
        const std::size_t len = m.encode(buf.data());
        const int64_t c = m.correlationId;
        if (!send(gw, stream, buf.data(), len)) return std::nullopt;
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = heartbeats_.find(c); it != heartbeats_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// OffsetCommit of one (topic, partition) entry: its result code (nullopt: no answer).
    std::optional<int16_t> offsetCommit(const std::string& gw, int32_t stream, int64_t sid, std::string_view group,
                                        const std::string& member, int32_t generation, int32_t topicId,
                                        int32_t partition, int64_t offset) {
        pc::OffsetCommit m;
        m.sessionId = sid; m.correlationId = ++corr_; m.generationId = generation;
        m.groupId  = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
        m.memberId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(member.data()), member.size());
        pc::OffsetCommitEntry e;
        e.topicId = topicId; e.partition = partition; e.offset = offset; e.metadata = {};
        std::array<pc::OffsetCommitEntry, 1> es{e};
        std::vector<uint8_t> buf(1024);
        const std::size_t len = m.encode(buf.data(), std::span<const pc::OffsetCommitEntry>(es));
        const int64_t c = m.correlationId;
        if (!send(gw, stream, buf.data(), len)) return std::nullopt;
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = commits_.find(c); it != commits_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// OffsetFetch of one entry: (offset, errorCode) (nullopt: no answer).
    std::optional<std::pair<int64_t, int16_t>> offsetFetch(const std::string& gw, int32_t stream, int64_t sid,
                                                           std::string_view group, int32_t topicId, int32_t partition) {
        pc::OffsetFetch m;
        m.sessionId = sid; m.correlationId = ++corr_;
        m.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
        pc::OffsetFetchEntry e;
        e.topicId = topicId; e.partition = partition;
        std::array<pc::OffsetFetchEntry, 1> es{e};
        std::vector<uint8_t> buf(512);
        const std::size_t len = m.encode(buf.data(), std::span<const pc::OffsetFetchEntry>(es));
        const int64_t c = m.correlationId;
        if (!send(gw, stream, buf.data(), len)) return std::nullopt;
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = fetches_.find(c); it != fetches_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// Invoker mode: one duty cycle of this client's driver + conductor WITHOUT reading the egress (a client that
    /// pumps its Aeron runtime — status messages flow — but does not consume its acks).  No-op when threaded.
    void pumpWithoutReading() { pump_(); }
    /// Catalog Task 12 fix round 0: a SLOW reader — one pass reading at most
    /// `fragments` egress fragments (keepalives still flow).
    void pollThrottled(int fragments) {
        pump_();
        keepalive_tick_();
        if (egress_) egress_->poll(assembler_.handler(), fragments);
    }

    /// DefineTopic (DDL) → the authoritative DefineTopicResponse (nullopt on timeout).
    std::optional<pc::DefineTopicResponse> defineTopic(const std::string& gatewayChannel, int32_t stream,
                                                       int64_t sessionId, std::string_view name,
                                                       uint32_t partitions, bool validate,
                                                       const std::vector<uint8_t>& descriptor) {
        pc::DefineTopic m;
        m.sessionId = sessionId; m.correlationId = ++corr_;
        m.topicName = name; m.partitionCount = partitions; m.validate = validate;
        m.descriptorBlob = descriptor.data();
        m.descriptorLen  = static_cast<uint32_t>(descriptor.size());
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        const int64_t c = m.correlationId;
        if (!send(gatewayChannel, stream, buf.data(), len)) return std::nullopt;
        for (int i = 0; i < kMaxIters * 4; ++i) {   // DDL commits through the schema log
            poll_();
            if (auto it = defines_.find(c); it != defines_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }

    // ── DDL conformance (Task 12): pipelined DDL — send now, await by corr later ──
    struct SchemaAnswer {
        int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0; int32_t partitionCount = 0;
        std::vector<uint8_t> blob;
        bool operator==(const SchemaAnswer&) const = default;
    };
    /// `rid` (Task 11): {0,0} = a fresh requestId; set = this DDL's id (a retry re-sends the SAME id).
    int64_t sendDefineTopic(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                            uint32_t partitions, const std::vector<uint8_t>& descriptor, uint8_t persistence = 0,
                            rune::protocol::RequestId rid = {}, int16_t replicationFactor = 0,
                            const rune::protocol::TopicConfigView& config = boundedTestTopicConfig(),
                            bool validate = false) {
        pc::DefineTopic m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.partitionCount = partitions;
        m.keyField = "id"; m.validate = validate; m.persistence = persistence;
        m.descriptorBlob = descriptor.data(); m.descriptorLen = static_cast<uint32_t>(descriptor.size());
        m.hasExt = true; m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        m.replicationFactor = replicationFactor; m.config = config;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    int64_t sendCreateView(const std::string& ch, int32_t stream, int64_t sid, std::string_view viewName,
                           std::string_view keyField, uint8_t persistence, const std::vector<uint8_t>& plan,
                           rune::protocol::RequestId rid = {}) {
        pc::CreateView m;
        m.sessionId = sid; m.correlationId = ++corr_; m.viewName = viewName; m.keyField = keyField;
        m.persistence = persistence; m.planBlob = plan.data(); m.planLen = static_cast<uint32_t>(plan.size());
        m.hasExt = true; m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    int64_t sendAlterTopic(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                           uint16_t expectedVersion, const std::vector<uint8_t>& ops,
                           rune::protocol::RequestId rid = {}) {
        pc::AlterTopic m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.expectedVersion = expectedVersion;
        m.opsBlob = ops.data(); m.opsLen = static_cast<uint32_t>(ops.size());
        m.hasExt = true; m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }

    // ── Catalog history Task 11: the v8 catalog DDL ops (74–77), answered by a
    //    DdlResponse (78) — or an ErrorResponse (Redirect / NotLeader / Internal)
    //    from a broker that is not the catalog leader. ──
    struct DdlAnswer {
        uint16_t errorCode = 0; uint64_t objectId = 0; uint32_t newVersion = 0; int32_t gateTopicId = -1;
        std::string detail;
    };
    int64_t sendDropTopic(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                          rune::protocol::RequestId rid = {}) {
        pc::DropTopic m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name;
        m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        return sendEncoded_(ch, stream, m);
    }
    int64_t sendAlterTopicConfig(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                                 uint32_t expectedVersion, const std::vector<pc::ConfigChangeWire>& changes,
                                 rune::protocol::RequestId rid = {}, std::string_view statement = {}) {
        std::vector<uint8_t> block(pc::configChangesEncodedSize(changes.size()));
        if (pc::putConfigChanges(block.data(), std::span<const pc::ConfigChangeWire>(changes)) != block.size())
            return 0;
        pc::AlterTopicConfig m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.expectedVersion = expectedVersion;
        m.changes = block;
        m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid); m.ext.statement = statement;
        return sendEncoded_(ch, stream, m);
    }
    int64_t sendDropView(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                         rune::protocol::RequestId rid = {}) {
        pc::DropView m;
        m.sessionId = sid; m.correlationId = ++corr_; m.viewName = name;
        m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        return sendEncoded_(ch, stream, m);
    }
    int64_t sendAlterView(const std::string& ch, int32_t stream, int64_t sid, std::string_view name,
                          std::string_view keyField, uint8_t persistence, uint32_t expectedVersion,
                          const std::vector<uint8_t>& plan, rune::protocol::RequestId rid = {}) {
        pc::AlterView m;
        m.sessionId = sid; m.correlationId = ++corr_; m.viewName = name; m.keyField = keyField;
        m.persistence = persistence; m.expectedVersion = expectedVersion;
        m.planBlob = plan.data(); m.planLen = static_cast<uint32_t>(plan.size());
        m.ext.clientName = "shard-test-client"; m.ext.requestId = ridOr_(rid);
        return sendEncoded_(ch, stream, m);
    }
    std::optional<DdlAnswer> awaitDdl(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(ddls_, corr, deadline);
    }
    /// Any answer to DDL `corr` — its own response type or an ErrorResponse —
    /// as (response template, code, message / detail).  nullopt: none by `deadline`.
    struct AnyAnswer { uint16_t responseTemplate = 0; int16_t code = 0; std::string message; };
    std::optional<AnyAnswer> awaitAnyDdlAnswer(int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto a = anyDdlAnswer_(corr)) return a;
            pause_();
        }
        return anyDdlAnswer_(corr);
    }
    /// How many answers of any kind arrived for `corr` (exactly-once checks).
    [[nodiscard]] std::size_t answersFor(int64_t corr) const {
        std::size_t n = (defines_.contains(corr) ? 1u : 0u) + (alters_.contains(corr) ? 1u : 0u) +
                        (views_.contains(corr) ? 1u : 0u) + (ddls_.contains(corr) ? 1u : 0u);
        for (const ErrorSeen& e : errors_) n += e.correlationId == corr ? 1u : 0u;
        return n;
    }
    int64_t sendGetTopicSchema(const std::string& ch, int32_t stream, int64_t sid, std::string_view name) {
        pc::GetTopicSchema m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicName = name; m.requestedFingerprint = 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    /// Catalog history Task 8 fix round 1: a GetViewRow point read; the answer's
    /// ViewRowResponse status by correlation id (awaitViewRow).
    int64_t sendGetViewRow(const std::string& ch, int32_t stream, int64_t sid, int32_t viewTopicId, uint64_t key) {
        pc::GetViewRow m;
        m.sessionId = sid; m.correlationId = ++corr_; m.viewTopicId = viewTopicId; m.keyU64 = key;
        std::vector<uint8_t> buf(pc::GetViewRow::ENCODED_SIZE);
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    std::optional<uint16_t> awaitViewRow(int64_t corr, Clock::time_point deadline) {
        const auto r = awaitIn_(viewRows_, corr, deadline);
        return r ? std::optional<uint16_t>(r->status) : std::nullopt;
    }
    /// Task 22 fix round 1 (C1): the answer to a GetViewRow, whichever kind arrived: a ViewRowResponse (status +
    /// the row bytes) or an ErrorResponse (a Redirect to the source leader / NotLeader: errorCode + message).
    struct ViewRowAnswer {
        bool                 isError = false;
        uint16_t             status  = 0;      // ViewRowResponse status (isError == false)
        std::vector<uint8_t> row;              // its row bytes (status == OK)
        int16_t              errorCode = 0;    // ErrorResponse code (isError == true)
        std::string          message;          // ... and message (the Redirect's leader endpoint)
    };
    std::optional<ViewRowAnswer> awaitViewRowOrError(int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        auto answer = [&]() -> std::optional<ViewRowAnswer> {
            if (auto it = viewRows_.find(corr); it != viewRows_.end())
                return ViewRowAnswer{false, it->second.status, it->second.row, 0, {}};
            for (const ErrorSeen& e : errors_)
                if (e.correlationId == corr) return ViewRowAnswer{true, 0, {}, e.code, e.message};
            return std::nullopt;
        };
        while (Clock::now() < deadline) {
            poll_();
            if (auto a = answer()) return a;
            pause_();
        }
        return answer();
    }
    std::optional<pc::DefineTopicResponse> awaitDefine(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(defines_, corr, deadline);
    }
    std::optional<pc::CreateViewResponse> awaitCreateView(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(views_, corr, deadline);
    }
    std::optional<pc::AlterTopicResponse> awaitAlter(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(alters_, corr, deadline);
    }
    std::optional<SchemaAnswer> awaitSchema(int64_t corr, Clock::time_point deadline) {
        return awaitIn_(schemas_, corr, deadline);
    }
    /// Subscribe and return its correlation id (0 = not sent).
    int64_t sendSubscribe(const std::string& ch, int32_t stream, int64_t sid, int32_t topicId, int32_t partition) {
        pc::Subscribe m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicId = topicId; m.partition = partition;
        m.partitionKnown = 1; m.fromOffset = 0; m.isolation = 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    /// Catalog history Task 12: a Subscribe on `__catalog` (−6, partition 0) from
    /// `fromOffset`; returns its correlation id (0 = not sent).
    int64_t sendCatalogSubscribe(const std::string& ch, int32_t stream, int64_t sid, int64_t fromOffset) {
        pc::Subscribe m;
        m.sessionId = sid; m.correlationId = ++corr_; m.topicId = pc::kCatalogTopicId; m.partition = 0;
        m.partitionKnown = 1; m.fromOffset = fromOffset; m.isolation = 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    /// Issue #188: whether any SubscribeResponse OK this client received carried `subId` (a Record of any other id is a
    /// subscription the client was never told exists).
    bool knowsSubscriptionId(int64_t subId) const {
        for (const auto& [corr, id] : subscribeIds_)
            if (id == subId) if (const auto c = subscribes_.find(corr); c != subscribes_.end() && c->second == 0) return true;
        return false;
    }
    /// The subscriptionId a SubscribeResponse on `corr` carried (nullopt: none seen yet).
    std::optional<int64_t> subscriptionIdOf(int64_t corr) const {
        if (auto it = subscribeIds_.find(corr); it != subscribeIds_.end()) return it->second;
        return std::nullopt;
    }
    /// The answer to a subscribe: its SubscribeResponse errorCode, or the code of an
    /// ErrorResponse on that correlation id (NotLeader / Redirect paths).
    std::optional<int16_t> awaitSubscribeCode(int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = subscribes_.find(corr); it != subscribes_.end()) return it->second;
            for (const ErrorSeen& e : errors_) if (e.correlationId == corr) return e.code;
            pause_();
        }
        return std::nullopt;
    }

    /// Re-check I5: a plain (read_uncommitted) Subscribe that honours the protocol's answer — the gateway answers a
    /// Subscribe it cannot bind yet with the RETRIABLE NotLeader (e.g. a transient partition whose lineage this
    /// broker has not learned yet, FU1); a client re-sends it.  Re-sends on NotLeader until bound (SubscribeResponse
    /// OK) or `deadline`; any other answer is an explicit failure (false).  `onRetry` (optional) runs before each
    /// re-send.  Never fire-and-forget on a gated partition: an unbound Subscribe delivers nothing, by contract.
    template <class OnRetry = void (*)()>
    bool subscribeUntilBound(const std::string& gatewayChannel, int32_t stream, int64_t sessionId, int32_t topicId,
                             int32_t partition, int64_t fromOffset, Clock::time_point deadline,
                             OnRetry onRetry = [] {}) {
        constexpr int16_t kNotLeader = static_cast<int16_t>(pc::ErrorCode::NotLeader);
        while (Clock::now() < deadline) {
            const int64_t corr = subscribeIsolated(gatewayChannel, stream, sessionId, topicId, partition, fromOffset, 0);
            const auto code = awaitSubscribeCode(corr, deadline);
            if (!code) return false;              // no answer by the deadline
            if (*code == 0) return true;          // bound
            if (*code != kNotLeader) return false;
            onRetry();
            for (int i = 0; i < 500 && Clock::now() < deadline; ++i) { poll_(); pause_(); }   // ~50 ms retry pacing
        }
        return false;
    }

    bool disconnect(const std::string& gatewayChannel, int32_t stream, int64_t sessionId) {
        pc::Disconnect d; d.sessionId = sessionId; d.correlationId = ++corr_;
        std::vector<uint8_t> buf(pc::Disconnect::ENCODED_SIZE);
        const std::size_t len = d.encode(buf.data());
        return send(gatewayChannel, stream, buf.data(), len);
    }

    /// Offer frames make(i, out) one at a time WITHOUT reading the egress (a
    /// client that has stopped reading its acks).  Stops at maxFrames, or when
    /// an offer is refused AND paused() confirms the broker paused this client
    /// (a state predicate — e.g. the gateway's pause counter — never elapsed
    /// time; a refusal alone is just transient back-pressure: offer again).
    /// Returns the frames accepted.
    template <class Make, class Paused>
    int64_t offerUntilPaused(const std::string& channel, int32_t stream, Make&& make, int64_t maxFrames,
                             Paused&& paused) {
        auto& pub = pubs_[{channel, stream}];
        if (!pub) {
            const int64_t reg = aeron_->addPublication(channel, stream);
            for (int i = 0; i < kMaxIters && !pub; ++i) { pub = aeron_->findPublication(reg); pause_(); }
            if (!pub) return 0;
        }
        return offerLoop_(*pub, std::forward<Make>(make), maxFrames, std::forward<Paused>(paused));
    }
    /// D8d: as offerUntilPaused, on this client's own EXCLUSIVE publication #`slot`
    /// to (channel, stream) — a separate image at the broker per slot (several
    /// stalled "clients" from one process).
    template <class Make, class Paused>
    int64_t offerUntilPausedExclusive(int slot, const std::string& channel, int32_t stream, Make&& make,
                                      int64_t maxFrames, Paused&& paused) {
        auto& pub = exclusivePubs_[slot];
        if (!pub) {
            const int64_t reg = aeron_->addExclusivePublication(channel, stream);
            for (int i = 0; i < kMaxIters && !(pub && pub->isConnected()); ++i) {
                if (!pub) pub = aeron_->findExclusivePublication(reg);
                pause_();
            }
            if (!pub) return 0;
        }
        return offerLoop_(*pub, std::forward<Make>(make), maxFrames, std::forward<Paused>(paused));
    }
    template <class Pub, class Make, class Paused>
    int64_t offerLoop_(Pub& p, Make&& make, int64_t maxFrames, Paused&& paused) {
        auto* pub = &p;
        std::vector<uint8_t> f;
        int64_t accepted = 0;
        // Hard cap: refused for this long without paused() → stall; returns maxFrames (the caller's "never
        // paused" assertion fails) instead of hanging the test.
        const auto stallBudget = std::chrono::seconds{30};
        auto refusedSince = std::chrono::steady_clock::time_point{};
        while (accepted < maxFrames) {
            make(accepted, f);
            aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
            const int64_t r = pub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size()));
            if (r > 0) { ++accepted; refusedSince = {}; continue; }
            if (paused()) break;
            const auto now = std::chrono::steady_clock::now();
            if (refusedSince == std::chrono::steady_clock::time_point{}) refusedSince = now;
            else if (now - refusedSince > stallBudget) { lastStallOffer_ = r; lastStallAccepted_ = accepted; return maxFrames; }
            pump_();
        }
        return accepted;
    }

    bool send(const std::string& channel, int32_t stream, const uint8_t* buf, std::size_t len,
              std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max()) {
        // Catalog history Task 9 (Task 4 M3): an encoder that returns 0 refused the
        // frame (a field over its wire width) — never send a zero-length frame: an
        // explicit failure for the caller (a DDL helper answers 0 / nullopt).
        if (len == 0) return false;
        auto& pub = pubs_[{channel, stream}];
        if (!pub) {
            const int64_t reg = aeron_->addPublication(channel, stream);
            for (int i = 0; i < kMaxIters && !pub; ++i) { pub = aeron_->findPublication(reg); pause_(); }
            if (!pub) return false;
        }
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), static_cast<aeron::util::index_t>(len));
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            if (pub->offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) return true;
            pause_();
        }
        return false;
    }

private:
    rune::protocol::RequestId ridOr_(rune::protocol::RequestId rid) noexcept {
        return rid.none() ? nextRequestId_() : rid;
    }
    template <class Msg>
    int64_t sendEncoded_(const std::string& ch, int32_t stream, const Msg& m) {
        if (!m.fits()) return 0;
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return len > 0 && send(ch, stream, buf.data(), len) ? m.correlationId : 0;
    }
    std::optional<AuthFrame> authFrameFor_(int64_t corr) const {
        for (const uint16_t t : {static_cast<uint16_t>(pc::TemplateId::AuthServerFirst), static_cast<uint16_t>(pc::TemplateId::AuthServerFinal)})
            if (auto it = authFrames_.find({t, corr}); it != authFrames_.end()) return AuthFrame{t, it->second};
        return std::nullopt;
    }
    std::optional<AnyAnswer> anyDdlAnswer_(int64_t corr) const {
        namespace T = rune::protocol::client;
        if (auto it = defines_.find(corr); it != defines_.end())
            return AnyAnswer{static_cast<uint16_t>(T::TemplateId::DefineTopicResponse),
                             static_cast<int16_t>(it->second.errorCode), {}};
        if (auto it = alters_.find(corr); it != alters_.end())
            return AnyAnswer{static_cast<uint16_t>(T::TemplateId::AlterTopicResponse),
                             static_cast<int16_t>(it->second.errorCode), {}};
        if (auto it = views_.find(corr); it != views_.end())
            return AnyAnswer{static_cast<uint16_t>(T::TemplateId::CreateViewResponse),
                             static_cast<int16_t>(it->second.errorCode), {}};
        if (auto it = ddls_.find(corr); it != ddls_.end())
            return AnyAnswer{static_cast<uint16_t>(T::TemplateId::DdlResponse),
                             static_cast<int16_t>(it->second.errorCode), it->second.detail};
        for (const ErrorSeen& e : errors_)
            if (e.correlationId == corr)
                return AnyAnswer{static_cast<uint16_t>(T::TemplateId::ErrorResponse), e.code, e.message};
        return std::nullopt;
    }
    /// A v8 DDL's 128-bit id: this client's instance tag + a counter (F14).
    // Sanitizer triage (2026-10-14, DdlConformance T5/T7 under TSan): the id was
    // keyed on `this` — a client re-created at the SAME address (the next block of
    // a restart test) restarted the counter and re-sent a previous client's
    // requestIds, which the broker's durable DDL dedupe (Task 11) rightly answered
    // with the ORIGINAL outcome (a "define t_disk" answered OK after a restart).
    // Which ids collided depended on how many NotLeader probes each boot took —
    // timing — so it surfaced only under TSan.  A fresh random nonce per instance.
    rune::protocol::RequestId nextRequestId_() noexcept {
        return {0x5C1E'0000'0000'0000ull | requestNonce_, ++requestCounter_};
    }
    uint64_t requestNonce_   = (static_cast<uint64_t>(std::random_device{}()) << 16 ^ std::random_device{}()) &
                               0x0000'FFFF'FFFF'FFFFull;
    uint64_t requestCounter_ = 0;
    /// `rcvWindowBytes` (catalog Task 12 fix round 1): the client's receive window
    /// (a small one also pins the MTU to 1408 so window >= MTU).
    static EmbeddedDriverConfig cfgFor(const std::filesystem::path& d, int32_t termBufferBytes,
                                       bool legacyTransport = false, int32_t rcvWindowBytes = 128 * 1024) {
        EmbeddedDriverConfig c; c.dir = d.string(); c.termBufferBytes = termBufferBytes;
        if (legacyTransport) {
            c.mtuBytes = 1408; c.socketBufferBytes = 2 * 1024 * 1024; c.termBufferSparse = true;
            // Pre-2026-10-02 = stock Aeron UDP: no GSO/GRO bindings, Aeron's 2 msgs/send.  GSO's 16-datagram
            // bursts deepen the pipeline exactly like the 64 KiB MTU (KNOWN ISSUE in ShardModeTest).
            c.udpGso = false;
        }
        // Pinned (test client, orthogonal to the broker defaults): the pre-2026-10-02
        // 128 KiB receive window, so back-pressure tests (a client that stops
        // reading its acks) hit it at the same depth whatever the host derives.
        c.rcvWindowBytes = rcvWindowBytes;
        if (rcvWindowBytes < 64 * 1024) c.mtuBytes = 1408;
        return c;
    }
    template <class M>
    std::optional<typename M::mapped_type> awaitIn_(M& m, int64_t corr, Clock::time_point deadline) {
        if (corr == 0) return std::nullopt;
        while (Clock::now() < deadline) {
            poll_();
            if (auto it = m.find(corr); it != m.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }
    /// Invoker mode: run the client's driver + conductor duty cycle (no-op when threaded).
    void pump_() {
        if (!invoker_) return;
        driver_.doWork();
        if (aeron_) aeron_->conductorAgentInvoker().invoke();
    }
    void pause_() {
        pump_();
        keepalive_tick_();
        std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    /// keepSessionsAlive: one Keepalive per connected session every 1 s (the
    /// real client's interval) — a single non-blocking offer, never a wait.
    void keepalive_tick_() {
        if (!keepalive_ || sessions_.empty()) return;
        const auto now = std::chrono::steady_clock::now();
        if (now < nextKeepalive_) return;
        nextKeepalive_ = now + std::chrono::seconds{1};
        for (auto& cs : sessions_) {
            auto it = pubs_.find({cs.channel, cs.stream});
            if (it == pubs_.end() || !it->second) continue;
            pc::Keepalive k; k.sessionId = cs.sessionId; k.correlationId = ++keepaliveCorr_;   // never corr_: lastCorrelationId() must stay the last REQUEST
            uint8_t buf[pc::Keepalive::ENCODED_SIZE];
            const std::size_t len = k.encode(buf);
            aeron::concurrent::AtomicBuffer ab(buf, static_cast<aeron::util::index_t>(len));
            if (it->second->offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) cs.lastSent = now;   // F-A
        }
    }

    void poll_() {
        if (!egress_) return;
        egress_->poll(assembler_.handler(), 64);
    }
    void onFrame_(const uint8_t* p, std::size_t n, int32_t imageSessionId) {
        auto hdr = pc::MessageHeader::decode(p, n);
        if (!hdr) return;
        if (hdr->correlationId != 0 && hdr->templateId != static_cast<uint16_t>(pc::TemplateId::Records)) ++frameCount_[hdr->correlationId];
        if (captureRaw_ && hdr->correlationId != 0 && hdr->templateId != static_cast<uint16_t>(pc::TemplateId::Records))
            raw_[hdr->correlationId] = std::vector<uint8_t>(p, p + n);   // authentication Task 12: byte-level answer parity
        switch (static_cast<pc::TemplateId>(hdr->templateId)) {
            case pc::TemplateId::ConnectResponse:
                if (auto r = pc::ConnectResponse::decode(p, n)) { connect_ = *r; connectImage_ = imageSessionId; }
                break;
            case pc::TemplateId::ProduceResponse:
                if (auto r = pc::ProduceResponse::decode(p, n)) {
                    acks_[r->correlationId] = *r;
                    ackImages_[r->correlationId] = imageSessionId;
                }
                break;
            case pc::TemplateId::DefineTopicResponse:
                if (auto r = pc::DefineTopicResponse::decode(p, n)) defines_[r->correlationId] = *r;
                break;
            case pc::TemplateId::SubscribeResponse:
                if (auto r = pc::SubscribeResponse::decode(p, n)) {
                    subscribes_[r->correlationId]   = r->errorCode;
                    subscribeIds_[r->correlationId] = r->subscriptionId;   // catalog Task 12
                }
                break;
            case pc::TemplateId::CreateViewResponse:
                if (auto r = pc::CreateViewResponse::decode(p, n)) views_[r->correlationId] = *r;
                break;
            case pc::TemplateId::ViewRowResponse:
                if (auto r = pc::ViewRowResponse::decode(p, n))
                    viewRows_[r->correlationId] = ViewRowSeen{
                        r->status, r->rowBytes != nullptr ? std::vector<uint8_t>(r->rowBytes, r->rowBytes + r->rowBytesLen)
                                                          : std::vector<uint8_t>{}};
                break;
            case pc::TemplateId::AlterTopicResponse:
                if (auto r = pc::AlterTopicResponse::decode(p, n)) alters_[r->correlationId] = *r;
                break;
            case pc::TemplateId::TopicSchemaResponse:
                if (auto r = pc::TopicSchemaResponse::decode(p, n))
                    schemas_[r->correlationId] = SchemaAnswer{
                        r->topicId, r->version, r->errorCode, r->partitionCount,
                        std::vector<uint8_t>(r->descriptorBlob, r->descriptorBlob + r->descriptorLen)};
                break;
            case pc::TemplateId::MetadataResponse:
                if (auto r = pc::MetadataResponse::decode(p, n)) {
                    std::vector<PartitionInfo> v;
                    for (const auto& e : r->partitions)
                        v.push_back({e.topicId, e.partition, e.leaderId, e.ingressStreamId,
                                     std::string(e.ingressEndpoint), e.egressControlPort});
                    metadata_ = std::move(v);
                }
                break;
            case pc::TemplateId::Records:
                if (auto r = pc::Records::decode(p, n))
                    records_.push_back({r->subscriptionId, r->baseOffset,
                                        {r->recordBatch.begin(), r->recordBatch.end()}, ++arrivalSeq_});
                break;
            case pc::TemplateId::JoinGroupResponse:
                if (auto r = pc::JoinGroupResponse::decode(p, n))
                    joins_[r->correlationId] = JoinAnswer{
                        r->errorCode, std::string(reinterpret_cast<const char*>(r->memberId.data()), r->memberId.size()),
                        r->generationId};
                break;
            case pc::TemplateId::HeartbeatResponse:
                if (auto r = pc::HeartbeatResponse::decode(p, n)) heartbeats_[r->correlationId] = r->errorCode;
                break;
            case pc::TemplateId::OffsetCommitResponse:
                if (auto r = pc::OffsetCommitResponse::decode(p, n))
                    commits_[r->correlationId] = r->results.empty() ? int16_t{-1} : r->results[0].errorCode;
                break;
            case pc::TemplateId::OffsetFetchResponse:
                if (auto r = pc::OffsetFetchResponse::decode(p, n))
                    if (!r->results.empty())
                        fetches_[r->correlationId] = {r->results[0].offset, r->results[0].errorCode};
                break;
            case pc::TemplateId::ErrorResponse:
                if (auto e = pc::ErrorResponse::decode(p, n)) {
                    errors_.push_back({e->correlationId, e->code, std::string(e->message), e->sessionId, ++arrivalSeq_});
                    // F-A: the broker said this session is gone (the reap's notice / an UnknownSession answer).
                    if (e->code == static_cast<int16_t>(pc::ErrorCode::SessionExpired) ||
                        e->code == static_cast<int16_t>(pc::ErrorCode::UnknownSession))
                        expired_.insert(e->sessionId);
                }
                break;
            case pc::TemplateId::AuthServerFirst:
            case pc::TemplateId::AuthServerFinal:
                authFrames_[{hdr->templateId, hdr->correlationId}] = std::vector<uint8_t>(p, p + n);
                break;
            case pc::TemplateId::DdlResponse:
                if (auto r = pc::DdlResponse::decode(p, n))
                    ddls_[r->correlationId] = DdlAnswer{r->errorCode, r->objectId, r->newVersion, r->gateTopicId,
                                                        std::string(r->detail)};
                break;
            default: break;
        }
    }

public:
    int64_t lastStallOffer_ = 0, lastStallAccepted_ = 0;   ///< offerUntilPaused stall diagnostics
private:
    EmbeddedDriver                                   driver_;
    bool                                             invoker_ = false;
    std::shared_ptr<aeron::Aeron>                    aeron_;
    std::string                                      egressChannel_;
    bool                                             keepalive_ = false;   // keepSessionsAlive
    /// lastSent (F-A): connect, or the last Keepalive whose offer was accepted - THIS client's own send gap.
    struct ConnectedSession { std::string channel; int32_t stream; int64_t sessionId;
                              std::chrono::steady_clock::time_point lastSent; };
    std::vector<ConnectedSession>                    sessions_;
    std::chrono::steady_clock::time_point            nextKeepalive_{};
    std::shared_ptr<aeron::Subscription>             egress_;
    std::map<std::pair<std::string, int32_t>, std::shared_ptr<aeron::Publication>> pubs_;
    std::map<int, std::shared_ptr<aeron::ExclusivePublication>> exclusivePubs_;   // D8d: offerUntilPausedExclusive
    aeron::FragmentAssembler assembler_{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                                               aeron::util::index_t len, aeron::Header& h) {
        onFrame_(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len), h.sessionId());
    }};
    // Aeron image (publication) session id each response arrived on — tells the
    // runtime's session publication from a worker's own (Task 13 worker acks).
    int32_t                                         connectImage_ = 0;
    std::map<int64_t, int32_t>                      ackImages_;
    std::optional<pc::ConnectResponse>              connect_;
    std::map<int64_t, pc::ProduceResponse>          acks_;
    std::map<int64_t, pc::DefineTopicResponse>      defines_;
    std::map<int64_t, pc::CreateViewResponse>       views_;
    std::map<int64_t, pc::AlterTopicResponse>       alters_;
    bool                                            captureRaw_ = false;
    std::map<int64_t, std::size_t>                  frameCount_;   // every non-Records frame by corr
    std::map<int64_t, std::vector<uint8_t>>         raw_;       // captureRawAnswers: every answer frame by corr
    std::map<int64_t, DdlAnswer>                    ddls_;      // DdlResponse (78) by corr (Task 11)
    std::map<std::pair<uint16_t, int64_t>, std::vector<uint8_t>> authFrames_;   // AuthServerFirst / Final raw bytes (auth Task 11)
    struct ViewRowSeen { uint16_t status = 0; std::vector<uint8_t> row; };
    std::map<int64_t, ViewRowSeen>                  viewRows_;   // ViewRowResponse (status + row bytes) by corr
    std::map<int64_t, SchemaAnswer>                 schemas_;
    std::map<int64_t, int16_t>                      subscribes_;   // SubscribeResponse errorCode by corr
    std::map<int64_t, int64_t>                      subscribeIds_; // SubscribeResponse subscriptionId by corr
    int32_t                                         clientVersion_ = 1;
    int64_t                                         connectHint_ = 0;
    std::map<int64_t, JoinAnswer>                   joins_;
    std::map<int64_t, int16_t>                      heartbeats_;
    std::map<int64_t, int16_t>                      commits_;
    std::map<int64_t, std::pair<int64_t, int16_t>>  fetches_;
    std::optional<std::vector<PartitionInfo>>       metadata_;
    std::vector<PushedRecords>                      records_;
    std::vector<ErrorSeen>                          errors_;
    std::set<int64_t>                               expired_;   // F-A: sessions the broker declared expired
    int64_t                                         corr_ = 0;
    int64_t                                         keepaliveCorr_ = int64_t{1} << 40;   // the Keepalive ticks' own correlation ids
    uint64_t                                        arrivalSeq_ = 0;   // one arrival order for records_ and errors_
};

} // namespace rune::node::test
