// ResponseEgressWorkerRegressionTest.cpp — NAT-safe egress Task 6 (spec D2, D6): in SHARD mode a v16 client's produce
// acks and subscription Records leave the WORKER on a per-(session, worker) Aeron response publication, correlated
// with the client's own ingress image to that worker.  The worker never learns a reply address: it learns an image,
// and only a client that proves the gateway-issued bind token (WorkerBind, the first frame on that ingress
// publication) can attach a session to an image.
//
// A REAL single-node BrokerNode (shard mode, fused placement, explicit collision-free ports) driven by raw-Aeron
// clients written here (own embedded driver per client, so each has its own source port).  Waits are bounded by an
// iteration cap AND a steady_clock deadline; the only real-time waits are on the real driver / broker.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/AuthMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/replication/PartitionReplica.h>
#include "support/AuthRig.h"          // AuthRig, grants::
#include "support/DdlHarness.h"       // charBlob
#include "support/ScramTestClient.h"  // scramAnswer
#include "support/ShardTestClient.h"  // boundedTestTopicConfig

#include <Aeron.h>
#include <ChannelUriStringBuilder.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;
using namespace std::chrono_literals;

namespace {

constexpr int32_t kClientStream = 110;
constexpr int32_t kRespStream   = 4343;   // the raw client's egressStreamId: the stream the worker answers on
constexpr int32_t kMdcStream    = 112;    // workerIngressBaseStreamId (111) + workerCount (1) + worker 0
const std::string kUnroutable   = "aeron:udp?endpoint=192.0.2.1:40200";   // TEST-NET-1: never reachable

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_respwk_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Ports {
    uint16_t cons, client, repl, ib, rb, eg, gwResp, wResp;
    int32_t  clientStream = kClientStream;   ///< the client endpoint's stream id (an AuthBroker uses 1)
    static Ports discover() {
        namespace t = rune::aeronutil::testing;
        Ports p{};
        p.ib = t::allocFreeUdpPortRange(1); p.rb = t::allocFreeUdpPortRange(1); p.eg = t::allocFreeUdpPortRange(1);
        p.wResp = t::allocFreeUdpPortRange(1);
        p.cons = t::allocFreeUdpPort(); p.client = t::allocFreeUdpPort(); p.repl = t::allocFreeUdpPort();
        p.gwResp = t::allocFreeUdpPort();
        return p;
    }
    [[nodiscard]] std::string gateway() const { return "127.0.0.1:" + std::to_string(client); }
    [[nodiscard]] std::string gwCtl() const { return "127.0.0.1:" + std::to_string(gwResp); }
    [[nodiscard]] std::string workerCtl() const { return "127.0.0.1:" + std::to_string(wResp); }
    [[nodiscard]] std::string workerIngress() const { return "127.0.0.1:" + std::to_string(ib); }
};

/// Applies the port layout `p` to `cfg` (single node, one worker, response ports on node + self peer).
void applyPorts(NodeConfig& cfg, PeerConfig& peer, const Ports& p) {
    cfg.workerIngressBasePort = p.ib;
    cfg.workerReplBasePort    = p.rb;
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg;
    cfg.workerIngressBaseStreamId = 111;
    cfg.gatewayResponseControlPort = p.gwResp;
    cfg.responseControlBasePort    = p.wResp;
    peer.gatewayResponseControlPort = p.gwResp;
    peer.responseControlBasePort    = p.wResp;
    peer.id = 0; peer.host = "127.0.0.1";
    peer.consensusPort = p.cons; peer.clientPort = p.client; peer.replicationPort = p.repl;
    peer.replBasePort = p.rb; peer.ingressBasePort = p.ib; peer.workerCount = 1;
    peer.egressControlBasePort = p.eg;
}

NodeConfig makeConfig(const fs::path& dir, const Ports& p) {
    NodeConfig cfg;
    cfg.nodeId = 0;
    cfg.dataDir = dir.string();
    cfg.aeronDir = (dir / "aeron").string();
    cfg.consensusStreamId = 90; cfg.replicationStreamId = 100; cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = 1;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Fused;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.gatewayMinIsr = 1;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;
    PeerConfig peer;
    applyPorts(cfg, peer, p);
    cfg.peers.push_back(peer);
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=" + p.gateway();
    cfg.clientEndpoint.streamId = kClientStream;
    return cfg;
}

struct Frame {
    uint16_t templateId = 0;
    int64_t sessionId = 0, correlationId = 0;
    std::vector<uint8_t> bytes;
};

constexpr uint16_t kProduceResp = static_cast<uint16_t>(pc::TemplateId::ProduceResponse);
constexpr uint16_t kRecords     = static_cast<uint16_t>(pc::TemplateId::Records);
constexpr uint16_t kSubResp     = static_cast<uint16_t>(pc::TemplateId::SubscribeResponse);
constexpr uint16_t kDefineResp  = static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse);
constexpr uint16_t kConnectResp = static_cast<uint16_t>(pc::TemplateId::ConnectResponse);

/// Resolve an async registration exactly once (Aeron 1.51: a second find with the same id throws).
template <class Add, class Find> auto resolveOnce(Add&& add, Find&& find) {
    const int64_t id = add();
    decltype(find(id)) r{};
    for (int i = 0; i < 100'000 && !r; ++i) {
        r = find(id);
        if (!r) std::this_thread::sleep_for(100us);
    }
    return r;
}

/// A raw-Aeron client with its own embedded driver.
///   Response : gateway response sub + request pub (response-correlation-id); worker response sub + ingress pub on
///              openWorker().  Frames of the gateway sub land in gw(), frames of the worker sub in wk().
///   Declared : a plain local subscription (the declared path, an OLD client) + plain publications; EVERY frame lands
///              in gw().
///   WorkerOnly: no gateway at all (an attacker that only talks to a worker).
class WClient {
public:
    enum class Mode { Response, Declared, WorkerOnly };
    WClient(const fs::path& dir, const Ports& p, Mode mode, int32_t respStream = kRespStream)
        : driver_(cfgAt(dir), DriverMode::Threaded), p_(p), mode_(mode), respStream_(respStream) {
        aeron::Context ctx;
        ctx.aeronDir(dir.string());
        aeron_ = aeron::Aeron::connect(ctx);
        if (mode_ == Mode::WorkerOnly) return;
        if (mode_ == Mode::Response) {
            gwSub_ = resolveOnce([&] { return aeron_->addSubscription(responseSubChannel(p.gwCtl()), respStream_); },
                                 [&](int64_t id) { return aeron_->findSubscription(id); });
            const std::string req = aeron::ChannelUriStringBuilder().media("udp").endpoint(p.gateway())
                                        .responseCorrelationId(gwSub_ ? gwSub_->registrationId() : 0).build();
            gwPub_ = resolveOnce([&] { return aeron_->addPublication(req, p.clientStream); },
                                 [&](int64_t id) { return aeron_->findPublication(id); });
        } else {
            localChannel_ = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
            gwSub_ = resolveOnce([&] { return aeron_->addSubscription(localChannel_, respStream_); },
                                 [&](int64_t id) { return aeron_->findSubscription(id); });
            gwPub_ = resolveOnce([&] { return aeron_->addPublication("aeron:udp?endpoint=" + p.gateway(), p.clientStream); },
                                 [&](int64_t id) { return aeron_->findPublication(id); });
        }
    }
    ~WClient() { wPub_.reset(); wSub_.reset(); gwPub_.reset(); gwSub_.reset(); aeron_.reset(); }

    [[nodiscard]] bool ready() const { return mode_ == Mode::WorkerOnly || (gwSub_ && gwPub_); }
    [[nodiscard]] const std::string& localChannel() const { return localChannel_; }

    /// The ingress publication to worker 0 (+ in Response mode its response subscription, same correlation).
    bool openWorker() {
        if (wPub_) return true;
        std::string ch = "aeron:udp?endpoint=" + p_.workerIngress();
        if (mode_ != Mode::Declared) {
            wSub_ = resolveOnce([&] { return aeron_->addSubscription(responseSubChannel(p_.workerCtl()), respStream_); },
                                [&](int64_t id) { return aeron_->findSubscription(id); });
            if (!wSub_) return false;
            ch = aeron::ChannelUriStringBuilder().media("udp").endpoint(p_.workerIngress())
                     .responseCorrelationId(wSub_->registrationId()).build();
        }
        wPub_ = resolveOnce([&] { return aeron_->addPublication(ch, p_.clientStream); },
                            [&](int64_t id) { return aeron_->findPublication(id); });
        return wPub_ != nullptr;
    }
    /// Close the ingress publication (the worker's image goes away).
    void closeWorkerIngress() { wPub_.reset(); }

    bool offerGw(const uint8_t* buf, std::size_t len) { return offer(gwPub_, buf, len); }
    bool offerWorker(const uint8_t* buf, std::size_t len) { return openWorker() && offer(wPub_, buf, len); }

    bool sendConnect(int64_t corr, int32_t version, bool responseCorrelated, const std::string& declared) {
        pc::ConnectRequest req;
        req.correlationId = corr; req.egressStreamId = respStream_; req.clientVersion = version;
        req.egressChannel = declared; req.responseCorrelated = responseCorrelated;
        std::vector<uint8_t> buf(req.encodedSize());
        return offerGw(buf.data(), req.encode(buf.data()));
    }
    std::optional<pc::ConnectResponse> connect(int64_t corr, int32_t version, bool responseCorrelated,
                                               const std::string& declared) {
        if (!sendConnect(corr, version, responseCorrelated, declared)) return std::nullopt;
        if (!waitGw([&] { return findGw(kConnectResp, corr) != nullptr; })) return std::nullopt;
        const Frame* f = findGw(kConnectResp, corr);
        return pc::ConnectResponse::decode(f->bytes.data(), f->bytes.size());
    }
    bool sendWorkerBind(int64_t sid, int64_t corr, const std::array<uint8_t, 16>& token) {
        pc::WorkerBind b; b.sessionId = sid; b.correlationId = corr; b.token = token;
        std::array<uint8_t, pc::WorkerBind::ENCODED_SIZE> buf{};
        return offerWorker(buf.data(), b.encode(buf.data()));
    }
    bool sendProduceWorker(int64_t sid, int64_t corr, int32_t topic, const std::vector<uint8_t>& batch, uint8_t acks = 1) {
        pc::Produce m;
        m.sessionId = sid; m.correlationId = corr; m.topicId = topic; m.partition = 0; m.partitionKnown = 1;
        m.acks = acks; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        return offerWorker(buf.data(), m.encode(buf.data()));
    }
    bool sendProduceGateway(int64_t sid, int64_t corr, int32_t topic, const std::vector<uint8_t>& batch, uint8_t acks = 1) {
        pc::Produce m;
        m.sessionId = sid; m.correlationId = corr; m.topicId = topic; m.partition = 0; m.partitionKnown = 1;
        m.acks = acks; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(m.encodedSize());
        return offerGw(buf.data(), m.encode(buf.data()));
    }
    bool sendSubscribe(int64_t sid, int64_t corr, int32_t topic, int64_t fromOffset = 0) {
        pc::Subscribe s;
        s.sessionId = sid; s.correlationId = corr; s.topicId = topic; s.partition = 0; s.partitionKnown = 1;
        s.fromOffset = fromOffset; s.isolation = 0;
        std::vector<uint8_t> buf(s.encodedSize());
        return offerGw(buf.data(), s.encode(buf.data()));
    }
    bool sendDefineTopic(int64_t sid, int64_t corr, std::string_view name, rune::protocol::RequestId rid) {
        const auto descriptor = charBlob();
        pc::DefineTopic m;
        m.sessionId = sid; m.correlationId = corr; m.topicName = name; m.partitionCount = 1;
        m.keyField = "id"; m.validate = false; m.persistence = 0;
        m.descriptorBlob = descriptor.data(); m.descriptorLen = static_cast<uint32_t>(descriptor.size());
        m.hasExt = true; m.ext.clientName = "raw-v16"; m.ext.requestId = rid;
        m.config = boundedTestTopicConfig();
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return len > 0 && offerGw(buf.data(), len);
    }

    void pollGw() { drain(gwSub_, gw_); }
    void pollWk() { drain(wSub_, wk_); }
    template <class Pred> bool waitGw(Pred pred, std::chrono::milliseconds budget = 10s) { return wait(pred, budget, true); }
    template <class Pred> bool waitWk(Pred pred, std::chrono::milliseconds budget = 10s) { return wait(pred, budget, false); }
    /// Keep draining both subscriptions for `ms` (to prove nothing more arrives).
    void drainFor(std::chrono::milliseconds ms) {
        const auto deadline = std::chrono::steady_clock::now() + ms;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            pollGw(); pollWk();
            std::this_thread::sleep_for(100us);
        }
    }
    [[nodiscard]] const Frame* findGw(uint16_t tid, int64_t corr) const {
        for (const auto& f : gw_) if (f.templateId == tid && f.correlationId == corr) return &f;
        return nullptr;
    }
    [[nodiscard]] const std::vector<Frame>& gw() const { return gw_; }
    [[nodiscard]] const std::vector<Frame>& wk() const { return wk_; }
    [[nodiscard]] int count(const std::vector<Frame>& v, uint16_t tid) const {
        int n = 0; for (const auto& f : v) n += f.templateId == tid ? 1 : 0; return n;
    }
    [[nodiscard]] int32_t workerImageSessionId() const { return wPub_ ? wPub_->sessionId() : 0; }

    /// An extra plain subscription in this client's Aeron (the MDC listener).
    std::shared_ptr<aeron::Subscription> addSubscription(const std::string& ch, int32_t stream) {
        return resolveOnce([&] { return aeron_->addSubscription(ch, stream); },
                           [&](int64_t id) { return aeron_->findSubscription(id); });
    }

private:
    static constexpr int kMaxIters = 100'000;
    static std::string responseSubChannel(const std::string& ctl) {
        return aeron::ChannelUriStringBuilder().media("udp").controlMode(aeron::CONTROL_MODE_RESPONSE)
                   .controlEndpoint(ctl).build();
    }
    static EmbeddedDriverConfig cfgAt(const fs::path& dir) {
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 1 << 20;
        return c;
    }
    template <class Pub> bool offer(Pub& pub, const uint8_t* buf, std::size_t len) {
        if (!pub) return false;
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), static_cast<aeron::util::index_t>(len));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            if (pub->offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) return true;
            std::this_thread::sleep_for(100us);
        }
        return false;
    }
    void drain(const std::shared_ptr<aeron::Subscription>& sub, std::vector<Frame>& out) {
        if (!sub) return;
        sub->poll([&out](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                         aeron::Header&) {
            Frame f;
            f.bytes.assign(b.buffer() + off, b.buffer() + off + len);
            if (auto h = pc::MessageHeader::decode(f.bytes.data(), f.bytes.size())) {
                f.templateId = h->templateId; f.sessionId = h->sessionId; f.correlationId = h->correlationId;
            }
            out.push_back(std::move(f));
        }, 16);
    }
    template <class Pred> bool wait(Pred pred, std::chrono::milliseconds budget, bool) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            pollGw(); pollWk();
            if (pred()) return true;
            std::this_thread::sleep_for(100us);
        }
        pollGw(); pollWk();
        return pred();
    }

    EmbeddedDriver driver_;
    Ports p_;
    Mode mode_;
    int32_t respStream_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::shared_ptr<aeron::Subscription> gwSub_, wSub_;
    std::shared_ptr<aeron::Publication> gwPub_, wPub_;
    std::string localChannel_;
    std::vector<Frame> gw_, wk_;
};

/// A running single-node broker + a topic.
struct Env {
    TmpDir dir{"n"}, setupDir{"s"};
    Ports p = Ports::discover();
    std::unique_ptr<BrokerNode> node;
    int32_t topic = -1;
    std::unique_ptr<WClient> setup;
    int64_t setupSid = 0;
    std::string nodeTag;

    Env() {
        node = std::make_unique<BrokerNode>(makeConfig(dir.path, p));
        node->start();
    }
    ~Env() { setup.reset(); if (node) node->stop(); }
    bool awaitLeader() { return node->awaitLeader(15s); }
    /// Define "resp_topic" through a v16 response-mode client (kept as the setup session).
    bool defineTopic() {
        setup = std::make_unique<WClient>(setupDir.path, p, WClient::Mode::Response);
        if (!setup->ready()) return false;
        const auto r = setup->connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
        if (!r || r->code != 0) return false;
        setupSid = r->assignedSessionId;
        int64_t corr = 10;
        const auto deadline = std::chrono::steady_clock::now() + 30s;
        while (std::chrono::steady_clock::now() < deadline) {
            const int64_t c0 = ++corr;
            if (!setup->sendDefineTopic(setupSid, c0, "resp_topic", {0xD1, 1})) return false;
            if (!setup->waitGw([&] { return setup->findGw(kDefineResp, c0) != nullptr; }, 5s)) continue;
            const Frame* f = setup->findGw(kDefineResp, c0);
            auto d = pc::DefineTopicResponse::decode(f->bytes.data(), f->bytes.size());
            if (d && d->errorCode == 0) { topic = d->topicId; return true; }
            if (d && d->errorCode != static_cast<uint16_t>(pc::ErrorCode::NotLeader)) return false;
            std::this_thread::sleep_for(20ms);
        }
        return false;
    }
    int64_t logEnd() {
        return node->onOwnerThreadForTest(topic, 0, [](rune::replication::PartitionReplica* r) {
            return r ? r->logEndOffset() : int64_t{-1};
        });
    }
    uint64_t spoofed() { return node->onWorkerThreadForTest(0, [](Worker& w) { return w.spoofedIngressDroppedForTest(); }); }
    uint64_t bindsAccepted() { return node->onWorkerThreadForTest(0, [](Worker& w) { return w.workerBindsAcceptedForTest(); }); }
    uint64_t bindsRefused() { return node->onWorkerThreadForTest(0, [](Worker& w) { return w.workerBindsRefusedForTest(); }); }
    uint64_t bindImageLookups() { return node->onWorkerThreadForTest(0, [](Worker& w) { return w.workerBindImageLookupsForTest(); }); }
    uint64_t imageUnbinds() { return node->onWorkerThreadForTest(0, [](Worker& w) { return w.imageUnbindsForTest(); }); }
    bool pubReady(int64_t sid) {
        return node->onWorkerThreadForTest(0, [sid](Worker& w) { return w.responsePubReadyForTest(sid); });
    }
    template <class Pred> bool awaitCounter(Pred pred, std::chrono::milliseconds budget = 10s) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        for (int i = 0; i < 100'000 && std::chrono::steady_clock::now() < deadline; ++i) {
            if (pred()) return true;
            std::this_thread::sleep_for(2ms);
        }
        return pred();
    }
};

std::vector<uint8_t> batchOf(const std::string& value) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", value, 1000);
    return b.build();
}

/// The values of every record in every Records frame of `frames` (arrival order), plus an error flag.
std::vector<std::string> recordValues(const std::vector<Frame>& frames, int64_t onlySessionId, bool* sessionOk = nullptr) {
    std::vector<std::string> out;
    for (const auto& f : frames) {
        if (f.templateId != kRecords) continue;
        if (sessionOk && f.sessionId != onlySessionId) *sessionOk = false;
        const auto rec = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (!rec) continue;
        rune::protocol::RecordBatchReader reader(rec->recordBatch.data(), rec->recordBatch.size());
        if (!reader.ensureValid()) continue;
        reader.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) { out.emplace_back(val); });
    }
    return out;
}

/// Produce `n` records with acks=1 straight to the worker (one per call, corr base+i) and wait for every ack on the
/// WORKER response subscription.  Returns the ack count with errorCode 0.
int produceAndAwaitAcks(WClient& c, int64_t sid, int32_t topic, int64_t corrBase, int n, const std::string& tag,
                        bool acksOnGw = false) {
    for (int i = 0; i < n; ++i)
        if (!c.sendProduceWorker(sid, corrBase + i, topic, batchOf(tag + std::to_string(i)))) return -1;
    const auto& v = [&]() -> const std::vector<Frame>& { return acksOnGw ? c.gw() : c.wk(); };
    auto okCount = [&] {
        int ok = 0;
        for (const auto& f : v()) {
            if (f.templateId != kProduceResp) continue;
            const auto r = pc::ProduceResponse::decode(f.bytes.data(), f.bytes.size());
            if (r && r->errorCode == 0 && f.correlationId >= corrBase && f.correlationId < corrBase + n) ++ok;
        }
        return ok;
    };
    c.waitGw([&] { return okCount() >= n; });
    return okCount();
}

}  // namespace

// Acks and Records ride the worker response publication: the declared channel is unroutable, so the only way a
// frame reaches the client is the response subscription the WorkerBind correlated.
TEST(ResponseEgressWorkerRegression, AcksAndRecordsRideTheResponsePublicationRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir cdir{"c1"};
    WClient c(cdir.path, e.p, WClient::Mode::Response);
    ASSERT_TRUE(c.ready());
    const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->code, 0);
    ASSERT_TRUE(r->v16) << "a response-mode shard session is told its bind token";
    ASSERT_NE(r->bindToken, (std::array<uint8_t, 16>{}));
    const int64_t sid = r->assignedSessionId;
    ASSERT_TRUE(c.sendWorkerBind(sid, 2, r->bindToken));
    constexpr int kN = 20;
    std::vector<std::string> sent;
    for (int i = 0; i < kN; ++i) sent.push_back("a" + std::to_string(i));
    ASSERT_EQ(produceAndAwaitAcks(c, sid, e.topic, 100, kN, "a"), kN) << "acks on the worker response subscription";
    for (const auto& f : c.wk()) EXPECT_EQ(f.sessionId, sid);
    ASSERT_TRUE(c.sendSubscribe(sid, 500, e.topic, 0));
    ASSERT_TRUE(c.waitGw([&] { return c.findGw(kSubResp, 500) != nullptr; })) << "SubscribeResponse on the gateway response sub";
    bool sessionOk = true;
    ASSERT_TRUE(c.waitWk([&] { return recordValues(c.wk(), sid).size() >= static_cast<std::size_t>(kN); }))
        << "Records on the worker response subscription";
    c.drainFor(200ms);
    const auto got = recordValues(c.wk(), sid, &sessionOk);
    EXPECT_TRUE(sessionOk);
    EXPECT_EQ(got, sent) << "byte-equal, in order";
    EXPECT_EQ(c.count(c.gw(), kRecords), 0) << "no Records on the gateway response publication";
    EXPECT_EQ(e.bindsAccepted(), 1u);
}

// The same with authentication ON (perSessionEgressRequired_): a SCRAM login over the gateway response publication,
// then bind, produce and subscribe.
TEST(ResponseEgressWorkerRegression, AcksAndRecordsRideTheResponsePublicationUnderAuthRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthBroker::Options o;
    o.shard = true;
    const Ports p = Ports::discover();
    // AuthBroker allocated its own client / ingress / repl / egress ports: only the response ports are new.
    o.configure = [&](NodeConfig& cfg) {
        cfg.gatewayResponseControlPort = p.gwResp;
        cfg.responseControlBasePort    = p.wResp;
        cfg.peers[0].gatewayResponseControlPort = p.gwResp;
        cfg.peers[0].responseControlBasePort    = p.wResp;
    };
    AuthRig rig("respauth", o);
    std::string user;
    (void)rig.newUser({exact(SO::Write, ST::Topic, "orders"), exact(SO::Read, ST::Topic, "orders")}, &user);
    const Ports q = [&] {
        Ports x = p;
        x.clientStream = 1;
        x.client = static_cast<uint16_t>(std::stoi(rig.broker().endpoint().substr(rig.broker().endpoint().rfind(':') + 1)));
        const std::string ing = rig.broker().workerIngressChannel();
        x.ib = static_cast<uint16_t>(std::stoi(ing.substr(ing.rfind(':') + 1)));
        return x;
    }();
    TmpDir cdir{"ca"};
    WClient c(cdir.path, q, WClient::Mode::Response);
    ASSERT_TRUE(c.ready());
    const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->code, 0);
    ASSERT_TRUE(r->v16);
    const int64_t sid = r->assignedSessionId;
    // SCRAM over the gateway response subscription.
    pc::AuthClientFirst cf;
    const std::string nonce = rune::crypto::scram::makeNonce();
    cf.sessionId = sid; cf.correlationId = 20; cf.username = user; cf.clientNonce = nonce;
    std::vector<uint8_t> b(cf.encodedSize()); b.resize(cf.encode(b.data()));
    ASSERT_TRUE(c.offerGw(b.data(), b.size()));
    const uint16_t kFirst = static_cast<uint16_t>(pc::TemplateId::AuthServerFirst);
    const uint16_t kFinal = static_cast<uint16_t>(pc::TemplateId::AuthServerFinal);
    ASSERT_TRUE(c.waitGw([&] { return c.findGw(kFirst, 20) != nullptr; }));
    const Frame* ff = c.findGw(kFirst, 20);
    const auto first = pc::AuthServerFirst::decode(ff->bytes.data(), ff->bytes.size());
    ASSERT_TRUE(first.has_value());
    const auto ans = scramAnswer(user, nonce, *first, {AuthRig::kPw, AuthRig::kPw}, sid, 20);
    ASSERT_TRUE(ans.has_value());
    std::vector<uint8_t> fb(ans->final.encodedSize()); fb.resize(ans->final.encode(fb.data()));
    ASSERT_TRUE(c.offerGw(fb.data(), fb.size()));
    ASSERT_TRUE(c.waitGw([&] { return c.findGw(kFinal, 20) != nullptr; }));
    const auto fin = pc::AuthServerFinal::decode(c.findGw(kFinal, 20)->bytes.data(), c.findGw(kFinal, 20)->bytes.size());
    ASSERT_TRUE(fin.has_value());
    ASSERT_EQ(fin->errorCode, 0) << "login";
    rig.broker().noteLoginOk();
    // bind + produce
    ASSERT_TRUE(c.sendWorkerBind(sid, 21, r->bindToken));
    constexpr int kN = 10;
    int ok = 0;
    for (int attempt = 0; attempt < 200 && ok < kN; ++attempt) {   // NotLeader / UnknownTopic while leadership settles
        ok = produceAndAwaitAcks(c, sid, rig.orders, 1000 + attempt * 100, kN, "x" + std::to_string(attempt) + "_");
        if (ok != kN) std::this_thread::sleep_for(25ms);
    }
    EXPECT_EQ(ok, kN) << "acks over the worker response publication under authentication";
    ASSERT_TRUE(c.sendSubscribe(sid, 600, rig.orders, 0));
    ASSERT_TRUE(c.waitGw([&] { return c.findGw(kSubResp, 600) != nullptr; }));
    ASSERT_TRUE(c.waitWk([&] { return c.count(c.wk(), kRecords) >= 1; })) << "Records on the worker response publication";
    for (const auto& f : c.wk()) EXPECT_EQ(f.sessionId, sid);
}

// An attacker image binding the victim's (sequential, guessable) session id with a wrong token is refused; the victim
// then binds and works; the attacker's response subscription receives nothing.
TEST(ResponseEgressWorkerRegression, WrongTokenNeverBindsRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir vdir{"v2"}, adir{"a2"};
    WClient victim(vdir.path, e.p, WClient::Mode::Response);
    WClient attacker(adir.path, e.p, WClient::Mode::WorkerOnly);
    ASSERT_TRUE(victim.ready());
    const auto r = victim.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    const int64_t sid = r->assignedSessionId;
    ASSERT_TRUE(attacker.openWorker());
    std::array<uint8_t, 16> wrong = r->bindToken;
    wrong[15] ^= 0x01;   // differs only in the last byte
    ASSERT_TRUE(attacker.sendWorkerBind(sid, 9, wrong));
    ASSERT_TRUE(e.awaitCounter([&] { return e.bindsRefused() >= 1; })) << "the wrong token was never refused";
    EXPECT_EQ(e.bindsAccepted(), 0u);
    // I4: the refusal happened BEFORE the image lookup / channel build (no work for an unauthenticated sender).
    EXPECT_EQ(e.bindImageLookups(), 0u);
    // ... and so does a bind for a session id nobody holds.
    ASSERT_TRUE(attacker.sendWorkerBind(sid + 1000, 10, r->bindToken));
    ASSERT_TRUE(e.awaitCounter([&] { return e.bindsRefused() >= 2; })) << "the unknown session was never refused";
    EXPECT_EQ(e.bindImageLookups(), 0u);
    ASSERT_TRUE(victim.sendWorkerBind(sid, 2, r->bindToken));
    ASSERT_EQ(produceAndAwaitAcks(victim, sid, e.topic, 100, 5, "w"), 5);
    EXPECT_EQ(e.bindsAccepted(), 1u);
    EXPECT_EQ(e.bindImageLookups(), 1u) << "only the authorized bind does the lookup";
    attacker.drainFor(300ms);
    EXPECT_TRUE(attacker.wk().empty() && attacker.gw().empty()) << "the attacker's response subscription received frames";
}

// The victim is bound; an attacker presenting the RIGHT token from another image is refused (a session never rebinds)
// and the victim keeps receiving.
TEST(ResponseEgressWorkerRegression, SecondBindNeverRebindsRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir vdir{"v3"}, adir{"a3"};
    WClient victim(vdir.path, e.p, WClient::Mode::Response);
    WClient attacker(adir.path, e.p, WClient::Mode::WorkerOnly);
    const auto r = victim.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    const int64_t sid = r->assignedSessionId;
    ASSERT_TRUE(victim.sendWorkerBind(sid, 2, r->bindToken));
    ASSERT_EQ(produceAndAwaitAcks(victim, sid, e.topic, 100, 3, "s"), 3);
    ASSERT_TRUE(attacker.openWorker());
    ASSERT_TRUE(attacker.sendWorkerBind(sid, 9, r->bindToken));   // the stolen, correct token
    ASSERT_TRUE(e.awaitCounter([&] { return e.bindsRefused() >= 1; }));
    EXPECT_EQ(e.bindsAccepted(), 1u) << "only the victim's bind was accepted";
    ASSERT_EQ(produceAndAwaitAcks(victim, sid, e.topic, 200, 3, "t"), 3) << "the victim is still answered";
    attacker.drainFor(300ms);
    EXPECT_TRUE(attacker.wk().empty()) << "the attacker received frames of the victim's session";
}

// A produce for the victim's session from ANOTHER image is consumed: nothing is appended, nothing answered anywhere.
TEST(ResponseEgressWorkerRegression, ProduceFromAnotherImageIsDroppedRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir vdir{"v4"}, adir{"a4"};
    WClient victim(vdir.path, e.p, WClient::Mode::Response);
    WClient attacker(adir.path, e.p, WClient::Mode::WorkerOnly);
    const auto r = victim.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    const int64_t sid = r->assignedSessionId;
    ASSERT_TRUE(victim.sendWorkerBind(sid, 2, r->bindToken));
    ASSERT_EQ(produceAndAwaitAcks(victim, sid, e.topic, 100, 2, "v"), 2);
    const int64_t before = e.logEnd();
    ASSERT_GE(before, 2);
    ASSERT_EQ(e.spoofed(), 0u);
    ASSERT_TRUE(attacker.openWorker());
    ASSERT_TRUE(attacker.sendProduceWorker(sid, 777, e.topic, batchOf("evil")));
    ASSERT_TRUE(e.awaitCounter([&] { return e.spoofed() >= 1; })) << "the foreign-image produce was not dropped";
    EXPECT_EQ(e.spoofed(), 1u);
    victim.drainFor(300ms); attacker.drainFor(300ms);
    EXPECT_EQ(e.logEnd(), before) << "nothing was appended";
    EXPECT_EQ(victim.findGw(kProduceResp, 777), nullptr);
    for (const auto& f : victim.wk()) EXPECT_NE(f.correlationId, 777);
    EXPECT_TRUE(attacker.wk().empty() && attacker.gw().empty()) << "no reply anywhere";
}

// Subscribe BEFORE the bind: the worker holds the subscription (cursor unchanged, no MDC fallback); the records another
// (old-client) session produced meanwhile all arrive once the client binds.
TEST(ResponseEgressWorkerRegression, SubscribeBeforeBindParksThenDeliversRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir adir{"a5"}, bdir{"b5"};
    WClient a(adir.path, e.p, WClient::Mode::Response);
    const auto r = a.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    const int64_t sid = r->assignedSessionId;
    // The MDC listener: a dynamic-MDC subscriber on the worker's shared egress.
    auto mdc = a.addSubscription("aeron:udp?control-mode=dynamic|control=127.0.0.1:" + std::to_string(e.p.eg), kMdcStream);
    ASSERT_NE(mdc, nullptr);
    int mdcFrames = 0;
    auto drainMdc = [&] {
        mdc->poll([&](aeron::concurrent::AtomicBuffer&, aeron::util::index_t, aeron::util::index_t, aeron::Header&) { ++mdcFrames; }, 16);
    };
    ASSERT_TRUE(a.sendSubscribe(sid, 500, e.topic, 0));
    ASSERT_TRUE(a.waitGw([&] { return a.findGw(kSubResp, 500) != nullptr; }));
    // An OLD client (declared path) produces 5 records through the gateway while A is unbound.
    WClient b(bdir.path, e.p, WClient::Mode::Declared);
    const auto rb = b.connect(1, 15, false, b.localChannel());
    ASSERT_TRUE(rb.has_value());
    ASSERT_EQ(rb->code, 0);
    std::vector<std::string> sent;
    for (int i = 0; i < 5; ++i) {
        sent.push_back("p" + std::to_string(i));
        ASSERT_TRUE(b.sendProduceGateway(rb->assignedSessionId, 100 + i, e.topic, batchOf(sent.back())));
    }
    ASSERT_TRUE(b.waitGw([&] { return b.count(b.gw(), kProduceResp) >= 5; })) << "the old client's acks";
    for (int i = 0; i < 200; ++i) { drainMdc(); a.pollGw(); a.pollWk(); std::this_thread::sleep_for(1ms); }
    EXPECT_EQ(mdcFrames, 0) << "the unbound session's Records must never ride the shared MDC egress";
    EXPECT_EQ(a.count(a.wk(), kRecords), 0);
    ASSERT_TRUE(a.sendWorkerBind(sid, 2, r->bindToken));
    ASSERT_TRUE(a.waitWk([&] { return recordValues(a.wk(), sid).size() >= 5; })) << "all 5 Records after the bind";
    a.drainFor(200ms);
    for (int i = 0; i < 100; ++i) { drainMdc(); std::this_thread::sleep_for(1ms); }
    EXPECT_EQ(recordValues(a.wk(), sid), sent);
    EXPECT_EQ(mdcFrames, 0);
}

// The client's ingress image goes away: the worker unbinds the session (the response publication is released).
TEST(ResponseEgressWorkerRegression, ImageGoneClosesTheResponsePublicationRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir cdir{"c6"};
    WClient c(cdir.path, e.p, WClient::Mode::Response);
    const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(r.has_value());
    const int64_t sid = r->assignedSessionId;
    ASSERT_TRUE(c.sendWorkerBind(sid, 2, r->bindToken));
    ASSERT_EQ(produceAndAwaitAcks(c, sid, e.topic, 100, 3, "g"), 3);
    ASSERT_TRUE(e.awaitCounter([&] { return e.pubReady(sid); })) << "the response publication is Ready after the bind";
    ASSERT_EQ(e.imageUnbinds(), 0u);
    c.closeWorkerIngress();
    // A real-driver wait (the driver ends the image): bounded wall loop.
    EXPECT_TRUE(e.awaitCounter([&] { return e.imageUnbinds() >= 1; }, 20s)) << "the worker never noticed the image ending";
    EXPECT_TRUE(e.awaitCounter([&] { return !e.pubReady(sid); }, 5s)) << "the response publication stayed Ready";
}

// An OLD (v15) client keeps the declared worker acks: no v16 tail, acks on its declared channel.
TEST(ResponseEgressWorkerRegression, OldClientKeepsDeclaredWorkerAcksRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir odir{"o7"};
    WClient o(odir.path, e.p, WClient::Mode::Declared);
    const auto r = o.connect(1, 15, false, o.localChannel());
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->code, 0);
    EXPECT_FALSE(r->v16) << "an old client's ConnectResponse has no v16 tail";
    EXPECT_EQ(r->bindToken, (std::array<uint8_t, 16>{}));
    ASSERT_TRUE(o.openWorker());
    ASSERT_EQ(produceAndAwaitAcks(o, r->assignedSessionId, e.topic, 100, 10, "o", /*acksOnGw=*/true), 10)
        << "acks on the declared channel";
    EXPECT_EQ(e.bindsAccepted(), 0u);
    EXPECT_EQ(e.spoofed(), 0u);
}

// Two clients on one host (distinct source ports) bound to the same worker, both declaring the SAME unroutable
// channel: each sees only its own acks and Records.
TEST(ResponseEgressWorkerRegression, TwoClientsOneHostSeeOnlyTheirOwnRegression) {
    Env e; ASSERT_TRUE(e.awaitLeader()); ASSERT_TRUE(e.defineTopic());
    TmpDir adir{"a8"}, bdir{"b8"};
    WClient a(adir.path, e.p, WClient::Mode::Response);
    WClient b(bdir.path, e.p, WClient::Mode::Response);
    const auto ra = a.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    const auto rb = b.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
    ASSERT_TRUE(ra.has_value() && rb.has_value());
    ASSERT_NE(ra->assignedSessionId, rb->assignedSessionId);
    ASSERT_NE(ra->bindToken, rb->bindToken);
    ASSERT_TRUE(a.sendWorkerBind(ra->assignedSessionId, 2, ra->bindToken));
    ASSERT_TRUE(b.sendWorkerBind(rb->assignedSessionId, 2, rb->bindToken));
    constexpr int kN = 50;
    ASSERT_EQ(produceAndAwaitAcks(a, ra->assignedSessionId, e.topic, 1000, kN, "A"), kN);
    ASSERT_EQ(produceAndAwaitAcks(b, rb->assignedSessionId, e.topic, 2000, kN, "B"), kN);
    a.drainFor(300ms); b.drainFor(300ms);
    for (const auto& f : a.wk()) {
        EXPECT_EQ(f.sessionId, ra->assignedSessionId);
        EXPECT_GE(f.correlationId, 1000); EXPECT_LT(f.correlationId, 1000 + kN);
    }
    for (const auto& f : b.wk()) {
        EXPECT_EQ(f.sessionId, rb->assignedSessionId);
        EXPECT_GE(f.correlationId, 2000); EXPECT_LT(f.correlationId, 2000 + kN);
    }
    ASSERT_TRUE(a.sendSubscribe(ra->assignedSessionId, 500, e.topic, 0));
    ASSERT_TRUE(b.sendSubscribe(rb->assignedSessionId, 500, e.topic, 0));
    ASSERT_TRUE(a.waitWk([&] { return recordValues(a.wk(), 0).size() >= 2u * kN; }));
    ASSERT_TRUE(b.waitWk([&] { return recordValues(b.wk(), 0).size() >= 2u * kN; }));
    a.drainFor(200ms); b.drainFor(200ms);
    bool aOk = true, bOk = true;
    EXPECT_EQ(recordValues(a.wk(), ra->assignedSessionId, &aOk).size(), 2u * kN);
    EXPECT_EQ(recordValues(b.wk(), rb->assignedSessionId, &bOk).size(), 2u * kN);
    EXPECT_TRUE(aOk) << "client A saw a Records frame of another session";
    EXPECT_TRUE(bOk) << "client B saw a Records frame of another session";
}
