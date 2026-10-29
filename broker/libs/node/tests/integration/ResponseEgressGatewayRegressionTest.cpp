// ResponseEgressGatewayRegressionTest.cpp — NAT-safe egress Task 5 (spec D2, D3, D4): the gateway answers a v16
// client that sets responseCorrelated on an Aeron RESPONSE publication correlated with the Connect's request image,
// never on the declared channel; an older client keeps the declared path; the DDL principal's source address is the
// image's source on both paths (#227).
//
// A REAL single-node BrokerNode (shard mode, fused placement - the default - on collision-free explicit ports) driven
// by a raw-Aeron client written here (own embedded driver per client, so each has its own source port).  Every wait
// is bounded by an iteration cap AND a steady_clock deadline.
#include <rune/aeronutil/testing/FreePort.h>
#include <rune/catalog/CatalogEvent.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/PingMessages.h>
#include <rune/protocol/client/SchemaMessages.h>
#include <rune/storage/Log.h>
#include "support/DdlHarness.h"       // charBlob
#include "support/ShardTestClient.h"  // ShardTestClient, boundedTestTopicConfig

#include <Aeron.h>
#include <ChannelUriStringBuilder.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
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
constexpr int32_t kRespStream   = 4343;   // the raw client's egressStreamId (S)

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_respgw_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Ports {
    uint16_t cons, client, repl, ib, rb, eg, gwResp, wResp;
    static Ports discover() {
        namespace t = rune::aeronutil::testing;
        Ports p{};
        p.ib = t::allocFreeUdpPortRange(1); p.rb = t::allocFreeUdpPortRange(1); p.eg = t::allocFreeUdpPortRange(1);
        p.cons = t::allocFreeUdpPort(); p.client = t::allocFreeUdpPort(); p.repl = t::allocFreeUdpPort();
        p.gwResp = t::allocFreeUdpPort();
        p.wResp = t::allocFreeUdpPortRange(1);
        return p;
    }
};

/// A single-node shard-mode config (fused placement = the default) on `p`.  `withResponsePort` = false models a node
/// with no gateway response-control port (every client uses the declared path).
NodeConfig makeConfig(const fs::path& dir, const Ports& p, bool withResponsePort) {
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
    cfg.workerIngressBasePort = p.ib;
    cfg.workerReplBasePort    = p.rb;
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg;
    cfg.workerIngressBaseStreamId = 111;
    cfg.gatewayMinIsr = 1;
    cfg.aeronTermBufferBytes = 1 << 20;
    cfg.workerCoreBase = -1;
    PeerConfig peer;
    peer.id = 0; peer.host = "127.0.0.1";
    peer.consensusPort = p.cons; peer.clientPort = p.client; peer.replicationPort = p.repl;
    peer.replBasePort = p.rb; peer.ingressBasePort = p.ib; peer.workerCount = 1;
    peer.egressControlBasePort = p.eg;
    if (withResponsePort) {
        cfg.gatewayResponseControlPort = p.gwResp;
        peer.gatewayResponseControlPort = p.gwResp;
        // Task 6: in shard mode response mode also needs the workers' response-control base.
        cfg.responseControlBasePort = p.wResp;
        peer.responseControlBasePort = p.wResp;
    }
    cfg.peers.push_back(peer);
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client);
    cfg.clientEndpoint.streamId = kClientStream;
    return cfg;
}

struct Frame {
    uint16_t templateId = 0;
    int64_t sessionId = 0, correlationId = 0;
    std::vector<uint8_t> bytes;
};

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

/// A raw-Aeron client with its own embedded driver (distinct source ports per client).
/// response == true : response subscription on the gateway's response-control endpoint + request publication
///                    carrying response-correlation-id (the Aeron 1.51 mechanics of the spike).
/// response == false: a plain local subscription (the declared path) + a plain request publication.
/// Aeron 1.51: any find*(regId) returns non-null ONCE - every resolved object is stashed, never found again.
class RawClient {
public:
    RawClient(const fs::path& dir, const std::string& gatewayClientEndpoint, bool response, const std::string& respCtl,
              int32_t respStream = kRespStream)
        : driver_(cfgAt(dir), DriverMode::Threaded) {
        aeron::Context ctx;
        ctx.aeronDir(dir.string());
        aeron_ = aeron::Aeron::connect(ctx);
        std::string subChannel;
        if (response) {
            subChannel = aeron::ChannelUriStringBuilder().media("udp").controlMode(aeron::CONTROL_MODE_RESPONSE)
                             .controlEndpoint(respCtl).build();
        } else {
            localChannel_ = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort());
            subChannel = localChannel_;
        }
        sub_ = resolveOnce([&] { return aeron_->addSubscription(subChannel, respStream); },
                        [&](int64_t id) { return aeron_->findSubscription(id); });
        std::string reqChannel = "aeron:udp?endpoint=" + gatewayClientEndpoint;
        if (response)
            reqChannel = aeron::ChannelUriStringBuilder().media("udp").endpoint(gatewayClientEndpoint)
                             .responseCorrelationId(sub_ ? sub_->registrationId() : 0).build();
        pub_ = resolveOnce([&] { return aeron_->addPublication(reqChannel, kClientStream); },
                        [&](int64_t id) { return aeron_->findPublication(id); });
    }
    ~RawClient() { pub_.reset(); sub_.reset(); aeron_.reset(); }
    [[nodiscard]] bool ready() const { return sub_ && pub_; }
    [[nodiscard]] const std::string& localChannel() const { return localChannel_; }

    bool offer(const uint8_t* buf, std::size_t len) {
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), static_cast<aeron::util::index_t>(len));
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            if (pub_->offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) return true;
            std::this_thread::sleep_for(100us);
        }
        return false;
    }
    bool sendConnect(int64_t corr, int32_t version, bool responseCorrelated, const std::string& declaredChannel) {
        pc::ConnectRequest req;
        req.correlationId = corr; req.egressStreamId = kRespStream; req.clientVersion = version;
        req.egressChannel = declaredChannel; req.responseCorrelated = responseCorrelated;
        std::vector<uint8_t> buf(req.encodedSize());
        return offer(buf.data(), req.encode(buf.data()));
    }
    bool sendPing(int64_t sid, int64_t corr, std::string_view echo) {
        pc::Ping p; p.sessionId = sid; p.correlationId = corr; p.echo = echo;
        std::vector<uint8_t> buf(pc::MessageHeader::ENCODED_SIZE + 2 + echo.size());
        return offer(buf.data(), p.encode(buf.data()));
    }
    bool sendDefineTopic(int64_t sid, int64_t corr, std::string_view name, rune::protocol::RequestId rid,
                         std::string_view clientName) {
        const auto descriptor = charBlob();
        pc::DefineTopic m;
        m.sessionId = sid; m.correlationId = corr; m.topicName = name; m.partitionCount = 1;
        m.keyField = "id"; m.validate = false; m.persistence = 0;
        m.descriptorBlob = descriptor.data(); m.descriptorLen = static_cast<uint32_t>(descriptor.size());
        m.hasExt = true; m.ext.clientName = clientName; m.ext.requestId = rid;
        m.config = boundedTestTopicConfig();
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        return len > 0 && offer(buf.data(), len);
    }

    /// Drain the subscription once into frames_.
    void poll() {
        sub_->poll([this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off, aeron::util::index_t len,
                          aeron::Header&) {
            Frame f;
            f.bytes.assign(b.buffer() + off, b.buffer() + off + len);
            if (auto h = pc::MessageHeader::decode(f.bytes.data(), f.bytes.size())) {
                f.templateId = h->templateId; f.sessionId = h->sessionId; f.correlationId = h->correlationId;
            }
            frames_.push_back(std::move(f));
        }, 16);
    }
    template <class Pred> bool waitFor(Pred pred, std::chrono::milliseconds budget = 10s) {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            poll();
            if (pred()) return true;
            std::this_thread::sleep_for(100us);
        }
        poll();
        return pred();
    }
    /// Keep draining for `ms` (to prove nothing more arrives).
    void drainFor(std::chrono::milliseconds ms) {
        const auto deadline = std::chrono::steady_clock::now() + ms;
        for (int i = 0; i < kMaxIters && std::chrono::steady_clock::now() < deadline; ++i) {
            poll();
            std::this_thread::sleep_for(100us);
        }
    }
    [[nodiscard]] const Frame* find(uint16_t templateId, int64_t corr) const {
        for (const auto& f : frames_) if (f.templateId == templateId && f.correlationId == corr) return &f;
        return nullptr;
    }
    [[nodiscard]] const std::vector<Frame>& frames() const { return frames_; }

    /// Connect and wait for the ConnectResponse; nullopt = none.
    std::optional<pc::ConnectResponse> connect(int64_t corr, int32_t version, bool responseCorrelated,
                                               const std::string& declaredChannel) {
        if (!sendConnect(corr, version, responseCorrelated, declaredChannel)) return std::nullopt;
        const auto ct = static_cast<uint16_t>(pc::TemplateId::ConnectResponse);
        if (!waitFor([&] { return find(ct, corr) != nullptr; })) return std::nullopt;
        return pc::ConnectResponse::decode(find(ct, corr)->bytes.data(), find(ct, corr)->bytes.size());
    }

private:
    static constexpr int kMaxIters = 100'000;
    static EmbeddedDriverConfig cfgAt(const fs::path& dir) {
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 1 << 20;
        return c;
    }
    EmbeddedDriver driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::shared_ptr<aeron::Subscription> sub_;
    std::shared_ptr<aeron::Publication> pub_;
    std::string localChannel_;
    std::vector<Frame> frames_;
};

constexpr uint16_t kPong = static_cast<uint16_t>(pc::TemplateId::Pong);
constexpr uint16_t kDefineResp = static_cast<uint16_t>(pc::TemplateId::DefineTopicResponse);
const std::string kUnroutable = "aeron:udp?endpoint=192.0.2.1:40200";   // TEST-NET-1: never reachable

std::string gwEndpoint(const Ports& p) { return "127.0.0.1:" + std::to_string(p.client); }
std::string respCtl(const Ports& p) { return "127.0.0.1:" + std::to_string(p.gwResp); }

/// DefineTopic until it is not answered NotLeader (the broker settles); returns the answer.
std::optional<pc::DefineTopicResponse> defineUntilServed(RawClient& c, int64_t sid, std::string_view name,
                                                         rune::protocol::RequestId rid, std::string_view clientName,
                                                         int64_t& corr) {
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
        const int64_t c0 = ++corr;
        if (!c.sendDefineTopic(sid, c0, name, rid, clientName)) return std::nullopt;
        if (!c.waitFor([&] { return c.find(kDefineResp, c0) != nullptr; }, 5s)) continue;
        const Frame* f = c.find(kDefineResp, c0);
        auto r = pc::DefineTopicResponse::decode(f->bytes.data(), f->bytes.size());
        if (r && r->errorCode != static_cast<uint16_t>(pc::ErrorCode::NotLeader)) return r;
        std::this_thread::sleep_for(20ms);
    }
    return std::nullopt;
}

}  // namespace

// A v16 client whose declared channel is unroutable still connects and is answered: the reply rides the response
// publication correlated with its request image.  (RED before the gateway chooses the response path: the
// ConnectResponse goes to 192.0.2.1 and the client times out.)
TEST(ResponseEgressGatewayRegression, UnroutableDeclaredChannelStillConnectsRegression) {
    TmpDir dir{"u"}, cdir{"uc"};
    const Ports p = Ports::discover();
    BrokerNode node{makeConfig(dir.path, p, true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(15s));
    {
        RawClient c(cdir.path, gwEndpoint(p), /*response=*/true, respCtl(p));
        ASSERT_TRUE(c.ready());
        const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
        ASSERT_TRUE(r.has_value()) << "no ConnectResponse on the response subscription";
        EXPECT_EQ(r->code, 0);
        EXPECT_EQ(r->brokerVersion, 9);
        ASSERT_GT(r->assignedSessionId, 0);
        ASSERT_TRUE(c.sendPing(r->assignedSessionId, 2, "hello"));
        ASSERT_TRUE(c.waitFor([&] { return c.find(kPong, 2) != nullptr; })) << "no Pong on the response subscription";
        EXPECT_EQ(c.find(kPong, 2)->sessionId, r->assignedSessionId);
    }
    node.stop();
}

// An old (v15) client with a reachable declared channel keeps the declared path; nothing is ever sent to a response
// subscription.
TEST(ResponseEgressGatewayRegression, OldClientDeclaredPathStillWorksRegression) {
    TmpDir dir{"o"}, cdir{"oc"}, edir{"oe"};
    const Ports p = Ports::discover();
    BrokerNode node{makeConfig(dir.path, p, true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(15s));
    {
        RawClient c(cdir.path, gwEndpoint(p), /*response=*/false, respCtl(p));
        // A response subscription of another client that nobody correlated: it must stay silent.
        RawClient bystander(edir.path, gwEndpoint(p), /*response=*/true, respCtl(p), kRespStream + 1);
        ASSERT_TRUE(c.ready());
        ASSERT_TRUE(bystander.ready());
        const auto r = c.connect(1, 15, false, c.localChannel());
        ASSERT_TRUE(r.has_value()) << "no ConnectResponse on the declared channel";
        EXPECT_EQ(r->code, 0);
        ASSERT_TRUE(c.sendPing(r->assignedSessionId, 2, "old"));
        ASSERT_TRUE(c.waitFor([&] { return c.find(kPong, 2) != nullptr; })) << "no Pong on the declared channel";
        bystander.drainFor(300ms);
        EXPECT_TRUE(bystander.frames().empty()) << "a response subscription received " << bystander.frames().size()
                                                << " frames for an old client";
    }
    node.stop();
}

// Two clients on one host (distinct source ports) declare the SAME unroutable channel: each gets exactly its own
// answers, and its own session id.
TEST(ResponseEgressGatewayRegression, TwoClientsOneHostNeverSeeEachOthersRepliesRegression) {
    TmpDir dir{"t"}, c0dir{"t0"}, c1dir{"t1"};
    const Ports p = Ports::discover();
    BrokerNode node{makeConfig(dir.path, p, true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(15s));
    {
        RawClient a(c0dir.path, gwEndpoint(p), true, respCtl(p));
        RawClient b(c1dir.path, gwEndpoint(p), true, respCtl(p));
        ASSERT_TRUE(a.ready() && b.ready());
        const auto ra = a.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
        const auto rb = b.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
        ASSERT_TRUE(ra.has_value() && rb.has_value());
        ASSERT_EQ(ra->code, 0);
        ASSERT_EQ(rb->code, 0);
        ASSERT_NE(ra->assignedSessionId, rb->assignedSessionId);
        constexpr int kPings = 50;
        RawClient* clients[2] = {&a, &b};
        const int64_t sids[2] = {ra->assignedSessionId, rb->assignedSessionId};
        for (int i = 0; i < kPings; ++i)
            for (int k = 0; k < 2; ++k) {
                const std::string echo = std::string(k == 0 ? "A" : "B") + std::to_string(i);
                ASSERT_TRUE(clients[k]->sendPing(sids[k], 1000 * (k + 1) + i, echo));
            }
        for (int k = 0; k < 2; ++k) {
            ASSERT_TRUE(clients[k]->waitFor([&] {
                int n = 0;
                for (const auto& f : clients[k]->frames()) n += f.templateId == kPong ? 1 : 0;
                return n >= kPings;
            })) << "client " << k << " missing pongs";
            clients[k]->drainFor(300ms);
            int pongs = 0;
            for (const auto& f : clients[k]->frames()) {
                if (f.templateId != kPong) continue;
                ++pongs;
                EXPECT_EQ(f.sessionId, sids[k]) << "client " << k << " saw another session's pong";
                EXPECT_GE(f.correlationId, 1000 * (k + 1));
                EXPECT_LT(f.correlationId, 1000 * (k + 1) + kPings);
                const auto pong = pc::Pong::decode(f.bytes.data(), f.bytes.size());
                ASSERT_TRUE(pong.has_value());
                EXPECT_EQ(pong->echo.substr(0, 1), k == 0 ? "A" : "B");
            }
            EXPECT_EQ(pongs, kPings) << "client " << k;
        }
    }
    node.stop();
}

namespace {
/// Every (clientName -> sourceAddress) of the CreateTopic principals in the stopped node's __catalog.
std::map<std::string, std::string> catalogSources(const fs::path& dataDir) {
    std::map<std::string, std::string> out;
    rune::storage::Log log((dataDir / "__catalog").string());
    log.recover();
    const std::vector<uint8_t> data = log.read(0, 1 << 22);
    std::size_t pos = 0;
    while (pos < data.size()) {
        rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
        if (!reader.ensureValid()) break;
        reader.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) {
            const auto ev = rune::catalog::decode(
                std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
            if (ev) out[std::string(ev->principal.clientName)] = std::string(ev->principal.sourceAddress);
        });
        if (reader.totalLength() == 0) break;
        pos += static_cast<std::size_t>(reader.totalLength());
    }
    return out;
}
}  // namespace

// The catalog principal's source address is the Connect image's source ("127.0.0.1:<port>"), never the declared
// channel - on the response path (v16, unroutable declared channel) AND on the declared path (v15 declaring a host
// name).  RED before the session records the image's source identity (#227).
TEST(ResponseEgressGatewayRegression, DdlPrincipalSourceIsTheImageRegression) {
    TmpDir dir{"d"}, cdir{"dc"}, odir{"do"};
    const Ports p = Ports::discover();
    {
        BrokerNode node{makeConfig(dir.path, p, true)};
        node.start();
        ASSERT_TRUE(node.awaitLeader(15s));
        {
            RawClient c(cdir.path, gwEndpoint(p), true, respCtl(p));
            ASSERT_TRUE(c.ready());
            const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, kUnroutable);
            ASSERT_TRUE(r.has_value());
            ASSERT_EQ(r->code, 0);
            int64_t corr = 10;
            const auto d = defineUntilServed(c, r->assignedSessionId, "resp_topic", {0xD1, 1}, "raw-v16", corr);
            ASSERT_TRUE(d.has_value());
            EXPECT_EQ(d->errorCode, 0u);
        }
        {
            ShardTestClient old(odir.path);
            old.setClientVersion(15);
            old.advertiseEgressHost("localhost");   // the declared channel names another host
            const int64_t sid = old.connect("aeron:udp?endpoint=" + gwEndpoint(p), kClientStream);
            ASSERT_GT(sid, 0);
            const auto d = old.defineTopicUntil("aeron:udp?endpoint=" + gwEndpoint(p), kClientStream, sid, "decl_topic", 1,
                                                false, charBlob(), std::chrono::steady_clock::now() + 30s);
            ASSERT_TRUE(d.has_value());
            EXPECT_EQ(d->errorCode, 0u);
        }
        node.stop();
    }
    const auto src = catalogSources(dir.path);
    ASSERT_TRUE(src.count("raw-v16")) << "the v16 client's CreateTopic is not in the catalog";
    ASSERT_TRUE(src.count("shard-test-client")) << "the v15 client's CreateTopic is not in the catalog";
    EXPECT_TRUE(src.at("raw-v16").starts_with("127.0.0.1:")) << src.at("raw-v16");
    EXPECT_EQ(src.at("raw-v16").find("192.0.2.1"), std::string::npos) << src.at("raw-v16");
    EXPECT_TRUE(src.at("shard-test-client").starts_with("127.0.0.1:")) << src.at("shard-test-client");
    EXPECT_EQ(src.at("shard-test-client").find("localhost"), std::string::npos) << src.at("shard-test-client");
}

// A node with no gateway response-control port answers a v16 client that sets responseCorrelated on its DECLARED
// channel (the client's request publication is a plain one; its declared subscription is reachable).
TEST(ResponseEgressGatewayRegression, ResponseUnavailableFallsBackToDeclaredRegression) {
    TmpDir dir{"f"}, cdir{"fc"};
    const Ports p = Ports::discover();
    BrokerNode node{makeConfig(dir.path, p, /*withResponsePort=*/false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(15s));
    {
        RawClient c(cdir.path, gwEndpoint(p), /*response=*/false, respCtl(p));
        ASSERT_TRUE(c.ready());
        const auto r = c.connect(1, pc::kClientVersionResponseEgress, true, c.localChannel());
        ASSERT_TRUE(r.has_value()) << "no ConnectResponse on the declared channel";
        EXPECT_EQ(r->code, 0);
        EXPECT_EQ(r->brokerVersion, 9);
        ASSERT_TRUE(c.sendPing(r->assignedSessionId, 2, "fb"));
        ASSERT_TRUE(c.waitFor([&] { return c.find(kPong, 2) != nullptr; })) << "no Pong on the declared channel";
    }
    node.stop();
}
