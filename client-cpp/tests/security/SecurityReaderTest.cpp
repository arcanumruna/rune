// Authentication Task 8 (spec §3): the client's SecurityReader — Subscribe(-7) from the cache end, the history-epoch
// note, one record per Records frame, the caught-up marker, the verifying replay with the shared rune::security
// library — on the SAME EventLogStream the catalog reader runs on.  A fake broker serves the golden formatVer-1
// script; the blocking Client::security* reads run with the fake pumped from a helper thread (the Invoker owner
// pumps only the client).
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/catalog/CatalogReader.h>
#include <rune/client/security/SecurityReader.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/security/testing/SecurityGoldenScript.h>

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <type_traits>
#include <unistd.h>

namespace pc   = rune::protocol::client;
namespace rc   = rune::client;
namespace rsec = rune::security;

namespace {
int64_t g_now = 1'000'000'000;
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = "/tmp/security_reader_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    return c;
}

/// Serves `__security` like the broker's read server (topic −7): SubscribeResponse, the history-epoch note,
/// every event from the requested offset (one record per Records frame), the caught-up marker.
class SecurityFeed {
public:
    SecurityFeed(test_support::InvokerGateway& gw, std::vector<rsec::SecurityEvent> log) : gw_(gw), log_(std::move(log)) {
        gw_.templateHandlers[pc::Subscribe::TEMPLATE_ID] = [this](int ep, const uint8_t* f, std::size_t len) { onSubscribe(ep, f, len); };
    }
    void refuseWith(pc::ErrorCode c) { refuse_ = c; }          // ErrorResponse{c} on the Subscribe's corr
    void tamperFingerprintAt(std::size_t i) { tamper_ = i; }   // the record at i claims another resulting state: fingerprint mismatch
    [[nodiscard]] int subscribes() const noexcept { return subscribes_.load(); }
    [[nodiscard]] std::vector<int64_t> froms() const { return froms_; }
private:
    void onSubscribe(int ep, const uint8_t* f, std::size_t len) {
        namespace rcc = rune::client::codec;
        const auto sv = rcc::decodeSubscribe(f, len);
        ASSERT_TRUE(sv.has_value());
        ASSERT_EQ(sv->topicId, pc::kSecurityTopicId) << "the security reader subscribes __security (-7)";
        subscribes_.fetch_add(1);
        froms_.push_back(sv->fromOffset);
        const int64_t sid = sv->sessionId, corr = sv->correlationId;
        if (refuse_) {
            gw_.sendError(ep, static_cast<int16_t>(*refuse_), "not authorized: ADMIN on CLUSTER", sid, corr);
            return;
        }
        uint8_t out[64];
        const int64_t subId = (int64_t{1} << 62) | nextSub_++;
        gw_.send(ep, out, rcc::encodeSubscribeResponse(out, sizeof out, 0, 0, subId, sid, corr));
        frame(ep, subId, sid, corr, -1, epochBytes());
        const int64_t from = sv->fromOffset < 0 ? 0 : std::min<int64_t>(sv->fromOffset, std::ssize(log_));
        for (int64_t off = from; off < std::ssize(log_); ++off) {
            rsec::SecurityEvent e = log_[static_cast<std::size_t>(off)];
            if (tamper_ && *tamper_ == static_cast<std::size_t>(off)) e.fingerprint ^= 1;   // the record no longer matches the state it claims
            auto bytes = rsec::encode(e);
            ASSERT_TRUE(bytes.has_value());
            rune::protocol::RecordBatchBuilder b(off, 1'000'000);
            b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(bytes->data()), bytes->size()), 1'000'000);
            frame(ep, subId, sid, corr, off, b.build());
        }
        frame(ep, subId, sid, corr, std::ssize(log_), epochBytes());   // the caught-up marker
    }
    [[nodiscard]] std::vector<uint8_t> epochBytes() const {
        std::vector<uint8_t> e(8);
        for (int i = 0; i < 8; ++i) e[static_cast<std::size_t>(i)] = static_cast<uint8_t>(epoch_ >> (8 * i));
        return e;
    }
    void frame(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t base, const std::vector<uint8_t>& payload) {
        namespace rcc = rune::client::codec;
        std::vector<uint8_t> out(payload.size() + 256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), subId, pc::kSecurityTopicId, 0, base,
                                                 std::span<const uint8_t>(payload), sid, corr);
        ASSERT_GT(n, 0u);
        gw_.send(ep, out.data(), n);
    }
    test_support::InvokerGateway& gw_;
    std::vector<rsec::SecurityEvent> log_;
    std::vector<int64_t> froms_;
    std::atomic<int> subscribes_{0};
    std::optional<pc::ErrorCode> refuse_;
    std::optional<std::size_t> tamper_;
    int64_t nextSub_ = 1;
    uint64_t epoch_ = 0x5ec0000000000001ULL;
};

struct Rig {
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

std::optional<rc::CatalogResult<void>> refreshOnce(Rig& r, rc::SecurityReader& reader, int64_t budgetNs) {
    if (reader.beginRefresh() != rc::OpResult::Accepted) return std::nullopt;
    const int64_t deadline = g_now + budgetNs;
    std::optional<rc::CatalogResult<void>> done;
    r.drive([&] { done = reader.pollRefresh(g_now, deadline); return done.has_value(); }, 1'000'000);
    return done;
}

/// Pumps the gateway from a helper thread while the test thread blocks in a Client call (Invoker mode pumps the client only).
struct GatewayPump {
    test_support::InvokerGateway& gw;
    std::atomic<bool> stopFlag{false};
    std::thread t;
    explicit GatewayPump(test_support::InvokerGateway& g) : gw(g), t([this] {
        while (!stopFlag.load(std::memory_order_acquire)) { if (gw.pump() == 0) std::this_thread::yield(); }
    }) {}
    void stop() { if (t.joinable()) { stopFlag.store(true, std::memory_order_release); t.join(); } }
    ~GatewayPump() { stop(); }
};

uint64_t libraryDigest(const std::vector<rsec::SecurityEvent>& log, std::size_t upTo) {
    rsec::SecurityReplayer lib;
    for (std::size_t i = 0; i < upTo; ++i) {
        if (!lib.feedDecoded(static_cast<int64_t>(i), log[i])) return 0;
    }
    return lib.state().digest();
}
} // namespace

// The extraction is behaviour-neutral AND shared: both readers are one template, two policies.
template <class A, class B> struct SameTemplate : std::false_type {};
template <template <class> class T, class P1, class P2> struct SameTemplate<T<P1>, T<P2>> : std::true_type {};
static_assert(SameTemplate<rc::CatalogReader, rc::SecurityReader>::value,
              "the catalog and the security reader must be the SAME stream template");
static_assert(!std::is_same_v<rc::CatalogReader, rc::SecurityReader>);

TEST(SecurityReaderRegression, CatalogReaderIsUnchangedByTheExtractionRegression) {
    // Both readers share one stream template and API surface; the catalog reader's own behaviour is pinned by
    // CatalogReaderTest (rune_client_catalog_tests), which passes unmodified on this extraction.
    using C = rc::CatalogReader; using S = rc::SecurityReader;
    static_assert(std::is_same_v<decltype(&C::beginRefresh), rc::OpResult (C::*)()>);
    static_assert(std::is_same_v<decltype(&S::beginRefresh), rc::OpResult (S::*)()>);
    SUCCEED();
}

TEST(SecurityReaderRegression, ReplaysToTheMarkerAndAnswersGrantsFor) {
    const auto script = rsec::testing::goldenScriptV1();
    Rig r(invokerCfg(3800, 3801));
    SecurityFeed feed(r.gw, script);
    r.connect();
    rc::SecurityReader& reader = r.client.securityReader();
    auto done = refreshOnce(r, reader, 5'000'000'000LL);
    ASSERT_TRUE(done.has_value()) << "the refresh never ended";
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(reader.nextOffset(), 16);
    EXPECT_EQ(reader.state().digest(), libraryDigest(script, script.size())) << "the cache is the library's replay";
    ASSERT_EQ(reader.events().size(), script.size());
    EXPECT_EQ(feed.froms().front(), 0);
    // A second refresh continues from the cache end (nothing new): one more Subscribe, from offset 16.
    auto again = refreshOnce(r, reader, 5'000'000'000LL);
    ASSERT_TRUE(again.has_value() && again->has_value());
    EXPECT_EQ(feed.froms().back(), 16);
    EXPECT_EQ(reader.events().size(), script.size());

    // The blocking reads (gateway pumped from a helper thread): grants-for as of offset 8 and the final users.
    GatewayPump pump(r.gw);
    auto g = r.client.securityGrantsFor(rsec::GranteeKind::User, "ws1", rsec::AsOf{rsec::AsOf::By::Offset, 8},
                                        std::chrono::seconds(5));
    ASSERT_TRUE(g.has_value()) << g.error().detail;
    EXPECT_TRUE(g->direct.empty());
    ASSERT_EQ(g->viaRoles.size(), 1u);
    EXPECT_EQ(g->viaRoles[0].first, "world_server");
    EXPECT_EQ(g->viaRoles[0].second.size(), 3u);
    EXPECT_EQ(g->effective.size(), 3u);
    auto users = r.client.securityUsers(std::chrono::seconds(5));
    ASSERT_TRUE(users.has_value());
    ASSERT_EQ(users->size(), 1u) << "ws1 was dropped at offset 15";
    EXPECT_EQ((*users)[0].name, "admin");
    auto roles = r.client.securityRoles(std::chrono::seconds(5));
    ASSERT_TRUE(roles.has_value());
    EXPECT_TRUE(roles->empty()) << "world_server was dropped at offset 14";
    auto hist = r.client.securityHistory(rsec::HistoryFilter{rsec::GranteeKind::User, "ws1", std::nullopt},
                                         std::chrono::seconds(5));
    ASSERT_TRUE(hist.has_value());
    EXPECT_GE(hist->size(), 6u);
    auto gone = r.client.securityGrantsFor(rsec::GranteeKind::User, "ws1", std::nullopt, std::chrono::seconds(5));
    ASSERT_FALSE(gone.has_value()) << "a dropped user has no grants";
    pump.stop();
}

TEST(SecurityReaderRegression, TamperedRecordIsAReplayErrorNamingTheOffset) {
    const auto script = rsec::testing::goldenScriptV1();
    Rig r(invokerCfg(3802, 3803));
    SecurityFeed feed(r.gw, script);
    feed.tamperFingerprintAt(3);
    r.connect();
    rc::SecurityReader& reader = r.client.securityReader();
    auto done = refreshOnce(r, reader, 5'000'000'000LL);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Replay);
    EXPECT_EQ(done->error().offset, 3);
    EXPECT_NE(done->error().detail.find("offset 3"), std::string::npos) << done->error().detail;
    EXPECT_EQ(reader.nextOffset(), 3) << "the verified prefix (0..2) stays cached";
}

TEST(SecurityReaderRegression, ANonAdminReadIsRejectedNotAuthorized) {
    const auto script = rsec::testing::goldenScriptV1();
    Rig r(invokerCfg(3804, 3805));
    SecurityFeed feed(r.gw, script);
    feed.refuseWith(pc::ErrorCode::NotAuthorized);
    r.connect();
    rc::SecurityReader& reader = r.client.securityReader();
    auto done = refreshOnce(r, reader, 5'000'000'000LL);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Rejected);
    EXPECT_EQ(done->error().brokerErrorCode, 45);
    EXPECT_NE(done->error().detail.find("security"), std::string::npos);
    EXPECT_EQ(feed.subscribes(), 1) << "NotAuthorized is never retried";
}
