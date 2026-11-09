// tests/lease/LeaseSessionTest.cpp - lease topics Task 15: LeaseSession (keepalive cadence, batching, callbacks) and the produce guard,
// against the InvokerGateway stub broker, on a fake clock.  Every drive loop is capped; nothing sleeps.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <poll.h>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::OpKind;
using rc::OpStatus;

namespace {

constexpr int64_t kMs = 1'000'000;
int64_t g_now = 0;   // the injected duty clock (the test thread is the only thread)
int64_t fakeNow() noexcept { return g_now; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

rune::codec::Descriptor leaseDescriptor() {
    return rune::codec::DescriptorBuilder("lease_t", 1).add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true).build();
}
const uint64_t kFp = rune::codec::fingerprint(leaseDescriptor());

struct Row {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    uint64_t id = 0;
    [[nodiscard]] uint64_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + 32 + 8; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 44);
        b[0] = 40;
        b[2] = 1;
        std::memcpy(b + 36, &id, 8);
        return 44;
    }
};
const uint64_t Row::kFingerprint = kFp;

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("lease_session_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
} g_dataDirs;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    c.nanoClock       = &fakeNow;
    g_now             = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

const rune::protocol::LeaseSpec kSpec{.ttlMs = 6000, .minTtlMs = 1000, .maxTtlMs = 60000, .maxKeys = 100000,
                                      .expiryBatch = 64, .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};

std::vector<uint8_t> verdicts(const std::vector<uint16_t>& codes) {
    std::vector<uint8_t> v(codes.size() * 8, 0);
    for (std::size_t i = 0; i < codes.size(); ++i) {
        v[i * 8 + 2] = static_cast<uint8_t>(codes[i] & 0xff);
        v[i * 8 + 3] = static_cast<uint8_t>(codes[i] >> 8);
    }
    return v;
}

struct Cb {
    struct Lost { rc::LeaseRef ref; rc::LostReason why; int64_t at; };
    struct Risk { rc::LeaseRef ref; rc::AtRiskEdge edge; int64_t at; };
    std::vector<Lost> lost;
    std::vector<Risk> risk;
    std::function<void(rc::LeaseRef, rc::AtRiskEdge)> riskHook;   // test-only
    [[nodiscard]] int entered() const {
        return static_cast<int>(std::count_if(risk.begin(), risk.end(), [](const Risk& r) { return r.edge == rc::AtRiskEdge::Entered; }));
    }
    [[nodiscard]] int cleared() const {
        return static_cast<int>(std::count_if(risk.begin(), risk.end(), [](const Risk& r) { return r.edge == rc::AtRiskEdge::Cleared; }));
    }
};
struct OnLostF {
    Cb* cb;
    void operator()(rc::LeaseRef r, rc::LostReason w) const { cb->lost.push_back({r, w, g_now}); }
};
struct OnRiskF {
    Cb* cb;
    void operator()(rc::LeaseRef r, rc::AtRiskEdge e) const {
        cb->risk.push_back({r, e, g_now});
        if (cb->riskHook) cb->riskHook(r, e);
    }
};

struct Rec { std::vector<uint8_t> key, value; uint8_t attrs; };
std::vector<Rec> recordsOf(const std::vector<uint8_t>& frame) {
    std::vector<Rec> out;
    const auto v = rc::codec::decodeProduce(frame.data(), frame.size());
    EXPECT_TRUE(v.has_value());
    if (!v) return out;
    rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
    EXPECT_TRUE(r.ensureValid());
    r.forEachRecord([&](std::string_view k, std::string_view val, int64_t, int64_t, uint8_t attrs) {
        out.push_back({{k.begin(), k.end()}, {val.begin(), val.end()}, attrs});
    });
    return out;
}
int64_t le64(const std::vector<uint8_t>& v, std::size_t at) {
    int64_t x = 0;
    std::memcpy(&x, v.data() + at, 8);
    return x;
}
std::vector<uint8_t> keyBytes(uint64_t id) {
    std::vector<uint8_t> k(8);
    std::memcpy(k.data(), &id, 8);
    return k;
}

template <uint32_t Cap>
struct SRig {
    using Session = rc::LeaseSession<Cap, OnLostF, OnRiskF>;
    std::unique_ptr<rc::Client>        client;
    test_support::InvokerGateway       gw;
    Cb                                 cb;
    std::optional<rc::LeaseTopic<Row>> topic;
    std::unique_ptr<Session>           session;
    int64_t                            t0 = 0;

    explicit SRig(const rc::ClientConfig& c, uint32_t partitions = 1, uint32_t maxDelayMs = 100, bool makeSession = true)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.idempotence            = true;
        gw.replySchemas           = true;
        gw.schemaBlob             = rune::codec::serialize(leaseDescriptor());
        gw.schemaTopicId          = 1;
        gw.schemaPartitionCount   = static_cast<int32_t>(partitions);
        gw.schemaLeaseViewTopicId = 9;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex = true;
        gw.schemaKeyFeatures = f;
        gw.schemaLease       = kSpec;
        gw.produceLeaseTrailer = [] {
            std::vector<uint8_t> t(16);
            const int64_t v = 77;
            const uint64_t ttl = 5000;
            std::memcpy(t.data(), &v, 8);
            std::memcpy(t.data() + 8, &ttl, 8);
            return t;
        }();
        connect();
        topic.emplace(openTopic());
        awaitProducer();
        if (makeSession) makeSessionNow(maxDelayMs);
        t0 = g_now;
    }
    void makeSessionNow(uint32_t maxDelayMs = 100) {
        rc::LeaseSessionOptions o;
        o.maxDelayMs = maxDelayMs;
        session = std::make_unique<Session>(*client, OnLostF{&cb}, OnRiskF{&cb}, o);
    }
    ~SRig() {
        session.reset();
        topic.reset();
    }
    template <class P>
    bool drive(P&& pred, int cap = 4000) {
        for (int i = 0; i < cap; ++i) {
            client->doWork();
            gw.pump();
            if (session) session->doWork(g_now);
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        for (int i = 0; i < 4000 && !gw.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(gw.ready());
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        for (int i = 0; i < 4000 && !f.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(f.ok()) << f.describe();
        for (int i = 0; i < 4000 && gw.metadataRequests == 0; ++i) { client->doWork(); gw.pump(); }
    }
    template <class Fn>
    void whilePumping(Fn&& fn) {
        int stop[2];
        ASSERT_EQ(::pipe(stop), 0);
        std::thread t([&] {
            pollfd pfd{stop[0], POLLIN, 0};
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (::poll(&pfd, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) gw.pump();
        });
        const auto release = [&] {
            const char b = 0;
            (void)!::write(stop[1], &b, 1);
            t.join();
            ::close(stop[0]);
            ::close(stop[1]);
        };
        try { fn(); } catch (...) { release(); throw; }
        release();
    }
    rc::LeaseTopic<Row> openTopic() {
        std::optional<rc::LeaseTopic<Row>> h;
        whilePumping([&] { h.emplace(client->openLeaseTopicAndWait<Row>("lease_t", {})); });
        EXPECT_TRUE(h.has_value());
        return std::move(*h);
    }
    void awaitProducer() {
        for (int i = 0; i < 4000 && topic->producer().producerId() < 0; ++i) { client->doWork(); gw.pump(); }
        ASSERT_GE(topic->producer().producerId(), 0);
    }
    /// Runs `rounds` full hops of client / stub / session at the current clock.
    void settle(int rounds = 10) {
        // The client sees the clock creep forward 20 us per round (so a producer's 100 us linger elapses); the session and the
        // callbacks always see the exact instant.
        for (int i = 0; i < rounds; ++i) {
            const int64_t base = g_now;
            g_now              = base + i * 20'000;
            client->doWork();
            g_now = base;
            gw.pump();
            if (session) session->doWork(g_now);
        }
    }
    void at(int64_t t) {
        g_now = t;
        settle();
    }
    [[nodiscard]] rc::LeaseRef add(uint64_t id, int64_t version = 5, uint64_t ttlMs = 3000, int64_t grantNs = 0, rc::OpResult* err = nullptr) {
        return session->add(*topic, Row{id}, version, ttlMs, grantNs != 0 ? grantNs : g_now, err);
    }
    [[nodiscard]] std::size_t kaCount() const { return gw.kaSeen.size(); }
};

bool hasKey(const test_support::InvokerGateway::KaSeen& k, uint64_t id) {
    const auto kb = keyBytes(id);
    return std::any_of(k.entries.begin(), k.entries.end(), [&](const auto& e) { return e.key == kb; });
}

std::size_t revokeFrames(const test_support::InvokerGateway& gw, std::size_t fromFrame = 0) {
    std::size_t n = 0;
    for (std::size_t i = fromFrame; i < gw.produceFrameBytes.size(); ++i)
        for (const Rec& r : recordsOf(gw.produceFrameBytes[i]))
            if (r.attrs == 0x06) ++n;
    return n;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------

TEST(LeaseSessionRegression, CadenceTtlThirdRegression) {
    SRig<4> r(invokerCfg(5001, 5002));
    r.gw.kaAutoOk = true;
    const int64_t t0 = g_now;
    rc::OpResult err = rc::OpResult::Accepted;
    const auto ref = r.add(1, 5, 3000, t0, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);
    ASSERT_NE(ref.slot, 0xFFFFFFFFu);
    for (int i = 1; i <= 6; ++i) {
        r.at(t0 + i * 1000 * kMs - 1);
        EXPECT_EQ(r.kaCount(), static_cast<std::size_t>(i - 1)) << "no keepalive before the ttl/3 deadline " << i;
        r.at(t0 + i * 1000 * kMs);
        EXPECT_EQ(r.kaCount(), static_cast<std::size_t>(i)) << "exactly one keepalive at grant + " << i << " s";
    }
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_EQ(r.cb.entered(), 0);
    ASSERT_FALSE(r.gw.kaSeen.empty());
    EXPECT_EQ(r.gw.kaSeen[0].entries.size(), 1u);
    EXPECT_EQ(r.gw.kaSeen[0].entries[0].version, 5);
    EXPECT_EQ(r.gw.kaSeen[0].entries[0].key, keyBytes(1));
}

TEST(LeaseSessionRegression, BatchingPerTopicPartitionRegression) {
    SRig<64> r(invokerCfg(5003, 5004), /*partitions=*/8);
    // a second lease topic (another topic id) on the same client
    r.gw.schemaTopicId = 2;
    auto topicB = r.openTopic();
    r.gw.kaAutoOk = true;
    const int64_t t0 = g_now;
    std::vector<std::set<uint64_t>> perPart[2];
    perPart[0].resize(8);
    perPart[1].resize(8);
    for (uint64_t id = 1; id < 100000; ++id) {
        const auto pa = static_cast<std::size_t>(r.topic->partitionOf(Row{id}));
        auto& a = perPart[0][pa];
        if (a.size() < 3) a.insert(id);
        const auto pb = static_cast<std::size_t>(topicB.partitionOf(Row{id}));
        auto& b = perPart[1][pb];
        if (b.size() < 3) b.insert(id);
        bool full = true;
        for (int t = 0; t < 2; ++t) for (auto& s : perPart[t]) full = full && s.size() == 3;
        if (full) break;
    }
    for (std::size_t p = 0; p < 8; ++p) {
        for (uint64_t id : perPart[0][p]) ASSERT_NE(r.session->add(*r.topic, Row{id}, 5, 3000, t0).slot, 0xFFFFFFFFu);
        for (uint64_t id : perPart[1][p]) ASSERT_NE(r.session->add(topicB, Row{id}, 5, 3000, t0).slot, 0xFFFFFFFFu);
    }
    for (int period = 1; period <= 2; ++period) {
        const std::size_t before = r.kaCount();
        r.at(t0 + period * 1000 * kMs - 1);
        EXPECT_EQ(r.kaCount(), before);
        r.at(t0 + period * 1000 * kMs);
        ASSERT_EQ(r.kaCount() - before, 16u) << "one request per (topic, partition)";
        std::set<std::pair<int32_t, int32_t>> seen;
        for (std::size_t i = before; i < r.kaCount(); ++i) {
            EXPECT_EQ(r.gw.kaSeen[i].entries.size(), 3u);
            seen.insert({r.gw.kaSeen[i].topicId, r.gw.kaSeen[i].partition});
        }
        EXPECT_EQ(seen.size(), 16u);
    }
}

TEST(LeaseSessionRegression, SpreadNotBurstyRegression) {
    SRig<320> r(invokerCfg(5005, 5006));
    r.gw.kaAutoOk = true;
    const int64_t base = g_now;
    constexpr int kKeys = 300;
    g_now = base + 1000 * kMs;   // every grant is in the past
    for (int i = 0; i < kKeys; ++i)
        ASSERT_NE(r.session->add(*r.topic, Row{static_cast<uint64_t>(i + 1)}, 5, 3000, base + i * (1000 * kMs / kKeys)).slot, 0xFFFFFFFFu);
    // 10 periods, stepped 10 ms; a period is [start + k s, start + (k+1) s)
    const int64_t start = g_now;
    std::vector<int> perPeriod(11, 0);
    std::size_t seen = r.kaCount();
    std::size_t maxEntries = 0;
    for (int step = 1; step <= 1100; ++step) {
        r.at(start + step * 10 * kMs);
        for (; seen < r.kaCount(); ++seen) {
            maxEntries = std::max(maxEntries, r.gw.kaSeen[seen].entries.size());
            const int idx = static_cast<int>((g_now - start - 1) / (1000 * kMs));
            if (idx >= 0 && idx < 11) ++perPeriod[static_cast<std::size_t>(idx)];
        }
    }
    EXPECT_LE(maxEntries, 80u) << "the spread is kept: no request carries the whole fleet";
    int total = 0;
    for (int p = 1; p <= 9; ++p) {
        EXPECT_GE(perPeriod[static_cast<std::size_t>(p)], 3) << "period " << p;   // 300 keys in requests of <= ~80: ~4 per period
        EXPECT_LE(perPeriod[static_cast<std::size_t>(p)], 5) << "period " << p;
        total += perPeriod[static_cast<std::size_t>(p)];
    }
    EXPECT_GE(total, 33) << "no collapse into a burst, no starvation over 9 periods";
    EXPECT_TRUE(r.cb.lost.empty());
}

TEST(LeaseSessionRegression, MaxDelayValidationRegression) {
    {
        SRig<4> r(invokerCfg(5007, 5008), 1, /*maxDelayMs=*/479);
        rc::OpResult err = rc::OpResult::Accepted;
        EXPECT_NE(r.add(1, 5, 1000, 0, &err).slot, 0xFFFFFFFFu) << "500 + 479 < 980";
        EXPECT_EQ(err, rc::OpResult::Accepted);
    }
    {
        SRig<4> r(invokerCfg(5009, 5010), 1, /*maxDelayMs=*/480);
        rc::OpResult err = rc::OpResult::Accepted;
        EXPECT_EQ(r.add(1, 5, 1000, 0, &err).slot, 0xFFFFFFFFu) << "500 + 480 >= 980";
        EXPECT_EQ(err, rc::OpResult::InvalidArgument);
        EXPECT_EQ(r.session->liveCount(), 0u);
    }
    SRig<4> r(invokerCfg(5011, 5012), 128);
    rc::OpResult err = rc::OpResult::Accepted;
    EXPECT_EQ(r.add(1, 5, 0, 0, &err).slot, 0xFFFFFFFFu);   // ttl 0
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
    err = rc::OpResult::Accepted;
    EXPECT_EQ(r.add(1, 5, 60001, 0, &err).slot, 0xFFFFFFFFu);   // above maxTtl
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
    const uint8_t k0[1] = {0};
    EXPECT_EQ(r.session->add(r.topic->core(), std::span<const uint8_t>{}, 5, 3000, g_now, &err).slot, 0xFFFFFFFFu);   // empty key
    std::vector<uint8_t> wide(129, 7);
    err = rc::OpResult::Accepted;
    EXPECT_EQ(r.session->add(r.topic->core(), wide, 5, 3000, g_now, &err).slot, 0xFFFFFFFFu);   // 129-byte key
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
    std::vector<uint8_t> k128(128, 7);
    EXPECT_NE(r.session->add(r.topic->core(), k128, 5, 3000, g_now, &err).slot, 0xFFFFFFFFu);   // 128 is fine
    (void)k0;
    // slot overflow: Capacity 4
    for (uint64_t id = 1; id <= 3; ++id) EXPECT_NE(r.add(id).slot, 0xFFFFFFFFu);
    err = rc::OpResult::Accepted;
    EXPECT_EQ(r.add(4, 5, 3000, 0, &err).slot, 0xFFFFFFFFu) << "the 5th add: no free slot";
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
    // the 65th group (64 groups = 64 distinct (topic, partition) pairs)
    SRig<100> g(invokerCfg(5013, 5014), 128);
    std::set<int32_t> parts;
    std::vector<uint64_t> ids;
    for (uint64_t id = 1; id < 1'000'000 && ids.size() < 65; ++id) {
        if (parts.insert(g.topic->partitionOf(Row{id})).second) ids.push_back(id);
    }
    ASSERT_EQ(ids.size(), 65u);
    for (std::size_t i = 0; i < 64; ++i) ASSERT_NE(g.add(ids[i]).slot, 0xFFFFFFFFu) << i;
    err = rc::OpResult::Accepted;
    EXPECT_EQ(g.add(ids[64], 5, 3000, 0, &err).slot, 0xFFFFFFFFu) << "the 65th group";
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
}

TEST(LeaseSessionRegression, OnLostEachTriggerRegression) {
    SRig<8> r(invokerCfg(5015, 5016));
    const auto ttl = 3000 * kMs;
    const int64_t margin = ttl - ttl / 50 - 100 * kMs;   // M = grant + this
    // ---- MarginExpired, connected: no answers at all
    {
        const int64_t t = g_now;
        const auto ref = r.add(1, 5, 3000, t);
        const std::size_t frames = r.gw.produceFrameBytes.size();
        r.at(t + margin - 1);
        EXPECT_TRUE(r.cb.lost.empty());
        r.at(t + margin);
        ASSERT_EQ(r.cb.lost.size(), 1u);
        EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::MarginExpired);
        EXPECT_EQ(r.cb.lost[0].ref, ref);
        EXPECT_EQ(r.cb.lost[0].at, t + margin) << "onLost fires at M exactly";
        r.settle();
        EXPECT_EQ(revokeFrames(r.gw, frames), 1u) << "one best-effort revoke";
        const std::size_t ka = r.kaCount();
        r.at(t + margin + 3000 * kMs);
        for (std::size_t i = ka; i < r.kaCount(); ++i) EXPECT_FALSE(hasKey(r.gw.kaSeen[i], 1)) << "no request carries a lost key";
        EXPECT_EQ(r.cb.lost.size(), 1u) << "exactly once";
    }
    // ---- NotFound: per-entry 62
    {
        r.cb.lost.clear();
        const int64_t t = g_now;
        (void)r.add(2, 5, 3000, t);
        const std::size_t ka = r.kaCount(), frames = r.gw.produceFrameBytes.size();
        r.at(t + 1000 * kMs);
        ASSERT_EQ(r.kaCount(), ka + 1);
        const test_support::InvokerGateway::KaAnswer a{0, 62, 0};
        r.gw.answerKa(ka, std::span(&a, 1), true);
        r.settle();
        ASSERT_EQ(r.cb.lost.size(), 1u);
        EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::NotFound);
        EXPECT_EQ(revokeFrames(r.gw, frames), 0u) << "the lease is gone: no revoke";
    }
    // ---- TopicDropped: per-entry UnknownTopic, then a whole-request UnknownTopic
    for (int whole = 0; whole < 2; ++whole) {
        r.cb.lost.clear();
        const int64_t t = g_now;
        (void)r.add(3, 5, 3000, t);
        const std::size_t ka = r.kaCount(), frames = r.gw.produceFrameBytes.size();
        r.at(t + 1000 * kMs);
        ASSERT_EQ(r.kaCount(), ka + 1);
        if (whole == 0) {
            const test_support::InvokerGateway::KaAnswer a{0, static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic), 0};
            r.gw.answerKa(ka, std::span(&a, 1), true);
        } else {
            r.gw.answerKaWhole(ka, static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic), 0);
        }
        r.settle();
        ASSERT_EQ(r.cb.lost.size(), 1u) << "whole=" << whole;
        EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::TopicDropped);
        EXPECT_EQ(revokeFrames(r.gw, frames), 0u);
    }
    // ---- the watched reasons: a version mismatch is ignored; a match is lost and revoked
    const rc::LostReason watched[] = {rc::LostReason::WatchedRevoked, rc::LostReason::WatchedExpired, rc::LostReason::WatchedVanished};
    for (const auto why : watched) {
        r.cb.lost.clear();
        const int64_t t = g_now;
        (void)r.add(4, 9, 3000, t);
        const std::size_t frames = r.gw.produceFrameBytes.size();
        const auto key = keyBytes(4);
        r.session->onWatchedEnd(1, r.topic->partitionOf(Row{4}), key, 8, why);
        r.settle();
        EXPECT_TRUE(r.cb.lost.empty()) << "version - 1 is another lease";
        r.session->onWatchedEnd(1, r.topic->partitionOf(Row{4}), keyBytes(99), 9, why);
        r.settle();
        EXPECT_TRUE(r.cb.lost.empty()) << "another key";
        r.session->onWatchedEnd(1, r.topic->partitionOf(Row{4}), key, 9, why);
        r.settle();
        ASSERT_EQ(r.cb.lost.size(), 1u);
        EXPECT_EQ(r.cb.lost[0].why, why);
        EXPECT_EQ(revokeFrames(r.gw, frames), 1u);
        EXPECT_EQ(r.session->liveCount(), 0u);
    }
    // ---- MarginExpired, disconnected: no revoke (the last leg: the session stays down)
    {
        r.cb.lost.clear();
        const int64_t t = g_now;
        (void)r.add(5, 5, 3000, t);
        const std::size_t frames = r.gw.produceFrameBytes.size();
        r.gw.expireSession(r.client->testSeedSessionId());
        r.gw.pauseIngress = true;
        r.at(t + 100 * kMs);
        ASSERT_NE(r.client->state(), rc::session::SessionState::Connected);
        r.at(t + margin);
        ASSERT_EQ(r.cb.lost.size(), 1u);
        EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::MarginExpired);
        r.settle();
        EXPECT_EQ(revokeFrames(r.gw, frames), 0u) << "disconnected: no best-effort revoke";
    }
}

TEST(LeaseSessionRegression, SessionRefusedOnLostRegression) {
    auto cfg = invokerCfg(5017, 5018);
    cfg.maxLeaseSessions = 0;   // the duty side has no block for this session
    SRig<4> r(cfg);
    rc::OpResult err = rc::OpResult::Accepted;
    ASSERT_NE(r.add(1, 5, 3000, 0, &err).slot, 0xFFFFFFFFu);
    r.settle();
    ASSERT_EQ(r.cb.lost.size(), 1u);
    EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::SessionRefused);
    EXPECT_EQ(r.add(2, 5, 3000, 0, &err).slot, 0xFFFFFFFFu) << "a refused session refuses every later add";
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
}

namespace {

// One at-risk leg: the lease is added at t; `setup` runs at t + 100 ms; `answer` runs when the keepalive at t + 1 s was sent (and
// again after each later request); Entered must fire at exactly t + 1500 ms (or at the first session pass after, for a park).
enum class Cause { NotLeader, Throttled, NamespaceSuspended, Timeout, NoAnswer, Disconnect, Park };

void runAtRiskLeg(SRig<8>& r, Cause cause, uint64_t id) {
    r.cb.risk.clear();
    r.cb.lost.clear();
    const int64_t t = g_now;
    const auto ref = r.add(id, 5, 3000, t);
    ASSERT_NE(ref.slot, 0xFFFFFFFFu);
    const std::size_t ka0 = r.kaCount();
    auto answerCode = [&](uint16_t code, uint64_t aux) {
        for (std::size_t i = ka0; i < r.kaCount(); ++i) {
            if (!hasKey(r.gw.kaSeen[i], id)) continue;
            const test_support::InvokerGateway::KaAnswer a{0, code, aux};
            r.gw.answerKa(i, std::span(&a, 1), true);
        }
    };
    auto stepTo = [&](int64_t when, uint16_t code, uint64_t aux) {
        // 50 ms steps so each retry fires; a retriable code answers each request as it appears
        for (int64_t x = g_now; x < when;) {
            x = std::min(when, x + 50 * kMs);
            r.at(x);
            if (code != 0) { answerCode(code, aux); r.settle(); }
        }
    };
    switch (cause) {
    case Cause::NotLeader:          stepTo(t + 1500 * kMs, static_cast<uint16_t>(rpc::ErrorCode::NotLeader), 0); break;
    case Cause::Throttled:          stepTo(t + 1500 * kMs, static_cast<uint16_t>(rpc::ErrorCode::Throttled), 100); break;
    case Cause::NamespaceSuspended: stepTo(t + 1500 * kMs, static_cast<uint16_t>(rpc::ErrorCode::NamespaceSuspended), 0); break;
    case Cause::Timeout:
    case Cause::NoAnswer:
        r.at(t + 1000 * kMs);
        stepTo(t + 1500 * kMs, 0, 0);
        break;
    case Cause::Disconnect:
        r.at(t + 500 * kMs);
        r.gw.expireSession(r.client->testSeedSessionId());
        r.gw.pauseIngress = true;
        stepTo(t + 1500 * kMs, 0, 0);
        break;
    case Cause::Park: {
        r.at(t + 900 * kMs);
        g_now = t + 1500 * kMs;
        for (int i = 0; i < 10; ++i) { r.client->doWork(); r.gw.pump(); }   // the client trips the guard; the session is parked
        g_now = t + 1600 * kMs;
        r.settle();
        break;
    }
    }
    ASSERT_EQ(r.cb.entered(), 1) << "cause " << static_cast<int>(cause);
    EXPECT_EQ(r.cb.risk[0].at, cause == Cause::Park ? t + 1600 * kMs : t + 1500 * kMs) << "cause " << static_cast<int>(cause);
    EXPECT_EQ(r.cb.risk[0].ref, ref);
    EXPECT_EQ(r.session->stateOf(ref), rc::LeaseEntryState::AtRisk);
    if (cause == Cause::Disconnect) {
        r.gw.pauseIngress = false;
        ASSERT_TRUE(r.drive([&] { return r.client->state() == rc::session::SessionState::Connected; }, 40000));
    }
    // an answered keepalive sent after t: Ok clears the episode
    const std::size_t before = r.kaCount();
    int64_t waitUntil = g_now;
    std::size_t idx = r.kaCount();
    for (int i = 0; i < 400 && idx == r.kaCount(); ++i) {
        waitUntil += 50 * kMs;
        r.at(waitUntil);
        for (std::size_t k = before; k < r.kaCount(); ++k) if (hasKey(r.gw.kaSeen[k], id)) idx = k;
        if (idx != r.kaCount() && !hasKey(r.gw.kaSeen[idx], id)) idx = r.kaCount();
    }
    // the newest request that carries the key
    std::size_t last = r.kaCount();
    for (std::size_t k = r.kaCount(); k-- > before;) if (hasKey(r.gw.kaSeen[k], id)) { last = k; break; }
    ASSERT_LT(last, r.kaCount()) << "a keepalive was sent while at risk (cause " << static_cast<int>(cause) << ")";
    const test_support::InvokerGateway::KaAnswer ok{0, 0, 3000};
    r.gw.answerKa(last, std::span(&ok, 1), true);
    r.settle();
    EXPECT_EQ(r.cb.cleared(), 1) << "cause " << static_cast<int>(cause);
    EXPECT_TRUE(r.cb.lost.empty()) << "cause " << static_cast<int>(cause);
    EXPECT_EQ(r.session->stateOf(ref), rc::LeaseEntryState::Held);
    EXPECT_EQ(r.session->remove(ref), rc::OpResult::Accepted);
    r.settle();
}

}  // namespace

TEST(LeaseSessionRegression, AtRiskEachCauseRegression) {
    SRig<8> r(invokerCfg(5019, 5020));
    uint64_t id = 10;
    for (const Cause c : {Cause::NotLeader, Cause::Throttled, Cause::NamespaceSuspended, Cause::Timeout, Cause::NoAnswer, Cause::Park,
                          Cause::Disconnect}) {
        runAtRiskLeg(r, c, id++);
        if (::testing::Test::HasFatalFailure()) return;
    }
}

TEST(LeaseSessionRegression, AtRiskOncePerEpisodeRegression) {
    SRig<4> r(invokerCfg(5021, 5022));
    const int64_t t = g_now;
    const auto ref = r.add(1, 5, 3000, t);
    const std::size_t ka0 = r.kaCount();
    // episode 1: three failing answers, all inside it
    std::size_t answered = ka0;
    for (int64_t x = t; x < t + 1900 * kMs; x += 50 * kMs) {
        r.at(x);
        for (; answered < r.kaCount(); ++answered) {
            const test_support::InvokerGateway::KaAnswer a{0, static_cast<uint16_t>(rpc::ErrorCode::NotLeader), 0};
            r.gw.answerKa(answered, std::span(&a, 1), true);
        }
    }
    ASSERT_GE(answered - ka0, 3u);
    EXPECT_EQ(r.cb.entered(), 1) << "three failing answers: one Entered";
    EXPECT_EQ(r.cb.cleared(), 0);
    // Ok for the newest request: Cleared
    const test_support::InvokerGateway::KaAnswer ok{0, 0, 3000};
    for (int i = 0; i < 100 && r.kaCount() == answered; ++i) r.at(g_now + 50 * kMs);   // a request that is still unanswered
    ASSERT_GT(r.kaCount(), answered);
    r.gw.answerKa(r.kaCount() - 1, std::span(&ok, 1), true);
    r.settle();
    EXPECT_EQ(r.cb.cleared(), 1);
    // episode 2: nothing answers until the new deadline passes
    const int64_t okSent = g_now;
    (void)okSent;
    answered = r.kaCount();
    const int64_t episode2End = g_now + 2000 * kMs;   // fixed: r.at(x) moves g_now, so the bound must not read it
    for (int64_t x = g_now; x < episode2End;) {
        x += 50 * kMs;
        r.at(x);
        for (; answered < r.kaCount(); ++answered) {
            const test_support::InvokerGateway::KaAnswer a{0, static_cast<uint16_t>(rpc::ErrorCode::NotLeader), 0};
            r.gw.answerKa(answered, std::span(&a, 1), true);
        }
        if (r.cb.entered() == 2) break;
    }
    EXPECT_EQ(r.cb.entered(), 2) << "a second episode enters again";
    EXPECT_EQ(r.cb.risk.size(), 3u);
    EXPECT_TRUE(r.cb.lost.empty());
    (void)ref;
}

// ---------------------------------------------------------------------------------------------------------------------------
// The guard against real producers
// ---------------------------------------------------------------------------------------------------------------------------

namespace {

rc::ProducerConfig guardedCfg() {
    rc::ProducerConfig c;
    c.acks              = rc::Acks::Leader;
    c.lingerUs          = 60'000'000;   // never lingers out inside a test
    c.batchBytes        = 64 * 1024;
    c.maxInFlight       = 8;
    c.produceTimeoutMs  = 60'000;
    c.deliveryTimeoutMs = 120'000;
    return c;
}
const std::vector<uint8_t> kVal(16, 0x5A);

struct Drain {
    int failed = 0, ok = 0, other = 0;
    void run(rc::Completions& q) {
        (void)q.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.status == OpStatus::DeliveryFailed && c.errorCode == static_cast<uint16_t>(rc::ClientErrorCode::LeaseAtRisk)) ++failed;
            else if (c.status == OpStatus::Ok) ++ok;
            else ++other;
        });
    }
};

template <class R>
void awaitProducerId(R& r, rc::Producer& p) {
    for (int i = 0; i < 4000 && p.producerId() < 0; ++i) { r.client->doWork(); r.gw.pump(); }
    ASSERT_GE(p.producerId(), 0);
}

}  // namespace

TEST(LeaseSessionRegression, GuardFailsPendingAndUnsentRegression) {
    SRig<4> r(invokerCfg(5023, 5024));
    r.gw.idempotence = false;   // the idempotence path of the stub does not hold acks
    r.gw.replyAcks = false;   // the broker holds the acks
    rc::Producer p = r.client->testProducerWithRingCap(guardedCfg(), 64 * 1024);
    awaitProducerId(r, p);
    rc::Completions pq(*r.client);
    const rc::TopicHandle topic{1, 1};
    const int64_t t = g_now;
    const auto ref = r.add(1, 5, 3000, t);
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    r.settle();
    rc::Token tok = 0;
    // 3 sent and unacked
    for (int i = 0; i < 3; ++i) ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    p.flush();
    r.settle();
    ASSERT_GE(r.gw.held.size(), 1u);
    // 2 in the builder (the linger is huge)
    for (int i = 0; i < 2; ++i) ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    r.client->doWork();
    // 1 in the ring: submitted just before the trip pass
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    Drain d;
    int failedInCallback = -1;
    r.cb.riskHook = [&](rc::LeaseRef, rc::AtRiskEdge e) {
        if (e == rc::AtRiskEdge::Entered) { d.run(pq); failedInCallback = d.failed; }
    };
    g_now = t + 1500 * kMs;
    r.settle();
    ASSERT_EQ(r.cb.entered(), 1);
    EXPECT_EQ(failedInCallback, 6) << "all six accepted records are already failed when onAtRisk(Entered) runs";
    EXPECT_EQ(d.other + d.ok, 0);
    // the broker's late acks of the three sent records: nothing more
    const std::vector<test_support::InvokerGateway::Produced> held = r.gw.held;
    for (const auto& pr : held) r.gw.ack(pr);
    r.settle();
    d.run(pq);
    EXPECT_EQ(d.failed, 6) << "a late ack of a failed batch produces no second completion";
    EXPECT_EQ(d.ok + d.other, 0);
}

TEST(LeaseSessionRegression, GuardBlocksReconnectResendRegression) {
    SRig<4> r(invokerCfg(5025, 5026));
    r.gw.idempotence = false;   // the idempotence path of the stub does not hold acks
    r.gw.replyAcks = false;
    rc::Producer p = r.client->testProducerWithRingCap(guardedCfg(), 64 * 1024);
    awaitProducerId(r, p);
    rc::Completions pq(*r.client);
    const rc::TopicHandle topic{1, 1};
    const int64_t t = g_now;
    const auto ref = r.add(1, 5, 3000, t);
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    r.settle();
    rc::Token tok = 0;
    Drain d;
    for (int i = 0; i < 2; ++i) ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    p.flush();
    r.settle();
    ASSERT_GE(r.gw.held.size(), 1u);
    r.at(t + 1500 * kMs);
    ASSERT_EQ(r.cb.entered(), 1);
    d.run(pq);
    EXPECT_EQ(d.failed, 2);
    const std::size_t frames = r.gw.produceFrameBytes.size();
    // disconnect and reconnect
    r.gw.expireSession(r.client->testSeedSessionId());
    ASSERT_TRUE(r.drive([&] { return r.client->state() != rc::session::SessionState::Connected; }));
    ASSERT_TRUE(r.drive([&] { return r.client->state() == rc::session::SessionState::Connected; }, 40000));
    r.settle(40);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames) << "the blocked producer re-sends nothing after the reconnect";
    // a new write fails at once, without a frame
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    p.flush();
    r.settle();
    d.run(pq);
    EXPECT_EQ(d.failed, 3);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames) << "a write while at risk: no frame";
    // Ok clears: writes go out again, with a sequence the stub's idempotence check accepts
    r.gw.replyAcks = true;
    std::size_t last = r.kaCount();
    for (std::size_t k = r.kaCount(); k-- > 0;) if (hasKey(r.gw.kaSeen[k], 1)) { last = k; break; }
    if (last == r.kaCount()) {   // a keepalive must be out by now; push time forward until one is
        for (int i = 0; i < 40 && last == r.kaCount(); ++i) {
            r.at(g_now + 50 * kMs);
            for (std::size_t k = r.kaCount(); k-- > 0;) if (hasKey(r.gw.kaSeen[k], 1)) { last = k; break; }
        }
    }
    ASSERT_LT(last, r.kaCount());
    const test_support::InvokerGateway::KaAnswer ok{0, 0, 3000};
    r.gw.answerKa(last, std::span(&ok, 1), true);
    r.settle();
    ASSERT_EQ(r.cb.cleared(), 1);
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    p.flush();
    r.settle();
    d.run(pq);
    EXPECT_EQ(d.ok, 1) << "after Cleared a write is sent and acked";
    EXPECT_GT(r.gw.produceFrameBytes.size(), frames);
    // ---- second leg: in-flight writes of the second episode are failed too, and a reconnect re-sends nothing
    r.gw.idempotence = false;   // the idempotence path of the stub does not hold acks
    r.gw.replyAcks = false;
    for (int i = 0; i < 2; ++i) ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    p.flush();
    r.settle();
    const std::size_t frames2 = r.gw.produceFrameBytes.size();
    const int failedBefore = d.failed;
    for (int i = 0; i < 400 && r.cb.entered() < 2; ++i) r.at(g_now + 50 * kMs);
    ASSERT_EQ(r.cb.entered(), 2);
    d.run(pq);
    EXPECT_EQ(d.failed, failedBefore + 2) << "the second trip fails the writes accepted since the first rearm";
    r.gw.expireSession(r.client->testSeedSessionId());
    ASSERT_TRUE(r.drive([&] { return r.client->state() != rc::session::SessionState::Connected; }));
    ASSERT_TRUE(r.drive([&] { return r.client->state() == rc::session::SessionState::Connected; }, 40000));
    r.settle(40);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames2);
}

TEST(LeaseSessionRegression, GuardNineProducersRefusedRegression) {
    SRig<4> r(invokerCfg(5027, 5028));
    const auto ref = r.add(1);
    std::vector<rc::Producer> ps;
    for (int i = 0; i < 9; ++i) ps.push_back(r.client->producer(guardedCfg()));
    for (int i = 0; i < 8; ++i) EXPECT_EQ(r.session->bindProducer(ref, ps[static_cast<std::size_t>(i)]), rc::OpResult::Accepted) << i;
    EXPECT_EQ(r.session->bindProducer(ref, ps[8]), rc::OpResult::InvalidArgument) << "the 9th producer";
    // a producer of another client
    auto other = invokerCfg(5029, 5030);
    rc::Client c2(other);
    rc::Producer foreign = c2.producer(guardedCfg());
    EXPECT_EQ(r.session->bindProducer(ref, foreign), rc::OpResult::InvalidArgument);
    r.settle();
}

TEST(LeaseSessionRegression, LocalOnLostRevokesBestEffortRegression) {
    SRig<4> r(invokerCfg(5031, 5032));
    const auto ttl = 3000 * kMs;
    const int64_t margin = ttl - ttl / 50 - 100 * kMs;
    const int64_t t = g_now;
    const auto ref = r.add(7, 11, 3000, t);
    const std::size_t frames = r.gw.produceFrameBytes.size();
    r.at(t + margin);
    ASSERT_EQ(r.cb.lost.size(), 1u);
    r.settle();
    ASSERT_EQ(revokeFrames(r.gw, frames), 1u);
    std::vector<Rec> revs;
    for (std::size_t i = frames; i < r.gw.produceFrameBytes.size(); ++i)
        for (const Rec& rec : recordsOf(r.gw.produceFrameBytes[i])) if (rec.attrs == 0x06) revs.push_back(rec);
    ASSERT_EQ(revs.size(), 1u);
    EXPECT_EQ(revs[0].key, keyBytes(7));
    EXPECT_EQ(le64(revs[0].value, 4), 11) << "fenced at the held version";
    // the broker answers 62 for the revoke: nothing else happens
    r.gw.produceVerdicts = verdicts({62});
    r.gw.replyAcks = true;
    for (const auto& pr : r.gw.held) r.gw.ack(pr);
    r.settle();
    EXPECT_EQ(r.cb.lost.size(), 1u);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames + 1);
    // the slot is reused with a new gen
    r.gw.produceVerdicts.reset();
    const auto again = r.add(8, 3, 3000, g_now);
    EXPECT_EQ(again.slot, ref.slot);
    EXPECT_NE(again.gen, ref.gen);
    EXPECT_EQ(r.session->stateOf(ref), rc::LeaseEntryState::Free);
    EXPECT_EQ(r.session->stateOf(again), rc::LeaseEntryState::Held);
}

TEST(LeaseSessionRegression, CloseRevokesAllRegression) {
    SRig<8> r(invokerCfg(5033, 5034));
    r.gw.idempotence = false;   // the idempotence path of the stub does not hold acks
    r.gw.replyAcks = false;
    const int64_t t = g_now;
    for (uint64_t id = 1; id <= 5; ++id) ASSERT_NE(r.add(id, 20 + static_cast<int64_t>(id), 3000, t).slot, 0xFFFFFFFFu);
    const std::size_t frames = r.gw.produceFrameBytes.size();
    const std::size_t ka = r.kaCount();
    r.session->close();
    EXPECT_FALSE(r.session->isClosed());
    r.at(g_now + 5 * kMs);
    r.settle(30);
    EXPECT_EQ(revokeFrames(r.gw, frames), 5u) << "five fenced revokes";
    EXPECT_FALSE(r.session->isClosed()) << "not closed until the revokes are answered";
    // one of the five answers 62
    std::vector<test_support::InvokerGateway::Produced> held = r.gw.held;
    ASSERT_FALSE(held.empty());
    r.gw.produceVerdicts = verdicts({0, 0, 62, 0, 0});
    r.gw.replyAcks = true;
    for (const auto& pr : held) r.gw.ack(pr);
    r.settle(30);
    EXPECT_TRUE(r.session->isClosed());
    EXPECT_EQ(r.session->liveCount(), 0u);
    r.at(g_now + 5000 * kMs);
    EXPECT_EQ(r.kaCount(), ka) << "no keepalive after close";
    EXPECT_TRUE(r.cb.lost.empty()) << "a close is not a loss";
    rc::OpResult err = rc::OpResult::Accepted;
    EXPECT_EQ(r.add(9, 5, 3000, 0, &err).slot, 0xFFFFFFFFu);
    EXPECT_EQ(err, rc::OpResult::InvalidArgument);
}

TEST(LeaseSessionRegression, StaleRefNoopRegression) {
    SRig<4> r(invokerCfg(5035, 5036));
    const auto ref = r.add(1);
    rc::Producer p = r.client->producer(guardedCfg());
    // an Ended for the old generation arrives after the slot was reused
    r.session->onWatchedEnd(1, r.topic->partitionOf(Row{1}), keyBytes(1), 5, rc::LostReason::WatchedRevoked);   // Losing; Ended is queued
    ASSERT_EQ(r.session->remove(ref), rc::OpResult::Accepted);
    const auto fresh = r.add(2);
    ASSERT_EQ(fresh.slot, ref.slot);
    ASSERT_NE(fresh.gen, ref.gen);
    const std::size_t frames = r.gw.produceFrameBytes.size(), ka = r.kaCount();
    r.settle();
    EXPECT_TRUE(r.cb.lost.empty()) << "a removed lease never calls back";
    EXPECT_EQ(r.session->stateOf(fresh), rc::LeaseEntryState::Held) << "the late Ended of the old gen does not touch the new lease";
    // every call on the stale ref is a no-op
    EXPECT_EQ(r.session->remove(ref), rc::OpResult::InvalidArgument);
    EXPECT_EQ(r.session->revoke(ref), rc::OpResult::InvalidArgument);
    const std::vector<uint8_t> row(44, 0);
    EXPECT_EQ(r.session->update(ref, row, rc::CompletionTarget(r.session->core().queue()), 1), rc::OpResult::InvalidArgument);
    EXPECT_EQ(r.session->bindProducer(ref, p), rc::OpResult::InvalidArgument);
    r.settle(30);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames);
    EXPECT_EQ(r.kaCount(), ka);
    EXPECT_EQ(r.session->liveCount(), 1u);
}

TEST(LeaseSessionRegression, SessionNoAllocRegression) {
    SRig<128> r(invokerCfg(5037, 5038));
    r.gw.kaAutoOk = true;
    r.cb.risk.reserve(8192);   // the test's own recorder must not allocate inside the window
    r.cb.lost.reserve(256);
    r.gw.replyAcks = true;
    rc::Producer p = r.client->producer(guardedCfg());
    awaitProducerId(r, p);
    rc::Completions pq(*r.client);
    const rc::TopicHandle topic{1, 1};
    const int64_t t = g_now;
    rc::LeaseRef first{};
    for (uint64_t id = 1; id <= 100; ++id) {
        const auto ref = r.add(id, 5, 3000, t);
        ASSERT_NE(ref.slot, 0xFFFFFFFFu);
        if (id == 1) first = ref;
    }
    ASSERT_EQ(r.session->bindProducer(first, p), rc::OpResult::Accepted);
    long allocs = 0, clientAllocs = 0, sessionAllocs = 0;
    auto counted = [&](long& bucket, auto&& fn) {
        alloc_counter::startThisThread();
        fn();
        const long n = alloc_counter::stopThisThread();
        allocs += n;
        bucket += n;
    };
    auto step = [&](int64_t when) {
        g_now = when;
        for (int i = 0; i < 6; ++i) {
            counted(clientAllocs, [&] { r.client->doWork(); });
            r.gw.pump();   // the stub allocates: outside the window
            counted(sessionAllocs, [&] { r.session->doWork(g_now); });
        }
    };
    int64_t x = t;
    auto period = [&] {
        for (int i = 0; i < 20; ++i) { x += 50 * kMs; step(x); }
        (void)pq.poll([](const rc::Completion&, std::span<const uint8_t>) {});
    };
    // One cycle: 3 periods of auto-Ok, a 1.6 s stall (no answers: an at-risk episode, but short of any margin), then every request
    // left unanswered is answered Ok at once (the catch-up sends the largest batches the session ever makes).
    r.gw.kaRecord = true;
    auto cycle = [&] {
        r.gw.kaAutoOk = true;
        for (int i = 0; i < 3; ++i) period();
        r.gw.kaAutoOk = false;
        const std::size_t from = r.gw.kaSeen.size();
        for (int k = 0; k < 32; ++k) { x += 50 * kMs; step(x); }
        for (std::size_t q = from; q < r.gw.kaSeen.size(); ++q) r.gw.answerKaWhole(q, 0, 3000);
        r.gw.kaAutoOk = true;
        for (int i = 0; i < 2; ++i) period();
    };
    cycle();
    cycle();   // warm-up: the high-water marks (frame buffers, partition state) are reached
    allocs = clientAllocs = sessionAllocs = 0;
    const int enteredBefore = r.cb.entered();
    cycle();
    cycle();
    EXPECT_GT(r.cb.entered(), enteredBefore) << "an at-risk episode ran inside the window";
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_EQ(sessionAllocs, 0L) << "the session allocates nothing in steady state";
    EXPECT_EQ(clientAllocs, 0L) << "the client duty path and the guard allocate nothing in steady state";
    EXPECT_EQ(allocs, 0L);
    (void)topic;
}

TEST(LeaseSessionRegression, InvokerModeOrderRegression) {
    SRig<4> r(invokerCfg(5039, 5040));
    r.gw.idempotence = false;   // the idempotence path of the stub does not hold acks
    r.gw.replyAcks = false;
    rc::Producer p = r.client->producer(guardedCfg());
    awaitProducerId(r, p);
    rc::Completions pq(*r.client);
    const rc::TopicHandle topic{1, 1};
    const int64_t t = g_now;
    const auto ref = r.add(1, 5, 3000, t);
    ASSERT_EQ(r.session->bindProducer(ref, p), rc::OpResult::Accepted);
    rc::Token tok = 0;
    for (int i = 0; i < 4; ++i) ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    Drain d;
    int seen = -1;
    r.cb.riskHook = [&](rc::LeaseRef, rc::AtRiskEdge e) { if (e == rc::AtRiskEdge::Entered) { d.run(pq); seen = d.failed; } };
    g_now = t + 1500 * kMs;
    r.client->doWork();   // the guard trips here, in the client's duty cycle
    EXPECT_EQ(r.cb.entered(), 0) << "the session has not run yet";
    r.session->doWork(g_now);
    ASSERT_EQ(r.cb.entered(), 1);
    EXPECT_EQ(seen, 4) << "the producer's failures were delivered before the callback ran";
    // Direct ingress: a write while at risk is appended in place (appendRecord_) and must fail at once, with no frame.
    const std::size_t frames = r.gw.produceFrameBytes.size();
    const int failedBefore = d.failed;
    ASSERT_EQ(p.trySend(topic, rc::Key(uint64_t{1}), kVal, pq, ++tok), rc::SendResult::Ok);
    r.settle();   // no flush: only the append-time block (not the flush-time one) can fail it, linger being 60 s
    d.run(pq);
    EXPECT_EQ(d.failed, failedBefore + 1) << "a Direct-mode write while at risk fails at once with LeaseAtRisk";
    EXPECT_EQ(r.gw.produceFrameBytes.size(), frames);
}

// ---------------------------------------------------------------------------------------------------------------------------
// Client::leaseKeepAliveBatch (no session)
// ---------------------------------------------------------------------------------------------------------------------------

namespace {

struct BatchRig {
    SRig<4> r;
    rc::Completions q;
    std::vector<rc::Completion> got;
    explicit BatchRig(const rc::ClientConfig& c) : r(c, 1, 100, /*makeSession=*/false), q(*r.client) {}
    rc::OpResult issue(std::size_t n, rc::Token tok, uint32_t timeoutMs = 1000) {
        uint32_t reserved = 0;
        for (; reserved < n + 1; ++reserved) if (q.reserve(48) != rc::OpResult::Accepted) break;
        EXPECT_EQ(reserved, n + 1);
        keys.clear();
        entries.clear();
        for (std::size_t i = 0; i < n; ++i) keys.push_back(keyBytes(i + 1));
        for (std::size_t i = 0; i < n; ++i) entries.push_back({keys[i], static_cast<int64_t>(i)});
        const auto res = r.client->leaseKeepAliveBatch(1, 0, entries, timeoutMs, rc::CompletionTarget(q), tok);
        if (res != rc::OpResult::Accepted) for (uint32_t i = 0; i < reserved; ++i) q.unreserve(48);
        return res;
    }
    void pump(int rounds = 10) {
        for (int i = 0; i < rounds; ++i) {
            r.client->doWork();
            r.gw.pump();
            (void)q.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got.push_back(c); });
        }
    }
    std::vector<std::vector<uint8_t>> keys;
    std::vector<rpc::LeaseKeepAlive::Entry> entries;
};

}  // namespace

TEST(LeaseSessionRegression, KeepAliveBatchClientRegression) {
    BatchRig b(invokerCfg(5041, 5042));
    using KA = test_support::InvokerGateway::KaAnswer;
    // 3 entries answered in two frames: (idx 0, 2) then (idx 1, final)
    ASSERT_EQ(b.issue(3, 1), rc::OpResult::Accepted);
    b.pump();
    ASSERT_EQ(b.r.gw.kaSeen.size(), 1u);
    ASSERT_EQ(b.r.gw.kaSeen[0].entries.size(), 3u);
    const KA f1[] = {{0, 0, 3000}, {2, 0, 3001}};
    b.r.gw.answerKa(0, f1, false);
    b.pump();
    EXPECT_EQ(b.got.size(), 2u) << "per-entry completions as the frames arrive";
    const KA f2[] = {{1, 0, 3002}};
    b.r.gw.answerKa(0, f2, true);
    b.pump();
    ASSERT_EQ(b.got.size(), 4u) << "3 entries + 1 terminal";
    int terminals = 0;
    for (const auto& c : b.got) {
        EXPECT_EQ(c.op, OpKind::LeaseKeepAlive);
        EXPECT_EQ(c.token, 1u);
        EXPECT_EQ(c.status, OpStatus::Ok);
        if (c.reserved0 == rc::kLeaseKeepAliveTerminal) ++terminals;
    }
    EXPECT_EQ(terminals, 1);
    EXPECT_EQ(b.got.back().reserved0, rc::kLeaseKeepAliveTerminal) << "the terminal comes last";
    EXPECT_EQ(b.q.outstanding(), 0u) << "every reserved credit came back";
    EXPECT_EQ(b.q.reservedBytes(), 0u);
    // a frame after final: dropped
    b.got.clear();
    b.r.gw.answerKa(0, f1, true);
    b.pump();
    EXPECT_TRUE(b.got.empty());
    // a whole-request Throttled: every entry answered with the code and the retry-after, then the terminal
    ASSERT_EQ(b.issue(3, 2), rc::OpResult::Accepted);
    b.pump();
    ASSERT_EQ(b.r.gw.kaSeen.size(), 2u);
    b.r.gw.answerKaWhole(1, static_cast<uint16_t>(rpc::ErrorCode::Throttled), 250);
    b.pump();
    ASSERT_EQ(b.got.size(), 4u);
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(b.got[i].status, OpStatus::BrokerError);
        EXPECT_EQ(b.got[i].errorCode, static_cast<uint16_t>(rpc::ErrorCode::Throttled));
        EXPECT_EQ(b.got[i].aux, 250);
    }
    EXPECT_EQ(b.got[3].reserved0, rc::kLeaseKeepAliveTerminal);
    EXPECT_EQ(b.got[3].status, OpStatus::Ok);
    EXPECT_EQ(b.q.outstanding(), 0u);
    // a timeout: only the terminal, carrying the credit of all three entries
    b.got.clear();
    ASSERT_EQ(b.issue(3, 3, 1000), rc::OpResult::Accepted);
    b.pump();
    g_now += 1500 * kMs;
    b.pump(30);
    ASSERT_EQ(b.got.size(), 1u);
    EXPECT_EQ(b.got[0].status, OpStatus::Timeout);
    EXPECT_EQ(b.got[0].reserved0, rc::kLeaseKeepAliveTerminal);
    EXPECT_EQ(b.got[0].reservedBytes, 48u * 4u);
    EXPECT_EQ(b.q.outstanding(), 0u);
    EXPECT_EQ(b.q.reservedBytes(), 0u);
    EXPECT_EQ(b.q.ring().lostCompletions(), 0u);
}

TEST(LeaseSessionRegression, KeepAliveBatchBadIndexRegression) {
    BatchRig b(invokerCfg(5043, 5044));
    using KA = test_support::InvokerGateway::KaAnswer;
    ASSERT_EQ(b.issue(3, 1), rc::OpResult::Accepted);
    b.pump();
    ASSERT_EQ(b.r.gw.kaSeen.size(), 1u);
    const KA dup[] = {{1, 0, 100}, {1, 0, 101}, {7, 0, 102}, {0xFFF0, 0, 103}};   // a duplicate and two out-of-range indexes
    b.r.gw.answerKa(0, dup, false);
    b.pump();
    ASSERT_EQ(b.got.size(), 1u) << "only the first answer of index 1 counts";
    EXPECT_EQ(b.got[0].reserved0, 1u);
    EXPECT_EQ(b.got[0].aux, 100);
    const KA rest[] = {{0, 0, 1}, {2, 0, 2}};
    b.r.gw.answerKa(0, rest, true);
    b.pump();
    ASSERT_EQ(b.got.size(), 4u);
    EXPECT_EQ(b.got.back().reserved0, rc::kLeaseKeepAliveTerminal);
    EXPECT_EQ(b.q.outstanding(), 0u) << "the credit balances: no entry was counted twice";
    EXPECT_EQ(b.q.reservedBytes(), 0u);
    EXPECT_EQ(b.q.ring().lostCompletions(), 0u);
}

TEST(LeaseSessionRegression, GuardEventPoolCoalesceRegression) {
    SRig<4> r(invokerCfg(5045, 5046), 1, 100, /*makeSession=*/false);
    constexpr uint32_t kCap = 4, kPool = 2 * kCap + 8;   // P = 16
    rc::CompletionsConfig cc;
    cc.capacityBytes = 1u << 14;
    cc.maxResultBytes = 256;
    cc.maxErrorText = 192;
    rc::Completions q(*r.client, cc);
    for (uint32_t i = 0; i < kPool; ++i) ASSERT_EQ(q.reserve(48), rc::OpResult::Accepted);
    const uint32_t sid = q.cqRef();
    auto cmd = [&](rc::detail::HubCommand::Op op, int64_t a, int64_t b = 0, int64_t c = 0) {
        rc::detail::HubCommand h;
        h.op = op; h.handle = sid; h.a = a; h.b = b; h.c = c;
        ASSERT_TRUE(r.client->submitGuardCommand(h));
    };
    rc::detail::HubCommand res;
    res.op = rc::detail::HubCommand::Op::GuardReserve;
    res.handle = sid;
    res.a = static_cast<int64_t>(uint64_t{kCap} | (uint64_t{kPool} << 32));
    rc::detail::OpTarget tg;
    tg.reservedBytes = 48;
    tg.cqRef = q.cqRef();
    rc::detail::encodeTarget(tg, res);
    ASSERT_TRUE(r.client->submitGuardCommand(res));
    // slot 0 / gen 1: armed, tripped, rearmed ... 40 edges with the queue never polled
    const int64_t gen = 1;
    uint32_t seq = 0;
    for (int i = 0; i < 20; ++i) {
        cmd(rc::detail::HubCommand::Op::GuardArm, 0 | (gen << 32), ++seq, g_now + 10 * kMs);   // armed with a deadline 10 ms out
        g_now += 20 * kMs;
        r.client->doWork();                                                                   // trips: Tripped edge
        cmd(rc::detail::HubCommand::Op::GuardArm, 0 | (gen << 32), ++seq, g_now + 1000 * kMs);   // re-arm: Rearmed edge
    }
    r.client->doWork();
    EXPECT_EQ(q.ring().lostCompletions(), 0u) << "more edges than the pool without a single session pass: nothing is lost";
    // the pool holds at most P events; drain, ack, receive the coalesced rest
    std::vector<uint32_t> evs;
    auto drain = [&] {
        (void)q.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.op == OpKind::LeaseGuard) evs.push_back(c.reserved0 & 0xFF);
        });
    };
    drain();
    const std::size_t firstBatch = evs.size();
    EXPECT_LE(firstBatch, kPool + 1) << "bounded by the pool";
    cmd(rc::detail::HubCommand::Op::GuardAck, static_cast<int64_t>(firstBatch));
    r.client->doWork();
    drain();
    cmd(rc::detail::HubCommand::Op::GuardAck, static_cast<int64_t>(evs.size()));
    r.client->doWork();
    drain();
    ASSERT_FALSE(evs.empty());
    EXPECT_EQ(evs.back(), static_cast<uint32_t>(rc::detail::GuardEvent::Rearmed)) << "the last event matches the duty side's final state (Armed)";
    for (std::size_t i = 1; i < evs.size(); ++i) EXPECT_NE(evs[i], evs[i - 1]) << "edges alternate";
    EXPECT_EQ(q.ring().lostCompletions(), 0u);
    EXPECT_LT(evs.size(), 40u) << "the surplus was coalesced";
    cmd(rc::detail::HubCommand::Op::GuardRelease, 0);
    r.client->doWork();
}
