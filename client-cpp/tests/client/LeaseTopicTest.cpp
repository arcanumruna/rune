// tests/client/LeaseTopicTest.cpp - lease topics Task 14a: LeaseTopic<T> writes (the lease SEND_RECORD form and its ack
// trailer), routed lease requests (keepAlive, timeToLive, scan), the open gates, against the InvokerGateway stub broker.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using rc::OpKind;
using rc::OpStatus;

namespace {

rune::codec::Descriptor leaseDescriptor() {
    return rune::codec::DescriptorBuilder("lease_t", 1).add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true).build();
}
const uint64_t kFp = rune::codec::fingerprint(leaseDescriptor());

// The generated-struct contract of a lease row: 4-byte header, the four system columns, then the user columns.
struct Row {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    uint64_t id = 0;
    [[nodiscard]] uint64_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + 32 + 8; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 44);
        b[0] = 40;   // blockLength
        b[2] = 1;    // version
        std::memcpy(b + 36, &id, 8);
        return 44;
    }
};
const uint64_t Row::kFingerprint = kFp;

// A byte-span key of any width (a string(N) key).
struct WideRow {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    std::vector<uint8_t> k;
    [[nodiscard]] std::span<const uint8_t> key() const noexcept { return k; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + 32 + k.size(); }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 36);
        b[0] = static_cast<uint8_t>(32 + k.size());
        b[2] = 1;
        if (!k.empty()) std::memcpy(b + 36, k.data(), k.size());
        return 36 + k.size();
    }
};
const uint64_t WideRow::kFingerprint = kFp;

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("lease_topic_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
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
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

constexpr int32_t kLeaseViewId = 9;
const rune::protocol::LeaseSpec kSpec{.ttlMs = 6000, .minTtlMs = 1000, .maxTtlMs = 60000, .maxKeys = 100,
                                      .expiryBatch = 64, .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};

std::vector<uint8_t> trailer(int64_t version, uint64_t ttl) {
    std::vector<uint8_t> t(16);
    std::memcpy(t.data(), &version, 8);
    std::memcpy(t.data() + 8, &ttl, 8);
    return t;
}
std::vector<uint8_t> verdict(uint16_t code) {
    std::vector<uint8_t> v(8, 0);
    v[2] = static_cast<uint8_t>(code & 0xff);
    v[3] = static_cast<uint8_t>(code >> 8);
    return v;
}

struct Rig {
    std::unique_ptr<rc::Client>  client;
    test_support::InvokerGateway gw;
    int                          keepAlives = 0, ttls = 0, scans = 0;   // lease frames the stub saw
    explicit Rig(const rc::ClientConfig& c)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.idempotence       = true;
        gw.replySchemas      = true;
        gw.schemaBlob        = rune::codec::serialize(leaseDescriptor());
        gw.schemaTopicId     = 1;
        gw.schemaLeaseViewTopicId = kLeaseViewId;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex       = true;
        gw.schemaKeyFeatures = f;
        gw.schemaLease   = kSpec;
        gw.produceLeaseTrailer = trailer(77, 5000);
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            client->doWork();
            gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
        ASSERT_TRUE(drive([&] { return gw.metadataRequests > 0; }));
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
        try {
            fn();
        } catch (...) {
            release();
            throw;
        }
        release();
    }
    // The lease request handlers: keepAlive answers per `kaMode`, timeToLive per `ttl*`, scans per `scanPages`.
    int      kaMode = 0;   // 0 ok(ttl 5000), 1 entry code 64, 2 whole-request NotLeader once then ok, 3 never answers, 4 ok twice
    int      kaSeen = 0;
    int64_t  ttlRemaining = -1;
    bool     ttlAnswers = true;
    struct Page { std::vector<std::vector<uint8_t>> keys; int64_t asOf; uint64_t lastHash; bool done; };
    std::vector<Page> scanPages;
    void installHandlers() {
        gw.templateHandlers[rpc::LeaseKeepAlive::TEMPLATE_ID] = [this](int ep, const uint8_t* b, std::size_t n) {
            const auto m = rpc::LeaseKeepAlive::decode(b, n);
            ASSERT_TRUE(m.has_value());
            ++keepAlives;
            ++kaSeen;
            if (kaMode == 3) return;
            auto reply = [&](uint16_t idx, uint16_t code, uint64_t ttl) {
                const rpc::LeaseKeepAliveResponse::Entry e{idx, code, ttl};
                rpc::LeaseKeepAliveResponse r;
                r.sessionId = m->sessionId;
                r.correlationId = m->correlationId;
                r.final = 1;
                r.entries = std::span<const rpc::LeaseKeepAliveResponse::Entry>(&e, 1);
                std::vector<uint8_t> out(r.encodedSize());
                const std::size_t k = r.encode(out.data());
                gw.send(ep, out.data(), k);
            };
            if (kaMode == 1) reply(0, 64, 0);
            else if (kaMode == 2 && kaSeen == 1) reply(rpc::kLeaseWholeRequest, static_cast<uint16_t>(rpc::ErrorCode::NotLeader), 0);
            else { reply(0, 0, 5000); if (kaMode == 4) reply(0, 0, 5000); }
        };
        gw.templateHandlers[rpc::LeaseTimeToLive::TEMPLATE_ID] = [this](int ep, const uint8_t* b, std::size_t n) {
            const auto m = rpc::LeaseTimeToLive::decode(b, n);
            ASSERT_TRUE(m.has_value());
            ++ttls;
            if (!ttlAnswers) return;
            rpc::LeaseTimeToLiveResponse r;
            r.sessionId = m->sessionId;
            r.correlationId = m->correlationId;
            r.grantedTtlMs = 5000;
            r.remainingMs = ttlRemaining;
            r.version = 9;
            r.holder = 0xAB;
            std::vector<uint8_t> out(r.encodedSize());
            const std::size_t k = r.encode(out.data());
            gw.send(ep, out.data(), k);
        };
        gw.templateHandlers[rpc::ScanLeases::TEMPLATE_ID] = [this](int ep, const uint8_t* b, std::size_t n) {
            const auto m = rpc::ScanLeases::decode(b, n);
            ASSERT_TRUE(m.has_value());
            ++scans;
            ASSERT_FALSE(scanPages.empty());
            const Page& p = scanPages[static_cast<std::size_t>(scans - 1) % scanPages.size()];
            std::vector<uint8_t> rows;
            for (const auto& k : p.keys) {
                rows.push_back(static_cast<uint8_t>(k.size()));
                rows.insert(rows.end(), k.begin(), k.end());
                const uint32_t rl = 44;
                const uint8_t* rp = reinterpret_cast<const uint8_t*>(&rl);
                rows.insert(rows.end(), rp, rp + 4);
                rows.insert(rows.end(), 44, 0x5A);
            }
            rpc::ScanLeasesResponse r;
            r.sessionId = m->sessionId;
            r.correlationId = m->correlationId;
            r.asOfOffset = p.asOf;
            r.lastHash = p.lastHash;
            r.done = p.done ? 1 : 0;
            r.rowCount = static_cast<uint16_t>(p.keys.size());
            r.rows = rows;
            std::vector<uint8_t> out(r.encodedSize());
            const std::size_t k = r.encode(out.data());
            gw.send(ep, out.data(), k);
        };
    }
    template <class T = Row>
    rc::LeaseTopic<T> open(uint32_t opTimeoutMs = 0) {
        rc::LeaseTopicConfig cfg;
        cfg.opTimeoutMs = opTimeoutMs;
        std::optional<rc::LeaseTopic<T>> h;
        whilePumping([&] { h.emplace(client->openLeaseTopicAndWait<T>("lease_t", cfg)); });
        EXPECT_TRUE(h.has_value());
        return std::move(*h);
    }
    template <class T>
    void awaitProducer(rc::LeaseTopic<T>& topic) {
        ASSERT_TRUE(drive([&] { return topic.producer().producerId() >= 0; }));
    }
};

// The (key, value, attributes) of the records of one captured produce frame.
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

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------

TEST(LeaseTopicRegression, GrantOkVersionTtlRegression) {
    Rig r(invokerCfg(4801, 4802));
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    EXPECT_EQ(topic.leaseViewTopicId(), kLeaseViewId);

    test_support::TestCq cq(*r.client);
    rc::LeaseOptions o;
    o.ttlMs  = 4000;
    o.holder = 0xAB;
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.grant(Row{42}, o, q, t); });
    ASSERT_TRUE(f.accepted());
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    const auto& c = f.outcome().c;
    EXPECT_EQ(c.op, OpKind::LeaseGrant);
    EXPECT_EQ(c.status, OpStatus::Ok);
    EXPECT_EQ(c.value, 77) << "the leaseVersion of the ack trailer, not the offset";
    EXPECT_EQ(c.aux, 5000);

    ASSERT_EQ(r.gw.produceFrameBytes.size(), 1u);
    const auto recs = recordsOf(r.gw.produceFrameBytes[0]);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].attrs, 0x01) << "Insert intent, lease op Put";
    EXPECT_EQ(le64(recs[0].value, 4), 0) << "lease_version 0 on a grant";
    EXPECT_EQ(le64(recs[0].value, 12), 4000);
    EXPECT_EQ(le64(recs[0].value, 20), 0xAB);
    EXPECT_EQ(le64(recs[0].value, 28), 0);
    EXPECT_EQ(recs[0].key, (std::vector<uint8_t>{42, 0, 0, 0, 0, 0, 0, 0}));
}

TEST(LeaseTopicRegression, GrantErrorsRegression) {
    struct Case { const char* name; int mode; uint16_t code; OpStatus status; uint16_t errorCode; };
    // mode 0: per-record verdict `code`; 1: a batch-level error `code`
    const Case cases[] = {
        {"verdict KeyExists", 0, 56, OpStatus::RecordsRejected, 56},
        {"verdict 21 on a grant", 0, 21, OpStatus::VerdictUnknown, 0},
        {"batch error 66", 1, 66, OpStatus::BrokerError, 66},
        {"batch InvalidRequest", 1, 39, OpStatus::BrokerError, 39},
    };
    int streams = 4810;
    for (const Case& k : cases) {
        SCOPED_TRACE(k.name);
        Rig r(invokerCfg(streams, streams + 1));
        streams += 2;
        r.connect();
        auto topic = r.open();
        r.awaitProducer(topic);
        if (k.mode == 0) r.gw.produceVerdicts = verdict(k.code);
        else r.gw.errorIf = [&](int32_t, int16_t, int32_t) { return static_cast<int16_t>(k.code); };
        test_support::TestCq cq(*r.client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.grant(Row{1}, {}, q, t); });
        ASSERT_TRUE(r.drive([&] { return f.ready(); }));
        EXPECT_EQ(f.outcome().c.op, OpKind::LeaseGrant);
        EXPECT_EQ(f.outcome().c.status, k.status);
        if (k.status != OpStatus::VerdictUnknown) EXPECT_EQ(f.outcome().c.errorCode, k.errorCode);
    }
}

TEST(LeaseTopicRegression, UpdateRevokeOkAndNotFoundRegression) {
    Rig r(invokerCfg(4820, 4821));
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    test_support::TestCq cq(*r.client);

    r.gw.produceLeaseTrailer = trailer(78, 5000);
    auto u = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.update(Row{5}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return u.ready(); }));
    EXPECT_EQ(u.outcome().c.op, OpKind::LeaseUpdate);
    EXPECT_EQ(u.outcome().c.status, OpStatus::Ok);
    EXPECT_EQ(u.outcome().c.value, 78);
    EXPECT_EQ(u.outcome().c.aux, 5000);

    auto v = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.revoke(Row{5}, 78, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return v.ready(); }));
    EXPECT_EQ(v.outcome().c.op, OpKind::LeaseRevoke);
    EXPECT_EQ(v.outcome().c.status, OpStatus::Ok);

    r.gw.produceVerdicts = verdict(64);
    auto nf = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.revoke(Row{6}, 1, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return nf.ready(); }));
    EXPECT_EQ(nf.outcome().c.status, OpStatus::RecordsRejected);
    EXPECT_EQ(nf.outcome().c.errorCode, 64);

    ASSERT_EQ(r.gw.produceFrameBytes.size(), 3u);
    const auto upd = recordsOf(r.gw.produceFrameBytes[0]);
    const auto rev = recordsOf(r.gw.produceFrameBytes[1]);
    ASSERT_EQ(upd.size(), 1u);
    ASSERT_EQ(rev.size(), 1u);
    EXPECT_EQ(upd[0].attrs, 0x02) << "Update intent, op Put";
    EXPECT_EQ(le64(upd[0].value, 4), 77) << "the compare-and-set version";
    EXPECT_EQ(rev[0].attrs, 0x06) << "Update intent, op Revoke";
    EXPECT_EQ(le64(rev[0].value, 4), 78);

    // Synchronous refusals: nothing is sent.
    const std::size_t sent = r.gw.produceFrameBytes.size();
    EXPECT_EQ(topic.update(Row{5}, -1, {}, cq.q(), 99), rc::OpResult::InvalidArgument);
    EXPECT_EQ(topic.revoke(Row{5}, -1, {}, cq.q(), 99), rc::OpResult::InvalidArgument);
    rc::LeaseOptions big;
    big.ttlMs = kSpec.maxTtlMs + 1;
    EXPECT_EQ(topic.grant(Row{5}, big, cq.q(), 99), rc::OpResult::InvalidArgument);
    topic.close();
    EXPECT_EQ(topic.grant(Row{5}, {}, cq.q(), 99), rc::OpResult::InvalidArgument);
    for (int i = 0; i < 20; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_EQ(r.gw.produceFrameBytes.size(), sent);
}

TEST(LeaseTopicRegression, ExactlyOneCompletionRegression) {
    Rig r(invokerCfg(4830, 4831));
    r.installHandlers();
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    test_support::TestCq cq(*r.client);
    constexpr int kOps = 50;
    std::vector<test_support::PendingOp> ops;
    for (int i = 0; i < kOps; ++i) {
        const Row row{static_cast<uint64_t>(i + 1)};
        switch (i % 5) {
        case 0: ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return topic.grant(row, {}, q, t); })); break;
        case 1: ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return topic.update(row, 77, {}, q, t); })); break;
        case 2: ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return topic.revoke(row, 77, {}, q, t); })); break;
        case 3: ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(row, 77, {}, q, t); })); break;
        default: ops.push_back(cq.issue([&](rc::Completions& q, rc::Token t) { return topic.timeToLive(row, {}, q, t); })); break;
        }
        ASSERT_TRUE(ops.back().accepted()) << i;
    }
    ASSERT_TRUE(r.drive([&] { return cq.completedCount() == static_cast<std::size_t>(kOps); }));
    for (int i = 0; i < 20; ++i) { r.client->doWork(); r.gw.pump(); }   // late duplicates would show here
    for (const auto& op : ops) {
        EXPECT_EQ(cq.timesCompleted(op.token()), 1) << "token " << op.token();
        EXPECT_EQ(op.status(), OpStatus::Ok) << "token " << op.token();
    }
}

TEST(LeaseTopicRegression, NonLeaseTopicRefusedRegression) {
    Rig r(invokerCfg(4840, 4841));
    r.gw.schemaLease.reset();   // an ordinary typed topic
    r.installHandlers();
    r.connect();
    test_support::TestCq cq(*r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->openLeaseTopic<Row>("lease_t", q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.op, OpKind::OpenLeaseTopic);
    EXPECT_EQ(f.outcome().c.status, OpStatus::InvalidArgument);
    r.whilePumping([&] { EXPECT_THROW((void)r.client->openLeaseTopicAndWait<Row>("lease_t"), std::exception); });
    for (int i = 0; i < 20; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_EQ(r.keepAlives + r.ttls + r.scans, 0);
    EXPECT_TRUE(r.gw.produceFrameBytes.empty()) << "no lease wire op after the refusal";
}

TEST(LeaseTopicRegression, RevokeThrottledResentRegression) {
    Rig r(invokerCfg(4850, 4851));
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    r.gw.errorFor[1] = static_cast<int16_t>(rpc::ErrorCode::Throttled);   // the first frame: Throttled, nothing appended
    test_support::TestCq cq(*r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.revoke(Row{3}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.status, OpStatus::Ok);
    EXPECT_EQ(f.outcome().c.op, OpKind::LeaseRevoke);
    EXPECT_EQ(cq.timesCompleted(f.token()), 1);
    EXPECT_EQ(r.gw.produceFrameBytes.size(), 2u) << "the throttled frame is re-sent unchanged";
}

TEST(LeaseTopicRegression, OpTimeoutRegression) {
    Rig r(invokerCfg(4860, 4861));
    r.installHandlers();
    r.kaMode = 3;   // never answers
    r.connect();
    auto topic = r.open(/*opTimeoutMs=*/150);
    r.awaitProducer(topic);
    test_support::TestCq cq(*r.client);
    auto ka = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(Row{1}, 77, {}, q, t); });
    rc::LeaseOptions own;
    own.opTimeoutMs = 40;
    auto ttl = (r.ttlAnswers = false, cq.issue([&](rc::Completions& q, rc::Token t) { return topic.timeToLive(Row{1}, own, q, t); }));
    r.gw.dropIf = [](int32_t, int16_t, int32_t) { return true; };   // every produce frame is lost
    auto w = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.grant(Row{2}, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return ka.ready() && ttl.ready() && w.ready(); }));
    EXPECT_EQ(ka.outcome().c.op, OpKind::LeaseKeepAlive);
    EXPECT_EQ(ka.outcome().c.status, OpStatus::Timeout);
    EXPECT_EQ(ttl.outcome().c.status, OpStatus::Timeout) << "the per-call opTimeout";
    EXPECT_EQ(w.outcome().c.op, OpKind::LeaseGrant);
    EXPECT_EQ(w.outcome().c.status, OpStatus::Timeout) << "a write is bounded by the handle's opTimeout";
}

TEST(LeaseTopicRegression, LeaseKeyWidthsRegression) {
    Rig r(invokerCfg(4870, 4871));
    r.connect();
    auto topic = r.open<WideRow>();
    r.awaitProducer(topic);
    test_support::TestCq cq(*r.client);
    for (const std::size_t width : {std::size_t{17}, std::size_t{128}}) {
        WideRow row;
        for (std::size_t i = 0; i < width; ++i) row.k.push_back(static_cast<uint8_t>(0x30 + i % 40));
        const std::size_t before = r.gw.produceFrameBytes.size();
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.grant(row, {}, q, t); });
        ASSERT_TRUE(r.drive([&] { return f.ready(); })) << width;
        EXPECT_EQ(f.outcome().c.status, OpStatus::Ok) << width;
        ASSERT_EQ(r.gw.produceFrameBytes.size(), before + 1);
        const auto recs = recordsOf(r.gw.produceFrameBytes.back());
        ASSERT_EQ(recs.size(), 1u);
        EXPECT_EQ(recs[0].key, row.k) << "the key bytes go out exact at width " << width;
    }
    WideRow tooWide;
    tooWide.k.assign(129, 0x41);
    const std::size_t sent = r.gw.produceFrameBytes.size();
    EXPECT_EQ(topic.grant(tooWide, {}, cq.q(), 99), rc::OpResult::InvalidArgument);
    for (int i = 0; i < 20; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_EQ(r.gw.produceFrameBytes.size(), sent);
}

TEST(LeaseTopicRegression, NonLeaseSendRecordUnchangedRegression) {
    Rig r(invokerCfg(4880, 4881));
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    // An ordinary 16-byte-key send through the same client: the plain form, attributes 0, key and value exact.
    rc::ProducerConfig pc;
    pc.acks = rc::Acks::All | rc::Acks::View;
    pc.lingerUs = 0;
    rc::Producer p = r.client->producer(pc);
    ASSERT_TRUE(r.drive([&] { return p.producerId() >= 0; }));
    const std::vector<uint8_t> key16(16, 0x6B), value{1, 2, 3, 4};
    r.gw.produceLeaseTrailer.reset();
    test_support::TestCq cq(*r.client);
    const rc::TopicHandle h = r.client->topic("t");
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return p.trySend(h, rc::Key(std::span<const uint8_t>(key16)), value, q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.op, OpKind::Send);
    EXPECT_EQ(f.outcome().c.status, OpStatus::Ok);
    ASSERT_EQ(r.gw.produceFrameBytes.size(), 1u);
    const auto recs = recordsOf(r.gw.produceFrameBytes[0]);
    ASSERT_EQ(recs.size(), 1u);
    EXPECT_EQ(recs[0].attrs, 0);
    EXPECT_EQ(recs[0].key, key16);
    EXPECT_EQ(recs[0].value, value);
    // The byte-exact golden frame of the non-lease form is asserted by CheckedWritesIntents.UpsertFramesAreByteIdenticalRegression.
}

TEST(LeaseTopicRegression, KeepAliveAndTtlRegression) {
    Rig r(invokerCfg(4890, 4891));
    r.installHandlers();
    r.connect();
    auto topic = r.open();
    test_support::TestCq cq(*r.client);

    auto ok = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(Row{1}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return ok.ready(); }));
    EXPECT_EQ(ok.outcome().c.op, OpKind::LeaseKeepAlive);
    EXPECT_EQ(ok.outcome().c.status, OpStatus::Ok);
    EXPECT_EQ(ok.outcome().c.aux, 5000);

    r.kaMode = 1;   // entry 0 refused with 64
    auto nf = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(Row{1}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return nf.ready(); }));
    EXPECT_EQ(nf.outcome().c.status, OpStatus::BrokerError);
    EXPECT_EQ(nf.outcome().c.errorCode, 64);

    r.kaMode = 2; r.kaSeen = 0;   // whole-request NotLeader: re-sent, then answered
    auto nl = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(Row{1}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return nl.ready(); }));
    EXPECT_EQ(nl.outcome().c.status, OpStatus::Ok);
    EXPECT_GE(r.kaSeen, 2) << "routed: the request was re-sent";

    // The same, as an ErrorResponse NotLeader (the broker's own shape).
    r.kaMode = 3;
    int64_t sess = 0, corr = 0;
    int     ep = 0;
    r.gw.templateHandlers[rpc::LeaseKeepAlive::TEMPLATE_ID] = [&](int e, const uint8_t* b, std::size_t n) {
        const auto m = rpc::LeaseKeepAlive::decode(b, n);
        ASSERT_TRUE(m.has_value());
        ++r.keepAlives;
        if (r.keepAlives % 2 == 1) {   // first arrival: ErrorResponse NotLeader
            std::vector<uint8_t> out(256);
            const std::size_t k = rune::client::codec::encodeErrorResponse(out.data(), out.size(), static_cast<int16_t>(rpc::ErrorCode::NotLeader),
                                                                           std::string_view(), m->sessionId, m->correlationId);
            r.gw.send(e, out.data(), k);
            return;
        }
        const rpc::LeaseKeepAliveResponse::Entry en{0, 0, 5000};
        rpc::LeaseKeepAliveResponse resp;
        resp.sessionId = m->sessionId; resp.correlationId = m->correlationId; resp.final = 1;
        resp.entries = std::span<const rpc::LeaseKeepAliveResponse::Entry>(&en, 1);
        std::vector<uint8_t> out(resp.encodedSize());
        r.gw.send(e, out.data(), resp.encode(out.data()));
        r.gw.send(e, out.data(), resp.encode(out.data()));   // a duplicate frame for the same correlation id
        (void)sess; (void)corr; (void)ep;
    };
    r.keepAlives = 0;
    auto el = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.keepAliveOnce(Row{1}, 77, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return el.ready(); }));
    EXPECT_EQ(el.outcome().c.status, OpStatus::Ok);
    for (int i = 0; i < 30; ++i) { r.client->doWork(); r.gw.pump(); }
    EXPECT_EQ(cq.timesCompleted(el.token()), 1) << "the duplicate answering frame is dropped";
    EXPECT_EQ(cq.timesCompleted(nl.token()), 1);

    // timeToLive: the stub's values, remaining -1 kept.
    r.installHandlers();
    auto t1 = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.timeToLive(Row{1}, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return t1.ready(); }));
    EXPECT_EQ(t1.outcome().c.op, OpKind::LeaseTimeToLive);
    EXPECT_EQ(t1.outcome().c.status, OpStatus::Ok);
    EXPECT_EQ(t1.outcome().c.value, 9);
    EXPECT_EQ(t1.outcome().c.aux, -1);
    const auto info = rc::LeaseTtlInfo::decode(t1.outcome().payload);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->grantedTtlMs, 5000u);
    EXPECT_EQ(info->remainingMs, -1);
    EXPECT_EQ(info->version, 9);
    EXPECT_EQ(info->holder, 0xABu);
    r.ttlRemaining = 1234;
    auto t2 = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.timeToLive(Row{1}, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return t2.ready(); }));
    EXPECT_EQ(t2.outcome().c.aux, 1234);
    EXPECT_EQ(rc::LeaseTtlInfo::decode(t2.outcome().payload)->remainingMs, 1234);
}

TEST(LeaseTopicRegression, ScanLeasesCompletionRegression) {
    Rig r(invokerCfg(4900, 4901));
    r.installHandlers();
    r.scanPages = {{{{1}, {2}, {3}}, 555, 0xFEEDFACE12345678ull, false}, {{{4}}, 556, 0x1111ull, true}};
    r.connect();
    auto topic = r.open();
    test_support::TestCq cq(*r.client);
    auto p1 = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.scanLeases(0, {}, 0, true, 100, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return p1.ready(); }));
    const auto& c = p1.outcome().c;
    EXPECT_EQ(c.op, OpKind::ScanLeases);
    EXPECT_EQ(c.status, OpStatus::Ok);
    EXPECT_EQ(c.value, 555);
    EXPECT_EQ(static_cast<uint64_t>(c.aux), 0xFEEDFACE12345678ull);
    EXPECT_EQ(c.reserved0, 3u);
    const auto page = rc::LeaseScanPage::of(c);
    EXPECT_FALSE(page.done);
    std::vector<std::vector<uint8_t>> keys;
    EXPECT_TRUE(rc::forEachLeaseRow(p1.outcome().payload, [&](std::span<const uint8_t> k, std::span<const uint8_t> row) {
        keys.emplace_back(k.begin(), k.end());
        EXPECT_EQ(row.size(), 44u);
    }));
    EXPECT_EQ(keys, (std::vector<std::vector<uint8_t>>{{1}, {2}, {3}}));
    auto p2 = cq.issue([&](rc::Completions& q, rc::Token t) { return topic.scanLeases(0, {}, page.lastHash, false, 100, {}, q, t); });
    ASSERT_TRUE(r.drive([&] { return p2.ready(); }));
    const auto last = rc::LeaseScanPage::of(p2.outcome().c);
    EXPECT_TRUE(last.done);
    EXPECT_EQ(last.rowCount, 1u);
    EXPECT_EQ(p2.outcome().c.reserved0, 1u | (1u << 31));
    EXPECT_EQ(topic.scanLeases(5, {}, 0, true, 10, {}, cq.q(), 99), rc::OpResult::InvalidArgument) << "no such partition";
}

TEST(LeaseTopicRegression, AsyncNoAllocRegression) {
    Rig r(invokerCfg(4910, 4911));
    r.installHandlers();
    r.connect();
    auto topic = r.open();
    r.awaitProducer(topic);
    rc::Completions q(*r.client);
    int done = 0;
    auto poll = [&] { done += q.poll([&](const rc::Completion&, std::span<const uint8_t>) {}); };
    long allocs = 0;
    auto counted = [&](auto&& fn) {
        alloc_counter::startThisThread();
        fn();
        allocs += alloc_counter::stopThisThread();
    };
    rc::Token tok = 0;
    auto round = [&](int n) {
        for (int i = 0; i < n; ++i) {
            const Row row{static_cast<uint64_t>(i % 7 + 1)};
            int issued = 0;
            counted([&] {
                ASSERT_EQ(topic.grant(row, {}, q, ++tok), rc::OpResult::Accepted); ++issued;
                ASSERT_EQ(topic.update(row, 77, {}, q, ++tok), rc::OpResult::Accepted); ++issued;
                ASSERT_EQ(topic.revoke(row, 77, {}, q, ++tok), rc::OpResult::Accepted); ++issued;
                ASSERT_EQ(topic.keepAliveOnce(row, 77, {}, q, ++tok), rc::OpResult::Accepted); ++issued;
            });
            const int target = done + issued;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (done < target && std::chrono::steady_clock::now() < deadline) {
                counted([&] { r.client->doWork(); poll(); });
                r.gw.pump();   // the stub broker allocates: outside the window
            }
            ASSERT_EQ(done, target);
        }
    };
    round(50);   // warm-up: vectors, maps and pools reach their steady size
    allocs = 0;
    round(250);  // 1000 ops
    EXPECT_EQ(allocs, 0L) << "grant / update / revoke / keepAliveOnce must not allocate in steady state (app thread + duty)";
}

TEST(LeaseTopicRegression, BrokerTooOldRegression) {
    Rig r(invokerCfg(4920, 4921));
    r.gw.advertisedBrokerVersion = rpc::kBrokerVersionLeases - 1;
    r.installHandlers();
    r.connect();
    test_support::TestCq cq(*r.client);
    auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->openLeaseTopic<Row>("lease_t", q, t); });
    ASSERT_TRUE(r.drive([&] { return f.ready(); }));
    EXPECT_EQ(f.outcome().c.op, OpKind::OpenLeaseTopic);
    EXPECT_EQ(f.outcome().c.status, OpStatus::BrokerTooOld);
    EXPECT_EQ(r.gw.schemaGets, 0) << "nothing was sent";
    // The routed requests are gated the same way.
    const std::vector<uint8_t> key{1};
    auto ka = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->leaseKeepAlive(1, 0, key, 1, 0, q, t); });
    auto tt = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->leaseTimeToLive(1, 0, key, 0, q, t); });
    auto sc = cq.issue([&](rc::Completions& q, rc::Token t) { return r.client->scanLeases(9, 0, {}, 0, true, 10, 0, q, t); });
    ASSERT_TRUE(r.drive([&] { return ka.ready() && tt.ready() && sc.ready(); }));
    EXPECT_EQ(ka.outcome().c.status, OpStatus::BrokerTooOld);
    EXPECT_EQ(tt.outcome().c.status, OpStatus::BrokerTooOld);
    EXPECT_EQ(sc.outcome().c.status, OpStatus::BrokerTooOld);
    EXPECT_EQ(r.keepAlives + r.ttls + r.scans, 0);
}
