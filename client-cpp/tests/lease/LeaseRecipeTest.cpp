// tests/lease/LeaseRecipeTest.cpp - lease topics Task 20: LeaseOwnership / LeaseElection (etcd Mutex / Election over lease records, spec 12.3),
// against the InvokerGateway stub broker, the model-based lease stub of support/LeaseScanModel.h and the real LeaseSession / LeaseWatch, on a
// fake clock.  Every drive loop is capped; nothing sleeps.
//
// The stub applies the recipe's own lease writes (Produce, attrs lease op + intent) to the model, so the recipe's grant / revoke / update records
// reach the watch's feed exactly as a broker would publish them.  "Other" candidates are granted straight into the model (a foreign holder id).
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/LeaseScanModel.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/lease/LeaseElection.h>
#include <rune/client/lease/LeaseOwnership.h>
#include <rune/client/lease/LeaseRecipe.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <filesystem>
#include <memory>
#include <optional>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using test_support::Bytes;
using test_support::LeaseModel;
using test_support::LeaseStub;
using Op = rune::protocol::record::LeaseOp;

namespace {

constexpr int64_t kMs = 1'000'000;
int64_t g_now = 0;
int64_t fakeNow() noexcept { return g_now; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

constexpr std::size_t kW = 64;   // the key column: string(64)

rune::codec::Descriptor recipeDescriptor() {
    using rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("lease_t", 1)
        .add("lease_version", PrimType::Int64, 1, false)
        .add("lease_ttl_ms", PrimType::UInt64, 1, false)
        .add("lease_holder", PrimType::UInt64, 1, false)
        .add("lease_remaining_ms", PrimType::UInt64, 1, false)
        .addFixedStr("path", kW, 1, /*isKey=*/true)
        .build();
}
const uint64_t kFp = rune::codec::fingerprint(recipeDescriptor());

struct Row {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    std::array<uint8_t, kW> path{};
    [[nodiscard]] std::array<uint8_t, kW> key() const noexcept { return path; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 36 + kW; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 36 + kW);
        b[0] = 32 + kW;
        b[2] = 1;
        std::memcpy(b + 36, path.data(), kW);
        return 36 + kW;
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
                        ("lease_recipe_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
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

struct Rec { Bytes key, value; uint8_t attrs; };
std::vector<Rec> recordsOf(const std::vector<uint8_t>& frame, int32_t* topicId = nullptr) {
    std::vector<Rec> out;
    const auto v = rc::codec::decodeProduce(frame.data(), frame.size());
    if (!v) return out;
    if (topicId != nullptr) *topicId = v->topicId;
    rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
    if (!r.ensureValid()) return out;
    r.forEachRecord([&](std::string_view k, std::string_view val, int64_t, int64_t, uint8_t attrs) {
        out.push_back({{k.begin(), k.end()}, {val.begin(), val.end()}, attrs});
    });
    return out;
}
int64_t le64(const Bytes& v, std::size_t at) {
    int64_t x = 0;
    std::memcpy(&x, v.data() + at, 8);
    return x;
}

/// name + '/' + 16 hex + '-' + 8 hex, zero padded to the key width (the recipe's key, spec 12.3).
Bytes keyOf(std::string_view name, uint64_t holder, uint32_t no) {
    char suffix[40];
    std::snprintf(suffix, sizeof suffix, "/%016llx-%08x", static_cast<unsigned long long>(holder), no);
    Bytes k(kW, 0);
    if (!name.empty()) std::memcpy(k.data(), name.data(), name.size());
    std::memcpy(k.data() + name.size(), suffix, 26);
    return k;
}
constexpr uint64_t kOther = 0x00C0FFEE12345678ull;   // a holder id that is not the session's

/// The stub broker's lease write path: every Produce frame of topic 1 is decoded and applied to the model (grant / update / revoke), the
/// ProduceResponse carries one lease trailer entry per record, and a queued "unknown" mode answers the next grant DuplicateSequence (21) which the
/// client reads as VerdictUnknown - applied to the model or not.
struct Applier {
    enum class Unk { Applied, NotApplied };
    LeaseStub*                    stub = nullptr;
    test_support::InvokerGateway* gw   = nullptr;
    std::deque<Unk>               unknown;
    std::vector<Bytes>            grants, grantValues, revokes, updates, updateValues;
    int                           reads = 0;

    int16_t apply() {
        int32_t topic = -1;
        const std::vector<Rec> recs = recordsOf(gw->produceFrameBytes.back(), &topic);
        if (topic != 1 || recs.empty()) return 0;
        LeaseModel& m = stub->parts[0];
        std::vector<uint8_t> trailer, verdict;
        int16_t              err = 0;
        const auto put64 = [](std::vector<uint8_t>& v, uint64_t x) { for (int i = 0; i < 8; ++i) v.push_back(static_cast<uint8_t>(x >> (8 * i))); };
        for (std::size_t i = 0; i < recs.size(); ++i) {
            const Rec& r  = recs[i];
            const Op   op = rune::protocol::record::leaseOpOf(r.attrs);
            const int64_t version = r.value.size() >= 12 ? le64(r.value, 4) : 0;
            const uint64_t holder = r.value.size() >= 28 ? static_cast<uint64_t>(le64(r.value, 20)) : 0;
            int64_t out = 0;
            if (op == Op::Put && (r.attrs & 3) == 1) {   // grant (insert intent)
                grants.push_back(r.key);
                grantValues.push_back(r.value);
                bool applied = !m.held(r.key);
                if (!unknown.empty()) {
                    const Unk u = unknown.front();
                    unknown.pop_front();
                    if (u == Unk::Applied && applied) out = m.grant(r.key, holder);
                    applied = false;
                    verdict.push_back(static_cast<uint8_t>(i & 0xff));
                    verdict.push_back(static_cast<uint8_t>(i >> 8));
                    verdict.push_back(21);
                    verdict.push_back(0);
                    for (int k = 0; k < 4; ++k) verdict.push_back(0);
                } else if (applied) {
                    out = m.grant(r.key, holder);
                }
            } else if (op == Op::Put) {   // fenced update
                updates.push_back(r.key);
                updateValues.push_back(r.value);
                if (m.held(r.key) && m.table.at(r.key).version == version) { (void)m.update(r.key); out = version; }
                else err = 64;   // LeaseNotFound: the broker no longer knows this (key, version)
            } else if (op == Op::Revoke) {
                revokes.push_back(r.key);
                if (m.held(r.key) && m.table.at(r.key).version == version) { (void)m.revoke(r.key); out = version; }
            }
            put64(trailer, static_cast<uint64_t>(out));
            put64(trailer, 3000);
        }
        gw->produceLeaseTrailer = trailer;
        gw->produceVerdicts     = verdict;
        return err;
    }
};

struct Done {
    std::vector<rc::Completion> v;
    static void fn(void* c, const rc::Completion& x, std::span<const uint8_t>) noexcept { static_cast<Done*>(c)->v.push_back(x); }
    [[nodiscard]] rc::CompletionTarget target() { return rc::CompletionTarget(rc::CompletionHandler{&Done::fn, this}); }
};

constexpr uint32_t kCap = 2048;

struct Fwd;
struct Cb {
    std::vector<rc::LostReason> lost;       // recipe `lost` callbacks
    std::vector<rc::AtRiskEdge> risk;       // recipe `atRisk` callbacks
    int                         sessionLost = 0, sessionRisk = 0;   // session callbacks no recipe claimed
    static void onLost(void* c, rc::LostReason w) { static_cast<Cb*>(c)->lost.push_back(w); }
    static void onRisk(void* c, rc::AtRiskEdge e) { static_cast<Cb*>(c)->risk.push_back(e); }
    [[nodiscard]] rc::LeaseRecipeCallbacks callbacks() { return rc::LeaseRecipeCallbacks{this, &Cb::onLost, &Cb::onRisk}; }
};
struct Fwd {
    rc::LeaseRecipeHost<kCap>* host = nullptr;
    Cb*                        cb   = nullptr;
};
struct OnLostF {
    Fwd* f;
    void operator()(rc::LeaseRef r, rc::LostReason w) const {
        if (f->host != nullptr && f->host->onLost(r, w)) return;
        ++f->cb->sessionLost;
    }
};
struct OnRiskF {
    Fwd* f;
    void operator()(rc::LeaseRef r, rc::AtRiskEdge e) const {
        if (f->host != nullptr && f->host->onAtRisk(r, e)) return;
        ++f->cb->sessionRisk;
    }
};

struct RRig {
    using Session = rc::LeaseSession<8, OnLostF, OnRiskF>;
    std::unique_ptr<rc::Client>             client;
    test_support::InvokerGateway            gw;
    std::optional<rc::LeaseTopic<Row>>      topic;
    std::unique_ptr<LeaseStub>              stub;
    Applier                                 applier;
    Cb                                      cb;
    Fwd                                     fwd;
    std::unique_ptr<Session>                session;
    std::unique_ptr<rc::LeaseWatch<Row>>    watch;
    std::unique_ptr<rc::LeaseRecipeHost<kCap>> host;

    explicit RRig(const rc::ClientConfig& c, uint32_t names = 16, uint32_t waiters = 8)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.idempotence            = true;
        gw.replySchemas           = true;
        gw.schemaBlob             = rune::codec::serialize(recipeDescriptor());
        gw.schemaTopicId          = 1;
        gw.schemaPartitionCount   = 1;
        gw.schemaLeaseViewTopicId = 9;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex = true;
        gw.schemaKeyFeatures = f;
        gw.schemaLease       = kSpec;
        gw.kaAutoOk          = true;
        gw.produceLeaseTrailer = std::vector<uint8_t>(16, 0);
        stub = std::make_unique<LeaseStub>(gw, 1, 1);
        stub->parts[0].keyWidth = kW;
        stub->install();
        applier.stub = stub.get();
        applier.gw   = &gw;
        gw.errorIf = [this](int32_t, int16_t, int32_t) { return applier.apply(); };
        gw.templateHandlers[static_cast<uint16_t>(rpc::GetViewRowK::TEMPLATE_ID)] = [this](int endpoint, const uint8_t* b, std::size_t n) { onRead_(endpoint, b, n); };
        connect();
        topic.emplace(openTopic());
        awaitProducer();
        fwd.cb = &cb;
        session = std::make_unique<Session>(*client, OnLostF{&fwd}, OnRiskF{&fwd}, rc::LeaseSessionOptions{});
        rc::LeaseWatchOptions o;
        o.names          = names;
        o.waitersPerName = waiters;
        o.maxLive        = 8192;
        watch = std::make_unique<rc::LeaseWatch<Row>>(*client, *topic, o);
        EXPECT_TRUE(session->observe(*watch));
        host     = std::make_unique<rc::LeaseRecipeHost<kCap>>(*client, *session, *watch, *topic);
        fwd.host = host.get();
        EXPECT_TRUE(host->usable());
        for (int i = 0; i < 6; ++i) round();
    }
    ~RRig() {
        fwd.host = nullptr;
        host.reset();
        watch.reset();
        session.reset();
        topic.reset();
    }
    void onRead_(int endpoint, const uint8_t* b, std::size_t n) {
        const auto m = rpc::GetViewRowK::decode(b, n);
        if (!m) return;
        ++applier.reads;
        const LeaseModel& lm = stub->parts[0];
        const Bytes key(m->key.begin(), m->key.end());
        Bytes row;
        rpc::ViewRowResponse resp;
        resp.sessionId     = m->sessionId;
        resp.correlationId = m->correlationId;
        if (const auto it = lm.table.find(key); it != lm.table.end()) {
            row            = LeaseModel::row(key, it->second, lm.keyWidth);
            resp.status    = rpc::ViewRowResponse::kOk;
            resp.rowLen    = static_cast<uint32_t>(row.size());
            resp.rowBytes  = row.data();
            resp.rowBytesLen = static_cast<uint32_t>(row.size());
        } else {
            resp.status = rpc::ViewRowResponse::kAbsentKey;
        }
        Bytes out(resp.encodedSize());
        out.resize(resp.encode(out.data()));
        gw.send(endpoint, out.data(), out.size());
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
    /// Starts the watch on every key and drives it ready (the recipes registered their names before this).
    void start() {
        const std::span<const uint8_t> all[1] = {std::span<const uint8_t>()};
        ASSERT_EQ(watch->startFromNow(all), rc::OpResult::Accepted);
        ASSERT_TRUE(drive([&] { return watch->isReady(); }));
    }
    /// One hop of everything on the one thread: feed, client, stub broker, session, watch, host.  The clock creeps 20 us a round.
    void round() {
        g_now += 20'000;
        (void)stub->feed(0);
        client->doWork();
        gw.pump();
        session->doWork(g_now);
        watch->doWork(g_now);
        host->doWork(g_now);
    }
    template <class P>
    bool drive(P&& pred, int cap = 3000) {
        for (int i = 0; i < cap; ++i) {
            round();
            if (pred()) return true;
        }
        return false;
    }
    void rounds(int n) {
        for (int i = 0; i < n; ++i) round();
    }
    LeaseModel& model() { return stub->parts[0]; }
    int64_t foreign(std::string_view name, uint32_t no) { return model().grant(keyOf(name, kOther, no), kOther); }
    void foreignEnd(std::string_view name, uint32_t no, bool expire = false) {
        const Bytes k = keyOf(name, kOther, no);
        if (expire) (void)model().expire(k);
        else (void)model().revoke(k);
    }
};

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------

// The key of every grant: name '/' <holder id, 16 hex> '-' <campaign no, 8 hex>, zero padded to the key width; a name that leaves no room
// for the 26-byte suffix is refused by valid().
TEST(LeaseRecipeRegression, KeyShapeRegression) {
    RRig r(invokerCfg(5201, 5202));
    rc::LeaseOwnership m(*r.host, "jobs/42");
    ASSERT_TRUE(m.valid());
    const std::string longest(kW - 26, 'x'), tooLong(kW - 25, 'y');
    rc::LeaseOwnership fits(*r.host, longest), over(*r.host, tooLong), none(*r.host, "");
    EXPECT_TRUE(fits.valid());
    EXPECT_FALSE(over.valid());
    EXPECT_FALSE(none.valid());
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 7), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(d.v[0].op, rc::OpKind::LeaseCampaign);
    EXPECT_EQ(d.v[0].token, 7u);
    ASSERT_EQ(r.applier.grants.size(), 1u);
    const Bytes want = keyOf("jobs/42", r.session->holderId(), 1);
    EXPECT_EQ(r.applier.grants[0], want);
    EXPECT_EQ(r.applier.grants[0].size(), kW);
    const std::string text(r.applier.grants[0].begin(), r.applier.grants[0].begin() + 33);
    EXPECT_EQ(text.substr(0, 8), "jobs/42/");
    EXPECT_EQ(text[24], '-');
    EXPECT_EQ(text.substr(25, 8), "00000001");
    EXPECT_TRUE(std::all_of(r.applier.grants[0].begin() + 33, r.applier.grants[0].end(), [](uint8_t b) { return b == 0; }));
    ASSERT_TRUE(r.model().held(want));
    EXPECT_EQ(r.model().table.at(want).holder, r.session->holderId()) << "the holder id rides in the grant's lease_holder";
    EXPECT_TRUE(m.isOwner());
    EXPECT_EQ(m.token(), d.v[0].value);
    EXPECT_EQ(m.token(), r.model().table.at(want).version);
    EXPECT_EQ(m.campaignNo(), 1u);
}

// A grant answered VerdictUnknown: the recipe reads its key.  Live with its holder id -> adopted at that version, no second grant.  Absent ->
// a new grant under the next campaign number.
TEST(LeaseRecipeRegression, VerdictUnknownAdoptOrRecampaignRegression) {
    {   // the grant WAS applied
        RRig r(invokerCfg(5203, 5204));
        rc::LeaseOwnership m(*r.host, "jobs/u");
        r.start();
        Done d;
        r.applier.unknown.push_back(Applier::Unk::Applied);
        ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
        EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
        EXPECT_EQ(r.applier.grants.size(), 1u) << "adopted: no second grant";
        EXPECT_EQ(r.applier.reads, 1);
        const Bytes k1 = keyOf("jobs/u", r.session->holderId(), 1);
        ASSERT_TRUE(r.model().held(k1));
        EXPECT_EQ(d.v[0].value, r.model().table.at(k1).version);
        EXPECT_TRUE(m.isOwner());
        EXPECT_EQ(m.campaignNo(), 1u);
    }
    {   // the grant was NOT applied
        RRig r(invokerCfg(5205, 5206));
        rc::LeaseOwnership m(*r.host, "jobs/u");
        r.start();
        Done d;
        r.applier.unknown.push_back(Applier::Unk::NotApplied);
        ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
        EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
        ASSERT_EQ(r.applier.grants.size(), 2u) << "re-granted";
        EXPECT_EQ(r.applier.reads, 1);
        const Bytes k1 = keyOf("jobs/u", r.session->holderId(), 1), k2 = keyOf("jobs/u", r.session->holderId(), 2);
        EXPECT_EQ(r.applier.grants[0], k1);
        EXPECT_EQ(r.applier.grants[1], k2) << "the next campaign number";
        EXPECT_FALSE(r.model().held(k1));
        ASSERT_TRUE(r.model().held(k2));
        EXPECT_EQ(d.v[0].value, r.model().table.at(k2).version);
        EXPECT_EQ(m.campaignNo(), 2u);
    }
}

// The owner is the lowest lease_version under the name, whatever the order of arrival and of the ends of the others.
TEST(LeaseRecipeRegression, OwnershipOrderByVersionRegression) {
    RRig r(invokerCfg(5207, 5208));
    rc::LeaseOwnership m(*r.host, "jobs/1");
    (void)r.foreign("jobs/1", 1);   // A: below the recipe
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.applier.grants.size() == 1; }));
    r.rounds(50);
    EXPECT_TRUE(d.v.empty()) << "A is below: the claim waits";
    EXPECT_FALSE(m.isOwner());
    const int64_t vb = r.foreign("jobs/1", 2);   // B: granted after the recipe's key, so above it
    r.rounds(50);
    EXPECT_TRUE(d.v.empty());
    r.foreignEnd("jobs/1", 1, /*expire=*/true);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
    EXPECT_TRUE(m.isOwner());
    EXPECT_LT(m.token(), vb) << "B is live, but above the recipe";
    const auto leader = m.owner();
    ASSERT_TRUE(leader.has_value());
    EXPECT_EQ(leader->first, m.token());
}

// etcd's waitDeletes: a waiter re-evaluates only when exactly its predecessor ends.
TEST(LeaseRecipeRegression, WaitDeletesPredecessorOnlyRegression) {
    RRig r(invokerCfg(5209, 5210));
    rc::LeaseOwnership m(*r.host, "jobs/3");
    (void)r.foreign("jobs/3", 1);
    (void)r.foreign("jobs/3", 2);
    (void)r.foreign("jobs/3", 3);
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return m.evaluations() == 1; }));
    r.rounds(30);
    EXPECT_EQ(m.evaluations(), 1u);
    // Events that are not the end of the predecessor (3): the end of 1, an update of 3, a new candidate above.
    r.foreignEnd("jobs/3", 1);
    r.rounds(60);
    EXPECT_EQ(m.evaluations(), 1u) << "the end of a non-predecessor does not re-evaluate";
    (void)r.model().update(keyOf("jobs/3", kOther, 3));
    (void)r.foreign("jobs/3", 4);
    r.rounds(60);
    EXPECT_EQ(m.evaluations(), 1u) << "an update of the predecessor and a candidate above do not either";
    // The predecessor (3) ends: a re-evaluation, a new predecessor (2).
    r.foreignEnd("jobs/3", 3);
    ASSERT_TRUE(r.drive([&] { return m.evaluations() == 2; }));
    r.rounds(30);
    EXPECT_TRUE(d.v.empty());
    EXPECT_FALSE(m.isOwner());
    // The new predecessor ends: the recipe is the lowest.
    r.foreignEnd("jobs/3", 2);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(m.evaluations(), 3u);
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
    EXPECT_TRUE(m.isOwner());
}

// tryClaim on an owned name completes LeaseClaimed and gives its own key back; on a free name it owns.
TEST(LeaseRecipeRegression, TryClaimCleansUpRegression) {
    RRig r(invokerCfg(5211, 5212));
    rc::LeaseOwnership m(*r.host, "jobs/4");
    (void)r.foreign("jobs/4", 1);
    r.start();
    Done d;
    ASSERT_EQ(m.tryClaim(d.target(), 5), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::DeliveryFailed);
    EXPECT_EQ(d.v[0].errorCode, static_cast<uint16_t>(rc::ClientErrorCode::LeaseClaimed));
    EXPECT_EQ(d.v[0].token, 5u);
    EXPECT_FALSE(m.isOwner());
    const Bytes mine = keyOf("jobs/4", r.session->holderId(), 1);
    ASSERT_TRUE(r.drive([&] { return !r.applier.revokes.empty(); }));
    ASSERT_TRUE(r.drive([&] { return !r.model().held(mine); })) << "the recipe's own key was revoked";
    EXPECT_EQ(r.applier.revokes[0], mine);
    EXPECT_TRUE(r.model().held(keyOf("jobs/4", kOther, 1))) << "the owner's key is untouched";
    // Free the name: a second tryClaim (once the first has settled) owns.
    r.foreignEnd("jobs/4", 1);
    d.v.clear();
    ASSERT_TRUE(r.drive([&] { return m.tryClaim(d.target(), 6) == rc::OpResult::Accepted; }));
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
    EXPECT_TRUE(m.isOwner());
    EXPECT_EQ(m.campaignNo(), 2u);
}

// observe(): once for the current leader, then once per change of the lowest (key, version) - not for events that leave it as it was.
TEST(LeaseRecipeRegression, ObserveOncePerChangeRegression) {
    RRig r(invokerCfg(5213, 5214));
    rc::LeaseElection e(*r.host, "leader/eu", 128);
    ASSERT_TRUE(e.valid());
    const int64_t v1 = r.foreign("leader/eu", 1);
    r.start();
    struct Seen { std::vector<int64_t> versions; std::vector<std::size_t> bytes; };
    Seen seen;
    ASSERT_TRUE(e.observe(+[](void* c, int64_t v, std::span<const uint8_t>, std::span<const uint8_t> b) {
        static_cast<Seen*>(c)->versions.push_back(v);
        static_cast<Seen*>(c)->bytes.push_back(b.size());
    }, &seen));
    EXPECT_FALSE(e.observe(+[](void*, int64_t, std::span<const uint8_t>, std::span<const uint8_t>) {}, nullptr)) << "one observer";
    r.rounds(30);
    ASSERT_EQ(seen.versions.size(), 1u) << "the current leader is reported first";
    EXPECT_EQ(seen.versions[0], v1);
    const int64_t v2 = r.foreign("leader/eu", 2);   // not the lowest
    r.rounds(40);
    (void)r.model().update(keyOf("leader/eu", kOther, 1));   // the leader's row changes, the leader does not
    r.rounds(40);
    EXPECT_EQ(seen.versions.size(), 1u) << "no change of the lowest: nothing reported";
    r.foreignEnd("leader/eu", 1, /*expire=*/true);
    ASSERT_TRUE(r.drive([&] { return seen.versions.size() == 2; }));
    EXPECT_EQ(seen.versions[1], v2);
    r.foreignEnd("leader/eu", 2);   // no leader
    r.rounds(40);
    EXPECT_EQ(seen.versions.size(), 2u) << "no leader: nothing to report";
    const int64_t v3 = r.foreign("leader/eu", 3);
    ASSERT_TRUE(r.drive([&] { return seen.versions.size() == 3; }));
    EXPECT_EQ(seen.versions[2], v3);
    EXPECT_EQ(seen.bytes[2], 36 + kW) << "a new leader that came with a log row reports the row";
    r.rounds(40);
    EXPECT_EQ(seen.versions.size(), 3u);
}

// A held lease at risk suspends the recipe (isOwner() == false, the callback ran); Cleared re-evaluates it: owner again while still lowest.
TEST(LeaseRecipeRegression, AtRiskSuspendsResumesRegression) {
    RRig r(invokerCfg(5215, 5216));
    rc::LeaseOwnership m(*r.host, "jobs/5", r.cb.callbacks());
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    ASSERT_TRUE(m.isOwner());
    const int64_t token = m.token();
    r.gw.kaAutoOk = false;
    const std::size_t from = r.gw.kaSeen.size();
    ASSERT_TRUE(r.drive([&] { g_now += 50 * kMs; return !r.cb.risk.empty(); }, 200));
    EXPECT_EQ(r.cb.risk.size(), 1u);
    EXPECT_EQ(r.cb.risk[0], rc::AtRiskEdge::Entered);
    EXPECT_FALSE(m.isOwner()) << "suspended while the lease is at risk";
    EXPECT_EQ(r.cb.sessionRisk, 0) << "the host claimed the session's callback";
    for (std::size_t q = from; q < r.gw.kaSeen.size(); ++q) r.gw.answerKaWhole(q, 0, 3000);
    r.gw.kaAutoOk = true;
    ASSERT_TRUE(r.drive([&] { return r.cb.risk.size() == 2; }, 400));
    EXPECT_EQ(r.cb.risk[1], rc::AtRiskEdge::Cleared);
    r.rounds(30);
    EXPECT_TRUE(m.isOwner()) << "still the lowest: owner again";
    EXPECT_EQ(m.token(), token);
    EXPECT_TRUE(r.cb.lost.empty());
}

// F1: every grant is a new key - after a release, after a lost lease.
TEST(LeaseRecipeRegression, RecampaignAlwaysNewKeyRegression) {
    RRig r(invokerCfg(5217, 5218));
    rc::LeaseOwnership m(*r.host, "jobs/6", r.cb.callbacks());
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    const Bytes k1 = keyOf("jobs/6", r.session->holderId(), 1);
    ASSERT_TRUE(r.model().held(k1));
    // Release, then claim again.
    ASSERT_EQ(m.release(), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return !r.model().held(k1); }));
    d.v.clear();
    ASSERT_TRUE(r.drive([&] { return m.claim(d.target(), 2) == rc::OpResult::Accepted; }));
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    const Bytes k2 = keyOf("jobs/6", r.session->holderId(), 2);
    EXPECT_TRUE(r.model().held(k2));
    EXPECT_TRUE(m.isOwner());
    // The lease expires under the owner (the broker's expiry): onLost, then a third key.
    (void)r.model().expire(k2);
    ASSERT_TRUE(r.drive([&] { return !r.cb.lost.empty(); }));
    EXPECT_EQ(r.cb.lost.size(), 1u);
    EXPECT_EQ(r.cb.sessionLost, 0) << "the host claimed the session's callback";
    EXPECT_FALSE(m.isOwner());
    d.v.clear();
    ASSERT_TRUE(r.drive([&] { return m.claim(d.target(), 3) == rc::OpResult::Accepted; }));
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    const Bytes k3 = keyOf("jobs/6", r.session->holderId(), 3);
    ASSERT_EQ(r.applier.grants.size(), 3u);
    EXPECT_EQ(r.applier.grants[0], k1);
    EXPECT_EQ(r.applier.grants[1], k2);
    EXPECT_EQ(r.applier.grants[2], k3);
    EXPECT_TRUE(r.model().held(k3));
}

// A campaign that waits for its predecessor completes LeaseLost when its own lease is lost first (and no `lost` callback: it never held).
TEST(LeaseRecipeRegression, PendingCampaignLostRegression) {
    RRig r(invokerCfg(5219, 5220));
    rc::LeaseOwnership m(*r.host, "jobs/7", r.cb.callbacks());
    (void)r.foreign("jobs/7", 1);
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 9), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.applier.grants.size() == 1; }));
    r.rounds(40);
    ASSERT_TRUE(d.v.empty());
    (void)r.model().expire(keyOf("jobs/7", r.session->holderId(), 1));
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::DeliveryFailed);
    EXPECT_EQ(d.v[0].errorCode, static_cast<uint16_t>(rc::ClientErrorCode::LeaseLost));
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_FALSE(m.isOwner());
}

// Election: campaign (typed row, Completions target), proclaim (a fenced update), resign (a fenced revoke).
TEST(LeaseRecipeRegression, ElectionCampaignProclaimResignRegression) {
    RRig r(invokerCfg(5221, 5222));
    rc::LeaseElection e(*r.host, "leader/us", 128);
    r.start();
    rc::Completions cq(*r.client);
    std::vector<rc::Completion> got;
    const auto pollAll = [&] { (void)cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got.push_back(c); }); };
    ASSERT_EQ(e.campaign(Row{}, cq, 11), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { pollAll(); return got.size() == 1; }));
    EXPECT_EQ(got[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(got[0].op, rc::OpKind::LeaseCampaign);
    EXPECT_EQ(got[0].token, 11u);
    EXPECT_TRUE(e.isLeader());
    const Bytes key = keyOf("leader/us", r.session->holderId(), 1);
    ASSERT_TRUE(r.model().held(key));
    EXPECT_EQ(cq.outstanding(), 0u) << "the credit of the campaign was returned";
    const auto leader = e.leader();
    ASSERT_TRUE(leader.has_value());
    EXPECT_EQ(leader->first, e.token());
    // proclaim: a fenced update of the leader's own key.
    got.clear();
    const std::size_t logBefore = r.model().log.size();
    ASSERT_EQ(e.proclaim(Row{}, cq, 12), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { pollAll(); return got.size() == 1; }));
    EXPECT_EQ(got[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(got[0].op, rc::OpKind::LeaseUpdate);
    ASSERT_EQ(r.applier.updates.size(), 1u);
    EXPECT_EQ(r.applier.updates[0], key);
    ASSERT_EQ(r.applier.updateValues[0].size(), 36 + kW);
    EXPECT_EQ(Bytes(r.applier.updateValues[0].begin() + 36, r.applier.updateValues[0].end()), key)
        << "the proclaimed row carries the recipe's own key in its key column (the broker requires row key == record key)";
    EXPECT_EQ(r.model().log.size(), logBefore + 1);
    EXPECT_TRUE(e.isLeader());
    // resign
    ASSERT_EQ(e.resign(), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return !r.model().held(key); }));
    EXPECT_FALSE(e.isLeader());
    ASSERT_EQ(r.applier.revokes.size(), 1u);
    r.rounds(30);
    EXPECT_FALSE(e.leader().has_value());
    EXPECT_EQ(e.proclaim(Row{}, cq, 13), rc::OpResult::InvalidArgument) << "not the leader";
}

// 2 000 elections share one host and one watch: one scan and ONE subscription, and every completion reaches the recipe of its own name.
TEST(LeaseRecipeRegression, SharedWatchOneSubscriptionRegression) {
    constexpr uint32_t kElections = 2000;
    RRig r(invokerCfg(5223, 5224), kCap, 4);
    std::vector<std::unique_ptr<rc::LeaseElection>> es;
    es.reserve(kElections);
    for (uint32_t i = 0; i < kElections; ++i) {
        char name[24];
        std::snprintf(name, sizeof name, "leader/%05u", i);
        es.push_back(std::make_unique<rc::LeaseElection>(*r.host, name, 128));
        ASSERT_TRUE(es.back()->valid()) << i;
    }
    EXPECT_EQ(r.host->recipeCount(), kElections);
    r.start();
    r.rounds(20);
    EXPECT_EQ(r.gw.subscribed.size(), 1u) << "one subscription for all of them";
    EXPECT_EQ(r.stub->seen.size(), 1u) << "and one scan page";
    std::vector<Done> done(8);
    const uint32_t picks[8] = {0, 1, 7, 255, 999, 1000, 1500, 1999};
    for (int i = 0; i < 8; ++i) ASSERT_EQ(es[picks[i]]->campaign(Row{}, done[static_cast<std::size_t>(i)].target(), picks[i]), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] {
        return std::all_of(done.begin(), done.end(), [](const Done& x) { return x.v.size() == 1; });
    }));
    for (int i = 0; i < 8; ++i) {
        const Done& x = done[static_cast<std::size_t>(i)];
        EXPECT_EQ(x.v[0].status, rc::OpStatus::Ok) << i;
        EXPECT_EQ(x.v[0].token, picks[i]) << "the completion reached the recipe it belongs to";
        EXPECT_TRUE(es[picks[i]]->isLeader());
    }
    for (uint32_t i = 0; i < kElections; ++i) {
        const bool picked = std::find(std::begin(picks), std::end(picks), i) != std::end(picks);
        if (!picked) EXPECT_FALSE(es[i]->isLeader()) << i;
    }
    EXPECT_EQ(r.gw.subscribed.size(), 1u);
    EXPECT_EQ(r.applier.grants.size(), 8u);
    // A foreign leader of one name is seen by that election only.
    (void)r.foreign("leader/00500", 1);
    r.rounds(40);
    const auto l = es[500]->leader();
    ASSERT_TRUE(l.has_value());
    EXPECT_FALSE(es[501]->leader().has_value());
    es.clear();
    EXPECT_EQ(r.host->recipeCount(), 0u);
}

// The recipe path allocates nothing in steady state: claim, the completions, the evaluation, the watch events, release.
TEST(LeaseRecipeRegression, RecipeNoAllocRegression) {
    RRig r(invokerCfg(5225, 5226));
    rc::LeaseElection e(*r.host, "leader/na", 128);
    rc::LeaseOwnership m(*r.host, "jobs/na");
    r.start();
    Done d;
    long allocs = 0;
    const auto counted = [&](auto&& fn) {
        alloc_counter::startThisThread();
        fn();
        allocs += alloc_counter::stopThisThread();
    };
    // One hop with only the recipe-side work inside the counting window (the stub broker and the client allocate on their own).
    const auto step = [&] {
        g_now += 20'000;
        (void)r.stub->feed(0);
        r.client->doWork();
        r.gw.pump();
        counted([&] {
            r.session->doWork(g_now);
            r.watch->doWork(g_now);
            r.host->doWork(g_now);
        });
    };
    const auto cycle = [&](bool measured) {
        const long before = allocs;
        d.v.clear();
        rc::OpResult res = rc::OpResult::QueueFull;
        for (int i = 0; i < 3000 && res != rc::OpResult::Accepted; ++i) {
            step();
            counted([&] { res = m.claim(d.target(), 1); });
        }
        ASSERT_EQ(res, rc::OpResult::Accepted);
        for (int i = 0; i < 3000 && d.v.empty(); ++i) step();
        ASSERT_EQ(d.v.size(), 1u);
        (void)r.foreign("jobs/na", 100);   // an end and a grant of a foreign candidate flow through the watch and the heaps
        for (int i = 0; i < 40; ++i) step();
        r.foreignEnd("jobs/na", 100);
        for (int i = 0; i < 40; ++i) step();
        counted([&] { res = m.release(); });
        ASSERT_EQ(res, rc::OpResult::Accepted);
        for (int i = 0; i < 200; ++i) step();
        if (!measured) allocs = before;
    };
    // Warm up (the high-water marks), then measure.
    cycle(false);
    cycle(false);
    allocs = 0;
    for (int i = 0; i < 4; ++i) cycle(true);
    EXPECT_EQ(allocs, 0L) << "claim / release / host / watch / session allocate nothing in steady state";
    EXPECT_FALSE(m.isOwner());
    (void)e;
}

// ---- REVIEW (batch Tasks 20/21/23): adversarial probes, uncommitted ----

// A releases (its session entry is freed by the revoke's answer) and B joins in the SAME host.doWork: the session hands B the slot A
// just freed (first free slot), host.mapRef(slot -> B), then A.pollReleasing() unmaps the slot by number only and erases B's mapping.
// B's loss is then never routed to B: B stays isOwner() after its lease is gone.
TEST(LeaseRecipeReviewAdversarialRegression, ReleasedSlotReusedKeepsLossRoutingAdversarialRegression) {
    RRig r(invokerCfg(5291, 5292));
    rc::LeaseOwnership a(*r.host, "jobs/a");
    rc::LeaseOwnership b(*r.host, "jobs/b", r.cb.callbacks());
    r.start();
    Done da, db;
    ASSERT_EQ(a.claim(da.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return da.v.size() == 1; }));
    ASSERT_TRUE(a.isOwner());
    // Back to back: A's release and B's claim go out together; both answers land in one round.
    ASSERT_EQ(a.release(), rc::OpResult::Accepted);
    ASSERT_EQ(b.claim(db.target(), 2), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return db.v.size() == 1; }));
    ASSERT_EQ(db.v[0].status, rc::OpStatus::Ok);
    ASSERT_TRUE(b.isOwner());
    r.rounds(50);
    // B's lease ends under it (the broker's expiry).
    (void)r.model().expire(keyOf("jobs/b", r.session->holderId(), 2));   // A's grant was campaign 1 of the host, B's is 2
    (void)r.drive([&] { return !r.cb.lost.empty() || r.cb.sessionLost != 0; }, 2000);
    EXPECT_EQ(r.cb.sessionLost, 0) << "B's onLost was not routed to B: the host's slot map lost B's mapping";
    EXPECT_EQ(r.cb.lost.size(), 1u) << "B's lost callback";
    EXPECT_FALSE(b.isOwner()) << "B still believes it owns jobs/b after its lease ended";
}

// The shared watch fails (here: a name's heap is full, waitersPerName = 2 and two other candidates already wait) while a claim is
// pending: the recipe ignores Failed, so the claim never completes and its lease stays in the session, kept alive, forever.
TEST(LeaseRecipeReviewAdversarialRegression, WatchFailedCompletesPendingClaimAdversarialRegression) {
    RRig r(invokerCfg(5293, 5294), /*names=*/16, /*waiters=*/2);
    rc::LeaseOwnership m(*r.host, "jobs/w");
    (void)r.foreign("jobs/w", 1);
    (void)r.foreign("jobs/w", 2);
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 7), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return r.applier.grants.size() == 1; }));
    ASSERT_TRUE(r.drive([&] { return r.watch->failed(); }, 500)) << "the watch failed (the case under test)";
    (void)r.drive([&] { return !d.v.empty(); }, 2000);
    EXPECT_EQ(d.v.size(), 1u) << "a pending claim never completes once the shared watch failed";
    EXPECT_EQ(r.session->liveCount(), d.v.empty() ? 1u : 0u);
}

// ---- Batch F fix round: recipe regressions ----

// A2: the shared watch fails (a name heap is full, waitersPerName = 2) while A owns jobs/a and B's claim on jobs/w is joining: both recipes
// end.  A's lost callback runs once with WatchFailed, B's pending claim completes LeaseWatchFailed, both leases are revoked and freed, and the
// recipes refuse every later call.
TEST(LeaseRecipeRegression, WatchFailedEndsOwnerAndWaiterRegression) {
    RRig r(invokerCfg(5295, 5296), /*names=*/16, /*waiters=*/2);
    rc::LeaseOwnership a(*r.host, "jobs/a", r.cb.callbacks());
    rc::LeaseOwnership b(*r.host, "jobs/w");
    (void)r.foreign("jobs/w", 1);
    (void)r.foreign("jobs/w", 2);
    r.start();
    Done da, db;
    ASSERT_EQ(a.claim(da.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return da.v.size() == 1; }));
    ASSERT_TRUE(a.isOwner());
    ASSERT_EQ(b.claim(db.target(), 2), rc::OpResult::Accepted);   // the third candidate of jobs/w: the heap overflows, the watch fails
    ASSERT_TRUE(r.drive([&] { return r.watch->failed(); }, 500));
    ASSERT_TRUE(r.drive([&] { return !db.v.empty() && r.session->liveCount() == 0; }, 2000));
    ASSERT_EQ(db.v.size(), 1u);
    EXPECT_EQ(db.v[0].status, rc::OpStatus::DeliveryFailed);
    EXPECT_EQ(db.v[0].errorCode, static_cast<uint16_t>(rc::ClientErrorCode::LeaseWatchFailed));
    EXPECT_FALSE(a.isOwner());
    ASSERT_EQ(r.cb.lost.size(), 1u);
    EXPECT_EQ(r.cb.lost[0], rc::LostReason::WatchFailed);
    EXPECT_EQ(r.cb.sessionLost, 0);
    EXPECT_EQ(r.applier.revokes.size(), 2u) << "both leases were given back";
    r.rounds(50);
    EXPECT_EQ(r.cb.lost.size(), 1u) << "the lost callback runs once";
    EXPECT_FALSE(a.valid());
    Done again;
    EXPECT_EQ(a.claim(again.target(), 3), rc::OpResult::InvalidArgument);
    EXPECT_EQ(b.tryClaim(again.target(), 4), rc::OpResult::InvalidArgument);
}

// A3: a host destroyed before its recipe gives the recipe's lease back (a fenced revoke), instead of keeping it alive in the session.
TEST(LeaseRecipeRegression, HostGoneRevokesHeldLeaseRegression) {
    RRig r(invokerCfg(5297, 5298));
    rc::LeaseOwnership m(*r.host, "jobs/hg");
    r.start();
    Done d;
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    ASSERT_TRUE(m.isOwner());
    const Bytes key = keyOf("jobs/hg", r.session->holderId(), 1);
    ASSERT_TRUE(r.model().held(key));
    r.fwd.host = nullptr;
    r.host.reset();   // the host goes first
    EXPECT_FALSE(m.valid());
    for (int i = 0; i < 3000 && (r.model().held(key) || r.session->liveCount() != 0); ++i) {   // a round without the host
        g_now += 20'000;
        (void)r.stub->feed(0);
        r.client->doWork();
        r.gw.pump();
        r.session->doWork(g_now);
        r.watch->doWork(g_now);
    }
    EXPECT_FALSE(r.model().held(key)) << "the lease was revoked";
    EXPECT_EQ(r.session->liveCount(), 0u) << "and the session entry is free";
    ASSERT_EQ(r.applier.revokes.size(), 1u);
    EXPECT_EQ(r.applier.revokes[0], key);
}

// A4: release() while the grant is in flight, the grant answered with an unknown outcome that DID land: the recipe reads the key and revokes it.
TEST(LeaseRecipeRegression, AbandonedUnknownGrantIsReadAndRevokedRegression) {
    RRig r(invokerCfg(5299, 5300));
    rc::LeaseOwnership m(*r.host, "jobs/ab");
    r.start();
    Done d;
    r.applier.unknown.push_back(Applier::Unk::Applied);   // the grant lands, its answer is VerdictUnknown
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    ASSERT_EQ(m.release(), rc::OpResult::Accepted);
    ASSERT_EQ(d.v.size(), 1u);
    EXPECT_EQ(d.v[0].errorCode, static_cast<uint16_t>(rc::ClientErrorCode::LeaseReleased));
    const Bytes key = keyOf("jobs/ab", r.session->holderId(), 1);
    ASSERT_TRUE(r.drive([&] { return r.applier.reads == 1; }));
    ASSERT_TRUE(r.drive([&] { return !r.applier.revokes.empty(); })) << "the landed grant is revoked";
    EXPECT_EQ(r.applier.revokes[0], key);
    ASSERT_TRUE(r.drive([&] { return !r.model().held(key); }));
    EXPECT_FALSE(m.isOwner());
}

// A5: a refused call never overwrites the stored row: the re-grant after an unknown outcome carries the row of the campaign in flight.
TEST(LeaseRecipeRegression, RefusedCallKeepsStoredRowRegression) {
    RRig r(invokerCfg(5301, 5302));
    rc::LeaseElection e(*r.host, "leader/rr", 128);
    r.start();
    std::array<uint8_t, 36 + kW> rowA{}, rowB{};
    Row{}.encode(rowA.data());
    Row{}.encode(rowB.data());
    rowA[1] = 0xA1;
    rowB[1] = 0xB2;
    Done d, d2;
    r.applier.unknown.push_back(Applier::Unk::NotApplied);
    ASSERT_EQ(e.campaign(std::span<const uint8_t>(rowA), d.target(), 1), rc::OpResult::Accepted);
    EXPECT_EQ(e.campaign(std::span<const uint8_t>(rowB), d2.target(), 2), rc::OpResult::InvalidArgument) << "not idle";
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok);
    ASSERT_EQ(r.applier.grantValues.size(), 2u) << "the unknown grant and its re-grant";
    EXPECT_EQ(r.applier.grantValues[0][1], 0xA1);
    EXPECT_EQ(r.applier.grantValues[1][1], 0xA1) << "the re-grant carries rowA, not the refused rowB";
    EXPECT_TRUE(d2.v.empty());
    // A proclaim that is refused (typed form: not even encoded) leaves the stored row too.
    EXPECT_EQ(e.campaign(Row{}, d2.target(), 3), rc::OpResult::InvalidArgument) << "an owner is not idle";
}

// A6: a send that fails transiently (the host's queue has no credit: QueueFull) is retried by host.doWork, not ended.  The first grant is
// answered with an unknown outcome; the key read that follows cannot be sent (the queue is out of credit), the campaign must not end: it
// re-grants as soon as the answer of the first grant returned its credit.
TEST(LeaseRecipeRegression, TransientSendFailureRetriesRegression) {
    RRig r(invokerCfg(5303, 5304));
    rc::LeaseOwnership m(*r.host, "jobs/tr");
    r.start();
    Done d;
    r.applier.unknown.push_back(Applier::Unk::NotApplied);
    ASSERT_EQ(m.claim(d.target(), 1), rc::OpResult::Accepted);
    rc::Completions& q = r.host->core().queue();
    const uint32_t   tiny = static_cast<uint32_t>(rc::detail::CompletionRing::completionRecordBytes(0));   // the smallest reservation
    uint32_t         held = 0;
    while (held < 5000000 && q.reserve(tiny) == rc::OpResult::Accepted) ++held;
    ASSERT_GT(held, 0u);
    ASSERT_EQ(q.reserve(tiny), rc::OpResult::QueueFull) << "no credit is left";
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    for (uint32_t i = 0; i < held; ++i) q.unreserve(tiny);
    EXPECT_EQ(d.v[0].status, rc::OpStatus::Ok) << "the campaign survived the QueueFull";
    EXPECT_EQ(r.applier.reads, 0) << "the key read could not be sent";
    EXPECT_EQ(r.applier.grants.size(), 2u);
    EXPECT_TRUE(m.isOwner());
}

// A7: campaign numbers are unique per host: the answer of a recipe that is gone never completes a recipe attached later at the same index.
TEST(LeaseRecipeRegression, StaleAnswerNeverReachesNewRecipeRegression) {
    RRig r(invokerCfg(5305, 5306));
    Done da, db;
    {
        rc::LeaseOwnership a(*r.host, "jobs/sa");
        ASSERT_EQ(a.claim(da.target(), 1), rc::OpResult::Accepted);   // the grant is in flight when A goes
    }
    ASSERT_EQ(da.v.size(), 1u) << "A's pending claim completed at its destruction";
    rc::LeaseOwnership b(*r.host, "jobs/sb");   // the same index A had
    ASSERT_EQ(b.claim(db.target(), 2), rc::OpResult::Accepted);
    r.start();
    ASSERT_TRUE(r.drive([&] { return db.v.size() == 1; }));
    EXPECT_EQ(db.v[0].status, rc::OpStatus::Ok);
    const Bytes kb = keyOf("jobs/sb", r.session->holderId(), 2);
    ASSERT_TRUE(r.model().held(kb)) << "B's own grant is campaign 2 of the host";
    EXPECT_EQ(db.v[0].value, r.model().table.at(kb).version) << "B completed on its own answer, not on A's";
    EXPECT_TRUE(b.isOwner());
}

// Matrix cell OwnTryClaim x BackToBack: a second tryClaim issued before the first's answer is refused (not idle): one grant, one completion.
TEST(LeaseRecipeRegression, TryClaimBackToBackRegression) {
    RRig r(invokerCfg(5307, 5308));
    rc::LeaseOwnership m(*r.host, "jobs/b2b");
    (void)r.foreign("jobs/b2b", 1);
    r.start();
    Done d, d2;
    ASSERT_EQ(m.tryClaim(d.target(), 1), rc::OpResult::Accepted);
    EXPECT_EQ(m.tryClaim(d2.target(), 2), rc::OpResult::InvalidArgument) << "back to back: the first is still settling";
    EXPECT_EQ(m.claim(d2.target(), 3), rc::OpResult::InvalidArgument);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    EXPECT_EQ(d.v[0].errorCode, static_cast<uint16_t>(rc::ClientErrorCode::LeaseClaimed));
    EXPECT_TRUE(d2.v.empty());
    EXPECT_EQ(r.applier.grants.size(), 1u) << "the refused calls sent nothing";
    EXPECT_FALSE(m.isOwner());
}

// Matrix cell ElectCampaign x Zombie: a leader whose lease is at risk (its keepalives are not answered; the key is still the lowest) is
// suspended (isLeader() == false), and a waiter does NOT become the leader until the leader's key actually ends.
TEST(LeaseRecipeRegression, ZombieLeaderSuspendsRegression) {
    RRig r(invokerCfg(5309, 5310));
    rc::LeaseElection e(*r.host, "leader/zb", 128, r.cb.callbacks());
    r.start();
    struct Seen { std::vector<int64_t> versions; };
    Seen seen;
    ASSERT_TRUE(e.observe(+[](void* c, int64_t v, std::span<const uint8_t>, std::span<const uint8_t>) { static_cast<Seen*>(c)->versions.push_back(v); }, &seen));
    Done d;
    ASSERT_EQ(e.campaign(Row{}, d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    const int64_t token = e.token();
    const int64_t vw    = r.foreign("leader/zb", 1);   // the waiter, above the leader
    r.rounds(40);
    r.gw.kaAutoOk = false;
    ASSERT_TRUE(r.drive([&] { g_now += 50 * kMs; return !r.cb.risk.empty(); }, 200));
    EXPECT_FALSE(e.isLeader()) << "suspended while the lease is at risk";
    r.rounds(60);
    ASSERT_TRUE(e.leader().has_value());
    EXPECT_EQ(e.leader()->first, token) << "the zombie's key is still the lowest: the waiter does not lead";
    ASSERT_EQ(seen.versions.size(), 1u);
    EXPECT_EQ(seen.versions[0], token) << "the waiter was not reported as the leader";
    // The leader's key really ends: only then the waiter leads.
    (void)r.model().expire(keyOf("leader/zb", r.session->holderId(), 1));
    ASSERT_TRUE(r.drive([&] { return seen.versions.size() == 2; }));
    EXPECT_EQ(seen.versions[1], vw);
    EXPECT_FALSE(e.isLeader());
    ASSERT_TRUE(r.drive([&] { return !r.cb.lost.empty(); }));
    EXPECT_EQ(r.cb.lost.size(), 1u);
}

// Matrix cell ElectProclaim x AroundExpireAfterFold: a proclaim sent just before the leader learns its key expired is answered
// LeaseNotFound by the broker and changes nothing; the leader then loses the name (lost callback once); a proclaim after that is refused.
TEST(LeaseRecipeRegression, ProclaimAfterExpiryRegression) {
    RRig r(invokerCfg(5311, 5312));
    rc::LeaseElection e(*r.host, "leader/pe", 128, r.cb.callbacks());
    r.start();
    Done d, dp, dp2;
    ASSERT_EQ(e.campaign(Row{}, d.target(), 1), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { return d.v.size() == 1; }));
    ASSERT_TRUE(e.isLeader());
    const Bytes key = keyOf("leader/pe", r.session->holderId(), 1);
    (void)r.model().expire(key);                       // the key ended and was folded; the leader has not heard yet
    rc::Completions cq(*r.client);
    std::vector<rc::Completion> got;
    ASSERT_EQ(e.proclaim(Row{}, cq, 5), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive([&] { (void)cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) { got.push_back(c); }); return got.size() == 1; }));
    EXPECT_NE(got[0].status, rc::OpStatus::Ok) << "the broker no longer knows the lease";
    EXPECT_EQ(got[0].op, rc::OpKind::LeaseUpdate);
    ASSERT_TRUE(r.drive([&] { return !r.cb.lost.empty(); }));
    EXPECT_EQ(r.cb.lost.size(), 1u);
    EXPECT_FALSE(e.isLeader());
    EXPECT_EQ(e.proclaim(Row{}, cq, 6), rc::OpResult::InvalidArgument) << "no longer the leader";
    r.rounds(40);
    EXPECT_EQ(r.cb.lost.size(), 1u);
    (void)dp; (void)dp2;
}
