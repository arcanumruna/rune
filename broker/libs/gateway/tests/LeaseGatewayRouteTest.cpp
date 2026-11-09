// LeaseGatewayRouteTest.cpp - lease topics Task 11b: LeaseKeepAlive (96) / LeaseTimeToLive (98) / ScanLeases (100) through a real
// ClientGateway in front of a REAL lease worker.
//
// Rig: the node tests' VerdictRig (a real Worker over a real lease index, judge and runtime) is hosted INSIDE the gateway's
// WorkerPool (RigTuning::hostWorker), so the gateway's tryEnqueueOp lands on the very worker the rig drives.  The rig runs the
// worker pass; its drained lease returns are handed to ClientGateway::handleWorkerReturnForTest (the return hop the pool's
// drainWorkerReturns_ does in production).  Single-threaded, the lease clock injected, every drive loop capped, no sleeps.
//
// Interaction matrix (operation x state; every cell asserts the exact frame):
//   | request               | state                                   | expected                                    | test
//   | keepalive / TTL       | session v17                             | InvalidRequest (whole / code)               | VersionGateRegression
//   | keepalive / TTL       | topic of another namespace / never made | byte-identical NotAuthorized                | ForeignNamespaceIsMissingRegression
//   | keepalive / TTL       | READ only / WRITE only / root           | per op NotAuthorized / served               | AuthorisationRegression
//   | keepalive / TTL       | ReadOnly / RejectAll namespace          | served / NamespaceSuspended                 | SuspendedNamespaceRegression
//   | keepalive / TTL       | partition led elsewhere                 | Redirect frame with the endpoint            | LedElsewhereRedirectsRegression
//   | keepalive / TTL       | granted key, real worker                | Ok / ttl, version, holder, remaining        | KeepAliveRoundTripRegression
//   | 2-entry keepalive     | entry 1 parks behind a clear            | two frames, final 0 then 1                  | ParkedAnswerForwardedRegression
//   | parked keepalive      | session closed before the clear folds   | no frame, park table drained                | SessionGoneDropsRegression
//   | keepalive             | worker ring full                        | Throttled, ttlMs 1                          | RingFullThrottledRegression
//   | ScanLeases            | any                                     | InvalidRequest (until Task 13)              | ScanLeasesNotYetRegression
#include "CheckedWritesRig.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>
#include <rune/gateway/RequestRejection.h>
#include <rune/gateway/SecurityView.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/Gpid.h>
#include <rune/metadata/RoutingImage.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>
#include <rune/node/lease/LeaseIndex.h>
#include <rune/node/lease/LeaseRuntime.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::cw;
namespace pc  = rune::protocol::client;
namespace rec = rune::protocol::record;
namespace rs  = rune::security;

namespace {

constexpr int64_t kMs = 1'000'000;
constexpr int kCap = 10'000;
constexpr int32_t kTopic = VerdictRig::kSrc;   // the lease topic: id 6, partition 0
constexpr int32_t kNode = 1, kOther = 2;
constexpr const char* kOtherEndpoint = "other-node:9000";
constexpr uint16_t kWhole = pc::kLeaseWholeRequest;

constexpr uint16_t code(pc::ErrorCode c) { return static_cast<uint16_t>(c); }
constexpr uint16_t kInvalid = code(pc::ErrorCode::InvalidRequest), kUnknownTopic = code(pc::ErrorCode::UnknownTopic),
                   kNotAuthorized = code(pc::ErrorCode::NotAuthorized), kSuspended = code(pc::ErrorCode::NamespaceSuspended),
                   kThrottled = code(pc::ErrorCode::Throttled), kNotLeader = code(pc::ErrorCode::NotLeader);

std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}
void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}
uint64_t le(const std::vector<uint8_t>& v, std::size_t at, int bytes = 8) {
    uint64_t x = 0;
    for (int i = 0; i < bytes; ++i) x |= uint64_t(v[at + std::size_t(i)]) << (8 * i);
    return x;
}
std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, const std::string& addr, uint64_t ttl = 5000, uint64_t remaining = 0) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, 1, 8);
    putLe(b, 28, remaining, 8);
    putLe(b, 36, id, 8);
    std::memcpy(b.data() + 44, addr.data(), std::min<std::size_t>(addr.size(), 16));
    return b;
}
RawRec grantRec(uint64_t id, uint64_t ttl = 5000) { return {keyOf(id), leaseValue(id, 0, "a", ttl), 1}; }

/// The (offset, key) of every Put record of the log: a lease's version is its grant offset.
int64_t grantOffsetOf(VerdictRig& r, uint64_t key) {
    for (int64_t off = 0; off < r.leo();) {
        const auto v = r.log->readView(off, 1 << 20);
        if (v.empty()) break;
        for (std::size_t p = 0; p + 12 <= v.len;) {
            rune::protocol::RecordBatchReader rd(v.data + p, v.len - p);
            const std::size_t total = static_cast<std::size_t>(rd.totalLength());
            if (total < 12 || p + total > v.len) break;
            int64_t found = -1;
            if ((rd.attributes() & rune::protocol::batch::kAttrControl) == 0)
                rd.forEachRecord([&](std::string_view k, std::string_view, int64_t o, int64_t, uint8_t a) {
                    if (k == keyOf(key) && rec::leaseOpOf(a) == rec::LeaseOp::Put) found = o;
                });
            if (found >= 0) return found;
            p += total;
        }
        off = v.nextOffset;
    }
    return -1;
}

struct Ans { uint16_t index; uint16_t code; uint64_t ttl; };
struct KaFrame { int64_t corr = 0; bool final = false; std::vector<Ans> es; };
struct TtlFrame { int64_t corr = 0; uint16_t code = 0; uint64_t granted = 0; int64_t remaining = 0; int64_t version = 0; uint64_t holder = 0; };

std::optional<KaFrame> asKa(const std::vector<uint8_t>& f) {
    const auto m = pc::LeaseKeepAliveResponse::decode(f.data(), f.size());
    if (!m) return std::nullopt;
    KaFrame out{m->correlationId, m->final == 1, {}};
    m->forEachEntry([&](uint16_t i, uint16_t c, uint64_t t) { out.es.push_back({i, c, t}); });
    return out;
}
std::optional<TtlFrame> asTtl(const std::vector<uint8_t>& f) {
    const auto m = pc::LeaseTimeToLiveResponse::decode(f.data(), f.size());
    if (!m) return std::nullopt;
    return TtlFrame{m->correlationId, m->code, m->grantedTtlMs, m->remainingMs, m->version, m->holder};
}

/// A real ClientGateway + WorkerPool(1) whose worker is the VerdictRig's real lease worker.
struct GwLease {
    int64_t leaseNow = 1'000'000'000;   // declared first: outlives the worker that reads it
    WorkerPool pool;                    // before r: hosts r's worker, destroyed after it
    VerdictRig r;
    std::string dir;
    std::unique_ptr<rune::gateway::TopicRegistry> registry;
    rune::gateway::ClientGateway gw;
    int64_t sid = 0;
    int64_t corr = 100;
    int32_t seq = 0;
    std::vector<std::vector<uint8_t>> out;   // every frame the gateway sent, in order

    static RigTuning hosted(WorkerPool& p) {
        RigTuning t;
        t.hostWorker = &p.worker(0);
        return t;
    }

    GwLease()
        : pool(1, rune::metadata::ShardRouter(1), 1 << 22, 1 << 25, 64, 64),
          r(false, 64LL * 1024 * 1024, hosted(pool)),
          dir(rigTmpPath("gwlease").string()),
          gw(rune::gateway::ClientGateway::makeForTest([this](const uint8_t* b, std::size_t n) -> int64_t { out.emplace_back(b, b + n); return 1; }, 1)) {
        r.worker->setLeaseClockForTest(&leaseNow);
        r.createLeaseTopic(1000);
        r.passes(30);
        WorkerOp level;
        level.type = WorkerOp::Type::ClusterFeatureLevel; level.nextOffset = rune::replication::MetadataFeature::kLeases;
        r.send(std::move(level));
        r.enableAcks(pc::kClientVersionLeases, 0);
        r.passes(30);

        registry = std::make_unique<rune::gateway::TopicRegistry>(std::vector<TopicSpec>{}, dir + "/reg");
        TopicSpec spec;
        spec.name = "instances";
        spec.partitionCount = 1;
        spec.leaseTopic = true;
        if (registry->createTopicAt(kTopic, spec) != kTopic) throw std::logic_error("GwLease: topic id");
        gw.setTopicRegistry(registry.get());
        gw.setWorkerPool(&pool);
        gw.setNodeIdForTest(kNode);
        setLeader(kNode);
        gw.setCommittedMetadataReady(true);
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) { out.emplace_back(b, b + n); });
        sid = gw.injectTestSession();
    }
    ~GwLease() { std::error_code ec; std::filesystem::remove_all(dir, ec); }

    void setLeader(int32_t node) {
        std::vector<rune::metadata::RoutingImage::BuildEntry> img;
        img.push_back({rune::metadata::gpidOf(kTopic, 0), node, 1});
        gw.setRoutingImageSnapshot(rune::metadata::RoutingImage::build(img, rune::metadata::RoutingImage::Source::Committed));
        gw.setNodeEndpointMap({{kOther, kOtherEndpoint}});
    }

    // ---- worker side -------------------------------------------------------------------------------------------------------
    int64_t leaseVersion(uint64_t key) { return grantOffsetOf(r, key); }
    bool produce(const std::vector<RawRec>& recs) {
        const int64_t c = r.produceBytes(VerdictRig::rawBatch(recs, 11, seq), pc::kAcksWireView, 11, seq);
        seq += int32_t(recs.size());
        const auto ack = r.ack(c, 600);
        return ack && ack->code == 0;
    }
    lease::LeaseRuntime* rt() { return r.worker->leaseRuntimeForTest(kTopic, 0); }
    bool setup(std::initializer_list<uint64_t> keys, uint64_t ttl = 5000) {
        std::vector<RawRec> recs;
        for (uint64_t k : keys) recs.push_back(grantRec(k, ttl));
        if (!produce(recs) || !r.driveUntilJudged()) return false;
        for (int i = 0; i < 400 && rt() == nullptr; ++i) r.pass();
        return rt() != nullptr;
    }
    /// A broker Checkpoint(remaining) of `key`: its row then shows remaining > 0 (a keepalive parks behind the clear).
    bool checkpoint(uint64_t key, int64_t version, uint64_t remainingMs, uint64_t ttl = 5000) {
        rune::protocol::RecordBatchBuilder b(0, 0);
        const auto v = leaseValue(key, version, "a", ttl, remainingMs);
        b.append(keyOf(key), std::string_view(reinterpret_cast<const char*>(v.data()), v.size()), 0, rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
        const auto batch = b.build();
        if (r.replica->appendLeaseBatch(batch) < 0) return false;
        for (int i = 0; i < kCap; ++i) {
            const auto lk = r.vs->leaseIndexFor(kTopic, 0, r.replica->leadershipGeneration(), true, true);
            uint32_t rid = 0;
            const uint8_t* row = lk.ref.table == nullptr ? nullptr : lk.ref.table->find(rune::view::ViewKey::fromU64(key), rid);
            if (row != nullptr && leaseRowRemaining(row, *lk.ref.layout) == remainingMs) return true;
            pass();
        }
        return false;
    }

    /// One worker pass, then the lease returns it produced go to the gateway (the production return hop).
    void pass() {
        r.pass();
        auto ka = std::move(r.leaseKaAnswers);
        r.leaseKaAnswers.clear();
        auto ttl = std::move(r.leaseTtlAnswers);
        r.leaseTtlAnswers.clear();
        for (const WorkerReturn& w : ka) gw.handleWorkerReturnForTest(w);
        for (const WorkerReturn& w : ttl) gw.handleWorkerReturnForTest(w);
    }
    /// Passes until `n` more frames than `before` were sent (hard cap).
    bool waitFrames(std::size_t before, std::size_t n) {
        for (int i = 0; i < kCap && out.size() < before + n; ++i) pass();
        return out.size() >= before + n;
    }
    /// A few passes: whatever is going to be answered has been.
    void settle(int n = 40) { for (int i = 0; i < n; ++i) pass(); }

    // ---- request frames ----------------------------------------------------------------------------------------------------
    std::vector<uint8_t> kaFrame(int64_t c, const std::vector<std::pair<uint64_t, int64_t>>& entries, int32_t topic = kTopic,
                                 int64_t session = 0, int32_t partition = 0) const {
        std::vector<std::string> keys;
        for (const auto& e : entries) keys.push_back(keyOf(e.first));
        std::vector<pc::LeaseKeepAlive::Entry> es;
        for (std::size_t i = 0; i < entries.size(); ++i)
            es.push_back({std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(keys[i].data()), keys[i].size()), entries[i].second});
        pc::LeaseKeepAlive m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = c; m.topicId = topic; m.partition = partition;
        m.entries = es;
        std::vector<uint8_t> f(m.encodedSize());
        f.resize(m.encode(f.data()));
        return f;
    }
    std::vector<uint8_t> ttlFrame(int64_t c, uint64_t key, int32_t topic = kTopic, int64_t session = 0, int32_t partition = 0) const {
        const std::string k = keyOf(key);
        pc::LeaseTimeToLive m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = c; m.topicId = topic; m.partition = partition;
        m.key = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
        std::vector<uint8_t> f(m.encodedSize());
        f.resize(m.encode(f.data()));
        return f;
    }
    std::vector<uint8_t> scanFrame(int64_t c, int64_t session = 0) const {
        pc::ScanLeases m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = c; m.viewTopicId = kTopic; m.partition = 0;
        std::vector<uint8_t> f(m.encodedSize());
        f.resize(m.encode(f.data()));
        return f;
    }
    /// The full ingress dispatch (checkFrame, the suspend and auth gates, the handler), then the frames sent since.
    std::vector<std::vector<uint8_t>> send(const std::vector<uint8_t>& f) {
        const std::size_t before = out.size();
        gw.applyClientFrameForTest(f.data(), f.size());
        return std::vector<std::vector<uint8_t>>(out.begin() + std::ptrdiff_t(before), out.end());
    }
    /// A request that is refused at the gateway: exactly one frame at once.
    KaFrame ka(const std::vector<uint8_t>& f) {
        const auto fr = send(f);
        EXPECT_EQ(fr.size(), 1u);
        const auto k = fr.empty() ? std::nullopt : asKa(fr.back());
        EXPECT_TRUE(k.has_value());
        return k.value_or(KaFrame{});
    }
    TtlFrame ttl(const std::vector<uint8_t>& f) {
        const auto fr = send(f);
        EXPECT_EQ(fr.size(), 1u);
        const auto t = fr.empty() ? std::nullopt : asTtl(fr.back());
        EXPECT_TRUE(t.has_value());
        return t.value_or(TtlFrame{});
    }

    // ---- authorisation -----------------------------------------------------------------------------------------------------
    /// Required mode with an authenticated, NON-admin session of namespace `nsId`, holding `grants`.  The lease topic is
    /// indexed in namespace `topicNs`.  Returns the session id.
    int64_t authedSession(const std::vector<rs::GrantSpec>& grants, uint32_t nsId = 0, uint32_t topicNs = 0, bool indexTopic = true) {
        view = std::make_unique<rune::gateway::SecurityView>(rs::SecurityState{});
        gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, view.get());
        idx = rune::gateway::ObjectIndex{};
        if (indexTopic) idx.onTopic(topicNs, kTopic, "instances");
        gw.setBootObjects(idx);
        const int64_t s = gw.injectTestSession();
        gw.setSessionAuthForTest(s, rune::gateway::AuthAuthenticated{1, "user", rune::gateway::PermissionTable::compile(grants, nsId, idx), "", nsId});
        return s;
    }
    std::unique_ptr<rune::gateway::SecurityView> view;
    rune::gateway::ObjectIndex idx;
};

rs::GrantSpec grant(rs::Operation o, rs::ResourceType t, rs::PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; }
rs::GrantSpec topicGrant(rs::Operation o) { return grant(o, rs::ResourceType::Topic, rs::PatternKind::Prefix, ""); }

}  // namespace

// 1. A session below client version 18 never reaches a lease request: a request-level InvalidRequest, the lease response itself.
//    Mutation: drop the version gate in leaseRouteAdmit_ -> the v17 legs are served (red).
TEST(LeaseGatewayRouteRegression, VersionGateRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t old = g.gw.injectTestSession(0, pc::kClientVersionLeases - 1);

    const KaFrame k = g.ka(g.kaFrame(7, {{1, v}}, kTopic, old));
    EXPECT_EQ(k.corr, 7);
    EXPECT_TRUE(k.final);
    ASSERT_EQ(k.es.size(), 1u);
    EXPECT_EQ(k.es[0].index, kWhole);
    EXPECT_EQ(k.es[0].code, kInvalid);
    EXPECT_EQ(k.es[0].ttl, 0u);
    const TtlFrame t = g.ttl(g.ttlFrame(8, 1, kTopic, old));
    EXPECT_EQ(t.corr, 8);
    EXPECT_EQ(t.code, kInvalid);
    EXPECT_EQ(t.remaining, -1);
    g.settle();
    EXPECT_EQ(g.out.size(), 2u) << "the refused requests never reached the worker";

    const std::size_t before = g.out.size();   // version 18 is served
    g.send(g.kaFrame(9, {{1, v}}));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto served = asKa(g.out.back());
    ASSERT_TRUE(served.has_value());
    ASSERT_EQ(served->es.size(), 1u);
    EXPECT_EQ(served->es[0].index, 0);
    EXPECT_EQ(served->es[0].code, 0);
    EXPECT_EQ(served->es[0].ttl, 5000u);
}

// 2. A topic of another namespace is answered EXACTLY like a topic that was never created (no permission bits, so NotAuthorized,
//    and no text names it).  Byte-identical frames: the same correlation id is used for both.  A root session is served.
TEST(LeaseGatewayRouteRegression, ForeignNamespaceIsMissingRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    constexpr int32_t kNever = 77;
    const std::vector<rs::GrantSpec> both = {topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)};
    // the session lives in namespace 1, the lease topic in namespace 2
    const int64_t user = g.authedSession(both, /*nsId=*/1, /*topicNs=*/2);

    const auto kaForeign = g.send(g.kaFrame(5, {{1, v}}, kTopic, user));
    const auto kaMissing = g.send(g.kaFrame(5, {{1, v}}, kNever, user));
    ASSERT_EQ(kaForeign.size(), 1u);
    ASSERT_EQ(kaMissing.size(), 1u);
    EXPECT_EQ(kaForeign[0], kaMissing[0]) << "a foreign topic is a missing topic, byte for byte";
    const auto k = asKa(kaForeign[0]);
    ASSERT_TRUE(k.has_value());
    ASSERT_EQ(k->es.size(), 1u);
    EXPECT_EQ(k->es[0].code, kNotAuthorized);
    EXPECT_EQ(k->es[0].index, kWhole);

    const auto ttlForeign = g.send(g.ttlFrame(6, 1, kTopic, user));
    const auto ttlMissing = g.send(g.ttlFrame(6, 1, kNever, user));
    ASSERT_EQ(ttlForeign.size(), 1u);
    ASSERT_EQ(ttlMissing.size(), 1u);
    EXPECT_EQ(ttlForeign[0], ttlMissing[0]);
    const auto t = asTtl(ttlForeign[0]);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->code, kNotAuthorized);

    // root (ADMIN ON CLUSTER) reaches every namespace
    const int64_t root = g.authedSession({grant(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),
                                          topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)},
                                         /*nsId=*/1, /*topicNs=*/2);
    const std::size_t before = g.out.size();
    g.send(g.kaFrame(10, {{1, v}}, kTopic, root));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto served = asKa(g.out.back());
    ASSERT_TRUE(served.has_value());
    ASSERT_EQ(served->es.size(), 1u);
    EXPECT_EQ(served->es[0].code, 0);
    EXPECT_EQ(served->es[0].ttl, 5000u);
}

// 3. A keepalive needs WRITE, TimeToLive needs READ.  Mutation: map the keepalive to READ -> the READ-only keepalive is served (red).
TEST(LeaseGatewayRouteRegression, AuthorisationRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);

    const int64_t readOnly = g.authedSession({topicGrant(rs::Operation::Read)});
    const KaFrame denied = g.ka(g.kaFrame(1, {{1, v}}, kTopic, readOnly));
    EXPECT_EQ(denied.corr, 1);
    EXPECT_TRUE(denied.final);
    ASSERT_EQ(denied.es.size(), 1u);
    EXPECT_EQ(denied.es[0].index, kWhole);
    EXPECT_EQ(denied.es[0].code, kNotAuthorized);
    EXPECT_EQ(denied.es[0].ttl, 0u);
    std::size_t before = g.out.size();
    g.send(g.ttlFrame(2, 1, kTopic, readOnly));   // READ serves TimeToLive
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto t = asTtl(g.out.back());
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->code, 0);
    EXPECT_EQ(t->corr, 2);
    EXPECT_EQ(t->version, v);

    const int64_t writeOnly = g.authedSession({topicGrant(rs::Operation::Write)});
    const TtlFrame tdenied = g.ttl(g.ttlFrame(3, 1, kTopic, writeOnly));
    EXPECT_EQ(tdenied.code, kNotAuthorized);
    EXPECT_EQ(tdenied.remaining, -1);
    before = g.out.size();
    g.send(g.kaFrame(4, {{1, v}}, kTopic, writeOnly));   // WRITE serves the keepalive
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto served = asKa(g.out.back());
    ASSERT_TRUE(served.has_value());
    ASSERT_EQ(served->es.size(), 1u);
    EXPECT_EQ(served->es[0].code, 0);
    EXPECT_EQ(served->es[0].ttl, 5000u);
}

// 4. A suspended namespace: ReadOnly still serves a keepalive (it writes nothing, spec 8.1) and TimeToLive; RejectAll refuses both
//    with NamespaceSuspended (the gate's own answer).  Mutation: 96 in the write rows of NamespaceSuspendTable.h -> the ReadOnly leg
//    is refused (red).
TEST(LeaseGatewayRouteRegression, SuspendedNamespaceRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)});

    g.gw.setSessionNsGateForTest(user, rune::catalog::NsStateTag::SuspendedReadOnly);
    std::size_t before = g.out.size();
    g.send(g.kaFrame(1, {{1, v}}, kTopic, user));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto ro = asKa(g.out.back());
    ASSERT_TRUE(ro.has_value());
    ASSERT_EQ(ro->es.size(), 1u);
    EXPECT_EQ(ro->es[0].index, 0);
    EXPECT_EQ(ro->es[0].code, 0) << "ReadOnly serves a keepalive";
    EXPECT_EQ(ro->es[0].ttl, 5000u);
    before = g.out.size();
    g.send(g.ttlFrame(2, 1, kTopic, user));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto rot = asTtl(g.out.back());
    ASSERT_TRUE(rot.has_value());
    EXPECT_EQ(rot->code, 0) << "ReadOnly serves TimeToLive";

    g.gw.setSessionNsGateForTest(user, rune::catalog::NsStateTag::SuspendedRejectAll);
    const KaFrame ra = g.ka(g.kaFrame(3, {{1, v}}, kTopic, user));
    EXPECT_EQ(ra.corr, 3);
    EXPECT_TRUE(ra.final);
    ASSERT_EQ(ra.es.size(), 1u);
    EXPECT_EQ(ra.es[0].index, kWhole);
    EXPECT_EQ(ra.es[0].code, kSuspended);
    EXPECT_EQ(ra.es[0].ttl, 0u);
    const TtlFrame rat = g.ttl(g.ttlFrame(4, 1, kTopic, user));
    EXPECT_EQ(rat.code, kSuspended);
    EXPECT_EQ(rat.remaining, -1);
    const auto scan = g.send(g.scanFrame(5, user));
    ASSERT_EQ(scan.size(), 1u);
    const auto sr = pc::ScanLeasesResponse::decode(scan[0].data(), scan[0].size());
    ASSERT_TRUE(sr.has_value());
    EXPECT_EQ(sr->code, kSuspended) << "ScanLeases (100) is gated like a read too";
}

// 5. A partition led by another node is a Redirect frame with that node's endpoint, for both requests (never an op).
TEST(LeaseGatewayRouteRegression, LedElsewhereRedirectsRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    g.setLeader(kOther);
    for (const auto& f : {g.kaFrame(1, {{1, v}}), g.ttlFrame(2, 1)}) {
        const auto fr = g.send(f);
        ASSERT_EQ(fr.size(), 1u);
        const auto err = pc::ErrorResponse::decode(fr[0].data(), fr[0].size());
        ASSERT_TRUE(err.has_value());
        EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
        EXPECT_EQ(err->message, kOtherEndpoint);
    }
    g.settle();
    EXPECT_EQ(g.out.size(), 2u) << "nothing reached the worker";
    g.setLeader(kNode);   // led here again: served
    const std::size_t before = g.out.size();
    g.send(g.ttlFrame(3, 1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_TRUE(asTtl(g.out.back()).has_value());
}

// 6. A real worker behind the gateway: a granted key is kept alive (Ok with its granted TTL) and its TimeToLive is exact.
TEST(LeaseGatewayRouteRegression, KeepAliveRoundTripRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);

    std::size_t before = g.out.size();
    g.send(g.kaFrame(11, {{1, v}}));
    ASSERT_TRUE(g.waitFrames(before, 1));
    g.settle();
    ASSERT_EQ(g.out.size(), before + 1) << "exactly one frame";
    const auto k = asKa(g.out.back());
    ASSERT_TRUE(k.has_value());
    EXPECT_EQ(k->corr, 11);
    EXPECT_TRUE(k->final);
    ASSERT_EQ(k->es.size(), 1u);
    EXPECT_EQ(k->es[0].index, 0);
    EXPECT_EQ(k->es[0].code, 0);
    EXPECT_EQ(k->es[0].ttl, 5000u);

    before = g.out.size();
    g.send(g.ttlFrame(12, 1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto t = asTtl(g.out.back());
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->corr, 12);
    EXPECT_EQ(t->code, 0);
    EXPECT_EQ(t->granted, 5000u);
    EXPECT_EQ(t->version, v);
    EXPECT_EQ(t->holder, 1u);
    EXPECT_EQ(t->remaining, 5000);

    before = g.out.size();   // a wrong version and an absent key: definite answers from the worker
    g.send(g.kaFrame(13, {{1, v + 1}, {99, 1}}));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto nf = asKa(g.out.back());
    ASSERT_TRUE(nf.has_value());
    ASSERT_EQ(nf->es.size(), 2u);
    EXPECT_EQ(nf->es[0].code, code(pc::ErrorCode::LeaseNotFound));
    EXPECT_EQ(nf->es[1].index, 1);
    EXPECT_EQ(nf->es[1].code, code(pc::ErrorCode::LeaseNotFound));
    before = g.out.size();
    g.send(g.ttlFrame(14, 99));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto absent = asTtl(g.out.back());
    ASSERT_TRUE(absent.has_value());
    EXPECT_EQ(absent->code, code(pc::ErrorCode::LeaseNotFound));
    EXPECT_EQ(absent->remaining, -1);
}

// 7. A request may get several frames: entry 0 is decided at once (final 0), entry 1 parks behind its clear and is answered later
//    (final 1).  Mutation: the gateway forwards only the first / only the final return -> a frame is missing (red).
TEST(LeaseGatewayRouteRegression, ParkedAnswerForwardedRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1, 2}));
    const int64_t v1 = g.leaseVersion(1), v2 = g.leaseVersion(2);
    ASSERT_TRUE(g.checkpoint(2, v2, 4000));
    g.r.holdJudge(true);   // the clear of key 2 cannot fold yet

    const std::size_t before = g.out.size();
    g.send(g.kaFrame(21, {{1, v1}, {2, v2}}));
    ASSERT_TRUE(g.waitFrames(before, 1));
    g.settle(10);
    ASSERT_EQ(g.out.size(), before + 1) << "the parked entry is not answered before its clear folds";
    const auto first = asKa(g.out.back());
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->corr, 21);
    EXPECT_FALSE(first->final);
    ASSERT_EQ(first->es.size(), 1u);
    EXPECT_EQ(first->es[0].index, 0);
    EXPECT_EQ(first->es[0].code, 0);
    EXPECT_EQ(first->es[0].ttl, 5000u);

    g.r.holdJudge(false);
    ASSERT_TRUE(g.waitFrames(before, 2));
    const auto second = asKa(g.out.back());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->corr, 21);
    EXPECT_TRUE(second->final);
    ASSERT_EQ(second->es.size(), 1u);
    EXPECT_EQ(second->es[0].index, 1);
    EXPECT_EQ(second->es[0].code, 0);
    EXPECT_EQ(second->es[0].ttl, 5000u);
}

// 8. The session goes away while its keepalive is parked: the late return finds no session and is dropped (no frame, no crash), and
//    the park table drains once the clear folds.  Mutation: send the return to a session that is absent -> crash / a frame (red).
TEST(LeaseGatewayRouteRegression, SessionGoneDropsRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    ASSERT_TRUE(g.checkpoint(1, v, 4000));
    g.r.holdJudge(true);
    const int64_t s2 = g.gw.injectTestSession();
    const std::size_t before = g.out.size();
    g.send(g.kaFrame(31, {{1, v}}, kTopic, s2));
    g.settle(10);
    ASSERT_EQ(g.r.worker->leaseParksForTest().inUse(), 1u) << "parked";
    ASSERT_EQ(g.out.size(), before);

    g.gw.closeSessionForTest(s2);
    ASSERT_FALSE(g.gw.hasSessionForTest(s2));
    const std::size_t afterClose = g.out.size();
    g.r.holdJudge(false);
    for (int i = 0; i < kCap && g.r.worker->leaseParksForTest().inUse() != 0; ++i) g.pass();
    g.settle();
    EXPECT_EQ(g.r.worker->leaseParksForTest().inUse(), 0u) << "the park table drained";
    EXPECT_EQ(g.out.size(), afterClose) << "no frame for a session that is gone";
}

// 9. A full worker ring is Throttled with ttlMs = 1 (the retry-after), the lease response itself.  Nothing runs the worker, so its
//    ingress ring fills.  Mutation: answer Busy / drop instead of Throttled -> red.
TEST(LeaseGatewayRouteRegression, RingFullThrottledRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const auto frame = g.kaFrame(41, {{1, v}});
    bool refused = false;
    for (int i = 0; i < 1'000'000 && !refused; ++i) {
        const auto fr = g.send(frame);
        if (!fr.empty()) {
            ASSERT_EQ(fr.size(), 1u);
            const auto k = asKa(fr[0]);
            ASSERT_TRUE(k.has_value());
            EXPECT_EQ(k->corr, 41);
            EXPECT_TRUE(k->final);
            ASSERT_EQ(k->es.size(), 1u);
            EXPECT_EQ(k->es[0].index, kWhole);
            EXPECT_EQ(k->es[0].code, kThrottled);
            EXPECT_EQ(k->es[0].ttl, 1u);
            refused = true;
        }
    }
    ASSERT_TRUE(refused) << "the ring never filled";
    const TtlFrame t = g.ttl(g.ttlFrame(42, 1));
    EXPECT_EQ(t.code, kThrottled);
    EXPECT_EQ(t.remaining, -1);
}

// 10. ScanLeases (100) is answered by the gateway itself until Task 13: InvalidRequest, never enqueued, never dropped.
//     (Removed by Task 13.)
TEST(LeaseGatewayRouteRegression, ScanLeasesNotYetRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const auto fr = g.send(g.scanFrame(51));
    ASSERT_EQ(fr.size(), 1u);
    const auto s = pc::ScanLeasesResponse::decode(fr[0].data(), fr[0].size());
    ASSERT_TRUE(s.has_value());
    EXPECT_EQ(s->correlationId, 51);
    EXPECT_EQ(s->code, kInvalid);
    EXPECT_EQ(s->rowCount, 0);
    EXPECT_EQ(s->done, 0);
    EXPECT_EQ(s->asOfOffset, -1);
    EXPECT_TRUE(s->rows.empty());
    g.settle();
    EXPECT_EQ(g.out.size(), 1u) << "no op reached the worker (it asserts ScanLeases is never routed)";
}

// Rejection cells: the lease responses encodeRejection produces, decoded.  Sizes 39 / 58 / 49 B within kMaxRejectionBytes.
TEST(LeaseGatewayRouteRegression, RejectionFramesRegression) {
    uint8_t buf[rune::gateway::kMaxRejectionBytes];
    for (const uint16_t c : {kInvalid, kUnknownTopic, kNotAuthorized, kSuspended, kThrottled, kNotLeader}) {
        std::size_t n = rune::gateway::encodeRejection(96, 5, 9, c, buf, sizeof buf);
        EXPECT_EQ(n, 39u);
        const auto k = pc::LeaseKeepAliveResponse::decode(buf, n);
        ASSERT_TRUE(k.has_value()) << c;
        EXPECT_EQ(k->sessionId, 5);
        EXPECT_EQ(k->correlationId, 9);
        EXPECT_EQ(k->final, 1);
        ASSERT_EQ(k->count, 1u);
        k->forEachEntry([&](uint16_t i, uint16_t code, uint64_t ttl) {
            EXPECT_EQ(i, kWhole);
            EXPECT_EQ(code, c);
            EXPECT_EQ(ttl, c == kThrottled ? 1u : 0u);
        });

        n = rune::gateway::encodeRejection(98, 5, 9, c, buf, sizeof buf);
        EXPECT_EQ(n, 58u);
        const auto t = pc::LeaseTimeToLiveResponse::decode(buf, n);
        ASSERT_TRUE(t.has_value()) << c;
        EXPECT_EQ(t->code, c);
        EXPECT_EQ(t->grantedTtlMs, 0u);
        EXPECT_EQ(t->remainingMs, -1);
        EXPECT_EQ(t->version, 0);
        EXPECT_EQ(t->holder, 0u);

        n = rune::gateway::encodeRejection(100, 5, 9, c, buf, sizeof buf);
        EXPECT_EQ(n, 49u);
        const auto s = pc::ScanLeasesResponse::decode(buf, n);
        ASSERT_TRUE(s.has_value()) << c;
        EXPECT_EQ(s->code, c);
        EXPECT_EQ(s->rowCount, 0);
        EXPECT_TRUE(s->rows.empty());
    }
}
