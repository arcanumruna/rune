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
//   | keepalive             | cannot fit one ingress frame            | MessageTooLarge (never Throttled)           | KeepAliveTooLargeForRingIsNotRetriableRegression
//   | keepalive / TTL       | undecodable body                        | InvalidRequest                              | UndecodableBodyRegression
//   | keepalive / TTL       | plain topic / partition out of range    | InvalidRequest                              | NonLeaseTopicAndPartitionRangeRegression
//   | keepalive / TTL       | unknown id / deleting ns / no pool      | UnknownTopic                                | UnknownTopicPathsRegression
//   | keepalive / TTL       | placement committed, not applied        | route refusal (NotLeader)                   | NonNotAuthorizedDenialIsRouteRefusalRegression
//   | keepalive / TTL       | led elsewhere, endpoint unknown         | bare NotLeader                              | LedElsewhereNoEndpointRegression
//   | keepalive / TTL       | not logged in / anonymous / sec. behind | AuthFailed+end / NotAuthorized / AuthUnavail| AuthGateCellsRegression
//   | keepalive / TTL       | grant removed between two requests      | served, then NotAuthorized                  | RevokedMidSessionRegression
//   | keepalive / TTL       | root under RejectAll / Deleting gate    | served / NamespaceSuspended                 | RootExemptUnderRejectAllRegression
//   | worker return         | count / chunk mismatch, TTL chunk != 16 | Internal + counter; good returns unchanged  | MalformedWorkerReturnRegression
//   | keepalive / TTL       | partition 1 of a 2-partition topic      | served by partition 1's leader              | SecondPartitionRoutedRegression
//   | GetViewRow (legacy)   | topic READ / VIEW-only / no grant       | Ok / NotAuthorized / NotAuthorized          | LegacyGetViewRowAuthorisesOnTopicRegression
//   | ScanViewRows          | lease view, authorised / not            | UnknownView / NotAuthorized                 | ScanViewRowsOnLeaseViewRegression
//   | GetViewRowK           | anonymous reader / not logged in        | NotAuthorized / AuthenticationFailed        | LeaseViewAnonymousRegression
//   | GetViewRowK           | grant revoked while the read is held    | the held row (authorised at request time)   | LeaseViewGrantRevokedWhileHeldRegression
//   | GetViewRowK           | worker demoted, image says another leader | Redirect with the leader's endpoint       | LeaseViewSentinelRedirectsOnReturnRegression
//   | GetViewRowK           | partition -1 / == count / led here / not | InvalidRequest / served / Redirect          | LeaseViewPartitionRangeRegression
//   | lease view mapping    | view sink dropped from the registry     | mapping -1, UnknownView                     | LeaseViewDropClearsMappingRegression
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

    int32_t rigPartition = 0;   // the partition of kTopic the rig's worker leads
    int32_t partitions = 1;     // the topic's partition count

    static RigTuning hosted(WorkerPool& p, int32_t partition) {
        RigTuning t;
        t.hostWorker = &p.worker(0);
        t.partition = partition;
        return t;
    }

    /// `ingressRingBytes`: the worker pool's ingress ring (RUNE_WORKER_INGRESS_RING_BYTES / the WorkerPool ctor's 3rd argument).
    explicit GwLease(std::size_t ingressRingBytes = 1 << 22, int32_t partitionOfRig = 0, int32_t partitionCount = 1)
        : rigPartition(partitionOfRig),
          partitions(partitionCount),
          pool(1, rune::metadata::ShardRouter(1), ingressRingBytes, 1 << 25, 64, 64),
          r(false, 64LL * 1024 * 1024, hosted(pool, partitionOfRig)),
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
        spec.partitionCount = partitions;
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
        for (int32_t p = 0; p < partitions; ++p) img.push_back({rune::metadata::gpidOf(kTopic, p), p == rigPartition ? node : kOther, 1});
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
    lease::LeaseRuntime* rt() { return r.worker->leaseRuntimeForTest(kTopic, rigPartition); }
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
            const auto lk = r.vs->leaseIndexFor(kTopic, rigPartition, r.replica->leadershipGeneration(), true, true);
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
        auto views = r.takeViewAnswers();
        for (const WorkerReturn& w : views) gw.handleWorkerReturnForTest(w);
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

    // ---- the lease view `<topic>.leases` (Task 13a) ------------------------------------------------------------------------
    static constexpr int32_t kView = VerdictRig::kLeaseView;
    /// Registers the lease view sink as the broker's sinkApplied path does (registry + source mapping + lease-view mapping).
    bool hasLeaseView = false;
    void addLeaseView() {
        hasLeaseView = true;
        TopicSpec sink;
        sink.name = "instances.leases";
        sink.partitionCount = partitions;
        sink.viewSink = true;
        sink.leaseSourceTopicId = kTopic;   // the registry's sink spec IS the lease view -> source mapping (Task 13a review fixes)
        if (registry->createTopicAt(kView, sink) != kView) throw std::logic_error("GwLease: lease view id");
    }
    std::vector<uint8_t> viewFrame(int64_t c, uint64_t key, int32_t partition = -1, int64_t session = 0, int32_t view = kView) const {
        const std::string k = keyOf(key);
        pc::GetViewRowK m;
        m.sessionId = session != 0 ? session : sid; m.correlationId = c; m.viewTopicId = view; m.partition = partition;
        m.minOffset = -1;
        m.key = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size());
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    }
    /// The status of a ViewRowResponse frame, -1 when the frame is not one.
    static int32_t viewStatus(const std::vector<uint8_t>& f) {
        const auto r = pc::ViewRowResponse::decode(f.data(), f.size());
        return r ? int32_t(r->status) : -1;
    }

    // ---- authorisation -----------------------------------------------------------------------------------------------------
    /// Required mode with an authenticated, NON-admin session of namespace `nsId`, holding `grants`.  The lease topic is
    /// indexed in namespace `topicNs`.  Returns the session id.
    int64_t authedSession(const std::vector<rs::GrantSpec>& grants, uint32_t nsId = 0, uint32_t topicNs = 0, bool indexTopic = true) {
        view = std::make_unique<rune::gateway::SecurityView>(rs::SecurityState{});
        gw.setAuthMode(rune::gateway::GatewayAuthMode::Required, view.get());
        idx = rune::gateway::ObjectIndex{};
        if (indexTopic) idx.onTopic(topicNs, kTopic, "instances");
        if (indexTopic && hasLeaseView) idx.onView(topicNs, VerdictRig::kLeaseView, "instances.leases");
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

// ---------------------------------------------------------------------------------------------------------------------------------
// Task 11b review fixes: the TooLarge answer, the malformed worker returns and the remaining matrix cells.
// ---------------------------------------------------------------------------------------------------------------------------------
namespace {

std::vector<uint8_t> withTrailingByte(std::vector<uint8_t> f) { f.push_back(0); return f; }
std::vector<uint8_t> withCount(std::vector<uint8_t> f, uint16_t n) {
    f[std::size_t(pc::lease_wire::kBase) + 8] = uint8_t(n);
    f[std::size_t(pc::lease_wire::kBase) + 9] = uint8_t(n >> 8);
    return f;
}
std::vector<uint8_t> truncated(std::vector<uint8_t> f, std::size_t by) { f.resize(f.size() - by); return f; }

std::vector<uint8_t> entryBytes(std::initializer_list<Ans> es) {
    std::vector<uint8_t> b(es.size() * 12, 0);
    std::size_t at = 0;
    for (const Ans& e : es) {
        putLe(b, at, e.index, 2);
        putLe(b, at + 2, e.code, 2);
        putLe(b, at + 4, e.ttl, 8);
        at += 12;
    }
    return b;
}
WorkerReturn kaReturn(const GwLease& g, int64_t corr, int32_t count, std::vector<uint8_t> chunk, bool final) {
    WorkerReturn w;
    w.type = WorkerReturn::Type::LeaseKeepAliveResult;
    w.sessionId = g.sid; w.correlationId = corr; w.recordCount = count; w.lastOffset = final ? 1 : 0;
    w.chunk = std::move(chunk);
    return w;
}
WorkerReturn ttlReturn(const GwLease& g, int64_t corr, std::vector<uint8_t> chunk) {
    WorkerReturn w;
    w.type = WorkerReturn::Type::LeaseTimeToLiveResult;
    w.sessionId = g.sid; w.correlationId = corr; w.errorCode = 0; w.baseOffset = 4000; w.lastOffset = 9;
    w.chunk = std::move(chunk);
    return w;
}
void expectWholeKa(const std::vector<uint8_t>& frame, int64_t corr, uint16_t c, uint64_t ttl = 0) {
    const auto k = asKa(frame);
    ASSERT_TRUE(k.has_value());
    EXPECT_EQ(k->corr, corr);
    EXPECT_TRUE(k->final);
    ASSERT_EQ(k->es.size(), 1u);
    EXPECT_EQ(k->es[0].index, kWhole);
    EXPECT_EQ(k->es[0].code, c);
    EXPECT_EQ(k->es[0].ttl, ttl);
}
void expectErrorFrame(const std::vector<uint8_t>& frame, pc::ErrorCode c) {
    const auto err = pc::ErrorResponse::decode(frame.data(), frame.size());
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, static_cast<int16_t>(c));
}

}  // namespace

// 11. A keepalive that can NEVER fit one worker-ingress frame is not back-pressure: MessageTooLarge (whole request, final, ttl 0),
//     never Throttled (a client would retry it forever).  The pool's ingress ring is the WorkerPool ctor's 3rd argument
//     (NodeConfig / RUNE_WORKER_INGRESS_RING_BYTES): 256 KiB gives a 32 KiB frame, a 256-entry keepalive of 128-byte keys is 35 KB.
//     The same request on the default ring is enqueued.  Mutation: map TooLarge to Throttled in answerLeaseEnqueue_ -> red.
TEST(LeaseGatewayRouteRegression, KeepAliveTooLargeForRingIsNotRetriableRegression) {
    const auto bigFrame = [](const GwLease& g, int64_t corr) {
        std::vector<std::string> keys(256, std::string(128, 'k'));
        std::vector<pc::LeaseKeepAlive::Entry> es;
        for (const auto& k : keys) es.push_back({std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k.data()), k.size()), 1});
        pc::LeaseKeepAlive m;
        m.sessionId = g.sid; m.correlationId = corr; m.topicId = kTopic; m.partition = 0;
        m.entries = es;
        std::vector<uint8_t> f(m.encodedSize());
        f.resize(m.encode(f.data()));
        return f;
    };
    {
        GwLease g(1 << 18);
        ASSERT_TRUE(g.setup({1}));
        const uint64_t opsBefore = g.r.worker->leaseKeepAlivesForTest();
        const auto fr = g.send(bigFrame(g, 61));
        ASSERT_EQ(fr.size(), 1u);
        expectWholeKa(fr[0], 61, code(pc::ErrorCode::MessageTooLarge));
        g.settle();
        EXPECT_EQ(g.out.size(), 1u) << "nothing reached the worker";
        EXPECT_EQ(g.r.worker->leaseKeepAlivesForTest(), opsBefore) << "the worker saw no keepalive op";
    }
    {
        GwLease g;   // the default ring takes the same request
        ASSERT_TRUE(g.setup({1}));
        const uint64_t opsBefore = g.r.worker->leaseKeepAlivesForTest();
        const auto fr = g.send(bigFrame(g, 62));
        EXPECT_TRUE(fr.empty()) << "enqueued: answered by the worker later";
        ASSERT_TRUE(g.waitFrames(0, 1)) << "the worker's answer reaches the client";
        ASSERT_EQ(g.out.size(), 1u);
        const auto served = asKa(g.out[0]);
        ASSERT_TRUE(served.has_value());
        EXPECT_EQ(served->corr, 62);
        EXPECT_TRUE(served->final);
        ASSERT_EQ(served->es.size(), 256u) << "one answer per entry";
        for (std::size_t i = 0; i < served->es.size(); ++i) {
            EXPECT_EQ(served->es[i].index, i);
            EXPECT_EQ(served->es[i].code, code(pc::ErrorCode::LeaseNotFound)) << "no 128-byte key is a lease";
        }
        EXPECT_EQ(g.r.worker->leaseKeepAlivesForTest(), opsBefore + 1) << "the worker executed the op exactly once";
    }
}

// 12. Bodies that do not decode: InvalidRequest, exactly one frame, never enqueued.
TEST(LeaseGatewayRouteRegression, UndecodableBodyRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const std::vector<std::vector<uint8_t>> kas = {withTrailingByte(g.kaFrame(1, {{1, v}})), withCount(g.kaFrame(2, {{1, v}}), 0),
                                                   withCount(g.kaFrame(3, {{1, v}}), 257), truncated(g.kaFrame(4, {{1, v}}), 3)};
    int64_t corr = 1;
    for (const auto& f : kas) {
        const auto fr = g.send(f);
        ASSERT_EQ(fr.size(), 1u) << corr;
        expectWholeKa(fr[0], corr, kInvalid);
        ++corr;
    }
    const std::vector<std::vector<uint8_t>> ttls = {withTrailingByte(g.ttlFrame(11, 1)), truncated(g.ttlFrame(12, 1), 3)};
    corr = 11;
    for (const auto& f : ttls) {
        const auto fr = g.send(f);
        ASSERT_EQ(fr.size(), 1u) << corr;
        const auto t = asTtl(fr[0]);
        ASSERT_TRUE(t.has_value());
        EXPECT_EQ(t->corr, corr);
        EXPECT_EQ(t->code, kInvalid);
        EXPECT_EQ(t->remaining, -1);
        ++corr;
    }
    g.settle();
    EXPECT_EQ(g.out.size(), kas.size() + ttls.size()) << "nothing reached the worker";
}

// 13. A topic that is not a lease topic, and a partition outside [0, partitionCount): InvalidRequest for both operations.
TEST(LeaseGatewayRouteRegression, NonLeaseTopicAndPartitionRangeRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    constexpr int32_t kPlain = 8;
    TopicSpec plain;
    plain.name = "plain";
    plain.partitionCount = 1;
    ASSERT_EQ(g.registry->createTopicAt(kPlain, plain), kPlain);
    struct Cell { int32_t topic, partition; };
    for (const Cell c : {Cell{kPlain, 0}, Cell{kTopic, -1}, Cell{kTopic, 1}}) {
        const auto ka = g.send(g.kaFrame(5, {{1, v}}, c.topic, 0, c.partition));
        ASSERT_EQ(ka.size(), 1u) << c.topic << "/" << c.partition;
        expectWholeKa(ka[0], 5, kInvalid);
        const auto tt = g.send(g.ttlFrame(6, 1, c.topic, 0, c.partition));
        ASSERT_EQ(tt.size(), 1u);
        const auto t = asTtl(tt[0]);
        ASSERT_TRUE(t.has_value());
        EXPECT_EQ(t->corr, 6);
        EXPECT_EQ(t->code, kInvalid);
        EXPECT_EQ(t->remaining, -1);
    }
    g.settle();
    EXPECT_EQ(g.out.size(), 6u);
}

// 14. UnknownTopic: an id nothing created, a topic of a DELETING namespace (a root session reaches it otherwise), and a gateway with
//     no worker pool / no topic registry.
TEST(LeaseGatewayRouteRegression, UnknownTopicPathsRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const auto expectUnknown = [&](GwLease& gl, int64_t session, int32_t topic) {
        const auto ka = gl.send(gl.kaFrame(7, {{1, v}}, topic, session));
        ASSERT_EQ(ka.size(), 1u);
        expectWholeKa(ka[0], 7, kUnknownTopic);
        const auto tt = gl.send(gl.ttlFrame(8, 1, topic, session));
        ASSERT_EQ(tt.size(), 1u);
        const auto t = asTtl(tt[0]);
        ASSERT_TRUE(t.has_value());
        EXPECT_EQ(t->code, kUnknownTopic);
        EXPECT_EQ(t->remaining, -1);
    };
    expectUnknown(g, g.sid, 77);   // root (allow-all): an unknown id

    const int64_t root = g.authedSession({grant(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),
                                          topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)},
                                         /*nsId=*/1, /*topicNs=*/2);
    rune::catalog::NamespaceRecord rec;
    rec.nsId = 2; rec.name = "ns2"; rec.version = 1; rec.state = rune::catalog::stateOf(rune::catalog::NsStateTag::Deleting);
    g.gw.onNamespaceUpserted(rec);
    expectUnknown(g, root, kTopic);   // the topic of a deleting namespace
    g.settle();

    // no pool, then a pool but no registry (a gateway the broker has not wired yet)
    std::vector<std::vector<uint8_t>> sent;
    auto bare = rune::gateway::ClientGateway::makeForTest([&](const uint8_t* b, std::size_t n) -> int64_t { sent.emplace_back(b, b + n); return 1; }, 1);
    bare.setOnEgressFrame([&](int64_t, const uint8_t* b, std::size_t n) { sent.emplace_back(b, b + n); });
    const int64_t bs = bare.injectTestSession();
    const auto sendBare = [&](const std::vector<uint8_t>& f) {
        sent.clear();
        bare.applyClientFrameForTest(f.data(), f.size());
        return sent;
    };
    for (int leg = 0; leg < 2; ++leg) {
        if (leg == 1) bare.setWorkerPool(&g.pool);
        const auto ka = sendBare(g.kaFrame(9, {{1, v}}, kTopic, bs));
        ASSERT_EQ(ka.size(), 1u) << leg;
        expectWholeKa(ka[0], 9, kUnknownTopic);
        const auto tt = sendBare(g.ttlFrame(10, 1, kTopic, bs));
        ASSERT_EQ(tt.size(), 1u) << leg;
        const auto t = asTtl(tt[0]);
        ASSERT_TRUE(t.has_value());
        EXPECT_EQ(t->code, kUnknownTopic);
    }
}

// 15. A denial that is not NotAuthorized (the topic is placed in the committed routing image but this node has not applied it to its
//     object index yet) is the route refusal, not a lease frame: a bare NotLeader the client retries.
TEST(LeaseGatewayRouteRegression, NonNotAuthorizedDenialIsRouteRefusalRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)}, 0, 0, /*indexTopic=*/false);
    for (const auto& f : {g.kaFrame(1, {{1, v}}, kTopic, user), g.ttlFrame(2, 1, kTopic, user)}) {
        const auto fr = g.send(f);
        ASSERT_EQ(fr.size(), 1u);
        expectErrorFrame(fr[0], pc::ErrorCode::NotLeader);
    }
    g.settle();
    EXPECT_EQ(g.out.size(), 2u);
}

// 16. Led elsewhere with no known endpoint of the leader: the bare NotLeader (no Redirect target to name).
TEST(LeaseGatewayRouteRegression, LedElsewhereNoEndpointRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    g.setLeader(kOther);
    g.gw.setNodeEndpointMap({});
    for (const auto& f : {g.kaFrame(1, {{1, v}}), g.ttlFrame(2, 1)}) {
        const auto fr = g.send(f);
        ASSERT_EQ(fr.size(), 1u);
        expectErrorFrame(fr[0], pc::ErrorCode::NotLeader);
        const auto err = pc::ErrorResponse::decode(fr[0].data(), fr[0].size());
        EXPECT_TRUE(err->message.empty());
    }
    g.settle();
    EXPECT_EQ(g.out.size(), 2u);
}

// 17. The auth gates in front of the handler: a session that has not logged in gets AuthenticationFailed (the lease frame) and ends;
//     the anonymous reader may not keep alive (WRITE): NotAuthorized; a __security view that is behind: AuthUnavailable.
TEST(LeaseGatewayRouteRegression, AuthGateCellsRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const auto kAuthFailed = code(pc::ErrorCode::AuthenticationFailed);
    const auto kUnavailable = code(pc::ErrorCode::AuthUnavailable);

    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)});
    const auto awaiting = [&] {   // a session that has sent nothing yet (injectTestSession gives an anonymous one)
        const int64_t s = g.gw.injectTestSession();
        g.gw.setSessionAuthForTest(s, rune::gateway::AuthAwaitingFirst{INT64_MAX / 2, 0, false});
        return s;
    };
    const int64_t fresh = awaiting();
    const auto ka = g.send(g.kaFrame(1, {{1, v}}, kTopic, fresh));
    ASSERT_EQ(ka.size(), 1u);
    expectWholeKa(ka[0], 1, kAuthFailed);
    const auto tt = g.send(g.ttlFrame(2, 1, kTopic, awaiting()));
    ASSERT_EQ(tt.size(), 1u);
    const auto t = asTtl(tt[0]);
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->code, kAuthFailed);
    EXPECT_FALSE(g.gw.hasSessionForTest(fresh)) << "the unauthenticated session ends after its answer (nothing was spilled)";

    g.gw.setAnonymousRead(true);
    const int64_t anon = awaiting();
    const auto ak = g.send(g.kaFrame(3, {{1, v}}, kTopic, anon));
    ASSERT_EQ(ak.size(), 1u);
    expectWholeKa(ak[0], 3, kNotAuthorized);
    EXPECT_TRUE(g.gw.hasSessionForTest(anon)) << "the anonymous reader stays";

    g.gw.setSecurityBehind(true);
    const auto bk = g.send(g.kaFrame(4, {{1, v}}, kTopic, user));
    ASSERT_EQ(bk.size(), 1u);
    expectWholeKa(bk[0], 4, kUnavailable);
    const auto bt = g.send(g.ttlFrame(5, 1, kTopic, user));
    ASSERT_EQ(bt.size(), 1u);
    const auto bttl = asTtl(bt[0]);
    ASSERT_TRUE(bttl.has_value());
    EXPECT_EQ(bttl->code, kUnavailable);
}

// 18. Authorisation is read per request: a keepalive is served, the grant is removed, the next one is NotAuthorized.
TEST(LeaseGatewayRouteRegression, RevokedMidSessionRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)});
    std::size_t before = g.out.size();
    g.send(g.kaFrame(1, {{1, v}}, kTopic, user));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto served = asKa(g.out.back());
    ASSERT_TRUE(served.has_value());
    ASSERT_EQ(served->es.size(), 1u);
    EXPECT_EQ(served->es[0].code, 0);

    g.gw.setSessionAuthForTest(user, rune::gateway::AuthAuthenticated{1, "user", rune::gateway::PermissionTable::compile({}, 0, g.idx), "", 0});
    const KaFrame k = g.ka(g.kaFrame(2, {{1, v}}, kTopic, user));
    EXPECT_EQ(k.corr, 2);
    ASSERT_EQ(k.es.size(), 1u);
    EXPECT_EQ(k.es[0].index, kWhole);
    EXPECT_EQ(k.es[0].code, kNotAuthorized);
    const TtlFrame t = g.ttl(g.ttlFrame(3, 1, kTopic, user));
    EXPECT_EQ(t.code, kNotAuthorized);
}

// 19. A cluster admin is exempt from a suspended namespace (RejectAll) and is served; a non-admin session under the Deleting gate is
//     refused (NamespaceSuspended), and a root session is refused with UnknownTopic once the namespace is deleting (inDeletingNs_).
TEST(LeaseGatewayRouteRegression, RootExemptUnderRejectAllRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t root = g.authedSession({grant(rs::Operation::Admin, rs::ResourceType::Cluster, rs::PatternKind::Exact, ""),
                                          topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)},
                                         /*nsId=*/0, /*topicNs=*/0);
    g.gw.setSessionNsGateForTest(root, rune::catalog::NsStateTag::SuspendedRejectAll);
    std::size_t before = g.out.size();
    g.send(g.kaFrame(1, {{1, v}}, kTopic, root));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto ok = asKa(g.out.back());
    ASSERT_TRUE(ok.has_value());
    ASSERT_EQ(ok->es.size(), 1u);
    EXPECT_EQ(ok->es[0].code, 0) << "root is served under RejectAll";
    ASSERT_EQ(ok->es[0].ttl, 5000u);

    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read), topicGrant(rs::Operation::Write)});
    g.gw.setSessionNsGateForTest(user, rune::catalog::NsStateTag::Deleting);
    const KaFrame k = g.ka(g.kaFrame(2, {{1, v}}, kTopic, user));
    ASSERT_EQ(k.es.size(), 1u);
    EXPECT_EQ(k.es[0].code, kSuspended) << "the Deleting gate refuses a non-admin";
    const TtlFrame t = g.ttl(g.ttlFrame(3, 1, kTopic, user));
    EXPECT_EQ(t.code, kSuspended);
}

// 20. A worker return the gateway cannot forward as it is (count / chunk mismatch, a TimeToLive chunk that is not 16 B) is a broker bug:
//     counted and answered Internal; well-formed returns (incl. the worker's own whole-request refusals and the NotLeader a demotion
//     gives a parked entry) are forwarded byte for byte.  Mutation: drop the count / chunk check -> the mismatch legs are forwarded.
TEST(LeaseGatewayRouteRegression, MalformedWorkerReturnRegression) {
    GwLease g;
    ASSERT_TRUE(g.setup({1}));
    const uint16_t kInternal = code(pc::ErrorCode::Internal);
    ASSERT_EQ(g.gw.leaseReturnMalformed(), 0u);

    uint64_t bad = 0;
    const auto malformed = [&](WorkerReturn w) {
        const std::size_t before = g.out.size();
        g.gw.handleWorkerReturnForTest(w);
        ++bad;
        ASSERT_EQ(g.out.size(), before + 1);
        expectWholeKa(g.out.back(), w.correlationId, kInternal);
        EXPECT_EQ(g.gw.leaseReturnMalformed(), bad);
    };
    malformed(kaReturn(g, 71, 2, entryBytes({{0, 0, 5000}}), true));                       // count says 2, chunk is 1 entry
    malformed(kaReturn(g, 72, 1, entryBytes({{0, 0, 5000}, {1, 0, 5000}}), true));         // count says 1, chunk is 2 entries
    malformed(kaReturn(g, 73, 0, {}, true));                                               // no entries
    malformed(kaReturn(g, 74, 257, std::vector<uint8_t>(257 * 12, 0), true));              // above the wire maximum
    malformed(kaReturn(g, 75, 1, std::vector<uint8_t>(11, 0), false));                     // short chunk; final is forced to 1

    const std::size_t before = g.out.size();
    g.gw.handleWorkerReturnForTest(ttlReturn(g, 76, std::vector<uint8_t>(15, 0)));
    ASSERT_EQ(g.out.size(), before + 1);
    const auto t = asTtl(g.out.back());
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->corr, 76);
    EXPECT_EQ(t->code, kInternal);
    EXPECT_EQ(t->remaining, -1);
    EXPECT_EQ(t->granted, 0u);
    EXPECT_EQ(t->holder, 0u);
    EXPECT_EQ(t->version, 0);
    EXPECT_EQ(g.gw.leaseReturnMalformed(), bad + 1);

    // well-formed: forwarded unchanged, byte for byte what LeaseKeepAliveResponse::encode gives for the same entries
    const std::array<pc::LeaseKeepAliveResponse::Entry, 3> es = {{{0, 0, 5000}, {1, code(pc::ErrorCode::LeaseNotFound), 0}, {2, 0, 123456789012ULL}}};
    pc::LeaseKeepAliveResponse expect;
    expect.sessionId = g.sid; expect.correlationId = 77; expect.final = 0;
    expect.entries = es;
    std::vector<uint8_t> bytes(expect.encodedSize());
    bytes.resize(expect.encode(bytes.data()));
    g.gw.handleWorkerReturnForTest(kaReturn(g, 77, 3, entryBytes({{0, 0, 5000}, {1, code(pc::ErrorCode::LeaseNotFound), 0}, {2, 0, 123456789012ULL}}), false));
    EXPECT_EQ(g.out.back(), bytes);
    for (const uint16_t c : {kNotLeader, kUnknownTopic}) {   // a worker's own whole-request refusal
        g.gw.handleWorkerReturnForTest(kaReturn(g, 78, 1, entryBytes({{kWhole, c, 0}}), true));
        expectWholeKa(g.out.back(), 78, c);
    }
    EXPECT_EQ(g.gw.leaseReturnMalformed(), bad + 1) << "well-formed returns are not counted";

    // a real worker: an entry parked behind a clear is answered NotLeader when the partition is demoted
    ASSERT_TRUE(g.setup({2}));
    const int64_t v2 = g.leaseVersion(2);
    ASSERT_TRUE(g.checkpoint(2, v2, 4000));
    g.r.holdJudge(true);
    const std::size_t b2 = g.out.size();
    g.send(g.kaFrame(79, {{2, v2}}));
    g.settle(10);
    ASSERT_EQ(g.r.worker->leaseParksForTest().inUse(), 1u);
    ASSERT_EQ(g.out.size(), b2);
    g.r.demote();
    g.settle();
    ASSERT_EQ(g.out.size(), b2 + 1);
    const auto parked = asKa(g.out.back());
    ASSERT_TRUE(parked.has_value());
    EXPECT_EQ(parked->corr, 79);
    EXPECT_TRUE(parked->final);
    ASSERT_EQ(parked->es.size(), 1u);
    EXPECT_EQ(parked->es[0].index, 0);
    EXPECT_EQ(parked->es[0].code, kNotLeader);
}

// 21. A two-partition lease topic whose partition 1 is led (and owned by the worker) here and partition 0 elsewhere: a keepalive and
//     a TimeToLive on partition 1 are served by partition 1's worker, partition 0 redirects.  Mutation: route every request as
//     partition 0 (the leader check in leaseRouteAdmit_) -> the partition 1 requests are redirected (red).
TEST(LeaseGatewayRouteRegression, SecondPartitionRoutedRegression) {
    GwLease g(1 << 22, /*partitionOfRig=*/1, /*partitionCount=*/2);
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);

    std::size_t before = g.out.size();
    g.send(g.kaFrame(1, {{1, v}}, kTopic, 0, /*partition=*/1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto k = asKa(g.out.back());
    ASSERT_TRUE(k.has_value());
    EXPECT_EQ(k->corr, 1);
    ASSERT_EQ(k->es.size(), 1u);
    EXPECT_EQ(k->es[0].index, 0);
    EXPECT_EQ(k->es[0].code, 0);
    EXPECT_EQ(k->es[0].ttl, 5000u);
    before = g.out.size();
    g.send(g.ttlFrame(2, 1, kTopic, 0, /*partition=*/1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto t = asTtl(g.out.back());
    ASSERT_TRUE(t.has_value());
    EXPECT_EQ(t->code, 0);
    EXPECT_EQ(t->version, v);

    const auto other = g.send(g.kaFrame(3, {{1, v}}, kTopic, 0, /*partition=*/0));
    ASSERT_EQ(other.size(), 1u);
    const auto err = pc::ErrorResponse::decode(other[0].data(), other[0].size());
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(err->message, kOtherEndpoint);
}

// 22. Lease topics Task 13a (spec 7.1 / 14.1): a lease view read needs READ on the lease TOPIC; a VIEW grant on `<t>.leases` is not
//     consulted.  A foreign-namespace topic answers byte for byte like a missing id.  Mutation: keep the VIEW check in
//     authorizeViewRead_ -> the topic-READ-only session is refused and the VIEW-only session is served (red).
TEST(LeaseGatewayRouteRegression, ReadOnTopicAuthorisesViewReadRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const uint16_t kOk = pc::ViewRowResponse::kOk, kNotAuth = pc::ViewRowResponse::kNotAuthorized;

    const int64_t topicRead = g.authedSession({topicGrant(rs::Operation::Read)});
    std::size_t before = g.out.size();
    g.send(g.viewFrame(1, 1, -1, topicRead));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(GwLease::viewStatus(g.out.back()), kOk) << "READ on the topic serves the lease view";

    const int64_t viewOnly = g.authedSession({grant(rs::Operation::Read, rs::ResourceType::View, rs::PatternKind::Prefix, "")});
    const auto denied = g.send(g.viewFrame(2, 1, -1, viewOnly));
    ASSERT_EQ(denied.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(denied[0]), kNotAuth) << "a VIEW grant on <t>.leases alone does not authorise";

    const int64_t none = g.authedSession({});
    const auto nothing = g.send(g.viewFrame(3, 1, -1, none));
    ASSERT_EQ(nothing.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(nothing[0]), kNotAuth);

    // a topic of another namespace is a missing id, byte for byte
    constexpr int32_t kNever = 78;
    const int64_t foreign = g.authedSession({topicGrant(rs::Operation::Read)}, /*nsId=*/1, /*topicNs=*/2);
    const auto fr = g.send(g.viewFrame(4, 1, -1, foreign));
    const auto missing = g.send(g.viewFrame(4, 1, -1, foreign, kNever));
    ASSERT_EQ(fr.size(), 1u);
    ASSERT_EQ(missing.size(), 1u);
    EXPECT_EQ(fr[0], missing[0]) << "a foreign lease topic is a missing id";
    EXPECT_EQ(GwLease::viewStatus(fr[0]), kNotAuth);
}

// 23. A lease view of a partition led elsewhere is redirected to the SOURCE leader (the sink mirrors its source's partitioning).
//     Mutation: skip noteViewSource for the lease view (addLeaseView) -> no source leader is found, the read is not redirected (red).
TEST(LeaseGatewayRouteRegression, LeaseViewRoutesToSourceLeaderRegression) {
    GwLease g(1 << 22, /*partitionOfRig=*/1, /*partitionCount=*/2);
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));

    const auto other = g.send(g.viewFrame(1, 1, /*partition=*/0));
    ASSERT_EQ(other.size(), 1u);
    const auto err = pc::ErrorResponse::decode(other[0].data(), other[0].size());
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(err->message, kOtherEndpoint);

    const std::size_t before = g.out.size();
    g.send(g.viewFrame(2, 1, /*partition=*/1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(GwLease::viewStatus(g.out.back()), int32_t(pc::ViewRowResponse::kOk)) << "the led partition is served";
}

// 24. The lease view -> source mapping has ONE source of truth: the TopicRegistry's view-sink spec.  Dropping the sink in the registry (what the
//     broker's dropRuntimeTopic_ does for a view sink) clears the mapping by construction: both accessors answer -1, the read is UnknownView at
//     the gate and never reaches the worker; a READ-on-topic session does not reach it through the lease rule either.
//     Mutation: make viewSinkSpec answer a Dropped slot's spec (the sink outlives its drop) -> the accessors keep the source (red).
TEST(LeaseGatewayRouteRegression, LeaseViewDropClearsMappingRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    EXPECT_EQ(g.gw.leaseSourceOfView_(GwLease::kView), kTopic);
    EXPECT_EQ(g.gw.viewSourceOf_(GwLease::kView), kTopic);
    EXPECT_EQ(g.gw.leaseSourceOfView_(kTopic), -1) << "the topic itself is not a lease view";
    EXPECT_EQ(g.gw.leaseSourceOfView_(99), -1);
    std::size_t before = g.out.size();
    g.send(g.viewFrame(1, 1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(GwLease::viewStatus(g.out.back()), int32_t(pc::ViewRowResponse::kOk));

    ASSERT_TRUE(g.registry->dropTopic(GwLease::kView));
    EXPECT_EQ(g.gw.leaseSourceOfView_(GwLease::kView), -1) << "the registry entry died with the sink";
    EXPECT_EQ(g.gw.viewSourceOf_(GwLease::kView), -1);
    const auto gone = g.send(g.viewFrame(2, 1));
    ASSERT_EQ(gone.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(gone[0]), int32_t(pc::ViewRowResponse::kUnknownView));
    g.settle();
    EXPECT_EQ(g.out.size(), before + 2) << "nothing was routed to the worker";

    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read)});
    const auto after = g.send(g.viewFrame(3, 1, -1, user));
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(after[0]), int32_t(pc::ViewRowResponse::kNotAuthorized)) << "a dropped lease view falls back to the VIEW rule: no grant";
}

// ---- Task 13a review fixes (M4): the matrix cells of the lease view reads through the gateway ----------------------------------------

namespace {
/// A legacy GetViewRow (72): the u64 key.
std::vector<uint8_t> legacyViewFrame(const GwLease& g, int64_t c, uint64_t key, int64_t session = 0, int32_t view = GwLease::kView) {
    pc::GetViewRow m;
    m.sessionId = session != 0 ? session : g.sid; m.correlationId = c; m.viewTopicId = view; m.keyU64 = key;
    std::vector<uint8_t> f(pc::GetViewRow::ENCODED_SIZE);
    m.encode(f.data());
    return f;
}
std::vector<uint8_t> scanViewFrame(const GwLease& g, int64_t c, int64_t session = 0, int32_t view = GwLease::kView) {
    pc::ScanViewRows m;
    m.sessionId = session != 0 ? session : g.sid; m.correlationId = c; m.viewTopicId = view; m.limit = 10;
    const uint8_t prefix[1] = {0};
    m.prefix = std::span<const uint8_t>(prefix, 1);
    std::vector<uint8_t> f(m.encodedSize());
    f.resize(m.encode(f.data()));
    return f;
}
int32_t scanStatus(const std::vector<uint8_t>& f) {
    const auto r = pc::ScanViewRowsResponse::decode(f.data(), f.size());
    return r ? int32_t(r->status) : -1;
}
}  // namespace

// 25. The legacy GetViewRow (72) authorises exactly like GetViewRowK: READ on the lease TOPIC; a VIEW-only grant on `<t>.leases` and no grant
//     are NotAuthorized.
//     Mutation: handleGetViewRow checks READ on the VIEW id again (revert authorizeViewRead_) -> the topic-READ session is refused (red).
TEST(LeaseGatewayRouteRegression, LegacyGetViewRowAuthorisesOnTopicRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const uint16_t kOk = pc::ViewRowResponse::kOk, kNotAuth = pc::ViewRowResponse::kNotAuthorized;
    const int64_t topicRead = g.authedSession({topicGrant(rs::Operation::Read)});
    std::size_t before = g.out.size();
    g.send(legacyViewFrame(g, 1, 1, topicRead));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(GwLease::viewStatus(g.out.back()), kOk);
    const auto ok = pc::ViewRowResponse::decode(g.out.back().data(), g.out.back().size());
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(ok->correlationId, 1);
    EXPECT_GT(ok->rowLen, 0u) << "the lease row";

    const int64_t viewOnly = g.authedSession({grant(rs::Operation::Read, rs::ResourceType::View, rs::PatternKind::Prefix, "")});
    const auto denied = g.send(legacyViewFrame(g, 2, 1, viewOnly));
    ASSERT_EQ(denied.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(denied[0]), kNotAuth);
    const int64_t none = g.authedSession({});
    const auto nothing = g.send(legacyViewFrame(g, 3, 1, none));
    ASSERT_EQ(nothing.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(nothing[0]), kNotAuth);
    g.settle();
}

// 26. ScanViewRows on a lease view is not served (Task 13b's ScanLeases is the scan): an authorised read reaches the worker and is
//     UnknownView; a session without READ on the topic is refused first (NotAuthorized).
//     Mutation: serve the scan through the plan-view scan in the worker's lease branch -> not UnknownView (red).
TEST(LeaseGatewayRouteRegression, ScanViewRowsOnLeaseViewRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const int64_t topicRead = g.authedSession({topicGrant(rs::Operation::Read)});
    std::size_t before = g.out.size();
    g.send(scanViewFrame(g, 1, topicRead));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(scanStatus(g.out.back()), int32_t(pc::ViewRowResponse::kUnknownView));
    const int64_t none = g.authedSession({});
    const auto denied = g.send(scanViewFrame(g, 2, none));
    ASSERT_EQ(denied.size(), 1u);
    EXPECT_EQ(scanStatus(denied[0]), int32_t(pc::ViewRowResponse::kNotAuthorized));
}

// 27. Not logged in / anonymous: a session that has sent nothing is AuthenticationFailed and ends; with anonymous read switched on the anonymous
//     reader is NotAuthorized for a lease view (no READ bit on the lease topic), whichever read frame it sends.
//     Mutation: authorise a lease view for the anonymous reader on the VIEW rule -> red.
TEST(LeaseGatewayRouteRegression, LeaseViewAnonymousRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    (void)g.authedSession({topicGrant(rs::Operation::Read)});   // Required mode, the object index
    const auto awaiting = [&] {
        const int64_t s = g.gw.injectTestSession();
        g.gw.setSessionAuthForTest(s, rune::gateway::AuthAwaitingFirst{INT64_MAX / 2, 0, false});
        return s;
    };
    const int64_t fresh = awaiting();
    const auto fr = g.send(g.viewFrame(1, 1, -1, fresh));
    ASSERT_EQ(fr.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(fr[0]), int32_t(pc::ErrorCode::AuthenticationFailed)) << "not logged in";
    g.gw.setAnonymousRead(true);
    const int64_t anon = awaiting();
    const auto k = g.send(g.viewFrame(2, 1, -1, anon));
    ASSERT_EQ(k.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(k[0]), int32_t(pc::ViewRowResponse::kNotAuthorized));
    const auto l = g.send(legacyViewFrame(g, 3, 1, anon));
    ASSERT_EQ(l.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(l[0]), int32_t(pc::ViewRowResponse::kNotAuthorized));
}

// 28. The gateway authorises a read at REQUEST time and keeps no per-request state: a read held at the worker behind the fold, whose grant is
//     revoked before it is re-served, is still answered with the row.  A read sent after the revoke is NotAuthorized.  (Documented behaviour:
//     revocation takes effect for the next request; the answer to an already-admitted read is not recalled.)
//     Mutation: authorise on the return path too -> the held row is not delivered (red).
TEST(LeaseGatewayRouteRegression, LeaseViewGrantRevokedWhileHeldRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    const int64_t user = g.authedSession({topicGrant(rs::Operation::Read)});
    g.r.holdJudge(true);
    rune::protocol::RecordBatchBuilder b(0, 0);
    const auto val = leaseValue(1, v, "a", 5000, 3000);
    b.append(keyOf(1), std::string_view(reinterpret_cast<const char*>(val.data()), val.size()), 0, rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
    const int64_t at = g.r.replica->appendLeaseBatch(b.build());
    ASSERT_GE(at, 0);
    g.settle(5);
    // a GetViewRowK that must see the checkpoint (minOffset = its offset): held while the judge is held
    pc::GetViewRowK m;
    m.sessionId = user; m.correlationId = 7; m.viewTopicId = GwLease::kView; m.partition = -1; m.minOffset = at;
    const std::string k1 = keyOf(1);
    m.key = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k1.data()), k1.size());
    std::vector<uint8_t> f(m.encodedSize());
    m.encode(f.data());
    const std::size_t before = g.out.size();
    g.send(f);
    g.settle(10);
    ASSERT_EQ(g.out.size(), before) << "held: no answer yet";
    ASSERT_EQ(g.r.worker->heldViewReadsForTest(), 1u);
    g.gw.setSessionAuthForTest(user, rune::gateway::AuthAuthenticated{1, "user", rune::gateway::PermissionTable::compile({}, 0, g.idx), "", 0});
    g.r.holdJudge(false);
    ASSERT_TRUE(g.waitFrames(before, 1));
    const auto held = pc::ViewRowResponse::decode(g.out.back().data(), g.out.back().size());
    ASSERT_TRUE(held.has_value());
    EXPECT_EQ(held->correlationId, 7);
    EXPECT_EQ(held->status, pc::ViewRowResponse::kOk) << "admitted before the revoke: the row is delivered";
    const auto after = g.send(g.viewFrame(8, 1, -1, user));
    ASSERT_EQ(after.size(), 1u);
    EXPECT_EQ(GwLease::viewStatus(after[0]), int32_t(pc::ViewRowResponse::kNotAuthorized)) << "the next request is refused";
}

// 29. The worker's not-leader sentinel on the RETURN path: the gateway admitted the read (its image said this node leads), the read is held,
//     the worker is demoted and the image now names another node: the held read's sentinel answers a Redirect to the image's leader through
//     viewSourceLedElsewhere_ (the lease view maps to its SOURCE topic's routing).  With the image still naming this node the sentinel is a
//     bare NotLeader.
//     Mutation: encode the sentinel as a ViewRowResponse status / skip viewSourceLedElsewhere_ -> no endpoint (red).
TEST(LeaseGatewayRouteRegression, LeaseViewSentinelRedirectsOnReturnRegression) {
    GwLease g;
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const int64_t v = g.leaseVersion(1);
    g.r.holdJudge(true);
    rune::protocol::RecordBatchBuilder b(0, 0);
    const auto val = leaseValue(1, v, "a", 5000, 3000);
    b.append(keyOf(1), std::string_view(reinterpret_cast<const char*>(val.data()), val.size()), 0, rec::withLeaseOp(0, rec::LeaseOp::Checkpoint));
    const int64_t at = g.r.replica->appendLeaseBatch(b.build());
    ASSERT_GE(at, 0);
    g.settle(5);
    const auto heldFrame = [&](int64_t c) {
        pc::GetViewRowK m;
        m.sessionId = g.sid; m.correlationId = c; m.viewTopicId = GwLease::kView; m.partition = -1; m.minOffset = at;
        static const std::string k1 = keyOf(1);
        m.key = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(k1.data()), k1.size());
        std::vector<uint8_t> f(m.encodedSize());
        m.encode(f.data());
        return f;
    };
    // (a) the image changes to another leader while the read is held
    std::size_t before = g.out.size();
    g.send(heldFrame(1));
    g.settle(10);
    ASSERT_EQ(g.out.size(), before);
    g.setLeader(kOther);
    g.r.demote();
    g.settle(20);
    ASSERT_EQ(g.out.size(), before + 1);
    const auto err = pc::ErrorResponse::decode(g.out.back().data(), g.out.back().size());
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
    EXPECT_EQ(err->message, kOtherEndpoint);
    EXPECT_EQ(err->correlationId, 1);
    // (b) the image still names this node: a bare NotLeader (the client retries)
    g.setLeader(kNode);
    before = g.out.size();
    g.send(heldFrame(2));
    g.settle(20);
    ASSERT_EQ(g.out.size(), before + 1);
    const auto bare = pc::ErrorResponse::decode(g.out.back().data(), g.out.back().size());
    ASSERT_TRUE(bare.has_value());
    EXPECT_EQ(bare->code, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_TRUE(bare->message.empty());
    EXPECT_EQ(bare->correlationId, 2);
    g.r.holdJudge(false);
}

// 30. The partition argument of a lease view read on a TWO-partition topic whose partition 1 this node leads: -1 (the whole-key form is only
//     valid on a one-partition sink) and == partitionCount are InvalidRequest at the gateway; partition 1 is served by its leader; partition
//     0 (led elsewhere) redirects.
//     Mutation: accept partition -1 / == count on a wider sink -> red.
TEST(LeaseGatewayRouteRegression, LeaseViewPartitionRangeRegression) {
    GwLease g(1 << 22, /*partitionOfRig=*/1, /*partitionCount=*/2);
    g.addLeaseView();
    ASSERT_TRUE(g.setup({1}));
    const uint16_t kInvalidReq = pc::ViewRowResponse::kInvalidRequest;
    for (const int32_t bad : {-1, 2, 3, INT32_MIN, INT32_MAX}) {
        const auto fr = g.send(g.viewFrame(10, 1, bad));
        ASSERT_EQ(fr.size(), 1u) << bad;
        EXPECT_EQ(GwLease::viewStatus(fr[0]), kInvalidReq) << "partition " << bad;
    }
    const std::size_t before = g.out.size();
    g.send(g.viewFrame(11, 1, /*partition=*/1));
    ASSERT_TRUE(g.waitFrames(before, 1));
    EXPECT_EQ(GwLease::viewStatus(g.out.back()), int32_t(pc::ViewRowResponse::kOk));
    const auto other = g.send(g.viewFrame(12, 1, /*partition=*/0));
    ASSERT_EQ(other.size(), 1u);
    const auto err = pc::ErrorResponse::decode(other[0].data(), other[0].size());
    ASSERT_TRUE(err.has_value());
    EXPECT_EQ(err->code, static_cast<int16_t>(pc::ErrorCode::Redirect));
}
