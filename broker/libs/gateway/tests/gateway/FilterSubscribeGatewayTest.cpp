// FilterSubscribeGatewayTest.cpp - Phase 2c Task 9: the gateway admits, refuses and forwards filtered Subscribes.
//
// A filter is read only from a session >= 10; it is validated against the topic's FilterSchema (installed by
// onFilterSchema), admitted under the per-(topic, partition) caps, and forwarded to the owning worker in the
// Subscribe op's tail.  The schema reaches every worker BEFORE any filtered Subscribe (FIFO with the queued ops).
// Every refusal is answered {InvalidRequest, detail} to a version-10 session; older sessions never see a detail.
//
// Deterministic: no Aeron, no sleeps; the gateway and the one worker are driven by hand, null-egress test sessions
// are observed through onEgressFrame (same pattern as SubscribeDedupeTest).
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/FilterWire.h>
#include <rune/codec/Serialize.h>
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/node/WorkerPool.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>

#include "filter/PlayerEventSchema.h"

#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using rune::codec::PrimType;

namespace {

constexpr int16_t kInvalid = static_cast<int16_t>(pc::ErrorCode::InvalidRequest);
constexpr int16_t kOk      = static_cast<int16_t>(pc::ErrorCode::OK);

// ---- the player_event schema (tags: playerId 0 u64, worldId 1 u32, channel 2 i32, zone 3 str(8), kind 4 enum,
//      alive 5 bool, hp 6 i64, name 7 string, pos 8 f64); v2 ADD guild u32 (tag 9); v3 ADD level u32 (tag 10) ----
rune::codec::Descriptor playerEvent(uint16_t uptoVersion) {
    using namespace rune::codec;
    Descriptor d = rune::examples::playerEventDescriptor();
    uint16_t nextTag = 9;
    if (uptoVersion >= 2) {
        auto r = applyOps(d, {{AlterOpType::Add, "guild", "", PrimType::UInt32, 0, {}}}, nextTag, 2);
        if (!r) throw std::runtime_error("add guild");
        d = *r;
    }
    if (uptoVersion >= 3) {
        auto r = applyOps(d, {{AlterOpType::Add, "level", "", PrimType::UInt32, 0, {}}}, nextTag, 3);
        if (!r) throw std::runtime_error("add level");
        d = *r;
    }
    return d;
}
std::vector<uint8_t> schemaBlob(uint16_t version) { return rune::codec::serialize(playerEvent(version)); }

// ---- raw canonical filter bytes: [1][n] term{n}; term = [tag u16][type u8][op u8][count u8][value x count] ----
struct RawTerm {
    uint16_t              tag;
    PrimType              type;
    uint8_t               width;
    uint8_t               op;       // 1 Eq, 2 In
    std::vector<uint64_t> values;   // little-endian, `width` bytes each (sorted, unique for In)
};
std::vector<uint8_t> rawFilter(const std::vector<RawTerm>& terms) {
    std::vector<uint8_t> b = {1, static_cast<uint8_t>(terms.size())};
    for (const auto& t : terms) {
        b.push_back(static_cast<uint8_t>(t.tag));
        b.push_back(static_cast<uint8_t>(t.tag >> 8));
        b.push_back(static_cast<uint8_t>(t.type));
        b.push_back(t.op);
        b.push_back(static_cast<uint8_t>(t.values.size()));
        for (uint64_t v : t.values)
            for (uint8_t i = 0; i < t.width; ++i) b.push_back(static_cast<uint8_t>(v >> (8 * i)));
    }
    return b;
}
std::vector<uint8_t> worldEq(uint32_t w) { return rawFilter({{1, PrimType::UInt32, 4, 1, {w}}}); }
std::vector<uint8_t> worldIn(std::vector<uint64_t> ws) { return rawFilter({{1, PrimType::UInt32, 4, 2, std::move(ws)}}); }

std::vector<uint8_t> subscribeFrame(int64_t sid, int64_t corr, int32_t partition, std::span<const uint8_t> filter,
                                    int32_t topicId = 0) {
    pc::Subscribe s;
    s.sessionId      = sid;
    s.correlationId  = corr;
    s.topicId        = topicId;
    s.partition      = partition;
    s.partitionKnown = 1;
    s.fromOffset     = 0;
    s.isolation      = 0;
    s.filter         = filter;
    std::vector<uint8_t> buf(s.encodedSize());
    buf.resize(s.encode(buf.data()));
    return buf;
}
std::vector<uint8_t> unsubscribeFrame(int64_t sid, int64_t subId) {
    pc::Unsubscribe u;
    u.sessionId      = sid;
    u.correlationId  = 0;
    u.subscriptionId = subId;
    std::vector<uint8_t> buf(64);
    buf.resize(u.encode(buf.data()));
    return buf;
}

struct TempDir {
    fs::path path;
    TempDir() {
        std::random_device rd;
        path = fs::temp_directory_path() / ("rune-filter-sub-" + std::to_string(rd()));
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

struct Answer {
    pc::SubscribeResponse r;
    std::string           detail;
    std::size_t           len = 0;
};

// Gateway + topic 0 ("events", one partition) + a one-worker pool with per-worker egress active.
struct Rig {
    TempDir                                 dir;
    std::unique_ptr<TopicRegistry>          registry;
    std::unique_ptr<rune::node::WorkerPool> pool;
    rune::replication::InMemoryTransport    transport;
    rune::node::ReplicaRegistry             replicas;
    ClientGateway                           gw;
    std::vector<Answer>                     answers;

    explicit Rig(bool workers = true, std::size_t ringBytes = 1 << 16, int32_t partitions = 1)
        : gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        rune::node::TopicSpec ts;
        ts.name           = "events";
        ts.partitionCount = partitions;
        ts.topicId        = 0;
        registry = std::make_unique<TopicRegistry>(std::vector<rune::node::TopicSpec>{ts}, dir.path.string());
        gw.setTopicRegistry(registry.get());
        gw.setOnEgressFrame([this](int64_t, const uint8_t* b, std::size_t n) {
            if (auto r = pc::SubscribeResponse::decode(b, n)) {
                Answer a;
                a.r      = *r;
                a.detail = std::string(r->detail);
                a.r.detail = {};
                a.len    = n;
                answers.push_back(std::move(a));
            }
        });
        if (workers) {
            pool = std::make_unique<rune::node::WorkerPool>(1, rune::metadata::ShardRouter(1), ringBytes, 1 << 16, 16, 16);
            gw.setWorkerPool(pool.get());
            gw.setShardMode(true);
            gw.setPerWorkerEgressActive(true);
            for (int32_t p = 0; p < partitions; ++p) {
                rune::replication::PartitionConfig c;
                c.topicId = 0; c.partitionId = p; c.selfId = 1; c.minIsr = 1;
                c.replicaLagMaxOffset = 1'000'000'000; c.replicaLagMaxMs = 600'000;
                auto rep = std::make_unique<rune::replication::PartitionReplica>(
                    c, rune::replication::Role::Leader, 1, std::vector<int32_t>{1}, std::vector<int32_t>{1},
                    registry->log(0, p), &transport);
                rep->setExternalFollowerFlush(true);
                replicas.add(0, p, std::move(rep));
            }
            gw.setReplicaRegistry(&replicas);
        }
    }
    int64_t session(int32_t clientVersion = 10) { return gw.injectTestSession(0, clientVersion); }
    void typed(uint16_t version = 1) { gw.onFilterSchema(0, version, schemaBlob(version)); }
    const Answer& subscribe(int64_t sid, int64_t corr, std::span<const uint8_t> filter, int32_t partition = 0,
                            int32_t topicId = 0) {
        const auto f = subscribeFrame(sid, corr, partition, filter, topicId);
        const std::size_t before = answers.size();
        gw.applySubscribeForTest(f.data(), f.size());
        EXPECT_EQ(answers.size(), before + 1) << "every Subscribe is answered exactly once";
        return answers.back();
    }
    void unsubscribe(int64_t sid, int64_t subId) {
        const auto f = unsubscribeFrame(sid, subId);
        gw.applyUnsubscribeForTest(f.data(), f.size());
    }
    void drain(int rounds = 64) {
        for (int i = 0; i < rounds; ++i) {
            pool->worker(0).doWork(10 + i);
            (void)gw.pumpRetryQueuesForTest();
        }
    }
    // Fill worker 0's tiny ingress ring until a Subscribe op is queued behind it.
    void fillRing() {
        for (int64_t i = 1000; i < 1400 && gw.pendingSubOpsForTest() == 0; ++i)
            gw.forwardSubscribeForTest(i, /*sessionId=*/0, 0, 0, 0, 0, false, false);
        ASSERT_GT(gw.pendingSubOpsForTest(), 0u) << "the ring is full";
    }
};

}  // namespace

TEST(GatewayFilter, ValidFilterIsAdmittedAndForwardedWithItsBytes) {
    Rig r;
    r.typed();
    const int64_t sid = r.session();
    const auto f = worldEq(7);
    const Answer& a = r.subscribe(sid, 1, f);
    ASSERT_EQ(a.r.errorCode, kOk) << a.detail;
    EXPECT_EQ(a.len, pc::SubscribeResponse::ENCODED_SIZE) << "a success carries no detail";
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 1u);
    EXPECT_EQ(r.gw.filterAdmissionForTest().valuesFor(0, 0), 1u);
    EXPECT_EQ(r.gw.subscriptionFilterForTest(a.r.subscriptionId), f) << "the gateway holds the exact bytes";

    r.pool->worker(0).doWork(1);
    EXPECT_EQ(r.pool->worker(0).filterBindFailuresForTest(), 0u);
    ASSERT_NE(r.pool->worker(0).filterGroupOfSubForTest(a.r.subscriptionId), nullptr)
        << "the worker bound the subscription to a FilterGroup: the Subscribe op carried TailKind::FILTER";
    EXPECT_EQ(r.pool->worker(0).filterBytesHeldForTest(), 1u) << "the worker kept the same filter bytes for recompiles";
}

TEST(GatewayFilterRegression, FilterSchemaReachesTheWorkerBeforeAnyFilteredSubscribeRegression) {
    Rig r(true, 1 << 12);
    r.fillRing();                       // the worker ring is full when the schema arrives
    r.typed();
    EXPECT_NE(r.gw.pendingSubOpKindsForTest(0).find('F'), std::string::npos) << "the schema op is queued, not dropped";
    const int64_t sid = r.session();
    const Answer& a = r.subscribe(sid, 1, worldEq(7));
    ASSERT_EQ(a.r.errorCode, kOk) << a.detail;
    const std::string kinds = r.gw.pendingSubOpKindsForTest(0);
    ASSERT_GE(kinds.size(), 2u);
    EXPECT_LT(kinds.find('F'), kinds.rfind('S')) << "queued order " << kinds;
    EXPECT_EQ(kinds.back(), 'S');

    r.drain();
    ASSERT_EQ(r.gw.pendingSubOpsForTest(), 0u);
    // A Subscribe that overtook its schema would have failed to bind ("owed" error on the worker).
    EXPECT_EQ(r.pool->worker(0).filterBindFailuresForTest(), 0u) << "SILENT: Subscribe overtook the FilterSchema op";
    EXPECT_NE(r.pool->worker(0).filterGroupOfSubForTest(a.r.subscriptionId), nullptr);
}

TEST(GatewayFilterRegression, QueuedSubscribeUnsubscribeAndSchemaOpsKeepTheirOrderRegression) {
    Rig r(true, 1 << 12);
    r.fillRing();
    const std::string fillers = r.gw.pendingSubOpKindsForTest(0);
    r.typed();                                                           // F
    const int64_t sid = r.session();
    const Answer& a = r.subscribe(sid, 1, worldEq(7));                   // S
    const int64_t subA = a.r.subscriptionId;
    ASSERT_EQ(a.r.errorCode, kOk) << a.detail;
    r.unsubscribe(sid, subA);                                            // U
    r.typed(2);                                                          // F (a schema replacement after them)
    const Answer& b = r.subscribe(sid, 2, worldEq(8));                   // S
    ASSERT_EQ(b.r.errorCode, kOk) << b.detail;
    EXPECT_EQ(r.gw.pendingSubOpKindsForTest(0), fillers + "FSUFS") << "FIFO: schema before Subscribe, per worker";

    r.drain();
    ASSERT_EQ(r.gw.pendingSubOpsForTest(), 0u);
    std::size_t aOnWorker = 0, bOnWorker = 0;
    for (const auto& s : r.pool->worker(0).subscriptionsForTest()) {
        aOnWorker += (s.subscriptionId == subA);
        bOnWorker += (s.subscriptionId == b.r.subscriptionId);
    }
    EXPECT_EQ(aOnWorker, 0u) << "the Unsubscribe ran AFTER its Subscribe (an inverted pair would leave a zombie)";
    EXPECT_EQ(bOnWorker, 1u);
    EXPECT_EQ(r.pool->worker(0).filterBindFailuresForTest(), 0u);
    EXPECT_NE(r.pool->worker(0).filterGroupOfSubForTest(b.r.subscriptionId), nullptr);
    EXPECT_EQ(r.pool->worker(0).filterBytesHeldForTest(), 1u) << "A's filter state was released with it";
}

TEST(GatewayFilterRegression, UntypedTopicIsRefusedWithDetailRegression) {
    Rig r;   // topic 0 has no installed FilterSchema: untyped
    const int64_t sid = r.session();
    const Answer a = r.subscribe(sid, 1, worldEq(7));
    EXPECT_EQ(a.r.errorCode, kInvalid);
    EXPECT_EQ(a.detail, "filter needs a typed topic");
    EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "nothing is registered";
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 0u);
    for (const int32_t internal : {pc::kCatalogTopicId, pc::kSecurityTopicId}) {
        const Answer b = r.subscribe(sid, 2, worldEq(7), 0, internal);
        EXPECT_EQ(b.r.errorCode, kInvalid) << internal;
        EXPECT_EQ(b.detail, "filter needs a typed topic") << internal;
    }
}

struct MismatchCase {
    const char*          name;
    std::vector<uint8_t> bytes;
    std::string          detail;
};
class GatewayFilterRegression : public ::testing::TestWithParam<MismatchCase> {};

TEST_P(GatewayFilterRegression, EverySchemaMismatchNamesItsTermRegression) {
    Rig r;
    r.typed();
    const int64_t sid = r.session();
    const Answer a = r.subscribe(sid, 1, GetParam().bytes);
    EXPECT_EQ(a.r.errorCode, kInvalid) << GetParam().name;
    EXPECT_EQ(a.detail, GetParam().detail) << GetParam().name;
    EXPECT_EQ(r.gw.subscriptionCount(), 0u);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 0u) << "a refusal holds no registration";
}

INSTANTIATE_TEST_SUITE_P(
    E2toE6, GatewayFilterRegression,
    ::testing::Values(
        MismatchCase{"BadFormatVersion", {9, 1, 1, 0, 3, 1, 1, 7, 0, 0, 0}, "filter: unknown format version"},
        MismatchCase{"TermCountZero", {1, 0}, "filter: term count must be 1..8"},
        MismatchCase{"UnknownTag", rawFilter({{99, PrimType::UInt32, 4, 1, {1}}}), "filter term 0 (tag 99): unknown tag"},
        MismatchCase{"UnknownTagSecondTerm",
                     rawFilter({{1, PrimType::UInt32, 4, 1, {1}}, {99, PrimType::UInt32, 4, 1, {1}}}),
                     "filter term 1 (tag 99): unknown tag"},
        MismatchCase{"TypeMismatch", rawFilter({{1, PrimType::Int32, 4, 1, {5}}}), "filter term 0 (tag 1): type mismatch"},
        MismatchCase{"VariableLength", rawFilter({{7, PrimType::String, 4, 1, {5}}}),
                     "filter term 0 (tag 7): variable-length field"},
        MismatchCase{"FloatField", rawFilter({{8, PrimType::Float64, 8, 1, {5}}}), "filter term 0 (tag 8): float field"}),
    [](const ::testing::TestParamInfo<MismatchCase>& i) { return std::string(i.param.name); });

TEST(GatewayFilterRegression, SubscriptionCapAnswersFilterCapacityRegression) {
    Rig r;
    r.typed();
    r.gw.setFilterLimits({.maxSubs = 2});
    const int64_t sid = r.session();
    const int64_t s1 = r.subscribe(sid, 1, worldEq(1)).r.subscriptionId;
    ASSERT_EQ(r.answers.back().r.errorCode, kOk);
    ASSERT_EQ(r.subscribe(sid, 2, worldEq(2)).r.errorCode, kOk);
    const Answer c = r.subscribe(sid, 3, worldEq(3));
    EXPECT_EQ(c.r.errorCode, kInvalid);
    EXPECT_EQ(c.detail, "filter capacity: 2 filtered subscriptions on topic 0 partition 0 (RUNE_FILTER_MAX_SUBS_PER_PARTITION)");
    EXPECT_EQ(r.gw.subscriptionCount(), 2u);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 2u);

    r.unsubscribe(sid, s1);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 1u);
    EXPECT_EQ(r.subscribe(sid, 4, worldEq(3)).r.errorCode, kOk) << "a freed slot is reusable";
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 2u);
}

TEST(GatewayFilterRegression, IndexValueCapAnswersFilterCapacityRegression) {
    Rig r;
    r.typed();
    r.gw.setFilterLimits({.maxValues = 3});
    const int64_t sid = r.session();
    const Answer big = r.subscribe(sid, 1, worldIn({1, 2, 3, 4}));
    EXPECT_EQ(big.r.errorCode, kInvalid);
    EXPECT_EQ(big.detail, "filter capacity: 3 index values on topic 0 partition 0 (RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION)");
    EXPECT_EQ(r.gw.filterAdmissionForTest().valuesFor(0, 0), 0u) << "a refusal registers nothing";

    ASSERT_EQ(r.subscribe(sid, 2, worldIn({1, 2})).r.errorCode, kOk);
    EXPECT_EQ(r.gw.filterAdmissionForTest().valuesFor(0, 0), 2u);
    ASSERT_EQ(r.subscribe(sid, 3, worldEq(2)).r.errorCode, kOk) << "another member reusing an admitted value costs nothing";
    EXPECT_EQ(r.gw.filterAdmissionForTest().valuesFor(0, 0), 2u);
    ASSERT_EQ(r.subscribe(sid, 4, worldEq(3)).r.errorCode, kOk) << "the third distinct value fits";
    EXPECT_EQ(r.subscribe(sid, 5, worldEq(9)).r.errorCode, kInvalid) << "the fourth does not";
}

// P19: the worker indexes at most kMaxIndexFields (8) distinct fields per partition and refuses a ninth AFTER the
// SubscribeResponse was OK - so the gateway must refuse it.
TEST(GatewayFilterRegression, NinthDistinctIndexFieldAnswersFilterCapacityRegression) {
    Rig r;
    r.gw.onFilterSchema(0, 3, schemaBlob(3));
    const int64_t sid = r.session();
    struct F { uint16_t tag; PrimType type; uint8_t width; uint64_t v; };
    const std::vector<F> fields = {{0, PrimType::UInt64, 8, 1}, {1, PrimType::UInt32, 4, 1}, {2, PrimType::Int32, 4, 1},
                                   {4, PrimType::Enum, 4, 1},   {5, PrimType::Bool, 1, 1},   {6, PrimType::Int64, 8, 1},
                                   {9, PrimType::UInt32, 4, 1}, {10, PrimType::UInt32, 4, 1}, {3, PrimType::FixedStr, 8, 1}};
    int64_t corr = 1;
    std::vector<int64_t> subIds;
    for (std::size_t i = 0; i < 8; ++i) {
        const auto& f = fields[i];
        const Answer a = r.subscribe(sid, corr++, rawFilter({{f.tag, f.type, f.width, 1, {f.v}}}));
        ASSERT_EQ(a.r.errorCode, kOk) << "field " << i << ": " << a.detail;
        subIds.push_back(a.r.subscriptionId);
    }
    EXPECT_EQ(r.gw.filterAdmissionForTest().indexFieldsFor(0, 0), 8u);

    const auto& n = fields[8];
    const Answer ninth = r.subscribe(sid, corr++, rawFilter({{n.tag, n.type, n.width, 1, {n.v}}}));
    EXPECT_EQ(ninth.r.errorCode, kInvalid);
    EXPECT_EQ(ninth.detail.rfind("filter capacity: 8 index fields on topic 0 partition 0", 0), 0u) << ninth.detail;
    EXPECT_EQ(r.gw.subscriptionCount(), 8u);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 8u) << "a refusal holds no registration";

    // A ninth subscription on an ALREADY indexed field is fine.
    EXPECT_EQ(r.subscribe(sid, corr++, rawFilter({{1, PrimType::UInt32, 4, 1, {2}}})).r.errorCode, kOk);
    // Releasing the only member of a field frees its slot for another field.
    r.unsubscribe(sid, subIds[0]);
    EXPECT_EQ(r.gw.filterAdmissionForTest().indexFieldsFor(0, 0), 7u);
    EXPECT_EQ(r.subscribe(sid, corr++, rawFilter({{n.tag, n.type, n.width, 1, {n.v}}})).r.errorCode, kOk);
    EXPECT_EQ(r.gw.filterAdmissionForTest().indexFieldsFor(0, 0), 8u);
}

TEST(GatewayFilterRegression, TicketsAreReleasedOnEveryRemovalPathRegression) {
    Rig r;
    r.typed();
    const auto& adm = r.gw.filterAdmissionForTest();
    // 1. Unsubscribe
    {
        const int64_t sid = r.session();
        const int64_t sub = r.subscribe(sid, 1, worldEq(1)).r.subscriptionId;
        ASSERT_EQ(adm.subsFor(0, 0), 1u);
        r.unsubscribe(sid, sub);
        EXPECT_EQ(adm.subsFor(0, 0), 0u) << "Unsubscribe";
        EXPECT_EQ(adm.valuesFor(0, 0), 0u);
    }
    // 2. session drop
    {
        const int64_t sid = r.session();
        ASSERT_EQ(r.subscribe(sid, 1, worldIn({1, 2, 3})).r.errorCode, kOk);
        ASSERT_EQ(adm.subsFor(0, 0), 1u);
        r.gw.closeSessionForTest(sid);
        EXPECT_EQ(adm.subsFor(0, 0), 0u) << "session drop";
        EXPECT_EQ(adm.valuesFor(0, 0), 0u);
    }
    // 3. topic drop
    {
        const int64_t sid = r.session();
        ASSERT_EQ(r.subscribe(sid, 1, worldEq(5)).r.errorCode, kOk);
        ASSERT_EQ(adm.subsFor(0, 0), 1u);
        EXPECT_EQ(r.gw.endTopicSubscriptions(0), 1u);
        EXPECT_EQ(adm.subsFor(0, 0), 0u) << "topic drop";
        EXPECT_EQ(adm.indexFieldsFor(0, 0), 0u);
    }
    // 4. moved-away retire: the partition's replica leaves this node
    {
        const int64_t sid = r.session();
        ASSERT_EQ(r.subscribe(sid, 1, worldEq(6)).r.errorCode, kOk);
        ASSERT_EQ(adm.subsFor(0, 0), 1u);
        r.replicas.remove(0, 0);
        for (int i = 0; i < 4; ++i) r.gw.pushSubscriptionsForTest();
        EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "the subscription was retired NotLeader";
        EXPECT_EQ(adm.subsFor(0, 0), 0u) << "moved-away retire";
    }
}

TEST(GatewayFilterRegression, LegacyModeRefusesFiltersRegression) {
    for (const bool workers : {true, false}) {
        Rig r(workers);
        if (workers) r.gw.setPerWorkerEgressActive(false);
        r.typed();
        const int64_t sid = r.session();
        const Answer a = r.subscribe(sid, 1, worldEq(7));
        EXPECT_EQ(a.r.errorCode, kInvalid) << workers;
        EXPECT_EQ(a.detail, "filter needs RUNE_WORKER_MODE=shard") << workers;
        EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "never silently unfiltered";
    }
}

TEST(GatewayFilterRegression, SessionBelowTenSendingTrailingBytesIsUnfilteredRegression) {
    Rig r;
    r.typed();
    const int64_t sid = r.session(9);
    const Answer a = r.subscribe(sid, 1, worldEq(7));   // the same frame: the trailer is ignored for a v9 session
    EXPECT_EQ(a.r.errorCode, kOk);
    EXPECT_EQ(a.len, pc::SubscribeResponse::ENCODED_SIZE);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 0u);
    EXPECT_TRUE(r.gw.subscriptionFilterForTest(a.r.subscriptionId).empty());
    r.pool->worker(0).doWork(1);
    EXPECT_EQ(r.pool->worker(0).filterBytesHeldForTest(), 0u);
    EXPECT_EQ(r.pool->worker(0).filterGroupOfSubForTest(a.r.subscriptionId), nullptr);
    // and garbage that would be refused for a v10 session is still never refused for a v9 one
    const Answer g = r.subscribe(sid, 2, std::vector<uint8_t>{9, 9, 9});
    EXPECT_EQ(g.r.errorCode, kOk);
}

TEST(GatewayFilterRegression, RefusalDetailIsSentOnlyToVersionTenSessionsRegression) {
    Rig r;
    r.typed();
    const int64_t v10 = r.session(10);
    const int64_t v9  = r.session(9);
    const Answer a = r.subscribe(v10, 1, worldEq(1), /*partition=*/5);   // an unknown partition, no filter involved
    EXPECT_NE(a.r.errorCode, kOk);
    EXPECT_EQ(a.len, pc::SubscribeResponse::ENCODED_SIZE) << "no detail to give: the frame stays 38 bytes";
    const Answer d = r.subscribe(v10, 2, std::vector<uint8_t>{1, 0});
    EXPECT_EQ(d.r.errorCode, kInvalid);
    EXPECT_GT(d.len, pc::SubscribeResponse::ENCODED_SIZE) << "a v10 session gets the detail trailer";
    EXPECT_EQ(d.detail, "filter: term count must be 1..8");
    const Answer b = r.subscribe(v9, 3, worldEq(1), /*partition=*/5);
    EXPECT_NE(b.r.errorCode, kOk);
    EXPECT_EQ(b.len, pc::SubscribeResponse::ENCODED_SIZE);
    EXPECT_TRUE(b.detail.empty());
}

TEST(GatewayFilter, UnfilteredSubscribeResponseIsByteIdentical) {
    Rig r;
    r.typed();
    std::vector<std::vector<uint8_t>> frames;
    r.gw.setOnEgressFrame([&](int64_t, const uint8_t* b, std::size_t n) { frames.emplace_back(b, b + n); });
    const int64_t sid = r.session(10);
    const auto f = subscribeFrame(sid, 77, 0, {});
    r.gw.applySubscribeForTest(f.data(), f.size());
    ASSERT_EQ(frames.size(), 1u);
    ASSERT_EQ(frames[0].size(), 38u);
    auto resp = pc::SubscribeResponse::decode(frames[0].data(), frames[0].size());
    ASSERT_TRUE(resp.has_value());
    std::vector<uint8_t> again(pc::SubscribeResponse::ENCODED_SIZE);
    again.resize(resp->encode(again.data()));
    EXPECT_EQ(again, frames[0]);
}

TEST(GatewayFilter, DuplicateFilteredSubscribeReanswersTheExistingSubscription) {
    Rig r;
    r.typed();
    const int64_t sid = r.session();
    const auto f = worldIn({1, 2});
    const int64_t first = r.subscribe(sid, 5, f).r.subscriptionId;
    const Answer second = r.subscribe(sid, 5, f);   // the client's resend
    EXPECT_EQ(second.r.errorCode, kOk);
    EXPECT_EQ(second.r.subscriptionId, first);
    EXPECT_EQ(r.gw.subscriptionCount(), 1u);
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 1u) << "one subscription, one ticket";
    EXPECT_EQ(r.gw.filterAdmissionForTest().valuesFor(0, 0), 2u);
}

// PR5: a tag the gateway's installed FilterSchema does not know yet is refused UnknownTag, terminally.  The application
// (here: the test) subscribes again once the schema is installed; the gateway never retries on its own.
TEST(GatewayFilterRegression, SubscribeRacingAlterUnknownTagThenAcceptedRegression) {
    Rig r;
    r.typed(1);
    const int64_t sid = r.session();
    const auto onGuild = rawFilter({{9, PrimType::UInt32, 4, 1, {3}}});
    const Answer refused = r.subscribe(sid, 1, onGuild);
    EXPECT_EQ(refused.r.errorCode, kInvalid);
    EXPECT_EQ(refused.detail, "filter term 0 (tag 9): unknown tag");
    EXPECT_EQ(r.gw.subscriptionCount(), 0u) << "terminal: nothing is parked or retried";
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 0u);
    r.drain(4);
    EXPECT_EQ(r.answers.size(), 1u) << "the gateway sends no second answer on its own";

    r.typed(2);                                              // the ALTER (ADD guild) reaches the gateway
    const Answer ok = r.subscribe(sid, 2, onGuild);          // the application re-subscribes
    EXPECT_EQ(ok.r.errorCode, kOk) << ok.detail;
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 1u);
}

// A schema that can never fit the worker ring is counted and loud; filtered Subscribes to that topic are refused.
TEST(GatewayFilterRegression, OversizedFilterSchemaIsCountedAndFilteredSubscribesRefusedRegression) {
    Rig r(true, 1 << 12);
    ASSERT_GT(r.pool->maxIngressTail(), 0u);
    rune::codec::DescriptorBuilder b("huge");
    b.addTagged("f_key", 0, PrimType::UInt32, 1, 0xFFFF, /*isKey=*/true, {});
    std::vector<uint8_t> blob;
    for (uint16_t t = 1; blob.size() <= r.pool->maxIngressTail(); ++t) {
        b.addTagged("a_rather_long_field_name_to_inflate_the_descriptor_" + std::to_string(t), t, PrimType::UInt32, 1,
                    0xFFFF, false, {});
        blob = rune::codec::serialize(b.build());
        ASSERT_LT(t, 2000);
    }
    r.gw.onFilterSchema(0, 1, blob);
    EXPECT_EQ(r.gw.filterSchemaTooLargeForTest(), 1u);
    const int64_t sid = r.session();
    const Answer a = r.subscribe(sid, 1, rawFilter({{0, PrimType::UInt32, 4, 1, {1}}}));
    EXPECT_EQ(a.r.errorCode, kInvalid);
    EXPECT_NE(a.detail.find("too large for the worker ring"), std::string::npos) << a.detail;
    EXPECT_EQ(r.gw.filterAdmissionForTest().subsFor(0, 0), 0u);
}
