// ---------------------------------------------------------------------------
// SchemaGatewayTest.cpp — Task 8a on-host integration test (RULING-2 E/F).
//
// Wires a real ClientGateway (rune_gateway) to a real AdminWorkerAgent (rune_node)
// via the SPSC rings, WITHOUT Aeron / the media driver.  Drives the full DDL
// round-trip deterministically on a single thread (the rings are SPSC; we just
// pump both ends in order):
//
//   client DefineTopic frame → gateway.onFragment → requestRing
//     → admin.doWork (drain requestRing → DDL state machine → createReqRing)
//     → [test plays the CONTROL thread: createTopic + reply on replyRing]
//     → admin.doWork (reply → appendKeyed → HW gate → respondDefine → egressRing)
//     → [control drains egressRing → gateway.sendControlFrame]
//     → captured DefineTopicResponse (65) on the session egress.
//
//   Asserts: the __topic_schemas replica got the record; a DefineTopicResponse
//   with errorCode=OK and the authoritative topicId is produced on the egress.
//
//   SchemaGatewayRegression.DdlDoesNotBlockProduce: a stalled AdminWorker (its
//   request ring drained but its doWork never advancing the DDL) does NOT delay
//   produce routing on the gateway — produce still flows within a tight bound.
//
// Docker-deferred to Task 8b (NOT faked here):
//   - real Aeron majority-durable commit (needs a media driver + follower fetch)
//   - Follower→Leader failover rebuild over the wire
//   - full client↔broker socket round-trip
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminWorker.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/ViewRegistry.h>
#include <rune/node/BrokerNode.h>   // AdminWorkerAgent (no-hang R2 egress backlog)
#include <rune/node/SchemaRegistry.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/replication/Types.h>
#include <rune/storage/Log.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/AlterOps.h>

#include <rune/protocol/client/SchemaMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/PartitionHash.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

namespace pc = rune::protocol::client;
using namespace rune::node;
using namespace rune::codec;
using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

std::string uniqDir(const char* tag) {
    static std::atomic<uint64_t> seq{0};
    const auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path().string() + "/rune_sgt_" + tag + "_"
           + std::to_string(seq.fetch_add(1)) + "_" + std::to_string(ts);
    fs::create_directories(p);
    return p;
}

/// Creates `p` (and its parents); returns it.
std::string madeDir(const std::string& p) { fs::create_directories(p); return p; }

std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters", 1)
        .add("id",     PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("name",   PrimType::String)
        .build());
}

class NullPartitionTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Build an ALTER ops blob (ADD a column) — Task 13's fingerprint-selecting
// GetTopicSchema test ALTERs v1 → v2 and must still be able to fetch v1.
rune::codec::AlterOp addColumnOp(const std::string& name, PrimType t) {
    rune::codec::AlterOp op{};
    op.type = rune::codec::AlterOpType::Add;
    op.name = name;
    op.primType = t;
    op.fixedLen = 0;
    return op;
}

// GC6b: a DROP op (for the long-lineage fingerprint-select test below).
rune::codec::AlterOp dropColumnOp(const std::string& name) {
    rune::codec::AlterOp op{};
    op.type = rune::codec::AlterOpType::Drop;
    op.name = name;
    return op;
}

// GC6b: a RENAME op (for the long-lineage fingerprint-select test below).
rune::codec::AlterOp renameColumnOp(const std::string& oldName, const std::string& newName) {
    rune::codec::AlterOp op{};
    op.type = rune::codec::AlterOpType::Rename;
    op.name = oldName;
    op.newName = newName;
    return op;
}

// A minimal record batch (for the produce regression test).
std::vector<uint8_t> makeBatch() {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, std::string_view{"hello"}, /*timestamp=*/0);
    return b.build();
}

// End-to-end harness: a real ClientGateway + the real AdminWorkerAgent (its rings,
// its catalog sink) over a single-node __catalog leader, single thread.  The test
// plays the CONTROL thread: the agent's failure ring carries the applier's effects
// (kA2cCatalogTopicApplied → TopicRegistry::createTopicAt when realControl), its
// egress ring the DDL answers (→ gateway.sendControlFrame).
struct SgHarness {
    /// Removes the harness's directories when it goes (declared FIRST: destroyed after every member that uses
    /// them).  They leaked before - 65..260 MB each (preallocated segments), enough to fill a CI disk.
    struct OwnedDirs {
        std::vector<std::string> dirs;
        const std::string& add(std::string d) { dirs.push_back(std::move(d)); return dirs.back(); }
        ~OwnedDirs() {
            std::error_code ec;
            for (const auto& d : dirs) fs::remove_all(d, ec);
        }
    } ownedDirs;
    std::string dataDir;
    rune::storage::Log catalogLog;
    std::unique_ptr<NullPartitionTransport> catalogTransport;
    std::unique_ptr<PartitionReplica> catalogReplica;
    SchemaRegistry schemaReg;
    ViewRegistry   viewReg;
    std::unique_ptr<AdminWorkerAgent> agent;
    /// Every kA2cCatalogTopicApplied the control thread saw (catalog id, count, mode).
    std::vector<CatalogTopicAppliedMsg> applied;

    // Gateway + its user topic registry.
    std::string topicDirPath;
    std::unique_ptr<rune::gateway::TopicRegistry> topicReg;

    // Captured egress frames (sessionId, bytes) via onControlFrame_.
    std::vector<std::pair<int64_t, std::vector<uint8_t>>> captured;
    // Count of frames delivered via the gateway test-egress fn (produce acks etc.).
    int egressFnFrames = 0;
    // When true, the control thread builds every applied topic in the REAL
    // topicReg (createTopicAt at its catalog id), exactly as BrokerNode does.
    bool realControl = false;
    int64_t nowNs = 0;

    // Gateway (by value — non-movable; guaranteed-copy-elision init in the list).
    // Declared LAST so egressFnFrames is alive when the egress fn captures it.
    rune::gateway::ClientGateway gw;

    SgHarness()
        : dataDir(ownedDirs.add(uniqDir("catalog"))), catalogLog(madeDir((fs::path(dataDir) / "__catalog").string())),
          topicDirPath(ownedDirs.add(uniqDir("topicdata"))),
          gw(rune::gateway::ClientGateway::makeForTest(
                 [c = &egressFnFrames](const uint8_t*, std::size_t){ ++(*c); return int64_t{1}; },
                 /*sessionId=*/1)) {
        catalogLog.recover();
        catalogTransport = std::make_unique<NullPartitionTransport>();
        NodeConfig nc;
        nc.nodeId = 1;
        catalogReplica = std::make_unique<PartitionReplica>(
            makeCatalogPartitionConfig(nc, 1), Role::Follower, 0, std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &catalogLog, catalogTransport.get());

        // AdminDeps: every answer → the agent's egress ring (as BrokerNode wires them).
        AdminDeps deps;
        deps.brokerCount = 1;
        deps.topicMinIsr = 1;
        deps.respondDefine = [this](int64_t session, int64_t corr, int32_t tid, uint16_t ver, uint16_t err) {
            pc::DefineTopicResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.version=ver; r.errorCode=err;
            uint8_t f[pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED];
            ASSERT_TRUE(agent->postEgressFrame(session, f, r.encode(f)));
        };
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t tid, uint16_t ver, int32_t parts,
                                 const std::vector<uint8_t>& blob, uint16_t err) {
            agent->postTopicSchemaResponse(session, corr, tid, ver, parts, blob, err);
        };
        deps.respondAlter = [this](int64_t session, int64_t corr, int32_t tid, uint16_t newVer, uint16_t err) {
            pc::AlterTopicResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.newVersion=newVer; r.errorCode=err;
            uint8_t f[pc::MessageHeader::ENCODED_SIZE + pc::AlterTopicResponse::BODY_FIXED];
            ASSERT_TRUE(agent->postEgressFrame(session, f, r.encode(f)));
        };
        deps.respondCreateView = [this](int64_t session, int64_t corr, int32_t vid, uint64_t fp, uint16_t err) {
            pc::CreateViewResponse r; r.sessionId=session; r.correlationId=corr;
            r.viewTopicId=vid; r.outputFingerprint=fp; r.errorCode=err;
            uint8_t f[pc::MessageHeader::ENCODED_SIZE + pc::CreateViewResponse::BODY_FIXED];
            ASSERT_TRUE(agent->postEgressFrame(session, f, r.encode(f)));
        };
        deps.respondDdl = [this](int64_t session, int64_t corr, uint16_t code, uint64_t objectId,
                                 uint32_t newVersion, int32_t gate, std::string_view detail) {
            agent->postDdlResponse(session, corr, code, objectId, newVersion, gate, detail);
        };
        deps.respondError = [this](int64_t session, int64_t corr, uint16_t code, std::string_view message) {
            agent->postErrorResponse(session, corr, code, message);
        };
        agent = std::make_unique<AdminWorkerAgent>(schemaReg, 1, std::move(deps), &viewReg, catalogReplica.get(), nullptr);
        (void)agent->worker().bootCatalog(agent->catalogBootEffects(), agent->catalogLiveEffects());
        AdminRoleMsg role;
        role.leader = true; role.epoch = 1; role.leaderId = 1; role.partitionEpoch = 0;
        role.isrCount = 1; role.isr[0] = 1;
        agent->worker().onCatalogRole(role);

        topicReg = std::make_unique<rune::gateway::TopicRegistry>(std::vector<rune::node::TopicSpec>{}, topicDirPath);
        gw.setTopicRegistry(topicReg.get());
        gw.setAdminSink(agent.get());
        gw.setOnControlFrame([this](int64_t sid, const uint8_t* b, std::size_t n) {
            captured.emplace_back(sid, std::vector<uint8_t>(b, b+n));
        });
        gw.setOnEgressFrame([this](int64_t sid, const uint8_t* b, std::size_t n) {
            captured.emplace_back(sid, std::vector<uint8_t>(b, b+n));
        });
        gw.injectTestSession(/*nowNs=*/0);  // session id 1
        for (int i = 0; i < 8; ++i) { nowNs += 1'000'000LL; agent->doWork(nowNs); }   // the leader catches up
        EXPECT_EQ(agent->worker().roleName(), "LeaderServing");
    }

    // Play the CONTROL thread: the applier's effects (failure ring) and the DDL
    // answers (egress ring → gateway.sendControlFrame).
    int serviceControl() {
        int work = 0;
        work += agent->failureRing().drain([this](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                                  aeron::util::index_t off, aeron::util::index_t len) {
            if (tid != kA2cCatalogTopicApplied) return;
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            CatalogTopicAppliedMsg m;
            ASSERT_TRUE(decodeCatalogTopicApplied(p, (std::size_t)len, m));
            applied.push_back(m);
            if (!realControl) return;
            rune::node::TopicSpec spec;
            spec.name = m.name; spec.partitionCount = m.partitions; spec.topicId = m.topicId;
            spec.persistence = m.persistence == 1 ? PersistenceMode::Transient : PersistenceMode::Disk;
            EXPECT_EQ(topicReg->createTopicAt(m.topicId, spec), m.topicId);
        }, 16);
        work += agent->egressRing().drain([this](int32_t, aeron::concurrent::AtomicBuffer& b,
                                                 aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            const auto n = (std::size_t)len; if (n < 8) return;
            int64_t session; std::memcpy(&session, p, 8);
            (void)gw.sendControlFrame(session, p+8, n-8);
        }, 16);
        return work;
    }

    // Drain the requestRing into the admin worker WITHOUT running it (counts the
    // frames) — the stalled-admin regression.
    int serviceAdminIngress() {
        return agent->requestRing().drain([this](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                                 aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            const auto n = (std::size_t)len;
            if (tid == kMsgDefineTopic) agent->worker().submit(decodeDefineTopic(p, n));
            else if (tid == kMsgGetSchema) agent->worker().submitGetSchema(decodeGetSchema(p, n));
            else if (tid == kMsgAlterTopic) agent->worker().submit(decodeAlterTopic(p, n));
        }, 16);
    }

    // Pump the agent (the admin thread) + the control thread until quiescent (bounded).
    void pump(int cycles = 60) {
        for (int i = 0; i < cycles; ++i) {
            nowNs += 1'000'000LL;
            agent->doWork(nowNs);
            serviceControl();
        }
    }

    /// An UNTYPED topic created by DDL (ruling 3: the catalog's schema-less topic)
    /// — what a static schema-less topic was; returns its catalog id.
    int32_t defineUntyped(const std::string& name, uint32_t partitions,
                          PersistenceMode persistence = PersistenceMode::Disk);
};
// Encode a DefineTopic client frame.
std::vector<uint8_t> encodeDefineFrame(int64_t session, int64_t corr,
                                       const std::string& name, uint32_t parts,
                                       const std::string& keyField, bool validate,
                                       const std::vector<uint8_t>& blob,
                                       uint8_t persistence = 0) {
    pc::DefineTopic m;
    m.persistence = persistence;
    m.sessionId = session; m.correlationId = corr; m.topicName = name;
    m.partitionCount = parts; m.keyField = keyField; m.validate = validate;
    m.descriptorBlob = blob.data(); m.descriptorLen = (uint32_t)blob.size();
    std::vector<uint8_t> frame(m.encodedSize());
    m.encode(frame.data());
    return frame;
}

int32_t SgHarness::defineUntyped(const std::string& name, uint32_t partitions, PersistenceMode persistence) {
    const bool savedReal = realControl;
    realControl = true;   // the topic must exist in the gateway's registry
    static int64_t corr = 900'000;
    const auto f = encodeDefineFrame(1, ++corr, name, partitions, "", false, {},
                                     persistence == PersistenceMode::Transient ? pc::DefineTopic::kPersistenceTransient
                                                                               : pc::DefineTopic::kPersistenceDisk);
    gw.applyDefineTopicForTest(f.data(), f.size());
    pump();
    realControl = savedReal;
    for (auto& [sid, bytes] : captured) {
        auto d = pc::DefineTopicResponse::decode(bytes.data(), bytes.size());
        if (d && d->correlationId == corr) {
            EXPECT_EQ(d->errorCode, 0u) << "setup: the untyped topic '" << name << "'";
            captured.clear();
            return d->topicId;
        }
    }
    ADD_FAILURE() << "setup: no answer for the untyped topic '" << name << "'";
    return -1;
}

// Encode an AlterTopic client frame (Task 14).
std::vector<uint8_t> encodeAlterFrame(int64_t session, int64_t corr,
                                      const std::string& name,
                                      uint16_t expectedVersion,
                                      const std::vector<uint8_t>& opsBlob) {
    pc::AlterTopic m;
    m.sessionId = session; m.correlationId = corr; m.topicName = name;
    m.expectedVersion = expectedVersion;
    m.opsBlob = opsBlob.data(); m.opsLen = (uint32_t)opsBlob.size();
    std::vector<uint8_t> frame(m.encodedSize());
    m.encode(frame.data());
    return frame;
}

} // namespace

// ---------------------------------------------------------------------------
// Phase 2b E: the wire persistence byte reaches the control thread's applied
// topic (kA2cCatalogTopicApplied) and the durable registry entry — and an OLD client's
// frame (no byte) still creates a disk topic.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DefineTopicTransientByteReachesControlAndRegistry) {
    SgHarness h;
    auto blob = charsBlob();
    auto frame = encodeDefineFrame(1, 5, "characters", 2, "userId", false, blob,
                                   pc::DefineTopic::kPersistenceTransient);
    h.gw.applyDefineTopicForTest(frame.data(), frame.size());
    h.pump();
    ASSERT_FALSE(h.applied.empty()) << "the applied topic reached the control thread";
    EXPECT_EQ(h.applied.back().persistence, 1u);
    ASSERT_NE(h.schemaReg.byName("characters"), nullptr);
    EXPECT_EQ(h.schemaReg.byName("characters")->persistence, PersistenceMode::Transient);
}

TEST(SchemaGatewayRegression, DefineTopicWithoutPersistenceByteCreatesDiskTopic) {
    SgHarness h;
    auto blob = charsBlob();
    auto frame = encodeDefineFrame(1, 6, "characters", 2, "userId", false, blob);   // old client
    ASSERT_EQ(frame.size(), pc::MessageHeader::ENCODED_SIZE + pc::DefineTopic::BODY_FIXED
                                + 2 + 10 + 2 + 6 + 4 + blob.size())
        << "precondition: a disk frame carries no persistence byte";
    h.gw.applyDefineTopicForTest(frame.data(), frame.size());
    h.pump();
    ASSERT_FALSE(h.applied.empty());
    EXPECT_EQ(h.applied.back().persistence, 0u);
    EXPECT_EQ(h.schemaReg.byName("characters")->persistence, PersistenceMode::Disk);
}

// ---------------------------------------------------------------------------
// End-to-end: DefineTopic through the gateway → admin ring → replica → response.
// ---------------------------------------------------------------------------
TEST(SchemaGateway, DefineTopicRoundTripThroughRings) {
    SgHarness h;
    auto blob = charsBlob();
    auto frame = encodeDefineFrame(/*session=*/1, /*corr=*/99, "characters",
                                   /*parts=*/4, "userId", /*validate=*/false, blob);

    // client → gateway (routes to admin sink via requestRing; returns immediately).
    h.gw.applyDefineTopicForTest(frame.data(), frame.size());

    // Pump the ring topology to completion.
    h.pump();

    // 1. __topic_schemas replica got the record.
    EXPECT_GE(h.catalogLog.logEndOffset(), 1LL)
        << "__topic_schemas must have a record after the DDL round-trip";
    ASSERT_NE(h.schemaReg.byName("characters"), nullptr)
        << "SchemaRegistry must contain the new topic after commit";

    // 2. A DefineTopicResponse (65) was produced on the session egress with OK.
    std::optional<pc::DefineTopicResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::DefineTopicResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value())
        << "a DefineTopicResponse must be produced on the session egress";
    EXPECT_EQ(resp->correlationId, 99);
    EXPECT_EQ(resp->errorCode, 0u) << "DDL must succeed (OK)";
    EXPECT_EQ(resp->topicId, 0) << "authoritative id from the control-thread reply";
    EXPECT_EQ(resp->version, 1u);
}

// ---------------------------------------------------------------------------
// GetTopicSchema round-trip (read of the admin-owned registry).
// ---------------------------------------------------------------------------
TEST(SchemaGateway, GetTopicSchemaRoundTrip) {
    SgHarness h;
    auto blob = charsBlob();
    h.gw.applyDefineTopicForTest(
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, blob).data(),
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, blob).size());
    h.pump();
    h.captured.clear();

    // GetTopicSchema for the created topic.
    pc::GetTopicSchema g; g.sessionId=1; g.correlationId=7; g.topicName="characters";
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->correlationId, 7);
    EXPECT_EQ(resp->errorCode, 0u);
    EXPECT_GT(resp->descriptorLen, 0u) << "the descriptor blob must be returned";
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.GetTopicSchemaCarriesPartitionCount — Phase 2b
// Part C: a topic defined with 8 partitions through the gateway answers
// GetTopicSchema with partitionCount == 8 on the session egress (the typed
// client routes by key over it; pre-2b every typed record hit partition 0).
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, GetTopicSchemaCarriesPartitionCount) {
    SgHarness h;
    auto blob = charsBlob();
    const auto def = encodeDefineFrame(1, 1, "characters", 8, "userId", false, blob);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    h.captured.clear();

    pc::GetTopicSchema g; g.sessionId=1; g.correlationId=7; g.topicName="characters";
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->correlationId, 7);
    EXPECT_EQ(resp->errorCode, 0u);
    EXPECT_GT(resp->descriptorLen, 0u);
    EXPECT_EQ(resp->partitionCount, 8);
}

namespace {
// Collect the first decodable frame of type R from the harness egress capture.
template <typename R>
std::optional<R> firstFrame(const SgHarness& h) {
    for (auto& [sid, bytes] : h.captured) {
        auto d = R::decode(bytes.data(), bytes.size());
        if (d) return d;
    }
    return std::nullopt;
}

std::optional<pc::TopicSchemaResponse> getSchema(SgHarness& h, const std::string& name,
                                                 int64_t corr) {
    h.captured.clear();
    pc::GetTopicSchema g; g.sessionId=1; g.correlationId=corr; g.topicName=name;
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();
    return firstFrame<pc::TopicSchemaResponse>(h);
}
} // namespace

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.DefineTopicOverExistingTopicRecordsAuthoritativePartitionCount
//
// Phase 2b C final review I1 (catalog form): DefineTopic("orders", 8, ...) over an
// EXISTING 3-partition UNTYPED topic binds the schema to that topic (CreateSchema) — and
// the registry used to record the REQUESTED 8.  GetTopicSchema then told typed
// clients to route over 8 partitions of a 3-partition topic: records to
// partitions that do not exist, and a producer and subscriber (which uses the
// broker's own TopicRegistry hash over 3) disagreeing on a key's partition.
// The schema must record the AUTHORITATIVE count (3), and a typed client routing
// over the reported count must land every key where the broker routes it.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DefineTopicOverExistingTopicRecordsAuthoritativePartitionCount) {
    SgHarness h;
    h.realControl = true;
    const int32_t ordersId = h.defineUntyped("orders", 3);   // pre-existing, untyped (ruling 3)

    const auto def = encodeDefineFrame(1, 1, "orders", /*parts=*/8, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto dresp = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(dresp.has_value());
    EXPECT_EQ(dresp->errorCode, 0u) << "a schema-less existing topic accepts its first schema";
    EXPECT_EQ(dresp->topicId, ordersId) << "the schema binds to the EXISTING topic";

    const auto resp = getSchema(h, "orders", 7);
    ASSERT_TRUE(resp.has_value());
    ASSERT_EQ(resp->errorCode, 0u);
    EXPECT_EQ(resp->topicId, ordersId);
    EXPECT_EQ(resp->partitionCount, 3) << "the topic's REAL partition count, not the requested 8";
    EXPECT_EQ(h.topicReg->topic(ordersId)->partitionCount, 3) << "the topic is not resized";

    // A typed client routes Key(k) over the reported count with the shared hash;
    // the broker's TopicRegistry routes the same key (subscribe(Key), produce
    // with no explicit partition) over the real partitions.  They must agree.
    for (uint64_t k = 0; k < 64; ++k) {
        uint8_t key[8]; std::memcpy(key, &k, 8);
        const std::span<const uint8_t> ks(key, 8);
        const int32_t client = rune::protocol::partitionForKey(ks, resp->partitionCount);
        EXPECT_LT(client, 3) << "key " << k << " routed to a nonexistent partition";
        EXPECT_EQ(client, h.topicReg->partitionForKey(ordersId, ks)) << "key " << k;
    }
}

// Phase 2b E review I1: createTopic is idempotent by name and ignores the requested
// spec for an existing schema-less topic.  The recorded persistence must be the
// topic's REAL one (what restart recovery reads), never the request's.
static void expectRecordedModeMatchesTopic(SgHarness& h, const std::string& name,
                                           int32_t id, PersistenceMode expected) {
    ASSERT_NE(h.topicReg->topic(id), nullptr);
    EXPECT_EQ(h.topicReg->topic(id)->persistence, expected) << "the live topic is unchanged";
    const RegistryEntry* e = h.schemaReg.byName(name);
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->persistence, expected) << "the schema record must match the live topic";
}

TEST(SchemaGatewayRegression, DiskDefineOverExistingTransientTopicRecordsTransient) {
    SgHarness h;
    h.realControl = true;
    const int32_t id = h.defineUntyped("orders", 2, PersistenceMode::Transient);   // pre-existing, untyped

    const auto def = encodeDefineFrame(1, 1, "orders", 2, "userId", false, charsBlob());   // disk
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 0u);
    expectRecordedModeMatchesTopic(h, "orders", id, PersistenceMode::Transient);
}

// Final review I1: the client EXPLICITLY asked for a transient topic over an existing
// schema-less DISK topic.  The topic keeps its log backend, so answering OK would
// hand back a topic that silently is not transient -> TopicExists, nothing recorded.
TEST(SchemaGatewayRegression, TransientDefineOverExistingDiskTopicIsTopicExistsRegression) {
    SgHarness h;
    h.realControl = true;
    const int32_t id = h.defineUntyped("orders", 2);   // disk, untyped
    const int64_t schemaEndBefore = h.catalogLog.logEndOffset();

    const auto def = encodeDefineFrame(1, 1, "orders", 2, "userId", false, charsBlob(),
                                       pc::DefineTopic::kPersistenceTransient);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 25u) << "TopicExists";
    EXPECT_EQ(h.schemaReg.byName("orders"), nullptr) << "no schema recorded";
    EXPECT_EQ(h.catalogLog.logEndOffset(), schemaEndBefore) << "no TopicSchema record appended";
    ASSERT_NE(h.topicReg->topic(id), nullptr);
    EXPECT_EQ(h.topicReg->topic(id)->persistence, PersistenceMode::Disk) << "live topic still Disk";
}

// Final review M3: a DefineTopic whose body fails protocol decode (unknown
// persistence byte) must be answered, not dropped (the client would wait its
// full timeout).
TEST(SchemaGatewayRegression, DefineTopicUnknownPersistenceByteIsAnsweredInvalidSchemaRegression) {
    SgHarness h;
    auto def = encodeDefineFrame(1, 77, "orders", 2, "userId", false, charsBlob(),
                                 pc::DefineTopic::kPersistenceTransient);
    def.back() = 2;   // one past Transient
    ASSERT_FALSE(pc::DefineTopic::decode(def.data(), def.size()).has_value());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value()) << "a malformed DefineTopic must get a response";
    EXPECT_EQ(r->correlationId, 77);
    EXPECT_EQ(r->errorCode, 23u) << "InvalidSchema";
    EXPECT_EQ(h.schemaReg.byName("orders"), nullptr);
}

// The control-thread `spec.persistence` line really creates a transient topic:
// no disk log objects, mode recorded in both the live topic and the schema.
TEST(SchemaGatewayRegression, NewTransientDefineCreatesTransientTopicOnControl) {
    SgHarness h;
    h.realControl = true;
    const auto def = encodeDefineFrame(1, 1, "orders", 2, "userId", false, charsBlob(),
                                       pc::DefineTopic::kPersistenceTransient);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->errorCode, 0u);
    expectRecordedModeMatchesTopic(h, "orders", r->topicId, PersistenceMode::Transient);
    EXPECT_EQ(h.topicReg->log(r->topicId, 0), nullptr) << "transient: no disk Log";
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.DefineTopicOverExistingTypedTopicIsTopicExists — the
// other half of the I1 ruling: an existing topic that ALREADY has a schema
// answers TopicExists (unchanged), and its schema keeps its partition count.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DefineTopicOverExistingTypedTopicIsTopicExists) {
    SgHarness h;
    h.realControl = true;
    const auto def1 = encodeDefineFrame(1, 1, "orders", 4, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def1.data(), def1.size());
    h.pump();
    h.captured.clear();

    const auto def2 = encodeDefineFrame(1, 2, "orders", 8, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def2.data(), def2.size());
    h.pump();
    const auto dresp = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(dresp.has_value());
    EXPECT_EQ(dresp->errorCode, static_cast<uint16_t>(pc::ErrorCode::TopicExists));

    const auto resp = getSchema(h, "orders", 9);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->partitionCount, 4);
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.CatalogTopicAppliedCarriesTheAuthoritativeSpecThroughTheRing
// — catalog history Task 9 (the create-topic reply is gone): the topic the
// control thread builds is the APPLIED event's — its catalog id, count and mode
// survive the real admin→control ring; an adoption (CreateSchema) builds nothing;
// an unknown mode byte on the ring is refused, never cast.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, CatalogTopicAppliedCarriesTheAuthoritativeSpecThroughTheRing) {
    SgHarness h;
    h.realControl = true;
    const int32_t id = h.defineUntyped("orders", 3);
    ASSERT_EQ(h.applied.size(), 1u);
    EXPECT_EQ(h.applied[0].topicId, id);
    EXPECT_EQ(h.applied[0].name, "orders");
    EXPECT_EQ(h.applied[0].partitions, 3);
    EXPECT_EQ(h.applied[0].persistence, 0u);
    const auto def = encodeDefineFrame(1, 2, "orders", 8, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    EXPECT_EQ(h.applied.size(), 1u) << "an adoption (CreateSchema) builds no second topic";
    EXPECT_EQ(h.topicReg->topic(id)->partitionCount, 3);

    // The frame layout refuses an unknown mode byte (never cast into the enum).
    std::vector<uint8_t> f(catalogTopicAppliedWireLen("x", rune::protocol::TopicConfig{}));
    writeCatalogTopicApplied(f.data(), 1, "x", 1, 0, /*persistence=*/1, rune::protocol::TopicConfig{});
    CatalogTopicAppliedMsg m;
    ASSERT_TRUE(decodeCatalogTopicApplied(f.data(), f.size(), m));
    f[4 + 2 + 1 + 4 + 2] = 2;   // the persistence byte
    EXPECT_FALSE(decodeCatalogTopicApplied(f.data(), f.size(), m));
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.AlterTopicRoundTripThroughGatewayRings
//
// Task 14: an AlterTopic frame fed through the GATEWAY (not injected directly
// on the admin ring, unlike Task 13's fingerprint test) must route via
// handleAlterTopic → IAdminRequestSink::submitAlterTopic → requestRing →
// AdminWorker, and the AlterTopicResponse (69) must come back on the session
// egress with the new version.  Mirrors DefineTopicRoundTripThroughRings.
//
// define v1 (gateway frame) → ALTER add column "email" (gateway frame) →
// response newVersion==2 → GetTopicSchema (gateway frame) confirms v2.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, AlterTopicRoundTripThroughGatewayRings) {
    SgHarness h;
    auto v1Blob = charsBlob();
    h.gw.applyDefineTopicForTest(
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).data(),
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).size());
    h.pump();

    ASSERT_NE(h.schemaReg.byName("characters"), nullptr);
    ASSERT_EQ(h.schemaReg.byName("characters")->version, 1u);
    h.captured.clear();

    // ALTER through the GATEWAY (not a direct ring injection): proves
    // handleAlterTopic actually routes the client frame.
    auto opsBlob = rune::codec::encodeAlterOps({addColumnOp("email", PrimType::String)});
    auto alterFrame = encodeAlterFrame(/*session=*/1, /*corr=*/42, "characters",
                                       /*expectedVersion=*/0, opsBlob);
    h.gw.applyAlterTopicForTest(alterFrame.data(), alterFrame.size());
    h.pump();

    // 1. The registry committed v2.
    const RegistryEntry* v2 = h.schemaReg.byName("characters");
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(v2->version, 2u) << "ALTER routed through the gateway must commit v2";

    // 2. An AlterTopicResponse (69) was produced on the session egress with OK
    //    and the new version.
    std::optional<pc::AlterTopicResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::AlterTopicResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value())
        << "an AlterTopicResponse must be produced on the session egress";
    EXPECT_EQ(resp->correlationId, 42);
    EXPECT_EQ(resp->errorCode, 0u) << "ALTER must succeed (OK)";
    EXPECT_EQ(resp->topicId, 0);
    EXPECT_EQ(resp->newVersion, 2u);

    h.captured.clear();

    // 3. GetTopicSchema (gateway frame) confirms the latest is v2.
    pc::GetTopicSchema g; g.sessionId = 1; g.correlationId = 77; g.topicName = "characters";
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> getResp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d) { getResp = d; break; }
    }
    ASSERT_TRUE(getResp.has_value());
    EXPECT_EQ(getResp->errorCode, 0u);
    EXPECT_EQ(getResp->version, 2u) << "GetTopicSchema must confirm v2 after the gateway ALTER";
}

// ---------------------------------------------------------------------------
// Task 13: GetTopicSchema(requestedFingerprint) selects a SPECIFIC version —
// define v1, ALTER to v2 (ADD a column), then:
//   requestedFingerprint = fp(v1) → version==1, blob decodes to fp==fp(v1).
//   requestedFingerprint = 0      → latest (version==2).
// ---------------------------------------------------------------------------
TEST(SchemaGateway, GetTopicSchemaByFingerprintReturnsBoundVersion) {
    SgHarness h;
    auto v1Blob = charsBlob();
    h.gw.applyDefineTopicForTest(
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).data(),
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).size());
    h.pump();
    h.captured.clear();

    const RegistryEntry* v1 = h.schemaReg.byName("characters");
    ASSERT_NE(v1, nullptr);
    ASSERT_EQ(v1->version, 1u);
    const uint64_t fpV1 = v1->fingerprint;

    // ALTER: ADD a column ("email") → bumps to v2. Submitted directly on the
    // admin request ring (the gateway doesn't yet route AlterTopic — only
    // DefineTopic/GetTopicSchema, per IAdminRequestSink); h.pump() drains it
    // exactly like a gateway-routed DDL op (same requestRing, same dispatch).
    AlterTopicMsg am;
    am.session = 1; am.corr = 42; am.topicName = "characters";
    am.expectedVersion = 0;  // unconditional
    am.opsBlob = rune::codec::encodeAlterOps({addColumnOp("email", PrimType::String)});
    ASSERT_TRUE(encodeAlterTopic(h.agent->requestRing(), am));
    h.pump();

    const RegistryEntry* v2 = h.schemaReg.byName("characters");
    ASSERT_NE(v2, nullptr);
    ASSERT_EQ(v2->version, 2u) << "ALTER must have committed v2";
    ASSERT_NE(v2->fingerprint, fpV1) << "ADD changes the materialized fingerprint";

    h.captured.clear();

    // requestedFingerprint = fp(v1) → must resolve back to v1, not latest.
    pc::GetTopicSchema g1; g1.sessionId = 1; g1.correlationId = 101;
    g1.topicName = "characters"; g1.requestedFingerprint = fpV1;
    std::vector<uint8_t> gf1(g1.encodedSize()); g1.encode(gf1.data());
    h.gw.applyGetTopicSchemaForTest(gf1.data(), gf1.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> r1;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d && d->correlationId == 101) { r1 = d; break; }
    }
    ASSERT_TRUE(r1.has_value());
    EXPECT_EQ(r1->errorCode, 0u);
    EXPECT_EQ(r1->version, 1u) << "the BOUND version of the requested fingerprint";
    ASSERT_GT(r1->descriptorLen, 0u);
    // The client-side typed-gate check: fingerprint(deserialize(blob)) == requested.
    const Descriptor d1 = rune::codec::deserialize(
        std::span<const uint8_t>(r1->descriptorBlob, r1->descriptorLen));
    EXPECT_EQ(rune::codec::fingerprint(d1), fpV1)
        << "returned blob must deserialize to the MATERIALIZED v1 descriptor";

    h.captured.clear();

    // requestedFingerprint = 0 → latest (v2).
    pc::GetTopicSchema g2; g2.sessionId = 1; g2.correlationId = 102;
    g2.topicName = "characters"; g2.requestedFingerprint = 0;
    std::vector<uint8_t> gf2(g2.encodedSize()); g2.encode(gf2.data());
    h.gw.applyGetTopicSchemaForTest(gf2.data(), gf2.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> r2;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d && d->correlationId == 102) { r2 = d; break; }
    }
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->errorCode, 0u);
    EXPECT_EQ(r2->version, 2u) << "requestedFingerprint==0 must return latest";
    ASSERT_GT(r2->descriptorLen, 0u);
    const Descriptor d2 = rune::codec::deserialize(
        std::span<const uint8_t>(r2->descriptorBlob, r2->descriptorLen));
    EXPECT_EQ(rune::codec::fingerprint(d2), v2->fingerprint);
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.GetTopicSchemaUnknownFingerprintReturnsError
//
// Task 13 regression (rule #6): a requestedFingerprint that matches NO version
// of a KNOWN topic must come back as an error (UnknownTopic), not silently
// fall back to latest and not crash.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, GetTopicSchemaUnknownFingerprintReturnsError) {
    SgHarness h;
    auto blob = charsBlob();
    h.gw.applyDefineTopicForTest(
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, blob).data(),
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, blob).size());
    h.pump();
    h.captured.clear();

    pc::GetTopicSchema g; g.sessionId = 1; g.correlationId = 55;
    g.topicName = "characters"; g.requestedFingerprint = 0xdeadbeefULL;
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->errorCode, 7u)
        << "UnknownTopic for a fingerprint that matches no version of a known topic";
    EXPECT_EQ(resp->descriptorLen, 0u);
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.GetTopicSchemaByFingerprintAcrossLongLineage
//
// GC6b (closes F2 from the GC6 review): GetTopicSchemaByFingerprintReturns-
// BoundVersion (above) only covers a 2-version lineage (v1 -> v2, one ADD).
// The test matrix's D3-D8 want GetTopicSchema(requestedFingerprint) proven
// to resolve EVERY version of a LONGER lineage mixing ADD/DROP/RENAME,
// broker-side, through the real gateway rings + AdminWorker (no faking).
//
// Lineage (6 versions, 5 ALTERs over the v1 DefineTopic), chosen so every
// version's LIVE field set is distinct (deliberately avoiding I10-14's
// open question — ADD-then-DROP returning to a PRIOR live set — which the
// test matrix flags as an undecided fingerprint-equality edge case, not
// this task's ask):
//   v1 DefineTopic : id, userId(key), name                  [charsBlob]
//   v2 ADD    email  : + email           -> {id,userId,name,email}
//   v3 ADD    age    : + age             -> {..,email,age}
//   v4 DROP   email  : - email           -> {id,userId,name,age}
//   v5 RENAME age->years                 -> {id,userId,name,years}
//   v6 ADD    score  : + score           -> {..,years,score}
//
// For each K in 1..6: GetTopicSchema(requestedFingerprint=fp(vK)) must
// return version==K and a descriptor blob whose fingerprint(deserialize(.))
// == fp(vK) — i.e. the MATERIALIZED-as-of-vK blob, exactly what a client
// compiled at vK needs to bind. Also: requestedFingerprint=0 -> latest (v6);
// an unknown fingerprint -> UnknownTopic, never a silent fallback to latest.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, GetTopicSchemaByFingerprintAcrossLongLineage) {
    SgHarness h;
    auto v1Blob = charsBlob();
    h.gw.applyDefineTopicForTest(
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).data(),
        encodeDefineFrame(1, 1, "characters", 4, "userId", false, v1Blob).size());
    h.pump();

    const RegistryEntry* v1 = h.schemaReg.byName("characters");
    ASSERT_NE(v1, nullptr);
    ASSERT_EQ(v1->version, 1u);
    const int32_t topicId = v1->topicId;

    // Drive the lineage through the real admin request ring: mirrors Task
    // 13's fingerprint test — the gateway only routes DefineTopic/GetSchema
    // today, so AlterTopic is injected on the SAME ring AdminWorker drains,
    // exercising the identical dispatch/state-machine path as a gateway-
    // routed ALTER would.
    auto applyAlter = [&](const std::vector<rune::codec::AlterOp>& ops, int64_t corr) {
        AlterTopicMsg am;
        am.session = 1; am.corr = corr; am.topicName = "characters";
        am.expectedVersion = 0;  // unconditional
        am.opsBlob = rune::codec::encodeAlterOps(ops);
        ASSERT_TRUE(encodeAlterTopic(h.agent->requestRing(), am));
        h.pump();
    };

    applyAlter({addColumnOp("email", PrimType::String)}, 201);    // v2
    applyAlter({addColumnOp("age", PrimType::Int32)}, 202);        // v3
    applyAlter({dropColumnOp("email")}, 203);                      // v4
    applyAlter({renameColumnOp("age", "years")}, 204);              // v5
    applyAlter({addColumnOp("score", PrimType::Int64)}, 205);      // v6

    const RegistryEntry* latest = h.schemaReg.byName("characters");
    ASSERT_NE(latest, nullptr);
    ASSERT_EQ(latest->version, 6u) << "5 ALTERs over a v1 DefineTopic must land on v6";

    // Snapshot fp(vK) for K=1..6 directly from the (admin-owned) registry —
    // the SAME source of truth GetTopicSchema's resolver consults.
    std::vector<uint64_t> fpByVersion(7, 0);  // index by version (1-based; [0] unused)
    for (uint16_t k = 1; k <= 6; ++k) {
        const RegistryEntry* e = h.schemaReg.byIdVersion(topicId, k);
        ASSERT_NE(e, nullptr) << "version " << k << " must be materialized in the registry";
        fpByVersion[k] = e->fingerprint;
    }
    // All 6 fingerprints must be pairwise distinct (I1): every ADD/DROP/
    // RENAME in this lineage changes the materialized descriptor.
    for (uint16_t a = 1; a <= 6; ++a)
        for (uint16_t b = a + 1; b <= 6; ++b)
            EXPECT_NE(fpByVersion[a], fpByVersion[b])
                << "fp(v" << a << ") must differ from fp(v" << b << ")";

    // For each K, GetTopicSchema(requestedFingerprint=fp(vK)) must resolve
    // to EXACTLY version K with a blob that re-fingerprints to fp(vK).
    for (uint16_t k = 1; k <= 6; ++k) {
        h.captured.clear();
        const int64_t corr = 300 + k;
        pc::GetTopicSchema g; g.sessionId = 1; g.correlationId = corr;
        g.topicName = "characters"; g.requestedFingerprint = fpByVersion[k];
        std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
        h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
        h.pump();

        std::optional<pc::TopicSchemaResponse> resp;
        for (auto& [sid, bytes] : h.captured) {
            auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
            if (d && d->correlationId == corr) { resp = d; break; }
        }
        ASSERT_TRUE(resp.has_value()) << "no response for fp(v" << k << ")";
        EXPECT_EQ(resp->errorCode, 0u) << "fp(v" << k << ") must resolve (OK)";
        EXPECT_EQ(resp->version, k)
            << "requestedFingerprint=fp(v" << k << ") must bind version " << k
            << ", not some other version in the lineage";
        ASSERT_GT(resp->descriptorLen, 0u);

        const Descriptor dk = rune::codec::deserialize(
            std::span<const uint8_t>(resp->descriptorBlob, resp->descriptorLen));
        EXPECT_EQ(rune::codec::fingerprint(dk), fpByVersion[k])
            << "the returned blob must be the MATERIALIZED-as-of-v" << k
            << " descriptor (a client compiled at v" << k << " must bind)";
    }

    // requestedFingerprint=0 -> latest (v6), even across a long lineage.
    {
        h.captured.clear();
        pc::GetTopicSchema g; g.sessionId = 1; g.correlationId = 400;
        g.topicName = "characters"; g.requestedFingerprint = 0;
        std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
        h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
        h.pump();

        std::optional<pc::TopicSchemaResponse> resp;
        for (auto& [sid, bytes] : h.captured) {
            auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
            if (d && d->correlationId == 400) { resp = d; break; }
        }
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->errorCode, 0u);
        EXPECT_EQ(resp->version, 6u)
            << "requestedFingerprint==0 must return latest across a long lineage";
        const Descriptor dLatest = rune::codec::deserialize(
            std::span<const uint8_t>(resp->descriptorBlob, resp->descriptorLen));
        EXPECT_EQ(rune::codec::fingerprint(dLatest), fpByVersion[6]);
    }

    // An unknown fingerprint (matches no version across the 6-version
    // lineage) -> UnknownTopic, never a silent fallback to latest.
    {
        h.captured.clear();
        pc::GetTopicSchema g; g.sessionId = 1; g.correlationId = 401;
        g.topicName = "characters"; g.requestedFingerprint = 0xabadcafeULL;
        std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
        h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
        h.pump();

        std::optional<pc::TopicSchemaResponse> resp;
        for (auto& [sid, bytes] : h.captured) {
            auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
            if (d && d->correlationId == 401) { resp = d; break; }
        }
        ASSERT_TRUE(resp.has_value());
        EXPECT_EQ(resp->errorCode, 7u)
            << "UnknownTopic for a fingerprint matching no version across a long lineage";
        EXPECT_EQ(resp->descriptorLen, 0u);
    }
}

// ---------------------------------------------------------------------------
// SchemaGatewayRegression.DdlDoesNotBlockProduce
//
// A stalled/slow AdminWorker (we NEVER pump its doWork) must not delay produce
// routing on the gateway.  We fire a DefineTopic (which only enqueues onto the
// admin ring and returns) and then a Produce for an existing topic; the produce
// ack must be produced on the gateway egress WITHOUT any admin progress.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DdlDoesNotBlockProduce) {
    SgHarness h;
    // A produce target independent of the (stalled) admin plane: created by DDL BEFORE the stall.
    ASSERT_EQ(h.defineUntyped("events", 1, PersistenceMode::Disk), 0);

    // Stall the admin plane entirely: enqueue a DefineTopic but never pump admin.
    auto blob = charsBlob();
    auto def = encodeDefineFrame(1, 1, "characters", 4, "userId", false, blob);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    // The request is now sitting in requestRing; admin has done NO work.
    // (Deliberately do NOT call h.pump() / serviceAdminIngress().)

    const int egressBefore = h.egressFnFrames;

    // Produce to the pre-existing user topic (id=0, "events"), acks=Leader.
    auto batch = makeBatch();
    pc::Produce prod;
    prod.sessionId=1; prod.correlationId=555; prod.topicId=0; prod.partition=0;
    prod.partitionKnown=1; prod.acks=0; prod.producerId=-1; prod.key={};
    prod.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
    std::vector<uint8_t> pframe(64*1024, 0);
    std::size_t plen = prod.encode(pframe.data());

    // Time the produce routing — it must complete promptly (no admin dependency).
    const auto t0 = std::chrono::steady_clock::now();
    h.gw.applyProduceForTest(pframe.data(), plen);
    const auto t1 = std::chrono::steady_clock::now();
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

    // The produce ack must have been produced (delivered on egress) despite the
    // stalled admin.
    EXPECT_GT(h.egressFnFrames, egressBefore)
        << "produce ack must flow while the AdminWorker is stalled (DDL must not block produce)";
    // Tight bound: the gateway produce path does zero admin work; 50ms is generous.
    EXPECT_LT(us, 50'000)
        << "produce routing must not wait on the DDL/admin plane";

    // Sanity: the admin request is still pending (proves we truly stalled it).
    // Draining it now should surface exactly the queued DefineTopic.
    int drained = h.serviceAdminIngress();
    EXPECT_EQ(drained, 1) << "the stalled DefineTopic was still queued on the admin ring";
}

// ===========================================================================
// No-hang wave (R2): every DDL request with a decodable header on a known
// session is answered (its own response type + InvalidSchema / Internal) —
// never dropped while the client waits out its timeout.
// ===========================================================================
namespace {
std::vector<uint8_t> ddlHeaderOnly(pc::TemplateId tid, int64_t corr) {
    pc::MessageHeader h;
    h.blockLength = 32; h.templateId = static_cast<uint16_t>(tid);
    h.sessionId = 1; h.correlationId = corr;
    std::vector<uint8_t> f(pc::MessageHeader::ENCODED_SIZE);
    h.encode(f.data());
    return f;
}
} // namespace

TEST(SchemaGatewayRegression, MalformedGetTopicSchemaIsAnsweredInvalidSchemaRegression) {
    SgHarness h;
    const auto f = ddlHeaderOnly(pc::TemplateId::GetTopicSchema, 91);
    ASSERT_FALSE(pc::GetTopicSchema::decode(f.data(), f.size()).has_value());
    h.gw.applyGetTopicSchemaForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::TopicSchemaResponse>(h);
    ASSERT_TRUE(r.has_value()) << "a malformed GetTopicSchema must get a response";
    EXPECT_EQ(r->correlationId, 91);
    EXPECT_EQ(r->errorCode, 23u) << "InvalidSchema";
}

TEST(SchemaGatewayRegression, MalformedAlterTopicIsAnsweredInvalidSchemaRegression) {
    SgHarness h;
    const auto f = ddlHeaderOnly(pc::TemplateId::AlterTopic, 92);
    ASSERT_FALSE(pc::AlterTopic::decode(f.data(), f.size()).has_value());
    h.gw.applyAlterTopicForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::AlterTopicResponse>(h);
    ASSERT_TRUE(r.has_value()) << "a malformed AlterTopic must get a response";
    EXPECT_EQ(r->correlationId, 92);
    EXPECT_EQ(r->errorCode, 23u) << "InvalidSchema";
}

// The finding's original case: a CreateView whose persistence byte is unknown
// fails CreateView::decode — answered, not dropped.
TEST(SchemaGatewayRegression, MalformedCreateViewIsAnsweredInvalidSchemaRegression) {
    SgHarness h;
    pc::CreateView cv;
    cv.sessionId = 1; cv.correlationId = 93; cv.viewName = "v"; cv.keyField = "id";
    cv.persistence = 1;
    const std::vector<uint8_t> plan = {1, 2, 3};
    cv.planBlob = plan.data(); cv.planLen = static_cast<uint32_t>(plan.size());
    std::vector<uint8_t> f(cv.encodedSize());
    cv.encode(f.data());
    bool patched = false;
    for (std::size_t i = pc::MessageHeader::ENCODED_SIZE; i < f.size(); ++i) {
        std::vector<uint8_t> g = f; g[i] = 2;
        if (!pc::CreateView::decode(g.data(), g.size()).has_value()) { f = g; patched = true; break; }
    }
    ASSERT_TRUE(patched) << "could not locate the persistence byte";
    h.gw.applyCreateViewForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::CreateViewResponse>(h);
    ASSERT_TRUE(r.has_value()) << "a malformed CreateView must get a response";
    EXPECT_EQ(r->correlationId, 93);
    EXPECT_EQ(r->errorCode, 23u) << "InvalidSchema";
}

// The gateway→admin request ring is full: the DDL is answered Internal at once
// (it used to be dropped — `(void)adminSink_->submit...`).
TEST(SchemaGatewayRegression, DdlOnAFullAdminRingIsAnsweredInternalRegression) {
    SgHarness h;
    const std::vector<uint8_t> big(4096, 0xAB);   // fills the ring in a few hundred frames
    int64_t corr = 1;
    std::optional<pc::DefineTopicResponse> r;
    for (; corr < 100'000 && !r; ++corr) {
        const auto def = encodeDefineFrame(1, corr, "t" + std::to_string(corr), 1, "", false, big);
        h.gw.applyDefineTopicForTest(def.data(), def.size());   // nobody drains requestRing
        r = firstFrame<pc::DefineTopicResponse>(h);
    }
    ASSERT_TRUE(r.has_value()) << "a DefineTopic refused by a full admin ring was dropped";
    EXPECT_EQ(r->correlationId, corr - 1);
    EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
}

TEST(SchemaGatewayRegression, DdlWithoutAnAdminSinkIsAnsweredInternalRegression) {
    SgHarness h;
    h.gw.setAdminSink(nullptr);
    const auto def = encodeDefineFrame(1, 94, "orders", 1, "", false, charsBlob());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 94);
    EXPECT_EQ(r->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
}

// R1 (catalog form): an unknown mode / cleanup-policy byte never reaches a create
// — the wire decode refuses them (InvalidSchema, the malformed cases below), the
// ring decode marks the request malformed (AdminWorkerTest), and the catalog's ONE
// config validator refuses a policy out of range (CatalogWritePathTest).

// The admin→control egress ring is full: a DDL answer goes to the admin-owned
// FIFO backlog and is delivered once the control thread drains — the respond*
// callbacks used to `return` on a failed tryClaim (answer lost, client hung).
TEST(SchemaGatewayRegression, DdlAnswerOnAFullEgressRingIsBackloggedNotDroppedRegression) {
    SchemaRegistry sreg;
    rune::node::AdminWorkerAgent agent(sreg, /*selfId=*/1, rune::node::AdminDeps{});
    pc::DefineTopicResponse resp;
    resp.sessionId = 1; resp.topicId = 3; resp.version = 1;
    uint8_t frame[pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED];
    int64_t posted = 0;
    while (agent.egressBacklogForTest() == 0 && posted < 1'000'000) {
        resp.correlationId = ++posted;
        ASSERT_TRUE(agent.postEgressFrame(1, frame, resp.encode(frame)));
    }
    ASSERT_EQ(agent.egressBacklogForTest(), 1u) << "the ring never filled";
    for (int extra = 0; extra < 3; ++extra) {   // FIFO behind the backlog, even if the ring has room
        resp.correlationId = ++posted;
        ASSERT_TRUE(agent.postEgressFrame(1, frame, resp.encode(frame)));
    }
    int64_t expect = 1;
    auto drain = [&] {
        agent.egressRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer& b,
                                     aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
            const auto r = pc::DefineTopicResponse::decode(p + 8, static_cast<std::size_t>(len) - 8);
            ASSERT_TRUE(r.has_value());
            EXPECT_EQ(r->correlationId, expect) << "answers out of order";
            ++expect;
        }, 1 << 20);
    };
    for (int round = 0; round < 64 && expect <= posted; ++round) {
        drain();
        (void)agent.flushEgressBacklogForTest();
    }
    drain();
    EXPECT_EQ(expect, posted + 1) << "every DDL answer delivered exactly once";
    EXPECT_EQ(agent.egressBacklogForTest(), 0u);
}

// Fix round 2: the admin egress backlog is bounded by construction — while it
// holds answers, the agent takes NO new DDL request (the request ring fills and
// the gateway answers Internal); a frame that can never fit one ring message is
// refused (the caller answers MessageTooLarge) instead of parking forever.
TEST(SchemaGatewayRegression, AdminEgressBacklogPausesIntakeAndRefusesUnfittableFramesRegression) {
    SchemaRegistry sreg;
    rune::node::AdminWorkerAgent agent(sreg, /*selfId=*/1, rune::node::AdminDeps{});
    std::vector<uint8_t> huge(agent.maxEgressFrameBytes() + 1, 0);
    EXPECT_FALSE(agent.postEgressFrame(1, huge.data(), huge.size()));
    EXPECT_EQ(agent.egressBacklogForTest(), 0u);

    pc::DefineTopicResponse resp; resp.sessionId = 1;
    uint8_t frame[pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED];
    int64_t corr = 0;
    while (agent.egressBacklogForTest() == 0 && corr < 1'000'000) {
        resp.correlationId = ++corr;
        ASSERT_TRUE(agent.postEgressFrame(1, frame, resp.encode(frame)));
    }
    ASSERT_GT(agent.egressBacklogForTest(), 0u);
    // A DDL request waiting in the request ring is NOT taken while answers wait.
    GetSchemaMsg g; g.session = 1; g.corr = 7; g.name = "x";
    ASSERT_TRUE(encodeGetSchema(agent.requestRing(), g));
    (void)agent.doWork(1);   // the egress ring is still full (nobody drains it)
    std::size_t pendingRequests = 0;
    agent.requestRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                  aeron::util::index_t) { ++pendingRequests; }, 16);
    EXPECT_EQ(pendingRequests, 1u) << "intake continued while DDL answers were backlogged";
}

// Fix round 3 (untested error path): a TopicSchemaResponse whose descriptor can
// never fit one admin egress message is answered MessageTooLarge (no blob) — on
// the ring and through the backlog — never parked forever, never dropped.
TEST(SchemaGatewayRegression, OversizedTopicSchemaAnswerIsMessageTooLargeRegression) {
    SchemaRegistry sreg;
    rune::node::AdminWorkerAgent agent(sreg, /*selfId=*/1, rune::node::AdminDeps{});
    const std::vector<uint8_t> huge(agent.maxEgressFrameBytes() + 1, 0x7A);
    agent.postTopicSchemaResponse(/*session=*/1, /*corr=*/55, /*topicId=*/3, /*version=*/1, /*parts=*/2,
                                  huge, /*errorCode=*/0);
    std::optional<pc::TopicSchemaResponse> got;
    agent.egressRing().drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                                 aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        got = pc::TopicSchemaResponse::decode(p + 8, static_cast<std::size_t>(len) - 8);
    }, 16);
    ASSERT_TRUE(got.has_value()) << "an oversized TopicSchemaResponse was dropped";
    EXPECT_EQ(got->correlationId, 55);
    EXPECT_EQ(got->errorCode, static_cast<uint16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_EQ(got->descriptorLen, 0u);
    EXPECT_EQ(agent.egressBacklogForTest(), 0u);
}

// ===========================================================================
// DDL conformance plan Task 11 — wire-level cells through the gateway with the
// control thread building every applied topic in the REAL TopicRegistry.
// ===========================================================================
namespace {

// ── adoption of a schema-less (untyped, catalog history Task 9) topic ─────────
struct AdoptCase {
    const char* label; PersistenceMode staticMode; int32_t staticParts;
    uint8_t definePersistence; uint32_t requestParts; uint16_t wantCode; PersistenceMode wantRecorded;
};
void PrintTo(const AdoptCase& c, std::ostream* os) { *os << c.label; }
const AdoptCase kAdoptCases[] = {
    {"DiskStatic_DiskDefine_MorePartitions",  PersistenceMode::Disk,      3, 0, 8, 0,  PersistenceMode::Disk},
    {"DiskStatic_DiskDefine_FewerPartitions", PersistenceMode::Disk,      3, 0, 1, 0,  PersistenceMode::Disk},
    {"DiskStatic_TransientDefine",            PersistenceMode::Disk,      3, 1, 3, 25, PersistenceMode::Disk},
    {"TransientStatic_DiskDefine",            PersistenceMode::Transient, 2, 0, 5, 0,  PersistenceMode::Transient},
    {"TransientStatic_TransientDefine",       PersistenceMode::Transient, 2, 1, 2, 0,  PersistenceMode::Transient},
};

class SchemaGatewayAdoptionRegression : public ::testing::TestWithParam<AdoptCase> {};

TEST_P(SchemaGatewayAdoptionRegression, DefineOverStaticSchemaLessTopic) {
    const AdoptCase& c = GetParam();
    SgHarness h;
    h.realControl = true;
    // Catalog history Task 9: the schema-less occupant is an UNTYPED topic created by DDL.
    const int32_t id = h.defineUntyped("orders", static_cast<uint32_t>(c.staticParts), c.staticMode);
    const int32_t topicsBefore = h.topicReg->topicCount();
    const int64_t schemaEnd = h.catalogLog.logEndOffset();

    const auto def = encodeDefineFrame(1, 1, "orders", c.requestParts, "userId", false, charsBlob(),
                                       c.definePersistence);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, c.wantCode);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore) << "adoption never creates a second topic";
    EXPECT_EQ(h.topicReg->topic(id)->persistence, c.staticMode) << "the live topic keeps its backend";
    EXPECT_EQ(h.topicReg->topic(id)->partitionCount, c.staticParts);
    if (c.wantCode == 0) {
        EXPECT_EQ(r->topicId, id);
        const RegistryEntry* e = h.schemaReg.byName("orders");
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(e->persistence, c.wantRecorded);
        EXPECT_EQ(e->partitionCount, c.staticParts) << "the authoritative count, never the requested one";
        EXPECT_EQ(h.catalogLog.logEndOffset(), schemaEnd + 1);
    } else {
        EXPECT_EQ(h.schemaReg.byName("orders"), nullptr);
        EXPECT_EQ(h.catalogLog.logEndOffset(), schemaEnd);
    }
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayAdoptionRegression, ::testing::ValuesIn(kAdoptCases),
    [](const ::testing::TestParamInfo<AdoptCase>& i) { return std::string(i.param.label); });

// ── names over the wire (B1/B2/B3/B6) ────────────────────────────────────────
struct WireNameCase { const char* label; std::string name; uint32_t parts; uint16_t wantCode; };
void PrintTo(const WireNameCase& c, std::ostream* os) { *os << c.label; }
const WireNameCase kWireNameCases[] = {
    {"Empty",              "",                     1, 40},
    {"Slash",              "a/b/c",                1, 40},   // namespaces: two '/' is malformed
    {"UnknownNamespace",   "a/b",                  1, 7},    // namespaces (D7): an unknown namespace answers missing
    {"DotDot",             "..",                   1, 40},
    {"ReservedViews",      "__views",              1, 40},
    {"ReservedSchemas",    "__topic_schemas",      1, 40},
    {"TooLong",            std::string(250, 'x'),  1, 40},
    {"CaseOfEvents",       "Events",               1, 25},
    {"ZeroPartitions",     "fresh",                0, 23},
};

class SchemaGatewayNameRegression : public ::testing::TestWithParam<WireNameCase> {};

TEST_P(SchemaGatewayNameRegression, DefineIsRejectedAndCreatesNothing) {
    const WireNameCase& c = GetParam();
    SgHarness h;
    h.realControl = true;
    ASSERT_EQ(h.defineUntyped("events", 1), 0);   // the occupant (was a static topic)
    const int32_t topicsBefore = h.topicReg->topicCount();
    const int64_t schemaEnd = h.catalogLog.logEndOffset();
    const auto def = encodeDefineFrame(1, 9, c.name, c.parts, "userId", false, charsBlob());
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->correlationId, 9);
    EXPECT_EQ(r->errorCode, c.wantCode);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore);
    EXPECT_EQ(h.catalogLog.logEndOffset(), schemaEnd);
    EXPECT_FALSE(h.topicReg->topicIdForName(c.name).has_value());
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayNameRegression, ::testing::ValuesIn(kWireNameCases),
    [](const ::testing::TestParamInfo<WireNameCase>& i) { return std::string(i.param.label); });

// ── malformed frames: every DDL op with a corrupt body is ANSWERED (never times out)
enum class DdlFrame : uint8_t { Define, CreateView, Alter, GetSchema };
enum class Corruption : uint8_t { UnknownPersistenceByte, HeaderOnly, CutMidBody, UnknownSession };
struct MalformedCase { const char* label; DdlFrame kind; Corruption corruption; };
void PrintTo(const MalformedCase& c, std::ostream* os) { *os << c.label; }
constexpr int64_t kMalformedCorr = 4242;

std::vector<uint8_t> validDdlFrame(DdlFrame k, int64_t session) {
    switch (k) {
        case DdlFrame::Define:
            return encodeDefineFrame(session, kMalformedCorr, "orders", 1, "userId", false, charsBlob(),
                                     pc::DefineTopic::kPersistenceTransient);
        case DdlFrame::CreateView: {
            const std::vector<uint8_t> plan{1, 2, 3};
            pc::CreateView m;
            m.sessionId = session; m.correlationId = kMalformedCorr; m.viewName = "v"; m.keyField = "id";
            m.persistence = 1; m.planBlob = plan.data(); m.planLen = static_cast<uint32_t>(plan.size());
            std::vector<uint8_t> f(m.encodedSize());
            m.encode(f.data());
            return f;
        }
        case DdlFrame::Alter:
            return encodeAlterFrame(session, kMalformedCorr, "orders", 0, {});
        case DdlFrame::GetSchema: {
            pc::GetTopicSchema g;
            g.sessionId = session; g.correlationId = kMalformedCorr; g.topicName = "orders";
            std::vector<uint8_t> f(g.encodedSize());
            g.encode(f.data());
            return f;
        }
    }
    return {};
}

std::vector<uint8_t> corrupt(DdlFrame k, Corruption c) {
    std::vector<uint8_t> f = validDdlFrame(k, c == Corruption::UnknownSession ? 99 : 1);
    switch (c) {
        case Corruption::UnknownPersistenceByte:
        case Corruption::UnknownSession:   // corrupt body on a session the gateway does not know
            if (k == DdlFrame::Define)          f.back() = 2;
            else if (k == DdlFrame::CreateView) f[pc::MessageHeader::ENCODED_SIZE] = 2;
            else                                f.resize(pc::MessageHeader::ENCODED_SIZE);
            break;
        case Corruption::HeaderOnly:
            f.resize(pc::MessageHeader::ENCODED_SIZE);
            break;
        case Corruption::CutMidBody:
            f.resize(f.size() - 3);
            break;
    }
    return f;
}

bool decodes(DdlFrame k, const std::vector<uint8_t>& f) {
    switch (k) {
        case DdlFrame::Define:     return pc::DefineTopic::decode(f.data(), f.size()).has_value();
        case DdlFrame::CreateView: return pc::CreateView::decode(f.data(), f.size()).has_value();
        case DdlFrame::Alter:      return pc::AlterTopic::decode(f.data(), f.size()).has_value();
        case DdlFrame::GetSchema:  return pc::GetTopicSchema::decode(f.data(), f.size()).has_value();
    }
    return false;
}

void apply(SgHarness& h, DdlFrame k, const std::vector<uint8_t>& f) {
    switch (k) {
        case DdlFrame::Define:     h.gw.applyDefineTopicForTest(f.data(), f.size()); break;
        case DdlFrame::CreateView: h.gw.applyCreateViewForTest(f.data(), f.size()); break;
        case DdlFrame::Alter:      h.gw.applyAlterTopicForTest(f.data(), f.size()); break;
        case DdlFrame::GetSchema:  h.gw.applyGetTopicSchemaForTest(f.data(), f.size()); break;
    }
}

/// Frames answering `corr`: the op's OWN response type carrying InvalidSchema (23) —
/// the no-hang wave's rule (RequestRejection.h::encodeRejection).  Any other frame on
/// `corr` (a generic ErrorResponse, another code) is counted in `other`.
template <class R>
int errorAnswers(const SgHarness& h, int64_t corr, int& other) {
    int n = 0;
    for (const auto& [sid, bytes] : h.captured) {
        if (auto r = R::decode(bytes.data(), bytes.size()); r && r->correlationId == corr) {
            if (r->errorCode == static_cast<uint16_t>(pc::ErrorCode::InvalidSchema)) ++n; else ++other;
        } else if (auto e = pc::ErrorResponse::decode(bytes.data(), bytes.size()); e && e->correlationId == corr) {
            ++other;
        }
    }
    return n;
}
int errorAnswersFor(const SgHarness& h, DdlFrame k, int& other) {
    switch (k) {
        case DdlFrame::Define:     return errorAnswers<pc::DefineTopicResponse>(h, kMalformedCorr, other);
        case DdlFrame::CreateView: return errorAnswers<pc::CreateViewResponse>(h, kMalformedCorr, other);
        case DdlFrame::Alter:      return errorAnswers<pc::AlterTopicResponse>(h, kMalformedCorr, other);
        case DdlFrame::GetSchema:  return errorAnswers<pc::TopicSchemaResponse>(h, kMalformedCorr, other);
    }
    return 0;
}

const MalformedCase kMalformedCases[] = {
    {"Define_UnknownPersistenceByte",     DdlFrame::Define,     Corruption::UnknownPersistenceByte},
    {"Define_HeaderOnly",                 DdlFrame::Define,     Corruption::HeaderOnly},
    {"Define_CutMidBody",                 DdlFrame::Define,     Corruption::CutMidBody},
    {"Define_UnknownSession",             DdlFrame::Define,     Corruption::UnknownSession},
    {"CreateView_UnknownPersistenceByte", DdlFrame::CreateView, Corruption::UnknownPersistenceByte},
    {"CreateView_HeaderOnly",             DdlFrame::CreateView, Corruption::HeaderOnly},
    {"CreateView_CutMidBody",             DdlFrame::CreateView, Corruption::CutMidBody},
    {"CreateView_UnknownSession",         DdlFrame::CreateView, Corruption::UnknownSession},
    {"Alter_HeaderOnly",                  DdlFrame::Alter,      Corruption::HeaderOnly},
    {"Alter_CutMidBody",                  DdlFrame::Alter,      Corruption::CutMidBody},
    {"Alter_UnknownSession",              DdlFrame::Alter,      Corruption::UnknownSession},
    {"GetSchema_HeaderOnly",              DdlFrame::GetSchema,  Corruption::HeaderOnly},
    {"GetSchema_CutMidBody",              DdlFrame::GetSchema,  Corruption::CutMidBody},
    {"GetSchema_UnknownSession",          DdlFrame::GetSchema,  Corruption::UnknownSession},
};

class SchemaGatewayMalformedDdlRegression : public ::testing::TestWithParam<MalformedCase> {};

TEST_P(SchemaGatewayMalformedDdlRegression, CorruptBodyIsAnsweredNeverDropped) {
    const MalformedCase& c = GetParam();
    SgHarness h;
    h.realControl = true;
    const std::vector<uint8_t> f = corrupt(c.kind, c.corruption);
    ASSERT_FALSE(decodes(c.kind, f)) << "precondition: the frame body really is corrupt";
    const int64_t schemaEnd = h.catalogLog.logEndOffset();
    const int32_t topicsBefore = h.topicReg->topicCount();

    apply(h, c.kind, f);
    h.pump();

    if (c.corruption == Corruption::UnknownSession) {
        // No session to answer on: the only DDL frame that is not answered (counted + warn-logged).
        EXPECT_TRUE(h.captured.empty()) << "nothing is sent to an unknown session";
    } else {
        int other = 0;
        EXPECT_EQ(errorAnswersFor(h, c.kind, other), 1)
            << "exactly one own-type InvalidSchema (23) answer on correlationId " << kMalformedCorr
            << " — none may time out";
        EXPECT_EQ(other, 0) << "no second / differently-coded answer";
    }
    EXPECT_EQ(h.agent->worker().pendingCount(), 0u) << "a corrupt frame never reaches the admin queue";
    EXPECT_EQ(h.catalogLog.logEndOffset(), schemaEnd);
    EXPECT_EQ(h.topicReg->topicCount(), topicsBefore);
}

INSTANTIATE_TEST_SUITE_P(Ddl, SchemaGatewayMalformedDdlRegression, ::testing::ValuesIn(kMalformedCases),
    [](const ::testing::TestParamInfo<MalformedCase>& i) { return std::string(i.param.label); });

} // namespace

// ── reads / alters of a schema-less topic (Occupant SchemaLessDisk: catalog
//    history Task 9 — an untyped topic created by DDL; F3: an alter is 7) ────
TEST(SchemaGatewayStaticRegression, GetSchemaOfStaticSchemaLessTopicIsUnknownTopic) {
    SgHarness h;
    h.realControl = true;
    ASSERT_GE(h.defineUntyped("events", 1), 0);   // the schema-less occupant: an untyped topic
    const auto r = getSchema(h, "events", 31);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 7u);
}

TEST(SchemaGatewayStaticRegression, AlterOfStaticSchemaLessTopicIsUnknownTopic) {
    SgHarness h;
    h.realControl = true;
    ASSERT_GE(h.defineUntyped("events", 1), 0);   // the schema-less occupant: an untyped topic
    const auto f = encodeAlterFrame(1, 32, "events", 0, rune::codec::encodeAlterOps({addColumnOp("x", PrimType::Int64)}));
    h.gw.applyAlterTopicForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::AlterTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->errorCode, 7u);
    EXPECT_EQ(h.schemaReg.byName("events"), nullptr);
}

// ===========================================================================
// Catalog history Task 9 — the catalog DDL ops (74–77) and the v8 extension,
// through the REAL gateway handlers, the real AdminWorkerAgent and its rings.
// ===========================================================================
namespace {

pc::DdlExtension extWith(std::string_view statement, rune::protocol::RequestId rid) {
    pc::DdlExtension e;
    e.statement  = statement;
    e.clientName = "sg-client";
    e.requestId  = rid;
    return e;
}

template <class M>
std::vector<uint8_t> encodeFrame(const M& m) {
    std::vector<uint8_t> f(m.encodedSize());
    f.resize(m.encode(f.data()));
    EXPECT_FALSE(f.empty()) << "the encoder refused the frame";
    return f;
}

std::optional<pc::DdlResponse> ddlAnswer(const SgHarness& h, int64_t corr) {
    for (const auto& [sid, bytes] : h.captured)
        if (auto r = pc::DdlResponse::decode(bytes.data(), bytes.size()); r && r->correlationId == corr) return r;
    return std::nullopt;
}

} // namespace

TEST(SchemaGatewayRegression, CatalogDdlOpsRouteThroughTheGatewayAndAnswerDdlResponseRegression) {
    SgHarness h;
    h.realControl = true;
    const int32_t id = h.defineUntyped("orders", 1);
    ASSERT_GE(id, 0);

    // AlterTopicConfig (75): the topic's object version 1 → 2.
    std::vector<uint8_t> changes(pc::configChangesEncodedSize(1));
    const pc::ConfigChangeWire one{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 1234};
    ASSERT_EQ(pc::putConfigChanges(changes.data(), std::span<const pc::ConfigChangeWire>(&one, 1)), changes.size());
    pc::AlterTopicConfig cfg;
    cfg.sessionId = 1; cfg.correlationId = 501; cfg.topicName = "orders"; cfg.expectedVersion = 1;
    cfg.changes = changes; cfg.ext = extWith("alter topic orders set retention.ms = 1234;", {0xA, 1});
    const auto cf = encodeFrame(cfg);
    h.gw.applyDdlFrameForTest(cf.data(), cf.size());
    h.pump();
    const auto ca = ddlAnswer(h, 501);
    ASSERT_TRUE(ca.has_value()) << "AlterTopicConfig is answered by a DdlResponse";
    EXPECT_EQ(ca->errorCode, 0u);
    EXPECT_EQ(ca->newVersion, 2u);
    EXPECT_EQ(ca->gateTopicId, -1);
    EXPECT_EQ(h.agent->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, "orders")->config.retentionMs, 1234);

    // DropTopic (74).
    pc::DropTopic dt;
    dt.sessionId = 1; dt.correlationId = 502; dt.topicName = "orders";
    dt.ext = extWith("drop topic orders;", {0xA, 2});
    const auto df = encodeFrame(dt);
    h.gw.applyDdlFrameForTest(df.data(), df.size());
    h.pump();
    const auto da = ddlAnswer(h, 502);
    ASSERT_TRUE(da.has_value());
    EXPECT_EQ(da->errorCode, 0u);
    EXPECT_EQ(da->newVersion, 3u);
    EXPECT_EQ(h.agent->worker().applier().state().topicByName(rune::catalog::kDefaultNsId, "orders"), nullptr) << "dropped";

    // DropView (76) of an unknown view: UnknownTopic with a detail.
    pc::DropView dv;
    dv.sessionId = 1; dv.correlationId = 503; dv.viewName = "nope";
    dv.ext = extWith("drop view nope;", {0xA, 3});
    const auto vf = encodeFrame(dv);
    h.gw.applyDdlFrameForTest(vf.data(), vf.size());
    h.pump();
    const auto va = ddlAnswer(h, 503);
    ASSERT_TRUE(va.has_value());
    EXPECT_EQ(va->errorCode, 7u);
    EXPECT_EQ(va->detail, "no view named 'nope'");
}

// The v8 extension (statement, client name, requestId) and the gateway's own
// session id + egress channel reach the event's principal (ruling 13).
TEST(SchemaGatewayRegression, DdlExtensionReachesTheEventPrincipalRegression) {
    SgHarness h;
    const int64_t sid = h.gw.injectTestSessionOnChannel("aeron:udp?endpoint=client-host:40999", 0, "127.0.0.1:51234");
    pc::DefineTopic m;
    m.sessionId = sid; m.correlationId = 600; m.topicName = "p"; m.partitionCount = 1;
    m.hasExt = true; m.replicationFactor = 0;
    m.ext = extWith("topic p partitions 1;", {0xBEEF, 77});
    const auto f = encodeFrame(m);
    h.gw.applyDefineTopicForTest(f.data(), f.size());
    h.pump();
    const auto r = firstFrame<pc::DefineTopicResponse>(h);
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->errorCode, 0u);
    // Read the event back from the catalog log.
    const std::vector<uint8_t> data = h.catalogReplica->read(0, 1 << 20);
    rune::protocol::RecordBatchReader reader(data.data(), data.size());
    ASSERT_TRUE(reader.ensureValid());
    std::optional<rune::catalog::CatalogEvent> ev;
    reader.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
        auto d = rune::catalog::decode(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
        if (d) ev = std::move(*d);
    });
    ASSERT_TRUE(ev.has_value());
    EXPECT_EQ(ev->statement, "topic p partitions 1;");
    EXPECT_EQ(ev->principal.clientName, "sg-client");
    EXPECT_EQ(ev->principal.requestId, (rune::protocol::RequestId{0xBEEF, 77}));
    EXPECT_EQ(ev->principal.sessionId, sid) << "the gateway's session id";
    EXPECT_TRUE(ev->principal.sourceAddress.starts_with("127.0.0.1:"))
        << "the Connect image's source identity (gateway-supplied, ruling 13, #227): " << ev->principal.sourceAddress;
    EXPECT_NE(ev->principal.sourceAddress, "aeron:udp?endpoint=client-host:40999") << "never the declared channel";
}

// An undecodable catalog DDL body is answered InvalidRequest in a DdlResponse;
// one larger than an admin ring message is answered InvalidRequest (never the
// retriable Internal of a full ring) — both explicit, nothing written.
TEST(SchemaGatewayRegression, CatalogDdlMalformedOrTooLargeIsAnsweredExplicitlyRegression) {
    SgHarness h;
    const auto hdr = ddlHeaderOnly(pc::TemplateId::DropTopic, 700);
    ASSERT_FALSE(pc::DropTopic::decode(hdr.data(), hdr.size()).has_value());
    h.gw.applyDdlFrameForTest(hdr.data(), hdr.size());
    const auto bad = ddlAnswer(h, 700);
    ASSERT_TRUE(bad.has_value()) << "a malformed DropTopic must be answered";
    EXPECT_EQ(bad->errorCode, 39u);

    const std::string huge(static_cast<std::size_t>(kAdminRequestRingBytes) / 8, 's');
    pc::DropTopic big;
    big.sessionId = 1; big.correlationId = 701; big.topicName = "t";
    big.ext = extWith(huge, {0xA, 9});
    const auto bf = encodeFrame(big);
    h.gw.applyDdlFrameForTest(bf.data(), bf.size());
    const auto large = ddlAnswer(h, 701);
    ASSERT_TRUE(large.has_value());
    EXPECT_EQ(large->errorCode, 39u) << "permanent: InvalidRequest, never the retriable Internal";
    EXPECT_EQ(h.catalogLog.logEndOffset(), 0);
}
