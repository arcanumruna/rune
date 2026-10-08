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
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/RecordBatch.h>

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

// A minimal record batch (for the produce regression test).
std::vector<uint8_t> makeBatch() {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, std::string_view{"hello"}, /*timestamp=*/0);
    return b.build();
}

// End-to-end harness: gateway + AdminWorkerAgent-equivalent wiring, single thread.
struct SgHarness {
    // Schema replica (single-node leader, minIsr=1).
    std::string schemaDirPath;
    rune::storage::Log schemaLog;
    std::unique_ptr<NullPartitionTransport> schemaTransport;
    std::unique_ptr<PartitionReplica> schemaReplica;
    SchemaRegistry schemaReg;

    // Rings (mirror BrokerNode::AdminWorkerAgent).
    OneToOneByteRing requestRing{kAdminRequestRingBytes};
    OneToOneByteRing replyRing{kCreateTopicRingBytes};
    OneToOneByteRing createReqRing{kCreateTopicRingBytes};
    OneToOneByteRing egressRing{kAdminRequestRingBytes};

    std::unique_ptr<AdminWorker> admin;

    // Gateway + its user topic registry.
    std::string topicDirPath;
    std::unique_ptr<rune::gateway::TopicRegistry> topicReg;

    // gateway → admin sink adapter (encodes onto requestRing).
    struct SinkAdapter : rune::gateway::IAdminRequestSink {
        OneToOneByteRing* ring;
        bool submitDefineTopic(int64_t s, int64_t c, std::string_view name,
                               uint32_t parts, std::string_view keyField, bool validate,
                               const uint8_t* blob, uint32_t blen) override {
            DefineTopicMsg m; m.session=s; m.corr=c; m.partitionCount=(int32_t)parts;
            m.validate=validate; m.name.assign(name); m.keyField.assign(keyField);
            if (blob && blen) m.blob.assign(blob, blob+blen);
            return encodeDefineTopic(*ring, m);
        }
        bool submitGetSchema(int64_t s, int64_t c, std::string_view name,
                             uint64_t requestedFingerprint) override {
            GetSchemaMsg m; m.session=s; m.corr=c; m.name.assign(name);
            m.requestedFingerprint = requestedFingerprint;
            return encodeGetSchema(*ring, m);
        }
    } sink;

    // Captured egress frames (sessionId, bytes) via onControlFrame_.
    std::vector<std::pair<int64_t, std::vector<uint8_t>>> captured;
    // Count of frames delivered via the gateway test-egress fn (produce acks etc.).
    int egressFnFrames = 0;

    // Control-thread state.
    int32_t nextTopicId = 0;

    // Gateway (by value — non-movable; guaranteed-copy-elision init in the list).
    // Declared LAST so egressFnFrames is alive when the egress fn captures it.
    rune::gateway::ClientGateway gw;

    SgHarness()
        : schemaDirPath(uniqDir("schema")), schemaLog(schemaDirPath),
          topicDirPath(uniqDir("topics")),
          gw(rune::gateway::ClientGateway::makeForTest(
                 [c = &egressFnFrames](const uint8_t*, std::size_t){ ++(*c); return int64_t{1}; },
                 /*sessionId=*/1)) {
        schemaLog.recover();
        schemaTransport = std::make_unique<NullPartitionTransport>();
        PartitionConfig cfg;
        cfg.topicId = kSchemaTopicId; cfg.partitionId = 0; cfg.selfId = 1;
        cfg.minIsr = 1; cfg.replicaLagMaxMs = 10'000; cfg.replicaLagMaxOffset = 10'000;
        cfg.fetchIntervalNs = 1'000'000LL; cfg.isrCheckIntervalNs = 1'000'000LL;
        cfg.maxFetchBytes = 1 << 20; cfg.cleanupPolicy = CleanupPolicy::Compact;
        schemaReplica = std::make_unique<PartitionReplica>(
            cfg, Role::Follower, 0, std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &schemaLog, schemaTransport.get());

        // AdminDeps: requestCreateTopic → createReqRing; respond* → egressRing.
        AdminDeps deps;
        deps.requestCreateTopic = [this](int64_t ticket, int32_t parts,
                                         const std::string& name) -> bool {
            CreateTopicReqMsg m; m.ticket=ticket; m.partitionCount=parts; m.name=name;
            return encodeCreateTopicReq(createReqRing, m);
        };
        deps.pushValidators = [](const RegistryEntry&, int32_t) {};
        deps.respondDefine = [this](int64_t session, int64_t corr, int32_t tid,
                                    uint16_t ver, uint16_t err) {
            pc::DefineTopicResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.version=ver; r.errorCode=err;
            uint8_t f[pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED];
            std::size_t n = r.encode(f);
            auto idx = egressRing.tryClaim(kMsgEgressFrame, 8+n);
            if (idx < 0) return;
            uint8_t* p = egressRing.base()+idx;
            std::memcpy(p, &session, 8); std::memcpy(p+8, f, n);
            egressRing.commit(idx);
        };
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t tid,
                                 uint16_t ver, const std::vector<uint8_t>& blob, uint16_t err) {
            pc::TopicSchemaResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.version=ver; r.errorCode=err;
            r.descriptorBlob = blob.empty()?nullptr:blob.data();
            r.descriptorLen = (uint32_t)blob.size();
            std::vector<uint8_t> f(r.encodedSize());
            std::size_t n = r.encode(f.data());
            auto idx = egressRing.tryClaim(kMsgEgressFrame, 8+n);
            if (idx < 0) return;
            uint8_t* p = egressRing.base()+idx;
            std::memcpy(p, &session, 8); std::memcpy(p+8, f.data(), n);
            egressRing.commit(idx);
        };
        // respondAlter (Task 13 test harness fix-on-sight): this harness never
        // wired AlterTopic responses, so an ALTER driven through it would call
        // an EMPTY std::function (deps_.respondAlter) and crash with
        // std::bad_function_call. GetTopicSchemaByFingerprintReturnsBoundVersion
        // drives a real ALTER through these rings, so wire it the same way as
        // respondDefine/respondGet (encode + egressRing).
        deps.respondAlter = [this](int64_t session, int64_t corr, int32_t tid,
                                   uint16_t newVer, uint16_t err) {
            pc::AlterTopicResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.newVersion=newVer; r.errorCode=err;
            uint8_t f[pc::MessageHeader::ENCODED_SIZE + pc::AlterTopicResponse::BODY_FIXED];
            std::size_t n = r.encode(f);
            auto idx = egressRing.tryClaim(kMsgEgressFrame, 8+n);
            if (idx < 0) return;
            uint8_t* p = egressRing.base()+idx;
            std::memcpy(p, &session, 8); std::memcpy(p+8, f, n);
            egressRing.commit(idx);
        };
        admin = std::make_unique<AdminWorker>(schemaReg, schemaReplica.get(), 1,
                                              std::vector<int32_t>{1}, std::move(deps));
        admin->onRole(AdminRoleMsg{true, 1, 1});

        // Gateway with a test-egress + one pre-existing user topic (id=0) for
        // the produce regression test.
        std::vector<rune::node::TopicSpec> specs;
        { rune::node::TopicSpec ts; ts.name="events"; ts.partitionCount=1; ts.topicId=0;
          specs.push_back(ts); }
        topicReg = std::make_unique<rune::gateway::TopicRegistry>(specs, topicDirPath);

        gw.setTopicRegistry(topicReg.get());
        sink.ring = &requestRing;
        gw.setAdminSink(&sink);
        gw.setOnControlFrame([this](int64_t sid, const uint8_t* b, std::size_t n) {
            captured.emplace_back(sid, std::vector<uint8_t>(b, b+n));
        });
        gw.setOnEgressFrame([this](int64_t sid, const uint8_t* b, std::size_t n) {
            captured.emplace_back(sid, std::vector<uint8_t>(b, b+n));
        });
        gw.injectTestSession(/*nowNs=*/0);  // session id 1
    }

    // Play the CONTROL thread: drain createReqRing → createTopic → replyRing;
    // drain egressRing → gateway.sendControlFrame.
    int serviceControl() {
        int work = 0;
        work += createReqRing.drain([this](int32_t, aeron::concurrent::AtomicBuffer& b,
                                           aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            CreateTopicReqMsg req = decodeCreateTopicReq(p, (std::size_t)len);
            CreateTopicReplyMsg reply; reply.ticket = req.ticket;
            reply.topicId = nextTopicId++;  // authoritative id
            (void)encodeCreateTopicReply(replyRing, reply);
        }, 8);
        work += egressRing.drain([this](int32_t, aeron::concurrent::AtomicBuffer& b,
                                        aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            const auto n = (std::size_t)len; if (n < 8) return;
            int64_t session; std::memcpy(&session, p, 8);
            (void)gw.sendControlFrame(session, p+8, n-8);
        }, 16);
        return work;
    }

    // Drain the requestRing into the admin worker (the AdminWorkerAgent does this).
    int serviceAdminIngress() {
        return requestRing.drain([this](int32_t tid, aeron::concurrent::AtomicBuffer& b,
                                        aeron::util::index_t off, aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
            const auto n = (std::size_t)len;
            if (tid == kMsgDefineTopic) admin->submitDefineTopic(decodeDefineTopic(p, n));
            else if (tid == kMsgGetSchema) admin->submitGetSchema(decodeGetSchema(p, n));
            // Task 13 fix-on-sight: this hand-rolled mirror of the
            // AdminWorkerAgent (BrokerNode.h) was missing the ALTER branch —
            // an AlterTopic frame on requestRing was silently consumed
            // (ring position advanced) and dropped (never reached
            // submitAlterTopic), so no ALTER driven directly through this
            // harness's ring ever took effect.
            else if (tid == kMsgAlterTopic) admin->submitAlterTopic(decodeAlterTopic(p, n));
        }, 16);
    }

    // Pump all rings + admin doWork + reply drain until quiescent (bounded).
    void pump(int cycles = 60) {
        int64_t nowNs = 0;
        for (int i = 0; i < cycles; ++i) {
            nowNs += 1'000'000LL;
            serviceAdminIngress();
            // admin drains its reply ring internally in the agent; here mirror it:
            replyRing.drain([this](int32_t, aeron::concurrent::AtomicBuffer& b,
                                   aeron::util::index_t off, aeron::util::index_t len) {
                const auto* p = reinterpret_cast<const uint8_t*>(b.buffer()+off);
                admin->onCreateTopicReply(decodeCreateTopicReply(p, (std::size_t)len));
            }, 16);
            admin->doWork(nowNs);
            serviceControl();
        }
    }
};

// Encode a DefineTopic client frame.
std::vector<uint8_t> encodeDefineFrame(int64_t session, int64_t corr,
                                       const std::string& name, uint32_t parts,
                                       const std::string& keyField, bool validate,
                                       const std::vector<uint8_t>& blob) {
    pc::DefineTopic m;
    m.sessionId = session; m.correlationId = corr; m.topicName = name;
    m.partitionCount = parts; m.keyField = keyField; m.validate = validate;
    m.descriptorBlob = blob.data(); m.descriptorLen = (uint32_t)blob.size();
    std::vector<uint8_t> frame(m.encodedSize());
    m.encode(frame.data());
    return frame;
}

} // namespace

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
    EXPECT_GE(h.schemaLog.logEndOffset(), 1LL)
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
    ASSERT_TRUE(encodeAlterTopic(h.requestRing, am));
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
// SchemaGatewayRegression.DdlDoesNotBlockProduce
//
// A stalled/slow AdminWorker (we NEVER pump its doWork) must not delay produce
// routing on the gateway.  We fire a DefineTopic (which only enqueues onto the
// admin ring and returns) and then a Produce for an existing topic; the produce
// ack must be produced on the gateway egress WITHOUT any admin progress.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DdlDoesNotBlockProduce) {
    SgHarness h;

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
