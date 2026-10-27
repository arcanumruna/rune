// ---------------------------------------------------------------------------
// GroupPushDeliveryTest.cpp — CG-4 fix: rewritten integration tests.
//
// Tests drive the REAL orchestration path:
//   - Real ClientGateway (embedded IPC Aeron driver) + setOnEgressFrame for
//     capture without a full network stack.
//   - Real TopicRegistry + ReplicaRegistry wired into the gateway.
//   - Real GroupCoordinationService wired via setGateway / setGroupService.
//   - service_->doWork() drives rebalance to Stable; the SERVICE (not the test)
//     calls addGroupSubscription + sendControlFrame.
//   - gateway_->pushSubscriptions() (driven via doWork) delivers Records;
//     captured by the onEgressFrame callback.
//
// Tests:
//   CG4_Delivery.EachPartitionToExactlyOneMember
//     2 members join a 2-partition topic → Stable → pushSubscriptions pumped.
//     Assert each member receives records for exactly ONE partition; every
//     partition owned by exactly one member; union of delivered = all produced.
//
//   CG4_Delivery.ResumeFromCommittedOffset
//     Commit offset 5 for (g, T:p0) BEFORE join → owning member's egress
//     delivers offsets 5..N-1 only (actual delivered baseOffsets verified).
//
//   CG4_Delivery.EagerRevokeAndReassign
//     3rd member joins after Stable → service sends empty Assign (revoke) then
//     reassigns; after second Stable still single-owner per partition.
//
//   CG4_Delivery.AssignFramesDelivered
//     Assert each member's egress receives an Assign frame with its partition
//     set on Stable (decoded from captured egress); and an empty Assign on revoke.
//
//   CG4_AssignFrameEncodeDecode.FullAssign  — pure encode/decode unit test (kept)
//   CG4_AssignFrameEncodeDecode.EmptyAssignIsRevoke — pure encode/decode (kept)
//
// Embedded Aeron IPC driver — no network ports, no Docker.
// All waits bounded by kCap + ASSERT.  No busy-spin.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>

// Gateway
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/TopicRegistry.h>

// Node / coordination
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/GroupCoordinationService.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/ReplicaRegistry.h>
#include <rune/coordination/GroupCoordinator.h>
#include <rune/coordination/ReplicatedOffsetStore.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

// Protocol
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/GroupMessages.h>
#include <rune/protocol/client/OffsetMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/client/ArrayView.h>

// Aeron
#include <Aeron.h>
extern "C" {
#include <aeronmd/aeronmd.h>
}

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::node;
using namespace rune::coordination;
using namespace rune::replication;
namespace pc = rune::protocol::client;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

// ===========================================================================
// Constants
// ===========================================================================

static constexpr int     kCap          = 1000;
static constexpr int64_t kStepNs       = 1'000'000LL;   // 1 ms
static constexpr int64_t kT0Ns         = 1'000'000'000LL;
static constexpr int32_t kTopicId      = 0;
static constexpr int32_t kNumParts     = 2;
static constexpr int32_t kNumRecords   = 10;  // records per partition
static constexpr std::size_t kBufSz    = 4096;

// ===========================================================================
// Helpers
// ===========================================================================

static std::string makeSuffix() {
    static std::atomic<uint64_t> g{0};
    return std::to_string(::getpid()) + "_" +
           std::to_string(g.fetch_add(1, std::memory_order_relaxed));
}

struct TempDir {
    std::string path;
    explicit TempDir(std::string_view tag) {
        path = fs::temp_directory_path().string()
             + "/rune_cg4d_" + std::string(tag) + "_" + makeSuffix();
        fs::create_directories(path);
    }
    ~TempDir() { std::error_code ec; fs::remove_all(path, ec); }
};

static TopicSpec makeTopicSpec(std::string name, int32_t partCount, int32_t id) {
    TopicSpec ts;
    ts.name           = std::move(name);
    ts.partitionCount = partCount;
    ts.topicId        = id;
    return ts;
}

static PartitionConfig makePartCfg(int32_t selfId = 1) {
    PartitionConfig cfg;
    cfg.partitionId         = 0;
    cfg.selfId              = selfId;
    cfg.replicaLagMaxMs     = 10'000;
    cfg.replicaLagMaxOffset = 10'000;
    cfg.minIsr              = 1;
    cfg.fetchIntervalNs     = 1'000'000LL;
    cfg.isrCheckIntervalNs  = 1'000'000LL;
    cfg.maxFetchBytes       = 1 << 20;
    cfg.pushOnAppend        = false;
    return cfg;
}

class NullTransport : public PartitionTransport {
public:
    bool sendFetchRequest(const FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(NodeId, const FetchResponseMsg&) override { return true; }
};

// Build kN individual record batches (one per record) and write them to the log.
// Each record is its own batch so that log->read(offset, ...) returns the batch
// starting exactly at that offset, enabling precise resume-from-committed testing.
static void appendRecords(rune::storage::Log& log, int32_t kN, int64_t baseTs = 2000LL) {
    for (int32_t i = 0; i < kN; ++i) {
        const int64_t base = log.logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, baseTs + i);
        b.append("k", "v", baseTs + i);
        auto bytes = b.build();
        log.appendNoSync(bytes.data(), bytes.size(), base);
        log.sync();
    }
}

// Decode a Records frame from raw bytes; return baseOffset or -1 on failure.
static int64_t decodeRecordsBaseOffset(const uint8_t* buf, std::size_t len) {
    auto dec = pc::Records::decode(buf, len);
    if (!dec) return -1;
    return dec->baseOffset;
}

// Decode an Assign frame from raw bytes; return the decoded Assign or empty.
static std::optional<pc::Assign> decodeAssign(const uint8_t* buf, std::size_t len) {
    return pc::Assign::decode(buf, len);
}

// Decode the templateId from a frame header.
static uint16_t frameTemplateId(const uint8_t* buf, std::size_t len) {
    auto hdr = pc::MessageHeader::decode(buf, len);
    if (!hdr) return 0;
    return hdr->templateId;
}

// ===========================================================================
// Embedded Aeron IPC driver — creates a shared single driver for the fixture.
// Uses IPC so no network ports are needed.
// ===========================================================================
struct EmbeddedAeronDriver {
    // One dedicated pumping thread (EmbeddedDriver::Threaded); no caller-side
    // readiness pumping — aeron_driver_init/start already wrote cnc.dat and
    // stamped the heartbeat synchronously before the driver thread even starts.
    std::unique_ptr<rune::node::EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron>                aeron;
    bool                                          ok = false;

    // Returns false on failure (call site checks ok flag).
    bool start(const std::string& aeronDir) {
        fs::create_directories(aeronDir);
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir             = aeronDir;
        cfg.termBufferBytes = 64 * 1024;
        try {
            driver = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
        } catch (const std::exception&) {
            return false;
        }

        aeron::Context ctx;
        ctx.aeronDir(aeronDir);
        aeron = aeron::Aeron::connect(ctx);
        if (!aeron) return false;
        ok = true;
        return true;
    }

    ~EmbeddedAeronDriver() {
        aeron.reset();
        driver.reset();
    }
};

// ===========================================================================
// Captured egress frame — records both raw bytes and the template id
// so tests can distinguish Records pushes from Assign control frames.
// ===========================================================================
struct EgressFrame {
    int64_t sessionId = 0;
    std::vector<uint8_t> bytes;
};

// ===========================================================================
// CG4DeliveryFixture
//
// Single fixture for all real-delivery tests.
//
// Setup:
//   - Embedded Aeron IPC driver.
//   - TopicRegistry with kNumParts=2 partitions of "test-topic".
//   - kNumRecords records pre-appended to EACH partition log.
//   - PartitionReplica (Leader role, minIsr=1) per partition → LEO == HW.
//   - ReplicaRegistry wired into the gateway.
//   - GroupCoordinationService wired into the gateway (both directions).
//   - injectTestSession for each member: null egress, setOnEgressFrame captures.
// ===========================================================================
class CG4DeliveryFixture : public ::testing::Test {
protected:
    static constexpr std::string_view kTopicName = "test-topic";
    static constexpr int32_t kIngressStream = 9901;

    CG4DeliveryFixture()
        : aeronDir_("aeron")
        , topicDir_("topicdata")
        , offsetDir_("offset")
        , offsetLog_(offsetDir_.path)
    {}

    void SetUp() override {
        // 1. Embedded Aeron IPC driver.
        aeron_drv_ = std::make_unique<EmbeddedAeronDriver>();
        ASSERT_TRUE(aeron_drv_->start(aeronDir_.path))
            << "Embedded Aeron driver failed to start";
        ASSERT_NE(aeron_drv_->aeron, nullptr);

        // 2. TopicRegistry: 2 partitions.
        std::vector<TopicSpec> specs = {
            makeTopicSpec(std::string(kTopicName), kNumParts, kTopicId)
        };
        topicReg_ = std::make_unique<rune::gateway::TopicRegistry>(specs, topicDir_.path);

        // 3. Append kNumRecords records to each partition log.
        for (int32_t p = 0; p < kNumParts; ++p) {
            rune::storage::Log* log = topicReg_->log(kTopicId, p);
            ASSERT_NE(log, nullptr);
            appendRecords(*log, kNumRecords);
            ASSERT_EQ(log->logEndOffset(), kNumRecords);
        }

        // 4. PartitionReplicas (one per partition) — Leader, minIsr=1 → HW == LEO.
        // ReplicaRegistry takes unique_ptr ownership; keep raw pointers for HW advance.
        nullTransport_ = std::make_unique<NullTransport>();
        replicaReg_ = std::make_unique<rune::node::ReplicaRegistry>();
        for (int32_t p = 0; p < kNumParts; ++p) {
            rune::storage::Log* log = topicReg_->log(kTopicId, p);
            auto cfg = makePartCfg(1);
            cfg.partitionId = p;
            auto r = std::make_unique<PartitionReplica>(
                cfg, Role::Leader, /*generation=*/1,
                std::vector<NodeId>{1}, std::vector<NodeId>{1},
                log, nullTransport_.get());
            r->onLeaderLogAppended();  // advance HW to LEO
            replicaRawPtrs_.push_back(r.get());
            replicaReg_->add(kTopicId, p, std::move(r));
        }

        // 5. ReplicatedOffsetStore (for committed-offset tests).
        offsetReplica_ = std::make_unique<PartitionReplica>(
            makePartCfg(1), Role::Leader, 1,
            std::vector<NodeId>{1}, std::vector<NodeId>{1},
            &offsetLog_, nullTransport_.get());
        offsetStore_ = std::make_unique<ReplicatedOffsetStore>(offsetReplica_.get());

        // 6. ClientGateway — ingress on IPC, IPC-only (no real clients connect).
        const std::string ingressCh = "aeron:ipc";
        gw_ = std::make_unique<rune::gateway::ClientGateway>(
            aeron_drv_->aeron, ingressCh, kIngressStream,
            /*thisNodeId=*/1,
            /*sessionTimeoutNs=*/rune::gateway::ClientGateway::kDefaultSessionTimeoutNs,
            /*leaderView=*/nullptr);

        gw_->setTopicRegistry(topicReg_.get());
        gw_->setReplicaRegistry(replicaReg_.get());

        // 7. Install egress capture callback.
        //    All sends through sendTo (push Records + Assign) land here.
        gw_->setOnEgressFrame([this](int64_t sid, const uint8_t* buf, std::size_t len) {
            std::vector<uint8_t> bytes(buf, buf + len);
            std::lock_guard lk(frameMx_);
            captured_[sid].push_back({sid, std::move(bytes)});
        });
        // Also capture control frames (sendControlFrame fires this BEFORE sendTo).
        gw_->setOnControlFrame([this](int64_t sid, const uint8_t* buf, std::size_t len) {
            std::vector<uint8_t> bytes(buf, buf + len);
            std::lock_guard lk(frameMx_);
            controlFrames_[sid].push_back({sid, std::move(bytes)});
        });

        // 8. GroupCoordinationService.
        service_ = std::make_unique<GroupCoordinationService>(
            offsetStore_.get(), topicReg_.get(),
            [] { return true; }, [] { return NodeId{1}; });

        // 9. Wire both directions:
        //    gateway → service (for group message dispatch).
        gw_->setGroupService(service_.get());
        //    service → gateway (for addGroupSubscription / revokeGroupSubscriptions / sendControlFrame).
        service_->setGateway(gw_.get());

        clock_ = kT0Ns;
    }

    void TearDown() override {
        service_.reset();
        gw_.reset();
        offsetStore_.reset();
        offsetReplica_.reset();
        replicaReg_.reset();   // destroys owned PartitionReplica instances
        replicaRawPtrs_.clear();
        topicReg_.reset();
        aeron_drv_.reset();
    }

    // -----------------------------------------------------------------------
    // Inject a test session (null egress — captured via onEgressFrame).
    // -----------------------------------------------------------------------
    int64_t injectSession() {
        return gw_->injectTestSession(clock_);
    }

    // -----------------------------------------------------------------------
    // Encode + call handleJoinGroup; return assigned memberId.
    // -----------------------------------------------------------------------
    std::string doJoin(int64_t sessionId, int64_t correlationId,
                       std::string_view groupId, std::string_view memberId = "") {
        pc::JoinGroup msg;
        msg.sessionId        = sessionId;
        msg.correlationId    = correlationId;
        msg.sessionTimeoutMs = 5000;
        msg.groupId  = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
        msg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        std::array<int32_t, 1> topics{kTopicId};
        uint8_t req[kBufSz]{}, out[kBufSz]{};
        const auto reqLen = msg.encode(req, std::span<const int32_t>(topics));
        const auto outLen = service_->handleJoinGroup(req, reqLen, rune::protocol::client::kClientVersion, clock_, out, kBufSz);
        if (outLen == 0) return {};
        const auto resp = pc::JoinGroupResponse::decode(out, outLen);
        if (!resp) return {};
        return std::string(reinterpret_cast<const char*>(resp->memberId.data()),
                           resp->memberId.size());
    }

    // -----------------------------------------------------------------------
    // Encode + call handleSyncGroup for one member.
    // Must be called AFTER the coordinator has reached Stable so that the
    // SyncGroup triggers addGroupSubscription for this member.
    // Returns the SyncGroupResponse error code (0 = OK), or -1 on decode failure.
    // -----------------------------------------------------------------------
    int doSync(int64_t sessionId, int64_t correlationId,
               std::string_view groupId, std::string_view memberId) {
        const int32_t gen = service_->coordinator(std::string(groupId)).generationId();
        pc::SyncGroup msg;
        msg.sessionId     = sessionId;
        msg.correlationId = correlationId;
        msg.generationId  = gen;
        msg.groupId  = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(groupId.data()), groupId.size());
        msg.memberId = std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(memberId.data()), memberId.size());
        uint8_t req[kBufSz]{}, out[kBufSz]{};
        const auto reqLen = msg.encode(req);
        const auto outLen = service_->handleSyncGroup(req, reqLen, clock_, out, kBufSz);
        if (outLen == 0) return -1;
        const auto resp = pc::SyncGroupResponse::decode(out, outLen);
        if (!resp) return -1;
        return resp->errorCode;
    }

    // -----------------------------------------------------------------------
    // CG routing redesign: the coordinator creates no subscription at SyncGroup
    // — the member subscribes explicitly to each assigned partition (on the
    // partition's leader; here the single node) after its OffsetFetch, from
    // the committed offset (else the log start).  Mirrors the client.
    // Returns the subscription ids (from the captured SubscribeResponses).
    // -----------------------------------------------------------------------
    std::vector<int64_t> subscribeAssigned(int64_t sessionId, const std::string& group,
                                           const std::string& memberId, int64_t corrBase) {
        std::vector<int64_t> ids;
        const auto& coord = service_->coordinator(group);
        for (const auto& member : coord.members()) {
            if (member.memberId != memberId) continue;
            for (const auto& mtp : member.assignment) {
                const auto committed = offsetStore_->fetch(group, mtp);
                pc::Subscribe msg;
                msg.sessionId      = sessionId;
                msg.correlationId  = corrBase++;
                msg.topicId        = kTopicId;
                msg.partition      = mtp.partition;
                msg.partitionKnown = 1;
                msg.fromOffset     = committed.has_value() ? committed->offset : 0;
                msg.isolation      = 0;
                uint8_t buf[kBufSz]{};
                const auto n = msg.encode(buf);
                const std::size_t before = capturedFrames(sessionId).size();
                gw_->applySubscribeForTest(buf, n);
                const auto frames = capturedFrames(sessionId);
                for (std::size_t i = before; i < frames.size(); ++i) {
                    const auto r = pc::SubscribeResponse::decode(frames[i].bytes.data(), frames[i].bytes.size());
                    if (r && r->correlationId == msg.correlationId && r->errorCode == 0) ids.push_back(r->subscriptionId);
                }
            }
        }
        return ids;
    }
    void unsubscribe(int64_t sessionId, int64_t subscriptionId) {
        pc::Unsubscribe msg;
        msg.sessionId      = sessionId;
        msg.correlationId  = 0;
        msg.subscriptionId = subscriptionId;
        uint8_t buf[kBufSz]{};
        const auto n = msg.encode(buf);
        gw_->applyUnsubscribeForTest(buf, n);
    }

    // -----------------------------------------------------------------------
    // Drive service_->doWork until predicate or cap.
    // -----------------------------------------------------------------------
    bool driveUntil(std::function<bool()> pred, int cap = kCap) {
        for (int i = 0; i < cap && !pred(); ++i) {
            clock_ += kStepNs;
            service_->doWork(clock_);
            gw_->doWork(clock_);
        }
        return pred();
    }

    // -----------------------------------------------------------------------
    // Drive pushSubscriptions alone (via gw_->doWork) N times.
    // -----------------------------------------------------------------------
    void pumpPush(int n = 30) {
        for (int i = 0; i < n; ++i) {
            clock_ += kStepNs;
            gw_->doWork(clock_);
        }
    }

    // -----------------------------------------------------------------------
    // Get all captured Records frames for a session.
    // -----------------------------------------------------------------------
    std::vector<EgressFrame> capturedFrames(int64_t sid) {
        std::lock_guard lk(frameMx_);
        auto it = captured_.find(sid);
        if (it == captured_.end()) return {};
        return it->second;
    }

    // -----------------------------------------------------------------------
    // Get all captured control frames (Assign) for a session.
    // -----------------------------------------------------------------------
    std::vector<EgressFrame> controlFrames(int64_t sid) {
        std::lock_guard lk(frameMx_);
        auto it = controlFrames_.find(sid);
        if (it == controlFrames_.end()) return {};
        return it->second;
    }

    // -----------------------------------------------------------------------
    // Clear captured frames for a session.
    // -----------------------------------------------------------------------
    void clearFrames(int64_t sid) {
        std::lock_guard lk(frameMx_);
        captured_.erase(sid);
        controlFrames_.erase(sid);
    }

    // Members
    TempDir aeronDir_;
    TempDir topicDir_;
    TempDir offsetDir_;
    rune::storage::Log offsetLog_;

    std::unique_ptr<EmbeddedAeronDriver>             aeron_drv_;
    std::unique_ptr<rune::gateway::TopicRegistry>    topicReg_;
    std::unique_ptr<NullTransport>                   nullTransport_;
    std::unique_ptr<rune::node::ReplicaRegistry>     replicaReg_;
    std::vector<PartitionReplica*>                   replicaRawPtrs_; // non-owning (registry owns)
    std::unique_ptr<PartitionReplica>                offsetReplica_;
    std::unique_ptr<ReplicatedOffsetStore>           offsetStore_;
    std::unique_ptr<rune::gateway::ClientGateway>    gw_;
    std::unique_ptr<GroupCoordinationService>        service_;

    int64_t clock_ = kT0Ns;

    std::mutex frameMx_;
    std::map<int64_t, std::vector<EgressFrame>> captured_;       // Records frames
    std::map<int64_t, std::vector<EgressFrame>> controlFrames_;  // Assign frames
};

// ===========================================================================
// Test: EachPartitionToExactlyOneMember
//
// 2 members join a 2-partition topic → SERVICE drives to Stable → pushSubscriptions
// pumped.  Assert:
//   - Each member receives Records for exactly ONE partition.
//   - No partition is owned by >1 member (single-owner).
//   - Union of delivered partitions = {0, 1} (all produced).
// ===========================================================================
TEST_F(CG4DeliveryFixture, EachPartitionToExactlyOneMember) {
    const std::string group = "g-cg4d-exact-owner";

    // Inject test sessions (null egress → captured via onEgressFrame).
    const int64_t sess1 = injectSession();
    const int64_t sess2 = injectSession();

    // Phase 1: initial joins → both members get assigned IDs.
    const std::string m1 = doJoin(sess1, 1, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 2, group);
    ASSERT_FALSE(m2.empty());

    EXPECT_EQ(service_->coordinator(group).state(), GroupState::PreparingRebalance);

    // Phase 2: re-joins with assigned memberIds.
    doJoin(sess1, 3, group, m1);
    doJoin(sess2, 4, group, m2);

    // Drive to Stable.  The SERVICE (not the test) calls addGroupSubscription +
    // sends Assign frames when it reaches Stable.
    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Must reach Stable after both members re-join";

    // SyncGroup for each member; then each subscribes explicitly (redesign:
    // the coordinator creates no subscription).
    EXPECT_EQ(doSync(sess1, 5, group, m1), 0) << "m1 SyncGroup must succeed";
    EXPECT_EQ(doSync(sess2, 6, group, m2), 0) << "m2 SyncGroup must succeed";
    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "SyncGroup must not create subscriptions";
    EXPECT_EQ(subscribeAssigned(sess1, group, m1, 100).size(), 1u);
    EXPECT_EQ(subscribeAssigned(sess2, group, m2, 200).size(), 1u);
    EXPECT_EQ(gw_->groupMemberSubCount(sess1), 0u) << "no coordinator-owned group subs";
    EXPECT_EQ(gw_->subscriptionCount(), 2u);

    // Pump pushSubscriptions to deliver Records.
    pumpPush(50);

    // Collect delivered partitions per member.
    const auto frames1 = capturedFrames(sess1);
    const auto frames2 = capturedFrames(sess2);

    // Records frames have templateId = Records.
    std::set<int32_t> partitions1, partitions2;
    for (const auto& f : frames1) {
        auto dec = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (dec) partitions1.insert(dec->partition);
    }
    for (const auto& f : frames2) {
        auto dec = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (dec) partitions2.insert(dec->partition);
    }

    EXPECT_FALSE(partitions1.empty()) << "m1 must receive Records";
    EXPECT_FALSE(partitions2.empty()) << "m2 must receive Records";

    // Each member received exactly one partition.
    EXPECT_EQ(partitions1.size(), 1u) << "m1 must receive records for exactly 1 partition";
    EXPECT_EQ(partitions2.size(), 1u) << "m2 must receive records for exactly 1 partition";

    // No overlap (single-owner).
    std::set<int32_t> overlap;
    std::set_intersection(partitions1.begin(), partitions1.end(),
                          partitions2.begin(), partitions2.end(),
                          std::inserter(overlap, overlap.begin()));
    EXPECT_TRUE(overlap.empty()) << "No partition may be owned by >1 member (dual-owner violation)";

    // Union = {0, 1} (all partitions covered).
    std::set<int32_t> allOwned;
    allOwned.insert(partitions1.begin(), partitions1.end());
    allOwned.insert(partitions2.begin(), partitions2.end());
    EXPECT_EQ(allOwned.size(), 2u) << "Both partitions must be assigned";
    EXPECT_TRUE(allOwned.count(0) && allOwned.count(1))
        << "Partitions 0 and 1 must both appear in the delivery union";
}

// ===========================================================================
// Test: ResumeFromCommittedOffset
//
// Commit offset 5 for (g, T:p0) BEFORE any member joins.
// 2 members join → Stable → pushSubscriptions pumped.
// Assert: the member who owns p0 receives Records with baseOffset >= 5 only
// (actual delivered baseOffsets verified from egress capture).
// ===========================================================================
TEST_F(CG4DeliveryFixture, ResumeFromCommittedOffset) {
    const std::string group = "g-cg4d-resume";
    const TopicPartition tp0{std::string(kTopicName), 0};

    // Commit offset 5 for p0 BEFORE join.
    offsetStore_->commit(group, tp0, /*offset=*/5, /*meta=*/"", clock_);
    ASSERT_TRUE(offsetStore_->fetch(group, tp0).has_value());
    ASSERT_EQ(offsetStore_->fetch(group, tp0)->offset, int64_t{5});

    const int64_t sess1 = injectSession();
    const int64_t sess2 = injectSession();

    const std::string m1 = doJoin(sess1, 10, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 11, group);
    ASSERT_FALSE(m2.empty());
    doJoin(sess1, 12, group, m1);
    doJoin(sess2, 13, group, m2);

    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable);

    // SyncGroup for each member, then the explicit subscriptions from the
    // committed offset (the member's OffsetFetch answer).
    EXPECT_EQ(doSync(sess1, 14, group, m1), 0) << "m1 SyncGroup must succeed";
    EXPECT_EQ(doSync(sess2, 15, group, m2), 0) << "m2 SyncGroup must succeed";
    EXPECT_EQ(subscribeAssigned(sess1, group, m1, 100).size(), 1u);
    EXPECT_EQ(subscribeAssigned(sess2, group, m2, 200).size(), 1u);

    // Determine which member owns p0 (via coordinator assignment).
    const auto& coord = service_->coordinator(group);
    int64_t ownerSess = -1;
    int64_t otherSess = -1;
    for (const auto& member : coord.members()) {
        for (const auto& mtp : member.assignment) {
            if (mtp.partition == 0) {
                ownerSess = (member.memberId == m1) ? sess1 : sess2;
                otherSess = (ownerSess == sess1)   ? sess2 : sess1;
            }
        }
    }
    ASSERT_GE(ownerSess, 0) << "Partition 0 must be assigned to some member";

    // Pump push delivery.
    pumpPush(50);

    // Collect baseOffsets delivered to the p0 owner.
    const auto frames = capturedFrames(ownerSess);
    std::vector<int64_t> baseOffsets;
    for (const auto& f : frames) {
        auto dec = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (dec && dec->partition == 0) {
            baseOffsets.push_back(dec->baseOffset);
        }
    }

    ASSERT_FALSE(baseOffsets.empty())
        << "Owner of p0 must receive Records after committed-offset resume";

    // All delivered baseOffsets must be >= 5 (resumed from committed offset).
    for (int64_t bo : baseOffsets) {
        EXPECT_GE(bo, int64_t{5})
            << "Owner of p0: delivered baseOffset=" << bo
            << " must be >= committed offset 5 (resume-from-committed violated)";
    }

    // The other member (p1, no commit) must have received Records starting from 0.
    const auto framesOther = capturedFrames(otherSess);
    bool gotP1From0 = false;
    for (const auto& f : framesOther) {
        auto dec = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (dec && dec->partition == 1 && dec->baseOffset == 0) {
            gotP1From0 = true;
        }
    }
    EXPECT_TRUE(gotP1From0)
        << "Non-committed partition p1 must start delivery from baseOffset=0";
}

// ===========================================================================
// Test: EagerRevokeAndReassign
//
// 2 members reach Stable → SERVICE adds group subs + sends Assign frames.
// 3rd member joins → SERVICE sends empty Assign (revoke) to existing members
// and drops their group subs.  After second Stable, still single-owner.
// ===========================================================================
TEST_F(CG4DeliveryFixture, EagerRevokeAndReassign) {
    const std::string group = "g-cg4d-eager-revoke";

    const int64_t sess1 = injectSession();
    const int64_t sess2 = injectSession();

    // Phase 1: 2 members → Stable.
    const std::string m1 = doJoin(sess1, 20, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 21, group);
    ASSERT_FALSE(m2.empty());
    doJoin(sess1, 22, group, m1);
    doJoin(sess2, 23, group, m2);

    bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Phase 1 must reach Stable";

    // SyncGroup for each member, then the members' explicit subscriptions.
    EXPECT_EQ(doSync(sess1, 28, group, m1), 0) << "m1 Phase1 SyncGroup must succeed";
    EXPECT_EQ(doSync(sess2, 29, group, m2), 0) << "m2 Phase1 SyncGroup must succeed";
    const auto ids1 = subscribeAssigned(sess1, group, m1, 100);
    const auto ids2 = subscribeAssigned(sess2, group, m2, 200);
    EXPECT_EQ(gw_->subscriptionCount(), 2u)
        << "Phase 1: 2 members, 2 partitions → 2 member subscriptions";

    // Phase 2: 3rd member joins → triggers rebalance.
    clearFrames(sess1);
    clearFrames(sess2);
    const int64_t sess3 = injectSession();
    const std::string m3 = doJoin(sess3, 24, group);
    ASSERT_FALSE(m3.empty());

    // After the 3rd join the coordinator revokes eagerly: an empty Assign to
    // every existing member, which (the client) unsubscribes its partitions.
    const bool revokeSent = driveUntil([&] {
        return !controlFrames(sess1).empty() && !controlFrames(sess2).empty();
    }, 50);
    EXPECT_TRUE(revokeSent) << "After 3rd member joins, each existing member must get a revoke Assign";
    for (const int64_t id : ids1) unsubscribe(sess1, id);
    for (const int64_t id : ids2) unsubscribe(sess2, id);
    EXPECT_EQ(gw_->subscriptionCount(), 0u) << "the members' Unsubscribes drop their subscriptions";

    // All 3 members re-join for Phase 2.
    doJoin(sess1, 25, group, m1);
    doJoin(sess2, 26, group, m2);
    doJoin(sess3, 27, group, m3);

    stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Phase 2 must reach Stable after 3rd member joins";

    // SyncGroup for all 3 members in Phase 2.
    EXPECT_EQ(doSync(sess1, 30, group, m1), 0) << "m1 Phase2 SyncGroup must succeed";
    EXPECT_EQ(doSync(sess2, 31, group, m2), 0) << "m2 Phase2 SyncGroup must succeed";
    EXPECT_EQ(doSync(sess3, 32, group, m3), 0) << "m3 Phase2 SyncGroup must succeed";

    // All partitions must be assigned exactly once.
    const auto& coord2 = service_->coordinator(group);
    EXPECT_EQ(coord2.memberCount(), 3u);
    std::set<int32_t> allAssigned;
    for (const auto& member : coord2.members()) {
        for (const auto& mtp : member.assignment) {
            EXPECT_EQ(allAssigned.count(mtp.partition), 0u)
                << "Partition " << mtp.partition << " assigned to >1 member (dual-owner)";
            allAssigned.insert(mtp.partition);
        }
    }
    EXPECT_EQ(allAssigned.size(), 2u)
        << "Both partitions must be assigned after phase-2 Stable";
}

// ===========================================================================
// Test: AssignFramesDelivered
//
// Assert that:
//  (a) On reaching Stable, each member's egress receives an Assign frame with
//      its partition set (decoded from captured control frames).
//  (b) On entering PreparingRebalance (3rd member joins), each existing member
//      receives an empty Assign frame (revoke).
// ===========================================================================
TEST_F(CG4DeliveryFixture, AssignFramesDelivered) {
    const std::string group = "g-cg4d-assign-frames";

    const int64_t sess1 = injectSession();
    const int64_t sess2 = injectSession();

    // Phase 1: 2 members → Stable.
    const std::string m1 = doJoin(sess1, 30, group);
    ASSERT_FALSE(m1.empty());
    const std::string m2 = doJoin(sess2, 31, group);
    ASSERT_FALSE(m2.empty());
    doJoin(sess1, 32, group, m1);
    doJoin(sess2, 33, group, m2);

    const bool stable = driveUntil([&] {
        return service_->coordinator(group).state() == GroupState::Stable;
    });
    ASSERT_TRUE(stable) << "Must reach Stable";

    // Each member must have received at least one Assign frame (Stable assign).
    const auto cf1 = controlFrames(sess1);
    const auto cf2 = controlFrames(sess2);

    ASSERT_FALSE(cf1.empty()) << "m1 must receive an Assign control frame on Stable";
    ASSERT_FALSE(cf2.empty()) << "m2 must receive an Assign control frame on Stable";

    // Find the Assign frame with a non-empty assignment (the Stable assign).
    bool m1GotAssign = false;
    bool m2GotAssign = false;
    for (const auto& f : cf1) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec && dec->assignment.size() == 1u) { m1GotAssign = true; }
    }
    for (const auto& f : cf2) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec && dec->assignment.size() == 1u) { m2GotAssign = true; }
    }
    EXPECT_TRUE(m1GotAssign) << "m1 must receive a non-empty Assign (partition set) on Stable";
    EXPECT_TRUE(m2GotAssign) << "m2 must receive a non-empty Assign (partition set) on Stable";

    // Verify single-owner: extract partitions from Assign frames.
    std::set<int32_t> assignedToM1, assignedToM2;
    for (const auto& f : cf1) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec) for (std::size_t i = 0; i < dec->assignment.size(); ++i)
            assignedToM1.insert(dec->assignment[i].partition);
    }
    for (const auto& f : cf2) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec) for (std::size_t i = 0; i < dec->assignment.size(); ++i)
            assignedToM2.insert(dec->assignment[i].partition);
    }
    // No overlap between final assignments.
    std::set<int32_t> overlap;
    std::set_intersection(assignedToM1.begin(), assignedToM1.end(),
                          assignedToM2.begin(), assignedToM2.end(),
                          std::inserter(overlap, overlap.begin()));
    EXPECT_TRUE(overlap.empty())
        << "No partition may be assigned to >1 member in the Assign frames";

    // Phase 2: 3rd member joins → each existing member must get an empty Assign (revoke).
    clearFrames(sess1);
    clearFrames(sess2);

    const int64_t sess3 = injectSession();
    const std::string m3 = doJoin(sess3, 34, group);
    ASSERT_FALSE(m3.empty());

    // Drive until revoke fires (subscriptionCount drops to 0 and control frames appear).
    driveUntil([&] {
        return !controlFrames(sess1).empty() || !controlFrames(sess2).empty();
    }, 100);

    // Each member (m1, m2) must have received an empty Assign frame (revoke).
    bool m1GotRevoke = false;
    bool m2GotRevoke = false;
    for (const auto& f : controlFrames(sess1)) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec && dec->assignment.size() == 0u) m1GotRevoke = true;
    }
    for (const auto& f : controlFrames(sess2)) {
        auto dec = decodeAssign(f.bytes.data(), f.bytes.size());
        if (dec && dec->assignment.size() == 0u) m2GotRevoke = true;
    }
    EXPECT_TRUE(m1GotRevoke) << "m1 must receive an empty Assign (revoke) when 3rd member joins";
    EXPECT_TRUE(m2GotRevoke) << "m2 must receive an empty Assign (revoke) when 3rd member joins";
}

// ===========================================================================
// Pure encode/decode unit tests — kept from original (no orchestration).
// ===========================================================================

// Redesign regression: SyncGroup must NOT create the member's subscriptions
// (they pushed before the member knew its position — records dropped, cursor
// ahead: stalled / lost partitions).  RED on main: one group sub per member.
TEST_F(CG4DeliveryFixture, SyncGroupCreatesNoSubscriptionRegression) {
    const std::string group = "g-cg-no-auto-sub";
    const int64_t sess1 = injectSession();
    const std::string m1 = doJoin(sess1, 1, group);
    ASSERT_FALSE(m1.empty());
    doJoin(sess1, 2, group, m1);
    ASSERT_TRUE(driveUntil([&] { return service_->coordinator(group).state() == GroupState::Stable; }));
    ASSERT_EQ(doSync(sess1, 3, group, m1), 0);
    EXPECT_EQ(gw_->subscriptionCount(), 0u);
    EXPECT_EQ(gw_->groupMemberSubCount(sess1), 0u);
    pumpPush(20);
    for (const auto& f : capturedFrames(sess1)) {
        EXPECT_FALSE(pc::Records::decode(f.bytes.data(), f.bytes.size()).has_value())
            << "records pushed before the member subscribed";
    }
}

// Redesign regression: session ids and subscription ids are cluster-unique —
// the node id is in the high bits ((nodeId + 1) << 40), so two nodes never
// hand a client the same id (a client holding one session per node
// demultiplexes Records by subscriptionId).  RED on main: small per-node
// counters (1, 2, ...) collided across nodes.
TEST_F(CG4DeliveryFixture, SessionAndSubscriptionIdsCarryTheNodeIdRegression) {
    const int64_t prefix = int64_t{2} << 40;  // (thisNodeId 1 + 1) << 40
    const int64_t sess1  = injectSession();
    EXPECT_EQ(sess1 >> 40, 2) << "session id " << sess1 << " lacks the node prefix";
    EXPECT_GT(sess1, 0);
    pc::Subscribe msg;
    msg.sessionId      = sess1;
    msg.correlationId  = 7;
    msg.topicId        = kTopicId;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    uint8_t buf[kBufSz]{};
    gw_->applySubscribeForTest(buf, msg.encode(buf));
    int64_t subId = 0;
    for (const auto& f : capturedFrames(sess1)) {
        const auto r = pc::SubscribeResponse::decode(f.bytes.data(), f.bytes.size());
        if (r && r->correlationId == 7) subId = r->subscriptionId;
    }
    EXPECT_EQ(subId & ~((int64_t{1} << 40) - 1), prefix) << "subscription id " << subId << " lacks the node prefix";
    EXPECT_GT(subId & ((int64_t{1} << 40) - 1), 0);
}

// Only the owning session may Unsubscribe a subscription (a stale client
// episode or another client naming the id is ignored).  RED on main: any
// session's Unsubscribe removed it.
TEST_F(CG4DeliveryFixture, UnsubscribeFromAnotherSessionIsIgnoredRegression) {
    const int64_t owner = injectSession();
    const int64_t other = injectSession();
    pc::Subscribe msg;
    msg.sessionId      = owner;
    msg.correlationId  = 9;
    msg.topicId        = kTopicId;
    msg.partition      = 1;
    msg.partitionKnown = 1;
    uint8_t buf[kBufSz]{};
    gw_->applySubscribeForTest(buf, msg.encode(buf));
    ASSERT_EQ(gw_->subscriptionCount(), 1u);
    int64_t subId = 0;
    for (const auto& f : capturedFrames(owner)) {
        const auto r = pc::SubscribeResponse::decode(f.bytes.data(), f.bytes.size());
        if (r && r->correlationId == 9) subId = r->subscriptionId;
    }
    ASSERT_NE(subId, 0);
    unsubscribe(other, subId);
    EXPECT_EQ(gw_->subscriptionCount(), 1u) << "another session removed the subscription";
    unsubscribe(owner, subId);
    EXPECT_EQ(gw_->subscriptionCount(), 0u);
}

// CG routing round 3 (MAJOR-1): the gateway's Records pushes carry the
// creating Subscribe's correlationId.  RED: 0.
TEST_F(CG4DeliveryFixture, GatewayRecordsCarryTheSubscribeCorrelationIdRegression) {
    const int64_t sess = injectSession();
    pc::Subscribe msg;
    msg.sessionId      = sess;
    msg.correlationId  = 0x2000000000000777LL;
    msg.topicId        = kTopicId;
    msg.partition      = 0;
    msg.partitionKnown = 1;
    msg.fromOffset     = 0;
    uint8_t buf[kBufSz]{};
    gw_->applySubscribeForTest(buf, msg.encode(buf));
    pumpPush(30);
    int records = 0;
    for (const auto& f : capturedFrames(sess)) {
        const auto r = pc::Records::decode(f.bytes.data(), f.bytes.size());
        if (!r) continue;
        ++records;
        EXPECT_EQ(r->correlationId, 0x2000000000000777LL);
    }
    EXPECT_GT(records, 0) << "precondition: the partition has records";
}

// CG routing round 3 (MAJOR-2): a JoinGroup from a client below version 3 is
// refused (ErrorCode::ProtocolVersion, non-retriable) — that client relies on
// coordinator-created subscriptions this broker no longer makes.  RED: joined.
TEST_F(CG4DeliveryFixture, JoinGroupFromAnOldClientIsRefusedRegression) {
    const int64_t sess = gw_->injectTestSession(clock_, /*clientVersion=*/2);
    pc::JoinGroup msg;
    msg.sessionId        = sess;
    msg.correlationId    = 1;
    msg.sessionTimeoutMs = 5000;
    const std::string group = "g-old-client";
    msg.groupId = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(group.data()), group.size());
    std::array<int32_t, 1> topics{kTopicId};
    uint8_t req[kBufSz]{}, out[kBufSz]{};
    const auto reqLen = msg.encode(req, std::span<const int32_t>(topics));
    const auto outLen = service_->handleJoinGroup(req, reqLen, rune::protocol::client::kClientVersion, clock_, out, kBufSz);
    const auto resp = pc::JoinGroupResponse::decode(out, outLen);
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->errorCode, static_cast<int16_t>(pc::ErrorCode::ProtocolVersion));
    EXPECT_TRUE(resp->memberId.empty());
    // A current client joins.
    const int64_t cur = injectSession();
    EXPECT_FALSE(doJoin(cur, 2, group).empty());
}

TEST(CG4_AssignFrameEncodeDecode, FullAssign) {
    static constexpr std::size_t kBufSz2 = 512;
    uint8_t buf[kBufSz2]{};

    pc::Assign frame;
    frame.sessionId     = 77;
    frame.correlationId = 0;
    frame.generationId  = 3;

    std::array<pc::TopicPartition32, 2> asgn{{
        {/*topicId=*/0, /*partition=*/0},
        {/*topicId=*/0, /*partition=*/1}
    }};
    const std::size_t encoded = frame.encode(buf, std::span<const pc::TopicPartition32>(asgn));
    ASSERT_GT(encoded, 0u);
    ASSERT_LE(encoded, kBufSz2);

    const auto decoded = pc::Assign::decode(buf, encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,    int64_t{77});
    EXPECT_EQ(decoded->generationId, int32_t{3});
    EXPECT_EQ(decoded->assignment.size(), std::size_t{2});

    std::set<int32_t> partitions;
    for (std::size_t i = 0; i < decoded->assignment.size(); ++i)
        partitions.insert(decoded->assignment[i].partition);
    EXPECT_TRUE(partitions.count(0));
    EXPECT_TRUE(partitions.count(1));
}

TEST(CG4_AssignFrameEncodeDecode, EmptyAssignIsRevoke) {
    static constexpr std::size_t kBufSz2 = 64;
    uint8_t buf[kBufSz2]{};

    pc::Assign frame;
    frame.sessionId     = 99;
    frame.correlationId = 0;
    frame.generationId  = 7;

    const std::size_t encoded = frame.encode(buf, std::span<const pc::TopicPartition32>{});
    ASSERT_GT(encoded, 0u);
    ASSERT_LE(encoded, kBufSz2);

    const auto decoded = pc::Assign::decode(buf, encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->sessionId,    int64_t{99});
    EXPECT_EQ(decoded->generationId, int32_t{7});
    EXPECT_EQ(decoded->assignment.size(), std::size_t{0})
        << "Empty assignment = revoke all";
}
