// WorkerWriteGrantsTest.cpp - authentication Task 13 (spec §6, D30) / issue #259: the shard worker's WRITE tables and
// the sessions bound to them.
// A REAL invoker driver over aeron:ipc (the ShardWorkerAckTest fixture): a client produces straight to the worker's
// ingress; the answer comes back on the session's own publication.
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/SessionAnnouncer.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kCap = 200'000;
constexpr int32_t kIngress = 7401, kEgress = 7402;
constexpr const char* kEgressChannel = "aeron:ipc?term-length=65536";

class NullTransport final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};

struct Fixture {
    fs::path dir, adir;
    std::unique_ptr<rune::storage::Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<WorkerIngress> ing;
    std::unique_ptr<Worker> worker;   // dies BEFORE the Aeron client
    std::shared_ptr<aeron::ExclusivePublication> producePub;
    std::shared_ptr<aeron::Subscription> egress;
    std::map<int64_t, pc::ProduceResponse> acks;
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                         aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o), static_cast<std::size_t>(l)))
            acks[r->correlationId] = *r;
    }};
    int64_t now = 1'000;

    Fixture() {
        worker = std::make_unique<Worker>(0, 1 << 16, 1 << 16, 64, 64);
        static std::atomic<int> seq{0};
        const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(seq++);
        dir  = fs::temp_directory_path() / ("rune_wwg_log_" + tag);
        adir = fs::temp_directory_path() / ("rune_wwg_aeron_" + tag);
        fs::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string(), 64LL * 1024 * 1024);
        log->recover();
        PartitionConfig c; c.topicId = 1; c.selfId = 1; c.minIsr = 1;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, std::vector<rune::replication::NodeId>{1},
                                                     std::vector<rune::replication::NodeId>{1}, log.get(), &t);
        EmbeddedDriverConfig dc; dc.dir = adir.string(); dc.termBufferBytes = 64 * 1024;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        ing = std::make_unique<WorkerIngress>(client, "aeron:ipc", kIngress, drv->pumpWith(client.get()));
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setWorkerIngress(ing.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client.get());
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress && producePub && producePub->isConnected()); ++i) {
            pump();
            if (!egress) egress = client->findSubscription(sreg);
            if (!producePub) producePub = client->findExclusivePublication(preg);
        }
    }
    ~Fixture() {
        worker.reset();
        producePub.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() { pump(); worker->doWork(now += 10'000); egress->poll(asmb.handler(), 64); }
    void steps(int n) { for (int i = 0; i < n; ++i) step(); }

    void announce(int64_t sid, bool grantAll) {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = sid;
        a.egressStreamId = kEgress; a.egressChannel = kEgressChannel; a.grantAll = grantAll;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    void close(int64_t sid) {
        WorkerOp c; c.type = WorkerOp::Type::SessionClose; c.sessionId = sid;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(c)));
    }
    std::map<int64_t, int64_t> tableOf_;   // the tests' sessions each own one table (ids < kMaxWritePermTables)
    int64_t tableOf(int64_t sid) { return tableOf_.try_emplace(sid, static_cast<int64_t>(tableOf_.size())).first->second; }
    /// One chunk of session `sid`'s own table, as the PermissionShipper builds it; the last one is followed by the
    /// session's bind to that table (epoch), as the gateway ships a login / refresh.
    void chunk(int64_t sid, uint64_t epoch, uint64_t first, uint64_t total, const std::vector<uint64_t>& words, bool last,
               bool grantAll = false) {
        tableChunk(tableOf(sid), first, total, words, last, grantAll);
        if (last) bind(sid, tableOf(sid), epoch);
    }
    void tableChunk(int64_t table, uint64_t first, uint64_t total, const std::vector<uint64_t>& words, bool last,
                    bool grantAll = false) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = table;
        o.nextOffset = static_cast<int64_t>(first); o.hwBound = static_cast<int64_t>(total);
        o.isolation = static_cast<uint8_t>(kPermFlagTable | (last ? kPermFlagLast : 0u) | (grantAll ? kPermFlagGrantAll : 0u));
        o.permWords = words.data(); o.permWordCount = words.size();
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    void bind(int64_t sid, int64_t table, uint64_t epoch, uint64_t owner = 7) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = sid; o.nextOffset = table;
        o.correlationId = static_cast<int64_t>(epoch); o.producerId = static_cast<int64_t>(owner);
        o.isolation = kPermFlagBind | kPermFlagLast;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    void patch(int64_t table, uint64_t first, uint64_t total, const std::vector<uint64_t>& words) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = table;
        o.nextOffset = static_cast<int64_t>(first); o.hwBound = static_cast<int64_t>(total);
        o.isolation = kPermFlagPatch | kPermFlagLast; o.permWords = words.data(); o.permWordCount = words.size();
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    void dropTable(int64_t table) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = table;
        o.isolation = kPermFlagDrop | kPermFlagLast;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    /// Issue #603: the control thread's "`__security` view behind" flag, as the PermissionShipper ships it.
    void securityBehind(bool on) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = 0;
        o.nextOffset = on ? 1 : 0; o.isolation = kPermFlagSecurityBehind | kPermFlagLast;
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    void produce(int64_t sid, int64_t corr, int32_t topicId = 1) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = sid; m.correlationId = corr; m.topicId = topicId; m.partition = 0;
        m.partitionKnown = 1; m.acks = 0; m.producerId = -1; m.producerEpoch = 0; m.baseSequence = -1;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        for (int i = 0; i < kCap && producePub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i) pump();
    }
    bool awaitAck(int64_t corr) {
        for (int i = 0; i < kCap && !acks.count(corr); ++i) step();
        return acks.count(corr) == 1;
    }
    [[nodiscard]] const WriteGrantSlot* slot(int64_t sid) const { return worker->ackSinkForTest()->writeGrants(sid); }
};

std::vector<uint64_t> bitsFor(std::initializer_list<int32_t> topics, std::size_t words) {
    std::vector<uint64_t> v(words, 0);
    for (const int32_t t : topics) v[static_cast<std::size_t>(t) >> 6] |= uint64_t{1} << (t & 63);
    return v;
}
// The sink's session hash (kMaxSessions = 4096 -> 12 bits), to find sessions that collide on one home slot.
std::size_t homeSlot(int64_t sid) { return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> 52); }
}  // namespace

TEST(WorkerWriteGrantRegression, UnauthenticatedSessionProduceToWorkerIsDeniedRegression) {
    Fixture f;
    f.announce(5, /*grantAll=*/false);                     // Enabled mode: announced, not yet authenticated
    f.produce(5, 41);
    ASSERT_TRUE(f.awaitAck(41));
    EXPECT_EQ(f.acks[41].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    EXPECT_EQ(f.log->logEndOffset(), 0) << "nothing is appended for a denied session";
    EXPECT_EQ(f.worker->workerNotAuthorizedForTest(), 1u);
}

// Task 14 ruling (Task 13 R2 reversed): the default is DENY.  An announcer that forgot to say anything - no grantAll, no
// shipped bitmap - must not open the log.
TEST(WorkerWriteGrantRegression, ASessionAnnouncedWithDefaultFlagsAndNoShippedBitmapIsDeniedRegression) {
    EXPECT_FALSE(WorkerOp{}.grantAll) << "the WorkerOp default is deny";
    EXPECT_FALSE(SessionAnnouncer{}.grantAll()) << "the announcer default is deny";
    Fixture f;
    {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 9;   // nothing else set: every default
        a.egressStreamId = kEgress; a.egressChannel = kEgressChannel;
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(a)));
    }
    f.produce(9, 51);
    ASSERT_TRUE(f.awaitAck(51));
    EXPECT_EQ(f.acks[51].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    EXPECT_EQ(f.log->logEndOffset(), 0);
}

TEST(WorkerWriteGrantRegression, GrantedTopicIsAppendedAndAnUngrantedOneIsNot) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), /*last=*/true);
    f.produce(5, 1, /*topicId=*/1);
    ASSERT_TRUE(f.awaitAck(1));
    EXPECT_EQ(f.acks[1].errorCode, 0);
    EXPECT_EQ(f.log->logEndOffset(), 1);
    f.produce(5, 2, /*topicId=*/2);                        // not granted (and not even a topic of this worker)
    ASSERT_TRUE(f.awaitAck(2));
    EXPECT_EQ(f.acks[2].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    EXPECT_EQ(f.log->logEndOffset(), 1);
}

TEST(WorkerWriteGrantRegression, ChunkedUpdateIsInstalledAtomicallyOnTheLastChunk) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), true);            // the old table: topic 1
    f.steps(8);
    ASSERT_NE(f.slot(5), nullptr);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1));
    // The new table: 1200 words, topic 1 revoked, a topic in word 1100 granted; three chunks.
    constexpr int32_t kFar = 1100 * 64 + 3;
    std::vector<uint64_t> all = bitsFor({kFar}, 1200);
    const std::vector<uint64_t> c1(all.begin(), all.begin() + 512), c2(all.begin() + 512, all.begin() + 1024),
                                c3(all.begin() + 1024, all.end());
    f.chunk(5, 2, 0, 1200, c1, false);
    f.chunk(5, 2, 512, 1200, c2, false);
    f.steps(8);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1)) << "after chunk 2 the OLD table still answers";
    EXPECT_FALSE(allowsWrite(*f.slot(5), kFar));
    EXPECT_EQ(f.slot(5)->epoch, 1u);
    f.chunk(5, 2, 1024, 1200, c3, true);
    f.steps(8);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1)) << "after the last chunk the NEW table answers";
    EXPECT_TRUE(allowsWrite(*f.slot(5), kFar));
    EXPECT_EQ(f.slot(5)->epoch, 2u);
}

TEST(WorkerWriteGrantRegression, AnOutOfOrderChunkIsDroppedAndNeverInstallsAPartialTable) {
    Fixture f;
    f.announce(5, false);
    f.bind(5, f.tableOf(5), 1);                              // bound to its (still empty) table
    f.tableChunk(f.tableOf(5), 0, 1024, bitsFor({1}, 512), false);
    f.tableChunk(f.tableOf(5), 768, 1024, bitsFor({}, 256), true);   // skips words 512..767: must not install
    f.steps(8);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1)) << "a partial table is never visible";
    EXPECT_EQ(f.slot(5)->epoch, 1u);
}

TEST(WorkerWriteGrantRegression, GrantAllAllowsEveryTopic) {
    Fixture f;
    f.announce(5, /*grantAll=*/true);                       // dev / in-process mode
    f.produce(5, 1);
    ASSERT_TRUE(f.awaitAck(1));
    EXPECT_EQ(f.acks[1].errorCode, 0);
    f.steps(2);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 0));
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1'000'000));
    // A later grantAll=false update (a login shipped a real table) narrows it again.
    f.chunk(5, 3, 0, 1, bitsFor({}, 1), true, /*grantAll=*/false);
    f.steps(4);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1'000'000));
}

TEST(WorkerWriteGrantRegression, ASecondAnnounceOfAKnownSessionKeepsItsGrants) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), true);
    f.steps(4);
    f.announce(5, false);                                   // the crypto-key second wave re-announces the session
    f.steps(4);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1)) << "a re-announce must not wipe an authenticated session's grants";
}

TEST(WorkerWriteGrantRegression, SlotsMoveWithBackwardShiftDeletion) {
    Fixture f;
    // Two sessions with the same home slot: the second sits one past it; erasing the first shifts it back.
    int64_t a = 100, b = 0;
    for (int64_t c = 101; c < 200'000 && b == 0; ++c) if (homeSlot(c) == homeSlot(a)) b = c;
    ASSERT_NE(b, 0);
    f.announce(a, false);
    f.announce(b, false);
    f.chunk(a, 1, 0, 1, bitsFor({1}, 1), true);
    f.chunk(b, 1, 0, 1, bitsFor({2}, 1), true);
    f.steps(8);
    ASSERT_NE(f.slot(a), nullptr);
    ASSERT_NE(f.slot(b), nullptr);
    f.close(a);
    f.steps(8);
    EXPECT_EQ(f.slot(a), nullptr);
    ASSERT_NE(f.slot(b), nullptr) << "the colliding session survived the shift";
    EXPECT_TRUE(allowsWrite(*f.slot(b), 2));
    EXPECT_FALSE(allowsWrite(*f.slot(b), 1)) << "and kept ITS bits, not the erased neighbour's";
}

TEST(WorkerWriteGrantRegression, DroppedSessionLosesItsGrants) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), true);
    f.steps(4);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 1));
    f.close(5);
    f.steps(4);
    EXPECT_EQ(f.slot(5), nullptr);
    f.chunk(5, 2, 0, 1, bitsFor({1}, 1), true);             // a late chunk for the closed session installs nothing
    f.steps(4);
    EXPECT_EQ(f.slot(5), nullptr);
    f.announce(5, false);                                    // the id is reused: it starts denied
    f.steps(4);
    ASSERT_NE(f.slot(5), nullptr);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1));
}

TEST(WorkerWriteGrantRegression, PermissionStagerReusesItsBuffersAfterTheFirstUpdate) {
    // No allocation once both buffers reached their peak size: the staging buffer and the slot buffer swap.
    struct FakeSink {
        std::vector<uint64_t> bits;
        bool installWriteGrants(int64_t, std::vector<uint64_t>& w, bool, uint64_t, uint64_t) noexcept { bits.swap(w); return true; }
    } sink;
    PermissionStager st;
    std::vector<uint64_t> chunkWords(512, 7);
    auto update = [&] {
        EXPECT_EQ(st.apply(sink, 1, 0, 1024, 0, 1, reinterpret_cast<const uint8_t*>(chunkWords.data()), 4096, /*owner=*/7), PermissionChunkResult::Staged);
        EXPECT_EQ(st.apply(sink, 1, 512, 1024, kPermFlagLast, 1, reinterpret_cast<const uint8_t*>(chunkWords.data()), 4096, /*owner=*/7),
                  PermissionChunkResult::Installed);
    };
    update();
    update();                                                // both buffers now hold their peak capacity
    const uint64_t* installed = sink.bits.data();
    update();
    update();
    EXPECT_TRUE(sink.bits.data() == installed || sink.bits.capacity() >= 1024) << "buffers circulate";
    EXPECT_EQ(sink.bits.size(), 1024u);
    EXPECT_EQ(st.droppedChunks(), 0u);
}

// Issue #259: one table serves every session bound to it - a patch (a created topic a prefix grant covers) reaches all
// of them at once, a drop (the grant set's last holder left) resets every slot still pointing at it to deny, and a
// rebind moves one session without touching the others.
TEST(WorkerWriteGrantRegression, SessionsSharingATableSeeItsPatchAndItsDropRegression) {
    Fixture f;
    f.announce(5, false);
    f.announce(6, false);
    f.tableChunk(3, 0, 1, bitsFor({1}, 1), true);
    f.bind(5, 3, 1);
    f.bind(6, 3, 1);
    f.steps(8);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 1));
    ASSERT_TRUE(allowsWrite(*f.slot(6), 1));
    constexpr int32_t kNew = 130 * 64 + 5;                   // a topic created later, beyond the table's words
    f.patch(3, 130, 131, bitsFor({5}, 1));
    f.steps(8);
    EXPECT_TRUE(allowsWrite(*f.slot(5), kNew)) << "one patch, every holder";
    EXPECT_TRUE(allowsWrite(*f.slot(6), kNew));
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1)) << "the patch keeps the other words";
    f.tableChunk(4, 0, 1, bitsFor({2}, 1), true);
    f.bind(6, 4, 2);                                         // session 6 moved to another grant set
    f.dropTable(3);                                          // ... and 3's last holder left
    f.steps(8);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1)) << "a slot still on a dropped table is denied (fail closed)";
    EXPECT_TRUE(allowsWrite(*f.slot(6), 2));
    EXPECT_FALSE(allowsWrite(*f.slot(6), 1));
    EXPECT_EQ(f.slot(6)->epoch, 2u);
}

TEST(WorkerWriteGrantRegression, ABindToNoTableDeniesEverythingRegression) {
    Fixture f;
    f.announce(5, /*grantAll=*/true);
    f.steps(2);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 1));
    f.bind(5, -1, 1);
    f.steps(4);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1));
}

// ── PR #441 adversarial review (issue #259 area): the worker's table store under hostile / racing ops ──

// Lifetime: a slot still bound to a table (its session's close not yet seen by the worker) when that table is dropped and
// its id reused by ANOTHER set must never inherit the new set's WRITE.
TEST(WorkerWriteGrantRegression, AStaleSlotNeverInheritsATableIdReusedByAnotherSetAdversarialRegression) {
    Fixture f;
    f.announce(5, false);
    f.announce(6, false);
    f.tableChunk(3, 0, 1, bitsFor({1}, 1), true);
    f.bind(5, 3, 1);
    f.steps(8);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 1));
    f.dropTable(3);                                  // 5's set freed (its close is still behind)
    f.tableChunk(3, 0, 1, bitsFor({2}, 1), true);   // the id reused by another set
    f.bind(6, 3, 2);
    f.steps(8);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1));
    EXPECT_FALSE(allowsWrite(*f.slot(5), 2)) << "the stale slot was reset at the drop: never the new set's bits";
    EXPECT_TRUE(allowsWrite(*f.slot(6), 2));
}

// Input bounds: hostile SESSION_PERMISSIONS ops (ids at / beyond the store, negative ids, a patch past its total, a tail
// that is not whole words, no variant flag, an unknown session) change nothing and never crash.
TEST(WorkerWriteGrantRegression, MalformedPermissionOpsChangeNothingAdversarialRegression) {
    Fixture f;
    f.announce(5, false);
    f.tableChunk(3, 0, 1, bitsFor({1}, 1), true);
    f.bind(5, 3, 1);
    f.steps(8);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 1));
    const int64_t kMax = static_cast<int64_t>(kMaxWritePermTables);
    f.bind(5, kMax, 2);                                    // beyond the store: refused, the slot keeps table 3
    f.tableChunk(kMax, 0, 1, bitsFor({1}, 1), true);
    f.tableChunk(-7, 0, 1, bitsFor({1}, 1), true);
    f.patch(kMax, 0, 1, bitsFor({4}, 1));
    f.patch(-1, 0, 1, bitsFor({4}, 1));
    f.patch(3, 5, 2, bitsFor({4}, 1));                     // first > total
    f.patch(3, 1, 1, bitsFor({4}, 1));                     // n > total - first
    f.dropTable(kMax);
    f.dropTable(-3);
    f.bind(424242, 3, 3);                                  // unknown session
    {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = 3;
        o.isolation = kPermFlagLast;                       // no variant flag
        const auto w = bitsFor({9}, 1); o.permWords = w.data(); o.permWordCount = w.size();
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(o)));
    }
    f.steps(16);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 1)) << "nothing of it touched table 3 or the slot";
    EXPECT_FALSE(allowsWrite(*f.slot(5), 4));
    EXPECT_FALSE(allowsWrite(*f.slot(5), 9));
    EXPECT_EQ(f.slot(5)->epoch, 1u);
}

// Issue #603: while the control thread says its `__security` view is behind committed events this broker holds, the
// worker answers every ingress produce AuthUnavailable (48) - whatever the session's table says, grantAll included -
// and appends nothing; once cleared, the tables decide again.  RED (mutation): drop the check in the WRITE path.
TEST(WorkerWriteGrantRegression, WhileTheSecurityViewIsBehindEveryIngressProduceIsUnavailableRegression) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), /*last=*/true);
    f.announce(6, /*grantAll=*/true);
    f.produce(5, 1);
    ASSERT_TRUE(f.awaitAck(1));
    ASSERT_EQ(f.acks[1].errorCode, 0);
    ASSERT_EQ(f.log->logEndOffset(), 1);

    f.securityBehind(true);
    int64_t corr = 10;
    for (const int64_t sid : {int64_t{5}, int64_t{6}}) {
        for (int i = 0; i < 3; ++i, ++corr) {
            f.produce(sid, corr);
            ASSERT_TRUE(f.awaitAck(corr));
            EXPECT_EQ(f.acks[corr].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable)) << "session " << sid;
        }
    }
    f.produce(5, corr, /*topicId=*/2);   // ungranted too: the behind answer comes first (retriable, not a verdict)
    ASSERT_TRUE(f.awaitAck(corr));
    EXPECT_EQ(f.acks[corr].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    EXPECT_EQ(f.log->logEndOffset(), 1) << "nothing appended while behind";
    EXPECT_TRUE(f.worker->securityBehindForTest());
    EXPECT_EQ(f.worker->securityBehindRefusalsForTest(), 7u);

    f.securityBehind(false);
    f.produce(5, ++corr);
    ASSERT_TRUE(f.awaitAck(corr));
    EXPECT_EQ(f.acks[corr].errorCode, 0) << "served again";
    f.produce(5, ++corr, /*topicId=*/2);
    ASSERT_TRUE(f.awaitAck(corr));
    EXPECT_EQ(f.acks[corr].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized)) << "and judged by its table";
    EXPECT_EQ(f.log->logEndOffset(), 2);
    EXPECT_FALSE(f.worker->securityBehindForTest());
}

// Issue #603, a revoke landing mid-stream on the worker: the flag op arrives on the lifecycle ring between produces of
// a steady stream - every produce answered after it is 48, the log never grows after the first 48, and the revoked
// table installed behind the clear answers 45 from then on.
TEST(WorkerWriteGrantRegression, ABehindFlagLandingMidStreamStopsTheStreamAtOnceRegression) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), true);
    std::vector<int16_t> codes;
    int64_t leoAtFirstRefusal = -1;
    for (int64_t corr = 1; corr <= 60; ++corr) {
        if (corr == 20) f.securityBehind(true);
        if (corr == 40) { f.patch(f.tableOf(5), 0, 1, {0}); f.securityBehind(false); }   // the REVOKE, then the clear
        f.produce(5, corr);
        ASSERT_TRUE(f.awaitAck(corr)) << corr;
        codes.push_back(f.acks[corr].errorCode);
        if (leoAtFirstRefusal < 0 && codes.back() != 0) leoAtFirstRefusal = f.log->logEndOffset();
    }
    for (std::size_t i = 0; i < codes.size(); ++i) {
        const int16_t want = i < 19 ? 0 : i < 39 ? static_cast<int16_t>(pc::ErrorCode::AuthUnavailable)
                                                 : static_cast<int16_t>(pc::ErrorCode::NotAuthorized);
        EXPECT_EQ(codes[i], want) << "produce " << i + 1;
    }
    EXPECT_EQ(leoAtFirstRefusal, 19);
    EXPECT_EQ(f.log->logEndOffset(), 19) << "never appended after the first refusal";
}
