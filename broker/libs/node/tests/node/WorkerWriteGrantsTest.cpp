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

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <functional>
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

    void announce(int64_t sid, bool grantAll, int32_t clientVersion = 0) {
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = sid;
        a.egressStreamId = kEgress; a.egressChannel = kEgressChannel; a.grantAll = grantAll;
        a.clientVersion = clientVersion;
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
    void importChunk(int64_t table, uint64_t first, uint64_t total, const std::vector<uint64_t>& words, bool last) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = table; o.topicId = kPermBitmapImport;
        o.nextOffset = static_cast<int64_t>(first); o.hwBound = static_cast<int64_t>(total);
        o.isolation = static_cast<uint8_t>(kPermFlagTable | (last ? kPermFlagLast : 0u));
        o.permWords = words.data(); o.permWordCount = words.size();
        ASSERT_TRUE(worker->tryEnqueueOp(std::move(o)));
    }
    void importPatch(int64_t table, uint64_t first, uint64_t total, const std::vector<uint64_t>& words) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = table; o.topicId = kPermBitmapImport;
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
    /// Issues #603 / #506: the lag LEVEL as the PermissionShipper ships it (0 current, 1 refuse all, 2 defer denials).
    void securityLag(int64_t level) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = 0;
        o.nextOffset = level; o.isolation = kPermFlagSecurityBehind | kPermFlagLast;
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

// Issue #198: a later announce wave (the crypto-key second wave) of a session that already holds a bound table never
// resets its grants.  A session is accepted only after its FIRST wave reached every worker and binds exist only for
// accepted sessions, so "first announce before bind" is the only cross-queue order that matters.
TEST(WorkerWriteGrantRegression, ALaterAnnounceWaveKeepsTheBoundGrantsRegression) {
    Fixture f;
    f.announce(5, false);                                    // first wave
    f.tableChunk(f.tableOf(5), 0, 1, bitsFor({1}, 1), true);
    f.bind(5, f.tableOf(5), /*epoch=*/4, /*owner=*/9);
    f.steps(8);
    ASSERT_NE(f.slot(5), nullptr);
    const WriteGrantSlot before = *f.slot(5);
    ASSERT_EQ(before.epoch, 4u);
    ASSERT_TRUE(allowsWrite(before, 1));
    f.announce(5, false);                                    // second wave (the key wave) of the same session
    f.steps(8);
    ASSERT_NE(f.slot(5), nullptr);
    EXPECT_EQ(f.slot(5)->table, before.table) << "still bound to the same table";
    EXPECT_EQ(f.slot(5)->epoch, 4u);
    EXPECT_EQ(f.slot(5)->ownerTag, before.ownerTag) << "the owner tag is unchanged";
    EXPECT_EQ(f.worker->permBindsUnknownSessionForTest(), 0u);
    f.produce(5, 1, /*topicId=*/1);
    ASSERT_TRUE(f.awaitAck(1));
    EXPECT_EQ(f.acks[1].errorCode, 0) << "a produce to the granted topic is accepted";
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
        bool installImportGrants(int64_t, std::vector<uint64_t>&) noexcept { return true; }
    } sink;
    PermissionStager st;
    std::vector<uint64_t> chunkWords(512, 7);
    auto update = [&] {
        EXPECT_EQ(st.apply(sink, 1, 0, 1024, 0, 1, reinterpret_cast<const uint8_t*>(chunkWords.data()), 4096, /*owner=*/7, kPermBitmapWrite), PermApply::Staged);
        EXPECT_EQ(st.apply(sink, 1, 512, 1024, kPermFlagLast, 1, reinterpret_cast<const uint8_t*>(chunkWords.data()), 4096, /*owner=*/7, kPermBitmapWrite),
                  PermApply::Applied);
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

// Namespaces Phase 2 (D21): a bind to the SUSPENDED table (-2, the session's namespace is ReadOnly) denies every topic
// like a bind to nothing, but the worker answers NamespaceSuspended (51) to a v14 client and InvalidRequest to an older
// one - as the gateway path does - never NotAuthorized; nothing is appended.  A plain deny bind still answers 45.
TEST(WorkerWriteGrantRegression, ASuspendedBindAnswersNamespaceSuspendedRegression) {
    Fixture f;
    f.announce(5, /*grantAll=*/true, pc::kClientVersionNamespaces);
    f.announce(6, /*grantAll=*/true, pc::kClientVersionNamespaces - 1);
    f.steps(2);
    f.bind(5, -2, 1);
    f.bind(6, -2, 1);
    f.steps(4);
    EXPECT_FALSE(allowsWrite(*f.slot(5), 1));
    EXPECT_EQ(f.slot(5)->table, suspendedWriteTable());
    const int64_t end = f.log->logEndOffset();
    f.produce(5, 1);
    f.produce(6, 2);
    ASSERT_TRUE(f.awaitAck(1));
    ASSERT_TRUE(f.awaitAck(2));
    EXPECT_EQ(f.acks[1].errorCode, static_cast<int16_t>(pc::ErrorCode::NamespaceSuspended));
    EXPECT_EQ(f.acks[2].errorCode, static_cast<int16_t>(pc::ErrorCode::InvalidRequest)) << "a pre-v14 client";
    EXPECT_EQ(f.log->logEndOffset(), end) << "a suspended produce appends nothing";
    EXPECT_EQ(f.worker->workerSuspendedRefusalsForTest(), 2u);
    EXPECT_EQ(f.worker->workerNotAuthorizedForTest(), 0u);
    f.bind(5, -1, 2);   // a plain deny (a revoke while suspended) is NotAuthorized again
    f.steps(4);
    f.produce(5, 3);
    ASSERT_TRUE(f.awaitAck(3));
    EXPECT_EQ(f.acks[3].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
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

// Issue #199: the lifecycle ring is in-process and FIFO, written only by the PermissionShipper, so a structurally
// malformed SESSION_PERMISSIONS op is a broker bug: the worker halts (fail-stop, like every other "cannot happen"
// lifecycle-ring frame) instead of silently dropping it.  One row per Violation class; each row is a death test (the
// halt is std::_Exit) whose child logs to stderr.  A tail that is not whole words is not constructible through
// tryEnqueueOp (the op carries a word count); the worker's check for it is defensive.
TEST(WorkerPermissionOpsRegression, AMalformedPermissionOpHaltsTheWorkerRegression) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const int64_t kMax = static_cast<int64_t>(kMaxWritePermTables);
    struct Row {
        const char* name;
        const char* reason;
        std::function<void(Fixture&)> feed;
    };
    const auto rawOp = [](Fixture& f, uint8_t flags, int64_t sid, int64_t first, int64_t total, int32_t bitmap,
                          const std::vector<uint64_t>& words, int64_t corr = 0) {
        WorkerOp o; o.type = WorkerOp::Type::SessionPermissions; o.sessionId = sid;
        o.nextOffset = first; o.hwBound = total; o.topicId = bitmap; o.isolation = flags; o.correlationId = corr;
        o.permWords = words.data(); o.permWordCount = words.size();
        ASSERT_TRUE(f.worker->tryEnqueueOp(std::move(o)));
    };
    const std::vector<Row> rows = {
        {"no variant flag", "no variant flag",
         [&](Fixture& f) { rawOp(f, kPermFlagLast, 3, 0, 1, 0, bitsFor({9}, 1)); }},
        {"two variant flags", "more than one variant flag",
         [&](Fixture& f) { rawOp(f, kPermFlagBind | kPermFlagDrop | kPermFlagLast, 5, 3, 0, 0, {}); }},
        {"first word beyond the total", "table chunk words lie outside the table",
         [&](Fixture& f) { f.tableChunk(3, 5, 2, bitsFor({1}, 1), true); }},
        {"more words than the table has left", "table chunk words lie outside the table",
         [&](Fixture& f) { f.tableChunk(3, 1, 1, bitsFor({1}, 1), true); }},
        {"a chunk that skips words", "table chunk is not the next one of the staged table",
         [&](Fixture& f) {
             f.tableChunk(3, 0, 1024, bitsFor({1}, 512), false);
             f.tableChunk(3, 768, 1024, bitsFor({}, 256), true);
         }},
        {"a chunk of another table", "table chunk is not the next one of the staged table",
         [&](Fixture& f) {
             f.tableChunk(3, 0, 1024, bitsFor({1}, 512), false);
             f.tableChunk(4, 512, 1024, bitsFor({}, 512), true);
         }},
        {"a continuation without a first chunk", "table chunk is not the next one of the staged table",
         [&](Fixture& f) { f.tableChunk(3, 512, 1024, bitsFor({}, 512), true); }},
        {"a last chunk that does not complete the table", "last table chunk does not complete the table",
         [&](Fixture& f) { f.tableChunk(3, 0, 1024, bitsFor({1}, 512), true); }},
        {"table id beyond the store", "table id beyond the store",
         [&](Fixture& f) { f.tableChunk(kMax, 0, 1, bitsFor({1}, 1), true); }},
        {"negative table id", "table id beyond the store",
         [&](Fixture& f) { f.tableChunk(-7, 0, 1, bitsFor({1}, 1), true); }},
        {"import table id beyond the store", "table id beyond the store",
         [&](Fixture& f) { f.importChunk(kMax, 0, 1, bitsFor({1}, 1), true); }},
        {"patch table id beyond the store", "patch table id beyond the store",
         [&](Fixture& f) { f.patch(kMax, 0, 1, bitsFor({4}, 1)); }},
        {"patch negative table id", "patch table id beyond the store",
         [&](Fixture& f) { f.patch(-1, 0, 1, bitsFor({4}, 1)); }},
        {"patch first beyond the total", "patch table id beyond the store or words outside the table",
         [&](Fixture& f) { f.patch(3, 5, 2, bitsFor({4}, 1)); }},
        {"patch words beyond the total", "patch table id beyond the store or words outside the table",
         [&](Fixture& f) { f.patch(3, 1, 1, bitsFor({4}, 1)); }},
        {"drop table id beyond the store", "drop table id beyond the store",
         [&](Fixture& f) { f.dropTable(kMax); }},
        {"drop negative table id", "drop table id beyond the store",
         [&](Fixture& f) { f.dropTable(-3); }},
        {"bind table id beyond the store", "bind table id beyond the store",
         [&](Fixture& f) { f.bind(5, kMax, 2); }},
    };
    for (const Row& row : rows) {
        SCOPED_TRACE(row.name);
        EXPECT_EXIT(
            {
                spdlog::set_default_logger(spdlog::stderr_color_mt("permdeath"));
                Fixture f;
                f.announce(5, false);
                f.tableChunk(3, 0, 1, bitsFor({1}, 1), true);
                f.bind(5, 3, 1);
                f.steps(8);
                row.feed(f);
                f.steps(16);
                std::_Exit(0);   // reached only if the worker did NOT halt
            },
            ::testing::ExitedWithCode(Worker::kExitInvariantViolation),
            std::string("session-permissions.*") + row.reason);
    }
}

// Issue #199: the one benign failure - a Bind for a session that closed while the bind was queued - is counted and
// never halts; the worker goes on applying valid ops.
TEST(WorkerPermissionOpsRegression, ABindForAClosedSessionIsCountedNotHaltedRegression) {
    Fixture f;
    f.announce(5, false);
    f.close(5);
    f.bind(5, 3, 1);
    f.steps(8);
    EXPECT_EQ(f.worker->permBindsUnknownSessionForTest(), 1u);
    EXPECT_EQ(f.slot(5), nullptr);
    f.announce(6, false);
    f.chunk(6, 1, 0, 1, bitsFor({1}, 1), true);
    f.steps(8);
    ASSERT_NE(f.slot(6), nullptr);
    EXPECT_TRUE(allowsWrite(*f.slot(6), 1)) << "a later valid op is applied normally";
    EXPECT_EQ(f.worker->permBindsUnknownSessionForTest(), 1u);
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

// Issue #506: while the view holds events it has not applied (level 2) a produce the worker would ALLOW is served, one it
// would DENY is answered AuthUnavailable (the grant may be in a held event) - nothing appended, not counted as a denial.
// RED before: the denied produce answered NotAuthorized (45).  Mutation: treat level 2 as current -> the 48 turns 45;
// treat level 2 as refuse-all -> the allowed produce is refused.
TEST(WorkerWriteGrantRegression, ADenialWhileTheViewHoldsUnappliedEventsIsRetriableOnTheWorkerRegression) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), /*last=*/true);   // WRITE on topic 1
    f.announce(6, false);
    f.chunk(6, 1, 0, 1, bitsFor({2}, 1), /*last=*/true);   // no WRITE on topic 1

    f.securityLag(2);
    f.produce(6, 1);
    ASSERT_TRUE(f.awaitAck(1));
    EXPECT_EQ(f.acks[1].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    EXPECT_EQ(f.log->logEndOffset(), 0) << "nothing appended";
    EXPECT_EQ(f.worker->workerDeniesDeferredForTest(), 1u);
    EXPECT_EQ(f.worker->workerNotAuthorizedForTest(), 0u) << "not a verdict";
    EXPECT_EQ(f.worker->securityLagForTest(), 2u);
    EXPECT_FALSE(f.worker->securityBehindForTest()) << "level 2 is not refuse-all";

    f.produce(5, 2);   // allowed requests are not refused
    ASSERT_TRUE(f.awaitAck(2));
    EXPECT_EQ(f.acks[2].errorCode, 0);
    EXPECT_EQ(f.log->logEndOffset(), 1);
    EXPECT_EQ(f.worker->workerDeniesDeferredForTest(), 1u);

    f.securityLag(0);   // current: the denial is a verdict again
    f.produce(6, 3);
    ASSERT_TRUE(f.awaitAck(3));
    EXPECT_EQ(f.acks[3].errorCode, static_cast<int16_t>(pc::ErrorCode::NotAuthorized));
    EXPECT_EQ(f.worker->workerNotAuthorizedForTest(), 1u);
    EXPECT_EQ(f.worker->workerDeniesDeferredForTest(), 1u);
    EXPECT_EQ(f.log->logEndOffset(), 1);

    f.securityLag(1);   // refuse-all (#603): both sessions, unchanged behaviour
    f.produce(5, 4);
    f.produce(6, 5);
    ASSERT_TRUE(f.awaitAck(4));
    ASSERT_TRUE(f.awaitAck(5));
    EXPECT_EQ(f.acks[4].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    EXPECT_EQ(f.acks[5].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    EXPECT_EQ(f.worker->workerDeniesDeferredForTest(), 1u) << "refuse-all does not count as a deferred denial";
    EXPECT_EQ(f.log->logEndOffset(), 1);
}

// Issue #506: the grant patch lands while the level is 2, then the level drops to 0 (the shipper's order): the retried
// produce is appended.
TEST(WorkerWriteGrantRegression, ADeferredDenialRetriedAfterTheGrantAndTheClearIsAppendedRegression) {
    Fixture f;
    f.announce(6, false);
    f.chunk(6, 1, 0, 1, bitsFor({2}, 1), /*last=*/true);
    f.securityLag(2);
    f.produce(6, 1);
    ASSERT_TRUE(f.awaitAck(1));
    ASSERT_EQ(f.acks[1].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable));
    f.patch(f.tableOf(6), 0, 1, bitsFor({1, 2}, 1));   // the GRANT, then the level drops
    f.securityLag(0);
    f.produce(6, 2);
    ASSERT_TRUE(f.awaitAck(2));
    EXPECT_EQ(f.acks[2].errorCode, 0) << "the same produce, retried";
    EXPECT_EQ(f.log->logEndOffset(), 1);
    EXPECT_EQ(f.worker->securityLagForTest(), 0u);
}

// Issue #506: a level this worker does not know fails closed as refuse-all, and is counted.
TEST(WorkerWriteGrantRegression, AnUnknownSecurityLagLevelIsTreatedAsRefuseAllRegression) {
    Fixture f;
    f.announce(5, false);
    f.chunk(5, 1, 0, 1, bitsFor({1}, 1), /*last=*/true);
    f.securityLag(7);
    f.produce(5, 1);
    ASSERT_TRUE(f.awaitAck(1));
    EXPECT_EQ(f.acks[1].errorCode, static_cast<int16_t>(pc::ErrorCode::AuthUnavailable)) << "even an allowed produce";
    EXPECT_EQ(f.log->logEndOffset(), 0);
    EXPECT_TRUE(f.worker->securityBehindForTest());
    EXPECT_EQ(f.worker->securityLagForTest(), 1u);
    EXPECT_EQ(f.worker->securityLagUnknownForTest(), 1u);
}

TEST(CheckedWritesImportGrant, StagerInstallsTheImportBitmapSeparately) {
    Fixture f;
    f.announce(5, false);
    f.tableChunk(3, 0, 1, bitsFor({5}, 1), true);
    f.importChunk(3, 0, 1, bitsFor({5}, 1), true);
    f.bind(5, 3, 1);
    f.steps(8);
    EXPECT_TRUE(allowsWrite(*f.slot(5), 5));
    EXPECT_TRUE(allowsImport(*f.slot(5), 5));
    EXPECT_FALSE(allowsImport(*f.slot(5), 6));
}

TEST(CheckedWritesImportGrant, TableWithoutImportItemDeniesImport) {
    Fixture f;
    f.announce(5, false);
    f.tableChunk(3, 0, 1, bitsFor({1, 5}, 1), true);
    f.bind(5, 3, 1);
    f.steps(8);
    ASSERT_TRUE(allowsWrite(*f.slot(5), 5));
    for (int32_t id = 0; id < 130; ++id) EXPECT_FALSE(allowsImport(*f.slot(5), id)) << id;
}

TEST(CheckedWritesImportGrant, GrantAllAllowsImport) {
    EXPECT_TRUE(allowsImport(WriteGrantSlot{allowAllWriteTable()}, 77));
    EXPECT_FALSE(allowsImport(WriteGrantSlot{denyWriteTable()}, 77));
}

TEST(CheckedWritesImportGrant, ImportPatchFlipsOneBit) {
    Fixture f;
    f.announce(5, false);
    f.tableChunk(3, 0, 1, bitsFor({1}, 1), true);
    f.importChunk(3, 0, 1, bitsFor({1}, 1), true);
    f.bind(5, 3, 1);
    f.steps(8);
    constexpr int32_t kNew = 130 * 64 + 5;
    f.importPatch(3, 130, 131, bitsFor({5}, 1));
    f.steps(8);
    EXPECT_TRUE(allowsImport(*f.slot(5), kNew));
    EXPECT_TRUE(allowsImport(*f.slot(5), 1)) << "the patch keeps the other words";
    EXPECT_FALSE(allowsWrite(*f.slot(5), kNew)) << "the WRITE bitmap is untouched";
    f.importPatch(3, 130, 131, bitsFor({}, 1));
    f.steps(8);
    EXPECT_FALSE(allowsImport(*f.slot(5), kNew));
}
