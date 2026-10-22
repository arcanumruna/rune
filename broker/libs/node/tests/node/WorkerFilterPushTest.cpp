// WorkerFilterPushTest.cpp - Phase 2c Task 8: the Worker serves FILTERED subscriptions through FilterGroups.
//
// A Worker owns a real disk Log (or a transient ring) + its own dynamic-MDC WorkerEgress.  FilterSchema ops install a
// typed topic's schema; a Subscribe op with a filter tail binds the subscription to the (topic, partition, isolation,
// source) FilterGroup; Worker::doWork then pushes only the matching records (zero-copy when a whole batch matches,
// rebuilt otherwise) and position notes for a quiet filter.  The unfiltered pass is unchanged (byte-identical frames).
//
// Uses an embedded UDP Aeron driver (same pattern as WorkerPushTest).  Time is the steady clock: the engine's version
// waits and position notes are wall-clock driven.
#include "support/FilterFakes.h"

#include <rune/aeronutil/testing/FreePort.h>
#include <rune/codec/Serialize.h>
#include <rune/crypto/Aead.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/CryptoEnvelope.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/transaction/ControlMarker.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <Subscription.h>
#include <concurrent/AtomicBuffer.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node;
using namespace rune::node::filter::fakes;
namespace pc = rune::protocol::client;

namespace {

constexpr std::size_t kRingBytes = 512 * 1024;
constexpr int32_t kTopic = 10;

inline std::string freeEndpoint() { return "127.0.0.1:" + std::to_string(rune::aeronutil::testing::allocFreeUdpPort()); }
inline int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// Filter bytes, canonical: worldId (tag 1) == w.
struct Fb {
    rune::codec::FilterBytes fb;
    std::size_t n = 0;
    [[nodiscard]] std::span<const uint8_t> view() const { return {fb.bytes.data(), n}; }
};
Fb worldEq(uint32_t w) {
    Fb f;
    rune::codec::FilterWriter fw(std::span<uint8_t>(f.fb.bytes.data(), f.fb.bytes.size()));
    fw.term(1, rune::codec::PrimType::UInt32, 4, rune::Match<uint32_t>(w));
    auto n = fw.finish();
    if (!n || *n == 0) throw std::runtime_error("filter encode failed");
    f.n = *n;
    return f;
}

/// Everything the egress delivers: Records (data and position notes) and ErrorResponses.  With `opener` set every
/// frame must open under that key first (an encrypted session).
struct FilterCollector {
    struct Frame { int64_t subId, corr, base; std::vector<uint8_t> bytes; };
    struct Err { int64_t sessionId, correlationId; int16_t code; std::string message; };
    std::vector<Frame> frames;
    std::vector<Err> errors;
    rune::crypto::AeadCipher* opener = nullptr;
    int opened = 0, openFailed = 0, plainDecodable = 0;
    aeron::FragmentAssembler fa;
    FilterCollector()
        : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
              const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
              const auto n = static_cast<std::size_t>(len);
              if (opener == nullptr) { take(p, n); return; }
              if (pc::Records::decode(p, n)) ++plainDecodable;   // a sealed frame must never decode as plaintext
              if (n < pc::kCryptoHeaderBytes + 16) { ++openFailed; return; }
              const auto sub = pc::CryptoSubHeader::decode(p + pc::MessageHeader::ENCODED_SIZE, n - pc::MessageHeader::ENCODED_SIZE);
              if (!sub) { ++openFailed; return; }
              std::vector<uint8_t> plain(pc::MessageHeader::ENCODED_SIZE + n);
              std::memcpy(plain.data(), p, pc::MessageHeader::ENCODED_SIZE);
              const long m = opener->open(sub->seq, p, pc::kCryptoHeaderBytes, p + pc::kCryptoHeaderBytes, n - pc::kCryptoHeaderBytes,
                                          plain.data() + pc::MessageHeader::ENCODED_SIZE);
              if (m < 0) { ++openFailed; return; }
              ++opened;
              take(plain.data(), pc::MessageHeader::ENCODED_SIZE + static_cast<std::size_t>(m));
          }) {}
    void take(const uint8_t* p, std::size_t n) {
        if (auto r = pc::Records::decode(p, n))
            frames.push_back({r->subscriptionId, r->correlationId, r->baseOffset, {r->recordBatch.begin(), r->recordBatch.end()}});
        else if (auto e = pc::ErrorResponse::decode(p, n))
            errors.push_back({e->sessionId, e->correlationId, e->code, std::string(e->message)});
    }
    /// Record offsets delivered to `sub`, in frame order.
    [[nodiscard]] std::vector<int64_t> offsets(int64_t sub) const {
        std::vector<int64_t> v;
        for (const auto& f : frames) {
            if (f.subId != sub || f.bytes.empty()) continue;
            rune::protocol::RecordBatchReader r(f.bytes.data(), f.bytes.size());
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { v.push_back(o); });
        }
        return v;
    }
    [[nodiscard]] std::vector<int64_t> notes(int64_t sub) const {
        std::vector<int64_t> v;
        for (const auto& f : frames) if (f.subId == sub && f.bytes.empty()) v.push_back(f.base);
        return v;
    }
    [[nodiscard]] std::size_t dataFrames(int64_t sub) const {
        std::size_t c = 0;
        for (const auto& f : frames) c += (f.subId == sub && !f.bytes.empty());
        return c;
    }
};

class WorkerFilterFixture : public ::testing::Test {
protected:
    std::unique_ptr<rune::node::EmbeddedDriver> driver_;
    std::shared_ptr<aeron::Aeron> aeron_;
    std::string aeronDir_;
    std::unique_ptr<WorkerEgress> eg_;
    std::shared_ptr<aeron::Subscription> sub_;

    virtual int32_t termBufferBytes() const { return 64 * 1024; }
    virtual int32_t rcvWindowBytes() const { return 128 * 1024; }

    void SetUp() override {
        aeronDir_ = (std::filesystem::temp_directory_path() / ("rune_wfilter_" + std::to_string(::getpid()))).string();
        std::filesystem::remove_all(aeronDir_);
        rune::node::EmbeddedDriverConfig cfg;
        cfg.dir = aeronDir_;
        cfg.termBufferBytes = termBufferBytes();
        cfg.rcvWindowBytes = rcvWindowBytes();
        try {
            driver_ = std::make_unique<rune::node::EmbeddedDriver>(cfg, rune::node::DriverMode::Threaded);
        } catch (const std::exception& e) {
            FAIL() << "EmbeddedDriver failed to start: " << e.what();
        }
        aeron::Context ctx;
        ctx.aeronDir(aeronDir_);
        aeron_ = aeron::Aeron::connect(ctx);
        ASSERT_NE(aeron_, nullptr);
    }
    void TearDown() override { sub_.reset(); eg_.reset(); aeron_.reset(); driver_.reset(); }

    template <typename Pred>
    bool waitFor(Pred p, std::chrono::milliseconds timeout = 5s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) { if (p()) return true; std::this_thread::sleep_for(1ms); }
        return p();
    }
    void openEgress(int32_t streamId) {
        const std::string control = freeEndpoint();
        eg_ = std::make_unique<WorkerEgress>(aeron_, control.c_str(), streamId);
        ASSERT_TRUE(eg_->resolved());
        const std::string ch = "aeron:udp?control=" + control + "|control-mode=dynamic|endpoint=" + freeEndpoint();
        const int64_t reg = aeron_->addSubscription(ch, streamId);
        waitFor([&]() { sub_ = aeron_->findSubscription(reg); return sub_ != nullptr; });
        ASSERT_NE(sub_, nullptr);
        ASSERT_TRUE(waitFor([&]() { return eg_->isConnected() && sub_->isConnected(); }));
    }
    std::filesystem::path tempLogDir() {
        static std::atomic<int> seq{0};
        auto d = std::filesystem::temp_directory_path() /
                 ("rune_wfilter_log_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
        std::filesystem::remove_all(d);
        std::filesystem::create_directories(d);
        return d;
    }
    /// One pass of the worker and the subscriber.
    void step(Worker& w, FilterCollector& c) { w.doWork(nowNs()); sub_->poll(c.fa.handler(), 64); }
    template <typename Pred>
    bool pump(Worker& w, FilterCollector& c, Pred p, std::chrono::milliseconds timeout = 10s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) { step(w, c); if (p()) return true; }
        step(w, c);
        return p();
    }
    void settle(Worker& w, FilterCollector& c, std::chrono::milliseconds d) {
        const auto deadline = std::chrono::steady_clock::now() + d;
        while (std::chrono::steady_clock::now() < deadline) { step(w, c); std::this_thread::sleep_for(100us); }
    }

    /// Append one typed batch at the log end; returns its base offset.
    static int64_t appendRows(rune::storage::Log& log, const std::vector<PlayerRow>& rows, uint16_t version = 2,
                              BatchOpts o = {}) {
        const int64_t base = log.logEndOffset();
        log.append(makePlayerBatch(base, version, rows, o), base + static_cast<int64_t>(rows.size()) - 1);
        return base;
    }
    static std::vector<PlayerRow> worlds(std::initializer_list<uint32_t> ws) {
        std::vector<PlayerRow> v;
        for (const uint32_t w : ws) v.push_back(PlayerRow{.worldId = w});
        return v;
    }

    static WorkerOp schemaOp(int32_t topic, uint16_t version) {
        WorkerOp op;
        op.type = WorkerOp::Type::FilterSchema;
        op.topicId = topic;
        op.clientVersion = version;
        op.schemaUpdateBlob = rune::codec::serialize(playerEvent(version));
        return op;
    }
    static WorkerOp plainSubscribeOp(int64_t subId, int32_t topic, int32_t partition, int64_t from, rune::storage::Log* log) {
        WorkerOp op;
        op.type = WorkerOp::Type::Subscribe;
        op.subscriptionId = subId;
        op.sessionId = 4200 + subId;
        op.correlationId = 9000 + subId;
        op.topicId = topic;
        op.partition = partition;
        op.nextOffset = from;
        op.logPtr = log;
        op.replica = nullptr;   // passthrough: bound = LEO
        return op;
    }
    static WorkerOp filteredSubscribeOp(int64_t subId, int32_t topic, int32_t partition, int64_t from, rune::storage::Log* log,
                                        const Fb& f) {
        WorkerOp op = plainSubscribeOp(subId, topic, partition, from, log);
        op.filterBytes = f.view();
        return op;
    }
    static WorkerOp unsubscribeOp(int64_t subId) {
        WorkerOp op;
        op.type = WorkerOp::Type::Unsubscribe;
        op.subscriptionId = subId;
        return op;
    }
    /// Park the worker's duty cycle until the filtered subscription has consumed the log (cursor at the log end).
    bool caughtUp(Worker& w, int64_t sub, int64_t end) { return w.subscriptionPositionForTest(sub) >= end; }
};

using WorkerFilterPush = WorkerFilterFixture;
using WorkerFilterPushRegression = WorkerFilterFixture;
using WorkerFilterRegression = WorkerFilterFixture;

// ---------------------------------------------------------------------------------------------------------------
TEST_F(WorkerFilterPush, FilteredSubscriptionReceivesOnlyMatches) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    std::vector<int64_t> expect;
    int64_t off = 0;
    for (int i = 0; i < 20; ++i) {
        std::vector<PlayerRow> rows;
        const int n = 1 + (i % 3);
        for (int j = 0; j < n; ++j) {
            const uint32_t world = static_cast<uint32_t>((i * 3 + j * 5) % 11);
            rows.push_back(PlayerRow{.worldId = world});
            if (world == 7) expect.push_back(off);
            ++off;
        }
        appendRows(log, rows);
    }
    ASSERT_FALSE(expect.empty());
    ASSERT_LT(expect.size(), static_cast<std::size_t>(off)) << "the model must mix matches and non-matches";

    openEgress(3401);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() >= expect.size() && caughtUp(w, 1, log.logEndOffset()); }))
        << "got " << c.offsets(1).size() << "/" << expect.size();
    settle(w, c, 30ms);
    EXPECT_EQ(c.offsets(1), expect);
    EXPECT_EQ(w.filterBindFailuresForTest(), 0u);
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 1u);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPush, UnfilteredNeighbourIsByteIdentical) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    std::vector<std::vector<uint8_t>> stored;
    for (int i = 0; i < 12; ++i) {
        const int64_t base = log.logEndOffset();
        auto b = makePlayerBatch(base, 2, worlds({static_cast<uint32_t>(i % 4), 7, static_cast<uint32_t>(i % 3)}));
        log.append(b, base + 2);
        stored.push_back(std::move(b));
    }
    openEgress(3402);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(1, kTopic, 0, 0, &log)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.dataFrames(1) >= stored.size() && c.offsets(2).size() >= stored.size(); }));
    settle(w, c, 30ms);
    ASSERT_EQ(c.dataFrames(1), stored.size());
    std::size_t k = 0;
    for (const auto& fr : c.frames) {
        if (fr.subId != 1) continue;
        EXPECT_EQ(fr.bytes, stored[k]) << "unfiltered frame " << k << " differs from the log batch";
        ++k;
    }
    EXPECT_TRUE(c.notes(1).empty()) << "a position note reached an unfiltered subscription";
    // The filtered neighbour got exactly the world-7 record of every batch (offset base + 1).
    std::vector<int64_t> expect;
    for (int i = 0; i < 12; ++i) expect.push_back(i * 3 + 1);
    EXPECT_EQ(c.offsets(2), expect);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPush, QuietFilterGetsPositionNotes) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 20; ++i) appendRows(log, worlds({1, 2}));
    openEgress(3403);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    filter::FilterConfig cfg;
    cfg.noteIntervalNs = 2'000'000;
    w.setFilterConfig(cfg);
    const Fb f = worldEq(7);   // never matches
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return !c.notes(1).empty() && c.notes(1).back() == log.logEndOffset(); }))
        << "notes=" << c.notes(1).size();
    EXPECT_TRUE(c.offsets(1).empty());
    EXPECT_EQ(c.notes(1).back(), log.logEndOffset());
    EXPECT_EQ(w.filterCountersForTest().notes, c.notes(1).size());
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPushRegression, EraseOfAFilteredSubFixesTheMovedMemberRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    auto model = [](int64_t from, int64_t to, uint32_t world) {
        std::vector<int64_t> v;
        for (int64_t o = from; o < to; ++o) if (static_cast<uint32_t>(o % 3) + 1 == world) v.push_back(o);
        return v;
    };
    for (int i = 0; i < 12; ++i) appendRows(log, worlds({1, 2, 3}));   // offset o has world (o % 3) + 1
    openEgress(3404);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f1 = worldEq(1), f2 = worldEq(2), f3 = worldEq(3);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f1)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &log, f2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(3, kTopic, 0, 0, &log, f3)));
    FilterCollector c;
    const int64_t end1 = log.logEndOffset();
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == model(0, end1, 1).size() && c.offsets(2).size() == model(0, end1, 2).size() &&
                                        c.offsets(3).size() == model(0, end1, 3).size(); }));
    // Remove the MIDDLE one: the last (filtered) subscription is moved into its slot.
    ASSERT_TRUE(w.tryEnqueueOp(unsubscribeOp(2)));
    for (int i = 0; i < 4; ++i) step(w, c);
    ASSERT_EQ(w.subscriptionsForTest().size(), 2u);
    // A NEW subscription takes the slot the swap-and-pop vacated: a member left pointing at the old last index would
    // now deliver sub 3's frames on sub 4's behalf.
    const int64_t pos4 = log.logEndOffset();
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(4, kTopic, 0, pos4, &log, f2)));
    for (int i = 0; i < 6; ++i) appendRows(log, worlds({1, 2, 3}));
    const int64_t end2 = log.logEndOffset();
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == model(0, end2, 1).size() && c.offsets(3).size() == model(0, end2, 3).size() &&
                                        c.offsets(4).size() == model(pos4, end2, 2).size(); }))
        << "moved member lost its matches: sub3=" << c.offsets(3).size() << "/" << model(0, end2, 3).size();
    settle(w, c, 30ms);
    EXPECT_EQ(c.offsets(1), model(0, end2, 1));
    EXPECT_EQ(c.offsets(3), model(0, end2, 3)) << "the moved member keeps receiving exactly its matches";
    EXPECT_EQ(c.offsets(2), model(0, end1, 2)) << "the removed subscription received nothing after its Unsubscribe";
    EXPECT_EQ(c.offsets(4), model(pos4, end2, 2)) << "the new subscription gets exactly its own matches";
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 3u);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPushRegression, EgressReleaseUnbindsFilteredSubscriptionsRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 6; ++i) appendRows(log, worlds({1, 2, 3}));
    openEgress(3405);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f1 = worldEq(1), f2 = worldEq(2), f3 = worldEq(3);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    WorkerOp a = filteredSubscribeOp(1, kTopic, 0, 0, &log, f1);
    WorkerOp b = filteredSubscribeOp(2, kTopic, 0, 0, &log, f2);
    a.egressPub = eg_->pub();
    b.egressPub = eg_->pub();
    ASSERT_TRUE(w.tryEnqueueOp(a));
    ASSERT_TRUE(w.tryEnqueueOp(b));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(3, kTopic, 0, 0, &log, f3)));   // a different egress binding (the shared pub)
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 6 && c.offsets(2).size() == 6 && c.offsets(3).size() == 6; }));
    ASSERT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 3u);

    WorkerOp rel;   // the runtime's RELEASE of that publication (Unsubscribe carrying egressPub)
    rel.type = WorkerOp::Type::Unsubscribe;
    rel.egressPub = eg_->pub();
    rel.sessionId = 77;
    rel.correlationId = 5;
    ASSERT_TRUE(w.tryEnqueueOp(rel));
    for (int i = 0; i < 4; ++i) step(w, c);
    ASSERT_EQ(w.egressReleasesForTest(), 1u);
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u) << "both subscriptions writing through the released publication are gone";
    EXPECT_EQ(w.subscriptionsForTest()[0].subscriptionId, 3);
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 1u) << "released filtered members must leave their group";
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({1, 2, 3}));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(3).size() == 10; })) << "the survivor was corrupted by the release";
    settle(w, c, 30ms);
    EXPECT_EQ(c.offsets(1).size(), 6u);
    EXPECT_EQ(c.offsets(2).size(), 6u);
    EXPECT_EQ(c.offsets(3).size(), 10u);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPushRegression, ResubscribeSameIdRebindsTheNewFilterRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    openEgress(3406);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    const Fb f7 = worldEq(7), f9 = worldEq(9);
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f7)));
    FilterCollector c;
    for (int i = 0; i < 5; ++i) appendRows(log, worlds({7, 9, 1}));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 5 && caughtUp(w, 1, log.logEndOffset()); }));
    // Phase B: same id, the client's position, a different filter.
    const int64_t pos = w.subscriptionPositionForTest(1);
    ASSERT_EQ(pos, 15);
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, pos, &log, f9)));
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({7, 9, 1}));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 5 + 4 && caughtUp(w, 1, log.logEndOffset()); }));
    EXPECT_EQ(c.offsets(1).back(), 15 + 3 * 3 + 1) << "after the re-subscribe only world 9 matches";
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 1u) << "the old member must be removed, not duplicated";
    ASSERT_EQ(w.subscriptionsForTest().size(), 1u);
    // Phase C: re-subscribe WITHOUT a filter - the subscription becomes an ordinary unfiltered one.
    const int64_t pos2 = w.subscriptionPositionForTest(1);
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(1, kTopic, 0, pos2, &log)));
    for (int i = 0; i < 2; ++i) appendRows(log, worlds({7, 9, 1}));
    ASSERT_TRUE(pump(w, c, [&] { return c.dataFrames(1) >= 5 + 4 + 2; }));
    EXPECT_EQ(w.filterGroupCountForTest(), 0u) << "the empty group is released";
    std::vector<int64_t> tail;
    for (const auto o : c.offsets(1)) if (o >= pos2) tail.push_back(o);
    EXPECT_EQ(tail.size(), 6u) << "unfiltered again: every record";
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, RecordOfALateVersionWaitsForItsSchemaThenDeliversRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendRows(log, worlds({7}), /*version=*/2);   // written with the v2 layout, schema only knows v1
    openEgress(3407);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 1)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    settle(w, c, 30ms);
    EXPECT_TRUE(c.offsets(1).empty()) << "a record of an unknown version must wait, not be dropped or sent";
    EXPECT_EQ(w.subscriptionPositionForTest(1), 0) << "and the cursor must not pass it";
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 1; })) << "the v2 schema arrived but the record never came";
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(1), (std::vector<int64_t>{0})) << "exactly once";
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, TombstonesAdvanceTheFilteredCursorRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    std::vector<int64_t> expect;
    uint64_t tombstones = 0;
    int64_t off = 0;
    for (int i = 0; i < 10; ++i) {
        std::vector<PlayerRow> rows;
        for (int j = 0; j < 4; ++j) {
            PlayerRow r{.worldId = static_cast<uint32_t>(j == 1 ? 7 : 3)};
            if (j == 2 || (i % 2 == 0 && j == 1)) { r.tombstone = true; ++tombstones; }
            else if (j == 1) expect.push_back(off);
            rows.push_back(r);
            ++off;
        }
        appendRows(log, rows);
    }
    ASSERT_FALSE(expect.empty());
    {   // a quiet tail (one tombstone, no match): the client's position only moves by a note
        PlayerRow t{.worldId = 7}; t.tombstone = true; ++tombstones;
        appendRows(log, {t, PlayerRow{.worldId = 3}});
    }
    openEgress(3408);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    filter::FilterConfig cfg;
    cfg.noteIntervalNs = 2'000'000;
    w.setFilterConfig(cfg);
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == expect.size() && !c.notes(1).empty() &&
                                        c.notes(1).back() == log.logEndOffset(); }));
    EXPECT_EQ(c.offsets(1), expect);
    EXPECT_EQ(w.filterCountersForTest().nullValueExcluded, tombstones);
    EXPECT_EQ(w.subscriptionPositionForTest(1), log.logEndOffset());
    std::filesystem::remove_all(dir);
}

// ---- read_committed via a real replica (the TxnRcRegression rig) ----
class NullTransportF final : public rune::replication::PartitionTransport {
public:
    bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
    bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
};
rune::replication::PartitionConfig leaderCfg() {
    rune::replication::PartitionConfig c;
    c.topicId = kTopic; c.partitionId = 0; c.selfId = 1; c.minIsr = 1; c.durabilityTier = 1;
    c.fetchIntervalNs = 1'000'000'000LL; c.isrCheckIntervalNs = 1'000'000'000LL; c.pushOnAppend = false;
    c.replicaLagMaxMs = 1'000'000;
    return c;
}
WorkerOp markerOp(rune::replication::PartitionReplica* r, int64_t pid, bool commit) {
    WorkerOp op;
    op.type = WorkerOp::Type::AppendControlRecord; op.topicId = r->topicId(); op.partition = 0;
    op.replica = r; op.logPtr = r->log(); op.correlationId = pid;
    op.recordBatchBytes = rune::transaction::ControlMarker::build(
        pid, 0, commit ? rune::transaction::ControlType::Commit : rune::transaction::ControlType::Abort, 0);
    return op;
}

TEST_F(WorkerFilterPushRegression, FilteredSubscriptionUnderReadCommittedSeesOnlyCommittedMatchesRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    BatchOpts txn7; txn7.transactional = true; txn7.producerId = 7;
    BatchOpts txn8; txn8.transactional = true; txn8.producerId = 8;
    appendRows(log, worlds({7}));                     // @0 committed data, match
    appendRows(log, worlds({7}), 2, txn7);            // @1 open txn 7, match
    appendRows(log, worlds({7}), 2, txn8);            // @2 txn 8 (aborted below), match
    appendRows(log, worlds({1}));                     // @3 data, no match
    appendRows(log, worlds({7}));                     // @4 data behind the LSO, match
    NullTransportF t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    ASSERT_EQ(rep.highWatermark(), 5);

    openEgress(3409);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    WorkerOp sop = filteredSubscribeOp(1, kTopic, 0, 0, &log, f);
    sop.replica = &rep;
    sop.isolation = 1;   // read_committed
    ASSERT_TRUE(w.tryEnqueueOp(sop));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() >= 1; }));
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(1), (std::vector<int64_t>{0})) << "the open transactions hold the LSO at 1";
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 8, /*commit=*/false)));   // abort txn 8 (marker @5)
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 7, /*commit=*/true)));    // commit txn 7 (marker @6)
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() >= 3; }));
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(1), (std::vector<int64_t>{0, 1, 4})) << "the aborted match must never be delivered";
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPushRegression, MissingSchemaAnswersInvalidRequestNeverBindsSilentlyRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendRows(log, worlds({7, 7}));
    openEgress(3410);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));   // no FilterSchema op
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return !c.errors.empty(); }));
    settle(w, c, 50ms);
    ASSERT_EQ(c.errors.size(), 1u) << "exactly one answer, no spin";
    EXPECT_EQ(c.errors[0].code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_EQ(c.errors[0].correlationId, 9001) << "correlated with the Subscribe";
    EXPECT_EQ(c.errors[0].sessionId, 4201);
    EXPECT_NE(c.errors[0].message.find("filter schema unavailable"), std::string::npos) << c.errors[0].message;
    EXPECT_TRUE(c.frames.empty()) << "a failed filter bind must never deliver (it would be unfiltered data)";
    EXPECT_EQ(w.filterBindFailuresForTest(), 1u);
    EXPECT_EQ(w.filterGroupCountForTest(), 0u);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterPushRegression, EncryptedSessionSealsFilteredFramesAndNotesRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 6; ++i) appendRows(log, worlds({7, 1}));
    appendRows(log, worlds({1, 1}));   // a quiet tail: the client's position only moves by a note
    openEgress(3411);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    filter::FilterConfig cfg;
    cfg.noteIntervalNs = 2'000'000;
    w.setFilterConfig(cfg);
    constexpr int64_t kSid = 4201;   // the Subscribe op's session (4200 + subId)
    WorkerOp ann;
    ann.type = WorkerOp::Type::SessionAnnounce; ann.grantAll = true;
    ann.sessionId = kSid; ann.egressStreamId = 7; ann.egressChannel = "aeron:ipc";
    ann.sessionEncrypted = true; ann.senderKeyValid = true;
    ann.senderCipher = static_cast<uint8_t>(rune::crypto::Cipher::Aes256Gcm);
    for (std::size_t i = 0; i < sizeof(ann.senderKey); ++i) ann.senderKey[i] = static_cast<uint8_t>(0x40 + i);
    for (std::size_t i = 0; i < sizeof(ann.senderKeyPrefix); ++i) ann.senderKeyPrefix[i] = static_cast<uint8_t>(0x10 + i);
    ASSERT_TRUE(w.tryEnqueueOp(ann));
    rune::crypto::AeadKey key{};
    std::memcpy(key.bytes, ann.senderKey, sizeof(key.bytes));
    rune::crypto::AeadCipher opener(rune::crypto::Cipher::Aes256Gcm, key, ann.senderKeyPrefix);

    const Fb f7 = worldEq(7), f99 = worldEq(99);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f7)));
    FilterCollector c;
    c.opener = &opener;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 6 && !c.notes(1).empty(); })) << "opened=" << c.opened << " failed=" << c.openFailed;
    settle(w, c, 30ms);
    EXPECT_EQ(c.openFailed, 0);
    EXPECT_EQ(c.plainDecodable, 0) << "a filtered frame left the worker in the clear";
    EXPECT_EQ(static_cast<std::size_t>(c.opened), c.frames.size() + c.errors.size());
    EXPECT_EQ(c.offsets(1).size(), 6u);
    EXPECT_GE(c.notes(1).size(), 1u) << "notes are sealed too";
    std::filesystem::remove_all(dir);
}

// ---- transient (ring) partitions ----
class WorkerFilterTransient : public WorkerFilterFixture {
protected:
    struct NullT final : rune::replication::PartitionTransport {
        bool sendFetchRequest(const rune::replication::FetchRequestMsg&) override { return true; }
        bool sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override { return true; }
    };
    static int64_t appendRing(rune::storage::InMemoryLog& mem, const std::vector<PlayerRow>& rows) {
        const int64_t base = mem.logEndOffset();
        EXPECT_GE(mem.append(makePlayerBatch(base, 2, rows), base + static_cast<int64_t>(rows.size()) - 1), 0);
        return base;
    }
    static WorkerOp ringSubscribeOp(int64_t subId, rune::replication::PartitionReplica* r, int64_t from, const Fb* f) {
        WorkerOp op = plainSubscribeOp(subId, r->topicId(), 0, from, /*log=*/nullptr);
        op.replica = r;
        op.targetReplicaId = r->registryId();
        op.incarnation = r->incarnation();   // as WorkerPool stamps it
        if (f != nullptr) op.filterBytes = f->view();
        return op;
    }
};

TEST_F(WorkerFilterTransient, TransientPartitionFilters) {
    rune::storage::InMemoryLog mem(1u << 20, 1000);
    std::vector<int64_t> expect;
    int64_t off = 0;
    for (int i = 0; i < 15; ++i) {
        const auto rows = worlds({static_cast<uint32_t>(i % 2 == 0 ? 7 : 1), static_cast<uint32_t>(i % 5 == 0 ? 7 : 2)});
        for (const auto& r : rows) { if (r.worldId == 7) expect.push_back(off); ++off; }
        appendRing(mem, rows);
    }
    NullT t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &mem, &t);
    rep.setRegistryHandle(3, 400);
    rep.onLeaderLogAppended();
    ASSERT_EQ(rep.highWatermark(), off);

    openEgress(3412);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(ringSubscribeOp(1, &rep, 0, &f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == expect.size() && caughtUp(w, 1, off); }))
        << "got " << c.offsets(1).size() << "/" << expect.size();
    settle(w, c, 30ms);
    EXPECT_EQ(c.offsets(1), expect);
    EXPECT_EQ(w.transientCursorSkipsForTest(), 0u);
}

TEST_F(WorkerFilterTransient, ReBoundReplicaLandsInANewGroupNotTheOldRegression) {
    rune::storage::InMemoryLog mem(1u << 20, 1000);
    for (int i = 0; i < 4; ++i) appendRing(mem, worlds({7, 1}));
    NullT t;
    rune::replication::PartitionReplica rep1(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &mem, &t);
    rep1.setRegistryHandle(3, 400);
    rep1.onLeaderLogAppended();

    openEgress(3413);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    w.setOwnedDirect(Worker::OwnedSet{&rep1});
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(ringSubscribeOp(1, &rep1, 0, &f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 4; }));
    const filter::FilterGroup* g1 = w.filterGroupOfSubForTest(1);
    ASSERT_NE(g1, nullptr);

    // The partition is re-bound: the SAME registry id, a NEW incarnation (a new replica object over the same ring).
    rune::replication::PartitionReplica rep2(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &mem, &t);
    rep2.setRegistryHandle(3, 401);
    rep2.onLeaderLogAppended();
    WorkerOp own; own.type = WorkerOp::Type::SetOwnership; own.ownedSet = {&rep2};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(own)));
    for (int i = 0; i < 4; ++i) step(w, c);
    ASSERT_TRUE(w.tryEnqueueOp(ringSubscribeOp(2, &rep2, 0, &f)));
    for (int i = 0; i < 4; ++i) step(w, c);
    // Re-read g1: binding sub 2 may have grown filterGroups_ and moved the groups (the worker keeps indices, not pointers).
    g1 = w.filterGroupOfSubForTest(1);
    ASSERT_NE(g1, nullptr);
    const filter::FilterGroup* g2 = w.filterGroupOfSubForTest(2);
    ASSERT_NE(g2, nullptr);
    EXPECT_NE(g1, g2) << "a member of a different replica incarnation must not join the old group (never mix sources)";
    EXPECT_EQ(w.filterGroupCountForTest(), 2u);
    EXPECT_EQ(g1->memberCount(), 1u);
    EXPECT_EQ(g2->memberCount(), 1u);
    EXPECT_EQ(w.filterCountersForTest().sourceRefused, 0u) << "separate groups: nothing was refused";
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(2).size() == 4; })) << "the re-bound subscription is served from the new source";
}

// ---- the engine's source contract through the Worker's real reads ----
class WorkerFilterBigTerm : public WorkerFilterFixture {
    int32_t termBufferBytes() const override { return 16 * 1024 * 1024; }   // max message 2 MiB: a 150 KB frame fits
};
TEST_F(WorkerFilterBigTerm, SourceReadReturnsTheBatchHoldingFromEvenWhenLargerThanTheCapRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    std::vector<PlayerRow> big;
    for (int i = 0; i < 2500; ++i) big.push_back(PlayerRow{.worldId = 7});   // one oversize batch (~150 KB)
    appendRows(log, worlds({1, 7}));   // @0..1 small
    const int64_t bigBase = appendRows(log, big);
    const std::size_t bigBytes = makePlayerBatch(bigBase, 2, big).size();
    ASSERT_GT(bigBytes, 100u * 1024u);
    appendRows(log, worlds({7}));      // behind it
    openEgress(3414);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    step(w, c);
    // cap far below the batch; `from` is a record in the MIDDLE of the batch.
    const std::size_t got = w.filterSourceReadBytesForTest(1, bigBase + 1000, 1000);
    EXPECT_GE(got, bigBytes) << "read(from, cap) must return at least the whole batch holding from";
    // A small cap over small batches still returns whole batches (>= the one holding from).
    EXPECT_GT(w.filterSourceReadBytesForTest(1, 0, 1), 0u);
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 1 + big.size() + 1; })) << "got " << c.offsets(1).size();
    std::filesystem::remove_all(dir);
}

// ---- PR4: a position reported for a filtered member goes through the group (an InSync cursor is stale) ----
TEST_F(WorkerFilterPushRegression, FilteredTooLargeNamesTheBatchOffsetNotAStaleCursorRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendRows(log, worlds({7}));   // @0 small match: the member is InSync afterwards
    std::vector<PlayerRow> huge;
    for (int i = 0; i < 300; ++i) huge.push_back(PlayerRow{.worldId = 7});   // ~18 KB > the 8 KiB egress max message
    openEgress(3415);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 1; }));
    ASSERT_TRUE(std::holds_alternative<filter::InSync>(w.filterGroupForTest(kTopic, 0, 0)->stateOf(0)));
    for (int i = 0; i < 3; ++i) appendRows(log, worlds({7}));   // delivered InSync: the member's own cursor goes stale (1)
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 4; }));
    appendRows(log, huge);          // @4.. does not fit the egress
    ASSERT_TRUE(pump(w, c, [&] { return !c.errors.empty(); }));
    settle(w, c, 50ms);
    ASSERT_EQ(c.errors.size(), 1u) << "exactly one error, no spin";
    EXPECT_EQ(c.errors[0].code, static_cast<int16_t>(pc::ErrorCode::MessageTooLarge));
    EXPECT_NE(c.errors[0].message.find("subscription 1 topic 10 partition 0 offset 4"), std::string::npos)
        << "the error must name the undeliverable batch, not the stale cursor; got: " << c.errors[0].message;
    EXPECT_EQ(w.subscriptionPositionForTest(1), 4) << "parked AT the batch";
    EXPECT_EQ(c.offsets(1).size(), 4u);
    std::filesystem::remove_all(dir);
}

// ---- PR2: the filtered pass and the unfiltered pass alternate; neither starves the other ----
class WorkerFilterStarvation : public WorkerFilterFixture {
    int32_t termBufferBytes() const override { return 16 * 1024 * 1024; }   // max message 2 MiB: 40 KB batches fit
    int32_t rcvWindowBytes() const override { return 4 * 1024 * 1024; }     // the unfiltered sub is not flow-controlled
};

TEST_F(WorkerFilterStarvation, FilteredPushNotStarvedByUnfilteredLoadRegression) {
    auto dirA = tempLogDir();
    auto dirB = tempLogDir();
    rune::storage::Log logA(dirA.string(), 256LL * 1024 * 1024);   // the unfiltered backlog: ~200 x 20 KB
    rune::storage::Log logB(dirB.string(), 64LL * 1024 * 1024);
    // 200 x 40 KB: every unfiltered cycle reads three batches (120 KB) and leaves ~8 KB of the 128 KiB pass budget, so a
    // filtered pass that runs second (never first, never with the fresh budget) cannot admit its 15 KB batches.
    constexpr int kBig = 200;
    for (int i = 0; i < kBig; ++i) {
        const int64_t base = logA.logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(40 * 1024, 'x'), 1000);
        const auto bytes = b.build();
        logA.appendNoSync(bytes.data(), bytes.size(), base);
    }
    std::vector<int64_t> expect;
    int64_t off = 0;
    for (int i = 0; i < 8; ++i) {   // 8 batches of 250 records (~15 KB each); every 5th record matches
        std::vector<PlayerRow> rows;
        for (int j = 0; j < 250; ++j, ++off) { rows.push_back(PlayerRow{.worldId = j % 5 == 0 ? 7u : 1u}); if (j % 5 == 0) expect.push_back(off); }
        appendRows(logB, rows);
    }
    openEgress(3416);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(1, kTopic, 0, 0, &logA)));              // unfiltered, saturates the budget
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 1, 0, &logB, f)));        // filtered, small
    FilterCollector c;
 // A few duty cycles, no sleeps: the passes alternate, so the filtered one runs first within two cycles.
    for (int k = 0; k < 10 && c.offsets(2).size() < expect.size(); ++k) step(w, c);
    for (int k = 0; k < 20; ++k) { step(w, c); std::this_thread::sleep_for(200us); }
    ASSERT_EQ(c.offsets(2).size(), expect.size())
        << "filtered starved: " << c.offsets(2).size() << "/" << expect.size() << " while unfiltered delivered " << c.dataFrames(1);
    EXPECT_LT(c.dataFrames(1), static_cast<std::size_t>(kBig)) << "the unfiltered backlog must still be pending (the load was real)";
    EXPECT_GT(c.dataFrames(1), 0u) << "and the unfiltered pass was not starved either";
    EXPECT_EQ(c.offsets(2), expect);
    std::filesystem::remove_all(dirA);
    std::filesystem::remove_all(dirB);
}


// ---- fix round 1: schema replacement, owed bind errors, the isolation-carrier wait, disk cursors ----
rune::codec::Descriptor incompatiblePlayerV1() {   // player_event v1 with `channel` widened to Int64: Incompatible with the installed one
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    return DescriptorBuilder("player_event")
        .addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, true, {})
        .addTagged("worldId", 1, PrimType::UInt32, 1, 0xFFFF, false, {})
        .addTagged("channel", 2, PrimType::Int64, 1, 0xFFFF, false, {})
        .addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false)
        .addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3})
        .addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {})
        .addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("name", 7, PrimType::String, 1, 0xFFFF, false, {})
        .addTagged("pos", 8, PrimType::Float64, 1, 0xFFFF, false, {})
        .build();
}

TEST_F(WorkerFilterRegression, IncompatibleFilterSchemaRecompilesMembersAtTheirPositionRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    auto model = [](int64_t from, int64_t to, int r) {   // offset o has world {7, 9, 1}[o % 3]
        std::vector<int64_t> v;
        for (int64_t o = from; o < to; ++o) if (o % 3 == r) v.push_back(o);
        return v;
    };
    for (int i = 0; i < 6; ++i) appendRows(log, worlds({7, 9, 1}), 1);
    openEgress(3420);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f7 = worldEq(7), f9 = worldEq(9);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 1)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f7)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 6 && caughtUp(w, 1, 18); }));
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({7, 9, 1}), 1);   // A advances while InSync: its own cursor goes stale
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 10 && caughtUp(w, 1, 30); }));
    ASSERT_TRUE(std::holds_alternative<filter::InSync>(w.filterGroupOfSubForTest(1)->stateOf(0)));
    // B binds Behind (nothing delivered yet) in the same drain as the conflicting schema.
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &log, f9)));
    WorkerOp bad = schemaOp(kTopic, 1);
    bad.schemaUpdateBlob = rune::codec::serialize(incompatiblePlayerV1());
    ASSERT_TRUE(w.tryEnqueueOp(std::move(bad)));
    step(w, c);
    EXPECT_EQ(w.filterSchemaReplacementsForTest(), 1u) << "the conflicting schema must replace the installed one";
    for (int i = 0; i < 2; ++i) appendRows(log, worlds({7, 9, 1}), 1);
    const int64_t end = log.logEndOffset();
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == model(0, end, 0).size() && c.offsets(2).size() == model(0, end, 1).size(); }))
        << "A=" << c.offsets(1).size() << " B=" << c.offsets(2).size();
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(1), model(0, end, 0)) << "no duplicates, no gaps after the recompile";
    EXPECT_EQ(c.offsets(2), model(0, end, 1));
    EXPECT_EQ(w.filterBindFailuresForTest(), 0u) << "no member may be failed by a schema replacement";
    EXPECT_TRUE(c.errors.empty());
    ASSERT_NE(w.filterGroupOfSubForTest(1), nullptr);
    EXPECT_EQ(w.filterBytesHeldForTest(), 2u) << "both members keep their bytes for a later replacement";
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, MalformedFilterSchemaKeepsTheOldSchemaRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    openEgress(3421);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    WorkerOp bad = schemaOp(kTopic, 2);
    bad.schemaUpdateBlob = {0xde, 0xad, 0xbe, 0xef, 1, 2, 3};
    ASSERT_TRUE(w.tryEnqueueOp(std::move(bad)));
    FilterCollector c;
    step(w, c);
    EXPECT_EQ(w.filterSchemaErrorsForTest(), 1u);
    appendRows(log, worlds({7, 1, 7}));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 2; })) << "delivery continues under the old schema";
    EXPECT_EQ(c.offsets(1), (std::vector<int64_t>{0, 2}));
    EXPECT_EQ(w.filterBindFailuresForTest(), 0u);
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, BindErrorIsRetriedUntilSentUnderBackPressureRegression) {
    auto dirA = tempLogDir();
    rune::storage::Log logA(dirA.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 120; ++i) {   // an unfiltered backlog that fills the egress while nobody polls
        const int64_t base = logA.logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(4 * 1024, 'x'), 1000);
        const auto bytes = b.build();
        logA.appendNoSync(bytes.data(), bytes.size(), base);
    }
    openEgress(3422);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(1, 11, 0, 0, &logA)));
    FilterCollector c;
    for (int i = 0; i < 300; ++i) { w.doWork(nowNs()); std::this_thread::sleep_for(100us); }   // no polling: the egress saturates
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, 12, 0, 0, &logA, f)));   // no FilterSchema for topic 12
    for (int i = 0; i < 20; ++i) { w.doWork(nowNs()); std::this_thread::sleep_for(100us); }
    bool owed = false;
    for (const auto& s : w.subscriptionsForTest()) if (s.subscriptionId == 2) owed = s.pushState == Worker::WorkerSubscription::PushState::ErrorOwed;
    ASSERT_TRUE(owed) << "precondition: the egress was back-pressured, so the error is still owed";
    EXPECT_EQ(w.filterBindFailuresForTest(), 1u);
    ASSERT_TRUE(pump(w, c, [&] { return !c.errors.empty(); })) << "the owed bind error never arrived (a hang)";
    settle(w, c, 100ms);
    ASSERT_EQ(c.errors.size(), 1u) << "exactly one";
    EXPECT_EQ(c.errors[0].code, static_cast<int16_t>(pc::ErrorCode::InvalidRequest));
    EXPECT_NE(c.errors[0].message.find("subscription 2"), std::string::npos) << c.errors[0].message;
    EXPECT_EQ(w.filterBytesHeldForTest(), 0u) << "a failed bind must not keep its filter bytes or debt";
    std::filesystem::remove_all(dirA);
}

TEST_F(WorkerFilterRegression, GroupMemberWaitsForTheIsolationCarrierBeforeFilteredDeliveryRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    BatchOpts txn8; txn8.transactional = true; txn8.producerId = 8;
    appendRows(log, worlds({7}));                 // @0 committed data
    appendRows(log, worlds({7}), 2, txn8);        // @1 aborted txn
    appendRows(log, worlds({7}));                 // @2 committed data
    NullTransportF t;
    rune::replication::PartitionReplica rep(leaderCfg(), rune::replication::Role::Leader, 1, {1}, {1}, &log, &t);
    rep.onLeaderLogAppended();
    openEgress(3423);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    w.setOwnedDirect(Worker::OwnedSet{&rep});
    FilterCollector c;
    ASSERT_TRUE(w.tryEnqueueOp(markerOp(&rep, 8, /*commit=*/false)));   // abort @3
    for (int i = 0; i < 8; ++i) step(w, c);
    rep.onLeaderLogAppended();
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    auto groupSub = [&](int64_t id, bool carrier, uint8_t iso) {
        WorkerOp op = filteredSubscribeOp(id, kTopic, 0, 0, &log, f);
        op.replica = &rep; op.isolation = iso; op.isGroup = true; op.groupCarrierReceived = carrier;
        return op;
    };
    // A: carrier arrives as read_uncommitted -> every match, aborted one included, exactly once.
    ASSERT_TRUE(w.tryEnqueueOp(groupSub(1, false, 1)));
    settle(w, c, 60ms);
    EXPECT_TRUE(c.offsets(1).empty()) << "nothing may be delivered before the isolation is known";
    EXPECT_EQ(w.subscriptionPositionForTest(1), 0);
    ASSERT_TRUE(w.tryEnqueueOp(groupSub(1, true, 0)));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() >= 3; }));
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(1), (std::vector<int64_t>{0, 1, 2}));
    // B: carrier confirms read_committed -> only the committed matches.
    ASSERT_TRUE(w.tryEnqueueOp(groupSub(2, false, 1)));
    settle(w, c, 60ms);
    EXPECT_TRUE(c.offsets(2).empty());
    ASSERT_TRUE(w.tryEnqueueOp(groupSub(2, true, 1)));
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(2).size() >= 2; }));
    settle(w, c, 50ms);
    EXPECT_EQ(c.offsets(2), (std::vector<int64_t>{0, 2}));
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, FilteredDiskCursorBelowTheLogStartIsRaisedRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 256);   // tiny roll: many sealed segments
    log.recover();
    for (int i = 0; i < 40; ++i) appendRows(log, worlds({7, 1}));
    ASSERT_GT(log.deleteOldSegments(/*retainBytes=*/0), 0);
    const int64_t start = log.logStartOffset();
    ASSERT_GT(start, 0);
    ASSERT_TRUE(log.read(0, 1 << 20).empty());
    openEgress(3424);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));   // Earliest
    FilterCollector c;
    std::vector<int64_t> expect;
    for (int64_t o = start; o < 80; o += 2) expect.push_back(o);   // world 7 is the even offset of each batch
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == expect.size(); })) << "stalled below the log start: " << c.offsets(1).size();
    EXPECT_EQ(c.offsets(1), expect);
    std::filesystem::remove_all(dir);
}

// ---- Task 16: the unfiltered pass walks a list once a filtered group exists; every mutation must invalidate it ----
TEST_F(WorkerFilterRegression, UnfilteredSubscribeAddedAfterAFilteredGroupExistsReceivesDataRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({7, 1}));
    openEgress(3425);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 4; }));
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr) << "precondition: a filtered group is alive";
    settle(w, c, 20ms);   // the unfiltered pass has already walked its list at least once without sub 2
    // Only now does the unfiltered subscription arrive; it must be served from the start, and keep being served.
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(2, kTopic, 0, 0, &log)));
    ASSERT_TRUE(pump(w, c, [&] { return c.dataFrames(2) == 4; })) << "unfiltered frames: " << c.dataFrames(2) << "/4";
    appendRows(log, worlds({7, 1}));
    ASSERT_TRUE(pump(w, c, [&] { return c.dataFrames(2) == 5 && c.offsets(1).size() == 5; }))
        << "the late unfiltered subscription stopped being served: " << c.dataFrames(2) << "/5";
    EXPECT_TRUE(c.notes(2).empty());
    std::filesystem::remove_all(dir);
}

TEST_F(WorkerFilterRegression, FilteredTooLargeOwedErrorIsRetriedUnderBackPressureWhileTheGroupLivesRegression) {
    auto dirA = tempLogDir();
    auto dirB = tempLogDir();
    rune::storage::Log logA(dirA.string(), 64LL * 1024 * 1024);
    rune::storage::Log logB(dirB.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 120; ++i) {   // an unfiltered backlog that fills the egress while nobody polls
        const int64_t base = logA.logEndOffset();
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", std::string(4 * 1024, 'x'), 1000);
        const auto bytes = b.build();
        logA.appendNoSync(bytes.data(), bytes.size(), base);
    }
    appendRows(logB, worlds({7, 9}));   // both members are InSync after this
    openEgress(3426);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f7 = worldEq(7), f9 = worldEq(9);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &logB, f7)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &logB, f9)));   // keeps the group alive
    FilterCollector c;
    ASSERT_TRUE(pump(w, c, [&] { return c.offsets(1).size() == 1 && c.offsets(2).size() == 1; }));
    ASSERT_TRUE(std::holds_alternative<filter::InSync>(w.filterGroupForTest(kTopic, 0, 0)->stateOf(0)));
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(3, 11, 0, 0, &logA)));
    for (int i = 0; i < 300; ++i) { w.doWork(nowNs()); std::this_thread::sleep_for(100us); }   // no polling: the egress saturates
    std::vector<PlayerRow> huge;
    for (int i = 0; i < 300; ++i) huge.push_back(PlayerRow{.worldId = 7});   // ~18 KB > the 8 KiB egress max message
    appendRows(logB, huge);
    for (int i = 0; i < 40; ++i) { w.doWork(nowNs()); std::this_thread::sleep_for(100us); }
    bool owed = false;
    for (const auto& s : w.subscriptionsForTest()) if (s.subscriptionId == 1) owed = s.pushState == Worker::WorkerSubscription::PushState::ErrorOwed;
    ASSERT_TRUE(owed) << "precondition: the egress was back-pressured, so the TooLarge error is still owed";
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    ASSERT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 2u) << "precondition: another filtered member keeps the group alive";
    ASSERT_TRUE(pump(w, c, [&] {
        for (const auto& e : c.errors) if (e.code == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge)) return true;
        return false;
    })) << "the owed TooLarge error never arrived (a hang)";
    settle(w, c, 100ms);
    std::size_t tooLarge = 0;
    for (const auto& e : c.errors) tooLarge += e.code == static_cast<int16_t>(pc::ErrorCode::MessageTooLarge);
    EXPECT_EQ(tooLarge, 1u) << "exactly one";
    std::filesystem::remove_all(dirA);
    std::filesystem::remove_all(dirB);
}


// ===== Final review M2: a FILTERED subscription ended by the push owner (issue #109) =====
/// Every frame in arrival order: (subId or -corr for an error, code, record offsets).
struct OrderedCollector {
    struct Ev { bool isErr; int64_t subId; int64_t corr; int16_t code; std::vector<int64_t> offs; };
    std::vector<Ev> evs;
    aeron::FragmentAssembler fa;
    OrderedCollector()
        : fa([this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off, aeron::util::index_t len, aeron::Header&) {
              const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
              const auto n = static_cast<std::size_t>(len);
              if (auto r = pc::Records::decode(p, n)) {
                  Ev e{false, r->subscriptionId, r->correlationId, 0, {}};
                  if (!r->recordBatch.empty()) {
                      rune::protocol::RecordBatchReader rd(r->recordBatch.data(), r->recordBatch.size());
                      rd.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { e.offs.push_back(o); });
                  }
                  evs.push_back(std::move(e));
              } else if (auto er = pc::ErrorResponse::decode(p, n)) {
                  evs.push_back(Ev{true, 0, er->correlationId, er->code, {}});
              }
          }) {}
    std::vector<int64_t> offsets(int64_t sub) const {
        std::vector<int64_t> v;
        for (const auto& e : evs) if (!e.isErr && e.subId == sub) v.insert(v.end(), e.offs.begin(), e.offs.end());
        return v;
    }
    int endIndex(int64_t corr, int16_t code) const {
        for (std::size_t i = 0; i < evs.size(); ++i) if (evs[i].isErr && evs[i].corr == corr && evs[i].code == code) return static_cast<int>(i);
        return -1;
    }
    int lastRecordsIndex(int64_t sub) const {
        int k = -1;
        for (std::size_t i = 0; i < evs.size(); ++i) if (!evs[i].isErr && evs[i].subId == sub) k = static_cast<int>(i);
        return k;
    }
};

class WorkerFilterEndedRegression : public WorkerFilterFixture {
protected:
    void ostep(Worker& w, OrderedCollector& c) { w.doWork(nowNs()); sub_->poll(c.fa.handler(), 64); }
    template <typename P> bool opump(Worker& w, OrderedCollector& c, P p, std::chrono::milliseconds t = 10s) {
        const auto d = std::chrono::steady_clock::now() + t;
        while (std::chrono::steady_clock::now() < d) { ostep(w, c); if (p()) return true; }
        ostep(w, c); return p();
    }
    static WorkerOp endOp(int64_t subId, int16_t code) {
        WorkerOp op;
        op.type = WorkerOp::Type::Unsubscribe;
        op.subscriptionId = subId;
        op.endCode = code;
        op.endDetail = "probe: READ revoked";
        return op;
    }
};

// End the FIRST of three filtered members (index 0): the sweep's swap-and-pop moves sub 3 into slot 0.
TEST_F(WorkerFilterEndedRegression, EndedFilteredSubSendsEndAfterLastRecordsLeavesGroupNoMisdeliveryRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    auto model = [](int64_t from, int64_t to, uint32_t world) {
        std::vector<int64_t> v;
        for (int64_t o = from; o < to; ++o) if (static_cast<uint32_t>(o % 3) + 1 == world) v.push_back(o);
        return v;
    };
    for (int i = 0; i < 10; ++i) appendRows(log, worlds({1, 2, 3}));
    openEgress(3491);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f1 = worldEq(1), f2 = worldEq(2), f3 = worldEq(3);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f1)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &log, f2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(3, kTopic, 0, 0, &log, f3)));
    OrderedCollector c;
    const int64_t end1 = log.logEndOffset();
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(1).size() == model(0, end1, 1).size() && c.offsets(2).size() == model(0, end1, 2).size() &&
                                         c.offsets(3).size() == model(0, end1, 3).size(); }));
    // More data is in the log when the end arrives (the member is InSync and has matches pending).
    for (int i = 0; i < 5; ++i) appendRows(log, worlds({1, 2, 3}));
    ASSERT_TRUE(w.tryEnqueueOp(endOp(1, 46)));
    for (int i = 0; i < 5; ++i) appendRows(log, worlds({1, 2, 3}));
    const int64_t end2 = log.logEndOffset();
    ASSERT_TRUE(opump(w, c, [&] { return c.endIndex(9001, 46) >= 0 && c.offsets(2).size() == model(0, end2, 2).size() &&
                                         c.offsets(3).size() == model(0, end2, 3).size(); }))
        << "end=" << c.endIndex(9001, 46) << " sub2=" << c.offsets(2).size() << " sub3=" << c.offsets(3).size();
    for (int i = 0; i < 50; ++i) { ostep(w, c); std::this_thread::sleep_for(200us); }
    EXPECT_GT(c.endIndex(9001, 46), c.lastRecordsIndex(1)) << "Records for sub 1 after its terminal frame";
    EXPECT_EQ(w.subscriptionEndsSentForTest(), 1u);
    EXPECT_EQ(w.subscriptionEndsOwedForTest(), 0u);
    ASSERT_EQ(w.subscriptionsForTest().size(), 2u) << "the ended filtered subscription is swept";
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 2u) << "the ended member left its filter group";
    // sub 1's records are a prefix of its model (nothing out of order, nothing foreign).
    const auto got1 = c.offsets(1);
    const auto all1 = model(0, end2, 1);
    ASSERT_LE(got1.size(), all1.size());
    EXPECT_TRUE(std::equal(got1.begin(), got1.end(), all1.begin())) << "sub 1 got a non-prefix";
    // After the sweep moved sub 3 into slot 0: new data, exact delivery to 2 and 3, nothing to 1.
    for (int i = 0; i < 6; ++i) appendRows(log, worlds({1, 2, 3}));
    const int64_t end3 = log.logEndOffset();
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(2).size() == model(0, end3, 2).size() && c.offsets(3).size() == model(0, end3, 3).size(); }));
    for (int i = 0; i < 50; ++i) { ostep(w, c); std::this_thread::sleep_for(200us); }
    EXPECT_EQ(c.offsets(2), model(0, end3, 2));
    EXPECT_EQ(c.offsets(3), model(0, end3, 3));
    EXPECT_EQ(c.offsets(1), got1) << "nothing more for the ended subscription";
    std::filesystem::remove_all(dir);
}

// The SOLE filtered member ends: the group is released and the worker returns to the unfiltered-only push path.
TEST_F(WorkerFilterEndedRegression, EndedSoleFilteredMemberReleasesTheGroupRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({7, 1}));
    openEgress(3492);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(1, kTopic, 0, 0, &log, f)));
    OrderedCollector c;
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(1).size() == 4; }));
    ASSERT_TRUE(w.tryEnqueueOp(endOp(1, 45)));
    ASSERT_TRUE(opump(w, c, [&] { return c.endIndex(9001, 45) >= 0 && w.subscriptionsForTest().empty(); }));
    EXPECT_EQ(w.filterGroupCountForTest(), 0u) << "the empty group is released";
    ASSERT_TRUE(w.tryEnqueueOp(plainSubscribeOp(2, kTopic, 0, 0, &log)));
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(2).size() == 8; }));
    std::filesystem::remove_all(dir);
}

// End + the session's egress release in the same op batch (a revoked session's close): the end is still sent, the
// release proceeds, and the filtered member is gone from its group.
TEST_F(WorkerFilterEndedRegression, EndedFilteredSubThenEgressReleaseLeavesTheGroupRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    for (int i = 0; i < 4; ++i) appendRows(log, worlds({7, 1}));
    openEgress(3493);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7), f1 = worldEq(1);
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    WorkerOp a = filteredSubscribeOp(1, kTopic, 0, 0, &log, f);
    a.egressPub = eg_->pub();
    ASSERT_TRUE(w.tryEnqueueOp(a));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(2, kTopic, 0, 0, &log, f1)));   // keeps the group alive
    OrderedCollector c;
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(1).size() == 4 && c.offsets(2).size() == 4; }));
    ASSERT_TRUE(w.tryEnqueueOp(endOp(1, 46)));
    WorkerOp rel;
    rel.type = WorkerOp::Type::Unsubscribe;
    rel.egressPub = eg_->pub();
    rel.sessionId = 4201;
    rel.correlationId = 5;
    ASSERT_TRUE(w.tryEnqueueOp(rel));
    ASSERT_TRUE(opump(w, c, [&] { return w.egressReleasesForTest() == 1u && w.subscriptionsForTest().size() == 1u; }))
        << "releases=" << w.egressReleasesForTest() << " subs=" << w.subscriptionsForTest().size();
    EXPECT_EQ(w.subscriptionEndsSentForTest(), 1u) << "sent=" << w.subscriptionEndsSentForTest();
    EXPECT_TRUE(opump(w, c, [&] { return c.endIndex(9001, 46) >= 0; }, 3s)) << "the owed end frame never arrived";
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), 1u);
    for (int i = 0; i < 3; ++i) appendRows(log, worlds({7, 1}));
    ASSERT_TRUE(opump(w, c, [&] { return c.offsets(2).size() == 7; }));
    EXPECT_EQ(c.offsets(1).size(), 4u);
    std::filesystem::remove_all(dir);
}

// Final review M1: an ended filtered member frees its filter-group slot when the ending op executes (the gateway releases
// its admission count at the same point), so a Subscribe queued right behind it is admitted at the 4,096 cap.
TEST_F(WorkerFilterEndedRegression, EndedFilteredMemberFreesItsSlotAtTheCapForTheNextSubscribeRegression) {
    auto dir = tempLogDir();
    rune::storage::Log log(dir.string(), 64LL * 1024 * 1024);
    appendRows(log, worlds({1}));   // world 7 never matches: nothing is pushed
    openEgress(3494);
    Worker w(0, kRingBytes, kRingBytes);
    w.setWorkerEgress(eg_.get());
    const Fb f = worldEq(7);
    constexpr int kCap = 4096;
    ASSERT_TRUE(w.tryEnqueueOp(schemaOp(kTopic, 2)));
    OrderedCollector c;
    for (int base = 1; base <= kCap; base += 128) {
        for (int id = base; id < base + 128; ++id) ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(id, kTopic, 0, 0, &log, f)));
        ostep(w, c);
    }
    ASSERT_NE(w.filterGroupForTest(kTopic, 0, 0), nullptr);
    ASSERT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), static_cast<uint32_t>(kCap));
    ASSERT_EQ(w.filterBindFailuresForTest(), 0u);
    // One op batch: the end of sub 1, then a new filtered Subscribe (FIFO: the end runs first).
    ASSERT_TRUE(w.tryEnqueueOp(endOp(1, 46)));
    ASSERT_TRUE(w.tryEnqueueOp(filteredSubscribeOp(kCap + 1, kTopic, 0, 0, &log, f)));
    w.doWork(nowNs());
    EXPECT_EQ(w.filterBindFailuresForTest(), 0u) << "the new Subscribe hit the cap: the ended member still held a slot";
    EXPECT_EQ(w.filterGroupForTest(kTopic, 0, 0)->memberCount(), static_cast<uint32_t>(kCap));
    EXPECT_EQ(w.subscriptionEndsSeenForTest(), 1u);
    std::filesystem::remove_all(dir);
}

}  // namespace
