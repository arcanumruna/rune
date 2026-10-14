// Catalog history (Task 13): the client's CatalogReader — Subscribe(-6) from the
// cache end, one record per Records frame, the caught-up marker, the verifying
// replay with the shared library, the bounded marker wait (Task 12 F1), and the
// cached prefix re-validated after a truncation ended the reads (Task 12: the
// retriable NotLeader on the Subscribe correlation id).
// Non-blocking on the single-threaded fake (F7): the test thread drives
// client.doWork() + gw.pump() and the reader's beginRefresh / pollRefresh; the
// blocking catalogHistory / catalogAsOf / catalogDiff run against a real
// BrokerNode (rune-broker: ClientCatalogReadBrokerRegressionTest).
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"
#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/client/Client.h>
#include <rune/client/catalog/CatalogReader.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace pc   = rune::protocol::client;
namespace rc   = rune::client;
namespace rcat = rune::catalog;

namespace {
int64_t g_now = 1'000'000'000;   // test thread only: invoker mode has one thread
int64_t fakeNow() noexcept { return g_now; }
int g_ctr = 0;                   // test thread only

rc::ClientConfig invokerCfg(int32_t in, int32_t eg) {   // as Task 5's invokerCfg, plus the clock
    rc::ClientConfig c;
    c.gatewayChannel = "aeron:ipc"; c.gatewayStreamId = in; c.egressChannel = "aeron:ipc"; c.egressStreamId = eg;
    c.dataDir = "/tmp/catalog_reader_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
    c.runnerMode = rc::RunnerMode::Invoker;
    c.nanoClock = &fakeNow;
    return c;
}
struct Rig {   // Task 5's Rig: client + fake on the client's embedded driver
    rc::Client                   client;
    test_support::InvokerGateway gw;
    explicit Rig(const rc::ClientConfig& c)
        : client(c), gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {}
    template <class P> bool drive(P&& pred, int64_t stepNs = 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            g_now += stepNs; client.doWork(); gw.pump();
            if (pred()) return true;
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client.connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

/// Serves `__catalog` like a broker's CatalogReadServer: a Subscribe on topic −6
/// is answered SubscribeResponse{OK}, then every event from the requested offset —
/// one record per Records frame — then the empty-payload caught-up marker at the
/// end.  Records each fromOffset; can withhold the marker, end the reads with the
/// retriable NotLeader (a truncation below a served HW), refuse, or lose the
/// session after N records.
class CatalogFeed {
public:
    CatalogFeed(test_support::InvokerGateway& gw, std::vector<rcat::CatalogEvent> log) : gw_(gw), log_(std::move(log)) {
        gw_.templateHandlers[pc::Subscribe::TEMPLATE_ID] = [this](int ep, const uint8_t* f, std::size_t len) {
            onSubscribe(ep, f, len);
        };
    }
    void append(std::vector<rcat::CatalogEvent> more) {
        for (auto& e : more) log_.push_back(std::move(e));
    }
    /// The committed history changed under the cache (an unclean leader upstream).
    void replaceHistory(std::vector<rcat::CatalogEvent> log) { log_ = std::move(log); }
    void suppressCaughtUpMarker(bool on = true) { noMarker_ = on; }
    /// The next Subscribe is bound, then ended NotLeader (Task 12's truncation end).
    void endReadsOnNextSubscribe() { endNext_ = true; }
    void refuseNextSubscribe(pc::ErrorCode code) { refuseNext_ = code; }
    /// The next Subscribe gets `n` records, then its session expires.
    void loseSessionAfter(std::size_t n) { loseAfter_ = n; }
    /// The next Subscribe gets `n` records, then nothing (no marker).
    void stallAfter(std::size_t n) { stallAfter_ = n; }
    [[nodiscard]] const std::vector<int64_t>& subscribeFromOffsets() const noexcept { return froms_; }

private:
    void onSubscribe(int ep, const uint8_t* f, std::size_t len) {
        namespace rcc = rune::client::codec;
        const auto sv = rcc::decodeSubscribe(f, len);
        ASSERT_TRUE(sv.has_value());
        ASSERT_EQ(sv->topicId, pc::kCatalogTopicId);
        ASSERT_EQ(sv->partition, 0);
        ASSERT_EQ(sv->partitionKnown, 1);
        froms_.push_back(sv->fromOffset);
        const int64_t sid = sv->sessionId, corr = sv->correlationId;
        uint8_t out[64];
        if (refuseNext_) {
            const auto code = static_cast<int16_t>(*refuseNext_);
            refuseNext_.reset();
            gw_.send(ep, out, rcc::encodeSubscribeResponse(out, sizeof out, code, 0, 0, sid, corr));
            return;
        }
        const int64_t subId = nextSubId_++;
        gw_.send(ep, out, rcc::encodeSubscribeResponse(out, sizeof out, 0, 0, subId, sid, corr));
        if (endNext_) {
            endNext_ = false;
            gw_.sendError(ep, static_cast<int16_t>(pc::ErrorCode::NotLeader), "", sid, corr);
            return;
        }
        const int64_t from = sv->fromOffset < 0 ? 0 : std::min<int64_t>(sv->fromOffset, std::ssize(log_));
        std::size_t sent = 0;
        for (int64_t off = from; off < std::ssize(log_); ++off) {
            if (loseAfter_ && sent == *loseAfter_) {
                loseAfter_.reset();
                gw_.expireSession(sid);
                return;
            }
            if (stallAfter_ && sent == *stallAfter_) {
                stallAfter_.reset();
                return;
            }
            push(ep, subId, sid, corr, off, log_[static_cast<std::size_t>(off)]);
            ++sent;
        }
        if (!noMarker_) pushMarker(ep, subId, sid, corr, std::ssize(log_));
    }
    void push(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t off, const rcat::CatalogEvent& e) {
        namespace rcc = rune::client::codec;
        const auto bytes = rcat::encode(e);
        ASSERT_TRUE(bytes.has_value());
        rune::protocol::RecordBatchBuilder b(off, 1'000'000);
        b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(bytes->data()), bytes->size()),
                 1'000'000);
        const std::vector<uint8_t> batch = b.build();
        std::vector<uint8_t> out(batch.size() + 256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), subId, pc::kCatalogTopicId, 0, off,
                                                 std::span<const uint8_t>(batch), sid, corr);
        ASSERT_GT(n, 0u);
        gw_.send(ep, out.data(), n);
    }
    void pushMarker(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t hw) {
        namespace rcc = rune::client::codec;
        uint8_t out[256];
        const std::size_t n = rcc::encodeRecords(out, sizeof out, subId, pc::kCatalogTopicId, 0, hw,
                                                 std::span<const uint8_t>(), sid, corr);
        ASSERT_GT(n, 0u);
        gw_.send(ep, out, n);
    }

    test_support::InvokerGateway&  gw_;
    std::vector<rcat::CatalogEvent> log_;
    std::vector<int64_t>            froms_;
    bool                            noMarker_ = false;
    bool                            endNext_  = false;
    std::optional<pc::ErrorCode>    refuseNext_;
    std::optional<std::size_t>      loseAfter_;
    std::optional<std::size_t>      stallAfter_;
    int64_t                         nextSubId_ = (int64_t{1} << 62) | 1;
};

/// Drive one non-blocking refresh to completion (or the 10 s hang guard).
std::optional<rc::CatalogResult<void>> refreshOnce(Rig& r, rc::CatalogReader& reader, int64_t budgetNs) {
    if (reader.beginRefresh() != rc::OpResult::Accepted) return std::nullopt;
    const int64_t deadline = g_now + budgetNs;
    std::optional<rc::CatalogResult<void>> done;
    r.drive([&] { done = reader.pollRefresh(g_now, deadline); return done.has_value(); }, /*stepNs=*/1'000'000);
    return done;
}

/// The library's own digest of `log` (offsets 0..n-1) — never hand-typed.
uint64_t libraryDigest(const std::vector<rcat::CatalogEvent>& log) {
    rcat::CatalogReplayer lib;
    for (std::size_t i = 0; i < log.size(); ++i) {
        if (!lib.feedDecoded(static_cast<int64_t>(i), log[i])) return 0;
    }
    return lib.state().digest();
}

/// `n` CreateTopic events t0..t{n-1}, then `diverge` events u{k}.. instead from `from` on —
/// valid fingerprints (built on a growing state, like the leader).
std::vector<rcat::CatalogEvent> topicsScript(int n, int divergeFrom = -1, std::size_t statementBytes = 0) {
    rcat::testing::golden_detail::ScriptBuilder b;
    for (int i = 0; i < n; ++i) {
        rcat::CreateTopicOps o;
        o.partitions = 1; o.replicationFactor = 1;
        const bool other = divergeFrom >= 0 && i >= divergeFrom;
        const std::string name = (other ? "u" : "t") + std::to_string(i);
        std::string stmt = "CREATE TOPIC " + name;
        if (statementBytes > stmt.size()) stmt.append(statementBytes - stmt.size(), ' ');
        b.createTopic(name, std::move(o), std::move(stmt));
    }
    return std::move(b).take();
}

void expectCacheIs(const rc::CatalogReader& reader, const std::vector<rcat::CatalogEvent>& log) {
    ASSERT_EQ(reader.events().size(), log.size());
    for (std::size_t i = 0; i < log.size(); ++i) {
        EXPECT_EQ(reader.events()[i].offset, static_cast<int64_t>(i));
        EXPECT_EQ(reader.events()[i].event, log[i]) << "event " << i;
    }
    EXPECT_EQ(reader.state().digest(), libraryDigest(log));
}
} // namespace

// ---- The brief's four (golden log: 23 events, offsets 0..22) ------------------------

TEST(CatalogReaderRegression, RefreshReplaysEveryEventAndStopsAtTheMarker) {
    Rig r(invokerCfg(3980, 3981)); r.connect();
    const auto golden = rcat::testing::goldenScriptV1();
    CatalogFeed feed(r.gw, golden);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(reader.events().size(), golden.size());
    rcat::CatalogReplayer lib;
    for (const auto& e : reader.events()) ASSERT_TRUE(lib.feedDecoded(e.offset, e.event));
    EXPECT_EQ(reader.state().digest(), lib.state().digest()) << "the reader replays with the shared library";
    EXPECT_EQ(reader.state().digest(), libraryDigest(golden));
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0}));
}

TEST(CatalogReaderRegression, SecondRefreshFetchesOnlyTheNewTail) {
    Rig r(invokerCfg(3982, 3983)); r.connect();
    auto log = rcat::testing::goldenScriptV1();
    CatalogFeed feed(r.gw, {log.begin(), log.begin() + 10});
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.append({log.begin() + 10, log.end()});
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10})) << "the replayed prefix is cached";
    EXPECT_EQ(reader.events().size(), log.size());
    expectCacheIs(reader, log);
}

TEST(CatalogReaderRegression, TamperedEventIsAReplayErrorNamingItsOffset) {
    Rig r(invokerCfg(3984, 3985)); r.connect();
    auto log = rcat::testing::goldenScriptV1();
    log[4].fingerprint ^= 1;
    CatalogFeed feed(r.gw, log);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Replay);
    EXPECT_EQ(done->error().offset, 4);
    EXPECT_NE(done->error().detail.find("offset 4"), std::string::npos) << done->error().detail;
    EXPECT_EQ(reader.events().size(), 4u) << "the verified prefix stays cached";
    // The next refresh retries from the verified prefix — and fails the same way:
    // explicit, never silently skipped.
    auto again = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(again.has_value());
    ASSERT_FALSE(again->has_value());
    EXPECT_EQ(again->error().offset, 4);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 4}));
    EXPECT_EQ(reader.events().size(), 4u);
}

TEST(CatalogReaderRegression, NoCaughtUpMarkerIsATimeoutNotAHang) {
    Rig r(invokerCfg(3986, 3987)); r.connect();
    CatalogFeed feed(r.gw, rcat::testing::goldenScriptV1());
    feed.suppressCaughtUpMarker();
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, /*budgetNs=*/500'000'000);
    ASSERT_TRUE(done.has_value()) << "the deadline ends it";
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Timeout);
    // A timed-out refresh leaves nothing in flight: the next one starts at once.
    feed.suppressCaughtUpMarker(false);
    auto next = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(next.has_value());
    EXPECT_TRUE(next->has_value()) << next->error().detail;
}

// ---- Binding 2: a truncation ended the reads — the cached prefix is re-validated ----

TEST(CatalogReaderRegression, TruncationEndOfReadsRevalidatesTheCacheAndDropsTheDivergedTailRegression) {
    Rig r(invokerCfg(3988, 3989)); r.connect();
    const auto a = topicsScript(10);
    const auto b = topicsScript(10, /*divergeFrom=*/6);   // equal 0..5, different 6..9
    ASSERT_EQ(a[5], b[5]);
    ASSERT_NE(a[6], b[6]);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, a);

    feed.replaceHistory(b);
    feed.endReadsOnNextSubscribe();
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 0}))
        << "the reads ended: re-read from 0, never resumed from the cache end";
    expectCacheIs(reader, b);   // never a stale event served
    EXPECT_EQ(reader.stats().readsEnded, 1u);
    EXPECT_EQ(reader.stats().prefixDivergences, 1u);
    EXPECT_FALSE(reader.needsRevalidation());
}

TEST(CatalogReaderRegression, TruncationToAShorterHistoryDropsTheTailNoLongerCommittedRegression) {
    Rig r(invokerCfg(3990, 3991)); r.connect();
    const auto a = topicsScript(10);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    const std::vector<rcat::CatalogEvent> shorter(a.begin(), a.begin() + 6);
    feed.replaceHistory(shorter);
    feed.endReadsOnNextSubscribe();
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    expectCacheIs(reader, shorter);
    EXPECT_EQ(reader.nextOffset(), 6);
}

TEST(CatalogReaderRegression, TruncationWithAnEqualPrefixKeepsTheCacheAndReadsOnlyNewEventsRegression) {
    Rig r(invokerCfg(3992, 3993)); r.connect();
    const auto all = topicsScript(12);
    CatalogFeed feed(r.gw, {all.begin(), all.begin() + 10});
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(all);
    feed.endReadsOnNextSubscribe();
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, all);
    EXPECT_EQ(reader.stats().revalidations, 1u);
    EXPECT_EQ(reader.stats().prefixDivergences, 0u) << "every cached event re-validated equal";
}

TEST(CatalogReaderRegression, RevalidationCutShortByTheDeadlineIsRevalidatedNextTimeRegression) {
    Rig r(invokerCfg(3994, 3995)); r.connect();
    const auto a = topicsScript(8);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.endReadsOnNextSubscribe();
    feed.stallAfter(3);   // the re-read stops mid-prefix: the marker is withheld (no catalog leader)
    auto cut = refreshOnce(r, reader, 500'000'000);
    ASSERT_TRUE(cut.has_value());
    ASSERT_FALSE(cut->has_value());
    EXPECT_EQ(cut->error().kind, rc::CatalogReadError::Kind::Timeout);
    EXPECT_TRUE(reader.needsRevalidation()) << "an unfinished re-validation is never taken as done";
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 8, 0, 0}))
        << "the next refresh re-validates from 0 again, not from the cache end";
    EXPECT_FALSE(reader.needsRevalidation());
    expectCacheIs(reader, a);
}

// ---- Every other way a read can end: explicit, never a hang -------------------------

TEST(CatalogReaderRegression, RefusedSubscribeIsAnExplicitRejectedError) {
    Rig r(invokerCfg(3996, 3997)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(3));
    feed.refuseNextSubscribe(pc::ErrorCode::UnknownTopic);   // a pre-v8 session (Task 12)
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Rejected);
    EXPECT_EQ(done->error().brokerErrorCode, static_cast<uint16_t>(pc::ErrorCode::UnknownTopic));
}

TEST(CatalogReaderRegression, LostSessionMidReadResubscribesFromTheCacheEnd) {
    Rig r(invokerCfg(3998, 3999)); r.connect();
    const auto log = rcat::testing::goldenScriptV1();
    CatalogFeed feed(r.gw, log);
    feed.loseSessionAfter(5);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 8'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    ASSERT_EQ(feed.subscribeFromOffsets().size(), 2u);
    EXPECT_EQ(feed.subscribeFromOffsets()[1], 5) << "resumed from the replayed prefix";
    EXPECT_GE(reader.stats().resubscribes, 1u);
    expectCacheIs(reader, log);
}

TEST(CatalogReaderRegression, ARecordLargerThanOneRingMessageIsReassembled) {
    Rig r(invokerCfg(4000, 4001)); r.connect();
    const auto log = topicsScript(2, -1, /*statementBytes=*/20'000);   // > the 16 KiB message of a 128 KiB ring
    CatalogFeed feed(r.gw, log);
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.ringBytes = 128 << 10});
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    expectCacheIs(reader, log);
    EXPECT_EQ(reader.stats().reassembled, 2u) << "each record crossed the ring as a frame sequence";
    EXPECT_EQ(reader.stats().ringOverflows, 0u);
}

TEST(CatalogReaderRegression, AFullRingResubscribesFromTheCacheEndAndLosesNothing) {
    Rig r(invokerCfg(4002, 4003)); r.connect();
    const auto log = topicsScript(60);
    CatalogFeed feed(r.gw, log);
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.ringBytes = 2048});
    auto done = refreshOnce(r, reader, 8'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_GT(reader.stats().ringOverflows, 0u) << "the ring really was full";
    expectCacheIs(reader, log);   // every event once, in order
}

TEST(CatalogReaderRegression, AClosedClientEndsTheRefreshClosed) {
    Rig r(invokerCfg(4004, 4005)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(3));
    rc::CatalogReader reader(r.client);
    r.client.closeImmediately();
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Closed);
}

TEST(CatalogReaderRegression, ACacheBeyondMaxEventsIsAnExplicitCacheFull) {
    Rig r(invokerCfg(4006, 4007)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(5));
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.maxEvents = 3});
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::CacheFull);
    EXPECT_EQ(reader.events().size(), 3u);
}

TEST(CatalogReaderRegression, OneOwnerThread) {
    Rig r(invokerCfg(4008, 4009)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(1));
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    bool threw = false;
    std::thread([&] {
        try { (void)reader.beginRefresh(); } catch (const std::logic_error&) { threw = true; }
    }).join();
    EXPECT_TRUE(threw) << "a reader is owned by one app thread (shared-nothing)";
}

TEST(CatalogReaderRegression, BlockingRefreshOffTheInvokerOwnerThreadIsRefused) {
    Rig r(invokerCfg(4010, 4011)); r.connect();
    rc::CatalogReader reader(r.client);
    std::thread([&] {
        // An Invoker-mode Client: blocking only from its owner thread.
        EXPECT_THROW((void)reader.refresh(std::chrono::milliseconds(10)), std::logic_error);
    }).join();
}
