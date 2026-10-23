// Catalog history (Task 13): the client's CatalogReader — Subscribe(-6) from the
// cache end, the stream's history-epoch note, one record per Records frame, the
// caught-up marker (with the epoch), the verifying replay with the shared
// library, the bounded marker wait (Task 12 F1), and the cache (prefix, epoch):
// continued only under an unchanged epoch, else dropped and re-read from 0 (fix
// round 2 — a truncation below a served HW changes the broker's epoch).
// Non-blocking on the single-threaded fake (F7): the test thread drives
// client.doWork() + gw.pump() and the reader's beginRefresh / pollRefresh; the
// blocking catalogHistory / catalogAsOf / catalogDiff run against a real
// BrokerNode (rune-broker: ClientCatalogReadBrokerRegressionTest).
#include <filesystem>
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

#include <atomic>
#include <optional>
#include <stdexcept>
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
    c.dataDir = std::filesystem::temp_directory_path().string() + "/catalog_reader_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr++);
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
/// is answered SubscribeResponse{OK}, then the stream's history-epoch note
/// (Records{-1, epoch}), then every event from the requested offset — one record
/// per Records frame — then the caught-up marker (Records{HW, epoch}).  Records
/// each fromOffset; can withhold the marker, end the reads with the retriable
/// NotLeader (a truncation below a served HW), refuse, or lose the session after
/// N records.  replaceHistory() is a truncation below a served HW: it changes the
/// epoch (what CatalogReadServer does).
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
    /// The committed history changed under the cache (an unclean leader upstream):
    /// the broker's history epoch changes with it.
    void replaceHistory(std::vector<rcat::CatalogEvent> log) { log_ = std::move(log); ++epoch_; }
    /// Fix round 2: the epoch changes with no visible byte change (an identical-bytes regrow).
    void bumpEpoch() { ++epoch_; }
    /// Fix round 2: another broker (its own epoch) serves the same log.
    void setEpoch(uint64_t e) { epoch_ = e; }
    [[nodiscard]] uint64_t epoch() const noexcept { return epoch_; }
    /// Fix round 2: the history is replaced (epoch changed) right before the `k`-th
    /// Subscribe (0-based, counted over the whole feed) is served.
    void replaceHistoryAtSubscribe(std::size_t k, std::vector<rcat::CatalogEvent> log) {
        replaceAt_ = k;
        replaceWith_ = std::move(log);
    }
    /// Fix round 2: the next Subscribe's marker names this epoch (a lying broker).
    void overrideNextMarkerEpoch(uint64_t e) { markerEpochOverride_ = e; }
    /// Fix round 2: no epoch note (a broker older than the epoch).
    void omitNotes(bool on = true) { noNote_ = on; }
    void suppressCaughtUpMarker(bool on = true) { noMarker_ = on; }
    /// The next Subscribe is bound, then ended NotLeader (Task 12's truncation end).
    void endReadsOnNextSubscribe() { endNext_ = true; }
    void refuseNextSubscribe(pc::ErrorCode code) { refuseNext_ = code; }
    /// The next Subscribe gets `n` records, then its session expires.
    void loseSessionAfter(std::size_t n) { loseAfter_ = n; }
    /// The next Subscribe gets `n` records, then nothing (no marker).
    void stallAfter(std::size_t n) { stallAfter_ = n; }
    /// The next Subscribe's caught-up marker names `hw` instead of the log's end.
    void overrideNextMarker(int64_t hw) { markerOverride_ = hw; }
    /// EVERY Subscribe is refused (a busy / leaderless intake).
    void refuseAllSubscribes(pc::ErrorCode code) { refuseAll_ = code; }
    /// The next Subscribe's record at `offset` is bytes that do not decode.
    void corruptNextSubscribeAt(int64_t offset) { corruptAt_ = offset; }
    /// The whole backlog goes in ONE Records frame of back-to-back batches (what the broker's
    /// CatalogReadServer sends: it coalesces up to 64 KiB of whole batches per frame).
    void coalesceBacklog(bool on = true) { coalesce_ = on; }
    /// Every record is delivered twice (a duplicated answer).
    void duplicateEveryRecord(bool on = true) { dup_ = on; }
    /// The last stalled Subscribe's remaining records and marker arrive NOW (an answer after a timeout).
    void releaseStalled() {
        if (!stalled_) return;
        const Stalled st = *stalled_;
        stalled_.reset();
        for (int64_t off = st.next; off < std::ssize(log_); ++off)
            push(st.ep, st.subId, st.sid, st.corr, off, log_[static_cast<std::size_t>(off)]);
        pushMarker(st.ep, st.subId, st.sid, st.corr, std::ssize(log_));
    }
    [[nodiscard]] const std::vector<int64_t>& subscribeFromOffsets() const noexcept { return froms_; }

private:
    void onSubscribe(int ep, const uint8_t* f, std::size_t len) {
        namespace rcc = rune::client::codec;
        const auto sv = rcc::decodeSubscribe(f, len);
        ASSERT_TRUE(sv.has_value());
        ASSERT_EQ(sv->topicId, pc::kCatalogTopicId);
        ASSERT_EQ(sv->partition, 0);
        ASSERT_EQ(sv->partitionKnown, 1);
        if (replaceAt_ && *replaceAt_ == froms_.size()) {
            replaceAt_.reset();
            replaceHistory(std::move(replaceWith_));
        }
        froms_.push_back(sv->fromOffset);
        const int64_t sid = sv->sessionId, corr = sv->correlationId;
        uint8_t out[64];
        if (refuseNext_ || refuseAll_) {
            const auto code = static_cast<int16_t>(refuseNext_ ? *refuseNext_ : *refuseAll_);
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
        if (!noNote_) pushEpochFrame(ep, subId, sid, corr, -1, epoch_);   // every stream opens with it
        const int64_t from = sv->fromOffset < 0 ? 0 : std::min<int64_t>(sv->fromOffset, std::ssize(log_));
        if (coalesce_ && from < std::ssize(log_) && !loseAfter_ && !stallAfter_) {
            std::vector<uint8_t> frame;
            for (int64_t off = from; off < std::ssize(log_); ++off) {
                const auto bytes = rcat::encode(log_[static_cast<std::size_t>(off)]);
                ASSERT_TRUE(bytes.has_value());
                rune::protocol::RecordBatchBuilder b(off, 1'000'000);
                b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(bytes->data()), bytes->size()),
                         1'000'000);
                const auto one = b.build();
                frame.insert(frame.end(), one.begin(), one.end());
            }
            std::vector<uint8_t> out(frame.size() + 256);
            const std::size_t n = rcc::encodeRecords(out.data(), out.size(), subId, pc::kCatalogTopicId, 0, from,
                                                     std::span<const uint8_t>(frame), sid, corr);
            ASSERT_GT(n, 0u);
            gw_.send(ep, out.data(), n);
            if (!noMarker_) pushMarker(ep, subId, sid, corr, markerOverride_.value_or(std::ssize(log_)));
            markerOverride_.reset();
            return;
        }
        std::size_t sent = 0;
        for (int64_t off = from; off < std::ssize(log_); ++off) {
            if (loseAfter_ && sent == *loseAfter_) {
                loseAfter_.reset();
                gw_.expireSession(sid);
                return;
            }
            if (stallAfter_ && sent == *stallAfter_) {
                stallAfter_.reset();
                stalled_ = Stalled{ep, subId, sid, corr, off};
                return;
            }
            if (corruptAt_ && *corruptAt_ == off) {
                corruptAt_.reset();
                pushBytes(ep, subId, sid, corr, off, std::vector<uint8_t>{0xde, 0xad, 0xbe, 0xef, 0x01});
            } else {
                push(ep, subId, sid, corr, off, log_[static_cast<std::size_t>(off)]);
            }
            if (dup_) push(ep, subId, sid, corr, off, log_[static_cast<std::size_t>(off)]);
            ++sent;
        }
        const int64_t markerHw = markerOverride_.value_or(std::ssize(log_));
        markerOverride_.reset();
        if (!noMarker_) pushMarker(ep, subId, sid, corr, markerHw);
    }
    void push(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t off, const rcat::CatalogEvent& e) {
        namespace rcc = rune::client::codec;
        const auto bytes = rcat::encode(e);
        ASSERT_TRUE(bytes.has_value());
        pushBytes(ep, subId, sid, corr, off, std::vector<uint8_t>(bytes->begin(), bytes->end()));
    }
    void pushBytes(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t off, const std::vector<uint8_t>& bytes) {
        namespace rcc = rune::client::codec;
        rune::protocol::RecordBatchBuilder b(off, 1'000'000);
        b.append(std::string_view{}, std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()),
                 1'000'000);
        const std::vector<uint8_t> batch = b.build();
        std::vector<uint8_t> out(batch.size() + 256);
        const std::size_t n = rcc::encodeRecords(out.data(), out.size(), subId, pc::kCatalogTopicId, 0, off,
                                                 std::span<const uint8_t>(batch), sid, corr);
        ASSERT_GT(n, 0u);
        gw_.send(ep, out.data(), n);
    }
    void pushMarker(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t hw) {
        const uint64_t e = markerEpochOverride_.value_or(epoch_);
        markerEpochOverride_.reset();
        pushEpochFrame(ep, subId, sid, corr, hw, e);
    }
    void pushEpochFrame(int ep, int64_t subId, int64_t sid, int64_t corr, int64_t base, uint64_t e) {
        namespace rcc = rune::client::codec;
        uint8_t epochBytes[8];
        for (int i = 0; i < 8; ++i) epochBytes[i] = static_cast<uint8_t>(e >> (8 * i));
        uint8_t out[256];
        const std::size_t n = rcc::encodeRecords(out, sizeof out, subId, pc::kCatalogTopicId, 0, base,
                                                 std::span<const uint8_t>(epochBytes, 8), sid, corr);
        ASSERT_GT(n, 0u);
        gw_.send(ep, out, n);
    }

    test_support::InvokerGateway&  gw_;
    std::vector<rcat::CatalogEvent> log_;
    std::vector<int64_t>            froms_;
    bool                            noMarker_ = false;
    bool                            endNext_  = false;
    std::optional<pc::ErrorCode>    refuseNext_;
    std::optional<pc::ErrorCode>    refuseAll_;
    struct Stalled { int ep; int64_t subId, sid, corr, next; };
    std::optional<Stalled>          stalled_;
    bool                            dup_ = false;
    bool                            coalesce_ = false;
    std::optional<int64_t>          markerOverride_;
    std::optional<int64_t>          corruptAt_;
    std::optional<std::size_t>      loseAfter_;
    std::optional<std::size_t>      stallAfter_;
    int64_t                         nextSubId_ = (int64_t{1} << 62) | 1;
    uint64_t                        epoch_ = 0x5eed0000'00000001ULL;
    std::optional<uint64_t>         markerEpochOverride_;
    std::optional<std::size_t>      replaceAt_;
    std::vector<rcat::CatalogEvent> replaceWith_;
    bool                            noNote_ = false;
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
    EXPECT_EQ(reader.historyEpoch(), feed.epoch()) << "the cache is (prefix, the broker's epoch)";
}

TEST(CatalogReaderRegression, SecondRefreshFetchesOnlyTheNewTail) {
    Rig r(invokerCfg(3982, 3983)); r.connect();
    auto log = rcat::testing::goldenScriptV1();
    CatalogFeed feed(r.gw, {log.begin(), log.begin() + 10});
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.append({log.begin() + 10, log.end()});
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10}))
        << "the replayed prefix is cached: the epoch is unchanged, so the read continues at the cache end";
    expectCacheIs(reader, log);
    EXPECT_EQ(reader.stats().epochChanges, 0u);
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

// ---- A truncation below a served HW: the broker's epoch changes -------------------

TEST(CatalogReaderRegression, TruncationEndOfReadsRereadsUnderTheNewEpochAndServesOnlyTheNewHistoryRegression) {
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
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 10, 0}))
        << "ended: resubscribed at the cache end, whose note named a new epoch — dropped and re-read from 0";
    expectCacheIs(reader, b);   // never a stale event served
    EXPECT_EQ(reader.stats().readsEnded, 1u);
    EXPECT_EQ(reader.stats().epochChanges, 1u);
    EXPECT_EQ(reader.historyEpoch(), feed.epoch());
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

// An equal prefix under a new epoch is still re-read: the epoch, not the bytes, vouches.
TEST(CatalogReaderRegression, TruncationWithAnEqualPrefixStillRereadsUnderTheNewEpochRegression) {
    Rig r(invokerCfg(3992, 3993)); r.connect();
    const auto all = topicsScript(12);
    CatalogFeed feed(r.gw, {all.begin(), all.begin() + 10});
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(all);
    feed.endReadsOnNextSubscribe();
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, all);
    EXPECT_EQ(reader.stats().epochChanges, 1u);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 10, 0}));
}

// A re-read cut short by the deadline keeps what it read under the NEW epoch: the
// next refresh continues it (same epoch), never from 0 again, never stale.
TEST(CatalogReaderRegression, AReReadCutShortByTheDeadlineContinuesUnderItsEpochRegression) {
    Rig r(invokerCfg(3994, 3995)); r.connect();
    const auto a = topicsScript(8);
    const auto b = topicsScript(8, /*divergeFrom=*/2);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(b);
    feed.endReadsOnNextSubscribe();
    feed.stallAfter(3);   // the re-read stops mid-prefix: the marker is withheld (no catalog leader)
    auto cut = refreshOnce(r, reader, 500'000'000);
    ASSERT_TRUE(cut.has_value());
    ASSERT_FALSE(cut->has_value());
    EXPECT_EQ(cut->error().kind, rc::CatalogReadError::Kind::Timeout);
    EXPECT_EQ(reader.events().size(), 3u) << "only what was read under the new epoch";
    EXPECT_EQ(reader.historyEpoch(), feed.epoch());
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 8, 8, 0, 3}))
        << "the next refresh continues at the cache end (3): the epoch is unchanged";
    expectCacheIs(reader, b);
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
    EXPECT_EQ(feed.subscribeFromOffsets()[1], 5) << "resumed at the cache end: the epoch is unchanged";
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
    const auto& froms = feed.subscribeFromOffsets();
    for (std::size_t i = 1; i < froms.size(); ++i)
        EXPECT_GT(froms[i], froms[i - 1]) << "every continuation progresses (" << i << ")";
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
    CatalogFeed feed(r.gw, topicsScript(2));
    rc::CatalogReader reader(r.client);
    std::thread([&] {
        // An Invoker-mode Client: blocking only from its owner thread.
        EXPECT_THROW((void)reader.refresh(std::chrono::milliseconds(10)), std::logic_error);
    }).join();
    // M1: the refused caller claimed nothing — the real owner still reads.
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    EXPECT_TRUE(done->has_value()) << (done->has_value() ? "" : done->error().detail);
}

// ---- Fix round 1 (C1), under the epoch -----------------------------------------------

// REQUIRED (fix round 2): a truncation between two refreshes that regrew past the
// cache.  The reader holds no subscription between refreshes; the next stream's
// epoch note tells the history changed.  RED (the epoch ignored): u10, u11 were
// appended onto the stale t0..t9 and served.
TEST(CatalogReaderRegression, ATruncationBetweenRefreshesThatRegrewPastTheCacheIsNeverServedStaleRegression) {
    Rig r(invokerCfg(4012, 4013)); r.connect();
    const auto a = topicsScript(10);
    const auto b = topicsScript(12, /*divergeFrom=*/6);   // valid events (distinct names), different from offset 6 on
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(b);   // truncated + regrown while no read was live
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 0}))
        << "the stream at the cache end named a new epoch: the cache is dropped and the catalog re-read from 0";
    expectCacheIs(reader, b);
    EXPECT_EQ(reader.stats().epochChanges, 1u);
}

TEST(CatalogReaderRegression, ATruncationBetweenRefreshesThatShrankBelowTheCacheIsNeverServedStaleRegression) {
    Rig r(invokerCfg(4014, 4015)); r.connect();
    const auto a = topicsScript(10);
    const auto b = topicsScript(7, /*divergeFrom=*/3);   // committed end 7 < cache end 10; 3..6 differ
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(b);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 0}));
    expectCacheIs(reader, b);
}

TEST(CatalogReaderRegression, AnUnchangedEpochContinuesAtTheCacheEndRegression) {
    Rig r(invokerCfg(4016, 4017)); r.connect();
    const auto a = topicsScript(10);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10})) << "no overlap event, no re-read";
    expectCacheIs(reader, a);
    EXPECT_EQ(reader.stats().epochChanges, 0u);
}

// REQUIRED (fix round 2): the identical-bytes regrow.  The prefix is rewritten
// (offsets 2..8) while the event at the cache end (9) is re-committed byte for
// byte.  Event fingerprints are per object, not chained, so nothing in the bytes
// at the cache end tells; the epoch does.  RED (round 1's overlap check / the
// epoch ignored): the overlap matched and t2..t8 — no longer committed — were served.
TEST(CatalogReaderRegression, AnIdenticalBytesRegrowStillRereadsBecauseTheEpochChangedRegression) {
    Rig r(invokerCfg(4018, 4019)); r.connect();
    const auto a = topicsScript(10);
    auto b = topicsScript(10, /*divergeFrom=*/2);
    b[9] = a[9];   // the cache-end event, byte-identical
    ASSERT_NE(libraryDigest(b), 0u) << "precondition: b is a valid history";
    ASSERT_EQ(rcat::encode(a[9]).value(), rcat::encode(b[9]).value());
    ASSERT_NE(a[5], b[5]);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(b);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    expectCacheIs(reader, b);
    EXPECT_EQ(reader.stats().epochChanges, 1u);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 0}));
}

// REQUIRED (fix round 2): a truncation between an Append CONTINUATION (a ring
// overflow restarted the stream) and the next stream of the same refresh.  RED
// (the epoch ignored): the continuation appended the new history's tail onto the
// first stream's stale prefix.
TEST(CatalogReaderRegression, ATruncationBetweenAContinuationAndItsNextStreamIsNeverServedStaleRegression) {
    Rig r(invokerCfg(4020, 4021)); r.connect();
    const auto a = topicsScript(60);
    const auto b = topicsScript(64, /*divergeFrom=*/3);
    CatalogFeed feed(r.gw, a);
    feed.replaceHistoryAtSubscribe(1, b);   // right before the first continuation is served
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.ringBytes = 2048});
    auto done = refreshOnce(r, reader, 8'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    ASSERT_GE(reader.stats().ringOverflows, 1u) << "precondition: the first stream overflowed (a continuation)";
    expectCacheIs(reader, b);
    EXPECT_EQ(reader.stats().epochChanges, 1u);
    const auto& froms = feed.subscribeFromOffsets();
    ASSERT_GE(froms.size(), 3u);
    EXPECT_GT(froms[1], 0) << "the continuation started at the cache end";
    EXPECT_EQ(froms[2], 0) << "its note named a new epoch: re-read from 0";
}

// REQUIRED (fix round 2): ... and between a continuation cut short (the deadline)
// and the next refresh's RESUME.  RED (the epoch ignored): the resume appended the
// new history's tail onto the stale prefix the continuation had kept.
TEST(CatalogReaderRegression, ATruncationBetweenACutShortContinuationAndItsResumeIsNeverServedStaleRegression) {
    Rig r(invokerCfg(4022, 4023)); r.connect();
    const auto a = topicsScript(60);
    const auto b = topicsScript(66, /*divergeFrom=*/1);
    CatalogFeed feed(r.gw, a);
    feed.suppressCaughtUpMarker();   // the refresh never ends on its own: the deadline cuts it
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.ringBytes = 2048});
    auto cut = refreshOnce(r, reader, 1'000'000'000);
    ASSERT_TRUE(cut.has_value());
    ASSERT_FALSE(cut->has_value());
    ASSERT_EQ(cut->error().kind, rc::CatalogReadError::Kind::Timeout);
    const std::size_t kept = reader.events().size();
    ASSERT_GT(kept, 0u) << "precondition: the cut-short read kept a prefix";
    feed.suppressCaughtUpMarker(false);
    feed.replaceHistory(b);
    auto done = refreshOnce(r, reader, 8'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    expectCacheIs(reader, b);
    EXPECT_GE(reader.stats().epochChanges, 1u);
}

// A read from ANOTHER broker (another epoch value) never continues this cache.
TEST(CatalogReaderRegression, AStreamFromAnotherBrokersEpochDropsTheCacheRegression) {
    Rig r(invokerCfg(4024, 4025)); r.connect();
    const auto a = topicsScript(6);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.setEpoch(0xb0b);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 6, 0}));
    expectCacheIs(reader, a);
    EXPECT_EQ(reader.historyEpoch(), 0xb0bu);
}

// A marker whose epoch differs from its own stream's note, or whose HW lies below
// what the stream delivered, vouches for nothing: dropped, re-read.
TEST(CatalogReaderRegression, AMarkerContradictingItsStreamDropsTheCacheAndRereadsRegression) {
    Rig r(invokerCfg(4026, 4027)); r.connect();
    const auto log = topicsScript(5);
    CatalogFeed feed(r.gw, log);
    feed.overrideNextMarker(2);   // contradicts the 5 records just delivered
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 0})) << "never taken as 'caught up'";
    expectCacheIs(reader, log);
    EXPECT_EQ(reader.stats().contradictions, 1u);

    feed.overrideNextMarkerEpoch(feed.epoch() + 7);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 0, 5, 0}));
    EXPECT_EQ(reader.stats().contradictions, 2u);
    expectCacheIs(reader, log);
}

// A broker that sends records with no epoch note is refused explicitly — never
// cached under an unknown history, never a hang.
TEST(CatalogReaderRegression, RecordsWithoutAnEpochNoteAreAnExplicitRejectedRegression) {
    Rig r(invokerCfg(4028, 4029)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(3));
    feed.omitNotes();
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Rejected) << done->error().detail;
    EXPECT_TRUE(reader.events().empty());
    EXPECT_FALSE(reader.historyEpoch().has_value());
}

// I3: the NotLeader refusal (the broker's intake is busy).
TEST(CatalogReaderRegression, ANotLeaderRefusalBacksOffAndResubscribesWithinTheDeadlineRegression) {
    Rig r(invokerCfg(4030, 4031)); r.connect();
    const auto log = topicsScript(4);
    CatalogFeed feed(r.gw, log);
    feed.refuseNextSubscribe(pc::ErrorCode::NotLeader);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 0}));
    EXPECT_GE(reader.stats().resubscribes, 1u);
    expectCacheIs(reader, log);
}

TEST(CatalogReaderRegression, APersistentNotLeaderRefusalEndsTimeoutNotRejectedNotAHangRegression) {
    Rig r(invokerCfg(4032, 4033)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(4));
    feed.refuseAllSubscribes(pc::ErrorCode::NotLeader);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 500'000'000);
    ASSERT_TRUE(done.has_value()) << "the deadline ends the back-off loop";
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Timeout);
    EXPECT_GE(feed.subscribeFromOffsets().size(), 2u) << "it did retry";
    EXPECT_TRUE(reader.events().empty());
}

// M2 / M3 / M4.
TEST(CatalogReaderRegression, ARecordThatCanNeverCrossTheRingIsAnExplicitErrorNotATimeoutRegression) {
    Rig r(invokerCfg(4034, 4035)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(2, -1, /*statementBytes=*/20'000));
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.ringBytes = 1024});
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::RecordTooLarge) << done->error().detail;
    EXPECT_EQ(done->error().offset, 0);
}

TEST(CatalogReaderRegression, ACacheBeyondMaxBytesIsAnExplicitCacheFullRegression) {
    Rig r(invokerCfg(4036, 4037)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(6, -1, /*statementBytes=*/2000));
    rc::CatalogReader reader(r.client, rc::CatalogReaderConfig{.maxBytes = 5000});
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::CacheFull);
    EXPECT_LT(reader.events().size(), 6u);
}

TEST(CatalogReaderRegression, AnUndecodableRecordIsADecodeErrorAndKeepsTheVerifiedPrefixRegression) {
    Rig r(invokerCfg(4038, 4039)); r.connect();
    const auto a = topicsScript(10);
    CatalogFeed feed(r.gw, a);
    feed.corruptNextSubscribeAt(5);
    rc::CatalogReader reader(r.client);
    auto done = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Replay);
    EXPECT_EQ(done->error().offset, 5);
    EXPECT_EQ(reader.events().size(), 5u) << "a corrupt record is an error: the verified prefix stays";
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value()) << "the next read continues past it";
    expectCacheIs(reader, a);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 5}));
}

// Edge-case checklist: duplicate / stale / late answers, close and session loss mid-op, back-pressure.
TEST(CatalogReaderRegression, DuplicatedRecordsAreSkippedNotReappliedRegression) {
    Rig r(invokerCfg(4040, 4041)); r.connect();
    const auto a = topicsScript(8);
    CatalogFeed feed(r.gw, a);
    feed.duplicateEveryRecord();
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, a);
    feed.bumpEpoch();   // and through a re-read under a new epoch
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, a);
}

TEST(CatalogReaderRegression, AnAnswerArrivingAfterTheTimeoutNeverCorruptsTheNextRefreshRegression) {
    Rig r(invokerCfg(4042, 4043)); r.connect();
    const auto a = topicsScript(8);
    CatalogFeed feed(r.gw, a);
    feed.stallAfter(3);
    rc::CatalogReader reader(r.client);
    auto cut = refreshOnce(r, reader, 300'000'000);
    ASSERT_TRUE(cut.has_value());
    ASSERT_FALSE(cut->has_value());
    feed.releaseStalled();   // the old subscription's records + marker, late
    for (int i = 0; i < 2000; ++i) { g_now += 1'000'000; r.client.doWork(); r.gw.pump(); }
    auto next = refreshOnce(r, reader, 5'000'000'000);
    ASSERT_TRUE(next.has_value());
    ASSERT_TRUE(next->has_value()) << next->error().detail;
    expectCacheIs(reader, a);
}

TEST(CatalogReaderRegression, SessionLossDuringAReReadResumesAtTheCacheEndUnderTheNewEpochRegression) {
    Rig r(invokerCfg(4044, 4045)); r.connect();
    const auto a = topicsScript(10);
    const auto b = topicsScript(10, /*divergeFrom=*/7);
    CatalogFeed feed(r.gw, a);
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    feed.replaceHistory(b);
    feed.loseSessionAfter(4);   // the re-read from 0 gets 0..3, then the session expires
    auto done = refreshOnce(r, reader, 8'000'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_TRUE(done->has_value()) << done->error().detail;
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 10, 0, 4}))
        << "resumed at the cache end (4): the same epoch";
    expectCacheIs(reader, b);
}

TEST(CatalogReaderRegression, ClosingTheClientMidRefreshEndsClosedAndBeginIsBackPressuredWhileInFlightRegression) {
    Rig r(invokerCfg(4046, 4047)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(8));
    feed.stallAfter(2);
    rc::CatalogReader reader(r.client);
    ASSERT_EQ(reader.beginRefresh(), rc::OpResult::Accepted);
    EXPECT_EQ(reader.beginRefresh(), rc::OpResult::BackPressured) << "one refresh in flight";
    const int64_t deadline = g_now + 5'000'000'000;
    ASSERT_TRUE(r.drive([&] {   // mid-read: two events replayed, the marker withheld
        EXPECT_FALSE(reader.pollRefresh(g_now, deadline).has_value());
        return reader.events().size() >= 2;
    }, 1'000'000));
    r.client.closeImmediately();
    std::optional<rc::CatalogResult<void>> done;
    r.drive([&] { done = reader.pollRefresh(g_now, deadline); return done.has_value(); }, 1'000'000);
    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->has_value());
    EXPECT_EQ(done->error().kind, rc::CatalogReadError::Kind::Closed);
}

// A Records frame of SEVERAL whole batches (the broker's catalog server coalesces) must deliver every
// record: the hub decoded only the first batch and the rest vanished.
TEST(CatalogReaderRegression, ACoalescedMultiBatchFrameDeliversEveryRecordRegression) {
    Rig r(invokerCfg(4048, 4049)); r.connect();
    const auto a = topicsScript(8);
    CatalogFeed feed(r.gw, a);
    feed.coalesceBacklog();
    rc::CatalogReader reader(r.client);
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, a);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0})) << "no gap re-read: nothing was dropped";
    EXPECT_EQ(reader.stats().markerGaps, 0u);
    auto more = topicsScript(11);
    feed.append({more.begin() + 8, more.end()});
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    expectCacheIs(reader, more);
    EXPECT_EQ(feed.subscribeFromOffsets(), (std::vector<int64_t>{0, 8}));
    EXPECT_EQ(reader.stats().markerGaps, 0u);
}

// ---- Fix round 2: every reader accessor is owner-thread only, in every mode -----------

namespace {
/// Calls every owner-only member from the current thread; how many threw logic_error.
int foreignCalls(rc::Client& client, rc::CatalogReader& reader) {
    int threw = 0;
    auto expectThrow = [&](auto&& f) {
        try { f(); } catch (const std::logic_error&) { ++threw; }
    };
    expectThrow([&] { (void)client.catalogReader(); });
    expectThrow([&] { (void)reader.events(); });
    expectThrow([&] { (void)reader.state(); });
    expectThrow([&] { (void)reader.stats(); });
    expectThrow([&] { (void)reader.nextOffset(); });
    expectThrow([&] { (void)reader.historyEpoch(); });
    expectThrow([&] { (void)reader.beginRefresh(); });
    expectThrow([&] { (void)reader.pollRefresh(0, 0); });
    expectThrow([&] { (void)client.catalogHistory(rcat::HistoryFilter{}, std::chrono::milliseconds(10)); });
    return threw;
}
constexpr int kOwnerOnlyCalls = 9;
}   // namespace

// RED (round 1): client.catalogReader(), events(), state(), stats() ran on any thread
// — a foreign thread iterated events_ while the owner's refresh reallocated it.
TEST(CatalogReaderRegression, EveryAccessorRefusesAForeignThreadInInvokerModeRegression) {
    Rig r(invokerCfg(4050, 4051)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(3));
    rc::CatalogReader& reader = r.client.catalogReader();
    ASSERT_TRUE(refreshOnce(r, reader, 5'000'000'000).value().has_value());
    int threw = 0;
    std::thread([&] { threw = foreignCalls(r.client, reader); }).join();
    EXPECT_EQ(threw, kOwnerOnlyCalls) << "every owner-only member refused the foreign thread";
    EXPECT_EQ(reader.events().size(), 3u) << "the owner still reads";
}

TEST(CatalogReaderRegression, TwoForeignThreadsFirstCatalogCallsRefuseAndTheOwnerStillReadsRegression) {
    Rig r(invokerCfg(4052, 4053)); r.connect();
    CatalogFeed feed(r.gw, topicsScript(3));
    bool threw[2] = {false, false};
    std::thread t0([&] {
        try { (void)r.client.catalogHistory(rcat::HistoryFilter{}, std::chrono::milliseconds(10)); }
        catch (const std::logic_error&) { threw[0] = true; }
    });
    std::thread t1([&] {
        try { (void)r.client.catalogReader(); }
        catch (const std::logic_error&) { threw[1] = true; }
    });
    t0.join(); t1.join();
    EXPECT_TRUE(threw[0]);
    EXPECT_TRUE(threw[1]);
    auto done = refreshOnce(r, r.client.catalogReader(), 5'000'000'000);
    ASSERT_TRUE(done.has_value());
    EXPECT_TRUE(done->has_value()) << (done->has_value() ? "" : done->error().detail);
    EXPECT_EQ(r.client.catalogReader().events().size(), 3u);
}

// The same rule in THREAD mode (a background runner; app threads may block): the
// foreign thread is refused before anything reaches the cache; the owner reads
// with the blocking refresh / catalogHistory.
TEST(CatalogReaderRegression, EveryAccessorRefusesAForeignThreadInThreadModeRegression) {
    rc::ClientConfig c = invokerCfg(4054, 4055);
    c.runnerMode = rc::RunnerMode::Thread;
    c.nanoClock  = nullptr;   // the runner thread reads the clock: the real one
    rc::Client client(c);
    test_support::InvokerGateway gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel,
                                    c.egressStreamId);
    const auto log = topicsScript(4);
    CatalogFeed feed(gw, log);
    std::atomic<bool> stop{false};   // test-only: the fake broker's pump thread (owns gw from here on)
    std::thread broker([&] { while (!stop.load()) gw.pump(); });
    struct Join { std::atomic<bool>& s; std::thread& t; ~Join() { s = true; t.join(); } } join{stop, broker};

    client.connectAndWait(std::chrono::seconds(5));
    rc::CatalogReader& reader = client.catalogReader();
    auto h = client.catalogHistory(rcat::HistoryFilter{}, std::chrono::seconds(5));
    ASSERT_TRUE(h.has_value()) << h.error().detail;
    EXPECT_EQ(h->size(), log.size());
    int threw = 0;
    std::thread([&] { threw = foreignCalls(client, reader); }).join();
    EXPECT_EQ(threw, kOwnerOnlyCalls) << "Thread mode: every owner-only member refused the foreign thread";
    expectCacheIs(reader, log);
    auto again = reader.refresh(std::chrono::seconds(5));
    EXPECT_TRUE(again.has_value()) << (again.has_value() ? "" : again.error().detail);
}
