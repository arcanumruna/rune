// tests/lease/LeaseWatchTest.cpp - lease topics Task 16: LeaseWatch (the hash-range scan -> subscription handoff of spec 12.4), against the
// InvokerGateway stub broker plus the model-based ScanLeases server of support/LeaseScanModel.h, on a fake clock.  Every drive loop is
// capped; nothing sleeps.
#include "../support/AllocCounter.h"
#include "../support/InvokerGateway.h"
#include "../support/LeaseScanModel.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseScanHash.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <poll.h>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc  = rune::client;
namespace rpc = rune::protocol::client;
using test_support::Bytes;
using test_support::LeaseModel;
using test_support::LeaseStub;
using Kind = rc::LeaseEventKind;
using Op   = rune::protocol::record::LeaseOp;

namespace {

constexpr int64_t kMs = 1'000'000;
int64_t g_now = 0;
int64_t fakeNow() noexcept { return g_now; }
int64_t realNow() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// lease_version, lease_ttl_ms, lease_holder, lease_remaining_ms, then the key: view row = bitmap(1) + 5 * 8 + 2 * 8 = 57, log value = 44.
rune::codec::Descriptor watchDescriptor() {
    using rune::codec::PrimType;
    return rune::codec::DescriptorBuilder("lease_t", 1)
        .add("lease_version", PrimType::Int64, 1, false)
        .add("lease_ttl_ms", PrimType::UInt64, 1, false)
        .add("lease_holder", PrimType::UInt64, 1, false)
        .add("lease_remaining_ms", PrimType::UInt64, 1, false)
        .add("id", PrimType::UInt64, 1, /*isKey=*/true)
        .build();
}
const uint64_t kFp = rune::codec::fingerprint(watchDescriptor());

struct Row {
    static const uint64_t             kFingerprint;
    static constexpr std::string_view kSchemaName = "lease_t";
    uint64_t id = 0;
    [[nodiscard]] uint64_t    key() const noexcept { return id; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 44; }
    std::size_t encode(uint8_t* b) const noexcept {
        std::memset(b, 0, 44);
        b[0] = 40;
        b[2] = 1;
        std::memcpy(b + 36, &id, 8);
        return 44;
    }
};
const uint64_t Row::kFingerprint = kFp;

struct DataDirs {
    std::vector<std::string> dirs;
    ~DataDirs() {
        std::error_code ec;
        for (const std::string& d : dirs) std::filesystem::remove_all(d, ec);
    }
    std::string next() {
        dirs.push_back((std::filesystem::temp_directory_path() /
                        ("lease_watch_" + std::to_string(::getpid()) + "_" + std::to_string(dirs.size()))).string());
        return dirs.back();
    }
} g_dataDirs;

rc::ClientConfig invokerCfg(int32_t in, int32_t eg, int32_t partitions) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir         = g_dataDirs.next();
    c.runnerMode      = rc::RunnerMode::Invoker;
    c.nanoClock       = &fakeNow;
    g_now             = realNow();
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = partitions;
    c.topics.push_back(ts);
    return c;
}

const rune::protocol::LeaseSpec kSpec{.ttlMs = 6000, .minTtlMs = 1000, .maxTtlMs = 60000, .maxKeys = 100000,
                                      .expiryBatch = 64, .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};

struct Cb {
    struct Lost { rc::LeaseRef ref; rc::LostReason why; };
    std::vector<Lost> lost;
};
struct OnLostF {
    Cb* cb;
    void operator()(rc::LeaseRef r, rc::LostReason w) const { cb->lost.push_back({r, w}); }
};
struct OnRiskF {
    void operator()(rc::LeaseRef, rc::AtRiskEdge) const {}
};

using Held = std::set<std::pair<Bytes, int64_t>>;

/// Records the watch's events and folds them: a Granted for a held (key, version), or an Updated / Revoked / Expired / Vanished for an
/// unheld one, is a protocol violation (`bad`).
struct Recorder {
    struct Ev { Kind kind; int32_t part; int64_t off; Bytes key; int64_t ver; bool fromScan; };
    std::vector<Ev> evs;
    Held            held;
    int             bad = 0, readies = 0, failures = 0;
    static void fn(void* c, const rc::LeaseEvent& e) { static_cast<Recorder*>(c)->on(e); }
    void on(const rc::LeaseEvent& e) {
        Bytes k(e.key.begin(), e.key.end());
        evs.push_back({e.kind, e.partition, e.offset, k, e.version, e.fromScan});
        const auto id = std::make_pair(k, e.version);
        switch (e.kind) {
        case Kind::Granted:  if (!held.insert(id).second) ++bad; break;
        case Kind::Updated:  if (held.count(id) == 0) ++bad; break;
        case Kind::Revoked:
        case Kind::Expired:
        case Kind::Vanished: if (held.erase(id) == 0) ++bad; break;
        case Kind::Ready:    ++readies; break;
        case Kind::Failed:   ++failures; break;
        }
    }
    [[nodiscard]] int count(Kind kind, const Bytes* key = nullptr) const {
        int n = 0;
        for (const Ev& e : evs) n += e.kind == kind && (key == nullptr || e.key == *key);
        return n;
    }
};

Bytes kb(int i) {   // 8 bytes: byte 0 is the group (i % 4), byte 1 the index
    Bytes k(8, 0);
    k[0] = static_cast<uint8_t>(i % 4);
    k[1] = static_cast<uint8_t>(i);
    return k;
}

struct WRig {
    using Session = rc::LeaseSession<4, OnLostF, OnRiskF>;
    std::unique_ptr<rc::Client>        client;
    test_support::InvokerGateway       gw;
    std::optional<rc::LeaseTopic<Row>> topic;
    std::unique_ptr<LeaseStub>         stub;
    Cb                                 cb;
    std::unique_ptr<Session>           session;
    uint32_t                           parts;

    explicit WRig(const rc::ClientConfig& c, uint32_t partitions = 2)
        : client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId),
          parts(partitions) {
        gw.idempotence            = true;
        gw.replySchemas           = true;
        gw.schemaBlob             = rune::codec::serialize(watchDescriptor());
        gw.schemaTopicId          = 1;
        gw.schemaPartitionCount   = static_cast<int32_t>(partitions);
        gw.schemaLeaseViewTopicId = 9;
        rune::protocol::TopicKeyFeatures f;
        f.keyIndex = true;
        gw.schemaKeyFeatures = f;
        gw.schemaLease       = kSpec;
        gw.kaAutoOk          = true;
        stub = std::make_unique<LeaseStub>(gw, 1, partitions);
        stub->install();
        connect();
        topic.emplace(openTopic());
    }
    ~WRig() {
        session.reset();
        topic.reset();
    }
    void makeSession() { session = std::make_unique<Session>(*client, OnLostF{&cb}, OnRiskF{}, rc::LeaseSessionOptions{}); }
    void connect() {
        for (int i = 0; i < 4000 && !gw.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(gw.ready());
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        for (int i = 0; i < 4000 && !f.ready(); ++i) { client->doWork(); gw.pump(); }
        ASSERT_TRUE(f.ok()) << f.describe();
        for (int i = 0; i < 4000 && gw.metadataRequests == 0; ++i) { client->doWork(); gw.pump(); }
    }
    template <class Fn>
    void whilePumping(Fn&& fn) {
        int stop[2];
        ASSERT_EQ(::pipe(stop), 0);
        std::thread t([&] {
            pollfd pfd{stop[0], POLLIN, 0};
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (::poll(&pfd, 1, 0) == 0 && std::chrono::steady_clock::now() < deadline) gw.pump();
        });
        const auto release = [&] {
            const char b = 0;
            (void)!::write(stop[1], &b, 1);
            t.join();
            ::close(stop[0]);
            ::close(stop[1]);
        };
        try { fn(); } catch (...) { release(); throw; }
        release();
    }
    rc::LeaseTopic<Row> openTopic() {
        std::optional<rc::LeaseTopic<Row>> h;
        whilePumping([&] { h.emplace(client->openLeaseTopicAndWait<Row>("lease_t", {})); });
        EXPECT_TRUE(h.has_value());
        return std::move(*h);
    }
    /// One hop of everything: the feed of the log to the latest subscriptions, the client, the stub broker, the watch (and the session).
    void round(rc::LeaseWatchBase& w) {
        for (uint32_t p = 0; p < parts; ++p) (void)stub->feed(static_cast<int32_t>(p));
        client->doWork();
        gw.pump();
        w.doWork(g_now);
        if (session) session->doWork(g_now);
    }
    template <class P>
    bool drive(rc::LeaseWatchBase& w, P&& pred, int cap = 3000) {
        for (int i = 0; i < cap; ++i) {
            round(w);
            if (pred()) return true;
        }
        return false;
    }
    void rounds(rc::LeaseWatchBase& w, int n) {
        for (int i = 0; i < n; ++i) round(w);
    }
};

rc::OpResult startWith(rc::LeaseWatchBase& w, const std::vector<Bytes>& prefixes) {
    std::vector<std::span<const uint8_t>> sp;
    for (const Bytes& p : prefixes) sp.emplace_back(p.data(), p.size());
    return w.startFromNow(sp);
}

bool matchesAny(const Bytes& key, const std::vector<Bytes>& prefixes) {
    return std::any_of(prefixes.begin(), prefixes.end(), [&](const Bytes& p) {
        return key.size() >= p.size() && std::equal(p.begin(), p.end(), key.begin());
    });
}

Held expectedLive(const LeaseStub& s, const std::vector<Bytes>& prefixes) {
    Held h;
    for (const LeaseModel& m : s.parts)
        for (const auto& [k, v] : m.table)
            if (matchesAny(k, prefixes)) h.insert({k, v.version});
    return h;
}

Held actualLive(const rc::LeaseWatchBase& w) {
    Held h;
    w.forEachLive([&](std::span<const uint8_t> k, int64_t v, int32_t) { h.insert({Bytes(k.begin(), k.end()), v}); });
    return h;
}

bool settled(const WRig& r, const rc::LeaseWatchBase& w, const std::vector<Bytes>& prefixes) {
    return w.isReady() && actualLive(w) == expectedLive(*r.stub, prefixes) && w.liveCount() == expectedLive(*r.stub, prefixes).size();
}

std::string describe(const Held& h) {
    std::string s;
    for (const auto& [k, v] : h) s += "(" + std::to_string(k.size() > 1 ? k[1] : 0) + "," + std::to_string(v) + ")";
    return s;
}

/// Keys sorted by their scan hash (topic id 1).
std::vector<Bytes> byHash(std::vector<Bytes> keys) {
    std::sort(keys.begin(), keys.end(), [](const Bytes& a, const Bytes& b) { return rune::protocol::scanHash(a, 1) < rune::protocol::scanHash(b, 1); });
    return keys;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------

// Scan + subscription equals a fresh fold of the log, for random logs, random interleavings of writes with the scan pages (answers held and
// released late), random handoff buffers (ring and list overflows), random page sizes and random prefix sets.  One rig for every seed.
TEST(LeaseWatchHandoffRegression, ScanPlusSubscriptionEqualsFreshFoldSeededRegression) {
    WRig r(invokerCfg(5101, 5102, 2), 2);
    uint64_t totalResubscribes = 0, totalRingOverflows = 0;
    for (uint32_t seed = 1; seed <= 20; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937 rng(seed);
        r.stub->reset();
        const auto mutate = [&] {
            const int i = static_cast<int>(rng() % 40);
            LeaseModel& m = r.stub->parts[static_cast<std::size_t>(i % 2)];
            const Bytes k = kb(i);
            if (!m.held(k)) { m.grant(k); return; }
            const uint32_t d = rng() % 100;
            if (d < 35) m.update(k);
            else if (d < 70) m.revoke(k);
            else if (d < 85) m.expire(k);
            else m.update(k);
        };
        std::vector<Bytes> prefixes;
        if (rng() % 5 == 0) prefixes.push_back({});
        const int np = 1 + static_cast<int>(rng() % 3);
        for (int i = 0; i < np; ++i) {
            const auto g = static_cast<uint8_t>(rng() % 4);
            if (rng() % 3 == 0) prefixes.push_back({g, static_cast<uint8_t>(rng() % 40)});
            else prefixes.push_back({g});
        }
        rc::LeaseWatchOptions o;
        const uint32_t hb[] = {4, 8, 64};
        const uint16_t mr[] = {1, 2, 3, 7, 1024};
        o.handoffBuffer = hb[rng() % 3];
        o.maxRows       = mr[rng() % 5];
        o.maxLive       = 256;
        for (int i = 0; i < 40; ++i)
            if (rng() % 2 == 0) mutate();
        for (int i = 0; i < 10; ++i) mutate();
        rc::LeaseWatch<Row> w(*r.client, *r.topic, o);   // typed form
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, prefixes), rc::OpResult::Accepted);
        r.stub->hold = rng() % 2 == 0;
        for (int round = 0; round < 60; ++round) {
            int n = rng() % 8 == 0 ? 40 : static_cast<int>(rng() % 3);
            if (seed % 4 == 0 && round == 20) n = 800;   // a burst far past the 32 KiB record ring (about 320 records a partition)
            for (int i = 0; i < n; ++i) mutate();
            if (rng() % 6 == 0) {
                if (r.stub->hold) r.stub->release();
                else r.stub->hold = true;
            }
            r.round(w);
        }
        r.stub->release();
        ASSERT_TRUE(r.drive(w, [&] { return settled(r, w, prefixes); })) << "live " << describe(actualLive(w)) << " expected "
                                                                         << describe(expectedLive(*r.stub, prefixes));
        r.rounds(w, 40);
        EXPECT_FALSE(w.failed());
        EXPECT_EQ(actualLive(w), expectedLive(*r.stub, prefixes));
        EXPECT_EQ(rec.held, actualLive(w)) << "the event stream folds to the live set";
        EXPECT_EQ(rec.bad, 0);
        EXPECT_EQ(rec.readies, 1);
        EXPECT_EQ(rec.count(Kind::Vanished), 0) << "no break: a record older than its page's asOf is never replayed over the page";
        totalResubscribes += w.resubscribes();
        totalRingOverflows += w.ringOverflows();
    }
    // The run must actually exercise the overflow -> resubscribe path, or its equality assertions prove nothing about it.
    std::fprintf(stderr, "[seeded fold] resubscribes=%llu ringOverflows=%llu\n", static_cast<unsigned long long>(totalResubscribes),
                 static_cast<unsigned long long>(totalRingOverflows));
    EXPECT_GT(totalResubscribes, 0u);
    EXPECT_GT(totalRingOverflows, 0u);
}

// Rule 3 over hash ranges: page 2 of a scan is computed at its arrival, AFTER a record that the subscription (opened at page 1's asOf)
// also delivers.  A key of page 2 is skipped (its page already reflects the record); a key of page 1 is applied.
TEST(LeaseWatchHandoffRegression, RuleThreeHashRangesRegression) {
    WRig r(invokerCfg(5103, 5104, 1), 1);
    std::vector<Bytes> keys;
    for (int i = 0; i < 4; ++i) keys.push_back(kb(i * 4 + 1));
    keys = byHash(keys);   // A < B < C < D
    for (int scenario = 0; scenario < 2; ++scenario) {
        SCOPED_TRACE("scenario " + std::to_string(scenario));
        r.stub->reset();
        for (const Bytes& k : keys) r.stub->parts[0].grant(k);   // offsets 0..3
        const Bytes target = scenario == 0 ? keys[2] : keys[0];   // page 2 key, then page 1 key
        r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
            if (r.stub->seen.size() == 2) r.stub->parts[0].update(target);   // offset 4: before page 2 is computed
        };
        rc::LeaseWatchOptions o;
        o.maxRows = 2;
        rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        r.rounds(w, 30);
        ASSERT_EQ(r.stub->seen.size(), 2u) << "two pages";
        EXPECT_EQ(rec.count(Kind::Granted), 4);
        if (scenario == 0) EXPECT_EQ(rec.count(Kind::Updated), 0) << "the page of the key already reflects offset 4";
        else EXPECT_EQ(rec.count(Kind::Updated, &target), 1) << "page 1 was computed before offset 4";
        EXPECT_EQ(rec.bad, 0);
        EXPECT_EQ(actualLive(w), expectedLive(*r.stub, {Bytes{}}));
    }
}

TEST(LeaseWatchHandoffRegression, NestedPrefixesDroppedRegression) {
    WRig r(invokerCfg(5105, 5106, 2), 2);
    for (int i = 0; i < 12; ++i) r.stub->parts[static_cast<std::size_t>(i % 2)].grant(kb(i));
    {
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        // {1,5} extends {1}; {} extends nothing but covers {2}...: the empty prefix swallows everything else
        ASSERT_EQ(startWith(w, {Bytes{1, 5}, Bytes{1}, Bytes{1}, Bytes{1, 9}}), rc::OpResult::Accepted);
        EXPECT_EQ(w.prefixCount(), 1u);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        EXPECT_EQ(w.scansIssued(), 2u) << "one scan a partition for the single surviving prefix";
        for (const auto& s : r.stub->seen) EXPECT_EQ(s.prefix, (Bytes{1}));
        EXPECT_EQ(rec.count(Kind::Granted), 3) << "keys 1, 5, 9";
        EXPECT_EQ(rec.bad, 0);
    }
    r.stub->seen.clear();
    {
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, {Bytes{2}, Bytes{}, Bytes{3, 3}}), rc::OpResult::Accepted);
        EXPECT_EQ(w.prefixCount(), 1u) << "the empty prefix covers the rest";
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        EXPECT_EQ(rec.count(Kind::Granted), 12);
        EXPECT_EQ(r.stub->seen.size(), 2u);
    }
    // argument checks
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    EXPECT_EQ(startWith(w, {}), rc::OpResult::InvalidArgument);
    EXPECT_EQ(startWith(w, {Bytes(128, 1)}), rc::OpResult::InvalidArgument);
    EXPECT_EQ(startWith(w, std::vector<Bytes>(17, Bytes{7})), rc::OpResult::InvalidArgument) << "more than 16 prefixes";
    EXPECT_EQ(startWith(w, std::vector<Bytes>(16, Bytes{7})), rc::OpResult::Accepted) << "16 identical prefixes collapse to one";
    EXPECT_EQ(w.prefixCount(), 1u);
    EXPECT_EQ(startWith(w, {Bytes{8}}), rc::OpResult::InvalidArgument) << "already started";
}

// Disjoint prefixes share ONE subscription a partition (opened at the first page of the first prefix) and scan one after the other.
TEST(LeaseWatchHandoffRegression, DisjointPrefixesOneSubscriptionRegression) {
    WRig r(invokerCfg(5107, 5108, 2), 2);
    for (int i = 0; i < 16; ++i) r.stub->parts[static_cast<std::size_t>(i % 2)].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    const std::vector<Bytes> prefixes{Bytes{1}, Bytes{2}};
    ASSERT_EQ(startWith(w, prefixes), rc::OpResult::Accepted);
    ASSERT_EQ(w.prefixCount(), 2u);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 20);
    EXPECT_EQ(w.scansIssued(), 4u) << "2 partitions x 2 prefixes";
    EXPECT_EQ(w.subscriptionsOpened(), 2u);
    std::size_t subs = 0;
    for (const auto& s : r.gw.subscribed) subs += s.topicId == 1;
    EXPECT_EQ(subs, 2u) << "one Subscribe a partition";
    EXPECT_EQ(rec.count(Kind::Granted), 8);
    EXPECT_EQ(actualLive(w), expectedLive(*r.stub, prefixes));
    EXPECT_EQ(rec.bad, 0);
}

// A full handoff list cancels the subscription at the record that did not fit; the partition re-subscribes from exactly that offset.
TEST(LeaseWatchHandoffRegression, OverflowResubscribesFromCursorRegression) {
    WRig r(invokerCfg(5109, 5110, 1), 1);
    const Bytes A = byHash({kb(1), kb(5), kb(9)})[0];   // page 1 under maxRows = 1
    Bytes B;
    for (const Bytes& k : byHash({kb(1), kb(5), kb(9)})) if (k != A) { B = k; break; }
    for (int i : {1, 5, 9}) r.stub->parts[0].grant(kb(i));
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) {
        if (r.stub->seen.size() == 2) r.stub->hold = true;   // page 2 is computed now, delivered late
    };
    rc::LeaseWatchOptions o;
    o.handoffBuffer = 4;
    o.maxRows       = 1;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    const int64_t first = static_cast<int64_t>(r.stub->parts[0].log.size());   // 3
    // ten records: four updates of B, the Revoke of A (the fifth: the one that does not fit), five more updates of B
    for (int i = 0; i < 4; ++i) r.stub->parts[0].update(B);
    r.stub->parts[0].revoke(A);
    for (int i = 0; i < 5; ++i) r.stub->parts[0].update(B);
    r.rounds(w, 30);
    EXPECT_EQ(rec.count(Kind::Updated), 0) << "still scanning: everything waits in the handoff list";
    r.stub->release();
    ASSERT_TRUE(r.drive(w, [&] { return w.resubscribes() == 1 && w.isReady(); }));
    r.rounds(w, 30);
    const auto* sub = r.gw.latestSubscription(1, 0);
    ASSERT_NE(sub, nullptr);
    EXPECT_EQ(sub->fromOffset, first + 4) << "the cursor is the fifth record";
    EXPECT_EQ(w.resubscribes(), 1u);
    EXPECT_EQ(rec.count(Kind::Updated, &B), 9);
    EXPECT_EQ(rec.count(Kind::Revoked, &A), 1);
    EXPECT_EQ(rec.bad, 0);
    EXPECT_EQ(actualLive(w), expectedLive(*r.stub, {Bytes{}}));
    EXPECT_FALSE(w.isLive(A, 0));
}

// Identity is (key, version): a Granted for a held (k, v) is ignored; at another version it emits Vanished(old) then Granted(new); an
// Updated / Revoked for an unknown (k, v) is ignored.
TEST(LeaseWatchHandoffRegression, IdentityDedupRegression) {
    WRig r(invokerCfg(5111, 5112, 1), 1);
    const Bytes A = kb(1);
    LeaseModel& m = r.stub->parts[0];
    m.grant(A);               // offset 0, version 0
    m.asOfOverride = 0;       // the scan claims asOf 0 although its row already holds offset 0: the record is delivered again
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 20);
    EXPECT_EQ(rec.count(Kind::Granted, &A), 1) << "scan row and record are the same (key, version)";
    EXPECT_TRUE(w.isLive(A, 0));
    const std::size_t before = rec.evs.size();
    (void)m.rawPut(A, 12345);                                     // an Updated of an unknown version
    (void)m.rawEnd(A, 777, Op::Revoke);                           // an end of an unknown version
    (void)m.rawEnd(kb(2), 0, Op::Expire);                         // an end of an unknown key
    r.rounds(w, 20);
    EXPECT_EQ(rec.evs.size(), before) << "unknown (key, version) records are ignored";
    EXPECT_TRUE(w.isLive(A, 0));
    const auto newVersion = static_cast<int64_t>(m.log.size());
    (void)m.rawPut(A, newVersion);                                // a Granted at another version
    r.rounds(w, 20);
    ASSERT_EQ(rec.evs.size(), before + 2);
    EXPECT_EQ(rec.evs[before].kind, Kind::Vanished);
    EXPECT_EQ(rec.evs[before].ver, 0);
    EXPECT_EQ(rec.evs[before + 1].kind, Kind::Granted);
    EXPECT_EQ(rec.evs[before + 1].ver, newVersion);
    EXPECT_EQ(w.liveCount(), 1u);
    (void)m.rawEnd(A, newVersion, Op::Revoke);
    r.rounds(w, 20);
    EXPECT_EQ(rec.count(Kind::Revoked, &A), 1);
    EXPECT_EQ(w.liveCount(), 0u);
    EXPECT_EQ(rec.bad, 0);
}

// A broken subscription re-runs its partition: rescan, resubscribe, and whatever the rescan does not confirm is Vanished; the confirmed
// entries are not granted twice; isReady() stays true throughout.
TEST(LeaseWatchHandoffRegression, BreakRerunsRegression) {
    WRig r(invokerCfg(5113, 5114, 2), 2);
    const Bytes A = kb(1), B = kb(5), C = kb(9), D = kb(2), E = kb(13);
    r.stub->parts[0].grant(A);
    r.stub->parts[0].grant(B);
    r.stub->parts[0].grant(C);
    r.stub->parts[1].grant(D);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    ASSERT_EQ(rec.count(Kind::Granted), 4);
    // the subscription of partition 0 fails (not authorized: terminal)
    const auto sub = *r.gw.latestSubscription(1, 0);
    r.gw.sendError(0, static_cast<int16_t>(rpc::ErrorCode::NotAuthorized), "denied", sub.sessionId, sub.corrId);
    r.stub->parts[0].revoke(B);   // while nobody watches partition 0
    r.stub->parts[0].grant(E);
    bool readyAlways = true;
    for (int i = 0; i < 40; ++i) {
        r.round(w);
        readyAlways = readyAlways && w.isReady();
    }
    EXPECT_EQ(w.reruns(), 0u) << "the re-run waits for its retry deadline";
    g_now += 2 * kMs;
    for (int i = 0; i < 400 && !settled(r, w, {Bytes{}}); ++i) {
        r.round(w);
        readyAlways = readyAlways && w.isReady();
    }
    r.rounds(w, 30);
    EXPECT_TRUE(readyAlways);
    EXPECT_EQ(w.reruns(), 1u);
    EXPECT_FALSE(w.failed());
    EXPECT_EQ(rec.count(Kind::Vanished, &B), 1);
    EXPECT_EQ(rec.count(Kind::Vanished), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &E), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &A), 1) << "a confirmed entry is not granted again";
    EXPECT_EQ(rec.count(Kind::Granted, &C), 1);
    EXPECT_EQ(rec.count(Kind::Granted, &D), 1);
    EXPECT_EQ(rec.bad, 0);
    EXPECT_EQ(actualLive(w), expectedLive(*r.stub, {Bytes{}}));
    EXPECT_EQ(rec.held, actualLive(w));
    EXPECT_EQ(w.liveCount(), 4u) << "A, C, D, E";
}

// Ready is emitted once, after every partition is live, and never again (a re-run keeps isReady() true).
TEST(LeaseWatchHandoffRegression, ReadyOnceRegression) {
    WRig r(invokerCfg(5115, 5116, 2), 2);
    r.stub->parts[0].grant(kb(1));
    r.stub->parts[1].grant(kb(2));
    r.stub->holdPartitions.insert(1);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return r.stub->heldCount() == 1; }));
    r.rounds(w, 60);
    EXPECT_FALSE(w.isReady());
    EXPECT_EQ(rec.readies, 0) << "partition 1 is still scanning";
    r.stub->release();
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 20);
    EXPECT_EQ(rec.readies, 1);
    // a re-run of partition 0
    const auto sub = *r.gw.latestSubscription(1, 0);
    r.gw.sendError(0, static_cast<int16_t>(rpc::ErrorCode::NotAuthorized), "denied", sub.sessionId, sub.corrId);
    r.rounds(w, 20);   // the watch sees the break at this instant; its retry deadline is one millisecond later
    g_now += 2 * kMs;
    bool readyAlways = true;
    for (int i = 0; i < 400 && w.reruns() == 0; ++i) {
        r.round(w);
        readyAlways = readyAlways && w.isReady();
    }
    r.rounds(w, 60);
    EXPECT_EQ(w.reruns(), 1u);
    EXPECT_TRUE(readyAlways);
    EXPECT_EQ(rec.readies, 1);
    EXPECT_EQ(rec.bad, 0);
}

// A burst far larger than the record ring pushed at a Live partition: the ring overflows, the partition re-subscribes from the first
// record that did not fit, and no record is lost or applied twice.
TEST(LeaseWatchHandoffRegression, RingOverflowWhileLiveResubscribesRegression) {
    WRig r(invokerCfg(5117, 5118, 1), 1);
    const Bytes A = kb(1);
    r.stub->parts[0].grant(A);
    rc::LeaseWatchOptions o;
    o.handoffBuffer = 4;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    for (int i = 0; i < 599; ++i) r.stub->parts[0].update(A);
    r.stub->parts[0].revoke(A);
    ASSERT_TRUE(r.drive(w, [&] { return rec.count(Kind::Revoked, &A) == 1; }));
    r.rounds(w, 20);
    EXPECT_GE(w.resubscribes(), 1u) << "600 records do not fit a 4-record ring";
    EXPECT_EQ(rec.count(Kind::Updated, &A), 599);
    EXPECT_EQ(rec.count(Kind::Revoked, &A), 1);
    EXPECT_EQ(rec.bad, 0);
    EXPECT_FALSE(w.failed());
    EXPECT_EQ(w.liveCount(), 0u);
}

// The steady-state watch allocates nothing: the record handler (duty side) and doWork (owner side), over live updates.
TEST(LeaseWatchHandoffRegression, WatchNoAllocRegression) {
    WRig r(invokerCfg(5119, 5120, 2), 2);
    for (int i = 0; i < 16; ++i) r.stub->parts[static_cast<std::size_t>(i % 2)].grant(kb(i));
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    struct Counts { long events = 0; } counts;
    w.setHandler([](void* c, const rc::LeaseEvent&) { ++static_cast<Counts*>(c)->events; }, &counts);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 20);
    long clientAllocs = 0, watchAllocs = 0;
    const long eventsBefore = counts.events;
    for (int i = 0; i < 300; ++i) {
        for (int k = 0; k < 3; ++k) r.stub->parts[static_cast<std::size_t>((i + k) % 2)].update(kb((i + k) % 16));
        for (uint32_t p = 0; p < 2; ++p) (void)r.stub->feed(static_cast<int32_t>(p));   // the stub allocates: outside the windows
        alloc_counter::startThisThread();
        r.client->doWork();
        clientAllocs += alloc_counter::stopThisThread();
        r.gw.pump();
        alloc_counter::startThisThread();
        w.doWork(g_now);
        watchAllocs += alloc_counter::stopThisThread();
    }
    EXPECT_EQ(w.resubscribes(), 0u) << "the window must not contain a resubscribe";
    EXPECT_GE(counts.events - eventsBefore, 900);
    EXPECT_EQ(watchAllocs, 0);
    EXPECT_EQ(clientAllocs, 0) << "the record handler runs inside client.doWork";
}

// Names: lowest(id) is the live lease of the name with the smallest version, through scan, grants, ends and a middle removal.
TEST(LeaseWatchHandoffRegression, NamesLowestRegression) {
    WRig r(invokerCfg(5121, 5122, 2), 2);
    const auto key = [](const std::string& s) { return test_support::bytesOf(s); };
    std::vector<Bytes> qKeys;
    for (int i = 0; i < 12; ++i) qKeys.push_back(key("q/k" + std::to_string(i)));
    // granted in a shuffled order, so versions (offsets) and scan (hash) order disagree
    std::mt19937 rng(7);
    std::vector<Bytes> order = qKeys;
    std::shuffle(order.begin(), order.end(), rng);
    std::map<Bytes, int64_t> version;
    for (std::size_t i = 0; i < order.size(); ++i) version[order[i]] = r.stub->parts[i % 2].grant(order[i]);
    // (the offsets are per partition; make the order global with a distinct version per key by the per-partition offset)
    (void)r.stub->parts[0].grant(key("r/x"));
    (void)r.stub->parts[1].grant(key("q/k3/deep"));   // two levels: not a member of "q"
    rc::LeaseWatchOptions o;
    o.names          = 4;
    o.waitersPerName = 32;
    o.maxLive        = 64;
    rc::LeaseWatchBase w(*r.client, r.topic->core(), o);
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    const int32_t q = w.registerName(key("q"));
    const int32_t rr = w.registerName(key("r"));
    ASSERT_GE(q, 0);
    ASSERT_GE(rr, 0);
    EXPECT_NE(q, rr);
    EXPECT_EQ(w.registerName({}), -1) << "empty";
    EXPECT_EQ(w.registerName(key("q")), q) << "the same name is the same id";
    EXPECT_FALSE(w.lowest(q).has_value()) << "nothing live yet";
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    EXPECT_EQ(w.registerName(key("late")), -1) << "after startFromNow";
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    // the live members of "q", by (version, key)
    std::map<int64_t, Bytes> byVersion;
    const auto refresh = [&] {
        byVersion.clear();
        for (const LeaseModel& m : r.stub->parts)
            for (const auto& [k, h] : m.table) {
                const std::string s(k.begin(), k.end());
                if (s.rfind("q/", 0) == 0 && s.find('/', 2) == std::string::npos) byVersion.emplace(h.version, k);
            }
    };
    // versions may collide across the two partitions (offsets are per partition); keep the check on the (version) ordering only
    const auto expectLowest = [&](const char* when) {
        refresh();
        const auto got = w.lowest(q);
        if (byVersion.empty()) {
            EXPECT_FALSE(got.has_value()) << when;
            return;
        }
        ASSERT_TRUE(got.has_value()) << when;
        EXPECT_EQ(got->first, byVersion.begin()->first) << when;
    };
    expectLowest("after the scan");
    ASSERT_TRUE(w.lowest(rr).has_value());
    EXPECT_EQ(w.lowest(rr)->first, r.stub->parts[0].table.at(key("r/x")).version);
    // end the lowest repeatedly, adding and removing members of the middle in between
    int added = 0;
    for (int step = 0; step < 12; ++step) {
        refresh();
        if (byVersion.empty()) break;
        const Bytes lowest = byVersion.begin()->second;
        const std::size_t part = r.stub->parts[0].held(lowest) ? 0 : 1;
        if (step % 3 == 0) (void)r.stub->parts[1].grant(key("q/n" + std::to_string(added++)));
        if (byVersion.size() > 3) {
            const auto mid = std::next(byVersion.begin(), 2);
            const std::size_t mp = r.stub->parts[0].held(mid->second) ? 0 : 1;
            (void)r.stub->parts[mp].update(mid->second);
            if (step % 2 == 0) (void)r.stub->parts[mp].revoke(mid->second);
        }
        if (r.stub->parts[part].held(lowest)) (void)(step % 2 == 0 ? r.stub->parts[part].revoke(lowest) : r.stub->parts[part].expire(lowest));
        r.rounds(w, 8);
        expectLowest("after an end");
        EXPECT_EQ(rec.bad, 0);
    }
    EXPECT_FALSE(w.failed());
    EXPECT_EQ(actualLive(w), expectedLive(*r.stub, {Bytes{}}));
}

// The LeaseSession is an observer of the watch: a Revoked of the lease it holds (same topic, key AND version) ends it WatchedRevoked.
TEST(LeaseWatchHandoffRegression, SessionObservesWatchRegression) {
    WRig r(invokerCfg(5123, 5124, 1), 1);
    r.makeSession();
    Bytes key1(8, 0), key2(8, 0);
    key1[0] = 1;
    key2[0] = 2;
    rc::OpResult err = rc::OpResult::Accepted;
    const auto l1 = r.session->add(*r.topic, Row{1}, 5, 3000, g_now, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);
    const auto l2 = r.session->add(*r.topic, Row{2}, 5, 3000, g_now, &err);
    ASSERT_EQ(err, rc::OpResult::Accepted);
    (void)l1;
    (void)l2;
    r.stub->parts[0].setRow(key1, 5, 0);
    r.stub->parts[0].setRow(key2, 5, 0);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    ASSERT_TRUE(r.session->observe(w));
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.rounds(w, 10);
    EXPECT_TRUE(r.cb.lost.empty());
    (void)r.stub->parts[0].rawEnd(key2, 4, Op::Revoke);   // another version: another lease
    r.rounds(w, 20);
    EXPECT_TRUE(r.cb.lost.empty());
    EXPECT_EQ(r.session->liveCount(), 2u);
    (void)r.stub->parts[0].rawEnd(key1, 5, Op::Revoke);
    r.rounds(w, 20);
    ASSERT_EQ(r.cb.lost.size(), 1u);
    EXPECT_EQ(r.cb.lost[0].why, rc::LostReason::WatchedRevoked);
    EXPECT_EQ(r.cb.lost[0].ref.slot, l1.slot);
    (void)r.stub->parts[0].rawEnd(key2, 5, Op::Expire);
    r.rounds(w, 20);
    ASSERT_EQ(r.cb.lost.size(), 2u);
    EXPECT_EQ(r.cb.lost[1].why, rc::LostReason::WatchedExpired);
    EXPECT_TRUE(r.session->unobserve(w));
}

// ---------------------------------------------------------------------------------------------------------------------------
// Checkpoint D: definitive vs transient scan errors (M2), the oversize row (L1), session / watch lifetimes (L2)
// ---------------------------------------------------------------------------------------------------------------------------

// M2: a scan answered with a DEFINITIVE error (a rescan answers the same) fails the watch: failed() and the Failed event, once.
TEST(LeaseWatchHandoffRegression, DefinitiveScanErrorFailsRegression) {
    for (const auto code : {rpc::ErrorCode::NotAuthorized, rpc::ErrorCode::UnknownTopic, rpc::ErrorCode::InvalidRequest}) {
        SCOPED_TRACE("code " + std::to_string(static_cast<int>(code)));
        WRig r(invokerCfg(5901, 5902, 1), 1);
        r.stub->parts[0].grant(kb(1));
        r.stub->errorCode = static_cast<int16_t>(code);
        r.stub->errorNext = 1'000'000;
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        Recorder rec;
        w.setHandler(&Recorder::fn, &rec);
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        const int64_t end = g_now + 600 * kMs;
        for (int i = 0; i < 300 && g_now < end && !w.failed(); ++i) { g_now += 2 * kMs; r.round(w); }
        EXPECT_TRUE(w.failed());
        for (int i = 0; i < 20; ++i) { g_now += 2 * kMs; r.round(w); }
        EXPECT_LE(r.stub->seen.size(), 1u) << "no rescan of a definitive answer";
        EXPECT_EQ(rec.failures, 1);
        EXPECT_FALSE(w.isReady());
    }
}

// ... while any other error is a transient break: the partition is re-run after 1, 2, 4, 8, 16 ms (doubling, 1 s cap), then goes Live.
TEST(LeaseWatchHandoffRegression, TransientScanErrorBacksOffRegression) {
    WRig r(invokerCfg(5903, 5904, 1), 1);
    r.stub->parts[0].grant(kb(1));
    r.stub->errorCode = static_cast<int16_t>(rpc::ErrorCode::Internal);
    r.stub->errorNext = 5;
    std::vector<int64_t> at;   // when each ScanLeases request reached the broker
    r.stub->onRequest = [&](const LeaseStub::ScanSeen&) { at.push_back(g_now); };
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    Recorder rec;
    w.setHandler(&Recorder::fn, &rec);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    const int64_t end = g_now + 2000 * kMs;
    for (int i = 0; i < 1500 && g_now < end && !w.isReady(); ++i) { g_now += kMs; r.round(w); }
    ASSERT_TRUE(w.isReady());
    EXPECT_FALSE(w.failed());
    ASSERT_GE(at.size(), 6u);
    const int64_t want[5] = {1, 2, 4, 8, 16};
    for (int k = 0; k < 5; ++k) {
        const int64_t gap = (at[static_cast<std::size_t>(k) + 1] - at[static_cast<std::size_t>(k)]) / kMs;
        EXPECT_GE(gap, want[k]) << "retry " << k;
        EXPECT_LE(gap, want[k] + 3) << "retry " << k << " (a few rounds of slack)";
    }
    EXPECT_EQ(w.reruns(), 5u);
    EXPECT_EQ(rec.readies, 1);
    EXPECT_EQ(rec.bad, 0);
}

// L1: a topic whose widest key + lease view row cannot ride one ring record is refused at start (it would fail as Fatal at the first record).
TEST(LeaseWatchHandoffRegression, OversizeRowRefusedRegression) {
    WRig r(invokerCfg(5905, 5906, 1), 1);
    {
        rc::LeaseWatchBase ok(*r.client, r.topic->core());
        EXPECT_EQ(startWith(ok, {Bytes{}}), rc::OpResult::Accepted) << "the regular row fits";
    }
    using rune::codec::PrimType;
    auto b = rune::codec::DescriptorBuilder("lease_t", 1)
                 .add("lease_version", PrimType::Int64, 1, false)
                 .add("lease_ttl_ms", PrimType::UInt64, 1, false)
                 .add("lease_holder", PrimType::UInt64, 1, false)
                 .add("lease_remaining_ms", PrimType::UInt64, 1, false)
                 .add("id", PrimType::UInt64, 1, /*isKey=*/true);
    for (int i = 0; i < 250; ++i) b.add("f" + std::to_string(i), PrimType::Int64, 1, false);
    std::vector<uint8_t> row(44);
    (void)Row{}.encode(row.data());
    r.topic->core().setRevokeTemplate(row, rune::codec::serialize(b.build()));
    ASSERT_GT(r.topic->core().viewRowLen(), 2048u - 48u - 8u);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    EXPECT_EQ(startWith(w, {Bytes{}}), rc::OpResult::InvalidArgument);
    EXPECT_FALSE(w.failed());
    EXPECT_EQ(w.scansIssued(), 0u);
}

// L2: either side of an observe may be destroyed first; the survivor forgets the other and is never called through a dangling pointer.
TEST(LeaseWatchHandoffRegression, SessionDestroyedWhileObservingRegression) {
    WRig r(invokerCfg(5907, 5908, 1), 1);
    r.makeSession();
    r.stub->parts[0].setRow(kb(1), 5, 0);
    rc::LeaseWatchBase w(*r.client, r.topic->core());
    ASSERT_TRUE(r.session->observe(w));
    EXPECT_FALSE(r.session->observe(w)) << "observed once";
    EXPECT_EQ(w.observerCount(), 1u);
    EXPECT_EQ(r.session->observedCount(), 1u);
    ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
    ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
    r.session.reset();
    ASSERT_EQ(w.observerCount(), 0u) << "the destroyed session left the watch's observer list";
    (void)r.stub->parts[0].rawEnd(kb(1), 5, Op::Revoke);   // the watch goes through an event
    r.rounds(w, 20);
    EXPECT_EQ(w.liveCount(), 0u);
    EXPECT_FALSE(w.failed());
}

TEST(LeaseWatchHandoffRegression, WatchDestroyedWhileObservedRegression) {
    WRig r(invokerCfg(5909, 5910, 1), 1);
    r.makeSession();
    ASSERT_NE(r.session->add(*r.topic, Row{1}, 5, 3000, g_now).slot, 0xFFFFFFFFu);
    r.stub->parts[0].setRow(Bytes(8, 0), 5, 0);
    {
        rc::LeaseWatchBase w(*r.client, r.topic->core());
        ASSERT_TRUE(r.session->observe(w));
        ASSERT_EQ(startWith(w, {Bytes{}}), rc::OpResult::Accepted);
        ASSERT_TRUE(r.drive(w, [&] { return w.isReady(); }));
        EXPECT_EQ(r.session->observedCount(), 1u);
    }
    ASSERT_EQ(r.session->observedCount(), 0u) << "the destroyed watch left the session's record";
    for (int i = 0; i < 20; ++i) { g_now += 10 * kMs; r.client->doWork(); r.gw.pump(); r.session->doWork(g_now); }   // the session is driven on
    EXPECT_EQ(r.session->liveCount(), 1u);
    rc::LeaseWatchBase w2(*r.client, r.topic->core());
    EXPECT_TRUE(r.session->observe(w2)) << "its slot is free again";
    EXPECT_TRUE(r.session->unobserve(w2));
    EXPECT_EQ(w2.observerCount(), 0u);
}
