// LeaseTopicBrokerTest.cpp - lease topics Task 14b: the typed client (LeaseTopic<T>, the generated withLease / LeasesRow) against
// a REAL single-node broker (shard runtime, default config): grant / KeyExists / revoke / re-grant, the read-your-writes barrier
// of get, keepAlive + timeToLive + a partition scan, the widest key, and a subscription's view of the lease ops.
// Expiry on a real broker is Task 17's (the clock seam).  The lease types are generated at build time by rune_gen_claim_schemas
// (client-cpp/tests/support/GenClaimSchemas.cpp, isLease = true): the build is the compile proof of the generated header.
#include "client/OneBroker.h"

#include "gen/lease_claim.gen.h"
#include "gen/lease_wide.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test;
namespace rc = rune::client;

namespace {

using rune::gen::LeaseClaim;
using rune::gen::LeaseWide;

constexpr uint16_t kKeyExists = static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists);

rune::protocol::LeaseSpec bigSpec() {
    return rune::protocol::LeaseSpec{.ttlMs = 60'000, .minTtlMs = 1000, .maxTtlMs = 600'000, .maxKeys = 1000, .expiryBatch = 64,
                                     .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
}

std::vector<uint8_t> claimBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                      .addFixedStr("claim", 32, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .addFixedStr("b", 8)
                                      .build());
}
std::vector<uint8_t> wideBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_wide")
                                      .addFixedStr("claim", 128, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .build());
}

int32_t defineLease(rc::Client& c, const std::string& name, uint32_t partitions, const std::string& keyField,
                    const std::vector<uint8_t>& blob) {
    rc::TopicOptions to;
    to.lease = bigSpec();
    to.config.cleanupPolicy = 1;   // a lease topic is compacted (it keeps every live lease)
    const auto r = c.defineTopicAndWait(name, partitions, keyField, blob, rc::Persistence::Disk, to, {}, 60s);
    EXPECT_EQ(r.errorCode, 0) << r.detail;
    return r.topicId;
}

LeaseClaim claimRow(std::string_view key, uint64_t a = 0, std::string_view b = {}) {
    LeaseClaim r;
    if (!key.empty()) std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    r.a = a;
    if (!b.empty()) std::memcpy(r.b.data(), b.data(), std::min(b.size(), r.b.size()));
    return r;
}

std::string text(const std::array<char, 8>& a) { return std::string(a.data(), strnlen(a.data(), a.size())); }

struct Collected {
    int32_t           partition;
    int64_t           offset;
    rc::LeaseRecordOp lop;
    rc::RecordOp      op;
};

/// A client of its own subscribed from the earliest offset to every partition of one topic; collects what the handler sees.
class Collector {
public:
    Collector(rc::ClientConfig cfg, const std::string& name, int32_t topicId, int32_t partitions)
        : client_(withTopic(std::move(cfg), name, topicId, partitions)) {
        client_.connectAndWait(30s);
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        for (int32_t p = 0; p < partitions; ++p)
            subs_.push_back(client_.subscribe(client_.topic(name), p, o, [this](const rc::Record& r) {
                std::lock_guard<std::mutex> g(mu_);
                got_.push_back(Collected{r.partition, r.offset, r.leaseOp(), r.op()});
            }));
    }
    ~Collector() {
        for (auto& s : subs_) s.unsubscribe();
        client_.close();
    }
    /// Waits (bounded) for `n` records; the records seen either way.
    std::vector<Collected> waitFor(std::size_t n) {
        for (const auto deadline = std::chrono::steady_clock::now() + 60s; std::chrono::steady_clock::now() < deadline;
             std::this_thread::sleep_for(20ms)) {
            std::lock_guard<std::mutex> g(mu_);
            if (got_.size() >= n) break;
        }
        std::lock_guard<std::mutex> g(mu_);
        return got_;
    }

private:
    static rc::ClientConfig withTopic(rc::ClientConfig c, const std::string& name, int32_t id, int32_t parts) {
        c.topics.push_back(rc::TopicSpec{name, id, parts});
        return c;
    }
    rc::Client                client_;
    std::mutex                mu_;
    std::vector<Collected>    got_;
    std::vector<rc::Subscription> subs_;
};

}  // namespace

TEST(LeaseTopicBrokerRegression, GrantKeyExistsRevokeRegrantRegression) {
    OneBroker b("lease-grant", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    ASSERT_EQ(lt.partitionCount(), 2);

    LeaseClaim row = claimRow("k-grant", 42, "hi");
    row.withLease(0, 0, 7);
    rc::LeaseOptions o;
    o.holder = 7;
    const rc::LeaseResult g1 = lt.grantAndWait(row, o);
    ASSERT_TRUE(g1.ok()) << int(g1.status) << " " << g1.errorCode;
    const int64_t v1 = g1.version;
    EXPECT_GE(v1, 0) << "a grant's version is the log offset the broker assigned it";
    EXPECT_EQ(g1.ttlMs, 60'000) << "the topic's ttl when the call names none";

    const rc::LeaseResult again = lt.grantAndWait(row, o);
    EXPECT_EQ(again.status, rc::OpStatus::RecordsRejected);
    EXPECT_EQ(again.errorCode, kKeyExists) << "a second grant of a live lease";

    const rc::LeaseResult rv = lt.revokeAndWait(row, v1);
    EXPECT_TRUE(rv.ok()) << int(rv.status) << " " << rv.errorCode;

    const rc::LeaseResult g2 = lt.grantAndWait(row, o);
    ASSERT_TRUE(g2.ok()) << int(g2.status) << " " << g2.errorCode;
    const int64_t v2 = g2.version;
    EXPECT_GT(v2, v1);

    const auto got = lt.getAndWait(row, v2);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->lease_version, v2);
    EXPECT_EQ(got->lease_granted_offset, v2) << "the grant's own offset";
    EXPECT_EQ(got->lease_holder, 7u);
    EXPECT_EQ(got->lease_ttl_ms, 60'000u);
    EXPECT_EQ(got->a, 42u);
    EXPECT_EQ(text(got->b), "hi");
    EXPECT_EQ(std::string(got->claim.data(), 7), "k-grant");

    EXPECT_FALSE(lt.getAndWait(claimRow("never-granted")).has_value()) << "no lease under that key";
}

TEST(LeaseTopicBrokerRegression, GetMinOffsetRegression) {
    OneBroker b("lease-minoff", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const int32_t topicId = defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    Collector col(b.clientConfig(), "claims", topicId, 2);

    const LeaseClaim row = claimRow("k-min", 1, "one");
    const rc::LeaseResult g = lt.grantAndWait(row);
    ASSERT_TRUE(g.ok());
    LeaseClaim upd = claimRow("k-min", 7, "seven");
    ASSERT_TRUE(lt.updateAndWait(upd, g.version).ok());

    const auto seen = col.waitFor(2);
    ASSERT_EQ(seen.size(), 2u);
    const auto updated = std::find_if(seen.begin(), seen.end(), [](const Collected& r) { return r.lop == rc::LeaseRecordOp::Updated; });
    ASSERT_NE(updated, seen.end());

    const auto got = lt.getAndWait(row, updated->offset);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->a, 7u) << "read-your-writes: the read waited for the update's offset";
    EXPECT_EQ(text(got->b), "seven");
    EXPECT_EQ(got->lease_updated_offset, updated->offset);
    EXPECT_EQ(got->lease_granted_offset, g.version);
    EXPECT_EQ(got->lease_version, g.version) << "an update keeps the lease version";
}

TEST(LeaseTopicBrokerRegression, KeepAliveTtlScanRegression) {
    constexpr int kKeys = 12;
    OneBroker b("lease-scan", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);

    std::set<std::string> granted;
    std::vector<int64_t>  versions;
    rc::LeaseOptions      o;
    o.holder = 99;
    for (int i = 0; i < kKeys; ++i) {
        const std::string k = "scan-" + std::to_string(i);
        const rc::LeaseResult g = lt.grantAndWait(claimRow(k), o);
        ASSERT_TRUE(g.ok()) << k;
        granted.insert(k);
        versions.push_back(g.version);
    }
    const LeaseClaim first = claimRow("scan-0");
    const int64_t    v2    = versions[0];

    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(first, v2);
    ASSERT_TRUE(ka.ok()) << int(ka.status) << " " << ka.errorCode;
    EXPECT_EQ(ka.ttlMs, 60'000) << "aux = the ttl";

    rc::LeaseTtlInfo info;
    const rc::LeaseResult ttl = lt.timeToLiveAndWait(first, &info);
    ASSERT_TRUE(ttl.ok());
    EXPECT_EQ(info.version, v2);
    EXPECT_EQ(info.holder, 99u);
    EXPECT_GT(info.remainingMs, 0);
    EXPECT_LE(info.remainingMs, 60'000);
    EXPECT_EQ(info.grantedTtlMs, 60'000u);

    // Scan each partition to its end, three rows a page: every granted key once.
    std::multiset<std::string> seen;
    rc::CompletionsConfig qc;
    qc.maxResultBytes = 1u << 14;
    qc.capacityBytes  = 1u << 18;
    rc::Completions q(c, qc);
    for (int32_t part = 0; part < lt.partitionCount(); ++part) {
        uint64_t resume = 0;
        bool     firstPage = true;
        bool     done = false;
        for (int page = 0; page < 40 && !done; ++page) {
            ASSERT_EQ(c.scanLeases(lt.leaseViewTopicId(), part, {}, resume, firstPage, 3, 10'000, q, 1), rc::OpResult::Accepted);
            rc::Completion cc{};
            bool           got = false;
            ASSERT_EQ(q.waitFor(1, 30s, [&](const rc::Completion& comp, std::span<const uint8_t> payload) {
                cc  = comp;
                got = true;
                EXPECT_TRUE(rc::forEachLeaseRow(payload, [&](std::span<const uint8_t> key, std::span<const uint8_t> row) {
                    seen.insert(std::string(reinterpret_cast<const char*>(key.data()), key.size()));
                    const auto decoded = rc::LeaseTopic<LeaseClaim>::decodeRow(row);
                    EXPECT_TRUE(decoded.has_value());
                    if (decoded) EXPECT_EQ(decoded->lease_holder, 99u);
                }));
            }), rc::Completions::Wait::Done);
            ASSERT_TRUE(got);
            ASSERT_EQ(cc.status, rc::OpStatus::Ok) << int(cc.status) << " " << cc.errorCode;
            const rc::LeaseScanPage pg = rc::LeaseScanPage::of(cc);
            done      = pg.done;
            resume    = pg.lastHash;
            firstPage = false;
        }
        EXPECT_TRUE(done) << "partition " << part << " scanned to its end";
    }
    std::set<std::string> keys;
    for (const std::string& s : seen) keys.insert(std::string(s.c_str(), strnlen(s.c_str(), s.size())));
    EXPECT_EQ(seen.size(), std::size_t(kKeys)) << "no key twice";
    EXPECT_EQ(keys, granted) << "the scan returns exactly the granted set";
}

TEST(LeaseTopicBrokerRegression, LeaseKeysWideRegression) {
    OneBroker b("lease-wide", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    defineLease(c, "wide", 1, "claim", wideBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseWide>("wide", {}, 30s);

    LeaseWide row;
    for (std::size_t i = 0; i < row.claim.size(); ++i) row.claim[i] = static_cast<char>('a' + i % 26);
    row.a = 5;
    const rc::LeaseResult g = lt.grantAndWait(row);
    ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    const auto got = lt.getAndWait(row, g.version);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->claim, row.claim) << "all 128 key bytes";
    EXPECT_EQ(got->a, 5u);
    EXPECT_EQ(got->lease_version, g.version);
}

TEST(LeaseTopicBrokerRegression, SubscribeSeesLeaseOpsRegression) {
    OneBroker b("lease-sub", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    const int32_t topicId = defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    Collector col(b.clientConfig(), "claims", topicId, 2);

    const LeaseClaim row = claimRow("k-sub", 1);
    const rc::LeaseResult g = lt.grantAndWait(row);
    ASSERT_TRUE(g.ok());
    ASSERT_TRUE(lt.updateAndWait(claimRow("k-sub", 2), g.version).ok());
    ASSERT_TRUE(lt.revokeAndWait(row, g.version).ok());

    auto seen = col.waitFor(3);
    ASSERT_EQ(seen.size(), 3u);
    std::sort(seen.begin(), seen.end(), [](const Collected& x, const Collected& y) { return x.offset < y.offset; });   // one key, one partition
    EXPECT_EQ(seen[0].lop, rc::LeaseRecordOp::Granted);
    EXPECT_EQ(seen[1].lop, rc::LeaseRecordOp::Updated);
    EXPECT_EQ(seen[2].lop, rc::LeaseRecordOp::Revoked);
    EXPECT_NE(seen[0].op, rc::RecordOp::Delete);
    EXPECT_NE(seen[1].op, rc::RecordOp::Delete);
    EXPECT_EQ(seen[2].op, rc::RecordOp::Delete) << "a revoke is the lease's deletion";
    EXPECT_EQ(seen[0].offset, g.version);
}

namespace {
struct NoLost {
    int* n;
    void operator()(rc::LeaseRef, rc::LostReason) const { ++*n; }
};
struct NoRisk {
    int* n;
    void operator()(rc::LeaseRef, rc::AtRiskEdge) const { ++*n; }
};
}  // namespace

// Task 15: a LeaseSession keeps three real leases alive across several ttl/3 periods (ttl 1 s), then close() revokes them all.
TEST(LeaseTopicBrokerRegression, SessionKeepsAliveAndClosesRegression) {
    OneBroker b("lease-session", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);

    int lost = 0, risk = 0;
    rc::LeaseSession<8, NoLost, NoRisk> session(c, NoLost{&lost}, NoRisk{&risk});
    rc::LeaseOptions o;
    o.ttlMs = 1000;
    std::vector<rc::LeaseRef> refs;
    std::vector<LeaseClaim>   rows;
    for (int i = 0; i < 3; ++i) {
        const LeaseClaim row = claimRow("sess-" + std::to_string(i));
        const int64_t    sentNs = c.nowNs();
        const rc::LeaseResult g = lt.grantAndWait(row, o);
        ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
        rc::OpResult err = rc::OpResult::Accepted;
        const rc::LeaseRef ref = session.add(lt, row, g.version, 1000, sentNs, &err);
        ASSERT_EQ(err, rc::OpResult::Accepted);
        refs.push_back(ref);
        rows.push_back(row);
    }
    // Drive the session on this thread until every key has had 2 keepalives answered Ok (3 keys x 2), within a 5 s wall cap.
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (session.core().okAnswers() < 6 && std::chrono::steady_clock::now() < deadline) {
        session.doWork(c.nowNs());
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_GE(session.core().okAnswers(), 6u);
    EXPECT_EQ(lost, 0);
    EXPECT_EQ(risk, 0) << "a healthy single node never puts a lease at risk";
    for (std::size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(session.stateOf(refs[i]), rc::LeaseEntryState::Held);
        rc::LeaseTtlInfo info;
        const rc::LeaseResult ttl = lt.timeToLiveAndWait(rows[i], &info);
        ASSERT_TRUE(ttl.ok()) << "the lease is live after the keepalives";
        EXPECT_GT(info.remainingMs, 0);
    }
    session.close();
    const auto closeDeadline = std::chrono::steady_clock::now() + 10s;
    while (!session.isClosed() && std::chrono::steady_clock::now() < closeDeadline) {
        session.doWork(c.nowNs());
        std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(session.isClosed());
    EXPECT_EQ(lost, 0) << "a close is not a loss";
    for (const LeaseClaim& row : rows) {
        rc::LeaseTtlInfo info;
        const rc::LeaseResult ttl = lt.timeToLiveAndWait(row, &info);
        EXPECT_FALSE(ttl.ok()) << "close() revoked the lease: status " << int(ttl.status) << " err " << ttl.errorCode << " remaining " << info.remainingMs << " ver " << info.version;
    }
}

namespace {
struct WatchLog {
    std::map<std::string, int64_t> live;   // key bytes -> version
    int bad = 0, readies = 0;
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<WatchLog*>(c);
        const std::string k(reinterpret_cast<const char*>(e.key.data()), e.key.size());
        switch (e.kind) {
        case rc::LeaseEventKind::Granted:
            if (!w.live.emplace(k, e.version).second) ++w.bad;
            break;
        case rc::LeaseEventKind::Updated:
            if (w.live.count(k) == 0) ++w.bad;
            break;
        case rc::LeaseEventKind::Revoked:
        case rc::LeaseEventKind::Expired:
            if (w.live.erase(k) == 0) ++w.bad;
            break;
        case rc::LeaseEventKind::Vanished: ++w.bad; break;   // no break happens: a lease never vanishes
        case rc::LeaseEventKind::Ready: ++w.readies; break;
        case rc::LeaseEventKind::Failed: ++w.bad; break;
        }
    }
};
}  // namespace

// Task 16: startFromNow against a real broker while grants and revokes are produced: after Ready and the producer's last ack the watch's live
// set is exactly the set of keys the broker reports live, each once (the hash-range handoff loses and duplicates nothing).
TEST(LeaseTopicBrokerRegression, WatchStartFromNowRealBrokerRegression) {
    constexpr int kInitial = 300, kGrants = 50, kRevokes = 50;
    OneBroker b("lease-watch", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    defineLease(c, "claims", 2, "claim", claimBlob());
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);

    rc::LeaseOptions o;
    o.holder = 5;
    std::map<std::string, int64_t> truth;   // the producer's own view: key -> version
    const auto keyOf = [](int i) { return "w-" + std::to_string(i); };
    for (int i = 0; i < kInitial; ++i) {
        const rc::LeaseResult g = lt.grantAndWait(claimRow(keyOf(i)), o);
        ASSERT_TRUE(g.ok()) << i;
        truth[keyOf(i)] = g.version;
    }

    rc::LeaseWatchOptions wo;
    wo.maxRows = 16;      // many pages a partition, so the produce loop below overlaps the scan
    wo.maxLive = 1024;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WatchLog log;
    watch.setHandler(&WatchLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);

    for (int i = 0; i < kGrants || i < kRevokes; ++i) {
        watch.doWork(c.nowNs());
        if (i < kGrants) {
            const rc::LeaseResult g = lt.grantAndWait(claimRow(keyOf(kInitial + i)), o);
            ASSERT_TRUE(g.ok()) << "grant " << i;
            truth[keyOf(kInitial + i)] = g.version;
        }
        watch.doWork(c.nowNs());
        if (i < kRevokes) {
            const std::string k = keyOf(i * 2);   // every other initial key
            const rc::LeaseResult r = lt.revokeAndWait(claimRow(k), truth.at(k));
            ASSERT_TRUE(r.ok()) << "revoke " << i;
            truth.erase(k);
        }
    }
    // The broker's answer for every key ever granted.
    std::map<std::string, int64_t> live;
    for (int i = 0; i < kInitial + kGrants; ++i) {
        rc::LeaseTtlInfo info;
        const LeaseClaim row = claimRow(keyOf(i));
        const rc::LeaseResult ttl = lt.timeToLiveAndWait(row, &info);
        if (ttl.ok()) live[std::string(row.claim.data(), row.claim.size())] = info.version;
    }
    ASSERT_EQ(live.size(), truth.size());
    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while ((!watch.isReady() || log.live != live) && std::chrono::steady_clock::now() < deadline) {
        watch.doWork(c.nowNs());
        std::this_thread::sleep_for(2ms);
    }
    for (int i = 0; i < 50; ++i) watch.doWork(c.nowNs());
    ASSERT_FALSE(watch.failed());
    EXPECT_TRUE(watch.isReady());
    EXPECT_EQ(log.readies, 1);
    EXPECT_EQ(log.bad, 0) << "each lease granted once, ended once";
    EXPECT_EQ(log.live, live) << "the watch's live set is the broker's";
    EXPECT_EQ(watch.liveCount(), live.size());
    EXPECT_EQ(live.size(), std::size_t(kInitial + kGrants - kRevokes));
}
