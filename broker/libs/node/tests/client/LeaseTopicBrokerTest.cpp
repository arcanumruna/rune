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
#include <rune/client/lease/LeaseTopic.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
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
    std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    r.a = a;
    std::memcpy(r.b.data(), b.data(), std::min(b.size(), r.b.size()));
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
