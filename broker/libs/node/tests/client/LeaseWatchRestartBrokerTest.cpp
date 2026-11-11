// LeaseWatchRestartBrokerTest.cpp - LeaseWatch closeAsync / restart / re-run events (review batch A, I8): the REAL client against a REAL
// single-node broker (shard runtime, default config), in Thread mode and in Invoker mode.  The watch is closed, a lease is updated, one is
// revoked and one is granted while it is closed, and restart() folds the watch's table to the broker's: Confirmed for the kept leases (the
// updated one with its new row), Vanished for the revoked one, Granted for the new one, one RerunBegin / RerunEnd pair a partition.
#include "client/OneBroker.h"

#include "gen/lease_claim.gen.h"

#include <rune/client/Client.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test;
namespace rc = rune::client;

namespace {

using rune::gen::LeaseClaim;
using Kind = rc::LeaseEventKind;
using Life = rc::LeaseWatchBase::Life;

LeaseClaim claimRow(std::string_view key, uint64_t a = 0) {
    LeaseClaim r;
    if (!key.empty()) std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    r.a = a;
    return r;
}

/// Every event, and the protocol rules the consumer relies on: a Granted for a held key, an end / Confirmed for an unheld one, a RerunBegin in
/// an open bracket or a RerunEnd outside one is a violation (`bad`).
struct Log {
    struct Ev { Kind kind; int32_t part; std::string key; int64_t ver; std::vector<uint8_t> bytes; };
    std::vector<Ev>                evs;
    std::map<std::string, int64_t> live;
    std::set<int32_t>              open;
    int                            bad = 0, readies = 0, failures = 0;
    static std::string keyOf(const rc::LeaseEvent& e) {
        const std::string raw(reinterpret_cast<const char*>(e.key.data()), e.key.size());
        return std::string(raw.c_str(), strnlen(raw.c_str(), raw.size()));
    }
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<Log*>(c);
        const std::string k = keyOf(e);
        w.evs.push_back({e.kind, e.partition, k, e.version, std::vector<uint8_t>(e.bytes.begin(), e.bytes.end())});
        switch (e.kind) {
        case Kind::Granted:    if (!w.live.emplace(k, e.version).second) ++w.bad; break;
        case Kind::Updated:    if (w.live.count(k) == 0) ++w.bad; break;
        case Kind::Revoked:
        case Kind::Expired:
        case Kind::Vanished:   if (w.live.erase(k) == 0) ++w.bad; break;
        case Kind::Confirmed:  if (w.live.count(k) == 0) ++w.bad; break;
        case Kind::Ready:      ++w.readies; break;
        case Kind::Failed:     ++w.failures; break;
        case Kind::RerunBegin: if (!w.open.insert(e.partition).second) ++w.bad; break;
        case Kind::RerunEnd:   if (w.open.erase(e.partition) == 0) ++w.bad; break;
        }
    }
    [[nodiscard]] int count(Kind kind, const std::string* key = nullptr) const {
        int n = 0;
        for (const Ev& e : evs) n += e.kind == kind && (key == nullptr || e.key == *key);
        return n;
    }
};

/// Drives the watch on this thread (real time, hard cap) until `done`; an Invoker client is pumped too.
template <class P>
bool drive(rc::LeaseWatchBase& w, rc::Client& c, bool invoker, P&& done) {
    const auto end = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < end) {
        if (invoker) (void)c.doWork();
        (void)w.doWork(c.nowNs());
        if (done()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return false;
}

void defineClaims(rc::Client& c, const std::string& name, uint32_t partitions) {
    rc::TopicOptions to;
    to.lease = rune::protocol::LeaseSpec{.ttlMs = 60'000, .minTtlMs = 1000, .maxTtlMs = 600'000, .maxKeys = 1000, .expiryBatch = 64,
                                         .checkpoint = 0, .checkpointIntervalMs = 1000, .checkpointRate = 1};
    to.config.cleanupPolicy = 1;
    const auto blob = rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                                 .addFixedStr("claim", 32, 1, true)
                                                 .add("a", rune::codec::PrimType::UInt64)
                                                 .addFixedStr("b", 8)
                                                 .build());
    const auto r = c.defineTopicAndWait(name, partitions, "claim", blob, rc::Persistence::Disk, to, {}, 60s);
    ASSERT_EQ(r.errorCode, 0) << r.detail;
}

/// The scenario, for the client `c` (Thread mode or Invoker mode).
void closeUpdateRestart(rc::Client& c, bool invoker) {
    constexpr uint32_t kParts = 2;
    defineClaims(c, "claims", kParts);
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("claims", {}, 30s);
    rc::LeaseOptions o;
    o.holder = 5;
    const rc::LeaseResult ga = lt.grantAndWait(claimRow("rs-a", 1), o);
    const rc::LeaseResult gb = lt.grantAndWait(claimRow("rs-b", 2), o);
    const rc::LeaseResult gc = lt.grantAndWait(claimRow("rs-c", 3), o);
    ASSERT_TRUE(ga.ok() && gb.ok() && gc.ok());

    rc::LeaseWatch<LeaseClaim> w(c, lt);
    Log log;
    w.setHandler(&Log::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> sp[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(w.startFromNow(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(drive(w, c, invoker, [&] { return w.isReady(); }));
    ASSERT_EQ(log.count(Kind::Granted), 3);
    EXPECT_EQ(log.count(Kind::RerunBegin), 0) << "a first scan that does not break has no bracket";
    const std::size_t events = log.evs.size();

    w.closeAsync();
    EXPECT_EQ(w.life(), Life::Closing);
    EXPECT_FALSE(w.isReady());
    ASSERT_TRUE(drive(w, c, invoker, [&] { return w.isClosed(); }));
    EXPECT_EQ(w.life(), Life::Closed);
    EXPECT_EQ(log.evs.size(), events) << "no event after closeAsync";

    // While the watch is closed: an update, an end and a grant; the lease view has them before the restart's scan starts.
    ASSERT_TRUE(lt.updateAndWait(claimRow("rs-a", 99), ga.version, o).ok());
    ASSERT_TRUE(lt.revokeAndWait(claimRow("rs-b"), gb.version).ok());
    const rc::LeaseResult gd = lt.grantAndWait(claimRow("rs-d", 4), o);
    ASSERT_TRUE(gd.ok());
    const auto until = std::chrono::steady_clock::now() + 30s;
    bool seen = false;
    for (; !seen && std::chrono::steady_clock::now() < until;) {
        const auto a = lt.getAndWait(claimRow("rs-a"));
        const auto b = lt.getAndWait(claimRow("rs-b"));
        const auto d = lt.getAndWait(claimRow("rs-d"));
        seen = a.has_value() && a->a == 99 && !b.has_value() && d.has_value();
        if (!seen) std::this_thread::sleep_for(2ms);
    }
    ASSERT_TRUE(seen) << "the lease view never showed the changes made while the watch was closed";

    ASSERT_EQ(w.restart(sp), rc::OpResult::Accepted);
    ASSERT_TRUE(drive(w, c, invoker, [&] { return log.readies == 2 && log.open.empty(); }));
    const std::string ka = "rs-a", kb = "rs-b", kc = "rs-c", kd = "rs-d";
    EXPECT_EQ(log.count(Kind::Confirmed, &ka), 1);
    EXPECT_EQ(log.count(Kind::Confirmed, &kc), 1);
    EXPECT_EQ(log.count(Kind::Confirmed, &kb), 0);
    EXPECT_EQ(log.count(Kind::Vanished, &kb), 1);
    EXPECT_EQ(log.count(Kind::Granted, &kd), 1);
    EXPECT_EQ(log.count(Kind::Confirmed), 2);
    EXPECT_EQ(log.count(Kind::Vanished), 1);
    EXPECT_EQ(log.count(Kind::RerunBegin), static_cast<int>(kParts));
    EXPECT_EQ(log.count(Kind::RerunEnd), static_cast<int>(kParts));
    for (const Log::Ev& e : log.evs) {
        if (e.kind != Kind::Confirmed || e.key != ka) continue;
        const auto row = rc::LeaseWatch<LeaseClaim>::decodeScanRow(e.bytes);
        ASSERT_TRUE(row.has_value());
        EXPECT_EQ(row->a, 99u) << "the Confirmed of the updated lease carries the new row";
    }
    EXPECT_EQ(log.live, (std::map<std::string, int64_t>{{ka, ga.version}, {kc, gc.version}, {kd, gd.version}}));
    EXPECT_EQ(w.liveCount(), 3u);
    EXPECT_EQ(log.bad, 0);
    EXPECT_EQ(log.failures, 0);

    // The restarted watch is live: a later grant is delivered.
    const rc::LeaseResult ge = lt.grantAndWait(claimRow("rs-e", 5), o);
    ASSERT_TRUE(ge.ok());
    ASSERT_TRUE(drive(w, c, invoker, [&] { return log.live.count("rs-e") != 0; }));
    EXPECT_EQ(log.bad, 0);

    w.closeAsync();
    ASSERT_TRUE(drive(w, c, invoker, [&] { return w.isClosed(); }));
}

}  // namespace

TEST(LeaseWatchRestartBrokerRegression, CloseUpdateRestartThreadModeRegression) {
    OneBroker b("lease-wrestart-thread", OneBroker::Runtime::Shard);
    rc::Client& c = *b.connectClient();
    closeUpdateRestart(c, false);
}

TEST(LeaseWatchRestartBrokerRegression, CloseUpdateRestartInvokerModeRegression) {
    OneBroker b("lease-wrestart-invoker", OneBroker::Runtime::Shard);
    rc::ClientConfig cfg = b.clientConfig();
    cfg.runnerMode = rc::RunnerMode::Invoker;
    rc::Client c(cfg);
    c.connectAndWait(20s);
    closeUpdateRestart(c, true);
}
