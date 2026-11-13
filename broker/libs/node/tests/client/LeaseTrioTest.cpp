// LeaseTrioTest.cpp - lease topics Task 17 (spec §16.2, decision 4): THREE real brokers (LeaseTrio.h), a real client, a replicated lease
// topic (replication factor 3, acks=all).  The lease clock runs in real time here (short TTLs: the read leases of a multi-node cluster
// are stamped on the real clock, so the injected clock is single-node only); every wait is bounded.
#include "support/LeaseTrio.h"

#include <rune/client/lease/LeaseWatch.h>

#include <gtest/gtest.h>

#include <map>
#include <atomic>
#include <set>
#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
using rune::gen::LeaseClaim;
using Clk = std::chrono::steady_clock;

namespace {
constexpr uint16_t kNotFound = static_cast<uint16_t>(rune::protocol::client::ErrorCode::LeaseNotFound);

int64_t msSince(Clk::time_point t0) { return std::chrono::duration_cast<std::chrono::milliseconds>(Clk::now() - t0).count(); }

/// A write/read through a leader change: the client follows redirects inside its own deadline; one more bounded retry covers the moment a
/// result is "in doubt" (a timeout while the new leader is still settling).  Returns the first non-transport result.
template <class F>
rc::LeaseResult retried(F&& f) {
    rc::LeaseResult r;
    for (int i = 0; i < 40; ++i) {
        try { r = f(); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        if (r.ok() || r.errorCode != 0) return r;
        std::this_thread::sleep_for(100ms);
    }
    return r;
}
std::optional<LeaseClaim::LeasesRow> getRetried(LeaseTrio::Lt& lt, const LeaseClaim& r) {
    for (int i = 0; i < 40; ++i) {
        try { return lt.getAndWait(r); } catch (const std::exception&) { std::this_thread::sleep_for(100ms); }
    }
    throw std::runtime_error("getAndWait never answered");
}
}  // namespace

TEST(LeaseTrioRegression, SmokeGrantExpireRegression) {
    LeaseTrio t("lease-trio-smoke");
    const auto topic = t.defineLease("claims", 2, 3000, 1000, 60'000);
    auto lt = t.open(topic);
    const LeaseClaim r = lt3::row("trio-k", 1);
    rc::LeaseOptions o;
    o.ttlMs = 1500;
    const rc::LeaseResult g = lt.grantAndWait(r, o);
    ASSERT_TRUE(g.ok()) << int(g.status) << " " << g.errorCode;
    EXPECT_TRUE(lt.getAndWait(r).has_value());
    EXPECT_TRUE(LeaseTrio::until(20s, [&] { return !lt.getAndWait(r).has_value(); })) << "the lease expires on the 3-node cluster";
}

// LeaderChange{Uncommitted, CommittedNotFolded, Folded} x {Revoke, KeepAlive1, TimeToLive, PointRead, BrokerExpire} (one leader kill):
//  - a revoke acknowledged before the kill stays revoked (Folded); an acked grant survives it;
//  - the keepalive, TimeToLive and the point read are served by the NEW leader for the pre-change version (the folded state is complete);
//  - a revoke racing the kill is applied or not, never half (Uncommitted / CommittedNotFolded: the key is gone, or live at the same version);
//  - a lease that was due while the leader died is expired by the new leader (the Expire is re-derived, not lost).
TEST(LeaseTrioRegression, LeaderChangeCellsRegression) {
    LeaseTrio t("lease-trio-lc");
    const auto topic = t.defineLease("claims", 1, 30'000, 1000, 600'000);
    auto lt = t.open(topic);
    rc::LeaseOptions longTtl;
    longTtl.ttlMs = 60'000;
    rc::LeaseOptions shortTtl;
    shortTtl.ttlMs = 2500;
    std::map<std::string, int64_t> v;
    for (const char* k : {"alive", "revoked", "racing", "ka"}) {
        const rc::LeaseResult g = lt.grantAndWait(lt3::row(k, 7), longTtl);
        ASSERT_TRUE(g.ok()) << k;
        v[k] = g.version;
    }
    const rc::LeaseResult gs = lt.grantAndWait(lt3::row("due", 8), shortTtl);
    ASSERT_TRUE(gs.ok());
    ASSERT_TRUE(lt.revokeAndWait(lt3::row("revoked"), v["revoked"]).ok());   // Folded: acked, then the leader dies
    // Uncommitted / CommittedNotFolded: a revoke issued on a helper thread races the kill.
    std::atomic<int> raceDone{0};
    rc::LeaseResult raceRes;
    std::thread racer([&] { raceRes = retried([&] { return lt.revokeAndWait(lt3::row("racing"), v["racing"]); }); raceDone = 1; });
    const int32_t killed = t.killLeader(topic, 0);
    racer.join();
    ASSERT_GT(killed, 0);

    // Folded: the acked revoke is still revoked, the acked grants are still live at the same versions.
    EXPECT_FALSE(getRetried(lt, lt3::row("revoked")).has_value()) << "an acked revoke survives the leader change";
    const auto alive = getRetried(lt, lt3::row("alive"));
    ASSERT_TRUE(alive.has_value()) << "an acked grant survives the leader change";
    EXPECT_EQ(alive->lease_version, v["alive"]);
    // PointRead / TimeToLive / KeepAlive1 on the new leader, pre-change version.
    rc::LeaseTtlInfo info;
    const rc::LeaseResult ttl = retried([&] { return lt.timeToLiveAndWait(lt3::row("alive"), &info); });
    ASSERT_TRUE(ttl.ok()) << int(ttl.status) << " " << ttl.errorCode;
    EXPECT_EQ(info.version, v["alive"]);
    EXPECT_GT(info.remainingMs, 0);
    EXPECT_TRUE(retried([&] { return lt.keepAliveOnceAndWait(lt3::row("ka"), v["ka"]); }).ok()) << "the pre-change version still keeps the lease";
    // The racing revoke: applied (gone) or not (live at the same version); the reply agrees with the state.
    const auto racing = getRetried(lt, lt3::row("racing"));
    if (raceRes.ok()) EXPECT_FALSE(racing.has_value()) << "an acked revoke is applied";
    else if (racing.has_value()) EXPECT_EQ(racing->lease_version, v["racing"]);
    // BrokerExpire: the lease that was due while the leader died is expired by the new leader.
    EXPECT_TRUE(LeaseTrio::until(40s, [&] { return !getRetried(lt, lt3::row("due")).has_value(); })) << "the new leader expires the due lease";
}

// R3 / M2: a promoted leader keeps the old leader in the ISR when it was in sync; once the old leader is back, the ISR is full again and
// acks=all writes keep succeeding with no loss.
TEST(LeaseTrioRegression, PromotionKeepsOldLeaderInIsrRegression) {
    LeaseTrio t("lease-trio-isr");
    const auto topic = t.defineLease("claims", 1, 30'000, 1000, 600'000);
    auto lt = t.open(topic);
    ASSERT_TRUE(t.awaitIsrSize(topic, 0, 3, 30s));
    std::set<std::string> acked;
    for (int i = 0; i < 20; ++i) {
        ASSERT_TRUE(lt.grantAndWait(lt3::row("isr-" + std::to_string(i), uint64_t(i))).ok());
        acked.insert("isr-" + std::to_string(i));
    }
    const int32_t old = t.killLeader(topic, 0);
    for (int i = 20; i < 30; ++i) {   // writes continue on the two survivors
        ASSERT_TRUE(retried([&] { return lt.grantAndWait(lt3::row("isr-" + std::to_string(i), uint64_t(i))); }).ok()) << i;
        acked.insert("isr-" + std::to_string(i));
    }
    t.restartNode(old, topic);
    EXPECT_TRUE(t.awaitIsrSize(topic, 0, 3, 60s)) << "the old leader rejoins the ISR";
    for (const std::string& k : acked) EXPECT_TRUE(getRetried(lt, lt3::row(k)).has_value()) << "acked grant lost: " << k;
    EXPECT_TRUE(retried([&] { return lt.grantAndWait(lt3::row("isr-after", 1)); }).ok()) << "acks=all writes succeed with the full ISR";
}

// PromotionBound (with and without checkpoints) + CheckpointInFlight + CheckpointClearLeaderKill: a lease's life across a leader change
// is never SHORTER than its ttl since the last keepalive (never early), and ends no later than ttl + the promotion bound.
namespace {
void promotionBound(bool checkpoint) {
    LeaseTrio t(checkpoint ? "lease-trio-pb-ck" : "lease-trio-pb");
    const auto topic = t.defineLease("claims", 1, 6000, 1000, 600'000, checkpoint, 1000);
    auto lt = t.open(topic);
    rc::LeaseOptions o;
    o.ttlMs = 6000;
    constexpr int kKeys = 8;
    const uint64_t ckpt0 = t.checkpointsOnLeader(topic, 0);
    std::map<int, int64_t> ver;
    for (int i = 0; i < kKeys; ++i) {
        const rc::LeaseResult g = lt.grantAndWait(lt3::row("pb-" + std::to_string(i), uint64_t(i)), o);
        ASSERT_TRUE(g.ok());
        ver[i] = g.version;
    }
    if (checkpoint) {   // a checkpoint pass covers every key: checkpoint records are in flight / appended
        ASSERT_TRUE(LeaseTrio::until(30s, [&] { return t.checkpointsOnLeader(topic, 0) >= ckpt0 + kKeys; }));
    } else {
        std::this_thread::sleep_for(1500ms);   // the leases age by about one checkpoint interval (the same timeline as the checkpointing pass)
    }
    // A keepalive of key 0 is the "clear" of its checkpoint (CheckpointClearLeaderKill): it re-arms to the full ttl, then the leader dies.
    const auto tKeep = Clk::now();
    const rc::LeaseResult ka = lt.keepAliveOnceAndWait(lt3::row("pb-0"), ver[0]);
    ASSERT_TRUE(ka.ok()) << int(ka.status) << " " << ka.errorCode;
    const auto tGrant = tKeep - 1500ms;
    ASSERT_GT(t.killLeader(topic, 0), 0);
    // Every other key ends no earlier than its grant + ttl (never early) and within ttl + a generous promotion bound; key 0 no earlier than
    // its keepalive + ttl.
    int64_t endedAtMs[kKeys];
    for (int i = 0; i < kKeys; ++i) endedAtMs[i] = -1;
    const auto end = Clk::now() + 40s;
    while (Clk::now() < end) {
        bool all = true;
        for (int i = 0; i < kKeys; ++i) {
            if (endedAtMs[i] >= 0) continue;
            bool live = true;
            try { live = lt.getAndWait(lt3::row("pb-" + std::to_string(i))).has_value(); } catch (const std::exception&) {}
            if (!live) endedAtMs[i] = msSince(i == 0 ? tKeep : tGrant);
            else all = false;
        }
        if (all) break;
    }
    for (int i = 0; i < kKeys; ++i) {
        ASSERT_GE(endedAtMs[i], 0) << "key " << i << " never expired";
        EXPECT_GE(endedAtMs[i], 6000 - 300) << "key " << i << " expired EARLY (a leader change must never shorten a lease)";
        EXPECT_LE(endedAtMs[i], 6000 + 25'000) << "key " << i << " expired later than ttl + the promotion bound";
    }
}
}  // namespace

TEST(LeaseTrioRegression, PromotionBoundRegression) {
    promotionBound(false);   // the checkpointing pass is CheckpointClearLeaderKillRegression (split to stay under 20 s)
}

TEST(LeaseTrioRegression, CheckpointClearLeaderKillRegression) { promotionBound(true); }

TEST(LeaseTrioRegression, CheckpointInFlightRegression) {
    // R4: checkpoint records are appended every interval for every live lease; the leader dies with some in flight.  No acked state is lost
    // and no lease is shortened (a lease with a long ttl is still live at the same version, with remaining time not above its ttl).
    LeaseTrio t("lease-trio-ckflight");
    const auto topic = t.defineLease("claims", 1, 60'000, 1000, 600'000, true, 1000);
    auto lt = t.open(topic);
    rc::LeaseOptions o;
    o.ttlMs = 60'000;
    const uint64_t ckpt0 = t.checkpointsOnLeader(topic, 0);
    std::map<int, int64_t> ver;
    std::map<int, lt3::Clock::time_point> sent;   // just before each grant is sent: the real time that lease has aged is at most msSince(sent)
    for (int i = 0; i < 12; ++i) {
        sent[i] = lt3::Clock::now();
        const rc::LeaseResult g = lt.grantAndWait(lt3::row("ck-" + std::to_string(i), uint64_t(i)), o);
        ASSERT_TRUE(g.ok());
        ver[i] = g.version;
    }
    // Two checkpoint passes: Checkpoint records for every live lease are appended.
    ASSERT_TRUE(LeaseTrio::until(30s, [&] { return t.checkpointsOnLeader(topic, 0) >= ckpt0 + 2 * 12; }));
    ASSERT_GT(t.killLeader(topic, 0), 0);
    for (int i = 0; i < 12; ++i) {
        const auto row = getRetried(lt, lt3::row("ck-" + std::to_string(i)));
        ASSERT_TRUE(row.has_value()) << i;
        EXPECT_EQ(row->lease_version, ver[i]);
        rc::LeaseTtlInfo info;
        ASSERT_TRUE(retried([&] { return lt.timeToLiveAndWait(lt3::row("ck-" + std::to_string(i)), &info); }).ok());
        EXPECT_LE(info.remainingMs, 60'000 + 1000) << "never more than the ttl";
        // Never shortened: the new leader resumes from the last checkpoint (taken before the kill), so the lease keeps at least its ttl minus
        // the real time since its grant, less one checkpoint interval (1 s). Time-independent: a slow failover ages it, never shortens it.
        const int64_t aged = std::chrono::duration_cast<std::chrono::milliseconds>(lt3::Clock::now() - sent[i]).count();
        EXPECT_GE(info.remainingMs, 60'000 - aged - 1000) << "a checkpointed lease keeps its time through the change (aged " << aged << " ms)";
    }
}

// All three brokers stop and start again: the leases survive, the versions do not move, a short lease is expired after the restart.
// (The 24 h / 400-day clock jumps of the plan need the injected clock, which is single-node only: LeaseIntegrationRegression.CompactionRestart
// covers the clean restart on one node; here is the real-time whole-cluster restart.)
TEST(LeaseTrioRegression, WholeClusterRestartRegression) {
    LeaseTrio t("lease-trio-restart");
    const auto topic = t.defineLease("claims", 1, 30'000, 1000, 86'400'000, true, 1000);
    auto lt = t.open(topic);
    rc::LeaseOptions longO;
    longO.ttlMs = 3'600'000;
    rc::LeaseOptions shortO;
    shortO.ttlMs = 4000;
    const rc::LeaseResult gl = lt.grantAndWait(lt3::row("long", 1), longO);
    const rc::LeaseResult gs = lt.grantAndWait(lt3::row("short", 2), shortO);
    ASSERT_TRUE(gl.ok() && gs.ok());
    lt.close();
    for (int id : {1, 2, 3}) t.nodes().stop(id);
    for (int id : {1, 2, 3}) t.nodes().startNode(id);
    ASSERT_TRUE(t.nodes().awaitCatalogLeader(60s));
    std::vector<int32_t> leaders;
    ASSERT_TRUE(catalog_cluster::awaitLeaders(t.nodes(), topic.id, topic.partitions, leaders, 90s));
    auto lt2 = t.open(topic);
    const auto row = getRetried(lt2, lt3::row("long"));
    ASSERT_TRUE(row.has_value()) << "the long lease survives the whole-cluster restart";
    EXPECT_EQ(row->lease_version, gl.version);
    rc::LeaseTtlInfo info;
    ASSERT_TRUE(retried([&] { return lt2.timeToLiveAndWait(lt3::row("long"), &info); }).ok());
    EXPECT_GT(info.remainingMs, 3'000'000);
    EXPECT_LE(info.remainingMs, 3'600'000 + 1000);
    EXPECT_TRUE(LeaseTrio::until(60s, [&] { return !getRetried(lt2, lt3::row("short")).has_value(); })) << "the short lease ends after the restart";
    const rc::LeaseResult g2 = retried([&] { return lt2.grantAndWait(lt3::row("short", 3)); });
    ASSERT_TRUE(g2.ok());
    EXPECT_GT(g2.version, gs.version) << "a regrant after the restart has a larger version";
}

// L22: a keepalive stream held at 50 ms (300 ms per-call deadline) keeps a lease alive through repeated leader changes; the longest gap
// between two successful keepalives (the outage) is measured per change.  2 forced cycles, each: kill the leader -> wait for the new leader ->
// 1.5 s -> restart the node -> wait (bounded, 30 s) until the restarted node leads the partition again -> 2 s (#883: one run measures both
// kinds of change; it was run twice, once per test, at over 20 s each).  Two kinds of leader change happen
// in a cycle: the KILL of the leader (a survivor is promoted) and the REJOIN of the killed node (leadership moves back).  A gap is classified
// by where it STARTS: in [kill, restart) it is a kill gap, in [restart, next kill) a rejoin gap.  A stream still failing when the run ends
// counts as a gap ending at the stop time.
namespace {
struct Outage { int64_t killMaxMs = 0, rejoinMaxMs = 0; };
Outage measureOutage(const char* tag) {
    LeaseTrio t(tag);
    const auto topic = t.defineLease("claims", 1, 30'000, 1000, 600'000);
    auto lt = t.open(topic);
    rc::LeaseOptions o;
    o.ttlMs = 30'000;
    const rc::LeaseResult g = lt.grantAndWait(lt3::row("outage", 1), o);
    if (!g.ok()) throw std::runtime_error("grant failed");
    // The keepalive stream runs on its own thread for the whole run (the only user of the lease handle until it is joined); this thread forces
    // the changes (kill + restart cycles, which block this thread but not the stream).
    std::atomic<bool> stop{false};
    const auto t0 = Clk::now();
    std::vector<std::pair<int64_t, int64_t>> gaps;   // (end ms since t0, gap ms) of every gap above 400 ms
    std::thread stream([&] {
        auto lastOk = Clk::now();
        while (!stop.load(std::memory_order_acquire)) {
            try {
                rc::LeaseOptions ko;
                ko.opTimeoutMs = 300;   // a keepalive in flight to the dead leader is retried after 300 ms, not after the handle's ttl/3
                const rc::LeaseResult ka = lt.keepAliveOnceAndWait(lt3::row("outage"), g.version, ko);
                if (ka.ok()) {
                    const int64_t gap = msSince(lastOk);
                    if (gap > 400) gaps.push_back({msSince(t0), gap});
                    lastOk = Clk::now();
                }
            } catch (const std::exception&) {}
            std::this_thread::sleep_for(50ms);
        }
        const int64_t gap = msSince(lastOk);   // still failing at the end of the run: a gap that ends at the stop time
        if (gap > 400) gaps.push_back({msSince(t0), gap});
    });
    std::this_thread::sleep_for(500ms);
    std::vector<std::pair<int64_t, int64_t>> killWindows;   // [kill, restart) in ms since t0
    for (int change = 0; change < 2; ++change) {
        const int64_t killAt = msSince(t0);
        const int32_t dead = t.killLeader(topic, 0);   // returns once a survivor leads
        std::this_thread::sleep_for(1500ms);   // > the 1.5 s budget: a kill gap ends (or is over budget) before the restart
        const int64_t restartAt = msSince(t0);
        killWindows.push_back({killAt, restartAt});
        t.restartNode(dead, topic);
        // The restarted node takes the partition back (bounded): the rejoin leader change happens here, not inside the next kill.
        if (!LeaseTrio::until(30s, [&] { return t.leaderOf(topic, 0) == dead; })) std::fprintf(stderr, "L22: node %d did not lead again within 30 s\n", dead);
        std::this_thread::sleep_for(2s);
    }
    stop.store(true, std::memory_order_release);
    stream.join();
    Outage out;
    for (const auto& gp : gaps) {
        const int64_t start = gp.first - gp.second;
        bool kill = false;
        for (const auto& w : killWindows) kill = kill || (start + 50 >= w.first && start < w.second);
        (kill ? out.killMaxMs : out.rejoinMaxMs) = std::max(kill ? out.killMaxMs : out.rejoinMaxMs, gp.second);
        std::fprintf(stderr, "L22 %s gap of %lld ms starting at %lld ms\n", kill ? "kill" : "rejoin", (long long)gp.second, (long long)start);
    }
    const auto alive = lt.getAndWait(lt3::row("outage"));
    if (!alive.has_value()) throw std::runtime_error("the lease died during the leader changes");
    return out;
}
}  // namespace

// The leader change that follows the REJOIN of the killed node is a leader change too (L22 is a Herald budget for any forced change).
TEST(LeaseTrioRegression, KeepAliveOutageUnderLeaderChangeRegression) {
    const Outage o = measureOutage("lease-trio-outage");
    if (lt3::kSanitized) GTEST_SKIP() << "the L22 1.5 s outage budget is meaningless under a sanitizer (the lease survived both changes)";
    EXPECT_LT(o.killMaxMs, 1500) << "the longest keepalive outage after the kill of the leader, over 2 forced changes (L22)";
    EXPECT_LT(o.rejoinMaxMs, 1500) << "the longest keepalive outage when the restarted node takes the leadership back (L22)";
}

// ScanLeases x LeaderChangeMidScan: page partition 0 to the middle, the leader dies, the scan resumes with the returned lastHash on the new
// leader: the union of the pages is every key exactly once.
TEST(LeaseTrioRegression, ScanLeasesLeaderChangeMidScanRegression) {
    constexpr int kKeys = 300;
    LeaseTrio t("lease-trio-scan");
    const auto topic = t.defineLease("claims", 1, 60'000, 1000, 600'000);
    auto lt = t.open(topic);
    std::set<std::string> granted;
    for (int i = 0; i < kKeys; ++i) {
        const std::string k = "scan-" + std::to_string(i);
        ASSERT_TRUE(lt.grantAndWait(lt3::row(k, uint64_t(i))).ok()) << i;
        granted.insert(k);
    }
    rc::CompletionsConfig qc;
    qc.maxResultBytes = 1u << 14;
    qc.capacityBytes = 1u << 18;
    rc::Completions q(t.client(), qc);
    std::multiset<std::string> seen;
    uint64_t resume = 0;
    bool first = true, done = false, killed = false;
    for (int page = 0; page < 200 && !done; ++page) {
        if (page == 1 && !killed) {   // after the first page: the middle of the scan
            ASSERT_GT(t.killLeader(topic, 0), 0);
            killed = true;
        }
        rc::OpResult acc = rc::OpResult::Closed;
        for (int attempt = 0; attempt < 60 && acc != rc::OpResult::Accepted; ++attempt) {
            acc = t.client().scanLeases(lt.leaseViewTopicId(), 0, {}, resume, first, 64, 10'000, q, 1);
            if (acc != rc::OpResult::Accepted) std::this_thread::sleep_for(100ms);
        }
        ASSERT_EQ(acc, rc::OpResult::Accepted);
        rc::Completion cc{};
        bool got = false;
        (void)q.waitFor(1, 30s, [&](const rc::Completion& comp, std::span<const uint8_t> payload) {
            cc = comp;
            got = true;
            (void)rc::forEachLeaseRow(payload, [&](std::span<const uint8_t> key, std::span<const uint8_t>) {
                seen.insert(std::string(reinterpret_cast<const char*>(key.data()), strnlen(reinterpret_cast<const char*>(key.data()), key.size())));
            });
        });
        ASSERT_TRUE(got);
        if (cc.status != rc::OpStatus::Ok) {   // a page lost with the leader: the scan resumes from the last good hash (nothing recorded)
            continue;
        }
        const rc::LeaseScanPage pg = rc::LeaseScanPage::of(cc);
        done = pg.done;
        resume = pg.lastHash;
        first = false;
    }
    ASSERT_TRUE(done);
    ASSERT_TRUE(killed);
    EXPECT_EQ(seen.size(), granted.size()) << "no key twice, none lost";
    EXPECT_EQ(std::set<std::string>(seen.begin(), seen.end()), granted);
}

// Grant x LeaderChangeGrantAck: grants issued right before the leader dies.  The client's reply is in doubt when the leader dies between the
// append and the ack; the outcome is one live lease per key at ONE version: an acked grant is that lease (no loss), an unacked one is either
// applied (the retry answers KeyExists) or not (the retry succeeds), never both and never neither.
TEST(LeaseTrioRegression, GrantAckLeaderChangeRegression) {
    constexpr int kKeys = 12;
    LeaseTrio t("lease-trio-grantack");
    const auto topic = t.defineLease("claims", 1, 120'000, 1000, 600'000);
    auto lt = t.open(topic);
    std::atomic<int> started{0};
    std::vector<rc::LeaseResult> first(kKeys);
    std::thread writer([&] {
        for (int i = 0; i < kKeys; ++i) {
            started.fetch_add(1, std::memory_order_release);
            rc::LeaseOptions o;
            o.opTimeoutMs = 3000;
            try { first[std::size_t(i)] = lt.grantAndWait(lt3::row("ga-" + std::to_string(i), uint64_t(i)), o); }
            catch (const std::exception&) { first[std::size_t(i)] = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        }
    });
    while (started.load(std::memory_order_acquire) < 3) std::this_thread::yield();
    ASSERT_GT(t.killLeader(topic, 0), 0);
    writer.join();
    for (int i = 0; i < kKeys; ++i) {
        const LeaseClaim r = lt3::row("ga-" + std::to_string(i), uint64_t(i));
        const rc::LeaseResult& f = first[std::size_t(i)];
        const rc::LeaseResult retry = f.ok() ? f : retried([&] { return lt.grantAndWait(r); });
        const bool live = getRetried(lt, r).has_value();
        if (f.ok()) {
            ASSERT_TRUE(live) << i << ": an acked grant is live after the change";
            EXPECT_EQ(getRetried(lt, r)->lease_version, f.version) << i << ": at the acked version";
        } else {
            EXPECT_TRUE(retry.ok() || retry.errorCode == static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists))
                << i << ": the retry of an in-doubt grant is accepted or KeyExists (status " << int(retry.status) << " code " << retry.errorCode << ")";
            EXPECT_TRUE(live) << i << ": one live lease after the retry";
        }
    }
}

// BrokerCheckpoint x {LeaderChange x3, RestartOne}: on a checkpointing topic the broker's Checkpoint records move across a follower restart and
// a leader kill without changing a lease's version, and never grow its remaining time beyond the ttl.
namespace {
void brokerCheckpointCell(bool followerRestart) {
    LeaseTrio t(followerRestart ? "lease-trio-ckcells-f" : "lease-trio-ckcells-l");
    const auto topic = t.defineLease("claims", 1, 90'000, 1000, 600'000, true, 1000);
    auto lt = t.open(topic);
    rc::LeaseOptions o;
    o.ttlMs = 90'000;
    const uint64_t ckpt0 = t.checkpointsOnLeader(topic, 0);
    std::map<int, int64_t> ver;
    for (int i = 0; i < 4; ++i) {
        const rc::LeaseResult g = lt.grantAndWait(lt3::row("bc-" + std::to_string(i), uint64_t(i)), o);
        ASSERT_TRUE(g.ok());
        ver[i] = g.version;
    }
    auto check = [&](const char* when) {
        for (int i = 0; i < 4; ++i) {
            const auto row = getRetried(lt, lt3::row("bc-" + std::to_string(i)));
            ASSERT_TRUE(row.has_value()) << when << " key " << i;
            EXPECT_EQ(row->lease_version, ver[i]) << when;
            rc::LeaseTtlInfo info;
            ASSERT_TRUE(retried([&] { return lt.timeToLiveAndWait(lt3::row("bc-" + std::to_string(i)), &info); }).ok());
            EXPECT_LE(info.remainingMs, 90'000 + 1000) << when << ": never more than the ttl";
            EXPECT_GT(info.remainingMs, 60'000) << when << ": a checkpointed lease keeps its time";
        }
    };
    ASSERT_TRUE(LeaseTrio::until(30s, [&] { return t.checkpointsOnLeader(topic, 0) >= ckpt0 + 4; }));   // Checkpoint records are appended
    check("before");
    if (followerRestart) {   // RestartOne: a follower dies and comes back
        const int32_t leader = t.leaderOf(topic, 0);
        int32_t follower = -1;
        for (int id : t.nodes().runningNodes()) if (id != leader) { follower = id; break; }
        ASSERT_GT(follower, 0);
        t.nodes().crash(follower);
        std::this_thread::sleep_for(500ms);
        t.restartNode(follower, topic);
        check("after a follower restart");
    } else {   // LeaderChange: checkpoints keep flowing under the new leader
        ASSERT_GT(t.killLeader(topic, 0), 0);
        std::this_thread::sleep_for(1500ms);
        check("after the leader change");
    }
}
}  // namespace

TEST(LeaseTrioRegression, BrokerCheckpointLeaderChangeRegression) { brokerCheckpointCell(false); }
TEST(LeaseTrioRegression, BrokerCheckpointRestartOneRegression) { brokerCheckpointCell(true); }

// ---- LeaseWatch against the trio (real client, real failover) ------------------------------------------------------------
namespace {
struct WLog {
    std::map<std::string, int64_t> live;
    std::map<std::string, int> ended;     // key -> how many Revoked / Expired / Vanished it got
    std::map<std::string, int> endedKind; // the kind (int) of the last ending
    int bad = 0, readies = 0, failed = 0;
    static std::string keyOf(const rc::LeaseEvent& e) {
        const std::string raw(reinterpret_cast<const char*>(e.key.data()), e.key.size());
        return std::string(raw.c_str(), strnlen(raw.c_str(), raw.size()));
    }
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<WLog*>(c);
        const std::string k = keyOf(e);
        switch (e.kind) {
        case rc::LeaseEventKind::Granted: w.live[k] = e.version; break;
        case rc::LeaseEventKind::Updated: break;
        case rc::LeaseEventKind::Revoked: case rc::LeaseEventKind::Expired: case rc::LeaseEventKind::Vanished:
            if (w.live.erase(k) == 0) ++w.bad;
            ++w.ended[k];
            w.endedKind[k] = int(e.kind);
            break;
        case rc::LeaseEventKind::Ready: ++w.readies; break;
        case rc::LeaseEventKind::Failed: ++w.failed; break;
        case rc::LeaseEventKind::Confirmed:    // a re-run's bookkeeping: the table is already what the event says
        case rc::LeaseEventKind::RerunBegin:
        case rc::LeaseEventKind::RerunEnd: break;
        }
    }
};
/// The broker's live set: key -> version, by TimeToLive over `names`.
std::map<std::string, int64_t> freshFold(LeaseTrio::Lt& lt, const std::vector<std::string>& names) {
    std::map<std::string, int64_t> live;
    for (const std::string& n : names) {
        rc::LeaseTtlInfo info;
        const rc::LeaseResult r = retried([&] { return lt.timeToLiveAndWait(lt3::row(n), &info); });
        if (r.ok()) live[n] = info.version;
    }
    return live;
}
bool drive(rc::LeaseWatch<LeaseClaim>& w, rc::Client& c, std::chrono::milliseconds within, const std::function<bool()>& done) {
    const auto end = Clk::now() + within;
    while (Clk::now() < end) {
        w.doWork(c.nowNs());
        if (done()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return done();
}
}  // namespace

// startFromNow, the scan is held between pages (the client is driven until the first rows are applied), the leader dies, the scan continues:
// the watch reaches Ready and its live set equals a fresh fold of the lease view (keys + versions); no spurious Vanished.
TEST(LeaseWatchTrioRegression, LeaderChangeMidScanRegression) {
    constexpr int kKeys = 200;
    LeaseTrio t("lease-trio-wmid");
    const auto topic = t.defineLease("claims", 1, 120'000, 1000, 600'000);
    auto lt = t.open(topic);
    std::vector<std::string> names;
    for (int i = 0; i < kKeys; ++i) {
        names.push_back("w-" + std::to_string(i));
        ASSERT_TRUE(lt.grantAndWait(lt3::row(names.back(), uint64_t(i))).ok()) << i;
    }
    rc::LeaseWatchOptions wo;
    wo.maxRows = 8;   // 25 pages
    wo.maxLive = 1024;
    rc::LeaseWatch<LeaseClaim> watch(t.client(), lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(drive(watch, t.client(), 30s, [&] { return watch.liveCount() > 0 && !watch.isReady(); })) << "the first page is applied, the scan is not done";
    ASSERT_GT(t.killLeader(topic, 0), 0);
    ASSERT_TRUE(drive(watch, t.client(), 60s, [&] { return watch.isReady(); })) << "the watch reaches Ready after the leader change";
    const auto fold = freshFold(lt, names);
    ASSERT_TRUE(drive(watch, t.client(), 30s, [&] { return log.live == fold; })) << "the live set equals a fresh fold";
    EXPECT_EQ(log.live, fold);
    EXPECT_FALSE(watch.failed());
    EXPECT_EQ(log.readies, 1);
    EXPECT_EQ(log.bad, 0);
    for (const auto& [k, n] : log.ended) EXPECT_EQ(fold.count(k), 0u) << "spurious end of a live lease " << k << " x" << n;
}

// A watch is live, the leader dies, a key is revoked during the outage: after the failover the watch re-runs (Broken -> rescan) and the
// revoked key comes out as ended exactly once (Revoked, or Vanished from the sweep), never twice and never left live.
TEST(LeaseWatchTrioRegression, BrokenRerunVanishedRegression) {
    LeaseTrio t("lease-trio-wbroken");
    const auto topic = t.defineLease("claims", 1, 120'000, 1000, 600'000);
    auto lt = t.open(topic);
    std::vector<std::string> names;
    std::map<std::string, int64_t> ver;
    for (int i = 0; i < 40; ++i) {
        names.push_back("b-" + std::to_string(i));
        const rc::LeaseResult g = lt.grantAndWait(lt3::row(names.back(), uint64_t(i)));
        ASSERT_TRUE(g.ok());
        ver[names.back()] = g.version;
    }
    rc::LeaseWatchOptions wo;
    wo.maxRows = 16;
    wo.maxLive = 1024;
    rc::LeaseWatch<LeaseClaim> watch(t.client(), lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(drive(watch, t.client(), 30s, [&] { return watch.isReady() && log.live.size() == names.size(); }));
    ASSERT_GT(t.killLeader(topic, 0), 0);
    // The key is revoked through the new leader while the watch's subscription is broken.
    const std::string victim = names[7];
    ASSERT_TRUE(retried([&] { return lt.revokeAndWait(lt3::row(victim), ver[victim]); }).ok());
    names.erase(names.begin() + 7);
    const auto fold = freshFold(lt, names);
    ASSERT_EQ(fold.size(), names.size());
    ASSERT_TRUE(drive(watch, t.client(), 60s, [&] { return log.live == fold; })) << "the watch converges on the fresh fold after the failover";
    // Let the re-run settle completely, then check the victim.
    drive(watch, t.client(), 2s, [] { return false; });
    EXPECT_EQ(log.live, fold);
    EXPECT_EQ(log.ended[victim], 1) << "the revoked key ends exactly once";
    EXPECT_FALSE(watch.failed() && log.failed > 1);
    EXPECT_EQ(log.bad, 0);
}
