// LeaseTransientTest.cpp - lease topics Task 32 (spec Appendix A.1 T1-T5, A.3 carry / floors, A.6 fence, A.8 matrix): a TRANSIENT lease topic on a
// REAL broker, driven by REAL clients, on a deliberately SMALL ring so the ring wraps many times.
//   1-node (LeaseBrokerRig, injected lease clock): carry under ring pressure, a ring full of live leases still expires, a slow LeaseWatch reruns.
//   3-node (LeaseTrio, real clock): a leader change after wraps, a follower whose ring reset is then promoted, a whole-cluster restart fence
//   followed by a leader change mid-fence.
// Every wait is a poll (<= 10 ms) under a hard wall-clock cap; the cap is the failure.
#include "support/LeaseBrokerRig.h"
#include "support/LeaseTrio.h"

#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/consensus/LeaseClock.h>
#include <rune/node/Worker.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>

using namespace rune::node::test;
using namespace std::chrono_literals;
namespace rc = rune::client;
using rune::gen::LeaseClaim;
using Clk = std::chrono::steady_clock;
using Lt  = rc::LeaseTopic<LeaseClaim>;

namespace {

namespace pc = rune::protocol::client;
constexpr uint16_t kThrottled   = static_cast<uint16_t>(pc::ErrorCode::Throttled);
constexpr uint16_t kNotFound    = static_cast<uint16_t>(pc::ErrorCode::LeaseNotFound);
constexpr uint16_t kKeyExists   = static_cast<uint16_t>(pc::ErrorCode::KeyExists);
constexpr uint16_t kLogFull     = static_cast<uint16_t>(pc::ErrorCode::TransientLogFull);
constexpr uint16_t kNotLeader   = static_cast<uint16_t>(pc::ErrorCode::NotLeader);

/// The smallest ring that works: a power of two (the RUNE_TRANSIENT_LOG_RING_BYTES rule) above the lease produce reserve (max(cap/8, 64 KiB));
/// a 64 KiB ring refuses the very first append (TransientLogFull).
constexpr int32_t kRingBytes = 256 * 1024;

std::string keyName(const char* prefix, int i) {
    char b[32];
    std::snprintf(b, sizeof b, "%s%02d", prefix, i);
    return b;
}
std::string trimKey(const char* p, std::size_t n) { return std::string(p, strnlen(p, n)); }

int64_t msSince(Clk::time_point t0) { return std::chrono::duration_cast<std::chrono::milliseconds>(Clk::now() - t0).count(); }

// ---------------------------------------------------------------- 1-node helpers (LeaseBrokerRig)

rc::DefineTopicResult defineTransientLease(LeaseBrokerRig& rig, const char* name, uint32_t maxKeys = 1000) {
    rc::TopicOptions to;
    to.lease = lbr::spec(60'000, 1000, 60'000);   // an explicit max_ttl: a transient lease topic needs one (1 s is the floor of min_ttl)
    to.lease->maxKeys = maxKeys;
    to.config.cleanupPolicy = 1;
    return rig.ua().defineTopicAndWait(name, 1, "claim", lbr::claimBlob(), rc::Persistence::Transient, to, {}, 60s);
}

struct RingSnap {
    bool     has       = false;
    int64_t  start     = -1, end = -1;
    uint64_t used      = 0, cap = 0;
    uint64_t carried   = 0;
    uint64_t liveBytes = 0;
    uint8_t  fence     = 0;
    int64_t  readyNs   = 0, deadlineNs = 0;
};
RingSnap ringOf(LeaseBrokerRig& rig, int32_t topic) {
    auto& node = rig.broker().node();
    for (int32_t w = 0; w < node.workerCountForTest(); ++w) {
        const RingSnap s = node.onWorkerThreadForTest(w, [topic](rune::node::Worker& wk) {
            RingSnap r;
            const auto* rep = wk.findOwnedReplicaForTest(topic, 0);
            if (rep == nullptr || rep->memoryLog() == nullptr) return r;
            r.has   = true;
            r.start = rep->memoryLog()->logStartOffset();
            r.end   = rep->memoryLog()->logEndOffset();
            r.used  = rep->memoryLog()->bytesFrom(0);
            r.cap   = rep->transientCapacityBytes();
            if (const auto* rt = wk.leaseRuntimeForTest(topic, 0)) {
                r.carried = rt->stats().carried;
                r.fence   = rt->fenceStateCode();
                r.readyNs = rt->readyNs();
                if (const auto* f = std::get_if<rune::node::lease::LeaseRuntime::FenceFencing>(&rt->fenceState())) r.deadlineNs = f->deadlineNs;
            }
            r.liveBytes = wk.leaseTransientLiveBytesForTest(topic, 0);
            return r;
        });
        if (s.has) return s;
    }
    return {};
}
uint64_t leaseRingEnds(LeaseBrokerRig& rig) {
    auto& node = rig.broker().node();
    uint64_t n = 0;
    for (int32_t w = 0; w < node.workerCountForTest(); ++w)
        n += node.onWorkerThreadForTest(w, [](rune::node::Worker& wk) { return wk.leaseRingEndsForTest(); });
    return n;
}
uint64_t transientFullRefusals(LeaseBrokerRig& rig) {
    auto& node = rig.broker().node();
    uint64_t n = 0;
    for (int32_t w = 0; w < node.workerCountForTest(); ++w)
        n += node.onWorkerThreadForTest(w, [](rune::node::Worker& wk) { return wk.transientFullRefusalsForTest(); });
    return n;
}
void holdPush(LeaseBrokerRig& rig, int32_t topicId) {
    auto& node = rig.broker().node();
    for (int32_t w = 0; w < node.workerCountForTest(); ++w)
        (void)node.onWorkerThreadForTest(w, [topicId](rune::node::Worker& wk) { wk.holdPushForTest(topicId); return 0; });
}

/// A grant retried on Throttled only, under a hard wall-clock cap.
rc::LeaseResult grantRetrying(Lt& lt, const LeaseClaim& row, const rc::LeaseOptions& o) {
    const auto end = Clk::now() + 60s;
    rc::LeaseResult g;
    do {
        g = lt.grantAndWait(row, o);
        if (g.ok() || g.errorCode != kThrottled) return g;
        std::this_thread::sleep_for(10ms);
    } while (Clk::now() < end);
    return g;
}

/// A watch's event log: the live set (key -> version), each key's end count, and protocol violations (an end of an unheld key).
struct WLog {
    std::map<std::string, int64_t> live;
    std::map<std::string, int>     ended, granted;
    int                            bad = 0, readies = 0, failed = 0;
    static std::string keyOf(const rc::LeaseEvent& e) { return trimKey(reinterpret_cast<const char*>(e.key.data()), e.key.size()); }
    static void fn(void* c, const rc::LeaseEvent& e) {
        auto& w = *static_cast<WLog*>(c);
        const std::string k = keyOf(e);
        switch (e.kind) {
        case rc::LeaseEventKind::Granted: w.live[k] = e.version; ++w.granted[k]; break;
        case rc::LeaseEventKind::Updated: break;
        case rc::LeaseEventKind::Revoked:
        case rc::LeaseEventKind::Expired:
        case rc::LeaseEventKind::Vanished:
            if (w.live.erase(k) == 0) ++w.bad;
            ++w.ended[k];
            break;
        case rc::LeaseEventKind::Ready: ++w.readies; break;
        case rc::LeaseEventKind::Failed: ++w.failed; break;
        }
    }
};
/// Drives the watch on this thread (real time, hard cap) until `done`.
template <class P>
bool driveWatch(rc::LeaseWatchBase& w, rc::Client& c, std::chrono::milliseconds within, P&& done) {
    const auto end = Clk::now() + within;
    while (Clk::now() < end) {
        w.doWork(c.nowNs());
        if (done()) return true;
        std::this_thread::sleep_for(2ms);
    }
    w.doWork(c.nowNs());
    return done();
}

/// One pass over every partition's ScanLeases pages -> key -> lease_version.  nullopt when a page failed (the caller retries the pass).
std::optional<std::map<std::string, int64_t>> scanOnce(rc::Client& c, Lt& lt) {
    rc::CompletionsConfig qc;
    qc.maxResultBytes = 1u << 14;
    qc.capacityBytes  = 1u << 18;
    rc::Completions q(c, qc);
    std::map<std::string, int64_t> out;
    for (int32_t part = 0; part < lt.partitionCount(); ++part) {
        uint64_t resume = 0;
        bool     first = true, done = false;
        for (int page = 0; page < 400 && !done; ++page) {
            if (c.scanLeases(lt.leaseViewTopicId(), part, {}, resume, first, 64, 10'000, q, 1) != rc::OpResult::Accepted) return std::nullopt;
            rc::Completion cc{};
            bool           got = false;
            (void)q.waitFor(1, 30s, [&](const rc::Completion& comp, std::span<const uint8_t> payload) {
                cc  = comp;
                got = true;
                (void)rc::forEachLeaseRow(payload, [&](std::span<const uint8_t> key, std::span<const uint8_t> row) {
                    const auto decoded = Lt::decodeRow(row);
                    out[trimKey(reinterpret_cast<const char*>(key.data()), key.size())] = decoded ? decoded->lease_version : INT64_MIN;
                });
            });
            if (!got || cc.status != rc::OpStatus::Ok) return std::nullopt;
            const rc::LeaseScanPage pg = rc::LeaseScanPage::of(cc);
            done   = pg.done;
            resume = pg.lastHash;
            first  = false;
        }
        if (!done) return std::nullopt;
    }
    return out;
}
/// scanOnce, retried (hard cap) until a pass completes.
std::map<std::string, int64_t> scanAll(rc::Client& c, Lt& lt, std::chrono::seconds within = 60s) {
    const auto end = Clk::now() + within;
    do {
        try {
            if (auto m = scanOnce(c, lt)) return *m;
        } catch (const std::exception&) {}
        std::this_thread::sleep_for(100ms);
    } while (Clk::now() < end);
    ADD_FAILURE() << "ScanLeases never completed a pass";
    return {};
}

/// `n` updates of the filler key (the ring churn).  Throttled is retried (hard cap); ANY other refusal (TransientLogFull included) is a failure.
/// Returns "" or the failure text.
std::string churn(Lt& lt, LeaseClaim& filler, int64_t& version, uint64_t& counter, int n) {
    for (int i = 0; i < n; ++i) {
        filler.a = ++counter;
        const auto end = Clk::now() + 30s;
        rc::LeaseResult r;
        for (;;) {
            r = lt.updateAndWait(filler, version);
            if (r.ok() || r.errorCode != kThrottled || Clk::now() >= end) break;
            std::this_thread::sleep_for(5ms);
        }
        if (!r.ok()) return "update #" + std::to_string(counter) + " refused: status " + std::to_string(int(r.status)) + " code " + std::to_string(r.errorCode);
        version = r.version;
    }
    return {};
}

}  // namespace

// ================================================================================================ 1-NODE

// Spec A.3 carry + the held constraint-set change: under sustained churn on a small ring every one of K live leases is carried forward (re-logged
// above the wrap), so no grant is lost and the judge still judges (KeyExists) - and the ring never refuses the churn.
// Mutation (a): LeaseRuntime::emitCarries returns 0 -> the live leases pin the ring floor, the churn is refused TransientLogFull.
// Mutation (b): drop relogHeldConstraintSet_ in Worker::leaseDuty_ -> the held constraint-set change pins the floor, the churn is refused.
TEST(LeaseTransientBrokerRegression, CarryKeepsEveryLeaseUnderRingPressureRegression) {
    constexpr int K = 20;
    LeaseBrokerRig rig("lease-tr-carry", 60'000, 1000, 60'000, [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto d = defineTransientLease(rig, "tr");
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    rc::Client& c = rig.ua();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 60'000;   // = max_ttl; the injected lease clock stands still, so nothing ever expires

    LeaseClaim filler = rig.row("f-fill", 0);
    const rc::LeaseResult fg = grantRetrying(lt, filler, o);   // also waits the generation-0 fence open
    ASSERT_TRUE(fg.ok()) << int(fg.status) << " " << fg.errorCode;
    int64_t fillerVersion = fg.version;
    std::map<std::string, int64_t> version;
    int64_t lastOrigOffset = -1;
    for (int i = 0; i < K; ++i) {
        const std::string k = keyName("k", i);
        const rc::LeaseResult g = grantRetrying(lt, rig.row(k, uint64_t(i)), o);
        ASSERT_TRUE(g.ok()) << k << " " << int(g.status) << " " << g.errorCode;
        version[k] = g.version;
        const auto row = lt.getAndWait(rig.row(k));
        ASSERT_TRUE(row.has_value()) << k;
        lastOrigOffset = std::max(lastOrigOffset, row->lease_granted_offset);
    }
    const RingSnap before = ringOf(rig, d.topicId);
    ASSERT_TRUE(before.has);
    ASSERT_EQ(before.cap, uint64_t(kRingBytes)) << "the config's small ring is the partition's ring";
    ASSERT_GT(before.end, before.start);
    const uint64_t avg = std::max<uint64_t>(1, before.used / uint64_t(before.end - before.start));
    const int ops = int((6 * before.cap + avg - 1) / avg) + 16;   // >= 6 x the ring capacity of appended bytes

    const auto t0 = Clk::now();
    uint64_t counter = 0;
    const std::string failure = churn(lt, filler, fillerVersion, counter, ops);
    ASSERT_EQ(failure, "") << "after " << counter << " churn ops; ring " << ringOf(rig, d.topicId).start << ".." << ringOf(rig, d.topicId).end
                           << " carried " << ringOf(rig, d.topicId).carried;
    const RingSnap after = ringOf(rig, d.topicId);
    std::printf("[carry] %d churn ops of ~%llu B in %lld ms; ring start %lld end %lld carried %llu liveBytes %llu\n", ops, (unsigned long long)avg,
                (long long)msSince(t0), (long long)after.start, (long long)after.end, (unsigned long long)after.carried,
                (unsigned long long)after.liveBytes);

    EXPECT_GT(after.start, lastOrigOffset) << "the ring wrapped past every original grant";
    EXPECT_GT(after.carried, 0u) << "the leases were carried";
    const auto scanned = scanAll(c, lt);
    for (const auto& [k, v] : version) {
        ASSERT_EQ(scanned.count(k), 1u) << k << " is in the scan";
        EXPECT_EQ(scanned.at(k), v) << k << " keeps its original version";
    }
    for (const auto& [k, v] : version) {
        const rc::LeaseResult ka = lt.keepAliveOnceAndWait(rig.row(k), v);
        EXPECT_TRUE(ka.ok()) << k << " " << int(ka.status) << " " << ka.errorCode;
        const rc::LeaseResult dup = lt.grantAndWait(rig.row(k, 99), o);
        EXPECT_FALSE(dup.ok()) << k;
        EXPECT_EQ(dup.errorCode, kKeyExists) << k << ": the judge still judges";
        const auto row = lt.getAndWait(rig.row(k));
        ASSERT_TRUE(row.has_value()) << k << " is in the lease view";
        EXPECT_EQ(row->lease_version, v) << k;
    }
}

// T3: a ring full of LIVE leases still has room for the Expire records - the keys end, the ring empties, and a new grant succeeds.
TEST(LeaseTransientBrokerRegression, RingFullOfLiveLeasesExpireStillAppendsRegression) {
    LeaseBrokerRig rig("lease-tr-full", 60'000, 1000, 60'000, [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto d = defineTransientLease(rig, "tr", 100'000);   // the ring, not max_keys, must be what refuses
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    rc::Client& c = rig.ua();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    rc::LeaseWatchOptions wo;
    wo.maxLive = 4096;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(driveWatch(watch, c, 30s, [&] { return watch.isReady(); }));

    // The grants go through their own handle: a refused grant is retried by the client's producer (TransientLogFull is a retriable answer) until the
    // call's deadline, so the caller sees a Timeout, not code 42; closing the handle drops what it was still retrying.  The worker's counter
    // proves the ring (not max_keys, not a bug) is what refused.
    auto grantLt = c.openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs        = 1000;   // min ttl
    o.opTimeoutMs  = 1500;
    int granted = 0;
    bool full   = false;
    const auto t0 = Clk::now();
    for (int i = 0; i < 5000 && !full; ++i) {   // hard cap on the count; the ring (256 KiB) fills long before
        const std::string k = "b-" + std::to_string(i);
        rc::LeaseResult g;
        const auto end = Clk::now() + 30s;
        do {
            g = grantLt.grantAndWait(rig.row(k, uint64_t(i)), o);
            if (g.ok() || g.errorCode != kThrottled) break;
            std::this_thread::sleep_for(5ms);
        } while (Clk::now() < end);
        if (g.ok()) {
            ++granted;
        } else {
            ASSERT_TRUE(g.errorCode == kLogFull || g.status == rc::OpStatus::Timeout || g.status == rc::OpStatus::DeliveryFailed)
                << "grant #" << i << ": status " << int(g.status) << " code " << g.errorCode;
            full = true;
        }
        watch.doWork(c.nowNs());
    }
    grantLt.close();
    ASSERT_TRUE(full) << "the ring never filled after " << granted << " grants";
    ASSERT_GT(granted, 10);
    EXPECT_GT(transientFullRefusals(rig), 0u) << "the ring refused the append (42)";
    std::printf("[full] %d live leases when the ring answered TransientLogFull (%lld ms)\n", granted, (long long)msSince(t0));
    ASSERT_TRUE(driveWatch(watch, c, 60s, [&] { return int(log.live.size()) == granted; })) << "live " << log.live.size() << " of " << granted;

    rig.advance(std::chrono::milliseconds(10'000));   // past ttl + slack on the injected clock
    ASSERT_TRUE(driveWatch(watch, c, 90s, [&] { return log.live.empty() && int(log.ended.size()) == granted; }))
        << "live " << log.live.size() << " ended " << log.ended.size() << " of " << granted;
    for (const auto& [k, n] : log.ended) EXPECT_EQ(n, 1) << k << " ends exactly once";
    EXPECT_EQ(log.bad, 0);
    EXPECT_FALSE(watch.failed());
    bool empty = false;
    const auto end = Clk::now() + 60s;
    while (!empty && Clk::now() < end) {
        const auto m = scanOnce(c, lt);
        empty = m.has_value() && m->empty();
        if (!empty) std::this_thread::sleep_for(10ms);
    }
    EXPECT_TRUE(empty) << "ScanLeases is empty once every lease expired";
    rc::LeaseOptions again;
    again.ttlMs = 60'000;
    const rc::LeaseResult g = grantRetrying(lt, rig.row("after", 1), again);
    EXPECT_TRUE(g.ok()) << "a new grant succeeds once the leases are gone: " << int(g.status) << " " << g.errorCode;
}

// A slow reader: its cursor falls below the ring start (the ring moved on, the Revokes it never saw were evicted).  The subscription ends
// (PartitionDataReset, broker code 43), the watch reruns and ends exactly the leases it lost: k0 and k1 once each, k5 and k6 are found.
// Mutation: disable the lease branch in pushOwnedSubscriptionsImpl_ (`false && rep->leaseTransient()`) -> the cursor skips ahead silently,
// k0 / k1 never end, red.
TEST(LeaseWatchEvictedAsOfRegression, SlowWatchRerunsRegression) {
    LeaseBrokerRig rig("lease-wd-slow", 60'000, 1000, 60'000, [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto d = defineTransientLease(rig, "tr");
    ASSERT_EQ(d.errorCode, 0) << d.detail;
    rc::Client& c = rig.ua();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>("tr", {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs = 60'000;
    LeaseClaim filler = rig.row("f-fill", 0);
    const rc::LeaseResult fg = grantRetrying(lt, filler, o);
    ASSERT_TRUE(fg.ok()) << int(fg.status) << " " << fg.errorCode;
    int64_t fillerVersion = fg.version;
    std::map<std::string, int64_t> version;
    for (int i = 0; i < 5; ++i) {
        const std::string k = "k" + std::to_string(i);
        const rc::LeaseResult g = grantRetrying(lt, rig.row(k, uint64_t(i)), o);
        ASSERT_TRUE(g.ok()) << k;
        version[k] = g.version;
    }
    rc::LeaseWatchOptions wo;
    wo.maxLive = 256;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const uint8_t                  kp[1] = {'k'};
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(kp, 1)};   // the filler (f-fill) is out of the watch
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(driveWatch(watch, c, 30s, [&] { return watch.isReady() && log.live.size() == 5; }));
    // A record delivered through the subscription after the scan: the cursor now sits at the ring's end.
    ASSERT_TRUE(grantRetrying(lt, rig.row("k9", 9), o).ok());
    ASSERT_TRUE(driveWatch(watch, c, 30s, [&] { return log.live.size() == 6; }));
    const int64_t cursorAtLeast = ringOf(rig, d.topicId).end;   // everything below was delivered
    ASSERT_EQ(watch.reruns(), 0u);
    holdPush(rig, d.topicId);   // from here on the subscription is not read: a slow reader

    ASSERT_TRUE(lt.revokeAndWait(rig.row("k0"), version["k0"]).ok());
    ASSERT_TRUE(lt.revokeAndWait(rig.row("k1"), version["k1"]).ok());
    const int64_t revokeK1End = ringOf(rig, d.topicId).end;
    ASSERT_GT(revokeK1End, cursorAtLeast);
    for (const char* k : {"k5", "k6"}) ASSERT_TRUE(grantRetrying(lt, rig.row(k, 5), o).ok()) << k;

    uint64_t counter = 0;
    int64_t start = -1;
    for (int round = 0; round < 200 && start <= revokeK1End + 8; ++round) {   // hard cap: 200 x 50 updates
        ASSERT_EQ(churn(lt, filler, fillerVersion, counter, 50), "") << "round " << round;
        start = ringOf(rig, d.topicId).start;
    }
    ASSERT_GT(start, revokeK1End + 8) << "the ring moved past the revoke of k1 (held reader)";
    // Held: the watch saw nothing of it.
    driveWatch(watch, c, 200ms, [] { return false; });
    EXPECT_EQ(watch.reruns(), 0u);
    EXPECT_EQ(log.live.count("k0"), 1u) << "k0's revoke was never delivered";

    holdPush(rig, -1);   // release
    const std::set<std::string> want = {"k2", "k3", "k4", "k5", "k6", "k9"};
    ASSERT_TRUE(driveWatch(watch, c, 60s, [&] {
        std::set<std::string> have;
        for (const auto& [k, v] : log.live) have.insert(k);
        return watch.reruns() >= 1 && have == want;
    })) << "reruns " << watch.reruns() << " live " << log.live.size() << " ended " << log.ended.size();
    EXPECT_GE(watch.reruns(), 1u) << "the watch reran (the subscription ended with broker code 43, not a client-side partition rebirth)";
    EXPECT_GT(leaseRingEnds(rig), 0u) << "the worker ended the subscription (the lease branch)";
    EXPECT_EQ(log.ended["k0"], 1);
    EXPECT_EQ(log.ended["k1"], 1);
    EXPECT_EQ(log.ended.size(), 2u);
    EXPECT_EQ(log.granted["k5"], 1);
    EXPECT_EQ(log.granted["k6"], 1);
    EXPECT_EQ(log.bad, 0);
    EXPECT_FALSE(watch.failed());
    EXPECT_EQ(log.failed, 0);
}

// ================================================================================================ 3-NODE

namespace {

std::optional<rune::node::LeaseLineageNote> noteOn(LeaseTrio& t, int node, int32_t topic) {
    auto& n = t.nodes().node(node);
    for (int32_t w = 0; w < n.workerCountForTest(); ++w) {
        auto note = n.onWorkerThreadForTest(w, [topic](rune::node::Worker& wk) { return wk.leaseLineageForTest(topic, 0); });
        if (note.has_value()) return note;
    }
    return std::nullopt;
}

/// A trio op retried (hard cap) through transport hiccups and leader changes: retries Timeout (code 0), Throttled and NotLeader; any other
/// answer (KeyExists, LeaseNotFound, TransientLogFull ...) is final.
template <class F>
rc::LeaseResult opRetry(F&& f, std::chrono::seconds within = 90s) {
    const auto end = Clk::now() + within;
    rc::LeaseResult r;
    do {
        try { r = f(); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        if (r.ok() || (r.errorCode != 0 && r.errorCode != kThrottled && r.errorCode != kNotLeader)) return r;
        std::this_thread::sleep_for(20ms);
    } while (Clk::now() < end);
    return r;
}

/// Trio churn: updates of the filler; Throttled / NotLeader retried, anything else (TransientLogFull) fails.
std::string trioChurn(Lt& lt, LeaseClaim& filler, int64_t& version, uint64_t& counter, int n) {
    for (int i = 0; i < n; ++i) {
        filler.a = ++counter;
        const rc::LeaseResult r = opRetry([&] { return lt.updateAndWait(filler, version); }, 30s);
        if (!r.ok()) return "update #" + std::to_string(counter) + " refused: status " + std::to_string(int(r.status)) + " code " + std::to_string(r.errorCode);
        version = r.version;
    }
    return {};
}

/// The K keys {prefix00..} granted (ttl `ttlMs`), their versions and the highest granted offset.
struct Granted {
    std::map<std::string, int64_t> version;
    int64_t                        lastOffset = -1;
};
Granted grantKeys(Lt& lt, int k, uint32_t ttlMs) {
    Granted g;
    rc::LeaseOptions o;
    o.ttlMs = ttlMs;
    for (int i = 0; i < k; ++i) {
        const std::string key = keyName("k", i);
        const rc::LeaseResult r = opRetry([&] { return lt.grantAndWait(lt3::row(key, uint64_t(i)), o); });
        EXPECT_TRUE(r.ok()) << key << " " << int(r.status) << " " << r.errorCode;
        g.version[key] = r.version;
        try {
            const auto row = lt.getAndWait(lt3::row(key));
            if (row) g.lastOffset = std::max(g.lastOffset, row->lease_granted_offset);
        } catch (const std::exception&) {}
    }
    return g;
}

/// Churn until the leader's ring has wrapped `wraps` times (bytes appended >= wraps x capacity, estimated from the bytes per record).
int opsForWraps(const LeaseTrio::RuntimeSnap& s, int wraps) {
    const uint64_t recs = s.ringEnd > s.ringStart ? uint64_t(s.ringEnd - s.ringStart) : 1;
    const uint64_t avg  = std::max<uint64_t>(1, s.usedBytes / recs);
    return int((uint64_t(wraps) * s.capBytes + avg - 1) / avg) + 16;
}

/// The same checks after a leader change: every key scans at its version, keepalives are Ok, a duplicate grant is KeyExists.
void expectEveryLeaseHeld(rc::Client& client, Lt& lt, const Granted& g, uint32_t ttlMs) {
    const auto scanned = scanAll(client, lt, 90s);
    for (const auto& [k, v] : g.version) {
        ASSERT_EQ(scanned.count(k), 1u) << k << " is in the scan";
        EXPECT_EQ(scanned.at(k), v) << k << " keeps its version";
    }
    for (const auto& [k, v] : g.version) {
        const rc::LeaseResult ka = opRetry([&] { return lt.keepAliveOnceAndWait(lt3::row(k), v); });
        EXPECT_TRUE(ka.ok()) << k << " keepalive: " << int(ka.status) << " " << ka.errorCode;
    }
    rc::LeaseOptions o;
    o.ttlMs = ttlMs;
    for (const auto& [k, v] : g.version) {
        const rc::LeaseResult dup = opRetry([&] { return lt.grantAndWait(lt3::row(k, 99), o); });
        EXPECT_FALSE(dup.ok()) << k;
        EXPECT_EQ(dup.errorCode, kKeyExists) << k << ": the constraint set survived the leader change";
    }
}

constexpr uint32_t kLongTtlMs = 120'000;

}  // namespace

// T1/T2 + Task 30b: K live leases, a small ring wrapped 3+ times (carried), then the leader dies.  The new leader keeps the lineage (generation
// unchanged -> no rebirth fence), serves every lease at its version and still judges (the constraint set was re-logged, not evicted).
// Mutation: LeaseRuntime::emitCarries returns 0 -> the churn is refused TransientLogFull, red.
TEST(LeaseTransientTrioRegression, LeaderChangeKeepsEveryLeaseRegression) {
    constexpr int K = 10;
    LeaseTrio t("lease-tr-lc", [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto topic = t.defineTransientLease("tl", 1, kLongTtlMs, 1000, kLongTtlMs);
    auto lt = t.open(topic);
    LeaseClaim filler = lt3::row("f-fill", 0);
    rc::LeaseOptions o;
    o.ttlMs = kLongTtlMs;
    const rc::LeaseResult fg = opRetry([&] { return lt.grantAndWait(filler, o); });   // also waits the generation-0 fence open
    ASSERT_TRUE(fg.ok()) << int(fg.status) << " " << fg.errorCode;
    int64_t fillerVersion = fg.version;
    const Granted g = grantKeys(lt, K, kLongTtlMs);
    ASSERT_EQ(g.version.size(), std::size_t(K));

    const int32_t leader0 = t.leaderOf(topic, 0);
    ASSERT_GT(leader0, 0);
    const auto before = t.runtimeOn(leader0, topic.id, 0);
    ASSERT_TRUE(before.has && before.hasRing);
    ASSERT_EQ(before.capBytes, uint64_t(kRingBytes));
    const auto t0 = Clk::now();
    uint64_t counter = 0;
    ASSERT_EQ(trioChurn(lt, filler, fillerVersion, counter, opsForWraps(before, 3)), "");
    const auto afterChurn = t.runtimeOn(leader0, topic.id, 0);
    std::printf("[trio-lc] churn %llu ops in %lld ms; leader %d ring %lld..%lld carried %llu\n", (unsigned long long)counter, (long long)msSince(t0),
                leader0, (long long)afterChurn.ringStart, (long long)afterChurn.ringEnd, (unsigned long long)afterChurn.carried);
    EXPECT_GT(afterChurn.ringStart, g.lastOffset) << "the ring wrapped past every original grant";
    EXPECT_GT(afterChurn.carried, 0u);
    const auto noteBefore = noteOn(t, leader0, topic.id);
    ASSERT_TRUE(noteBefore.has_value());

    const int32_t dead = t.killLeader(topic, 0);
    ASSERT_EQ(dead, leader0);
    const int32_t leader1 = t.leaderOf(topic, 0);
    ASSERT_GT(leader1, 0);
    ASSERT_NE(leader1, dead);
    // The new leader's runtime: Open, never Fencing (the lineage is kept).
    bool open = false, fenced = false;
    const auto end = Clk::now() + 60s;
    while (!open && Clk::now() < end) {
        const auto s = t.runtimeOn(leader1, topic.id, 0);
        if (s.has && s.fence == 2) fenced = true;
        open = s.has && s.fence == 3;
        if (!open) std::this_thread::sleep_for(5ms);
    }
    EXPECT_TRUE(open) << "the new leader's fence is Open";
    EXPECT_FALSE(fenced) << "no Fencing phase after a leader change";
    const auto noteAfter = noteOn(t, leader1, topic.id);
    ASSERT_TRUE(noteAfter.has_value());
    EXPECT_EQ(noteAfter->generation, noteBefore->generation) << "the lineage generation is unchanged";
    EXPECT_EQ(noteAfter->lineage, noteBefore->lineage) << "the lineage is kept";

    expectEveryLeaseHeld(t.client(), lt, g, kLongTtlMs);
    const rc::LeaseResult fresh = opRetry([&] { return lt.grantAndWait(lt3::row("fresh", 7), o); });
    EXPECT_TRUE(fresh.ok()) << int(fresh.status) << " " << fresh.errorCode;

    t.restartNode(dead, topic);
    EXPECT_TRUE(t.awaitIsrSize(topic, 0, 3, 90s)) << "the old leader rejoins the ISR";
    const auto rejoined = t.runtimeOn(dead, topic.id, 0);
    EXPECT_TRUE(rejoined.hasRing) << "the old leader owns the partition again, with a (reset) ring";
}

// T1/T2 + the follower ring reset: a follower restarts empty while the leader's ring moves far past anything it held; it resets to the leader's
// ring start and re-folds the lease index.  When it is promoted it serves every lease at its version.
TEST(LeaseTransientTrioRegression, FollowerRingResetThenPromotedRegression) {
    constexpr int K = 10;
    LeaseTrio t("lease-tr-fr", [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto topic = t.defineTransientLease("tl", 1, kLongTtlMs, 1000, kLongTtlMs);
    // The election takes the most complete in-sync replica, ties to the LOWEST id.  F (restarted below) is the lower-id follower, X the higher one;
    // the test's client is seeded on X, which is never stopped, so F is the one that wins when the leader dies.
    const int32_t leader = t.leaderOf(topic, 0);
    ASSERT_GT(leader, 0);
    std::vector<int32_t> others;
    for (int id : {1, 2, 3})
        if (id != leader) others.push_back(id);
    const int32_t follower = others[0], keeper = others[1];
    auto cfg = catalog_cluster::clientConfigFor(t.nodes(), keeper);
    cfg.ddlTimeoutMs = 60'000;
    auto client = catalog_cluster::connectClient(t.nodes(), cfg);
    std::optional<Lt> opened;
    for (const auto end = Clk::now() + 90s; !opened && Clk::now() < end;) {
        try { opened.emplace(client->openLeaseTopicAndWait<LeaseClaim>(topic.name, {}, 30s)); }
        catch (const std::exception&) { std::this_thread::sleep_for(200ms); }
    }
    ASSERT_TRUE(opened.has_value());
    Lt& lt = *opened;
    LeaseClaim filler = lt3::row("f-fill", 0);
    rc::LeaseOptions o;
    o.ttlMs = kLongTtlMs;
    const rc::LeaseResult fg = opRetry([&] { return lt.grantAndWait(filler, o); });
    ASSERT_TRUE(fg.ok()) << int(fg.status) << " " << fg.errorCode;
    int64_t fillerVersion = fg.version;
    const Granted g = grantKeys(lt, K, kLongTtlMs);
    ASSERT_EQ(g.version.size(), std::size_t(K));
    ASSERT_TRUE(t.awaitIsrSize(topic, 0, 3, 60s));
    const auto followerBefore = t.runtimeOn(follower, topic.id, 0);
    ASSERT_TRUE(followerBefore.hasRing);
    t.nodes().stop(follower);

    const auto leaderBefore = t.runtimeOn(leader, topic.id, 0);
    ASSERT_TRUE(leaderBefore.has && leaderBefore.hasRing);
    uint64_t counter = 0;
    int rounds = 0;
    while (t.runtimeOn(leader, topic.id, 0).ringStart <= followerBefore.ringEnd + 8 && rounds++ < 200) {   // hard cap
        ASSERT_EQ(trioChurn(lt, filler, fillerVersion, counter, opsForWraps(leaderBefore, 1)), "");
    }
    const auto leaderMoved = t.runtimeOn(leader, topic.id, 0);
    ASSERT_GT(leaderMoved.ringStart, followerBefore.ringEnd) << "the leader's ring start passed the follower's last offset";
    std::printf("[trio-fr] follower %d held ..%lld; leader %d ring %lld..%lld carried %llu after %llu churn ops\n", follower,
                (long long)followerBefore.ringEnd, leader, (long long)leaderMoved.ringStart, (long long)leaderMoved.ringEnd,
                (unsigned long long)leaderMoved.carried, (unsigned long long)counter);

    t.nodes().startNode(follower);
    ASSERT_TRUE(t.awaitIsrSize(topic, 0, 3, 90s)) << "the restarted follower is back in the ISR";
    const auto fol = t.runtimeOn(follower, topic.id, 0);
    ASSERT_TRUE(fol.hasRing);
    EXPECT_GE(fol.ringStart, leaderMoved.ringStart) << "the follower's ring starts at the leader's start (or above)";
    EXPECT_GE(fol.resets, 1u) << "the follower reset its ring to the leader's start";
    EXPECT_GE(fol.ringEnd, leaderMoved.ringEnd) << "the follower caught up to the leader's ring end";
    EXPECT_GT(fol.usedBytes, 0u);

    // Promote the restarted follower: kill the leader while it is not that follower (bounded rounds; each other victim is restarted).
    int32_t cur = leader;
    for (int round = 0; round < 6 && t.leaderOf(topic, 0) != follower; ++round) {
        cur = t.leaderOf(topic, 0);
        ASSERT_GT(cur, 0);
        ASSERT_NE(cur, follower);
        const int32_t killed = t.killLeader(topic, 0);
        ASSERT_EQ(killed, cur);
        if (t.leaderOf(topic, 0) != follower) {
            t.restartNode(killed, topic);
            ASSERT_TRUE(t.awaitIsrSize(topic, 0, 3, 90s));
        }
    }
    ASSERT_EQ(t.leaderOf(topic, 0), follower) << "the restarted follower was promoted";
    expectEveryLeaseHeld(*client, lt, g, kLongTtlMs);
    const rc::LeaseResult fresh = opRetry([&] { return lt.grantAndWait(lt3::row("fresh", 7), o); });
    EXPECT_TRUE(fresh.ok()) << int(fresh.status) << " " << fresh.errorCode;
    lt.close();
}

// T4/A.6: a whole-cluster restart loses every ring: a new lineage, the leader fences (grants Throttled, max_ttl after its ready time) and the old
// leases are gone (LeaseNotFound, a watch ends them exactly once).  A leader change MID-fence fences again - never for a shorter time.
TEST(LeaseTransientTrioRegression, FullRestartFencesThenLeaderChangeMidFenceRegression) {
    constexpr int K = 6;
    constexpr uint32_t kMaxTtlMs = 12'000;   // long enough to observe the fence and kill the leader inside it, short enough to wait it out
    LeaseTrio t("lease-tr-fence", [](rune::node::NodeConfig& c) { c.memoryLogRingBytes = kRingBytes; });
    const auto topic = t.defineTransientLease("tl", 1, kMaxTtlMs, 1000, kMaxTtlMs);
    auto lt = t.open(topic);
    rc::Client& c = t.client();
    const Granted g = grantKeys(lt, K, kMaxTtlMs);
    ASSERT_EQ(g.version.size(), std::size_t(K));

    rc::LeaseWatchOptions wo;
    wo.maxLive = 256;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WLog log;
    watch.setHandler(&WLog::fn, &log);
    const std::vector<uint8_t> none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    ASSERT_EQ(watch.startFromNow(prefixes), rc::OpResult::Accepted);
    ASSERT_TRUE(driveWatch(watch, c, 60s, [&] { return watch.isReady() && log.live.size() == std::size_t(K); }));

    for (int id : {1, 2, 3}) t.nodes().stop(id);
    for (int id : {1, 2, 3}) t.nodes().startNode(id);
    // Catch the fence: poll every node's runtime (tight) until one is Fencing.  The deadline is on the lease clock.
    LeaseTrio::RuntimeSnap fencing;
    int32_t fencingNode = -1;
    const auto end = Clk::now() + 120s;
    while (fencingNode < 0 && Clk::now() < end) {
        for (int id : {1, 2, 3}) {
            const auto s = t.runtimeOn(id, topic.id, 0);
            if (s.has && s.fence == 2) { fencing = s; fencingNode = id; break; }
        }
        watch.doWork(c.nowNs());
        if (fencingNode < 0) std::this_thread::sleep_for(5ms);
    }
    ASSERT_GT(fencingNode, 0) << "no leader ever fenced after the whole-cluster restart";
    EXPECT_GE(fencing.deadlineNs, fencing.readyNs + int64_t(kMaxTtlMs) * 1'000'000) << "the fence lasts at least max_ttl from the runtime's ready time";
    const int64_t deadline1 = fencing.deadlineNs;
    ASSERT_GT(deadline1, rune::consensus::leaseClockNowNs()) << "caught inside the fence";

    // During the fence: grants are held (Throttled), the old leases are gone, the watch ended every old key once.
    {
        rc::LeaseOptions fo;
        fo.opTimeoutMs = 1500;
        rc::LeaseResult held;
        try { held = lt.grantAndWait(lt3::row("during", 1), fo); } catch (const std::exception&) { held = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        EXPECT_FALSE(held.ok()) << "a grant is held while the leader fences";
        EXPECT_TRUE(held.errorCode == kThrottled || held.status == rc::OpStatus::Timeout) << "status " << int(held.status) << " code " << held.errorCode;
        std::printf("[trio-fence] held grant answered status %d code %u\n", int(held.status), unsigned(held.errorCode));
        for (const auto& [k, v] : g.version) {
            const rc::LeaseResult ka = opRetry([&] { return lt.keepAliveOnceAndWait(lt3::row(k), v); }, 30s);
            EXPECT_FALSE(ka.ok()) << k;
            EXPECT_EQ(ka.errorCode, kNotFound) << k << ": the pre-restart lease died with the ring";
        }
    }
    ASSERT_TRUE(driveWatch(watch, c, 90s, [&] { return log.live.empty() && log.ended.size() == std::size_t(K); }))
        << "live " << log.live.size() << " ended " << log.ended.size() << " reruns " << watch.reruns();
    for (const auto& [k, v] : g.version) EXPECT_EQ(log.ended[k], 1) << k << " ends exactly once";
    EXPECT_EQ(log.bad, 0);

    // Mid-fence: the leader dies.  The next leader fences again, for no shorter a time.
    const int32_t dead = t.killLeader(topic, 0);
    ASSERT_GT(dead, 0);
    const int32_t next = t.leaderOf(topic, 0);
    ASSERT_GT(next, 0);
    ASSERT_NE(next, dead);
    LeaseTrio::RuntimeSnap again;
    bool sawFencing = false, sawOpen = false;
    const auto end2 = Clk::now() + 60s;
    while (!sawFencing && !sawOpen && Clk::now() < end2) {
        again = t.runtimeOn(next, topic.id, 0);
        sawFencing = again.has && again.fence == 2;
        sawOpen    = again.has && again.fence == 3;
        if (!sawFencing && !sawOpen) std::this_thread::sleep_for(5ms);
    }
    ASSERT_TRUE(sawFencing) << "the next leader fences again (open=" << sawOpen << ")";
    EXPECT_GE(again.deadlineNs, deadline1) << "the second fence never ends before the first would have";
    const int64_t deadline2 = again.deadlineNs;

    // Grants stay held until that deadline: every attempt that answers Ok must complete AFTER it (a held call may block for the client's delivery
    // timeout, so an attempt that starts before the deadline may legitimately finish after it).  Hard caps on the attempts and on the time.
    rc::LeaseOptions fo;
    fo.opTimeoutMs = 1000;
    bool granted = false;
    int  heldCount = 0;
    for (int i = 0; i < 60 && !granted; ++i) {
        rc::LeaseResult r;
        try { r = lt.grantAndWait(lt3::row("during", 2), fo); } catch (const std::exception&) { r = rc::LeaseResult{rc::OpStatus::Timeout, 0}; }
        const int64_t doneNs = rune::consensus::leaseClockNowNs();
        if (r.ok()) {
            granted = true;
            EXPECT_GE(doneNs, deadline2) << "a grant completed " << (deadline2 - doneNs) / 1'000'000 << " ms before the second fence's deadline";
        } else {
            ++heldCount;
            EXPECT_TRUE(r.errorCode == kThrottled || r.errorCode == kNotLeader || r.status == rc::OpStatus::Timeout)
                << "status " << int(r.status) << " code " << r.errorCode;
        }
        if (!granted && doneNs > deadline2 + 20'000'000'000LL) break;
        if (!granted) std::this_thread::sleep_for(50ms);
    }
    EXPECT_GT(heldCount, 0) << "at least one grant was held by the second fence";
    EXPECT_TRUE(granted) << "grants succeed once the second fence's deadline has passed";
    EXPECT_GE(rune::consensus::leaseClockNowNs(), deadline2);
    t.restartNode(dead, topic);
}
