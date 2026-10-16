// ClientFailoverBrokerRegressionTest.cpp — DR client bugs (2026-10-15, dr-e2e-speed-report.md): the REAL
// client (rune::client::Client, Thread runner) against THREE real in-process BrokerNodes over Aeron UDP
// (the broker's support/CatalogBrokerCluster.h ThreeNodes rig), a partition leader SIGKILL-crashed
// (BrokerNode::crashForTest: no controlled shutdown, no goodbye) mid-produce / mid-subscribe.
//
// Bug 1 (dr3 mid_produce, dr13 reconnect storm, dr15): after the leader of a partition is crashed the
// producer never re-routed that partition: every later record waited out its 30-40 s delivery timeout
// ("records never sent within the delivery timeout (no route / session)").  Required: the dead session is
// detected, metadata is refreshed through ANY live broker, the in-flight batches are retried on the new
// leader within their budget and every later record goes there: every record acked, well inside the
// delivery timeout, none lost and none duplicated (the idempotent producer's sequences).
//
// Target rune_client_failover_broker_tests (libs/node/CMakeLists.txt, RUN_SERIAL: 3-node tests at -j <= 3).
#include "client/ClusterClient.h"
#include "support/DdlHarness.h"   // charBlob / viewPlanBlob

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/producer/TransactionalProducer.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <chrono>
#include <cstdio>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::node::test::catalog_cluster;
namespace rc = rune::client;
using Clock = std::chrono::steady_clock;

namespace {

constexpr auto    kDisk       = rc::Persistence::Disk;
constexpr int32_t kPartitions = 3;

/// The leader of (topicId, partition) as broker `n`'s control thread sees it (-1: none / unknown).
int32_t leaderOn(rune::node::BrokerNode& n, int32_t topicId, int32_t partition) {
    return n.onControlThreadForTest([&n, topicId, partition]() -> int32_t {
        const auto* ms = n.metadataServiceForTest();
        if (ms == nullptr) return -1;
        const auto* pa = ms->assignment(topicId, partition);
        return pa != nullptr ? pa->leaderNode : -1;
    });
}

/// Every running broker agrees on one live leader for every partition of `topicId` (bounded wait).
bool awaitLeaders(ThreeNodes& c, int32_t topicId, std::vector<int32_t>& leaders, std::chrono::milliseconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline; std::this_thread::sleep_for(20ms)) {
        leaders.assign(kPartitions, -1);
        bool ok = true;
        const auto running = c.runningNodes();
        for (int32_t p = 0; p < kPartitions && ok; ++p) {
            for (int id : running) {
                const int32_t l = leaderOn(c.node(id), topicId, p);
                if (l <= 0 || std::find(running.begin(), running.end(), l) == running.end()) { ok = false; break; }
                if (leaders[static_cast<std::size_t>(p)] == -1) leaders[static_cast<std::size_t>(p)] = l;
                else if (leaders[static_cast<std::size_t>(p)] != l) { ok = false; break; }
            }
        }
        if (ok) return true;
    }
    return false;
}

std::string valueOf(int seq) { return "seq=" + std::to_string(seq); }
int seqOf(std::span<const uint8_t> v) {
    const std::string s(reinterpret_cast<const char*>(v.data()), v.size());
    return s.rfind("seq=", 0) == 0 ? std::atoi(s.c_str() + 4) : -1;
}

struct SendOutcome {
    bool              done   = false;
    rc::OpStatus      status = rc::OpStatus::Ok;
    int64_t           offset = -1;
    int32_t           partition = -1;
    Clock::time_point sentAt{}, doneAt{};
};

/// Read every partition of the topic from the beginning through a NEW client seeded on `via`, until every
/// seq in `mustSee` was delivered and nothing new arrived for 1 s (bounded).  Returns seq -> times seen.
std::map<int, int> readBack(ThreeNodes& c, int via, const rc::TopicSpec& spec, const std::set<int>& mustSee) {
    auto cfg = clientConfigFor(c, via);
    cfg.topics = {spec};
    auto reader = connectClient(c, cfg);
    std::mutex mu;
    std::map<int, int> seen;
    Clock::time_point lastNew = Clock::now();
    std::vector<rc::Subscription> subs;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    const rc::TopicHandle th = reader->topic(spec.name);
    for (int32_t p = 0; p < kPartitions; ++p)
        subs.push_back(reader->subscribe(th, p, o, [&](const rc::Record& r) {
            std::lock_guard lk(mu);
            ++seen[seqOf(r.value)];
            lastNew = Clock::now();
        }));
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline; std::this_thread::sleep_for(50ms)) {
        std::lock_guard lk(mu);
        bool all = true;
        for (int s : mustSee) all = all && seen.count(s) != 0;
        if (all && Clock::now() - lastNew > 1s) break;
    }
    for (auto& s : subs) s.unsubscribe();
    reader->close();
    std::lock_guard lk(mu);
    return seen;
}

enum class Victim { SeedLeader, OtherLeader };

struct FailoverRun {
    std::vector<SendOutcome> out;
    int                      killedAt = -1;   ///< the first seq sent after the crash began
    int32_t                  victim = -1, seed = -1, victimPartition = -1;
    rc::TopicSpec            spec;
    Clock::time_point        killT{};
};

/// Produce `total` records (acks=All, ~4 ms apart, round-robin over the 3 partitions) through a client
/// seeded on `seed`; crash the broker leading one partition once `killAfter` records were sent.
FailoverRun produceThroughLeaderCrash(ThreeNodes& c, Victim which, int total, int killAfter) {
    FailoverRun run;
    EXPECT_TRUE(c.awaitCatalogLeader(30s));
    auto admin = connectClient(c, c.runningNodes().front());
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto def = admin->defineTopicAndWait("events", kPartitions, "", {}, false, kDisk, to, {}, 60s);
    EXPECT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    run.spec = rc::TopicSpec{"events", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    EXPECT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s)) << "every partition has a leader";
    // The victim leads at least one partition; the seed is the victim (SeedLeader) or another broker.
    run.victimPartition = 0;
    run.victim          = leaders[0];
    if (which == Victim::SeedLeader) {
        run.seed = run.victim;
    } else {
        for (int id : c.runningNodes()) if (id != run.victim) { run.seed = id; break; }
    }
    std::fprintf(stderr, "[failover] leaders p0=%d p1=%d p2=%d victim=%d seed=%d\n", leaders[0], leaders[1],
                 leaders[2], run.victim, run.seed);

    auto cfg = clientConfigFor(c, run.seed);
    cfg.topics           = {run.spec};
    cfg.connectTimeoutMs = 30'000;   // as dr_client (RUNE_TIMEOUT_MS)
    cfg.pingTimeoutMs    = 30'000;
    auto client = connectClient(c, cfg);
    rc::ProducerConfig pcfg;          // as dr_client::doProduce
    pcfg.acks              = rc::Acks::All;
    pcfg.lingerUs          = 1;
    pcfg.batchBytes        = 1;
    pcfg.maxInFlight       = 16;
    pcfg.produceTimeoutMs  = 30'000;
    pcfg.deliveryTimeoutMs = 40'000;
    auto producer = client->producer(pcfg);
    rc::Completions cq(*client);
    const rc::TopicHandle th = client->topic("events");

    run.out.resize(static_cast<std::size_t>(total));
    std::thread killer;
    auto drain = [&] {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) {
            auto& o = run.out[static_cast<std::size_t>(d.token - 1)];
            o.done = true; o.status = d.status; o.offset = d.value; o.doneAt = Clock::now();
        });
    };
    for (int seq = 0; seq < total; ++seq) {
        if (seq == killAfter) {
            run.killT    = Clock::now();
            run.killedAt = seq;
            killer = std::thread([&c, v = run.victim] { c.crash(v); });   // a REAL crash, mid-produce
        }
        const std::string v = valueOf(seq);
        const auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
        const int32_t part = seq % kPartitions;
        rc::Key key(static_cast<uint64_t>(seq));
        // A key that hashes to `part` (round-robin over the partitions).
        for (uint64_t k = static_cast<uint64_t>(seq) * 16;; ++k) {
            key = rc::Key(k);
            if (key.partition(kPartitions) == part) break;
        }
        auto& o = run.out[static_cast<std::size_t>(seq)];
        o.partition = part;
        o.sentAt    = Clock::now();
        for (;;) {
            const rc::SendResult r = producer.trySend(th, key, bytes, cq, static_cast<rc::Token>(seq) + 1);
            if (r == rc::SendResult::Ok) break;
            if (r == rc::SendResult::QueueFull || r == rc::SendResult::BackPressured) { drain(); std::this_thread::yield(); continue; }
            ADD_FAILURE() << "trySend refused seq " << seq << ": " << static_cast<int>(r);
            o.done = true; o.status = rc::OpStatus::Closed;
            break;
        }
        drain();
        std::this_thread::sleep_for(4ms);
    }
    const auto deadline = Clock::now() + 60s;
    while (Clock::now() < deadline &&
           std::any_of(run.out.begin(), run.out.end(), [](const SendOutcome& o) { return !o.done; })) {
        drain();
        std::this_thread::sleep_for(2ms);
    }
    if (killer.joinable()) killer.join();
    producer.close();
    client->close();
    return run;
}

/// The bug-1 contract for one run: every record acked; every record sent after the crash acked within
/// `bound` (well under the 40 s delivery timeout); the acked set read back complete and exactly once.
void expectFailoverContract(ThreeNodes& c, const FailoverRun& run, std::chrono::milliseconds bound) {
    int notDone = 0, failed = 0, slow = 0;
    std::chrono::milliseconds worst{0};
    std::set<int> acked;
    for (int seq = 0; seq < static_cast<int>(run.out.size()); ++seq) {
        const auto& o = run.out[static_cast<std::size_t>(seq)];
        if (!o.done) { ++notDone; continue; }
        if (o.status != rc::OpStatus::Ok) {
            if (failed++ < 5) ADD_FAILURE() << "seq " << seq << " (partition " << o.partition << ") failed, status "
                                            << static_cast<int>(o.status);
            continue;
        }
        acked.insert(seq);
        if (seq >= run.killedAt) {
            const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(o.doneAt - o.sentAt);
            worst = std::max(worst, took);
            if (took > bound) ++slow;
        }
    }
    EXPECT_EQ(notDone, 0) << "every send completes (no hang)";
    EXPECT_EQ(failed, 0) << "no record fails: the dead leader's partition is re-routed within the budget";
    EXPECT_EQ(slow, 0) << "every record after the crash is acked within " << bound.count() << " ms (worst "
                       << worst.count() << " ms; the delivery timeout is 40 s)";
    std::fprintf(stderr, "[failover] acked %zu/%zu, worst post-crash latency %lld ms\n", acked.size(), run.out.size(),
                 static_cast<long long>(worst.count()));
    int via = -1;
    for (int id : c.runningNodes()) via = id;
    ASSERT_GT(via, 0);
    const auto seen = readBack(c, via, run.spec, acked);
    int missing = 0, dup = 0;
    for (int s : acked) if (seen.count(s) == 0) ++missing;
    for (const auto& [s, n] : seen) if (n > 1) { if (dup++ < 5) ADD_FAILURE() << "seq " << s << " stored " << n << " times"; }
    EXPECT_EQ(missing, 0) << "every acked record is in the log (acks=All)";
    EXPECT_EQ(dup, 0) << "no record is stored twice (idempotent retries)";
}

}  // namespace

// Bug 1 (dr13 reconnect storm / dr3 mid_produce): the client is seeded on the broker that leads partition
// 0, and that broker is crashed mid-produce.  The seed session is the one that dies, so the metadata the
// re-route needs must come from another (live) broker.
// RED (before the fix): partition 0's records fail after the 40 s delivery timeout ("no route / session").
TEST(ClientFailoverBrokerRegression, SeedLeaderCrashedMidProduceReRoutesWithinTheBudgetRegression) {
    ThreeNodes c = ThreeNodes::start("fo_seed");
    const FailoverRun run = produceThroughLeaderCrash(c, Victim::SeedLeader, 1500, 300);
    expectFailoverContract(c, run, 20s);
}

// Bug 1, the sibling timing: the crashed leader is NOT the seed (the seed stays live).
// GUARD (never RED: this path re-routed before the fix too) — kept so it stays that way.
TEST(ClientFailoverBrokerRegression, NonSeedLeaderCrashedMidProduceReRoutesWithinTheBudgetRegression) {
    ThreeNodes c = ThreeNodes::start("fo_other");
    const FailoverRun run = produceThroughLeaderCrash(c, Victim::OtherLeader, 1500, 300);
    expectFailoverContract(c, run, 20s);
}

// ---------------------------------------------------------------------------------------------------------
// Bug 2 (dr2 multiclient, 5 clients x 500 records x 3 partitions): a client printed a successful summary
// and then hung in Client::close() for > 200 s.  close() must be bounded by a configured deadline and must
// answer every pending operation exactly once (TimedOut or Closed) — never wait forever on a peer, a thread
// join or a ring drain.
// ---------------------------------------------------------------------------------------------------------
namespace {

/// close() on another thread, bounded by `bound`: returns how long it took.  A close() that does not
/// return in time FAILS the test and ends the process (a hung close cannot be joined); with
/// RUNE_TEST_SAMPLE_ON_HANG set, the stacks are sampled into $RUNE_TEST_SAMPLE_ON_HANG first (macOS).
std::chrono::milliseconds closeBounded(rc::Client& client, std::chrono::milliseconds bound, const char* who) {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    const auto t0 = Clock::now();
    std::thread t([&] {
        client.close();
        std::lock_guard lk(mu);
        done = true;
        cv.notify_all();
    });
    std::unique_lock lk(mu);
    if (!cv.wait_for(lk, bound, [&] { return done; })) {
        std::fprintf(stderr, "[close] %s: Client::close() did not return within %lld ms — HANG\n", who,
                     static_cast<long long>(bound.count()));
        if (const char* out = std::getenv("RUNE_TEST_SAMPLE_ON_HANG")) {
#ifdef __APPLE__
            const std::string cmd = "sample " + std::to_string(::getpid()) + " 3 -file " + out + " >/dev/null 2>&1";
#else
            const std::string cmd = "gdb -p " + std::to_string(::getpid()) +
                                    " -batch -ex 'thread apply all bt' > " + out + " 2>&1";
#endif
            (void)std::system(cmd.c_str());
            std::fprintf(stderr, "[close] stacks sampled into %s\n", out);
        }
        ADD_FAILURE() << who << ": Client::close() hung past " << bound.count() << " ms";
        std::fflush(stderr);
        std::fflush(stdout);
        std::_Exit(3);
    }
    lk.unlock();
    t.join();
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0);
}

/// One dr_client "produce_subscribe" run (examples/dr_client.cpp doProduce + doSubscribe): `records` acked
/// sends one at a time (acks=Leader), then a subscription per partition from earliest until this client's
/// own `records` arrived, unsubscribe, close() — bounded.
struct DrClientResult { int produced = 0, delivered = 0; std::chrono::milliseconds closeTook{0}; };
DrClientResult runDrClient(ThreeNodes& c, rc::ClientConfig cfg, const rc::TopicSpec& spec, int cid, int records,
                           std::chrono::milliseconds closeBound) {
    DrClientResult res;
    cfg.topics           = {spec};
    cfg.connectTimeoutMs = 40'000;
    cfg.pingTimeoutMs    = 40'000;
    cfg.idleMode         = rc::IdleMode::LowLatency;
    auto client = connectClient(c, cfg);
    const rc::TopicHandle th = client->topic(spec.name);
    {
        rc::ProducerConfig pcfg;
        pcfg.acks = rc::Acks::Leader; pcfg.lingerUs = 1; pcfg.batchBytes = 1; pcfg.maxInFlight = 16;
        pcfg.produceTimeoutMs = 40'000;
        auto p = client->producer(pcfg);
        rc::Completions cq(*client);
        for (int seq = 0; seq < records; ++seq) {
            const std::string v = "cid=" + std::to_string(cid) + ";" + valueOf(seq);
            const auto bytes = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size());
            const rc::Key key(static_cast<uint64_t>(cid) * 1'000'000ULL + static_cast<uint64_t>(seq % 18));
            for (;;) {
                const rc::SendResult r = p.trySend(th, key, bytes, cq, static_cast<rc::Token>(seq) + 1);
                if (r == rc::SendResult::Ok) break;
                if (r == rc::SendResult::QueueFull) { (void)cq.poll([](const rc::Completion&, std::span<const uint8_t>) {}); continue; }
                if (r != rc::SendResult::BackPressured) break;
                std::this_thread::yield();
            }
            rc::Completion out;
            const auto w = cq.waitFor(static_cast<rc::Token>(seq) + 1, 40s,
                                      [&](const rc::Completion& d, std::span<const uint8_t>) { out = d; });
            if (w == rc::Completions::Wait::Done && out.status == rc::OpStatus::Ok) ++res.produced;
        }
    }
    {
        std::atomic<int> mine{0};
        const std::string tag = "cid=" + std::to_string(cid) + ";";
        std::vector<rc::Subscription> subs;
        rc::SubscribeOptions o;
        o.from = rc::From::earliest();
        for (int32_t p = 0; p < spec.partitionCount; ++p)
            subs.push_back(client->subscribe(th, p, o, [&](const rc::Record& r) {
                const std::string_view s(reinterpret_cast<const char*>(r.value.data()), r.value.size());
                if (s.rfind(tag, 0) == 0) mine.fetch_add(1, std::memory_order_relaxed);
            }));
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline && mine.load() < records;)
            std::this_thread::sleep_for(2ms);
        res.delivered = mine.load();
        for (auto& s : subs) s.unsubscribe();
    }
    res.closeTook = closeBounded(*client, closeBound, ("cid " + std::to_string(cid)).c_str());
    return res;
}

}  // namespace

// Bug 2: five concurrent dr_client produce_subscribe runs through the consensus leader; every close()
// returns within the bound (the producer's 30 s delivery timeout + margin), however the run went.
// GUARD (never RED: the dr2 hang did not reproduce in-process) — the dr2 shape, kept under a hang watchdog.
TEST(ClientFailoverBrokerRegression, MultiClientProduceSubscribeCloseIsBoundedRegression) {
    ThreeNodes c = ThreeNodes::start("fo_close");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    int32_t leader = -1;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && leader < 0; std::this_thread::sleep_for(20ms))
        for (rune::node::BrokerNode* n : c.live()) if (n->isLeader()) leader = n->nodeIdForTest();
    ASSERT_GT(leader, 0);
    auto admin = connectClient(c, leader);
    const auto def = admin->defineTopicAndWait("multi", kPartitions, "", {}, false, kDisk, {}, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    const rc::TopicSpec spec{"multi", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s));
    constexpr int kClients = 5, kRecords = 500;
    std::vector<DrClientResult> res(kClients);
    std::vector<rc::ClientConfig> cfgs;   // built here: the free-port allocator is not thread-safe
    for (int i = 0; i < kClients; ++i) cfgs.push_back(clientConfigFor(c, leader));
    std::vector<std::thread> ts;
    for (int i = 0; i < kClients; ++i)
        ts.emplace_back([&, i] {
            res[static_cast<std::size_t>(i)] = runDrClient(c, cfgs[static_cast<std::size_t>(i)], spec, 162000 + i, kRecords, 60s);
        });
    for (auto& t : ts) t.join();
    for (int i = 0; i < kClients; ++i) {
        const auto& r = res[static_cast<std::size_t>(i)];
        EXPECT_EQ(r.produced, kRecords) << "client " << i;
        EXPECT_EQ(r.delivered, kRecords) << "client " << i;
        EXPECT_LT(r.closeTook, 45s) << "client " << i << " close() took " << r.closeTook.count() << " ms";
        std::fprintf(stderr, "[close] client %d produced %d delivered %d close %lld ms\n", i, r.produced, r.delivered,
                     static_cast<long long>(r.closeTook.count()));
    }
}

// ---------------------------------------------------------------------------------------------------------
// Bug 1, sibling paths on the client whose SEED broker is crashed: consume (a node-homed subscription keeps
// delivering; the seed-bound one ends explicitly — SessionLost, never active and silent — and a re-subscribe
// from its position() delivers the post-crash records from the new leader), a NEW subscribe, and the
// control plane that rides the seed (a DDL, a GetTopicSchema read) — each within a bound well under the
// operation timeouts.
// RED (before the fix): the seed never leaves the dead broker — the re-subscribe, the new subscribe, the DDL
// and the read all wait on it until their own timeouts.
// ---------------------------------------------------------------------------------------------------------
namespace {
struct SubLedger {
    std::mutex mu;
    std::map<int32_t, std::set<int>> seqs;        // partition -> seqs delivered
    std::map<int32_t, std::set<int64_t>> offs;    // partition -> offsets delivered
    int dupes = 0;
    rc::RecordHandler handler(int32_t p) {
        return [this, p](const rc::Record& r) {
            std::lock_guard lk(mu);
            if (!seqs[p].insert(seqOf(r.value)).second) ++dupes;
            offs[p].insert(r.offset);
        };
    }
    std::size_t count(int32_t p) { std::lock_guard lk(mu); return seqs[p].size(); }
    bool has(int32_t p, int seq) { std::lock_guard lk(mu); return seqs[p].count(seq) != 0; }
};
}  // namespace

TEST(ClientFailoverBrokerRegression, SeedCrashedSubscribeAndControlPlaneFailOverWithinBoundsRegression) {
    ThreeNodes c = ThreeNodes::start("fo_sib");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto admin = connectClient(c, c.runningNodes().front());
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto def = admin->defineTopicAndWait("events", kPartitions, "", {}, false, kDisk, to, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    const rc::TopicSpec spec{"events", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s));
    const int32_t victim = leaders[0];                    // leads p0; the subscriber's seed
    int32_t other = -1, survivor = -1;                    // a partition another broker leads; a producer seed
    for (int32_t p = 1; p < kPartitions; ++p) if (leaders[static_cast<std::size_t>(p)] != victim) { other = p; break; }
    ASSERT_GE(other, 1) << "the 3 partitions spread over the brokers";
    for (int id : c.runningNodes()) if (id != victim) { survivor = id; break; }

    // The subscriber: seeded on the victim; p0 bound on the seed, `other` homed on its leader's session.
    auto scfg = clientConfigFor(c, victim);
    scfg.topics = {spec};
    scfg.connectTimeoutMs = 30'000; scfg.pingTimeoutMs = 30'000;
    scfg.ddlTimeoutMs = 30'000;
    auto sub = connectClient(c, scfg);
    const rc::TopicHandle sth = sub->topic("events");
    SubLedger led;
    rc::SubscribeOptions o; o.from = rc::From::earliest();
    auto s0 = sub->subscribe(sth, 0, o, led.handler(0));
    auto sO = sub->subscribe(sth, other, o, led.handler(other));

    // The producer: seeded on a survivor, acks=All, one record per partition every 20 ms.
    auto pcfg0 = clientConfigFor(c, survivor);
    pcfg0.topics = {spec};
    auto prodClient = connectClient(c, pcfg0);
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::All; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 30'000; pcfg.deliveryTimeoutMs = 40'000;
    auto producer = prodClient->producer(pcfg);
    const rc::TopicHandle pth = prodClient->topic("events");
    std::atomic<bool> stop{false};
    std::atomic<int>  sent{0};
    std::thread pump([&] {
        for (int seq = 0; !stop.load(); ++seq) {
            const int32_t part = seq % kPartitions;
            uint64_t k = static_cast<uint64_t>(seq) * 16;
            while (rc::Key(k).partition(kPartitions) != part) ++k;
            const std::string v = valueOf(seq);
            producer.send(pth, rc::Key(k), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
            sent.store(seq + 1);
            std::this_thread::sleep_for(7ms);
        }
    });
    struct JoinOnExit {   // an ASSERT failure below must not std::terminate on a joinable thread
        std::atomic<bool>& stop; std::thread& t;
        ~JoinOnExit() { stop.store(true); if (t.joinable()) t.join(); }
    } joinPump{stop, pump};
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && (led.count(0) < 5 || led.count(other) < 5);)
        std::this_thread::sleep_for(10ms);
    ASSERT_GE(led.count(0), 5u) << "precondition: p0 delivers through the seed";
    ASSERT_GE(led.count(other), 5u) << "precondition: the homed partition delivers";

    c.crash(victim);
    const auto tCrash = Clock::now();
    const int sentAtCrash = sent.load();

    // 1. The homed subscription keeps delivering records produced after the crash.
    const auto otherBefore = led.count(other);
    bool homedLive = false;
    for (const auto deadline = tCrash + 25s; Clock::now() < deadline && !homedLive; std::this_thread::sleep_for(20ms))
        homedLive = led.count(other) > otherBefore + 20;
    EXPECT_TRUE(homedLive) << "the homed subscription keeps delivering after the seed's broker died";

    // 2. The seed-bound subscription ends explicitly (never active and silent).
    bool ended = false;
    for (const auto deadline = tCrash + 25s; Clock::now() < deadline && !ended; std::this_thread::sleep_for(20ms))
        ended = !s0.active();
    ASSERT_TRUE(ended) << "SILENT STALL: the seed-bound subscription stayed active on a dead broker";
    EXPECT_EQ(s0.failReason(), rc::SubscriptionFailReason::SessionLost);
    const int64_t pos0 = s0.position();

    // 3. The control plane rides the seed: a DDL and a schema read succeed within the bound.
    const auto tDdl = Clock::now();
    rc::DefineTopicResult d2;
    EXPECT_NO_THROW(d2 = sub->defineTopicAndWait("after", 1, "", {}, false, kDisk, {}, {}, 25s));
    EXPECT_EQ(d2.errorCode, 0) << d2.detail;
    EXPECT_LT(Clock::now() - tDdl, 25s);
    // A catalog read (Subscribe on __catalog through the seed): both DDLs, committed.
    const auto tRead = Clock::now();
    auto hist = sub->catalogHistory({}, 15s);
    EXPECT_TRUE(hist.has_value()) << (hist.has_value() ? std::string() : hist.error().detail);
    if (hist.has_value()) EXPECT_GE(hist->size(), 2u) << "events + after";
    EXPECT_LT(Clock::now() - tRead, 15s);

    // 4. Re-subscribe p0 from its position, and a NEW subscribe of the third partition: both deliver
    //    records produced after the crash (from the new leaders).
    const int32_t third = 3 - other;   // the remaining partition of {1, 2}
    SubLedger led2;
    rc::SubscribeOptions o2; o2.from = rc::From::offset(pos0);
    auto r0 = sub->subscribe(sth, 0, o2, led2.handler(0));
    auto sT = sub->subscribe(sth, third, o, led2.handler(third));
    const int target = sentAtCrash + 3 * kPartitions;   // a seq produced after the crash, on every partition
    auto deliveredAfter = [&](int32_t p) {
        for (int s = target; s < target + kPartitions; ++s) if (s % kPartitions == p && led2.has(p, s)) return true;
        return false;
    };
    bool resumed = false;
    for (const auto deadline = Clock::now() + 25s; Clock::now() < deadline && !resumed; std::this_thread::sleep_for(20ms))
        resumed = deliveredAfter(0) && deliveredAfter(third);
    EXPECT_TRUE(deliveredAfter(0)) << "the re-subscribed p0 delivers post-crash records from its new leader";
    EXPECT_TRUE(deliveredAfter(third)) << "a NEW subscribe after the seed's death is served";
    std::fprintf(stderr, "[sibling] recovered %lld ms after the crash\n",
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tCrash).count()));
    {
        // Fix round 1 (M6): no duplicate and no gap on p0 across the explicit end + the re-subscribe from
        // position(): the two ledgers' offsets are disjoint and their union is contiguous.
        std::lock_guard a(led.mu), b(led2.mu);
        EXPECT_EQ(led.dupes, 0);
        EXPECT_EQ(led2.dupes, 0);
        const auto& o1 = led.offs[0];
        const auto& o2 = led2.offs[0];
        int both = 0;
        for (int64_t o : o2) both += o1.count(o) != 0 ? 1 : 0;
        EXPECT_EQ(both, 0) << "a p0 record delivered before the end AND after the re-subscribe";
        std::set<int64_t> all(o1);
        all.insert(o2.begin(), o2.end());
        ASSERT_FALSE(all.empty());
        EXPECT_EQ(static_cast<int64_t>(all.size()), *all.rbegin() - *all.begin() + 1)
            << "a gap in p0's delivered offsets across the end + re-subscribe";
        if (!o2.empty()) EXPECT_EQ(*o2.begin(), pos0) << "the re-subscribe starts at the ended subscription's position";
    }
    stop.store(true);
    if (pump.joinable()) pump.join();
    for (auto* s : {&sO, &r0, &sT}) s->unsubscribe();
    producer.close();
    prodClient->close();
    sub->close();
}

// Bug 1, sibling path: a CONSUMER GROUP member whose seed broker — the consensus leader, so also the group
// coordinator — is crashed.  The member must rejoin on the new coordinator and keep consuming every
// partition (the crashed broker's partition from its new leader), within a bound well under the session /
// operation timeouts.
// RED (before the fix): the seed (= the coordinator's session) never leaves the dead broker.
TEST(ClientFailoverBrokerRegression, SeedCoordinatorCrashedGroupMemberKeepsConsumingRegression) {
    ThreeNodes c = ThreeNodes::start("fo_grp");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    int32_t ctl = -1;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && ctl < 0; std::this_thread::sleep_for(20ms))
        for (rune::node::BrokerNode* n : c.live()) if (n->isLeader()) ctl = n->nodeIdForTest();
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto def = admin->defineTopicAndWait("events", kPartitions, "", {}, false, kDisk, to, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    const rc::TopicSpec spec{"events", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s));
    int32_t survivor = -1;
    for (int id : c.runningNodes()) if (id != ctl) { survivor = id; break; }

    // The member: seeded on the controller (the coordinator), subscribeGroup before connect.
    auto gcfg0 = clientConfigFor(c, ctl);
    gcfg0.topics = {spec};
    gcfg0.connectTimeoutMs = 30'000; gcfg0.pingTimeoutMs = 30'000;
    rc::Client member(gcfg0);
    rc::GroupConfig gcfg;
    gcfg.autoCommit = true; gcfg.autoCommitIntervalMs = 500; gcfg.sessionTimeoutMs = 10'000;
    gcfg.autoOffsetReset = rc::OffsetReset::Earliest;
    auto gc = member.subscribeGroup("g", {"events"}, gcfg);
    SubLedger led;
    gc.onRecords([&](const rc::RecordSet& rs) {
        rs.forEachRecord([&](std::string_view, std::string_view v, int64_t off, int64_t) {
            std::lock_guard lk(led.mu);
            if (!led.seqs[rs.partition].insert(seqOf(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()))).second) ++led.dupes;
            led.offs[rs.partition].insert(off);
        });
    });
    member.connectAndWait(30s);

    auto pcfg0 = clientConfigFor(c, survivor);
    pcfg0.topics = {spec};
    auto prodClient = connectClient(c, pcfg0);
    rc::ProducerConfig pcfg; pcfg.acks = rc::Acks::All; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 30'000; pcfg.deliveryTimeoutMs = 40'000;
    auto producer = prodClient->producer(pcfg);
    const rc::TopicHandle pth = prodClient->topic("events");
    std::atomic<bool> stop{false};
    std::atomic<int>  sent{0};
    std::thread pump([&] {
        for (int seq = 0; !stop.load(); ++seq) {
            const int32_t part = seq % kPartitions;
            uint64_t k = static_cast<uint64_t>(seq) * 16;
            while (rc::Key(k).partition(kPartitions) != part) ++k;
            const std::string v = valueOf(seq);
            producer.send(pth, rc::Key(k), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
            sent.store(seq + 1);
            std::this_thread::sleep_for(7ms);
        }
    });
    struct JoinOnExit {
        std::atomic<bool>& stop; std::thread& t;
        ~JoinOnExit() { stop.store(true); if (t.joinable()) t.join(); }
    } joinPump{stop, pump};
    auto everyPartitionPast = [&](int seq) {
        std::lock_guard lk(led.mu);
        for (int32_t p = 0; p < kPartitions; ++p) {
            const auto& s = led.seqs[p];
            if (s.empty() || *s.rbegin() < seq) return false;
        }
        return true;
    };
    bool warm = false;
    for (const auto deadline = Clock::now() + 40s; Clock::now() < deadline && !warm; std::this_thread::sleep_for(20ms))
        warm = everyPartitionPast(30);
    ASSERT_TRUE(warm) << "precondition: the member consumes every partition";

    c.crash(ctl);   // the seed AND the group coordinator
    const auto tCrash = Clock::now();
    const int target = sent.load() + 3 * kPartitions;   // produced after the crash, on every partition
    bool resumed = false;
    for (const auto deadline = tCrash + 40s; Clock::now() < deadline && !resumed; std::this_thread::sleep_for(20ms))
        resumed = everyPartitionPast(target);
    EXPECT_TRUE(resumed) << "the member keeps consuming every partition after its seed/coordinator broker died";
    {
        // Fix round 1 (M6): at-least-once may redeliver (duplicates allowed), but never skip: every
        // partition's consumed offsets are contiguous from the first one consumed.
        std::lock_guard lk(led.mu);
        for (int32_t p = 0; p < kPartitions; ++p) {
            const auto& o = led.offs[p];
            ASSERT_FALSE(o.empty());
            EXPECT_EQ(static_cast<int64_t>(o.size()), *o.rbegin() - *o.begin() + 1) << "a gap on partition " << p;
        }
    }
    std::fprintf(stderr, "[group] resumed=%d after %lld ms\n", resumed ? 1 : 0,
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tCrash).count()));
    stop.store(true);
    if (pump.joinable()) pump.join();
    gc.close();
    producer.close();
    prodClient->close();
    member.close();
}


// ---------------------------------------------------------------------------------------------------------
// Fix round 1 (I7): the remaining sibling paths that ride the seed.
// ---------------------------------------------------------------------------------------------------------
namespace {
int32_t awaitController(ThreeNodes& c) {
    int32_t ctl = -1;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && ctl < 0; std::this_thread::sleep_for(20ms))
        for (rune::node::BrokerNode* n : c.live()) if (n->isLeader()) ctl = n->nodeIdForTest();
    return ctl;
}
}  // namespace

// The transactional producer's coordinator (the consensus leader) is the seed's broker and dies mid-
// transaction.  The commit answers within the stale threshold + the txn op timeout — OK (the records then
// visible exactly once to read_committed), or an explicit failure after which a retried transaction
// commits — never a Timeout from waiting on the dead broker.
// RED (no seed failover): the commit waits on the dead coordinator until transactionOpTimeoutMs (Timeout),
// and the retry cannot reach any coordinator.
TEST(ClientFailoverBrokerRegression, SeedCoordinatorCrashedMidTransactionEndsCommittedOrRetriableRegression) {
    ThreeNodes c = ThreeNodes::start("fo_txn");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto def = admin->defineTopicAndWait("events", kPartitions, "", {}, false, kDisk, to, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    const rc::TopicSpec spec{"events", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s));

    auto cfg = clientConfigFor(c, ctl);
    cfg.topics = {spec};
    cfg.connectTimeoutMs = 30'000; cfg.pingTimeoutMs = 30'000;
    rc::Client client(cfg);
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 30'000; pcfg.deliveryTimeoutMs = 40'000;
    pcfg.transactionOpTimeoutMs = 25'000;
    auto tp = client.transactionalProducer("tx-failover", pcfg);
    client.connectAndWait(30s);
    tp.initTransactionsAndWait(60s);
    const rc::TopicHandle th = client.topic("events");
    auto sendTxn = [&](int attempt) {
        tp.beginTransaction();
        for (int i = 0; i < 10; ++i) {
            const std::string v = "tx=" + std::to_string(attempt) + ";" + valueOf(i);
            tp.send(th, rc::Key(static_cast<uint64_t>(i)), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()));
        }
    };
    sendTxn(1);
    c.crash(ctl);   // the coordinator AND the seed
    const auto t0 = Clock::now();
    int committedAttempt = -1;
    std::string firstError;
    try {
        tp.commitTransactionAndWait(60s);
        committedAttempt = 1;
    } catch (const std::exception& e) {
        firstError = e.what();
    }
    const auto took = Clock::now() - t0;
    const auto staleSession = std::chrono::nanoseconds(rune::client::session::SessionConfig{}.responseTimeoutNs);
    EXPECT_LT(took, staleSession + 25s + 5s) << "the commit answered within stale + txn op timeout";
    EXPECT_EQ(firstError.find("timed out"), std::string::npos) << "never a Timeout from a hang: " << firstError;
    if (committedAttempt < 0) {
        // An explicit failure: the transaction is abortable / retriable — abort, retry it once.
        try { tp.abortTransactionAndWait(30s); } catch (const std::exception&) {}
        try { tp.initTransactionsAndWait(30s); } catch (const std::exception&) {}
        sendTxn(2);
        EXPECT_NO_THROW(tp.commitTransactionAndWait(60s)) << "the retried transaction commits (first: " << firstError << ")";
        committedAttempt = 2;
    }
    std::fprintf(stderr, "[txn] committed attempt %d after %lld ms (first error: '%s')\n", committedAttempt,
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()),
                 firstError.c_str());
    tp.close();
    client.close();

    // read_committed read-back through a survivor: the committed attempt's 10 records, each exactly once.
    int via = -1;
    for (int id : c.runningNodes()) via = id;
    auto rcfg = clientConfigFor(c, via);
    rcfg.topics = {spec};
    auto reader = connectClient(c, rcfg);
    std::mutex mu;
    std::map<std::string, int> seen;
    std::vector<rc::Subscription> subs;
    rc::SubscribeOptions o;
    o.from = rc::From::earliest();
    o.isolation = rc::Isolation::ReadCommitted;
    for (int32_t p = 0; p < kPartitions; ++p)
        subs.push_back(reader->subscribe(reader->topic("events"), p, o, [&](const rc::Record& r) {
            std::lock_guard lk(mu);
            ++seen[std::string(reinterpret_cast<const char*>(r.value.data()), r.value.size())];
        }));
    const std::string prefix = "tx=" + std::to_string(committedAttempt) + ";";
    auto committedSeen = [&] {
        std::lock_guard lk(mu);
        int n = 0;
        for (const auto& [v, k] : seen) n += v.rfind(prefix, 0) == 0 ? 1 : 0;
        return n;
    };
    for (const auto deadline = Clock::now() + 40s; Clock::now() < deadline && committedSeen() < 10;) std::this_thread::sleep_for(50ms);
    std::this_thread::sleep_for(500ms);
    {
        std::lock_guard lk(mu);
        int n = 0;
        for (const auto& [v, k] : seen) {
            if (v.rfind(prefix, 0) != 0) continue;
            ++n;
            EXPECT_EQ(k, 1) << v << " visible " << k << " times";
        }
        EXPECT_EQ(n, 10) << "every committed record visible to read_committed";
    }
    for (auto& sub : subs) sub.unsubscribe();
    reader->close();
}

// GetViewRow rides the seed (isRoutedCtlOp follows the source leader's Redirect).  The seed's broker — the
// view source partition's leader — dies; a read issued after the crash ends Ok (or a retriable kNotLeader /
// kNotReady / an explicit session-lost failure, retried) within the bound — never a Timeout, never "not connected"
// (fix round 2 I-1: a read issued while the seed fails over waits for it).
// RED (no seed failover): every retried read fails "not connected" on the dead seed; no Ok within the bound.
TEST(ClientFailoverBrokerRegression, SeedCrashedGetViewRowFollowsTheNewSeedRegression) {
    ThreeNodes c = ThreeNodes::start("fo_gvr");
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    const int32_t ctl = awaitController(c);
    ASSERT_GT(ctl, 0);
    auto admin = connectClient(c, ctl);
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto src = admin->defineTopicAndWait("src", 1, "id", rune::node::test::charBlob(), false, kDisk, to, {}, 60s);
    ASSERT_EQ(src.errorCode, 0) << src.detail;
    const auto view = admin->createViewAndWait("v", "id", 0, rune::node::test::viewPlanBlob("v", src.topicId, 0), {}, 60s);
    ASSERT_EQ(view.errorCode, 0) << view.detail;
    admin->close();
    std::vector<int32_t> leaders;
    {
        int32_t l = -1;
        for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && l <= 0; std::this_thread::sleep_for(20ms))
            l = leaderOn(c.node(c.runningNodes().front()), src.topicId, 0);
        ASSERT_GT(l, 0);
        leaders.push_back(l);
    }
    const int32_t victim = leaders[0];   // leads the view's source partition; the reader's seed

    // One character {id 101, hp 7}, acks=All, through a survivor.
    int32_t survivor = -1;
    for (int id : c.runningNodes()) if (id != victim) { survivor = id; break; }
    auto pcfg0 = clientConfigFor(c, survivor);
    pcfg0.topics = {rc::TopicSpec{"src", src.topicId, 1}};
    auto prod = connectClient(c, pcfg0);
    {
        rc::ProducerConfig pc; pc.acks = rc::Acks::All; pc.lingerUs = 1; pc.batchBytes = 1;
        auto p = prod->producer(pc);
        rc::Completions cq(*prod);
        std::string value;
        auto le = [&](uint64_t v, int n) { for (int i = 0; i < n; ++i) value.push_back(static_cast<char>(v >> (8 * i))); };
        le(16, 2); le(1, 2); le(101, 8); le(7, 8);
        ASSERT_EQ(p.trySend(prod->topic("src"), rc::Key(uint64_t{101}),
                            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data()), value.size()), cq, 1),
                  rc::SendResult::Ok);
        rc::Completion d;
        ASSERT_EQ(cq.waitFor(1, 30s, [&](const rc::Completion& x, std::span<const uint8_t>) { d = x; }), rc::Completions::Wait::Done);
        ASSERT_EQ(d.status, rc::OpStatus::Ok);
        p.close();
    }
    prod->close();

    auto rcfg = clientConfigFor(c, victim);
    rcfg.connectTimeoutMs = 30'000; rcfg.pingTimeoutMs = 30'000;
    auto reader = connectClient(c, rcfg);
    rc::ViewRowResult r0;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline;) {   // precondition: served
        r0 = reader->getViewRowAndWait(view.viewTopicId, 101, 0, 10s);
        if (r0.status == rc::ViewRowResult::kOk) break;
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_EQ(r0.status, rc::ViewRowResult::kOk) << "precondition: the row is served before the crash";   // fix round 2 (M-4)
    c.crash(victim);
    std::this_thread::sleep_for(1s);
    const auto t0 = Clock::now();
    rc::ViewRowResult r;
    std::vector<std::string> errors;
    bool ok = false;
    while (!ok && Clock::now() - t0 < 40s) {
        try {
            r = reader->getViewRowAndWait(view.viewTopicId, 101, 0, 35s);
            ok = r.status == rc::ViewRowResult::kOk;
        } catch (const std::exception& e) {
            errors.emplace_back(e.what());
        }
        if (!ok) std::this_thread::sleep_for(100ms);
    }
    EXPECT_TRUE(ok) << "a view row read through the failed-over seed (" << errors.size() << " errors, last: "
                    << (errors.empty() ? std::string() : errors.back()) << ")";
    for (const auto& e : errors) EXPECT_EQ(e.find("timed out"), std::string::npos) << "never a Timeout: " << e;
    // Fix round 2 (I-1 / M-4): a read issued while the seed fails over WAITS for it — never "not connected".
    for (const auto& e : errors) EXPECT_EQ(e.find("not connected"), std::string::npos) << e;
    std::fprintf(stderr, "[gvr] ok=%d after %lld ms, %zu explicit errors\n", ok ? 1 : 0,
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count()),
                 errors.size());
    reader->close();
}

// ---------------------------------------------------------------------------------------------------------
// Fix round 3 (client-failover-review.md "Fix round 2 check", N-I1 + M-2): failover with TLS ON.  Every
// broker has a certificate; `require` makes the brokers require_tls and the client requireCrypto, otherwise
// crypto is offered and negotiated but not required.  The seed's broker (it leads p0) is crashed; every
// record produced after the crash must be acked — on a session negotiated AFRESH with the new broker.
// RED (N-I1 reverted, a shard client): the failover move's fresh connect is PLAINTEXT and the session keeps
// the old keys — requireCrypto: terminal Rejected (the client fails for good); offered: every frame sealed
// under keys the new broker never had (dropped), records never acked.  The plain-client cells are GUARDS
// (its failover always used forceReconnect, which already started a fresh TLS episode).
// ---------------------------------------------------------------------------------------------------------
namespace {
std::string readFile(const std::string& path) {
    std::ifstream f(path, std::ios::binary);
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}
void tlsFailover(const char* tag, bool shard, bool require) {
    const std::string dir = RUNE_CRYPTO_TESTDATA_DIR;
    ThreeNodes c = ThreeNodes::startWith(tag, [&](rune::node::NodeConfig& n) {
        n.tlsCertFile = dir + "/server.pem";
        n.tlsKeyFile  = dir + "/server.key";
        n.requireTls  = require;
    });
    ASSERT_TRUE(c.awaitCatalogLeader(30s));
    auto acfg = clientConfigFor(c, c.runningNodes().front());
    acfg.offerCrypto = true; acfg.requireCrypto = require;
    acfg.tlsTrustPem = readFile(dir + "/ca.pem"); acfg.tlsExpectHost = "localhost";
    auto admin = connectClient(c, acfg);
    rc::TopicOptions to;
    to.replicationFactor = 3;
    const auto def = admin->defineTopicAndWait("events", kPartitions, "", {}, false, kDisk, to, {}, 60s);
    ASSERT_EQ(def.errorCode, 0) << def.detail;
    admin->close();
    const rc::TopicSpec spec{"events", def.topicId, kPartitions};
    std::vector<int32_t> leaders;
    ASSERT_TRUE(awaitLeaders(c, def.topicId, leaders, 30s));
    const int32_t victim = leaders[0];

    auto cfg = clientConfigFor(c, victim);
    cfg.topics = {spec};
    cfg.offerCrypto = true; cfg.requireCrypto = require;
    cfg.tlsTrustPem = acfg.tlsTrustPem; cfg.tlsExpectHost = "localhost";
    cfg.reconnectForeverAfterLive = shard;
    cfg.connectTimeoutMs = 5'000; cfg.pingTimeoutMs = 30'000;
    auto client = connectClient(c, cfg);
    rc::ProducerConfig pcfg;
    pcfg.acks = rc::Acks::All; pcfg.lingerUs = 1; pcfg.batchBytes = 1;
    pcfg.produceTimeoutMs = 30'000; pcfg.deliveryTimeoutMs = 60'000;
    auto producer = client->producer(pcfg);
    rc::Completions cq(*client);
    const rc::TopicHandle th = client->topic("events");
    auto sendOne = [&](int seq) {
        uint64_t k = static_cast<uint64_t>(seq) * 16;
        while (rc::Key(k).partition(kPartitions) != seq % kPartitions) ++k;
        const std::string v = valueOf(seq);
        for (;;) {
            const auto r = producer.trySend(th, rc::Key(k), std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(v.data()), v.size()),
                                            cq, static_cast<rc::Token>(seq) + 1);
            if (r == rc::SendResult::Ok) return true;
            if (r != rc::SendResult::QueueFull && r != rc::SendResult::BackPressured) return false;
            (void)cq.poll([](const rc::Completion&, std::span<const uint8_t>) {});
        }
    };
    std::map<rc::Token, rc::OpStatus> done;
    auto drain = [&] {
        (void)cq.poll([&](const rc::Completion& d, std::span<const uint8_t>) { done[d.token] = d.status; });
    };
    for (int seq = 0; seq < 9; ++seq) ASSERT_TRUE(sendOne(seq));
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && done.size() < 9; std::this_thread::sleep_for(5ms)) drain();
    ASSERT_EQ(done.size(), 9u) << "precondition: encrypted produce works";
    for (const auto& [t, st] : done) ASSERT_EQ(st, rc::OpStatus::Ok);

    c.crash(victim);
    const auto tCrash = Clock::now();
    constexpr int kAfter = 30;
    for (int seq = 9; seq < 9 + kAfter; ++seq) { ASSERT_TRUE(sendOne(seq)); std::this_thread::sleep_for(20ms); }
    for (const auto deadline = tCrash + 45s; Clock::now() < deadline && done.size() < 9u + kAfter;) {
        drain();
        std::this_thread::sleep_for(5ms);
    }
    int ok = 0;
    for (int seq = 9; seq < 9 + kAfter; ++seq) ok += done.count(seq + 1) != 0 && done[seq + 1] == rc::OpStatus::Ok ? 1 : 0;
    EXPECT_EQ(ok, kAfter) << "records produced after the seed broker's crash, acked on the fresh TLS session";
    // The SEED's own session (the failed-over one) carries the control plane: a DDL through it succeeds.
    // (Records may ride node sessions that never failed over; this pins the seed's fresh TLS episode.)
    rc::DefineTopicResult d2;
    EXPECT_NO_THROW(d2 = client->defineTopicAndWait("after", 1, "", {}, false, kDisk, {}, {}, 30s));
    EXPECT_EQ(d2.errorCode, 0) << d2.detail;
    std::fprintf(stderr, "[tls-failover %s %s] acked %d/%d after %lld ms\n", shard ? "shard" : "plain",
                 require ? "require" : "offered", ok, kAfter,
                 static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - tCrash).count()));
    producer.close();
    client->close();
}
}  // namespace

TEST(ClientFailoverBrokerRegression, ShardClientWithRequireCryptoFailsOverToALiveBrokerRegression) {
    tlsFailover("fo_tls_sr", /*shard=*/true, /*require=*/true);
}
TEST(ClientFailoverBrokerRegression, ShardClientWithOfferedCryptoFailsOverToALiveBrokerRegression) {
    tlsFailover("fo_tls_so", /*shard=*/true, /*require=*/false);
}
TEST(ClientFailoverBrokerRegression, PlainClientWithRequireCryptoFailsOverToALiveBrokerRegression) {
    tlsFailover("fo_tls_pr", /*shard=*/false, /*require=*/true);   // GUARD
}
TEST(ClientFailoverBrokerRegression, PlainClientWithOfferedCryptoFailsOverToALiveBrokerRegression) {
    tlsFailover("fo_tls_po", /*shard=*/false, /*require=*/false);   // GUARD
}
