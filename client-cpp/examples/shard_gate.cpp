// examples/shard_gate.cpp — T15b gates 2-5 driver (real broker, real transport).
//
// One producer run per invocation, in one of four client arms:
//   --arm client-thread   plain Client, RunnerMode::Thread (today's default)
//   --arm client-invoker  plain Client, RunnerMode::Invoker (this thread drives it)
//   --arm shard-agent     ShardGroup (Agent; --shards N, 0 = AUTO), any-thread sends
//   --arm shard-invoker   one ClientShard (Invoker; this thread drives it)
// plus  --arm verify      read every partition back from offset 0 (Invoker
//                         Client) and check record ids: distinct / duplicates /
//                         missing against --expect.
//
// Records: value = [uint64 id][padding to --value-bytes], key = id.  Open loop
// (--rate R): record i is SCHEDULED at t0 + i/R and its latency is measured from
// that schedule (coordinated-omission safe: a stalled client is charged for
// the records it could not send); --rate 0 = saturation (latency from the send).
// Every accepted record's batch ack is matched by (partition, sequence) -> id;
// the full latency array is kept (no sampling) and percentiles are exact.
//
// Threads: no atomics / mutexes.  The sender writes an id's schedule and its
// (partition, sequence) slot BEFORE the producer accepts it (the producer ring's
// release/acquire orders them before the runner's ack handler reads them);
// the handlers' counters are read only after close() joined the runners.
//
// --sigstop: run (at --rate) until a line arrives on stdin, printing every shard
// status as `EVENT <shard> <state>`; then close (drain) and print the ledger —
// the SIGSTOP DR gate drives it from a script.
//
// Output: one `RESULT key=value ...` line (and `VERIFY ...` for --arm verify).
// Exit code 0 = ran to completion (the gate scripts judge the numbers).
#include <rune/client/Client.h>
#include <rune/client/detail/CpuAffinity.h>
#include <rune/client/shard/ClientShard.h>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <sys/select.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
namespace rs = rune::client::shard;
using clk    = std::chrono::steady_clock;

namespace {

struct Args {
    std::string arm         = "shard-agent";
    std::string gateway     = "127.0.0.1:40200";
    int32_t     gwStream    = 1;
    std::string egress      = "127.0.0.1:41000";
    int32_t     egStream    = 2;
    std::string topic       = "bench";
    int32_t     topicId     = 0;
    int32_t     partitions  = 8;
    int64_t     records     = 200'000;
    double      seconds     = 0;  // cap on the send phase (0 = until --records)
    int64_t     rate        = 0;  // records/s, 0 = saturation
    int32_t     valueBytes  = 1024;
    std::string acks        = "all";
    uint32_t    shards      = 1;  // shard-agent: 0 = AUTO
    int32_t     core        = -1;  // pin: runner (client-thread), first shard runner, or this thread (invoker)
    bool        busySpin    = false;
    std::size_t mtu         = 1408;
    int64_t     lingerUs    = 0;
    bool        sigstop     = false;
    int64_t     expect      = 0;  // verify
    std::string dataDir     = "/tmp/shard_gate";
    int32_t     maxInFlight = 0;  // 0 = producer default
};

[[noreturn]] void usage(const char* why) {
    std::fprintf(stderr, "shard_gate: %s (see the header of examples/shard_gate.cpp)\n", why);
    std::exit(2);
}

Args parse(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        auto v = [&]() -> std::string {
            if (i + 1 >= argc) usage(("missing value for " + k).c_str());
            return argv[++i];
        };
        if (k == "--arm") a.arm = v();
        else if (k == "--gateway") a.gateway = v();
        else if (k == "--gateway-stream") a.gwStream = std::atoi(v().c_str());
        else if (k == "--egress") a.egress = v();
        else if (k == "--egress-stream") a.egStream = std::atoi(v().c_str());
        else if (k == "--topic") a.topic = v();
        else if (k == "--topic-id") a.topicId = std::atoi(v().c_str());
        else if (k == "--partitions") a.partitions = std::atoi(v().c_str());
        else if (k == "--records") a.records = std::atoll(v().c_str());
        else if (k == "--seconds") a.seconds = std::atof(v().c_str());
        else if (k == "--rate") a.rate = std::atoll(v().c_str());
        else if (k == "--value-bytes") a.valueBytes = std::atoi(v().c_str());
        else if (k == "--acks") a.acks = v();
        else if (k == "--shards") a.shards = static_cast<uint32_t>(std::atoi(v().c_str()));
        else if (k == "--core") a.core = std::atoi(v().c_str());
        else if (k == "--busy-spin") a.busySpin = true;
        else if (k == "--mtu") a.mtu = static_cast<std::size_t>(std::atoll(v().c_str()));
        else if (k == "--linger-us") a.lingerUs = std::atoll(v().c_str());
        else if (k == "--sigstop") a.sigstop = true;
        else if (k == "--expect") a.expect = std::atoll(v().c_str());
        else if (k == "--data-dir") a.dataDir = v();
        else if (k == "--max-in-flight") a.maxInFlight = std::atoi(v().c_str());
        else usage(("unknown option " + k).c_str());
    }
    if (a.valueBytes < 8) a.valueBytes = 8;
    return a;
}

std::string udp(const std::string& hostPort) { return "aeron:udp?endpoint=" + hostPort; }

rc::ClientConfig baseConfig(const Args& a) {
    rc::ClientConfig c;
    c.gatewayChannel  = udp(a.gateway);
    c.gatewayStreamId = a.gwStream;
    c.egressChannel   = udp(a.egress);
    c.egressStreamId  = a.egStream;
    c.dataDir         = a.dataDir + "_" + std::to_string(::getpid());
    c.mtuBytes        = a.mtu;
    c.idleMode        = a.busySpin ? rc::IdleMode::BusySpin : rc::IdleMode::Balanced;
    rc::TopicSpec t;
    t.name           = a.topic;
    t.topicId        = a.topicId;
    t.partitionCount = a.partitions;
    c.topics.push_back(t);
    return c;
}

rc::ProducerConfig producerConfig(const Args& a) {
    rc::ProducerConfig p;
    p.acks     = a.acks == "leader" ? rc::Acks::Leader : (a.acks == "none" ? rc::Acks::None : rc::Acks::All);
    p.lingerUs = a.lingerUs;
    if (a.maxInFlight > 0) p.maxInFlight = a.maxInFlight;
    return p;
}

// ---- the ledger -------------------------------------------------------------

struct Ledger {
    // Sender side (written before the producer accepts the record).
    std::vector<int64_t>               sched;      // id -> scheduled (or send) time, ns
    std::vector<std::vector<int64_t>>  seqToId;    // partition -> sequence -> id
    std::vector<int32_t>               nextSeq;    // partition -> next sequence (sender)
    // Ack side (one writer per id: the runner that acks it).
    std::vector<int64_t>               latencyNs;  // id -> ack - sched, -1 = none
    int64_t                            accepted = 0;

    void init(const Args& a, int64_t cap) {
        sched.assign(static_cast<std::size_t>(cap), 0);
        latencyNs.assign(static_cast<std::size_t>(cap), -1);
        // Keys hash evenly: 2x the fair share per partition (+ slack); a record
        // past it is counted as `unmatched` (no latency sample), never mis-attributed.
        const auto perPartition = static_cast<std::size_t>(cap / std::max(1, a.partitions) * 2 + 4096);
        seqToId.assign(static_cast<std::size_t>(a.partitions), std::vector<int64_t>(perPartition, -1));
        nextSeq.assign(static_cast<std::size_t>(a.partitions), 0);
    }
};

// One per ack-delivering thread (a shard's runner / the client's runner / this thread).
struct AckCtx {
    Ledger* L       = nullptr;
    int64_t ok      = 0;
    int64_t failed  = 0;
    int64_t records = 0;
    int64_t lastAck = 0;
    static int64_t nowNs() noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count();
    }
    static void on(void* ctx, const rc::BatchAck& a) noexcept {
        auto* self      = static_cast<AckCtx*>(ctx);
        const int64_t t = nowNs();
        self->records += a.recordCount;
        self->lastAck = t;
        if (a.errorCode != 0) {
            self->failed += a.recordCount;
            return;
        }
        self->ok += a.recordCount;
        const auto& map = self->L->seqToId[static_cast<std::size_t>(a.partition)];
        for (int32_t i = 0; i < a.recordCount; ++i) {
            const int64_t seq = static_cast<int64_t>(a.baseSequence) + i;
            if (seq < 0 || seq >= static_cast<int64_t>(map.size())) continue;
            const int64_t id = map[static_cast<std::size_t>(seq)];
            if (id >= 0) self->L->latencyNs[static_cast<std::size_t>(id)] = t - self->L->sched[static_cast<std::size_t>(id)];
        }
    }
};

bool stdinReady() {
    fd_set rd;
    FD_ZERO(&rd);
    FD_SET(0, &rd);
    timeval tv{0, 0};
    return ::select(1, &rd, nullptr, nullptr, &tv) == 1;
}

double pct(std::vector<int64_t>& v, double p) {
    if (v.empty()) return -1;
    const std::size_t i = std::min(v.size() - 1, static_cast<std::size_t>(p * static_cast<double>(v.size())));
    std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(i), v.end());
    return static_cast<double>(v[i]) / 1000.0;  // µs
}

// The send loop shared by every arm.  trySend(id, key, value) -> SendResult;
// pump() runs between attempts (Invoker: doWork + polls; Agent: events).
template <class TrySend, class Pump>
void sendLoop(const Args& a, Ledger& L, int64_t cap, TrySend&& trySend, Pump&& pump, int64_t& tFirst,
              int64_t& tLast) {
    std::vector<uint8_t> val(static_cast<std::size_t>(a.valueBytes), 0x5a);
    const int64_t        t0     = AckCtx::nowNs();
    const int64_t        period = a.rate > 0 ? 1'000'000'000LL / a.rate : 0;
    const int64_t        stopAt = a.seconds > 0 ? t0 + static_cast<int64_t>(a.seconds * 1e9) : INT64_MAX;
    tFirst = t0;
    for (int64_t id = 0; id < cap; ++id) {
        const int64_t due = a.rate > 0 ? t0 + id * period : 0;
        for (;;) {  // pace (open loop): busy-wait to the schedule, pumping
            const int64_t now = AckCtx::nowNs();
            if (now >= stopAt || (a.sigstop && stdinReady())) {
                tLast = now;
                return;
            }
            if (now >= due) break;
            pump();
        }
        const rc::Key   key(static_cast<uint64_t>(id));
        const int32_t   p   = key.partition(a.partitions);
        const int32_t   seq = L.nextSeq[static_cast<std::size_t>(p)];
        std::memcpy(val.data(), &id, sizeof id);
        L.sched[static_cast<std::size_t>(id)] = a.rate > 0 ? due : AckCtx::nowNs();
        if (seq < static_cast<int32_t>(L.seqToId[static_cast<std::size_t>(p)].size())) {
            L.seqToId[static_cast<std::size_t>(p)][static_cast<std::size_t>(seq)] = id;
        }
        for (;;) {
            const rc::SendResult r = trySend(key, std::span<const uint8_t>(val));
            if (r == rc::SendResult::Ok) break;
            if (r != rc::SendResult::BackPressured && r != rc::SendResult::QueueFull) {
                std::fprintf(stderr, "shard_gate: send refused (%d) at id %" PRId64 "\n", static_cast<int>(r), id);
                tLast = AckCtx::nowNs();
                return;
            }
            pump();
            if (a.sigstop && stdinReady()) {
                tLast = AckCtx::nowNs();
                return;
            }
        }
        ++L.nextSeq[static_cast<std::size_t>(p)];
        ++L.accepted;
    }
    tLast = AckCtx::nowNs();
}

void report(const Args& a, Ledger& L, const std::vector<AckCtx*>& ctxs, int64_t tFirst, int64_t tLast,
            const char* closeStatus, const std::string& events) {
    int64_t ok = 0, failed = 0, records = 0, lastAck = tFirst;
    for (const AckCtx* c : ctxs) {
        ok += c->ok;
        failed += c->failed;
        records += c->records;
        lastAck = std::max(lastAck, c->lastAck);
    }
    std::vector<int64_t> lat;
    lat.reserve(static_cast<std::size_t>(L.accepted));
    int64_t unmatched = 0;
    for (int64_t id = 0; id < L.accepted; ++id) {
        const int64_t v = L.latencyNs[static_cast<std::size_t>(id)];
        if (v >= 0) {
            lat.push_back(v);
        } else {
            ++unmatched;
        }
    }
    const double sendSec = static_cast<double>(tLast - tFirst) / 1e9;
    const double ackSec  = static_cast<double>(lastAck - tFirst) / 1e9;
    const double maxUs   = lat.empty() ? -1 : static_cast<double>(*std::max_element(lat.begin(), lat.end())) / 1000.0;
    std::printf("RESULT arm=%s shards=%u rate=%" PRId64 " value=%d acks=%s accepted=%" PRId64 " completed=%" PRId64
                " ok=%" PRId64 " failed=%" PRId64 " unmatched=%" PRId64
                " send_rps=%.0f ack_rps=%.0f p50_us=%.1f p90_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f close=%s"
                " events=%s\n",
                a.arm.c_str(), a.shards, a.rate, a.valueBytes, a.acks.c_str(), L.accepted, records, ok, failed,
                unmatched, sendSec > 0 ? static_cast<double>(L.accepted) / sendSec : 0.0,
                ackSec > 0 ? static_cast<double>(ok) / ackSec : 0.0, pct(lat, 0.50), pct(lat, 0.90), pct(lat, 0.99),
                pct(lat, 0.999), maxUs, closeStatus, events.empty() ? "-" : events.c_str());
    std::fflush(stdout);
}

const char* statusName(rc::OpStatus s) {
    switch (s) {
    case rc::OpStatus::Ok: return "Ok";
    case rc::OpStatus::Timeout: return "Timeout";
    case rc::OpStatus::Closed: return "Closed";
    default: return "Other";
    }
}

// ---- arms -----------------------------------------------------------------------

int runShardAgent(const Args& a, Ledger& L, int64_t cap) {
    rs::ShardGroupConfig g;
    g.base       = baseConfig(a);
    g.shardCount = a.shards;
    g.mode       = rs::ShardMode::Agent;
    g.firstCore  = a.core;
    g.producer   = producerConfig(a);
    rs::ShardGroup        grp(g);
    std::vector<AckCtx>   ctx(grp.size());
    std::vector<AckCtx*>  ptrs;
    for (uint32_t i = 0; i < grp.size(); ++i) {
        ctx[i].L = &L;
        grp.onBatchAck(i, &AckCtx::on, &ctx[i]);
        ptrs.push_back(&ctx[i]);
    }
    std::string events;
    auto        poll = [&] {
        grp.pollEvents([&](const rs::ShardStatus& s) {
            const std::string e = std::to_string(s.shard) + ":" + rs::toString(s.state);
            events += (events.empty() ? "" : ",") + e;
            if (a.sigstop) {
                std::printf("EVENT %u %s\n", s.shard, rs::toString(s.state));
                std::fflush(stdout);
            }
        });
    };
    grp.connectAll(std::chrono::seconds(30));
    poll();
    const rc::TopicHandle topic = grp.shard(0).topic(a.topic);
    int64_t               tFirst = 0, tLast = 0;
    int                   spin   = 0;
    sendLoop(
        a, L, cap, [&](const rc::Key& k, std::span<const uint8_t> v) { return grp.trySend(topic, k, v); },
        [&] {
            if ((++spin & 1023) == 0) poll();
        },
        tFirst, tLast);
    const rc::OpStatus st = grp.close(std::chrono::seconds(60));
    poll();
    // Per shard: records it accepted (by the partition -> shard map) vs acked Ok
    // by ITS runner — a shard only subscribes its own egress port (base + i), so
    // ok == accepted per shard proves every ack came back on the owner's port.
    std::vector<int64_t> acc(grp.size(), 0);
    for (int64_t id = 0; id < L.accepted; ++id) {
        const int32_t part = rc::Key(static_cast<uint64_t>(id)).partition(a.partitions);
        ++acc[rs::shardFor(a.topicId, part, grp.size())];
    }
    std::string per;
    for (uint32_t i = 0; i < grp.size(); ++i) {
        per += (i ? "," : "") + std::to_string(ctx[i].ok) + "/" + std::to_string(acc[i]);
    }
    events += " per_shard_ok/accepted=" + per;
    report(a, L, ptrs, tFirst, tLast, statusName(st), events);
    return 0;
}

int runShardInvoker(const Args& a, Ledger& L, int64_t cap) {
    if (a.core >= 0) (void)rc::detail::pinCurrentThreadToCore(a.core);
    rs::ShardGroupConfig g;
    g.base       = baseConfig(a);
    g.shardCount = 1;
    g.mode       = rs::ShardMode::Invoker;
    g.producer   = producerConfig(a);
    rs::ClientShard shard(g, 0);
    AckCtx          ctx;
    ctx.L = &L;
    std::string events;
    auto        pump = [&] {
        shard.doWork();
        shard.pollBatchAcks([&](const rc::BatchAck& b) { AckCtx::on(&ctx, b); });
        shard.pollEvents([&](const rs::ShardStatus& s) {
            events += (events.empty() ? "" : ",") + std::string(rs::toString(s.state));
        });
    };
    shard.connectAndWait(std::chrono::seconds(30));
    const rc::TopicHandle topic = shard.topic(a.topic);
    int64_t               tFirst = 0, tLast = 0;
    sendLoop(
        a, L, cap, [&](const rc::Key& k, std::span<const uint8_t> v) { return shard.trySend(topic, k, v); }, pump,
        tFirst, tLast);
    // Drain: pump until every accepted record completed (bounded).
    const auto deadline = clk::now() + std::chrono::seconds(60);
    while (ctx.records < L.accepted && clk::now() < deadline) pump();
    const rc::OpStatus st = shard.closeAndWait(std::chrono::seconds(60));
    shard.pollBatchAcks([&](const rc::BatchAck& b) { AckCtx::on(&ctx, b); });
    report(a, L, {&ctx}, tFirst, tLast, statusName(st), events);
    return 0;
}

int runClient(const Args& a, Ledger& L, int64_t cap, bool invoker) {
    rc::ClientConfig c = baseConfig(a);
    c.runnerMode       = invoker ? rc::RunnerMode::Invoker : rc::RunnerMode::Thread;
    if (!invoker) c.runnerCore = a.core;
    if (invoker && a.core >= 0) (void)rc::detail::pinCurrentThreadToCore(a.core);
    rc::Client   client(c);
    rc::Producer prod = client.producer(producerConfig(a));
    AckCtx       ctx;
    ctx.L = &L;
    prod.onBatchAck(&AckCtx::on, &ctx);
    client.connectAndWait(std::chrono::seconds(30));
    const rc::TopicHandle topic = client.topic(a.topic);
    int64_t               tFirst = 0, tLast = 0;
    sendLoop(
        a, L, cap, [&](const rc::Key& k, std::span<const uint8_t> v) { return prod.trySend(topic, k, v); },
        [&] {
            if (invoker) client.doWork();
        },
        tFirst, tLast);
    const rc::OpStatus st = prod.closeAndWait(std::chrono::seconds(60));
    client.close();  // joins the runner (Thread mode): the ack counters are now ours
    report(a, L, {&ctx}, tFirst, tLast, statusName(st), "");
    return 0;
}

// Read every partition back from offset 0 and check the ids (single-threaded:
// an Invoker Client; the handler runs on this thread).
int runVerify(const Args& a) {
    rc::ClientConfig c = baseConfig(a);
    c.runnerMode       = rc::RunnerMode::Invoker;
    rc::Client client(c);
    client.connectAndWait(std::chrono::seconds(30));
    const rc::TopicHandle topic = client.topic(a.topic);
    const int64_t         n     = a.expect;
    std::vector<uint8_t>  seen(static_cast<std::size_t>(n > 0 ? n : 1), 0);
    int64_t               total = 0, dups = 0, foreign = 0, distinct = 0;
    rc::SubscribeOptions  opts;
    opts.from = rc::From::earliest();
    std::vector<rc::Subscription> subs;
    for (int32_t p = 0; p < a.partitions; ++p) {
        subs.push_back(client.subscribe(topic, p, opts, [&](const rc::Record& r) {
            ++total;
            if (r.value.size() < 8) {
                ++foreign;
                return;
            }
            int64_t id = 0;
            std::memcpy(&id, r.value.data(), 8);
            if (id < 0 || id >= n) {
                ++foreign;
                return;
            }
            if (seen[static_cast<std::size_t>(id)]++ != 0) {
                ++dups;
            } else {
                ++distinct;
            }
        }));
    }
    auto       last     = clk::now();
    int64_t    lastSeen = -1;
    const auto hardStop = clk::now() + std::chrono::seconds(120);
    while (clk::now() < hardStop) {
        client.doWork();
        if (total != lastSeen) {
            lastSeen = total;
            last     = clk::now();
        }
        if (distinct >= n && clk::now() - last > std::chrono::seconds(2)) break;  // settle: late duplicates
        if (clk::now() - last > std::chrono::seconds(10)) break;                    // nothing more is coming
    }
    std::printf("VERIFY expect=%" PRId64 " total=%" PRId64 " distinct=%" PRId64 " duplicates=%" PRId64
                " missing=%" PRId64 " foreign=%" PRId64 "\n",
                n, total, distinct, dups, n - distinct, foreign);
    std::fflush(stdout);
    for (auto& s : subs) s.unsubscribe();
    client.close();
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const Args a = parse(argc, argv);
    try {
        if (a.arm == "verify") return runVerify(a);
        const int64_t cap = a.records > 0 ? a.records : 1;
        Ledger        L;
        L.init(a, cap);
        if (a.arm == "shard-agent") return runShardAgent(a, L, cap);
        if (a.arm == "shard-invoker") return runShardInvoker(a, L, cap);
        if (a.arm == "client-thread") return runClient(a, L, cap, false);
        if (a.arm == "client-invoker") return runClient(a, L, cap, true);
        usage("unknown --arm");
    } catch (const std::exception& e) {
        std::printf("RESULT arm=%s error=\"%s\"\n", a.arm.c_str(), e.what());
        std::fflush(stdout);
        return 1;
    }
}
