// examples/shard_gate.cpp — T15b gates 2-5 driver (real broker, real transport).
//
// One producer run per invocation, in one of four client arms:
//   --arm client-thread   plain Client, RunnerMode::Thread (today's default)
//   --arm client-invoker  plain Client, RunnerMode::Invoker (this thread drives it)
//   --arm shard-agent     ShardGroup (Agent; --shards N, 0 = AUTO), any-thread sends
//   --arm shard-invoker   one ClientShard (Invoker; this thread drives it)
// plus  --arm verify      read every partition back from offset 0 (Invoker
//                         Client) and check record ids: distinct / duplicates /
//                         missing against --expect, and (--acked-file) the ids
//                         acked OK but missing from the log.
//
// Records: value = [uint64 id][padding to --value-bytes], key = id.  Open loop
// (--rate R): record i is SCHEDULED at t0 + i/R and its latency is measured from
// that schedule (coordinated-omission safe: a stalled client is charged for
// the records it could not send); --rate 0 = saturation (latency from the send).
// Every accepted record's batch ack is matched to its id by (partition,
// producerEpoch, sequence): the sender records each partition's ids in send
// order; the acks are logged and, at report time, sorted per partition by
// (epoch, baseSequence) — the producer's encode order, since an epoch bump
// restarts sequences at 0 for every batch encoded after it — so the n-th
// sequenced record of a partition is its n-th sent id.  Records failed before
// they were ever sequenced (baseSequence -1) do not take a sequence; if any
// occur the match is reported `match=approx`.  The full latency array is kept
// (no sampling) and percentiles are exact.
//
// --acked-file PATH: the produce run writes the set of ids acked OK there (see
// AckedFile below); --arm verify reads it and reports ok_missing = ids acked OK
// but absent from the log (must be 0: an OK ack is a durability promise).
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
#include "AtomicFile.h"
#include "ExampleAuth.h"
#include <rune/client/Client.h>
#include <rune/client/detail/CpuAffinity.h>
#include <rune/client/shard/ClientShard.h>

#include "FrRun.h"
#include "OpenLoopSchedule.h"

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
    std::string ackedFile;        // produce: write the OK-acked id set; verify: read it
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
        else if (k == "--acked-file") a.ackedFile = v();
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
    rune::examples::applyEnvCredentials(c);   // RUNE_USER / RUNE_PASSWORD_FILE
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
    std::vector<std::vector<int64_t>>  ordToId;    // partition -> send ordinal -> id
    std::vector<int32_t>               nextOrd;    // partition -> next send ordinal (sender)
    // Report side (after every ack runner stopped).
    std::vector<int64_t>               latencyNs;  // id -> ack - sched, -1 = none
    std::vector<uint8_t>               okIds;      // id -> 1 if acked OK
    std::vector<uint8_t>               failedIds;  // id -> 1 if reported failed (sequenced batches)
    int64_t                            accepted = 0;

    void init(const Args& a, int64_t cap) {
        sched.assign(static_cast<std::size_t>(cap), 0);
        latencyNs.assign(static_cast<std::size_t>(cap), -1);
        okIds.assign(static_cast<std::size_t>(cap), 0);
        failedIds.assign(static_cast<std::size_t>(cap), 0);
        // Keys hash evenly: 2x the fair share per partition (+ slack); a record
        // past it is counted as `unmatched` (no latency sample), never mis-attributed.
        const auto perPartition = static_cast<std::size_t>(cap / std::max(1, a.partitions) * 2 + 4096);
        ordToId.assign(static_cast<std::size_t>(a.partitions), std::vector<int64_t>(perPartition, -1));
        nextOrd.assign(static_cast<std::size_t>(a.partitions), 0);
    }
};

// One sequenced batch ack, logged by the runner that delivered it.
struct AckRec {
    int64_t t;
    int64_t producerId;  // -1: unknown (the Completions form carries none)
    int32_t partition;
    int32_t baseSequence;
    int32_t recordCount;
    int16_t producerEpoch;
    bool    ok;
};

// One per ack-delivering thread (a shard's runner / the client's runner / this thread).
struct AckCtx {
    Ledger*             L           = nullptr;
    int64_t             ok          = 0;
    int64_t             failed      = 0;
    int64_t             records     = 0;
    int64_t             lastAck     = 0;
    int64_t             unseqFailed = 0;  // records failed before they were sequenced
    int64_t             logDropped  = 0;  // acks the full log could not keep
    std::vector<AckRec> log;              // sequenced acks (reserved up front: no growth in the handler)
    void reserve(int64_t cap) { log.reserve(static_cast<std::size_t>(cap) + 1024); }
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
        } else {
            self->ok += a.recordCount;  // baseOffset -1 = a known-old duplicate: stored, offset unknown
        }
        if (a.baseSequence < 0) {
            if (a.errorCode != 0) self->unseqFailed += a.recordCount;
            return;  // acks=None or never sequenced: no (epoch, sequence) to match
        }
        if (self->log.size() < self->log.capacity()) {
            self->log.push_back(
                {t, a.producerId, a.partition, a.baseSequence, a.recordCount, a.producerEpoch, a.errorCode == 0});
        } else {
            ++self->logDropped;
        }
    }
};

// Report time (every runner stopped): per partition, the sequenced acks sorted
// by (producerId, epoch, baseSequence) are the producer's encode order -> the
// n-th sequenced record is the n-th sent id.  Within one producerId the epoch
// only grows; a producerId change is the INT16_MAX rotation: the group that
// reached epoch INT16_MAX comes first (more than two groups, or an unknown id
// next to a rotation, cannot be ordered: approx).  Fills latencyNs / okIds /
// failedIds.  Returns whether the match is exact (no unsequenced failures, no
// lost log entries, no sequence gaps, an unambiguous identity order).
bool matchAcks(Ledger& L, const std::vector<AckCtx*>& ctxs, int32_t partitions) {
    std::vector<std::vector<AckRec>> byPart(static_cast<std::size_t>(partitions));
    bool exact = true;
    for (const AckCtx* c : ctxs) {
        if (c->unseqFailed != 0 || c->logDropped != 0) exact = false;
        for (const AckRec& r : c->log) {
            if (r.partition >= 0 && r.partition < partitions) byPart[static_cast<std::size_t>(r.partition)].push_back(r);
        }
    }
    for (int32_t p = 0; p < partitions; ++p) {
        auto& v = byPart[static_cast<std::size_t>(p)];
        // Identity groups, ordered: the one that reached INT16_MAX first.
        std::vector<int64_t> pids;
        bool rotatedFromMax = false;
        for (const AckRec& r : v) {
            if (std::find(pids.begin(), pids.end(), r.producerId) == pids.end()) pids.push_back(r.producerId);
            if (r.producerEpoch == INT16_MAX) rotatedFromMax = true;
        }
        if (pids.size() > 2 || (pids.size() == 2 && !rotatedFromMax) ||
            (rotatedFromMax && std::find(pids.begin(), pids.end(), int64_t{-1}) != pids.end())) {
            exact = false;
        }
        auto rank = [&](int64_t pid) {
            if (pids.size() < 2) return 0;
            for (const AckRec& r : v) {
                if (r.producerId == pid && r.producerEpoch == INT16_MAX) return 0;
            }
            return 1;
        };
        std::sort(v.begin(), v.end(), [&](const AckRec& x, const AckRec& y) {
            const int rx = rank(x.producerId), ry = rank(y.producerId);
            if (rx != ry) return rx < ry;
            return x.producerEpoch != y.producerEpoch ? x.producerEpoch < y.producerEpoch
                                                      : x.baseSequence < y.baseSequence;
        });
        const auto& ids = L.ordToId[static_cast<std::size_t>(p)];
        int64_t ord = 0;
        int64_t pid = INT64_MIN;
        int16_t epoch = -1;
        int32_t nextSeq = 0;
        for (const AckRec& r : v) {
            if (r.producerEpoch != epoch || r.producerId != pid) {
                pid     = r.producerId;
                epoch   = r.producerEpoch;
                nextSeq = 0;
            }
            if (r.baseSequence != nextSeq) exact = false;  // a sequence gap: ordinals may shift
            nextSeq = r.baseSequence + r.recordCount;
            for (int32_t i = 0; i < r.recordCount; ++i, ++ord) {
                if (ord >= static_cast<int64_t>(ids.size())) continue;
                const int64_t id = ids[static_cast<std::size_t>(ord)];
                if (id < 0) continue;
                if (!r.ok) {
                    L.failedIds[static_cast<std::size_t>(id)] = 1;
                    continue;
                }
                L.okIds[static_cast<std::size_t>(id)] = 1;
                L.latencyNs[static_cast<std::size_t>(id)] = r.t - L.sched[static_cast<std::size_t>(id)];
            }
        }
    }
    return exact;
}

// --acked-file format (little-endian): u32 magic 'SGAK' (0x4B414753), u32
// version 2, u64 nbits, then two bitmaps of ceil(nbits / 8) bytes — bit
// (id % 8) of byte (id / 8): the ids acked OK, then the ids reported FAILED (in
// a sequenced batch).  Version 1 (the OK bitmap only) is still read.
constexpr uint32_t kAckedMagic = 0x4B414753u;

bool writeAckedFile(const std::string& path, const std::vector<uint8_t>& okIds,
                    const std::vector<uint8_t>& failedIds) {
    const uint32_t hdr[2] = {kAckedMagic, 2u};
    const uint64_t nbits  = okIds.size();
    auto pack = [&](const std::vector<uint8_t>& ids) {
        std::vector<uint8_t> bits((nbits + 7) / 8, 0);
        for (std::size_t i = 0; i < ids.size() && i < nbits; ++i) {
            if (ids[i] != 0) bits[i / 8] = static_cast<uint8_t>(bits[i / 8] | (1u << (i % 8)));
        }
        return bits;
    };
    const std::vector<uint8_t> okBits = pack(okIds), failedBits = pack(failedIds);
    std::string bytes;
    bytes.append(reinterpret_cast<const char*>(hdr), sizeof hdr);
    bytes.append(reinterpret_cast<const char*>(&nbits), sizeof nbits);
    bytes.append(reinterpret_cast<const char*>(okBits.data()), okBits.size());
    if (!okBits.empty()) bytes.append(reinterpret_cast<const char*>(failedBits.data()), failedBits.size());
    return rune::examples::writeFileAtomically(path, bytes);   // #44: a poller never sees a partial file
}

// Returns false on a missing / malformed file.  failedIds is empty for v1.
bool readAckedFile(const std::string& path, std::vector<uint8_t>& okIds, std::vector<uint8_t>& failedIds) {
    std::FILE* f = std::fopen(path.c_str(), "rb");
    if (f == nullptr) return false;
    uint32_t hdr[2] = {0, 0};
    uint64_t nbits  = 0;
    bool ok = std::fread(hdr, sizeof hdr, 1, f) == 1 && std::fread(&nbits, sizeof nbits, 1, f) == 1 &&
              hdr[0] == kAckedMagic && (hdr[1] == 1u || hdr[1] == 2u) && nbits <= (uint64_t{1} << 40);
    std::vector<uint8_t> okBits, failedBits;
    if (ok) {
        const std::size_t n = static_cast<std::size_t>((nbits + 7) / 8);
        okBits.resize(n);
        ok = n == 0 || std::fread(okBits.data(), n, 1, f) == 1;
        if (ok && hdr[1] == 2u) {
            failedBits.resize(n);
            ok = n == 0 || std::fread(failedBits.data(), n, 1, f) == 1;
        }
    }
    std::fclose(f);
    if (!ok) return false;
    okIds.assign(static_cast<std::size_t>(nbits), 0);
    for (std::size_t i = 0; i < okIds.size(); ++i) okIds[i] = (okBits[i / 8] >> (i % 8)) & 1u;
    failedIds.clear();
    if (!failedBits.empty()) {
        failedIds.assign(static_cast<std::size_t>(nbits), 0);
        for (std::size_t i = 0; i < failedIds.size(); ++i) failedIds[i] = (failedBits[i / 8] >> (i % 8)) & 1u;
    }
    return true;
}

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
    // Exact integer schedule (no per-record truncation drift at rates that do
    // not divide 1e9) — shared with rune_produce_throughput's invoker bench.
    const rune::bench::OpenLoopSchedule schedule(t0, a.rate > 0 ? static_cast<uint64_t>(a.rate) : 1);
    const int64_t        stopAt = a.seconds > 0 ? t0 + static_cast<int64_t>(a.seconds * 1e9) : INT64_MAX;
    tFirst = t0;
    for (int64_t id = 0; id < cap; ++id) {
        const int64_t due = a.rate > 0 ? schedule.scheduledNs(static_cast<uint64_t>(id)) : 0;
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
        const int32_t   ord = L.nextOrd[static_cast<std::size_t>(p)];
        std::memcpy(val.data(), &id, sizeof id);
        L.sched[static_cast<std::size_t>(id)] = a.rate > 0 ? due : AckCtx::nowNs();
        if (ord < static_cast<int32_t>(L.ordToId[static_cast<std::size_t>(p)].size())) {
            L.ordToId[static_cast<std::size_t>(p)][static_cast<std::size_t>(ord)] = id;
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
        ++L.nextOrd[static_cast<std::size_t>(p)];
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
    const bool exact = matchAcks(L, ctxs, a.partitions);
    int64_t    okIdCount = 0;
    for (uint8_t b : L.okIds) okIdCount += b;
    std::string acked = "-";
    if (!a.ackedFile.empty()) {
        acked = writeAckedFile(a.ackedFile, L.okIds, L.failedIds) ? a.ackedFile : std::string("WRITE_FAILED");
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
                " ok_ids=%" PRId64 " match=%s acked_file=%s"
                " send_rps=%.0f ack_rps=%.0f p50_us=%.1f p90_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f close=%s"
                " events=%s\n",
                a.arm.c_str(), a.shards, a.rate, a.valueBytes, a.acks.c_str(), L.accepted, records, ok, failed,
                unmatched, okIdCount, exact ? "exact" : "approx", acked.c_str(),
                sendSec > 0 ? static_cast<double>(L.accepted) / sendSec : 0.0,
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
        ctx[i].reserve(cap);
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
    ctx.reserve(cap);
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
    AckCtx       ctx;  // outlives the producer (its close completes into it)
    ctx.L = &L;
    ctx.reserve(cap);
    rc::Producer prod = client.producer(producerConfig(a));
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
    // Where each id was first seen: a duplicate id at the SAME (partition, offset)
    // is a re-delivery by the reader; at a DIFFERENT offset it is a second copy
    // physically in the log.
    std::vector<int64_t>  firstOff(static_cast<std::size_t>(n > 0 ? n : 1), -1);
    std::vector<int32_t>  firstPart(static_cast<std::size_t>(n > 0 ? n : 1), -1);
    int64_t               redelivered = 0, logCopies = 0;
    rc::SubscribeOptions  opts;
    opts.from = rc::From::earliest();
    std::vector<rc::Subscription> subs;
    for (int32_t p = 0; p < a.partitions; ++p) {
        subs.push_back(client.subscribe(topic, p, opts, [&, p](const rc::Record& r) {
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
            const auto u = static_cast<std::size_t>(id);
            if (seen[u]++ != 0) {
                ++dups;
                if (firstOff[u] == r.offset && firstPart[u] == p) ++redelivered; else ++logCopies;
            } else {
                ++distinct;
                firstOff[u]  = r.offset;
                firstPart[u] = p;
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
    // ok_missing: acked OK by the produce run (--acked-file) but absent from the
    // log — an OK ack is a durability promise, so this must be 0.  -1 = no file.
    // failed_stored: reported FAILED (a sequenced batch) but present in the log
    // — a false failure (an app retry would duplicate it).  -1 = not available.
    int64_t okAcked = -1, okMissing = -1, failedReported = -1, failedStored = -1;
    if (!a.ackedFile.empty()) {
        std::vector<uint8_t> okIds, failedIds;
        if (readAckedFile(a.ackedFile, okIds, failedIds)) {
            okAcked = okMissing = 0;
            for (std::size_t i = 0; i < okIds.size(); ++i) {
                if (okIds[i] == 0) continue;
                ++okAcked;
                if (i >= seen.size() || seen[i] == 0) ++okMissing;
            }
            if (!failedIds.empty()) {
                failedReported = failedStored = 0;
                for (std::size_t i = 0; i < failedIds.size(); ++i) {
                    if (failedIds[i] == 0) continue;
                    ++failedReported;
                    if (i < seen.size() && seen[i] != 0) ++failedStored;
                }
            }
        } else {
            std::fprintf(stderr, "shard_gate: cannot read --acked-file %s\n", a.ackedFile.c_str());
        }
    }
    std::printf("VERIFY expect=%" PRId64 " total=%" PRId64 " distinct=%" PRId64 " duplicates=%" PRId64
                " missing=%" PRId64 " foreign=%" PRId64 " dup_same_offset=%" PRId64 " dup_log_copies=%" PRId64
                " ok_acked=%" PRId64 " ok_missing=%" PRId64 " failed_reported=%" PRId64 " failed_stored=%" PRId64
                "\n",
                n, total, distinct, dups, n - distinct, foreign, redelivered, logCopies, okAcked, okMissing,
                failedReported, failedStored);
    std::fflush(stdout);
    for (auto& s : subs) s.unsubscribe();
    client.close();
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    const Args a = parse(argc, argv);
    // flight-recorder: armed by FLIGHT_RECORDER_ENABLE before any client thread; report on exit.
    rune::bench::FrRun fr("gate-" + a.arm);
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
