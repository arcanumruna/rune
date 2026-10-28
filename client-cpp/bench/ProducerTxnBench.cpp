// bench/ProducerTxnBench.cpp - Phase 2b F Task 14: the unified Producer, transactional vs plain acked, on every
// produce path, in-process (no Aeron; ProducerAckedBench's shape).
//
// Paths: send (void), trysend_cq (trySend(.., cq, token)), batch8 (Tier-2 batch() of 8 records), tryclaim
// (tryClaim + commit), claimbatch8 (claimBatch of 8 + commit), typed_claim (TypedTopic<Position, ProducerRef>
// ::claim + commit).  Modes: plain (acks=All, broker-assigned id, no transactionalId) and txn (the same producer
// with a transactionalId: initTransactions once, then beginTransaction / 1000 records / commitTransaction, the
// commit waited on through the Completions queue).
//
// App thread: the Producer handle (Thread-mode ring ingress).  Duty thread: owns a ProducerHub - drain + flush,
// answers every coordinator request at once (InitProducerId, AddPartitionsToTxn, EndTxn: OK) and every Produce
// frame at once (ProduceResponse OK).  The only cross-thread traffic is the producer ring (app -> duty) and the
// Completions ring (duty -> app); the duty thread stops by itself once it has acknowledged every record and
// answered every EndTxn (counts fixed before it starts), so the app joins it - nothing is shared, nobody sleeps.
//
// Per-record latency = the app-side call(s) for one record including any back-pressure retry (steady_clock
// around each op; an 8-record op counts op_ns / 8 for each of its records); the commit wait is not inside it.
// records/s = steady records / (first steady op -> the duty thread's last ack), commits included.  Allocations
// = operator new on the app thread over its steady ops PLUS on the duty thread from the moment it has
// acknowledged the warm-up records until its last pass (each thread counts its own: the duty side may lag the
// app by a whole ring, so one shared window would misattribute its warm-up).  Warm-up = 10 transactions' worth.
// Prints, per path and mode:
//   PRODUCER_TXN path=<p> mode=<plain|txn> p50_ns=.. p99_ns=.. recs_per_s=.. allocs=.. allocs_per_rec=..
// and per path `PRODUCER_TXN_RATIO path=<p> txn_over_plain_p50=..` (the 2x budget is reported, not gated).
// Exit 1 if any steady phase allocated (FD4: zero allocation on every path, transactional or not).
// Env: TXN_RECORDS (records per run, a multiple of 1000; default 200000), TXN_PATHS (comma list; default all),
//      TXN_MODES (plain,txn).
#include <filesystem>
#include "../tests/support/AllocCounter.h"
#include "../tests/support/FakeCoordinator.h"
#include "gen/position.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerHub.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/client/producer/ProducerRef.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace rune::client;
namespace rp  = rune::client::producer;
namespace rpc = rune::protocol::client;
using rp::ProducerImpl;
using rune::gen::Position;
using clk = std::chrono::steady_clock;

namespace {

constexpr int64_t kTxnRecords  = 1000;  // records per transaction
constexpr int64_t kWarmTxns    = 10;    // warm-up transactions (excluded from latency and allocations)
constexpr int32_t kPartitions  = 8;
constexpr uint32_t kValueLen   = 64;
constexpr int64_t kDutyLimitNs = 300'000'000'000LL;  // a wedged run fails loudly instead of hanging

int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::atoll(v) : d;
}
std::string envStr(const char* n, const char* d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::string(v) : std::string(d);
}
int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count();
}
[[noreturn]] void die(const char* what) {
    std::fprintf(stderr, "PRODUCER_TXN FAIL: %s\n", what);
    std::_Exit(2);
}

enum class Path : uint8_t { Send, TrySendCq, Batch8, TryClaim, ClaimBatch8, TypedClaim };
constexpr std::array<Path, 6> kPaths{Path::Send, Path::TrySendCq, Path::Batch8, Path::TryClaim, Path::ClaimBatch8,
                                     Path::TypedClaim};
const char* pathName(Path p) {
    switch (p) {
    case Path::Send:        return "send";
    case Path::TrySendCq:   return "trysend_cq";
    case Path::Batch8:      return "batch8";
    case Path::TryClaim:    return "tryclaim";
    case Path::ClaimBatch8: return "claimbatch8";
    case Path::TypedClaim:  return "typed_claim";
    }
    return "?";
}
uint32_t recordsPerOp(Path p) { return (p == Path::Batch8 || p == Path::ClaimBatch8) ? 8u : 1u; }

// ---- the Completions seam of a bare ProducerImpl -------------------------------------------------------------
// The producer's per-record CQ completions arrive through Hooks::cqDeliver(hooks.ctx = the Client: the identity
// Completions::client() is checked against).  The bench has ONE Completions queue; its address is written here
// once, before any producer or thread exists, and only read afterwards.
Completions* g_cq = nullptr;

void deliverRecord(void*, CqRef, const Completion& c) noexcept { (void)g_cq->ring().deliver(c, {}); }
void deliverOp(void*, const rune::client::detail::OpTarget& t, const Completion& c,
               std::span<const uint8_t> payload) noexcept {
    if (t.handler.fn != nullptr) {
        Completion done = c;
        done.payloadLen = static_cast<uint32_t>(payload.size());
        t.handler(done, payload);
        return;
    }
    Completion done   = c;
    done.reservedBytes = t.reservedBytes;
    (void)g_cq->ring().deliver(done, payload);
}

// ---- the duty thread: a ProducerHub + an at-once coordinator / broker -----------------------------------------
struct Duty {
    struct Req { int64_t corrId; uint16_t templateId; };
    struct Batch { int64_t corrId; int32_t records; };
    ProducerImpl*          impl;
    int64_t                warmRecords;   // count allocations once this many records are acknowledged ...
    int64_t                records;       // ... acknowledge this many records ...
    int64_t                ends;          // ... and answer this many EndTxn, then stop
    std::array<Req, 64>    reqs{};
    std::size_t            nReqs = 0;
    std::array<Batch, 4096> batches{};
    std::size_t            nBatches = 0;
    int64_t                acked = 0;
    int64_t                endsAnswered = 0;
    int64_t                lastAckNs = 0;   // read by the app after join()
    long                   allocs = 0;      // the duty thread's steady allocations (read after join())

    static rp::CoordinatorSink::Offer offer(void* ctx, const uint8_t* b, std::size_t n) noexcept {
        auto* d = static_cast<Duty*>(ctx);
        if (d->nReqs == d->reqs.size()) return rp::CoordinatorSink::Offer::Retry;
        const auto h = rpc::MessageHeader::decode(b, n);
        if (h) d->reqs[d->nReqs++] = Req{h->correlationId, h->templateId};
        return rp::CoordinatorSink::Offer::Accepted;
    }
    static int64_t session(void*) noexcept { return 77; }
    static bool    rediscover(void*) noexcept { return true; }   // #376: CoordinatorSink::rediscover returns bool now (the bench did not build)

    void run() {
        rp::ProducerHub hub;
        hub.setCoordinatorSink(rp::CoordinatorSink{&Duty::offer, &Duty::session, this, &Duty::rediscover});
        hub.add(impl);
        const ProducerImpl::SendFn sink = [this](int32_t, int32_t, int32_t, const uint8_t* b, std::size_t n) {
            if (nBatches == batches.size()) return false;   // transport back-pressure: re-offered next pass
            const auto h = rpc::MessageHeader::decode(b, n);
            const auto v = codec::decodeProduce(b, n);
            if (!h || !v || v->recordBatch.size() < 61) die("undecodable Produce frame");
            int32_t recs = 0;
            std::memcpy(&recs, v->recordBatch.data() + 57, sizeof recs);   // RecordBatch.recordCount
            batches[nBatches++] = Batch{h->correlationId, recs};
            return true;
        };
        const int64_t deadline = nowNs() + kDutyLimitNs;
        int64_t off = 0;
        bool counting = false;
        for (;;) {
            if (!counting && acked >= warmRecords) {
                alloc_counter::startThisThread();
                counting = true;
            }
            const int64_t now = nowNs();
            hub.drainAndFlush(now, 42, sink);
            for (std::size_t i = 0; i < nReqs; ++i) {
                const Req& r = reqs[i];
                rp::ReqKind kind;
                switch (r.templateId) {
                case rpc::InitProducerId::TEMPLATE_ID:     kind = rp::ReqKind::InitProducerId; break;
                case rpc::AddPartitionsToTxn::TEMPLATE_ID: kind = rp::ReqKind::AddPartitions; break;
                case rpc::AddOffsetsToTxn::TEMPLATE_ID:    kind = rp::ReqKind::AddOffsets; break;
                case rpc::TxnOffsetCommit::TEMPLATE_ID:    kind = rp::ReqKind::TxnOffsetCommit; break;
                case rpc::EndTxn::TEMPLATE_ID:             kind = rp::ReqKind::EndTxn; ++endsAnswered; break;
                default: die("unexpected coordinator request");
                }
                const auto a = test_support::FakeCoordinator::answer(kind, r.corrId, 0);
                (void)hub.onCoordinatorResponse(a.templateId, a.data(), a.len, now);
            }
            nReqs = 0;
            for (std::size_t i = 0; i < nBatches; ++i) {
                hub.onProduceResponse(batches[i].corrId, off, 0);
                off += batches[i].records;
                acked += batches[i].records;
                lastAckNs = now;
            }
            nBatches = 0;
            if (acked >= records && endsAnswered >= ends) break;
            if (now > deadline) die("the duty thread did not see every record acknowledged in time");
        }
        allocs = counting ? alloc_counter::stopThisThread() : -1;
        hub.remove(impl);
    }
};

template <class Enc>
void encodeInto(Enc& e, int64_t i) {
    e.zone(static_cast<int32_t>(i & 7)).x(1.5).y(2.5).z(-3.5).seq(static_cast<uint32_t>(i))
        .tick(static_cast<uint64_t>(i)).alive(true).kind(1).tag("bench");
}

struct Result {
    int64_t p50 = 0, p99 = 0;
    double  recsPerS = 0;
    long    allocs = 0;       // app + duty
    long    appAllocs = 0;
    long    dutyAllocs = 0;
    int64_t steadyRecords = 0;
};

ProducerConfig cfgFor(bool txn) {
    ProducerConfig c;
    c.acks              = Acks::All;
    c.lingerUs          = 0;
    c.batchRecords      = 64;
    c.maxInFlight       = 5;
    c.deliveryTimeoutMs = 600'000;
    c.produceTimeoutMs  = 600'000;
    if (txn) {
        c.transactionalId        = "bench-txn";
        c.transactionOpTimeoutMs = 600'000;
        c.transactionTimeoutMs   = 600'000;
    }
    return c;
}

Result runOne(Client& client, Completions& cq, Path path, bool txn, int64_t N) {
    auto* raw = new ProducerImpl(cfgFor(txn), 4 * 1024 * 1024);
    ProducerImpl::Hooks hk;
    hk.ctx       = &client;
    hk.cqDeliver = &deliverRecord;
    hk.sink      = rune::client::detail::CompletionSink{&deliverOp, nullptr};
    raw->setHooks(hk);
    Producer p(raw);   // a bare impl: the handle owns it
    TypedTopic<Position, ProducerRef> typed(ProducerRef{p}, TopicHandle{0, kPartitions}, uint16_t{1});
    const TopicHandle topic{0, kPartitions};

    const int64_t txns = N / kTxnRecords;
    Duty duty{raw, kWarmTxns * kTxnRecords, N, txn ? txns : 0};
    std::thread dutyThread([&duty] { duty.run(); });

    // Completions of the CQ forms: one counter per kind (app thread only).
    struct Seen { int64_t records = 0; int64_t ops = 0; OpStatus last = OpStatus::Ok; } seen;
    auto pollCq = [&] {
        (void)cq.poll([&seen](const Completion& c, std::span<const uint8_t>) {
            if (c.op == OpKind::Send) {
                ++seen.records;
                if (c.status != OpStatus::Ok) die("a record completed with a failure");
            } else {
                ++seen.ops;
                seen.last = c.status;
            }
        });
    };
    Token opToken = 0;
    auto txnOpAndWait = [&](auto issue) {
        const int64_t want = seen.ops + 1;
        for (;;) {
            const OpResult r = issue(++opToken);
            if (r == OpResult::Accepted) break;
            if (r != OpResult::BackPressured && r != OpResult::QueueFull) die("a transaction op was refused");
            pollCq();
            std::this_thread::yield();
        }
        while (seen.ops < want) {
            pollCq();
            std::this_thread::yield();
        }
        if (seen.last != OpStatus::Ok) die("a transaction op did not complete Ok");
    };
    if (txn) txnOpAndWait([&](Token t) { return p.initTransactions(cq, t); });

    const std::vector<uint8_t> val(kValueLen, 0xAB);
    auto relieve = [&](SendResult r) {
        if (r != SendResult::BackPressured && r != SendResult::QueueFull) die("a produce was refused");
        pollCq();
        std::this_thread::yield();
    };
    const uint32_t per = recordsPerOp(path);
    Token recToken = 0;
    auto produceOne = [&](int64_t i) {   // one op = `per` records
        const Key key(static_cast<uint64_t>(i));
        switch (path) {
        case Path::Send:
            p.send(topic, key, val);
            break;
        case Path::TrySendCq:
            for (;;) {
                const SendResult r = p.trySend(topic, key, val, cq, ++recToken);
                if (r == SendResult::Ok) break;
                relieve(r);
            }
            break;
        case Path::Batch8:
            for (;;) {
                BatchBuilder b = p.batch(topic, key);
                for (uint32_t k = 0; k < 8; ++k) b.add(val);
                const OpResult r = b.send();
                if (r == OpResult::Accepted) break;
                if (r != OpResult::BackPressured) die("a Tier-2 batch was refused");
                relieve(SendResult::BackPressured);
            }
            break;
        case Path::TryClaim:
            for (;;) {
                Claim c = p.tryClaim(topic, key, kValueLen);
                if (c) {
                    std::memset(c.buffer().data(), static_cast<int>(i & 0xFF), kValueLen);
                    if (!c.commit()) die("a claim commit failed");
                    break;
                }
                relieve(c.result());
            }
            break;
        case Path::ClaimBatch8:
            for (;;) {
                BatchClaim c = p.claimBatch(topic, key, 8, kValueLen);
                if (c) {
                    for (uint32_t k = 0; k < 8; ++k) std::memset(c.record(k).data(), static_cast<int>(k), kValueLen);
                    if (!c.commit()) die("a batch claim commit failed");
                    break;
                }
                relieve(c.result());
            }
            break;
        case Path::TypedClaim:
            for (;;) {
                auto w = typed.claim(i);
                if (w) {
                    encodeInto(w, i);
                    if (!w.commit()) die("a typed claim commit failed");
                    break;
                }
                relieve(w.result());
            }
            break;
        }
    };

    const int64_t opsPerTxn  = kTxnRecords / per;
    const int64_t warmOps    = kWarmTxns * opsPerTxn;
    const int64_t totalOps   = txns * opsPerTxn;
    std::vector<int64_t> lat(static_cast<std::size_t>(totalOps - warmOps));
    int64_t tSteady = 0;
    long    allocs  = 0;
    for (int64_t t = 0; t < txns; ++t) {
        if (txn) p.beginTransaction();
        for (int64_t k = 0; k < opsPerTxn; ++k) {
            const int64_t op = t * opsPerTxn + k;
            if (op == warmOps) {
                tSteady = nowNs();
                alloc_counter::startThisThread();
            }
            const int64_t t0 = nowNs();
            produceOne(op);
            const int64_t dt = nowNs() - t0;
            if (op >= warmOps) lat[static_cast<std::size_t>(op - warmOps)] = dt;
            if (path == Path::TrySendCq && (k & 63) == 63) pollCq();   // return the credit (untimed)
        }
        if (txn) txnOpAndWait([&](Token tok) { return p.commitTransaction(cq, tok); });
    }
    allocs = alloc_counter::stopThisThread();
    dutyThread.join();   // it stops once every record is acknowledged and every EndTxn answered
    while (path == Path::TrySendCq && seen.records < N) pollCq();   // already delivered: the duty thread is done

    Result res;
    res.appAllocs     = allocs;
    res.dutyAllocs    = duty.allocs;
    res.allocs        = allocs + (duty.allocs < 0 ? 0 : duty.allocs);
    if (duty.allocs < 0) die("the duty thread never reached the steady phase");
    res.steadyRecords = (totalOps - warmOps) * per;
    res.recsPerS      = static_cast<double>(res.steadyRecords) * 1e9 / static_cast<double>(duty.lastAckNs - tSteady);
    auto pct = [&](double q) {
        const auto k = static_cast<std::size_t>(q * static_cast<double>(lat.size() - 1));
        std::nth_element(lat.begin(), lat.begin() + static_cast<std::ptrdiff_t>(k), lat.end());
        return lat[k] / static_cast<int64_t>(per);
    };
    res.p50 = pct(0.50);
    res.p99 = pct(0.99);
    return res;
}

}  // namespace

int main() {
    const int64_t N = envI64("TXN_RECORDS", 200'000);
    if (N % kTxnRecords != 0 || N / kTxnRecords <= kWarmTxns) {
        std::fprintf(stderr, "TXN_RECORDS must be a multiple of %lld above %lld\n", static_cast<long long>(kTxnRecords),
                     static_cast<long long>(kWarmTxns * kTxnRecords));
        return 2;
    }
    const std::string paths = "," + envStr("TXN_PATHS", "send,trysend_cq,batch8,tryclaim,claimbatch8,typed_claim") + ",";
    const std::string modes = "," + envStr("TXN_MODES", "plain,txn") + ",";

    // An unconnected Invoker Client: only the identity the Completions queue is registered with.
    ClientConfig ccfg;
    ccfg.gatewayChannel  = "aeron:ipc";
    ccfg.gatewayStreamId = 4711;
    ccfg.egressChannel   = "aeron:ipc";
    ccfg.egressStreamId  = 4712;
    ccfg.dataDir         = std::filesystem::temp_directory_path().string() + "/rune_producer_txn_bench_" + std::to_string(::getpid());
    ccfg.runnerMode      = RunnerMode::Invoker;
    Client client(ccfg);
    CompletionsConfig qcfg;
    qcfg.capacityBytes  = 1u << 22;
    qcfg.maxResultBytes = 64;
    qcfg.maxErrorText   = 256;
    Completions cq(client, qcfg);
    g_cq = &cq;

    int rc = 0;
    for (const Path path : kPaths) {
        if (paths.find("," + std::string(pathName(path)) + ",") == std::string::npos) continue;
        Result r[2];
        bool   ran[2] = {false, false};
        for (int m = 0; m < 2; ++m) {
            const bool txn = m == 1;
            if (modes.find(txn ? ",txn," : ",plain,") == std::string::npos) continue;
            r[m]   = runOne(client, cq, path, txn, N);
            ran[m] = true;
            std::printf("PRODUCER_TXN path=%s mode=%s p50_ns=%lld p99_ns=%lld recs_per_s=%.0f allocs=%ld "
                        "allocs_per_rec=%.6f app_allocs=%ld duty_allocs=%ld records=%lld\n",
                        pathName(path), txn ? "txn" : "plain", static_cast<long long>(r[m].p50),
                        static_cast<long long>(r[m].p99), r[m].recsPerS, r[m].allocs,
                        static_cast<double>(r[m].allocs) / static_cast<double>(r[m].steadyRecords),
                        r[m].appAllocs, r[m].dutyAllocs, static_cast<long long>(r[m].steadyRecords));
            std::fflush(stdout);
            if (r[m].allocs != 0) {
                std::fprintf(stderr, "GATE FAIL: path=%s mode=%s allocated %ld times in steady state (FD4: 0)\n",
                             pathName(path), txn ? "txn" : "plain", r[m].allocs);
                rc = 1;
            }
        }
        if (ran[0] && ran[1]) {
            std::printf("PRODUCER_TXN_RATIO path=%s txn_over_plain_p50=%.2f txn_over_plain_p99=%.2f within_2x=%s\n",
                        pathName(path), static_cast<double>(r[1].p50) / static_cast<double>(std::max<int64_t>(1, r[0].p50)),
                        static_cast<double>(r[1].p99) / static_cast<double>(std::max<int64_t>(1, r[0].p99)),
                        r[1].p50 <= 2 * std::max<int64_t>(1, r[0].p50) ? "yes" : "no");
        }
    }
    g_cq = nullptr;
    return rc;
}
