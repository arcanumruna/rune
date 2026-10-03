// examples/bench/InvokerBench.cpp — open-loop, coordinated-omission-safe produce
// bench on invoker-mode client shards (spec §5).  Each shard thread IS its shard:
// it schedules records, trySend()s them, runs doWork(), and records every ack into
// a LogLinearHistogram, latency measured from the record's SCHEDULED send time
// (open loop) or its send time (saturation).
//
// Hot loop (per record): no allocation (timestamps rings, keys and the value are
// preallocated), no virtual call / std::function (the ack handler is a lambda
// template), no logging.  Everything printed is printed before the schedule
// starts or after every shard thread joined.
//
// Threads: no atomics, mutexes or futures.  The start barrier is a pair of
// one-record OneToOneRingBuffers per shard (shard -> main "ready", main -> shard
// "start" carrying the common schedule origin); each ShardResult is written
// only by its shard thread and read by main after join().
#include "InvokerBench.h"
#include "BenchEnv.h"
#include "LogLinearHistogram.h"
#include "OpenLoopSchedule.h"
#include "PartitionTimestamps.h"
#include "ProcStat.h"

#include <rune/client/detail/CpuAffinity.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/client/shard/ClientShard.h>
#include <rune/client/transport/TransportConfig.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/protocol/net/rune_gso_bindings.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace rune::bench {
namespace {

namespace rc = rune::client;
namespace rs = rune::client::shard;

int64_t monoNs() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

rc::Key keyFor(int32_t partition, int32_t partitions) {
    for (uint64_t k = 0; k < 10'000'000; ++k) {
        if (rc::Key(k).partition(partitions) == partition) return rc::Key(k);
    }
    throw std::runtime_error("no key maps to partition " + std::to_string(partition));
}

struct ShardResult {
    LogLinearHistogram hist;
    uint64_t    sent = 0, acked = 0, ackedInWindow = 0, errors = 0;
    int64_t     lastAckNs = 0;
    rc::BatchAck firstError{};       // the first failed batch (reported after the run)
    bool        haveFirstError = false;
    int         core = -1;
    double      cpuSeconds = -1.0;
    bool        ok = true;
    std::string error;
};

// Start barrier with no atomics (F8 law: cross-thread state only through rings):
// per shard, two one-record OneToOneRingBuffers — shard -> main "ready" (connected,
// or failed), then main -> shard "start" carrying the common schedule origin.  The
// ring's release/acquire on the record header is the only ordering needed.
struct StartLink {
    static constexpr std::size_t kCapacity = 256;  // pow2; maxMsgLength = 32 >= 8
    static constexpr std::size_t kBytes = kCapacity + rc::detail::OneToOneRingBuffer::trailerLength();
    alignas(64) std::uint8_t readyBuf[kBytes] = {};
    alignas(64) std::uint8_t startBuf[kBytes] = {};
    rc::detail::OneToOneRingBuffer ready{readyBuf, kBytes};
    rc::detail::OneToOneRingBuffer start{startBuf, kBytes};
};
constexpr std::int32_t kMsgReady = 1;
constexpr std::int32_t kMsgStart = 2;

void signalReady(StartLink& l) {
    if (!l.ready.writeWith(kMsgReady, 0, [](aeron::concurrent::AtomicBuffer&, aeron::util::index_t) {})) {
        std::abort();  // unreachable: one record into an empty ring
    }
}
bool tryReadReady(StartLink& l) {
    return l.ready.read([](std::int32_t, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                           aeron::util::index_t) {},
                        1) == 1;
}
void sendStart(StartLink& l, int64_t startNs) {
    if (!l.start.writeWith(kMsgStart, static_cast<aeron::util::index_t>(sizeof(int64_t)),
                           [&](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off) {
                               b.putInt64(off, startNs);
                           })) {
        std::abort();  // unreachable: one record into an empty ring
    }
}
int64_t tryReadStart(StartLink& l) {  // 0 = not yet
    int64_t v = 0;
    l.start.read([&](std::int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                     aeron::util::index_t) { v = b.getInt64(off); },
                 1);
    return v;
}

// Per-partition timestamp ring capacity: in-flight records of one partition
// (open loop: rate x latency; saturation: --window).  A full ring pauses that
// shard's sends (the records stay due: their latency keeps accruing).
constexpr uint32_t kTimestampSlots = 1u << 18;

void runShard(const BenchArgs& a, const rs::ShardGroupConfig& g, uint32_t index, const std::string& topicName,
              StartLink& link, ShardResult& out) {
    if (a.core >= 0 && !rc::detail::pinCurrentThreadToCore(a.core + static_cast<int>(index))) {
        std::fprintf(stderr, "WARN: could not pin shard %u to core %d\n", index, a.core + static_cast<int>(index));
    }
    bool signaled = false;
    try {
        rs::ClientShard shard(g, index);
        // F8c: Invoker owner — connectAndWait pumps this shard's doWork() itself
        // and throws on any non-Ok outcome or after 10 s.
        shard.connectAndWait(std::chrono::seconds(10));
        const rc::TopicHandle topic = shard.topic(topicName);
        if (!topic.valid()) throw std::runtime_error("unknown topic " + topicName);

        const bool sequenced = a.acks != "none";
        std::vector<int32_t> owned;
        std::vector<rc::Key> keyOf(static_cast<std::size_t>(topic.partitionCount));
        std::vector<std::optional<PartitionTimestamps>> sched(static_cast<std::size_t>(topic.partitionCount));
        for (int32_t p = 0; p < topic.partitionCount; ++p) {
            if (!shard.owns(topic.topicId, p)) continue;
            owned.push_back(p);
            keyOf[static_cast<std::size_t>(p)] = keyFor(p, topic.partitionCount);
            sched[static_cast<std::size_t>(p)].emplace(kTimestampSlots, sequenced);
        }
        if (owned.empty()) throw std::runtime_error("shard owns no partition of " + topicName);
        const std::vector<uint8_t> value(static_cast<std::size_t>(a.valueBytes), uint8_t{0xAB});

        signalReady(link);
        signaled = true;
        int64_t startNs = 0;
        while ((startNs = tryReadStart(link)) == 0) shard.doWork();

        const bool     open      = a.mode == BenchArgs::Mode::Open;
        const uint64_t shardRate = a.rate / a.shards + (index == 0 ? a.rate % a.shards : 0);
        const OpenLoopSchedule schedule(startNs, shardRate);
        const int64_t  warmEnd   = startNs + a.warmupS * 1'000'000'000LL;
        const int64_t  endNs     = warmEnd + a.durationS * 1'000'000'000LL;
        const uint64_t total     = open ? (shardRate == 0 ? 0 : schedule.dueCount(endNs - 1))
                                        : a.records / a.shards + (index == 0 ? a.records % a.shards : 0);
        // The producer fails a record after deliveryTimeoutMs; the drain outlasts
        // that, so a stuck record surfaces as an error ack, not a bench timeout.
        const int64_t  drainDeadline = (open ? endNs : startNs + 600'000'000'000LL) +
                                      g.producer.deliveryTimeoutMs * 1'000'000LL + 10'000'000'000LL;

        std::size_t rr       = 0;
        int64_t     ackNow   = 0;
        bool        lost     = false;  // an ack could not be attributed exactly
        rc::BatchAck lostAck{};
        auto onAck = [&](const rc::BatchAck& ack) {
            out.acked += static_cast<uint64_t>(ack.recordCount);
            out.lastAckNs = ackNow;
            const bool failed = ack.errorCode != 0;
            if (failed) {
                out.errors += static_cast<uint64_t>(ack.recordCount);
                if (!out.haveFirstError) {
                    out.firstError     = ack;
                    out.haveFirstError = true;
                }
            }
            const bool known = ack.partition >= 0 && static_cast<std::size_t>(ack.partition) < sched.size();
            auto&      ring  = sched[known ? static_cast<std::size_t>(ack.partition) : 0];
            const bool attributed =
                known && ring.has_value() &&
                ring->onAck(ack.baseSequence, ack.recordCount, ack.producerEpoch, [&](int64_t t) {
                    if (failed) return;
                    if (t >= warmEnd && (!open || t < endNs)) out.hist.record(ackNow - t);
                    if (ackNow >= warmEnd && ackNow < endNs) ++out.ackedInWindow;
                });
            if (!attributed && !lost) {
                lost    = true;
                lostAck = ack;
            }
        };
        for (;;) {
            const int64_t  now = monoNs();
            // Saturation also starts at the common origin (every shard together;
            // the warm-up window is measured from it).
            const uint64_t due = open ? std::min(total, schedule.dueCount(now)) : (now < startNs ? 0 : total);
            while (out.sent < due && (open || out.sent - out.acked < static_cast<uint64_t>(a.window))) {
                const int32_t p = owned[rr % owned.size()];
                PartitionTimestamps& ring = *sched[static_cast<std::size_t>(p)];
                if (ring.full()) break;
                const rc::SendResult r = shard.trySend(topic, keyOf[static_cast<std::size_t>(p)], value);
                // Stays due (its latency keeps accruing).  QueueFull: the batch-ack
                // queue is out of credit until pollBatchAcks() below drains it.
                if (r == rc::SendResult::BackPressured || r == rc::SendResult::QueueFull) break;
                if (r != rc::SendResult::Ok) {
                    throw std::runtime_error("trySend failed: SendResult=" + std::to_string(static_cast<int>(r)));
                }
                ring.push(open ? schedule.scheduledNs(out.sent) : now);
                ++out.sent;
                ++rr;
            }
            shard.doWork();
            ackNow = monoNs();
            shard.pollBatchAcks(onAck);
            if (lost) {
                throw std::runtime_error(
                    "ack not attributable to sent records (latency attribution lost): partition=" +
                    std::to_string(lostAck.partition) + " baseSequence=" + std::to_string(lostAck.baseSequence) +
                    " recordCount=" + std::to_string(lostAck.recordCount) +
                    " epoch=" + std::to_string(lostAck.producerEpoch) +
                    " errorCode=" + std::to_string(lostAck.errorCode));
            }
            if (out.sent >= total && out.acked >= out.sent) break;
            if (shard.closed()) throw std::runtime_error("shard transport closed");
            if (now > drainDeadline) {
                throw std::runtime_error("drain timeout: sent=" + std::to_string(out.sent) +
                                         " acked=" + std::to_string(out.acked));
            }
        }
        out.cpuSeconds = readThreadSelfCpuSeconds();
        out.core       = rc::detail::currentCore();
        try {
            (void)shard.closeAndWait(std::chrono::seconds(5));  // everything acked: quick, clean teardown
        } catch (const std::exception&) {
            // The measurement is complete; the destructor tears down whatever is left.
        }
    } catch (const std::exception& e) {
        out.ok    = false;
        out.error = e.what();
        if (!signaled) signalReady(link);  // never leave main waiting
    }
    if (out.cpuSeconds < 0) out.cpuSeconds = readThreadSelfCpuSeconds();
    if (out.core < 0) out.core = rc::detail::currentCore();
}

int idleCheck(const BenchArgs& a) {  // 0 = idle (or not checked), 3 = busy
    if (a.idleCheckCores.empty()) return 0;
    const auto before = readPerCpuTimes();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const auto after = readPerCpuTimes();
    const auto busy  = busyFractions(before, after);
    if (busy.empty()) {
        std::printf("IDLE_CHECK unsupported (no /proc/stat)\n");
        return 0;
    }
    bool idle = true;
    std::printf("IDLE_CHECK");
    for (int c : a.idleCheckCores) {
        const double b = static_cast<std::size_t>(c) < busy.size() ? busy[static_cast<std::size_t>(c)] : 1.0;
        std::printf(" cpu%d=%.1f%%", c, b * 100.0);
        if (b > 0.05) idle = false;
    }
    std::printf(idle ? " -> PASS\n" : " -> FAIL\n");
    std::fflush(stdout);
    return idle ? 0 : 3;
}

} // namespace

int invokerBenchMain(int argc, char** argv) {
    std::string err;
    const auto parsed = parseBenchArgs(argc, argv, err);
    if (!parsed) {
        std::cerr << "usage error: " << err << "\n" << kBenchUsage;
        return 2;
    }
    const BenchArgs a = *parsed;

    if (const int rc = idleCheck(a); rc != 0) return rc;
    if (a.idleCheckOnly) return 0;

    const std::string target = envStr("RUNE_TARGET"), egress = envStr("RUNE_EGRESS"), dataDir = envStr("RUNE_DATA_DIR");
    if (target.empty() || egress.empty() || dataDir.empty()) {
        std::cerr << "RUNE_TARGET, RUNE_EGRESS, RUNE_DATA_DIR are required\n" << kBenchUsage;
        return 2;
    }
    rs::ShardGroupConfig g;
    g.base.gatewayChannel  = target;
    g.base.gatewayStreamId = static_cast<int32_t>(envI64("RUNE_TARGET_STREAM", 1));
    g.base.egressChannel   = egress;
    g.base.egressStreamId  = static_cast<int32_t>(envI64("RUNE_EGRESS_STREAM", 2));
    g.base.dataDir         = dataDir;
    g.base.topics          = parseTopics(envStr("RUNE_TOPICS", "bench:0:8"));
    g.base.mtuBytes        = a.mtu;
    if (g.base.topics.empty()) {
        std::cerr << "no topics parsed from RUNE_TOPICS\n";
        return 2;
    }
    g.shardCount           = a.shards;
    g.mode                 = rs::ShardMode::Invoker;
    g.producer.acks        = a.acks == "none" ? rc::Acks::None : a.acks == "leader" ? rc::Acks::Leader : rc::Acks::All;
    g.producer.maxInFlight = a.maxInFlight;
    g.producer.batchBytes  = a.batchBytes;
    g.invokerLingerUs      = a.lingerUs;

    // Effective values (what the shards will actually run with), for the RESULT line.
    rc::transport::TransportConfig tc;
    tc.mtuLength = a.mtu;
    (void)rc::transport::applyEnvOverrides(tc);  // RUNE_CLIENT_AERON_MTU wins, as in the Client
    const int32_t effInFlight =
        a.acks == "none" ? a.maxInFlight
                         : std::min<int32_t>(a.maxInFlight, rune::protocol::client::kIdempotentMaxInFlightBatches);
    const int32_t maxFrame = g.producer.maxFrameBytes;

    std::printf("CORES online=%u affinity=%s shards=%u first_core=%d\n", std::thread::hardware_concurrency(),
                cpuAffinityList().c_str(), a.shards, a.core);
    std::fflush(stdout);

    const auto links = std::make_unique<StartLink[]>(a.shards);  // rings point into themselves: never moved
    std::vector<ShardResult> results(a.shards);
    std::vector<std::thread> threads;
    threads.reserve(a.shards);
    const std::string topicName = g.base.topics[0].name;
    for (uint32_t i = 0; i < a.shards; ++i) {
        threads.emplace_back(runShard, std::cref(a), std::cref(g), i, std::cref(topicName), std::ref(links[i]),
                             std::ref(results[i]));
    }
    // Every shard reports exactly once (connected, or failed — connectAndWait is
    // itself bounded), so this wait needs no deadline of its own.
    std::vector<bool> isReady(a.shards, false);
    for (uint32_t readyCount = 0; readyCount < a.shards;) {
        for (uint32_t i = 0; i < a.shards; ++i) {
            if (!isReady[i] && tryReadReady(links[i])) {
                isReady[i] = true;
                ++readyCount;
            }
        }
        std::this_thread::yield();
    }
    const int64_t origin = monoNs() + 100'000'000LL;  // common schedule origin
    const auto cpuBefore = readPerCpuTimes();
    for (uint32_t i = 0; i < a.shards; ++i) sendStart(links[i], origin);
    for (auto& t : threads) t.join();
    const auto cpuAfter = readPerCpuTimes();

    LogLinearHistogram all;
    uint64_t sent = 0, acked = 0, inWindow = 0, errors = 0;
    int64_t  lastAck = origin;
    bool     ok = true;
    for (uint32_t i = 0; i < a.shards; ++i) {
        const ShardResult& r = results[i];
        all.merge(r.hist);
        sent += r.sent;
        acked += r.acked;
        inWindow += r.ackedInWindow;
        errors += r.errors;
        lastAck = std::max(lastAck, r.lastAckNs);
        std::printf("THREAD shard=%u core=%d cpu_s=%.3f", i, r.core, r.cpuSeconds);
        if (r.haveFirstError) {
            std::printf(" first_failed_batch=partition:%d,seq:%d,records:%d,epoch:%d,errorCode:%d",
                        r.firstError.partition, r.firstError.baseSequence, r.firstError.recordCount,
                        r.firstError.producerEpoch, r.firstError.errorCode);
        }
        std::printf("%s%s\n", r.ok ? "" : " error=", r.ok ? "" : r.error.c_str());
        ok = ok && r.ok;
    }
    const auto busy = busyFractions(cpuBefore, cpuAfter);
    if (!busy.empty()) {
        std::printf("CPU_BUSY");
        for (std::size_t c = 0; c < busy.size(); ++c) std::printf(" cpu%zu=%.1f%%", c, busy[c] * 100.0);
        std::printf("\n");
    }
    // Open loop: records acked inside the measurement window per second (falling
    // behind shows up here).  Saturation: acked records per second from the
    // schedule origin to the last ack.
    const double wallS = static_cast<double>(lastAck - origin) / 1e9;
    const double achieved = a.mode == BenchArgs::Mode::Open
                                ? static_cast<double>(inWindow) / static_cast<double>(a.durationS)
                                : static_cast<double>(acked) / (wallS > 0 ? wallS : 1.0);
    auto us = [](uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    const double p99us = us(all.valueAtQuantile(0.99));
    std::printf("RESULT mode=%s shards=%u core=%d rate=%llu duration_s=%lld warmup_s=%lld window=%lld records=%llu "
                "value=%d acks=%s max_in_flight=%d linger_us=%lld batch_bytes=%d max_frame=%d mtu=%zu "
                "sent=%llu acked=%llu errors=%llu measured=%llu achieved_rps=%.0f "
                "p50_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f gso=%s%s\n",
                a.mode == BenchArgs::Mode::Open ? "open" : "saturate", a.shards, a.core,
                static_cast<unsigned long long>(a.rate), static_cast<long long>(a.durationS),
                static_cast<long long>(a.warmupS), static_cast<long long>(a.window),
                static_cast<unsigned long long>(a.records), a.valueBytes, a.acks.c_str(), effInFlight,
                static_cast<long long>(a.lingerUs), a.batchBytes, maxFrame, tc.mtuLength,
                static_cast<unsigned long long>(sent), static_cast<unsigned long long>(acked),
                static_cast<unsigned long long>(errors), static_cast<unsigned long long>(all.count()), achieved,
                us(all.valueAtQuantile(0.50)), p99us, us(all.valueAtQuantile(0.999)), us(all.max()),
                (tc.udpGso && rune_gso_platform_supported() == 3) ? "on" : "off", ok ? "" : " run_error=1");
    std::fflush(stdout);
    if (!ok || errors != 0) return 1;
    if (a.gateP99Us > 0 || a.gateRps > 0) {
        const bool pass = (a.gateP99Us == 0 || p99us <= static_cast<double>(a.gateP99Us)) &&
                          (a.gateRps == 0 || achieved >= static_cast<double>(a.gateRps));
        std::printf("GATE p99_us<=%lld achieved_rps>=%llu -> %s\n", static_cast<long long>(a.gateP99Us),
                    static_cast<unsigned long long>(a.gateRps), pass ? "PASS" : "FAIL");
        std::fflush(stdout);
        if (!pass) return 4;
    }
    return 0;
}

} // namespace rune::bench
