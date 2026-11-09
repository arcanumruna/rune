// examples/lease_e2e_driver.cpp - lease topics Task 19: the Phase 1 end-to-end legs on 3 authenticated brokers on the default
// config (docker/lease-e2e.sh).
//
// One binary, argv[1] selects the subcommand; the script owns the kills, the restarts, the timing and the iptables.  Every subcommand
// prints `LEASE-E2E-<NAME>: OK` on success, or `LEASE-E2E-<NAME>: FAIL: <reason>` (stderr) and exits non-zero.  usage (no argument,
// -h/--help or an unknown subcommand) prints the table below and exits 2 without touching the network.
//
//   setup       --topic T --partitions N --ttl-ms N [--checkpoint 0|1] [--ckpt-interval-ms N] [--min-ttl-ms N] | --topic T --raw 1
//                 defines the lease topic T (rf 3, compacted, descriptor lease_claim; the descriptor name is part of the schema
//                 fingerprint the generated type checks, so a lease topic the typed driver opens is defined here, not by QL), or with
//                 --raw 1 an untyped delete-policy topic of one partition (the GUARD leg's data topic); prints TOPIC_ID=<id>.
//   grant       --topic T --key K [--ttl-ms N] [--expect ok|exists]
//                 one grant; prints `GRANT sent_ms=<realtime before the call> status=<ok|err> err=<code> version=<v>`.
//   ttl         --topic T --key K [--expect live|absent]
//                 one LeaseTimeToLive; prints `TTL live=<0|1> remaining_ms=<n> version=<v>`.
//   scan        --topic T [--expect-present K] [--expect-absent K]
//                 scans every partition of the lease view; prints `LIVE <key> <version>` per row and `COUNT <n>`.
//   hold        --topic T --key K --ttl-ms N --out FILE [--keep-ms N] [--stop-file F] [--max-s N]
//                 grants K and keeps it alive through a LeaseSession until keep-ms passed or F exists (whichever first; both 0 =
//                 until the session loses it); checks the lease is live, then session.remove (NO revoke) and records S.  Appends
//                 `GRANTED <version> <sent_ms> <ack_ms>`, then `STOPPED <S realtime ms>` or `LOST <reason> <ms>` to FILE.
//   watch       --topic T --log FILE --ready FILE --stop-file F [--max-s N] [--ready-live N] [--expect-expired N]
//                 a LeaseWatch from now; every event is one line `EV <kind> <key> <version> <recv_realtime_ms>` in FILE (the receive
//                 time is CLOCK_REALTIME, taken in the handler: Expire records carry timestamp 0).  Writes FILE-ready (READY) once the
//                 initial scan is done and the live set holds ready-live leases.  Ends on F, on expect-expired distinct Expired keys
//                 (plus 2 s to catch duplicates), or at max-s.  Fails on a key Expired twice.
//   scale-hold  --topic T --count N --ttl-ms N --out FILE [--stop-file F] [--max-s N]
//                 grants N leases through 3 LeaseSessions (one session holds at most 4095), writes `HELD <n>` and keeps them alive,
//                 appending `ALIVE lost=<n> atrisk=<n> ok=<n>` every second.
//   guard       --lease-topic L --data-topic D --ttl-ms N --out FILE [--key K] [--ready-file F] [--window N] [--after-ms N]
//                 R5 (spec 12.2): holds a lease, binds a producer to it and sends sequence-numbered records.  After the script cut
//                 the lease leader (onAtRisk Entered at E) it records O (see below), keeps sending for after-ms, then reads the data
//                 topic from offset 0 and checks the log against the send log.
//
// Guard check.  The client has no end-offset query for a data partition (onCaughtUp is the catalog's), so O = 1 + the highest offset
// acknowledged Ok by a completion polled at realtime <= E + d, d = LeaseSessionOptions::maxDelayMs (100).  A record the producer
// appended later than E + d has an offset >= O (its ack, if any, comes after E + d).  A record appended just before E + d whose ack
// is polled after E + d would be reported too: the failure line lists its sequence and ack time.
//
// Environment: RUNE_TARGET, RUNE_EGRESS_HOST + RUNE_EGRESS_PORT, RUNE_DATA_DIR, RUNE_USER / RUNE_PASSWORD_FILE / RUNE_TLS_CA_FILE,
// RUNE_TIMEOUT_MS.  Single-threaded per client: one loop with Client::nowNs() and a 1 ms idle sleep; every loop has a hard time cap.

#include "ExampleAuth.h"
#include "lease_claim.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/lease/LeaseSession.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/lease/LeaseWatch.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std::chrono_literals;
namespace rc = rune::client;
using rune::gen::LeaseClaim;

namespace {

constexpr uint16_t kKeyExists = static_cast<uint16_t>(rune::protocol::client::ErrorCode::KeyExists);

// ---- plumbing ---------------------------------------------------------------------------------------------------------

std::string envStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}
long envInt(const char* name, long def) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::strtol(v, nullptr, 10) : def;
}

/// Wall-clock milliseconds (CLOCK_REALTIME): the one domain shared with the script's `date +%s%3N` (containers share the host kernel).
int64_t realtimeMs() {
    timespec ts{};
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1'000'000;
}

int failWith(const char* leg, const std::string& msg) {
    std::fprintf(stderr, "LEASE-E2E-%s: FAIL: %s\n", leg, msg.c_str());
    std::fflush(stderr);
    return 1;
}
int ok(const char* leg, const std::string& extra = {}) {
    std::printf("LEASE-E2E-%s: OK%s%s\n", leg, extra.empty() ? "" : " ", extra.c_str());
    std::fflush(stdout);
    return 0;
}

void appendLine(const std::string& path, const std::string& line) {
    if (path.empty()) return;
    if (std::FILE* f = std::fopen(path.c_str(), "a")) {
        std::fputs(line.c_str(), f);
        std::fputc('\n', f);
        std::fclose(f);
    }
}
bool fileExists(const std::string& path) { return !path.empty() && std::filesystem::exists(path); }

struct Args {
    std::map<std::string, std::string> kv;
    [[nodiscard]] std::string str(const char* k, const std::string& def = {}) const {
        const auto it = kv.find(k);
        return it == kv.end() ? def : it->second;
    }
    [[nodiscard]] int64_t num(const char* k, int64_t def) const {
        const auto it = kv.find(k);
        return it == kv.end() ? def : std::strtoll(it->second.c_str(), nullptr, 10);
    }
    [[nodiscard]] bool has(const char* k) const { return kv.count(k) != 0; }
};
bool parseArgs(int argc, char** argv, Args& a) {
    for (int i = 2; i < argc; i += 2) {
        if (std::strncmp(argv[i], "--", 2) != 0 || i + 1 >= argc) return false;
        a.kv[argv[i] + 2] = argv[i + 1];
    }
    return true;
}

rc::ClientConfig makeConfig() {
    rc::ClientConfig cfg;
    cfg.gatewayChannel   = envStr("RUNE_TARGET");
    cfg.gatewayStreamId  = static_cast<int32_t>(envInt("RUNE_TARGET_STREAM", 1));
    cfg.egressChannel    = "aeron:udp?endpoint=" + envStr("RUNE_EGRESS_HOST") + ":" + std::to_string(envInt("RUNE_EGRESS_PORT", 40500));
    cfg.egressStreamId   = static_cast<int32_t>(envInt("RUNE_EGRESS_STREAM", 2));
    cfg.dataDir          = envStr("RUNE_DATA_DIR") + "/c0";
    cfg.connectTimeoutMs = static_cast<int>(envInt("RUNE_TIMEOUT_MS", 30000));
    cfg.pingTimeoutMs    = cfg.connectTimeoutMs;
    rune::examples::applyEnvCredentials(cfg);
    return cfg;
}

LeaseClaim claimRow(std::string_view key) {
    LeaseClaim r;
    std::memcpy(r.claim.data(), key.data(), std::min(key.size(), r.claim.size()));
    return r;
}
std::string keyText(std::span<const uint8_t> key) {
    return std::string(reinterpret_cast<const char*>(key.data()), strnlen(reinterpret_cast<const char*>(key.data()), key.size()));
}

std::vector<uint8_t> claimBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("lease_claim")
                                      .addFixedStr("claim", 32, 1, true)
                                      .add("a", rune::codec::PrimType::UInt64)
                                      .addFixedStr("b", 8)
                                      .build());
}

void idle() { std::this_thread::sleep_for(1ms); }

// ---- callbacks (functors: the session takes no std::function) --------------------------------------------------------------

struct LostLog {
    int*         n;
    int64_t*     atMs;
    std::string* reason;
    void operator()(rc::LeaseRef, rc::LostReason r) const {
        if (*n == 0) {
            *atMs   = realtimeMs();
            *reason = std::to_string(static_cast<int>(r));
        }
        ++*n;
    }
};
struct RiskCount {
    int* n;
    void operator()(rc::LeaseRef, rc::AtRiskEdge e) const {
        if (e == rc::AtRiskEdge::Entered) ++*n;
    }
};
struct CountLost {
    uint64_t* n;
    void operator()(rc::LeaseRef, rc::LostReason) const { ++*n; }
};
struct CountRisk {
    uint64_t* n;
    void operator()(rc::LeaseRef, rc::AtRiskEdge e) const {
        if (e == rc::AtRiskEdge::Entered) ++*n;
    }
};

// ---- setup ---------------------------------------------------------------------------------------------------------------

int runSetup(const Args& a) {
    rc::Client c(makeConfig());
    c.connectAndWait();
    rc::TopicOptions to;
    to.replicationFactor = 3;
    if (a.num("raw", 0) != 0) {
        const auto r = c.defineTopicAndWait(a.str("topic"), 1, "", std::span<const uint8_t>{}, rc::Persistence::Disk, to, {}, 60s);
        if (r.errorCode != 0) return failWith("SETUP", "define " + a.str("topic") + ": error " + std::to_string(r.errorCode) + " " + r.detail);
        std::printf("TOPIC_ID=%d\n", r.topicId);
        return ok("SETUP");
    }
    to.config.cleanupPolicy = 1;   // a lease topic is compacted
    rune::protocol::LeaseSpec spec;
    spec.ttlMs                = static_cast<uint64_t>(a.num("ttl-ms", 3000));
    spec.minTtlMs             = static_cast<uint64_t>(a.num("min-ttl-ms", 1000));
    spec.maxTtlMs             = 600'000;
    spec.checkpoint           = static_cast<uint8_t>(a.num("checkpoint", 0));
    spec.checkpointIntervalMs = static_cast<uint32_t>(a.num("ckpt-interval-ms", 300'000));
    to.lease                  = spec;
    const auto r = c.defineTopicAndWait(a.str("topic"), static_cast<uint32_t>(a.num("partitions", 1)), "claim", claimBlob(),
                                        rc::Persistence::Disk, to, {}, 60s);
    if (r.errorCode != 0) return failWith("SETUP", "define " + a.str("topic") + ": error " + std::to_string(r.errorCode) + " " + r.detail);
    std::printf("TOPIC_ID=%d\n", r.topicId);
    return ok("SETUP");
}

// ---- grant / ttl / scan ------------------------------------------------------------------------------------------------

int runGrant(const Args& a) {
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    rc::LeaseOptions o;
    o.ttlMs  = static_cast<uint64_t>(a.num("ttl-ms", 0));
    o.holder = 1;
    const int64_t sentMs = realtimeMs();   // taken BEFORE the grant is sent
    const rc::LeaseResult g = lt.grantAndWait(claimRow(a.str("key")), o);
    std::printf("GRANT sent_ms=%lld status=%s err=%u version=%lld\n", static_cast<long long>(sentMs), g.ok() ? "ok" : "err",
                static_cast<unsigned>(g.errorCode), static_cast<long long>(g.version));
    const std::string expect = a.str("expect", "ok");
    if (expect == "ok" && !g.ok()) return failWith("GRANT", "grant refused: status " + std::to_string(int(g.status)) + " err " + std::to_string(g.errorCode));
    if (expect == "exists" && g.errorCode != kKeyExists)
        return failWith("GRANT", "want KeyExists (" + std::to_string(kKeyExists) + "), got status " + std::to_string(int(g.status)) + " err " + std::to_string(g.errorCode));
    return ok("GRANT");
}

int runTtl(const Args& a) {
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    rc::LeaseTtlInfo info;
    const rc::LeaseResult r = lt.timeToLiveAndWait(claimRow(a.str("key")), &info);
    const bool live = r.ok() && info.remainingMs > 0;
    std::printf("TTL live=%d remaining_ms=%lld version=%lld\n", live ? 1 : 0, static_cast<long long>(info.remainingMs), static_cast<long long>(info.version));
    const std::string expect = a.str("expect");
    if (expect == "live" && !live) return failWith("TTL", "the lease is not live");
    if (expect == "absent" && live) return failWith("TTL", "the lease is live, want absent");
    return ok("TTL");
}

/// Every live lease of the topic: scans each partition of the lease view to its end.
bool scanAll(rc::Client& c, rc::LeaseTopic<LeaseClaim>& lt, std::vector<std::pair<std::string, int64_t>>& out, std::string& why) {
    rc::CompletionsConfig qc;
    qc.maxResultBytes = 1u << 16;
    qc.capacityBytes  = 1u << 20;
    rc::Completions q(c, qc);
    for (int32_t part = 0; part < lt.partitionCount(); ++part) {
        uint64_t resume    = 0;
        bool     firstPage = true;
        bool     done      = false;
        for (int page = 0; page < 4096 && !done; ++page) {
            if (c.scanLeases(lt.leaseViewTopicId(), part, {}, resume, firstPage, 256, 10'000, q, 1) != rc::OpResult::Accepted) {
                why = "scanLeases refused";
                return false;
            }
            rc::Completion cc{};
            bool           got = false;
            bool           rowsOk = true;
            if (q.waitFor(1, 30s, [&](const rc::Completion& comp, std::span<const uint8_t> payload) {
                    cc  = comp;
                    got = true;
                    rowsOk = rc::forEachLeaseRow(payload, [&](std::span<const uint8_t> key, std::span<const uint8_t> row) {
                        const auto d = rc::LeaseTopic<LeaseClaim>::decodeRow(row);
                        out.emplace_back(keyText(key), d ? d->lease_version : -1);
                    });
                }) != rc::Completions::Wait::Done || !got) {
                why = "scan page timed out";
                return false;
            }
            if (cc.status != rc::OpStatus::Ok || !rowsOk) {
                why = "scan page failed: status " + std::to_string(int(cc.status)) + " err " + std::to_string(cc.errorCode);
                return false;
            }
            const rc::LeaseScanPage pg = rc::LeaseScanPage::of(cc);
            done      = pg.done;
            resume    = pg.lastHash;
            firstPage = false;
        }
        if (!done) {
            why = "partition " + std::to_string(part) + " not scanned to its end in 4096 pages";
            return false;
        }
    }
    return true;
}

int runScan(const Args& a) {
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    std::vector<std::pair<std::string, int64_t>> rows;
    std::string                                  why;
    if (!scanAll(c, lt, rows, why)) return failWith("SCAN", why);
    std::set<std::string> keys;
    for (const auto& [k, v] : rows) {
        std::printf("LIVE %s %lld\n", k.c_str(), static_cast<long long>(v));
        keys.insert(k);
    }
    std::printf("COUNT %zu\n", rows.size());
    if (a.has("expect-present") && keys.count(a.str("expect-present")) == 0) return failWith("SCAN", "key " + a.str("expect-present") + " is not in the view");
    if (a.has("expect-absent") && keys.count(a.str("expect-absent")) != 0) return failWith("SCAN", "key " + a.str("expect-absent") + " is still in the view");
    return ok("SCAN");
}

// ---- hold --------------------------------------------------------------------------------------------------------------

int runHold(const Args& a) {
    const std::string out = a.str("out");
    const int64_t     ttl = a.num("ttl-ms", 3000), keepMs = a.num("keep-ms", 0), maxS = a.num("max-s", 300);
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    int         lost = 0, risk = 0;
    int64_t     lostMs = 0;
    std::string lostReason;
    rc::LeaseSession<4, LostLog, RiskCount> session(c, LostLog{&lost, &lostMs, &lostReason}, RiskCount{&risk});

    const LeaseClaim row = claimRow(a.str("key"));
    rc::LeaseOptions o;
    o.ttlMs  = static_cast<uint64_t>(ttl);
    o.holder = 1;
    const int64_t sentNs = c.nowNs(), sentMs = realtimeMs();
    const rc::LeaseResult g = lt.grantAndWait(row, o);
    if (!g.ok()) return failWith("HOLD", "grant of " + a.str("key") + ": status " + std::to_string(int(g.status)) + " err " + std::to_string(g.errorCode));
    rc::OpResult      err = rc::OpResult::Accepted;
    const rc::LeaseRef ref = session.add(lt, row, g.version, static_cast<uint64_t>(ttl), sentNs, &err);
    if (err != rc::OpResult::Accepted) return failWith("HOLD", "session.add refused: " + std::to_string(int(err)));
    appendLine(out, "GRANTED " + std::to_string(g.version) + " " + std::to_string(sentMs) + " " + std::to_string(realtimeMs()));

    const int64_t startMs = realtimeMs();
    const std::string stopFile = a.str("stop-file");
    for (;;) {
        session.doWork(c.nowNs());
        if (lost != 0) {
            appendLine(out, "LOST " + lostReason + " " + std::to_string(lostMs));
            std::printf("HOLD lost reason=%s\n", lostReason.c_str());
            ok("HOLD", "lost");
            std::_Exit(0);   // no destructors: the post-loss best-effort revoke must not run (the script checks the Expired)
        }
        const int64_t el = realtimeMs() - startMs;
        if (keepMs > 0 && el >= keepMs) break;
        if (fileExists(stopFile)) break;
        if (el > maxS * 1000) return failWith("HOLD", "no stop condition within " + std::to_string(maxS) + " s");
        idle();
    }
    rc::LeaseTtlInfo info;
    const rc::LeaseResult live = lt.timeToLiveAndWait(row, &info);
    if (!live.ok() || info.remainingMs <= 0) return failWith("HOLD", "the lease is not live after being kept alive");
    if (session.stateOf(ref) != rc::LeaseEntryState::Held) return failWith("HOLD", "the lease is not Held");
    if (session.remove(ref) != rc::OpResult::Accepted) return failWith("HOLD", "session.remove refused");
    const int64_t s = realtimeMs();   // S: keepalives stop here (no revoke)
    appendLine(out, "STOPPED " + std::to_string(s));
    std::printf("HOLD live_before_stop=1 remaining_ms=%lld S=%lld\n", static_cast<long long>(info.remainingMs), static_cast<long long>(s));
    return ok("HOLD");
}

// ---- watch -------------------------------------------------------------------------------------------------------------

struct WatchCtx {
    std::string                           buf;
    std::unordered_map<std::string, int>  expired;
    uint64_t                              expiredEvents = 0;
    int                                   dup = 0;
    int                                   failed = 0;
    static const char* name(rc::LeaseEventKind k) {
        switch (k) {
        case rc::LeaseEventKind::Granted: return "Granted";
        case rc::LeaseEventKind::Updated: return "Updated";
        case rc::LeaseEventKind::Revoked: return "Revoked";
        case rc::LeaseEventKind::Expired: return "Expired";
        case rc::LeaseEventKind::Vanished: return "Vanished";
        case rc::LeaseEventKind::Ready: return "Ready";
        case rc::LeaseEventKind::Failed: return "Failed";
        }
        return "?";
    }
    static void fn(void* ctx, const rc::LeaseEvent& e) {
        auto&         w   = *static_cast<WatchCtx*>(ctx);
        const int64_t now = realtimeMs();   // the receive time
        const std::string k = keyText(e.key);
        if (e.kind == rc::LeaseEventKind::Expired) {
            ++w.expiredEvents;
            if (++w.expired[k] > 1) ++w.dup;
        }
        if (e.kind == rc::LeaseEventKind::Failed) ++w.failed;
        w.buf += "EV ";
        w.buf += name(e.kind);
        w.buf += ' ';
        w.buf += k;
        w.buf += ' ';
        w.buf += std::to_string(e.version);
        w.buf += ' ';
        w.buf += std::to_string(now);
        w.buf += '\n';
    }
    void flush(std::FILE* f) {
        if (buf.empty()) return;
        std::fwrite(buf.data(), 1, buf.size(), f);
        std::fflush(f);
        buf.clear();
    }
};

int runWatch(const Args& a) {
    const std::string log = a.str("log"), ready = a.str("ready"), stopFile = a.str("stop-file");
    const int64_t     maxS = a.num("max-s", 300), readyLive = a.num("ready-live", 0), expectExpired = a.num("expect-expired", 0);
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    rc::LeaseWatchOptions wo;
    wo.maxLive = 16384;
    wo.maxRows = 256;
    rc::LeaseWatch<LeaseClaim> watch(c, lt, wo);
    WatchCtx ctx;
    watch.setHandler(&WatchCtx::fn, &ctx);
    std::FILE* f = std::fopen(log.c_str(), "a");
    if (f == nullptr) return failWith("WATCH", "cannot open " + log);
    const std::vector<uint8_t>     none;
    const std::span<const uint8_t> prefixes[] = {std::span<const uint8_t>(none)};
    if (watch.startFromNow(prefixes) != rc::OpResult::Accepted) return failWith("WATCH", "startFromNow refused");

    const int64_t startMs = realtimeMs();
    bool          readyWritten = false;
    int64_t       doneAtMs = 0;
    for (;;) {
        watch.doWork(c.nowNs());
        ctx.flush(f);
        if (watch.failed() || ctx.failed != 0) return failWith("WATCH", "the watch failed");
        if (!readyWritten && watch.isReady() && static_cast<int64_t>(watch.liveCount()) >= readyLive) {
            appendLine(ready, "READY live=" + std::to_string(watch.liveCount()));
            readyWritten = true;
        }
        const int64_t now = realtimeMs();
        if (expectExpired > 0 && doneAtMs == 0 && static_cast<int64_t>(ctx.expired.size()) >= expectExpired) doneAtMs = now;
        if (doneAtMs != 0 && now - doneAtMs >= 2000) break;   // 2 s more: a duplicate would show
        if (expectExpired == 0 && fileExists(stopFile)) break;
        if (expectExpired > 0 && fileExists(stopFile) && doneAtMs == 0) break;
        if (now - startMs > maxS * 1000) {
            ctx.flush(f);
            if (expectExpired > 0)
                return failWith("WATCH", "only " + std::to_string(ctx.expired.size()) + " of " + std::to_string(expectExpired) + " Expired within " + std::to_string(maxS) + " s");
            return failWith("WATCH", "no stop file within " + std::to_string(maxS) + " s");
        }
        idle();
    }
    ctx.flush(f);
    std::fclose(f);
    if (ctx.dup != 0) return failWith("WATCH", std::to_string(ctx.dup) + " keys Expired more than once");
    if (expectExpired > 0 && static_cast<int64_t>(ctx.expired.size()) != expectExpired)
        return failWith("WATCH", std::to_string(ctx.expired.size()) + " Expired keys, want " + std::to_string(expectExpired));
    return ok("WATCH", "expired=" + std::to_string(ctx.expired.size()) + " events=" + std::to_string(ctx.expiredEvents));
}

// ---- scale-hold --------------------------------------------------------------------------------------------------------

int runScaleHold(const Args& a) {
    constexpr uint32_t kCap = 3334, kSessions = 3;   // one LeaseSession holds at most 4095 (the slot rides in 12 bits)
    const int64_t      count = a.num("count", 10000), ttl = a.num("ttl-ms", 10000), maxS = a.num("max-s", 900);
    const std::string  out = a.str("out"), stopFile = a.str("stop-file");
    if (count > static_cast<int64_t>(kCap) * kSessions) return failWith("SCALE-HOLD", "count above " + std::to_string(kCap * kSessions));
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt = c.openLeaseTopicAndWait<LeaseClaim>(a.str("topic"), {}, 30s);
    uint64_t lost = 0, risk = 0;
    using Session = rc::LeaseSession<kCap, CountLost, CountRisk>;
    std::vector<std::unique_ptr<Session>> sessions;
    for (uint32_t i = 0; i < kSessions; ++i) sessions.push_back(std::make_unique<Session>(c, CountLost{&lost}, CountRisk{&risk}));
    const auto drive = [&] {
        for (auto& s : sessions) s->doWork(c.nowNs());
    };
    rc::LeaseOptions o;
    o.ttlMs  = static_cast<uint64_t>(ttl);
    o.holder = 1;
    const int64_t startMs = realtimeMs();
    for (int64_t i = 0; i < count; ++i) {
        char key[32];
        std::snprintf(key, sizeof key, "k-%08lld", static_cast<long long>(i));
        const LeaseClaim row = claimRow(key);
        const int64_t    sentNs = c.nowNs();
        const rc::LeaseResult g = lt.grantAndWait(row, o);
        if (!g.ok()) return failWith("SCALE-HOLD", std::string("grant ") + key + ": status " + std::to_string(int(g.status)) + " err " + std::to_string(g.errorCode));
        rc::OpResult err = rc::OpResult::Accepted;
        (void)sessions[static_cast<std::size_t>(i) % kSessions]->add(lt, row, g.version, static_cast<uint64_t>(ttl), sentNs, &err);
        if (err != rc::OpResult::Accepted) return failWith("SCALE-HOLD", std::string("add ") + key + " refused: " + std::to_string(int(err)));
        if (i % 32 == 31) drive();   // keepalives flow while the grants go on
        if (realtimeMs() - startMs > 300'000) return failWith("SCALE-HOLD", "the grants took over 300 s (at " + std::to_string(i) + ")");
    }
    appendLine(out, "HELD " + std::to_string(count));
    std::printf("HELD %lld in %lld ms\n", static_cast<long long>(count), static_cast<long long>(realtimeMs() - startMs));
    std::fflush(stdout);
    int64_t lastStatus = 0;
    for (;;) {
        drive();
        const int64_t now = realtimeMs();
        if (now - lastStatus >= 1000) {
            uint64_t okAns = 0;
            for (auto& s : sessions) okAns += s->core().okAnswers();
            appendLine(out, "ALIVE lost=" + std::to_string(lost) + " atrisk=" + std::to_string(risk) + " ok=" + std::to_string(okAns));
            lastStatus = now;
        }
        if (fileExists(stopFile) || now - startMs > maxS * 1000) break;
        idle();
    }
    for (auto& s : sessions) s->close();   // reached only without a SIGKILL: revoke
    return ok("SCALE-HOLD");
}

// ---- guard -------------------------------------------------------------------------------------------------------------

struct Sent {
    int64_t sentMs      = 0;
    bool    afterEntered = false;
    uint8_t outcome     = 0;   // 0 pending, 1 Ok, 2 LeaseAtRisk, 3 other failure
    int64_t offset      = -1;
    int64_t ackMs       = 0;
    uint16_t status     = 0;
    uint16_t errorCode  = 0;
};
struct GuardRisk {
    int64_t* enteredMs;
    int*     enters;
    void operator()(rc::LeaseRef, rc::AtRiskEdge e) const {
        if (e == rc::AtRiskEdge::Entered) {
            if (*enters == 0) *enteredMs = realtimeMs();
            ++*enters;
        }
    }
};

int runGuard(const Args& a) {
    const std::string out = a.str("out"), readyFile = a.str("ready-file"), key = a.str("key", "guard-1");
    const int64_t     ttl = a.num("ttl-ms", 3000), window = a.num("window", 64), afterMs = a.num("after-ms", 3000);
    const int64_t     dMs = 100;   // LeaseSessionOptions::maxDelayMs (the default)
    rc::Client c(makeConfig());
    c.connectAndWait();
    auto lt   = c.openLeaseTopicAndWait<LeaseClaim>(a.str("lease-topic"), {}, 30s);
    auto data = c.rawTopicAndWait(a.str("data-topic"));
    int         lost = 0, enters = 0;
    int64_t     lostMs = 0, enteredMs = 0;
    std::string lostReason;
    struct LostGuard {
        int* n;
        void operator()(rc::LeaseRef, rc::LostReason) const { ++*n; }
    };
    rc::LeaseSession<2, LostGuard, GuardRisk> session(c, LostGuard{&lost}, GuardRisk{&enteredMs, &enters});

    const LeaseClaim row = claimRow(key);
    rc::LeaseOptions o;
    o.ttlMs  = static_cast<uint64_t>(ttl);
    o.holder = 1;
    const int64_t sentNs = c.nowNs();
    const rc::LeaseResult g = lt.grantAndWait(row, o);
    if (!g.ok()) return failWith("GUARD", "grant: status " + std::to_string(int(g.status)) + " err " + std::to_string(g.errorCode));
    rc::OpResult       err = rc::OpResult::Accepted;
    const rc::LeaseRef ref = session.add(lt, row, g.version, static_cast<uint64_t>(ttl), sentNs, &err);
    if (err != rc::OpResult::Accepted) return failWith("GUARD", "session.add refused: " + std::to_string(int(err)));

    rc::ProducerConfig pc;
    pc.acks              = rc::Acks::All;
    pc.deliveryTimeoutMs = 20'000;
    rc::Producer prod = c.producer(pc);
    // The bind travels the producer's ring (FIFO with its writes): bind BEFORE the first write.  BackPressured until the session's guard block exists.
    rc::OpResult bind = rc::OpResult::BackPressured;
    for (int i = 0; i < 2000 && bind == rc::OpResult::BackPressured; ++i) {
        session.doWork(c.nowNs());
        bind = session.bindProducer(ref, prod);
        if (bind == rc::OpResult::BackPressured) idle();
    }
    if (bind != rc::OpResult::Accepted) return failWith("GUARD", "bindProducer: " + std::to_string(int(bind)));

    rc::Completions q(c);
    std::vector<Sent> sent;
    sent.reserve(400'000);
    int64_t     inflight = 0, nextSendMs = 0, okAcks = 0, maxAckedOffset = -1, oFrozen = -1, frozenAtMs = 0;
    bool        readyWritten = false, oDone = false;
    int64_t     afterDeadlineMs = 0;
    const int64_t startMs = realtimeMs();
    int           badAfterEntered = 0;
    std::string   firstBad;

    const auto handle = [&](const rc::Completion& cc, std::span<const uint8_t>) {
        const std::size_t seq = static_cast<std::size_t>(cc.token);
        if (seq >= sent.size()) return;
        Sent& s = sent[seq];
        --inflight;
        s.ackMs     = realtimeMs();
        s.status    = static_cast<uint16_t>(cc.status);
        s.errorCode = cc.errorCode;
        if (cc.status == rc::OpStatus::Ok) {
            s.outcome = 1;
            s.offset  = cc.value;
            ++okAcks;
            if (!oDone) maxAckedOffset = std::max(maxAckedOffset, cc.value);
        } else if (cc.status == rc::OpStatus::DeliveryFailed && cc.errorCode == static_cast<uint16_t>(rc::ClientErrorCode::LeaseAtRisk)) {
            s.outcome = 2;
        } else {
            s.outcome = 3;
        }
        if (s.afterEntered && s.outcome != 2) {
            ++badAfterEntered;
            if (firstBad.empty()) firstBad = "seq " + std::to_string(seq) + " status " + std::to_string(s.status) + " err " + std::to_string(s.errorCode);
        }
    };

    for (;;) {
        session.doWork(c.nowNs());
        q.poll(handle);
        const int64_t now = realtimeMs();
        if (lost != 0 && enters == 0) return failWith("GUARD", "the lease was lost before Entered");
        // O: frozen at the first pass at or after E + d (acks polled up to now are counted).
        if (enters != 0 && !oDone && now >= enteredMs + dMs) {
            q.poll(handle);
            oFrozen    = maxAckedOffset + 1;
            frozenAtMs = realtimeMs();
            oDone      = true;
            afterDeadlineMs = frozenAtMs + afterMs;
        }
        if (oDone && now >= afterDeadlineMs) break;
        if (!readyWritten && okAcks >= 200) {
            appendLine(readyFile, "READY " + std::to_string(g.version) + " " + std::to_string(realtimeMs()));
            readyWritten = true;
        }
        if (now - startMs > 180'000) return failWith("GUARD", "no Entered within 180 s (ready " + std::string(readyWritten ? "yes" : "no") + ", ok acks " + std::to_string(okAcks) + ")");
        if (inflight < window && now >= nextSendMs && sent.size() < 399'000) {
            const std::size_t seq = sent.size();
            uint8_t           val[16] = {};
            std::memcpy(val, &seq, 8);
            sent.push_back(Sent{now, enters != 0, 0, -1, 0, 0, 0});
            const rc::SendResult r = prod.trySend(data, rc::Key(static_cast<uint64_t>(seq)), std::span<const uint8_t>(val, 16), q, seq);
            if (r == rc::SendResult::Ok) {
                ++inflight;
                nextSendMs = now + 1;
            } else {
                sent.pop_back();
                if (r != rc::SendResult::BackPressured && r != rc::SendResult::QueueFull)
                    return failWith("GUARD", "trySend refused synchronously: " + std::to_string(int(r)) + " (after Entered: " + (enters != 0 ? "yes" : "no") + ")");
            }
        }
        idle();
    }
    // Drain the in-flight sends (each is answered by the delivery timeout at the latest).
    for (int i = 0; i < 30'000 && inflight > 0; ++i) {
        session.doWork(c.nowNs());
        q.poll(handle);
        idle();
    }
    if (inflight != 0) return failWith("GUARD", std::to_string(inflight) + " sends never completed");
    if (badAfterEntered != 0) return failWith("GUARD", std::to_string(badAfterEntered) + " sends submitted after Entered did not complete DeliveryFailed/LeaseAtRisk (first: " + firstBad + ")");

    appendLine(out, "ENTERED_MS=" + std::to_string(enteredMs));
    appendLine(out, "O=" + std::to_string(oFrozen));
    // Read the data topic from offset 0 (the data leader is reachable): seq -> offset.
    std::mutex                        mu;
    std::map<uint64_t, int64_t>       logged;      // seq -> offset
    int64_t                           lastGrowMs = realtimeMs(), maxLogged = -1;
    std::size_t                       nLogged = 0;
    rc::SubscribeOptions              so;
    so.from = rc::From::earliest();
    rc::Subscription sub = c.subscribe(data, 0, so, [&](const rc::Record& r) {
        if (r.value.size() < 8) return;
        uint64_t seq = 0;
        std::memcpy(&seq, r.value.data(), 8);
        std::lock_guard<std::mutex> lk(mu);
        logged[seq] = r.offset;
        maxLogged   = std::max<int64_t>(maxLogged, r.offset);
    });
    const int64_t readStart = realtimeMs();
    for (;;) {
        {
            std::lock_guard<std::mutex> lk(mu);
            if (logged.size() != nLogged) { nLogged = logged.size(); lastGrowMs = realtimeMs(); }
        }
        const int64_t now = realtimeMs();
        if (now - lastGrowMs >= 3000 && now - readStart >= 5000) break;   // quiet for 3 s
        if (now - readStart > 120'000) return failWith("GUARD", "reading the data topic took over 120 s");
        session.doWork(c.nowNs());
        idle();
    }
    sub.unsubscribe();

    std::lock_guard<std::mutex> lk(mu);
    int         late = 0, missing = 0, mismatch = 0, riskLogged = 0;
    std::string detail;
    for (const auto& [seq, off] : logged) {
        if (off >= oFrozen) {
            if (++late <= 5) detail += " late(seq=" + std::to_string(seq) + " off=" + std::to_string(off) + ")";
        }
        if (seq < sent.size() && sent[seq].outcome == 2) {
            if (++riskLogged <= 5) detail += " atrisk-in-log(seq=" + std::to_string(seq) + ")";
        }
    }
    for (std::size_t seq = 0; seq < sent.size(); ++seq) {
        if (sent[seq].outcome != 1) continue;
        const auto it = logged.find(seq);
        if (it == logged.end()) { if (++missing <= 5) detail += " missing(seq=" + std::to_string(seq) + ")"; }
        else if (it->second != sent[seq].offset) { if (++mismatch <= 5) detail += " offset-mismatch(seq=" + std::to_string(seq) + ")"; }
    }
    std::printf("GUARD sends=%zu ok=%lld entered_ms=%lld O=%lld logged=%zu max_offset=%lld late=%d atrisk_in_log=%d missing=%d mismatch=%d\n", sent.size(),
                static_cast<long long>(okAcks), static_cast<long long>(enteredMs), static_cast<long long>(oFrozen), logged.size(),
                static_cast<long long>(maxLogged), late, riskLogged, missing, mismatch);
    if (late != 0) return failWith("GUARD", std::to_string(late) + " records appended at offset >= O (after Entered + d):" + detail);
    if (riskLogged != 0) return failWith("GUARD", std::to_string(riskLogged) + " records that completed LeaseAtRisk are in the log:" + detail);
    if (missing != 0 || mismatch != 0) return failWith("GUARD", "an acknowledged record is missing or at another offset:" + detail);
    return ok("GUARD");
}

int usage() {
    std::fprintf(stderr,
                 "usage: lease_e2e_driver <subcommand> [--flag value ...]\n"
                 "  setup       --topic T --partitions N --ttl-ms N [--checkpoint 0|1] [--ckpt-interval-ms N] [--min-ttl-ms N] | --topic T --raw 1\n"
                 "  grant       --topic T --key K [--ttl-ms N] [--expect ok|exists]\n"
                 "  ttl         --topic T --key K [--expect live|absent]\n"
                 "  scan        --topic T [--expect-present K] [--expect-absent K]\n"
                 "  hold        --topic T --key K --ttl-ms N --out FILE [--keep-ms N] [--stop-file F] [--max-s N]\n"
                 "  watch       --topic T --log FILE --ready FILE --stop-file F [--max-s N] [--ready-live N] [--expect-expired N]\n"
                 "  scale-hold  --topic T --count N --ttl-ms N --out FILE [--stop-file F] [--max-s N]\n"
                 "  guard       --lease-topic L --data-topic D --ttl-ms N --out FILE [--key K] [--ready-file F] [--window N] [--after-ms N]\n"
                 "environment: RUNE_TARGET, RUNE_EGRESS_HOST, RUNE_EGRESS_PORT, RUNE_DATA_DIR, RUNE_USER, RUNE_PASSWORD_FILE, RUNE_TLS_CA_FILE\n");
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) return usage();
    const std::string mode = argv[1];
    Args              a;
    if (mode == "-h" || mode == "--help" || !parseArgs(argc, argv, a)) return usage();
    try {
        if (mode == "setup") return runSetup(a);
        if (mode == "grant") return runGrant(a);
        if (mode == "ttl") return runTtl(a);
        if (mode == "scan") return runScan(a);
        if (mode == "hold") return runHold(a);
        if (mode == "watch") return runWatch(a);
        if (mode == "scale-hold") return runScaleHold(a);
        if (mode == "guard") return runGuard(a);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "LEASE-E2E: %s threw: %s\n", mode.c_str(), e.what());
        return 1;
    }
    return usage();
}
