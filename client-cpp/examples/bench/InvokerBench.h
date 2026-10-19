// examples/bench/InvokerBench.h — invoker-shard produce bench: arguments (Aeron-free).
//
// rune_produce_throughput with ANY argument runs this bench (InvokerBench.cpp);
// with none it keeps the legacy env-driven agent-mode path.  Spec §5: open-loop
// fixed rate, every record's latency from its SCHEDULED send time into a
// log-linear histogram (no sampling), a saturation mode, an idle check, core
// layout + per-thread CPU split, and the effective configuration printed in the
// RESULT line.  Producer knobs are explicit options (the RUNE_* producer env
// variables are NOT read), so the RESULT line is the whole configuration; the
// one transport env override that still applies (RUNE_CLIENT_AERON_MTU) is
// reflected in the printed mtu.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

namespace rune::bench {

struct BenchArgs {
    enum class Mode { Open, Saturate };
    Mode        mode        = Mode::Open;
    uint64_t    rate        = 100'000;    // --rate: offered records/s (all shards together)
    int64_t     durationS   = 60;         // --duration-s: measurement window
    int64_t     warmupS     = 5;          // --warmup-s: offered but not measured
    int32_t     valueBytes  = 1024;       // --value-bytes
    uint32_t    shards      = 1;          // --shards: invoker shards (one app thread each)
    int32_t     core        = -1;         // --core: shard i pinned to core + i (-1 = no pin)
    int64_t     window      = 100'000;    // --window: saturate mode max outstanding
    uint64_t    records     = 3'000'000;  // --records: saturate mode total
    std::string acks        = "all";      // --acks none|leader|all
    int32_t     maxInFlight = 32;         // --max-in-flight: per-partition in-flight batches
    int64_t     gateP99Us   = 0;          // --gate-p99-us: fail (exit 4) if p99 above (0 = off)
    uint64_t    gateRps     = 0;          // --gate-rps: fail (exit 4) if achieved below (0 = off)
    std::vector<int> idleCheckCores;      // --idle-check-cores 0,1,2,3: each must be < 5% busy
    bool        idleCheckOnly = false;    // --idle-check-only: run the idle check and exit
    // Transport / batching (explicit, printed in RESULT):
    std::size_t mtu         = 1408;       // --mtu: client Aeron MTU (B5: real-network default; 8192 jumbo opt-in)
    int64_t     lingerUs    = 0;          // --linger-us: Invoker linger (0 = end-of-pass batching)
    int32_t     batchBytes  = 64 * 1024;  // --batch-bytes: flush a partition's batch at this size
};

inline constexpr const char* kBenchUsage =
    "usage: rune_produce_throughput [--mode open|saturate] [--rate N] [--duration-s S]\n"
    "         [--warmup-s S] [--value-bytes B] [--shards N] [--core C] [--window W]\n"
    "         [--records N] [--acks none|leader|all] [--max-in-flight N]\n"
    "         [--gate-p99-us U] [--gate-rps R] [--idle-check-cores 0,1,..] [--idle-check-only]\n"
    "         [--mtu BYTES] [--linger-us U] [--batch-bytes B]\n"
    "  env: RUNE_TARGET RUNE_TARGET_STREAM RUNE_EGRESS RUNE_EGRESS_STREAM RUNE_DATA_DIR RUNE_CLIENT_TOPIC_TABLE\n"
    "  (no arguments = legacy env-driven agent-mode bench)\n";

[[nodiscard]] inline std::optional<BenchArgs> parseBenchArgs(int argc, char** argv, std::string& error) {
    BenchArgs a;
    for (int i = 1; i < argc; ++i) {
        const std::string k = argv[i];
        auto value = [&](std::string& out) -> bool {
            if (i + 1 >= argc) {
                error = "missing value for " + k;
                return false;
            }
            out = argv[++i];
            return true;
        };
        // allowNegative: only --core (-1 = no pin).
        auto number = [&](long long& out, bool allowNegative = false) -> bool {
            std::string v;
            if (!value(v)) return false;
            char* end = nullptr;
            out = std::strtoll(v.c_str(), &end, 10);
            if (v.empty() || end == nullptr || *end != '\0' || (out < 0 && !allowNegative)) {
                error = "bad number for " + k + ": " + v;
                return false;
            }
            return true;
        };
        long long n = 0;
        std::string s;
        if (k == "--mode") {
            if (!value(s)) return std::nullopt;
            if (s == "open") a.mode = BenchArgs::Mode::Open;
            else if (s == "saturate") a.mode = BenchArgs::Mode::Saturate;
            else { error = "--mode must be open|saturate"; return std::nullopt; }
        } else if (k == "--rate") { if (!number(n)) return std::nullopt; a.rate = static_cast<uint64_t>(n); }
        else if (k == "--duration-s") { if (!number(n)) return std::nullopt; a.durationS = n; }
        else if (k == "--warmup-s") { if (!number(n)) return std::nullopt; a.warmupS = n; }
        else if (k == "--value-bytes") { if (!number(n)) return std::nullopt; a.valueBytes = static_cast<int32_t>(n); }
        else if (k == "--shards") { if (!number(n)) return std::nullopt; a.shards = static_cast<uint32_t>(n); }
        else if (k == "--core") { if (!number(n, true)) return std::nullopt; a.core = static_cast<int32_t>(n); }
        else if (k == "--window") { if (!number(n)) return std::nullopt; a.window = n; }
        else if (k == "--records") { if (!number(n)) return std::nullopt; a.records = static_cast<uint64_t>(n); }
        else if (k == "--max-in-flight") { if (!number(n)) return std::nullopt; a.maxInFlight = static_cast<int32_t>(n); }
        else if (k == "--gate-p99-us") { if (!number(n)) return std::nullopt; a.gateP99Us = n; }
        else if (k == "--gate-rps") { if (!number(n)) return std::nullopt; a.gateRps = static_cast<uint64_t>(n); }
        else if (k == "--mtu") { if (!number(n)) return std::nullopt; a.mtu = static_cast<std::size_t>(n); }
        else if (k == "--linger-us") { if (!number(n)) return std::nullopt; a.lingerUs = n; }
        else if (k == "--batch-bytes") { if (!number(n)) return std::nullopt; a.batchBytes = static_cast<int32_t>(n); }
        else if (k == "--acks") {
            if (!value(s)) return std::nullopt;
            if (s != "none" && s != "leader" && s != "all") { error = "--acks must be none|leader|all"; return std::nullopt; }
            a.acks = s;
        } else if (k == "--idle-check-cores") {
            if (!value(s)) return std::nullopt;
            std::size_t pos = 0;
            while (pos <= s.size()) {
                const std::size_t comma = s.find(',', pos);
                const std::string tok = s.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
                char* end = nullptr;
                const long c = std::strtol(tok.c_str(), &end, 10);
                if (tok.empty() || *end != '\0' || c < 0) { error = "bad --idle-check-cores: " + s; return std::nullopt; }
                a.idleCheckCores.push_back(static_cast<int>(c));
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        } else if (k == "--idle-check-only") {
            a.idleCheckOnly = true;
        } else {
            error = "unknown option " + k;
            return std::nullopt;
        }
    }
    if (a.mode == BenchArgs::Mode::Open && a.rate == 0) { error = "--rate must be > 0"; return std::nullopt; }
    if (a.mode == BenchArgs::Mode::Saturate && (a.window <= 0 || a.records == 0)) {
        error = "--window and --records must be > 0 in saturate mode";
        return std::nullopt;
    }
    if (a.shards == 0) { error = "--shards must be >= 1"; return std::nullopt; }
    if (a.valueBytes <= 0 || a.valueBytes > 16368) { error = "--value-bytes must be in [1, 16368]"; return std::nullopt; }
    if (a.maxInFlight <= 0) { error = "--max-in-flight must be > 0"; return std::nullopt; }
    if (a.batchBytes <= 0) { error = "--batch-bytes must be > 0"; return std::nullopt; }
    if (a.mtu < 1024 || a.mtu > 65504 || a.mtu % 32 != 0) {
        error = "--mtu must be a multiple of 32 in [1024, 65504]";
        return std::nullopt;
    }
    if (a.durationS <= 0 && a.mode == BenchArgs::Mode::Open) { error = "--duration-s must be > 0"; return std::nullopt; }
    return a;
}

/// Runs the invoker-shard bench (see kBenchUsage).  Exit codes: 0 ok, 1 run error,
/// 2 usage, 3 box not idle, 4 gate failed.
int invokerBenchMain(int argc, char** argv);

} // namespace rune::bench
