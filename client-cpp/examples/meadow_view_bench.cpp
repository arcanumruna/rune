// examples/meadow_view_bench.cpp - open-loop view-update bench shaped like meadow-server's Rune use.
//
// Topics and views (created by QL before the run; the .gen.h headers come from `rune schema generate`, -DMEADOW_GEN_DIR):
//   meadow_positions  (4 partitions) -> meadow_position      replace view, key id
//   meadow_characters (1 partition)  -> meadow_character     replace view, key id
//   meadow_gold       (1 partition)  -> meadow_gold_balance  sum(delta) group by characterId
//
// One thread, one Producer, acks All | View: an ack means every view folded the record.  Records are scheduled at
// --rate (open loop, latency from the scheduled send time) or kept --window in flight (--mode saturate).  The mix per
// 20 records: 16 position updates (upsert of a random player), 3 gold appends (upsert, random entryId), 1 character
// create (insert of a new id, or key 0 with --auto-id 1: the broker assigns it).  --mix positions|gold|create runs one kind only.  The thread never spins: it sleeps
// until the next scheduled send when there is nothing to poll.
//
// Prints one line: RESULT mode=... rate=... achieved_rps=... p50_us=... p99_us=... p999_us=... max_us=... errors=...
#include "ExampleAuth.h"
#include "meadow_characters.gen.h"
#include "meadow_gold.gen.h"
#include "meadow_positions.gen.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

std::string envStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : def;
}

std::map<std::string, std::string> parseArgs(int argc, char** argv) {
    std::map<std::string, std::string> a;
    for (int i = 1; i + 1 < argc; i += 2) a[argv[i]] = argv[i + 1];
    return a;
}

int64_t nowNs() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }

}  // namespace

int main(int argc, char** argv) {
    auto args = parseArgs(argc, argv);
    auto num = [&](const char* k, long long def) { return args.count(k) ? std::atoll(args[k].c_str()) : def; };
    const std::string mode = args.count("--mode") ? args["--mode"] : "open";
    const std::string mix  = args.count("--mix") ? args["--mix"] : "meadow";
    const long long rate     = num("--rate", 20000);
    const long long durS     = num("--duration-s", 8);
    const long long warmS    = num("--warmup-s", 2);
    const long long window   = num("--window", 4096);
    const long long players  = num("--players", 10000);
    const uint64_t  charBase = static_cast<uint64_t>(num("--char-base", 1'000'000'000));
    const bool      autoId   = num("--auto-id", 0) != 0;   // creates send key 0: the broker assigns the id (autoincrement topic)

    ClientConfig cfg;
    cfg.gatewayChannel   = envStr("RUNE_TARGET");
    cfg.egressChannel    = envStr("RUNE_EGRESS");
    cfg.dataDir          = envStr("RUNE_DATA_DIR");
    cfg.connectTimeoutMs = 30000;
    cfg.pingTimeoutMs    = 30000;
    rune::examples::applyEnvCredentials(cfg);
    Client c(cfg);
    c.connectAndWait();

    ProducerConfig pc;
    pc.acks     = Acks::All | Acks::View;
    pc.lingerUs = 0;
    Producer p = c.producer(pc);
    auto pos  = c.openTopicAndWait<rune::gen::MeadowPositions>("meadow_positions", p, 60s);
    auto gold = c.openTopicAndWait<rune::gen::MeadowGold>("meadow_gold", p, 60s);
    auto chr  = c.openTopicAndWait<rune::gen::MeadowCharacters>("meadow_characters", p, 60s);
    Completions cq(c);

    std::mt19937_64 rng(42);
    const auto kindOf = [&](uint64_t i) {
        if (mix == "positions") return 0;
        if (mix == "gold") return 1;
        if (mix == "create") return 2;
        const uint64_t r = i % 20;
        return r == 0 ? 2 : r <= 3 ? 1 : 0;
    };

    const int64_t start     = nowNs() + 100'000'000;
    const int64_t measureAt = start + warmS * 1'000'000'000LL;
    const int64_t endAt     = measureAt + durS * 1'000'000'000LL;
    const int64_t periodNs  = mode == "open" ? 1'000'000'000LL / std::max(1LL, rate) : 0;
    constexpr uint64_t kRing = 1u << 22;
    std::vector<int64_t> sched(kRing);
    std::vector<int64_t> lat;
    lat.reserve(static_cast<size_t>(mode == "open" ? rate * durS + 1024 : 1 << 24));
    uint64_t issued = 0, acked = 0, errors = 0, measured = 0;
    std::map<uint16_t, uint64_t> errByCode;
    uint64_t nextChar = charBase;

    auto onDone = [&](const Completion& done, std::span<const uint8_t>) {
        const int64_t t = nowNs();
        ++acked;
        if (done.status != OpStatus::Ok) {
            ++errors;
            ++errByCode[done.errorCode != 0 ? done.errorCode : static_cast<uint16_t>(10000 + static_cast<int>(done.status))];
            return;
        }
        const int64_t s = sched[done.token & (kRing - 1)];
        if (s >= measureAt && s < endAt) { lat.push_back(t - s); ++measured; }
    };

    auto issue = [&](int64_t when) {
        const Token tok = issued;
        sched[tok & (kRing - 1)] = when;
        const int64_t ts = nowNs();
        switch (kindOf(issued)) {
        case 0: {
            rune::gen::MeadowPositions r{};
            r.id = rng() % static_cast<uint64_t>(players) + 1;
            r.mapId = 1; r.x = static_cast<double>(rng() % 512); r.y = 0; r.z = static_cast<double>(rng() % 512); r.yaw = 1.5;
            r.tick = issued; r.savedAt = ts;
            pos.upsert(r, cq, tok);
            break;
        }
        case 1: {
            rune::gen::MeadowGold r{};
            r.entryId = rng() | 1; r.characterId = rng() % static_cast<uint64_t>(players) + 1;
            r.delta = static_cast<int64_t>(rng() % 100) - 20; r.reason = 2; r.writerId = 7; r.ownerEpoch = 1; r.at = ts;
            gold.upsert(r, cq, tok);
            break;
        }
        default: {
            rune::gen::MeadowCharacters r{};
            const uint64_t n = nextChar++;
            r.id = autoId ? 0 : n;
            std::snprintf(r.name.data(), r.name.size(), "hero%llu", static_cast<unsigned long long>(n % 100000000));
            std::memcpy(r.nameKey.data(), r.name.data(), r.nameKey.size());
            r.createdAt = ts; r.lastLoginAt = ts;
            chr.insert(r, cq, tok);
            break;
        }
        }
        ++issued;
    };

    while (true) {
        const int64_t now = nowNs();
        if (now >= endAt + 2'000'000'000LL || (now >= endAt && acked == issued)) break;
        bool did = false;
        if (now < endAt) {
            if (mode == "open") {
                while (issued < UINT64_MAX && start + static_cast<int64_t>(issued) * periodNs <= now &&
                       issued - acked < kRing / 2) {
                    issue(start + static_cast<int64_t>(issued) * periodNs);
                    did = true;
                }
            } else {
                while (static_cast<long long>(issued - acked) < window) { issue(nowNs()); did = true; }
            }
        }
        if (cq.poll(onDone) > 0) did = true;
        if (!did) {
            const int64_t next = mode == "open" ? start + static_cast<int64_t>(issued) * periodNs : now + 20'000;
            const int64_t wait = std::clamp<int64_t>(next - nowNs(), 0, 20'000);
            if (wait > 0) std::this_thread::sleep_for(std::chrono::nanoseconds(wait));
        }
    }

    std::sort(lat.begin(), lat.end());
    auto pct = [&](double q) { return lat.empty() ? 0.0 : lat[std::min(lat.size() - 1, static_cast<size_t>(q * lat.size()))] / 1000.0; };
    std::string errs;
    for (const auto& [code, n] : errByCode) errs += " err" + std::to_string(code) + "=" + std::to_string(n);
    std::printf("RESULT mode=%s mix=%s rate=%lld window=%lld players=%lld issued=%llu acked=%llu measured=%llu achieved_rps=%.0f "
                "p50_us=%.1f p99_us=%.1f p999_us=%.1f max_us=%.1f errors=%llu%s\n",
                mode.c_str(), mix.c_str(), rate, window, players, static_cast<unsigned long long>(issued),
                static_cast<unsigned long long>(acked), static_cast<unsigned long long>(measured),
                static_cast<double>(measured) / static_cast<double>(durS), pct(0.50), pct(0.99), pct(0.999),
                lat.empty() ? 0.0 : lat.back() / 1000.0, static_cast<unsigned long long>(errors), errs.c_str());
    p.closeImmediately();
    return 0;
}
