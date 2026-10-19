// tests/e2e/evo_probe_multi.cpp — GC7 real-Aeron schema-evolution e2e, the
// multi-producer-different-versions + long-lineage-reopen scenario (D12–D16 /
// C51–C55). Runs AFTER the full v1→v2→v3→v4→v5 lineage (phases 1–7 of
// docker/schema-evolution-e2e.sh), i.e. the topic has long since evolved past v3.
//
// Reuses evo_probe_v3's exact mechanism (Client/openTopicAndWait/send/subscribe-
// from-earliest/rawVersion-group), extended to drive TWO producers bound to
// DIFFERENT versions on ONE client, and keyed on UNIQUE sentinel id ranges so it
// is robust to everything the earlier phases already wrote into the log.
//
// What it proves (non-zero exit + "PROBE: FAIL ..." on ANY mismatch):
//
//   (a) Multi-producer-different-versions: a v1-bound producer AND a v3-bound
//       producer both write the SAME topic "chars_evo" after it has evolved to
//       v5. Both send()s must land (we re-read our own sentinel records back).
//
//   (b) Mixed-log decode per WRITER version: the consumer reads the mixed log
//       and decodes each of OUR records with the generated struct matching its
//       OWN embedded wire version — v1 sentinels via gen_v1::CharsEvo (carry the
//       original `name` tag field), v3 sentinels via gen_v3::CharsEvo (reflect
//       the drop/rename lineage, no `name`). No record cross-reads another
//       version's layout: a v1 sentinel id never appears in a v3-stamped record
//       and vice versa.
//
//   (c) Long-lineage reopen: BOTH the v1-bound and the v3-bound openTopic bind
//       to their OWN version (1 and 3) via fingerprint-select — NOT to latest
//       (v5) — after the topic has evolved through the full v1→…→v5 chain.
//
// Environment: as evo_probe_v3.cpp, plus RUNE_RECORDS_MULTI (records per
// producer, default 3). Bounded/time-independent waits everywhere (#16).

#include "../../examples/ExampleAuth.h"
#include "chars_evo_v1.gen.h"   // rune::gen_v1::CharsEvo — id,userId,name
#include "chars_evo_v3.gen.h"   // rune::gen_v3::CharsEvo — id,userId,level (name DROPPED)

#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/subscriber/Subscription.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

namespace {

std::string getEnvStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}
int getEnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::atoi(v) : def;
}

// Unique sentinel id ranges so these records are unmistakable amid everything
// phases 1–7 already wrote (whose ids live in 1xxxx..4xxxx).
constexpr int64_t kV1Base = 910000;
constexpr int64_t kV3Base = 930000;

struct ExpectedV1 { int64_t id; int64_t userId; std::string name; };
ExpectedV1 makeMultiV1(int i) {
    return ExpectedV1{kV1Base + i, static_cast<int64_t>(5000 + i), "multi-v1-" + std::to_string(i)};
}
struct ExpectedV3 { int64_t id; int64_t userId; int32_t level; };
ExpectedV3 makeMultiV3(int i) {
    return ExpectedV3{kV3Base + i, static_cast<int64_t>(7000 + i), 700 + i};
}

uint16_t rawVersion(std::span<const uint8_t> rec) {
    if (rec.size() < 4) return 0;
    return static_cast<uint16_t>(rec[2]) | (static_cast<uint16_t>(rec[3]) << 8);
}

} // namespace

int main() {
    const std::string target       = getEnvStr("RUNE_TARGET");
    const int         targetStream = getEnvInt("RUNE_TARGET_STREAM", 1);
    const std::string egress       = getEnvStr("RUNE_EGRESS");
    const int         egressStream = getEnvInt("RUNE_EGRESS_STREAM", 2);
    const std::string dataDir      = getEnvStr("RUNE_DATA_DIR");
    const int         m            = getEnvInt("RUNE_RECORDS_MULTI", 3);
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 15000);

    if (target.empty())  { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";   return 1; }
    if (egress.empty())  { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";   return 1; }
    if (dataDir.empty()) { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }

    std::cerr << "PROBE multi: target=" << target << " recordsPerProducer=" << m
              << " timeoutMs=" << timeoutMs << "\n";
    std::cerr << "PROBE multi: v1 fingerprint=" << rune::gen_v1::CharsEvo::kFingerprint
              << " v=" << rune::gen_v1::CharsEvo::kVersion
              << " | v3 fingerprint=" << rune::gen_v3::CharsEvo::kFingerprint
              << " v=" << rune::gen_v3::CharsEvo::kVersion << "\n";

    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    rune::examples::applyEnvCredentials(cfg);   // RUNE_USER / RUNE_PASSWORD_FILE
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    int rc = 0;
    try {
        Client c(cfg);
        c.onError([](const std::string& msg) { std::cerr << "PROBE multi: onError: " << msg << "\n"; });

        std::cerr << "PROBE multi: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE multi: connected\n";

        // ---- (c) long-lineage reopen: each producer binds to its OWN version --
        std::cerr << "PROBE multi: openTopic<v1>(\"chars_evo\") after full lineage...\n";
        std::optional<TypedTopic<rune::gen_v1::CharsEvo>> v1Opt;
        std::optional<TypedTopic<rune::gen_v3::CharsEvo>> v3Opt;
        try {
            v1Opt.emplace(c.openTopicAndWait<rune::gen_v1::CharsEvo>(
                "chars_evo", std::chrono::milliseconds(timeoutMs)));
            v3Opt.emplace(c.openTopicAndWait<rune::gen_v3::CharsEvo>(
                "chars_evo", std::chrono::milliseconds(timeoutMs)));
        } catch (const std::exception& e) {
            std::cerr << "PROBE multi: FAIL openTopic: " << e.what() << "\n";
            c.close();
            return 4;
        }
        TypedTopic<rune::gen_v1::CharsEvo>& v1Topic = *v1Opt;
        TypedTopic<rune::gen_v3::CharsEvo>& v3Topic = *v3Opt;
        std::cerr << "PROBE multi: v1 boundVersion=" << v1Topic.boundVersion()
                  << " v3 boundVersion=" << v3Topic.boundVersion() << "\n";
        if (v1Topic.boundVersion() != 1) {
            std::cerr << "PROBE multi: FAIL v1 producer bound version=" << v1Topic.boundVersion()
                      << " (want 1 — fingerprint-select must bind own version, not latest)\n";
            c.close();
            return 5;
        }
        if (v3Topic.boundVersion() != 3) {
            std::cerr << "PROBE multi: FAIL v3 producer bound version=" << v3Topic.boundVersion()
                      << " (want 3 — fingerprint-select must bind own version, not latest)\n";
            c.close();
            return 5;
        }

        int32_t topicId = -1;
        {
            TopicSchemaResult sr;
            try {
                sr = c.getTopicSchemaAndWait("chars_evo", std::chrono::milliseconds(timeoutMs));
            } catch (const std::exception& e) {
                std::cerr << "PROBE multi: FAIL getTopicSchema: " << e.what() << "\n";
                c.close();
                return 4;
            }
            if (sr.errorCode != 0 || sr.topicId < 0) {
                std::cerr << "PROBE multi: FAIL getTopicSchema errorCode=" << sr.errorCode << "\n";
                c.close();
                return 4;
            }
            topicId = sr.topicId;
        }
        const TopicHandle th{topicId, /*partitionCount=*/1};

        // ---- (a) two producers at DIFFERENT versions both write the topic -----
        std::cerr << "PROBE multi: v1-bound producer sending " << m << " records...\n";
        for (int i = 0; i < m; ++i) {
            ExpectedV1 e = makeMultiV1(i);
            rune::gen_v1::CharsEvo rec;
            rec.id     = e.id;
            rec.userId = e.userId;
            rec.name   = e.name;
            v1Topic.send(rec);
        }
        std::cerr << "PROBE multi: v3-bound producer sending " << m << " records...\n";
        for (int i = 0; i < m; ++i) {
            ExpectedV3 e = makeMultiV3(i);
            rune::gen_v3::CharsEvo rec;
            rec.id     = e.id;
            rec.userId = e.userId;
            rec.level  = e.level;
            v3Topic.send(rec);
        }

        // ---- (b) consume the mixed log; decode OUR records per WRITER version --
        std::cerr << "PROBE multi: subscribing from earliest to re-read both producers' records...\n";
        std::mutex mtx;
        std::vector<std::vector<uint8_t>> got;

        SubscribeOptions opts;
        opts.from      = From::earliest();
        opts.isolation = Isolation::ReadUncommitted;

        auto sub = c.subscribe(th, /*partition=*/0, opts, [&](const Record& r) {
            std::lock_guard<std::mutex> lk(mtx);
            got.emplace_back(r.value.begin(), r.value.end());
        });

        // Poll to a deadline until BOTH producers' sentinel records are present.
        std::vector<bool> seenV1(static_cast<std::size_t>(m), false);
        std::vector<bool> seenV3(static_cast<std::size_t>(m), false);
        int foundV1 = 0, foundV3 = 0, crossReads = 0;
        {
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(timeoutMs * 3);
            std::size_t scanned = 0;
            while (std::chrono::steady_clock::now() < dl) {
                std::lock_guard<std::mutex> lk(mtx);
                for (; scanned < got.size(); ++scanned) {
                    const auto& raw = got[scanned];
                    const uint16_t ver = rawVersion(raw);
                    if (ver == 1) {
                        rune::gen_v1::CharsEvo d;
                        d.decode(std::span<const uint8_t>(raw));
                        // A v3 sentinel id must NEVER surface in a v1-stamped record.
                        if (d.id >= kV3Base && d.id < kV3Base + m) { ++crossReads; continue; }
                        if (d.id >= kV1Base && d.id < kV1Base + m) {
                            const int i = static_cast<int>(d.id - kV1Base);
                            ExpectedV1 exp = makeMultiV1(i);
                            if (d.userId == exp.userId && std::string(d.name) == exp.name
                                && !seenV1[static_cast<std::size_t>(i)]) {
                                seenV1[static_cast<std::size_t>(i)] = true;
                                ++foundV1;
                            }
                        }
                    } else if (ver == 3) {
                        rune::gen_v3::CharsEvo d;
                        d.decode(std::span<const uint8_t>(raw));
                        // A v1 sentinel id must NEVER surface in a v3-stamped record.
                        if (d.id >= kV1Base && d.id < kV1Base + m) { ++crossReads; continue; }
                        if (d.id >= kV3Base && d.id < kV3Base + m) {
                            const int i = static_cast<int>(d.id - kV3Base);
                            ExpectedV3 exp = makeMultiV3(i);
                            if (d.userId == exp.userId && d.level == exp.level
                                && !seenV3[static_cast<std::size_t>(i)]) {
                                seenV3[static_cast<std::size_t>(i)] = true;
                                ++foundV3;
                            }
                        }
                    }
                }
                if (foundV1 >= m && foundV3 >= m) break;
                std::this_thread::sleep_for(10ms);
            }
        }
        sub.unsubscribe();

        if (crossReads != 0) {
            std::cerr << "PROBE multi: FAIL cross-version read detected (" << crossReads
                      << "): a sentinel id surfaced under the WRONG writer version\n";
            c.close();
            return 11;
        }
        if (foundV1 < m) {
            std::cerr << "PROBE multi: FAIL v1-producer records: found " << foundV1 << " / " << m
                      << " (v1-bound producer's writes did not all land/decode)\n";
            c.close();
            return 12;
        }
        if (foundV3 < m) {
            std::cerr << "PROBE multi: FAIL v3-producer records: found " << foundV3 << " / " << m
                      << " (v3-bound producer's writes did not all land/decode)\n";
            c.close();
            return 13;
        }

        std::cerr << "PROBE multi: OK — v1-bound(" << foundV1 << ") + v3-bound(" << foundV3
                  << ") producers both wrote chars_evo @ v5; each record decoded per its own"
                  << " writer version; no cross-version read\n";
        std::cout << "PROBE: OK node=" << target << " topicId=" << topicId
                  << " multi v1=" << foundV1 << " v3=" << foundV3
                  << " v1bound=" << v1Topic.boundVersion() << " v3bound=" << v3Topic.boundVersion()
                  << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE multi: FAIL exception: " << ex.what() << "\n";
        rc = 20;
    }
    return rc;
}
