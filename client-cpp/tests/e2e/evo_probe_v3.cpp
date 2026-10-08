// tests/e2e/evo_probe_v3.cpp — Task 21 real-Aeron schema-evolution e2e, phase 3.
//
// After `rune schema alter tests/e2e/schema_evolution/v3_drop.rql` (DROP COLUMN
// name; chars_evo v2 -> v3, under the default BACKWARD compatibility mode —
// the drop is ACCEPTED): open the topic bound to v3, produce RUNE_RECORDS_V3
// typed v3 records (no `name` field — it no longer exists on the v3 struct,
// a compile-time proof that new records omit it), then subscribe FROM
// EARLIEST — consuming the v1 + v2 + v3 records — and decode each per its OWN
// embedded version.  The v1/v2 groups are decoded with the v1/v2 generated
// structs, which STILL have the `name` member (they were generated before the
// drop): this proves old records still decode the dropped field correctly.
//
// Environment: as evo_probe_v2.cpp, plus RUNE_RECORDS_V3 (default 5).

#include "chars_evo_v1.gen.h"   // rune::gen_v1::CharsEvo — id,userId,name
#include "chars_evo_v2.gen.h"   // rune::gen_v2::CharsEvo — id,userId,name,level
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

struct ExpectedV1 { int64_t id; int64_t userId; std::string name; };
ExpectedV1 makeExpectedV1(int i) {
    return ExpectedV1{10000 + i, static_cast<int64_t>(1 + i), "v1-" + std::to_string(i)};
}
struct ExpectedV2 { int64_t id; int64_t userId; std::string name; int32_t level; };
ExpectedV2 makeExpectedV2(int i) {
    return ExpectedV2{20000 + i, static_cast<int64_t>(1000 + i), "v2-" + std::to_string(i), 100 + i};
}
struct ExpectedV3 { int64_t id; int64_t userId; int32_t level; };
ExpectedV3 makeExpectedV3(int i) {
    return ExpectedV3{30000 + i, static_cast<int64_t>(2000 + i), 200 + i};
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
    const int         n1           = getEnvInt("RUNE_RECORDS_V1", 5);
    const int         n2           = getEnvInt("RUNE_RECORDS_V2", 5);
    const int         n3           = getEnvInt("RUNE_RECORDS_V3", 5);
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 15000);

    if (target.empty())  { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";   return 1; }
    if (egress.empty())  { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";   return 1; }
    if (dataDir.empty()) { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }

    std::cerr << "PROBE v3: target=" << target << " n1=" << n1 << " n2=" << n2
              << " n3=" << n3 << " timeoutMs=" << timeoutMs << "\n";

    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    int rc = 0;
    try {
        Client c(cfg);
        c.onError([](const std::string& m) { std::cerr << "PROBE v3: onError: " << m << "\n"; });

        std::cerr << "PROBE v3: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE v3: connected\n";

        std::cerr << "PROBE v3: openTopic<CharsEvo>(v3)...\n";
        std::optional<TypedTopic<rune::gen_v3::CharsEvo>> topicOpt;
        try {
            topicOpt.emplace(c.openTopicAndWait<rune::gen_v3::CharsEvo>(
                "chars_evo", std::chrono::milliseconds(timeoutMs)));
        } catch (const std::exception& e) {
            std::cerr << "PROBE v3: FAIL openTopic: " << e.what() << "\n";
            c.close();
            return 4;
        }
        TypedTopic<rune::gen_v3::CharsEvo>& topic = *topicOpt;
        std::cerr << "PROBE v3: openTopic OK boundVersion=" << topic.boundVersion() << "\n";
        if (topic.boundVersion() != 3) {
            std::cerr << "PROBE v3: FAIL boundVersion=" << topic.boundVersion() << " (want 3)\n";
            c.close();
            return 5;
        }

        int32_t topicId = -1;
        {
            TopicSchemaResult sr;
            try {
                sr = c.getTopicSchemaAndWait("chars_evo", std::chrono::milliseconds(timeoutMs));
            } catch (const std::exception& e) {
                std::cerr << "PROBE v3: FAIL getTopicSchema: " << e.what() << "\n";
                c.close();
                return 4;
            }
            if (sr.errorCode != 0 || sr.topicId < 0) {
                std::cerr << "PROBE v3: FAIL getTopicSchema errorCode=" << sr.errorCode << "\n";
                c.close();
                return 4;
            }
            topicId = sr.topicId;
        }
        const TopicHandle th{topicId, /*partitionCount=*/1};

        std::cerr << "PROBE v3: producing " << n3 << " typed v3 records (no 'name')...\n";
        for (int i = 0; i < n3; ++i) {
            ExpectedV3 e = makeExpectedV3(i);
            rune::gen_v3::CharsEvo rec;
            rec.id     = e.id;
            rec.userId = e.userId;
            rec.level  = e.level;
            topic.send(rec);
        }

        const int total = n1 + n2 + n3;
        std::cerr << "PROBE v3: subscribing from earliest (expect " << total << " total)...\n";
        std::mutex mtx;
        std::vector<std::vector<uint8_t>> got;
        got.reserve(static_cast<std::size_t>(total));

        SubscribeOptions opts;
        opts.from      = From::earliest();
        opts.isolation = Isolation::ReadUncommitted;

        auto sub = c.subscribe(th, /*partition=*/0, opts, [&](const Record& r) {
            std::lock_guard<std::mutex> lk(mtx);
            got.emplace_back(r.value.begin(), r.value.end());
        });

        {
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(timeoutMs * 3);
            while (std::chrono::steady_clock::now() < dl) {
                { std::lock_guard<std::mutex> lk(mtx); if (static_cast<int>(got.size()) >= total) break; }
                std::this_thread::sleep_for(10ms);
            }
        }
        sub.unsubscribe();

        std::lock_guard<std::mutex> lk(mtx);
        if (static_cast<int>(got.size()) < total) {
            std::cerr << "PROBE v3: FAIL consumed " << got.size() << " / " << total << " records\n";
            c.close();
            return 6;
        }

        // Group 1 [0,n1): v1 records — "name" (the field about to be/already
        // dropped) must STILL decode correctly via the pre-drop v1 struct.
        for (int i = 0; i < n1; ++i) {
            const auto& raw = got[static_cast<std::size_t>(i)];
            if (rawVersion(raw) != 1) {
                std::cerr << "PROBE v3: FAIL record " << i << " version!=1\n";
                c.close();
                return 8;
            }
            rune::gen_v1::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV1 exp = makeExpectedV1(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name) {
                std::cerr << "PROBE v3: FAIL v1-group (dropped-field decode) mismatch at i=" << i << "\n";
                c.close();
                return 9;
            }
        }
        // Group 2 [n1,n1+n2): v2 records — "name" AND "level" still present.
        for (int i = 0; i < n2; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + i)];
            if (rawVersion(raw) != 2) {
                std::cerr << "PROBE v3: FAIL record " << (n1 + i) << " version!=2\n";
                c.close();
                return 10;
            }
            rune::gen_v2::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV2 exp = makeExpectedV2(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name ||
                d.level != exp.level) {
                std::cerr << "PROBE v3: FAIL v2-group (dropped-field decode) mismatch at i=" << i << "\n";
                c.close();
                return 11;
            }
        }
        // Group 3 [n1+n2,total): v3 records — no "name" member at all (the
        // gen_v3::CharsEvo struct simply has none; this loop only reads
        // id/userId/level, which is itself the "new records omit it" proof).
        for (int i = 0; i < n3; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + n2 + i)];
            if (rawVersion(raw) != 3) {
                std::cerr << "PROBE v3: FAIL record " << (n1 + n2 + i) << " version!=3\n";
                c.close();
                return 12;
            }
            rune::gen_v3::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV3 exp = makeExpectedV3(i);
            if (d.id != exp.id || d.userId != exp.userId || d.level != exp.level) {
                std::cerr << "PROBE v3: FAIL v3-group mismatch at i=" << i << "\n";
                c.close();
                return 13;
            }
        }

        std::cerr << "PROBE v3: round-trip OK — v1(" << n1 << ")+v2(" << n2 << ") dropped-field "
                  << "still decodes; v3(" << n3 << ") new records correctly omit it\n";

        std::cout << "PROBE: OK node=" << target << " topicId=" << topicId
                  << " v1=" << n1 << " v2=" << n2 << " v3=" << n3 << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE v3: FAIL exception: " << ex.what() << "\n";
        rc = 20;
    }
    return rc;
}
