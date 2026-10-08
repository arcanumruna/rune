// tests/e2e/evo_probe_v2.cpp — Task 21 real-Aeron schema-evolution e2e, phase 2.
//
// After `rune schema alter tests/e2e/schema_evolution/v2_add.rql` (ADD COLUMN
// level int32; chars_evo v1 -> v2): open the topic bound to v2, produce
// RUNE_RECORDS_V2 typed v2 records, then subscribe FROM EARLIEST — consuming
// BOTH the v1 records phase 1 already produced AND these new v2 records —
// and decode EACH record per its OWN embedded version (the u16 LE at bytes
// [2..3] of the record, stamped by TypedTopic::send; see Generator.cpp's
// encode() layout and TypedTopic.h's version-binding comment).
//
// Environment:
//   RUNE_TARGET, RUNE_TARGET_STREAM, RUNE_EGRESS, RUNE_EGRESS_STREAM,
//   RUNE_DATA_DIR, RUNE_TIMEOUT_MS   — as the other probes.
//   RUNE_RECORDS_V1   — prior v1 record count (phase 1), default 5
//   RUNE_RECORDS_V2   — v2 records to produce now, default 5
//
// Exit 0 AND prints "PROBE: OK" iff every leg passes; otherwise "PROBE: FAIL ..."
// to stderr + non-zero exit.

#include "chars_evo_v1.gen.h"   // rune::gen_v1::CharsEvo — the v1 shape (id,userId,name)
#include "chars_evo_v2.gen.h"   // rune::gen_v2::CharsEvo — the v2 shape (+level)

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

// Record header layout (Generator.cpp encode()): [blockLength:u16 LE][version:u16 LE]...
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
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 15000);

    if (target.empty())  { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";   return 1; }
    if (egress.empty())  { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";   return 1; }
    if (dataDir.empty()) { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }

    std::cerr << "PROBE v2: target=" << target << " egress=" << egress
              << " dataDir=" << dataDir << " n1=" << n1 << " n2=" << n2
              << " timeoutMs=" << timeoutMs << "\n";

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
        c.onError([](const std::string& m) { std::cerr << "PROBE v2: onError: " << m << "\n"; });

        std::cerr << "PROBE v2: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE v2: connected\n";

        std::cerr << "PROBE v2: openTopic<CharsEvo>(v2)...\n";
        std::optional<TypedTopic<rune::gen_v2::CharsEvo>> topicOpt;
        try {
            topicOpt.emplace(c.openTopicAndWait<rune::gen_v2::CharsEvo>(
                "chars_evo", std::chrono::milliseconds(timeoutMs)));
        } catch (const std::exception& e) {
            std::cerr << "PROBE v2: FAIL openTopic: " << e.what() << "\n";
            c.close();
            return 4;
        }
        TypedTopic<rune::gen_v2::CharsEvo>& topic = *topicOpt;
        std::cerr << "PROBE v2: openTopic OK boundVersion=" << topic.boundVersion() << "\n";
        if (topic.boundVersion() != 2) {
            std::cerr << "PROBE v2: FAIL boundVersion=" << topic.boundVersion() << " (want 2)\n";
            c.close();
            return 5;
        }

        int32_t topicId = -1;
        {
            TopicSchemaResult sr;
            try {
                sr = c.getTopicSchemaAndWait("chars_evo", std::chrono::milliseconds(timeoutMs));
            } catch (const std::exception& e) {
                std::cerr << "PROBE v2: FAIL getTopicSchema: " << e.what() << "\n";
                c.close();
                return 4;
            }
            if (sr.errorCode != 0 || sr.topicId < 0) {
                std::cerr << "PROBE v2: FAIL getTopicSchema errorCode=" << sr.errorCode << "\n";
                c.close();
                return 4;
            }
            topicId = sr.topicId;
        }
        const TopicHandle th{topicId, /*partitionCount=*/1};

        std::cerr << "PROBE v2: producing " << n2 << " typed v2 records...\n";
        std::vector<std::string> names;
        names.reserve(static_cast<std::size_t>(n2));
        for (int i = 0; i < n2; ++i) {
            ExpectedV2 e = makeExpectedV2(i);
            names.push_back(e.name);
            rune::gen_v2::CharsEvo rec;
            rec.id     = e.id;
            rec.userId = e.userId;
            rec.name   = names.back();
            rec.level  = e.level;
            topic.send(rec);
        }

        std::cerr << "PROBE v2: subscribing from earliest (expect " << (n1 + n2) << " total)...\n";
        std::mutex mtx;
        std::vector<std::vector<uint8_t>> got;   // raw bytes; decoded per-record below
        got.reserve(static_cast<std::size_t>(n1 + n2));

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
                { std::lock_guard<std::mutex> lk(mtx); if (static_cast<int>(got.size()) >= n1 + n2) break; }
                std::this_thread::sleep_for(10ms);
            }
        }
        sub.unsubscribe();

        std::lock_guard<std::mutex> lk(mtx);
        if (static_cast<int>(got.size()) < n1 + n2) {
            std::cerr << "PROBE v2: FAIL consumed " << got.size() << " / " << (n1 + n2) << " records\n";
            c.close();
            return 6;
        }

        // Group 1 [0, n1): v1 records, decode per their OWN version (1).
        for (int i = 0; i < n1; ++i) {
            const auto& raw = got[static_cast<std::size_t>(i)];
            const uint16_t ver = rawVersion(raw);
            if (ver != 1) {
                std::cerr << "PROBE v2: FAIL record " << i << " version=" << ver << " (want 1)\n";
                c.close();
                return 8;
            }
            rune::gen_v1::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV1 exp = makeExpectedV1(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name) {
                std::cerr << "PROBE v2: FAIL v1-group mismatch at i=" << i
                          << " got{id=" << d.id << ",userId=" << d.userId << ",name=" << d.name << "}\n";
                c.close();
                return 9;
            }
        }
        // Group 2 [n1, n1+n2): v2 records, decode per their OWN version (2).
        for (int i = 0; i < n2; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + i)];
            const uint16_t ver = rawVersion(raw);
            if (ver != 2) {
                std::cerr << "PROBE v2: FAIL record " << (n1 + i) << " version=" << ver << " (want 2)\n";
                c.close();
                return 10;
            }
            rune::gen_v2::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV2 exp = makeExpectedV2(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name ||
                d.level != exp.level) {
                std::cerr << "PROBE v2: FAIL v2-group mismatch at i=" << i
                          << " got{id=" << d.id << ",userId=" << d.userId
                          << ",name=" << d.name << ",level=" << d.level << "}\n";
                c.close();
                return 11;
            }
        }

        std::cerr << "PROBE v2: round-trip OK — " << n1 << " v1 + " << n2
                  << " v2 records each decoded per their own version\n";

        std::cout << "PROBE: OK node=" << target << " topicId=" << topicId
                  << " v1=" << n1 << " v2=" << n2 << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE v2: FAIL exception: " << ex.what() << "\n";
        rc = 20;
    }
    return rc;
}
