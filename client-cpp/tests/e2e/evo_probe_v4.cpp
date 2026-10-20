// tests/e2e/evo_probe_v4.cpp — Task 21 real-Aeron schema-evolution e2e, phase 4.
//
// After `rune console -f tests/e2e/schema_evolution/v4_rename.rql` (RENAME
// COLUMN level TO power; chars_evo v3 -> v4 — a rename changes only the field's
// declared name, never its durable tag/offset): open the topic bound to v4,
// produce RUNE_RECORDS_V4 typed v4 records (field `power`), then subscribe
// FROM EARLIEST and decode every record per its own version.
//
// The key proof for "rename -> decode byte-identical; new name resolves": a
// v3-produced record (field `level`) is decoded TWICE from the exact same raw
// bytes — once via the pre-rename rune::gen_v3::CharsEvo struct (member
// `level`) and once via the post-rename rune::gen_v4::CharsEvo struct (member
// `power`) — and the two decoded integers must be bit-identical.  Since
// rename is a pure metadata change (tag/offset untouched), v4's decode() uses
// the IDENTICAL byte offset as v3's; `power` on a v3 record is exactly
// `level`'s value, read through the new name.
//
// Environment: as evo_probe_v3.cpp, plus RUNE_RECORDS_V4 (default 5).

#include "../../examples/ExampleAuth.h"
#include "chars_evo_v1.gen.h"   // rune::gen_v1::CharsEvo — id,userId,name
#include "chars_evo_v2.gen.h"   // rune::gen_v2::CharsEvo — id,userId,name,level
#include "chars_evo_v3.gen.h"   // rune::gen_v3::CharsEvo — id,userId,level
#include "chars_evo_v4.gen.h"   // rune::gen_v4::CharsEvo — id,userId,power (level renamed)

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
struct ExpectedV4 { int64_t id; int64_t userId; int32_t power; };
ExpectedV4 makeExpectedV4(int i) {
    return ExpectedV4{40000 + i, static_cast<int64_t>(3000 + i), 300 + i};
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
    const int         n4           = getEnvInt("RUNE_RECORDS_V4", 5);
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 15000);

    if (target.empty())  { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";   return 1; }
    if (egress.empty())  { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";   return 1; }
    if (dataDir.empty()) { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }

    std::cerr << "PROBE v4: target=" << target << " n1=" << n1 << " n2=" << n2
              << " n3=" << n3 << " n4=" << n4 << " timeoutMs=" << timeoutMs << "\n";

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
        c.onError([](const std::string& m) { std::cerr << "PROBE v4: onError: " << m << "\n"; });

        std::cerr << "PROBE v4: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE v4: connected\n";

        std::cerr << "PROBE v4: openTopic<CharsEvo>(v4)...\n";
        std::optional<TypedTopic<rune::gen_v4::CharsEvo>> topicOpt;
        try {
            topicOpt.emplace(c.openTopicAndWait<rune::gen_v4::CharsEvo>(
                "chars_evo", std::chrono::milliseconds(timeoutMs)));
        } catch (const std::exception& e) {
            std::cerr << "PROBE v4: FAIL openTopic: " << e.what() << "\n";
            c.close();
            return 4;
        }
        TypedTopic<rune::gen_v4::CharsEvo>& topic = *topicOpt;
        std::cerr << "PROBE v4: openTopic OK boundVersion=" << topic.boundVersion() << "\n";
        if (topic.boundVersion() != 4) {
            std::cerr << "PROBE v4: FAIL boundVersion=" << topic.boundVersion() << " (want 4)\n";
            c.close();
            return 5;
        }

        int32_t topicId = -1;
        {
            TopicSchemaResult sr;
            try {
                sr = c.getTopicSchemaAndWait("chars_evo", std::chrono::milliseconds(timeoutMs));
            } catch (const std::exception& e) {
                std::cerr << "PROBE v4: FAIL getTopicSchema: " << e.what() << "\n";
                c.close();
                return 4;
            }
            if (sr.errorCode != 0 || sr.topicId < 0) {
                std::cerr << "PROBE v4: FAIL getTopicSchema errorCode=" << sr.errorCode << "\n";
                c.close();
                return 4;
            }
            topicId = sr.topicId;
        }
        const TopicHandle th{topicId, /*partitionCount=*/1};

        std::cerr << "PROBE v4: producing " << n4 << " typed v4 records ('power')...\n";
        for (int i = 0; i < n4; ++i) {
            ExpectedV4 e = makeExpectedV4(i);
            rune::gen_v4::CharsEvo rec;
            rec.id     = e.id;
            rec.userId = e.userId;
            rec.power  = e.power;
            topic.send(rec);
        }

        const int total = n1 + n2 + n3 + n4;
        std::cerr << "PROBE v4: subscribing from earliest (expect " << total << " total)...\n";
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
            std::cerr << "PROBE v4: FAIL consumed " << got.size() << " / " << total << " records\n";
            c.close();
            return 6;
        }

        // Group 1 [0,n1): v1 — unaffected by the rename two versions later.
        for (int i = 0; i < n1; ++i) {
            const auto& raw = got[static_cast<std::size_t>(i)];
            if (rawVersion(raw) != 1) { std::cerr << "PROBE v4: FAIL record " << i << " version!=1\n"; c.close(); return 8; }
            rune::gen_v1::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV1 exp = makeExpectedV1(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name) {
                std::cerr << "PROBE v4: FAIL v1-group mismatch at i=" << i << "\n"; c.close(); return 9;
            }
        }
        // Group 2 [n1,n1+n2): v2 — unaffected.
        for (int i = 0; i < n2; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + i)];
            if (rawVersion(raw) != 2) { std::cerr << "PROBE v4: FAIL record " << (n1+i) << " version!=2\n"; c.close(); return 10; }
            rune::gen_v2::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV2 exp = makeExpectedV2(i);
            if (d.id != exp.id || d.userId != exp.userId || std::string(d.name) != exp.name || d.level != exp.level) {
                std::cerr << "PROBE v4: FAIL v2-group mismatch at i=" << i << "\n"; c.close(); return 11;
            }
        }
        // Group 3 [n1+n2,n1+n2+n3): v3 — THE RENAME PROOF. Decode the SAME raw
        // bytes via BOTH the pre-rename (v3, field `level`) and post-rename
        // (v4, field `power`) structs; both must yield the exact same integer.
        for (int i = 0; i < n3; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + n2 + i)];
            if (rawVersion(raw) != 3) { std::cerr << "PROBE v4: FAIL record " << (n1+n2+i) << " version!=3\n"; c.close(); return 12; }
            rune::gen_v3::CharsEvo dOld;   // pre-rename view: field `level`
            dOld.decode(std::span<const uint8_t>(raw));
            rune::gen_v4::CharsEvo dNew;   // post-rename view: field `power`
            dNew.decode(std::span<const uint8_t>(raw));
            ExpectedV3 exp = makeExpectedV3(i);
            if (dOld.id != exp.id || dOld.userId != exp.userId || dOld.level != exp.level) {
                std::cerr << "PROBE v4: FAIL v3-group (old view) mismatch at i=" << i << "\n"; c.close(); return 13;
            }
            if (dNew.id != dOld.id || dNew.userId != dOld.userId || dNew.power != dOld.level) {
                std::cerr << "PROBE v4: FAIL rename not byte-identical at i=" << i
                          << " old.level=" << dOld.level << " new.power=" << dNew.power << "\n";
                c.close();
                return 14;
            }
        }
        // Group 4 [n1+n2+n3,total): v4 — new records under the new name.
        for (int i = 0; i < n4; ++i) {
            const auto& raw = got[static_cast<std::size_t>(n1 + n2 + n3 + i)];
            if (rawVersion(raw) != 4) { std::cerr << "PROBE v4: FAIL record " << (n1+n2+n3+i) << " version!=4\n"; c.close(); return 15; }
            rune::gen_v4::CharsEvo d;
            d.decode(std::span<const uint8_t>(raw));
            ExpectedV4 exp = makeExpectedV4(i);
            if (d.id != exp.id || d.userId != exp.userId || d.power != exp.power) {
                std::cerr << "PROBE v4: FAIL v4-group mismatch at i=" << i << "\n"; c.close(); return 16;
            }
        }

        std::cerr << "PROBE v4: round-trip OK — rename decodes byte-identical "
                     "(v3.level == v4.power on the same bytes); new name resolves on new records\n";

        std::cout << "PROBE: OK node=" << target << " topicId=" << topicId
                  << " v1=" << n1 << " v2=" << n2 << " v3=" << n3 << " v4=" << n4 << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE v4: FAIL exception: " << ex.what() << "\n";
        rc = 20;
    }
    return rc;
}
