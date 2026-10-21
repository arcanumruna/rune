// examples/rune_schema_probe.cpp — Task 13 headline typed round-trip probe
//
// The #11 real-e2e gate: against a REAL single-node rune-broker over REAL Aeron
// (default config, NOT in-process), a typed client exercises the FULL 0b typed
// transport path:
//
//   1. connectAndWait()                                     — real Aeron session
//   2. defineTopic("characters", 1, "userId",          — runtime DDL, validate ON
//                  validate=true, <characters blob>)      broker registers schema
//   3. openTopicAndWait<rune::gen::Characters>("characters")— EXACT-fingerprint gate
//   4. topic.send(Characters{...}) × N                 — typed produce
//   5. subscribe(from earliest) + decode each record   — typed consume + decode
//      via rune::gen::Characters::decode → assert fields == what was sent
//   6. produce a deliberately MALFORMED raw record      — server-side validation
//      to the validated topic → assert rejected with      must reject it
//      SchemaValidationFailed (ErrorCode 24)
//
// The generated struct rune::gen::Characters is produced at BUILD TIME from the
// checked-in descriptor blob tests/generated/characters.blob by rune_codec_gen
// (see the add_custom_command in CMakeLists.txt) into characters.gen.h.  Because
// the SAME blob is used both to (a) generate the struct and (b) defineTopic on
// the broker, the fingerprint gate in openTopic<T> matches by construction.
//
// Environment (mirrors examples/rune_produce_subscribe.cpp):
//   RUNE_TARGET          — node client ingress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-1:40200"
//   RUNE_TARGET_STREAM   — int, default 1
//   RUNE_EGRESS          — this client's own egress channel (required)
//                          e.g. "aeron:udp?endpoint=rune-schema-probe:40500"
//   RUNE_EGRESS_STREAM   — int, default 2
//   RUNE_DATA_DIR        — Aeron driver dir (required)
//   RUNE_BLOB_PATH       — path to characters.blob (required)
//   RUNE_RECORDS         — records to produce/consume, default 5
//   RUNE_TIMEOUT_MS      — per-operation timeout ms, default 10000
//
// Exit 0 AND prints "PROBE: OK" iff every leg passes.  Any failure prints a
// "PROBE: FAIL ..." line to stderr and returns non-zero.
//
// Design: bounded/time-independent waits everywhere (poll to a deadline, never
// a fixed sleep as a correctness gate).

#include "ExampleAuth.h"
#include "characters.gen.h"   // build-time generated from characters.blob

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/subscriber/Subscription.h>

#include <rune/protocol/client/Protocol.h>   // ErrorCode::SchemaValidationFailed

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <string_view>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;

// F8c C6: one acked send waited on a Completions (examples only — the library
// deliberately offers no per-record *AndWait sugar).  false = rejected or the
// wait timed out (`out.status` = Timeout then; the op still completes later).
static bool sendAndWaitAck(Completions& cq, Producer& p, const TopicHandle& th, const Key& key,
                           std::span<const uint8_t> v, Token t, int timeoutMs, Completion& out) {
    for (;;) {
        const SendResult r = p.trySend(th, key, v, cq, t);
        if (r == SendResult::Ok) break;
        if (r == SendResult::QueueFull) { (void)cq.poll([](const Completion&, std::span<const uint8_t>) {}); continue; }
        if (r != SendResult::BackPressured) { out.status = OpStatus::Closed; return false; }
        std::this_thread::yield();
    }
    const auto w = cq.waitFor(t, std::chrono::milliseconds(timeoutMs),
                              [&](const Completion& c, std::span<const uint8_t>) { out = c; });
    if (w != Completions::Wait::Done) { out.status = OpStatus::Timeout; return false; }
    return true;
}

// ---------------------------------------------------------------------------
// Env helpers
// ---------------------------------------------------------------------------
static std::string getEnvStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}
static int getEnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::atoi(v) : def;
}

// One expected record per index — deterministic, so consume can assert equality.
struct Expected {
    int64_t     id;
    int64_t     userId;
    int32_t     strength;
    std::string name;
};

static Expected makeExpected(int i) {
    return Expected{
        /*id*/       1000 + i,
        /*userId*/   static_cast<int64_t>(42 + i),
        /*strength*/ 7 * (i + 1),
        /*name*/     "hero-" + std::to_string(i),
    };
}

int main() {
    const std::string target       = getEnvStr("RUNE_TARGET");
    const int         targetStream = getEnvInt("RUNE_TARGET_STREAM", 1);
    const std::string egress       = getEnvStr("RUNE_EGRESS");
    const int         egressStream = getEnvInt("RUNE_EGRESS_STREAM", 2);
    const std::string dataDir      = getEnvStr("RUNE_DATA_DIR");
    const std::string blobPath     = getEnvStr("RUNE_BLOB_PATH");
    const int         records      = getEnvInt("RUNE_RECORDS", 5);
    const int         timeoutMs    = getEnvInt("RUNE_TIMEOUT_MS", 10000);

    if (target.empty())   { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";   return 1; }
    if (egress.empty())   { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";   return 1; }
    if (dataDir.empty())  { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }
    if (blobPath.empty()) { std::cerr << "PROBE: FAIL RUNE_BLOB_PATH is required\n";return 1; }

    std::cerr << "PROBE: target=" << target << " egress=" << egress
              << " dataDir=" << dataDir << " blob=" << blobPath
              << " records=" << records << " timeoutMs=" << timeoutMs << "\n";

    // ---- Read the checked-in descriptor blob ---------------------------------
    std::vector<uint8_t> blob;
    {
        std::ifstream fin(blobPath, std::ios::binary);
        if (!fin) { std::cerr << "PROBE: FAIL cannot open blob '" << blobPath << "'\n"; return 2; }
        blob.assign(std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>());
        if (blob.empty()) { std::cerr << "PROBE: FAIL blob is empty\n"; return 2; }
    }
    std::cerr << "PROBE: read blob " << blob.size() << " bytes; struct fingerprint="
              << rune::gen::Characters::kFingerprint << "\n";

    // ---- ClientConfig --------------------------------------------------------
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
    // No static topics — "characters" is created at RUNTIME via defineTopic().

    int rc = 0;
    try {
        Client c(cfg);
        c.onConnected   ([](int64_t sid) { std::cerr << "PROBE: onConnected sessionId=" << sid << "\n"; });
        c.onDisconnected([]()            { std::cerr << "PROBE: onDisconnected\n"; });
        c.onError       ([](const std::string& m) { std::cerr << "PROBE: onError: " << m << "\n"; });

        // ---- 1. Connect ------------------------------------------------------
        std::cerr << "PROBE: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE: connected\n";

        // ---- 2. defineTopic (validate = ON) ----------------------------------
        std::cerr << "PROBE: defineTopic(\"characters\", 1, \"userId\", validate=true, blob)\n";
        DefineTopicResult def = c.defineTopicAndWait("characters", /*partitions=*/1, /*keyField=*/"userId",
                                                     /*validate=*/true, std::span<const uint8_t>(blob));
        if (def.errorCode != 0) {
            std::cerr << "PROBE: FAIL defineTopic errorCode=" << def.errorCode << "\n";
            c.close();
            return 3;
        }
        std::cerr << "PROBE: defineTopic OK topicId=" << def.topicId
                  << " version=" << def.version << "\n";
        const TopicHandle th{def.topicId, /*partitionCount=*/1};

        // ---- 3. openTopic<Characters> — EXACT-fingerprint gate ---------------
        std::cerr << "PROBE: openTopic<Characters>(\"characters\")...\n";
        // F8c C3: openTopicAndWait — no std::async thread; throws on a stale
        // fingerprint, a broker error or the wait timeout.
        std::optional<TypedTopic<rune::gen::Characters>> topicOpt;
        try {
            topicOpt.emplace(c.openTopicAndWait<rune::gen::Characters>(
                "characters", std::chrono::milliseconds(timeoutMs)));
        } catch (const std::exception& e) {
            if (std::string_view(e.what()).find("timed out") != std::string_view::npos) {
                std::cerr << "PROBE: FAIL openTopic TIMEOUT\n";
                c.close();
                return 4;
            }
            throw;
        }
        TypedTopic<rune::gen::Characters>& topic = *topicOpt;
        std::cerr << "PROBE: openTopic OK (fingerprint matched broker schema)\n";

        // ---- 4. Produce N typed records --------------------------------------
        std::cerr << "PROBE: producing " << records << " typed records...\n";
        std::vector<std::string> names;  // keep name storage alive across send()
        names.reserve(static_cast<std::size_t>(records));
        for (int i = 0; i < records; ++i) {
            Expected e = makeExpected(i);
            names.push_back(e.name);
            rune::gen::Characters rec;
            rec.id       = e.id;
            rec.userId   = e.userId;
            rec.strength = e.strength;
            rec.name     = names.back();   // string_view into stable storage
            topic.send(rec);
            std::cerr << "PROBE: sent id=" << rec.id << " userId=" << rec.userId
                      << " strength=" << rec.strength << " name=" << rec.name << "\n";
        }

        // ---- 5. Subscribe from earliest, decode + assert each ----------------
        std::cerr << "PROBE: subscribing from earliest...\n";
        std::mutex mtx;
        std::vector<Expected> got;          // decoded records in delivery order
        got.reserve(static_cast<std::size_t>(records));

        SubscribeOptions opts;
        opts.from      = From::earliest();
        opts.isolation = Isolation::ReadUncommitted;

        auto sub = c.subscribe(th, /*partition=*/0, opts, [&](const Record& r) {
            rune::gen::Characters d;
            if (!d.decode(r.value)) return;          // 0a generated zero-copy decode; #418: never misread
            std::lock_guard<std::mutex> lk(mtx);
            got.push_back(Expected{d.id, d.userId, d.strength, std::string(d.name)});
            std::cerr << "PROBE: consumed offset=" << r.offset
                      << " id=" << d.id << " userId=" << d.userId
                      << " strength=" << d.strength << " name=" << d.name << "\n";
        });

        {
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::milliseconds(timeoutMs * 3);
            while (std::chrono::steady_clock::now() < dl) {
                { std::lock_guard<std::mutex> lk(mtx); if (static_cast<int>(got.size()) >= records) break; }
                std::this_thread::sleep_for(10ms);
            }
        }
        sub.unsubscribe();

        {
            std::lock_guard<std::mutex> lk(mtx);
            if (static_cast<int>(got.size()) < records) {
                std::cerr << "PROBE: FAIL consumed " << got.size() << " / " << records << " records\n";
                c.close();
                return 5;
            }
            // Assert the decoded round-trip field-by-field.  Records on a single
            // partition are delivered in offset (produce) order.
            for (int i = 0; i < records; ++i) {
                Expected exp = makeExpected(i);
                const Expected& a = got[static_cast<std::size_t>(i)];
                if (a.id != exp.id || a.userId != exp.userId ||
                    a.strength != exp.strength || a.name != exp.name) {
                    std::cerr << "PROBE: FAIL round-trip mismatch at i=" << i
                              << " got{id=" << a.id << ",userId=" << a.userId
                              << ",strength=" << a.strength << ",name=" << a.name << "}"
                              << " want{id=" << exp.id << ",userId=" << exp.userId
                              << ",strength=" << exp.strength << ",name=" << exp.name << "}\n";
                    c.close();
                    return 6;
                }
            }
            std::cerr << "PROBE: round-trip OK — all " << records
                      << " decoded records match what was sent\n";
        }

        // ---- 6. Malformed raw record → must be REJECTED (validate=ON) --------
        // A validated "characters" record must be at least the fixed block
        // (4-byte SBE header + 20-byte block + 4-byte name length).  We offer a
        // deliberately too-short garbage payload as a RAW produce; the broker's
        // per-worker validation hook must reject it with SchemaValidationFailed.
        std::cerr << "PROBE: producing a MALFORMED raw record (expect reject)...\n";
        {
            ProducerConfig pcfg;
            pcfg.acks             = Acks::Leader;   // wait for the broker's response
            pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
            auto p = c.producer(pcfg);

            // Garbage: 3 bytes — far shorter than a valid Characters record.
            const std::vector<uint8_t> bad{0x01, 0x02, 0x03};
            Completions cq(c);
            Completion  done;
            const bool  waited = sendAndWaitAck(cq, p, th, Key(static_cast<uint64_t>(999)),
                                                std::span<const uint8_t>(bad), 1, static_cast<int>(timeoutMs), done);
            if (!waited && done.status == OpStatus::Timeout) {
                std::cerr << "PROBE: FAIL malformed-record produce TIMEOUT "
                             "(broker neither acked nor rejected)\n";
                c.close();
                return 7;
            }
            bool rejected = false;
            std::string why;
            if (waited && done.status == OpStatus::Ok) {
                std::cerr << "PROBE: FAIL malformed record was ACCEPTED at offset="
                          << done.value << " (validation did NOT reject)\n";
            } else {
                rejected = true;
                why = "produce rejected by broker (errorCode=" + std::to_string(done.errorCode) + ")";
            }
            if (!rejected) { c.close(); return 8; }

            // The rejection must carry SchemaValidationFailed (24).  The Producer
            // surfaces the ProduceResponse errorCode in the exception message.
            const int kSVF = static_cast<int>(
                rune::protocol::client::ErrorCode::SchemaValidationFailed);
            if (why.find(std::to_string(kSVF)) == std::string::npos &&
                why.find("SchemaValidation") == std::string::npos &&
                why.find("validation") == std::string::npos) {
                std::cerr << "PROBE: FAIL malformed record rejected but not with "
                             "SchemaValidationFailed(" << kSVF << "); reason='" << why << "'\n";
                c.close();
                return 9;
            }
            std::cerr << "PROBE: malformed record correctly REJECTED: " << why << "\n";
        }

        // ---- Success ---------------------------------------------------------
        std::cout << "PROBE: OK node=" << target << " topicId=" << def.topicId
                  << " records=" << records << "\n";
        std::cout.flush();
        c.close();
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL exception: " << ex.what() << "\n";
        rc = 20;
    }
    return rc;
}
