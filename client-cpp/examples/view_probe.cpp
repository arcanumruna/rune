// examples/view_probe.cpp — Task 21 view-engine client API + docker e2e probe.
//
// Drives the FULL Phase 1 view-engine client flow against a REAL single-node
// rune-broker over REAL Aeron (default config, NOT in-process):
//
//   1. connectAndWait()                                  — real Aeron session
//   2. defineTopicAndWait("character", 1, "id", ...)      — a keyed source topic
//      {id: Int64 (key), hp: Int64}
//   3. createViewAndWait(...) x2                          — a PERSISTED view
//      ("char_persisted") and a TRANSIENT view ("char_transient"), both a
//      {id, hp} Replace-projection over "character" keyed by "id". Phase 1 has
//      NO open-by-name wire: the broker hands back {viewTopicId,
//      outputFingerprint} once, here, and every later getViewRow() pins that
//      fingerprint.
//   4. produce(key=id, {id,hp})                           — typed-by-hand
//      produce (raw codec RCD2 record bytes; no generated struct needed —
//      the view decodes per the SOURCE schema, independent of this probe).
//   5. getViewRowAndWait(viewTopicId, keyU64=id, fp) x2    — the folded row,
//      decoded positionally via rune::client::view::readSlot (ViewRowCodec.h)
//      against a RowLayout built from the view's own authored output fields.
//
// Two modes (RUNE_MODE), driving the docker/view-engine-e2e.sh restart leg
// across TWO separate container runs (a `docker run --rm` container cannot
// itself survive `docker compose restart rune-1` on the broker side):
//
//   RUNE_MODE=setup (default): phases 1-5 above. On success prints
//     VIEW_SRC_TOPIC_ID=<id>
//     VIEW_TOPIC_ID_PERSISTED=<id>
//     VIEW_FP_PERSISTED=<fp>
//     VIEW_TOPIC_ID_TRANSIENT=<id>
//     VIEW_FP_TRANSIENT=<fp>
//     VIEW_KEY_ID=<id>
//     VIEW_HP_INITIAL=<hp>
//     VIEW-PROBE-SETUP: OK
//   to stdout — the e2e script scrapes these into env vars for mode 2.
//
//   RUNE_MODE=verify_restart: takes the above as RUNE_VIEW_* env vars (set by
//   the e2e script after it has restarted the broker and awaited a fresh
//   leader). Asserts:
//     (a) the PERSISTED view still answers OK with the ORIGINAL row (the
//         broker's readiness gate replayed it from the source log / its own
//         checkpoint across the restart — no reproduce needed);
//     (b) the TRANSIENT view answers AbsentKey (no checkpoint survives a
//         transient view's restart — "empty" per spec);
//     (c) re-producing the SAME key with a NEW hp value refills the
//         transient view (it folds live off the source log, which IS
//         persisted) — getViewRow eventually returns OK with the NEW value.
//   Prints "VIEW-ENGINE-E2E: OK" on success.
//
// Environment (both modes):
//   RUNE_TARGET / RUNE_TARGET_STREAM / RUNE_EGRESS / RUNE_EGRESS_STREAM /
//   RUNE_DATA_DIR / RUNE_TIMEOUT_MS — same as rune_schema_probe.cpp.
//
// Design: bounded/time-independent waits everywhere (poll to a deadline,
// never a fixed sleep as a correctness gate) — CLAUDE.md engineering
// philosophy #5/#7 (test everything; regression-grade e2e).

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/client/view/ViewRowCodec.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace rune::client;
namespace rcv = rune::client::view;

// ---------------------------------------------------------------------------
// Env helpers (mirrors examples/rune_schema_probe.cpp)
// ---------------------------------------------------------------------------
static std::string getEnvStr(const char* name, const std::string& def = {}) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::string(v) : def;
}
static int getEnvInt(const char* name, int def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::atoi(v) : def;
}
static uint64_t getEnvU64(const char* name, uint64_t def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::strtoull(v, nullptr, 10) : def;
}
static int64_t getEnvI64(const char* name, int64_t def) {
    const char* v = std::getenv(name);
    return (v && *v) ? std::strtoll(v, nullptr, 10) : def;
}

// One acked send waited on a Completions (examples only, same helper as
// rune_schema_probe.cpp: the library deliberately offers no per-record
// *AndWait sugar).
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
// "character" source schema: {id: Int64 (key), hp: Int64}.
// ---------------------------------------------------------------------------
static rune::codec::Descriptor characterDescriptor() {
    return rune::codec::DescriptorBuilder("character", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build();
}

// [blockLength u16][version u16][id i64][hp i64] — fields packed in
// declaration order starting at byte 0 (rune::codec::computeOffsets).
static std::vector<uint8_t> encodeCharacter(int64_t id, int64_t hp, uint16_t version) {
    std::vector<uint8_t> b;
    b.reserve(20);
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(16 /*blockLength = 8+8*/);
    le16(version);
    le64(id);
    le64(hp);
    return b;
}

// char_{persisted,transient}(id, hp) <- source "character", Replace-projected.
static rcv::ViewPlan charHpPlan(const std::string& viewName, int32_t srcTopicId, uint8_t persistence) {
    rcv::ViewPlan p;
    p.viewName    = viewName;
    p.key         = "id";
    p.persistence = persistence;
    p.sources.push_back(rcv::SourceSpec{srcTopicId, "id"});
    p.output.push_back(rcv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rcv::MergeRule::Replace});
    p.output.push_back(rcv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rcv::MergeRule::Replace});
    return p;
}

// Output row layout: slot 0 = id (Int64), slot 1 = hp (Int64) — same order as
// charHpPlan's `output` vector above (RowLayout slot order == ViewPlan output
// declaration order; see PlanCompiler.cpp / ViewRow.h).
static rcv::RowLayout charHpRowLayout() {
    const rune::codec::PrimType types[] = {rune::codec::PrimType::Int64, rune::codec::PrimType::Int64};
    return rcv::buildRowLayout(types);
}

// Poll getViewRow until it resolves past infra "not yet" states (kNotReady:
// the view-partition instance hasn't folded anything yet; kUnknownView: the
// broker hasn't registered the view on this worker yet, e.g. just after a
// restart) — returns whatever TERMINAL status it then sees (kOk or
// kAbsentKey), never spinning on those two.  Bounded: never a fixed sleep as
// a correctness gate (CLAUDE.md #5/#7).
static ViewRowResult waitResolved(Client& c, int32_t viewTopicId, uint64_t key, uint64_t fp,
                                  int boundMs, int perCallTimeoutMs) {
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewTopicId, key, fp, std::chrono::milliseconds(perCallTimeoutMs));
        if (r.status != ViewRowResult::kNotReady && r.status != ViewRowResult::kUnknownView) return r;
        if (std::chrono::steady_clock::now() >= dl) return r;
        std::this_thread::sleep_for(50ms);
    }
}

// Poll getViewRow until it returns kOk (the row has folded) or the bound
// expires — AbsentKey/NotReady/UnknownView are all "not there yet" here
// (used right after producing: the fold may lag the produce ack briefly).
static ViewRowResult waitForRow(Client& c, int32_t viewTopicId, uint64_t key, uint64_t fp,
                                int boundMs, int perCallTimeoutMs) {
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewTopicId, key, fp, std::chrono::milliseconds(perCallTimeoutMs));
        if (r.status == ViewRowResult::kOk) return r;
        if (std::chrono::steady_clock::now() >= dl) return r;
        std::this_thread::sleep_for(50ms);
    }
}

static bool decodeAndCheck(const ViewRowResult& r, int64_t wantId, int64_t wantHp, const char* who) {
    const rcv::RowLayout layout = charHpRowLayout();
    if (r.rowBytes.size() != layout.rowLen) {
        std::cerr << "PROBE: FAIL " << who << " row size=" << r.rowBytes.size()
                  << " want=" << layout.rowLen
                  << " status=" << static_cast<int>(r.status)
                  << " (0=Ok 1=AbsentKey 2=NotReady 3=SchemaMismatch 4=UnknownView)\n";
        return false;
    }
    const auto idF = rcv::readSlot(std::span<const uint8_t>(r.rowBytes), layout, 0);
    const auto hpF = rcv::readSlot(std::span<const uint8_t>(r.rowBytes), layout, 1);
    if (!idF.present || !hpF.present || idF.i != wantId || hpF.i != wantHp) {
        std::cerr << "PROBE: FAIL " << who << " decoded id=" << idF.i << " hp=" << hpF.i
                  << " (present id=" << idF.present << " hp=" << hpF.present << ")"
                  << " want id=" << wantId << " hp=" << wantHp << "\n";
        return false;
    }
    std::cerr << "PROBE: " << who << " OK id=" << idF.i << " hp=" << hpF.i << "\n";
    return true;
}

int main() {
    const std::string mode        = getEnvStr("RUNE_MODE", "setup");
    const std::string target      = getEnvStr("RUNE_TARGET");
    const int targetStream        = getEnvInt("RUNE_TARGET_STREAM", 1);
    const std::string egress      = getEnvStr("RUNE_EGRESS");
    const int egressStream        = getEnvInt("RUNE_EGRESS_STREAM", 2);
    const std::string dataDir     = getEnvStr("RUNE_DATA_DIR");
    const int timeoutMs           = getEnvInt("RUNE_TIMEOUT_MS", 10000);
    const int boundMs             = getEnvInt("RUNE_WAIT_BOUND_MS", 60000);

    if (target.empty())  { std::cerr << "PROBE: FAIL RUNE_TARGET is required\n";  return 1; }
    if (egress.empty())  { std::cerr << "PROBE: FAIL RUNE_EGRESS is required\n";  return 1; }
    if (dataDir.empty()) { std::cerr << "PROBE: FAIL RUNE_DATA_DIR is required\n"; return 1; }

    std::cerr << "PROBE: mode=" << mode << " target=" << target << " egress=" << egress
              << " dataDir=" << dataDir << " timeoutMs=" << timeoutMs << "\n";

    ClientConfig cfg;
    cfg.gatewayChannel   = target;
    cfg.gatewayStreamId  = targetStream;
    cfg.egressChannel    = egress;
    cfg.egressStreamId   = egressStream;
    cfg.dataDir          = dataDir;
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    const auto wait = std::chrono::milliseconds(timeoutMs);

    try {
        Client c(cfg);
        c.onError([](const std::string& m) { std::cerr << "PROBE: onError: " << m << "\n"; });

        std::cerr << "PROBE: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE: connected\n";

        if (mode == "setup") {
            // ---- 1. defineTopic "character" ------------------------------
            const auto blob = rune::codec::serialize(characterDescriptor());
            DefineTopicResult def = c.defineTopicAndWait(
                "character", /*partitions=*/1, /*keyField=*/"id", /*validate=*/false,
                std::span<const uint8_t>(blob), wait);
            if (def.errorCode != 0) {
                std::cerr << "PROBE: FAIL defineTopic errorCode=" << def.errorCode << "\n";
                return 2;
            }
            std::cerr << "PROBE: defineTopic(character) OK topicId=" << def.topicId << "\n";
            const TopicHandle srcTh{def.topicId, /*partitionCount=*/1};

            // ---- 2. createView x2 (persisted + transient) ------------------
            const auto persistedBlob  = rcv::serializeAuthored(charHpPlan("char_persisted",  def.topicId, 0));
            const auto transientBlob  = rcv::serializeAuthored(charHpPlan("char_transient",  def.topicId, 1));

            CreateViewResult viewP = c.createViewAndWait("char_persisted", "id", /*persistence=*/0,
                                                         std::span<const uint8_t>(persistedBlob), wait);
            if (viewP.errorCode != 0) {
                std::cerr << "PROBE: FAIL createView(char_persisted) errorCode=" << viewP.errorCode << "\n";
                return 3;
            }
            std::cerr << "PROBE: createView(char_persisted) OK viewTopicId=" << viewP.viewTopicId
                      << " fp=" << viewP.outputFingerprint << "\n";

            CreateViewResult viewT = c.createViewAndWait("char_transient", "id", /*persistence=*/1,
                                                         std::span<const uint8_t>(transientBlob), wait);
            if (viewT.errorCode != 0) {
                std::cerr << "PROBE: FAIL createView(char_transient) errorCode=" << viewT.errorCode << "\n";
                return 4;
            }
            std::cerr << "PROBE: createView(char_transient) OK viewTopicId=" << viewT.viewTopicId
                      << " fp=" << viewT.outputFingerprint << "\n";

            // ---- 3. produce one "character" record -------------------------
            const int64_t keyId = 4242;
            const int64_t hp0   = 100;
            {
                ProducerConfig pcfg;
                pcfg.acks             = Acks::Leader;
                pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
                auto p = c.producer(pcfg);
                Completions cq(c);
                Completion  done;
                const auto value = encodeCharacter(keyId, hp0, /*version=*/1);
                const bool waited = sendAndWaitAck(cq, p, srcTh, Key(static_cast<uint64_t>(keyId)),
                                                   std::span<const uint8_t>(value), 1,
                                                   timeoutMs, done);
                if (!waited || done.status != OpStatus::Ok) {
                    std::cerr << "PROBE: FAIL produce(character) waited=" << waited
                              << " status=" << static_cast<int>(done.status) << "\n";
                    return 5;
                }
                std::cerr << "PROBE: produced character id=" << keyId << " hp=" << hp0
                          << " offset=" << done.value << "\n";
            }

            // ---- 4. getViewRow on both views ---------------------------------
            const uint64_t keyU64 = static_cast<uint64_t>(keyId);
            const ViewRowResult rp = waitForRow(c, viewP.viewTopicId, keyU64, viewP.outputFingerprint,
                                                boundMs, timeoutMs);
            if (!decodeAndCheck(rp, keyId, hp0, "char_persisted (initial)")) return 6;

            const ViewRowResult rt = waitForRow(c, viewT.viewTopicId, keyU64, viewT.outputFingerprint,
                                                boundMs, timeoutMs);
            if (!decodeAndCheck(rt, keyId, hp0, "char_transient (initial)")) return 7;

            // ---- Handoff for the restart leg (scraped by the e2e script) ----
            std::cout << "VIEW_SRC_TOPIC_ID="       << def.topicId            << "\n";
            std::cout << "VIEW_TOPIC_ID_PERSISTED=" << viewP.viewTopicId      << "\n";
            std::cout << "VIEW_FP_PERSISTED="       << viewP.outputFingerprint << "\n";
            std::cout << "VIEW_TOPIC_ID_TRANSIENT=" << viewT.viewTopicId      << "\n";
            std::cout << "VIEW_FP_TRANSIENT="       << viewT.outputFingerprint << "\n";
            std::cout << "VIEW_KEY_ID="             << keyId                  << "\n";
            std::cout << "VIEW_HP_INITIAL="         << hp0                   << "\n";
            std::cout << "VIEW-PROBE-SETUP: OK\n";
            std::cout.flush();
            c.close();
            return 0;
        }

        if (mode == "verify_restart") {
            const int32_t  srcTopicId     = static_cast<int32_t>(getEnvI64("RUNE_VIEW_SRC_TOPIC_ID", -1));
            const int32_t  viewIdP        = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TOPIC_ID_PERSISTED", -1));
            const uint64_t fpP            = getEnvU64("RUNE_VIEW_FP_PERSISTED", 0);
            const int32_t  viewIdT        = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TOPIC_ID_TRANSIENT", -1));
            const uint64_t fpT            = getEnvU64("RUNE_VIEW_FP_TRANSIENT", 0);
            const int64_t  keyId          = getEnvI64("RUNE_VIEW_KEY_ID", -1);
            const int64_t  hpInitial      = getEnvI64("RUNE_VIEW_HP_INITIAL", -1);
            const int64_t  hpRefill       = getEnvI64("RUNE_VIEW_HP_REFILL", hpInitial + 1);

            if (srcTopicId < 0 || viewIdP < 0 || viewIdT < 0 || keyId < 0) {
                std::cerr << "PROBE: FAIL verify_restart requires RUNE_VIEW_SRC_TOPIC_ID / "
                             "RUNE_VIEW_TOPIC_ID_PERSISTED / RUNE_VIEW_TOPIC_ID_TRANSIENT / RUNE_VIEW_KEY_ID\n";
                return 10;
            }
            const uint64_t keyU64 = static_cast<uint64_t>(keyId);
            const TopicHandle srcTh{srcTopicId, /*partitionCount=*/1};

            // ---- (a) persisted view: recovered OK with the ORIGINAL row -----
            const ViewRowResult rp = waitResolved(c, viewIdP, keyU64, fpP, boundMs, timeoutMs);
            if (rp.status != ViewRowResult::kOk) {
                std::cerr << "PROBE: FAIL char_persisted post-restart status=" << rp.status
                          << " (want kOk — the readiness gate must replay a persisted view)\n";
                return 11;
            }
            if (!decodeAndCheck(rp, keyId, hpInitial, "char_persisted (post-restart, recovered)")) return 12;

            // ---- (b) transient view: AbsentKey (no checkpoint survives) -----
            const ViewRowResult rt0 = waitResolved(c, viewIdT, keyU64, fpT, boundMs, timeoutMs);
            if (rt0.status != ViewRowResult::kAbsentKey) {
                std::cerr << "PROBE: FAIL char_transient post-restart status=" << rt0.status
                          << " (want kAbsentKey — a transient view must NOT survive a restart)\n";
                return 13;
            }
            std::cerr << "PROBE: char_transient post-restart correctly AbsentKey (empty)\n";

            // ---- (c) re-produce -> transient refills (folds live off the ----
            //          source log, which IS persisted) --------------------------
            {
                ProducerConfig pcfg;
                pcfg.acks             = Acks::Leader;
                pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
                auto p = c.producer(pcfg);
                Completions cq(c);
                Completion  done;
                const auto value = encodeCharacter(keyId, hpRefill, /*version=*/1);
                const bool waited = sendAndWaitAck(cq, p, srcTh, Key(keyU64),
                                                   std::span<const uint8_t>(value), 1,
                                                   timeoutMs, done);
                if (!waited || done.status != OpStatus::Ok) {
                    std::cerr << "PROBE: FAIL re-produce(character) waited=" << waited
                              << " status=" << static_cast<int>(done.status) << "\n";
                    return 14;
                }
                std::cerr << "PROBE: re-produced character id=" << keyId << " hp=" << hpRefill
                          << " offset=" << done.value << "\n";
            }

            const ViewRowResult rt1 = waitForRow(c, viewIdT, keyU64, fpT, boundMs, timeoutMs);
            if (!decodeAndCheck(rt1, keyId, hpRefill, "char_transient (refilled)")) return 15;

            std::cout << "VIEW-ENGINE-E2E: OK\n";
            std::cout.flush();
            c.close();
            return 0;
        }

        std::cerr << "PROBE: FAIL unknown RUNE_MODE='" << mode << "' (want setup|verify_restart)\n";
        c.close();
        return 20;
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL exception: " << ex.what() << "\n";
        return 21;
    }
}
