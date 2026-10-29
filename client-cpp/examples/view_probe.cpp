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
//   6. (Phase 2b C) typed_route: an 8-partition typed topic routed by key.
//   7. (Phase 2b E/E2) defineTopicAndWait("presence_t", Persistence::Transient);
//      produce with acks None/Leader/All (offsets 0-2); subscribe from earliest
//      (position 3), offset 2 (one record), latest (exactly the next record); a
//      committed and an aborted transaction + one record — read_committed sees
//      exactly 7.
//
// Modes (RUNE_MODE); setup + verify_restart drive the docker/view-engine-e2e.sh restart leg
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
//     VIEW_ORDER_TOPIC_ID=<id>
//     VIEW_TOPIC_ID_AGG=<id>
//     VIEW_FP_AGG=<fp>
//     VIEW_TT_TOPIC_ID=<id>
//     VIEW-PROBE-SETUP: OK
//   to stdout — the e2e script scrapes these into env vars for mode 2.
//
//   RUNE_MODE=verify_restart: takes the above as RUNE_VIEW_* env vars (set by
//   the e2e script after it has restarted the broker and awaited a fresh
//   leader). Asserts:
//     (a) the PERSISTED view still answers OK with the ORIGINAL row (the
//         broker's readiness gate replayed it from the source log / its own
//         checkpoint across the restart — no reproduce needed);
//     (b) the TRANSIENT view ALSO answers OK with the ORIGINAL row: it has
//         no checkpoint, so the readiness gate rebuilds it by re-folding the
//         retained source log from its floor (spec §9.7) — the source topic
//         is persisted, so the row comes back;
//     (c) re-producing the SAME key with a NEW hp value is folded live by
//         BOTH views after the restart — each converges to the NEW value;
//     (d) Phase 2a/2b: the PERSISTED aggregate view user_totals (GROUP BY userId:
//         Count(*), Sum, Avg, Max, Min, Last over "order") recovers its rows,
//         contributor map, MRU lists/caches AND Min/Max value blocks from the v2 checkpoint:
//         u100 = {3, 85, 85/3, max 50, min 5, last 5}, u200 absent (emptied by a
//         pre-restart group move); o3 5 -> 25 retracts the pre-image AND the
//         cached min -> {3, 105, 35, 50, 25, 25}; o1 50 -> 20 retracts the cached
//         max -> {3, 75, 25, 30, 20, 20} (both rescans scan the recovered value block).
//     (e) (Phase 2b E/E2) presence_t: TopicExists with the same id, first
//         produce at offset 0, subscribe and read_committed from earliest ->
//         position 1.
//   Prints "VIEW-ENGINE-E2E: OK" on success.
//
//   RUNE_MODE=ddl_race (DDL conformance): connect, print "DDL-RACE: READY role=<a|b>",
//     wait for <RUNE_BARRIER_DIR>/go, then race the other container: define
//     "race_topic" (a: disk, b: transient), createView "race_view", and a/b define vs
//     createView "race_mixed".  Prints one "DDL_RACE leg=<leg> role=<r> code=<n>" per leg.
//   RUNE_MODE=catalog_dump: getTopicSchema for each RUNE_CATALOG_NAMES entry, one
//     "CATALOG name=… code=… topicId=… version=… parts=… fp=…" line each.
//   RUNE_MODE=group_commit (E2 final fix wave C1; after setup): a consumer group
//     ("e2-presence-group", earliest) consumes presence_t (RUNE_VIEW_TT_TOPIC_ID)
//     to its end (RUNE_VIEW_TT_END) and commits that position.  Prints
//     "GROUP-COMMIT: OK committed=<n>".  Exit codes 150..152.
//     RUNE_GROUP_TOPIC (default presence_t) names the topic; RUNE_GROUP_PRODUCE
//     (default 0) records are produced first (the 3-node leg's empty topic).
//   RUNE_MODE=group_restart (C1; after verify_restart): the ring restarted
//     empty and holds fewer records than the group committed.  Produces two
//     more (offsets 1, 2 after verify_restart's 0); the SAME group rejoins and
//     must deliver every post-restart record (0, 1, 2) exactly once — never
//     resume at the committed position of the ring's previous life (silent
//     loss).  Prints "GROUP-RESTART: OK".  Exit codes 153..157.
//   RUNE_MODE=transient_failover (3-node, docker/transient-failover-e2e.sh): a
//     read_committed + a Latest read_committed subscription on the transient
//     topic "presence_failover"; prints FAILOVER_LEADER=<node> and
//     "FAILOVER: READY", then (the script stops that leader) asserts both move,
//     keep receiving, lose nothing produced around the move and never
//     re-deliver.  Prints "TRANSIENT-FAILOVER-E2E: OK" (see transientFailoverMode).
//
// `view_probe --create-only` (catalog docker e2e, docker/catalog-e2e.sh): create the view RUNE_VIEW_NAME over
// the existing topic RUNE_VIEW_SOURCE and exit (0 = Ok; 1 = code + detail on stderr), reading no rows.
//
// Environment (all modes):
//   RUNE_TARGET / RUNE_TARGET_STREAM / RUNE_EGRESS / RUNE_EGRESS_STREAM /
//   RUNE_DATA_DIR / RUNE_TIMEOUT_MS — same as rune_schema_probe.cpp.
//
// Design: bounded/time-independent waits everywhere (poll to a deadline,
// never a fixed sleep as a correctness gate) — CLAUDE.md engineering
// philosophy #5/#7 (test everything; regression-grade e2e).

#include "ExampleAuth.h"
#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/view/ViewPlanAuthor.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/group/GroupConsumer.h>

#include "characters.gen.h"   // Phase 2b C: build-time codegen (tests/generated/characters.blob)

#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <set>
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

// Poll getViewRow until the row decodes to (wantId, wantHp) or the bound
// expires — used after a re-produce, where the view first still serves the
// OLD row (a plain waitForRow would return on that stale OK).
static ViewRowResult waitForHp(Client& c, int32_t viewTopicId, uint64_t key, uint64_t fp,
                               int64_t wantId, int64_t wantHp, int boundMs, int perCallTimeoutMs) {
    const rcv::RowLayout layout = charHpRowLayout();
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewTopicId, key, fp, std::chrono::milliseconds(perCallTimeoutMs));
        if (r.status == ViewRowResult::kOk && r.rowBytes.size() == layout.rowLen) {
            const auto idF = rcv::readSlot(std::span<const uint8_t>(r.rowBytes), layout, 0);
            const auto hpF = rcv::readSlot(std::span<const uint8_t>(r.rowBytes), layout, 1);
            if (idF.present && hpF.present && idF.i == wantId && hpF.i == wantHp) return r;
        }
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

// ---------------------------------------------------------------------------
// Phase 2a: "order" source {orderId: Int64 (key), userId: Int64, amount: Int64}
// and the aggregate view user_totals = GROUP BY userId: Count(*), Sum, Avg.
// ---------------------------------------------------------------------------
static rune::codec::Descriptor orderDescriptor() {
    return rune::codec::DescriptorBuilder("order", 1)
        .add("orderId", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("userId", rune::codec::PrimType::Int64)
        .add("amount", rune::codec::PrimType::Int64)
        .build();
}
// [blockLength u16 = 24][version u16 = 1][orderId i64][userId i64][amount i64]
static std::vector<uint8_t> encodeOrder(int64_t orderId, int64_t userId, int64_t amount) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(24); le16(1); le64(orderId); le64(userId); le64(amount);
    return b;
}
static rcv::ViewPlan userTotalsPlan(int32_t srcTopicId) {
    using P = rune::codec::PrimType;
    rcv::ViewPlan p;
    p.viewName = "user_totals"; p.key = "userId"; p.persistence = 0;
    p.sources.push_back(rcv::SourceSpec{srcTopicId, "userId"});
    p.output.push_back(rcv::AuthoredField{"userId", P::Int64, 0, "userId", rcv::MergeRule::Replace});
    p.output.push_back(rcv::AuthoredField{"n", P::Int64, 0, "", rcv::MergeRule::CountAll});
    p.output.push_back(rcv::AuthoredField{"total", P::Int64, 0, "amount", rcv::MergeRule::Sum});
    p.output.push_back(rcv::AuthoredField{"avg", P::Float64, 0, "amount", rcv::MergeRule::Avg});
    p.output.push_back(rcv::AuthoredField{"max", P::Int64, 0, "amount", rcv::MergeRule::Max});
    p.output.push_back(rcv::AuthoredField{"min", P::Int64, 0, "amount", rcv::MergeRule::Min});
    p.output.push_back(rcv::AuthoredField{"last", P::Int64, 0, "amount", rcv::MergeRule::Last});
    return p;
}
// Slot order == userTotalsPlan output order: userId, n, total, avg, max, min, last.
static rcv::RowLayout userTotalsRowLayout() {
    using P = rune::codec::PrimType;
    const P types[] = {P::Int64, P::Int64, P::Int64, P::Float64, P::Int64, P::Int64, P::Int64};
    return rcv::buildRowLayout(types);
}
// Sends one order (acked); false + message on failure.
static bool sendOrder(Client& c, const TopicHandle& th, int64_t o, int64_t u, int64_t amt, int timeoutMs) {
    // Acks::All: the e2e restarts a broker after this (#51)
    ProducerConfig pcfg; pcfg.acks = Acks::All;
    pcfg.produceTimeoutMs = timeoutMs;
    auto p = c.producer(pcfg);
    Completions cq(c); Completion done;
    const auto v = encodeOrder(o, u, amt);
    if (!sendAndWaitAck(cq, p, th, Key(static_cast<uint64_t>(o)), std::span<const uint8_t>(v), 1, timeoutMs, done) ||
        done.status != OpStatus::Ok) {
        std::cerr << "PROBE: FAIL produce(order " << o << ") status=" << static_cast<int>(done.status) << "\n";
        return false;
    }
    return true;
}
// Expected user_totals row: Phase 2a columns + Phase 2b max/min/last.
struct Totals { int64_t n; int64_t total; double avg; int64_t max; int64_t min; int64_t last; };
// Polls user `u` until the row is {userId=u, want...} with every slot present
// (non-null; avg within 1e-9) or the bound expires.
static bool waitForTotals(Client& c, int32_t viewId, uint64_t fp, int64_t u, const Totals& w,
                          int boundMs, int timeoutMs, const char* who) {
    const rcv::RowLayout L = userTotalsRowLayout();
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    ViewRowResult r;
    for (;;) {
        r = c.getViewRowAndWait(viewId, static_cast<uint64_t>(u), fp, std::chrono::milliseconds(timeoutMs));
        if (r.status == ViewRowResult::kOk && r.rowBytes.size() == L.rowLen) {
            const auto row = std::span<const uint8_t>(r.rowBytes);
            const auto uf = rcv::readSlot(row, L, 0), nf = rcv::readSlot(row, L, 1),
                       tf = rcv::readSlot(row, L, 2), af = rcv::readSlot(row, L, 3),
                       xf = rcv::readSlot(row, L, 4), mf = rcv::readSlot(row, L, 5),
                       lf = rcv::readSlot(row, L, 6);
            const bool allPresent = uf.present && nf.present && tf.present && af.present &&
                                    xf.present && mf.present && lf.present;
            if (allPresent && uf.i == u && nf.i == w.n && tf.i == w.total && std::abs(af.f - w.avg) < 1e-9 &&
                xf.i == w.max && mf.i == w.min && lf.i == w.last) {
                std::cerr << "PROBE: " << who << " OK userId=" << u << " n=" << w.n << " total=" << w.total
                          << " avg=" << w.avg << " max=" << w.max << " min=" << w.min << " last=" << w.last << "\n";
                return true;
            }
            if (std::chrono::steady_clock::now() >= dl) {
                std::cerr << "PROBE: FAIL " << who << " got userId=" << uf.i << " n=" << nf.i << " total=" << tf.i
                          << " avg=" << af.f << " max=" << xf.i << " min=" << mf.i << " last=" << lf.i
                          << " (allPresent=" << allPresent << ")"
                          << " want n=" << w.n << " total=" << w.total << " avg=" << w.avg << " max=" << w.max
                          << " min=" << w.min << " last=" << w.last << "\n";
                return false;
            }
        } else if (std::chrono::steady_clock::now() >= dl) {
            std::cerr << "PROBE: FAIL " << who << " status=" << r.status << "\n";
            return false;
        }
        std::this_thread::sleep_for(50ms);
    }
}
// Polls until user `u` reads AbsentKey (group emptied) or the bound expires.
static bool waitForAbsent(Client& c, int32_t viewId, uint64_t fp, int64_t u, int boundMs, int timeoutMs,
                          const char* who) {
    const ViewRowResult r = waitResolved(c, viewId, static_cast<uint64_t>(u), fp, boundMs, timeoutMs);
    if (r.status != ViewRowResult::kAbsentKey) {
        std::cerr << "PROBE: FAIL " << who << " status=" << r.status << " (want AbsentKey)\n";
        return false;
    }
    std::cerr << "PROBE: " << who << " correctly AbsentKey\n";
    return true;
}

// ---------------------------------------------------------------------------
// Phase 2b Part C: typed topics route by key (real broker, real Aeron).
//
// defineTopic "typed_route" with 8 partitions and the generated Characters
// schema (userId is the key field) -> GetTopicSchema must report
// partitionCount 8 -> openTopicAndWait<Characters> binds TopicHandle{id, 8}
// -> 64 typed sends -> subscribe to EVERY partition from earliest -> every
// record must sit on Key(userId).partition(8) with record key == Key(userId)
// bytes, and the set of partitions hit must equal the expected set (>1).
// Pre-2b every typed record landed on partition 0 with an empty key.
// Returns 0 on success, else a probe exit code (50..56) — its own range: the
// verify_restart aggregate leg uses 40..44, so a code names exactly one check.
// ---------------------------------------------------------------------------
static int typedRoutingLeg(Client& c, std::chrono::milliseconds wait, int boundMs) {
    constexpr int32_t kParts   = 8;
    constexpr int     kRecords = 64;

    const auto blob = rune::codec::serialize(rune::gen::Characters::descriptor());
    DefineTopicResult def = c.defineTopicAndWait("typed_route", kParts, /*keyField=*/"userId", std::span<const uint8_t>(blob), wait);
    if (def.errorCode != 0) {
        std::cerr << "PROBE: FAIL defineTopic(typed_route) errorCode=" << def.errorCode << "\n";
        return 50;
    }

    const TopicSchemaResult sr = c.getTopicSchemaAndWait("typed_route", wait);
    if (sr.errorCode != 0 || sr.partitionCount != kParts) {
        std::cerr << "PROBE: FAIL getTopicSchema(typed_route) errorCode=" << sr.errorCode
                  << " partitionCount=" << sr.partitionCount << " (want " << kParts << ")\n";
        return 51;
    }

    TypedTopic<rune::gen::Characters> topic =
        c.openTopicAndWait<rune::gen::Characters>("typed_route", wait);
    if (topic.handle().topicId != def.topicId || topic.handle().partitionCount != kParts) {
        std::cerr << "PROBE: FAIL typed handle {" << topic.handle().topicId << ","
                  << topic.handle().partitionCount << "} want {" << def.topicId << "," << kParts << "}\n";
        return 52;
    }

    std::set<int32_t> expectedParts;
    for (int i = 0; i < kRecords; ++i) {
        rune::gen::Characters rec;
        rec.id       = 9000 + i;
        rec.userId   = 1000 + i;
        rec.strength = i;
        rec.name     = "typed";
        topic.send(rec);
        expectedParts.insert(Key(static_cast<int64_t>(rec.userId)).partition(kParts));
    }

    struct Seen { int32_t partition; int64_t userId; bool keyMatches; };
    std::mutex        mtx;
    std::vector<Seen> seen;
    seen.reserve(static_cast<std::size_t>(kRecords));

    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;
    const TopicHandle th{def.topicId, kParts};
    std::vector<Subscription> subs;
    subs.reserve(static_cast<std::size_t>(kParts));
    for (int32_t p = 0; p < kParts; ++p) {
        subs.push_back(c.subscribe(th, p, opts, [&, p](const Record& r) {
            rune::gen::Characters d;
            if (!d.decode(r.value)) return;   // #418: an unknown version is missing, never misread
            const Key want(static_cast<int64_t>(d.userId));
            const bool keyOk = r.key.size() == want.size() &&
                               std::equal(r.key.begin(), r.key.end(), want.bytes().begin());
            std::lock_guard<std::mutex> lk(mtx);
            seen.push_back(Seen{p, d.userId, keyOk});
        }));
    }

    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (std::chrono::steady_clock::now() < dl) {
        { std::lock_guard<std::mutex> lk(mtx); if (static_cast<int>(seen.size()) >= kRecords) break; }
        std::this_thread::sleep_for(10ms);   // poll to a deadline — the count decides, not the sleep
    }
    for (auto& s : subs) s.unsubscribe();

    std::lock_guard<std::mutex> lk(mtx);
    if (static_cast<int>(seen.size()) != kRecords) {
        std::cerr << "PROBE: FAIL typed_route consumed " << seen.size() << " / " << kRecords << "\n";
        return 53;
    }
    std::set<int32_t> gotParts;
    for (const Seen& s : seen) {
        const int32_t want = Key(static_cast<int64_t>(s.userId)).partition(kParts);
        if (!s.keyMatches) {
            std::cerr << "PROBE: FAIL typed_route userId=" << s.userId << " record key != Key(userId)\n";
            return 54;
        }
        if (s.partition != want) {
            std::cerr << "PROBE: FAIL typed_route userId=" << s.userId << " on partition " << s.partition
                      << " want " << want << "\n";
            return 55;
        }
        gotParts.insert(s.partition);
    }
    if (gotParts != expectedParts || gotParts.size() < 2) {
        std::cerr << "PROBE: FAIL typed_route partitions hit=" << gotParts.size()
                  << " expected=" << expectedParts.size() << "\n";
        return 56;
    }
    std::cerr << "PROBE: typed_route OK — " << kRecords << " records over " << gotParts.size()
              << " partitions, each on Key(userId).partition(8) with record key == Key(userId)\n";
    std::cout << "TYPED-ROUTING: OK parts=" << kParts << " records=" << kRecords
              << " partitionsHit=" << gotParts.size() << "\n";
    return 0;
}

// ---------------------------------------------------------------------------
// DDL conformance legs (rune-broker DDL conformance plan, Task 13).
// ---------------------------------------------------------------------------
static uint64_t fnv1a64(const std::vector<uint8_t>& b) {
    uint64_t h = 1469598103934665603ULL;
    for (const uint8_t x : b) { h ^= x; h *= 1099511628211ULL; }
    return h;
}

static bool waitForGo(const std::string& dir, int boundMs) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (std::chrono::steady_clock::now() < deadline) {
        std::error_code ec;
        if (std::filesystem::exists(dir + "/go", ec)) return true;
        std::this_thread::sleep_for(10ms);   // poll interval, not a correctness gate
    }
    return false;
}

static int ddlRaceMode(Client& c, const std::string& role, std::chrono::milliseconds wait, int boundMs) {
    const std::string barrier = getEnvStr("RUNE_BARRIER_DIR", "/barrier");
    const TopicSchemaResult src = c.getTopicSchemaAndWait("character", wait);
    if (src.errorCode != 0) {
        std::cerr << "PROBE: FAIL ddl_race getTopicSchema(character) errorCode=" << src.errorCode << "\n";
        return 70;
    }
    std::cout << "DDL-RACE: READY role=" << role << "\n";
    std::cout.flush();
    if (!waitForGo(barrier, boundMs)) { std::cerr << "PROBE: FAIL ddl_race no go signal\n"; return 71; }

    const auto blob = rune::codec::serialize(characterDescriptor());
    const DefineTopicResult t = c.defineTopicAndWait(
        "race_topic", 1, "id", std::span<const uint8_t>(blob),
        role == "a" ? Persistence::Disk : Persistence::Transient, wait);
    std::cout << "DDL_RACE leg=topic role=" << role << " code=" << t.errorCode << "\n";

    const auto plan = rcv::serializeAuthored(charHpPlan("race_view", src.topicId, 0));
    const CreateViewResult v = c.createViewAndWait("race_view", "id", 0, std::span<const uint8_t>(plan), wait);
    std::cout << "DDL_RACE leg=view role=" << role << " code=" << v.errorCode << "\n";

    uint16_t mixed = 0;
    if (role == "a") {
        mixed = c.defineTopicAndWait("race_mixed", 1, "id", std::span<const uint8_t>(blob), wait).errorCode;
    } else {
        const auto mplan = rcv::serializeAuthored(charHpPlan("race_mixed", src.topicId, 0));
        mixed = c.createViewAndWait("race_mixed", "id", 0, std::span<const uint8_t>(mplan), wait).errorCode;
    }
    std::cout << "DDL_RACE leg=mixed role=" << role << " code=" << mixed << "\n";
    std::cout << "DDL-RACE: DONE role=" << role << "\n";
    std::cout.flush();
    return 0;
}

static int catalogDumpMode(Client& c, std::chrono::milliseconds wait) {
    const std::string names = getEnvStr("RUNE_CATALOG_NAMES");
    if (names.empty()) { std::cerr << "PROBE: FAIL catalog_dump needs RUNE_CATALOG_NAMES\n"; return 73; }
    std::size_t start = 0;
    while (start <= names.size()) {
        const std::size_t comma = names.find(',', start);
        const std::string name = names.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
        const TopicSchemaResult r = c.getTopicSchemaAndWait(name, wait);
        std::cout << "CATALOG name=" << name << " code=" << r.errorCode << " topicId=" << r.topicId
                  << " version=" << r.version << " parts=" << r.partitionCount
                  << " fp=" << fnv1a64(r.descriptorBlob) << "\n";
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    std::cout << "CATALOG-DUMP: OK\n";
    std::cout.flush();
    return 0;
}

// ---------------------------------------------------------------------------
// Phase 2b E/E2 Task 14: transient-topic helpers.
// ---------------------------------------------------------------------------

// Subscribe with `from` / `iso` and wait (bounded) until the subscription's
// published position reaches `atLeast`; returns the final position after
// unsubscribe + finished().  position() comes off the subscription's own status
// ring.  *seenOut = records the handler saw — read only after finished(): the
// handler never runs again (Subscription's documented resume pattern), so no
// state is shared with the duty thread while it can still run.
static int64_t subscribeUntil(Client& c, int32_t topicId, From from, Isolation iso, int64_t atLeast, int boundMs,
                              int64_t* seenOut = nullptr) {
    SubscribeOptions opts;
    opts.from      = from;
    opts.isolation = iso;
    int64_t seen = 0;
    auto sub = c.subscribe(TopicHandle{topicId, 1}, /*partition=*/0, opts, [&seen](const Record&) { ++seen; });
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (sub.position() < atLeast && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);   // poll interval, not correctness
    sub.unsubscribe();
    const auto fin = std::chrono::steady_clock::now() + 5s;
    while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    if (seenOut != nullptr) *seenOut = seen;
    return sub.position();
}

// One record with the given acks; offsetOut = its offset (-1 for Acks::None: the
// client reports Ok when the record is handed to the transport).
static bool produceOne(Client& c, int32_t topicId, int64_t key, Acks acks, int timeoutMs, int64_t& offsetOut) {
    ProducerConfig pcfg;
    pcfg.acks             = acks;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);
    Completions cq(c);
    Completion  done;
    const auto value = encodeCharacter(key, 1, /*version=*/1);
    const bool ok = sendAndWaitAck(cq, p, TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(key)),
                                   std::span<const uint8_t>(value), 1, timeoutMs, done);
    offsetOut = done.value;
    return ok && done.status == OpStatus::Ok;
}

// One transaction of `n` records on the transient topic, committed or aborted.
static bool transientTxn(Client& c, int32_t topicId, const std::string& txnId, int64_t firstKey, int n, bool commit,
                         int timeoutMs) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    pcfg.transactionalId  = txnId;
    try {
        auto tp = c.producer(pcfg);
        tp.initTransactionsAndWait(std::chrono::milliseconds(timeoutMs * 2));
        tp.beginTransaction();
        for (int i = 0; i < n; ++i) {
            const auto value = encodeCharacter(firstKey + i, 1, 1);
            tp.send(TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(firstKey + i)), std::span<const uint8_t>(value));
        }
        if (commit) {
            tp.commitTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
        } else {
            tp.flushAndWait(std::chrono::milliseconds(timeoutMs * 2));   // the aborted records reach the log
            tp.abortTransactionAndWait(std::chrono::milliseconds(timeoutMs * 2));
        }
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL transient txn (" << (commit ? "commit" : "abort") << "): " << ex.what() << "\n";
        return false;
    }
    return true;
}

// ---------------------------------------------------------------------------
// RUNE_MODE=transient_failover (Task 14, Task 13 re-review N4) — the 3-node
// leg, driven by docker/transient-failover-e2e.sh.  A real client against a
// real cluster: a read_committed subscription on a (static) transient topic that has
// delivered data, plus a Latest read_committed subscription that has delivered
// NOTHING, both bound to the partition leader; the script then stops that
// leader's container (SIGTERM = controlled shutdown: leadership hands off, the
// demoted node answers both subscriptions a position note + NotLeader).
//
// Asserts:
//   (a) both subscriptions move to the new leader and keep receiving committed
//       data — bounded waits that fail loudly on a stall;
//   (b) the Latest subscription delivers EVERY record from its bind-time
//       position on: an open transaction (offsets 5..9, replicated, so the
//       HW is past it) is flushed before the move and committed after it on
//       the new leader — re-resolving Latest (= the HW) there would skip it;
//       only resuming at the broker's position note (5) delivers it.  Plus
//       200 acks=all records produced around the move;
//   (c) no record is delivered twice, on either subscription.
//
// Records are counted per offset in handler-owned arrays read only after
// finished() (no state shared with the duty thread while it can run).  The
// leader node id is printed from the read_committed handler — a duty-cycle
// callback, where Client::testLeaderFor may be called — and the main thread
// only ever reads the subscriptions' own status rings.
// ---------------------------------------------------------------------------
static int transientFailoverMode(Client& c, std::chrono::milliseconds wait, int timeoutMs, int boundMs) {
    constexpr int     kPre      = 5;      // records before the move (offsets 0..4)
    constexpr int     kTxn      = 5;      // an open transaction before the move (offsets 5..9)
    constexpr int     kAround   = 200;    // records produced around the move
    constexpr int     kMaxOff   = 4096;   // per-offset delivery counters
    const int         moveBoundMs = getEnvInt("RUNE_MOVE_BOUND_MS", 180000);

    // A transient topic created by DDL (rune-broker docker/topics/transient-failover.rql, id from the script).
    (void)wait;
    struct { int32_t topicId; } tt{static_cast<int32_t>(getEnvI64("RUNE_FAILOVER_TOPIC_ID", -1))};
    if (tt.topicId < 0) { std::cerr << "PROBE: FAIL transient_failover needs RUNE_FAILOVER_TOPIC_ID\n"; return 100; }
    const TopicHandle th{tt.topicId, 1};

    for (int i = 0; i < kPre; ++i) {
        int64_t off = -1;
        if (!produceOne(c, tt.topicId, i, Acks::All, timeoutMs, off) || off != i) {
            std::cerr << "PROBE: FAIL pre-move produce #" << i << " offset=" << off << "\n"; return 101;
        }
    }

    // Handler-owned state, then the subscriptions, then a guard that — on
    // EVERY exit — unsubscribes both and waits (bounded) for finished(): no
    // handler can run once the state it writes is gone (~Subscription never
    // waits).
    std::vector<uint16_t> rcCount(kMaxOff, 0), ltCount(kMaxOff, 0);
    int64_t rcOutOfRange = 0, ltOutOfRange = 0;
    bool    leaderPrinted = false;
    int32_t leaderFirst = -2, leaderLast = -2;   // handler-owned: the partition leader at the first / last record
    std::optional<Subscription> rc, lt;
    struct Quiesce {
        std::optional<Subscription>& a;
        std::optional<Subscription>& b;
        ~Quiesce() {
            if (a) a->unsubscribe();
            if (b) b->unsubscribe();
            const auto fin = std::chrono::steady_clock::now() + 5s;
            while (((a && !a->finished()) || (b && !b->finished())) && std::chrono::steady_clock::now() < fin)
                std::this_thread::sleep_for(1ms);
        }
    } quiesce{rc, lt};

    // (1) read_committed from earliest: delivers 0..4 before the move.
    SubscribeOptions rco;
    rco.from      = From::earliest();
    rco.isolation = Isolation::ReadCommitted;
    rc.emplace(c.subscribe(th, 0, rco, [&](const Record& r) {
        if (r.offset >= 0 && r.offset < kMaxOff) ++rcCount[static_cast<std::size_t>(r.offset)]; else ++rcOutOfRange;
        leaderLast = c.testLeaderFor(tt.topicId, 0);   // duty thread: the hook's documented caller
        if (!leaderPrinted) {
            leaderPrinted = true;
            leaderFirst   = leaderLast;
            std::cout << "FAILOVER_LEADER=" << c.testLeaderFor(tt.topicId, 0) << "\n";
            std::cout.flush();
        }
    }));
    auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (rc->position() < kPre && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
    if (rc->position() != kPre) { std::cerr << "PROBE: FAIL read_committed pre-move position=" << rc->position() << " (want " << kPre << ")\n"; return 102; }

    // (2) Latest, read_committed: bound, and NOTHING is produced until it moved.
    // read_committed: a demoted leader answers exactly its read_committed
    // subscriptions NotLeader (a read_uncommitted one keeps being served by the
    // node as a follower until the node is gone).
    SubscribeOptions lto;
    lto.from      = From::latest();
    lto.isolation = Isolation::ReadCommitted;
    lt.emplace(c.subscribe(th, 0, lto, [&](const Record& r) {
        if (r.offset >= 0 && r.offset < kMaxOff) ++ltCount[static_cast<std::size_t>(r.offset)]; else ++ltOutOfRange;
    }));
    dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while ((lt->subscriptionId() <= 0 || rc->subscriptionId() <= 0) && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);
    const int64_t rcId0 = rc->subscriptionId();
    const int64_t ltId0 = lt->subscriptionId();
    if (rcId0 <= 0 || ltId0 <= 0) { std::cerr << "PROBE: FAIL subscriptions not bound (rc=" << rcId0 << " latest=" << ltId0 << ")\n"; return 103; }
    std::cerr << "PROBE: bound read_committed id=" << rcId0 << " (position " << rc->position()
              << ") + Latest id=" << ltId0 << " (position " << lt->position() << ", nothing delivered)\n";
    // (2b) An OPEN transaction of kTxn records (offsets 5..9), flushed: acks=all
    // replicated, so the HW passes them, but read_committed delivers nothing of
    // it.  Latest resolves to the HW (GR-6): re-resolving it on the new leader
    // would start at 10 and lose these records once committed; resuming at the
    // position note (5) delivers them.  The Latest subscription still has
    // delivered nothing when it moves.  Deterministic: no race with the
    // re-subscribe.
    std::optional<Producer> tp;
    {
        ProducerConfig tcfg;
        tcfg.acks             = Acks::All;
        tcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
        tcfg.transactionalId  = "e2-failover-txn";
        try {
            tp.emplace(c.producer(tcfg));
            tp->initTransactionsAndWait(std::chrono::milliseconds(timeoutMs * 2));
            tp->beginTransaction();
            for (int i = 0; i < kTxn; ++i) {
                const auto value = encodeCharacter(500 + i, 1, 1);
                tp->send(th, Key(static_cast<uint64_t>(500 + i)), std::span<const uint8_t>(value));
            }
            tp->flushAndWait(std::chrono::milliseconds(timeoutMs * 2));
        } catch (const std::exception& ex) {
            std::cerr << "PROBE: FAIL open transaction before the move: " << ex.what() << "\n";
            return 120;
        }
    }
    std::this_thread::sleep_for(200ms);   // settle: give a (wrong) delivery the chance to show below
    if (lt->position() > kPre || rc->position() > kPre) {
        std::cerr << "PROBE: FAIL read_committed delivered an OPEN transaction's record (Latest position "
                  << lt->position() << ", read_committed " << rc->position() << ")\n";
        return 121;
    }
    std::cerr << "PROBE: open transaction (offsets " << kPre << ".." << kPre + kTxn - 1
              << ") flushed; nothing of it delivered\n";
    std::cout << "FAILOVER: READY topicId=" << tt.topicId << "\n";
    std::cout.flush();

    // (3) The script stops the leader.  The Latest subscription moving (its id
    // leaves ltId0: the note + NotLeader arrived and it re-subscribes) starts
    // the burst — never earlier, so the subscription has delivered nothing
    // when it moves.
    dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(moveBoundMs);
    while (lt->subscriptionId() == ltId0 && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(1ms);
    if (lt->subscriptionId() == ltId0) {
        std::cerr << "PROBE: FAIL STALL: the Latest subscription never moved off the stopped leader within "
                  << moveBoundMs << " ms (still id " << ltId0 << ")\n";
        return 104;
    }
    const auto tMove = std::chrono::steady_clock::now();
    std::cerr << "PROBE: Latest subscription moved (id " << ltId0 << " -> " << lt->subscriptionId()
              << ", position " << lt->position() << ")\n";

    // (3b) Commit the open transaction on the new leader.
    try {
        tp->commitTransactionAndWait(std::chrono::milliseconds(boundMs));
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL commit after the move: " << ex.what() << "\n";
        return 122;
    }
    std::cerr << "PROBE: transaction committed after the move\n";

    // (4) Records around the move: async acks=All sends, paced 2 ms; the
    // producer follows the leader on its own (NotLeader / Redirect retries).
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;
    pcfg.produceTimeoutMs = static_cast<int64_t>(boundMs);
    auto p = c.producer(pcfg);
    Completions cq(c);
    std::vector<int64_t> ackedOffsets;
    ackedOffsets.reserve(kAround);
    int  failedSends = 0;
    int  done        = 0;
    int64_t ackedBeforeRebind = 0;   // acks the client saw before the Latest re-bind
    int64_t ltIdNow  = lt->subscriptionId();
    bool rebound     = ltIdNow > 0;
    const auto drain = [&] {
        (void)cq.poll([&](const Completion& cc, std::span<const uint8_t>) {
            ++done;
            if (cc.status == OpStatus::Ok) {
                ackedOffsets.push_back(cc.value);
                if (!rebound) ++ackedBeforeRebind;
            } else {
                ++failedSends;
                std::cerr << "PROBE: send token=" << cc.token << " status=" << static_cast<int>(cc.status)
                          << " code=" << cc.errorCode << " (in doubt: may or may not be in the log)\n";
            }
        });
        if (!rebound && lt->subscriptionId() > 0) {
            rebound = true;
            std::cerr << "PROBE: Latest re-bound id=" << lt->subscriptionId() << " after "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - tMove).count()
                      << " ms; acks seen before it: " << ackedBeforeRebind << "\n";
        }
    };
    for (int i = 0; i < kAround; ++i) {
        const auto value = encodeCharacter(1000 + i, 1, 1);
        for (;;) {
            const SendResult r = p.trySend(th, Key(static_cast<uint64_t>(1000 + i)), std::span<const uint8_t>(value), cq,
                                           static_cast<Token>(i + 1));
            if (r == SendResult::Ok) break;
            if (r == SendResult::QueueFull || r == SendResult::BackPressured) { drain(); std::this_thread::sleep_for(1ms); continue; }
            std::cerr << "PROBE: FAIL trySend #" << i << " result=" << static_cast<int>(r) << "\n";
            return 105;
        }
        drain();
        std::this_thread::sleep_for(2ms);
    }
    dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (done < kAround && std::chrono::steady_clock::now() < dl) { drain(); std::this_thread::sleep_for(2ms); }
    if (done < kAround) { std::cerr << "PROBE: FAIL STALL: only " << done << "/" << kAround << " sends completed\n"; return 106; }
    if (ackedOffsets.empty()) { std::cerr << "PROBE: FAIL no record acked around the move\n"; return 107; }
    // Every send around the move must be acked — a refused / in-doubt one would
    // make "nothing lost around the move" vacuous.
    if (static_cast<int>(ackedOffsets.size()) != kAround || failedSends != 0) {
        std::cerr << "PROBE: FAIL acked " << ackedOffsets.size() << "/" << kAround << ", in doubt " << failedSends
                  << " (every record around the move must be acked)\n";
        return 125;
    }
    {
        std::vector<int64_t> sorted = ackedOffsets;
        std::sort(sorted.begin(), sorted.end());
        if (std::adjacent_find(sorted.begin(), sorted.end()) != sorted.end()) {
            std::cerr << "PROBE: FAIL two acks named the same offset\n";
            return 127;
        }
    }
    const int64_t maxAcked = *std::max_element(ackedOffsets.begin(), ackedOffsets.end());
    const int64_t minAcked = *std::min_element(ackedOffsets.begin(), ackedOffsets.end());
    if (minAcked < 0) { std::cerr << "PROBE: FAIL an Ok ack carried offset " << minAcked << "\n"; return 126; }
    if (maxAcked >= kMaxOff) { std::cerr << "PROBE: FAIL acked offset " << maxAcked << " beyond the counters\n"; return 108; }

    // (a) both keep receiving committed data on the new leader — bounded.
    dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while ((rc->position() <= maxAcked || lt->position() <= maxAcked) && std::chrono::steady_clock::now() < dl) {
        drain();
        std::this_thread::sleep_for(5ms);
    }
    const int64_t rcPos = rc->position(), ltPos = lt->position();
    const int64_t rcId1 = rc->subscriptionId(), ltId1 = lt->subscriptionId();
    rc->unsubscribe();
    lt->unsubscribe();
    const auto fin = std::chrono::steady_clock::now() + 5s;
    while ((!rc->finished() || !lt->finished()) && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    if (!rc->finished() || !lt->finished()) { std::cerr << "PROBE: FAIL subscriptions did not finish after unsubscribe\n"; return 109; }
    std::cerr << "PROBE: after the move: read_committed id " << rcId0 << " -> " << rcId1 << " position " << rcPos
              << "; Latest id " << ltId0 << " -> " << ltId1 << " position " << ltPos << "; acked " << ackedOffsets.size()
              << "/" << kAround << " (offsets " << minAcked << ".." << maxAcked << "), in doubt " << failedSends << "\n";
    if (rcPos <= maxAcked || ltPos <= maxAcked) {
        std::cerr << "PROBE: FAIL STALL: positions read_committed=" << rcPos << " Latest=" << ltPos
                  << " never passed the last acked offset " << maxAcked << " within " << boundMs << " ms\n";
        return 110;
    }
    std::cout << "FAILOVER_NEW_LEADER=" << leaderLast << "\n";
    if (getEnvInt("RUNE_EXPECT_LEADER_CHANGE", 1) != 0 && (leaderLast < 0 || leaderLast == leaderFirst)) {
        std::cerr << "PROBE: FAIL the partition leader did not change: before " << leaderFirst << ", after "
                  << leaderLast << "\n";
        return 124;
    }
    if (rcId1 == rcId0 || rcId1 <= 0) { std::cerr << "PROBE: FAIL the read_committed subscription did not move (id " << rcId0 << " -> " << rcId1 << ")\n"; return 111; }

    // (c) nothing twice; (a)/(b) every acked record delivered to both.
    if (rcOutOfRange != 0 || ltOutOfRange != 0) { std::cerr << "PROBE: FAIL offsets beyond the counters (rc " << rcOutOfRange << ", latest " << ltOutOfRange << ")\n"; return 112; }
    for (int64_t o = 0; o < kMaxOff; ++o) {
        const auto ix = static_cast<std::size_t>(o);
        if (rcCount[ix] > 1 || ltCount[ix] > 1) {
            std::cerr << "PROBE: FAIL offset " << o << " re-delivered (read_committed x" << rcCount[ix] << ", Latest x" << ltCount[ix] << ")\n";
            return 113;
        }
        if (ltCount[ix] != 0 && o < kPre) {
            std::cerr << "PROBE: FAIL Latest delivered offset " << o << " from before its bind (position " << kPre << ")\n";
            return 114;
        }
    }
    for (int64_t o = 0; o < kPre; ++o)
        if (rcCount[static_cast<std::size_t>(o)] != 1) { std::cerr << "PROBE: FAIL read_committed lost pre-move offset " << o << "\n"; return 115; }
    for (const int64_t o : ackedOffsets) {
        const auto ix = static_cast<std::size_t>(o);
        if (rcCount[ix] != 1) { std::cerr << "PROBE: FAIL read_committed lost acked offset " << o << "\n"; return 116; }
        if (ltCount[ix] != 1) {
            std::cerr << "PROBE: FAIL Latest lost acked offset " << o << " (produced around the move; the moved "
                         "subscription did not resume at the broker's position note " << kPre << ")\n";
            return 117;
        }
    }
    // (b) beyond the acked set (a send in doubt may still be in the log): from
    // its bind-time position on, Latest saw exactly what read_committed saw.
    for (int64_t o = kPre; o < kMaxOff; ++o) {
        const auto ix = static_cast<std::size_t>(o);
        if (ltCount[ix] != rcCount[ix]) {
            std::cerr << "PROBE: FAIL offset " << o << ": Latest x" << ltCount[ix] << " but read_committed x"
                      << rcCount[ix] << " (the Latest subscription lost a record around the move)\n";
            return 118;
        }
    }
    for (int64_t o = kPre; o < kPre + kTxn; ++o) {
        const auto ix = static_cast<std::size_t>(o);
        if (rcCount[ix] != 1 || ltCount[ix] != 1) {
            std::cerr << "PROBE: FAIL transaction offset " << o << " (committed after the move): read_committed x"
                      << rcCount[ix] << ", Latest x" << ltCount[ix]
                      << " (Latest must resume at its position note " << kPre
                      << "; re-resolving Latest on the new leader skips the whole transaction)\n";
            return 123;
        }
    }
    if (rcCount[static_cast<std::size_t>(kPre)] != 1) {
        std::cerr << "PROBE: FAIL offset " << kPre << " (the first record after the Latest bind) never delivered\n";
        return 119;
    }

    std::cerr << "PROBE: transient failover OK — both subscriptions moved; " << ackedOffsets.size()
              << " acked records (" << ackedBeforeRebind << " acked before the Latest re-bind) delivered exactly once to each\n";
    std::cout << "FAILOVER_ACKED_BEFORE_REBIND=" << ackedBeforeRebind << "\n";
    std::cout << "TRANSIENT-FAILOVER-E2E: OK acked=" << ackedOffsets.size() << " inDoubt=" << failedSends << "\n";
    std::cout.flush();
    return 0;
}

// ---------------------------------------------------------------------------
// E2 final fix wave (C1): GroupConsume x Restart on the transient topic.
// The group consumer is created in the client's setup phase (before connect),
// with presence_t in ClientConfig::topics (the id comes from the setup handoff).
// Records are counted per offset on the runner thread (atomics: the app thread
// polls them to a deadline, then closes the consumer and waits for finished()).
// ---------------------------------------------------------------------------
static constexpr const char* kPresenceGroup = "e2-presence-group";

// The group legs' topic (RUNE_GROUP_TOPIC, default presence_t; the 3-node
// failover e2e uses its static presence_failover).
static std::string groupTopic() { return getEnvStr("RUNE_GROUP_TOPIC", "presence_t"); }

static int groupCommitMode(ClientConfig cfg, int timeoutMs, int boundMs) {
    const auto ttTopic = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));
    const int64_t ttEnd = getEnvI64("RUNE_VIEW_TT_END", -1);
    const int64_t preProduce = getEnvI64("RUNE_GROUP_PRODUCE", 0);   // records to produce first (an empty topic)
    if (ttTopic < 0 || ttEnd <= 0) { std::cerr << "PROBE: FAIL group_commit needs RUNE_VIEW_TT_TOPIC_ID / RUNE_VIEW_TT_END\n"; return 150; }
    const std::string topic = groupTopic();
    cfg.topics.push_back(TopicSpec{topic, ttTopic, 1});
    Client c(cfg);
    GroupConfig g;
    g.autoCommit      = false;
    auto gc = c.subscribeGroup(kPresenceGroup, {topic}, g);
    std::atomic<int64_t> next{0};
    gc.onRecords([&next](const RecordSet& rs) {
        if (rs.nextOffset() > next.load(std::memory_order_relaxed)) next.store(rs.nextOffset(), std::memory_order_relaxed);
    });
    c.connectAndWait();
    for (int64_t k = 0; k < preProduce; ++k) {
        int64_t off = -1;
        if (!produceOne(c, ttTopic, 100 + k, Acks::All, timeoutMs, off)) {
            std::cerr << "PROBE: FAIL group_commit produce #" << k << "\n"; gc.close(); c.close(); return 150;
        }
    }
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (next.load(std::memory_order_relaxed) < ttEnd && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);   // poll interval, not correctness
    if (next.load() < ttEnd) {
        std::cerr << "PROBE: FAIL group_commit consumed to " << next.load() << " (want " << ttEnd << ")\n";
        gc.close(); c.close(); return 151;
    }
    try {
        gc.commitSyncAndWait(std::chrono::milliseconds(timeoutMs * 2));
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL group_commit commitSync: " << ex.what() << "\n";
        gc.close(); c.close(); return 152;
    }
    gc.close();
    std::cout << "GROUP-COMMIT: OK committed=" << next.load() << "\n";
    std::cout.flush();
    c.close();
    return 0;
}

static int groupRestartMode(ClientConfig cfg, int timeoutMs, int boundMs) {
    const auto ttTopic = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));
    if (ttTopic < 0) { std::cerr << "PROBE: FAIL group_restart needs RUNE_VIEW_TT_TOPIC_ID\n"; return 153; }
    const std::string topic = groupTopic();
    cfg.topics.push_back(TopicSpec{topic, ttTopic, 1});
    // The 2 new records first, on a short-lived client: acks=All waits (bounded)
    // until the partition has a writable leader again, so the group below
    // starts against a settled partition (its data lineage confirmed).
    {
        Client p(cfg);
        p.connectAndWait();
        for (int64_t key = 40; key < 42; ++key) {
            int64_t off = -1;
            if (!produceOne(p, ttTopic, key, Acks::All, timeoutMs, off)) {
                std::cerr << "PROBE: FAIL group_restart produce\n"; p.close(); return 154;
            }
        }
        p.close();
    }
    Client c(cfg);
    GroupConfig g;
    g.autoCommit      = false;
    auto gc = c.subscribeGroup(kPresenceGroup, {topic}, g);
    // Records the restarted ring already holds (RUNE_GROUP_PRE; default 1 —
    // verify_restart's record) + the 2 produced above: offsets 0..kWant-1.
    const int64_t kWant = getEnvI64("RUNE_GROUP_PRE", 1) + 2;
    if (kWant > 64) { std::cerr << "PROBE: FAIL group_restart RUNE_GROUP_PRE too large\n"; return 153; }
    std::array<std::atomic<int>, 64> seen{};
    gc.onRecords([&seen](const RecordSet& rs) {
        rs.forEachRecord([&seen](std::string_view, std::string_view, int64_t off, int64_t) {
            if (off >= 0 && off < 64) seen[static_cast<std::size_t>(off)].fetch_add(1, std::memory_order_relaxed);
        });
    });
    c.connectAndWait();
    auto all = [&] {
        for (int64_t o = 0; o < kWant; ++o) if (seen[static_cast<std::size_t>(o)].load() == 0) return false;
        return true;
    };
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (!all() && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
    gc.close();
    const auto fin = std::chrono::steady_clock::now() + 10s;
    while (!gc.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    if (!all()) {
        std::cerr << "PROBE: FAIL group_restart SILENT LOSS: the group did not deliver every post-restart record:";
        for (int64_t o = 0; o < kWant; ++o) std::cerr << " offset " << o << " x" << seen[static_cast<std::size_t>(o)].load();
        std::cerr << " (it resumed at the committed position of the ring's previous life)\n";
        c.close(); return 155;
    }
    for (int64_t o = 0; o < 64; ++o) {
        if (seen[static_cast<std::size_t>(o)].load() > 1) {
            std::cerr << "PROBE: FAIL group_restart offset " << o << " delivered " << seen[static_cast<std::size_t>(o)].load() << " times\n";
            c.close(); return 156;
        }
        if (o >= kWant && seen[static_cast<std::size_t>(o)].load() != 0) {
            std::cerr << "PROBE: FAIL group_restart delivered offset " << o << " (the ring holds only " << kWant << ")\n";
            c.close(); return 157;
        }
    }
    std::cout << "GROUP-RESTART: OK delivered the " << kWant << " post-restart records exactly once\n";
    std::cout.flush();
    c.close();
    return 0;
}

// --create-only (catalog docker e2e): connect, look up the source topic RUNE_VIEW_SOURCE (default
// "character"; its fields must include Int64 "id" (the key) and "hp"), create the view
// RUNE_VIEW_NAME (default "<source>_hp") over it through createViewAndWait and exit.  0 on Ok;
// 1 printing the error code + the broker's detail otherwise.  No rows are read.
static int createOnlyMode(Client& c, std::chrono::milliseconds wait) {
    const std::string source = getEnvStr("RUNE_VIEW_SOURCE", "character");
    const std::string name   = getEnvStr("RUNE_VIEW_NAME", source + "_hp");
    const TopicSchemaResult src = c.getTopicSchemaAndWait(source, wait);
    if (src.errorCode != 0) {
        std::cerr << "PROBE: FAIL create-only getTopicSchema(" << source << ") errorCode=" << src.errorCode << "\n";
        return 1;
    }
    const auto plan = rcv::serializeAuthored(charHpPlan(name, src.topicId, 0));
    const CreateViewResult v = c.createViewAndWait(name, "id", /*persistence=*/0, std::span<const uint8_t>(plan), wait);
    if (v.errorCode != 0) {
        std::cerr << "PROBE: FAIL create-only createView(" << name << ") errorCode=" << v.errorCode
                  << " detail=" << v.detail << "\n";
        return 1;
    }
    std::cout << "CREATE-ONLY: OK view=" << name << " viewTopicId=" << v.viewTopicId << "\n";
    return 0;
}

int main(int argc, char** argv) {
    bool createOnly = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--create-only") createOnly = true;
        else { std::cerr << "PROBE: FAIL unknown argument " << argv[i] << " (only --create-only)\n"; return 1; }
    }
    const std::string mode        = createOnly ? std::string("create_only") : getEnvStr("RUNE_MODE", "setup");
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
    rune::examples::applyEnvCredentials(cfg);   // RUNE_USER / RUNE_PASSWORD_FILE
    cfg.connectTimeoutMs = static_cast<int64_t>(timeoutMs);
    cfg.pingTimeoutMs    = static_cast<int64_t>(timeoutMs);
    cfg.idleMode         = IdleMode::LowLatency;

    const auto wait = std::chrono::milliseconds(timeoutMs);

    // E2 final fix wave (C1): the group legs build their own client (the group
    // consumer is registered before connect).
    if (mode == "group_commit" || mode == "group_restart") {
        try {
            return mode == "group_commit" ? groupCommitMode(cfg, timeoutMs, boundMs)
                                          : groupRestartMode(cfg, timeoutMs, boundMs);
        } catch (const std::exception& ex) {
            std::cerr << "PROBE: FAIL " << mode << " exception: " << ex.what() << "\n";
            return 21;
        }
    }

    try {
        Client c(cfg);
        c.onError([](const std::string& m) { std::cerr << "PROBE: onError: " << m << "\n"; });

        std::cerr << "PROBE: connecting...\n";
        c.connectAndWait();
        std::cerr << "PROBE: connected\n";

        if (mode == "create_only") {
            const int rc = createOnlyMode(c, wait);
            c.close();
            return rc;
        }

        if (mode == "setup") {
            // ---- 1. defineTopic "character" ------------------------------
            const auto blob = rune::codec::serialize(characterDescriptor());
            DefineTopicResult def = c.defineTopicAndWait(
                "character", /*partitions=*/1, /*keyField=*/"id",
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

            // ---- 2b. DDL conformance: ALTER on a view's output -> ViewNotAlterable (38)
            {
                rune::codec::AlterOp add{};
                add.type = rune::codec::AlterOpType::Add; add.name = "extra";
                add.primType = rune::codec::PrimType::Int64; add.fixedLen = 0;
                const auto ops = rune::codec::encodeAlterOps({add});
                const AlterTopicResult ar = c.alterTopicAndWait("char_persisted", 0,
                                                                std::span<const uint8_t>(ops), wait);
                if (ar.errorCode != 38) {
                    std::cerr << "PROBE: FAIL alterTopic(char_persisted) errorCode=" << ar.errorCode
                              << " (want 38 ViewNotAlterable)\n";
                    return 60;
                }
                std::cout << "ALTER-VIEW: OK code=38\n";
            }

            // ---- 3. produce one "character" record -------------------------
            const int64_t keyId = 4242;
            const int64_t hp0   = 100;
            {
                ProducerConfig pcfg;
                // Acks::All: the e2e restarts a broker after this (#51)
                pcfg.acks             = Acks::All;
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

            // ---- 5. Phase 2a: aggregate view user_totals over 'order' -------
            const auto oblob = rune::codec::serialize(orderDescriptor());
            DefineTopicResult ord = c.defineTopicAndWait("order", /*partitions=*/1, /*keyField=*/"orderId", std::span<const uint8_t>(oblob), wait);
            if (ord.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(order) errorCode=" << ord.errorCode << "\n"; return 30; }
            const auto aggBlob = rcv::serializeAuthored(userTotalsPlan(ord.topicId));
            CreateViewResult viewA = c.createViewAndWait("user_totals", "userId", /*persistence=*/0,
                                                         std::span<const uint8_t>(aggBlob), wait);
            if (viewA.errorCode != 0) { std::cerr << "PROBE: FAIL createView(user_totals) errorCode=" << viewA.errorCode << "\n"; return 31; }
            const TopicHandle ordTh{ord.topicId, 1};
            if (!sendOrder(c, ordTh, 1, 100, 10, timeoutMs) || !sendOrder(c, ordTh, 2, 100, 30, timeoutMs) ||
                !sendOrder(c, ordTh, 3, 200, 5, timeoutMs)  || !sendOrder(c, ordTh, 1, 100, 50, timeoutMs) ||
                !sendOrder(c, ordTh, 3, 100, 5, timeoutMs)) return 32;
            // MRU after the five writes: o3 (5), o1 (50), o2 (30).
            if (!waitForTotals(c, viewA.viewTopicId, viewA.outputFingerprint, 100,
                               Totals{3, 85, 85.0 / 3, 50, 5, 5},
                               boundMs, timeoutMs, "user_totals u100 (initial)")) return 33;
            if (!waitForAbsent(c, viewA.viewTopicId, viewA.outputFingerprint, 200, boundMs, timeoutMs,
                               "user_totals u200 (emptied by group move)")) return 34;

            // ---- 6. Phase 2b Part C: typed topics route by key -------------
            if (const int rc = typedRoutingLeg(c, wait, boundMs); rc != 0) return rc;

            // ---- 7. Phase 2b E/E2: a TRANSIENT topic — define at runtime (the OK is
            //         held until it serves: C1), produce in all three ack modes,
            //         subscribe from earliest / an offset / latest, read_committed.
            //         Exit codes 130..142, one per check (50..56 are typedRoutingLeg's,
            //         60 ALTER's). ----
            DefineTopicResult tt = c.defineTopicAndWait(
                "presence_t", /*partitions=*/1, /*keyField=*/"id",
                std::span<const uint8_t>(blob), Persistence::Transient, wait);
            if (tt.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(presence_t transient) errorCode=" << tt.errorCode << "\n"; return 130; }
            {
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 1, Acks::None, timeoutMs, off))   { std::cerr << "PROBE: FAIL produce(presence_t, acks=None)\n"; return 131; }
                // acks=None reports Ok on hand-off: wait (bounded) until that record is
                // in the log before the next producer sends — the offsets below are
                // then deterministic (never a None record overtaken by a Leader one).
                if (const int64_t landed = subscribeUntil(c, tt.topicId, From::earliest(), Isolation::ReadUncommitted, 1, boundMs); landed != 1) {
                    std::cerr << "PROBE: FAIL presence_t acks=None record never landed (position " << landed << ")\n"; return 132;
                }
                if (!produceOne(c, tt.topicId, 2, Acks::Leader, timeoutMs, off) || off != 1) { std::cerr << "PROBE: FAIL produce(presence_t, acks=Leader) offset=" << off << "\n"; return 133; }
                if (!produceOne(c, tt.topicId, 3, Acks::All, timeoutMs, off) || off != 2)    { std::cerr << "PROBE: FAIL produce(presence_t, acks=All) offset=" << off << "\n"; return 134; }
            }
            if (const int64_t pos = subscribeUntil(c, tt.topicId, From::earliest(), Isolation::ReadUncommitted, 3, boundMs); pos < 3) {
                std::cerr << "PROBE: FAIL subscribe(presence_t, earliest) position=" << pos << " (want 3)\n"; return 135;
            }
            {
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, tt.topicId, From::offset(2), Isolation::ReadUncommitted, 3, boundMs, &seen);
                if (pos != 3 || seen != 1) { std::cerr << "PROBE: FAIL subscribe(presence_t, offset 2) position=" << pos << " seen=" << seen << " (want 3, 1)\n"; return 136; }
            }
            {
                // Latest never replays: the subscription is live (its id is known) before
                // the next record is produced; it sees exactly that one record.
                SubscribeOptions o;
                o.from = From::latest();
                int64_t seen = 0;
                auto sub = c.subscribe(TopicHandle{tt.topicId, 1}, 0, o, [&seen](const Record&) { ++seen; });
                const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
                while (sub.subscriptionId() <= 0 && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 4, Acks::All, timeoutMs, off) || off != 3) { std::cerr << "PROBE: FAIL produce(presence_t) #4 offset=" << off << "\n"; return 137; }
                while (sub.position() < 4 && std::chrono::steady_clock::now() < dl) std::this_thread::sleep_for(5ms);
                sub.unsubscribe();
                const auto fin = std::chrono::steady_clock::now() + 5s;
                while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
                if (sub.position() != 4 || seen != 1) { std::cerr << "PROBE: FAIL subscribe(presence_t, latest) position=" << sub.position() << " seen=" << seen << " (want 4, 1)\n"; return 138; }
            }
            int64_t ttEnd = -1;   // C1: presence_t's end, handed to the group_commit leg
            {
                // read_committed: offsets 0-3 plain; a committed txn (2 records + marker),
                // an aborted txn (2 records + marker), then one plain record (acks=All).
                // read_committed must see exactly 4 + 2 + 1 = 7 records and reach its end.
                if (!transientTxn(c, tt.topicId, "e2-presence-commit", 10, 2, /*commit=*/true, timeoutMs))  return 139;
                if (!transientTxn(c, tt.topicId, "e2-presence-abort", 20, 2, /*commit=*/false, timeoutMs))  return 140;
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, 30, Acks::All, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after txns\n"; return 141; }
                ttEnd = off + 1;
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, tt.topicId, From::earliest(), Isolation::ReadCommitted, off + 1, boundMs, &seen);
                if (pos != off + 1 || seen != 7) {
                    std::cerr << "PROBE: FAIL read_committed(presence_t) position=" << pos << " seen=" << seen
                              << " (want " << off + 1 << ", 7: an aborted or open record leaked, or a committed one was lost)\n";
                    return 142;
                }
            }
            std::cerr << "PROBE: transient topic presence_t topicId=" << tt.topicId
                      << " produced (None/Leader/All) + subscribed (earliest/offset/latest) + read_committed OK\n";

            // ---- Handoff for the restart leg (scraped by the e2e script) ----
            std::cout << "VIEW_SRC_TOPIC_ID="       << def.topicId            << "\n";
            std::cout << "VIEW_TOPIC_ID_PERSISTED=" << viewP.viewTopicId      << "\n";
            std::cout << "VIEW_FP_PERSISTED="       << viewP.outputFingerprint << "\n";
            std::cout << "VIEW_TOPIC_ID_TRANSIENT=" << viewT.viewTopicId      << "\n";
            std::cout << "VIEW_FP_TRANSIENT="       << viewT.outputFingerprint << "\n";
            std::cout << "VIEW_KEY_ID="             << keyId                  << "\n";
            std::cout << "VIEW_HP_INITIAL="         << hp0                   << "\n";
            std::cout << "VIEW_ORDER_TOPIC_ID=" << ord.topicId              << "\n";
            std::cout << "VIEW_TOPIC_ID_AGG="   << viewA.viewTopicId        << "\n";
            std::cout << "VIEW_FP_AGG="         << viewA.outputFingerprint  << "\n";
            std::cout << "VIEW_TT_TOPIC_ID="    << tt.topicId               << "\n";
            std::cout << "VIEW_TT_END="         << ttEnd                    << "\n";   // C1: the group_commit leg
            std::cout << "VIEW-PROBE-SETUP: OK\n";
            std::cout.flush();
            c.close();
            return 0;
        }

        if (mode == "ddl_race") {
            const int rc = ddlRaceMode(c, getEnvStr("RUNE_RACE_ROLE", "a"), wait, boundMs);
            c.close();
            return rc;
        }
        if (mode == "catalog_dump") {
            const int rc = catalogDumpMode(c, wait);
            c.close();
            return rc;
        }
        if (mode == "transient_failover") {
            const int rc = transientFailoverMode(c, wait, timeoutMs, boundMs);
            c.close();
            return rc;
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
            const int32_t  orderTopic     = static_cast<int32_t>(getEnvI64("RUNE_VIEW_ORDER_TOPIC_ID", -1));
            const int32_t  viewIdA        = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TOPIC_ID_AGG", -1));
            const uint64_t fpA            = getEnvU64("RUNE_VIEW_FP_AGG", 0);
            const int32_t  ttTopic        = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));

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

            // ---- (b) transient view: rebuilt from the retained source --------
            //     No checkpoint — the readiness gate re-folds the source log from
            //     its floor (spec §9.7); the source is persisted, so the ORIGINAL
            //     row comes back.  waitResolved rides through NotReady while the
            //     rebuild runs.
            const ViewRowResult rt0 = waitResolved(c, viewIdT, keyU64, fpT, boundMs, timeoutMs);
            if (rt0.status != ViewRowResult::kOk) {
                std::cerr << "PROBE: FAIL char_transient post-restart status=" << rt0.status
                          << " (want kOk — a transient view is rebuilt by re-folding the "
                             "retained source on restart)\n";
                return 13;
            }
            if (!decodeAndCheck(rt0, keyId, hpInitial, "char_transient (post-restart, re-folded)")) return 16;

            // ---- (c) re-produce -> BOTH views fold it live after the restart ---
            {
                ProducerConfig pcfg;
                // Acks::All: the e2e restarts a broker after this (#51)
                pcfg.acks             = Acks::All;
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

            const ViewRowResult rp1 = waitForHp(c, viewIdP, keyU64, fpP, keyId, hpRefill, boundMs, timeoutMs);
            if (!decodeAndCheck(rp1, keyId, hpRefill, "char_persisted (live after restart)")) return 15;
            const ViewRowResult rt1 = waitForHp(c, viewIdT, keyU64, fpT, keyId, hpRefill, boundMs, timeoutMs);
            if (!decodeAndCheck(rt1, keyId, hpRefill, "char_transient (live after restart)")) return 17;

            // ---- (d) aggregate view recovered: rows AND contributor map --------
            if (orderTopic < 0 || viewIdA < 0) { std::cerr << "PROBE: FAIL missing aggregate handoff env\n"; return 40; }
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 85, 85.0 / 3, 50, 5, 5}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart, recovered)")) return 41;
            if (!waitForAbsent(c, viewIdA, fpA, 200, boundMs, timeoutMs, "user_totals u200 (post-restart)")) return 42;
            // Update a PRE-restart contributor: o3 5 -> 25. Correct only if its
            // pre-image survived the restart (lost map => n 4, total 110); it also
            // retracts the cached MIN (5) with a higher value -> rescan of the
            // RECOVERED per-group value block -> min 25.
            if (!sendOrder(c, TopicHandle{orderTopic, 1}, 3, 100, 25, timeoutMs)) return 43;
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 105, 35.0, 50, 25, 25}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart update retracts pre-image + min)")) return 44;
            // o1 50 -> 20 retracts the cached MAX with a lower value -> rescan -> max 30 (o2).
            if (!sendOrder(c, TopicHandle{orderTopic, 1}, 1, 100, 20, timeoutMs)) return 45;
            if (!waitForTotals(c, viewIdA, fpA, 100, Totals{3, 75, 25.0, 30, 20, 20}, boundMs, timeoutMs,
                               "user_totals u100 (post-restart max retraction rescans)")) return 46;

            // ---- (e) Phase 2b E/E2: the transient topic EXISTS and is EMPTY ------
            //         Exit codes 90..95 (60..65 would collide with setup's ALTER 60).
            if (ttTopic < 0) { std::cerr << "PROBE: FAIL missing RUNE_VIEW_TT_TOPIC_ID\n"; return 90; }
            {
                // Metadata survived: re-defining answers TopicExists with the SAME id.
                const auto ttBlob = rune::codec::serialize(characterDescriptor());
                DefineTopicResult again = c.defineTopicAndWait(
                    "presence_t", 1, "id", std::span<const uint8_t>(ttBlob),
                    Persistence::Transient, wait);
                if (again.errorCode != 25 || again.topicId != ttTopic) {
                    std::cerr << "PROBE: FAIL presence_t after restart errorCode=" << again.errorCode
                              << " topicId=" << again.topicId << " (want TopicExists, id " << ttTopic << ")\n";
                    return 91;
                }
                // Data did not: the first post-restart record lands at offset 0.
                int64_t off = -1;
                if (!produceOne(c, ttTopic, 9, Acks::All, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after restart\n"; return 92; }
                if (off != 0) { std::cerr << "PROBE: FAIL presence_t not empty after restart: first offset=" << off << "\n"; return 93; }
                int64_t seen = 0;
                const int64_t pos = subscribeUntil(c, ttTopic, From::earliest(), Isolation::ReadUncommitted, 1, boundMs, &seen);
                if (pos != 1 || seen != 1) { std::cerr << "PROBE: FAIL presence_t subscribe after restart position=" << pos << " seen=" << seen << " (want 1, 1)\n"; return 94; }
                const int64_t rc = subscribeUntil(c, ttTopic, From::earliest(), Isolation::ReadCommitted, 1, boundMs, &seen);
                if (rc != 1 || seen != 1) { std::cerr << "PROBE: FAIL presence_t read_committed after restart position=" << rc << " seen=" << seen << "\n"; return 95; }
                std::cerr << "PROBE: presence_t exists after restart, empty, produce+subscribe+read_committed OK\n";
            }

            std::cout << "VIEW-ENGINE-E2E: OK\n";
            std::cout.flush();
            c.close();
            return 0;
        }

        std::cerr << "PROBE: FAIL unknown RUNE_MODE='" << mode << "' (want setup|verify_restart|ddl_race|catalog_dump|transient_failover)\n";
        c.close();
        return 20;
    } catch (const std::exception& ex) {
        std::cerr << "PROBE: FAIL exception: " << ex.what() << "\n";
        return 21;
    }
}
