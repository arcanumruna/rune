// CatalogClusterIntegrationTest.cpp — catalog history Task 22 (spec §10): the interaction matrix's
// 3-node slice on THREE in-process BrokerNodes over REAL Aeron UDP, driven by the wire client
// ShardTestClient.  It closes the E2 carry-over "multi-node runtime DDL fails": runtime DDL through
// every broker, with a catalog-leader crash mid-DDL, and — after EVERY cell — acks=all produce +
// subscribe-from-earliest THROUGH EACH RUNNING BROKER'S CLIENT ENDPOINT, asserting every record produced
// is delivered exactly once (count + content).
//
// 32 cells = {CreateTopic, CreateView, DropTopic, AlterSchema} x {Steady via node 1 / 2 / 3,
// LeaderChangeMidDdl, RestartMidApply, NewBrokerJoins, DropAndReuseName, CreateRacesPlacement}; every
// expected answer is read from cmatrix::expectedCodes (F16) - no hard-coded outcome.
//
// CreateRacesPlacement (CreateTopic / CreateView) really races placement (fix round 1, R7 OkAfterPlacement): the initial
// placement is HELD on every broker, the DDL commits + applies, every broker is probed for 1.5 s (a produce + Subscribe per
// partition / a GetViewRow: retriable refusal only), no DDL answer may arrive in the window; then the placement is released.
// Views are read through EVERY broker (a non-source broker redirects to the source leader, C1) and the hp checked.
//
// Test-only seams (the Task 22 report lists them): BrokerNode::crashForTest (a REAL crash: no controlled
// shutdown), BrokerNode::holdCatalogAnswersForTest (the leader's DDL commits but is never answered),
// BrokerNode::holdCatalogApplyForTest (a promoted leader stays CatchingUp), MetadataService::
// holdPlacementForTest (the initial placement of a runtime topic held), ThreeNodes::{crash, crashAfterNextAppend}.
//
// An answer lost to a crashed leader is IN DOUBT (the session died with the DDL in flight, the cell crashed it
// itself - Hooks::afterSend returned true): the cell maps it to the retriable NotLeader (2) - the code every
// client treats it as - exactly like the stress driver's R2c ("answers lost to it are in doubt"); the retry
// carries the same requestId.  ANY OTHER missing answer is a failure (cNoAnswer): a broker that never answers
// a DDL must not be masked by the retry + the requestId dedupe (R-I1).
#include "support/CatalogBrokerCluster.h"
#include "support/CatalogMatrixExpectations.h"
#include "support/DdlHarness.h"          // charBlob / viewPlanBlob / alterOpsFor
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/MetadataService.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::catalog_cluster;
using namespace std::chrono_literals;
namespace pc = rune::protocol::client;
namespace cm = rune::node::test::cmatrix;
using Clock = std::chrono::steady_clock;
using rune::protocol::RequestId;

namespace {

constexpr int cOk = 0, cNotLeader = 2, cRedirect = 3, cUnknown = 7;
constexpr int cNoAnswer = -2;   // R-I1: a broker that never answered (NOT the in-doubt NotLeader of a crashed session)

[[nodiscard]] std::vector<uint8_t> keyBytes(int64_t id) {
    std::vector<uint8_t> b(8);
    for (int i = 0; i < 8; ++i) b[static_cast<std::size_t>(i)] = static_cast<uint8_t>(uint64_t(id) >> (8 * i));
    return b;
}
/// A v1 "character" value ({id Int64 key, hp Int64}): [blockLength u16][version u16][id i64][hp i64].
[[nodiscard]] std::vector<uint8_t> characterValue(int64_t id, int64_t hp) {
    std::vector<uint8_t> b;
    auto le16 = [&](uint16_t v) { b.push_back(uint8_t(v)); b.push_back(uint8_t(v >> 8)); };
    auto le64 = [&](int64_t v) { for (int i = 0; i < 8; ++i) b.push_back(uint8_t(uint64_t(v) >> (8 * i))); };
    le16(16); le16(1); le64(id); le64(hp);
    return b;
}
[[nodiscard]] std::string asString(const std::vector<uint8_t>& b) { return {b.begin(), b.end()}; }
[[nodiscard]] std::vector<uint8_t> oneRecord(const std::string& key, const std::string& value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(key, value, 0);
    return b.build();
}

/// The hp dataCheck writes into the character it produces through node `n` into topic `name` (so a view row can be checked).
[[nodiscard]] int64_t hpFor(int n, const std::string& name) {
    return 1000 * n + static_cast<int64_t>(std::hash<std::string>{}(name) % 997);
}
/// A view row of the "id, hp" view (viewPlanBlob): [null bitmap 1 B][id i64][hp i64] - its hp, or -1 when malformed.
[[nodiscard]] int64_t rowHp(const std::vector<uint8_t>& row) {
    if (row.size() != 17) return -1;
    int64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<int64_t>(row[9 + static_cast<std::size_t>(i)]) << (8 * i);
    return v;
}

/// The batch of one record: a typed topic's carries the 8-byte LE key; an untyped one a text key.
[[nodiscard]] std::vector<uint8_t> batchFor(bool typed, int64_t id, const std::string& value) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    if (typed) {
        const auto k = keyBytes(id);
        b.append(std::string_view(reinterpret_cast<const char*>(k.data()), k.size()), value, 0);
    } else {
        b.append("k" + std::to_string(id), value, 0);
    }
    return b.build();
}

/// Every record value in a pushed Records payload (one or more batches).
[[nodiscard]] std::vector<std::string> valuesOf(const std::vector<uint8_t>& bytes) {
    std::vector<std::string> out;
    std::size_t pos = 0;
    while (pos < bytes.size()) {
        rune::protocol::RecordBatchReader reader(bytes.data() + pos, bytes.size() - pos);
        if (!reader.ensureValid()) break;
        reader.forEachRecord([&](std::string_view, std::string_view val, int64_t, int64_t) { out.emplace_back(val); });
        if (reader.totalLength() == 0) break;
        pos += static_cast<std::size_t>(reader.totalLength());
    }
    return out;
}

/// One answer to a DDL (got = false: none arrived - the broker died with it in flight).
struct Answer {
    bool        got  = false;
    int         code = -1;
    std::string msg;
};
using Sender = std::function<int64_t(ShardTestClient&, const std::string& ch, int64_t sid, RequestId rid)>;

/// A topic's identity as a broker's applied catalog states it.
struct TopicInfo { int32_t id = -1; int32_t partitions = 0; uint16_t version = 0; };

/// The per-cell record of what happened (printed on a failure).
struct CellResult {
    bool        viaWasLeader = false;
    int         firstCode = -100, afterRedirectCode = -1, finalCode = -100;
    std::string trace;
    RequestId   rid{};
};

/// Cell wall times, printed once at the end of the run.
struct CellTimes {
    static std::vector<std::pair<std::string, double>>& rows() {
        static std::vector<std::pair<std::string, double>> r;
        return r;
    }
};
class CellTimesPrinter : public ::testing::Environment {
public:
    void TearDown() override {
        double total = 0;
        for (const auto& [n, s] : CellTimes::rows()) { total += s; std::fprintf(stderr, "[ cell time ] %-60s %7.1f s\n", n.c_str(), s); }
        if (!CellTimes::rows().empty()) std::fprintf(stderr, "[ cell time ] %-60s %7.1f s\n", "TOTAL", total);
    }
};
[[maybe_unused]] ::testing::Environment* const kCellTimesPrinter = ::testing::AddGlobalTestEnvironment(new CellTimesPrinter);

/// Exactly the records this cell's checks produced, per partition (values), for the exactly-once comparison.
using Expected = std::map<int, std::vector<std::string>>;

/// Per-DDL hooks of Rig::drive.
struct Hooks {
    /// After the sendNo-th frame went to `node` (before its answer is awaited); true: the answer is in doubt.
    std::function<bool(int sendNo, int node)> afterSend;
    bool followRedirects = true;   ///< false: the Redirect is the terminal answer (the RED of "no redirect following")
};

// ===========================================================================
// The rig: a wire client over a ThreeNodes cluster.  Test thread only.
// ===========================================================================
class Rig {
public:
    explicit Rig(ThreeNodes& c)
        : c_(c), cl_(c.clientsDir->path / "wire") {
        cl_.keepSessionsAlive(true);
        cl_.setClientVersion(pc::kClientVersion);   // a v8 session: DDL Redirects are followed
    }
    ShardTestClient& cl() { return cl_; }

    /// A session on broker `n` (connected once, retried while its client endpoint warms up).
    int64_t session(int n) {
        if (auto it = sid_.find(n); it != sid_.end()) return it->second;
        if (!c_.running(n)) return -1;
        for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline;) {
            const int64_t s = cl_.connect(c_.clientChannel(n), kClientStream);
            if (s > 0) { sid_[n] = s; return s; }
            std::this_thread::sleep_for(50ms);
        }
        return -1;
    }
    void forget(int n) { sid_.erase(n); }
    [[nodiscard]] int nodeOf(const std::string& channel) const {
        for (int n = 1; n <= 3; ++n) if (c_.clientChannel(n) == channel) return n;
        return -1;
    }
    /// Keep the sessions alive while `f` blocks (a node start / crash).
    template <class F> auto pumping(F&& f) { return cl_.whilePumping(std::forward<F>(f)); }

    // ── DDL ──────────────────────────────────────────────────────────────────────────────────────
    int64_t sendOnly(int node, const Sender& s, RequestId rid) {
        const int64_t sid = session(node);
        if (sid <= 0) return 0;
        lastCorr_ = s(cl_, c_.clientChannel(node), sid, rid);
        return lastCorr_;
    }
    Answer await(int64_t corr, std::chrono::milliseconds wait) {
        if (corr == 0) return {};
        const auto a = cl_.awaitAnyDdlAnswer(corr, Clock::now() + wait);
        if (!a) return {};
        return Answer{true, a->code, a->message};
    }

    /// Send `s` (stable `rid`) starting at `via` until a TERMINAL answer: a Redirect is followed, NotLeader / an
    /// answer lost to a crash is retried on the next running broker.  first / afterRedirect / final as in the matrix.
    CellResult drive(const Sender& s, int via, RequestId rid, const Hooks& hooks = Hooks{},
                     std::chrono::seconds budget = 150s) {
        CellResult r;
        r.rid = rid;
        r.viaWasLeader = via == c_.settledLeader();
        int cur = via, sendNo = 0;
        for (const auto deadline = Clock::now() + budget; Clock::now() < deadline;) {
            const int64_t corr = sendOnly(cur, s, rid);
            bool inDoubt = false;
            Answer a;
            if (hooks.afterSend) inDoubt = hooks.afterSend(sendNo, cur);
            if (!inDoubt) a = await(corr, 25s);   // in doubt: the test crashed the session itself - nothing can arrive
            std::ostringstream t;
            t << " [send " << sendNo << " -> n" << cur << ": " << (a.got ? std::to_string(a.code) : std::string("no answer")) << "]";
            r.trace += t.str();
            // in doubt, or a stale Redirect naming a broker the test crashed (nothing to send to) = the retriable NotLeader;
            // silence from a RUNNING broker is the failure
            const int code = a.got ? a.code : ((inDoubt || !c_.running(cur)) ? cNotLeader : cNoAnswer);
            if (code == cNoAnswer) {
                ADD_FAILURE() << "node " << cur << (corr == 0 ? " could not be sent the DDL" : " never answered the DDL (25 s)") << r.trace;
                r.finalCode = cNoAnswer;
                return r;
            }
            if (sendNo == 0) r.firstCode = code;
            else if (sendNo == 1 && r.firstCode == cRedirect) r.afterRedirectCode = code;
            ++sendNo;
            if (code == cRedirect) {
                const int target = hooks.followRedirects ? nodeOf(a.msg) : -1;
                if (target < 0) { r.finalCode = cRedirect; r.trace += " (redirect not followed)"; return r; }
                cur = target;
                continue;
            }
            if (code == cNotLeader) {
                std::this_thread::sleep_for(100ms);
                const auto run = c_.runningNodes();
                const auto it = std::find(run.begin(), run.end(), cur);
                cur = run[(static_cast<std::size_t>(it - run.begin()) + 1) % run.size()];   // rotate (a stale route)
                continue;
            }
            r.finalCode = code;
            return r;
        }
        r.trace += " (budget spent)";
        return r;
    }
    /// A precondition DDL: must end 0.
    void must(const Sender& s, const char* what, int via = -1) {
        const int start = via > 0 ? via : leaderOrAny_();
        const CellResult r = drive(s, start, freshRid_());
        ASSERT_EQ(r.finalCode, cOk) << what << r.trace;
    }
    RequestId freshRid_() { return RequestId{0xCE11'0022ull, ++rid_}; }
    /// The correlation id of the last DDL frame `drive` / `sendOnly` sent (0: none).
    [[nodiscard]] int64_t lastCorr() const noexcept { return lastCorr_; }

    // ── reads: a topic as a broker's applied catalog states it ───────────────────────────────────
    std::optional<TopicInfo> topicInfo(int node, const std::string& name, std::chrono::milliseconds wait, int* code = nullptr) {
        const auto deadline = Clock::now() + wait;
        do {
            const int64_t sid = session(node);
            if (sid > 0) {
                const auto a = cl_.awaitSchema(cl_.sendGetTopicSchema(c_.clientChannel(node), kClientStream, sid, name),
                                               Clock::now() + 5s);
                if (a) {
                    if (code) *code = a->errorCode;
                    if (a->errorCode == 0) return TopicInfo{a->topicId, a->partitionCount, a->version};
                }
            }
            std::this_thread::sleep_for(30ms);
        } while (Clock::now() < deadline);
        return std::nullopt;
    }

    // ── data: produce acks=all / subscribe from earliest THROUGH broker `node`'s client endpoint ──
    /// One produce attempt through `node`; the answer's code, or -1 (none); `redirect` = the Redirect's endpoint.
    int produceOnce(int node, int32_t topicId, int p, const std::vector<uint8_t>& batch, std::string* redirect = nullptr) {
        const int64_t sid = session(node);
        if (sid <= 0) return -1;
        const int64_t corr = cl_.nextCorrelationId();
        if (!cl_.produce(c_.clientChannel(node), kClientStream, sid, corr, topicId, p, /*acks=All*/1, batch)) return -1;
        cl_.pollUntil([&] { return cl_.ackFor(corr).has_value() || errorFor_(corr) != nullptr; });
        if (const auto ack = cl_.ackFor(corr)) return ack->errorCode;
        if (const auto* e = errorFor_(corr)) { if (redirect) *redirect = e->message; return e->code; }
        return -1;
    }
    /// One Subscribe attempt through `node`: the answer's code (0 = accepted), or -1 (none); `redirect` = the Redirect's endpoint.
    int subscribeOnce(int node, int32_t topicId, int p, std::string* redirect = nullptr) {
        const int64_t sid = session(node);
        if (sid <= 0) return -1;
        const int64_t corr = cl_.sendSubscribe(c_.clientChannel(node), kClientStream, sid, topicId, p);
        const auto code = cl_.awaitSubscribeCode(corr, Clock::now() + 5s);
        if (!code) return -1;
        if (*code == cRedirect && redirect) if (const auto* e = errorFor_(corr)) *redirect = e->message;
        return *code;
    }
    /// Produce through `node` until OK (a Redirect is followed one hop; the retriable answers are retried);
    /// `seen` collects every answer code.  false: not OK by `within` (or an answer never came).
    bool produceFollowing(int node, int32_t topicId, int p, const std::vector<uint8_t>& batch, std::chrono::seconds within,
                          std::vector<int>* seen = nullptr) {
        for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
            int cur = node;
            for (int hop = 0; hop < 3; ++hop) {
                std::string redirect;
                const int code = produceOnce(cur, topicId, p, batch, &redirect);
                if (seen) seen->push_back(code);
                if (code == cOk) return true;
                if (code == -1) { ADD_FAILURE() << "produce through node " << node << " topic " << topicId << " p" << p << ": no answer"; return false; }
                if (code != cRedirect || nodeOf(redirect) < 0) break;
                cur = nodeOf(redirect);
            }
            std::this_thread::sleep_for(50ms);
        }
        return false;
    }
    /// The TERMINAL answer to a produce through `node`: a Redirect is followed, the retriable codes (NotLeader) retried
    /// until `want` (or `within`); returns the last code seen.
    int produceTerminal(int node, int32_t topicId, int p, const std::vector<uint8_t>& batch, int want,
                        std::chrono::seconds within, std::vector<int>* seen = nullptr) {
        int last = -1;
        for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
            int cur = node;
            for (int hop = 0; hop < 3; ++hop) {
                std::string redirect;
                last = produceOnce(cur, topicId, p, batch, &redirect);
                if (seen) seen->push_back(last);
                if (last != cRedirect || nodeOf(redirect) < 0) break;
                cur = nodeOf(redirect);
            }
            if (last == want) return last;
            std::this_thread::sleep_for(50ms);
        }
        return last;
    }
    /// An open Subscribe(topic, p) from the earliest offset (id 0: none could be opened).
    struct OpenSub { int node = 0; int p = 0; int64_t id = 0; };
    /// Open Subscribe(topic, p) from the earliest offset through `node` (a Redirect is followed, up to `within`).
    OpenSub openSubscription(int node, int32_t topicId, int p, std::chrono::seconds within, std::string* why = nullptr) {
        OpenSub sub{node, p, 0};
        for (const auto deadline = Clock::now() + within; Clock::now() < deadline && sub.id == 0;) {
            int cur = node;
            for (int hop = 0; hop < 3; ++hop) {
                const int64_t sid = session(cur);
                if (sid <= 0) break;
                const int64_t corr = cl_.sendSubscribe(c_.clientChannel(cur), kClientStream, sid, topicId, p);
                const auto code = cl_.awaitSubscribeCode(corr, Clock::now() + 10s);
                if (code && *code == cOk) { if (const auto id = cl_.subscriptionIdOf(corr)) { sub.id = *id; break; } }
                if (code && *code == cRedirect) {
                    const auto* e = errorFor_(corr);
                    if (e != nullptr && nodeOf(e->message) > 0) { cur = nodeOf(e->message); continue; }
                }
                if (why) *why = "subscribe answered " + (code ? std::to_string(*code) : std::string("nothing"));
                break;
            }
            if (sub.id == 0) std::this_thread::sleep_for(50ms);
        }
        return sub;
    }
    /// Every value pushed on subscription `id` since record index `firstRecord`, in arrival order; `frames` (optional)
    /// is filled with the frame trail for a failure message.
    std::vector<std::string> collected(int64_t id, std::size_t firstRecord, std::string* frames = nullptr) const {
        std::vector<std::string> got;
        std::ostringstream trail;
        const auto& all = cl_.records();
        for (std::size_t i = firstRecord; i < all.size(); ++i) {
            const auto& rec = all[i];
            if (rec.subscriptionId != id) continue;
            trail << " @" << rec.baseOffset << "x" << valuesOf(rec.bytes).size();
            for (auto& v : valuesOf(rec.bytes)) got.push_back(std::move(v));
        }
        if (frames) *frames = " sub=" + std::to_string(id) + trail.str();
        return got;
    }
    /// Everything pushed on a Subscribe(topic, p) from the earliest offset, through `node`: collected until
    /// `want` values arrived (or `within`), then 300 ms more (a duplicate would show).
    std::vector<std::string> readAll(int node, int32_t topicId, int p, std::size_t want, std::chrono::seconds within,
                                     std::string* why = nullptr) {
        // Subscription ids derive from the (restart-reset) session counter and repeat after a broker restart: only the
        // records pushed from here on belong to this subscription.
        const std::size_t firstRecord = cl_.records().size();
        const OpenSub sub = openSubscription(node, topicId, p, within, why);
        if (sub.id == 0) return {};
        const auto deadline = Clock::now() + within;
        cl_.pollUntil([&] { return collected(sub.id, firstRecord).size() >= want || Clock::now() >= deadline; });
        const auto settle = Clock::now() + 300ms;
        cl_.pollUntil([&] { return Clock::now() >= settle; });
        std::string frames;
        auto got = collected(sub.id, firstRecord, &frames);
        if (why) *why += " frames:" + frames;
        return got;
    }

    /// For topic `name` on every running broker: it is applied (and has the same id / partition count), a
    /// record per (broker, partition) is produced acks=all THROUGH that broker, then every broker's client
    /// endpoint subscribes each partition from the earliest offset and receives exactly what the partition
    /// holds (count + content).  `have` carries what the partition already held.  `typed`: character values.
    void dataCheck(const std::string& name, bool typed, Expected& have, const std::string& cell, TopicInfo* out = nullptr,
                   std::vector<int>* produceCodes = nullptr) {
        std::optional<TopicInfo> info;
        for (int n : c_.runningNodes()) {
            const auto ti = topicInfo(n, name, 60s);
            ASSERT_TRUE(ti.has_value()) << cell << ": topic '" << name << "' never applied on node " << n;
            if (!info) info = ti;
            EXPECT_EQ(ti->id, info->id) << cell << ": node " << n << " has another id for '" << name << "'";
            EXPECT_EQ(ti->partitions, info->partitions) << cell << ": node " << n;
        }
        ASSERT_TRUE(info.has_value());
        if (out) *out = *info;
        std::ostringstream produced;   // every produce answer seen, for the failure message
        for (int n : c_.runningNodes()) {
            for (int p = 0; p < info->partitions; ++p) {
                const int64_t id = 100 * n + 10 * p + 1;
                const std::string value = typed ? asString(characterValue(id, hpFor(n, name)))
                                                : "v:" + cell + ":n" + std::to_string(n) + ":p" + std::to_string(p);
                std::vector<int> seenCodes;
                const bool accepted = produceFollowing(n, info->id, p, batchFor(typed, id, value), 60s, &seenCodes);
                produced << " n" << n << "p" << p << ":[";
                for (int sc : seenCodes) produced << sc << ' ';
                produced << ']';
                if (produceCodes) produceCodes->insert(produceCodes->end(), seenCodes.begin(), seenCodes.end());
                ASSERT_TRUE(accepted)
                    << cell << ": acks=all produce through node " << n << " to '" << name << "' p" << p << " was never accepted";
                have[p].push_back(value);
            }
        }
        // M2: subscribe EVERY (broker, partition) first, then one wait until every count is reached, one 300 ms settle
        // (a duplicate would show), then compare each pair.  Subscription ids are unique per broker incarnation, and
        // only records pushed from here on belong to these subscriptions.
        struct Check { OpenSub sub; std::string why; };
        std::vector<Check> checks;
        const std::size_t firstRecord = cl_.records().size();
        for (int n : c_.runningNodes()) {
            for (int p = 0; p < info->partitions; ++p) {
                Check ck;
                ck.sub = openSubscription(n, info->id, p, 30s, &ck.why);
                checks.push_back(std::move(ck));
            }
        }
        const auto allIn = [&] {
            for (const Check& ck : checks)
                if (ck.sub.id == 0 || collected(ck.sub.id, firstRecord).size() < have[ck.sub.p].size()) return false;
            return true;
        };
        const auto waitEnd = Clock::now() + 30s;
        cl_.pollUntil([&] { return allIn() || Clock::now() >= waitEnd; });
        const auto settle = Clock::now() + 300ms;
        cl_.pollUntil([&] { return Clock::now() >= settle; });
        for (Check& ck : checks) {
            std::string frames;
            auto got = ck.sub.id == 0 ? std::vector<std::string>{} : collected(ck.sub.id, firstRecord, &frames);
            auto want = have[ck.sub.p];
            std::sort(got.begin(), got.end());
            std::sort(want.begin(), want.end());
            EXPECT_EQ(got.size(), want.size()) << cell << ": subscribe from earliest through node " << ck.sub.node << " '" << name << "' p" << ck.sub.p
                << " " << ck.why << " frames:" << frames << " (produce answers:" << produced.str() << ")";
            EXPECT_EQ(got, want) << cell << ": delivery through node " << ck.sub.node << " '" << name << "' p" << ck.sub.p << " (exactly once, same content)";
        }
    }

    /// One GetViewRow read through `n`, a Redirect followed (at most 2 hops): the FIRST answer and the FINAL one.
    struct ViewRead {
        bool                 answered = false;
        bool                 firstIsError = false;
        int                  firstCode = -1;        // the first answer's ErrorResponse code, or its ViewRowResponse status
        std::string          firstMessage;          // ... and its message (the Redirect's leader endpoint)
        bool                 finalIsError = false;
        int                  finalCode = -1;        // the final answer's ErrorResponse code, or its ViewRowResponse status
        std::vector<uint8_t> row;                   // the final answer's row bytes (status OK)
        int                  finalNode = -1;
        [[nodiscard]] bool ok() const { return answered && !finalIsError && finalCode == pc::ViewRowResponse::kOk; }
    };
    ViewRead viewRowFollowing(int n, int32_t viewTopicId, uint64_t key) {
        ViewRead r;
        int cur = n;
        for (int hop = 0; hop < 3; ++hop) {
            const int64_t sid = session(cur);
            if (sid <= 0) return r;
            const int64_t corr = cl_.sendGetViewRow(c_.clientChannel(cur), kClientStream, sid, viewTopicId, key);
            const auto a = cl_.awaitViewRowOrError(corr, Clock::now() + 10s);
            if (!a) return r;
            const int code = a->isError ? a->errorCode : a->status;
            if (hop == 0) { r.firstIsError = a->isError; r.firstCode = code; r.firstMessage = a->message; }
            r.answered = true; r.finalIsError = a->isError; r.finalCode = code; r.row = a->row; r.finalNode = cur;
            if (a->isError && code == cRedirect && nodeOf(a->message) > 0) { cur = nodeOf(a->message); continue; }
            return r;
        }
        return r;
    }
    /// (UnknownView counts as retriable only until the broker has applied the CreateView and its plan.)
    /// GetViewRow through `n` until the read is no longer retriable (a NotReady / a bare NotLeader / UnknownView while the
    /// view's plan reaches this broker), or `within`; the last read.
    ViewRead viewRowSettled(int n, int32_t viewTopicId, uint64_t key, std::chrono::seconds within) {
        ViewRead r;
        for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
            r = viewRowFollowing(n, viewTopicId, key);
            const bool retriable = !r.answered ||
                (r.finalIsError ? (r.finalCode == cNotLeader || r.finalCode == cRedirect)
                                : (r.finalCode == pc::ViewRowResponse::kNotReady || r.finalCode == pc::ViewRowResponse::kUnknownView));
            if (!retriable) return r;
            std::this_thread::sleep_for(50ms);
        }
        return r;
    }

    /// Hold / release the initial placement of runtime topics on every running broker (only the controller places).
    void holdPlacementEverywhere(bool on) {
        for (int n : c_.runningNodes())
            c_.node(n).onControlThreadForTest([this, n, on] { c_.node(n).metadataServiceForTest()->holdPlacementForTest(on); return 0; });
    }

    /// Probe partition `p` of `topicId` through broker `n` with a produce and a Subscribe: each must be REFUSED RETRIABLY -
    /// a Redirect (3) or NotLeader (2), one Redirect hop followed - never served, never UnknownTopic, never silence (-1).
    /// Returns the number of probes made (2).
    int expectRetriableRefusal(int n, int32_t topicId, int p, const std::string& what) {
        int made = 0;
        int target = n;
        for (int hop = 0; hop < 2; ++hop) {
            std::string redirect;
            const int code = produceOnce(target, topicId, p, batchFor(true, 1, "probe"), &redirect);
            if (hop == 1 || code != cRedirect || nodeOf(redirect) < 0) {
                EXPECT_TRUE(code == cRedirect || code == cNotLeader)
                    << what << ": node " << n << " p" << p << " answered " << code << " to a PRODUCE (-1 = no answer)";
                ++made;
                break;
            }
            target = nodeOf(redirect);
        }
        target = n;
        for (int hop = 0; hop < 2; ++hop) {
            std::string redirect;
            const int code = subscribeOnce(target, topicId, p, &redirect);
            if (hop == 1 || code != cRedirect || nodeOf(redirect) < 0) {
                EXPECT_TRUE(code == cRedirect || code == cNotLeader)
                    << what << ": node " << n << " p" << p << " answered " << code << " to a SUBSCRIBE (-1 = no answer)";
                ++made;
                break;
            }
            target = nodeOf(redirect);
        }
        return made;
    }

    /// The broker that leads (topicId, p) per the metadata `node` serves (-1: unknown).
    int partitionLeader(int node, int32_t topicId, int p) {
        const int64_t sid = session(node);
        if (sid <= 0) return -1;
        for (const auto& pi : cl_.metadata(c_.clientChannel(node), kClientStream, sid))
            if (pi.topicId == topicId && pi.partition == p) return pi.leaderId;
        return -1;
    }

    int leaderOrAny_() {
        const int l = c_.settledLeader();
        if (l > 0) return l;
        const auto run = c_.runningNodes();
        return run.empty() ? 1 : run.front();
    }
    ThreeNodes& cluster() { return c_; }

private:
    const ShardTestClient::ErrorSeen* errorFor_(int64_t corr) const {
        for (const auto& e : cl_.errors()) if (e.correlationId == corr) return &e;
        return nullptr;
    }
    ThreeNodes&               c_;
    ShardTestClient           cl_;
    std::map<int, int64_t>    sid_;
    uint64_t                  rid_ = 0;
    int64_t                   lastCorr_ = 0;
};

// ── senders ─────────────────────────────────────────────────────────────────────────────────────
Sender defineSender(const std::string& name, uint32_t partitions, bool typed) {
    return [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
        return cl.sendDefineTopic(ch, kClientStream, sid, name, partitions,
                                  typed ? charBlob() : std::vector<uint8_t>{}, /*persistence=Disk*/0, rid);
    };
}
Sender dropSender(const std::string& name) {
    return [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
        return cl.sendDropTopic(ch, kClientStream, sid, name, rid);
    };
}
Sender dropViewSender(const std::string& name) {
    return [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
        return cl.sendDropView(ch, kClientStream, sid, name, rid);
    };
}
Sender createViewSender(const std::string& name, int32_t srcTopicId) {
    return [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
        return cl.sendCreateView(ch, kClientStream, sid, name, "id", 0, viewPlanBlob(name, srcTopicId, 0), rid);
    };
}
Sender alterSender(const std::string& name, uint64_t uniq) {
    return [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
        return cl.sendAlterTopic(ch, kClientStream, sid, name, 0, alterOpsFor(Op::AlterAdd, uniq), rid);
    };
}

/// The catalog objectIds of every CreateTopic / CreateView event named `name` in broker `id`'s log.
std::vector<uint64_t> createdObjectIds(ThreeNodes& c, int id, const std::string& name) {
    std::vector<uint64_t> out;
    for (const auto& re : c.catalogEventsOn(id))
        if (re.event.objectName == name && (re.event.kind == rune::catalog::EventKind::CreateTopic ||
                                            re.event.kind == rune::catalog::EventKind::CreateView))
            out.push_back(re.event.objectId);
    return out;
}

// ===========================================================================
// The 32-cell slice
// ===========================================================================
struct CellParam { cm::Kind kind; cm::Timing timing; int viaNode; };   // viaNode: Steady only (1..3)

std::string cellName(const CellParam& p) {
    std::string s = std::string(cm::name(p.kind)) + "_" + std::string(cm::name(p.timing));
    if (p.timing == cm::Timing::Steady) s += "_via" + std::to_string(p.viaNode);
    return s;
}

std::vector<CellParam> allCells() {
    std::vector<CellParam> v;
    for (cm::Kind k : {cm::Kind::CreateTopic, cm::Kind::CreateView, cm::Kind::DropTopic, cm::Kind::AlterSchema}) {
        for (int via = 1; via <= 3; ++via) v.push_back({k, cm::Timing::Steady, via});
        for (cm::Timing t : {cm::Timing::LeaderChangeMidDdl, cm::Timing::RestartMidApply, cm::Timing::NewBrokerJoins,
                             cm::Timing::DropAndReuseName, cm::Timing::CreateRacesPlacement})
            v.push_back({k, t, 0});
    }
    return v;
}

class CatalogClusterSliceRegression : public ::testing::TestWithParam<CellParam> {};

} // namespace

TEST_P(CatalogClusterSliceRegression, RuntimeDdlThroughEveryBrokerThenDataThroughEveryBroker) {
    const auto [kind, timing, viaParam] = GetParam();
    const std::string cell = cellName(GetParam());
    if (cm::naRule(kind, {cm::Via::Leader, cm::Persist::Disk}, timing) != 0) GTEST_SKIP() << "R1";   // DropTopic x CreateRacesPlacement
    const auto t0 = Clock::now();
    struct TimeRecorder { std::string n; Clock::time_point t0; ~TimeRecorder() {
        CellTimes::rows().emplace_back(n, std::chrono::duration<double>(Clock::now() - t0).count()); } } recorder{cell, t0};

    ThreeNodes c = ThreeNodes::start("sl" + std::to_string(static_cast<int>(kind)) + std::to_string(static_cast<int>(timing)) + std::to_string(viaParam),
                                     timing == cm::Timing::NewBrokerJoins ? std::vector<int>{1, 2} : std::vector<int>{1, 2, 3});
    ASSERT_TRUE(c.awaitCatalogLeader(60s)) << cell;
    Rig rig(c);
    const int leader = c.settledLeader();
    ASSERT_GT(leader, 0);

    // ── precondition (through the leader, with the settle of a young cluster) ─────────────────────
    const bool reuse = timing == cm::Timing::DropAndReuseName;
    Expected tHave, readyHave, srcHave;
    int32_t srcId = -1, droppedId = -1;
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("ready", 1, true), "define ready", leader));
    switch (kind) {
    case cm::Kind::CreateTopic:
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t (to be dropped)", leader));
            TopicInfo old;
            Expected oldHave;   // the dropped topic's data: must never reappear in the reused name
            ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, oldHave, cell + "/old", &old));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropSender("t"), "drop t", leader));
        }
        break;
    case cm::Kind::CreateView:
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("src", 1, true), "define src", leader));
        {
            const auto si = rig.topicInfo(leader, "src", 30s);
            ASSERT_TRUE(si.has_value());
            srcId = si->id;
        }
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(createViewSender("v", srcId), "create v (to be dropped)", leader));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropViewSender("v"), "drop v", leader));
        }
        break;
    case cm::Kind::DropTopic:
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t", leader));
        { TopicInfo di; ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, tHave, cell + "/pre", &di)); droppedId = di.id; }   // data in the topic about to be dropped
        break;
    case cm::Kind::AlterSchema:
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t (to be dropped)", leader));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropSender("t"), "drop t", leader));
        }
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t", leader));
        break;
    default: FAIL() << "kind not in the slice";
    }
    // Every running broker has applied the precondition and publishes its DDL route before the cell starts.
    {
        std::string why;
        ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << cell << " precondition: " << why;
    }

    // ── the cell's DDL ─────────────────────────────────────────────────────────────────────────────
    Sender op;
    std::string target = "t";
    switch (kind) {
    case cm::Kind::CreateTopic: op = defineSender("t", 3, true); break;
    case cm::Kind::CreateView:  op = createViewSender("v", srcId); target = "v"; break;
    case cm::Kind::DropTopic:   op = dropSender("t"); break;
    case cm::Kind::AlterSchema: op = alterSender("t", 1); break;
    default: FAIL();
    }
    // The broker the DDL goes to: Steady - the param; every other timing - a follower of the catalog (so the
    // Redirect path runs in each cell) that is running; DropAndReuse / CreateRacesPlacement likewise.
    auto aRunningFollower = [&](int not1 = -1) {
        for (int n : c.runningNodes()) if (n != leader && n != not1) return n;
        return -1;
    };
    int via = timing == cm::Timing::Steady ? viaParam : aRunningFollower();
    ASSERT_GT(via, 0) << cell;
    if (!c.running(via)) via = aRunningFollower();   // NewBrokerJoins: node 3 is not up yet

    // Wait for the via broker's DDL route (a follower answers NotLeader until the control thread published it):
    // a probe DDL (a drop of a name that never exists) answers Redirect from a follower, UnknownTopic from the leader.
    {
        const auto deadline = Clock::now() + 60s;
        int probe = -1;
        while (Clock::now() < deadline) {
            const Answer a = rig.await(rig.sendOnly(via, dropSender("no_such_topic_probe"), rig.freshRid_()), 10s);
            probe = a.got ? a.code : -1;
            if (probe != cNotLeader && probe != -1) break;
            std::this_thread::sleep_for(100ms);
        }
        ASSERT_TRUE(probe == cRedirect || probe == cUnknown) << cell << ": the via broker never published its DDL route (" << probe << ")";
    }

    const RequestId rid = rig.freshRid_();
    Hooks hooks;
    int victim = -1;
    bool crashedLeader = false;
    bool placementReleased = false;
    uint64_t baseline = 0;
    switch (timing) {
    case cm::Timing::LeaderChangeMidDdl:
        c.node(leader).holdCatalogAnswersForTest(true);                 // the leader commits the DDL and never answers it
        baseline = c.node(leader).catalogEventsAppendedForTest();
        hooks.afterSend = [&](int, int node) {
            if (node != leader || crashedLeader) return false;
            crashedLeader = true;
            bool ok = false;
            rig.pumping([&] { ok = c.crashAfterNextAppend(leader, baseline, /*waitCommit=*/true, 30s); });
            EXPECT_TRUE(ok) << cell << ": the leader never appended + committed the DDL";
            rig.forget(leader);
            return true;                                               // in doubt: the crash ate the answer
        };
        break;
    case cm::Timing::RestartMidApply:
        victim = aRunningFollower(via);
        ASSERT_GT(victim, 0) << cell;
        rig.pumping([&] { c.stop(victim); });
        rig.forget(victim);
        ASSERT_TRUE(c.awaitCatalogIsrSizeOn(leader, 2, 60s)) << cell << ": the leader's ISR never shrank to the 2 running brokers";
        hooks.afterSend = [&](int, int node) {
            if (node != leader || victim < 0) return false;
            const int v = victim;
            victim = -1;
            rig.pumping([&] { c.startNode(v); });                       // restarted while the DDL commits / applies
            return false;
        };
        break;
    case cm::Timing::CreateRacesPlacement:
        if (kind == cm::Kind::CreateTopic || kind == cm::Kind::CreateView) {
            // R7 (OkAfterPlacement): the DDL commits and applies everywhere while the initial placement is HELD; the
            // object must refuse retriably through every broker, and the DDL's OK must not arrive, until the
            // placement is released.  (The DdlAnswerGate holds a DDL OK for at most 5 s: the 1.5 s window below is
            // well inside it.)  The hold covers every placement path (MetadataService::holdPlacementForTest).
            rig.holdPlacementEverywhere(true);
            hooks.afterSend = [&](int, int node) {
                if (node != leader || placementReleased) return false;
                const int64_t ddlCorr = rig.lastCorr();
                const std::string what = cell + " (placement held)";
                // The object is applied (read through a NON-leader broker: the leader's session queues behind its held DDL
                // answer), then every broker's control thread applies it a hop later: until it does, a probe answers
                // UnknownTopic / UnknownView - the warm-up waits for the first retriable refusal on each broker.
                const auto info = rig.topicInfo(via, target, 20s);
                EXPECT_TRUE(info.has_value()) << what << ": '" << target << "' never applied on node " << via;
                int probes = 0;
                if (info) {
                    auto refusing = [&](int n) {
                        if (kind == cm::Kind::CreateTopic) {
                            const int code = rig.produceOnce(n, info->id, 0, batchFor(true, 1, "warm"));
                            return code == cNotLeader || code == cRedirect;
                        }
                        const auto r = rig.viewRowFollowing(n, info->id, 101);
                        return r.answered && !r.finalIsError && r.finalCode == pc::ViewRowResponse::kNotReady;
                    };
                    for (int n : c.runningNodes()) {
                        const auto upTo = Clock::now() + 20s;
                        while (Clock::now() < upTo && !refusing(n)) std::this_thread::sleep_for(20ms);
                    }
                    for (const auto windowEnd = Clock::now() + 1500ms; Clock::now() < windowEnd;) {
                        for (int n : c.runningNodes()) {
                            if (kind == cm::Kind::CreateTopic) {
                                for (int p = 0; p < info->partitions; ++p) probes += rig.expectRetriableRefusal(n, info->id, p, what);
                            } else {
                                const auto r = rig.viewRowFollowing(n, info->id, 101);
                                ++probes;
                                EXPECT_TRUE(r.answered && !r.finalIsError && r.finalCode == pc::ViewRowResponse::kNotReady)
                                    << what << ": GetViewRow through node " << n << " answered "
                                    << (r.answered ? std::string(r.finalIsError ? "error " : "status ") + std::to_string(r.finalCode)
                                                   : std::string("nothing"))
                                    << " (want NotReady: never AbsentKey / UnknownView / OK before the placement commits)";
                            }
                        }
                    }
                }
                EXPECT_GE(probes, 6) << what << ": the no-placement window was observed (vacuity guard)";
                EXPECT_FALSE(rig.await(ddlCorr, 20ms).got) << what << ": the DDL was answered before its placement committed (R7)";
                rig.holdPlacementEverywhere(false);
                placementReleased = true;
                return false;
            };
        }
        break;
    default: break;
    }
    CellResult res = rig.drive(op, via, rid, hooks);
    res.viaWasLeader = via == leader;
    const cm::ExpectedCodes ec = cm::expectedCodes(kind, {res.viaWasLeader ? cm::Via::Leader : cm::Via::Follower, cm::Persist::Disk}, timing);
    EXPECT_EQ(res.firstCode, ec.first) << cell << res.trace;
    EXPECT_EQ(res.afterRedirectCode, ec.afterRedirect) << cell << res.trace;
    EXPECT_EQ(res.finalCode, ec.final) << cell << res.trace;
    ASSERT_EQ(res.finalCode, cOk) << cell << res.trace;

    // ── the timing's after-effects, then the identical-catalogs gate ───────────────────────────────
    if (timing == cm::Timing::LeaderChangeMidDdl) {
        ASSERT_TRUE(crashedLeader) << cell;
        ASSERT_TRUE(c.awaitCatalogLeader(60s)) << cell << ": no catalog leader after the crash";
        rig.pumping([&] { c.startNode(leader); });                      // the crashed leader rejoins as a follower
        rig.forget(leader);
    }
    if (timing == cm::Timing::NewBrokerJoins) { rig.pumping([&] { c.startNode(3); }); rig.forget(3); }
    if (timing == cm::Timing::RestartMidApply && victim > 0) { rig.pumping([&] { c.startNode(victim); }); rig.forget(victim); }
    {
        std::string why;
        ASSERT_TRUE(c.awaitCatalogDigestsEqual(90s, &why)) << cell << ": identical catalogs on every broker: " << why;
    }
    EXPECT_EQ(c.countEventsWithRequestId(res.rid), 1u) << cell << ": exactly one event per requestId, on every broker";

    // ── the effect on the data plane, through every broker ─────────────────────────────────────────
    switch (kind) {
    case cm::Kind::CreateTopic: {
        TopicInfo ti;
        std::vector<int> codes;
        ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, tHave, cell, &ti, &codes));
        if (reuse) {
            // A fresh, EMPTY topic: the exactly-once check above saw only this cell's records (none of the dropped topic's).
            const auto ids = createdObjectIds(c, leader, "t");
            ASSERT_EQ(ids.size(), 2u) << cell;
            EXPECT_NE(ids[0], ids[1]) << cell << ": a reused name gets a NEW objectId";
        }
        break;
    }
    case cm::Kind::CreateView: {
        TopicInfo si;
        ASSERT_NO_FATAL_FAILURE(rig.dataCheck("src", true, srcHave, cell, &si));
        // C1 / I5: a GetViewRow is answered by the broker that leads the source's partition (the view folds there); every
        // other broker redirects to it.  So the row of EVERY record every broker produced is read through EVERY running
        // broker and its hp is the value dataCheck produced (not merely "present", not only on the folder).
        const auto vi = rig.topicInfo(leader, "v", 30s);
        ASSERT_TRUE(vi.has_value()) << cell << ": view v is in no broker's catalog";
        for (int n : c.runningNodes()) {
            for (int m : c.runningNodes()) {
                const int64_t key = 100 * m + 1;   // the record node m produced through its own endpoint into p0
                const auto r = rig.viewRowSettled(n, vi->id, static_cast<uint64_t>(key), 30s);
                ASSERT_TRUE(r.ok()) << cell << ": GetViewRow(" << key << ") through node " << n << " ended "
                                    << (r.answered ? std::string(r.finalIsError ? "error " : "status ") + std::to_string(r.finalCode) : std::string("nothing"));
                EXPECT_EQ(rowHp(r.row), hpFor(m, "src")) << cell << ": the row of key " << key << " read through node " << n;
            }
        }
        if (reuse) {
            const auto ids = createdObjectIds(c, leader, "v");
            ASSERT_EQ(ids.size(), 2u) << cell;
            EXPECT_NE(ids[0], ids[1]) << cell << ": a reused view name gets a NEW objectId";
        }
        break;
    }
    case cm::Kind::DropTopic: {
        // The dropped topic is unknown on every broker (UnknownTopic), including to a producer that still holds its id.
        for (int n : c.runningNodes()) {
            int code = 0;
            const auto deadline = Clock::now() + 60s;
            while (Clock::now() < deadline) {
                (void)rig.topicInfo(n, "t", 100ms, &code);
                if (code == cUnknown) break;
                std::this_thread::sleep_for(50ms);
            }
            EXPECT_EQ(code, cUnknown) << cell << ": the dropped topic is still known on node " << n;
            std::vector<int> seenCodes;
            const int produceCode = rig.produceTerminal(n, droppedId, 0, batchFor(true, 1, "late"), cUnknown, 30s, &seenCodes);
            EXPECT_EQ(produceCode, cUnknown) << cell << ": a produce to the dropped id through node " << n;
            // M1: every answer on the way is a retriable refusal or the final UnknownTopic - a record accepted into the dropped topic (0)
            // or silence is never one of them.
            for (int sc : seenCodes) EXPECT_TRUE(sc == cNotLeader || sc == cRedirect || sc == cUnknown)
                << cell << ": a produce to the dropped id through node " << n << " answered " << sc << " (0 = accepted into a dropped topic)";
        }
        ASSERT_NO_FATAL_FAILURE(rig.dataCheck("ready", true, readyHave, cell));   // the survivor still serves everywhere
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "re-define t over the dropped name", leader));
            Expected fresh;
            ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, fresh, cell + "/reused"));
            const auto ids = createdObjectIds(c, leader, "t");
            ASSERT_EQ(ids.size(), 2u) << cell;
            EXPECT_NE(ids[0], ids[1]) << cell << ": the reused name got a NEW objectId";
        }
        break;
    }
    case cm::Kind::AlterSchema: {
        for (int n : c.runningNodes()) {
            const auto deadline = Clock::now() + 60s;
            std::optional<TopicInfo> ti;
            while (Clock::now() < deadline) {
                ti = rig.topicInfo(n, "t", 1s);
                if (ti && ti->version >= 2) break;
                std::this_thread::sleep_for(50ms);
            }
            ASSERT_TRUE(ti.has_value()) << cell << ": node " << n;
            EXPECT_GE(ti->version, 2) << cell << ": the altered schema (v2) on node " << n;
        }
        ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, tHave, cell));
        break;
    }
    default: break;
    }
}

INSTANTIATE_TEST_SUITE_P(Slice, CatalogClusterSliceRegression, ::testing::ValuesIn(allCells()),
                         [](const ::testing::TestParamInfo<CellParam>& i) { return cellName(i.param); });

// ===========================================================================
// The carry-over, pinned by its old symptom (E2 "multi-node runtime DDL fails")
// ===========================================================================

// Before the catalog: a runtime DefineTopic sent to a broker that was not the consensus leader failed (or the
// topic served on one broker only).  Now: defined through broker 2 (whichever role it holds), the topic is
// applied and placed on EVERY broker and serves produce(acks=all) + subscribe through each one.
TEST(CatalogClusterRegression, MultiNodeRuntimeDefineTopicServesOnEveryBrokerRegression) {
    ThreeNodes c = ThreeNodes::start("co_define");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("settle", 1, true), "settle", c.settledLeader()));
    const CellResult r = rig.drive(defineSender("t", 3, true), /*via=*/2, rig.freshRid_());
    ASSERT_EQ(r.finalCode, cOk) << r.trace;
    Expected have;
    ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, have, "co_define"));
    std::string why;
    EXPECT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(r.rid), 1u);
}

// The same for a runtime CreateView: it folds on the source partition's leader (the broker that owns the
// source's log) and its rows are readable there.  The view is created through broker 2.
TEST(CatalogClusterRegression, MultiNodeRuntimeCreateViewFoldsOnTheSourceLeaderRegression) {
    ThreeNodes c = ThreeNodes::start("co_view");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int leader = c.settledLeader();
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("src", 1, true), "define src", leader));
    const auto si = rig.topicInfo(leader, "src", 30s);
    ASSERT_TRUE(si.has_value());
    const CellResult r = rig.drive(createViewSender("v", si->id), /*via=*/2, rig.freshRid_());
    ASSERT_EQ(r.finalCode, cOk) << r.trace;
    Expected have;
    TopicInfo src;
    ASSERT_NO_FATAL_FAILURE(rig.dataCheck("src", true, have, "co_view", &src));
    const auto vi = rig.topicInfo(leader, "v", 30s);
    ASSERT_TRUE(vi.has_value());
    // C1 / I5: the source partition's leader folds it; the row of every record every broker produced is read through
    // EVERY broker (a non-folder redirects to the folder) and its hp is the value dataCheck produced.
    for (int n = 1; n <= 3; ++n) {
        for (int m = 1; m <= 3; ++m) {
            const auto r = rig.viewRowSettled(n, vi->id, static_cast<uint64_t>(100 * m + 1), 30s);
            ASSERT_TRUE(r.ok()) << "GetViewRow(" << 100 * m + 1 << ") through node " << n << " ended "
                                << (r.answered ? std::string(r.finalIsError ? "error " : "status ") + std::to_string(r.finalCode) : std::string("nothing"));
            EXPECT_EQ(rowHp(r.row), hpFor(m, "src")) << "the row of node " << m << "'s record, read through node " << n;
        }
    }
}

// ===========================================================================
// Task 22 fix round 1 (C1): a view row is served ONLY by the caught-up leader of its source partition.  Every other
// broker redirects to that leader (ErrorResponse{Redirect, the leader's client endpoint}); a read through ANY broker
// ends with the row of the key asked, never an AbsentKey for an existing key.
// RED (gateway Part A and the worker-return sentinel reverted): the non-folder brokers answer AbsentKey (1).
// RED (the worker's role check reverted, gateway kept): see GetViewRowRegression.ViewReadOnANonLeaderSourceReplica...
// ===========================================================================
TEST(CatalogClusterRegression, GetViewRowThroughEveryBrokerAnswersTheRowNeverAbsentKeyRegression) {
    ThreeNodes c = ThreeNodes::start("gvr_all");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int leader = c.settledLeader();
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("src", 1, true), "define src", leader));
    const auto si = rig.topicInfo(leader, "src", 30s);
    ASSERT_TRUE(si.has_value());
    ASSERT_NO_FATAL_FAILURE(rig.must(createViewSender("v", si->id), "create v", leader));
    const auto vi = rig.topicInfo(leader, "v", 30s);
    ASSERT_TRUE(vi.has_value());
    const std::pair<int64_t, int64_t> rows[] = {{101, 7}, {201, 8}, {301, 9}};
    for (const auto& [id, hp] : rows)
        ASSERT_TRUE(rig.produceFollowing(leader, si->id, 0, batchFor(true, id, asString(characterValue(id, hp))), 60s))
            << "record " << id << " was never acked";
    int folder = -1;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && folder < 0;) {
        folder = rig.partitionLeader(leader, si->id, 0);
        if (folder < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(folder, 0) << "the source partition has a leader";

    // Warm-up: the folder folds the three records; a non-folder broker's plan may still be on its way (NotReady / a bare
    // NotLeader / UnknownView while it is).  NotReady (2) is allowed only HERE, before the first OK of each broker.
    for (int n = 1; n <= 3; ++n) {
        for (const auto& [id, hp] : rows) {
            const auto r = rig.viewRowSettled(n, vi->id, static_cast<uint64_t>(id), 30s);
            ASSERT_TRUE(r.ok()) << "GetViewRow(" << id << ") through node " << n << " never reached OK: "
                                << (r.answered ? std::string(r.finalIsError ? "error " : "status ") + std::to_string(r.finalCode) : std::string("nothing"));
            EXPECT_EQ(rowHp(r.row), hp) << "node " << n << " key " << id;
        }
    }
    // Strict, from here on: one read per (broker, key) - the first answer is OK on the folder, the Redirect to the folder
    // everywhere else, and the final answer is the row (the hp asked for); AbsentKey / NotReady never.
    for (int n = 1; n <= 3; ++n) {
        for (const auto& [id, hp] : rows) {
            const auto r = rig.viewRowFollowing(n, vi->id, static_cast<uint64_t>(id));
            ASSERT_TRUE(r.ok()) << "node " << n << " key " << id << ": final " << r.finalCode << (r.finalIsError ? " (error)" : "");
            EXPECT_EQ(rowHp(r.row), hp) << "node " << n << " key " << id;
            if (n == folder) {
                EXPECT_FALSE(r.firstIsError) << "the source leader answers the row itself";
                EXPECT_EQ(r.finalNode, n);
            } else {
                EXPECT_TRUE(r.firstIsError && r.firstCode == cRedirect) << "node " << n << " is not the source leader: Redirect (first " << r.firstCode << ")";
                EXPECT_EQ(r.firstMessage, c.clientChannel(folder)) << "...to the source partition's leader";
                EXPECT_EQ(r.finalNode, folder);
            }
        }
        const auto absent = rig.viewRowFollowing(n, vi->id, 999);
        ASSERT_TRUE(absent.answered);
        EXPECT_TRUE(!absent.finalIsError && absent.finalCode == pc::ViewRowResponse::kAbsentKey)
            << "node " << n << ": a key that does not exist is AbsentKey after the redirect (got " << absent.finalCode << ")";
    }
}

// ===========================================================================
// Binding B1 (Task 8): after the LAST user topic is dropped the runtime publishes the static-balancer
// image, which names a leader for a NEW topic before its placement commits.  The new topic must not serve
// (nor silently take a record) until its placement is COMMITTED.  The controller's initial placement of the
// new topic is held (MetadataService::holdPlacementForTest) so the window is observable:
// every produce probe through every broker, while no placement exists for the partition, is refused
// retriably; once the placement is released the topic serves everywhere and holds exactly the records produced after.
// RED (remove the Subscribe gate: ClientGateway::handleSubscribe's NotPlaced branch answers nothing): the Subscribe probes see
// -1 (silence) and the test fails.  The PRODUCE probe is defense in depth with NO single-gate RED: besides servedPartition_'s
// NotPlaced answer, a partition with no committed placement has no replica anywhere (so the intended-role hint is Unknown and
// the owning worker has no replica to lead - its role gate refuses), and the routing image names no placed leader; removing
// only the NotPlaced answer leaves those layers refusing it.  (M3: recorded, not a gap.)
// ===========================================================================
TEST(CatalogClusterRegression, NewTopicAfterLastUserTopicDroppedWaitsForCommittedPlacementRegression) {
    ThreeNodes c = ThreeNodes::start("b1");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int leader = c.settledLeader();
    // The ONLY user topic: defined, served everywhere, then dropped.
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("a", 3, true), "define a", leader));
    Expected aHave;
    ASSERT_NO_FATAL_FAILURE(rig.dataCheck("a", true, aHave, "b1/a"));
    ASSERT_NO_FATAL_FAILURE(rig.must(dropSender("a"), "drop a", leader));
    auto userAssignments = [&](int n) {
        return c.node(n).onControlThreadForTest([&c, n]() -> int {
            int count = 0;
            c.node(n).metadataServiceForTest()->committedMap().forEach(
                [&](int32_t t, int32_t, const rune::metadata::PartitionAssignment&) { if (t >= 0) ++count; });
            return count;
        });
    };
    for (int n : c.runningNodes()) {
        const auto deadline = Clock::now() + 60s;
        while (Clock::now() < deadline && userAssignments(n) != 0) std::this_thread::sleep_for(50ms);
        ASSERT_EQ(userAssignments(n), 0) << "node " << n << ": the dropped topic's placements are still committed";
    }
    // Hold the initial placement of runtime topics (every broker; only the controller places).
    rig.holdPlacementEverywhere(true);
    const RequestId rid = rig.freshRid_();
    const int64_t corr = rig.sendOnly(leader, defineSender("b", 3, true), rid);
    ASSERT_GT(corr, 0);
    // (read through ANOTHER broker's session: the leader's session queues behind the held DDL answer, FIFO)
    const auto info = rig.topicInfo(leader == 1 ? 2 : 1, "b", 20s);
    ASSERT_TRUE(info.has_value()) << "the create applied";
    // Every probe is refused RETRIABLY (Redirect / NotLeader, one Redirect hop followed) - never served, never
    // UnknownTopic, never silence (R-I2) - on every broker, for produce and Subscribe alike.  No "placed before and after"
    // filter: the hold covers every placement path (MetadataService::holdPlacementForTest), so there is no placement.
    // (the DDL's OK is held by the answer gate for at most 5 s: probe for ~2 s, then release the placement)
    // Warm-up (N-M1): each broker's control thread applies the topic a hop after the admin thread; until it does a probe answers
    // UnknownTopic (7).  Wait for the first retriable refusal on every broker.
    for (int n : c.runningNodes()) {
        const auto upTo = Clock::now() + 20s;
        while (Clock::now() < upTo) {
            const int code = rig.produceOnce(n, info->id, 0, batchFor(true, 1, "warm"));
            if (code == cNotLeader || code == cRedirect) break;
            std::this_thread::sleep_for(20ms);
        }
    }
    int probes = 0;
    const auto windowEnd = Clock::now() + 2000ms;
    while (Clock::now() < windowEnd) {
        for (int n : c.runningNodes())
            for (int p = 0; p < info->partitions && Clock::now() < windowEnd; ++p)
                probes += rig.expectRetriableRefusal(n, info->id, p, "b1");
        std::this_thread::sleep_for(20ms);
    }
    // Vacuity guard: at least one full sweep (a produce + a Subscribe per broker per partition).
    EXPECT_GE(probes, 2 * static_cast<int>(c.runningNodes().size()) * info->partitions) << "the no-placement window was probed";
    rig.holdPlacementEverywhere(false);   // the placement is emitted and commits; the DDL's OK (held until the topic serves) arrives
    const Answer a = rig.await(corr, 30s);
    ASSERT_TRUE(a.got);
    EXPECT_EQ(a.code, cOk);
    Expected bHave;
    ASSERT_NO_FATAL_FAILURE(rig.dataCheck("b", true, bHave, "b1/b"));   // exactly the records produced AFTER the placement
    for (int n : c.runningNodes()) EXPECT_EQ(userAssignments(n), info->partitions) << "node " << n;
}

// ===========================================================================
// Binding B3 (Task 13): an UNCOMMITTED catalog event on the leader that dies.  The leader appended a DDL
// event while both followers were down, then crashed before it could commit; the followers return and
// elect a leader WITHOUT it; a different DDL commits at the same offset.  The old leader rejoining
// truncates the event; every broker's catalog digest is equal; the DDL's client was left in doubt and
// the SAME-requestId retry gets a fresh decision - executed exactly once.
// RED (skip the truncation on rejoin): the old leader keeps the event - digests never converge.
// ===========================================================================
TEST(CatalogClusterRegression, UncommittedCatalogEventIsTruncatedOnTheRejoiningOldLeaderRegression) {
    ThreeNodes c = ThreeNodes::startWith("b3", [](NodeConfig& cfg) { cfg.replicaLagMaxMs = 120'000; });   // the ISR keeps both followers
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int L = c.settledLeader();
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("ready", 1, true), "ready", L));
    std::string why;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    std::vector<int> followers;
    for (int n : c.runningNodes()) if (n != L) followers.push_back(n);
    ASSERT_EQ(followers.size(), 2u);

    rig.pumping([&] { for (int f : followers) c.crash(f); });
    for (int f : followers) rig.forget(f);
    const RequestId R{0xB3, 1};
    const uint64_t base = c.node(L).catalogEventsAppendedForTest();
    const int64_t corr = rig.sendOnly(L, defineSender("z", 1, true), R);
    ASSERT_GT(corr, 0);
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && c.node(L).catalogEventsAppendedForTest() <= base;)
        std::this_thread::sleep_for(5ms);
    ASSERT_GT(c.node(L).catalogEventsAppendedForTest(), base) << "the leader appended the DDL's event";
    const auto snap = ThreeNodes::snapOf(c.node(L));
    ASSERT_GT(snap.leo, snap.hw) << "the event is NOT committed (both followers are down)";
    const int64_t zOffset = snap.leo - 1;
    ASSERT_EQ(c.eventsWithRequestIdOn(L, R), 1u);
    rig.pumping([&] { c.crash(L); });                       // dies with it
    rig.forget(L);

    rig.pumping([&] { for (int f : followers) c.startNode(f); });
    ASSERT_TRUE(c.awaitCatalogLeader(90s));
    for (int f : followers) EXPECT_EQ(c.eventsWithRequestIdOn(f, R), 0u) << "node " << f << " never saw the uncommitted event";
    const Answer lost = rig.await(corr, 200ms);
    if (lost.got) EXPECT_EQ(lost.code, cNotLeader) << "the original client is told NotLeader or left in doubt - never OK";
    const RequestId R2{0xB3, 2};
    const CellResult w = rig.drive(defineSender("w", 1, true), c.settledLeader(), R2);
    ASSERT_EQ(w.finalCode, cOk) << w.trace;

    rig.pumping([&] { c.startNode(L); });                   // the old leader rejoins with its uncommitted tail
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(90s, &why)) << "identical catalogs on every broker after the rejoin: " << why;
    for (int n = 1; n <= 3; ++n) {
        EXPECT_EQ(c.eventsWithRequestIdOn(n, R), 0u) << "node " << n << ": the uncommitted event was truncated";
        EXPECT_EQ(c.eventsWithRequestIdOn(n, R2), 1u) << "node " << n;
    }
    const auto ev = c.catalogEventsOn(L);
    ASSERT_GT(static_cast<int64_t>(ev.size()), zOffset);
    EXPECT_EQ(ev[static_cast<std::size_t>(zOffset)].event.principal.requestId, R2) << "the new leader's event took the truncated one's offset";

    // The retry of the original DDL (same requestId) is decided afresh - executed exactly once.
    const CellResult retry = rig.drive(defineSender("z", 1, true), c.settledLeader(), R);
    ASSERT_EQ(retry.finalCode, cOk) << retry.trace;
    const CellResult again = rig.drive(defineSender("z", 1, true), c.settledLeader(), R);
    EXPECT_EQ(again.finalCode, cOk) << "a second retry gets the original result, not TopicExists: " << again.trace;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(R), 1u);
}

// ===========================================================================
// Binding B4 (Task 20 I1): the new-leader catch-up gate on real nodes.  The followers' appliers are held,
// a DDL commits (applied only by the old leader), the old leader is crashed: the new leader holds a
// committed event it has NOT applied.  A DDL sent at once is answered NotLeader (retriable) - never OK,
// never appended onto a state that lacks the event - until the applier runs; then it succeeds exactly once.
// RED (remove the gate): the DDL is appended (and left unanswered while the applier is held).
// ===========================================================================
TEST(CatalogClusterRegression, NewCatalogLeaderRefusesDdlUntilCaughtUpRegression) {
    ThreeNodes c = ThreeNodes::start("b4");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int L = c.settledLeader();
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("ready", 1, true), "ready", L));
    std::string why;
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(60s, &why)) << why;
    std::vector<int> followers;
    for (int n : c.runningNodes()) if (n != L) followers.push_back(n);
    for (int f : followers) c.node(f).holdCatalogApplyForTest(true);
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("x", 1, true), "x (applied by the old leader only)", L));
    const auto leaderView = ThreeNodes::snapOf(c.node(L));
    for (int f : followers) ASSERT_TRUE(c.awaitCatalogEndOffset(c.node(f), leaderView.leo, 20s)) << "node " << f << " holds x (replicated, unapplied)";
    rig.pumping([&] { c.crash(L); });
    rig.forget(L);
    ASSERT_TRUE(c.awaitCatalogLeaderOtherThan(L, 60s));
    const int N = c.settledLeader();
    ASSERT_GT(N, 0);
    ASSERT_NE(N, L);

    const RequestId Ry{0xB4, 1};
    const uint64_t base = c.node(N).catalogEventsAppendedForTest();
    int refused = 0;
    for (const auto until = Clock::now() + 2500ms; Clock::now() < until;) {
        const Answer a = rig.await(rig.sendOnly(N, defineSender("y", 1, true), Ry), 8s);
        ASSERT_TRUE(a.got) << "a DDL is answered (never left to hang)";
        EXPECT_EQ(a.code, cNotLeader) << "the new leader has not applied the committed tail: retriable NotLeader, never OK";
        if (a.code == cNotLeader) ++refused;
        std::this_thread::sleep_for(100ms);
    }
    EXPECT_GE(refused, 5);
    EXPECT_EQ(c.node(N).catalogEventsAppendedForTest(), base) << "nothing was appended onto a state without x";
    EXPECT_EQ(c.eventsWithRequestIdOn(N, Ry), 0u);
    EXPECT_FALSE(rig.topicInfo(N, "x", 200ms).has_value()) << "x is not applied on the new leader yet";

    for (int f : followers) c.node(f).holdCatalogApplyForTest(false);   // the applier runs
    const CellResult y = rig.drive(defineSender("y", 1, true), N, Ry);
    ASSERT_EQ(y.finalCode, cOk) << y.trace;
    EXPECT_TRUE(rig.topicInfo(N, "x", 30s).has_value()) << "x is applied on the new leader";
    {   // M7: x is before y in the log, and the applier is past y (x was applied before y was accepted - not merely "eventually")
        int64_t xOff = -1, yOff = -1;
        for (const auto& re : c.catalogEventsOn(N)) {
            if (re.event.objectName == "x" && re.event.kind == rune::catalog::EventKind::CreateTopic) xOff = static_cast<int64_t>(re.offset);
            if (re.event.principal.requestId == Ry) yOff = static_cast<int64_t>(re.offset);
        }
        ASSERT_GE(xOff, 0);
        ASSERT_GE(yOff, 0);
        EXPECT_LT(xOff, yOff) << "x precedes y in the catalog log";
        EXPECT_GT(ThreeNodes::snapOf(c.node(N)).applied, yOff) << "the applier is past y";
    }
    rig.pumping([&] { c.startNode(L); });
    ASSERT_TRUE(c.awaitCatalogDigestsEqual(90s, &why)) << why;
    EXPECT_EQ(c.countEventsWithRequestId(Ry), 1u);
}
