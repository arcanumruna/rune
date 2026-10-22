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
// DropTopic x CreateRacesPlacement (R10): the topic is dropped after its create applied everywhere and before its placement committed.
//
// CreateRacesPlacement (CreateTopic / CreateView) really races placement (fix round 1, R7 OkAfterPlacement): the initial
// placement is HELD on every broker, the DDL commits + applies, every broker is probed for 1.5 s (a produce + Subscribe per
// partition / a GetViewRow: retriable refusal only), no DDL answer may arrive in the window; then the placement is released.
// Views are read through EVERY broker (a non-source broker redirects to the source leader, C1) and the hp checked.
// CreateView under VLS B.6 (checkpoint fix B): a view sink is replica-less and owns NO placement, so the held placement gates
// nothing of it - the cell instead pre-loads a sentinel key into the source and checks the view's REAL readiness gates
// (committed + applied, plan delivered, source folded): never AbsentKey / UnknownView / a wrong row of the sentinel once
// applied, the sentinel row served through every broker while the placement is held, and the OK allowed in the window.
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
#include "support/SchedulerWatchdog.h"   // the harness-side process-starvation signal
#include "support/ShardTestClient.h"

#include <rune/gateway/ClientGateway.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/MetadataService.h>
#include <rune/protocol/PartitionHash.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>

#include <fcntl.h>
#if defined(__APPLE__)
#include <sys/sysctl.h>
#endif
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <csignal>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <map>
#include <set>
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

constexpr int cOk = 0, cNotLeader = 2, cInternal = 6, cRedirect = 3, cUnknown = 7;
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
/// CreateView x CreateRacesPlacement (VLS B.6): the key / hp the source holds before the CreateView (never a dataCheck key 100n+10p+1).
inline constexpr int64_t kRacesViewSentinelKey = 9;
inline constexpr int64_t kRacesViewSentinelHp  = 4242;
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

/// diag-duplicate F1: an idempotent identity - one per LOGICAL record (seq 0, epoch 0); every retry of that record reuses it,
/// so an in-doubt NotLeader (the batch may already be in the log and commit when leadership comes back) retried is
/// deduplicated by the broker instead of stored twice.  A fresh pid per record: no sequence bookkeeping, a first send is
/// always Accept (an unknown pid at seq 0), a retry is always a Duplicate, and topic-id reuse is irrelevant.
struct ProduceIdentity { int64_t pid = -1; int16_t epoch = 0; int32_t seq = 0; };
/// Test-thread only (the Rig is single-threaded); unique within the process, so within every cluster a test builds.
inline int64_t gNextTestPid = 7'000'000;
[[nodiscard]] ProduceIdentity freshIdentity() { return ProduceIdentity{gNextTestPid++, 0, 0}; }
/// batchFor with the identity in the batch header (it must equal the frame's: ProducePath refuses a mismatch).
[[nodiscard]] std::vector<uint8_t> batchFor(bool typed, int64_t id, const std::string& value, const ProduceIdentity& ident) {
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.setProducerId(ident.pid);
    b.setProducerEpoch(ident.epoch);
    b.setBaseSequence(ident.seq);
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
    std::vector<int> codes;   ///< every NON-final answer (Redirect / NotLeader / the gate's in-doubt Internal), in order
};

/// Cell wall times, printed once at the end of the run.
struct CellTimes {
    static std::vector<std::pair<std::string, double>>& rows() {
        static std::vector<std::pair<std::string, double>> r;
        return r;
    }
    static int& perturbed() { static int n = 0; return n; }   ///< cells whose exact codes were relaxed (product evidence + a stall)
    static int& notExercised() { static int n = 0; return n; }   ///< cells whose timing never ran (a stall explains it)
    static int& slowPasses() { static int n = 0; return n; }     ///< cells with a >= 100 ms broker pass, watchdog quiet, no evidence
};
class CellTimesPrinter : public ::testing::Environment {
public:
    void TearDown() override {
        double total = 0;
        for (const auto& [n, s] : CellTimes::rows()) { total += s; std::fprintf(stderr, "[ cell time ] %-60s %7.1f s\n", n.c_str(), s); }
        if (!CellTimes::rows().empty()) std::fprintf(stderr, "[ cell time ] %-60s %7.1f s\n", "TOTAL", total);
        std::fprintf(stderr, "[ cell time ] perturbed cells: %d, not-exercised cells: %d, slow-pass cells: %d\n",
                     CellTimes::perturbed(), CellTimes::notExercised(), CellTimes::slowPasses());
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
    /// reconnectSilent = false: the F-A guard is off (T-P2 only: it must see the broker's own expiry notice).
    explicit Rig(ThreeNodes& c, bool reconnectSilent = true)
        : c_(c), cl_(c.clientsDir->path / "wire"), reconnectSilent_(reconnectSilent) {
        cl_.keepSessionsAlive(true);
        cl_.setClientVersion(pc::kClientVersion);   // a v8 session: DDL Redirects are followed
    }
    ShardTestClient& cl() { return cl_; }

    /// A session on broker `n` (connected once, retried while its client endpoint warms up).
    int64_t session(int n) {
        // F-A: the gateway reaps a session silent for kDefaultSessionTimeoutNs (10 s).  A sid THIS harness left silent
        // for half of that (a harness-observed cause, never product evidence) or that the broker said expired is
        // reconnected - like the real client's stale-session Resuming - and counted.
        constexpr auto kLeaseMargin = std::chrono::nanoseconds(rune::gateway::ClientGateway::kDefaultSessionTimeoutNs / 2);
        if (auto it = sid_.find(n); it != sid_.end()) {
            const auto silent  = cl_.silentFor(it->second);
            const bool expired = cl_.sessionExpired(it->second);
            if (!reconnectSilent_ || (silent < kLeaseMargin && !expired)) return it->second;
            // F-A review I2: the broker's own expiry of a session THIS client kept alive (silent < half the lease) is
            // product evidence with no harness-observed cause - a premature reap / a lost touch: the test fails.
            if (expired && silent < kLeaseMargin)
                ADD_FAILURE() << "broker n" << n << " expired session " << it->second << " that this client kept alive ("
                              << std::chrono::duration_cast<std::chrono::milliseconds>(silent).count()
                              << " ms silent < half the lease)";
            std::fprintf(stderr, "[ rig ] n%d session %lld silent %lld ms%s: reconnecting\n", n,
                         static_cast<long long>(it->second),
                         static_cast<long long>(
                             std::chrono::duration_cast<std::chrono::milliseconds>(cl_.silentFor(it->second)).count()),
                         cl_.sessionExpired(it->second) ? " (broker: expired)" : "");
            cl_.dropSession(it->second);
            sid_.erase(it);
            ++silentReconnects_;
        }
        if (!c_.running(n)) return -1;
        for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline;) {
            const int64_t s = cl_.connect(c_.clientChannel(n), kClientStream);
            if (s > 0) { sid_[n] = s; return s; }
            std::this_thread::sleep_for(50ms);
        }
        return -1;
    }
    void forget(int n) {
        if (auto it = sid_.find(n); it != sid_.end()) { cl_.dropSession(it->second); sid_.erase(it); }
    }
    /// F-A: how many cached sessions this Rig found silent past half the lease (or broker-expired) and reconnected.
    [[nodiscard]] int silentReconnects() const noexcept { return silentReconnects_; }
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
        silentAtSend_ = cl_.silentFor(sid);   // F-A: always < half the lease after session(); the no-answer message shows it
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
                ADD_FAILURE() << "node " << cur << (corr == 0 ? " could not be sent the DDL" : " never answered the DDL (25 s)")
                              << " (session silent before send: "
                              << std::chrono::duration_cast<std::chrono::milliseconds>(silentAtSend_).count() << " ms)"
                              << r.trace;
                r.finalCode = cNoAnswer;
                return r;
            }
            if (sendNo == 0) r.firstCode = code;
            else if (sendNo == 1 && r.firstCode == cRedirect) r.afterRedirectCode = code;
            ++sendNo;
            // Slice Recipe 2 step 4: the DdlAnswerGate's overdue answer ("placement pending") is the IN-DOUBT Internal - the
            // event committed, the topic does not serve yet: retried with the SAME rid on the same broker, like NotLeader.
            // Any other Internal stays terminal.
            if (code == cInternal && a.msg.find("placement pending") != std::string::npos) {
                r.codes.push_back(cInternal);
                std::this_thread::sleep_for(100ms);
                continue;
            }
            if (code == cRedirect || code == cNotLeader) r.codes.push_back(code);
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
    [[nodiscard]] bool placementHeld() const noexcept { return placementHeld_; }

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
    /// `ident` (diag-duplicate F1): sent idempotently with that identity (the batch must carry it too); null = pid -1
    /// (the probes / warm-ups / late produces, which no exactly-once check reads).  `offset`: the OK ack's base offset.
    int produceOnce(int node, int32_t topicId, int p, const std::vector<uint8_t>& batch, std::string* redirect = nullptr,
                    const ProduceIdentity* ident = nullptr, int64_t* offset = nullptr) {
        const int64_t sid = session(node);
        if (sid <= 0) return -1;
        const int64_t corr = cl_.nextCorrelationId();
        const bool sent = ident != nullptr
            ? cl_.produceIdempotent(c_.clientChannel(node), kClientStream, sid, corr, topicId, p, /*acks=All*/1, ident->pid,
                                    ident->epoch, ident->seq, batch)
            : cl_.produce(c_.clientChannel(node), kClientStream, sid, corr, topicId, p, /*acks=All*/1, batch);
        if (!sent) return -1;
        cl_.pollUntil([&] { return cl_.ackFor(corr).has_value() || errorFor_(corr) != nullptr; });
        if (const auto ack = cl_.ackFor(corr)) {
            if (offset && ack->errorCode == 0) *offset = ack->baseOffset;
            return ack->errorCode;
        }
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
    /// diag-duplicate F1: every send of the record - the first, each Redirect hop, each retry after NotLeader / Internal -
    /// carries the SAME bytes and identity (an in-doubt answer's retry is then a broker Duplicate, never a second record).
    bool produceFollowing(int node, int32_t topicId, int p, const std::vector<uint8_t>& batch, std::chrono::seconds within,
                          const ProduceIdentity& ident, std::vector<int>* seen = nullptr, int64_t* offset = nullptr) {
        for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
            int cur = node;
            for (int hop = 0; hop < 3; ++hop) {
                std::string redirect;
                const int code = produceOnce(cur, topicId, p, batch, &redirect, &ident, offset);
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
                const ProduceIdentity ident = freshIdentity();   // diag-duplicate F1: one identity per logical record
                const bool accepted = produceFollowing(n, info->id, p, batchFor(typed, id, value, ident), 60s, ident, &seenCodes);
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
        placementHeld_ = on;
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
    bool                      placementHeld_ = false;
    bool                      reconnectSilent_ = true;   ///< F-A guard (Rig::session)
    int                       silentReconnects_ = 0;
    std::chrono::nanoseconds  silentAtSend_{0};
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
/// R3-M1: a non-injected perturbed cell is appended to $RUNE_PERTURBED_LOG (scripts/run-regression.sh prints it after the run
/// for a human look; it does not fail the run).  One O_APPEND write per line (concurrent test processes do not interleave).
inline void reportPerturbedForCheckpoint(const std::string& cell, const std::string& why, const std::string& stall) {
    const char* path = std::getenv("RUNE_PERTURBED_LOG");
    if (path == nullptr || *path == 0) return;
    const std::string line = "[ perturbed ] " + cell + ":" + why + " " + stall + "\n";
    const int fd = ::open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) return;
    (void)!::write(fd, line.data(), line.size());
    ::close(fd);
}

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

// ===========================================================================
// Perturbation evidence (slice Recipe 3).  A host stall can move the catalog topology DURING a cell (re-election at
// 150 ms of heartbeat silence, false fencing at 450 ms).  The cell detects it from PRODUCT evidence independent of the
// client's answers - the leader epoch, the by-cause NotLeader counters of every broker, the gate's overdue counter -
// and then asserts the contract of THAT event; with no evidence the matrix's exact codes apply.
// ===========================================================================
//
// Task 23 fix round 3 (R2-I1): that product evidence relaxes the exact codes ONLY when the harness also sees a cause for it -
// a broker admin / control thread that was demonstrably not scheduled for >= kStallEvidenceNs (its max inter-pass gap,
// measured by the broker thread itself, independent of every protocol decision), or a stall the test injected (CellSeams::
// injectsStall).  Product evidence with every thread scheduled is a spurious fencing / demotion / re-election: a FAILURE.
// ===========================================================================
/// The stall that can legitimately move the catalog topology: the election timeout is 150 ms (CatalogBrokerCluster's
/// electionTimeoutMs, the production default) and a broker is fenced after 3 x 150 = 450 ms of heartbeat silence; a thread
/// unscheduled for two-thirds of the election timeout (100 ms) can lose a heartbeat.  Anything shorter cannot explain a move.
constexpr int64_t kStallEvidenceNs = 100'000'000;
inline long long msOf(int64_t ns) { return static_cast<long long>(ns / 1'000'000); }
/// The harness-observed stall: any of the three signals - the brokers' admin pass gap, their control pass gap (both folded into
/// `maxPassGapNs`) and the test process's SchedulerWatchdog overshoot - reached kStallEvidenceNs.
/// A stall takes effect after it ends: heartbeat silence (150 ms) -> fencing (3 x 150 = 450 ms) -> an election and the
/// controller's AlterIsr commit (a few 20 ms status intervals).  So a stall excuses a piece of evidence at time t only if it
/// overlaps [t - kStallLookbackNs, t]: it PRECEDES the evidence by at most 1 s (Task 23 fix round 4, R3-M1).
constexpr int64_t kStallLookbackNs = 1'000'000'000;

/// Task 23 fix round 4 (R3-I1): ONLY the scheduler watchdog - a test-process thread doing no product work - measures a stall.
/// A broker's own pass gap is produced by the very regression this test must catch (a blocking runtime / admin pass misses
/// heartbeats -> re-election / fencing), so it never excuses evidence; it is a product signal of its own.
inline bool schedulerStall(int64_t watchdogGapNs) { return watchdogGapNs >= kStallEvidenceNs; }

/// How a cell's codes are judged.
enum class CellVerdict { Exact, SlowPass, Perturbed, BlockingPass, NoStall };
/// evidence: product evidence of a topology change; excused: a watchdog stall precedes it within the lookback, or the test
/// injected the stall; watchdogAnyNs: the largest watchdog overshoot anywhere in the cell's window; maxPassGapNs: the
/// largest broker admin / control pass gap.
inline CellVerdict classifyCell(bool evidence, bool excused, int64_t watchdogAnyNs, int64_t maxPassGapNs, bool injected) {
    const bool passBlocked = maxPassGapNs >= kStallEvidenceNs && !schedulerStall(watchdogAnyNs) && !injected;
    if (!evidence) return passBlocked ? CellVerdict::SlowPass : CellVerdict::Exact;
    if (excused) return CellVerdict::Perturbed;
    return passBlocked ? CellVerdict::BlockingPass : CellVerdict::NoStall;
}

struct Perturbation {
    int32_t                                     catalogLeaderEpoch = -1;
    std::map<int, BrokerNode::CatalogNotLeaderCounts> nl;   // per running broker
    std::map<int, uint64_t>                     overdue;    // per running broker
    std::map<int, BrokerNode::PassGaps>         gaps;       // per running broker: max inter-pass gaps since the reset
    bool                                        leaderMovedInCell = false, gateOverdueObserved = false;
    bool                                        injectedStall = false;   ///< the test's own seam stalled a broker / the harness
    bool                                        earlyNotLeader = false;  ///< a NotLeader reached the R7 window (evidence)
    std::map<int, BrokerNode::EvidenceTimes>    ev;         // per running broker: when it produced evidence since the reset
    int64_t                                     harnessFirstNs = 0, harnessLastNs = 0;   // leader-moved / early in-doubt seen
    void stampHarness() {
        const int64_t t = rune::node::testing::SchedulerWatchdog::nowNs();
        if (harnessFirstNs == 0) harnessFirstNs = t;
        harnessLastNs = t;
    }
    /// The first and last evidence time over the brokers present in both readings and not `excluded`, and the harness's.
    [[nodiscard]] std::pair<int64_t, int64_t> evidenceSpan(const Perturbation& p0, const std::set<int>& excluded,
                                                           const Perturbation& harness) const {
        int64_t first = harness.harnessFirstNs, last = harness.harnessLastNs;
        for (const auto& [id, e] : ev) {
            if (p0.ev.count(id) == 0 || excluded.count(id) != 0 || e.firstNs == 0) continue;
            if (first == 0 || e.firstNs < first) first = e.firstNs;
            if (e.lastNs > last) last = e.lastNs;
        }
        return {first, last};
    }
    /// The baseline: resets every running broker's pass-gap maxima (on their owner threads), then reads the counters.
    static Perturbation snapshot(ThreeNodes& c) {
        for (int n : c.runningNodes()) c.node(n).resetPassGapsForTest();
        return read(c);
    }
    /// The after-drive reading (no reset): the counters and every running broker's max pass gaps since the baseline.
    static Perturbation read(ThreeNodes& c) {
        Perturbation p;
        for (int n : c.runningNodes()) {
            if (p.catalogLeaderEpoch < 0)
                if (const auto pa = catalogAssignmentOf(c.node(n))) p.catalogLeaderEpoch = pa->leaderEpoch;
            p.nl[n] = c.node(n).catalogNotLeaderCountsForTest();
            p.overdue[n] = c.node(n).ddlAnswersOverdueForTest();
            p.gaps[n] = c.node(n).passGapsForTest();
            p.ev[n] = c.node(n).evidenceTimesForTest();
        }
        return p;
    }
    /// The largest admin / control pass gap of the brokers present in both readings and not `excluded` (a broker the
    /// timing stops / restarts measures from its restart; it is excluded exactly like its counters).
    [[nodiscard]] int64_t maxGapNs(const Perturbation& p0, const std::set<int>& excluded) const {
        int64_t m = 0;
        for (const auto& [id, g] : gaps)
            if (p0.gaps.count(id) != 0 && excluded.count(id) == 0) m = std::max({m, g.adminMaxGapNs, g.controlMaxGapNs});
        return m;
    }
    /// What changed since `p0` ("" = unperturbed).  Brokers the cell's own timing stops / restarts (`excluded`) are not
    /// compared (a restarted broker's counters restart); only brokers present in both snapshots are.  `useEpoch` false:
    /// the timing moves the leader itself (LeaderChangeMidDdl).
    [[nodiscard]] std::string since(const Perturbation& p0, const std::set<int>& excluded, bool useEpoch) const {
        std::ostringstream o;
        if (useEpoch && catalogLeaderEpoch != p0.catalogLeaderEpoch)
            o << " leader-epoch " << p0.catalogLeaderEpoch << "->" << catalogLeaderEpoch;
        for (const auto& [id, now] : nl) {
            const auto it = p0.nl.find(id);
            if (it == p0.nl.end() || excluded.count(id) != 0) continue;
            const auto& was = it->second;
            if (now.belowMinIsr > was.belowMinIsr) o << " n" << id << ":notLeader(belowMinIsr)+" << now.belowMinIsr - was.belowMinIsr;
            if (now.commitTimeout > was.commitTimeout) o << " n" << id << ":notLeader(commitTimeout)+" << now.commitTimeout - was.commitTimeout;
            if (now.demoted > was.demoted) o << " n" << id << ":notLeader(demoted)+" << now.demoted - was.demoted;
        }
        for (const auto& [id, now] : overdue) {
            const auto it = p0.overdue.find(id);
            if (it != p0.overdue.end() && excluded.count(id) == 0 && now > it->second) o << " n" << id << ":gate-overdue+" << now - it->second;
        }
        return o.str();
    }
};

/// The roles a cell resolved (after its precondition).
struct CellRoles { int preLeader = -1, leader = -1, via = -1; int silentReconnects = 0; };   ///< silentReconnects: F-A
/// What a cell observed (for the seam tests).
struct CellOutcome {
    CellResult  res;
    CellRoles   roles;
    std::string perturbedWhy;   ///< "" = no product evidence (the exact codes were asserted)
    uint64_t    belowMinIsrOnLeader = 0;
    bool        stallObserved = false;   ///< a watchdog stall precedes the evidence within kStallLookbackNs (the excuse)
    bool        passBlocked = false;     ///< a broker pass gap >= kStallEvidenceNs with the watchdog quiet (not injected)
    int64_t     maxGapNs = 0;            ///< the max admin / control pass gap
    int64_t     watchdogGapNs = 0;       ///< the test process's SchedulerWatchdog overshoot, anywhere in [baseline - 1 s, end]
};
/// Test seams of runCell (all optional).
struct CellSeams {
    std::function<void(NodeConfig&)>                   configTweak;
    std::function<void(ThreeNodes&, Rig&)>             afterPrecondition;
    std::function<void(ThreeNodes&, Rig&, int)>        onLeaderSend;
    std::function<void(ThreeNodes&, Rig&)>             afterAnswerRelease;
    bool                                               answerHoldInWindow = true;   ///< false: the RED of Recipe 2 step 2
    bool                                               injectsStall = false;  ///< the seams stall a broker / the harness themselves
    bool                                               suppressTimingHook = false;   ///< T9: RestartMidApply's hook never acts
    std::function<void()>                              beforeBaseline;   ///< T10: runs right before the baseline snapshot
    std::function<void(ThreeNodes&)>                   afterIsrShrink;   ///< F-A T-H3: runs inside the ISR wait's pump
};

/// SIGSTOP this whole process for `ms` from a forked child, and return only once the stop really took effect for `ms`.
///
/// T10 flake (root cause, measured): forking the 3-broker test process takes ~35-40 ms on macOS, during which the kernel holds
/// every thread of the parent; the child runs ~35-40 ms after fork() started, while the parent's forking thread is still inside
/// fork().  The child's SIGSTOP landed there.  In ~15% of runs only the forking thread stayed stopped (it returned from fork()
/// ~340 ms later); the fork's own release resumed the other threads, so the watchdog thread saw only the 36-38 ms fork hold, not
/// the 300 ms stop.  Fix: (1) the child waits on a pipe until the parent is back from fork() - the stop now always hits a fully
/// running process; (2) it sends SIGSTOP and confirms the kernel's process state (processIsStopped: sysctl on macOS, /proc/<pid>/stat on Linux) before
/// (3) holding it `ms` from that point, then SIGCONT.  The
/// child's exit status reports whether the stop was confirmed; the parent throws if not (never a silent short stop).
/// The child only makes plain syscalls (read, kill, sysctl or open/read, nanosleep, _exit).
/// Whether the kernel reports `pid` stopped (SSTOP / state 'T'). Plain syscalls only: it runs in the forked child.
inline bool processIsStopped(pid_t pid) {
#if defined(__APPLE__)
    int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
    struct kinfo_proc kp{};
    std::size_t len = sizeof kp;
    return ::sysctl(mib, 4, &kp, &len, nullptr, 0) == 0 && len == sizeof kp && kp.kp_proc.p_stat == SSTOP;
#else
    char path[32];
    (void)std::snprintf(path, sizeof path, "/proc/%d/stat", static_cast<int>(pid));
    const int fd = ::open(path, O_RDONLY);
    if (fd < 0) return false;
    char buf[512];
    const ssize_t n = ::read(fd, buf, sizeof buf - 1);
    ::close(fd);
    if (n <= 0) return false;
    buf[n] = '\0';
    const char* rp = std::strrchr(buf, ')');   // "pid (comm) S ...": comm may hold spaces or ')'
    return rp != nullptr && rp[1] == ' ' && rp[2] == 'T';
#endif
}

inline void stopThisProcessFor(long ms) {
    int ready[2];
    if (::pipe(ready) != 0) throw std::runtime_error("pipe failed");
#ifdef F_SETNOSIGPIPE
    (void)::fcntl(ready[1], F_SETNOSIGPIPE, 1);   // a child that died before reading never SIGPIPEs the test process
#endif
    const pid_t parent = ::getpid();
    const pid_t child = ::fork();
    if (child < 0) { ::close(ready[0]); ::close(ready[1]); throw std::runtime_error("fork failed"); }
    if (child == 0) {
        ::close(ready[1]);
        char b = 0;
        while (::read(ready[0], &b, 1) < 0 && errno == EINTR) {}   // the parent is back from fork()
        ::kill(parent, SIGSTOP);
        bool stopped = false;
        for (int i = 0; i < 2000 && !stopped; ++i) {   // <= 2 s: the stop takes effect (it is asynchronous)
            stopped = processIsStopped(parent);
            if (!stopped) { const timespec t{0, 1'000'000}; ::nanosleep(&t, nullptr); }
        }
        const timespec ts{ms / 1000, (ms % 1000) * 1'000'000};
        ::nanosleep(&ts, nullptr);
        ::kill(parent, SIGCONT);
        ::_exit(stopped ? 0 : 1);
    }
    ::close(ready[0]);
    const char go = 1;
    (void)!::write(ready[1], &go, 1);
    ::close(ready[1]);
    int st = 0;
    while (::waitpid(child, &st, 0) < 0 && errno == EINTR) {}
    if (!WIFEXITED(st) || WEXITSTATUS(st) != 0) throw std::runtime_error("stopThisProcessFor: the stop never took effect");
}

/// The settled catalog leader, polled (a stalled host can leave the topology unsettled for a moment between two reads): -1 after `within`.
int resolveLeader(ThreeNodes& c, std::chrono::seconds within) {
    for (const auto deadline = Clock::now() + within; Clock::now() < deadline;) {
        if (const int l = c.settledLeader(); l > 0) return l;
        std::this_thread::sleep_for(20ms);
    }
    return -1;
}

/// One cell of the slice (also the unit the seam tests drive).
void runCell(const CellParam& param, const CellSeams& seams = {}, CellOutcome* outcome = nullptr) {
    const auto [kind, timing, viaParam] = param;
    const std::string cell = cellName(param);
    if (cm::naRule(kind, {cm::Via::Leader, cm::Persist::Disk}, timing) != 0) { GTEST_SKIP() << "R1"; return; }   // AlterSchema x CreateRacesPlacement
    const auto t0 = Clock::now();
    struct TimeRecorder { std::string n; Clock::time_point t0; ~TimeRecorder() {
        CellTimes::rows().emplace_back(n, std::chrono::duration<double>(Clock::now() - t0).count()); } } recorder{cell, t0};

    // The third stall signal (see runCell's baseline): runs from the start of the cell, asked for the stalls that ended since
    // (baseline - kStallLookbackNs); destroyed (joined) on every exit.
    rune::node::testing::SchedulerWatchdog watchdog;
    watchdog.start();
    ThreeNodes c("sl" + std::to_string(static_cast<int>(kind)) + std::to_string(static_cast<int>(timing)) + std::to_string(viaParam), Mode::Shard);
    c.configTweak = seams.configTweak;
    for (int id : (timing == cm::Timing::NewBrokerJoins ? std::vector<int>{1, 2} : std::vector<int>{1, 2, 3})) c.startNode(id);
    ASSERT_TRUE(c.awaitCatalogLeader(60s)) << cell;
    Rig rig(c);
    // F-A: the reconnect count of the WHOLE cell reaches the outcome on every exit (a T-H3 zero means the cell itself kept
    // every session alive - never the Rig's guard rescuing an unpumped wait).
    struct ReconnectsOut { CellOutcome* o; Rig& r; ~ReconnectsOut() { if (o) o->roles.silentReconnects = r.silentReconnects(); } }
        reconnectsOut{outcome, rig};
    const int preLeader = resolveLeader(c, 60s);   // for the PRECONDITION sends only
    ASSERT_GT(preLeader, 0);

    // ── precondition (through the preLeader, with the settle of a young cluster) ─────────────────────
    const bool reuse = timing == cm::Timing::DropAndReuseName;
    Expected tHave, readyHave, srcHave;
    int32_t srcId = -1, droppedId = -1;
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("ready", 1, true), "define ready", preLeader));
    switch (kind) {
    case cm::Kind::CreateTopic:
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t (to be dropped)", preLeader));
            TopicInfo old;
            Expected oldHave;   // the dropped topic's data: must never reappear in the reused name
            ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, oldHave, cell + "/old", &old));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropSender("t"), "drop t", preLeader));
        }
        break;
    case cm::Kind::CreateView:
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("src", 1, true), "define src", preLeader));
        {
            const auto si = rig.topicInfo(preLeader, "src", 30s);
            ASSERT_TRUE(si.has_value());
            srcId = si->id;
        }
        if (timing == cm::Timing::CreateRacesPlacement) {
            // VLS B.6 (checkpoint fix B): the source already holds the sentinel key BEFORE the CreateView, so the window can
            // tell a servable read (its row) from a premature / stale one (AbsentKey of a key the source holds).
            const ProduceIdentity ident = freshIdentity();
            const std::string value = asString(characterValue(kRacesViewSentinelKey, kRacesViewSentinelHp));
            ASSERT_TRUE(rig.produceFollowing(preLeader, srcId, 0, batchFor(true, kRacesViewSentinelKey, value, ident), 60s, ident))
                << cell << ": the sentinel record was never accepted into src";
            srcHave[0].push_back(value);
        }
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(createViewSender("v", srcId), "create v (to be dropped)", preLeader));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropViewSender("v"), "drop v", preLeader));
        }
        break;
    case cm::Kind::DropTopic:
        if (timing == cm::Timing::CreateRacesPlacement) break;   // R10: the cell itself creates t (placement held), then drops it
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t", preLeader));
        { TopicInfo di; ASSERT_NO_FATAL_FAILURE(rig.dataCheck("t", true, tHave, cell + "/pre", &di)); droppedId = di.id; }   // data in the topic about to be dropped
        break;
    case cm::Kind::AlterSchema:
        if (reuse) {
            ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t (to be dropped)", preLeader));
            ASSERT_NO_FATAL_FAILURE(rig.must(dropSender("t"), "drop t", preLeader));
        }
        ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("t", 3, true), "define t", preLeader));
        break;
    default: FAIL() << "kind not in the slice";
    }
    // Every running broker has applied the precondition and publishes its DDL route before the cell starts.
    {
        std::string why;
        bool equal = false;
        rig.pumping([&] { equal = c.awaitCatalogDigestsEqual(60s, &why); });   // F-A: the Rig's sessions stay alive
        ASSERT_TRUE(equal) << cell << " precondition: " << why;
    }
    if (seams.afterPrecondition) seams.afterPrecondition(c, rig);

    // Slice Recipe 1: THE cell's leader is resolved NOW, after the precondition (under load the catalog leadership can move
    // during it); every role the cell acts on (via, victim, the hooks) derives from this value.
    int leader = -1;
    rig.pumping([&] { leader = resolveLeader(c, 60s); });   // F-A: the Rig's sessions stay alive
    ASSERT_GT(leader, 0);
    Perturbation pert;
    pert.injectedStall = seams.injectsStall;
    // The third stall signal (controller ruling on round-3 concern 1): the watchdog thread of THIS process (started with the
    // cell) measures how long the scheduler kept it off the CPU - process-wide starvation that also delays the media-driver
    // heartbeat threads, which the brokers' admin / control pass gaps cannot see.  Joined right before the after-drive reading.
    if (seams.beforeBaseline) seams.beforeBaseline();
    const int64_t tBaselineNs = rune::node::testing::SchedulerWatchdog::nowNs();
    const Perturbation pert0 = Perturbation::snapshot(c);

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
    int victimId = -1;               // RestartMidApply: the stopped follower (victim is cleared once the hook restarted it)
    bool restartedInHook = false;    // RestartMidApply: the hook restarted the victim while the DDL committed / applied
    int crashedNode = -1;
    bool placementReleased = false;
    int answersHeldOn = -1;
    uint64_t baseline = 0;
    std::function<void()> afterDrive;   // R10: the checks that run after the drop's answer, before the placement is released
    int64_t dropCreateCorr = 0;
    Clock::time_point tRelease{};
    switch (timing) {
    case cm::Timing::LeaderChangeMidDdl:
        c.node(leader).holdCatalogAnswersForTest(true);                 // the leader commits the DDL and never answers it
        baseline = c.node(leader).catalogEventsAppendedForTest();
        hooks.afterSend = [&](int, int node) {
            if (node != c.catalogLeaderId() || crashedNode > 0) return false;   // keyed on whoever leads when the frame is sent
            if (node != leader) { pert.leaderMovedInCell = true; pert.stampHarness(); return false; }   // the held / baselined broker is not the leader any more
            crashedNode = node;
            bool ok = false;
            rig.pumping([&] { ok = c.crashAfterNextAppend(node, baseline, /*waitCommit=*/true, 30s); });
            EXPECT_TRUE(ok) << cell << ": the leader never appended + committed the DDL";
            rig.forget(node);
            return true;                                               // in doubt: the crash ate the answer
        };
        break;
    case cm::Timing::RestartMidApply:
        victim = aRunningFollower(via);
        victimId = victim;
        ASSERT_GT(victim, 0) << cell;
        rig.pumping([&] { c.stop(victim); });
        rig.forget(victim);
        {
            int isrLeader = -1;
            bool shrank = false;
            // F-A: the ISR wait can take many seconds under a flapping leadership - the Rig's sessions stay alive through it
            // (an unpumped 13 s wait let n2 reap the Rig's session; the next DDL on the dead sid had no reply channel).
            rig.pumping([&] { shrank = c.awaitCatalogIsrSize(2, 60s, &isrLeader); });
            if (seams.afterIsrShrink) rig.pumping([&] { seams.afterIsrShrink(c); });   // test seam (T-H3), inside the pump
            ASSERT_TRUE(shrank) << cell << ": the catalog leader's ISR never shrank to the 2 running brokers";
            if (isrLeader != leader) { pert.leaderMovedInCell = true; pert.stampHarness(); }   // the topology moved between resolution and the shrink
        }
        hooks.afterSend = [&](int, int node) {
            if (seams.suppressTimingHook) return false;   // T9: the timing never runs (the victim restarts after the drive)
            if (node != c.catalogLeaderId() || victim < 0) return false;
            const int v = victim;
            victim = -1;
            restartedInHook = true;
            rig.pumping([&] { c.startNode(v); });                       // restarted while the DDL commits / applies
            return false;
        };
        break;
    case cm::Timing::CreateRacesPlacement:
        if (kind == cm::Kind::CreateTopic || kind == cm::Kind::CreateView) {
            // R7 (OkAfterPlacement): the DDL commits and applies everywhere while the initial placement is HELD; the
            // object must refuse retriably through every broker, and the DDL's OK must not arrive, until the
            // placement is released.  The hold covers every placement path (MetadataService::holdPlacementForTest).
            // Slice Recipe 2: the leader's DDL ANSWERS are held too until the warm-up is done (the event still commits and
            // applies), so no answer - not the OK, not a below-minIsr NotLeader - can reach the gate before the window, however
            // long the warm-up takes; the window (1.5 s) is then bounded inside the gate's hold cap (kDdlAnswerMaxHoldNs).
            rig.holdPlacementEverywhere(true);
            if (seams.answerHoldInWindow) { c.node(leader).holdCatalogAnswersForTest(true); answersHeldOn = leader; }
            hooks.afterSend = [&](int, int node) {
                const int lead = c.catalogLeaderId();
                if (node != lead || placementReleased) return false;
                if (lead != leader) { pert.leaderMovedInCell = true; pert.stampHarness(); }
                if (seams.onLeaderSend) seams.onLeaderSend(c, rig, lead);
                const int64_t ddlCorr = rig.lastCorr();
                const std::string what = cell + " (placement held)";
                // The object is applied (read through a broker that is NOT the leader: the leader's session queues behind
                // its held DDL answer), then every broker's control thread applies it a hop later: until it does, a probe
                // answers UnknownTopic / UnknownView - the warm-up waits for the first retriable refusal on each broker.
                int reader = -1;
                for (int n : c.runningNodes()) if (n != lead) { reader = n; break; }
                const auto info = rig.topicInfo(reader, target, 20s);
                EXPECT_TRUE(info.has_value()) << what << ": '" << target << "' never applied on node " << reader;
                int probes = 0;
                bool released = false;
                auto release = [&] {
                    if (released) return;
                    released = true;
                    if (answersHeldOn > 0 && c.running(answersHeldOn)) c.node(answersHeldOn).holdCatalogAnswersForTest(false);
                    if (answersHeldOn != lead && c.running(lead)) c.node(lead).holdCatalogAnswersForTest(false);
                    tRelease = Clock::now();
                };
                // VLS B.6 (checkpoint fix B): a view sink is replica-less and catalog-only - it owns NO placement, so the
                // held placement gates nothing of the view.  Its real readiness gates are: the CreateView committed AND
                // applied on the broker (the CatalogApplier applies [next, HW) only), the plan on every worker (the
                // gateway's viewSinksAwaitingPlan_), the SOURCE partition placed and led, and the instance folded to the
                // source's committed tail under THIS leadership.  The source holds the sentinel key before the CreateView,
                // so a read before the view is servable shows as AbsentKey (stale) or UnknownView (applied, yet unknown);
                // the only legitimate answers once it is applied are NotReady and the sentinel's row.
                auto viewServableRead = [&](const Rig::ViewRead& r) {
                    return r.ok() && rowHp(r.row) == kRacesViewSentinelHp;
                };
                auto viewLegitimate = [&](const Rig::ViewRead& r) {
                    return viewServableRead(r) || (r.answered && !r.finalIsError && r.finalCode == pc::ViewRowResponse::kNotReady);
                };
                auto viewAnswerText = [](const Rig::ViewRead& r) {
                    return r.answered ? std::string(r.finalIsError ? "error " : "status ") + std::to_string(r.finalCode) +
                                            (r.ok() ? " hp " + std::to_string(rowHp(r.row)) : std::string())
                                      : std::string("nothing");
                };
                if (info) {
                    auto refusing = [&](int n) {
                        if (kind == cm::Kind::CreateTopic) {
                            const int code = rig.produceOnce(n, info->id, 0, batchFor(true, 1, "warm"));
                            return code == cNotLeader || code == cRedirect;
                        }
                        // B.6: warm up until the view SERVES through this broker - the held placement must not block it.  Before
                        // that (not applied here yet / plan / fold pending) the answer is UnknownView, NotReady or a route
                        // refusal; an AbsentKey or a wrong row of the sentinel the source already holds is a premature read.
                        const auto r = rig.viewRowFollowing(n, info->id, static_cast<uint64_t>(kRacesViewSentinelKey));
                        ++probes;
                        EXPECT_FALSE(r.answered && !r.finalIsError &&
                                     (r.finalCode == pc::ViewRowResponse::kAbsentKey || (r.ok() && !viewServableRead(r))))
                            << what << ": GetViewRow(sentinel) through node " << n << " answered " << viewAnswerText(r)
                            << " before the view served (a premature / stale read)";
                        return viewServableRead(r);
                    };
                    for (int n : c.runningNodes()) {
                        const auto upTo = Clock::now() + 20s;
                        bool reached = false;
                        while (Clock::now() < upTo && !(reached = refusing(n))) std::this_thread::sleep_for(20ms);
                        if (kind == cm::Kind::CreateView)
                            EXPECT_TRUE(reached) << what << ": the view never served the sentinel row through node " << n
                                                 << " (B.2: a sink owns no placement, the held placement must gate nothing)";
                    }
                    release();   // the event is applied: the OK enters the gate NOW (headSince starts), the window is inside its cap
                    if (seams.afterAnswerRelease) seams.afterAnswerRelease(c, rig);
                    for (const auto windowEnd = Clock::now() + 1500ms; Clock::now() < windowEnd;) {
                        for (int n : c.runningNodes()) {
                            if (kind == cm::Kind::CreateTopic) {
                                for (int p = 0; p < info->partitions; ++p) probes += rig.expectRetriableRefusal(n, info->id, p, what);
                            } else {
                                const auto r = rig.viewRowFollowing(n, info->id, static_cast<uint64_t>(kRacesViewSentinelKey));
                                ++probes;
                                EXPECT_TRUE(viewLegitimate(r))
                                    << what << ": GetViewRow(sentinel) through node " << n << " answered " << viewAnswerText(r)
                                    << " (want the sentinel row or NotReady: never AbsentKey / UnknownView / a wrong row once applied)";
                                // A key the source does not hold yet: AbsentKey (served) or NotReady, never a row / UnknownView.
                                const auto a = rig.viewRowFollowing(n, info->id, 101);
                                ++probes;
                                EXPECT_TRUE(a.answered && !a.finalIsError &&
                                            (a.finalCode == pc::ViewRowResponse::kAbsentKey || a.finalCode == pc::ViewRowResponse::kNotReady))
                                    << what << ": GetViewRow(101, not in the source yet) through node " << n << " answered "
                                    << viewAnswerText(a) << " (want AbsentKey or NotReady)";
                            }
                        }
                    }
                }
                release();
                EXPECT_GE(probes, 6) << what << ": the no-placement window was observed (vacuity guard)";
                // The R7 check (slice Recipe 2): an OK while the placement is held is the breach.  Legitimate early answers: the
                // gate's in-doubt Internal once the hold really exceeded its cap, and (fix round 4, found by the stall injector) the
                // retriable NotLeader of a leader demoted / fenced in the window - recorded as evidence, so the cell's verdict
                // requires a scheduler stall (or an injected one) before it; without one it fails as a product bug.
                const Answer early = rig.await(ddlCorr, 20ms);
                const auto held = Clock::now() - tRelease;
                const auto gateCap = std::chrono::nanoseconds(BrokerNode::kDdlAnswerMaxHoldNs);
                if (early.got && kind == cm::Kind::CreateView) {
                    // VLS B.6: the sink owns no placement, so the CreateView's OK legitimately arrives while every placement
                    // is held - here only after the event committed + applied on every broker and the view served the sentinel
                    // through each one (above).  Never the gate's in-doubt Internal: there is no placement to overrun.
                    if (early.code == cNotLeader) { pert.earlyNotLeader = true; pert.stampHarness(); }
                    else EXPECT_EQ(early.code, cOk) << what << ": the CreateView's early answer (no placement gates a view sink)";
                } else if (early.got) {
                    EXPECT_NE(early.code, cOk) << what << ": the DDL's OK arrived while its placement was HELD (R7)";
                    if (early.code == cNotLeader) { pert.earlyNotLeader = true; pert.stampHarness(); }
                    else EXPECT_TRUE(early.code == cInternal && held >= gateCap - 250ms)
                        << what << ": early answer " << early.code << " after "
                        << std::chrono::duration_cast<std::chrono::milliseconds>(held).count()
                        << " ms (only Internal past the " << std::chrono::duration_cast<std::chrono::milliseconds>(gateCap).count()
                        << " ms gate cap, or a retriable NotLeader, are legitimate)";
                    if (early.code == cInternal) { pert.gateOverdueObserved = true; pert.stampHarness(); }
                }
                rig.holdPlacementEverywhere(false);
                placementReleased = true;
                return false;
            };
        }
        else if (kind == cm::Kind::DropTopic) {
            // R10: DROP of a topic whose create is applied on every broker but whose placement is HELD.  The create's OK (held by the
            // DdlAnswerGate: the topic does not serve) must be released as a tombstone once the drop applied - never Internal "in doubt",
            // never behind the drop's own OK - and the placement, released afterwards, must find nothing to place on any broker.
            rig.holdPlacementEverywhere(true);
            const RequestId createRid = rig.freshRid_();
            dropCreateCorr = rig.sendOnly(leader, defineSender("t", 3, true), createRid);
            ASSERT_NE(dropCreateCorr, 0) << cell << ": the create could not be sent";
            for (int n : c.runningNodes()) {
                if (n == leader) continue;   // the leader's admin answers queue behind its held create OK (the DdlAnswerGate is FIFO)
                const auto ti = rig.topicInfo(n, "t", 30s);
                ASSERT_TRUE(ti.has_value()) << cell << ": the create never applied on node " << n;
                droppedId = ti->id;
            }
            int refused = 0;
            for (int n : c.runningNodes()) refused += rig.expectRetriableRefusal(n, droppedId, 0, cell + " (create applied, placement held)");
            EXPECT_GE(refused, 6) << cell << ": the no-placement window was observed (vacuity guard)";
            EXPECT_FALSE(rig.await(dropCreateCorr, 20ms).got) << cell << ": the create answered before its placement committed (R7)";
            const int32_t createdId = droppedId;
            afterDrive = [&, createdId] {
                const auto t0 = Clock::now();
                const Answer created = rig.await(dropCreateCorr, 10s);
                const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0).count();
                ASSERT_TRUE(created.got) << cell << ": the create was never answered";
                EXPECT_EQ(created.code, cOk) << cell << ": the create's held OK is released as a tombstone, not Internal (in doubt): " << created.msg;
                EXPECT_LT(ms, 2000) << cell << ": ... promptly (the gate's in-doubt cap is " << BrokerNode::kDdlAnswerMaxHoldNs / 1'000'000 << " ms)";
                for (int n : c.runningNodes())
                    EXPECT_EQ(c.node(n).ddlAnswersOverdueForTest(), 0u) << cell << ": node " << n << " answered an in-doubt Internal";
                rig.holdPlacementEverywhere(false);
                rig.pumping([] { std::this_thread::sleep_for(1500ms); });   // a stray placement has every chance to commit
                for (int n : c.runningNodes()) {
                    bool placed = false;
                    for (int p = 0; p < 3; ++p)
                        placed = placed || c.node(n).onControlThreadForTest([&c, n, createdId, p] {
                            return c.node(n).metadataServiceForTest()->assignment(createdId, p) != nullptr;
                        });
                    EXPECT_FALSE(placed) << cell << ": the dropped topic id " << createdId << " was placed (node " << n << "'s committed map)";
                    EXPECT_EQ(rig.produceTerminal(n, createdId, 0, batchFor(true, 1, "late"), cUnknown, 30s), cUnknown) << cell << ": node " << n;
                }
            };
        }
        break;
    default: break;
    }
    // The generic onLeaderSend seam for the timings without a hook of their own (T5/T6): fires once, at the first frame that
    // reaches whoever leads when it is sent.
    bool leaderSeamFired = false;
    if (seams.onLeaderSend && !hooks.afterSend) {
        hooks.afterSend = [&](int, int node) {
            if (leaderSeamFired || node != c.catalogLeaderId()) return false;
            leaderSeamFired = true;
            seams.onLeaderSend(c, rig, node);
            return false;
        };
    }
    CellResult res = rig.drive(op, via, rid, hooks);
    if (afterDrive) afterDrive();
    ASSERT_FALSE(::testing::Test::HasFatalFailure());
    // A hook that never fired (a perturbed run) must not leave the placement / the answers held: the gate would answer Internal.
    if (rig.placementHeld()) rig.holdPlacementEverywhere(false);
    if (answersHeldOn > 0 && c.running(answersHeldOn)) c.node(answersHeldOn).holdCatalogAnswersForTest(false);
    if (timing == cm::Timing::LeaderChangeMidDdl && crashedNode <= 0 && c.running(leader)) c.node(leader).holdCatalogAnswersForTest(false);
    res.viaWasLeader = via == leader;
    const cm::ExpectedCodes ec = cm::expectedCodes(kind, {res.viaWasLeader ? cm::Via::Leader : cm::Via::Follower, cm::Persist::Disk}, timing);
    // The by-cause NotLeader counters + the leader epoch, compared against the snapshot taken when the roles were resolved.
    // Excluded: exactly the brokers the cell's own timing stops / restarts (the crashed leader, the RestartMidApply victim);
    // the epoch is not used where the timing moves the leader itself.
    std::set<int> excluded;
    if (crashedNode > 0) excluded.insert(crashedNode);
    if (victimId > 0) excluded.insert(victimId);
    const Perturbation after = Perturbation::read(c);
    const int64_t tReadNs = rune::node::testing::SchedulerWatchdog::nowNs();
    watchdog.stop();   // the join hands over its result
    const std::string why = after.since(pert0, excluded, timing != cm::Timing::LeaderChangeMidDdl) +
                            (pert.leaderMovedInCell ? " leader-moved" : "") +
                            (pert.gateOverdueObserved ? " gate-overdue" : "") +
                            (pert.earlyNotLeader ? " early-notLeader(R7 window)" : "");
    // R3-I1 / R3-M1: the excuse is the scheduler watchdog only (or an injected stall), and only a watchdog stall that overlaps
    // [t - 1 s, t] for the FIRST and the LAST evidence time t (stamped by the brokers' owner threads / the harness).  With no
    // stamp at all (cannot happen for the counters, kept for safety) the evidence is placed at [baseline, read].
    auto [evFirst, evLast] = after.evidenceSpan(pert0, excluded, pert);
    if (evFirst == 0) { evFirst = tBaselineNs; evLast = tReadNs; }
    const auto watchdogBefore = [&](int64_t t) { return watchdog.maxOvershootOverlapping(t - kStallLookbackNs, t); };
    const int64_t watchdogAtFirst = watchdogBefore(evFirst), watchdogAtLast = watchdogBefore(evLast);
    const int64_t watchdogAny = watchdog.maxOvershootOverlapping(tBaselineNs - kStallLookbackNs, tReadNs);
    const bool excusedByWatchdog = schedulerStall(watchdogAtFirst) && schedulerStall(watchdogAtLast);
    const int64_t maxGap = after.maxGapNs(pert0, excluded);   // diagnostic + the blocking-pass product signal, never an excuse
    const bool stall = excusedByWatchdog || pert.injectedStall;
    const CellVerdict verdict = classifyCell(!why.empty(), stall, watchdogAny, maxGap, pert.injectedStall);
    const std::string stallWhat = "stall(watchdog " + std::to_string(msOf(watchdogAtFirst)) + " ms before the first evidence, " +
                                  std::to_string(msOf(watchdogAtLast)) + " ms before the last, " + std::to_string(msOf(watchdogAny)) +
                                  " ms in the cell; max pass gap " + std::to_string(msOf(maxGap)) + " ms" +
                                  (pert.injectedStall ? ", injected" : "") + ")";
    switch (verdict) {
    case CellVerdict::Exact:
    case CellVerdict::SlowPass:   // the cell's own timing only: the matrix's exact codes
        if (verdict == CellVerdict::SlowPass) {   // a slow pass that moved nothing: not this test's subject, but visible
            std::fprintf(stderr, "[ slow pass ] %s: %lld ms (watchdog %lld ms)\n", cell.c_str(), msOf(maxGap), msOf(watchdogAny));
            ++CellTimes::slowPasses();
        }
        EXPECT_EQ(res.firstCode, ec.first) << cell << res.trace;
        EXPECT_EQ(res.afterRedirectCode, ec.afterRedirect) << cell << res.trace;
        EXPECT_EQ(res.finalCode, ec.final) << cell << res.trace;
        break;
    case CellVerdict::Perturbed:   // a host / injected stall moved the topology: the contract of THAT event (spec 5.5 / 9, F13)
        std::fprintf(stderr, "[ perturbed ] %s:%s %s %s\n", cell.c_str(), why.c_str(), stallWhat.c_str(), res.trace.c_str());
        ++CellTimes::perturbed();
        if (!pert.injectedStall) reportPerturbedForCheckpoint(cell, why, stallWhat);
        break;
    case CellVerdict::BlockingPass:
        ADD_FAILURE() << cell << ": product evidence (" << why << ") while a broker owner thread's pass took " << msOf(maxGap)
                      << " ms with the scheduler watchdog quiet (" << msOf(watchdogAny)
                      << " ms): blocking pass, product liveness bug (a blocking runtime/admin pass)" << res.trace;
        break;
    case CellVerdict::NoStall:
        ADD_FAILURE() << cell << ": product evidence of a topology change (" << why << ") with NO stall preceding it ("
                      << stallWhat << ", the bound is " << msOf(kStallEvidenceNs)
                      << " ms within 1 s before the evidence) - a spurious fencing / demotion / re-election: product bug" << res.trace;
        break;
    }
    // R2-I1 step 5: a cell whose timing never ran is a failure unless the leader moved under a stall (watchdog / injected).
    const bool timingRan = (timing != cm::Timing::LeaderChangeMidDdl || crashedNode > 0) &&
                           (timing != cm::Timing::RestartMidApply || restartedInHook) &&
                           (timing != cm::Timing::CreateRacesPlacement ||
                            (kind != cm::Kind::CreateTopic && kind != cm::Kind::CreateView) || placementReleased);
    if (!timingRan) {
        const bool moveExcused = pert.injectedStall ||
                                 (pert.harnessFirstNs != 0 && schedulerStall(watchdogBefore(pert.harnessFirstNs)));
        if (pert.leaderMovedInCell && moveExcused) {
            std::fprintf(stderr, "[ not exercised ] %s: the leader moved under a %s\n", cell.c_str(), stallWhat.c_str());
            ++CellTimes::notExercised();
        } else {
            ADD_FAILURE() << cell << ": the timing never ran and no stall explains it (leader moved: " << pert.leaderMovedInCell
                          << ", " << stallWhat << ")" << res.trace;
        }
    }
    if (outcome) {
        outcome->res = res; outcome->roles = CellRoles{preLeader, leader, via, rig.silentReconnects()}; outcome->perturbedWhy = why;
        outcome->stallObserved = excusedByWatchdog; outcome->maxGapNs = maxGap; outcome->watchdogGapNs = watchdogAny;
        outcome->passBlocked = verdict == CellVerdict::SlowPass || verdict == CellVerdict::BlockingPass;
        if (after.nl.count(leader) && pert0.nl.count(leader)) outcome->belowMinIsrOnLeader = after.nl.at(leader).belowMinIsr - pert0.nl.at(leader).belowMinIsr;
    }
    ASSERT_EQ(res.finalCode, cOk) << cell << res.trace;    // ALWAYS

    // ── the timing's after-effects, then the identical-catalogs gate ───────────────────────────────
    if (timing == cm::Timing::LeaderChangeMidDdl) {
        if (crashedNode > 0) {   // (crashedNode <= 0: the timing never ran - judged above, a failure without a stall)
            bool led = false;
            rig.pumping([&] { led = c.awaitCatalogLeader(60s); });   // F-A: the Rig's sessions stay alive
            ASSERT_TRUE(led) << cell << ": no catalog leader after the crash";
            rig.pumping([&] { c.startNode(crashedNode); });             // the crashed leader rejoins as a follower
            rig.forget(crashedNode);
        }
    }
    if (timing == cm::Timing::NewBrokerJoins) { rig.pumping([&] { c.startNode(3); }); rig.forget(3); }
    if (timing == cm::Timing::RestartMidApply && victim > 0) { rig.pumping([&] { c.startNode(victim); }); rig.forget(victim); }
    {
        std::string why2;
        bool equal = false;
        rig.pumping([&] { equal = c.awaitCatalogDigestsEqual(90s, &why2); });   // F-A: the Rig's sessions stay alive
        ASSERT_TRUE(equal) << cell << ": identical catalogs on every broker: " << why2;
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

} // namespace

TEST_P(CatalogClusterSliceRegression, RuntimeDdlThroughEveryBrokerThenDataThroughEveryBroker) { runCell(GetParam()); }

INSTANTIATE_TEST_SUITE_P(Slice, CatalogClusterSliceRegression, ::testing::ValuesIn(allCells()),
                         [](const ::testing::TestParamInfo<CellParam>& i) { return cellName(i.param); });

// ===========================================================================
// Task 23 fix round 2: the slice cells resolve their roles AFTER the precondition, bound the R7 window inside the answer
// gate, and assert the contract of a perturbing event only on product evidence (slice-races-diagnosis Recipes 1-3).
// ===========================================================================
namespace {
/// A move of the catalog leadership in the precondition: stop the leader, wait for another, restart it as a follower.
struct MovedLeader { int old = -1, moved = -1; };
void moveLeader(ThreeNodes& c, Rig& rig, MovedLeader& m) {
    m.old = c.settledLeader();
    ASSERT_GT(m.old, 0);
    rig.pumping([&] { c.stop(m.old); });
    rig.forget(m.old);
    bool ok = false;
    rig.pumping([&] { ok = c.awaitCatalogLeaderOtherThan(m.old, 60s); });   // F-A: every wait keeps the Rig's sessions alive
    ASSERT_TRUE(ok);
    rig.pumping([&] { c.startNode(m.old); });
    rig.forget(m.old);
    std::string why;
    rig.pumping([&] { ok = c.awaitCatalogDigestsEqual(60s, &why); });
    ASSERT_TRUE(ok) << why;
    rig.pumping([&] { ok = c.awaitCatalogLeader(60s); });
    ASSERT_TRUE(ok);
    m.moved = c.settledLeader();
}
}  // namespace

// T1: DropTopic x RestartMidApply and CreateTopic x CreateRacesPlacement with the catalog leadership moved during the precondition.
// RED (runCell: `leader = preLeader` instead of re-resolving): the ISR wait polls a follower / the hook never fires.
TEST(CatalogClusterRegression, SliceCellResolvesItsRolesAfterALeaderMoveInThePreconditionRegression) {
    for (const CellParam& param : {CellParam{cm::Kind::DropTopic, cm::Timing::RestartMidApply, 0},
                                   CellParam{cm::Kind::CreateTopic, cm::Timing::CreateRacesPlacement, 0}}) {
        SCOPED_TRACE(cellName(param));
        MovedLeader m;
        CellSeams seams;
        seams.afterPrecondition = [&](ThreeNodes& c, Rig& rig) { moveLeader(c, rig, m); };
        CellOutcome out;
        runCell(param, seams, &out);
        ASSERT_FALSE(::testing::Test::HasFatalFailure());
        EXPECT_NE(m.moved, m.old) << "vacuity guard: the leadership really moved";
        EXPECT_EQ(out.roles.leader, m.moved) << "the cell's leader is the one that leads AFTER the precondition";
        if (param.timing == cm::Timing::CreateRacesPlacement)
            EXPECT_EQ(out.res.trace.find(": 6]"), std::string::npos) << "no Internal (sibling B)" << out.res.trace;
    }
}

// T1b: awaitCatalogIsrSize follows the CURRENT leader (a follower's isr() is always empty).
// RED (poll a fixed id captured before the move): times out.
TEST(ThreeNodesRegression, AwaitCatalogIsrSizeFollowsTheCurrentLeaderRegression) {
    ThreeNodes c = ThreeNodes::start("isrfollow");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    MovedLeader m;
    ASSERT_NO_FATAL_FAILURE(moveLeader(c, rig, m));
    ASSERT_NE(m.moved, m.old);
    int follower = -1;
    for (int n : c.runningNodes()) if (n != m.moved) { follower = n; break; }
    ASSERT_GT(follower, 0);
    rig.pumping([&] { c.stop(follower); });
    int l = -1;
    EXPECT_TRUE(c.awaitCatalogIsrSize(2, 30s, &l));
    EXPECT_EQ(l, m.moved);
    if (follower != m.old) EXPECT_EQ(c.catalogIsrSizeOn(m.old), 0u) << "a follower's isr() is empty";
}

namespace {
void racesPlacementBelowMinIsr(cm::Kind kind, bool holdAnswers) {
    const CellParam param{kind, cm::Timing::CreateRacesPlacement, 0};
    CellSeams seams;
    seams.configTweak = [](NodeConfig& cfg) { cfg.replicaLagMaxMs = 1000; };
    seams.answerHoldInWindow = holdAnswers;
    seams.injectsStall = true;   // both followers' admin threads are frozen 4 s
    seams.onLeaderSend = [](ThreeNodes& c, Rig& rig, int lead) {
        std::vector<std::thread> th;
        for (int f : c.runningNodes())
            if (f != lead) th.emplace_back([&c, f] { try { c.node(f).onAdminThreadForTest([] { std::this_thread::sleep_for(4s); return 0; }, 15s); } catch (...) {} });
        bool shrunk = false;
        rig.pumping([&] {
            for (const auto until = Clock::now() + 10s; Clock::now() < until && !shrunk;) {
                shrunk = c.catalogIsrSizeOn(lead) == 1;
                if (!shrunk) std::this_thread::sleep_for(50ms);
            }
        });
        EXPECT_TRUE(shrunk) << "vacuity guard: the catalog really is below minIsr while the DDL awaits its commit";
        rig.pumping([&] { for (auto& t : th) t.join(); });
    };
    CellOutcome out;
    runCell(param, seams, &out);
    if (::testing::Test::HasFatalFailure()) return;
    EXPECT_EQ(out.res.trace.find(": 2]"), std::string::npos) << "no NotLeader: " << out.res.trace;
    EXPECT_EQ(out.res.trace.find(": 6]"), std::string::npos) << "no Internal: " << out.res.trace;
    EXPECT_EQ(out.belowMinIsrOnLeader, 0u) << "the answer hold suppressed the pre-commit below-minIsr NotLeader";
}
}  // namespace

// T2 (failure 1, deterministic): the catalog is below minIsr before the event commits; with the answers held until the event
// applied, no pre-commit NotLeader exists, so R7 sees only the real OK path.
// RED (a): holdAnswers=false -> the below-minIsr NotLeader reaches the hook window -> the early-answer check fails.
// RED (b): DdlAnswerGate::offer passes a gated frame -> the OK arrives in the window -> EXPECT_NE(code, cOk) fails.
TEST(CatalogClusterRegression, RacesPlacementWithTheCatalogBelowMinIsrBeforeCommitNeverAnswersEarlyRegression) {
    racesPlacementBelowMinIsr(cm::Kind::CreateTopic, /*holdAnswers=*/true);
}
// T3b: the CreateView sibling (VLS B.6: the view's gates are its commit + apply + plan + source fold, not a placement).
// RED (checkpoint fix B): a GetViewRow answered AbsentKey for the sentinel the source holds (an unfolded / premature read).
TEST(CatalogClusterRegression, RacesPlacementCreateViewWithTheCatalogBelowMinIsrNeverAnswersEarlyRegression) {
    racesPlacementBelowMinIsr(cm::Kind::CreateView, /*holdAnswers=*/true);
}

// T3 (sibling A, deterministic): the hold exceeds the gate's 5 s cap -> the gate answers the in-doubt Internal ("placement pending"),
// never the OK; the cell recognises it as an observed stall, the same-rid retry gets the result after the placement release.
// RED: (a) R7 check back to EXPECT_FALSE(early.got); (b) DdlAnswerGate::release sends the OK on overdue; (c) no in-doubt retry in drive.
TEST(CatalogClusterRegression, RacesPlacementGateOverdueIsInDoubtNeverOkAndTheRetryGetsTheResultRegression) {
    CellSeams seams;
    seams.afterAnswerRelease = [](ThreeNodes&, Rig& rig) { rig.pumping([] { std::this_thread::sleep_for(6s); }); };
    seams.injectsStall = true;   // the harness sleeps past the gate's cap (no broker thread is frozen)
    CellOutcome out;
    runCell({cm::Kind::CreateTopic, cm::Timing::CreateRacesPlacement, 0}, seams, &out);
    if (::testing::Test::HasFatalFailure()) return;
    EXPECT_NE(out.perturbedWhy.find("gate-overdue"), std::string::npos) << out.perturbedWhy;
    const auto six = out.res.trace.find(": 6]");
    ASSERT_NE(six, std::string::npos) << out.res.trace;
    EXPECT_NE(out.res.trace.find(": 0]", six), std::string::npos) << "the retry ends OK: " << out.res.trace;
    EXPECT_EQ(out.res.finalCode, cOk);
}

// T5: a fenced catalog ISR (both followers frozen > replicaLagMaxMs) before the commit: the leader answers the retriable NotLeader; the
// cell names it from the product counter, relaxes ONLY to the retriable contract, and the final result is still OK, one event.
// RED: Perturbation::since() always "" (no evidence) -> the exact afterRedirect expectation (0) fails against 2.
TEST(CatalogClusterRegression, SliceCellUnderAFencedIsrAssertsTheRetriableContractRegression) {
    CellSeams seams;
    seams.configTweak = [](NodeConfig& cfg) { cfg.replicaLagMaxMs = 1000; };
    seams.injectsStall = true;   // both followers' admin threads are frozen 4 s
    seams.onLeaderSend = [](ThreeNodes& c, Rig& rig, int lead) {
        std::vector<std::thread> th;
        for (int f : c.runningNodes())
            if (f != lead) th.emplace_back([&c, f] { try { c.node(f).onAdminThreadForTest([] { std::this_thread::sleep_for(4s); return 0; }, 15s); } catch (...) {} });
        bool shrunk = false;
        rig.pumping([&] {
            for (const auto until = Clock::now() + 10s; Clock::now() < until && !shrunk;) {
                shrunk = c.catalogIsrSizeOn(lead) == 1;
                if (!shrunk) std::this_thread::sleep_for(50ms);
            }
        });
        EXPECT_TRUE(shrunk) << "vacuity guard";
        rig.pumping([&] { for (auto& t : th) t.join(); });
    };
    CellOutcome out;
    runCell({cm::Kind::DropTopic, cm::Timing::Steady, 2}, seams, &out);
    if (::testing::Test::HasFatalFailure()) return;
    EXPECT_NE(out.perturbedWhy.find("belowMinIsr"), std::string::npos) << "the cause is named by the product counter: " << out.perturbedWhy;
    EXPECT_EQ(out.res.finalCode, cOk);
}

// T6 (negative): an unperturbed cell reports no product evidence without a stall behind it (on a loaded host a genuine stall
// may produce evidence - then the measured pass gap explains it).
// RED (count a NotLeader on every answer): evidence with no stall -> runCell's "NO stall" failure and this assertion fail.
TEST(CatalogClusterRegression, UnperturbedSliceCellReportsNoPerturbationRegression) {
    CellOutcome out;
    runCell({cm::Kind::CreateTopic, cm::Timing::Steady, 1}, {}, &out);
    if (::testing::Test::HasFatalFailure()) return;
    EXPECT_TRUE(out.perturbedWhy.empty() || out.stallObserved)
        << out.perturbedWhy << " (max pass gap " << msOf(out.maxGapNs) << " ms, watchdog " << msOf(out.watchdogGapNs) << " ms)";
}

// T7 (Task 23 fix round 3, R2-I1): product evidence with NO stall behind it fails the cell as a product bug.  The seam fabricates
// one "demoted" NotLeader count on the leader's admin thread at the first frame the leader receives (after the baseline; the
// recipe's afterPrecondition runs BEFORE the baseline, which would absorb the bump) - no thread is frozen.  The failures of the
// cell are intercepted: with no measured stall exactly the "NO stall ... product bug" failure must be reported; were a genuine
// host stall measured in this run, the relaxation is legitimate and no such failure may appear.
// RED: treat `stall` as always true in runCell -> no failure is reported although no stall was measured.
TEST(CatalogClusterRegression, ProductEvidenceWithoutAStallFailsTheCellRegression) {
    CellSeams seams;
    bool bumped = false;
    seams.onLeaderSend = [&bumped](ThreeNodes& c, Rig&, int lead) {
        c.node(lead).bumpCatalogNotLeaderDemotedForTest();
        bumped = true;
    };
    CellOutcome out;
    ::testing::TestPartResultArray failures;
    {
        ::testing::ScopedFakeTestPartResultReporter intercept(::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
                                                              &failures);
        runCell({cm::Kind::CreateTopic, cm::Timing::Steady, 1}, seams, &out);
    }
    ASSERT_TRUE(bumped) << "vacuity guard: the fabricated evidence was planted";
    EXPECT_NE(out.perturbedWhy.find("notLeader(demoted)+1"), std::string::npos) << "the cell saw the evidence: " << out.perturbedWhy;
    int noStall = 0;
    for (int i = 0; i < failures.size(); ++i) {
        const auto& f = failures.GetTestPartResult(i);
        const std::string m = f.message();   // a coincidental >= 100 ms broker pass reports the same evidence as a blocking pass
        if ((m.find("with NO stall") != std::string::npos || m.find("blocking pass, product liveness bug") != std::string::npos) &&
            f.nonfatally_failed()) { ++noStall; continue; }
        ADD_FAILURE() << "unexpected cell failure: " << f.message();   // anything else is a real failure of the cell
    }
    if (!out.stallObserved) EXPECT_EQ(noStall, 1) << "product evidence without a stall must fail the cell (max pass gap "
                                                  << msOf(out.maxGapNs) << " ms)";
    else EXPECT_EQ(noStall, 0) << "a measured " << msOf(out.maxGapNs) << " ms stall explains the evidence";
}

// T9 (Task 23 fix round 3, R2-I1 step 5): a cell whose timing never ran - here RestartMidApply's hook is suppressed, the victim
// is only restarted after the drive - fails, because neither a leader move nor a stall explains it.  Deterministic: without
// a leader move the failure is due whatever the measured gap.
// RED: drop the `timingRan` check in runCell -> the cell passes without exercising its timing.
TEST(CatalogClusterRegression, TimingThatNeverRanFailsTheCellRegression) {
    CellSeams seams;
    seams.suppressTimingHook = true;
    CellOutcome out;
    ::testing::TestPartResultArray failures;
    {
        ::testing::ScopedFakeTestPartResultReporter intercept(::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
                                                              &failures);
        runCell({cm::Kind::CreateTopic, cm::Timing::RestartMidApply, 0}, seams, &out);
    }
    int neverRan = 0;
    for (int i = 0; i < failures.size(); ++i) {
        const auto& f = failures.GetTestPartResult(i);
        if (std::string(f.message()).find("the timing never ran and no stall explains it") != std::string::npos) { ++neverRan; continue; }
        ADD_FAILURE() << "unexpected cell failure: " << f.message();   // the cell is otherwise correct (exact codes, data)
    }
    EXPECT_EQ(neverRan, 1) << "an un-exercised timing must fail the cell";
}

// T10 (controller ruling, lookback): a whole-process stop that ENDS right before the cell's baseline still explains product
// evidence inside the cell (a stall takes effect after it ends: heartbeat timeout -> fencing -> election).  The broker pass gaps
// are reset after it (the brokers ran 50 ms since), so only the watchdog's lookback sees it.  The evidence is fabricated (as T7).
// RED: no lookback (stalls ending since the baseline only) -> watchdog 0 -> the "NO stall" failure.
TEST(CatalogClusterRegression, StallEndingBeforeTheBaselineExplainsLaterEvidenceRegression) {
    CellSeams seams;
    seams.beforeBaseline = [] { stopThisProcessFor(300); std::this_thread::sleep_for(50ms); };
    seams.onLeaderSend = [](ThreeNodes& c, Rig&, int lead) { c.node(lead).bumpCatalogNotLeaderDemotedForTest(); };
    CellOutcome out;
    runCell({cm::Kind::CreateTopic, cm::Timing::Steady, 1}, seams, &out);
    EXPECT_GE(out.watchdogGapNs, 250'000'000) << "the watchdog saw the 300 ms stop through its lookback";
    EXPECT_TRUE(out.stallObserved);
}

// T11 (Task 23 fix round 4, R3-I1): a blocking runtime pass is the regression, never its own excuse.  At the first frame the leader
// receives, its CONTROL (runtime) thread - the one that drives consensus heartbeats - is blocked 1 s by a test task, NOT marked
// injectsStall: a product-shaped block.  1 s, not the recipe's 400 ms: a follower fails over only after 3 x 150 = 450 ms of
// heartbeat silence (NodeRuntimeAgent failoverTimeoutNs), so a 400 ms block moved nothing (observed: "[ slow pass ] 404 ms", no
// evidence).  With the scheduler watchdog quiet the cell must report exactly the "blocking pass, product
// liveness bug" failure if the block moved anything; if it moved nothing, the long pass is a [ slow pass ] (passBlocked) and the
// exact codes hold.  If the watchdog itself saw a stall, nothing may fail.
// RED: restore the pass-gap excuse (stall = ... || maxGap >= kStallEvidenceNs) -> the cell passes as [ perturbed ], no failure.
TEST(CatalogClusterRegression, BlockedRuntimePassDoesNotExcuseItsOwnEvidenceRegression) {
    CellSeams seams;
    bool blocked = false;
    seams.onLeaderSend = [&blocked](ThreeNodes& c, Rig& rig, int lead) {
        rig.pumping([&] { c.node(lead).onControlThreadForTest([] { std::this_thread::sleep_for(1000ms); return 0; }, 15s); });
        blocked = true;
    };
    CellOutcome out;
    ::testing::TestPartResultArray failures;
    {
        ::testing::ScopedFakeTestPartResultReporter intercept(::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
                                                              &failures);
        runCell({cm::Kind::CreateTopic, cm::Timing::Steady, 1}, seams, &out);
    }
    ASSERT_TRUE(blocked) << "vacuity guard: the leader's runtime pass was blocked";
    int blockingPass = 0;
    for (int i = 0; i < failures.size(); ++i) {
        const auto& f = failures.GetTestPartResult(i);
        if (std::string(f.message()).find("blocking pass, product liveness bug") != std::string::npos) { ++blockingPass; continue; }
        ADD_FAILURE() << "unexpected cell failure: " << f.message();
    }
    std::fprintf(stderr, "T11: evidence '%s', max pass gap %lld ms, watchdog %lld ms, blocking-pass failures %d\n",
                 out.perturbedWhy.c_str(), msOf(out.maxGapNs), msOf(out.watchdogGapNs), blockingPass);
    if (schedulerStall(out.watchdogGapNs)) {
        EXPECT_EQ(blockingPass, 0) << "a measured scheduler stall: no blocking-pass verdict";
    } else if (!out.perturbedWhy.empty()) {
        EXPECT_EQ(blockingPass, 1) << "the block moved the topology: a product liveness bug, never excused by its own pass gap";
    } else {
        EXPECT_EQ(blockingPass, 0);
        EXPECT_TRUE(out.passBlocked) << "the 1 s pass is reported as a [ slow pass ]";
    }
    EXPECT_GE(out.maxGapNs, 900'000'000) << "the pass gap measured the block";
    EXPECT_FALSE(out.perturbedWhy.empty()) << "vacuity guard: a 1 s heartbeat silence (> the 450 ms failover) moved the topology";
}

// T12 (Task 23 fix round 4, R3-M1): a watchdog stall excuses only evidence it PRECEDES by at most 1 s.  A 300 ms whole-process stop
// right before the baseline, then the fabricated evidence 1.5 s later (the harness waits before the bump; no broker is frozen): the
// stall is in the cell but not before the evidence -> the "NO stall preceding it" failure.
// RED: the whole-cell window (excused = schedulerStall(watchdogAny)) -> the late evidence is excused -> no failure.
TEST(CatalogClusterRegression, StallLongBeforeTheEvidenceDoesNotExcuseItRegression) {
    CellSeams seams;
    seams.beforeBaseline = [] { stopThisProcessFor(300); std::this_thread::sleep_for(50ms); };
    seams.onLeaderSend = [](ThreeNodes& c, Rig& rig, int lead) {
        rig.pumping([] { std::this_thread::sleep_for(1500ms); });
        c.node(lead).bumpCatalogNotLeaderDemotedForTest();
    };
    CellOutcome out;
    ::testing::TestPartResultArray failures;
    {
        ::testing::ScopedFakeTestPartResultReporter intercept(::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
                                                              &failures);
        runCell({cm::Kind::CreateTopic, cm::Timing::Steady, 1}, seams, &out);
    }
    int noStall = 0;
    for (int i = 0; i < failures.size(); ++i) {
        const std::string m = failures.GetTestPartResult(i).message();
        if (m.find("with NO stall preceding it") != std::string::npos) { ++noStall; continue; }
        ADD_FAILURE() << "unexpected cell failure: " << m;
    }
    EXPECT_GE(out.watchdogGapNs, 250'000'000) << "vacuity guard: the stop is inside the cell's window";
    EXPECT_FALSE(out.stallObserved) << "... but it does not precede the evidence";
    EXPECT_EQ(noStall, 1);
}

// T13 (fix round 4, found by the stall injector): a retriable NotLeader that reaches the R7 window (the catalog fenced below
// minIsr while the DDL awaits its commit; answers NOT held, so it is answered at once) is never an OK and is legitimate under a
// stall - injected here (both followers' admin threads frozen 4 s): no failure.  The same block NOT marked injected is a broker's
// own blocked pass with the watchdog quiet: the cell fails (blocking pass / no stall), never excused.
// RED: the round-2 R7 check (only Internal past the cap is legitimate) -> the injected run fails "early answer 2".
TEST(CatalogClusterRegression, EarlyNotLeaderInThePlacementWindowNeedsAStallRegression) {
    for (const bool injected : {true, false}) {
        SCOPED_TRACE(injected ? "injected" : "not injected");
        CellSeams seams;
        seams.configTweak = [](NodeConfig& cfg) { cfg.replicaLagMaxMs = 1000; };
        seams.answerHoldInWindow = false;
        seams.injectsStall = injected;
        seams.onLeaderSend = [](ThreeNodes& c, Rig& rig, int lead) {
            std::vector<std::thread> th;
            for (int f : c.runningNodes())
                if (f != lead) th.emplace_back([&c, f] { try { c.node(f).onAdminThreadForTest([] { std::this_thread::sleep_for(4s); return 0; }, 15s); } catch (...) {} });
            rig.pumping([&] {
                for (const auto until = Clock::now() + 10s; Clock::now() < until && c.catalogIsrSizeOn(lead) != 1;) std::this_thread::sleep_for(50ms);
            });
            rig.pumping([&] { for (auto& t : th) t.join(); });
        };
        CellOutcome out;
        ::testing::TestPartResultArray failures;
        {
            ::testing::ScopedFakeTestPartResultReporter intercept(::testing::ScopedFakeTestPartResultReporter::INTERCEPT_ONLY_CURRENT_THREAD,
                                                                  &failures);
            runCell({cm::Kind::CreateTopic, cm::Timing::CreateRacesPlacement, 0}, seams, &out);
        }
        int notExcused = 0;
        for (int i = 0; i < failures.size(); ++i) {
            const std::string m = failures.GetTestPartResult(i).message();
            if (!injected && (m.find("blocking pass, product liveness bug") != std::string::npos ||
                              m.find("with NO stall preceding it") != std::string::npos)) { ++notExcused; continue; }
            ADD_FAILURE() << "unexpected cell failure: " << m;
        }
        EXPECT_NE(out.perturbedWhy.find("notLeader(belowMinIsr)"), std::string::npos) << "vacuity guard: " << out.perturbedWhy;
        if (!injected && !schedulerStall(out.watchdogGapNs)) EXPECT_EQ(notExcused, 1) << "a broker's own block is never an excuse";
    }
}

// R3-M2: a watchdog whose thread cannot start (after pipe()) leaks no fd.
// RED: drop the catch in start() -> the two pipe fds stay open (the next free fd number moves by 2).
TEST(ThreeNodesRegression, SchedulerWatchdogFailedStartLeaksNoFdRegression) {
    auto lowestFreeFd = [] { const int fd = ::open("/dev/null", O_RDONLY); ::close(fd); return fd; };
    const int before = lowestFreeFd();
    {
        rune::node::testing::SchedulerWatchdog w;
        rune::node::testing::SchedulerWatchdog::failNextStartForTest = true;
        EXPECT_THROW(w.start(), std::system_error);
    }
    EXPECT_EQ(lowestFreeFd(), before);
}

// Task 23 fix round 4 (R3-I1): only the scheduler watchdog is a stall; a broker's own long pass never excuses evidence - with
// evidence it is a blocking-pass product bug, without it a [ slow pass ]; an injected stall excuses and is never "blocked".
// RED: restore the pass-gap excuse (excused = excused || maxPassGap >= bound) -> BlockingPass becomes Perturbed.
TEST(ThreeNodesRegression, StallEvidenceCountsEveryHarnessSignalRegression) {
    constexpr int64_t k = kStallEvidenceNs;
    EXPECT_TRUE(schedulerStall(k));
    EXPECT_FALSE(schedulerStall(k - 1));
    using V = CellVerdict;
    EXPECT_EQ(classifyCell(false, false, 0, 0, false), V::Exact);
    EXPECT_EQ(classifyCell(false, false, 0, k, false), V::SlowPass) << "a long pass that moved nothing";
    EXPECT_EQ(classifyCell(false, false, k, k, false), V::Exact) << "the watchdog explains the long pass";
    EXPECT_EQ(classifyCell(true, true, k, 0, false), V::Perturbed) << "a watchdog stall precedes the evidence";
    EXPECT_EQ(classifyCell(true, false, 0, k, false), V::BlockingPass) << "a pass gap alone is not a stall";
    EXPECT_EQ(classifyCell(true, false, 0, k - 1, false), V::NoStall);
    EXPECT_EQ(classifyCell(true, false, k, 0, false), V::NoStall) << "a watchdog stall that does not precede the evidence";
    EXPECT_EQ(classifyCell(true, true, 0, k, true), V::Perturbed) << "an injected stall (frozen admin threads) excuses";
}

// Controller ruling (round-3 concern 1): the SchedulerWatchdog detects a SIGSTOP of the whole test process - the starvation that
// also stalls the media-driver heartbeat threads.  A forked child (async-signal-safe calls only) stops this process for 300 ms.
// RED: the watchdog never records an overshoot -> 0 < 250 ms.
TEST(ThreeNodesRegression, SchedulerWatchdogDetectsAWholeProcessStopRegression) {
    rune::node::testing::SchedulerWatchdog w;
    w.start();
    std::this_thread::sleep_for(50ms);
    stopThisProcessFor(300);
    std::this_thread::sleep_for(50ms);   // the watchdog's next tick observes the stop
    w.stop();
    EXPECT_GE(w.maxOvershootNs(), 250'000'000) << "the 300 ms stop of the whole process (watchdog " << msOf(w.maxOvershootNs()) << " ms)";
    using WD = rune::node::testing::SchedulerWatchdog;
    const int64_t now = WD::nowNs();
    EXPECT_GE(w.maxOvershootOverlapping(now - 1'000'000'000, now), 250'000'000) << "the stop overlaps the last second";
    EXPECT_EQ(w.maxOvershootOverlapping(now + 1'000'000'000, now + 2'000'000'000), 0) << "no stop in the future";
    EXPECT_EQ(w.maxOvershootOverlapping(now - 60'000'000'000LL, now - 50'000'000'000LL), 0) << "none a minute ago";
    // A second run starts from 0 (the result of a run is its own).
    w.start();
    std::this_thread::sleep_for(30ms);
    w.stop();
    EXPECT_LT(w.maxOvershootNs(), 250'000'000);
}

// R2-I1 step 1 on real brokers: each broker's admin and control threads record their own max inter-pass gap, read and reset ON
// the owner threads.  A test task that occupies the owner thread for 300 ms is a 300 ms gap of exactly that thread.
// RED: remove the update in AdminControlPlane::doWork -> controlMaxGapNs stays below 300 ms (likewise AdminWorker::doWork).
TEST(ThreeNodesRegression, PassGapsMeasureAnOwnerThreadThatWasNotScheduledRegression) {
    ThreeNodes c = ThreeNodes::start("passgaps");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    BrokerNode& n = c.node(1);
    n.resetPassGapsForTest();
    const auto idle = n.passGapsForTest();
    EXPECT_LT(idle.adminMaxGapNs, kStallEvidenceNs) << "an idle broker is scheduled (backoff park <= a few ms)";
    EXPECT_LT(idle.controlMaxGapNs, kStallEvidenceNs);
    // A pass records the gap at its own start, so the pass AFTER the occupied one records it: the read is polled briefly.
    auto readUntil = [&n](bool admin) {
        BrokerNode::PassGaps g;
        for (const auto until = Clock::now() + 2s; Clock::now() < until; std::this_thread::sleep_for(5ms)) {
            g = n.passGapsForTest();
            if ((admin ? g.adminMaxGapNs : g.controlMaxGapNs) >= 300'000'000) break;
        }
        return g;
    };
    n.onControlThreadForTest([] { std::this_thread::sleep_for(300ms); return 0; });
    const auto ctl = readUntil(false);
    EXPECT_GE(ctl.controlMaxGapNs, 300'000'000) << "the control thread was occupied 300 ms";
    EXPECT_LT(ctl.adminMaxGapNs, 300'000'000) << "only the occupied thread shows the gap";
    n.resetPassGapsForTest();
    n.onAdminThreadForTest([] { std::this_thread::sleep_for(300ms); return 0; });
    const auto adm = readUntil(true);
    EXPECT_GE(adm.adminMaxGapNs, 300'000'000) << "the admin thread was occupied 300 ms";
    EXPECT_LT(adm.controlMaxGapNs, 300'000'000) << "only the occupied thread shows the gap";
}

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
    for (const auto& [id, hp] : rows) {
        const ProduceIdentity ident = freshIdentity();   // diag-duplicate F1
        ASSERT_TRUE(rig.produceFollowing(leader, si->id, 0, batchFor(true, id, asString(characterValue(id, hp)), ident), 60s, ident))
            << "record " << id << " was never acked";
    }
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

// ===========================================================================
// F-A (2026-10-16): "node 2 never answered the DDL (25 s)" was the HARNESS - an unpumped test-thread wait longer than the
// gateway lease (10 s) let the broker reap the Rig's session, and the Rig then sent the DDL on the dead sid (no reply
// channel: the frame could only be dropped).  The Rig now reconnects a session IT left silent past half the lease (a
// harness-observed cause), the cell's waits keep the sessions alive, and the broker's reap tells the client.
// ===========================================================================
namespace {
/// The lease-test setup: a settled cluster, a ready topic, a warm session on a follower whose probe DDL is answered.
struct LeaseRig {
    ThreeNodes c;
    std::unique_ptr<Rig> rig;
    int via = -1;
    int64_t sid0 = 0;
    explicit LeaseRig(const std::string& tag) : c(ThreeNodes::start(tag)) {}
    void setUp(bool reconnectSilent = true) {
        ASSERT_TRUE(c.awaitCatalogLeader(60s));
        rig = std::make_unique<Rig>(c, reconnectSilent);
        const int L = c.settledLeader();
        ASSERT_GT(L, 0);
        ASSERT_NO_FATAL_FAILURE(rig->must(defineSender("ready", 1, true), "define", L));
        for (int n : c.runningNodes()) if (n != L) { via = n; break; }
        ASSERT_GT(via, 0);
        bool answered = false;
        for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline && !answered;) {
            const Answer a = rig->await(rig->sendOnly(via, dropSender("no_such"), rig->freshRid_()), 10s);
            answered = a.got && (a.code == cRedirect || a.code == cUnknown);
            if (!answered) std::this_thread::sleep_for(100ms);
        }
        ASSERT_TRUE(answered) << "precondition: the via broker answers the probe";
        sid0 = rig->session(via);
        ASSERT_GT(sid0, 0);
    }
    uint64_t unanswerable() {
        return c.node(via).onControlThreadForTest([this] { return c.node(via).gatewayForTest()->unanswerableFramesForTest(); });
    }
};
}  // namespace

// T-H1 (Recipe 2): the repro.  RED (Rig::session returns the cached sid unconditionally): no answer, unanswerable +1.
TEST(CatalogClusterRegression, RigReconnectsASessionItLeftSilentPastTheLeaseRegression) {
    LeaseRig g("silentlease");
    ASSERT_NO_FATAL_FAILURE(g.setUp());
    const uint64_t u0 = g.unanswerable();
    std::this_thread::sleep_for(11s);   // like an unpumped ISR wait: no Keepalive leaves this client
    const Answer a = g.rig->await(g.rig->sendOnly(g.via, dropSender("no_such"), g.rig->freshRid_()), 10s);
    ASSERT_TRUE(a.got) << "the probe after a silent wait was never answered (sent on the reaped session)";
    EXPECT_TRUE(a.code == cRedirect || a.code == cUnknown) << a.code;
    EXPECT_EQ(g.rig->silentReconnects(), 1);
    EXPECT_NE(g.rig->session(g.via), g.sid0);
    EXPECT_EQ(g.unanswerable(), u0) << "a frame was sent on the dead session";
}

// I2 (review): the broker expires a session the Rig kept alive (a forced reap on the via broker's control thread):
// Rig::session must FAIL the test, never reconnect silently.  RED: drop the ADD_FAILURE.
TEST(CatalogClusterRegression, RigFailsWhenTheBrokerExpiresAKeptAliveSessionRegression) {
    LeaseRig g("keptexpired");
    ASSERT_NO_FATAL_FAILURE(g.setUp());
    auto& node = g.c.node(g.via);
    (void)node.onControlThreadForTest([&] { node.gatewayForTest()->reapExpiredForTest(INT64_MAX / 2); return 0; });
    g.rig->pumping([] { std::this_thread::sleep_for(500ms); });   // the expiry notice arrives
    ASSERT_TRUE(g.rig->cl().sessionExpired(g.sid0)) << "precondition: the broker told this client";
    EXPECT_NONFATAL_FAILURE((void)g.rig->session(g.via), "kept alive");
}

// T-H2 (Recipe 2, negative): a session kept alive across a long wait is never churned.  RED (kLeaseMargin = 0).
TEST(CatalogClusterRegression, RigKeepsAPumpedSessionAcrossALongWaitRegression) {
    LeaseRig g("pumpedlease");
    ASSERT_NO_FATAL_FAILURE(g.setUp());
    g.rig->pumping([] { std::this_thread::sleep_for(11s); });
    const Answer a = g.rig->await(g.rig->sendOnly(g.via, dropSender("no_such"), g.rig->freshRid_()), 10s);
    ASSERT_TRUE(a.got);
    EXPECT_TRUE(a.code == cRedirect || a.code == cUnknown) << a.code;
    EXPECT_EQ(g.rig->silentReconnects(), 0);
    EXPECT_EQ(g.rig->session(g.via), g.sid0);
}

// T-P2 (Recipe 3, end to end): a reachable client that went quiet (its runtime runs, it sends nothing) is TOLD its session
// expired.  The Rig's guard is off (it must see the broker's own notice).  RED (reapExpired closes silently): never told.
TEST(CatalogClusterRegression, ReapedSessionIsToldSessionExpiredOnTheWireRegression) {
    LeaseRig g("toldexpired");
    ASSERT_NO_FATAL_FAILURE(g.setUp(/*reconnectSilent=*/false));
    g.rig->cl().keepSessionsAlive(false);   // quiet: no Keepalive - the Aeron runtime and the egress poll keep running
    g.rig->pumping([] { std::this_thread::sleep_for(12s); });
    EXPECT_TRUE(g.rig->cl().pollUntil([&] { return g.rig->cl().sessionExpired(g.sid0); }))
        << "the lease reap closed the session silently";
}

// T-H3 (Recipe 1): RestartMidApply's ISR wait (13 s in F-A, modelled by an 11 s seam inside it) keeps the cell's sessions
// alive by itself - zero reconnects proves it was not the Rig's guard rescuing it.  RED (the ISR wait + seam outside
// rig.pumping): silentReconnects == 1 (with the guard also reverted: "never answered", exactly as in F-A).
TEST(CatalogClusterRegression, RestartMidApplyIsrWaitKeepsTheCellSessionsAliveRegression) {
    CellSeams seams;
    seams.afterIsrShrink = [](ThreeNodes&) { std::this_thread::sleep_for(11s); };
    CellOutcome out;
    runCell(CellParam{cm::Kind::DropTopic, cm::Timing::RestartMidApply, 0}, seams, &out);
    ASSERT_FALSE(::testing::Test::HasFailure());
    EXPECT_EQ(out.res.trace.find("no answer"), std::string::npos) << out.res.trace;
    EXPECT_EQ(out.roles.silentReconnects, 0) << "an unpumped wait in the cell let a session go silent past half the lease";
}

// ===========================================================================
// diag-duplicate F1 (2026-10-16): an acks=all NotLeader is IN DOUBT (the batch may be in the log and commit when leadership
// comes back); the slice's data-check records are produced idempotently, so the retry of an in-doubt answer is the broker's
// Duplicate (the original offset), never a second record.  RED (produceOnce ignores the identity: pid -1): two records.
// ===========================================================================
// Review M6: this pins the HARNESS wiring (the data check's resend carries the same identity, so the broker dedupes
// it) - the resend follows an OK, not an in-doubt NotLeader; the in-doubt edge itself is pinned by WorkerPsmRegression
// P1-P4b (F15EosRegressionTest).
TEST(CatalogClusterRegression, SliceDataCheckResendWithTheSameIdentityIsDeduplicatedRegression) {
    ThreeNodes c = ThreeNodes::start("dupretry");
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int L = c.settledLeader();
    ASSERT_GT(L, 0);
    for (const uint8_t persistence : {uint8_t{0}, uint8_t{1}}) {   // disk, transient
        const std::string name = persistence == 0 ? "dup" : "dup_t";
        SCOPED_TRACE(name);
        const Sender define = [=](ShardTestClient& cl, const std::string& ch, int64_t sid, RequestId rid) {
            return cl.sendDefineTopic(ch, kClientStream, sid, name, 1, charBlob(), persistence, rid);
        };
        ASSERT_NO_FATAL_FAILURE(rig.must(define, "define", L));
        const auto ti = rig.topicInfo(L, name, 60s);
        ASSERT_TRUE(ti.has_value());
        const ProduceIdentity ident = freshIdentity();
        const auto b = batchFor(true, 1, "x", ident);
        int64_t o1 = -1, o2 = -1;
        int leaderOfP0 = -1;
        for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && leaderOfP0 < 0;) {
            leaderOfP0 = rig.partitionLeader(L, ti->id, 0);
            if (leaderOfP0 < 0) std::this_thread::sleep_for(50ms);
        }
        ASSERT_GT(leaderOfP0, 0);
        ASSERT_TRUE(rig.produceFollowing(leaderOfP0, ti->id, 0, b, 30s, ident, nullptr, &o1));
        // The in-doubt retry: the same bytes and identity again (as after a NotLeader whose batch had been appended).
        int code = -1;
        for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline;) {
            code = rig.produceOnce(leaderOfP0, ti->id, 0, b, nullptr, &ident, &o2);
            if (code == cOk) break;
            std::this_thread::sleep_for(50ms);
        }
        EXPECT_EQ(code, cOk);
        EXPECT_EQ(o2, o1) << "the retry was stored as a new record, not answered with the original offset";
        std::string why;
        const auto got = rig.readAll(L, ti->id, 0, 1, 10s, &why);
        EXPECT_EQ(got.size(), 1u) << "the retried record is stored " << got.size() << " times" << why;
    }
}

// ===========================================================================
// Issue #82 (VLS Part A, the view read lease): a partitioned ex-leader never serves a stale GetViewRow after a write the
// new leader acked.  The source has three partitions spread over the three brokers, so one is led by the controller (the
// stale-controller case: the zombie is the deposed controller too) and another by a plain broker.  The source leader is cut
// off its peers (consensus and replication; its client link stays up), the majority fails the partition over, the new
// leader acks key K -> hp 2, and every read of K through the zombie is refused retriably (a bare NotLeader, a Redirect or
// NotReady), never answered OK with its frozen hp 1.  Healed, reads through it reach hp 2.
// RED before the fix: the zombie answers OK with hp 1 (its role and its leadership generation never change).
// ===========================================================================
namespace {
/// The smallest key >= 1 a `parts`-partition view routes to partition `p` (the gateway's rune::protocol::partitionForKey).
int64_t viewKeyFor(int32_t p, int32_t parts) {
    for (int64_t k = 1; k < 100'000; ++k) {
        const auto kb = keyBytes(k);
        if (rune::protocol::partitionForKey(std::span<const uint8_t>(kb.data(), kb.size()), parts) == p) return k;
    }
    return -1;
}

void zombieViewReadCase(bool zombieIsController, const std::string& tag) {
    ThreeNodes c = ThreeNodes::start(tag);
    ASSERT_TRUE(c.awaitCatalogLeader(60s));
    Rig rig(c);
    const int leader = c.settledLeader();
    ASSERT_NO_FATAL_FAILURE(rig.must(defineSender("src", 3, true), "define src", leader));
    const auto si = rig.topicInfo(leader, "src", 30s);
    ASSERT_TRUE(si.has_value());
    ASSERT_NO_FATAL_FAILURE(rig.must(createViewSender("v", si->id), "create v", leader));
    const auto vi = rig.topicInfo(leader, "v", 30s);
    ASSERT_TRUE(vi.has_value());
    int64_t keys[3];
    for (int32_t p = 0; p < 3; ++p) {
        keys[p] = viewKeyFor(p, 3);
        ASSERT_GT(keys[p], 0);
        const ProduceIdentity ident = freshIdentity();
        ASSERT_TRUE(rig.produceFollowing(leader, si->id, p, batchFor(true, keys[p], asString(characterValue(keys[p], 1)), ident),
                                         60s, ident)) << "hp 1 for key " << keys[p] << " (p" << p << ") was never acked";
    }
    // The controller (the consensus leader) and the partition whose leader is (not) it.
    int ctrl = -1, zp = -1, zombie = -1;
    for (const auto deadline = Clock::now() + 30s; Clock::now() < deadline && zombie < 0;) {
        ctrl = -1;
        for (int n = 1; n <= 3; ++n) if (c.node(n).isLeader()) ctrl = n;
        for (int32_t p = 0; p < 3 && ctrl > 0 && zombie < 0; ++p) {
            const int l = rig.partitionLeader(leader, si->id, p);
            if (l > 0 && (l == ctrl) == zombieIsController) { zp = p; zombie = l; }
        }
        if (zombie < 0) std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(zombie, 0) << "no source partition is led " << (zombieIsController ? "by" : "outside") << " the controller "
                         << ctrl << " (the three partitions are expected spread over the three brokers)";
    const int64_t key = keys[zp];
    const auto warm = rig.viewRowSettled(zombie, vi->id, static_cast<uint64_t>(key), 30s);
    ASSERT_TRUE(warm.ok()) << "the source leader serves the row before the partition";
    ASSERT_EQ(rowHp(warm.row), 1);
    ASSERT_EQ(warm.finalNode, zombie) << "... itself (it folds the view)";

    c.node(zombie).partitionFromPeersForTest(true);
    const int survivor = zombie == 1 ? 2 : 1;
    int newLeader = -1;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline;) {
        const int l = rig.partitionLeader(survivor, si->id, zp);
        if (l > 0 && l != zombie) { newLeader = l; break; }
        std::this_thread::sleep_for(50ms);
    }
    ASSERT_GT(newLeader, 0) << "the majority fails partition " << zp << " over";
    const ProduceIdentity ident = freshIdentity();
    ASSERT_TRUE(rig.produceFollowing(newLeader, si->id, zp, batchFor(true, key, asString(characterValue(key, 2)), ident),
                                     60s, ident)) << "the new leader acks hp 2";

    int refused = 0;
    for (int i = 0; i < 20; ++i) {
        const auto r = rig.viewRowFollowing(zombie, vi->id, static_cast<uint64_t>(key));
        ASSERT_TRUE(r.answered) << "read " << i << " through the partitioned ex-leader was never answered";
        EXPECT_FALSE(!r.firstIsError && r.firstCode == pc::ViewRowResponse::kOk)
            << "read " << i << ": the partitioned ex-leader answered OK (hp " << rowHp(r.row)
            << ") after the new leader acked hp 2 - a stale read";
        EXPECT_TRUE(r.firstIsError ? (r.firstCode == cNotLeader || r.firstCode == cRedirect)
                                   : r.firstCode == pc::ViewRowResponse::kNotReady)
            << "read " << i << ": a retriable refusal (got " << (r.firstIsError ? "error " : "status ") << r.firstCode << ")";
        if (r.ok()) EXPECT_EQ(rowHp(r.row), 2) << "read " << i << ": a row reached by a redirect is the new one";
        refused += r.firstIsError || r.firstCode != pc::ViewRowResponse::kOk;
        std::this_thread::sleep_for(20ms);
    }
    EXPECT_EQ(refused, 20);
    const auto fresh = rig.viewRowSettled(newLeader, vi->id, static_cast<uint64_t>(key), 30s);
    ASSERT_TRUE(fresh.ok()) << "the new leader serves the row";
    EXPECT_EQ(rowHp(fresh.row), 2);

    c.node(zombie).partitionFromPeersForTest(false);
    bool healed = false;
    for (const auto deadline = Clock::now() + 60s; Clock::now() < deadline && !healed;) {
        const auto r = rig.viewRowFollowing(zombie, vi->id, static_cast<uint64_t>(key));
        if (r.ok()) {
            ASSERT_EQ(rowHp(r.row), 2) << "healed: a read through the old leader is never the stale row";
            healed = true;
        } else {
            std::this_thread::sleep_for(50ms);
        }
    }
    EXPECT_TRUE(healed) << "healed: reads through the old leader reach the new row";
}
}  // namespace

TEST(ViewReadLeaseRegression, PartitionedExLeaderNeverServesAStaleRowRegression) {
    zombieViewReadCase(/*zombieIsController=*/false, "vls_zombie");
}
// VLS A.7 ViewReadLeaseStaleControllerRegression: the zombie is the deposed controller as well (it granted its own lease).
TEST(ViewReadLeaseRegression, PartitionedControllerExLeaderNeverServesAStaleRowRegression) {
    zombieViewReadCase(/*zombieIsController=*/true, "vls_ctrl_zombie");
}
