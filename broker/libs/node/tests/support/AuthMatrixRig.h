#pragma once
// AuthMatrixRig.h - the executable side of the authentication interaction matrix (Task 17): ONE real authenticating
// BrokerNode (shard runtime, one worker, fused placement) with a bootstrapped admin, and per cell a fresh prefix
// "c<ordinal>.", user, role and objects.  Every request type of AuthMatrixExpectations.h has one issue() branch that
// sends the op's real frame and returns THE ANSWER CODE the broker gave (0 = the op's normal answer) plus whatever the
// cell asserts on: the answer's response template (assertion 1: the op's OWN response type), the answer frame without
// its ids (assertion 3), the Metadata presence, the ViewRowResponse status.  Every wait is bounded; a missing answer is
// code kNoAnswer, which no expectation names - the deadline is the failure.
//
// ViewOpen is GetViewRow by view topic id, the same request as ViewRead (the protocol has no name-addressed open).

#include "support/AuthLogReader.h"
#include "support/AuthMatrixExpectations.h"
#include "support/AuthRig.h"

#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace rune::node::test {

class AuthMatrixRig {
public:
    using Op = amatrix::Op;
    using Grant = amatrix::Grant;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    using SP = rune::protocol::SecurityPatternKind;
    using TID = rune::protocol::client::TemplateId;
    static constexpr int32_t kMissingId = 98'765;
    static constexpr int kInvalidTxnState = 19;
    static constexpr std::chrono::milliseconds kInFlightWindow{60};   ///< records already on the wire behind the end frame
    static constexpr std::chrono::milliseconds kQuietWindow{300};   ///< how long "nothing after the end" is watched

    struct Answer {
        int code = kNoAnswer;
        bool present = false;        ///< Metadata: the cell's topic is listed
        int viewStatus = -1;         ///< GetViewRow: the ViewRowResponse status
        int64_t corr = 0;            ///< the request's correlation id (streams: the subscription's)
        int64_t subId = 0;           ///< Subscribe: the subscription id
        std::string member;          ///< GroupJoin: the member id
        std::vector<uint8_t> frame;  ///< the answer frame without its header ids (the no-leak twin comparison)
        uint16_t templ = 0;          ///< the answer frame's template id
        uint16_t wantTempl = 0;      ///< the template the op's own response type has (at the stage that answered)
        int stage = 0;               ///< TxnProduce: 0 InitProducerId, 1 AddPartitionsToTxn, 2 Produce
    };
    /// One transactional producer of a session (TxnProduce): initialised once, its sequence advanced per produce.
    struct TxnState { bool inited = false; int64_t pid = 1; int16_t epoch = 0; int32_t seq = 0; };

    AuthMatrixRig() : rig_("matrix", shardOptions()) {
        // The writer that makes records flow into the topic a subscription watches (assertion 4).
        (void)rig_.newUser({grants::prefix(SO::Write, ST::Topic, "")}, &writerName_);
        writerSid_ = rig_.login(writerName_);
    }
    AuthRig& rig() noexcept { return rig_; }
    int64_t writerSession() const noexcept { return writerSid_; }

    class Cell;
    Cell newCell();

private:
    static AuthBroker::Options shardOptions() { AuthBroker::Options o; o.shard = true; return o; }
    AuthRig rig_;
    int ordinal_ = 0;
    std::string writerName_;
    int64_t writerSid_ = 0;
    // The catalog / security events read so far (a cursor each: a cell reads only what is new, never the whole log).
    std::vector<rune::catalog::CatalogEvent> catalogEvents_;
    std::vector<rune::security::SecurityEvent> securityEvents_;
    int64_t catalogCursor_ = 0, securityCursor_ = 0;
};

class AuthMatrixRig::Cell {
public:
    Cell(AuthMatrixRig& m, int ordinal) : m_(m), r_(m.rig_), ord_(ordinal) {
        prefix_ = "c" + std::to_string(ord_) + ".";
        user_ = "u" + std::to_string(1000 + ord_);
        role_ = "r" + std::to_string(ord_);
        okAtStart_ = counterNow(kLoginsOkCounterTypeId);
        failedAtStart_ = counterNow(kLoginsFailedCounterTypeId);
        revokedAtStart_ = counterNow(kSessionsRevokedCounterTypeId);
    }
    const std::string& userName() const noexcept { return user_; }
    int64_t session() const noexcept { return sid_; }

    // ── names ───────────────────────────────────────────────────────────────────────────────────────────────
    std::string t() const { return prefix_ + "t"; }
    std::string u() const { return prefix_ + "u"; }
    std::string v() const { return prefix_ + "v"; }
    std::string g() const { return prefix_ + "g"; }
    std::string x() const { return prefix_ + "x"; }
    std::string newTopic() const { return prefix_ + "n"; }
    std::string newView() const { return prefix_ + "w"; }
    std::string newRole() const { return "q" + std::to_string(ord_); }

    // ── objects (created by the builder) ────────────────────────────────────────────────────────────────────
    void createObjects(Op op, bool supportingOnly = false) {
        auto topic = [&] { if (tid_ >= 0) return; tid_ = r_.defineTopic(r_.builder, t()); ASSERT_GE(tid_, 0); ASSERT_TRUE(r_.awaitListed(tid_)); };
        auto view  = [&] { if (vid_ >= 0) return; vid_ = r_.createView(r_.builder, v(), tid_); ASSERT_GE(vid_, 0); };
        switch (op) {
            case Op::Produce: case Op::ProduceRouted: case Op::TxnProduce: case Op::Subscribe: case Op::Metadata:
            case Op::AlterSchema: case Op::SetCompatibility: case Op::AlterTopicConfig: case Op::DropTopic:
                if (!supportingOnly) topic();
                break;
            case Op::GroupJoin: case Op::OffsetCommit: case Op::OffsetFetch: case Op::CreateView:
                topic();
                break;
            case Op::ViewRead: case Op::ViewOpen: case Op::AlterView: case Op::DropView:
                if (supportingOnly) { if (op == Op::AlterView) topic(); }
                else { topic(); view(); }
                break;
            case Op::CreateSchema:
                if (!supportingOnly && uid_ < 0) { uid_ = defineUntyped(u()); ASSERT_GE(uid_, 0); }
                break;
            default: break;
        }
    }
    /// A schema-less topic (the catalog's untyped topic: empty key field and descriptor).
    int32_t defineUntyped(const std::string& name) {
        pc::DefineTopic m;
        m.sessionId = r_.builder; m.correlationId = r_.c().nextCorrelationId(); m.topicName = name; m.partitionCount = 1;
        m.keyField = ""; m.validate = false; m.persistence = 0;
        m.hasExt = true; m.ext.clientName = "auth-matrix"; m.ext.requestId = {0xA07E'1700'0000'0000ull, static_cast<uint64_t>(m.correlationId)};
        std::vector<uint8_t> buf(m.encodedSize());
        const std::size_t len = m.encode(buf.data());
        if (!r_.c().send(r_.ch(), r_.stream(), buf.data(), len)) return -1;
        const auto d = r_.c().awaitDefine(m.correlationId, AuthRig::dl(20));
        return d && d->errorCode == 0 ? d->topicId : -1;
    }

    // ── grants ──────────────────────────────────────────────────────────────────────────────────────────────
    struct Req { SO op; ST type; std::string name; };
    std::vector<Req> requires_(Op op) const {
        switch (op) {
            case Op::Produce: case Op::ProduceRouted: return {{SO::Write, ST::Topic, t()}};
            case Op::TxnProduce: return {{SO::Write, ST::TransactionalId, x()}, {SO::Write, ST::Topic, t()}};
            case Op::TxnInit: case Op::TxnEnd: return {{SO::Write, ST::TransactionalId, x()}};
            case Op::Subscribe: return {{SO::Read, ST::Topic, t()}};
            case Op::GroupJoin: case Op::OffsetCommit: case Op::OffsetFetch: return {{SO::Read, ST::Group, g()}, {SO::Read, ST::Topic, t()}};
            case Op::ViewRead: case Op::ViewOpen: return {{SO::Read, ST::View, v()}};
            case Op::Metadata: case Op::CatalogRead: return {{SO::Describe, ST::Topic, t()}};
            case Op::CreateTopic: return {{SO::Create, ST::Topic, newTopic()}};
            case Op::CreateView: return {{SO::Create, ST::View, newView()}, {SO::Read, ST::Topic, t()}};
            case Op::CreateSchema: return {{SO::Create, ST::Topic, u()}};
            case Op::AlterSchema: case Op::SetCompatibility: case Op::AlterTopicConfig: return {{SO::Alter, ST::Topic, t()}};
            case Op::AlterView: return {{SO::Alter, ST::View, v()}, {SO::Read, ST::Topic, t()}};
            case Op::DropTopic: return {{SO::Drop, ST::Topic, t()}};
            case Op::DropView: return {{SO::Drop, ST::View, v()}};
            case Op::SecurityDdl: case Op::SecurityRead: return {{SO::Admin, ST::Cluster, ""}};
            default: return {};
        }
    }
    static bool clusterLevel(Op op) { return op == Op::CatalogRead || amatrix::clusterOnly(op); }
    void grantTo(const std::string& who, bool isRole, SO op, ST type, SP kind, const std::string& pattern) {
        const auto a = r_.broker().admin().grant(r_.ch(), r_.broker().adminSession(), who, isRole, op, type, kind, pattern);
        ASSERT_EQ(a.errorCode, 0) << a.detail;
    }
    void revokeFrom(const std::string& who, bool isRole, SO op, ST type, SP kind, const std::string& pattern) {
        const auto a = r_.broker().admin().revoke(r_.ch(), r_.broker().adminSession(), who, isRole, op, type, kind, pattern);
        ASSERT_EQ(a.errorCode, 0) << a.detail;
    }
    /// Issue exactly the op's Requires row in the grant's form; returns after the DDL answer (the gate, D24).
    void grantFor(Op op, Grant gr) {
        if (!userCreated_) {
            const auto a = r_.broker().admin().createUser(r_.ch(), r_.broker().adminSession(), user_, AuthRig::kPw);
            ASSERT_EQ(a.errorCode, 0) << a.detail;
            userCreated_ = true;
        }
        if (gr == Grant::None) return;
        const auto reqs = requires_(op);
        std::string who = user_;
        bool isRole = false;
        if (gr == Grant::ViaRole) {
            const auto c = r_.broker().admin().createRole(r_.ch(), r_.broker().adminSession(), role_);
            ASSERT_EQ(c.errorCode, 0) << c.detail;
            who = role_; isRole = true;
        }
        for (std::size_t i = 0; i < reqs.size(); ++i) {
            const Req& q = reqs[i];
            if (gr == Grant::Prefix) grantTo(who, isRole, q.op, q.type, SP::Prefix, prefix_);
            else if (gr == Grant::Cluster && i == 0) grantTo(who, isRole, q.op, ST::Cluster, SP::Exact, "");
            else if (gr == Grant::ViaRole && clusterLevel(op)) grantTo(who, isRole, q.op, ST::Cluster, SP::Exact, "");
            else grantTo(who, isRole, q.op, q.type, SP::Exact, q.name);
        }
        if (gr == Grant::ViaRole) {
            const auto a = r_.broker().admin().grantRole(r_.ch(), r_.broker().adminSession(), user_, role_);
            ASSERT_EQ(a.errorCode, 0) << a.detail;
        }
    }
    /// Revoke the op's first requirement (or the role) in the grant's form.
    void revokeFor(Op op, Grant gr) {
        const Req q = requires_(op).front();
        if (gr == Grant::ViaRole) {
            const auto a = r_.broker().admin().revokeRole(r_.ch(), r_.broker().adminSession(), user_, role_);
            ASSERT_EQ(a.errorCode, 0) << a.detail;
        } else if (gr == Grant::Prefix) revokeFrom(user_, false, q.op, q.type, SP::Prefix, prefix_);
        else if (gr == Grant::Cluster) revokeFrom(user_, false, q.op, ST::Cluster, SP::Exact, "");
        else revokeFrom(user_, false, q.op, q.type, SP::Exact, q.name);
    }
    void disableUser() {
        const auto a = r_.broker().admin().alterPassword(r_.ch(), r_.broker().adminSession(), user_,
                                                        rune::protocol::client::UserChangeKind::Disable);
        ASSERT_EQ(a.errorCode, 0) << a.detail;
    }

    // ── sessions ────────────────────────────────────────────────────────────────────────────────────────────
    /// End the cell's sessions.  (Hygiene only: the memory growth of a long run is the live view partitions' row slabs -
    /// AuthRig's view plans use small chunks - not dead sessions.)
    void close() {
        for (const int64_t s : {sid_, twinSid_})
            if (s != 0) (void)r_.c().disconnect(r_.ch(), r_.stream(), s);
        sid_ = twinSid_ = 0;
        r_.c().clearObserved();
    }
    void login() { sid_ = r_.login(user_); ++loginsOk_; }
    /// A reconnect attempt of the (disabled) user: the AuthServerFinal error code.
    int relogin() {
        const int64_t s = r_.wire().connect(r_.ch());
        if (s <= 0) return kNoAnswer;
        const int code = r_.wire().login(r_.ch(), s, user_, {AuthRig::kPw, AuthRig::kPw}).errorCode();
        if (code == 0) ++loginsOk_; else ++loginsFailed_;
        return code;
    }
    /// The session-level frame (corr 0) code after a disable, bounded.
    int awaitSessionLevel() { return errorOn(0, sid_); }
    int errorOn(int64_t corr, int64_t sessionId = 0) {
        int code = kNoAnswer;
        r_.c().pollUntil([&] {
            for (const auto& e : r_.c().errors())
                if (e.correlationId == corr && (sessionId == 0 || e.sessionId == sessionId)) { code = e.code; endSeq_ = e.seq; return true; }
            return false;
        });
        return code;
    }
    /// A subscription's end: the ErrorResponse on its correlation id (GroupJoin: the member's next Heartbeat).
    int awaitEnd(Op op, const Answer& a) {
        if (op == Op::GroupJoin) {
            const int code = r_.heartbeat(sid_, g(), a.member);
            note(r_.c().lastCorrelationId());
            return code;
        }
        return errorOn(a.corr);
    }

    // ── assertion 4: records flow into a live subscription, and none after its end ──────────────────────────
    /// Make one record flow into the subscription and wait for its delivery: the "succeeds once, subscription live and
    /// receiving" half of the RevokedMid / UserDisabledMid timings.  Returns false (with a reason) if it never arrives.
    bool makeLive(Op op, const Answer& a, std::string& why) {
        const std::size_t before = recordsOf(a.subId);
        if (!trigger(op, why)) return false;
        if (!r_.c().pollUntil([&] { return recordsOf(a.subId) > before; })) { why = "no record reached the live subscription"; return false; }
        return true;
    }
    /// After the end frame was observed: produce more, wait the quiet window, and assert that NO Records frame for the
    /// subscription arrived after the end frame (arrival order, not a sample: a record pushed right behind the end counts).
    bool noDeliveryAfterEnd(Op op, const Answer& a, std::string& why) {
        if (endSeq_ == 0) { why = "no end frame was observed"; return false; }
        auto after = [&] { std::size_t n = 0; for (const auto& p : r_.c().records()) n += p.subscriptionId == a.subId && p.seq > endSeq_ ? 1 : 0; return n; };
        // The admin-log reads (catalog / security) are pushed by the admin worker's own publication while the gateway sends the
        // end from its control thread: the event that CAUSED the end (the revoke / disable) may still be in flight behind the end
        // frame.  At most that one record is tolerated (deferred: rune-followups/issues.md, "admin-log read end is not ordered
        // with the admin worker's pushes"); a topic subscription tolerates none.  Nothing caused by a LATER event may arrive.
        const std::size_t allowedInFlight = op == Op::CatalogRead || op == Op::SecurityRead ? 1 : 0;
        r_.c().pollFor(kInFlightWindow, [] { return false; });
        const std::size_t inFlight = after();
        if (inFlight > allowedInFlight) { why = std::to_string(inFlight) + " Records frames were delivered after the end"; return false; }
        if (!trigger(op, why)) return false;
        r_.c().pollFor(kQuietWindow, [] { return false; });
        if (after() != inFlight) { why = "a Records frame caused by a later event was delivered after the end"; return false; }
        return true;
    }

    // ── one request as the user (or as another session) ─────────────────────────────────────────────────────
    Answer issue(Op op, bool missing) { return issueAs(sid_, op, missing, txn_); }

    Answer issueAs(int64_t sid, Op op, bool missing, TxnState& txn) {
        using namespace amatrix;
        Answer a;
        // R3: only the id-addressed requests name an id that never existed; the implicit objects (group, transactional
        // id) are missing by never having been created, and the name-addressed requests use the (uncreated) name.
        const bool idAddressed = op == Op::Produce || op == Op::ProduceRouted || op == Op::TxnProduce ||
                                 op == Op::Subscribe || op == Op::ViewRead || op == Op::ViewOpen;
        const int32_t tid = missing && idAddressed ? kMissingId : tid_;
        const int32_t vid = missing && idAddressed ? kMissingId : vid_;
        switch (op) {
            case Op::Produce: a.wantTempl = tmpl(TID::ProduceResponse); a.code = produceVia(r_.broker().workerIngressChannel(), sid, tid, makeBatch(1), a); break;
            case Op::ProduceRouted: a.wantTempl = tmpl(TID::ProduceResponse); a.code = produceVia(r_.ch(), sid, tid, makeBatch(1), a); break;
            case Op::TxnProduce: a.code = txnProduce(sid, tid, txn, a); break;
            case Op::TxnInit: {
                a.wantTempl = tmpl(TID::InitProducerIdResponse);
                const auto init = txnInit(sid, x(), a);
                a.code = init.code;
                break;
            }
            case Op::TxnEnd: {
                Answer scratch;
                auto init = txnInit(sid, x(), scratch);
                if (init.code != 0) init = {init.code, 1, 0};
                a.wantTempl = tmpl(TID::EndTxnResponse);
                a.code = txnEnd(sid, init, a);
                // An authorized EndTxn with no transaction open answers InvalidTxnState (19): authorization passed, which
                // is all the matrix asserts (WRITE on the transactional id is the whole requirement).
                if (a.code == kInvalidTxnState) a.code = 0;
                break;
            }
            case Op::Subscribe: a.wantTempl = tmpl(TID::SubscribeResponse); a.corr = r_.c().sendSubscribe(r_.ch(), r_.stream(), sid, tid, 0); a.code = subscribeCode(a); break;
            case Op::CatalogRead: a.wantTempl = tmpl(TID::SubscribeResponse); a.corr = r_.c().sendCatalogSubscribe(r_.ch(), r_.stream(), sid, 0); a.code = subscribeCode(a); break;
            case Op::SecurityRead:
                a.wantTempl = tmpl(TID::SubscribeResponse);
                a.corr = r_.c().sendSubscribe(r_.ch(), r_.stream(), sid, rune::protocol::client::kSecurityTopicId, 0);
                a.code = subscribeCode(a);
                break;
            case Op::GroupJoin: {
                a.wantTempl = tmpl(TID::JoinGroupResponse);
                const auto j = r_.c().joinGroup(r_.ch(), r_.stream(), sid, g(), {}, {tid});
                a.corr = r_.c().lastCorrelationId();
                note(a.corr);
                captureFrame(a);
                a.code = j ? j->errorCode : kNoAnswer;
                if (j) a.member = j->memberId;
                break;
            }
            case Op::OffsetCommit: {
                // A commit needs a live member: join first (a denied join leaves the commit to answer on its own).
                std::string member = "m";
                int32_t generation = 1;
                if (const auto j = r_.c().joinGroup(r_.ch(), r_.stream(), sid, g(), {}, {tid})) {
                    note(r_.c().lastCorrelationId());
                    if (j->errorCode == 0) { member = j->memberId; generation = j->generation; }
                }
                a.wantTempl = tmpl(TID::OffsetCommitResponse);
                const auto code = r_.c().offsetCommit(r_.ch(), r_.stream(), sid, g(), member, generation, tid, 0, 1);
                a.corr = r_.c().lastCorrelationId();
                note(a.corr);
                captureFrame(a);
                a.code = code ? *code : kNoAnswer;
                break;
            }
            case Op::OffsetFetch: {
                a.wantTempl = tmpl(TID::OffsetFetchResponse);
                const auto f = r_.c().offsetFetch(r_.ch(), r_.stream(), sid, g(), tid, 0);
                a.corr = r_.c().lastCorrelationId();
                note(a.corr);
                captureFrame(a);
                a.code = f ? f->second : kNoAnswer;
                break;
            }
            case Op::ViewRead: case Op::ViewOpen: {
                a.wantTempl = tmpl(TID::ViewRowResponse);
                a.corr = r_.c().sendGetViewRow(r_.ch(), r_.stream(), sid, vid, 1);
                note(a.corr);
                const auto ans = r_.c().awaitViewRowOrError(a.corr, AuthRig::dl());
                captureFrame(a);
                if (!ans) break;
                a.viewStatus = ans->isError ? ans->errorCode : ans->status;
                // 0 OK / 1 AbsentKey / 2 NotReady: authorized and answered (the view is empty); the rest as is.
                a.code = !ans->isError && a.viewStatus >= 0 && a.viewStatus <= 2 ? 0 : a.viewStatus;
                break;
            }
            case Op::Metadata: metadata(sid, a); break;
            case Op::CreateTopic: a.wantTempl = tmpl(TID::DefineTopicResponse); a.corr = r_.c().sendDefineTopic(r_.ch(), r_.stream(), sid, newTopic(), 1, authRigCharBlob()); a.code = ddl(a); okDdl(a, newTopic(), false); break;
            case Op::CreateView: a.wantTempl = tmpl(TID::CreateViewResponse); a.corr = r_.c().sendCreateView(r_.ch(), r_.stream(), sid, newView(), "id", 0, authRigViewPlan(newView(), {tid_})); a.code = ddl(a); okDdl(a, newView(), false); break;
            case Op::CreateSchema: a.wantTempl = tmpl(TID::DefineTopicResponse); a.corr = r_.c().sendDefineTopic(r_.ch(), r_.stream(), sid, u(), 1, authRigCharBlob()); a.code = ddl(a); okDdl(a, u(), false); break;
            case Op::AlterSchema: {
                rune::codec::AlterOp o{};
                o.type = rune::codec::AlterOpType::Add; o.name = "extra"; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
                a.wantTempl = tmpl(TID::AlterTopicResponse);
                a.corr = r_.c().sendAlterTopic(r_.ch(), r_.stream(), sid, t(), 0, rune::codec::encodeAlterOps({o}));
                a.code = ddl(a);
                okDdl(a, t(), false);
                break;
            }
            case Op::SetCompatibility: {
                rune::codec::AlterOp o{};
                o.type = rune::codec::AlterOpType::SetCompatibility; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
                o.mode = rune::codec::CompatMode::Forward;
                a.wantTempl = tmpl(TID::AlterTopicResponse);
                a.corr = r_.c().sendAlterTopic(r_.ch(), r_.stream(), sid, t(), 0, rune::codec::encodeAlterOps({o}));
                a.code = ddl(a);
                okDdl(a, t(), false);
                break;
            }
            case Op::AlterTopicConfig: {
                const pc::ConfigChangeWire cc{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 600'000};
                a.wantTempl = tmpl(TID::DdlResponse);
                a.corr = r_.c().sendAlterTopicConfig(r_.ch(), r_.stream(), sid, t(), 0, {cc});
                a.code = ddl(a);
                okDdl(a, t(), false);
                break;
            }
            case Op::AlterView: a.wantTempl = tmpl(TID::DdlResponse); a.corr = r_.c().sendAlterView(r_.ch(), r_.stream(), sid, v(), "id", 0, 0, authRigViewPlan(v(), {tid_})); a.code = ddl(a); okDdl(a, v(), false); break;
            case Op::DropTopic: a.wantTempl = tmpl(TID::DdlResponse); a.corr = r_.c().sendDropTopic(r_.ch(), r_.stream(), sid, t()); a.code = ddl(a); okDdl(a, t(), false); break;
            case Op::DropView: a.wantTempl = tmpl(TID::DdlResponse); a.corr = r_.c().sendDropView(r_.ch(), r_.stream(), sid, v()); a.code = ddl(a); okDdl(a, v(), false); break;
            case Op::SecurityDdl: {
                a.wantTempl = tmpl(TID::DdlResponse);
                const auto ans = r_.wire().createRole(r_.ch(), sid, newRole());
                a.corr = r_.c().lastCorrelationId();
                note(a.corr);
                captureFrame(a);
                a.code = ans.errorCode == 0xFFFF ? kNoAnswer : ans.errorCode;
                okDdl(a, newRole(), true);
                break;
            }
            case Op::kCount: break;
        }
        return a;
    }

    /// Assertion 3 (D22 / D23): the frame of a NotAuthorized answer equals the frame a session without grants gets for
    /// the same request against the EXISTING object (a TxnProduce twin holds WRITE on the transactional id when the
    /// denial came after the init).  Empty string = holds; otherwise the reason.  Call at the end of the cell.
    std::string hiddenTwinProblem(Op op, bool missing, const Answer& denied) {
        if (denied.frame.empty()) return "no answer frame was captured for the NotAuthorized answer";
        if (missing || tid_ < 0) { createObjects(op); if (::testing::Test::HasFatalFailure()) return "twin object creation failed"; }
        if (!twinCreated_) {
            const std::string n = "h" + std::to_string(1000 + ord_);
            const auto a = r_.broker().admin().createUser(r_.ch(), r_.broker().adminSession(), n, AuthRig::kPw);
            if (a.errorCode != 0) return "twin user: " + a.detail;
            if (op == Op::TxnProduce && denied.stage >= 1) grantTo(n, false, SO::Write, ST::TransactionalId, SP::Exact, x());
            twinSid_ = r_.login(n);
            ++loginsOk_;
            twinCreated_ = true;
        }
        const Answer twin = issueAs(twinSid_, op, /*missing=*/false, twinTxn_);
        if (twin.code != 45) return "the twin on the existing object answered " + std::to_string(twin.code);
        if (twin.templ != denied.templ) return "the twin's response type differs";
        if (blank(twin.frame) != blank(denied.frame)) return "the answer frames differ: NotAuthorized leaks existence";
        return {};
    }

    // ── end-of-cell assertions ──────────────────────────────────────────────────────────────────────────────
    /// Assertion 2: every one-shot request got exactly one answer frame, every subscription at least its response.
    std::string answersProblem() {
        r_.c().pollFor(std::chrono::milliseconds(30), [] { return false; });
        for (const int64_t c : oneShot_)
            if (r_.c().framesFor(c) != 1) return "request corr " + std::to_string(c) + " got " + std::to_string(r_.c().framesFor(c)) + " answers";
        return {};
    }
    /// Assertion 5: the logins ok / failed and sessions-revoked counters moved exactly as the cell implies.
    std::string countersProblem(bool disabled) {
        struct C { std::int32_t id; int64_t start; int64_t delta; const char* name; };
        for (const C& c : {C{kLoginsOkCounterTypeId, okAtStart_, loginsOk_, "logins ok"},
                           C{kLoginsFailedCounterTypeId, failedAtStart_, loginsFailed_, "logins failed"},
                           C{kSessionsRevokedCounterTypeId, revokedAtStart_, disabled ? 1 : 0, "sessions revoked"}}) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (counterNow(c.id) < c.start + c.delta && std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            if (counterNow(c.id) < c.start + c.delta)
                return std::string(c.name) + " counter is " + std::to_string(counterNow(c.id)) + ", expected " + std::to_string(c.start + c.delta);
        }
        r_.c().pollFor(std::chrono::milliseconds(30), [] { return false; });   // a stray extra tick would land here
        for (const C& c : {C{kLoginsOkCounterTypeId, okAtStart_, loginsOk_, "logins ok"},
                           C{kLoginsFailedCounterTypeId, failedAtStart_, loginsFailed_, "logins failed"},
                           C{kSessionsRevokedCounterTypeId, revokedAtStart_, disabled ? 1 : 0, "sessions revoked"}}) {
            const int64_t now = counterNow(c.id);
            if (now != c.start + c.delta)
                return std::string(c.name) + " counter is " + std::to_string(now) + ", expected " + std::to_string(c.start + c.delta);
        }
        return {};
    }
    /// Assertion 6: every DDL this cell's user got an OK for is recorded with principal.authenticated == the user.
    std::string principalProblem() {
        if (okDdl_.empty()) return {};
        BrokerNode& n = r_.broker().node();
        for (auto& e : readCatalogEvents(n, &m_.catalogCursor_)) m_.catalogEvents_.push_back(std::move(e));
        for (auto& e : readSecurityEvents(n, &m_.securityCursor_)) m_.securityEvents_.push_back(std::move(e));
        const auto& cat = m_.catalogEvents_;
        const auto& sec = m_.securityEvents_;
        for (const auto& [name, security] : okDdl_) {
            std::string last;
            bool found = false;
            if (security) { for (const auto& e : sec) if (e.objectName == name) { found = true; last = e.principal.authenticated; } }
            else { for (const auto& e : cat) if (e.objectName == name) { found = true; last = e.principal.authenticated; } }
            if (!found) return "no " + std::string(security ? "__security" : "__catalog") + " event for " + name;
            if (last != user_) return "event for " + name + " records principal '" + last + "', expected '" + user_ + "'";
        }
        return {};
    }

private:
    struct TxnIds { int code = kNoAnswer; int64_t pid = 1; int16_t epoch = 0; };
    static uint16_t tmpl(TID t) { return static_cast<uint16_t>(t); }
    static constexpr std::size_t kHeader = pc::MessageHeader::ENCODED_SIZE;
    static std::vector<uint8_t> stripHeader(std::vector<uint8_t> f) {
        if (f.size() <= kHeader) return {};
        f.erase(f.begin(), f.begin() + kHeader);
        return f;
    }
    /// Blank the topic / view ids an answer may echo (a real id and the never-existing one must not tell apart).
    std::vector<uint8_t> blank(std::vector<uint8_t> f) const {
        for (const int32_t id : {tid_, vid_, kMissingId}) {
            if (id < 0 || f.size() < 4) continue;
            uint8_t le[4];
            std::memcpy(le, &id, 4);
            for (std::size_t i = 0; i + 4 <= f.size(); ++i)
                if (std::memcmp(f.data() + i, le, 4) == 0) std::memset(f.data() + i, 0, 4);
        }
        return f;
    }
    int64_t counterNow(std::int32_t id) const { return r_.broker().counter(id).value_or(-1); }
    void note(int64_t corr) { if (corr != 0) oneShot_.push_back(corr); }
    std::size_t recordsOf(int64_t subId) {
        std::size_t n = 0;
        for (const auto& p : r_.c().records()) n += p.subscriptionId == subId ? 1 : 0;
        return n;
    }
    /// Capture the answer frame of a.corr right now (before any later frame of the same correlation overwrites it).
    void captureFrame(Answer& a) {
        if (a.corr == 0) return;
        const auto raw = r_.c().awaitRaw(a.corr, AuthRig::dl(2));
        if (!raw) return;
        if (const auto h = pc::MessageHeader::decode(raw->data(), raw->size())) a.templ = h->templateId;
        a.frame = stripHeader(*raw);
    }
    void fill(Answer& a, int64_t corr, const std::vector<uint8_t>& raw) {
        a.corr = corr;
        if (raw.empty()) return;
        if (const auto h = pc::MessageHeader::decode(raw.data(), raw.size())) a.templ = h->templateId;
        a.frame = stripHeader(raw);
    }
    void okDdl(const Answer& a, const std::string& name, bool security) {
        if (a.code == 0) okDdl_.emplace_back(name, security);
    }
    int ddl(Answer& a) {
        note(a.corr);
        const int code = r_.ddlCode(a.corr);
        captureFrame(a);
        return code;
    }
    int subscribeCode(Answer& a) {
        const auto code = r_.c().awaitSubscribeCode(a.corr, AuthRig::dl());
        captureFrame(a);
        if (!code) return kNoAnswer;
        if (*code == 0) a.subId = r_.c().subscriptionIdOf(a.corr).value_or(0);
        return *code;
    }
    /// The record that must reach a live subscription of `op` (assertion 4).
    bool trigger(Op op, std::string& why) {
        ++triggers_;
        switch (op) {
            case Op::Subscribe:
                if (r_.produce(m_.writerSid_, tid_, /*untilOk=*/true) != 0) { why = "the writer's produce failed"; return false; }
                return true;
            case Op::CatalogRead:
                if (r_.defineTopic(r_.builder, prefix_ + "lv" + std::to_string(triggers_)) < 0) { why = "the builder's DDL failed"; return false; }
                return true;
            case Op::SecurityRead: {
                const auto a = r_.broker().admin().createRole(r_.ch(), r_.broker().adminSession(), prefix_ + "lv" + std::to_string(triggers_));
                if (a.errorCode != 0) { why = "the admin's CREATE ROLE failed: " + a.detail; return false; }
                return true;
            }
            default: return true;
        }
    }
    /// A produce on `channel`, retried while leadership / placement settles (NotLeader, UnknownTopic from the worker
    /// that has not heard of the topic yet); NotAuthorized and OK are final.  Bounded: 200 x 25 ms.
    int produceVia(const std::string& channel, int64_t sid, int32_t topicId, const std::vector<uint8_t>& batch, Answer& a,
                   const TxnState* txn = nullptr) {
        int code = kNoAnswer;
        for (int attempt = 0; attempt < 200; ++attempt) {
            a.corr = r_.c().nextCorrelationId();
            note(a.corr);
            const bool sent = txn ? r_.c().produceIdempotent(channel, r_.stream(), sid, a.corr, topicId, 0, 1, txn->pid, txn->epoch, txn->seq, batch)
                                  : r_.c().produce(channel, r_.stream(), sid, a.corr, topicId, 0, 1, batch);
            if (!sent) return kNoAnswer;
            const auto ack = r_.c().awaitProduceResponseUntil(a.corr, AuthRig::dl(10));
            code = ack ? ack->errorCode : kNoAnswer;
            if (code != static_cast<int>(pc::ErrorCode::NotLeader) && code != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
                code != kNoAnswer)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        captureFrame(a);
        return code;
    }
    template <class Msg, class... Rest>
    std::vector<uint8_t> sendRaw(const Msg& m, std::size_t cap, Rest&&... rest) {
        std::vector<uint8_t> buf(cap);
        const std::size_t len = m.encode(buf.data(), std::forward<Rest>(rest)...);
        if (len == 0 || !r_.c().send(r_.ch(), r_.stream(), buf.data(), len)) return {};
        note(m.correlationId);
        return r_.rawAnswer(m.correlationId);
    }
    static std::span<const uint8_t> bytes(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
    static int errorCodeOf(const std::vector<uint8_t>& f) {
        if (const auto e = pc::ErrorResponse::decode(f.data(), f.size())) return e->code;
        return kNoAnswer;
    }
    TxnIds txnInit(int64_t sid, const std::string& txnId, Answer& a) {
        pc::InitProducerId m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.transactionTimeoutMs = 30'000;
        m.transactionalId = bytes(txnId);
        const auto f = sendRaw(m, 256);
        fill(a, m.correlationId, f);
        TxnIds out;
        if (f.empty()) return out;
        if (a.templ == tmpl(TID::InitProducerIdResponse)) {
            if (const auto r = pc::InitProducerIdResponse::decode(f.data(), f.size())) {
                out.code = r->errorCode; out.pid = r->producerId; out.epoch = r->producerEpoch;
            }
        } else out.code = errorCodeOf(f);
        return out;
    }
    /// InitProducerId once per producer, then AddPartitionsToTxn, then a transactional Produce: the first non-zero code
    /// answers (stage 0 / 1 / 2).  The WRITE TOPIC check is the one the add and the produce run.
    int txnProduce(int64_t sid, int32_t topicId, TxnState& st, Answer& a) {
        if (!st.inited) {
            a.wantTempl = tmpl(TID::InitProducerIdResponse);
            a.stage = 0;
            const auto init = txnInit(sid, x(), a);
            if (init.code != 0) return init.code;
            st.inited = true; st.pid = init.pid; st.epoch = init.epoch;
        }
        pc::AddPartitionsToTxn m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.producerId = st.pid; m.producerEpoch = st.epoch;
        const std::string txn = x();
        m.transactionalId = bytes(txn);
        const pc::TxnTopicPartition tp{topicId, 0};
        const auto f = sendRaw(m, 256, std::span<const pc::TxnTopicPartition>(&tp, 1));
        a.wantTempl = tmpl(TID::AddPartitionsToTxnResponse);
        a.stage = 1;
        fill(a, m.correlationId, f);
        if (f.empty()) return kNoAnswer;
        int code = kNoAnswer;
        if (const auto r = pc::AddPartitionsToTxnResponse::decode(f.data(), f.size())) code = r->results.empty() ? 0 : r->results[0].errorCode;
        else code = errorCodeOf(f);
        if (code != 0) return code;
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setTransactional(true); b.setProducerId(st.pid); b.setProducerEpoch(st.epoch); b.setBaseSequence(st.seq);
        b.append("k", "txn", 1000);
        a.wantTempl = tmpl(TID::ProduceResponse);
        a.stage = 2;
        const int pcode = produceVia(r_.broker().workerIngressChannel(), sid, topicId, b.build(), a, &st);
        if (pcode == 0) ++st.seq;
        return pcode;
    }
    int txnEnd(int64_t sid, const TxnIds& ids, Answer& a) {
        pc::EndTxn m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.producerId = ids.pid; m.producerEpoch = ids.epoch; m.commit = 1;
        const std::string txn = x();
        m.transactionalId = bytes(txn);
        const auto f = sendRaw(m, 256);
        fill(a, m.correlationId, f);
        if (f.empty()) return kNoAnswer;
        if (const auto r = pc::EndTxnResponse::decode(f.data(), f.size())) return r->errorCode;
        return errorCodeOf(f);
    }
    /// MetadataRequest{all}: no answer or an ErrorResponse is its code (never "filtered out"); a MetadataResponse is 0
    /// with the cell's topic's presence taken from the decoded partitions.
    void metadata(int64_t sid, Answer& a) {
        a.wantTempl = tmpl(TID::MetadataResponse);
        pc::MetadataRequest req;
        req.sessionId = sid; req.correlationId = r_.c().nextCorrelationId();
        std::vector<uint8_t> buf(req.encodedSize());
        const std::size_t len = req.encode(buf.data());
        if (!r_.c().send(r_.ch(), r_.stream(), buf.data(), len)) return;
        note(req.correlationId);
        const auto f = r_.rawAnswer(req.correlationId);
        fill(a, req.correlationId, f);
        if (f.empty()) return;
        if (a.templ == tmpl(TID::MetadataResponse)) {
            if (const auto r = pc::MetadataResponse::decode(f.data(), f.size())) {
                a.code = 0;
                for (const auto& e : r->partitions) a.present = a.present || (tid_ >= 0 && e.topicId == tid_);
            }
        } else a.code = errorCodeOf(f);
    }

    AuthMatrixRig& m_;
    AuthRig& r_;
    int ord_;
    std::string prefix_, user_, role_;
    int64_t sid_ = 0, twinSid_ = 0;
    int32_t tid_ = -1, vid_ = -1, uid_ = -1;
    bool userCreated_ = false, twinCreated_ = false;
    int triggers_ = 0;
    uint64_t endSeq_ = 0;   ///< arrival sequence of the last end frame errorOn() matched
    TxnState txn_, twinTxn_;
    std::vector<int64_t> oneShot_;
    std::vector<std::pair<std::string, bool>> okDdl_;
    int64_t okAtStart_ = 0, failedAtStart_ = 0, revokedAtStart_ = 0, loginsOk_ = 0, loginsFailed_ = 0;
};

inline AuthMatrixRig::Cell AuthMatrixRig::newCell() { return Cell(*this, ++ordinal_); }

} // namespace rune::node::test
