#pragma once
// AuthMatrixRig.h - the executable side of the authentication interaction matrix (Task 17): ONE real authenticating
// BrokerNode (shard runtime, one worker, fused placement) with a bootstrapped admin, and per cell a fresh prefix
// "c<ordinal>.", user, role and objects.  Every request type of AuthMatrixExpectations.h has one issue() branch that
// sends the op's real frame and returns THE ANSWER CODE the broker gave (0 = the op's normal answer) plus whatever the
// cell asserts on (the Metadata presence, the ViewRowResponse status, the answer frame for the no-leak check).
// Every wait is bounded; a missing answer is code kNoAnswer, which no expectation names - the deadline is the failure.

#include "support/AuthMatrixExpectations.h"
#include "support/AuthRig.h"

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
    static constexpr int32_t kMissingId = 98'765;
    static constexpr int kInvalidTxnState = 19;

    struct Answer {
        int code = kNoAnswer;
        bool present = false;        ///< Metadata: the cell's topic is listed
        int viewStatus = -1;         ///< GetViewRow: the ViewRowResponse status
        int64_t corr = 0;            ///< the request's correlation id (streams: the subscription's)
        int64_t subId = 0;           ///< Subscribe: the subscription id
        std::string member;          ///< GroupJoin: the member id
        std::vector<uint8_t> frame;  ///< the answer frame without its header ids (no-leak twin comparison)
    };

    AuthMatrixRig() : rig_("matrix", shardOptions()) {}
    AuthRig& rig() noexcept { return rig_; }

    class Cell;
    Cell newCell();

private:
    static AuthBroker::Options shardOptions() { AuthBroker::Options o; o.shard = true; return o; }
    AuthRig rig_;
    int ordinal_ = 0;
    friend class Cell;
};

class AuthMatrixRig::Cell {
public:
    Cell(AuthMatrixRig& m, int ordinal) : m_(m), r_(m.rig_), ord_(ordinal) {
        prefix_ = "c" + std::to_string(ord_) + ".";
        user_ = "u" + std::to_string(1000 + ord_);
        role_ = "r" + std::to_string(ord_);
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
    void login() { sid_ = r_.login(user_); }
    int relogin() {
        const int64_t s = r_.wire().connect(r_.ch());
        if (s <= 0) return kNoAnswer;
        return r_.wire().login(r_.ch(), s, user_, {AuthRig::kPw, AuthRig::kPw}).errorCode();
    }
    /// The session-level frame (corr 0) code after a disable, bounded.
    int awaitSessionLevel() { return errorOn(0, sid_); }
    int errorOn(int64_t corr, int64_t sessionId = 0) {
        int code = kNoAnswer;
        r_.c().pollUntil([&] {
            for (const auto& e : r_.c().errors())
                if (e.correlationId == corr && (sessionId == 0 || e.sessionId == sessionId)) { code = e.code; return true; }
            return false;
        });
        return code;
    }
    /// A subscription's end: the ErrorResponse on its correlation id (GroupJoin: the member's next Heartbeat).
    int awaitEnd(Op op, const Answer& a) {
        if (op == Op::GroupJoin) return r_.heartbeat(sid_, g(), a.member);
        return errorOn(a.corr);
    }
    /// No Records frame for the subscription arrives within a short window after its end.
    bool deliveredAfterEnd(const Answer& a) {
        const std::size_t before = recordsOf(a.subId);
        r_.c().pollFor(std::chrono::milliseconds(300), [] { return false; });
        return recordsOf(a.subId) != before;
    }

    // ── one request as the user (or as another session) ─────────────────────────────────────────────────────
    Answer issue(Op op, bool missing, int64_t as = 0) { return issueAs(as ? as : sid_, op, missing); }

    Answer issueAs(int64_t sid, Op op, bool missing) {
        using namespace amatrix;
        Answer a;
        // R3: only the id-addressed requests name an id that never existed; the implicit objects (group, transactional
        // id) are missing by never having been created, and the name-addressed requests use the (uncreated) name.
        const bool idAddressed = op == Op::Produce || op == Op::ProduceRouted || op == Op::TxnProduce ||
                                 op == Op::Subscribe || op == Op::ViewRead || op == Op::ViewOpen;
        const int32_t tid = missing && idAddressed ? kMissingId : tid_;
        const int32_t vid = missing && idAddressed ? kMissingId : vid_;
        switch (op) {
            case Op::Produce: a.code = produceVia(r_.broker().workerIngressChannel(), sid, tid, a); break;
            case Op::ProduceRouted: a.code = produceVia(r_.ch(), sid, tid, a); break;
            case Op::TxnProduce: {
                const auto init = txnInit(sid, x());
                if (init.code != 0) { a.code = init.code; break; }
                a.code = txnAddPartitions(sid, init, tid, a);
                break;
            }
            case Op::TxnInit: a.code = txnInit(sid, x()).code; break;
            case Op::TxnEnd: {
                auto init = txnInit(sid, x());
                if (init.code != 0) init = {init.code, 1, 0};
                a.code = txnEnd(sid, init, a);
                // An authorized EndTxn with no transaction open answers InvalidTxnState (19): authorization passed, which
                // is all the matrix asserts (WRITE on the transactional id is the whole requirement).
                if (a.code == kInvalidTxnState) a.code = 0;
                break;
            }
            case Op::Subscribe: a.corr = r_.c().sendSubscribe(r_.ch(), r_.stream(), sid, tid, 0); a.code = subscribeCode(a); break;
            case Op::CatalogRead: a.corr = r_.c().sendCatalogSubscribe(r_.ch(), r_.stream(), sid, 0); a.code = subscribeCode(a); break;
            case Op::SecurityRead:
                a.corr = r_.c().sendSubscribe(r_.ch(), r_.stream(), sid, rune::protocol::client::kSecurityTopicId, 0);
                a.code = subscribeCode(a);
                break;
            case Op::GroupJoin: a.code = r_.joinGroup(sid, g(), {tid}, &a.member); break;
            case Op::OffsetCommit: {
                // A commit needs a live member: join first (a denied join leaves the commit to answer on its own).
                std::string member = "m";
                int32_t generation = 1;
                if (const auto j = r_.c().joinGroup(r_.ch(), r_.stream(), sid, g(), {}, {tid}); j && j->errorCode == 0) {
                    member = j->memberId; generation = j->generation;
                }
                const auto code = r_.c().offsetCommit(r_.ch(), r_.stream(), sid, g(), member, generation, tid, 0, 1);
                a.code = code ? *code : kNoAnswer;
                break;
            }
            case Op::OffsetFetch: a.code = r_.offsetFetch(sid, g(), tid); break;
            case Op::ViewRead: case Op::ViewOpen: {
                a.corr = r_.c().sendGetViewRow(r_.ch(), r_.stream(), sid, vid, 1);
                const auto ans = r_.c().awaitViewRowOrError(a.corr, AuthRig::dl());
                if (!ans) break;
                a.viewStatus = ans->isError ? ans->errorCode : ans->status;
                // 0 OK / 1 AbsentKey / 2 NotReady: authorized and answered (the view is empty); the rest as is.
                a.code = a.viewStatus >= 0 && a.viewStatus <= 2 ? 0 : a.viewStatus;
                break;
            }
            case Op::Metadata: {
                const auto ids = r_.metadataTopics(sid);
                a.code = 0;
                a.present = !missing && std::find(ids.begin(), ids.end(), tid_) != ids.end();
                break;
            }
            case Op::CreateTopic: a.corr = r_.c().sendDefineTopic(r_.ch(), r_.stream(), sid, newTopic(), 1, authRigCharBlob()); a.code = ddl(a); break;
            case Op::CreateView: a.corr = r_.c().sendCreateView(r_.ch(), r_.stream(), sid, newView(), "id", 0, authRigViewPlan(newView(), {tid_})); a.code = ddl(a); break;
            case Op::CreateSchema: a.corr = r_.c().sendDefineTopic(r_.ch(), r_.stream(), sid, u(), 1, authRigCharBlob()); a.code = ddl(a); break;
            case Op::AlterSchema: {
                rune::codec::AlterOp o{};
                o.type = rune::codec::AlterOpType::Add; o.name = "extra"; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
                a.corr = r_.c().sendAlterTopic(r_.ch(), r_.stream(), sid, t(), 0, rune::codec::encodeAlterOps({o}));
                a.code = ddl(a);
                break;
            }
            case Op::SetCompatibility: {
                rune::codec::AlterOp o{};
                o.type = rune::codec::AlterOpType::SetCompatibility; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
                o.mode = rune::codec::CompatMode::Forward;
                a.corr = r_.c().sendAlterTopic(r_.ch(), r_.stream(), sid, t(), 0, rune::codec::encodeAlterOps({o}));
                a.code = ddl(a);
                break;
            }
            case Op::AlterTopicConfig: {
                const pc::ConfigChangeWire cc{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 600'000};
                a.corr = r_.c().sendAlterTopicConfig(r_.ch(), r_.stream(), sid, t(), 0, {cc});
                a.code = ddl(a);
                break;
            }
            case Op::AlterView: a.corr = r_.c().sendAlterView(r_.ch(), r_.stream(), sid, v(), "id", 0, 0, authRigViewPlan(v(), {tid_})); a.code = ddl(a); break;
            case Op::DropTopic: a.corr = r_.c().sendDropTopic(r_.ch(), r_.stream(), sid, t()); a.code = ddl(a); break;
            case Op::DropView: a.corr = r_.c().sendDropView(r_.ch(), r_.stream(), sid, v()); a.code = ddl(a); break;
            case Op::SecurityDdl: {
                const auto ans = securityWire().createRole(r_.ch(), sid, "q" + std::to_string(ord_) + "_" + std::to_string(++ddlSeq_));
                a.code = ans.errorCode == 0xFFFF ? kNoAnswer : ans.errorCode;
                break;
            }
            case Op::kCount: break;
        }
        return a;
    }

    /// Assertion 3 (D22 / D23): the frame of a NotAuthorized answer equals the frame a session without any grant gets for
    /// the same request against the EXISTING object.  Call at the end of the cell.
    bool hiddenTwinMatches(Op op, bool missing, const Answer& denied) {
        if (denied.frame.empty()) return true;   // no frame captured for this op (txn / group / offset / metadata)
        if (missing || tid_ < 0) createObjects(op);   // the existing object the twin must not learn about
        if (!twinCreated_) {
            const std::string n = "h" + std::to_string(1000 + ord_);
            const auto a = r_.broker().admin().createUser(r_.ch(), r_.broker().adminSession(), n, AuthRig::kPw);
            if (a.errorCode != 0) return false;
            twinSid_ = r_.login(n);
            twinCreated_ = true;
        }
        const Answer twin = issueAs(twinSid_, op, /*missing=*/false);
        return twin.code == 45 && twin.frame == denied.frame;
    }

private:
    struct TxnIds { int code = kNoAnswer; int64_t pid = 1; int16_t epoch = 0; };
    static std::vector<uint8_t> stripHeader(std::vector<uint8_t> f) {
        constexpr std::size_t kHeader = pc::MessageHeader::ENCODED_SIZE;
        if (f.size() <= kHeader) return {};
        f.erase(f.begin(), f.begin() + kHeader);
        return f;
    }
    AuthWireClient& securityWire() { return r_.wire(); }
    std::size_t recordsOf(int64_t subId) {
        std::size_t n = 0;
        for (const auto& p : r_.c().records()) n += p.subscriptionId == subId ? 1 : 0;
        return n;
    }
    void captureFrame(Answer& a) {
        if (a.corr == 0) return;
        if (const auto raw = r_.c().awaitRaw(a.corr, AuthRig::dl(2))) a.frame = stripHeader(*raw);
    }
    int ddl(Answer& a) {
        const int code = r_.ddlCode(a.corr);
        captureFrame(a);
        return code;
    }
    int subscribeCode(Answer& a) {
        const auto code = r_.c().awaitSubscribeCode(a.corr, AuthRig::dl());
        if (!code) return kNoAnswer;
        if (*code == 0) a.subId = r_.c().subscriptionIdOf(a.corr).value_or(0);
        else captureFrame(a);
        return *code;
    }
    /// A produce on `channel`, retried while leadership / placement settles (NotLeader, UnknownTopic from the worker
    /// that has not heard of the topic yet); NotAuthorized and OK are final.  Bounded: 200 x 25 ms.
    int produceVia(const std::string& channel, int64_t sid, int32_t topicId, Answer& a) {
        int code = kNoAnswer;
        for (int attempt = 0; attempt < 200; ++attempt) {
            a.corr = r_.c().nextCorrelationId();
            if (!r_.c().produce(channel, r_.stream(), sid, a.corr, topicId, 0, 1, makeBatch(1))) return kNoAnswer;
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
        return r_.rawAnswer(m.correlationId);
    }
    static std::span<const uint8_t> bytes(const std::string& s) { return {reinterpret_cast<const uint8_t*>(s.data()), s.size()}; }
    TxnIds txnInit(int64_t sid, const std::string& txnId) {
        pc::InitProducerId m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.transactionTimeoutMs = 30'000;
        m.transactionalId = bytes(txnId);
        const auto a = sendRaw(m, 256);
        TxnIds out;
        if (a.empty()) return out;
        if (const auto h = pc::MessageHeader::decode(a.data(), a.size());
            h && h->templateId == static_cast<uint16_t>(pc::TemplateId::InitProducerIdResponse)) {
            if (const auto r = pc::InitProducerIdResponse::decode(a.data(), a.size())) {
                out.code = r->errorCode; out.pid = r->producerId; out.epoch = r->producerEpoch;
            }
        } else if (const auto e = pc::ErrorResponse::decode(a.data(), a.size())) out.code = e->code;
        return out;
    }
    int txnAddPartitions(int64_t sid, const TxnIds& ids, int32_t topicId, Answer& a) {
        pc::AddPartitionsToTxn m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.producerId = ids.pid; m.producerEpoch = ids.epoch;
        const std::string txn = x();
        m.transactionalId = bytes(txn);
        const pc::TxnTopicPartition tp{topicId, 0};
        const auto f = sendRaw(m, 256, std::span<const pc::TxnTopicPartition>(&tp, 1));
        if (f.empty()) return kNoAnswer;
        // (the response echoes the requested topic id, so its frame is not compared between a real and a missing id)
        if (const auto r = pc::AddPartitionsToTxnResponse::decode(f.data(), f.size())) return r->results.empty() ? 0 : r->results[0].errorCode;
        if (const auto e = pc::ErrorResponse::decode(f.data(), f.size())) return e->code;
        return kNoAnswer;
    }
    int txnEnd(int64_t sid, const TxnIds& ids, Answer& a) {
        pc::EndTxn m;
        m.sessionId = sid; m.correlationId = r_.c().nextCorrelationId(); m.producerId = ids.pid; m.producerEpoch = ids.epoch; m.commit = 1;
        const std::string txn = x();
        m.transactionalId = bytes(txn);
        const auto f = sendRaw(m, 256);
        if (f.empty()) return kNoAnswer;
        a.frame = stripHeader(f);
        if (const auto r = pc::EndTxnResponse::decode(f.data(), f.size())) return r->errorCode;
        if (const auto e = pc::ErrorResponse::decode(f.data(), f.size())) return e->code;
        return kNoAnswer;
    }

    AuthMatrixRig& m_;
    AuthRig& r_;
    int ord_;
    std::string prefix_, user_, role_;
    int64_t sid_ = 0, twinSid_ = 0;
    int32_t tid_ = -1, vid_ = -1, uid_ = -1;
    bool userCreated_ = false, twinCreated_ = false;
    int ddlSeq_ = 0;
};

inline AuthMatrixRig::Cell AuthMatrixRig::newCell() { return Cell(*this, ++ordinal_); }

} // namespace rune::node::test
