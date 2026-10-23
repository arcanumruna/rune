#pragma once
// AuthRig.h - one real authenticating BrokerNode plus the helpers the enforcement tests share (authentication Task 12;
// used by Tasks 12-19): users with exact grants, sessions that log in over raw frames, and one function per request
// type returning THE ANSWER CODE the broker gave (0 = the request's normal answer, 45 = NotAuthorized).
//
//   newUser(grants)      - a fresh user "uN" holding exactly those grants, logged in; returns its session id
//   builder              - a user holding CREATE/ALTER/DROP/DESCRIBE ON CLUSTER + READ TOPIC prefix '' (it makes the
//                          objects the tests ask about: topics "orders" / "secret", the view "board" over "orders")
//   <request>(sid, ...)  - send one request as that session, wait (bounded) for its answer, return its code
//
// Every wait is bounded; a missing answer is code kNoAnswer, never a hang.

#include "support/AuthBroker.h"

#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/view/ViewPlan.h>

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace rune::node::test {

constexpr int kNoAnswer = -1000;
constexpr int kNotAuthorized = 45;

inline std::vector<uint8_t> authRigCharBlob() {
    return rune::codec::serialize(rune::codec::DescriptorBuilder("character", 1)
        .add("id", rune::codec::PrimType::Int64, 1, /*isKey=*/true)
        .add("hp", rune::codec::PrimType::Int64)
        .build());
}
inline std::vector<uint8_t> authRigViewPlan(const std::string& viewName, const std::vector<int32_t>& sources) {
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = viewName;
    p.key = "id";
    p.sizing.chunkBytes = 64u << 10;   // not the 32 MiB row-slab default: a rig with hundreds of views must stay small
    for (const int32_t id : sources) p.sources.push_back(rv::SourceSpec{id, "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
    return rv::serializeAuthored(p);
}

/// The rig's topics roll at 1 MiB, not the 64 MiB default: the log preallocates (fallocate) its active segment to
/// segmentRollBytes, ~67.7 MB per topic, ~35 GB for the 720-cell matrix in one process (#571).
inline constexpr int64_t kRigSegmentRollBytes = 1LL << 20;
inline rune::protocol::TopicConfigView authRigTopicConfig() {
    rune::protocol::TopicConfigView c;
    c.segmentRollBytes = kRigSegmentRollBytes;
    return c;
}

namespace grants {
using SO = rune::protocol::SecurityOperation;
using ST = rune::protocol::SecurityResourceType;
using SP = rune::protocol::SecurityPatternKind;
inline AuthGrant exact(SO op, ST t, std::string name) { return AuthGrant{op, t, SP::Exact, std::move(name)}; }
inline AuthGrant prefix(SO op, ST t, std::string p) { return AuthGrant{op, t, SP::Prefix, std::move(p)}; }
inline AuthGrant cluster(SO op) { return AuthGrant{op, ST::Cluster, SP::Exact, ""}; }
}  // namespace grants

class AuthRig {
public:
    using Clock = std::chrono::steady_clock;
    static constexpr const char* kPw = "user-password-12345";

    explicit AuthRig(const std::string& tag = "enf", AuthBroker::Options options = {}) : b_(tag, std::move(options)) {
        b_.bootstrapAdmin();
        w_ = &b_.wire();
        c().captureRawAnswers(true);
        using namespace grants;
        b_.createUser("builder", kPw, {cluster(SO::Create), cluster(SO::Alter), cluster(SO::Drop), cluster(SO::Describe),
                                       prefix(SO::Read, ST::Topic, "")});
        builder = login("builder");
        orders = defineTopic(builder, "orders");
        secret = defineTopic(builder, "secret");
        board = createView(builder, "board", orders);
        EXPECT_GE(orders, 0);
        EXPECT_GE(secret, 0);
        EXPECT_GE(board, 0);
    }

    AuthBroker& broker() noexcept { return b_; }
    AuthWireClient& wire() noexcept { return *w_; }
    ShardTestClient& c() noexcept { return w_->raw(); }
    const std::string& ch() const noexcept { return b_.channel(); }
    static constexpr int32_t stream() noexcept { return AuthWireClient::kClientStream; }
    static Clock::time_point dl(int seconds = 10) { return Clock::now() + std::chrono::seconds(seconds); }

    int64_t builder = 0;
    int32_t orders = -1, secret = -1, board = -1;

    /// Restart the broker over the same data dir; the rig's wire and the builder session are re-established.  Sessions
    /// obtained before (newUser) are dead: log the users in again with login(name).
    void restart() {
        b_.restart();
        w_ = &b_.wire();
        c().captureRawAnswers(true);
        builder = login("builder");
    }

    // ── users and sessions ──────────────────────────────────────────────────────────────────────────────────
    int64_t login(const std::string& user, const std::string& pw = kPw) {
        const int64_t sid = w_->connect(ch());
        if (sid <= 0) throw std::runtime_error("AuthRig: connect failed");
        const auto l = w_->login(ch(), sid, user, {pw, pw});
        if (!l.ok()) throw std::runtime_error("AuthRig: login failed for " + user);
        b_.noteLoginOk();
        return sid;
    }
    /// Issue #138: the broker's gateway no longer holds session `sid` within `within` (a bounded poll of the control
    /// thread's session table).  A ping cannot tell this: the gateway answers nothing to a dead session's ping, and a
    /// timed-out ping looks the same.
    bool awaitSessionGone(int64_t sid, std::chrono::milliseconds within = std::chrono::seconds(5)) {
        const auto deadline = Clock::now() + within;
        for (;;) {
            if (!b_.node().gatewayHasSessionForTest(sid)) return true;
            if (Clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    /// A new user with exactly `g`; returns its logged-in session.  `name` (optional) receives the user name.
    int64_t newUser(const std::vector<AuthGrant>& g, std::string* name = nullptr) {
        const std::string n = "u" + std::to_string(++seq_);
        b_.createUser(n, kPw, g);
        if (name) *name = n;
        return login(n);
    }

    // ── DDL as the builder (precondition setup) ─────────────────────────────────────────────────────────────
    int32_t defineTopic(int64_t sid, const std::string& name, bool typed = true) {
        const auto r = c().awaitDefine(c().sendDefineTopic(ch(), stream(), sid, name, 1,
                                                           typed ? authRigCharBlob() : std::vector<uint8_t>{},
                                                           0, {}, 0, authRigTopicConfig()), dl(20));
        return r && r->errorCode == 0 ? r->topicId : -1;
    }
    int32_t createView(int64_t sid, const std::string& name, int32_t source) {
        const auto r = c().awaitCreateView(c().sendCreateView(ch(), stream(), sid, name, "id", 0, authRigViewPlan(name, {source})), dl(20));
        return r && r->errorCode == 0 ? r->viewTopicId : -1;
    }

    // ── one function per request type: the answer code ──────────────────────────────────────────────────────
    int produce(int64_t sid, int32_t topicId, bool untilOk = false) {
        if (untilOk) return c().produceUntilOk(ch(), stream(), sid, topicId, 0, 1, makeBatch(1)) ? 0 : kNoAnswer;
        const int64_t corr = c().nextCorrelationId();
        if (!c().produce(ch(), stream(), sid, corr, topicId, 0, 1, makeBatch(1))) return kNoAnswer;
        const auto a = c().awaitProduceResponseUntil(corr, dl());
        return a ? a->errorCode : kNoAnswer;
    }
    int subscribe(int64_t sid, int32_t topicId) {
        const auto code = c().awaitSubscribeCode(c().sendSubscribe(ch(), stream(), sid, topicId, 0), dl());
        return code ? *code : kNoAnswer;
    }
    int catalogSubscribe(int64_t sid) {
        const auto code = c().awaitSubscribeCode(c().sendCatalogSubscribe(ch(), stream(), sid, 0), dl());
        return code ? *code : kNoAnswer;
    }
    int joinGroup(int64_t sid, const std::string& group, std::vector<int32_t> topics, std::string* member = nullptr) {
        const auto a = c().joinGroup(ch(), stream(), sid, group, {}, std::move(topics));
        if (a && member) *member = a->memberId;
        return a ? a->errorCode : kNoAnswer;
    }
    int heartbeat(int64_t sid, const std::string& group, const std::string& member) {
        const auto a = c().heartbeat(ch(), stream(), sid, group, member, 1);
        return a ? *a : kNoAnswer;
    }
    int offsetCommit(int64_t sid, const std::string& group, int32_t topicId) {
        const auto a = c().offsetCommit(ch(), stream(), sid, group, "m", 1, topicId, 0, 1);
        return a ? *a : kNoAnswer;
    }
    int offsetFetch(int64_t sid, const std::string& group, int32_t topicId) {
        const auto a = c().offsetFetch(ch(), stream(), sid, group, topicId, 0);
        return a ? a->second : kNoAnswer;
    }
    int leaveGroup(int64_t sid, const std::string& group, const std::string& member = "nobody") {
        pc::LeaveGroup m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId();
        m.groupId = bytes(group); m.memberId = bytes(member);   // `member` outlives the encode: the message holds views
        return rawCode(sendRaw(m, 512, m.correlationId));
    }
    // ── member-scoped group requests (issues #123 / #124) ──────────────────────────────────────────────────
    /// Join as a new member and re-join once: a lone member reaches Stable.  Returns (member, generation).
    std::pair<std::string, int32_t> joinStable(int64_t sid, const std::string& group, std::vector<int32_t> topics) {
        const auto a = c().joinGroup(ch(), stream(), sid, group, {}, topics);
        if (!a || a->errorCode != 0) return {{}, -1};
        const auto b = c().joinGroup(ch(), stream(), sid, group, a->memberId, topics);
        if (!b || b->errorCode != 0) return {{}, -1};
        return {b->memberId, b->generation};
    }
    /// JoinGroup naming `member`: (errorCode, the member id answered).
    std::pair<int, std::string> rejoin(int64_t sid, const std::string& group, const std::string& member,
                                       std::vector<int32_t> topics) {
        const auto a = c().joinGroup(ch(), stream(), sid, group, member, std::move(topics));
        return a ? std::pair<int, std::string>{a->errorCode, a->memberId} : std::pair<int, std::string>{kNoAnswer, {}};
    }
    int heartbeatAt(int64_t sid, const std::string& group, const std::string& member, int32_t gen) {
        const auto a = c().heartbeat(ch(), stream(), sid, group, member, gen);
        return a ? *a : kNoAnswer;
    }
    int syncGroup(int64_t sid, const std::string& group, const std::string& member, int32_t gen) {
        pc::SyncGroup m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.generationId = gen;
        m.groupId = bytes(group); m.memberId = bytes(member);
        return rawCode(sendRaw(m, 512, m.correlationId));
    }
    int commitAs(int64_t sid, const std::string& group, const std::string& member, int32_t gen, int32_t topicId,
                 int64_t offset) {
        const auto a = c().offsetCommit(ch(), stream(), sid, group, member, gen, topicId, 0, offset);
        return a ? *a : kNoAnswer;
    }
    /// The committed offset of (topicId, 0) (or kNoAnswer).
    int64_t committedOffset(int64_t sid, const std::string& group, int32_t topicId) {
        const auto a = c().offsetFetch(ch(), stream(), sid, group, topicId, 0);
        return a && a->second == 0 ? a->first : kNoAnswer;
    }
    int getTopicSchema(int64_t sid, const std::string& name) {
        const auto a = c().awaitSchema(c().sendGetTopicSchema(ch(), stream(), sid, name), dl());
        return a ? a->errorCode : kNoAnswer;
    }
    /// ViewRowResponse status (45 = NotAuthorized), or the code of an ErrorResponse.
    int getViewRow(int64_t sid, int32_t viewId) {
        const auto a = c().awaitViewRowOrError(c().sendGetViewRow(ch(), stream(), sid, viewId, 1), dl());
        return !a ? kNoAnswer : a->isError ? a->errorCode : a->status;
    }
    int ddlCode(int64_t corr) {
        const auto a = c().awaitAnyDdlAnswer(corr, dl(15));
        return a ? a->code : kNoAnswer;
    }
    int defineTopicCode(int64_t sid, const std::string& name) {
        return ddlCode(c().sendDefineTopic(ch(), stream(), sid, name, 1, authRigCharBlob(), 0, {}, 0,
                                              authRigTopicConfig()));
    }
    int createViewCode(int64_t sid, const std::string& name, int32_t source) {
        return ddlCode(c().sendCreateView(ch(), stream(), sid, name, "id", 0, authRigViewPlan(name, {source})));
    }
    int alterViewCode(int64_t sid, const std::string& name, int32_t source) {
        return ddlCode(c().sendAlterView(ch(), stream(), sid, name, "id", 0, 0, authRigViewPlan(name, {source})));
    }
    int alterTopicCode(int64_t sid, const std::string& name) {
        rune::codec::AlterOp o{};
        o.type = rune::codec::AlterOpType::Add; o.name = "extra"; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
        return ddlCode(c().sendAlterTopic(ch(), stream(), sid, name, 0, rune::codec::encodeAlterOps({o})));
    }
    int alterConfigCode(int64_t sid, const std::string& name) {
        const pc::ConfigChangeWire cc{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 600'000};
        return ddlCode(c().sendAlterTopicConfig(ch(), stream(), sid, name, 0, {cc}));
    }
    int dropTopicCode(int64_t sid, const std::string& name) { return ddlCode(c().sendDropTopic(ch(), stream(), sid, name)); }
    int dropViewCode(int64_t sid, const std::string& name) { return ddlCode(c().sendDropView(ch(), stream(), sid, name)); }
    std::string ddlDetail(int64_t corr) {
        const auto a = c().awaitAnyDdlAnswer(corr, dl(15));
        return a ? a->message : std::string{};
    }

    // ── transactions (raw frames; the answer decoded by its own response type) ─────────────────────────────
    int initProducerId(int64_t sid, const std::string& txnId) {
        pc::InitProducerId m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.transactionTimeoutMs = 30'000;
        m.transactionalId = bytes(txnId);
        return rawCode(sendRaw(m, 256, m.correlationId));
    }
    int addPartitions(int64_t sid, const std::string& txnId, int32_t topicId, int64_t pid = 1, int16_t epoch = 0) {
        pc::AddPartitionsToTxn m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.producerId = pid; m.producerEpoch = epoch;
        m.transactionalId = bytes(txnId);
        const pc::TxnTopicPartition tp{topicId, 0};
        return rawCode(sendRaw(m, 256, m.correlationId, std::span<const pc::TxnTopicPartition>(&tp, 1)));
    }
    int addOffsets(int64_t sid, const std::string& txnId, const std::string& group, int64_t pid = 1, int16_t epoch = 0) {
        pc::AddOffsetsToTxn m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.producerId = pid; m.producerEpoch = epoch;
        m.transactionalId = bytes(txnId); m.groupId = bytes(group);
        return rawCode(sendRaw(m, 256, m.correlationId));
    }
    int txnOffsetCommit(int64_t sid, const std::string& txnId, const std::string& group, int32_t topicId, int64_t pid = 1,
                        int16_t epoch = 0) {
        pc::TxnOffsetCommit m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.producerId = pid; m.producerEpoch = epoch;
        m.transactionalId = bytes(txnId); m.groupId = bytes(group);
        pc::TxnOffsetCommitEntry e;
        e.topicId = topicId; e.partition = 0; e.offset = 1; e.metadata = {};
        return rawCode(sendRaw(m, 512, m.correlationId, std::span<const pc::TxnOffsetCommitEntry>(&e, 1)));
    }
    int endTxn(int64_t sid, const std::string& txnId, int64_t pid = 1, int16_t epoch = 0, bool commit = true) {
        pc::EndTxn m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.producerId = pid; m.producerEpoch = epoch;
        m.commit = commit ? 1 : 0;
        m.transactionalId = bytes(txnId);
        return rawCode(sendRaw(m, 256, m.correlationId));
    }

    // ── producer identity (issue #122) ─────────────────────────────────────────────────────────────────────
    struct ProducerIds { int code = kNoAnswer; int64_t pid = -1; int16_t epoch = -1; };
    ProducerIds initProducer(int64_t sid, const std::string& txnId = "") {
        pc::InitProducerId m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.transactionTimeoutMs = 30'000;
        m.transactionalId = bytes(txnId);
        const auto a = sendRaw(m, 256, m.correlationId);
        const auto r = a.empty() ? std::nullopt : pc::InitProducerIdResponse::decode(a.data(), a.size());
        return r ? ProducerIds{r->errorCode, r->producerId, r->producerEpoch} : ProducerIds{};
    }
    struct ProduceAnswer { int code = kNoAnswer; int64_t baseOffset = -1; };
    /// An idempotent (or, `transactional`, transactional) produce of one record as (pid, epoch, seq) - the frame and
    /// the batch header carry the same identity.  Retried while the partition's producer state is being built
    /// (NotLeader); every other answer is final.  Bounded.
    ProduceAnswer produceAs(int64_t sid, int32_t topicId, int64_t pid, int16_t epoch, int32_t seq,
                            bool transactional = false, const std::string& value = "v", const std::string* channel = nullptr) {
        rune::protocol::RecordBatchBuilder b(0, 1000);
        b.setProducerId(pid); b.setProducerEpoch(epoch); b.setBaseSequence(seq); b.setTransactional(transactional);
        b.append("k", value, 1000);
        const std::vector<uint8_t> batch = b.build();
        ProduceAnswer out;
        for (int attempt = 0; attempt < 200; ++attempt) {
            const int64_t corr = c().nextCorrelationId();
            if (!c().produceIdempotent(channel ? *channel : ch(), stream(), sid, corr, topicId, 0, 1, pid, epoch, seq, batch))
                return out;
            const auto a = c().awaitProduceResponseUntil(corr, dl());
            out = a ? ProduceAnswer{a->errorCode, a->baseOffset} : ProduceAnswer{};
            if (out.code != static_cast<int>(pc::ErrorCode::NotLeader) && out.code != static_cast<int>(pc::ErrorCode::UnknownTopic))
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return out;
    }

    /// The topic ids a MetadataRequest{all} lists for the session.
    std::vector<int32_t> metadataTopics(int64_t sid) {
        std::vector<int32_t> ids;
        for (const PartitionInfo& p : c().metadata(ch(), stream(), sid))
            if (std::find(ids.begin(), ids.end(), p.topicId) == ids.end()) ids.push_back(p.topicId);
        return ids;
    }
    /// Metadata until `want` is listed for the builder (the placement is committed), bounded.
    bool awaitListed(int32_t want) {
        for (int i = 0; i < 400; ++i) {
            const auto t = metadataTopics(builder);
            if (std::find(t.begin(), t.end(), want) != t.end()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return false;
    }

    /// PR #466 review: TxnOffsetCommit of several entries (topic ids, partition 0, offset 1), optionally with the
    /// version-10 member block (`generation` >= 0).  Returns the FIRST NON-OK entry code (0 = all OK).
    int txnOffsetCommitMany(int64_t sid, const std::string& txnId, const std::string& group,
                            const std::vector<int32_t>& topicIds, int64_t pid = 1, int16_t epoch = 0,
                            int32_t generation = -1, const std::string& member = {}) {
        pc::TxnOffsetCommit m;
        m.sessionId = sid; m.correlationId = c().nextCorrelationId(); m.producerId = pid; m.producerEpoch = epoch;
        m.transactionalId = bytes(txnId); m.groupId = bytes(group);
        m.generationId = generation; m.memberId = bytes(member);
        std::vector<pc::TxnOffsetCommitEntry> es;
        for (const int32_t id : topicIds) es.push_back(pc::TxnOffsetCommitEntry{id, 0, 1, {}});
        const auto a = sendRaw(m, 1024, m.correlationId, std::span<const pc::TxnOffsetCommitEntry>(es));
        if (const auto r = a.empty() ? std::nullopt : pc::TxnOffsetCommitResponse::decode(a.data(), a.size())) {
            for (const auto& e : r->results) if (e.errorCode != 0) return e.errorCode;
            return 0;
        }
        return rawCode(a);
    }

    /// The raw answer frame of `corr` (bounded wait); empty = none.
    std::vector<uint8_t> rawAnswer(int64_t corr) {
        const auto a = c().awaitRaw(corr, dl());
        return a ? *a : std::vector<uint8_t>{};
    }

private:
    static std::span<const uint8_t> bytes(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }
    static std::span<const uint8_t> bytes(std::string&&) = delete;   // the view would dangle past the full expression
    /// Encode `m` (with optional trailing args) into a scratch buffer, send it, return the answer bytes.
    template <class Msg, class... Rest>
    std::vector<uint8_t> sendRaw(const Msg& m, std::size_t cap, int64_t corr, Rest&&... rest) {
        std::vector<uint8_t> buf(cap);
        const std::size_t len = m.encode(buf.data(), std::forward<Rest>(rest)...);
        if (len == 0 || !c().send(ch(), stream(), buf.data(), len)) return {};
        return rawAnswer(corr);
    }
    /// The error code of a raw answer frame, by its response type.
    static int rawCode(const std::vector<uint8_t>& a) {
        if (a.empty()) return kNoAnswer;
        const auto hdr = pc::MessageHeader::decode(a.data(), a.size());
        if (!hdr) return kNoAnswer;
        const uint8_t* p = a.data(); const std::size_t n = a.size();
        switch (static_cast<pc::TemplateId>(hdr->templateId)) {
            case pc::TemplateId::InitProducerIdResponse: { auto r = pc::InitProducerIdResponse::decode(p, n); return r ? r->errorCode : kNoAnswer; }
            case pc::TemplateId::AddPartitionsToTxnResponse: { auto r = pc::AddPartitionsToTxnResponse::decode(p, n); return r ? (r->results.empty() ? 0 : r->results[0].errorCode) : kNoAnswer; }
            case pc::TemplateId::AddOffsetsToTxnResponse: { auto r = pc::AddOffsetsToTxnResponse::decode(p, n); return r ? r->errorCode : kNoAnswer; }
            case pc::TemplateId::TxnOffsetCommitResponse: { auto r = pc::TxnOffsetCommitResponse::decode(p, n); return r ? (r->results.empty() ? 0 : r->results[0].errorCode) : kNoAnswer; }
            case pc::TemplateId::EndTxnResponse: { auto r = pc::EndTxnResponse::decode(p, n); return r ? r->errorCode : kNoAnswer; }
            case pc::TemplateId::LeaveGroupResponse: { auto r = pc::LeaveGroupResponse::decode(p, n); return r ? r->errorCode : kNoAnswer; }
            case pc::TemplateId::SyncGroupResponse: { auto r = pc::SyncGroupResponse::decode(p, n); return r ? r->errorCode : kNoAnswer; }
            case pc::TemplateId::ErrorResponse: { auto r = pc::ErrorResponse::decode(p, n); return r ? r->code : kNoAnswer; }
            default: return kNoAnswer;
        }
    }

    AuthBroker b_;
    AuthWireClient* w_ = nullptr;
    int seq_ = 0;
};

}  // namespace rune::node::test
