#pragma once
// NamespaceLifecycleRig.h - the rig behind the namespace LIFECYCLE matrix (NamespaceLifecycleMatrixExpectations.h):
// ONE real authenticating broker on the default config.  Every cell prepares its OWN target namespace (`c<ord>`) in the
// requested state, logs the caller in (or records why it could not), sends the lifecycle op as raw frames and observes
// the target by name through the cluster admin's ListNamespaces.
//
//   run(op, caller, target, ord)     - one sequential cell
//   runTwin(op, caller, target, ord) - the same op as the same caller on a never-created name (`z<ord>`)
//
// The reaper of the `__catalog` leader is HELD before its first event (NamespaceReaper::setIssueBudgetForTest(0), set on
// the admin thread), so a namespace the rig deletes stays DELETING: the "Deleting" target of the matrix.  Plan note: the
// plan holds it at DroppingTopics; no public observation tells the two apart (a DELETING name answers as a missing one),
// and holding it at its first step keeps every older DELETING namespace from consuming a later cell's budget.
//
// Every wait is bounded (AuthRig::dl); no sleeps.

#include "support/AuthRig.h"
#include "support/NamespaceLifecycleMatrixExpectations.h"
#include "support/NamespaceRig.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/node/BrokerNode.h>
#include <rune/node/Worker.h>
#include <rune/security/SecurityQueries.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace rune::node::test {

class LcRig {
public:
    using Op = nslc::LcOp;
    using Caller = nslc::LcCaller;
    using Target = nslc::LcTarget;
    using Answer = NamespaceRig::Answer;
    using RequestId = rune::protocol::RequestId;

    struct Seen {
        std::string name;
        uint32_t nsId = 0;
        uint8_t state = 0;   ///< NsStateTag
        rune::protocol::NamespaceLimits limits;
    };

    /// What one cell observed.
    struct Cell {
        Answer a;                       ///< the op's answer (or the login refusal's code)
        bool loginRefused = false;      ///< the caller could not log in; the op was never sent
        Target after = Target::Missing;       ///< the target by name, from the cluster admin's listing
        bool deletingEntry = false;     ///< a DELETING entry of that name is listed
        uint32_t nsIdBefore = 0, nsIdAfter = 0;
        bool topicsRemain = true;       ///< LowerBelowUsage: both topics still exist
        int followUpCode = kNoAnswer;   ///< LowerBelowUsage: a CreateTopic after the op (root; resumed first)
        std::string warning;            ///< OkWithWarning: the answer's detail
        rune::protocol::NamespaceLimits limitsBefore;  ///< the target's stored limits before the op (by name)
        rune::protocol::NamespaceLimits limitsAfter;   ///< the target's stored limits after the op (by name)
    };

    explicit LcRig(const std::string& tag, bool anonymous = false, bool holdReaper = true)
        : w_(std::make_unique<World>(tag, optionsFor(anonymous))), anonymous_(anonymous) {
        setup(holdReaper);
    }

    // ── primitives ────────────────────────────────────────────────────────────────────────────────────────────────
    AuthRig& auth() noexcept { return w_->r; }
    AuthBroker& broker() noexcept { return w_->r.broker(); }
    int64_t rootSid() { return root(); }
    static std::string nameOf(Target t, int ord, bool twin) {
        char buf[16];
        std::snprintf(buf, sizeof buf, "%c%04d", twin ? 'z' : t == Target::Missing ? 'm' : 'c', ord % 10000);
        return t == Target::Default && !twin ? "default" : buf;
    }

    /// Root's full listing (paged).
    std::vector<Seen> list() {
        std::vector<Seen> out;
        uint32_t after = 0;
        for (int page = 0; page < 200; ++page) {
            pc::ListNamespaces m;
            m.sessionId = root(); m.correlationId = w_->r.c().nextCorrelationId(); m.afterNsId = after; m.max = 100;
            std::vector<uint8_t> buf(m.encodedSize());
            buf.resize(m.encode(buf.data()));
            need(w_->r.c().send(w_->r.ch(), AuthRig::stream(), buf.data(), buf.size()), "send ListNamespaces");
            const auto raw = w_->r.rawAnswer(m.correlationId);
            const auto l = pc::ListNamespacesResponse::decode(raw.data(), raw.size());
            need(l.has_value(), "ListNamespaces answer");
            for (const auto& i : l->namespaces) out.push_back({i.name, i.nsId, i.state, i.limits});
            if (l->nextAfter == 0) return out;
            after = l->nextAfter;
        }
        need(false, "ListNamespaces paging cap");
        return out;
    }
    static Target targetOf(const std::string& name, uint8_t tag) {
        if (tag == 3) return Target::Deleting;
        if (name == "default") return tag == 0 ? Target::Default : tag == 1 ? Target::SuspendedRO : Target::SuspendedRA;
        return tag == 0 ? Target::Active : tag == 1 ? Target::SuspendedRO : Target::SuspendedRA;
    }
    /// The target by NAME (a DELETING entry has no name index: Missing here), the id, and whether a DELETING entry exists.
    struct Observed { Target byName = Target::Missing; uint32_t nsId = 0; bool deleting = false; rune::protocol::NamespaceLimits limits; };
    Observed observe(const std::string& name) {
        Observed o;
        for (const Seen& s : list()) {
            if (s.name != name) continue;
            if (s.state == 3) { o.deleting = true; continue; }
            o.byName = targetOf(name, s.state);
            o.nsId = s.nsId;
            o.limits = s.limits;
        }
        return o;
    }
    uint32_t deletingIdOf(const std::string& name) {
        for (const Seen& s : list()) if (s.name == name && s.state == 3) return s.nsId;
        return 0;
    }

    // ── one namespace DDL ─────────────────────────────────────────────────────────────────────────────────────────
    /// Sends one NamespaceDdl as `sid`; returns its correlation id without awaiting.
    int64_t sendNs(int64_t sid, pc::NamespaceDdlKind kind, const std::string& name, uint8_t mode = 1,
                   RequestId rid = {}, int field = -1, uint64_t value = 0) {
        pc::NamespaceDdlRequest m;
        m.sessionId = sid;
        m.correlationId = w_->r.c().nextCorrelationId();
        m.ext.clientName = "ns-lifecycle";
        m.ext.requestId = rid.none() ? RequestId{0x1FEC, ++w_->rid} : rid;
        m.kind = kind;
        m.name = name;
        if (kind == pc::NamespaceDdlKind::Suspend) m.mode = static_cast<pc::NsSuspendMode>(mode);
        if ((kind == pc::NamespaceDdlKind::SetLimits || kind == pc::NamespaceDdlKind::Create) && field >= 0) {
            m.presentMask[static_cast<std::size_t>(field) >> 3] = static_cast<uint8_t>(1u << (field & 7));
            setField(m.limits, field, value);
        }
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        need(w_->r.c().send(w_->r.ch(), AuthRig::stream(), buf.data(), buf.size()), "send namespace DDL");
        return m.correlationId;
    }
    Answer await(int64_t corr) {
        Answer a;
        a.corr = corr;
        const auto any = w_->r.c().awaitAnyDdlAnswer(corr, AuthRig::dl(15));
        a.code = any ? any->code : kNoAnswer;
        if (any) a.detail = any->message;
        if (const auto raw = w_->r.c().awaitRaw(corr, AuthRig::dl(2))) {
            a.body = *raw;
            constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
            if (a.body.size() > h) a.body.erase(a.body.begin(), a.body.begin() + h); else a.body.clear();
        }
        return a;
    }
    Answer ns(int64_t sid, pc::NamespaceDdlKind kind, const std::string& name, uint8_t mode = 1, int field = -1,
              uint64_t value = 0) {
        return await(sendNs(sid, kind, name, mode, {}, field, value));
    }
    static void setField(rune::protocol::NamespaceLimits& l, int field, uint64_t v) {
        uint64_t* p = &l.maxTopics;
        if (field < 28) p[field] = v; else l.featureBits = static_cast<uint32_t>(v);
    }
    static uint64_t getField(const rune::protocol::NamespaceLimits& l, int field) {
        const uint64_t* p = &l.maxTopics;
        return field < 28 ? p[field] : l.featureBits;
    }

    // ── sessions ──────────────────────────────────────────────────────────────────────────────────────────────────
    struct Login { int64_t sid = 0; int code = 0; };
    Login tryLogin(const std::string& user, const std::string& pw = AuthRig::kPw) {
        // A matrix of refused logins is a brute-force curve to the gateway: every attempt starts from a clean throttle
        // (test hygiene, as the auth suites do), so a refusal here is the namespace gate's verdict, never the throttle's.
        w_->r.broker().node().resetLoginThrottleForTest();
        AuthWireClient& wc = w_->r.wire();
        const int64_t sid = wc.connect(w_->r.ch(), pc::kClientVersionNamespaces);
        if (sid <= 0) return {0, kNoAnswer};
        const auto l = wc.login(w_->r.ch(), sid, user, {pw, pw});
        if (!l.ok()) return {0, l.answered() ? static_cast<int>(l.errorCode()) : kNoAnswer};
        w_->r.broker().noteLoginOk();
        return {sid, 0};
    }

    /// The wire request of a lifecycle op (not awaited).  `t` = the target's state (SwitchMode picks the opposite mode).
    int64_t sendOp(Op op, int64_t sid, Target t, const std::string& name, const rune::protocol::NamespaceLimits& cur,
                   RequestId rid = {}) {
        using K = pc::NamespaceDdlKind;
        switch (op) {
            case Op::SuspendRO: return sendNs(sid, K::Suspend, name, 1, rid);
            case Op::SuspendRA: return sendNs(sid, K::Suspend, name, 2, rid);
            case Op::SwitchMode: return sendNs(sid, K::Suspend, name, t == Target::SuspendedRA ? 1 : 2, rid);
            case Op::Resume: return sendNs(sid, K::Resume, name, 1, rid);
            case Op::Delete: return sendNs(sid, K::Delete, name, 1, rid);
            case Op::Recreate: return sendNs(sid, K::Create, name, 1, rid);
            case Op::SetLimitsRaise: return sendNs(sid, K::SetLimits, name, 1, rid, /*maxTopics*/ 0, std::max<uint64_t>(cur.maxTopics, 5000) + 1);
            case Op::SetLimitsLowerBelowUsage: return sendNs(sid, K::SetLimits, name, 1, rid, 0, 1);
            case Op::SetLimitsLowerRetention: return sendNs(sid, K::SetLimits, name, 1, rid, /*maxRetentionMs*/ 9, 1);
        }
        return 0;
    }

    // ── a cell ────────────────────────────────────────────────────────────────────────────────────────────────────
    Cell run(Op op, Caller c, Target t, int ord, bool twin = false) {
        Cell cell;
        const std::string name = nameOf(t, ord, twin);
        if (!twin) holdReaper();   // a DELETING namespace stays DELETING for the whole cell
        if (!twin) prepare(name, t, ord, c);
        const Observed before = observe(name);
        cell.limitsBefore = before.limits;
        cell.nsIdBefore = before.nsId != 0 ? before.nsId : deletingIdOf(name);
        const Login s = login(c, t, name, ord);
        if (s.code != 0) {
            cell.loginRefused = true;
            cell.a.code = s.code;
        } else {
            // Review U3: a name is not recreated until the old incarnation's grants are purged from `__security`
            // (asynchronous after the namespace turns DELETING): wait for that, bounded, so the cell is deterministic.
            if (op == Op::Recreate && t == Target::Deleting && cell.nsIdBefore != 0)
                need(until([&] { return securityPurged(cell.nsIdBefore, name); }), "security purge of " + name);
            cell.a = issue(op, s.sid, t, name, before.limits);
        }
        const Observed after = observe(name);
        cell.after = after.byName;
        cell.deletingEntry = after.deleting;
        cell.nsIdAfter = after.nsId;
        cell.limitsAfter = after.limits;
        if (cell.a.code == 0 && c == Caller::ClusterAdmin && !twin) {
            cell.warning = cell.a.detail;
            if (op == Op::SetLimitsLowerBelowUsage) followUp(cell, name, t);
        }
        if (!twin) cleanup(name, t);
        return cell;
    }

    /// End of a cell: the default namespace back to normal, the cell's own namespace deleted, the reaper released so
    /// every DELETING namespace runs to its end (bounded memory and slots over hundreds of cells).
    void cleanup(const std::string& name, Target t) {
        if (t == Target::Default) restoreDefault();
        else if (observe(name).byName != Target::Missing) (void)ns(root(), pc::NamespaceDdlKind::Delete, name);
        releaseReaper();
    }

    /// CREATE NAMESPACE as root (an Ok answer or a throw).
    void createNs(const std::string& name) {
        need(ns(root(), pc::NamespaceDdlKind::Create, name).code == 0, "CREATE NAMESPACE " + name);
    }

    /// Prepare the target namespace `name` in state `t` (Missing: nothing).  Users only for the callers that need them.
    void prepare(const std::string& name, Target t, int ord, Caller c) {
        if (t == Target::Missing || t == Target::Default) return;
        createNs(name);
        for (const char* tn : {"t", "t2"}) {
            const int32_t id = w_->r.defineTopic(root(), name + "/" + tn);
            need(id >= 0, "topic " + name + "/" + tn);
            w_->tid[name + "/" + tn] = id;
        }
        if (c == Caller::NsAdminSame)
            broker().createUser(name + "/admin", AuthRig::kPw, {grants::exact(grants::SO::Admin, grants::ST::Namespace, name)});
        if (c == Caller::UserInNs)
            broker().createUser(name + "/user", AuthRig::kPw, {grants::prefix(grants::SO::Read, grants::ST::Topic, "")});
        switch (t) {
            case Target::SuspendedRO: need(ns(root(), pc::NamespaceDdlKind::Suspend, name, 1).code == 0, "suspend RO " + name); break;
            case Target::SuspendedRA: need(ns(root(), pc::NamespaceDdlKind::Suspend, name, 2).code == 0, "suspend RA " + name); break;
            case Target::Deleting: need(ns(root(), pc::NamespaceDdlKind::Delete, name).code == 0, "delete " + name); break;
            default: break;
        }
        (void)ord;
    }

    /// The default namespace back to Active with its original limits (a Default cell may have changed both).
    void restoreDefault() {
        need(ns(root(), pc::NamespaceDdlKind::Resume, "default").code == 0, "resume default");
        pc::NamespaceDdlRequest m;
        m.sessionId = root();
        m.correlationId = w_->r.c().nextCorrelationId();
        m.ext.clientName = "ns-lifecycle";
        m.ext.requestId = RequestId{0x1FEC, ++w_->rid};
        m.kind = pc::NamespaceDdlKind::SetLimits;
        m.name = "default";
        for (std::size_t i = 0; i < rune::protocol::kNamespaceLimitFieldCount; ++i) m.presentMask[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        m.limits = w_->defaultLimits;
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        need(w_->r.c().send(w_->r.ch(), AuthRig::stream(), buf.data(), buf.size()), "send restore");
        need(await(m.correlationId).code == 0, "restore default limits");
    }

    /// The first topic ids of a prepared namespace, for "the objects remain" checks.
    std::vector<int32_t> topicIds(const std::string& name) {
        std::vector<int32_t> v;
        for (const char* tn : {"t", "t2"}) {
            const std::string k = name == "default" ? std::string(tn) : name + "/" + tn;
            auto it = w_->tid.find(k);
            if (it != w_->tid.end()) v.push_back(it->second);
        }
        return v;
    }
    std::vector<int32_t> rootMetadata() {
        auto ids = w_->r.metadataTopics(root());
        std::sort(ids.begin(), ids.end());
        return ids;
    }

    /// Restart the broker over the same data and log root in again (the other sessions are gone).  The reaper is held again.
    void restart(bool holdReaper) {
        if (holdReaper) w_->r.broker().beforeStartForTest = [](BrokerNode& n) { n.holdReaperAtBootForTest(true); };
        w_->r.restart();
        w_->r.broker().beforeStartForTest = nullptr;
        w_->root = loginRoot();
        w_->rootUsed = Clock::now();
        if (holdReaper) holdReaper_();
    }
    void releaseReaper() { setBudget(-1); }
    void holdReaper() { setBudget(0); }
    void setBudget(int64_t n) {
        BrokerNode& node = w_->r.broker().node();
        node.onAdminThreadForTest([&] { node.adminWorkerAgentForTest()->worker().reaperForTest().setIssueBudgetForTest(n); return 0; });
    }

    /// Data of every kind in `name` (created here): two topics, a user `<name>/u` (data + group + txn grants), a committed
    /// group offset and a transactional id.  Returns the user's session (kept open: the delete must fence it).
    int64_t seedDeleteWorkload(const std::string& name) {
        using namespace grants;
        World& w = *w_;
        createNs(name);
        std::vector<int32_t> tids;
        for (const char* tn : {"t", "t2"}) {
            const int32_t id = w.r.defineTopic(root(), name + "/" + tn);
            need(id >= 0, "topic " + name + "/" + tn);
            w.tid[name + "/" + tn] = id;
            tids.push_back(id);
        }
        broker().createUser(name + "/u", AuthRig::kPw,
                            {prefix(SO::Read, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""),
                             prefix(SO::Read, ST::Group, ""), prefix(SO::Write, ST::TransactionalId, ""),
                             prefix(SO::Describe, ST::TransactionalId, "")});
        const int64_t sid = w.r.login(name + "/u", AuthRig::kPw, pc::kClientVersionNamespaces);
        const auto [member, gen] = w.r.joinStable(sid, "g", {tids[0]});
        need(gen >= 0, "join g");
        need(w.r.commitAs(sid, "g", member, gen, tids[0], 1) == 0, "commit offset");
        pc::InitProducerId m;
        m.sessionId = sid; m.correlationId = w.r.c().nextCorrelationId(); m.transactionTimeoutMs = 30'000;
        const std::string txn = "x";
        m.transactionalId = {reinterpret_cast<const uint8_t*>(txn.data()), txn.size()};
        std::vector<uint8_t> buf(256);
        const std::size_t len = m.encode(buf.data());
        need(len != 0 && w.r.c().send(w.r.ch(), AuthRig::stream(), buf.data(), len), "send InitProducerId");
        need(!w.r.rawAnswer(m.correlationId).empty(), "InitProducerId answered");
        return sid;
    }
    [[nodiscard]] std::filesystem::path dataDir() { return w_->r.broker().dir() / "broker"; }
    [[nodiscard]] uint64_t reaperEventsIssued() {
        BrokerNode& node = w_->r.broker().node();
        return node.onAdminThreadForTest([&] { return node.adminWorkerAgentForTest()->worker().reaper().eventsIssued(); });
    }
    /// Polls (bounded) until `pred()`; the caller pumps nothing: the broker runs on its own threads.
    template <class F> bool until(F&& pred, int tries = 3000) {
        for (int i = 0; i < tries; ++i) {
            if (pred()) return true;
            w_->r.c().pollFor(std::chrono::milliseconds(10), [] { return false; });
        }
        return pred();
    }
    /// The number of `kind` events of namespace `nsId` in the node's whole __catalog log (committed or not).
    [[nodiscard]] std::size_t catalogEventCount(rune::catalog::EventKind kind, uint32_t nsId) {
        BrokerNode& n = w_->r.broker().node();
        return n.onAdminThreadForTest([&n, kind, nsId]() -> std::size_t {
            rune::replication::PartitionReplica* r = n.catalogReplicaForTest();
            if (r == nullptr) return 0;
            rune::storage::Log& log = *r->log();
            std::size_t count = 0;
            int64_t next = 0;
            const int64_t leo = log.logEndOffset();
            while (next < leo) {
                const std::vector<uint8_t> data = log.read(next, 1 << 22);
                if (data.empty()) break;
                const int64_t before = next;
                std::size_t pos = 0;
                while (pos < data.size()) {
                    rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
                    if (!reader.ensureValid()) break;
                    const auto tl = static_cast<std::size_t>(reader.totalLength());
                    reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                        if (off < next) return;
                        auto ev = rune::catalog::decode(
                            std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()), val.size()));
                        if (!ev) throw std::logic_error("LcRig: a __catalog record does not decode");
                        count += ev->kind == kind && ev->nsId == nsId ? 1 : 0;
                        next = off + 1;
                    });
                    if (tl == 0) break;
                    pos += tl;
                }
                if (next == before) break;
            }
            return count;
        });
    }
    /// The namespace's slot in the node's authoritative slot table is free (read ON the control thread).
    [[nodiscard]] bool nsSlotFreed(uint32_t nsId) { return w_->r.broker().node().nsSlotOfForTest(nsId) == kNoNsSlot; }
    /// The id is gone from the root listing (erased).
    /// True once `__security` holds no record of the namespace (read on the admin thread).
    bool securityPurged(uint32_t nsId, const std::string& name) {
        BrokerNode& n = w_->r.broker().node();
        return n.onAdminThreadForTest([&] {
            const SecurityApplier* sa = n.adminWorkerAgentForTest()->worker().securityApplierForTest();
            return sa == nullptr || !rune::security::hasNamespaceRecords(sa->state(), nsId, name);
        });
    }
    bool erased(uint32_t nsId) {
        for (const Seen& s : list()) if (s.nsId == nsId) return false;
        return true;
    }
    int64_t waitSessionGone(int64_t sid) {
        return until([&] { return !w_->r.broker().node().gatewayHasSessionForTest(sid); }, 1000) ? 1 : 0;
    }

private:
    struct World {
        AuthRig r;
        int64_t root = 0;
        std::chrono::steady_clock::time_point rootUsed{};
        uint64_t rid = 0;
        std::map<std::string, int32_t> tid;
        rune::protocol::NamespaceLimits defaultLimits;
        World(const std::string& tag, AuthBroker::Options o) : r(tag, std::move(o)) {}
    };
    std::unique_ptr<World> w_;
    bool anonymous_;

    using Clock = std::chrono::steady_clock;
    /// The cluster admin's session.  The broker reaps a session idle for 10 s: one idle for 2 s is logged in again first.
    int64_t root() {
        const auto now = Clock::now();
        if (w_->root == 0 || now - w_->rootUsed > std::chrono::seconds(2)) w_->root = loginRoot();
        w_->rootUsed = now;
        return w_->root;
    }

    static AuthBroker::Options optionsFor(bool anonymous) {
        AuthBroker::Options o;
        if (anonymous)   // one anonymous session per cell, never closed: lift the anonymous-connection cap
            o.configure = [](NodeConfig& c) { c.anonymousRead = true; c.anonymousMaxConnections = NodeConfig::kMaxAnonymousCap; };
        return o;
    }
    static void need(bool ok, const std::string& what) {
        if (!ok) throw std::runtime_error("LcRig: " + what);
    }
    int64_t loginRoot() {
        const int64_t sid = w_->r.login("admin", AuthBroker::kAdminPassword, pc::kClientVersionNamespaces);
        return sid;
    }
    void holdReaper_() { setBudget(0); }

    void setup(bool hold) {
        World& w = *w_;
        w.root = loginRoot();
        w.rootUsed = Clock::now();
        createNs("s");
        createNs("f");
        w_->root = loginRoot();
        w_->rootUsed = Clock::now();
        broker().createUser("s/admin", AuthRig::kPw, {grants::exact(grants::SO::Admin, grants::ST::Namespace, "s")});
        broker().createUser("s/user", AuthRig::kPw, {grants::prefix(grants::SO::Read, grants::ST::Topic, "")});
        broker().createUser("f/admin", AuthRig::kPw, {grants::exact(grants::SO::Admin, grants::ST::Namespace, "f")});
        broker().createUser("f/user", AuthRig::kPw, {grants::prefix(grants::SO::Read, grants::ST::Topic, "")});
        broker().createUser("dadmin", AuthRig::kPw, {grants::exact(grants::SO::Admin, grants::ST::Namespace, "default")});
        broker().createUser("duser", AuthRig::kPw, {grants::prefix(grants::SO::Read, grants::ST::Topic, "")});
        for (const char* tn : {"t", "t2"}) {
            const int32_t id = w.r.defineTopic(root(), tn);
            need(id >= 0, std::string("default topic ") + tn);
            w.tid[tn] = id;
        }
        for (const Seen& s : list()) if (s.name == "default") w.defaultLimits = s.limits;
        if (hold) setBudget(0);
    }

    /// The caller's session for the cell: the cluster admin's, a fresh login of the namespace's user, a foreign user, or an
    /// anonymous connection.  `code` != 0: the login was refused with it.
    Login login(Caller c, Target t, const std::string& name, int) {
        // On target Missing the NsAdminSame / UserInNs callers log in as s/admin and s/user: the standing users of the
        // foreign namespace `s`, because the missing namespace has no users of its own.
        const bool standing = t == Target::Missing;
        switch (c) {
            case Caller::ClusterAdmin: return {root(), 0};
            case Caller::NsAdminSame:
                return tryLogin(t == Target::Default ? "dadmin" : standing ? "s/admin" : name + "/admin");
            case Caller::UserInNs:
                return tryLogin(t == Target::Default ? "duser" : standing ? "s/user" : name + "/user");
            case Caller::NsAdminForeign: return tryLogin("f/admin");
            case Caller::UserForeign: return tryLogin("f/user");
            case Caller::Anonymous: {
                const int64_t sid = w_->r.wire().connect(w_->r.ch(), pc::kClientVersionNamespaces);
                need(sid > 0, "anonymous connect");
                return {sid, 0};
            }
        }
        return {};
    }

    Answer issue(Op op, int64_t sid, Target t, const std::string& name, const rune::protocol::NamespaceLimits& cur) {
        return await(sendOp(op, sid, t, name, cur));
    }

    /// After LowerBelowUsage: the objects remain and a new topic is refused NamespaceLimitExceeded.  A suspended target
    /// is resumed first (the catalog refuses DDL in a suspended namespace with its own code).
    void followUp(Cell& cell, const std::string& name, Target t) {
        const auto ids = topicIds(name);
        const auto md = rootMetadata();
        cell.topicsRemain = !ids.empty();
        for (const int32_t id : ids) cell.topicsRemain = cell.topicsRemain && std::binary_search(md.begin(), md.end(), id);
        if (t == Target::SuspendedRO || t == Target::SuspendedRA)
            need(ns(root(), pc::NamespaceDdlKind::Resume, name).code == 0, "resume before follow-up");
        cell.followUpCode = w_->r.defineTopicCode(root(), name == "default" ? "extra" : name + "/extra");
    }
};

}  // namespace rune::node::test
