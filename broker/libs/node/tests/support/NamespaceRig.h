#pragma once
// NamespaceRig.h - the rig behind the namespaces Op x Caller x Target matrix (NamespaceMatrixExpectations.h): ONE real
// authenticating broker on the default config with namespaces `a` and `b` (the same-named topic t / view v / user x /
// role r in `default`, `a` and `b`, plus b/only and b/onlyv), the seven callers of the matrix as logged-in sessions,
// and a second broker with RUNE_ANONYMOUS_READ on for the Anonymous rows (built when the first such cell runs).
//
//   run(op, caller, target, ordinal)      - one sequential cell: the op as the caller, the answer code + frame body
//   runTwin(op, caller, target, ordinal)  - the same op as the same caller on a truly missing twin (`zz/...`, id 98765)
//
// Every wait is bounded (AuthRig::dl); no sleeps.

#include "support/AuthRig.h"
#include "support/NamespaceMatrixExpectations.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace rune::node::test {

class NamespaceRig {
public:
    using Op = nsmatrix::Op;
    using Caller = nsmatrix::Caller;
    using Target = nsmatrix::Target;

    static constexpr int32_t kMissingId = 98'765;
    static constexpr const char* kTwinNs = "z";

    struct Answer {
        int code = kNoAnswer;
        std::vector<uint8_t> body;          ///< the answer frame without the message header
        int64_t corr = 0;
        std::string detail;                 ///< the broker's message, for failure texts
        bool listed = false;                ///< Metadata: the target id is in the listing
        std::vector<std::string> nsNames;   ///< ListNamespaces: the namespaces the answer lists
        std::vector<int32_t> ids;           ///< the topic / view ids the REQUEST sent (an answer may echo them)
        std::string askedNs;                ///< the namespace the request named (an answer may echo it)
        /// body with the correlation id, the sent ids and the asked namespace blanked: a hidden and a missing answer
        /// must then be byte-equal.  Only what the request itself carried is blanked; any other id in a hidden
        /// answer is a leak and stays visible.
        /// Symmetric: `other`'s correlation id and ids are blanked too, so a byte run that happens to equal one
        /// side's correlation id (e.g. 255 next to zeros) is blanked on both sides alike.
        [[nodiscard]] std::vector<uint8_t> normalized(const Answer& other) const {
            std::vector<uint8_t> f = body;
            for (const Answer* x : {this, &other}) {
                blank(f, &x->corr, 8);
                for (const int32_t id : x->ids) blank(f, &id, 4);
            }
            if (!askedNs.empty()) {
                const std::string quoted = "'" + askedNs + "'";
                for (std::size_t i = 0; i + quoted.size() <= f.size(); ++i)
                    if (std::memcmp(f.data() + i, quoted.data(), quoted.size()) == 0)
                        std::memset(f.data() + i + 1, '?', askedNs.size());
            }
            return f;
        }
    private:
        static void blank(std::vector<uint8_t>& f, const void* v, std::size_t n) {
            uint8_t le[8];
            std::memcpy(le, v, n);
            for (std::size_t i = 0; i + n <= f.size(); ++i)
                if (std::memcmp(f.data() + i, le, n) == 0) std::memset(f.data() + i, 0, n);
        }
    };

    NamespaceRig() : main_(std::make_unique<World>("nsmx", AuthBroker::Options{})) { setupMain(); }

    Answer run(Op op, Caller c, Target t, int ordinal) { return exec(op, c, t, ordinal, /*twin=*/false); }
    Answer runTwin(Op op, Caller c, Target t, int ordinal) { return exec(op, c, t, ordinal, /*twin=*/true); }

private:
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    using SP = rune::protocol::SecurityPatternKind;

    struct World {
        AuthRig r;
        std::map<std::string, int32_t> tid, vid;   ///< namespace -> its t / v
        int32_t tOnly = kMissingId, vOnly = kMissingId;
        int64_t root = 0;
        std::map<std::string, int64_t> sess;       ///< logged-in principals by name
        uint64_t rid = 0;
        World(const std::string& tag, AuthBroker::Options o) : r(tag, std::move(o)) {}
    };
    struct Sess { int64_t sid = 0; std::string ns; };
    struct Names {
        std::string ns, topic, view, user, role, group, txn, pattern, askedNs;
        int32_t tid = kMissingId, vid = kMissingId, src = kMissingId;
    };

    std::unique_ptr<World> main_, anon_;

    static std::string full(const std::string& ns, const std::string& local) {
        return ns == "default" ? local : ns + "/" + local;
    }
    /// The name a session of namespace `sessNs` uses for `local` in `objNs`: unqualified when it is its own.
    static std::string q(const std::string& sessNs, const std::string& objNs, const std::string& local) {
        return sessNs == objNs ? local : objNs + "/" + local;
    }
    static std::vector<uint8_t> strip(std::vector<uint8_t> f) {
        constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
        if (f.size() <= h) return {};
        f.erase(f.begin(), f.begin() + h);
        return f;
    }
    static std::span<const uint8_t> bytes(const std::string& s) {
        return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
    }
    static std::span<const uint8_t> bytes(std::string&&) = delete;

    // ── setup ───────────────────────────────────────────────────────────────────────────────────────────────
    static int64_t loginNs(AuthRig& r, const std::string& user, const std::string& pw) {
        return r.login(user, pw, pc::kClientVersionNamespaces);
    }
    static std::vector<AuthGrant> dataGrants() {
        using namespace grants;
        std::vector<AuthGrant> g;
        for (const SO op : {SO::Read, SO::Write, SO::Describe, SO::Create, SO::Alter, SO::Drop})
            g.push_back(prefix(op, ST::Topic, ""));
        for (const SO op : {SO::Read, SO::Create, SO::Alter, SO::Drop}) g.push_back(prefix(op, ST::View, ""));
        g.push_back(prefix(SO::Read, ST::Group, ""));
        g.push_back(prefix(SO::Write, ST::TransactionalId, ""));
        g.push_back(prefix(SO::Describe, ST::TransactionalId, ""));
        return g;
    }
    /// A namespace admin: ADMIN on its namespace plus the data grants (ADMIN alone gives DDL, not Read / Write, so the
    /// matrix's granted(op, NsAdmin) = true for the data ops needs both).
    static std::vector<AuthGrant> nsAdminGrants(const std::string& ns) {
        auto g = dataGrants();
        g.push_back(grants::exact(SO::Admin, ST::Namespace, ns));
        return g;
    }
    static void need(bool ok, const std::string& what) {
        if (!ok) throw std::runtime_error("NamespaceRig: " + what);
    }
    /// The shared objects of namespace `ns` (default: unqualified): topic t, view v, user x, role r.
    void makeShared(World& w, const std::string& ns) {
        AuthRig& r = w.r;
        const int32_t t = r.defineTopic(w.root, full(ns, "t"));
        need(t >= 0, "topic t in " + ns);
        const int32_t v = r.createView(w.root, full(ns, "v"), t);
        need(v >= 0, "view v in " + ns);
        w.tid[ns] = t;
        w.vid[ns] = v;
    }
    /// The objects only namespace b holds (the ForeignNs targets).
    void makeOnly(World& w) {
        w.tOnly = w.r.defineTopic(w.root, "b/only");
        need(w.tOnly >= 0, "topic b/only");
        w.vOnly = w.r.createView(w.root, "b/onlyv", w.tOnly);
        need(w.vOnly >= 0, "view b/onlyv");
    }
    /// User x and role r of namespace `ns` (the security ops' targets).
    void makeSecurityTargets(World& w, const std::string& ns) {
        AuthBroker& b = w.r.broker();
        b.createUser(full(ns, "x"), AuthRig::kPw, {});
        need(b.admin().createRole(w.r.ch(), b.adminSession(), full(ns, "r")).errorCode == 0, "role r in " + ns);
    }
    void setupMain() {
        World& w = *main_;
        AuthRig& r = w.r;
        r.createNamespace("a");
        r.createNamespace("b");
        w.root = loginNs(r, "admin", AuthBroker::kAdminPassword);
        w.sess["admin"] = w.root;
        makeShared(w, "default");
        makeShared(w, "a");
        makeShared(w, "b");
        makeOnly(w);
        using namespace grants;
        AuthBroker& b = r.broker();
        b.createUser("a/admin", AuthRig::kPw, nsAdminGrants("a"));
        b.createUser("b/admin", AuthRig::kPw, nsAdminGrants("b"));
        b.createUser("a/granted", AuthRig::kPw, dataGrants());
        b.createUser("b/granted", AuthRig::kPw, dataGrants());
        b.createUser("a/ungranted", AuthRig::kPw, {});
        for (const char* ns : {"default", "a", "b"}) makeSecurityTargets(w, ns);
        // The placements are committed before the first cell (a listing names them).
        for (const auto& [who, ns] : std::vector<std::pair<std::string, std::string>>{{"a/admin", "a"}, {"b/admin", "b"}}) {
            const int64_t sid = session(w, who);
            for (int i = 0; i < 400; ++i) {
                const auto ids = r.metadataTopics(sid);
                if (std::find(ids.begin(), ids.end(), w.tid[ns]) != ids.end()) break;
                r.c().pollFor(std::chrono::milliseconds(25), [] { return false; });
            }
        }
    }
    void setupAnon() {
        AuthBroker::Options o;
        o.configure = [](NodeConfig& c) { c.anonymousRead = true; };
        anon_ = std::make_unique<World>("nsmxanon", std::move(o));
        World& w = *anon_;
        w.r.createNamespace("b");
        w.root = loginNs(w.r, "admin", AuthBroker::kAdminPassword);
        makeShared(w, "default");
        makeShared(w, "b");
        makeOnly(w);
        for (const char* ns : {"default", "b"}) makeSecurityTargets(w, ns);
        const int32_t t = w.tid["default"];
        for (int i = 0; i < 400; ++i) {   // placed: the anonymous listing names it
            const auto ids = w.r.metadataTopics(w.root);
            if (std::find(ids.begin(), ids.end(), t) != ids.end()) break;
            w.r.c().pollFor(std::chrono::milliseconds(25), [] { return false; });
        }
    }
    int64_t session(World& w, const std::string& user) {
        auto it = w.sess.find(user);
        if (it == w.sess.end()) it = w.sess.emplace(user, loginNs(w.r, user, AuthRig::kPw)).first;
        return it->second;
    }

    // ── the cell ────────────────────────────────────────────────────────────────────────────────────────────
    Sess sessionFor(World& w, Caller c, Target t, bool anon) {
        if (anon) {
            const int64_t sid = w.r.wire().connect(w.r.ch(), pc::kClientVersionNamespaces);
            need(sid > 0, "anonymous connect");
            return {sid, "default"};
        }
        const bool foreign = t == Target::ForeignNs;
        switch (c) {
            case Caller::ClusterAdmin: return {w.root, "default"};
            case Caller::NsAdminSame: return foreign ? Sess{session(w, "b/admin"), "b"} : Sess{session(w, "a/admin"), "a"};
            case Caller::NsAdminForeign: return foreign ? Sess{session(w, "a/admin"), "a"} : Sess{session(w, "b/admin"), "b"};
            case Caller::UserGranted: return {session(w, "a/granted"), "a"};
            case Caller::UserUngranted: return {session(w, "a/ungranted"), "a"};
            // The object lives in b for ForeignNs: a UserForeign naming it is foreign to it only from namespace a.
            case Caller::UserForeign: return foreign ? Sess{session(w, "a/granted"), "a"} : Sess{session(w, "b/granted"), "b"};
            default: break;
        }
        return {};
    }

    Names namesFor(World& w, Op op, Target t, int ord, const Sess& s, bool anon, bool twin) {
        const std::string o = std::to_string(ord);
        const std::string ownNs = anon ? "default" : "a";
        const std::string objNs = t == Target::OwnNs || t == Target::Missing ? ownNs : t == Target::ForeignNs ? "b" : s.ns;
        const bool miss = t == Target::Missing;
        auto pick = [&](std::string cons, std::string foreign, std::string base) {
            return miss ? "m" + o : !cons.empty() ? cons : t == Target::ForeignNs ? foreign : base;
        };
        const bool alterOrDropTopic = op == Op::AlterTopic || op == Op::DropTopic;
        const std::string tLocal = pick(alterOrDropTopic ? "d" + o : "", "only", "t");
        const std::string vLocal = pick(op == Op::DropView ? "dv" + o : "", "onlyv", "v");
        const std::string uLocal = pick(op == Op::DropUser ? "d" + o : "", "x", "x");
        const std::string rLocal = pick("", "r", "r");
        Names n;
        n.pattern = (op == Op::Revoke ? "d" : "p") + o;
        // A view's source must be readable by the caller: its own namespace's t unless it is root or in the object's.
        const std::string srcNs = s.ns == "default" || s.ns == objNs ? objNs : s.ns;
        n.src = w.tid.contains(srcNs) ? w.tid[srcNs] : kMissingId;
        if (twin) {
            // The truly missing twin: namespace `z` never exists and has the same length as `a` / `b`, the ids are
            // missing, the source is the real cell's.
            n.ns = n.askedNs = kTwinNs;
            n.topic = std::string(kTwinNs) + "/" + tLocal;
            n.view = std::string(kTwinNs) + "/" + vLocal;
            n.user = std::string(kTwinNs) + "/" + (op == Op::Revoke ? std::string("x") : uLocal);
            n.role = std::string(kTwinNs) + "/" + rLocal;
            n.group = "g" + o;
            n.txn = "x" + o;
            return n;
        }
        n.askedNs = isNamespaceOp(op) ? n.ns : s.ns == objNs ? std::string{} : objNs;
        n.ns = t == Target::OwnNs ? ownNs : t == Target::ForeignNs ? "b" : "m" + o;
        n.topic = q(s.ns, objNs, tLocal);
        n.view = q(s.ns, objNs, vLocal);
        n.user = q(s.ns, objNs, op == Op::Revoke && !miss ? "x" : uLocal);
        n.role = q(s.ns, objNs, rLocal);
        // Groups and transactional ids: unqualified in `default`; a Missing target is created unqualified.
        n.group = miss ? "g" + o : q(s.ns, objNs, "g" + o);
        n.txn = miss ? "x" + o : q(s.ns, objNs, "x" + o);
        if (!miss) {
            n.tid = t == Target::ForeignNs ? w.tOnly : w.tid.contains(objNs) ? w.tid[objNs] : kMissingId;
            n.vid = t == Target::ForeignNs ? w.vOnly : w.vid.contains(objNs) ? w.vid[objNs] : kMissingId;
        }
        if (!miss) preCreate(w, op, t, ord, s, objNs, ownNs);
        return n;
    }

    /// Root makes the per-cell fresh objects the consuming ops (alter / drop topic, drop view, drop user, revoke) hit.
    void preCreate(World& w, Op op, Target t, int ord, const Sess& s, const std::string& objNs, const std::string& ownNs) {
        const std::string o = std::to_string(ord);
        std::vector<std::string> nss{objNs};
        if (t == Target::SameNameBoth && ownNs != objNs) nss.push_back(ownNs);
        AuthRig& r = w.r;
        for (const std::string& ns : nss) {
            switch (op) {
                case Op::AlterTopic: case Op::DropTopic:
                    need(r.defineTopic(w.root, full(ns, "d" + o)) >= 0, "fresh topic d" + o + " in " + ns);
                    break;
                case Op::DropView:
                    need(r.createView(w.root, full(ns, "dv" + o), w.tid[ns]) >= 0, "fresh view dv" + o + " in " + ns);
                    break;
                case Op::DropUser:
                    r.broker().createUser(full(ns, "d" + o), AuthRig::kPw, {});
                    break;
                case Op::Revoke: {
                    const auto a = r.broker().admin().grant(r.ch(), r.broker().adminSession(), full(ns, "x"), false,
                                                            SO::Read, ST::Topic, SP::Prefix, "d" + o);
                    need(a.errorCode == 0, "root's grant to revoke");
                    break;
                }
                default: break;
            }
        }
        (void)s;
    }

    Answer exec(Op op, Caller c, Target t, int ordinal, bool twin) {
        const bool anon = c == Caller::Anonymous;
        if (anon && !anon_) setupAnon();
        World& w = anon ? *anon_ : *main_;
        const Sess s = sessionFor(w, c, t, anon);
        const Names n = namesFor(w, op, t, ordinal, s, anon, twin);
        Answer a = issue(w, op, s.sid, n);
        a.askedNs = n.askedNs;
        switch (op) {   // the ids the request carried
            case Op::Produce: case Op::Fetch: case Op::Metadata: case Op::GroupJoin: a.ids = {n.tid}; break;
            case Op::GetViewRow: a.ids = {n.vid}; break;
            case Op::CreateView: a.ids = {n.src}; break;
            default: break;
        }
        return a;
    }

    // ── the ops, one case each ──────────────────────────────────────────────────────────────────────────────
    void frameOf(World& w, Answer& a, int64_t corr) {
        a.corr = corr;
        if (const auto raw = w.r.c().awaitRaw(corr, AuthRig::dl(2))) a.body = strip(*raw);
    }
    void ddl(World& w, Answer& a, int64_t corr) {
        const auto any = w.r.c().awaitAnyDdlAnswer(corr, AuthRig::dl(15));
        a.code = any ? any->code : kNoAnswer;
        if (any) a.detail = any->message;
        frameOf(w, a, corr);
    }
    void nsDdl(World& w, Answer& a, int64_t sid, const Names& n, pc::NamespaceDdlKind kind) {
        pc::NamespaceDdlRequest m;
        m.sessionId = sid;
        m.correlationId = w.r.c().nextCorrelationId();
        m.ext.clientName = "ns-matrix";
        m.ext.requestId = rune::protocol::RequestId{0xB0B, ++w.rid};
        m.kind = kind;
        m.name = n.ns;
        if (kind == pc::NamespaceDdlKind::SetLimits) {
            m.presentMask[0] = 1;   // limit field 0 = maxTopics
            m.limits.maxTopics = 100;
        }
        std::vector<uint8_t> buf(m.encodedSize());
        buf.resize(m.encode(buf.data()));
        need(w.r.c().send(w.r.ch(), AuthRig::stream(), buf.data(), buf.size()), "send namespace DDL");
        ddl(w, a, m.correlationId);
    }
    static int errorCodeOf(const std::vector<uint8_t>& f) {
        if (const auto e = pc::ErrorResponse::decode(f.data(), f.size())) return e->code;
        return kNoAnswer;
    }
    Answer issue(World& w, Op op, int64_t sid, const Names& n) {
        Answer a;
        AuthRig& r = w.r;
        auto& c = r.c();
        const std::string& ch = r.ch();
        constexpr int32_t st = AuthRig::stream();
        switch (op) {
            case Op::CreateNamespace: nsDdl(w, a, sid, n, pc::NamespaceDdlKind::Create); break;
            case Op::SetNamespaceLimits: nsDdl(w, a, sid, n, pc::NamespaceDdlKind::SetLimits); break;
            case Op::ListNamespaces: {
                pc::ListNamespaces m;
                m.sessionId = sid; m.correlationId = c.nextCorrelationId(); m.afterNsId = 0; m.max = 100;
                std::vector<uint8_t> buf(m.encodedSize());
                buf.resize(m.encode(buf.data()));
                need(c.send(ch, st, buf.data(), buf.size()), "send ListNamespaces");
                const auto raw = r.rawAnswer(m.correlationId);
                a.corr = m.correlationId;
                if (raw.empty()) break;
                a.body = strip(raw);
                const auto h = pc::MessageHeader::decode(raw.data(), raw.size());
                if (h && h->templateId == pc::ListNamespacesResponse::TEMPLATE_ID) {
                    if (const auto l = pc::ListNamespacesResponse::decode(raw.data(), raw.size())) {
                        a.code = 0;
                        for (const auto& i : l->namespaces) a.nsNames.push_back(i.name);
                    }
                } else {
                    a.code = errorCodeOf(raw);
                }
                break;
            }
            case Op::CreateTopic:
                ddl(w, a, c.sendDefineTopic(ch, st, sid, n.topic, 1, authRigCharBlob(), 0, {}, 0, authRigTopicConfig()));
                break;
            case Op::AlterTopic: {
                rune::codec::AlterOp o{};
                o.type = rune::codec::AlterOpType::Add; o.name = "extra"; o.primType = rune::codec::PrimType::Int64; o.fixedLen = 0;
                ddl(w, a, c.sendAlterTopic(ch, st, sid, n.topic, 0, rune::codec::encodeAlterOps({o})));
                break;
            }
            case Op::DropTopic: ddl(w, a, c.sendDropTopic(ch, st, sid, n.topic)); break;
            case Op::Produce: {
                for (int attempt = 0; attempt < 40; ++attempt) {
                    const int64_t corr = c.nextCorrelationId();
                    if (!c.produce(ch, st, sid, corr, n.tid, 0, 1, makeBatch(1))) break;
                    const auto ack = c.awaitProduceResponseUntil(corr, AuthRig::dl());
                    a.code = ack ? ack->errorCode : kNoAnswer;
                    frameOf(w, a, corr);
                    if (a.code != static_cast<int>(pc::ErrorCode::NotLeader) && a.code != kNoAnswer) break;
                    c.pollFor(std::chrono::milliseconds(25), [] { return false; });
                }
                break;
            }
            case Op::Fetch: {
                for (int attempt = 0; attempt < 40; ++attempt) {
                    const int64_t corr = c.sendSubscribe(ch, st, sid, n.tid, 0);
                    const auto code = c.awaitSubscribeCode(corr, AuthRig::dl());
                    a.code = code ? *code : kNoAnswer;
                    frameOf(w, a, corr);
                    if (a.code != static_cast<int>(pc::ErrorCode::NotLeader) && a.code != kNoAnswer) break;
                    c.pollFor(std::chrono::milliseconds(25), [] { return false; });
                }
                break;
            }
            case Op::Metadata: {
                const auto ids = r.metadataTopics(sid);
                a.corr = c.lastCorrelationId();
                a.listed = std::find(ids.begin(), ids.end(), n.tid) != ids.end();
                a.code = a.listed ? 0 : kNotAuthorized;   // "not listed" is the Missing answer of the id ops
                break;
            }
            case Op::GetTopicSchema: {
                const int64_t corr = c.sendGetTopicSchema(ch, st, sid, n.topic);
                const auto ans = c.awaitSchema(corr, AuthRig::dl());
                a.code = ans ? ans->errorCode : kNoAnswer;
                frameOf(w, a, corr);
                break;
            }
            case Op::CreateView:
                ddl(w, a, c.sendCreateView(ch, st, sid, n.view, "id", 0, authRigViewPlan(localOf(n.view), {n.src})));
                break;
            case Op::DropView: ddl(w, a, c.sendDropView(ch, st, sid, n.view)); break;
            case Op::GetViewRow: {
                const int64_t corr = c.sendGetViewRow(ch, st, sid, n.vid, 1);
                const auto ans = c.awaitViewRowOrError(corr, AuthRig::dl());
                frameOf(w, a, corr);
                if (!ans) break;
                const int status = ans->isError ? ans->errorCode : ans->status;
                a.code = !ans->isError && status >= 0 && status <= 2 ? 0 : status;   // 0 OK / 1 AbsentKey / 2 NotReady
                break;
            }
            case Op::CreateUser: sec(w, a, r.wire().createUser(ch, sid, n.user, AuthRig::kPw)); break;
            case Op::DropUser: sec(w, a, r.wire().dropUser(ch, sid, n.user)); break;
            case Op::CreateRole: sec(w, a, r.wire().createRole(ch, sid, n.role)); break;
            case Op::Grant:
                sec(w, a, r.wire().grant(ch, sid, n.user, false, SO::Read, ST::Topic, SP::Prefix, n.pattern));
                break;
            case Op::Revoke:
                sec(w, a, r.wire().revoke(ch, sid, n.user, false, SO::Read, ST::Topic, SP::Prefix, n.pattern));
                break;
            case Op::GroupJoin: {
                const auto j = c.joinGroup(ch, st, sid, n.group, {}, {n.tid});
                frameOf(w, a, c.lastCorrelationId());
                a.code = j ? j->errorCode : kNoAnswer;
                break;
            }
            case Op::TxnInit: {
                pc::InitProducerId m;
                m.sessionId = sid; m.correlationId = c.nextCorrelationId(); m.transactionTimeoutMs = 30'000;
                m.transactionalId = bytes(n.txn);
                std::vector<uint8_t> buf(256);
                const std::size_t len = m.encode(buf.data());
                if (len == 0 || !c.send(ch, st, buf.data(), len)) break;
                const auto raw = r.rawAnswer(m.correlationId);
                a.corr = m.correlationId;
                if (raw.empty()) break;
                a.body = strip(raw);
                const auto h = pc::MessageHeader::decode(raw.data(), raw.size());
                if (h && h->templateId == static_cast<uint16_t>(pc::TemplateId::InitProducerIdResponse)) {
                    if (const auto ans = pc::InitProducerIdResponse::decode(raw.data(), raw.size())) a.code = ans->errorCode;
                } else {
                    a.code = errorCodeOf(raw);
                }
                break;
            }
            case Op::kCount: break;
        }
        return a;
    }
    void sec(World& w, Answer& a, const AuthWireClient::DdlAnswer& ans) {
        frameOf(w, a, w.r.c().lastCorrelationId());
        a.code = ans.errorCode == 0xFFFF ? kNoAnswer : ans.errorCode;
        a.detail = ans.detail;
    }
    static std::string localOf(const std::string& name) {
        const auto slash = name.rfind('/');
        return slash == std::string::npos ? name : name.substr(slash + 1);
    }
};

}  // namespace rune::node::test
