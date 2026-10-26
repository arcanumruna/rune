// SecurityAdmin.cpp — the `__security` leadership on the admin thread (authentication plan D39).  See SecurityAdmin.h.
#include <rune/gateway/SecurityRequestFingerprint.h>
#include <cctype>
#include <rune/node/SecurityAdmin.h>

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/catalog/NsName.h>
#include <rune/protocol/client/SecurityMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/security/SecurityState.h>
#include <rune/security/SecurityStatementText.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <type_traits>
#include <cstring>
#include <utility>
#include <vector>

namespace rune::node {

namespace {
constexpr uint16_t kOK = 0, kNotLeader = 2, kInternal = 6, kInvalidRequest = 39, kNotAuthorized = 45;
namespace sec = rune::security;
namespace pc  = rune::protocol::client;

int32_t epochOf(const SecurityRole& r) noexcept {
    return std::visit([](const auto& st) { return st.epoch; }, r);
}
} // namespace

SecurityAdmin::SecurityAdmin(SecurityApplier& applier, rune::replication::PartitionReplica* replica,
                             SecurityRespond respond, int32_t selfId)
    : applier_(applier), replica_(replica), respond_(std::move(respond)), selfId_(selfId) {}

std::string_view SecurityAdmin::roleName() const noexcept {
    switch (role_.index()) {
    case 0:  return "Follower";
    case 1:  return "LeaderCatchingUp";
    case 2:  return "LeaderServing";
    default: return "Halted";
    }
}

void SecurityAdmin::setRole_(SecurityRole next) {
    const bool changed = next.index() != role_.index() || epochOf(next) != epochOf(role_);
    role_ = std::move(next);
    if (changed) spdlog::info("AdminWorker: __security role -> {} (epoch {})", roleName(), epochOf(role_));
}

bool SecurityAdmin::caughtUp_() const noexcept {
    // Every record the leader holds — a previous epoch's tail included — is committed (HW = min LEO over the ISR)
    // AND applied here.
    const int64_t hw = replica_->highWatermark();
    return !applier_.halted() && hw == replica_->logEndOffset() && applier_.nextOffset() == hw;
}

// The role is absolute and epoch-stamped (the committed assignment); the replica's own role machine handles
// re-asserts and refuses a leadership at an epoch it already led at (restart fence → asks the controller for a bump).
void SecurityAdmin::onRole(const AdminRoleMsg& role) {
    if (replica_ == nullptr) return;
    namespace rr = rune::replication;
    if (role.leader) {
        std::vector<rr::NodeId> isr;
        isr.reserve(static_cast<std::size_t>(role.isrCount));
        for (int i = 0; i < role.isrCount; ++i)
            isr.push_back(static_cast<rr::NodeId>(role.isr[static_cast<std::size_t>(i)]));
        replica_->becomeLeader(static_cast<rr::Epoch>(role.epoch), std::move(isr), role.partitionEpoch);
        if (replica_->role() == rr::Role::Leader) {
            // A halted broker stays Halted (its replica still leads until the controller moves the leadership;
            // it accepts no security change).
            if (std::holds_alternative<SecurityHalted>(role_)) return;
            // A promotion (or a leadership at a new epoch) starts catching up — the gate opens in updateRole_ once
            // the whole log is committed and applied; a same-epoch re-assert (an ISR update) keeps serving.
            const auto* s = std::get_if<SecurityLeaderServing>(&role_);
            if (s == nullptr || s->epoch != role.epoch) setRole_(SecurityLeaderCatchingUp{role.epoch});
            return;
        }
        demote_(-1, role.epoch);   // the replica refused the leadership (restart fence)
        return;
    }
    replica_->becomeFollower(static_cast<rr::Epoch>(role.epoch), static_cast<rr::NodeId>(role.leaderId));
    demote_(role.leaderId, role.epoch);
}

void SecurityAdmin::onHoldLeadership(const AdminRoleMsg& role) {
    if (replica_ != nullptr) replica_->holdLeadershipForStorage(role.epoch, role.partitionEpoch);
    demote_(-1, role.epoch);
}

void SecurityAdmin::demote_(int32_t leaderId, int32_t epoch) {
    if (!std::holds_alternative<SecurityHalted>(role_)) setRole_(SecurityFollower{leaderId, epoch});
    // Spec §5.2: an in-flight request is answered at once - the retriable NotLeader (its event may still commit under
    // the new leader; the retry dedupes).
    if (auto* a = std::get_if<AwaitingCommit>(&inFlight_)) {
        const SecurityRequest req = std::move(a->req);
        inFlight_ = Idle{};
        answer_(req, kNotLeader, "the security leader changed while the request awaited its commit");
    }
}

void SecurityAdmin::onAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r) {
    if (replica_ == nullptr) return;
    replica_->onAlterIsrResponse(r);
}

// Follower ⇄ CatchingUp ⇄ Serving, from the replica's role and what the applier has applied: a leader serves
// security DDL only once every event in its log is applied (so the state it validates against is the state its
// append will extend).
bool SecurityAdmin::updateRole_() {
    if (replica_ == nullptr) return false;
    namespace rr = rune::replication;
    const auto epoch = static_cast<int32_t>(replica_->epoch());
    if (applier_.halted()) {   // terminal: every security change answers Internal from now on
        if (std::holds_alternative<SecurityHalted>(role_)) return false;
        const rune::security::ApplyError* e = applier_.haltError();
        setRole_(SecurityHalted{e != nullptr ? e->offset : -1, epoch});
        return true;
    }
    const bool leads = replica_->role() == rr::Role::Leader;
    if (!leads) {
        if (!std::holds_alternative<SecurityFollower>(role_)) {
            demote_(-1, epoch);
            return true;
        }
        return false;
    }
    const bool caughtUp = caughtUp_();
    if (const auto* c = std::get_if<SecurityLeaderCatchingUp>(&role_); c && caughtUp && !hasInFlight()) {
        spdlog::info("AdminWorker: security leader (epoch {}) caught up at offset {} — serving security DDL",
                     c->epoch, applier_.nextOffset());
        setRole_(SecurityLeaderServing{c->epoch});
        return true;
    }
    if (const auto* s = std::get_if<SecurityLeaderServing>(&role_); s && !caughtUp && !hasInFlight()) {
        // An appended event this broker has not applied: no new event may be built on a state that does not
        // include it.
        setRole_(SecurityLeaderCatchingUp{s->epoch});
        return true;
    }
    if (std::holds_alternative<SecurityFollower>(role_)) {
        setRole_(SecurityLeaderCatchingUp{epoch});
        return true;
    }
    return false;
}


// ─── ingress ────────────────────────────────────────────────────────────────
std::pair<int64_t, int64_t> SecurityAdmin::sessionCorrOf_(const SecurityRequest& req) noexcept {
    return std::visit([](const auto& m) { return std::pair<int64_t, int64_t>{m.session, m.corr}; }, req);
}

void SecurityAdmin::answer_(const SecurityRequest& req, uint16_t code, std::string_view detail, uint64_t objectId,
                            uint32_t newVersion, int64_t gateOffset) {
    ++answered_;
    const auto [session, corr] = sessionCorrOf_(req);
    if (respond_) respond_(session, corr, code, objectId, newVersion, gateOffset, detail);
}

void SecurityAdmin::submit(SecurityRequest r) {
    if (pending_.size() >= kMaxPending) [[unlikely]] {
        ++pendingRefused_;
        // Not started: answered directly, outside the started/answered pair.
        const auto [session, corr] = sessionCorrOf_(r);
        if (respond_) respond_(session, corr, kInternal, 0, 0, -1, "too many security requests waiting");
        return;
    }
    pending_.push_back(std::move(r));
}

// ─── the request fingerprint (F14, for __security): FNV-1a-64 over every field that makes the statement what it is -
//     never the requestId, clientName, session or the principal.
namespace {
struct Fnv {
    uint64_t h = 0xcbf29ce484222325ULL;
    void u(uint64_t v) { h = rune::catalog::detail::fnvU64(h, v); }
    void s(std::string_view v) { h = rune::catalog::detail::fnvBytes(h, v.data(), v.size()); }
    void b(const void* p, std::size_t n) { h = rune::catalog::detail::fnvBytes(h, p, n); }
    void verifier(const sec::ScramVerifier& v) {
        b(v.salt.data(), v.salt.size()); u(v.iterations);
        b(v.storedKey.data(), v.storedKey.size()); b(v.serverKey.data(), v.serverKey.size());
    }
};
uint64_t fingerprintOf(const SecurityDdlMsg& m) {
    // The ONE definition (PR #410 review F2): the gateway computes the same from the wire request.
    const rune::gateway::VerifierBytes v{m.verifier.salt, m.verifier.iterations, m.verifier.storedKey, m.verifier.serverKey};
    return rune::gateway::securityDdlFingerprint(m.meta.statement, m.kind, m.name, m.change, m.passwordIndex,
                                                 m.terminateSessions, v, m.granteeKind, m.op, m.resourceType,
                                                 m.patternKind, m.pattern, m.roleName);
}
uint64_t fingerprintOf(const SecurityBootstrapMsg& m) {
    Fnv f;
    f.u(kTidSecurityBootstrap); f.s(m.adminName); f.verifier(m.verifier);
    f.b(m.fakeSaltSeed.data(), m.fakeSaltSeed.size());
    return f.h;
}

int64_t wallClockMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

/// "<refusal>: <detail>" - the user-facing text of a refused apply.
std::string refusalText(const sec::ApplyError& e) {
    std::string t = e.refusal ? std::string{sec::refusalName(*e.refusal)} : std::string{sec::applyErrorName(e.code)};
    if (!e.detail.empty()) t += ": " + e.detail;
    return t;
}
} // namespace

// ─── doWork / advance ────────────────────────────────────────────────────────
int SecurityAdmin::doWork(int64_t nowNs) {
    int work = 0;
    if (advance_(nowNs)) ++work;
    if (updateRole_()) ++work;
    // Refusals answer at once (nothing in flight): take up to a bounded batch per pass; an accepted request goes in
    // flight and ends the batch.
    for (int i = 0; i < 16 && !hasInFlight() && !pending_.empty(); ++i) {
        startNext_(nowNs);
        ++work;
    }
    return work;
}

bool SecurityAdmin::advance_(int64_t nowNs) {
    auto* a = std::get_if<AwaitingCommit>(&inFlight_);
    if (a == nullptr) return false;
    // Committed AND applied locally (checked before the halt: an event applied before a later halt still gets its OK).
    if (applier_.lastAppliedOffset() >= a->offset) {
        const SecurityRequest req = std::move(a->req);
        const uint64_t objectId = a->event.objectId;
        const uint32_t newVersion = a->event.newVersion;
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        answer_(req, kOK, {}, objectId, newVersion, offset);   // gated on this offset (D24)
        return true;
    }
    if (applier_.halted()) {   // halted at or before the event: it will never be applied here
        const SecurityRequest req = std::move(a->req);
        const int64_t offset = a->offset;
        inFlight_ = Idle{};
        answer_(req, kInternal, "security apply halted on this broker at offset " +
                                    std::to_string(applier_.haltError() ? applier_.haltError()->offset : -1) +
                                    "; the request's event (offset " + std::to_string(offset) +
                                    ") may have committed - retry with the same requestId on another broker");
        return true;
    }
    // Below minIsr while the request awaited its commit: it cannot commit until a broker returns - the retriable
    // NotLeader now (never wait out the client's deadline); the retry dedupes to its result once it commits.
    if (static_cast<int32_t>(replica_->isr().size()) < replica_->minIsr()) {
        const SecurityRequest req = std::move(a->req);
        inFlight_ = Idle{};
        answer_(req, kNotLeader, "__security fell below its minIsr while the request awaited its commit");
        return true;
    }
    if (nowNs - a->startedNs > kDdlTimeoutNs) {
        const SecurityRequest req = std::move(a->req);
        inFlight_ = Idle{};
        answer_(req, kNotLeader, "the security event did not commit in time; retry with the same requestId");
        return true;
    }
    return false;
}

// ─── startNext_ — gate, dedupe, build, dry-run, append ───────────────────────
void SecurityAdmin::startNext_(int64_t nowNs) {
    ++started_;
    SecurityRequest req = std::move(pending_.front());
    pending_.pop_front();

    if (replica_ == nullptr) {
        answer_(req, kInternal, "this broker has no __security");
        return;
    }
    if (applier_.halted() && !std::holds_alternative<SecurityHalted>(role_)) (void)updateRole_();
    if (const auto* h = std::get_if<SecurityHalted>(&role_)) {
        answer_(req, kInternal, "security apply halted at offset " + std::to_string(h->offset));   // never a redirect
        return;
    }
    if (!std::holds_alternative<SecurityLeaderServing>(role_)) {   // follower / catching up: retriable
        answer_(req, kNotLeader);
        return;
    }
    if (const auto* bad = std::get_if<MalformedDdl>(&req)) {
        answer_(req, kInvalidRequest, "malformed request: " + bad->reason);
        return;
    }
    if (const auto* d = std::get_if<SecurityDdlMsg>(&req)) startDdl_(*d, nowNs, req);
    else startBootstrap_(std::get<SecurityBootstrapMsg>(req), nowNs, req);
}

namespace {
/// The Principal of an event from the request's meta.
sec::Principal principalOf(const DdlMeta& meta, std::string authenticated, uint64_t fingerprint) {
    sec::Principal p;
    p.sessionId = meta.sessionId;
    p.clientName = meta.clientName;
    p.sourceAddress = meta.sourceAddress;
    p.authenticated = std::move(authenticated);
    p.requestId = meta.requestId;
    p.requestFingerprint = fingerprint;
    return p;
}

/// Review C I1 (D10): `__security` is never compacted and readable by every ADMIN forever, so the leader is the last line
/// of defence for a password literal.  True when `statement` has a PASSWORD keyword (any case) followed by a quoted
/// literal (' " or $$) that is not exactly '<redacted>'.  Cold path.
bool statementHasUnredactedPassword(std::string_view statement) {
    constexpr std::string_view kWord = "PASSWORD", kRedacted = "'<redacted>'";
    for (std::size_t i = 0; i + kWord.size() <= statement.size(); ++i) {
        bool match = true;
        for (std::size_t k = 0; k < kWord.size() && match; ++k)
            match = std::toupper(static_cast<unsigned char>(statement[i + k])) == kWord[k];
        if (!match) continue;
        std::size_t j = i + kWord.size();
        while (j < statement.size() && std::isspace(static_cast<unsigned char>(statement[j]))) ++j;
        if (j >= statement.size()) continue;
        const char q = statement[j];
        if (q != '\'' && q != '"' && q != '$') continue;
        if (statement.substr(j, kRedacted.size()) != kRedacted) return true;
    }
    return false;
}
/// Issue #121: true when applying `e` to `state` leaves no enabled security admin.  A copy of the state takes the event
/// (the library's own apply, so every op kind is judged by the rules that will really run); an event that does not
/// apply is not judged here (appendEvent_'s dry run answers it).
bool removesLastSecurityAdmin(const sec::SecurityState& state, const sec::SecurityEvent& e) {
    sec::SecurityState after = state;
    sec::SecurityEvent probe = e;
    auto fp = sec::previewFingerprint(state, probe);
    if (!fp) return false;
    probe.fingerprint = *fp;
    if (!sec::apply(after, probe, state.appliedOffset() + 1)) return false;
    return !after.hasSecurityAdmin();
}

// ── Namespaces Task 8 (spec §4): name resolution and the namespace guard ─────────────────────────────────────────────
/// One namespace as the security write path sees it.  Deleting reads as Missing (spec: a deleting namespace is gone
/// to every caller), Suspended refuses security DDL.
enum class NsLookup : uint8_t { Active, Suspended, Missing };
struct NsHit {
    NsLookup         kind = NsLookup::Missing;
    uint32_t         nsId = 0;
    std::string_view name;   // the registry's name (stable while the catalog is); "" when Missing
};
NsHit hitOf(const rune::catalog::NamespaceRecord* r) {
    if (r == nullptr) return {};
    return std::visit([&](const auto& st) -> NsHit {
        using S = std::decay_t<decltype(st)>;
        if constexpr (std::is_same_v<S, rune::catalog::NsActive>) return {NsLookup::Active, r->nsId, r->name};
        else if constexpr (std::is_same_v<S, rune::catalog::NsSuspended>) return {NsLookup::Suspended, r->nsId, r->name};
        else return {};
    }, r->state);
}
/// nullptr catalog (a rig without one): only `default` exists.
NsHit lookupNsId(const rune::catalog::CatalogState* c, uint32_t nsId) {
    if (nsId == rune::catalog::kDefaultNsId && (c == nullptr || c->namespaces().byId(nsId) == nullptr))
        return {NsLookup::Active, rune::catalog::kDefaultNsId, rune::catalog::kDefaultNsName};
    if (c == nullptr) return {};
    return hitOf(c->namespaces().byId(nsId));
}
NsHit lookupNsName(const rune::catalog::CatalogState* c, std::string_view ns) {
    if (ns == rune::catalog::kDefaultNsName) return lookupNsId(c, rune::catalog::kDefaultNsId);
    if (c == nullptr) return {};
    return hitOf(c->namespaces().byName(ns));
}

/// A grant that makes its holder a cluster admin or a namespace admin: ANY grant on Cluster, or ADMIN on a Namespace
/// (DESCRIBE on a Namespace is not one - D11).
bool privilegedGrant(const sec::GrantSpec& g) noexcept {
    return g.resource.type == sec::ResourceType::Cluster ||
           (g.resource.type == sec::ResourceType::Namespace && g.op == sec::Operation::Admin);
}
bool privilegedRole(const sec::RoleObject& r) {
    for (const auto& g : r.grants) if (privilegedGrant(g)) return true;
    return false;
}
/// Directly or through a role; a DISABLED user counts (re-enabling it would restore the privilege).
bool privilegedUser(const sec::SecurityState& state, const sec::UserObject& u) {
    for (const auto& g : u.grants) if (privilegedGrant(g)) return true;
    for (const uint64_t rid : u.roles)
        if (const sec::RoleObject* r = state.roleById(rid); r != nullptr && privilegedRole(*r)) return true;
    return false;
}
} // namespace

GuardVerdict nsGuard(const CallerPerms& caller, uint32_t targetNsId, const SecurityDdl& ddl) {
    if (caller.clusterAdmin) return GuardVerdict::Allow;
    if (targetNsId != caller.nsId) return GuardVerdict::Missing;
    if (!caller.nsAdmin) return caller.denied ? GuardVerdict::NotAuthorized : GuardVerdict::Allow;
    // A namespace admin in its own namespace: everything but minting or touching admin power.
    if (ddl.spec != nullptr && privilegedGrant(*ddl.spec)) return GuardVerdict::NotAuthorized;
    if (ddl.memberRoleIsPrivileged) return GuardVerdict::NotAuthorized;
    if (ddl.targetIsPrivileged && !ddl.targetIsCaller) return GuardVerdict::NotAuthorized;
    return GuardVerdict::Allow;
}

void SecurityAdmin::startDdl_(const SecurityDdlMsg& m, int64_t nowNs, SecurityRequest& req) {
    const sec::SecurityState& state = applier_.state();
    if (statementHasUnredactedPassword(m.meta.statement)) {
        answer_(req, kInvalidRequest, "the statement carries a password literal; the client must send the redacted statement (D10)");
        return;
    }
    const uint64_t fp = fingerprintOf(m);
    // 1. Dedupe (F14) on (requestId, statement fingerprint).  PR #410 review F1: BEFORE the gateway's verdict - a retry
    // of an applied request gets its original result even when the gateway now denies it (the first attempt changed
    // what the check reads: the password the retry is proven with, the ADMIN it needed).
    if (!m.meta.requestId.none()) {
        if (const auto rec = state.request(m.meta.requestId)) {
            if (rec->requestFingerprint == fp) {
                answer_(req, kOK, {}, rec->objectId, rec->newVersion, rec->offset);   // a retry: its ORIGINAL result
            } else if (m.proofChecked != 0) {
                // PR #410 review F2: a reused request id on a proof-checked own change answers the SAME as a bad proof,
                // right proof or wrong - otherwise "requestId reused" for a right one is a password oracle.
                answer_(req, kNotAuthorized, rune::gateway::kOwnCredentialProofRefusal);
            } else if (!m.denial.empty()) {
                answer_(req, kNotAuthorized, m.denial);
            } else {
                answer_(req, kInvalidRequest, "requestId reused for a different statement");
            }
            return;
        }
    }
    // PR #410 review F2: a failed own-credential proof answers ONE refusal, whatever the request names.
    if (m.proofChecked != 0 && !m.denial.empty()) {
        answer_(req, kNotAuthorized, m.denial);
        return;
    }
    // 2. Namespaces Task 8 (spec §4): who is asking, and in which namespace.
    const NsHit callerNs = lookupNsId(catalog_, m.callerNsId);
    const std::string callerNsName = callerNs.kind == NsLookup::Missing ? std::to_string(m.callerNsId)
                                                                         : std::string{callerNs.name};
    CallerPerms caller;
    caller.nsId = m.callerNsId;
    caller.clusterAdmin = m.denial.empty() && m.proofChecked == 0;   // the gateway's ADMIN ON CLUSTER check passed
    caller.denied = !m.denial.empty();
    uint64_t callerUserId = 0;
    if (callerNs.kind != NsLookup::Missing) {
        if (const auto who = rune::catalog::parseQualified(m.meta.authenticated)) {
            if (const sec::UserObject* u = state.userByName(m.callerNsId, who->local)) {
                callerUserId = u->objectId;
                const std::vector<uint64_t> admins = state.nsAdminOf(m.callerNsId, callerNs.name);
                caller.nsAdmin = std::find(admins.begin(), admins.end(), u->objectId) != admins.end();
            }
        }
    }
    // A namespace that does not exist, is being deleted, or is not the caller's (unless a cluster admin asks) answers
    // these SAME bytes: a non-cluster-admin learns nothing about another tenant.
    const auto answerMissing = [&](std::string_view ns) {
        answer_(req, kInvalidRequest, "no namespace '" + std::string{ns} + "'");
    };
    struct Resolved { uint32_t nsId = 0; std::string_view nsName; std::string_view local; };
    // "ns/local" | "local" (the caller's namespace).  nullopt = answered.
    const auto resolve = [&](std::string_view qualified) -> std::optional<Resolved> {
        const auto q = rune::catalog::parseQualified(qualified);
        if (!q) {
            answer_(req, kInvalidRequest, "malformed name '" + std::string{qualified} +
                                              "' (expected <name> or <namespace>/<name>)");
            return std::nullopt;
        }
        const NsHit hit = q->ns.empty() ? callerNs : lookupNsName(catalog_, q->ns);
        const std::string_view nsText = q->ns.empty() ? std::string_view{callerNsName} : q->ns;
        if (hit.kind == NsLookup::Missing || (!caller.clusterAdmin && hit.nsId != m.callerNsId)) {
            answerMissing(nsText);
            return std::nullopt;
        }
        if (hit.kind == NsLookup::Suspended) {
            answer_(req, kInvalidRequest, "namespace '" + std::string{nsText} + "' is suspended");
            return std::nullopt;
        }
        return Resolved{hit.nsId, hit.name, q->local};
    };
    const auto opKind = static_cast<pc::SecurityOpKind>(m.kind);
    const auto target = resolve(m.name);
    if (!target) return;
    // 3. Build the operation from the wire fields.
    sec::SecurityEvent e;
    sec::Ops ops;
    bool roleTarget = false;
    bool create = false;
    std::optional<sec::GrantSpec> spec;
    std::optional<Resolved> memberRole;   // GrantRole / RevokeRole
    switch (opKind) {
    case pc::SecurityOpKind::CreateUser: ops = sec::CreateUserOps{m.verifier}; create = true; break;
    case pc::SecurityOpKind::AlterUser:
        switch (static_cast<pc::UserChangeKind>(m.change)) {
        case pc::UserChangeKind::AddPassword:  ops = sec::AlterUserOps{sec::AddPassword{m.verifier}}; break;
        case pc::UserChangeKind::DropPassword: ops = sec::AlterUserOps{sec::DropPassword{m.passwordIndex}}; break;
        case pc::UserChangeKind::SetPassword:  ops = sec::AlterUserOps{sec::SetPassword{m.verifier, m.terminateSessions != 0}}; break;
        case pc::UserChangeKind::Disable:      ops = sec::AlterUserOps{sec::DisableUser{}}; break;
        case pc::UserChangeKind::Enable:       ops = sec::AlterUserOps{sec::EnableUser{}}; break;
        }
        break;
    case pc::SecurityOpKind::DropUser:   ops = sec::DropUserOps{}; break;
    case pc::SecurityOpKind::CreateRole: ops = sec::CreateRoleOps{}; create = true; roleTarget = true; break;
    case pc::SecurityOpKind::DropRole:   ops = sec::DropRoleOps{}; roleTarget = true; break;
    case pc::SecurityOpKind::Grant:
    case pc::SecurityOpKind::Revoke: {
        spec = sec::GrantSpec{static_cast<sec::Operation>(m.op),
                              {static_cast<sec::ResourceType>(m.resourceType),
                               static_cast<sec::PatternKind>(m.patternKind), m.pattern}};
        const auto grantee = static_cast<sec::GranteeKind>(m.granteeKind);
        if (opKind == pc::SecurityOpKind::Grant) ops = sec::GrantOps{grantee, *spec};
        else ops = sec::RevokeOps{grantee, *spec};
        roleTarget = grantee == sec::GranteeKind::Role;
        // A Namespace grant names a namespace: another tenant's reads as Missing to a non-cluster-admin (row 3).
        if (spec->resource.type == sec::ResourceType::Namespace && !caller.clusterAdmin &&
            spec->resource.name != callerNsName) {
            answerMissing(spec->resource.name);
            return;
        }
        break;
    }
    case pc::SecurityOpKind::GrantRole:
    case pc::SecurityOpKind::RevokeRole:
        memberRole = resolve(m.roleName);
        if (!memberRole) return;
        break;
    default:
        answer_(req, kInvalidRequest, "unknown security op kind");
        return;
    }
    // 4. The target object (ids and versions come from the applied state - the leader's serialization) and the
    // membership role, looked up in their RESOLVED namespaces.  An unknown one is answered only after the guard (a
    // denied caller learns nothing about which objects exist).
    const sec::UserObject* targetUser = (create || roleTarget) ? nullptr : state.userByName(target->nsId, target->local);
    const sec::RoleObject* targetRole = (!create && roleTarget) ? state.roleByName(target->nsId, target->local) : nullptr;
    const sec::RoleObject* role = memberRole ? state.roleByName(memberRole->nsId, memberRole->local) : nullptr;
    SecurityDdl view;
    view.spec = spec ? &*spec : nullptr;
    view.memberRoleIsPrivileged = role != nullptr && privilegedRole(*role);
    view.targetIsPrivileged = (targetUser != nullptr && privilegedUser(state, *targetUser)) ||
                              (targetRole != nullptr && privilegedRole(*targetRole));
    view.targetIsCaller = targetUser != nullptr && callerUserId != 0 && targetUser->objectId == callerUserId;
    switch (nsGuard(caller, target->nsId, view)) {
    case GuardVerdict::Allow: break;
    case GuardVerdict::Missing: answerMissing(callerNsName); return;   // unreachable: resolve() hid a foreign target
    case GuardVerdict::NotAuthorized:
        answer_(req, kNotAuthorized, caller.denied ? std::string_view{m.denial}
                                                   : std::string_view{"a namespace admin cannot grant, revoke or change "
                                                                      "cluster or namespace admin rights"});
        return;
    }
    if (memberRole) {
        if (role == nullptr) {
            answer_(req, kInvalidRequest, std::string{sec::refusalName(sec::Refusal::UnknownRole)} + ": no role '" +
                                              m.roleName + "'");
            return;
        }
        if (opKind == pc::SecurityOpKind::GrantRole) ops = sec::GrantRoleOps{role->objectId, role->name};
        else ops = sec::RevokeRoleOps{role->objectId, role->name};
    }
    // D10/D16: the front end has the catalog, so a grant is checked against the grantee's REAL namespace name (the
    // apply rules only check a Namespace pattern's shape) - ADMIN ON NAMESPACE a for a role of b is refused here.
    if (opKind == pc::SecurityOpKind::Grant && !sec::validGrant(target->nsId, target->nsName, *spec)) {
        answer_(req, kInvalidRequest, std::string{sec::refusalName(sec::Refusal::InvalidGrant)} + ": that grant is not "
                                          "valid for '" + m.name + "'");
        return;
    }
    e.nsId = target->nsId;
    e.kind = sec::kindOf(ops);
    e.ops = std::move(ops);
    e.objectName = std::string{target->local};
    // Issue #228: rendered from the op, never the client's text (the audit log cannot be made to lie).
    e.statement = sec::securityStatementText(m.kind, m.name, m.change, m.passwordIndex, m.terminateSessions, m.force,
                                             m.granteeKind, m.op, m.resourceType, m.patternKind, m.pattern, m.roleName);
    if (create) {
        e.objectId = state.nextObjectId();
        e.prevVersion = 0;
    } else if (roleTarget) {
        if (targetRole == nullptr) {
            answer_(req, kInvalidRequest, std::string{sec::refusalName(sec::Refusal::UnknownRole)} + ": no role '" + m.name + "'");
            return;
        }
        e.objectId = targetRole->objectId;
        e.prevVersion = targetRole->version;
    } else {
        if (targetUser == nullptr) {
            answer_(req, kInvalidRequest, std::string{sec::refusalName(sec::Refusal::UnknownUser)} + ": no user '" + m.name + "'");
            return;
        }
        e.objectId = targetUser->objectId;
        e.prevVersion = targetUser->version;
    }
    e.newVersion = e.prevVersion + 1;
    e.principal = principalOf(m.meta, m.meta.authenticated, fp);
    e.timestampMs = wallClockMs();
    // Issue #121: the last-admin guard.  Removing the last ENABLED user that effectively holds ADMIN ON CLUSTER (a
    // DISABLE, a DROP USER, a REVOKE of ADMIN or of the role carrying it, a DROP ROLE ...) reopens the bootstrap window:
    // only the cluster-key holder can then manage security.  Refused unless the statement says FORCE.  Decided on the
    // event itself - the same frozen rules every broker applies, dry-run on a copy of the state (cold: DDL rate) - so
    // every path that can remove the last admin is covered, not a list of statement kinds.
    if (m.force == 0 && state.hasSecurityAdmin() && removesLastSecurityAdmin(state, e)) {
        answer_(req, kInvalidRequest, "this removes the last enabled security admin (no enabled user would hold ADMIN ON "
                                      "CLUSTER): create or enable another admin first, or say FORCE");
        return;
    }
    (void)appendEvent_(req, std::move(e), nowNs);
}

void SecurityAdmin::startBootstrap_(const SecurityBootstrapMsg& m, int64_t nowNs, SecurityRequest& req) {
    const sec::SecurityState& state = applier_.state();
    const uint64_t fp = fingerprintOf(m);
    // The bootstrap's identity is its nonce (the MAC covers it): a retry after a leader change dedupes on it.
    sec::RequestId rid;
    std::memcpy(&rid.hi, m.nonce.data(), 8);
    std::memcpy(&rid.lo, m.nonce.data() + 8, 8);
    if (const auto rec = state.request(rid)) {
        if (rec->requestFingerprint == fp) answer_(req, kOK, {}, rec->objectId, rec->newVersion, rec->offset);
        else answer_(req, kInvalidRequest, "requestId reused for a different statement");
        return;
    }
    if (state.hasSecurityAdmin()) {
        answer_(req, kInvalidRequest, "security is already bootstrapped (a security admin exists)");
        return;
    }
    sec::BootstrapOps ops{m.verifier, m.fakeSaltSeed};
    sec::SecurityEvent e;
    e.kind = sec::EventKind::Bootstrap;
    e.ops = std::move(ops);
    e.objectName = m.adminName;
    e.statement = "BOOTSTRAP USER " + m.adminName + " WITH PASSWORD '<redacted>';";
    e.objectId = state.nextObjectId();
    e.prevVersion = 0;
    e.newVersion = 1;
    e.principal = principalOf(m.meta, "bootstrap", fp);   // D3
    e.principal.requestId = rid;
    e.timestampMs = wallClockMs();
    (void)appendEvent_(req, std::move(e), nowNs);
}

bool SecurityAdmin::appendEvent_(SecurityRequest& req, sec::SecurityEvent e, int64_t nowNs) {
    const sec::SecurityState& state = applier_.state();
    // The dry run is the gate: the SAME frozen rules every broker applies, on the state this append extends.
    auto fp = sec::previewFingerprint(state, e);
    if (!fp) {
        const sec::ApplyError& err = fp.error();
        if (err.refusal && *err.refusal == sec::Refusal::AlreadyBootstrapped)
            answer_(req, kInvalidRequest, "security is already bootstrapped (a security admin exists)");
        else if (err.refusal || err.code == sec::ApplyError::Code::Precondition || err.code == sec::ApplyError::Code::BadOps)
            answer_(req, kInvalidRequest, refusalText(err));
        else
            answer_(req, kInternal, refusalText(err));
        return false;
    }
    e.fingerprint = *fp;
    if (auto a = sec::previewApplied(state, e); !a) {   // the leader's dry run, fingerprint included
        answer_(req, kInternal, "the leader's own event does not apply: " + refusalText(a.error()));
        return false;
    }
    if (auto enc = sec::encodeInto(e, eventBuf_); !enc) {
        if (enc.error() == sec::EncodeError::KindOpsMismatch || enc.error() == sec::EncodeError::UnknownKind ||
            enc.error() == sec::EncodeError::UnknownFormatVer) {
            spdlog::error("AdminWorker: a __security event does not encode ({}) - a leader bug; answered Internal, "
                          "nothing appended", sec::encodeErrorName(enc.error()));
            answer_(req, kInternal, "the security event cannot be encoded");
        } else {
            answer_(req, kInvalidRequest, "the security event cannot be encoded: " + std::string{sec::encodeErrorName(enc.error())});
        }
        return false;
    }
    if (eventBuf_.size() > kSecurityMaxEventBytes) {
        answer_(req, kInvalidRequest, "the security event is " + std::to_string(eventBuf_.size()) +
                                          " bytes, larger than the " + std::to_string(kSecurityMaxEventBytes) + "-byte limit");
        return false;
    }
    uint8_t key[8];
    for (int i = 0; i < 8; ++i) key[i] = static_cast<uint8_t>((e.objectId >> (8 * i)) & 0xff);
    const rune::replication::Offset base = replica_->appendKeyed(
        std::span<const uint8_t>(key, 8), std::span<const uint8_t>(eventBuf_.data(), eventBuf_.size()), /*acksAll=*/true);
    if (base < 0) {
        answer_(req, kNotLeader, "__security is below its minIsr");   // spec §3: retriable
        return false;
    }
    ++eventsAppended_;
    replica_->onLeaderLogAppended();   // reconcile LEO / HW, push to followers
    inFlight_ = AwaitingCommit{std::move(req), std::move(e), base, nowNs};
    return true;
}

} // namespace rune::node
