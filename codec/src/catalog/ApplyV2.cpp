// src/catalog/ApplyV2.cpp — the formatVer-2 apply rules (plan D5, spec §3).
//
//   object kinds 1..9   the namespace named by e.nsId exists (else BadNsId) and is ACTIVE (else Precondition); then the
//                       shared object rules of ApplyV1.cpp, with every name lookup scoped to e.nsId.  A reference
//                       (a view's source) or an objectId that names another namespace's object is "not live" here.
//   NamespaceCreated    e.nsId == nextNsId() (else BadNsId); name valid (NsName.h) and not "default"; e.objectName ==
//                       the name; the name is free; versions 0 -> 1; the record is ACTIVE with the event's limits
//   NamespaceLimitsSet  the namespace exists (else BadNsId); versions chain; the FULL limits record replaces the old
//   NamespaceStateChanged
//                       the namespace exists; versions chain; edges ACTIVE <-> SUSPENDED(mode) and
//                       ACTIVE | SUSPENDED -> DELETING only; nsId 0 never enters DELETING
//   NamespaceDeleted    the namespace exists, is DELETING, is not nsId 0; versions chain.  The id is never reused:
//                       nextNsId() does not move back
//   every kind          a requestId other than {0,0} not already applied; the event's own values valid
#include "ApplyV2.h"
#include "Wire.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogFingerprintV2.h>
#include <rune/codec/Fingerprint.h>
#include <rune/catalog/NsName.h>
#include <rune/catalog/TopicName.h>

#include <limits>
#include <string>
#include <utility>

namespace rune::catalog {
namespace {

using Code = ApplyError::Code;
using Result = std::expected<StagedV2, ApplyError>;

std::unexpected<ApplyError> fail(int64_t offset, Code code, std::string detail) {
    return std::unexpected(ApplyError{offset, code, 0, 0, std::move(detail)});
}
std::string quoted(std::string_view s) { return "'" + escapeNameForLog(s) + "'"; }
std::string prefix(const CatalogEvent& e) { return std::string(kindName(e.kind)) + ": "; }
bool isNamespaceKind(EventKind k) noexcept {
    const auto v = static_cast<uint16_t>(k);
    return v >= kReservedKindFirst && v <= kLastNamespaceKind;
}

/// The namespace `e` names, which must exist.
std::expected<const NamespaceRecord*, ApplyError> subject(const CatalogState& s, const CatalogEvent& e, int64_t off) {
    const NamespaceRecord* r = s.namespaces().byId(e.nsId);
    if (r == nullptr)
        return fail(off, Code::BadNsId, prefix(e) + "namespace " + std::to_string(e.nsId) + " does not exist");
    if (e.objectName != r->name)
        return fail(off, Code::Precondition, prefix(e) + "nsId " + std::to_string(e.nsId) + " is namespace " +
                                                 quoted(r->name) + ", not " + quoted(e.objectName));
    if (r->version == std::numeric_limits<uint32_t>::max())
        return fail(off, Code::Precondition, prefix(e) + "namespace version " + std::to_string(r->version) + " is exhausted");
    if (e.prevVersion != r->version || e.newVersion != r->version + 1)
        return fail(off, Code::Precondition, prefix(e) + "version chain " + std::to_string(e.prevVersion) + " -> " +
                                                 std::to_string(e.newVersion) + ", namespace at " + std::to_string(r->version));
    return r;
}

Result created(const CatalogState& s, const CatalogEvent& e, int64_t off, const NamespaceCreatedOps& o) {
    const uint32_t next = s.namespaces().nextNsId();
    if (e.nsId != next)
        return fail(off, Code::BadNsId, prefix(e) + "nsId " + std::to_string(e.nsId) + ", next is " + std::to_string(next));
    if (next == std::numeric_limits<uint32_t>::max()) return fail(off, Code::Precondition, prefix(e) + "namespace ids are exhausted");
    if (!validNamespaceName(o.name))
        return fail(off, Code::Precondition, prefix(e) + "namespace name " + quoted(o.name) +
                                                 " is invalid ([a-z0-9][a-z0-9-]{0,62}, not \"default\")");
    if (e.objectName != o.name)
        return fail(off, Code::Precondition, prefix(e) + "objectName " + quoted(e.objectName) + " is not the namespace name " + quoted(o.name));
    if (s.namespaces().byName(o.name) != nullptr)
        return fail(off, Code::Precondition, prefix(e) + "namespace name " + quoted(o.name) + " is taken");
    if (e.prevVersion != 0 || e.newVersion != 1)
        return fail(off, Code::Precondition, prefix(e) + "version chain " + std::to_string(e.prevVersion) + " -> " +
                                                 std::to_string(e.newVersion) + ", a new namespace is 0 -> 1");
    NamespaceRecord r{.nsId = e.nsId, .name = o.name, .limits = o.limits, .state = NsActive{}, .version = 1, .fingerprint = 0};
    r.fingerprint = nsFingerprint(r.nsId, r.name, r.limits, NsStateTag::Active);
    return StagedNsCreate{std::move(r)};
}

Result limitsSet(const CatalogState& s, const CatalogEvent& e, int64_t off, const NamespaceLimitsSetOps& o) {
    auto cur = subject(s, e, off);
    if (!cur) return std::unexpected(std::move(cur.error()));
    NamespaceRecord r = **cur;
    r.limits = o.limits;
    r.version = e.newVersion;
    r.fingerprint = nsFingerprint(r.nsId, r.name, r.limits, tagOf(r.state));
    return StagedNsUpdate{std::move(r)};
}

/// D5: ACTIVE <-> SUSPENDED(mode); ACTIVE | SUSPENDED -> DELETING.  Nothing leaves DELETING but Deleted.
bool stateEdgeAllowed(const NsState& from, NsStateTag to) noexcept {
    if (std::holds_alternative<NsDeleting>(from)) return false;
    if (std::holds_alternative<NsActive>(from)) return to != NsStateTag::Active;
    return to == NsStateTag::Active || to == NsStateTag::Deleting;   // SUSPENDED: resume or delete, not a mode switch
}

Result stateChanged(const CatalogState& s, const CatalogEvent& e, int64_t off, const NamespaceStateChangedOps& o) {
    auto cur = subject(s, e, off);
    if (!cur) return std::unexpected(std::move(cur.error()));
    if (!stateEdgeAllowed((*cur)->state, o.to))
        return fail(off, Code::Precondition, prefix(e) + "namespace " + quoted((*cur)->name) + " cannot go from state " +
                                                 std::to_string(static_cast<int>(tagOf((*cur)->state))) + " to " +
                                                 std::to_string(static_cast<int>(o.to)));
    if (e.nsId == kDefaultNsId && o.to == NsStateTag::Deleting)
        return fail(off, Code::Precondition, prefix(e) + "the default namespace is never deleted");
    NamespaceRecord r = **cur;
    r.state = stateOf(o.to);
    r.version = e.newVersion;
    r.fingerprint = nsFingerprint(r.nsId, r.name, r.limits, o.to);
    return StagedNsUpdate{std::move(r)};
}

Result deleted(const CatalogState& s, const CatalogEvent& e, int64_t off) {
    auto cur = subject(s, e, off);
    if (!cur) return std::unexpected(std::move(cur.error()));
    if (e.nsId == kDefaultNsId) return fail(off, Code::Precondition, prefix(e) + "the default namespace is never deleted");
    if (!std::holds_alternative<NsDeleting>((*cur)->state))
        return fail(off, Code::Precondition, prefix(e) + "namespace " + quoted((*cur)->name) + " is not DELETING");
    return StagedNsDelete{e.nsId, (*cur)->name, e.newVersion};
}

} // namespace

std::expected<StagedV2, ApplyError> ApplyV2::run(const CatalogState& s, const CatalogEvent& e, int64_t offset) {
    if (auto v = wire::validateEvent(e))
        return fail(offset, Code::BadOps, prefix(e) + "invalid event: " + std::string(encodeErrorName(*v)));
    if (!e.principal.requestId.none() && s.request(e.principal.requestId))
        return fail(offset, Code::Precondition, prefix(e) + "requestId " + std::to_string(e.principal.requestId.hi) + ":" +
                                                    std::to_string(e.principal.requestId.lo) + " is already applied");
    if (isNamespaceKind(e.kind)) {
        return std::visit([&](const auto& o) -> Result {
            using T = std::decay_t<decltype(o)>;
            if constexpr (std::is_same_v<T, NamespaceCreatedOps>)           return created(s, e, offset, o);
            else if constexpr (std::is_same_v<T, NamespaceLimitsSetOps>)    return limitsSet(s, e, offset, o);
            else if constexpr (std::is_same_v<T, NamespaceStateChangedOps>) return stateChanged(s, e, offset, o);
            else if constexpr (std::is_same_v<T, NamespaceDeletedOps>)      return deleted(s, e, offset);
            else return fail(offset, Code::BadOps, prefix(e) + "not a namespace body");   // unreachable: kind/ops agree
        }, e.ops);
    }
    // object kinds: the gate, then the shared object rules in this namespace
    const NamespaceRecord* ns = s.namespaces().byId(e.nsId);
    if (ns == nullptr)
        return fail(offset, Code::BadNsId, prefix(e) + "namespace " + std::to_string(e.nsId) + " does not exist");
    if (!std::holds_alternative<NsActive>(ns->state))
        return fail(offset, Code::Precondition, prefix(e) + "namespace " + quoted(ns->name) + " is not ACTIVE");
    auto staged = ApplyV1::run(s, e, offset);
    if (!staged) return std::unexpected(std::move(staged.error()));
    return StagedV2{std::move(*staged)};
}

uint64_t ApplyV2::fingerprintOfStaged(const CatalogEvent& e, const StagedV2& st) noexcept {
    return std::visit([&](const auto& x) -> uint64_t {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, Staged>)              return objectFingerprintV2(ApplyV1::fingerprintOfStaged(x), e.nsId);
        else if constexpr (std::is_same_v<T, StagedNsDelete>) return nsTombstoneFingerprint(x.nsId, x.name, x.finalVersion);
        else                                                  return x.record.fingerprint;
    }, st);
}

Applied ApplyV2::commit(CatalogState& s, const CatalogEvent& e, int64_t offset, uint64_t fingerprint, StagedV2&& st) {
    if (auto* obj = std::get_if<Staged>(&st)) return ApplyV1::commit(s, e, offset, fingerprint, std::move(*obj));

    NamespaceRegistry& reg = s.namespaces_;
    std::visit([&](auto&& x) {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_same_v<T, StagedNsCreate>) {
            reg.byName_.emplace(x.record.name, x.record.nsId);
            reg.nextNsId_ = x.record.nsId + 1;
            reg.byId_.emplace(x.record.nsId, std::move(x.record));
        } else if constexpr (std::is_same_v<T, StagedNsUpdate>) {
            reg.byId_.at(x.record.nsId) = std::move(x.record);
        } else if constexpr (std::is_same_v<T, StagedNsDelete>) {
            reg.byName_.erase(x.name);   // nextNsId_ stays: the id is never reused
            reg.byId_.erase(x.nsId);
            reg.tombstoneFold_ = detail::fnvU64(reg.tombstoneFold_ == 0 ? rune::codec::detail::kFnvSeed : reg.tombstoneFold_,
                                                fingerprint);
        }
    }, st);

    s.appliedOffset_ = offset;
    if (!e.principal.requestId.none())
        s.requests_.emplace(e.principal.requestId,
                            RequestRecord{.offset = offset, .kind = e.kind, .objectId = e.objectId, .newVersion = e.newVersion,
                                          .topicId = -1, .schemaVersion = 0, .requestFingerprint = e.principal.requestFingerprint});
    return Applied{.kind = e.kind, .objectId = e.objectId, .newVersion = e.newVersion, .fingerprint = fingerprint,
                   .createdTopicId = -1, .droppedTopicId = -1, .nsId = e.nsId};
}

} // namespace rune::catalog
