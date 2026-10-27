#pragma once
// src/security/ApplyV2.h — the formatVer-2 apply rules (namespaces plan D10).  The object rules are the shared ones of
// ApplyV1.cpp, scoped to the event's nsId (users and roles are keyed (nsId, local); a role membership stays inside one
// namespace; a Bootstrap is a default-namespace event; Cluster grants exist in the default namespace only; Namespace
// and Truncate grants are valid).  What formatVer 2 adds on top is the fingerprint: the resulting object's v1
// fingerprint mixed with the nsId (rune::catalog::objectFingerprintV2), exactly as __catalog's v2.
//
// There is no namespace-existence gate here: the security codec has no catalog.  The broker's SecurityAdmin, which
// reads the admin thread's CatalogState, refuses an event for a namespace that does not exist or is not ACTIVE before
// it writes it (plan Task 8).
#include "ApplyV1.h"

#include <rune/security/SecurityState.h>

#include <cstdint>
#include <expected>

namespace rune::security {

struct ApplyV2 {
    /// Check every rule against `s` and build the change; its Applied.fingerprint is the v2 one.  Never mutates `s`.
    static std::expected<Staged, ApplyError> run(const SecurityState& s, const SecurityEvent& e, int64_t offset);
    /// Install a change `run` produced on this very state.
    static Applied commit(SecurityState& s, const SecurityEvent& e, int64_t offset, Staged&& st);
};

} // namespace rune::security
