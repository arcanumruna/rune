#pragma once
// Authorizer.h - the spec §2.1 table, in ONE place (authentication Task 12).  Every gateway handler asks it, and it is
// the only place a NotAuthorized code or detail text is chosen (D23): `not authorized: <OP> on <TYPE> <requested name
// or id>`, built ONLY from the request, so a hidden object and a missing one answer identically.
//
// Pure functions of (table, request): no allocation on the granted path, nor on a refusal (the Denial text is a fixed
// buffer, #263).
#include <rune/gateway/IAdminRequestSink.h>   // kKindTopic / kKindView / kKindAll
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionTable.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace rune::gateway {

/// The longest part of a requested name the detail text carries (cut identically for hidden and missing objects).
inline constexpr std::size_t kMaxNameInDetail = 256;
/// "not authorized: " (16) + the longest operation name (TRUNCATE / DESCRIBE, 8) + " on " (4) + the longest type name
/// (TRANSACTIONAL_ID, 16) + ' ' (1) + kMaxNameInDetail + "..." (3), rounded up to a multiple of 16.  The text built by
/// the Authorizer always fits (a unit test pins the longest case); `append` truncates silently should it ever not.
inline constexpr std::size_t kDenialDetailBytes = ((16 + 8 + 4 + 16 + 1 + kMaxNameInDetail + 3) + 15) / 16 * 16;

/// A refusal: the code and the D23 detail text, in a fixed buffer so a denied request allocates nothing (#263).
struct Denial {
    uint16_t code = 45;   // ErrorCode::NotAuthorized
    std::array<char, kDenialDetailBytes> text{};
    uint16_t len = 0;     // kDenialDetailBytes does not fit a byte: the name part alone may be kMaxNameInDetail (256)

    [[nodiscard]] std::string_view detail() const noexcept { return {text.data(), len}; }   // D23
    /// Appends as much of `s` as fits; truncates silently, never overflows.
    void append(std::string_view s) noexcept {
        const std::size_t room = text.size() - len;
        const std::size_t n = s.size() < room ? s.size() : room;
        for (std::size_t i = 0; i < n; ++i) text[len + i] = s[i];
        len = static_cast<uint16_t>(len + n);
    }
};

/// Id-addressed (Produce / Subscribe / group topics / txn partitions / views): `op` on the object `topicId`.  D22: a
/// non-live id is NotAuthorized unless the session holds DESCRIBE ON CLUSTER (then no denial: the request's existing
/// "unknown topic" answer follows).  The pass-through is for user object ids (>= 0) only: a negative (internal) id is
/// never passed through.  `shownType` is the type named in the detail text (the request's, never the
/// object's: the text must not reveal what the id is).
[[nodiscard]] std::optional<Denial> authorizeTopicId(const PermissionTable&, int32_t topicId, rune::security::Operation,
                                                     const ObjectIndex&,
                                                     rune::security::ResourceType shownType = rune::security::ResourceType::Topic);
/// A view plan's source: the caller must hold READ on the id, full stop.  Unlike authorizeTopicId there is NO D22 pass-through:
/// the admin thread resolves the id later against a catalog that may be ahead of this thread's ObjectIndex, so a
/// not-yet-indexed id (a CREATE TOPIC in flight) must not pass for a DESCRIBE ON CLUSTER holder.
[[nodiscard]] std::optional<Denial> authorizeViewSource(const PermissionTable&, int32_t topicId);
/// Name-addressed: `op` on (type, name) - matched against the requested name whether or not the object exists.
/// Issue #640: `nsId` is the TARGET's resolved namespace (NsResolve.h).  A non-root table never authorizes a name of
/// another namespace (defence in depth behind NsResolve's hide; grants are per namespace, spec §11).  kHiddenNsId
/// (already answered as missing) and a root table pass through.  Required: no caller may forget it.
[[nodiscard]] std::optional<Denial> authorizeName(const PermissionTable&, rune::security::ResourceType, std::string_view,
                                                  rune::security::Operation, uint32_t nsId);
/// GetTopicSchema (#504, #505): the object kinds under `name` that `t` may `op`, as a bit set of kKindTopic |
/// kKindView (IAdminRequestSink.h); 0 = none (the caller answers NotAuthorized).  The gateway never consults its
/// ObjectIndex here: it lags the admin thread, which serves the object only if its real kind is in the set.
[[nodiscard]] uint8_t allowedKindsForName(const PermissionTable&, std::string_view, rune::security::Operation,
                                          uint32_t nsId) noexcept;
[[nodiscard]] std::optional<Denial> authorizeCluster(const PermissionTable&, rune::security::Operation);
/// #211: `op` on at least one resource of `type` (no resource is named: a non-transactional InitProducerId needs
/// WRITE on some topic, Kafka's rule).  Detail: `not authorized: <OP> on any <TYPE>`.
[[nodiscard]] std::optional<Denial> authorizeAnyGrant(const PermissionTable&, rune::security::ResourceType,
                                                      rune::security::Operation);

} // namespace rune::gateway
