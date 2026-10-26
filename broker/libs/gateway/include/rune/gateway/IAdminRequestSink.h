#pragma once
// IAdminRequestSink.h — the gateway's abstract sink for DDL requests, owned by
// the gateway lib so ClientGateway depends only on this interface rather than on
// rune_node symbols (mirrors ITxnRequestHandler / IGroupRequestHandler).
//
// The AdminWorkerAgent (in rune_node) implements it by ENCODING the request onto
// the gateway→admin SPSC ring.  The gateway decodes the frame, requires a known
// session, copies the fields into the sink and RETURNS IMMEDIATELY — no registry
// access, no blocking on the gateway hot path.  DDL therefore never delays
// produce routing (SchemaGatewayRegression).
//
// Catalog history Task 9: every DDL submit carries a DdlMetaView — the DDL
// extension the client sent (statement, client name, request id, replayedFrom;
// empty with a {0, 0} request id for a pre-v8 frame) plus the gateway's own
// session id and the session's egress channel as the source address (ruling 13).
// Every view points into the ingress buffer and MUST NOT be retained past return.
//
// Namespace: rune::gateway.  C++23.

#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/NamespaceMessages.h>
#include <rune/protocol/client/SecurityMessages.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

namespace rune::gateway {

/// Where a replayed DDL came from (`rune catalog replay`, spec §7).
struct ReplayedFromView {
    std::string_view clusterId;
    int64_t          offset = -1;
    std::string_view principal;
};

/// The principal + statement of one DDL (catalog history spec §4; ruling 13).
struct DdlMetaView {
    std::string_view                statement;
    std::string_view                clientName;
    std::string_view                sourceAddress;   // the session's egress channel (gateway-supplied)
    std::string_view                authenticated;   // the session's user (authentication Task 10; "" = anonymous)
    int64_t                         sessionId = 0;   // the session (gateway-supplied)
    rune::protocol::RequestId       requestId{};     // {0, 0} = a pre-v8 client (F14)
    std::optional<ReplayedFromView> replayedFrom;    // absent unless the DDL is a replay
};

/// The outcome of handing a request to the admin thread.
///   Accepted — enqueued; the admin thread answers it.
///   RingFull — back-pressure (retriable): the gateway answers Internal.
///   TooLarge — the request can never fit one admin ring message: the gateway
///              answers InvalidRequest with a detail (permanent, never a retry loop).
enum class AdminSubmit : uint8_t { Accepted, RingFull, TooLarge };

/// Catalog history Task 12: the ids of `__catalog` subscriptions (served by the
/// admin thread's CatalogReadServer) have bit 62 set; a gateway subscription id
/// (node prefix << 40 | a 40-bit counter) never does — an Unsubscribe is routed
/// by its id alone.
inline constexpr int64_t kCatalogSubscriptionIdBase = int64_t{1} << 62;
[[nodiscard]] constexpr bool isCatalogSubscriptionId(int64_t subscriptionId) noexcept {
    return subscriptionId > 0 && (subscriptionId & kCatalogSubscriptionIdBase) != 0;
}

/// Authentication Task 10 (D39): `__security` subscriptions (served by the admin thread's SECOND read server) carry bit 62
/// like every admin-log subscription AND bit 61 - a distinct id space from `__catalog`'s, so one id names one server.
inline constexpr int64_t kSecuritySubscriptionIdBit = int64_t{1} << 61;
[[nodiscard]] constexpr bool isSecuritySubscriptionId(int64_t subscriptionId) noexcept {
    return isCatalogSubscriptionId(subscriptionId) && (subscriptionId & kSecuritySubscriptionIdBit) != 0;
}

/// Authentication Task 14: a live permission refresh ends ONE kind of admin-log read of a session (the session stays).
/// Pseudo subscription ids of an Unsubscribe: -1 (session close) ends both servers' reads, these end one server's.
inline constexpr int64_t kEndCatalogReadsOnly  = -2;   ///< every `__catalog` read of the session
inline constexpr int64_t kEndSecurityReadsOnly = -3;   ///< every `__security` read of the session

struct IAdminRequestSink {
    virtual ~IAdminRequestSink() = default;

    // DefineTopic (64).  An empty descriptor is an UNTYPED topic (ruling 3).
    // v8: `replicationFactor` (0 = every broker) and the creation `config`
    // (the defaults for a pre-v8 frame).
    // `adoptDenial` (issue #129): the gateway's verdict on the request turning into a NEW SCHEMA VERSION of an
    // existing untyped topic (CreateSchemaOps), which needs ALTER on the topic (as AlterTopic), not only CREATE on
    // the name.  Only the admin thread knows whether the name exists, so the gateway sends its verdict along: "" = the
    // session may adopt, otherwise the NotAuthorized detail (D23) the admin thread answers if adoption is what the
    // request would do.
    virtual AdminSubmit submitDefineTopic(int64_t sessionId, int64_t correlationId,
                                          std::string_view name, uint32_t partitionCount,
                                          std::string_view keyField, bool validate,
                                          const uint8_t* descriptorBlob, uint32_t descriptorLen,
                                          uint8_t persistence,   // Phase 2b E: 0 disk, 1 transient
                                          int16_t replicationFactor,
                                          const rune::protocol::TopicConfigView& config,
                                          const DdlMetaView& meta, std::string_view adoptDenial) = 0;

    // GetTopicSchema (66) — a read, no DDL extension.  requestedFingerprint
    // (Task 13): 0 = latest; non-zero = the SPECIFIC version whose per-version
    // fingerprint matches.  true = enqueued, false = ring full.
    virtual bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                                 std::string_view name,
                                 uint64_t requestedFingerprint) = 0;

    // AlterTopic (68).  expectedVersion: 0 = unconditional; non-zero = the
    // topic's schema version (SchemaConflict on a mismatch).  opsBlob:
    // rune::codec::encodeAlterOps() wire bytes (opaque to the gateway).
    virtual AdminSubmit submitAlterTopic(int64_t sessionId, int64_t correlationId,
                                         std::string_view topicName, uint16_t expectedVersion,
                                         const uint8_t* opsBlob, uint32_t opsLen,
                                         const DdlMetaView& meta) = 0;

    // CreateView (70): the authored ViewPlan (serializeAuthored) in planBlob.
    virtual AdminSubmit submitCreateView(int64_t sessionId, int64_t correlationId,
                                         std::string_view viewName, std::string_view keyField,
                                         uint8_t persistence, const uint8_t* planBlob, uint32_t planLen,
                                         const DdlMetaView& meta) = 0;

    // Catalog history (client version 8): DropTopic (74), AlterTopicConfig (75),
    // DropView (76), AlterView (77) — each answered by a DdlResponse (78).
    virtual AdminSubmit submitDropTopic(int64_t sessionId, int64_t correlationId,
                                        std::string_view name, const DdlMetaView& meta) = 0;
    /// `changes` = the wire's count-prefixed (key:u8, value:i64)* block
    /// (SchemaMessages.h AlterTopicConfig; keys already range-checked by its decode).
    virtual AdminSubmit submitAlterTopicConfig(int64_t sessionId, int64_t correlationId,
                                               std::string_view name, uint32_t expectedVersion,
                                               std::span<const uint8_t> changes,
                                               const DdlMetaView& meta) = 0;
    virtual AdminSubmit submitDropView(int64_t sessionId, int64_t correlationId,
                                       std::string_view name, const DdlMetaView& meta) = 0;
    virtual AdminSubmit submitAlterView(int64_t sessionId, int64_t correlationId,
                                        std::string_view name, std::string_view keyField,
                                        uint8_t persistence, uint32_t expectedVersion,
                                        const uint8_t* planBlob, uint32_t planLen,
                                        const DdlMetaView& meta) = 0;

    // Catalog history Task 12 (spec §7, F19): a Subscribe on `__catalog`
    // (topic id −6, partition 0) — served by the admin thread from this broker's
    // replica up to its HW; the admin thread sends the SubscribeResponse, the
    // Records and the caught-up marker.  true = enqueued, false = ring full (the
    // gateway answers the retriable NotLeader).
    virtual bool submitCatalogSubscribe(int64_t sessionId, int64_t correlationId, int64_t fromOffset) = 0;
    // An Unsubscribe of a catalog subscription id; subscriptionId −1 = every
    // catalog subscription of the session (its close).  false = ring full (the
    // gateway retries it — never dropped).
    virtual bool submitCatalogUnsubscribe(int64_t sessionId, int64_t subscriptionId) = 0;

    // Authentication Task 10 (D36): the security DDL (79) and the one-time bootstrap (80), both answered by a
    // DdlResponse (78) from the admin thread; `meta.statement` is the REDACTED statement.  The bootstrap's MAC was
    // verified by the gateway before this call.
    // `denial` (PR #410 review F1): the gateway's authorization verdict - "" = authorized, otherwise the NotAuthorized
    // detail (no ADMIN, or an own credential change without a valid proof).  A request that carries a request id is
    // forwarded even when denied: the leader first answers a RETRY of an already-applied request (its dedupe table) with
    // its original result - the check may depend on state the first attempt changed (a SET PASSWORD replaced the
    // password the retry is proven with; a REVOKE removed the ADMIN it needed) - and refuses everything else 45.
    // `proofChecked` (F2): the gateway verified an own-credential proof for it - the leader then answers a request-id
    // reuse with a different statement exactly like a bad proof.
    virtual AdminSubmit submitSecurityDdl(const rune::protocol::client::SecurityDdl& m, const DdlMetaView& meta,
                                          std::string_view denial, bool proofChecked) = 0;
    virtual AdminSubmit submitSecurityBootstrap(const rune::protocol::client::SecurityBootstrap& m,
                                                const DdlMetaView& meta) = 0;
    // A Subscribe / Unsubscribe on `__security` (topic -7): the admin thread's second read server.  Same ring-full rules
    // as the catalog's; an Unsubscribe of a security subscription id (isSecuritySubscriptionId) uses this one.
    virtual bool submitSecuritySubscribe(int64_t sessionId, int64_t correlationId, int64_t fromOffset) = 0;
    virtual bool submitSecurityUnsubscribe(int64_t sessionId, int64_t subscriptionId) = 0;

    // Namespaces Task 7: NamespaceDdl (81), answered by a DdlResponse (78) from the admin thread.  `denial` follows
    // submitSecurityDdl's rules ("" = cluster admin; otherwise forwarded with the NotAuthorized detail when it carries a
    // request id, so a retry of an applied request still gets its original answer).  `clientVersion` picks the
    // NamespaceLimitExceeded code (52, v14+) or InvalidRequest (39) for an older session.
    virtual AdminSubmit submitNamespaceDdl(const rune::protocol::client::NamespaceDdlRequest& m, const DdlMetaView& meta,
                                           std::string_view denial, uint16_t clientVersion) = 0;
    // ListNamespaces (82): answered on ANY broker from the admin thread's applied catalog (no route, no append) by a
    // ListNamespacesResponse (83).  A non-admin caller sees only `callerNsId`.
    virtual AdminSubmit submitListNamespaces(int64_t sessionId, int64_t correlationId, uint32_t afterNsId, uint16_t max,
                                             uint32_t callerNsId, bool clusterAdmin) = 0;
};

} // namespace rune::gateway
