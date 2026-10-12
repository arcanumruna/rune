#pragma once
// IAdminRequestSink.h — Task 8a: abstract sink for schema DDL requests, owned by
// the gateway lib so ClientGateway depends only on this interface rather than on
// rune_node symbols (mirrors ITxnRequestHandler / IGroupRequestHandler).
//
// The AdminWorkerAgent (in rune_node) provides an adapter that implements this
// interface by ENCODING the request onto the gateway→admin SPSC ring.  The
// gateway holds an IAdminRequestSink* (set via setAdminSink) and, on DefineTopic
// (64) / GetTopicSchema (66), copies the decoded fields into the sink and RETURNS
// IMMEDIATELY — no registry access, no blocking on the gateway hot path.  DDL
// therefore never delays produce routing (SchemaGatewayRegression).
//
// Namespace: rune::gateway.  C++23.

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace rune::gateway {

struct IAdminRequestSink {
    virtual ~IAdminRequestSink() = default;

    // DefineTopic (templateId 64) — the gateway has decoded the frame and
    // validated the session.  The sink OWNS the copied bytes (name/keyField/blob
    // views point into the ingress buffer and MUST NOT be retained past return).
    //
    // Returns true if the request was accepted (enqueued on the admin ring);
    // false on back-pressure (ring full) — the caller may drop (client retries).
    virtual bool submitDefineTopic(int64_t sessionId, int64_t correlationId,
                                   std::string_view name,
                                   uint32_t partitionCount,
                                   std::string_view keyField,
                                   bool validate,
                                   const uint8_t* descriptorBlob,
                                   uint32_t descriptorLen,
                                   uint8_t persistence) = 0;   // Phase 2b E: 0 disk, 1 transient

    // GetTopicSchema (templateId 66) — likewise, copy-and-return.
    // requestedFingerprint (Task 13): 0 = latest; non-zero = the SPECIFIC
    // version whose per-version fingerprint matches (so a typed client can
    // fetch the older descriptor it was compiled against).
    virtual bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                                 std::string_view name,
                                 uint64_t requestedFingerprint) = 0;

    // AlterTopic (templateId 68) — Task 14: the gateway has decoded the frame
    // (expectedVersion CAS + the opaque ALTER ops blob) and copies the fields
    // into the sink, which OWNS the copy; RETURNS IMMEDIATELY (copy-and-return,
    // same discipline as submitDefineTopic/submitGetSchema — no registry
    // access, no blocking on the gateway hot path).
    //
    // expectedVersion: 0 = unconditional (no CAS); non-zero = the admin thread
    // rejects with SchemaConflict if the topic's current version differs.
    // opsBlob: rune::codec::encodeAlterOps() wire bytes (opaque to the gateway).
    //
    // Returns true if the request was accepted (enqueued on the admin ring);
    // false on back-pressure (ring full) — the caller may drop (client retries).
    virtual bool submitAlterTopic(int64_t sessionId, int64_t correlationId,
                                  std::string_view topicName,
                                  uint16_t expectedVersion,
                                  const uint8_t* opsBlob,
                                  uint32_t opsLen) = 0;

    // CreateView (templateId 70) — Task 19: the gateway has decoded the frame
    // (viewName + keyField + persistence + the authored ViewPlan planBlob) and
    // copies the fields into the sink, which OWNS the copy; RETURNS IMMEDIATELY
    // (copy-and-return, same discipline as the other DDL submits — no registry
    // access, no blocking on the gateway hot path).  The admin thread
    // deserializes + compiles + validates the plan asynchronously and produces
    // the CreateViewResponse on the session egress.
    //
    // Returns true if the request was accepted (enqueued on the admin ring);
    // false on back-pressure (ring full) — the caller may drop (client retries).
    virtual bool submitCreateView(int64_t sessionId, int64_t correlationId,
                                  std::string_view viewName,
                                  std::string_view keyField,
                                  uint8_t persistence,
                                  const uint8_t* planBlob,
                                  uint32_t planLen) = 0;
};

} // namespace rune::gateway
