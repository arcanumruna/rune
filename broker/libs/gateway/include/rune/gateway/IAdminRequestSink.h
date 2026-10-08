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
                                   uint32_t descriptorLen) = 0;

    // GetTopicSchema (templateId 66) — likewise, copy-and-return.
    virtual bool submitGetSchema(int64_t sessionId, int64_t correlationId,
                                 std::string_view name) = 0;
};

} // namespace rune::gateway
