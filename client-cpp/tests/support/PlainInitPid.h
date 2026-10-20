// tests/support/PlainInitPid.h - Phase 2b F (FD4): every acked producer asks the broker for its id with an
// InitProducerId whose transactional id is empty.  A hand-rolled test gateway answers it with this helper.
#pragma once

#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <cstddef>
#include <cstdint>

namespace test_support {

/// If `buf` is an InitProducerId with an EMPTY transactional id, encode the OK response (`pid`, epoch 0)
/// into `out` and return its length; 0 for any other frame.  `headerOnly`: a gateway behind TLS sees the
/// body AEAD-sealed (only the header is readable): every InitProducerId is answered from its header.
inline std::size_t plainInitPidReply(const uint8_t* buf, std::size_t len, int64_t pid, uint8_t* out,
                                     bool headerOnly = false) noexcept {
    namespace rpc = rune::protocol::client;
    const auto hdr = rpc::MessageHeader::decode(buf, len);
    if (!hdr || hdr->templateId != rpc::InitProducerId::TEMPLATE_ID) return 0;
    if (!headerOnly) {
        const auto req = rpc::InitProducerId::decode(buf, len);
        if (!req || !req->transactionalId.empty()) return 0;
    }
    rpc::InitProducerIdResponse rsp;
    rsp.sessionId     = hdr->sessionId;
    rsp.correlationId = hdr->correlationId;
    rsp.producerId    = pid;
    rsp.producerEpoch = 0;
    return rsp.encode(out);
}

}  // namespace test_support
