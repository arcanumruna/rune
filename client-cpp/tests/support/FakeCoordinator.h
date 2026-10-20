// tests/support/FakeCoordinator.h - Phase 2b F Task 2: a transaction-coordinator stand-in for CoordinatorLink
// and ProducerHub tests.  Captures every frame the link offers (corrId, the session it was stamped with,
// template) and builds the matching response frame with rune-protocol's encoders.  No threads, no atomics.
#pragma once

#include <rune/client/producer/TxnTypes.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace test_support {

namespace rpc = rune::protocol::client;
using rune::client::producer::CoordinatorSink;
using rune::client::producer::ReqKind;

/// One encoded coordinator answer (fixed buffer: building it never allocates).
struct AnswerFrame {
    std::array<uint8_t, 128> buf{};
    std::size_t              len = 0;
    uint16_t                 templateId = 0;
    [[nodiscard]] const uint8_t* data() const noexcept { return buf.data(); }
};

struct OfferedFrame {
    int64_t  corrId    = 0;
    int64_t  sessionId = 0;
    uint16_t templateId = 0;
};

class FakeCoordinator {
public:
    int64_t                   sid = 77;          ///< the session id the link reads (0 = not connected)
    CoordinatorSink::Offer    next = CoordinatorSink::Offer::Accepted;
    std::vector<OfferedFrame> offered;           ///< every frame the link offered, in order

    FakeCoordinator() { offered.reserve(1 << 15); }

    [[nodiscard]] CoordinatorSink sink() noexcept { return {&offerThunk, &sessionThunk, this}; }

    [[nodiscard]] std::size_t offeredFor(int64_t corrId) const noexcept {
        std::size_t n = 0;
        for (const auto& f : offered) n += f.corrId == corrId ? 1 : 0;
        return n;
    }

    /// The response a coordinator would send for a request of `kind` with `corrId`.
    [[nodiscard]] static AnswerFrame answer(ReqKind kind, int64_t corrId, int16_t errorCode) noexcept {
        AnswerFrame a;
        switch (kind) {
        case ReqKind::InitProducerId: {
            rpc::InitProducerIdResponse m;
            m.correlationId = corrId;
            m.producerId    = 4242;
            m.producerEpoch = 0;
            m.errorCode     = errorCode;
            a.len        = m.encode(a.buf.data());
            a.templateId = rpc::InitProducerIdResponse::TEMPLATE_ID;
            break;
        }
        case ReqKind::AddPartitions: {
            rpc::AddPartitionsToTxnResponse m;
            m.correlationId = corrId;
            const rpc::TxnPartitionResult r{1, 0, errorCode};
            a.len        = m.encode(a.buf.data(), std::span<const rpc::TxnPartitionResult>(&r, 1));
            a.templateId = rpc::AddPartitionsToTxnResponse::TEMPLATE_ID;
            break;
        }
        case ReqKind::AddOffsets: {
            rpc::AddOffsetsToTxnResponse m;
            m.correlationId = corrId;
            m.errorCode     = errorCode;
            a.len        = m.encode(a.buf.data());
            a.templateId = rpc::AddOffsetsToTxnResponse::TEMPLATE_ID;
            break;
        }
        case ReqKind::TxnOffsetCommit: {
            rpc::TxnOffsetCommitResponse m;
            m.correlationId = corrId;
            const rpc::TxnPartitionResult r{1, 0, errorCode};
            a.len        = m.encode(a.buf.data(), std::span<const rpc::TxnPartitionResult>(&r, 1));
            a.templateId = rpc::TxnOffsetCommitResponse::TEMPLATE_ID;
            break;
        }
        case ReqKind::EndTxn: {
            rpc::EndTxnResponse m;
            m.correlationId = corrId;
            m.errorCode     = errorCode;
            a.len        = m.encode(a.buf.data());
            a.templateId = rpc::EndTxnResponse::TEMPLATE_ID;
            break;
        }
        }
        return a;
    }

    /// ErrorResponse{code, message} for `corrId` (e.g. NotCoordinator naming the coordinator endpoint).
    [[nodiscard]] static AnswerFrame errorResponse(int64_t corrId, rpc::ErrorCode code, std::string_view msg) noexcept {
        AnswerFrame a;
        rpc::ErrorResponse m;
        m.correlationId = corrId;
        m.code          = static_cast<int16_t>(code);
        m.message       = msg;
        a.len        = m.encode(a.buf.data());
        a.templateId = rpc::ErrorResponse::TEMPLATE_ID;
        return a;
    }

private:
    static CoordinatorSink::Offer offerThunk(void* ctx, const uint8_t* buf, std::size_t len) noexcept {
        auto* self = static_cast<FakeCoordinator*>(ctx);
        if (self->next != CoordinatorSink::Offer::Retry) {
            const auto h = rpc::MessageHeader::decode(buf, len);
            if (h) self->offered.push_back({h->correlationId, h->sessionId, h->templateId});
        }
        return self->next;
    }
    static int64_t sessionThunk(void* ctx) noexcept { return static_cast<FakeCoordinator*>(ctx)->sid; }
};

}  // namespace test_support
