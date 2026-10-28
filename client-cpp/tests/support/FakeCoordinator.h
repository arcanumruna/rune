// tests/support/FakeCoordinator.h - Phase 2b F Task 2: a transaction-coordinator stand-in for CoordinatorLink
// and ProducerHub tests.  Captures every frame the link offers (corrId, the session it was stamped with,
// template) and builds the matching response frame with rune-protocol's encoders.  No threads, no atomics.
#pragma once

#include <rune/client/producer/TxnTypes.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <array>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace test_support {

namespace rpc = rune::protocol::client;
using rune::client::producer::CoordinatorSink;
using rune::client::producer::ReqKind;

/// One encoded coordinator answer (fixed buffer: building it never allocates).
struct AnswerFrame {
    std::array<uint8_t, 5120> buf{};   // a full AddPartitionsToTxn answer: 475 results x 10 bytes + header
    std::size_t              len = 0;
    uint16_t                 templateId = 0;
    [[nodiscard]] const uint8_t* data() const noexcept { return buf.data(); }
};

struct OfferedFrame {
    int64_t  corrId    = 0;
    int64_t  sessionId = 0;
    uint16_t templateId = 0;
    uint16_t len       = 0;
    std::array<uint8_t, 96> head{};   ///< the frame's first bytes (an InitProducerId with a short id fits whole)
};

class FakeCoordinator {
public:
    int64_t                   sid = 77;          ///< the session id the link reads (0 = not connected)
    CoordinatorSink::Offer    next = CoordinatorSink::Offer::Accepted;
    std::vector<OfferedFrame> offered;           ///< every frame the link offered, in order
    int                       rediscovers = 0;   ///< re-discovery requests (NotCoordinator answered)
    int                       refuseRediscovers = 0;   ///< #351: the next N asks are refused (return false), not counted
    int                       refusedRediscovers = 0;  ///< #351: asks that were refused

    FakeCoordinator() { offered.reserve(1 << 15); }

    [[nodiscard]] CoordinatorSink sink() noexcept { return {&offerThunk, &sessionThunk, this, &rediscoverThunk}; }

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

    /// #698: the answer a coordinator gives an AddPartitionsToTxn: EVERY partition of the request named, each with
    /// `errorCode`.  (`answer(AddPartitions, ...)` names only topic 1 / partition 0, and the producer fails a partition
    /// an answer does not name.)  Allocation-free: at most one frame's worth (475) of partitions.
    [[nodiscard]] static AnswerFrame answerNaming(int64_t corrId, int16_t errorCode,
                                                  std::span<const rpc::TxnPartitionResult> parts) noexcept {
        AnswerFrame a;
        rpc::AddPartitionsToTxnResponse m;
        m.correlationId = corrId;
        std::array<rpc::TxnPartitionResult, 480> named{};
        const std::size_t n = parts.size() < named.size() ? parts.size() : named.size();
        for (std::size_t i = 0; i < n; ++i) named[i] = {parts[i].topicId, parts[i].partition, errorCode};
        a.len        = m.encode(a.buf.data(), std::span<const rpc::TxnPartitionResult>(named.data(), n));
        a.templateId = rpc::AddPartitionsToTxnResponse::TEMPLATE_ID;
        return a;
    }

    /// The same for a request this fake was offered (by its corrId); an unknown corrId falls back to `answer()`.
    [[nodiscard]] AnswerFrame answerAdd(int64_t corrId, int16_t errorCode) const noexcept {
        for (const auto& r : addRequests_) {
            if (r.corrId == corrId) {
                return answerNaming(corrId, errorCode, std::span<const rpc::TxnPartitionResult>(r.parts.data(), r.parts.size()));
            }
        }
        return answer(ReqKind::AddPartitions, corrId, errorCode);
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
    struct AddRequest { int64_t corrId; std::vector<rpc::TxnPartitionResult> parts; };
    std::vector<AddRequest> addRequests_;   ///< the partitions of every AddPartitionsToTxn offered (for answerAdd)

    static CoordinatorSink::Offer offerThunk(void* ctx, const uint8_t* buf, std::size_t len) noexcept {
        auto* self = static_cast<FakeCoordinator*>(ctx);
        if (self->next != CoordinatorSink::Offer::Retry) {
            const auto h = rpc::MessageHeader::decode(buf, len);
            if (h) {
                OfferedFrame f{h->correlationId, h->sessionId, h->templateId, static_cast<uint16_t>(len), {}};
                std::memcpy(f.head.data(), buf, len < f.head.size() ? len : f.head.size());
                self->offered.push_back(f);
                if (h->templateId == rpc::AddPartitionsToTxn::TEMPLATE_ID) {
                    if (const auto m = rpc::AddPartitionsToTxn::decode(buf, len)) {
                        AddRequest r{h->correlationId, {}};
                        for (std::size_t i = 0; i < m->partitions.size(); ++i) {
                            r.parts.push_back({m->partitions[i].topicId, m->partitions[i].partition, 0});
                        }
                        self->addRequests_.push_back(std::move(r));
                    }
                }
            }
        }
        return self->next;
    }
    static int64_t sessionThunk(void* ctx) noexcept { return static_cast<FakeCoordinator*>(ctx)->sid; }
    static bool rediscoverThunk(void* ctx) noexcept {
        auto* self = static_cast<FakeCoordinator*>(ctx);
        if (self->refuseRediscovers > 0) {
            --self->refuseRediscovers;
            ++self->refusedRediscovers;
            return false;
        }
        ++self->rediscovers;
        return true;
    }
};

}  // namespace test_support
