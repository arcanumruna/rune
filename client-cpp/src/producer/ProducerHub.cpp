// src/producer/ProducerHub.cpp
//
// CB-2 — ProducerHub non-inline method implementations.
//
// Shared-nothing, single-owner model (NO atomics, NO locks):
//   The producer list is owned by the runner (agent) thread.  add() and every
//   duty-cycle iterator run on that one thread, so the list is a plain vector
//   iterated without any synchronization.  Registration crosses the app→runner
//   boundary through the agent's M2O command ring (kMsgRegisterProducer), the
//   sanctioned cross-thread mechanism — never a shared atomic or mutex here.

#include <rune/client/producer/ProducerHub.h>

#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/TxnMessages.h>

#include <stdexcept>

namespace rune::client::producer {

// ---------------------------------------------------------------------------
// add() — RUNNER-THREAD ONLY (invoked from the kMsgRegisterProducer drain).
// A plain push_back on the single-owner vector; the producer's position is its
// ordinal, encoded into every batch correlation id it emits.
// ---------------------------------------------------------------------------

void ProducerHub::add(ProducerImpl* impl)
{
    uint32_t ord = 0;
    if (producers_.size() <= batchcorr::kMaxOrdinal) {
        ord = static_cast<uint32_t>(producers_.size());
        producers_.push_back(impl);
    } else if (freeHead_ < freeOrdinals_.size()) {
        // F8c C7: every fresh ordinal is taken — reuse the oldest released one
        // (FIFO: the longest time for late ProduceResponses of its previous
        // producer to have arrived and been dropped).
        ord = freeOrdinals_[freeHead_++];
        if (freeHead_ == freeOrdinals_.size()) {
            freeOrdinals_.clear();
            freeHead_ = 0;
        }
        producers_[ord] = impl;
    } else {
        throw std::length_error("ProducerHub: at most 4096 live producers per client");
    }
    if (nextBase_.size() <= ord) nextBase_.resize(ord + 1, 0);
    impl->setOrdinal(ord, nextBase_[ord]);  // F2: past every sequence a previous incarnation used
    if (coordSink_.offer != nullptr) impl->enableCoordinator(coordSink_);   // Phase 2b F: after the ordinal
    active_.push_back(impl);  // m2: walks visit only the live producers
}

void ProducerHub::remove(ProducerImpl* impl) noexcept
{
    const uint32_t ord = impl->ordinal();
    if (ord >= producers_.size() || producers_[ord] != impl) return;
    producers_[ord] = nullptr;
    // F2: the next producer on this ordinal starts past every sequence this one
    // ever used (cold: once per released producer).
    if (nextBase_.size() > ord) nextBase_[ord] = impl->sequenceEnd() + 1;
    for (std::size_t i = 0; i < active_.size(); ++i) {
        if (active_[i] == impl) {
            active_[i] = active_.back();
            active_.pop_back();
            break;
        }
    }
    try {
        freeOrdinals_.push_back(ord);
    } catch (...) {
        // Out of memory: the ordinal is simply never reused.
    }
}

// ---------------------------------------------------------------------------
// Duty-cycle methods — runner thread only; iterate the single-owner vector.
// ---------------------------------------------------------------------------

void ProducerHub::onProduceResponse(int64_t correlationId, int64_t baseOffset, int16_t errorCode,
                                    int64_t logStartOffset) {
    // O(1): the batch id names its producer.  Matching is by correlation id only,
    // never by the publisher the response arrived on.
    if (!batchcorr::isBatch(correlationId)) return;       // txn / non-producer id
    const uint32_t o = batchcorr::ordinal(correlationId);
    if (o < producers_.size() && producers_[o] != nullptr) {
        producers_[o]->onProduceResponse(correlationId, baseOffset, errorCode, logStartOffset);
    }
}

namespace {
namespace rpc = rune::protocol::client;

/// The most severe per-partition verdict decides (fenced > fatal > abortable/reject > rediscover > retry >
/// ok) so a 27 never masks a non-retriable error.  Returns the code that earned it.
template <class Results>
int16_t worstResult(ReqKind kind, const Results& results) noexcept {
    auto rank = [](Verdict v) {
        switch (v) {
        case Verdict::Fenced:     return 5;
        case Verdict::Fatal:      return 4;
        case Verdict::Abortable:
        case Verdict::Reject:     return 3;
        case Verdict::Rediscover: return 2;
        case Verdict::Retry:      return 1;
        case Verdict::Ok:         return 0;
        }
        return 0;
    };
    int16_t worst = 0;
    int     best  = 0;
    for (std::size_t i = 0; i < results.size(); ++i) {
        const int16_t code = results[i].errorCode;
        const int     r    = rank(CoordinatorLink::classify(kind, code));
        if (r > best) { best = r; worst = code; }
    }
    return worst;
}
}  // namespace

bool ProducerHub::onCoordinatorResponse(uint16_t templateId, const uint8_t* buf, std::size_t len, int64_t nowNs) {
    const auto hdr = rpc::MessageHeader::decode(buf, len);
    if (!hdr || !coordcorr::isCoord(hdr->correlationId)) return false;
    const uint32_t o = coordcorr::ordinal(hdr->correlationId);
    if (o >= producers_.size() || producers_[o] == nullptr) return false;

    CoordAnswer a{ReqKind::InitProducerId, hdr->correlationId, 0, std::span<const uint8_t>(buf, len), false};
    switch (static_cast<rpc::TemplateId>(templateId)) {
    case rpc::TemplateId::InitProducerIdResponse: {
        const auto m = rpc::InitProducerIdResponse::decode(buf, len);
        if (!m) return false;
        a.kind = ReqKind::InitProducerId;
        a.errorCode = m->errorCode;
        break;
    }
    case rpc::TemplateId::AddPartitionsToTxnResponse: {
        const auto m = rpc::AddPartitionsToTxnResponse::decode(buf, len);
        if (!m) return false;
        a.kind = ReqKind::AddPartitions;
        a.errorCode = worstResult(a.kind, m->results);
        break;
    }
    case rpc::TemplateId::AddOffsetsToTxnResponse: {
        const auto m = rpc::AddOffsetsToTxnResponse::decode(buf, len);
        if (!m) return false;
        a.kind = ReqKind::AddOffsets;
        a.errorCode = m->errorCode;
        break;
    }
    case rpc::TemplateId::TxnOffsetCommitResponse: {
        const auto m = rpc::TxnOffsetCommitResponse::decode(buf, len);
        if (!m) return false;
        a.kind = ReqKind::TxnOffsetCommit;
        a.errorCode = worstResult(a.kind, m->results);
        break;
    }
    case rpc::TemplateId::EndTxnResponse: {
        const auto m = rpc::EndTxnResponse::decode(buf, len);
        if (!m) return false;
        a.kind = ReqKind::EndTxn;
        a.errorCode = m->errorCode;
        break;
    }
    case rpc::TemplateId::ErrorResponse: {
        const auto m = rpc::ErrorResponse::decode(buf, len);
        if (!m) return false;
        a.anyKind = true;
        a.errorCode = m->code;
        break;
    }
    default:
        return false;
    }
    producers_[o]->onCoordinatorAnswer(a, nowNs);
    return true;
}

void ProducerHub::onCoordinatorMoved() noexcept {
    for (ProducerImpl* impl : active_) {
        if (impl != nullptr) impl->onCoordinatorMoved();
    }
}

void ProducerHub::onCoordinatorSessionChanged() noexcept {
    for (ProducerImpl* impl : active_) {
        if (impl != nullptr) impl->onCoordinatorSessionChanged();
    }
}

void ProducerHub::failAll(OpStatus st) {
    for (std::size_t i = 0; i < active_.size(); ++i) {
        if (active_[i] != nullptr) active_[i]->failAllPending(st);
    }
}

void ProducerHub::closeFinalAll(OpStatus st, int stalledYieldsBeforeUnblock) {
    for (std::size_t i = 0; i < active_.size(); ++i) {
        if (active_[i] != nullptr) active_[i]->closeFinal(st, stalledYieldsBeforeUnblock);
    }
}

void ProducerHub::failPending(int64_t corrId) {
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        impl->failBatch(corrId);
    }
}

bool ProducerHub::retryRedirectedProduce(
    int64_t corrId,
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        if (impl->retryRedirectedProduce(corrId, sendFn)) {
            return true;
        }
    }
    return false;
}

int ProducerHub::resendAllRedirected(
    int64_t newSessionId,
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int resent = 0;
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        resent += impl->resendAllRedirected(newSessionId, sendFn);
    }
    return resent;
}

int ProducerHub::resendAllViaRoute(
    std::function<bool(int32_t, int32_t, int32_t, const uint8_t*, std::size_t)> sendFn)
{
    int resent = 0;
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        resent += impl->resendAllViaRoute(sendFn);
    }
    return resent;
}

int ProducerHub::resendPartitionViaRoute(int32_t topicId, int32_t partition) noexcept
{
    int queued = 0;
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        queued += impl->resendPartitionViaRoute(topicId, partition);
    }
    return queued;
}

int ProducerHub::resendRouteParked() noexcept
{
    int queued = 0;
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        queued += impl->resendRouteParked();
    }
    return queued;
}

void ProducerHub::noteRouteChanged(int32_t topicId, int32_t partition) noexcept
{
    for (ProducerImpl* impl : active_) {
        if (impl != nullptr) impl->noteRouteChanged(topicId, partition);
    }
}

bool ProducerHub::routeParked(int32_t topicId, int32_t partition) const noexcept
{
    for (const ProducerImpl* impl : active_) {
        if (impl != nullptr && impl->routeParked(topicId, partition)) return true;
    }
    return false;
}

bool ProducerHub::hasInFlight() const noexcept {
    for (const ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        if (impl->hasInFlight()) return true;
    }
    return false;
}

void ProducerHub::sweepTimedOut(const std::chrono::steady_clock::time_point& now) {
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        impl->sweepTimedOut(now);
    }
}

bool ProducerHub::partitionForCorrId(int64_t corrId,
                                     int32_t& topicId,
                                     int32_t& partition) noexcept
{
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        if (impl->partitionForCorrId(corrId, topicId, partition)) {
            return true;
        }
    }
    return false;
}

bool ProducerHub::firstPendingPartition(int32_t& topicId,
                                        int32_t& partition) const noexcept
{
    for (const ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        if (impl->firstPendingPartition(topicId, partition)) {
            return true;
        }
    }
    return false;
}

void ProducerHub::updateIngressRoute(int32_t topicId, int32_t partition,
                                     int32_t ingressStreamId) noexcept
{
    for (ProducerImpl* impl : active_) {
        if (impl == nullptr) continue;
        impl->updateIngressRoute(topicId, partition, ingressStreamId);
    }
}

} // namespace rune::client::producer
