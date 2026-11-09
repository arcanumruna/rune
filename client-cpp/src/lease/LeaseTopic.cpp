// src/lease/LeaseTopic.cpp - the non-template core of LeaseTopic<T> (Task 14a): writes and routed requests.
#include <rune/client/lease/LeaseTopic.h>

#include <rune/protocol/LeaseRecord.h>

#include <algorithm>

namespace rune::client::detail {

namespace {
constexpr std::size_t kMinLeaseRowBytes = rune::protocol::record::kLeaseRemainingValueOffset + 8;   // 36

void putLe64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }   // little-endian hosts (the wire contract)

[[nodiscard]] OpResult toOpResult(rune::client::SendResult r) noexcept {
    switch (r) {
    case rune::client::SendResult::Ok:            return OpResult::Accepted;
    case rune::client::SendResult::BackPressured: return OpResult::BackPressured;
    case rune::client::SendResult::QueueFull:     return OpResult::QueueFull;
    case rune::client::SendResult::Closed:        return OpResult::Closed;
    case rune::client::SendResult::WrongThread:   return OpResult::WrongThread;
    default:                        return OpResult::InvalidArgument;
    }
}
}  // namespace

LeaseTopicCore::LeaseTopicCore(Client& client, Producer&& producer, int32_t topicId, int32_t viewTopicId,
                               int32_t partitionCount, rune::protocol::Partitioner partitioner,
                               const rune::protocol::LeaseSpec& spec, uint16_t version, uint32_t opTimeoutMs)
    : client_(&client),
      producer_(std::move(producer)),
      topicId_(topicId),
      viewTopicId_(viewTopicId),
      partitionCount_(partitionCount > 0 ? partitionCount : 1),
      partitioner_(partitioner),
      spec_(spec),
      version_(version),
      opTimeoutMs_(opTimeoutMs == 0 ? 1 : opTimeoutMs) {
    scratch_.resize(256);
}

int32_t LeaseTopicCore::partitionOf(std::span<const uint8_t> key) const noexcept {
    return rune::protocol::partitionFor(key, partitionCount_, partitioner_);
}

OpResult LeaseTopicCore::write(OpKind kind, std::span<const uint8_t> key, std::size_t rowBytes, int64_t version,
                               const LeaseOptions& opts, CompletionTarget target, Token token) noexcept {
    if (closed_) return OpResult::InvalidArgument;
    if (key.empty() || key.size() > rune::protocol::kMaxLeaseKeyBytes) return OpResult::InvalidArgument;
    if (rowBytes < kMinLeaseRowBytes || rowBytes > scratch_.size()) return OpResult::InvalidArgument;
    if (target.cq == nullptr) return OpResult::InvalidArgument;   // a lease write completes into a Completions queue only
    WriteIntent intent = WriteIntent::Insert;
    auto        op     = rune::protocol::record::LeaseOp::Put;
    uint64_t    ttl    = 0;
    int64_t     ver    = 0;
    switch (kind) {
    case OpKind::LeaseGrant:
        ttl = opts.ttlMs != 0 ? opts.ttlMs : spec_.ttlMs;
        break;
    case OpKind::LeaseUpdate:
        if (version < 0) return OpResult::InvalidArgument;
        intent = WriteIntent::Update;
        ttl    = opts.ttlMs != 0 ? opts.ttlMs : spec_.ttlMs;
        ver    = version;
        break;
    case OpKind::LeaseRevoke:
        if (version < 0) return OpResult::InvalidArgument;
        intent = WriteIntent::Update;
        op     = rune::protocol::record::LeaseOp::Revoke;
        ver    = version;
        break;
    default:
        return OpResult::InvalidArgument;
    }
    if (ttl > spec_.maxTtlMs) return OpResult::InvalidArgument;
    uint8_t* row = scratch_.data();
    putLe64(row + rune::protocol::record::kLeaseVersionValueOffset, static_cast<uint64_t>(ver));
    putLe64(row + rune::protocol::record::kLeaseTtlValueOffset, ttl);
    putLe64(row + rune::protocol::record::kLeaseHolderValueOffset, opts.holder);
    putLe64(row + rune::protocol::record::kLeaseRemainingValueOffset, 0);
    return toOpResult(producer_.trySendLease(topicId_, partitionOf(key), key, std::span<const uint8_t>(row, rowBytes), intent,
                                             static_cast<uint8_t>(op), kind, *target.cq, token));
}

OpResult LeaseTopicCore::keepAliveOnce(std::span<const uint8_t> key, int64_t version, const LeaseOptions& opts,
                                       CompletionTarget target, Token token) {
    if (closed_ || version < 0) return OpResult::InvalidArgument;
    return client_->leaseKeepAlive(topicId_, partitionOf(key), key, version,
                                   opts.opTimeoutMs != 0 ? opts.opTimeoutMs : opTimeoutMs_, target, token);
}

OpResult LeaseTopicCore::timeToLive(std::span<const uint8_t> key, const LeaseOptions& opts, CompletionTarget target,
                                    Token token) {
    if (closed_) return OpResult::InvalidArgument;
    return client_->leaseTimeToLive(topicId_, partitionOf(key), key,
                                    opts.opTimeoutMs != 0 ? opts.opTimeoutMs : opTimeoutMs_, target, token);
}

OpResult LeaseTopicCore::scanLeases(int32_t partition, std::span<const uint8_t> prefix, uint64_t resumeHash, bool first,
                                    uint16_t maxRows, const LeaseOptions& opts, CompletionTarget target, Token token) {
    if (closed_ || partition < 0 || partition >= partitionCount_) return OpResult::InvalidArgument;
    return client_->scanLeases(viewTopicId_, partition, prefix, resumeHash, first, maxRows,
                               opts.opTimeoutMs != 0 ? opts.opTimeoutMs : opTimeoutMs_, target, token);
}

OpResult LeaseTopicCore::get(std::span<const uint8_t> key, int64_t minOffset, CompletionTarget target, Token token) {
    if (closed_) return OpResult::InvalidArgument;
    ViewReadOptions ro;
    ro.partition = partitionOf(key);
    ro.minOffset = minOffset;
    return client_->getViewRowK(viewTopicId_, key, ro, target, token);
}

}  // namespace rune::client::detail
