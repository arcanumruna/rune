// src/lease/LeaseTopic.cpp - the non-template core of LeaseTopic<T> (Task 14a): writes and routed requests.
#include <rune/client/lease/LeaseTopic.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
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

void LeaseTopicCore::setRevokeTemplate(std::span<const uint8_t> row, std::span<const uint8_t> descriptorBlob) {
    revokeKeyOffset_ = 0;
    revokeKeyWidth_  = 0;
    viewRowLen_ = viewVersionOffset_ = viewGrantedOffset_ = viewGrantedSlot_ = 0;
    try {
        const rune::codec::Descriptor d = rune::codec::deserialize(descriptorBlob);
        // The lease view row (generated LeasesRow): bitmap of ceil((fields + 2) / 8) bytes, one fixed slot per field ever declared
        // (dropped ones too), then the two Int64 offsets.  The generator pins the first field to lease_version.
        if (d.fields.size() >= 4 && d.fields[0].name == "lease_version") {
            uint32_t off = static_cast<uint32_t>((d.fields.size() + 2 + 7) / 8);
            viewVersionOffset_ = off;
            for (const auto& f : d.fields) off += rune::codec::fieldWidth(f);
            viewGrantedOffset_ = off;
            viewGrantedSlot_   = static_cast<uint32_t>(d.fields.size());
            viewRowLen_        = off + 16;
        }
        for (const auto& f : d.fields) {
            if (!f.isKey || f.sinceVersion > version_ || version_ >= f.untilVersion) continue;
            revokeKeyOffset_ = 4u + f.offset;
            revokeKeyWidth_  = rune::codec::fieldWidth(f);
            break;
        }
    } catch (...) {
        revokeKeyWidth_ = 0;   // a schema we cannot read: revokeKey refuses
    }
    revokeRow_.assign(row.begin(), row.end());
    if (revokeRow_.size() >= 4) {   // the same schema-version stamp LeaseTopic::encode_ applies
        revokeRow_[2] = static_cast<uint8_t>(version_ & 0xffu);
        revokeRow_[3] = static_cast<uint8_t>((version_ >> 8) & 0xffu);
    }
    if (scratch_.size() < revokeRow_.size()) scratch_.resize(revokeRow_.size());   // so a revoke never grows the scratch
}

OpResult LeaseTopicCore::revokeKey(std::span<const uint8_t> key, int64_t version, const LeaseOptions& opts, CompletionTarget target,
                                   Token token) noexcept {
    if (closed_ || revokeRow_.size() < kMinLeaseRowBytes || revokeRow_.size() > scratch_.size()) return OpResult::InvalidArgument;
    if (!revokeKeyFits(key.size())) return OpResult::InvalidArgument;
    std::memcpy(scratch_.data(), revokeRow_.data(), revokeRow_.size());
    if (!patchKey(std::span<uint8_t>(scratch_.data(), revokeRow_.size()), key)) return OpResult::InvalidArgument;   // the row's key column must equal the record key
    return write(OpKind::LeaseRevoke, key, revokeRow_.size(), version, opts, target, token);
}

OpResult LeaseTopicCore::grantKey(std::span<const uint8_t> key, const LeaseOptions& opts, CompletionTarget target, Token token) noexcept {
    if (closed_ || revokeRow_.size() < kMinLeaseRowBytes || revokeRow_.size() > scratch_.size()) return OpResult::InvalidArgument;
    if (!revokeKeyFits(key.size())) return OpResult::InvalidArgument;
    std::memcpy(scratch_.data(), revokeRow_.data(), revokeRow_.size());
    if (!patchKey(std::span<uint8_t>(scratch_.data(), revokeRow_.size()), key)) return OpResult::InvalidArgument;
    return write(OpKind::LeaseGrant, key, revokeRow_.size(), 0, opts, target, token);
}

OpResult LeaseTopicCore::writeRaw(OpKind kind, std::span<const uint8_t> key, std::span<const uint8_t> row, int64_t version,
                                  const LeaseOptions& opts, CompletionTarget target, Token token) noexcept {
    if (closed_ || row.size() < kMinLeaseRowBytes) return OpResult::InvalidArgument;
    if (row.size() > scratch_.size()) {   // cold: the high-water mark grows once
        try { scratch_.resize(row.size()); } catch (...) { return OpResult::InvalidArgument; }
    }
    if (!row.empty()) std::memcpy(scratch_.data(), row.data(), row.size());
    scratch_[2] = static_cast<uint8_t>(version_ & 0xffu);
    scratch_[3] = static_cast<uint8_t>((version_ >> 8) & 0xffu);
    return write(kind, key, row.size(), version, opts, target, token);
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
