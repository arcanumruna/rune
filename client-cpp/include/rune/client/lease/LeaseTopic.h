// include/rune/client/lease/LeaseTopic.h - LeaseTopic<T>: the typed client handle of a lease topic (Task 14a).
//
// A lease topic is a keyed log whose rows carry four system columns (lease_version, lease_ttl_ms, lease_holder,
// lease_remaining_ms; value offsets 4 / 12 / 20 / 28).  The handle:
//   * writes (grant / update / revoke) go through a DEDICATED producer (acks All|View, idempotent) in the lease SEND_RECORD
//     form (a key slot of 128 bytes), and complete as LeaseGrant / LeaseUpdate / LeaseRevoke, value = leaseVersion,
//     aux = ttlMs.  Their target must be a Completions queue.
//   * requests (keepAliveOnce / timeToLive / scanLeases / get) are routed control ops, followed to the partition leader.
// T contract: kFingerprint, key() (a fixed integer or a byte span of 1..128 bytes), encodedSize(), encode(uint8_t*); the row
// starts with the 4-byte header and holds the four system columns right after it (a generated lease row does).
// A generated lease type also has withLease(version, ttlMs, holder) and T::LeasesRow (rune schema generate does both for a lease
// topic); getAndWait / decodeRow need LeasesRow.
// Threading: NOT thread-safe - one owner thread (the Producer's rule).  Move-only.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/lease/LeaseOptions.h>
#include <rune/client/lease/LeaseResult.h>
#include <rune/client/producer/Producer.h>
#include <rune/protocol/LeaseSpec.h>
#include <rune/protocol/PartitionHash.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace rune::client {
namespace detail {

/// The non-template body of a LeaseTopic (src/lease/LeaseTopic.cpp, LeaseWire.cpp).
class LeaseTopicCore {
public:
    LeaseTopicCore(Client& client, Producer&& producer, int32_t topicId, int32_t viewTopicId, int32_t partitionCount,
                   rune::protocol::Partitioner partitioner, const rune::protocol::LeaseSpec& spec, uint16_t version,
                   uint32_t opTimeoutMs);
    LeaseTopicCore(LeaseTopicCore&&) noexcept            = default;
    LeaseTopicCore& operator=(LeaseTopicCore&&) noexcept = default;

    [[nodiscard]] int32_t topicId() const noexcept { return topicId_; }
    [[nodiscard]] int32_t leaseViewTopicId() const noexcept { return viewTopicId_; }
    [[nodiscard]] int32_t partitionCount() const noexcept { return partitionCount_; }
    [[nodiscard]] const rune::protocol::LeaseSpec& spec() const noexcept { return spec_; }
    [[nodiscard]] uint16_t boundVersion() const noexcept { return version_; }
    [[nodiscard]] uint32_t opTimeoutMs() const noexcept { return opTimeoutMs_; }
    [[nodiscard]] bool closed() const noexcept { return closed_; }
    void close() noexcept { closed_ = true; }
    [[nodiscard]] std::vector<uint8_t>& scratch() noexcept { return scratch_; }
    [[nodiscard]] Producer& producer() noexcept { return producer_; }
    [[nodiscard]] Client& client() noexcept { return *client_; }

    /// The partition of `key` under the topic's partitioner.
    [[nodiscard]] int32_t partitionOf(std::span<const uint8_t> key) const noexcept;

    /// One lease write: `row` (n encoded bytes in scratch()) gets its system columns patched, then goes out in the lease form.
    /// InvalidArgument (synchronously, nothing sent): a closed handle, a key outside 1..128 bytes, a ttl above maxTtlMs, a
    /// version < 0 on an update or revoke, a row too short for the system columns, a target that is not a Completions queue.
    [[nodiscard]] OpResult write(OpKind kind, std::span<const uint8_t> key, std::size_t rowBytes, int64_t version,
                                 const LeaseOptions& opts, CompletionTarget target, Token token) noexcept;

    [[nodiscard]] OpResult keepAliveOnce(std::span<const uint8_t> key, int64_t version, const LeaseOptions& opts,
                                         CompletionTarget target, Token token);
    [[nodiscard]] OpResult timeToLive(std::span<const uint8_t> key, const LeaseOptions& opts, CompletionTarget target,
                                      Token token);
    [[nodiscard]] OpResult scanLeases(int32_t partition, std::span<const uint8_t> prefix, uint64_t resumeHash, bool first,
                                      uint16_t maxRows, const LeaseOptions& opts, CompletionTarget target, Token token);
    [[nodiscard]] OpResult get(std::span<const uint8_t> key, int64_t minOffset, CompletionTarget target, Token token);

    /// Blocking forms (tools and tests): issue into a fresh queue, wait, return the lease fields.  Throws on a client that
    /// closed; otherwise the status is in the result (Timeout included).
    [[nodiscard]] LeaseResult writeAndWait(OpKind kind, std::span<const uint8_t> key, std::size_t rowBytes, int64_t version,
                                           const LeaseOptions& opts, std::chrono::nanoseconds timeout);
    [[nodiscard]] LeaseResult keepAliveAndWait(std::span<const uint8_t> key, int64_t version, const LeaseOptions& opts,
                                               std::chrono::nanoseconds timeout);
    [[nodiscard]] LeaseResult timeToLiveAndWait(std::span<const uint8_t> key, const LeaseOptions& opts,
                                                std::chrono::nanoseconds timeout, LeaseTtlInfo* info = nullptr);

private:
    Client*                      client_ = nullptr;
    Producer                     producer_;
    int32_t                      topicId_        = -1;
    int32_t                      viewTopicId_    = -1;
    int32_t                      partitionCount_ = 1;
    rune::protocol::Partitioner  partitioner_    = rune::protocol::Partitioner::Fnv1a;
    rune::protocol::LeaseSpec    spec_{};
    uint16_t                     version_        = 0;
    uint32_t                     opTimeoutMs_    = 1;
    bool                         closed_         = false;
    std::vector<uint8_t>         scratch_;   ///< the encoded row; grown to the high-water mark, never freed per write

    template <class Issue>
    LeaseResult waitOne_(std::chrono::nanoseconds timeout, Issue&& issue, LeaseTtlInfo* info);
};

}  // namespace detail

template <class T>
class LeaseTopic {
    static_assert(HasRecordKey<T>, "LeaseTopic<T>: T must expose key() (a lease row is keyed)");

public:
    LeaseTopic(LeaseTopic&&) noexcept            = default;
    LeaseTopic& operator=(LeaseTopic&&) noexcept = default;
    LeaseTopic(const LeaseTopic&)                = delete;
    LeaseTopic& operator=(const LeaseTopic&)     = delete;

    /// Builds the handle from a completed OpKind::OpenLeaseTopic (its status Ok and its schema payload).  Throws
    /// std::runtime_error when the completion is not an Ok open of T's lease topic.
    static LeaseTopic fromOpen(Client& client, const Completion& c, std::span<const uint8_t> payload,
                               const LeaseTopicConfig& cfg = {});

    [[nodiscard]] int32_t topicId() const noexcept { return core_.topicId(); }
    [[nodiscard]] int32_t leaseViewTopicId() const noexcept { return core_.leaseViewTopicId(); }
    [[nodiscard]] int32_t partitionCount() const noexcept { return core_.partitionCount(); }
    [[nodiscard]] const rune::protocol::LeaseSpec& spec() const noexcept { return core_.spec(); }
    [[nodiscard]] uint32_t opTimeoutMs() const noexcept { return core_.opTimeoutMs(); }
    /// The partition `row`'s key routes to.
    [[nodiscard]] int32_t partitionOf(const T& row) const noexcept {
        KeyBuf k;
        return keyOf_(row, k) ? core_.partitionOf(k.span()) : 0;
    }
    void close() noexcept { core_.close(); }
    /// The handle's dedicated writer (test seam: producerId() tells when the idempotent identity is assigned).
    [[nodiscard]] Producer& producer() noexcept { return core_.producer(); }

    // ---- writes (completion: LeaseGrant / LeaseUpdate / LeaseRevoke) ----
    /// Takes the lease of row.key(): lease_version 0, ttl = opts.ttlMs (0 = the topic's), holder = opts.holder.  Insert intent.
    [[nodiscard]] OpResult grant(const T& row, const LeaseOptions& opts, CompletionTarget target, Token token) noexcept {
        return writeRow_(OpKind::LeaseGrant, row, 0, opts, target, token);
    }
    /// Replaces the row if the lease is at `version` (compare-and-set).  Update intent.
    [[nodiscard]] OpResult update(const T& row, int64_t version, const LeaseOptions& opts, CompletionTarget target,
                                  Token token) noexcept {
        return writeRow_(OpKind::LeaseUpdate, row, version, opts, target, token);
    }
    /// Revokes the lease of keyRow.key() at `version`; only the key (and the system columns) of `keyRow` are significant.
    [[nodiscard]] OpResult revoke(const T& keyRow, int64_t version, const LeaseOptions& opts, CompletionTarget target,
                                  Token token) noexcept {
        return writeRow_(OpKind::LeaseRevoke, keyRow, version, opts, target, token);
    }

    // ---- requests (routed control ops) ----
    [[nodiscard]] OpResult keepAliveOnce(const T& row, int64_t version, const LeaseOptions& opts, CompletionTarget target,
                                         Token token) {
        KeyBuf k;
        if (!keyOf_(row, k)) return OpResult::InvalidArgument;
        return core_.keepAliveOnce(k.span(), version, opts, target, token);
    }
    [[nodiscard]] OpResult timeToLive(const T& row, const LeaseOptions& opts, CompletionTarget target, Token token) {
        KeyBuf k;
        if (!keyOf_(row, k)) return OpResult::InvalidArgument;
        return core_.timeToLive(k.span(), opts, target, token);
    }
    /// One page of one partition's lease rows whose key starts with `prefix`.
    [[nodiscard]] OpResult scanLeases(int32_t partition, std::span<const uint8_t> prefix, uint64_t resumeHash, bool first,
                                      uint16_t maxRows, const LeaseOptions& opts, CompletionTarget target, Token token) {
        return core_.scanLeases(partition, prefix, resumeHash, first, maxRows, opts, target, token);
    }
    /// A point read of the lease view (GetViewRowK completion, unchanged).
    [[nodiscard]] OpResult get(const T& row, int64_t minOffset, CompletionTarget target, Token token) {
        KeyBuf k;
        if (!keyOf_(row, k)) return OpResult::InvalidArgument;
        return core_.get(k.span(), minOffset, target, token);
    }

    /// Decodes the payload of a get completion (a lease view row) into T::LeasesRow; nullopt = not a row of this layout.
    [[nodiscard]] static auto decodeRow(std::span<const uint8_t> row) noexcept {
        static_assert(requires { typename T::LeasesRow; },
                      "LeaseTopic<T>::decodeRow needs a generated lease type (T::LeasesRow): regenerate T with `rune schema generate`");
        return T::LeasesRow::decode(row);
    }

    // ---- blocking forms (tools and tests) ----
    /// The lease view's row of row.key() as a T::LeasesRow (read-your-writes: the read waits for the view to pass `minOffset`;
    /// -1 = no barrier).  nullopt = no lease under that key.  Throws std::invalid_argument for a key outside 1..128 bytes or a
    /// closed handle, and std::runtime_error for any other answer (the status is in the message), like getViewRowAndWait for a
    /// transport failure.
    [[nodiscard]] auto getAndWait(const T& row, int64_t minOffset = -1,
                                  std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        static_assert(requires { typename T::LeasesRow; },
                      "LeaseTopic<T>::getAndWait needs a generated lease type (T::LeasesRow): regenerate T with `rune schema generate`");
        KeyBuf k;
        if (core_.closed() || !keyOf_(row, k)) throw std::invalid_argument("LeaseTopic::getAndWait: closed handle or invalid key");
        ViewReadOptions ro;
        ro.partition = core_.partitionOf(k.span());
        ro.minOffset = minOffset;
        const ViewRowResult r = core_.client().getViewRowAndWait(core_.leaseViewTopicId(), k.span(), ro, timeout);
        if (r.status == ViewRowResult::kAbsentKey) return std::optional<typename T::LeasesRow>{};
        if (r.status != ViewRowResult::kOk)
            throw std::runtime_error("LeaseTopic::getAndWait: the lease view answered status " + std::to_string(r.status));
        auto decoded = T::LeasesRow::decode(r.rowBytes);
        if (!decoded) throw std::runtime_error("LeaseTopic::getAndWait: the lease view row has an unexpected length");
        return decoded;
    }

    [[nodiscard]] LeaseResult grantAndWait(const T& row, const LeaseOptions& opts = {},
                                           std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        return writeAndWait_(OpKind::LeaseGrant, row, 0, opts, timeout);
    }
    [[nodiscard]] LeaseResult updateAndWait(const T& row, int64_t version, const LeaseOptions& opts = {},
                                            std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        return writeAndWait_(OpKind::LeaseUpdate, row, version, opts, timeout);
    }
    [[nodiscard]] LeaseResult revokeAndWait(const T& keyRow, int64_t version, const LeaseOptions& opts = {},
                                            std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        return writeAndWait_(OpKind::LeaseRevoke, keyRow, version, opts, timeout);
    }
    [[nodiscard]] LeaseResult keepAliveOnceAndWait(const T& row, int64_t version, const LeaseOptions& opts = {},
                                                   std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        KeyBuf k;
        if (!keyOf_(row, k)) return LeaseResult{OpStatus::InvalidArgument, 0, 0, 0};
        return core_.keepAliveAndWait(k.span(), version, opts, timeout);
    }
    [[nodiscard]] LeaseResult timeToLiveAndWait(const T& row, LeaseTtlInfo* info = nullptr, const LeaseOptions& opts = {},
                                                std::chrono::nanoseconds timeout = std::chrono::seconds(10)) {
        KeyBuf k;
        if (!keyOf_(row, k)) return LeaseResult{OpStatus::InvalidArgument, 0, 0, 0};
        return core_.timeToLiveAndWait(k.span(), opts, timeout, info);
    }

private:
    friend class Client;
    explicit LeaseTopic(detail::LeaseTopicCore&& core) noexcept : core_(std::move(core)) {}

    /// The key bytes of a row: at most 128, held inline (no heap).
    struct KeyBuf {
        uint8_t     bytes[rune::protocol::kMaxLeaseKeyBytes]{};
        std::size_t n = 0;
        [[nodiscard]] std::span<const uint8_t> span() const noexcept { return {bytes, n}; }
    };
    /// False when the key is empty or wider than 128 bytes.
    [[nodiscard]] static bool keyOf_(const T& row, KeyBuf& out) noexcept {
        const auto kv = row.key();
        using KeyT = std::remove_cvref_t<decltype(kv)>;
        if constexpr (std::is_same_v<KeyT, std::span<const uint8_t>>) {
            if (kv.empty() || kv.size() > sizeof out.bytes) return false;
            std::memcpy(out.bytes, kv.data(), kv.size());
            out.n = kv.size();
        } else {
            static_assert(std::is_trivially_copyable_v<KeyT> && sizeof(KeyT) <= sizeof out.bytes,
                          "LeaseTopic: key() must return a trivially-copyable value or a byte span");
            std::memcpy(out.bytes, &kv, sizeof kv);
            out.n = sizeof kv;
        }
        return true;
    }
    /// Encodes `row` into the handle's scratch (stamped with the bound version); 0 = nothing to send.
    [[nodiscard]] std::size_t encode_(const T& row) {
        auto& buf = core_.scratch();
        const std::size_t need = row.encodedSize();
        if (need > buf.size()) buf.resize(need);   // grows to the high-water mark only
        const std::size_t n = row.encode(buf.data());
        if (n >= 4) {
            buf[2] = static_cast<uint8_t>(core_.boundVersion() & 0xffu);
            buf[3] = static_cast<uint8_t>((core_.boundVersion() >> 8) & 0xffu);
        }
        return n;
    }
    [[nodiscard]] OpResult writeRow_(OpKind kind, const T& row, int64_t version, const LeaseOptions& opts,
                                     CompletionTarget target, Token token) noexcept {
        KeyBuf k;
        if (core_.closed() || !keyOf_(row, k)) return OpResult::InvalidArgument;
        const std::size_t n = encode_(row);
        if (n == 0) return OpResult::InvalidArgument;
        return core_.write(kind, k.span(), n, version, opts, target, token);
    }
    [[nodiscard]] LeaseResult writeAndWait_(OpKind kind, const T& row, int64_t version, const LeaseOptions& opts,
                                            std::chrono::nanoseconds timeout) {
        KeyBuf k;
        if (core_.closed() || !keyOf_(row, k)) return LeaseResult{OpStatus::InvalidArgument, 0, 0, 0};
        const std::size_t n = encode_(row);
        if (n == 0) return LeaseResult{OpStatus::InvalidArgument, 0, 0, 0};
        return core_.writeAndWait(kind, k.span(), n, version, opts, timeout);
    }

    detail::LeaseTopicCore core_;
};

template <class T>
LeaseTopic<T> LeaseTopic<T>::fromOpen(Client& client, const Completion& c, std::span<const uint8_t> payload,
                                      const LeaseTopicConfig& cfg) {
    if (c.op != OpKind::OpenLeaseTopic || c.status != OpStatus::Ok)
        throw std::runtime_error("openLeaseTopic: the open did not complete Ok");
    const auto lease = schemaLeaseOf(c, payload);
    if (!lease) throw std::invalid_argument("openLeaseTopic: the topic is not a lease topic");
    if (!schemaMatches(T::kFingerprint, schemaBlobOf(c, payload)))
        throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
    const int32_t viewId = schemaLeaseViewTopicIdOf(c, payload);
    if (viewId < 0) throw std::runtime_error("openLeaseTopic: the broker did not report the lease view");
    const auto partitions = typedPartitionCount(static_cast<int32_t>(c.reserved0));
    const auto kf = schemaHasKeyFeatures(c) ? schemaKeyFeaturesOf(c, payload).partitioner : rune::protocol::Partitioner::Fnv1a;
    uint32_t opTimeout = cfg.opTimeoutMs != 0 ? cfg.opTimeoutMs : static_cast<uint32_t>(lease->ttlMs / 3);
    if (opTimeout == 0) opTimeout = 1;
    ProducerConfig pc;
    pc.acks              = Acks::All | Acks::View;
    pc.lingerUs          = 100;
    pc.leaseWrites       = true;
    pc.produceTimeoutMs  = opTimeout;
    pc.deliveryTimeoutMs = opTimeout;
    return LeaseTopic(detail::LeaseTopicCore(client, client.producer(pc), static_cast<int32_t>(c.value), viewId, partitions,
                                             kf, *lease, static_cast<uint16_t>(c.aux & 0xFFFFu), opTimeout));
}

template <class T>
LeaseTopic<T> Client::openLeaseTopicAndWait(std::string_view name, const LeaseTopicConfig& cfg,
                                            std::chrono::nanoseconds timeout) {
    const TopicSchemaResult r = openLeaseSchemaAndWait(name, T::kFingerprint, timeout);
    if (r.errorCode != 0) throwOpenTopicFailure(name, r.errorCode, timeout);
    if (!r.lease) throw std::invalid_argument("openLeaseTopic: '" + std::string(name) + "' is not a lease topic");
    if (!schemaMatches(T::kFingerprint, std::span<const uint8_t>(r.descriptorBlob)))
        throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
    if (r.leaseViewTopicId < 0) throw std::runtime_error("openLeaseTopic: the broker did not report the lease view");
    uint32_t opTimeout = cfg.opTimeoutMs != 0 ? cfg.opTimeoutMs : static_cast<uint32_t>(r.lease->ttlMs / 3);
    if (opTimeout == 0) opTimeout = 1;
    ProducerConfig pc;
    pc.acks              = Acks::All | Acks::View;
    pc.lingerUs          = 100;
    pc.leaseWrites       = true;
    pc.produceTimeoutMs  = opTimeout;
    pc.deliveryTimeoutMs = opTimeout;
    return LeaseTopic<T>(detail::LeaseTopicCore(*this, producer(pc), r.topicId, r.leaseViewTopicId,
                                                typedPartitionCount(r.partitionCount), r.keyFeatures.partitioner, *r.lease,
                                                r.version, opTimeout));
}

}  // namespace rune::client
