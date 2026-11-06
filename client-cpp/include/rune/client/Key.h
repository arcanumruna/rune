// include/rune/client/Key.h
//
// CB-2 — Binary Key for the Producer hot path.
//
// Design:
//   - Holds up to 16 bytes inline (no heap alloc for small keys).  A key is NEVER truncated:
//     a span longer than kMaxInline makes an over-long key (tooLong(), no bytes) that every
//     producer entry refuses with SendResult::KeyTooLong; a POD wider than kMaxInline does
//     not compile.
//   - Accepts:  uint64_t / int64_t (stored as little-endian bytes),
//               std::span<const uint8_t> (arbitrary byte slice),
//               any trivially-copyable POD via hashing its raw bytes.
//   - ZERO per-construction heap allocation for payloads ≤ kMaxInline bytes.
//   - Partition = rune::protocol::partitionForKey(bytes(), partitionCount)
//     — the SAME function the broker (GB-1, TopicRegistry::partitionForKey)
//     calls; an empty key is partition 0 (rule lives in PartitionHash.h).
//   - Copyable and movable (trivial for small keys).

#pragma once

#include <rune/protocol/PartitionHash.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <type_traits>

namespace rune::client {

// ---------------------------------------------------------------------------
// Key
// ---------------------------------------------------------------------------
class Key {
public:
    // Maximum bytes stored inline (no heap).
    static constexpr std::size_t kMaxInline = 16;

    // ---- Constructors -------------------------------------------------------

    /// Default: empty key (zero bytes).  Partition = 0 for any partitionCount.
    Key() noexcept : len_(0) { buf_.fill(0); }

    /// Construct from an unsigned 64-bit integer (stored as 8 LE bytes).
    explicit Key(uint64_t v) noexcept : len_(8) {
        buf_.fill(0);
        for (int i = 0; i < 8; ++i)
            buf_[static_cast<std::size_t>(i)] = static_cast<uint8_t>(v >> (i * 8));
    }

    /// Construct from a signed 64-bit integer (same bit pattern as uint64_t).
    explicit Key(int64_t v) noexcept : Key(static_cast<uint64_t>(v)) {}

    /// Construct from a byte span.  Longer than kMaxInline bytes is an over-long key (tooLong(), no
    /// bytes): never truncated, so the producer refuses it instead of routing and keying a prefix.
    explicit Key(std::span<const uint8_t> bytes) noexcept {
        buf_.fill(0);
        if (bytes.size() > kMaxInline) {
            len_ = kTooLongLen;
            return;
        }
        len_ = static_cast<uint8_t>(bytes.size());
        std::memcpy(buf_.data(), bytes.data(), len_);
    }

    /// The record key of a string(`fieldWidth`) key field holding `value` (at most `fieldWidth` bytes): for a field up to
    /// kMaxInline wide the zero-padded field; for a wider one (send cannot route it) the value without trailing NULs, so
    /// "abc" and "abc\0" route the same.  TypedTopic::claim and ViewKeyBuilder::partition() both key with it.
    [[nodiscard]] static Key forStringField(std::string_view value, std::size_t fieldWidth) noexcept {
        if (fieldWidth > kMaxInline) {
            std::size_t len = value.size();
            while (len > 0 && value[len - 1] == '\0') --len;
            return Key(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value.data()), len));
        }
        std::array<uint8_t, kMaxInline> field{};
        std::memcpy(field.data(), value.data(), value.size() < fieldWidth ? value.size() : fieldWidth);
        return Key(std::span<const uint8_t>(field.data(), fieldWidth));
    }

    /// Construct from a trivially-copyable POD by treating its raw bytes as the
    /// key.  Enables Key(MyPodStruct{...}) — the struct's byte layout is the key.
    /// A POD wider than kMaxInline does not compile.
    template <typename T,
              typename = std::enable_if_t<std::is_trivially_copyable_v<T> && (sizeof(T) <= kMaxInline) &&
                                          !std::is_same_v<T, uint64_t> &&
                                          !std::is_same_v<T, int64_t>>>
    explicit Key(const T& pod) noexcept {
        buf_.fill(0);
        len_ = static_cast<uint8_t>(sizeof(T));
        std::memcpy(buf_.data(), &pod, sizeof(T));
    }

    // ---- Accessors ----------------------------------------------------------

    /// Return a span over the raw key bytes.
    [[nodiscard]] std::span<const uint8_t> bytes() const noexcept {
        return { buf_.data(), size() };
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return len_ == kTooLongLen ? std::size_t{0} : static_cast<std::size_t>(len_);
    }

    /// True for a key built from more than kMaxInline bytes (bytes() is empty): producers refuse it.
    [[nodiscard]] bool tooLong() const noexcept { return len_ == kTooLongLen; }

    [[nodiscard]] bool empty() const noexcept { return len_ == 0; }

    // ---- Partition resolution ------------------------------------------------

    /// Compute the partition for this key given partitionCount (≥ 1).
    /// Exactly rune::protocol::partitionForKey(bytes(), partitionCount) — the
    /// one rule the broker (TopicRegistry::partitionForKey, used whenever a
    /// request carries partitionKnown=0) also applies, so a client-routed
    /// produce and a broker-resolved subscribe of the same Key always agree.
    /// No client-side special cases: an empty key is partition 0 because
    /// partitionForKey says so (Phase 2b C).
    [[nodiscard]] int32_t partition(int32_t partitionCount) const noexcept {
        return rune::protocol::partitionForKey(bytes(), partitionCount);
    }
    /// The partition under the topic's partitioner (Fnv1a is the form above; Modulo reads the key as an id).
    [[nodiscard]] int32_t partition(int32_t partitionCount, rune::protocol::Partitioner partitioner) const noexcept {
        return rune::protocol::partitionFor(bytes(), partitionCount, partitioner);
    }

private:
    static constexpr uint8_t kTooLongLen = 0xFF;
    std::array<uint8_t, kMaxInline> buf_{};
    uint8_t                         len_ = 0;
};

} // namespace rune::client
