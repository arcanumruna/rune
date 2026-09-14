// include/rune/client/Key.h
//
// CB-2 — Binary Key for the Producer hot path.
//
// Design:
//   - Holds up to 16 bytes inline (no heap alloc for small keys).
//   - Accepts:  uint64_t / int64_t (stored as little-endian bytes),
//               std::span<const uint8_t> (arbitrary byte slice),
//               any trivially-copyable POD via hashing its raw bytes.
//   - ZERO per-construction heap allocation for payloads ≤ kMaxInline bytes.
//   - Partition = rune::protocol::partitionForKey(bytes(), partitionCount)
//     — the SAME FNV-1a hash the broker (GB-1) uses.
//   - Copyable and movable (trivial for small keys).

#pragma once

#include <rune/protocol/PartitionHash.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
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

    /// Construct from a byte span.  Truncates silently to kMaxInline bytes
    /// (caller must ensure the key is not longer than the limit; the partition
    /// hash remains stable on the bytes actually stored).
    explicit Key(std::span<const uint8_t> bytes) noexcept {
        buf_.fill(0);
        len_ = static_cast<uint8_t>(
            bytes.size() < kMaxInline ? bytes.size() : kMaxInline);
        std::memcpy(buf_.data(), bytes.data(), len_);
    }

    /// Construct from a trivially-copyable POD by treating its raw bytes as the
    /// key.  Enables Key(MyPodStruct{...}) — the struct's byte layout is the key.
    /// Truncates to kMaxInline bytes.
    template <typename T,
              typename = std::enable_if_t<std::is_trivially_copyable_v<T> &&
                                          !std::is_same_v<T, uint64_t> &&
                                          !std::is_same_v<T, int64_t>>>
    explicit Key(const T& pod) noexcept {
        buf_.fill(0);
        const auto sz = sizeof(T) < kMaxInline ? sizeof(T) : kMaxInline;
        len_ = static_cast<uint8_t>(sz);
        std::memcpy(buf_.data(), &pod, sz);
    }

    // ---- Accessors ----------------------------------------------------------

    /// Return a span over the raw key bytes.
    [[nodiscard]] std::span<const uint8_t> bytes() const noexcept {
        return { buf_.data(), static_cast<std::size_t>(len_) };
    }

    [[nodiscard]] std::size_t size() const noexcept {
        return static_cast<std::size_t>(len_);
    }

    [[nodiscard]] bool empty() const noexcept { return len_ == 0; }

    // ---- Partition resolution ------------------------------------------------

    /// Compute the partition for this key given partitionCount (≥ 1).
    /// Uses the shared FNV-1a hash — identical to the broker (GB-1).
    [[nodiscard]] int32_t partition(int32_t partitionCount) const noexcept {
        if (partitionCount <= 1) return 0;
        return rune::protocol::partitionForKey(bytes(), partitionCount);
    }

private:
    std::array<uint8_t, kMaxInline> buf_{};
    uint8_t                         len_ = 0;
};

} // namespace rune::client
