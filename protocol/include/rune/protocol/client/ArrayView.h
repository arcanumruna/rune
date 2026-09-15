#pragma once
// ArrayView.h — Zero-copy, allocation-free views over decoded array fields.
//
// Provides:
//   Int32ArrayView         — fixed-stride view of int32_t values (stride 4)
//   TopicPartition32View   — fixed-stride view of (topicId,partition) pairs (stride 8)
//   OffsetCommitResultView — fixed-stride view of commit results (stride 10)
//   OffsetFetchEntryView   — fixed-stride view of fetch entries (stride 8)
//   OffsetFetchResultView  — fixed-stride view of fetch results (stride 18)
//   OffsetCommitEntriesView — forward-iterable view over variable-stride commit entries
//
// Lifetime: ALL views are valid ONLY while the source buffer (the raw receive
// buffer passed to decode()) is alive.  Do not outlive the buffer.
//
// LE decode happens on each element access (no intermediate copies).  This
// matches the zero-alloc hot-path discipline established by Subscribe::key
// and Produce::recordBatch in DataMessages.h.

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <span>
#include "MessageHeader.h"  // pulls in detail::getLE16/getLE64 (namespace detail)
#include "Codec.h"          // pulls in detail::getLE32/getLE16s

namespace rune::protocol::client {

// ---------------------------------------------------------------------------
// Decoded value types (returned by operator[]).
// ---------------------------------------------------------------------------

/// Decoded element from a TopicPartition32 array (stride 8).
struct TopicPartition32Decoded {
    int32_t topicId   = 0;
    int32_t partition = 0;
};

/// Decoded element from an OffsetCommitResponse.results array (stride 10).
struct OffsetCommitResultDecoded {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int16_t errorCode = 0;
};

/// Decoded element from an OffsetFetch.entries array (stride 8).
struct OffsetFetchEntryDecoded {
    int32_t topicId   = 0;
    int32_t partition = 0;
};

/// Decoded element from an OffsetFetchResponse.results array (stride 18).
struct OffsetFetchResultDecoded {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    int16_t errorCode = 0;
};

/// Zero-copy decoded view of one OffsetCommit entry (variable-stride).
struct OffsetCommitEntryRef {
    int32_t topicId   = 0;
    int32_t partition = 0;
    int64_t offset    = 0;
    std::span<const uint8_t> metadata;  // zero-copy; may be empty
};

// ---------------------------------------------------------------------------
// FixedArrayView<Decoded, Stride, DecodeFn>
//
// Wraps a pointer to the first element byte + count.
// Decodes element i on operator[](i) or on iterator dereference.
// buf must outlive this view.
// ---------------------------------------------------------------------------
template<typename Decoded, std::size_t Stride,
         Decoded (*DecodeFn)(const uint8_t*, std::size_t)>
class FixedArrayView {
public:
    FixedArrayView() noexcept = default;
    FixedArrayView(const uint8_t* data, uint16_t count) noexcept
        : data_(data), count_(count) {}

    std::size_t size()  const noexcept { return count_; }
    bool        empty() const noexcept { return count_ == 0; }

    Decoded operator[](std::size_t i) const noexcept {
        return DecodeFn(data_, i);
    }

    // Forward iterator
    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = Decoded;
        using difference_type   = std::ptrdiff_t;
        using pointer           = void;
        using reference         = Decoded;  // value, not ref

        const uint8_t* ptr;
        uint16_t       remaining;

        Decoded operator*() const noexcept { return DecodeFn(ptr, 0); }

        Iterator& operator++() noexcept { ptr += Stride; --remaining; return *this; }
        Iterator  operator++(int) noexcept { auto c = *this; ++(*this); return c; }
        bool operator==(const Iterator& o) const noexcept { return remaining == o.remaining; }
        bool operator!=(const Iterator& o) const noexcept { return remaining != o.remaining; }
    };

    Iterator begin() const noexcept { return {data_,                    count_}; }
    Iterator end()   const noexcept { return {data_ + count_ * Stride,  0u}; }

private:
    const uint8_t* data_  = nullptr;
    uint16_t       count_ = 0;
};

// ---------------------------------------------------------------------------
// Concrete decode functions (no-alloc, LE on access).
// ---------------------------------------------------------------------------

inline int32_t decodeInt32(const uint8_t* base, std::size_t idx) noexcept {
    return detail::getLE32(base, static_cast<int>(idx * 4u));
}

inline TopicPartition32Decoded decodeTP32(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 8u);
    return { detail::getLE32(base, off), detail::getLE32(base, off + 4) };
}

inline OffsetCommitResultDecoded decodeOCResult(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 10u);
    return { detail::getLE32(base, off),
             detail::getLE32(base, off + 4),
             detail::getLE16s(base, off + 8) };
}

inline OffsetFetchEntryDecoded decodeOFEntry(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 8u);
    return { detail::getLE32(base, off), detail::getLE32(base, off + 4) };
}

inline OffsetFetchResultDecoded decodeOFResult(const uint8_t* base, std::size_t idx) noexcept {
    const int off = static_cast<int>(idx * 18u);
    return { detail::getLE32(base, off),
             detail::getLE32(base, off + 4),
             detail::getLE64(base, off + 8),
             detail::getLE16s(base, off + 16) };
}

// ---------------------------------------------------------------------------
// Concrete view aliases.
// ---------------------------------------------------------------------------

using Int32ArrayView        = FixedArrayView<int32_t,                  4,  decodeInt32>;
using TopicPartition32View  = FixedArrayView<TopicPartition32Decoded,  8,  decodeTP32>;
using OffsetCommitResultView= FixedArrayView<OffsetCommitResultDecoded, 10, decodeOCResult>;
using OffsetFetchEntryView  = FixedArrayView<OffsetFetchEntryDecoded,  8,  decodeOFEntry>;
using OffsetFetchResultView = FixedArrayView<OffsetFetchResultDecoded, 18, decodeOFResult>;

// ---------------------------------------------------------------------------
// OffsetCommitEntriesView — forward-iterable zero-copy view over a
// variable-stride OffsetCommit entries array.
//
// Each entry: topicId(4) + partition(4) + offset(8) + metaLen(2) + metaBytes
// Minimum entry size: 18 bytes (empty metadata).
// ---------------------------------------------------------------------------
class OffsetCommitEntriesView {
public:
    OffsetCommitEntriesView() noexcept = default;
    OffsetCommitEntriesView(const uint8_t* buf, std::size_t startPos,
                            std::size_t avail, uint16_t count) noexcept
        : buf_(buf), startPos_(startPos), avail_(avail), count_(count) {}

    std::size_t size()  const noexcept { return count_; }
    bool        empty() const noexcept { return count_ == 0; }

    /// O(n) random access via linear scan.  Prefer range-for on the hot path.
    OffsetCommitEntryRef operator[](std::size_t idx) const noexcept {
        auto it = begin();
        for (std::size_t i = 0; i < idx && it.remaining > 0; ++i) ++it;
        return *it;
    }

    // Forward iterator — decodes and advances through variable-length entries.
    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = OffsetCommitEntryRef;
        using difference_type   = std::ptrdiff_t;
        using pointer           = const OffsetCommitEntryRef*;
        using reference         = OffsetCommitEntryRef;

        const uint8_t* buf;
        std::size_t    pos;
        std::size_t    avail;
        uint16_t       remaining;

        OffsetCommitEntryRef operator*() const noexcept {
            if (pos + 18u > avail) return {};
            OffsetCommitEntryRef ref;
            ref.topicId   = detail::getLE32(buf, static_cast<int>(pos));
            ref.partition = detail::getLE32(buf, static_cast<int>(pos) + 4);
            ref.offset    = detail::getLE64(buf, static_cast<int>(pos) + 8);
            const uint16_t metaLen = detail::getLE16(buf, static_cast<int>(pos) + 16);
            const std::size_t dataOff = pos + 18u;
            if (dataOff + metaLen <= avail)
                ref.metadata = std::span<const uint8_t>(buf + dataOff, metaLen);
            return ref;
        }

        Iterator& operator++() noexcept {
            if (pos + 18u > avail) { remaining = 0; return *this; }
            const uint16_t metaLen = detail::getLE16(buf, static_cast<int>(pos) + 16);
            pos += static_cast<std::size_t>(18u) + metaLen;
            --remaining;
            return *this;
        }

        Iterator operator++(int) noexcept { auto c = *this; ++(*this); return c; }
        bool operator==(const Iterator& o) const noexcept { return remaining == o.remaining; }
        bool operator!=(const Iterator& o) const noexcept { return remaining != o.remaining; }
    };

    Iterator begin() const noexcept { return {buf_, startPos_, avail_, count_}; }
    Iterator end()   const noexcept { return {buf_, 0,         avail_, 0u}; }

private:
    const uint8_t* buf_      = nullptr;
    std::size_t    startPos_ = 0;
    std::size_t    avail_    = 0;
    uint16_t       count_    = 0;
};

} // namespace rune::protocol::client
