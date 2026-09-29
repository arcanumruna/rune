// include/rune/client/TypedTopic.h
//
// Task 11 — TypedTopic<T>: typed, zero-per-send-alloc producer wrapper.
//
// schemaMatches(expected, blob):
//   Deserialises `blob` and computes its fingerprint; returns true iff it
//   equals `expected`.  Returns false (never throws) on any decode error.
//   Unit-testable without a live broker.
//
// TypedTopic<T>:
//   Wraps a Producer handle and a topicId.  send(rec) encodes `rec` into a
//   REUSED buffer (buf_ capacity persists; never freed per send — RULE #4),
//   then routes the encoded bytes through the existing Producer::send path
//   keyed by topicId.
//
//   T contract (generated struct):
//     static constexpr uint64_t     kFingerprint
//     static constexpr std::string_view kSchemaName
//     size_t encodedSize() const noexcept
//     size_t encode(uint8_t*)  const noexcept
//
// Threading:
//   TypedTopic is NOT thread-safe.  It must be called from a single thread
//   (the app thread that owns it).  The underlying Producer::send is lock-free
//   and thread-safe, but the reuse buffer is owned here.

#pragma once

#include <rune/client/producer/Producer.h>
#include <rune/client/TopicHandle.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <span>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// schemaMatches — unit-testable, no live broker required.
//
// Returns true iff deserialise(blob) succeeds AND fingerprint(d) == expected.
// Returns false on any exception (malformed blob, truncation, etc.).
// ---------------------------------------------------------------------------
[[nodiscard]] inline bool schemaMatches(uint64_t                    expected,
                                         std::span<const uint8_t>   blob) noexcept
{
    try {
        return rune::codec::fingerprint(rune::codec::deserialize(blob)) == expected;
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// TypedTopic<T>
// ---------------------------------------------------------------------------
template <class T>
class TypedTopic {
public:
    /// Construct from an existing Producer handle and topicId.
    /// Typically obtained from Client::openTopic<T>().
    TypedTopic(Producer producer, int32_t topicId)
        : producer_(std::move(producer))
        , topicId_(topicId)
    {}

    // Copyable / movable (Producer is shared_ptr-backed).
    TypedTopic(const TypedTopic&) = default;
    TypedTopic& operator=(const TypedTopic&) = default;
    TypedTopic(TypedTopic&&) = default;
    TypedTopic& operator=(TypedTopic&&) = default;

    ~TypedTopic() = default;

    // ---- Hot path — ZERO per-send heap allocation ----------------------------

    /// Encode `rec` into the reused buffer, then route through the producer.
    ///
    /// RULE #4: buf_ capacity is never freed between sends.  resize() only
    /// grows the buffer (never shrinks below its current capacity because we
    /// only call resize to the max of current size and needed size, so the
    /// underlying storage always stays at the high-water mark).
    ///
    /// The encoded bytes are passed to Producer::send as a fire-and-forget
    /// void send (zero-alloc path).  The topicId is used to build a
    /// TopicHandle{topicId, 1} — partition routing on typed topics uses an
    /// empty key (partition 0).  T13 (e2e) will exercise the actual routing.
    void send(const T& rec) noexcept {
        const std::size_t needed = rec.encodedSize();
        // Grow buf_ if needed but never shrink (capacity persists — RULE #4).
        if (needed > buf_.size()) {
            buf_.resize(needed);
        }
        const std::size_t n = rec.encode(buf_.data());
        if (n == 0) return;

        // Route through the existing Producer::send (TopicHandle + empty key).
        // TopicHandle{topicId_, 1}: partitionCount=1 → partition=0.
        // For a real deployment with multiple partitions the caller should
        // obtain the TopicHandle from Client::topic() and use a keyed send;
        // this default covers the single-partition / T13 e2e case.
        producer_.send(TopicHandle{topicId_, 1},
                       Key{},
                       std::span<const uint8_t>(buf_.data(), n));
    }

private:
    Producer               producer_;
    int32_t                topicId_;
    std::vector<uint8_t>   buf_;   // reused; grown to high-water mark, never freed per send
};

} // namespace rune::client
