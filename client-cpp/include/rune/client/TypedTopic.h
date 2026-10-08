// include/rune/client/TypedTopic.h
//
// Task 11 — TypedTopic<T>: typed, zero-per-send-alloc producer wrapper.
//
// schemaMatches(expected, blob):
//   Deserialises `blob` and computes its fingerprint; returns true iff it
//   equals `expected`.  Returns false (never throws) on any decode error.
//   Unit-testable without a live broker.
//
// TypedTopic<T, ProducerT>:
//   Wraps a producer handle and a topicId.  send(rec) encodes `rec` into a
//   REUSED buffer (buf_ capacity persists; never freed per send — RULE #4),
//   then routes the encoded bytes through ProducerT::send.
//
//   T contract (generated struct):
//     static constexpr uint64_t     kFingerprint
//     static constexpr std::string_view kSchemaName
//     size_t encodedSize() const noexcept
//     size_t encode(uint8_t*)  const noexcept
//
//   ProducerT contract (default = Producer):
//     void send(TopicHandle, const Key&, std::span<const uint8_t>)
//
//   The second template parameter exists as a test seam: production code uses
//   TypedTopic<MyStruct> (ProducerT defaults to Producer); unit tests that must
//   not link Aeron use TypedTopic<StubT, StubProducer>.
//
// Threading:
//   TypedTopic is NOT thread-safe.  It must be called from a single thread
//   (the app thread that owns it).  The underlying Producer::send is lock-free
//   and thread-safe, but the reuse buffer is owned here.

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/TopicHandle.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
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
        // Intentionally maps ANY failure — malformed blob OR std::bad_alloc from
        // fingerprint()'s internal allocation — to false.  Cold path: called once
        // per openTopic<T>; OOM here is fatal to the open anyway.
        return false;
    }
}

// ---------------------------------------------------------------------------
// TypedTopic<T, ProducerT>
//
// ProducerT defaults to Producer (the real Aeron-backed producer).  The second
// parameter is a test seam: unit tests that must not link Aeron supply a
// no-op stub so buf_ reuse can be asserted without any live transport.
// ---------------------------------------------------------------------------
template <class T, class ProducerT = Producer>
class TypedTopic {
public:
    /// Construct from an existing producer handle, topicId, and the schema
    /// VERSION this topic is bound to (Task 19).  `version` is the version
    /// whose fingerprint matched T::kFingerprint when the broker answered the
    /// open gate (Client::openTopicAndWait / TypedTopic::fromSchema) — NOT
    /// necessarily T::kVersion (the codegen-baked compile-time version): an
    /// old generated client stays bound to the version it compiled against
    /// even after the topic evolves.  Every send() stamps this bound version
    /// onto the encoded record, overwriting the codegen-baked kVersion.
    TypedTopic(ProducerT producer, int32_t topicId, uint16_t version)
        : producer_(std::move(producer))
        , topicId_(topicId)
        , version_(version)
    {}

    // F8c C7 (spec §3.1 rule 3): move-only — it owns its Producer, which is
    // move-only (one owning thread: the reuse buffer is not thread-safe).
    TypedTopic(const TypedTopic&) = delete;
    TypedTopic& operator=(const TypedTopic&) = delete;
    TypedTopic(TypedTopic&&) = default;
    TypedTopic& operator=(TypedTopic&&) = default;

    ~TypedTopic() = default;

    /// F8c C3: build a typed topic from a GetTopicSchema completion (spec
    /// §2.5), on the CALLER's thread — the replacement for openTopic's
    /// async-task continuation.  `client` is the Client the op was issued on
    /// (any type with producer(): a template, so this header does not include
    /// Client.h).  Throws std::runtime_error unless the completion is Ok AND
    /// the descriptor's fingerprint equals T::kFingerprint exactly.
    template <class ClientT>
    [[nodiscard]] static TypedTopic fromSchema(ClientT& client, const Completion& c,
                                               std::span<const uint8_t> descriptorBlob) {
        if (c.status != OpStatus::Ok) {
            throw std::runtime_error("openTopic: schema lookup failed (status " +
                                     std::to_string(static_cast<int>(c.status)) + ", errorCode " +
                                     std::to_string(c.errorCode) + ")");
        }
        if (!schemaMatches(T::kFingerprint, descriptorBlob)) {
            throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
        }
        // c.aux carries the schema version bound to this descriptor (Task 18
        // wire contract: GetTopicSchema completion value=topicId, aux=version).
        return TypedTopic(client.producer(), static_cast<int32_t>(c.value),
                          static_cast<uint16_t>(c.aux));
    }

    // ---- Hot path — ZERO per-send heap allocation ----------------------------

    /// Encode `rec` into the reused buffer, then route through the producer.
    ///
    /// RULE #4: buf_ capacity is never freed between sends.  resize() only
    /// grows the buffer (never shrinks below its current capacity because we
    /// only call resize to the max of current size and needed size, so the
    /// underlying storage always stays at the high-water mark).
    ///
    /// The encoded bytes are passed to ProducerT::send as a fire-and-forget
    /// void send (zero-alloc path).  The topicId is used to build a
    /// TopicHandle{topicId, 1} — partition routing on typed topics uses an
    /// empty key (partition 0).  T13 (e2e) will exercise the actual routing.
    void send(const T& rec) {
        const std::size_t needed = rec.encodedSize();
        // Grow buf_ if needed but never shrink (capacity persists — RULE #4).
        if (needed > buf_.size()) {
            buf_.resize(needed);
        }
        const std::size_t n = rec.encode(buf_.data());
        if (n == 0) return;

        // Task 19: stamp the BOUND version onto the encoded record header,
        // overwriting the codegen-baked T::kVersion at bytes [2..3] (LE u16 —
        // Generator.cpp's encode() layout: [blockLength:u16][version:u16]...).
        // This is what keeps a client compiled against an older schema
        // version writing records tagged with that older version after the
        // topic has evolved out from under it.
        if (n >= 4) {
            buf_[2] = static_cast<uint8_t>(version_ & 0xffu);
            buf_[3] = static_cast<uint8_t>((version_ >> 8) & 0xffu);
        }

        // Route through the existing ProducerT::send (TopicHandle + empty key).
        // TopicHandle{topicId_, 1}: partitionCount=1 → partition=0.
        // For a real deployment with multiple partitions the caller should
        // obtain the TopicHandle from Client::topic() and use a keyed send;
        // this default covers the single-partition / T13 e2e case.
        producer_.send(TopicHandle{topicId_, 1},
                       Key{},
                       std::span<const uint8_t>(buf_.data(), n));
    }

    // ---- Test seam -----------------------------------------------------------

    /// Returns the current capacity of the reuse buffer.
    /// Used in unit tests to assert that buf_ does not reallocate across sends
    /// (capacity must be non-decreasing — proves RULE #4 on the zero-alloc path).
    [[nodiscard]] std::size_t bufCapacity() const noexcept { return buf_.capacity(); }

    /// The schema version this topic is bound to (Task 19 test seam).
    [[nodiscard]] uint16_t boundVersion() const noexcept { return version_; }

private:
    ProducerT              producer_;
    int32_t                topicId_;
    uint16_t               version_ = 0;  // bound schema version; stamped onto every send()
    std::vector<uint8_t>   buf_;   // reused; grown to high-water mark, never freed per send
};

} // namespace rune::client
