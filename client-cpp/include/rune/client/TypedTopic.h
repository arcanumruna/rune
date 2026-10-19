// include/rune/client/TypedTopic.h
//
// Task 11 — TypedTopic<T>: typed, zero-per-send-alloc producer wrapper.
// Phase 2b Part C — routes each record by its key field over the topic's
// partitions (was: always partition 0, empty key).
//
// schemaMatches(expected, blob):
//   Deserialises `blob` and computes its fingerprint; returns true iff it
//   equals `expected`.  Returns false (never throws) on any decode error.
//   Unit-testable without a live broker.
//
// TypedTopic<T, ProducerT>:
//   Wraps a producer handle and the topic's TopicHandle{topicId,
//   partitionCount} (partitionCount as reported by the broker's GetTopicSchema
//   response).  send(rec) encodes `rec` into a REUSED buffer (buf_ capacity
//   persists; never freed per send — RULE #4) and routes it EXACTLY like
//   ProducerT::send(handle, Key(rec.key()), bytes): same partition, real
//   record key.
//
//   T contract (generated struct):
//     static constexpr uint64_t     kFingerprint
//     static constexpr std::string_view kSchemaName
//     size_t encodedSize() const noexcept
//     size_t encode(uint8_t*)  const noexcept
//     key() const noexcept     — OPTIONAL: generated only when the schema has a
//                                key field (rune-codec generateHeader).  A
//                                schema with NO key field keeps the pre-2b
//                                behaviour: partition 0, empty record key.
//
//   Keys are at most Key::kMaxInline (16) bytes and are NEVER truncated: a
//   fixed key type wider than that fails to compile; a string/bytes key longer
//   than that makes send() throw std::invalid_argument (nothing is sent).
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
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/TopicHandle.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
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

/// Phase 2b Part C: true iff the generated record type exposes its key field
/// (rune-codec emits key() only for schemas with an isKey field).
template <class R>
concept HasRecordKey = requires(const R& r) { r.key(); };

/// Phase 2b Part C: the partition count a typed topic routes over, from the
/// broker-reported value (TopicSchemaResult::partitionCount /
/// Completion::reserved0).  Any value <= 0 means "not reported" (a pre-2b
/// broker reports nothing: 0): fall back to 1 — every record lands on
/// partition 0 exactly as before 2b, never on an out-of-range partition.
[[nodiscard]] constexpr int32_t typedPartitionCount(int32_t reported) noexcept {
    return reported > 0 ? reported : 1;
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
    /// Construct from an existing producer handle, the topic's handle
    /// {topicId, partitionCount} (Phase 2b Part C: partitionCount as reported
    /// by the broker — see typedPartitionCount), and the schema VERSION this
    /// topic is bound to (Task 19).  `version` is the version whose
    /// fingerprint matched T::kFingerprint when the broker answered the open
    /// gate (Client::openTopicAndWait / TypedTopic::fromSchema) — NOT
    /// necessarily T::kVersion (the codegen-baked compile-time version): an
    /// old generated client stays bound to the version it compiled against
    /// even after the topic evolves.  Every send() stamps this bound version
    /// onto the encoded record, overwriting the codegen-baked kVersion.
    TypedTopic(ProducerT producer, TopicHandle topic, uint16_t version)
        : producer_(std::move(producer))
        , topic_(topic)
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
        // GetTopicSchema completion wire contract: value = topicId, aux =
        // version (Task 18), reserved0 = partitionCount (Phase 2b Part C).
        return TypedTopic(client.producer(),
                          TopicHandle{static_cast<int32_t>(c.value),
                                      typedPartitionCount(static_cast<int32_t>(c.reserved0))},
                          static_cast<uint16_t>(c.aux));
    }

    // ---- Hot path — ZERO per-send heap allocation ----------------------------

    /// Encode `rec` into the reused buffer, then route it like
    /// ProducerT::send(handle(), Key(rec.key()), bytes) — or with an empty key
    /// (partition 0) when the schema has no key field.
    ///
    /// RULE #4: buf_ capacity is never freed between sends (it only grows to
    /// the high-water mark); the Key is a 17-byte inline value.
    ///
    /// Throws std::invalid_argument (nothing sent) when a string/bytes key is
    /// longer than Key::kMaxInline — a record key is never truncated.
    void send(const T& rec) {
        if constexpr (HasRecordKey<T>) {
            sendWithKey_(rec, recordKey_(rec));
        } else {
            // No key field in the schema: partition 0, empty record key
            // (documented pre-2b behaviour, kept by spec §C).
            sendWithKey_(rec, Key{});
        }
    }

    // ---- Accessors / test seams ------------------------------------------------

    /// The topic handle every send routes over: {topicId, partitionCount}.
    [[nodiscard]] TopicHandle handle() const noexcept { return topic_; }

    /// Returns the current capacity of the reuse buffer.
    /// Used in unit tests to assert that buf_ does not reallocate across sends
    /// (capacity must be non-decreasing — proves RULE #4 on the zero-alloc path).
    [[nodiscard]] std::size_t bufCapacity() const noexcept { return buf_.capacity(); }

    /// The schema version this topic is bound to (Task 19 test seam).
    [[nodiscard]] uint16_t boundVersion() const noexcept { return version_; }

private:
    /// Phase 2b Part C: the record key, exactly as a Producer::send caller
    /// would build it from the key field — never truncated.
    [[nodiscard]] static Key recordKey_(const T& rec) {
        using KeyT = std::remove_cvref_t<decltype(rec.key())>;
        if constexpr (std::is_same_v<KeyT, std::span<const uint8_t>>) {
            const std::span<const uint8_t> kb = rec.key();
            if (kb.size() > Key::kMaxInline) [[unlikely]] {
                throw std::invalid_argument(
                    "TypedTopic::send: key field is " + std::to_string(kb.size()) +
                    " bytes; a record key holds at most " + std::to_string(Key::kMaxInline) +
                    " (it is never truncated)");
            }
            return Key(kb);
        } else {
            static_assert(std::is_trivially_copyable_v<KeyT>,
                          "TypedTopic: key() must return a trivially-copyable value or a byte span");
            static_assert(sizeof(KeyT) <= Key::kMaxInline,
                          "TypedTopic: the key field is wider than Key::kMaxInline (16 bytes) and "
                          "cannot be a record key without truncation");
            return Key(rec.key());
        }
    }

    void sendWithKey_(const T& rec, const Key& key) {
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

        producer_.send(topic_, key, std::span<const uint8_t>(buf_.data(), n));
    }

    ProducerT              producer_;
    TopicHandle            topic_;         // {topicId, partitionCount} — Phase 2b Part C
    uint16_t               version_ = 0;   // bound schema version; stamped onto every send()
    std::vector<uint8_t>   buf_;           // reused; grown to high-water mark, never freed per send
};

} // namespace rune::client
