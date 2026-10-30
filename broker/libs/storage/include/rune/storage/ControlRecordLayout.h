#pragma once
// ControlRecordLayout.h -- control records of the checked-writes pipeline (types 16-19; 20 is Part 09's).
//
// A control batch with attributes = kAttrControl ONLY (0x0020, transactional clear), producerId / producerEpoch /
// baseSequence = -1 and one record in standard record framing.  The record key is the int16 LE type, the value is
// [version u8 = 1][payload], all little-endian.  producerId -1 keeps these out of TxnCleanState and the PSM feed.
// Builders write into a caller buffer with baseOffset 0 (appendControlBatch restamps it, CRC-safe) and never allocate.
//
//   kVerdictMarker       [fromOffset i64][finalThrough i64][constraintSetId u32][count u32]
//                        count x [offsetDelta u32][code u16][constraintId u32]
//   kConstraintSetChange [constraintSetId u32][count u16] count x [viewObjectId u64][kind u8][planFingerprint u64]
//   kReservation (06) and kTxnJudge (11) are recognised and skipped by every reader; their payloads are those parts'.
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>

namespace rune::storage::control_record {

inline constexpr int16_t     kReservation         = 16;
inline constexpr int16_t     kVerdictMarker       = 17;
inline constexpr int16_t     kConstraintSetChange = 18;
inline constexpr int16_t     kTxnJudge            = 19;
inline constexpr int         kUnparseable         = -1;
inline constexpr uint8_t     kPayloadVersion      = 1;
inline constexpr std::size_t kMaxMarkerEntries    = 4096;

struct MarkerEntry  { uint32_t offsetDelta; uint16_t code; uint32_t constraintId; };
struct MarkerHeader { int64_t fromOffset; int64_t finalThrough; uint32_t constraintSetId; uint32_t count; };
struct ChangeMember { uint64_t viewObjectId; uint8_t kind; uint64_t planFingerprint; };

inline constexpr std::size_t kMarkerFixedBytes = 1 + 8 + 8 + 4 + 4;
inline constexpr std::size_t kMarkerEntryBytes = 4 + 2 + 4;
inline constexpr std::size_t kChangeFixedBytes = 1 + 4 + 2;
inline constexpr std::size_t kChangeMemberBytes = 8 + 1 + 8;

namespace detail {

namespace b = rune::protocol::batch;

inline std::size_t zigzagLen(int64_t v) noexcept {
    uint64_t u = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    std::size_t n = 1;
    while (u >= 0x80) { u >>= 7; ++n; }
    return n;
}
inline std::size_t putZigzag(uint8_t* p, int64_t v) noexcept {
    uint64_t u = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    std::size_t n = 0;
    while (u >= 0x80) { p[n++] = static_cast<uint8_t>(u | 0x80); u >>= 7; }
    p[n++] = static_cast<uint8_t>(u);
    return n;
}
inline bool getZigzag(const uint8_t* p, std::size_t end, std::size_t& pos, int64_t& out) noexcept {
    uint64_t u = 0;
    for (int shift = 0; shift < 64 && pos < end; shift += 7) {
        const uint8_t byte = p[pos++];
        u |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) { out = static_cast<int64_t>((u >> 1) ^ (~(u & 1) + 1)); return true; }
    }
    return false;
}

template <class T> inline void put(uint8_t* p, T v) noexcept { std::memcpy(p, &v, sizeof v); }   // little-endian host
template <class T> inline T get(const uint8_t* p) noexcept { T v; std::memcpy(&v, p, sizeof v); return v; }

// bodyLen of the one record: attributes, tsDelta, offsetDelta, keyLen, key(2), valueLen, value, headerCount
inline std::size_t bodyBytes(std::size_t valueLen) noexcept {
    return 1 + 1 + 1 + 1 + 2 + zigzagLen(static_cast<int64_t>(valueLen)) + valueLen + 1;
}
inline std::size_t batchBytes(std::size_t valueLen) noexcept {
    const std::size_t body = bodyBytes(valueLen);
    return static_cast<std::size_t>(b::kRecordBatchOverhead) + zigzagLen(static_cast<int64_t>(body)) + body;
}

// Writes the batch header and the record framing for a value of `valueLen` bytes; returns the offset of value[0].
inline std::size_t beginBatch(uint8_t* out, int16_t type, std::size_t valueLen) noexcept {
    const std::size_t total = batchBytes(valueLen);
    std::memset(out, 0, static_cast<std::size_t>(b::kRecordBatchOverhead));
    put<int32_t>(out + b::kLengthOffset, static_cast<int32_t>(total - 12));
    put<int32_t>(out + 12, -1);   // partitionLeaderEpoch
    out[16] = static_cast<uint8_t>(b::kMagicValue);
    put<int16_t>(out + b::kAttributesOffset, b::kAttrControl);
    put<int64_t>(out + 43, -1);   // producerId
    put<int16_t>(out + 51, -1);   // producerEpoch
    put<int32_t>(out + 53, -1);   // baseSequence
    put<int32_t>(out + b::kRecordCountOffset, 1);
    std::size_t pos = static_cast<std::size_t>(b::kRecordBatchOverhead);
    pos += putZigzag(out + pos, static_cast<int64_t>(bodyBytes(valueLen)));
    out[pos++] = 0;   // record attributes
    out[pos++] = 0;   // timestampDelta
    out[pos++] = 0;   // offsetDelta
    out[pos++] = 0x04;   // keyLength zigzag(2)
    put<int16_t>(out + pos, type);
    pos += 2;
    pos += putZigzag(out + pos, static_cast<int64_t>(valueLen));
    return pos;
}
inline void sealBatch(uint8_t* out, std::size_t valueEnd) noexcept {
    out[valueEnd] = 0;   // headerCount
    const std::size_t total = valueEnd + 1;
    put<uint32_t>(out + b::kCrcOffset, rune::protocol::crc32c(out + b::kAttributesOffset, total - b::kAttributesOffset));
}

// The value of a well-formed checked-writes control batch of any known type; `type` is its key.
inline std::optional<std::span<const uint8_t>> valueOf(const uint8_t* batch, std::size_t len, int16_t& type) noexcept {
    if (batch == nullptr || len < static_cast<std::size_t>(b::kRecordBatchOverhead) + 8) return std::nullopt;
    if (get<int32_t>(batch + b::kLengthOffset) != static_cast<int32_t>(len) - 12) return std::nullopt;
    if (batch[16] != static_cast<uint8_t>(b::kMagicValue)) return std::nullopt;
    if (get<int16_t>(batch + b::kAttributesOffset) != b::kAttrControl) return std::nullopt;
    if (get<int32_t>(batch + 23) != 0 || get<int64_t>(batch + 43) != -1 || get<int16_t>(batch + 51) != -1 ||
        get<int32_t>(batch + 53) != -1 || get<int32_t>(batch + b::kRecordCountOffset) != 1)
        return std::nullopt;
    if (get<uint32_t>(batch + b::kCrcOffset) != rune::protocol::crc32c(batch + b::kAttributesOffset, len - b::kAttributesOffset))
        return std::nullopt;
    std::size_t pos = static_cast<std::size_t>(b::kRecordBatchOverhead);
    int64_t body = 0, klen = 0, vlen = 0;
    if (!getZigzag(batch, len, pos, body) || body < 0 || pos + static_cast<std::size_t>(body) != len) return std::nullopt;
    if (batch[pos] != 0 || batch[pos + 1] != 0 || batch[pos + 2] != 0) return std::nullopt;
    pos += 3;
    if (!getZigzag(batch, len, pos, klen) || klen != 2 || pos + 2 > len) return std::nullopt;
    type = get<int16_t>(batch + pos);
    pos += 2;
    if (!getZigzag(batch, len, pos, vlen) || vlen < 1 || pos + static_cast<std::size_t>(vlen) + 1 != len) return std::nullopt;
    if (batch[pos] != kPayloadVersion || batch[len - 1] != 0) return std::nullopt;
    return std::span<const uint8_t>(batch + pos, static_cast<std::size_t>(vlen));
}

}  // namespace detail

[[nodiscard]] inline std::size_t markerBatchBytes(uint32_t count) noexcept {
    return detail::batchBytes(kMarkerFixedBytes + count * kMarkerEntryBytes);
}
[[nodiscard]] inline std::size_t changeBatchBytes(uint16_t count) noexcept {
    return detail::batchBytes(kChangeFixedBytes + count * kChangeMemberBytes);
}

/// 0 = `out` too small, or `entries.size() != h.count`, or more than kMaxMarkerEntries.
inline std::size_t buildVerdictMarker(std::span<uint8_t> out, const MarkerHeader& h, std::span<const MarkerEntry> entries) noexcept {
    if (entries.size() != h.count || h.count > kMaxMarkerEntries) return 0;
    const std::size_t total = markerBatchBytes(h.count);
    if (out.size() < total) return 0;
    const std::size_t valueLen = kMarkerFixedBytes + h.count * kMarkerEntryBytes;
    std::size_t p = detail::beginBatch(out.data(), kVerdictMarker, valueLen);
    uint8_t* d = out.data();
    d[p++] = kPayloadVersion;
    detail::put<int64_t>(d + p, h.fromOffset); p += 8;
    detail::put<int64_t>(d + p, h.finalThrough); p += 8;
    detail::put<uint32_t>(d + p, h.constraintSetId); p += 4;
    detail::put<uint32_t>(d + p, h.count); p += 4;
    for (const MarkerEntry& e : entries) {
        detail::put<uint32_t>(d + p, e.offsetDelta); p += 4;
        detail::put<uint16_t>(d + p, e.code); p += 2;
        detail::put<uint32_t>(d + p, e.constraintId); p += 4;
    }
    detail::sealBatch(d, p);
    return total;
}

inline std::size_t buildConstraintSetChange(std::span<uint8_t> out, uint32_t constraintSetId, std::span<const ChangeMember> members) noexcept {
    if (members.size() > UINT16_MAX) return 0;
    const std::size_t total = changeBatchBytes(static_cast<uint16_t>(members.size()));
    if (out.size() < total) return 0;
    const std::size_t valueLen = kChangeFixedBytes + members.size() * kChangeMemberBytes;
    std::size_t p = detail::beginBatch(out.data(), kConstraintSetChange, valueLen);
    uint8_t* d = out.data();
    d[p++] = kPayloadVersion;
    detail::put<uint32_t>(d + p, constraintSetId); p += 4;
    detail::put<uint16_t>(d + p, static_cast<uint16_t>(members.size())); p += 2;
    for (const ChangeMember& m : members) {
        detail::put<uint64_t>(d + p, m.viewObjectId); p += 8;
        d[p++] = m.kind;
        detail::put<uint64_t>(d + p, m.planFingerprint); p += 8;
    }
    detail::sealBatch(d, p);
    return total;
}

/// The type (16-19) of a well-formed checked-writes control batch, else kUnparseable.
[[nodiscard]] inline int recordType(const uint8_t* batch, std::size_t len) noexcept {
    int16_t type = 0;
    if (!detail::valueOf(batch, len, type)) return kUnparseable;
    return type >= kReservation && type <= kTxnJudge ? type : kUnparseable;
}

[[nodiscard]] inline std::optional<MarkerHeader> parseMarkerHeader(const uint8_t* batch, std::size_t len) noexcept {
    int16_t type = 0;
    const auto v = detail::valueOf(batch, len, type);
    if (!v || type != kVerdictMarker || v->size() < kMarkerFixedBytes) return std::nullopt;
    const uint8_t* p = v->data() + 1;
    MarkerHeader h{detail::get<int64_t>(p), detail::get<int64_t>(p + 8), detail::get<uint32_t>(p + 16), detail::get<uint32_t>(p + 20)};
    if (h.count > kMaxMarkerEntries || v->size() != kMarkerFixedBytes + h.count * kMarkerEntryBytes) return std::nullopt;
    return h;
}

/// fn(int64_t offset, uint16_t code, uint32_t constraintId) per entry; false when `batch` is not a marker.
template <class Fn>
bool forEachMarkerEntry(const uint8_t* batch, std::size_t len, Fn&& fn) noexcept {
    const auto h = parseMarkerHeader(batch, len);
    if (!h) return false;
    int16_t type = 0;
    const uint8_t* p = detail::valueOf(batch, len, type)->data() + kMarkerFixedBytes;
    for (uint32_t i = 0; i < h->count; ++i, p += kMarkerEntryBytes)
        fn(h->fromOffset + static_cast<int64_t>(detail::get<uint32_t>(p)), detail::get<uint16_t>(p + 4), detail::get<uint32_t>(p + 6));
    return true;
}

/// false when `batch` is not a change record or holds more members than `out`.
[[nodiscard]] inline bool parseConstraintSetChange(const uint8_t* batch, std::size_t len, uint32_t& setId,
                                                   std::span<ChangeMember> out, uint16_t& count) noexcept {
    int16_t type = 0;
    const auto v = detail::valueOf(batch, len, type);
    if (!v || type != kConstraintSetChange || v->size() < kChangeFixedBytes) return false;
    const uint8_t* p = v->data() + 1;
    setId = detail::get<uint32_t>(p);
    count = detail::get<uint16_t>(p + 4);
    if (v->size() != kChangeFixedBytes + count * kChangeMemberBytes || count > out.size()) return false;
    p += 6;
    for (uint16_t i = 0; i < count; ++i, p += kChangeMemberBytes)
        out[i] = ChangeMember{detail::get<uint64_t>(p), p[8], detail::get<uint64_t>(p + 9)};
    return true;
}

}  // namespace rune::storage::control_record
