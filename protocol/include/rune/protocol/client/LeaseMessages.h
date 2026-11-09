#pragma once
// LeaseMessages.h — lease-topic client messages (client version 18, templateIds 96-101).
//
// Every frame: MessageHeader (24 bytes, blockLength = the fixed part) + fixed part + var part, little-endian.
// Every decode checks schemaId / templateId, never reads past `avail`, validates the whole frame before returning and
// refuses trailing bytes (the frame must be consumed exactly).  Keys, prefixes and rows are zero-copy spans into the
// input buffer.  A key is a u8 length prefix + bytes (at most kMaxLeaseKeyBytes).
//
//   LeaseKeepAlive (96)          fixed: topicId i32, partition i32, count u16
//                                var:   count x { keyLen u8, key, version i64 }          (count 1..256)
//   LeaseKeepAliveResponse (97)  fixed: final u8 (0|1), count u16
//                                var:   count x { index u16, code u16, ttlMs u64 }       (count 1..256)
//   LeaseTimeToLive (98)         fixed: topicId i32, partition i32;  var: keyLen u8 + key
//   LeaseTimeToLiveResponse (99) fixed: code u16, grantedTtlMs u64, remainingMs i64 (-1 = none), version i64, holder u64
//   ScanLeases (100)             fixed: viewTopicId i32, partition i32, resumeHash u64, first u8 (0|1), maxRows u16
//                                var:   prefixLen u8 + prefix                             (maxRows 1..1024, prefix <= 127)
//   ScanLeasesResponse (101)     fixed: code u16, asOfOffset i64, lastHash u64, done u8 (0|1), rowCount u16, rowsLen u32
//                                var:   rowsLen opaque row bytes: rowCount x { keyLen u8, key, rowLen u32 LE, row payload }
//                                       (Task 13b) - the row payload is exactly what a GetViewRowK of that key answers (the lease view's
//                                       row image).  Rows are in ascending order of the broker's scan hash of the key; `lastHash` is the
//                                       hash of the last row (the resume cursor), `done` = 1 when no matching row has a larger hash.
//                                       Rows with an equal hash are always in the same page (a page may then exceed maxRows).
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>

#include "../LeaseSpec.h"
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

inline constexpr std::size_t kLeaseKeepAliveMaxEntries = 256;
inline constexpr std::size_t kScanLeasesMaxRows        = 1024;
inline constexpr std::size_t kScanLeasesMaxPrefixBytes = 127;
/// The most row bytes one ScanLeasesResponse carries: a scan page is bounded like a ScanViewRows page (kMaxPageBytes), minus this response's
/// own fixed part (the 24-byte header and the 25-byte block).  A group of equal-hash rows that does not fit is left for the next page.
inline constexpr std::size_t kScanLeasesMaxPageBytes = 61440 - (24 + 25);

/// A LeaseKeepAliveResponse entry with this index answers EVERY entry of the request with its code (a request-level refusal:
/// NotLeader, Throttled, UnknownTopic, NotAuthorized, InvalidRequest, NamespaceSuspended); it is always the only entry and
/// final = 1. For Throttled, ttlMs is the retry-after in ms (ceil).
inline constexpr uint16_t kLeaseWholeRequest = 0xFFFF;

/// Whether `entryBytes` holds exactly `count` whole LeaseKeepAlive entries (keyLen u8, key, version i64): every entry inside the
/// remaining bytes, key <= kMaxLeaseKeyBytes, exactly the span consumed.
[[nodiscard]] inline bool validateLeaseKeepAliveEntries(std::span<const uint8_t> entryBytes, uint16_t count) noexcept {
    std::size_t pos = 0;
    const std::size_t avail = entryBytes.size();
    for (uint16_t i = 0; i < count; ++i) {
        if (pos >= avail) return false;
        const std::size_t klen = entryBytes[pos++];
        if (klen > kMaxLeaseKeyBytes || avail - pos < klen + 8u) return false;
        pos += klen + 8u;
    }
    return pos == avail;
}

/// Walks `count` entries that validateLeaseKeepAliveEntries accepted: fn(index, key, version).  UNCHECKED (the hot path validates once,
/// then serves); behaviour on unvalidated bytes is undefined.  Not noexcept: the callback may throw.
template <class Fn>
inline void forEachValidatedLeaseKeepAliveEntry(std::span<const uint8_t> entryBytes, uint16_t count, Fn&& fn) {
    const uint8_t* p = entryBytes.data();
    for (uint16_t i = 0; i < count; ++i) {
        const std::size_t klen = *p++;
        const std::span<const uint8_t> key(p, klen);
        p += klen;
        const int64_t version = detail::getLE64(p, 0);
        p += 8;
        fn(i, key, version);
    }
}

/// Walks `count` LeaseKeepAlive entries (keyLen u8, key, version i64) in raw bytes: fn(index, key, version).  The whole span is
/// validated FIRST; false on any mismatch, and fn is then never called.  Not noexcept: the callback may throw.
template <class Fn>
[[nodiscard]] inline bool forEachLeaseKeepAliveEntry(std::span<const uint8_t> entryBytes, uint16_t count, Fn&& fn) {
    if (!validateLeaseKeepAliveEntries(entryBytes, count)) return false;
    forEachValidatedLeaseKeepAliveEntry(entryBytes, count, static_cast<Fn&&>(fn));
    return true;
}

namespace lease_wire {

inline void putHeader(uint8_t* buf, uint16_t blockLength, uint16_t templateId, int64_t sessionId,
                      int64_t correlationId) noexcept {
    MessageHeader hdr;
    hdr.blockLength   = blockLength;
    hdr.templateId    = templateId;
    hdr.schemaId      = kSchemaId;
    hdr.version       = kProtocolVersion;
    hdr.sessionId     = sessionId;
    hdr.correlationId = correlationId;
    hdr.encode(buf);
}

/// Decodes and checks the header; on success `pos` is the start of the var part (header + blockLength) and the fixed
/// part is guaranteed to lie inside `avail`.
inline std::optional<MessageHeader> checkHeader(const uint8_t* buf, std::size_t avail, uint16_t templateId,
                                                uint16_t fixed, std::size_t& pos) noexcept {
    auto h = MessageHeader::decode(buf, avail);
    if (!h || h->schemaId != kSchemaId || h->templateId != templateId || h->blockLength < fixed) return std::nullopt;
    if (avail < MessageHeader::ENCODED_SIZE + std::size_t{h->blockLength}) return std::nullopt;
    pos = MessageHeader::ENCODED_SIZE + h->blockLength;
    return h;
}

constexpr int kBase = static_cast<int>(MessageHeader::ENCODED_SIZE);

} // namespace lease_wire

// ---------------------------------------------------------------------------
// LeaseKeepAlive (96)
// ---------------------------------------------------------------------------
struct LeaseKeepAlive {
    struct Entry {
        std::span<const uint8_t> key;
        int64_t                  version = 0;
    };

    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int32_t topicId       = 0;
    int32_t partition     = 0;
    std::span<const Entry>   entries;      // encode input
    uint16_t                 count = 0;    // decode output
    std::span<const uint8_t> entryBytes;   // decode output: the raw (validated) entries

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaseKeepAlive);   // 96
    static constexpr uint16_t BODY_FIXED  = 10;   // topicId(4) + partition(4) + count(2)
    static constexpr std::size_t kMaxEntries = kLeaseKeepAliveMaxEntries;

    [[nodiscard]] bool fits() const noexcept {
        if (entries.empty() || entries.size() > kMaxEntries) return false;
        for (const Entry& e : entries)
            if (e.key.size() > kMaxLeaseKeyBytes) return false;
        return true;
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept {
        std::size_t n = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        for (const Entry& e : entries) n += 1u + e.key.size() + 8u;
        return n;
    }
    /// Returns the bytes written, 0 when !fits().
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        detail::putLE32(buf, lease_wire::kBase + 0, topicId);
        detail::putLE32(buf, lease_wire::kBase + 4, partition);
        detail::putLE16(buf, lease_wire::kBase + 8, static_cast<uint16_t>(entries.size()));
        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        for (const Entry& e : entries) {
            buf[pos++] = static_cast<uint8_t>(e.key.size());
            if (!e.key.empty()) std::memcpy(buf + pos, e.key.data(), e.key.size());
            pos += e.key.size();
            detail::putLE64(buf, static_cast<int>(pos), e.version);
            pos += 8;
        }
        return pos;
    }
    static std::optional<LeaseKeepAlive> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        LeaseKeepAlive m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.topicId       = detail::getLE32(buf, lease_wire::kBase + 0);
        m.partition     = detail::getLE32(buf, lease_wire::kBase + 4);
        m.count         = detail::getLE16(buf, lease_wire::kBase + 8);
        if (m.count == 0 || m.count > kMaxEntries) return std::nullopt;
        const std::size_t start = pos;
        for (uint16_t i = 0; i < m.count; ++i) {
            if (pos >= avail) return std::nullopt;
            const std::size_t klen = buf[pos++];
            if (klen > kMaxLeaseKeyBytes || avail - pos < klen + 8u) return std::nullopt;
            pos += klen + 8u;
        }
        if (pos != avail) return std::nullopt;
        m.entryBytes = std::span<const uint8_t>(buf + start, avail - start);
        return m;
    }
    /// Visit the entries of a decoded message: fn(index, key, version).
    template <class Fn>
    void forEachEntry(Fn&& fn) const noexcept { (void)forEachLeaseKeepAliveEntry(entryBytes, count, fn); }
};

// ---------------------------------------------------------------------------
// LeaseKeepAliveResponse (97)
// ---------------------------------------------------------------------------
struct LeaseKeepAliveResponse {
    struct Entry {
        uint16_t index = 0;
        uint16_t code  = 0;
        uint64_t ttlMs = 0;
    };

    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    uint8_t final         = 0;   // 1 on the last frame answering the request
    std::span<const Entry>   entries;      // encode input
    uint16_t                 count = 0;    // decode output
    std::span<const uint8_t> entryBytes;   // decode output: count x 12 bytes

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaseKeepAliveResponse);   // 97
    static constexpr uint16_t BODY_FIXED  = 3;    // final(1) + count(2)
    static constexpr std::size_t kEntryBytes  = 12;
    static constexpr std::size_t kMaxEntries  = kLeaseKeepAliveMaxEntries;

    [[nodiscard]] bool fits() const noexcept {
        return final <= 1 && !entries.empty() && entries.size() <= kMaxEntries;
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + kEntryBytes * entries.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        buf[lease_wire::kBase] = final;
        detail::putLE16(buf, lease_wire::kBase + 1, static_cast<uint16_t>(entries.size()));
        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        for (const Entry& e : entries) {
            detail::putLE16(buf, static_cast<int>(pos), e.index);
            detail::putLE16(buf, static_cast<int>(pos + 2), e.code);
            detail::putLE64(buf, static_cast<int>(pos + 4), static_cast<int64_t>(e.ttlMs));
            pos += kEntryBytes;
        }
        return pos;
    }
    static std::optional<LeaseKeepAliveResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        LeaseKeepAliveResponse m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.final         = buf[lease_wire::kBase];
        m.count         = detail::getLE16(buf, lease_wire::kBase + 1);
        if (m.final > 1 || m.count == 0 || m.count > kMaxEntries) return std::nullopt;
        if (avail - pos != kEntryBytes * m.count) return std::nullopt;
        m.entryBytes = std::span<const uint8_t>(buf + pos, avail - pos);
        return m;
    }
    /// Visit the entries of a decoded message: fn(index, code, ttlMs).
    template <class Fn>
    void forEachEntry(Fn&& fn) const noexcept {
        for (uint16_t i = 0; i < count; ++i) {
            const uint8_t* e = entryBytes.data() + kEntryBytes * i;
            fn(detail::getLE16(e, 0), detail::getLE16(e, 2), static_cast<uint64_t>(detail::getLE64(e, 4)));
        }
    }
};

// ---------------------------------------------------------------------------
// LeaseTimeToLive (98)
// ---------------------------------------------------------------------------
struct LeaseTimeToLive {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int32_t topicId       = 0;
    int32_t partition     = 0;
    std::span<const uint8_t> key;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaseTimeToLive);   // 98
    static constexpr uint16_t BODY_FIXED  = 8;

    [[nodiscard]] bool fits() const noexcept { return key.size() <= kMaxLeaseKeyBytes; }
    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 1u + key.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        detail::putLE32(buf, lease_wire::kBase + 0, topicId);
        detail::putLE32(buf, lease_wire::kBase + 4, partition);
        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        buf[pos++] = static_cast<uint8_t>(key.size());
        if (!key.empty()) std::memcpy(buf + pos, key.data(), key.size());
        return pos + key.size();
    }
    static std::optional<LeaseTimeToLive> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h || pos >= avail) return std::nullopt;
        LeaseTimeToLive m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.topicId       = detail::getLE32(buf, lease_wire::kBase + 0);
        m.partition     = detail::getLE32(buf, lease_wire::kBase + 4);
        const std::size_t klen = buf[pos++];
        if (klen > kMaxLeaseKeyBytes || avail - pos != klen) return std::nullopt;
        m.key = std::span<const uint8_t>(buf + pos, klen);
        return m;
    }
};

// ---------------------------------------------------------------------------
// LeaseTimeToLiveResponse (99)
// ---------------------------------------------------------------------------
struct LeaseTimeToLiveResponse {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    uint16_t code          = 0;
    uint64_t grantedTtlMs  = 0;
    int64_t  remainingMs   = -1;   // -1 = none
    int64_t  version       = 0;
    uint64_t holder        = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::LeaseTimeToLiveResponse);   // 99
    static constexpr uint16_t BODY_FIXED  = 34;   // code(2) + grantedTtlMs(8) + remainingMs(8) + version(8) + holder(8)

    [[nodiscard]] bool fits() const noexcept { return true; }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        detail::putLE16(buf, lease_wire::kBase + 0, code);
        detail::putLE64(buf, lease_wire::kBase + 2, static_cast<int64_t>(grantedTtlMs));
        detail::putLE64(buf, lease_wire::kBase + 10, remainingMs);
        detail::putLE64(buf, lease_wire::kBase + 18, version);
        detail::putLE64(buf, lease_wire::kBase + 26, static_cast<int64_t>(holder));
        return MessageHeader::ENCODED_SIZE + BODY_FIXED;
    }
    static std::optional<LeaseTimeToLiveResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h || pos != avail) return std::nullopt;
        LeaseTimeToLiveResponse m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.code          = detail::getLE16(buf, lease_wire::kBase + 0);
        m.grantedTtlMs  = static_cast<uint64_t>(detail::getLE64(buf, lease_wire::kBase + 2));
        m.remainingMs   = detail::getLE64(buf, lease_wire::kBase + 10);
        m.version       = detail::getLE64(buf, lease_wire::kBase + 18);
        m.holder        = static_cast<uint64_t>(detail::getLE64(buf, lease_wire::kBase + 26));
        return m;
    }
};

// ---------------------------------------------------------------------------
// ScanLeases (100)
// ---------------------------------------------------------------------------
struct ScanLeases {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    int32_t  viewTopicId   = 0;
    int32_t  partition     = 0;
    uint64_t resumeHash    = 0;
    uint8_t  first         = 1;   // 1 = start of the scan, 0 = resume after resumeHash
    uint16_t maxRows       = 1;
    std::span<const uint8_t> prefix;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ScanLeases);   // 100
    static constexpr uint16_t BODY_FIXED  = 19;   // viewTopicId(4) + partition(4) + resumeHash(8) + first(1) + maxRows(2)

    [[nodiscard]] bool fits() const noexcept {
        return first <= 1 && maxRows >= 1 && maxRows <= kScanLeasesMaxRows && prefix.size() <= kScanLeasesMaxPrefixBytes;
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 1u + prefix.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        detail::putLE32(buf, lease_wire::kBase + 0, viewTopicId);
        detail::putLE32(buf, lease_wire::kBase + 4, partition);
        detail::putLE64(buf, lease_wire::kBase + 8, static_cast<int64_t>(resumeHash));
        buf[lease_wire::kBase + 16] = first;
        detail::putLE16(buf, lease_wire::kBase + 17, maxRows);
        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        buf[pos++] = static_cast<uint8_t>(prefix.size());
        if (!prefix.empty()) std::memcpy(buf + pos, prefix.data(), prefix.size());
        return pos + prefix.size();
    }
    static std::optional<ScanLeases> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h || pos >= avail) return std::nullopt;
        ScanLeases m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.viewTopicId   = detail::getLE32(buf, lease_wire::kBase + 0);
        m.partition     = detail::getLE32(buf, lease_wire::kBase + 4);
        m.resumeHash    = static_cast<uint64_t>(detail::getLE64(buf, lease_wire::kBase + 8));
        m.first         = buf[lease_wire::kBase + 16];
        m.maxRows       = detail::getLE16(buf, lease_wire::kBase + 17);
        if (m.first > 1 || m.maxRows < 1 || m.maxRows > kScanLeasesMaxRows) return std::nullopt;
        const std::size_t plen = buf[pos++];
        if (plen > kScanLeasesMaxPrefixBytes || avail - pos != plen) return std::nullopt;
        m.prefix = std::span<const uint8_t>(buf + pos, plen);
        return m;
    }
};

// ---------------------------------------------------------------------------
// ScanLeasesResponse (101)
// ---------------------------------------------------------------------------
struct ScanLeasesResponse {
    int64_t  sessionId     = 0;
    int64_t  correlationId = 0;
    uint16_t code          = 0;
    int64_t  asOfOffset    = -1;
    uint64_t lastHash      = 0;
    uint8_t  done          = 0;
    uint16_t rowCount      = 0;
    std::span<const uint8_t> rows;   // opaque encoded rows; zero-copy on decode

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::ScanLeasesResponse);   // 101
    static constexpr uint16_t BODY_FIXED  = 25;   // code(2) + asOfOffset(8) + lastHash(8) + done(1) + rowCount(2) + rowsLen(4)

    [[nodiscard]] bool fits() const noexcept { return done <= 1 && rows.size() <= 0xFFFFFFFFu; }
    [[nodiscard]] std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + rows.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        if (!fits()) return 0;
        lease_wire::putHeader(buf, BODY_FIXED, TEMPLATE_ID, sessionId, correlationId);
        detail::putLE16(buf, lease_wire::kBase + 0, code);
        detail::putLE64(buf, lease_wire::kBase + 2, asOfOffset);
        detail::putLE64(buf, lease_wire::kBase + 10, static_cast<int64_t>(lastHash));
        buf[lease_wire::kBase + 18] = done;
        detail::putLE16(buf, lease_wire::kBase + 19, rowCount);
        detail::putLE32(buf, lease_wire::kBase + 21, static_cast<int32_t>(static_cast<uint32_t>(rows.size())));
        std::size_t pos = MessageHeader::ENCODED_SIZE + BODY_FIXED;
        if (!rows.empty()) std::memcpy(buf + pos, rows.data(), rows.size());
        return pos + rows.size();
    }
    static std::optional<ScanLeasesResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        std::size_t pos = 0;
        auto h = lease_wire::checkHeader(buf, avail, TEMPLATE_ID, BODY_FIXED, pos);
        if (!h) return std::nullopt;
        ScanLeasesResponse m;
        m.sessionId     = h->sessionId;
        m.correlationId = h->correlationId;
        m.code          = detail::getLE16(buf, lease_wire::kBase + 0);
        m.asOfOffset    = detail::getLE64(buf, lease_wire::kBase + 2);
        m.lastHash      = static_cast<uint64_t>(detail::getLE64(buf, lease_wire::kBase + 10));
        m.done          = buf[lease_wire::kBase + 18];
        m.rowCount      = detail::getLE16(buf, lease_wire::kBase + 19);
        const uint32_t rowsLen = static_cast<uint32_t>(detail::getLE32(buf, lease_wire::kBase + 21));
        if (m.done > 1 || avail - pos != rowsLen) return std::nullopt;
        m.rows = std::span<const uint8_t>(buf + pos, rowsLen);
        return m;
    }
};

static_assert(kScanLeasesMaxPageBytes == 61440 - (MessageHeader::ENCODED_SIZE + ScanLeasesResponse::BODY_FIXED),
              "the scan page cap is kMaxPageBytes minus the response's fixed part");

} // namespace rune::protocol::client
