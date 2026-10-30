#pragma once
// CheckedWritesValidationModel.h — checked writes Part 02: an independent reference for checkTypedBatch.
// It shares nothing with WriteState.h / BatchRebuild.h / Validator / Utf8.h except the data it reads (the spec's
// descriptors): its own bounded byte cursor over the batch, a straight code-point decoder for UTF-8, field reads at
// the descriptor's offsets, and rules P1-P5 in the order the plan's matrix states them.
#include <rune/node/WriteState.h>

#include <cstdint>
#include <span>

namespace rune::node::cw {

namespace model {

struct Cursor {
    const uint8_t* d;
    std::size_t    end;
    std::size_t    pos = 0;
    bool varint(int64_t& out) {
        uint64_t raw = 0;
        for (int shift = 0; shift < 64 && pos < end; shift += 7) {
            const uint8_t b = d[pos++];
            raw |= static_cast<uint64_t>(b & 0x7F) << shift;
            if (!(b & 0x80)) { out = static_cast<int64_t>(raw >> 1) ^ -static_cast<int64_t>(raw & 1); return true; }
        }
        return false;
    }
};

inline uint32_t le(const uint8_t* p, int n) { uint32_t v = 0; for (int i = 0; i < n; ++i) v |= static_cast<uint32_t>(p[i]) << (8 * i); return v; }

inline bool utf8(const uint8_t* s, std::size_t n) {
    std::size_t i = 0;
    while (i < n) {
        const uint8_t b = s[i];
        if (b < 0x80) { ++i; continue; }
        std::size_t len; uint32_t cp, min;
        if ((b & 0xE0) == 0xC0)      { len = 2; cp = b & 0x1F; min = 0x80; }
        else if ((b & 0xF0) == 0xE0) { len = 3; cp = b & 0x0F; min = 0x800; }
        else if ((b & 0xF8) == 0xF0) { len = 4; cp = b & 0x07; min = 0x10000; }
        else return false;
        if (i + len > n) return false;
        for (std::size_t k = 1; k < len; ++k) {
            if ((s[i + k] & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (s[i + k] & 0x3F);
        }
        if (cp < min || cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += len;
    }
    return true;
}

/// One record's verdict.  `key` / `value` are views into the batch.
inline TypedCheck record(const TopicWriteSpec& spec, const uint8_t* key, std::size_t keyLen, const uint8_t* val, std::size_t valLen, bool checked) {
    using rune::codec::PrimType;
    if (valLen == 0) {
        if (spec.keyFieldId == kNoKeyField) return TypedCheck::Reject;
        if (checked && spec.keyWidth != 0 && keyLen != spec.keyWidth) return TypedCheck::Reject;
        return TypedCheck::Accept;
    }
    if (valLen < 4) return TypedCheck::Reject;
    const uint16_t ver = static_cast<uint16_t>(le(val + 2, 2));
    const rune::codec::Descriptor* d = spec.descriptorFor(ver);
    if (d == nullptr) return ver > spec.maxVersion() ? TypedCheck::VersionAhead : TypedCheck::Reject;
    if (le(val, 2) != d->blockLength) return TypedCheck::Reject;
    const std::size_t fixedEnd = 4 + d->blockLength;
    if (valLen < fixedEnd) return TypedCheck::Reject;
    std::size_t varPos = fixedEnd;
    for (const auto& f : d->fields) {
        if (f.type == PrimType::String || f.type == PrimType::Bytes) {
            if (varPos + 4 > valLen) return TypedCheck::Reject;
            const std::size_t n = le(val + varPos, 4);
            varPos += 4;
            if (n > valLen - varPos) return TypedCheck::Reject;
            if (checked && f.type == PrimType::String && !utf8(val + varPos, n)) return TypedCheck::Reject;
            varPos += n;
            continue;
        }
        const uint8_t* at = val + 4 + f.offset;
        if (f.type == PrimType::Enum && !f.enumValues.empty()) {
            const int32_t e = static_cast<int32_t>(le(at, 4));
            bool known = false;
            for (int32_t a : f.enumValues) known = known || a == e;
            if (!known) return TypedCheck::Reject;
        }
        if (checked && f.type == PrimType::FixedStr) {
            std::size_t content = 0;
            while (content < f.fixedLen && at[content] != 0) ++content;
            for (std::size_t k = content; k < f.fixedLen; ++k)
                if (at[k] != 0) return TypedCheck::Reject;
            if (!utf8(at, content)) return TypedCheck::Reject;
        }
    }
    if (checked && spec.keyFieldId != kNoKeyField && spec.keyWidth != 0) {
        if (keyLen != spec.keyWidth) return TypedCheck::Reject;
        for (const auto& f : d->fields) {
            if (f.id != spec.keyFieldId) continue;
            for (std::size_t k = 0; k < keyLen; ++k)
                if (key[k] != val[4 + f.offset + k]) return TypedCheck::Reject;
            return TypedCheck::Accept;
        }
        return TypedCheck::Reject;
    }
    return TypedCheck::Accept;
}

}  // namespace model

[[nodiscard]] inline TypedCheck referenceCheck(const TopicWriteSpec& spec, std::span<const uint8_t> b, int16_t level) {
    const bool checked = level >= 5;
    if (!checked && !spec.legacyValidate) return TypedCheck::Accept;
    if (b.size() < 61) return TypedCheck::Reject;
    const int64_t total = static_cast<int64_t>(model::le(b.data() + 8, 4)) + 12;
    if (total < 61 || total > static_cast<int64_t>(b.size())) return TypedCheck::Reject;
    const int32_t lastDelta = static_cast<int32_t>(model::le(b.data() + 23, 4));
    const int32_t count     = static_cast<int32_t>(model::le(b.data() + 57, 4));
    if (count < 0) return TypedCheck::Reject;
    model::Cursor c{b.data(), static_cast<std::size_t>(total), 61};
    TypedCheck verdict = TypedCheck::Accept;
    for (int32_t i = 0; i < count; ++i) {
        int64_t bodyLen, ts, offDelta, klen, vlen;
        if (!c.varint(bodyLen) || bodyLen < 0 || static_cast<uint64_t>(bodyLen) > c.end - c.pos) return TypedCheck::Reject;
        const std::size_t recEnd = c.pos + static_cast<std::size_t>(bodyLen);
        if (bodyLen == 0) return TypedCheck::Reject;
        model::Cursor r{b.data(), recEnd, c.pos + 1};   // past the attributes byte
        if (!r.varint(ts) || !r.varint(offDelta) || offDelta < 0 || offDelta > lastDelta || !r.varint(klen)) return TypedCheck::Reject;
        const uint8_t* key = b.data() + r.pos;
        std::size_t keyLen = 0;
        if (klen > 0) {
            if (static_cast<uint64_t>(klen) > recEnd - r.pos) return TypedCheck::Reject;
            keyLen = static_cast<std::size_t>(klen);
            r.pos += keyLen;
        }
        if (!r.varint(vlen)) return TypedCheck::Reject;
        std::size_t valLen = 0;
        if (vlen > 0) {
            if (static_cast<uint64_t>(vlen) > recEnd - r.pos) return TypedCheck::Reject;
            valLen = static_cast<std::size_t>(vlen);
        }
        if (verdict == TypedCheck::Accept) verdict = model::record(spec, key, keyLen, b.data() + r.pos, valLen, checked);
        c.pos = recEnd;
    }
    return verdict;
}

}  // namespace rune::node::cw
