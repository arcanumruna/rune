// src/Validator.cpp
#include <rune/codec/Validator.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Utf8.h>
#include <algorithm>
#include <cstring>

namespace rune::codec {

// ---------------------------------------------------------------------------
// Constructor — build precomputed field table once from the descriptor
// ---------------------------------------------------------------------------
Validator::Validator(const Descriptor& d) {
    // Issue #149: a cumulative (evolving) descriptor still lists its retired fields; taken as live they
    // shift the fixed offsets and the var-data walk, and valid records are falsely rejected.  Validate
    // against exactly the fields live at the descriptor's own version (a cold path: construction only).
    const bool hasRetired = std::any_of(d.fields.begin(), d.fields.end(),
                                        [](const FieldDescriptor& f) { return f.untilVersion != 0xFFFF; });
    if (hasRetired) build_(materializeAsOf(d, d.version));
    else            build_(d);
}

void Validator::build_(const Descriptor& d) {
    blockLength_ = d.blockLength;
    version_     = d.version;
    for (const auto& f : d.fields) {
        FieldEntry e;
        e.id           = f.id;
        e.sinceVersion = f.sinceVersion;
        e.type         = f.type;
        e.offset       = f.offset;
        e.size         = isVarLen(f.type) ? 0u : fieldWidth(f);
        e.enumValues   = f.enumValues;   // copy whitelist (int32_t, may be negative)

        if (isVarLen(f.type))
            varFields_.push_back(std::move(e));
        else
            fixedFields_.push_back(std::move(e));
    }
}

// ---------------------------------------------------------------------------
// validate — allocation-free, noexcept
// ---------------------------------------------------------------------------
ValResult Validator::validate(std::span<const uint8_t> rec, StringCheck check) const noexcept {
    // ----------------------------------------------------------------
    // 1. Header must be present (blockLength:u16 + version:u16 = 4 bytes)
    // ----------------------------------------------------------------
    if (rec.size() < 4)
        return {false, 0, "record too short for header"};

    const uint16_t recBlockLen =
        uint16_t(rec[0]) | (uint16_t(rec[1]) << 8);
    const uint16_t recVersion =
        uint16_t(rec[2]) | (uint16_t(rec[3]) << 8);

    // ----------------------------------------------------------------
    // 2. blockLength consistency: for a known version, the record's
    //    declared blockLength must equal the descriptor's blockLength.
    //    (If the record is from a newer version, we can't check — just
    //    accept the declared value so future fields are tolerated.)
    // ----------------------------------------------------------------
    if (recVersion <= version_ && recBlockLen != uint16_t(blockLength_))
        return {false, 0, "blockLength mismatch"};

    // The fixed block ends at byte 4 + recBlockLen.
    const size_t fixedEnd = size_t{4} + recBlockLen;
    if (rec.size() < fixedEnd)
        return {false, 0, "record shorter than declared blockLength"};

    // ----------------------------------------------------------------
    // 3. Fixed fields present in this record version
    // ----------------------------------------------------------------
    for (const auto& e : fixedFields_) {
        if (e.sinceVersion > recVersion)
            continue;   // field not present in this record version

        const size_t off = size_t{4} + e.offset;
        const size_t end = off + e.size;

        if (end > fixedEnd)
            return {false, e.id, "fixed field extends past blockLength"};

        if (e.type == PrimType::FixedStr && check == StringCheck::Utf8) {
            const uint8_t* p = rec.data() + off;
            const auto* nul = static_cast<const uint8_t*>(std::memchr(p, 0, e.size));
            const size_t content = nul ? size_t(nul - p) : e.size;
            for (size_t k = content; k < e.size; ++k)
                if (p[k] != 0)
                    return {false, e.id, "string(N) padding is not zero"};
            if (!validateUtf8({p, content}).ok())
                return {false, e.id, "string is not valid UTF-8"};
        }

        // Enum whitelist check (interpret as signed int32_t per spec)
        if (e.type == PrimType::Enum && !e.enumValues.empty()) {
            int32_t raw;
            uint32_t u = uint32_t(rec[off])
                       | (uint32_t(rec[off + 1]) << 8)
                       | (uint32_t(rec[off + 2]) << 16)
                       | (uint32_t(rec[off + 3]) << 24);
            // reinterpret bit-pattern as signed
            std::memcpy(&raw, &u, sizeof(raw));

            bool found = false;
            for (int32_t allowed : e.enumValues) {
                if (allowed == raw) { found = true; break; }
            }
            if (!found)
                return {false, e.id, "enum value not in whitelist"};
        }
    }

    // ----------------------------------------------------------------
    // 4. Var fields — walk the var-data section sequentially
    // ----------------------------------------------------------------
    size_t pos = fixedEnd;

    // Invariant (additive-only evolution): var fields are appended in
    // non-decreasing sinceVersion order, so walking var-index order while
    // gating each by `sinceVersion <= recordVersion` matches the writer's
    // framing. Out-of-order var-field sinceVersions are unsupported.
    for (const auto& e : varFields_) {
        if (e.sinceVersion > recVersion)
            continue;

        // Read 4-byte LE length prefix
        if (pos + 4 > rec.size())
            return {false, e.id, "var field length prefix truncated"};

        const uint32_t len =
            uint32_t(rec[pos])
            | (uint32_t(rec[pos + 1]) << 8)
            | (uint32_t(rec[pos + 2]) << 16)
            | (uint32_t(rec[pos + 3]) << 24);
        pos += 4;

        if (pos + len > rec.size())
            return {false, e.id, "var field data truncated"};

        if (e.type == PrimType::String && check == StringCheck::Utf8 && !validateUtf8({rec.data() + pos, len}).ok())
            return {false, e.id, "string is not valid UTF-8"};

        pos += len;
    }

    return {true, 0, nullptr};
}

} // namespace rune::codec
