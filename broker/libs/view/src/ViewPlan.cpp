// libs/view/src/ViewPlan.cpp
//
// Flat little-endian (de)serialization for ViewPlan (authored) and
// CompiledViewPlan (resolved). Both forms lead with a 1-byte VERSION for
// forward-compat; strings and vectors are length-prefixed with a u32 count.
// Every read is bounds-checked; any truncation/overrun/bad-version yields
// std::nullopt rather than reading out of bounds.
#include <rune/view/ViewPlan.h>
#include <cstring>

namespace rune::view {
namespace {

constexpr uint8_t kCompiledVersion = 1;
constexpr uint8_t kAuthoredVersion = 1;

// ---------------------------------------------------------------------------
// Little-endian write helpers (no heap beyond the destination buffer itself,
// which the caller owns and sizes; no per-write allocation).
// ---------------------------------------------------------------------------
void writeU8(std::vector<uint8_t>& buf, uint8_t v) { buf.push_back(v); }

void writeU16LE(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
}

void writeU32LE(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
}

void writeI32LE(std::vector<uint8_t>& buf, int32_t v) {
    uint32_t u;
    std::memcpy(&u, &v, 4);
    writeU32LE(buf, u);
}

void writeU64LE(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
    }
}

void writeString(std::vector<uint8_t>& buf, const std::string& s) {
    writeU32LE(buf, static_cast<uint32_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

void writeSizing(std::vector<uint8_t>& buf, const ViewSizing& s) {
    writeU32LE(buf, s.chunkBytes);
    writeU32LE(buf, s.capacity);
    writeU64LE(buf, s.maxRows);
}

// ---------------------------------------------------------------------------
// Little-endian read helpers — each returns false (without touching `out`
// meaningfully) on truncation; `pos` only advances on success.
// ---------------------------------------------------------------------------
bool readU8(std::span<const uint8_t> buf, size_t& pos, uint8_t& out) {
    if (pos + 1 > buf.size()) return false;
    out = buf[pos++];
    return true;
}

bool readU16LE(std::span<const uint8_t> buf, size_t& pos, uint16_t& out) {
    if (pos + 2 > buf.size()) return false;
    out = static_cast<uint16_t>(buf[pos]) |
          (static_cast<uint16_t>(buf[pos + 1]) << 8);
    pos += 2;
    return true;
}

bool readU32LE(std::span<const uint8_t> buf, size_t& pos, uint32_t& out) {
    if (pos + 4 > buf.size()) return false;
    out = static_cast<uint32_t>(buf[pos]) |
          (static_cast<uint32_t>(buf[pos + 1]) << 8) |
          (static_cast<uint32_t>(buf[pos + 2]) << 16) |
          (static_cast<uint32_t>(buf[pos + 3]) << 24);
    pos += 4;
    return true;
}

bool readI32LE(std::span<const uint8_t> buf, size_t& pos, int32_t& out) {
    uint32_t u = 0;
    if (!readU32LE(buf, pos, u)) return false;
    std::memcpy(&out, &u, 4);
    return true;
}

bool readU64LE(std::span<const uint8_t> buf, size_t& pos, uint64_t& out) {
    if (pos + 8 > buf.size()) return false;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(buf[pos + i]) << (8 * i);
    }
    pos += 8;
    out = v;
    return true;
}

bool readString(std::span<const uint8_t> buf, size_t& pos, std::string& out) {
    uint32_t len = 0;
    if (!readU32LE(buf, pos, len)) return false;
    if (pos + len > buf.size()) return false;  // overrun guard, no OOB read
    out.assign(reinterpret_cast<const char*>(buf.data() + pos), len);
    pos += len;
    return true;
}

bool readSizing(std::span<const uint8_t> buf, size_t& pos, ViewSizing& out) {
    if (!readU32LE(buf, pos, out.chunkBytes)) return false;
    if (!readU32LE(buf, pos, out.capacity))   return false;
    if (!readU64LE(buf, pos, out.maxRows))    return false;
    return true;
}

bool isValidPrimType(uint8_t v) {
    return v <= static_cast<uint8_t>(rune::codec::PrimType::FixedStr);
}

bool isValidMergeRule(uint8_t v) {
    return v <= static_cast<uint8_t>(MergeRule::LastNonNull);
}

bool readPrimType(std::span<const uint8_t> buf, size_t& pos, rune::codec::PrimType& out) {
    uint8_t v = 0;
    if (!readU8(buf, pos, v)) return false;
    if (!isValidPrimType(v)) return false;
    out = static_cast<rune::codec::PrimType>(v);
    return true;
}

bool readMergeRule(std::span<const uint8_t> buf, size_t& pos, MergeRule& out) {
    uint8_t v = 0;
    if (!readU8(buf, pos, v)) return false;
    if (!isValidMergeRule(v)) return false;
    out = static_cast<MergeRule>(v);
    return true;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// CompiledViewPlan
//
// Layout: version:u8, viewName:str, viewTopicId:i32,
//         keyFieldId: u32 count + u16 each,
//         sourceTopicId: u32 count + i32 each,
//         outputFingerprint:u64,
//         output: u32 count + { outSlot:u16, type:u8, sourceIdx:u16,
//                                srcFieldId:u16, rule:u8 } each,
//         persistence:u8, sizing:{chunkBytes:u32, capacity:u32, maxRows:u64}
// ---------------------------------------------------------------------------
std::vector<uint8_t> serialize(const CompiledViewPlan& plan) {
    std::vector<uint8_t> buf;
    buf.reserve(64 + plan.viewName.size() + plan.keyFieldId.size() * 2 +
                plan.sourceTopicId.size() * 4 + plan.output.size() * 8);

    writeU8(buf, kCompiledVersion);
    writeString(buf, plan.viewName);
    writeI32LE(buf, plan.viewTopicId);

    writeU32LE(buf, static_cast<uint32_t>(plan.keyFieldId.size()));
    for (uint16_t k : plan.keyFieldId) writeU16LE(buf, k);

    writeU32LE(buf, static_cast<uint32_t>(plan.sourceTopicId.size()));
    for (int32_t t : plan.sourceTopicId) writeI32LE(buf, t);

    writeU64LE(buf, plan.outputFingerprint);

    writeU32LE(buf, static_cast<uint32_t>(plan.output.size()));
    for (const CompiledField& f : plan.output) {
        writeU16LE(buf, f.outSlot);
        writeU8(buf, static_cast<uint8_t>(f.type));
        writeU16LE(buf, f.sourceIdx);
        writeU16LE(buf, f.srcFieldId);
        writeU8(buf, static_cast<uint8_t>(f.rule));
    }

    writeU8(buf, plan.persistence);
    writeSizing(buf, plan.sizing);

    return buf;
}

std::optional<CompiledViewPlan> deserializeCompiledViewPlan(std::span<const uint8_t> buf) {
    size_t pos = 0;
    CompiledViewPlan p;

    uint8_t version = 0;
    if (!readU8(buf, pos, version))          return std::nullopt;
    if (version != kCompiledVersion)         return std::nullopt;

    if (!readString(buf, pos, p.viewName))   return std::nullopt;
    if (!readI32LE(buf, pos, p.viewTopicId)) return std::nullopt;

    // NOTE: counts below are untrusted wire data -- do NOT .reserve(count)
    // before the loop (an adversarial/corrupt count like 0xFFFFFFFF would
    // attempt a multi-GB allocation and crash with std::bad_alloc/
    // length_error instead of returning nullopt). push_back grows safely,
    // and each loop's per-element bounds check already rejects a count
    // that doesn't fit the remaining bytes.
    uint32_t keyCount = 0;
    if (!readU32LE(buf, pos, keyCount))      return std::nullopt;
    for (uint32_t i = 0; i < keyCount; ++i) {
        uint16_t k = 0;
        if (!readU16LE(buf, pos, k)) return std::nullopt;
        p.keyFieldId.push_back(k);
    }

    uint32_t srcCount = 0;
    if (!readU32LE(buf, pos, srcCount))      return std::nullopt;
    for (uint32_t i = 0; i < srcCount; ++i) {
        int32_t t = 0;
        if (!readI32LE(buf, pos, t)) return std::nullopt;
        p.sourceTopicId.push_back(t);
    }

    if (!readU64LE(buf, pos, p.outputFingerprint)) return std::nullopt;

    uint32_t outCount = 0;
    if (!readU32LE(buf, pos, outCount))      return std::nullopt;
    for (uint32_t i = 0; i < outCount; ++i) {
        CompiledField f{};
        if (!readU16LE(buf, pos, f.outSlot))      return std::nullopt;
        if (!readPrimType(buf, pos, f.type))      return std::nullopt;
        if (!readU16LE(buf, pos, f.sourceIdx))    return std::nullopt;
        if (!readU16LE(buf, pos, f.srcFieldId))   return std::nullopt;
        if (!readMergeRule(buf, pos, f.rule))     return std::nullopt;
        p.output.push_back(f);
    }

    if (!readU8(buf, pos, p.persistence))    return std::nullopt;
    if (!readSizing(buf, pos, p.sizing))     return std::nullopt;

    return p;
}

// ---------------------------------------------------------------------------
// ViewPlan (authored)
//
// Layout: version:u8, viewName:str, key:str,
//         sources: u32 count + { topicId:i32, keyField:str } each,
//         output: u32 count + { name:str, type:u8, sourceIdx:u16,
//                                srcField:str, rule:u8 } each,
//         persistence:u8, sizing:{chunkBytes:u32, capacity:u32, maxRows:u64}
// ---------------------------------------------------------------------------
std::vector<uint8_t> serializeAuthored(const ViewPlan& plan) {
    std::vector<uint8_t> buf;
    buf.reserve(64 + plan.viewName.size() + plan.key.size());

    writeU8(buf, kAuthoredVersion);
    writeString(buf, plan.viewName);
    writeString(buf, plan.key);

    writeU32LE(buf, static_cast<uint32_t>(plan.sources.size()));
    for (const SourceSpec& s : plan.sources) {
        writeI32LE(buf, s.topicId);
        writeString(buf, s.keyField);
    }

    writeU32LE(buf, static_cast<uint32_t>(plan.output.size()));
    for (const AuthoredField& f : plan.output) {
        writeString(buf, f.name);
        writeU8(buf, static_cast<uint8_t>(f.type));
        writeU16LE(buf, f.sourceIdx);
        writeString(buf, f.srcField);
        writeU8(buf, static_cast<uint8_t>(f.rule));
    }

    writeU8(buf, plan.persistence);
    writeSizing(buf, plan.sizing);

    return buf;
}

std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t> buf) {
    size_t pos = 0;
    ViewPlan p;

    uint8_t version = 0;
    if (!readU8(buf, pos, version))        return std::nullopt;
    if (version != kAuthoredVersion)       return std::nullopt;

    if (!readString(buf, pos, p.viewName)) return std::nullopt;
    if (!readString(buf, pos, p.key))      return std::nullopt;

    // NOTE: see the matching NOTE in deserializeCompiledViewPlan above --
    // these counts are untrusted wire data (deserializeAuthored decodes the
    // client-supplied CreateView planBlob), so no .reserve(count) here.
    uint32_t srcCount = 0;
    if (!readU32LE(buf, pos, srcCount))    return std::nullopt;
    for (uint32_t i = 0; i < srcCount; ++i) {
        SourceSpec s{};
        if (!readI32LE(buf, pos, s.topicId))   return std::nullopt;
        if (!readString(buf, pos, s.keyField)) return std::nullopt;
        p.sources.push_back(std::move(s));
    }

    uint32_t outCount = 0;
    if (!readU32LE(buf, pos, outCount))    return std::nullopt;
    for (uint32_t i = 0; i < outCount; ++i) {
        AuthoredField f{};
        if (!readString(buf, pos, f.name))       return std::nullopt;
        if (!readPrimType(buf, pos, f.type))      return std::nullopt;
        if (!readU16LE(buf, pos, f.sourceIdx))    return std::nullopt;
        if (!readString(buf, pos, f.srcField))    return std::nullopt;
        if (!readMergeRule(buf, pos, f.rule))     return std::nullopt;
        p.output.push_back(std::move(f));
    }

    if (!readU8(buf, pos, p.persistence))  return std::nullopt;
    if (!readSizing(buf, pos, p.sizing))   return std::nullopt;

    return p;
}

} // namespace rune::view
