// libs/view/src/ViewPlan.cpp
//
// Flat little-endian (de)serialization for
// CompiledViewPlan (resolved; the authored form lives in the codec). Leads with a 1-byte VERSION for
// forward-compat; strings and vectors are length-prefixed with a u32 count.
// Every read is bounds-checked; any truncation/overrun/bad-version yields
// std::nullopt rather than reading out of bounds.
#include <rune/view/ViewPlan.h>
#include <algorithm>
#include <cstring>
#include <rune/codec/ViewKeyEncoding.h>

namespace rune::view {
namespace {

constexpr uint8_t kCompiledVersionV1 = 1, kCompiledVersion = 2;

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

bool isValidPredicateOp(uint8_t v) {
    return v >= static_cast<uint8_t>(PredicateOp::Column) && v <= static_cast<uint8_t>(PredicateOp::Not);
}

// A lowered component only knows its encoded width, so for lower() the width is checked for being non-zero, not recomputed.
bool validKeyComponent(const CompiledKeyComponent& c) {
    if (c.keyFunction > rune::codec::kKeyFunctionLower || c.width == 0) return false;
    if (c.keyFunction == rune::codec::kKeyFunctionLower) return c.type == rune::codec::PrimType::FixedStr;
    return rune::codec::viewKeyComponentWidth(c.type, c.width, c.keyFunction) == c.width;
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

// The single component a v1 blob implies: the source key field, plain, at output slot 0.
std::optional<CompiledKeyComponent> synthesizedKeyComponent(const CompiledViewPlan& p) {
    if (p.keyFieldId.empty() || p.output.empty()) return std::nullopt;
    const auto type = p.output[0].type;
    return CompiledKeyComponent{p.keyFieldId[0], 0, static_cast<uint16_t>(rune::codec::fixedSize(type)), 0, type};
}

bool sameComponent(const CompiledKeyComponent& a, const CompiledKeyComponent& b) {
    return a.srcFieldId == b.srcFieldId && a.keyFunction == b.keyFunction && a.width == b.width && a.outSlot == b.outSlot && a.type == b.type;
}

bool isAggregate(const CompiledViewPlan& p) {
    return std::any_of(p.output.begin(), p.output.end(), [](const CompiledField& f) { return f.rule != MergeRule::Replace; });
}

// Exactly one plain component, on the source key field.
bool isSourceKeyOnly(const CompiledViewPlan& p) {
    return p.keyComponents.size() == 1 && p.keyComponents[0].keyFunction == 0 && !p.keyFieldId.empty() &&
           p.keyComponents[0].srcFieldId == p.keyFieldId[0];
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
// v2 (only when needsV2) appends:
//         keyComponents: u32 count + { srcFieldId:u16, keyFunction:u8, width:u16, outSlot:u16, type:u8 } each,
//         keyWidth:u16, unique:u8,
//         constraints: u32 count + { ordinal:u16, name:str, nodes: u32 count +
//                                    { op:u8, a:u16, b:u16, outSlot:u16, constType:u8, constBytes:16 } each } each,
//         unicodeVersion:u32, viewObjectId:u64
// ---------------------------------------------------------------------------
std::vector<uint8_t> serialize(const CompiledViewPlan& plan) {
    std::vector<uint8_t> buf;
    buf.reserve(64 + plan.viewName.size() + plan.keyFieldId.size() * 2 +
                plan.sourceTopicId.size() * 4 + plan.output.size() * 8);

    const bool v2 = needsV2(plan);
    writeU8(buf, v2 ? kCompiledVersion : kCompiledVersionV1);
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

    if (v2) {
        writeU32LE(buf, static_cast<uint32_t>(plan.keyComponents.size()));
        for (const CompiledKeyComponent& c : plan.keyComponents) {
            writeU16LE(buf, c.srcFieldId);
            writeU8(buf, c.keyFunction);
            writeU16LE(buf, c.width);
            writeU16LE(buf, c.outSlot);
            writeU8(buf, static_cast<uint8_t>(c.type));
        }
        writeU16LE(buf, plan.keyWidth);
        writeU8(buf, plan.unique ? 1 : 0);
        writeU32LE(buf, static_cast<uint32_t>(plan.constraints.size()));
        for (const CompiledConstraint& c : plan.constraints) {
            writeU16LE(buf, c.ordinal);
            writeString(buf, c.name);
            writeU32LE(buf, static_cast<uint32_t>(c.predicate.size()));
            for (const CompiledPredicateNode& n : c.predicate) {
                writeU8(buf, static_cast<uint8_t>(n.op));
                writeU16LE(buf, n.a);
                writeU16LE(buf, n.b);
                writeU16LE(buf, n.outSlot);
                writeU8(buf, static_cast<uint8_t>(n.constType));
                buf.insert(buf.end(), n.constBytes.begin(), n.constBytes.end());
            }
        }
        writeU32LE(buf, plan.unicodeVersion);
        writeU64LE(buf, plan.viewObjectId);
    }

    return buf;
}

std::optional<CompiledViewPlan> deserializeCompiledViewPlan(std::span<const uint8_t> buf) {
    size_t pos = 0;
    CompiledViewPlan p;

    uint8_t version = 0;
    if (!readU8(buf, pos, version))          return std::nullopt;
    if (version != kCompiledVersionV1 && version != kCompiledVersion) return std::nullopt;

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

    if (version == kCompiledVersionV1) {
        if (const auto key = synthesizedKeyComponent(p)) {
            p.keyComponents.push_back(*key);
            p.keyWidth = key->width;
        }
    } else {
        uint32_t compCount = 0;
        if (!readU32LE(buf, pos, compCount)) return std::nullopt;
        for (uint32_t i = 0; i < compCount; ++i) {
            CompiledKeyComponent c{};
            if (!readU16LE(buf, pos, c.srcFieldId))  return std::nullopt;
            if (!readU8(buf, pos, c.keyFunction))    return std::nullopt;
            if (!readU16LE(buf, pos, c.width))       return std::nullopt;
            if (!readU16LE(buf, pos, c.outSlot))     return std::nullopt;
            if (!readPrimType(buf, pos, c.type))     return std::nullopt;
            if (!validKeyComponent(c))               return std::nullopt;
            p.keyComponents.push_back(c);
        }
        uint8_t unique = 0;
        if (!readU16LE(buf, pos, p.keyWidth))        return std::nullopt;
        if (!readU8(buf, pos, unique) || unique > 1) return std::nullopt;
        p.unique = unique == 1;

        uint32_t constraintCount = 0;
        if (!readU32LE(buf, pos, constraintCount))   return std::nullopt;
        for (uint32_t i = 0; i < constraintCount; ++i) {
            CompiledConstraint c{};
            if (!readU16LE(buf, pos, c.ordinal))     return std::nullopt;
            if (!readString(buf, pos, c.name))       return std::nullopt;
            uint32_t nodeCount = 0;
            if (!readU32LE(buf, pos, nodeCount))     return std::nullopt;
            for (uint32_t j = 0; j < nodeCount; ++j) {
                CompiledPredicateNode n{};
                uint8_t op = 0;
                if (!readU8(buf, pos, op) || !isValidPredicateOp(op)) return std::nullopt;
                n.op = static_cast<PredicateOp>(op);
                if (!readU16LE(buf, pos, n.a))       return std::nullopt;
                if (!readU16LE(buf, pos, n.b))       return std::nullopt;
                if (!readU16LE(buf, pos, n.outSlot)) return std::nullopt;
                if (!readPrimType(buf, pos, n.constType)) return std::nullopt;
                if (pos + n.constBytes.size() > buf.size()) return std::nullopt;
                std::memcpy(n.constBytes.data(), buf.data() + pos, n.constBytes.size());
                pos += n.constBytes.size();
                c.predicate.push_back(n);
            }
            p.constraints.push_back(std::move(c));
        }
        if (!readU32LE(buf, pos, p.unicodeVersion))  return std::nullopt;
        if (!readU64LE(buf, pos, p.viewObjectId))    return std::nullopt;
    }

    if (pos != buf.size())                   return std::nullopt;   // trailing bytes
    return p;
}

ViewKeyMode keyModeOf(const CompiledViewPlan& plan) noexcept {
    if (plan.keyComponents.size() != 1) return plan.keyComponents.empty() ? ViewKeyMode::LegacyU64 : ViewKeyMode::Keyed;
    const CompiledKeyComponent& c = plan.keyComponents[0];
    if (c.keyFunction != 0) return ViewKeyMode::Keyed;
    if (isAggregate(plan)) {
        return c.type == rune::codec::PrimType::Int64 || c.type == rune::codec::PrimType::UInt64 ? ViewKeyMode::LegacyU64 : ViewKeyMode::Keyed;
    }
    return isSourceKeyOnly(plan) && isIntegerKeyType(c.type) ? ViewKeyMode::LegacyU64 : ViewKeyMode::Keyed;
}

bool ownerMappedOf(const CompiledViewPlan& plan) noexcept {
    return keyModeOf(plan) == ViewKeyMode::Keyed && !isAggregate(plan) && !isSourceKeyOnly(plan);
}

uint16_t prefixWidthOf(const CompiledViewPlan& plan) noexcept {
    uint16_t width = 0;
    for (size_t i = 0; i + 1 < plan.keyComponents.size(); ++i) width = static_cast<uint16_t>(width + plan.keyComponents[i].width);
    return width;
}

bool needsV2(const CompiledViewPlan& plan) noexcept {
    if (plan.unique || !plan.constraints.empty() || plan.unicodeVersion != 0) return true;
    if (plan.keyComponents.empty()) return false;
    const auto key = synthesizedKeyComponent(plan);
    return plan.keyComponents.size() != 1 || !key || !sameComponent(plan.keyComponents[0], *key);
}

} // namespace rune::view
