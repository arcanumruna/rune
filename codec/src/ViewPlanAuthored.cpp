// codec/src/ViewPlanAuthored.cpp
//
// Flat little-endian (de)serialization for the authored ViewPlan.  Every read is bounds-checked;
// any truncation/overrun/bad-version/trailing byte yields std::nullopt rather than reading out
// of bounds.  Counts are untrusted wire data: nothing is reserved from them.
#include <rune/codec/ViewPlanAuthored.h>

#include <cstring>

namespace rune::view {
namespace {

// ---------------------------------------------------------------------------
// Little-endian write helpers.
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
// Little-endian read helpers - each returns false on truncation; `pos` only advances on success.
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
    if (len > buf.size() - pos) return false;  // overrun guard, no OOB read
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

bool readPrimType(std::span<const uint8_t> buf, size_t& pos, rune::codec::PrimType& out) {
    uint8_t v = 0;
    if (!readU8(buf, pos, v)) return false;
    if (v > static_cast<uint8_t>(rune::codec::PrimType::FixedStr)) return false;
    out = static_cast<rune::codec::PrimType>(v);
    return true;
}

bool readMergeRule(std::span<const uint8_t> buf, size_t& pos, MergeRule& out) {
    uint8_t v = 0;
    if (!readU8(buf, pos, v)) return false;
    if (v > static_cast<uint8_t>(MergeRule::LastNonNull)) return false;
    out = static_cast<MergeRule>(v);
    return true;
}

bool readPredicateOp(std::span<const uint8_t> buf, size_t& pos, PredicateOp& out) {
    uint8_t v = 0;
    if (!readU8(buf, pos, v)) return false;
    if (v < static_cast<uint8_t>(PredicateOp::Column) || v > static_cast<uint8_t>(PredicateOp::Not)) return false;
    out = static_cast<PredicateOp>(v);
    return true;
}

// ---------------------------------------------------------------------------
// v2 sections.
// ---------------------------------------------------------------------------
void writeComponents(std::vector<uint8_t>& buf, const std::vector<KeyComponent>& comps) {
    writeU32LE(buf, static_cast<uint32_t>(comps.size()));
    for (const KeyComponent& c : comps) {
        writeString(buf, c.column);
        writeU8(buf, c.keyFunction);
        writeString(buf, c.outName);
    }
}

bool readComponents(std::span<const uint8_t> buf, size_t& pos, std::vector<KeyComponent>& out) {
    uint32_t n = 0;
    if (!readU32LE(buf, pos, n)) return false;
    for (uint32_t i = 0; i < n; ++i) {
        KeyComponent c;
        if (!readString(buf, pos, c.column))  return false;
        if (!readU8(buf, pos, c.keyFunction)) return false;
        if (!readString(buf, pos, c.outName)) return false;
        out.push_back(std::move(c));
    }
    return true;
}

void writeConstraints(std::vector<uint8_t>& buf, const std::vector<AuthoredConstraint>& constraints) {
    writeU32LE(buf, static_cast<uint32_t>(constraints.size()));
    for (const AuthoredConstraint& c : constraints) {
        writeString(buf, c.name);
        writeU32LE(buf, static_cast<uint32_t>(c.predicate.size()));
        for (const PredicateNode& n : c.predicate) {
            writeU8(buf, static_cast<uint8_t>(n.op));
            writeU16LE(buf, n.a);
            writeU16LE(buf, n.b);
            writeString(buf, n.column);
            writeU8(buf, static_cast<uint8_t>(n.constType));
            buf.insert(buf.end(), n.constBytes.begin(), n.constBytes.end());
        }
    }
}

bool readConstraints(std::span<const uint8_t> buf, size_t& pos, std::vector<AuthoredConstraint>& out) {
    uint32_t count = 0;
    if (!readU32LE(buf, pos, count)) return false;
    for (uint32_t i = 0; i < count; ++i) {
        AuthoredConstraint c;
        if (!readString(buf, pos, c.name)) return false;
        uint32_t nodes = 0;
        if (!readU32LE(buf, pos, nodes)) return false;
        for (uint32_t j = 0; j < nodes; ++j) {
            PredicateNode n{};
            if (!readPredicateOp(buf, pos, n.op))      return false;
            if (!readU16LE(buf, pos, n.a))             return false;
            if (!readU16LE(buf, pos, n.b))             return false;
            if (!readString(buf, pos, n.column))       return false;
            if (!readPrimType(buf, pos, n.constType))  return false;
            if (n.constBytes.size() > buf.size() - pos) return false;
            std::memcpy(n.constBytes.data(), buf.data() + pos, n.constBytes.size());
            pos += n.constBytes.size();
            c.predicate.push_back(std::move(n));
        }
        out.push_back(std::move(c));
    }
    return true;
}

bool isPlainSingleKey(const ViewPlan& p) {
    if (p.keyComponents.size() != 1) return false;
    const KeyComponent& c = p.keyComponents[0];
    return c.keyFunction == 0 && c.outName == c.column && (p.key.empty() || p.key == c.column);
}

} // anonymous namespace

bool needsV2(const ViewPlan& p) noexcept {
    if (p.unique || !p.constraints.empty() || p.unicodeVersion != 0 || !p.groupBy.empty()) return true;
    return !p.keyComponents.empty() && !isPlainSingleKey(p);
}

std::string keyDisplayName(const ViewPlan& p) {
    if (p.keyComponents.empty()) return p.key;
    std::string out;
    for (const KeyComponent& c : p.keyComponents) {
        if (!out.empty()) out += ',';
        out += c.outName;
    }
    return out;
}

// ---------------------------------------------------------------------------
// ViewPlan (authored)
//
// v1: version:u8, viewName:str, key:str,
//     sources: u32 count + { topicId:i32, keyField:str } each,
//     output: u32 count + { name:str, type:u8, sourceIdx:u16, srcField:str, rule:u8 } each,
//     persistence:u8, sizing:{chunkBytes:u32, capacity:u32, maxRows:u64}
// v2: the v1 fields with version 2, then
//     keyComponents: u32 count + { column:str, keyFunction:u8, outName:str } each, unique:u8,
//     constraints: u32 count + { name:str, nodes: u32 count + { op:u8, a:u16, b:u16, column:str,
//                                                               constType:u8, constBytes:16 } each } each,
//     unicodeVersion:u32, groupBy: u32 count + { column:str, keyFunction:u8, outName:str } each
// ---------------------------------------------------------------------------
std::vector<uint8_t> serializeAuthored(const ViewPlan& plan) {
    const bool v2 = needsV2(plan);
    // A single plain component rides in a v1 blob as `key = column`.
    const std::string& key = (!v2 && plan.key.empty() && !plan.keyComponents.empty()) ? plan.keyComponents[0].column : plan.key;

    std::vector<uint8_t> buf;
    buf.reserve(64 + plan.viewName.size() + key.size());

    writeU8(buf, v2 ? kAuthoredVersion : kAuthoredVersionV1);
    writeString(buf, plan.viewName);
    writeString(buf, key);

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

    if (v2) {
        writeComponents(buf, plan.keyComponents);
        writeU8(buf, plan.unique ? 1 : 0);
        writeConstraints(buf, plan.constraints);
        writeU32LE(buf, plan.unicodeVersion);
        writeComponents(buf, plan.groupBy);
    }
    return buf;
}

std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t> buf) {
    size_t pos = 0;
    ViewPlan p;

    uint8_t version = 0;
    if (!readU8(buf, pos, version))        return std::nullopt;
    if (version != kAuthoredVersionV1 && version != kAuthoredVersion) return std::nullopt;

    if (!readString(buf, pos, p.viewName)) return std::nullopt;
    if (!readString(buf, pos, p.key))      return std::nullopt;

    // These counts are untrusted wire data (the client-supplied CreateView planBlob), so no
    // .reserve(count) here: a hostile count must fail on the first missing element.
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

    if (version == kAuthoredVersion) {
        uint8_t unique = 0;
        if (!readComponents(buf, pos, p.keyComponents)) return std::nullopt;
        if (!readU8(buf, pos, unique) || unique > 1)    return std::nullopt;
        p.unique = unique == 1;
        if (!readConstraints(buf, pos, p.constraints))  return std::nullopt;
        if (!readU32LE(buf, pos, p.unicodeVersion))     return std::nullopt;
        if (!readComponents(buf, pos, p.groupBy))       return std::nullopt;
    }

    if (pos != buf.size()) return std::nullopt;
    return p;
}

} // namespace rune::view
