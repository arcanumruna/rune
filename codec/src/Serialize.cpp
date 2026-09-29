// src/Serialize.cpp
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
#include <format>
#include <sstream>
#include <string_view>

namespace rune::codec {

// ─── toText helpers ───────────────────────────────────────────────────────────

static std::string_view primTypeName(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:   return "int32";
        case PrimType::Int64:   return "int64";
        case PrimType::UInt16:  return "uint16";
        case PrimType::UInt32:  return "uint32";
        case PrimType::UInt64:  return "uint64";
        case PrimType::Float64: return "float64";
        case PrimType::Bool:    return "bool";
        case PrimType::Enum:    return "enum";
        case PrimType::String:  return "string";
        case PrimType::Bytes:   return "bytes";
    }
    return "unknown";
}

// Pad s with trailing spaces to reach at least width characters.
static std::string padRight(std::string_view s, size_t width) {
    std::string out(s);
    if (out.size() < width)
        out.append(width - out.size(), ' ');
    return out;
}



// ─── Magic ────────────────────────────────────────────────────────────────────
// "RCD1" as a little-endian u32: 'R'=0x52, 'C'=0x43, 'D'=0x44, '1'=0x31
static constexpr uint32_t kMagic = 0x31444352u;

// ─── Serialize helpers ────────────────────────────────────────────────────────

static void push_u8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

static void push_u16le(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xffu));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xffu));
}

static void push_u32le(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xffu));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xffu));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xffu));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xffu));
}

static void push_str(std::vector<uint8_t>& buf, const std::string& s) {
    if (s.size() > 0xffffu) {
        throw std::runtime_error("serialize: string too long");
    }
    push_u16le(buf, static_cast<uint16_t>(s.size()));
    for (unsigned char c : s) {
        buf.push_back(c);
    }
}

// ─── Deserialize helpers ──────────────────────────────────────────────────────

static uint8_t read_u8(std::span<const uint8_t> blob, size_t& pos) {
    if (pos >= blob.size()) {
        throw std::runtime_error("deserialize: unexpected end of blob (u8)");
    }
    return blob[pos++];
}

static uint16_t read_u16le(std::span<const uint8_t> blob, size_t& pos) {
    if (pos + 2 > blob.size()) {
        throw std::runtime_error("deserialize: unexpected end of blob (u16)");
    }
    uint16_t v = static_cast<uint16_t>(blob[pos])
               | static_cast<uint16_t>(static_cast<uint16_t>(blob[pos + 1]) << 8);
    pos += 2;
    return v;
}

static uint32_t read_u32le(std::span<const uint8_t> blob, size_t& pos) {
    if (pos + 4 > blob.size()) {
        throw std::runtime_error("deserialize: unexpected end of blob (u32)");
    }
    uint32_t v = static_cast<uint32_t>(blob[pos])
               | (static_cast<uint32_t>(blob[pos + 1]) << 8)
               | (static_cast<uint32_t>(blob[pos + 2]) << 16)
               | (static_cast<uint32_t>(blob[pos + 3]) << 24);
    pos += 4;
    return v;
}

static std::string read_str(std::span<const uint8_t> blob, size_t& pos) {
    uint16_t len = read_u16le(blob, pos);
    if (pos + len > blob.size()) {
        throw std::runtime_error("deserialize: unexpected end of blob (string bytes)");
    }
    std::string s(reinterpret_cast<const char*>(blob.data() + pos), len);
    pos += len;
    return s;
}

// ─── Public API ───────────────────────────────────────────────────────────────

std::vector<uint8_t> serialize(const Descriptor& d) {
    std::vector<uint8_t> buf;
    // Reserve an approximate capacity to avoid repeated reallocations
    buf.reserve(64 + d.fields.size() * 32);

    // Magic
    push_u32le(buf, kMagic);

    // Schema name
    push_str(buf, d.name);

    // Version
    push_u16le(buf, d.version);

    // Field count
    if (d.fields.size() > 0xffffu) {
        throw std::runtime_error("serialize: too many fields");
    }
    push_u16le(buf, static_cast<uint16_t>(d.fields.size()));

    // Fields
    for (const auto& f : d.fields) {
        push_str(buf, f.name);
        push_u16le(buf, f.id);
        push_u8(buf, static_cast<uint8_t>(f.type));
        push_u16le(buf, f.sinceVersion);
        push_u8(buf, f.isKey ? uint8_t{1} : uint8_t{0});

        // Enum values
        if (f.enumValues.size() > 0xffffu) {
            throw std::runtime_error("serialize: too many enum values");
        }
        push_u16le(buf, static_cast<uint16_t>(f.enumValues.size()));
        for (int32_t ev : f.enumValues) {
            // Serialize int32 as its bit-pattern in u32 LE
            push_u32le(buf, static_cast<uint32_t>(ev));
        }
    }

    return buf;
}

Descriptor deserialize(std::span<const uint8_t> blob) {
    size_t pos = 0;

    // Magic check
    uint32_t magic = read_u32le(blob, pos);
    if (magic != kMagic) {
        throw std::runtime_error("deserialize: bad magic number");
    }

    // Schema name
    std::string schemaName = read_str(blob, pos);

    // Version
    uint16_t version = read_u16le(blob, pos);

    // Field count
    uint16_t fieldCount = read_u16le(blob, pos);

    // Build the descriptor via DescriptorBuilder (recomputes offsets/blockLength)
    DescriptorBuilder builder(std::move(schemaName), version);

    for (uint16_t i = 0; i < fieldCount; ++i) {
        std::string fieldName = read_str(blob, pos);

        uint16_t id           = read_u16le(blob, pos);
        uint8_t  typeByte     = read_u8(blob, pos);
        uint16_t sinceVersion = read_u16le(blob, pos);
        uint8_t  isKeyByte    = read_u8(blob, pos);
        uint16_t enumCount    = read_u16le(blob, pos);

        // Validate type byte
        constexpr uint8_t kMaxType = static_cast<uint8_t>(PrimType::Bytes);
        if (typeByte > kMaxType) {
            throw std::runtime_error("deserialize: unknown PrimType value");
        }
        PrimType type = static_cast<PrimType>(typeByte);

        std::vector<int32_t> enumValues;
        enumValues.reserve(enumCount);
        for (uint16_t j = 0; j < enumCount; ++j) {
            uint32_t raw = read_u32le(blob, pos);
            enumValues.push_back(static_cast<int32_t>(raw));
        }

        // id is not passed to builder — builder assigns it sequentially (0,1,2,…)
        // which matches the serialized id since fields are stored in declaration order.
        // Validate that the stored id matches the expected sequential position.
        if (id != i) {
            throw std::runtime_error("deserialize: field id out of sequence");
        }

        builder.add(std::move(fieldName), type, sinceVersion,
                    isKeyByte != 0, std::move(enumValues));
    }

    return builder.build();
}

std::string toText(const Descriptor& d) {
    std::ostringstream out;

    // Header line: schema <name> fp=0x<hex16> block=<N> version=<V> {
    uint64_t fp = fingerprint(d);
    out << "schema " << d.name
        << " fp=0x" << std::hex << std::uppercase << fp << std::dec
        << " block=" << d.blockLength
        << " version=" << d.version
        << " {\n";

    // Field lines
    constexpr size_t kNameWidth = 10;
    constexpr size_t kTypeWidth = 8;

    for (const auto& f : d.fields) {
        out << "  ";
        out << padRight(f.name, kNameWidth);
        out << padRight(std::string(primTypeName(f.type)), kTypeWidth);

        if (isVarLen(f.type)) {
            out << "vardata#" << f.offset;
        } else {
            out << "@" << f.offset;
        }

        if (f.isKey) {
            out << "    key";
        }
        if (f.sinceVersion > 1) {
            out << "    sinceVersion=" << f.sinceVersion;
        }
        if (!f.enumValues.empty()) {
            out << "    enum(";
            for (size_t i = 0; i < f.enumValues.size(); ++i) {
                if (i > 0) out << ",";
                out << f.enumValues[i];
            }
            out << ")";
        }

        out << "\n";
    }

    out << "}\n";
    return out.str();
}

} // namespace rune::codec
