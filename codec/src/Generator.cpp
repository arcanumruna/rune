// src/Generator.cpp
#include <rune/codec/Generator.h>
#include <rune/codec/Serialize.h>
#include <sstream>
#include <cctype>
#include <algorithm>
#include <stdexcept>

namespace rune::codec {

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Convert a schema name like "my_schema" or "mySchema" to PascalCase.
static std::string toPascalCase(std::string_view name) {
    std::string out;
    out.reserve(name.size());
    bool cap = true;
    for (char c : name) {
        if (c == '_' || c == '-') {
            cap = true;
        } else {
            if (cap) {
                out.push_back(static_cast<char>(std::toupper(static_cast<unsigned char>(c))));
                cap = false;
            } else {
                out.push_back(c);
            }
        }
    }
    return out;
}

/// Map a PrimType to its C++ type string for use as a struct member.
static std::string_view cppType(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:   return "int32_t";
        case PrimType::Int64:   return "int64_t";
        case PrimType::UInt16:  return "uint16_t";
        case PrimType::UInt32:  return "uint32_t";
        case PrimType::UInt64:  return "uint64_t";
        case PrimType::Float64: return "double";
        case PrimType::Bool:    return "bool";
        case PrimType::Enum:    return "int32_t";
        case PrimType::String:  return "std::string_view";
        case PrimType::Bytes:   return "std::string_view";
        // string(N) has no width-free spelling: every emitter spells it
        // std::array<char, fixedLen> itself and never asks cppType for it.
        case PrimType::FixedStr: return "/* string(N): emitted as std::array<char, N> */";
    }
    return "/* unknown */";
}

// ─── Phase 2b Part B: zero-copy flyweights (all-fixed schemas) ───────────────

/// True when every field lives in the fixed block (no String / Bytes): only
/// then is a record's encoded length known before it is written.
static bool allFixed(const Descriptor& d) {
    return std::none_of(d.fields.begin(), d.fields.end(),
                        [](const FieldDescriptor& f) { return isVarLen(f.type); });
}

/// The schema's single key field; nullptr when it has none or several
/// (composite keys are Phase 3).
static const FieldDescriptor* singleKeyField(const Descriptor& d) {
    const FieldDescriptor* k = nullptr;
    for (const auto& f : d.fields) {
        if (!f.isKey) continue;
        if (k != nullptr) return nullptr;
        k = &f;
    }
    return k;
}

/// Flyweights are emitted for all-fixed schemas.  (A non-key field named `key`
/// next to a key field is already rejected above, so no collision can reach here.)
static bool wantsFlyweights(const Descriptor& d) { return allFixed(d); }

/// The fly::load / fly::store template argument of a fixed scalar field.
static std::string_view flyType(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:
        case PrimType::Enum:    return "int32_t";
        case PrimType::Int64:   return "int64_t";
        case PrimType::UInt16:  return "uint16_t";
        case PrimType::UInt32:  return "uint32_t";
        case PrimType::UInt64:  return "uint64_t";
        case PrimType::Float64: return "double";
        case PrimType::Bool:    return "bool";
        default:                return "/* not a fixed scalar */";
    }
}

/// The generated struct's member type for a field (string(N): std::array<char, N>).
static std::string memberType(const FieldDescriptor& f) {
    if (f.type == PrimType::FixedStr) return "std::array<char, " + std::to_string(f.fixedLen) + ">";
    return std::string(cppType(f.type));
}

static void emitEncoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls = structName + "Encoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — zero-copy writer over a claimed buffer (Producer::tryClaim,\n"
      << "// TypedTopic::claim).  Same packed little-endian bytes as " << structName << "::encode();\n"
      << "// every field is stored with memcpy (a claimed buffer is not aligned).\n";
    o << "class " << cls << " {\n";
    o << "public:\n";
    o << "    static constexpr uint32_t kBlockLength   = " << d.blockLength << "u;\n";
    o << "    static constexpr uint32_t kEncodedLength = 4u + kBlockLength;\n";
    o << "    static constexpr uint16_t kVersion       = " << d.version << "u;\n";
    o << "    static constexpr bool     kHasKey        = " << (k != nullptr ? "true" : "false") << ";\n";
    if (k != nullptr) o << "    using KeyType = " << memberType(*k) << ";\n";
    o << "\n";
    o << "    /// Binds to `buf` (>= kEncodedLength bytes, else std::length_error), writes\n";
    o << "    /// the record header [blockLength][version] and zeroes the block, so an\n";
    o << "    /// unset field reads as 0 / empty (never stale ring bytes).\n";
    o << "    explicit " << cls << "(std::span<uint8_t> buf) : p_(buf.data()) {\n";
    o << "        if (buf.size() < kEncodedLength) {\n";
    o << "            throw std::length_error(\"" << cls << ": buffer is smaller than kEncodedLength\");\n";
    o << "        }\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p_, static_cast<uint16_t>(kBlockLength));\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p_ + 2, kVersion);\n";
    o << "        std::memset(p_ + 4, 0, kBlockLength);\n";
    o << "    }\n\n";
    for (const auto& f : d.fields) {
        if (f.type == PrimType::FixedStr) {
            o << "    " << cls << "& " << f.name << "(std::string_view v) noexcept {\n"
              << "        ::rune::codec::fly::storeFixedStr(p_ + 4u + " << f.offset << "u, v, "
              << f.fixedLen << "u);\n"
              << "        return *this;\n"
              << "    }\n";
        } else {
            o << "    " << cls << "& " << f.name << "(" << cppType(f.type) << " v) noexcept {\n"
              << "        ::rune::codec::fly::store<" << flyType(f.type) << ">(p_ + 4u + " << f.offset
              << "u, v);\n"
              << "        return *this;\n"
              << "    }\n";
        }
    }
    if (k != nullptr) {
        if (k->name != "key") {  // a key field named `key` already has this setter
            if (k->type == PrimType::FixedStr) {
                o << "    " << cls << "& key(const KeyType& v) noexcept {\n"
                  << "        std::memcpy(p_ + 4u + " << k->offset << "u, v.data(), " << k->fixedLen << "u);\n"
                  << "        return *this;\n"
                  << "    }\n";
            } else {
                o << "    " << cls << "& key(KeyType v) noexcept { return " << k->name << "(v); }\n";
            }
        }
        if (k->type == PrimType::FixedStr) {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        KeyType v{};\n"
              << "        std::memcpy(v.data(), p_ + 4u + " << k->offset << "u, " << k->fixedLen << "u);\n"
              << "        return v;\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        return ::rune::codec::fly::load<" << flyType(k->type) << ">(p_ + 4u + " << k->offset
              << "u);\n"
              << "    }\n";
        }
    }
    o << "\nprivate:\n";
    o << "    uint8_t* p_;\n";
    o << "};\n\n";
}

// The writer TypedTopic::claim hands out: the Encoder with the KEY FIELD hidden
// (private base; the key is written once, by the constructor, and is exactly
// what the record was routed with — it cannot be rewritten afterwards).
static void emitClaimEncoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls  = structName + "ClaimEncoder";
    const std::string base = structName + "Encoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — " << base << " without the key field's accessors: the key is fixed\n"
      << "// at construction (the routing key of the claim).\n";
    o << "class " << cls << " : private " << base << " {\n";
    o << "public:\n";
    o << "    using " << base << "::kBlockLength;\n";
    o << "    using " << base << "::kEncodedLength;\n";
    o << "    using " << base << "::kVersion;\n";
    o << "    using " << base << "::kHasKey;\n";
    if (k != nullptr) {
        o << "    using " << base << "::KeyType;\n\n";
        o << "    " << cls << "(std::span<uint8_t> buf, const KeyType& keyValue) : " << base << "(buf) {\n"
          << "        " << base << "::key(keyValue);\n"
          << "    }\n";
    } else {
        o << "\n    explicit " << cls << "(std::span<uint8_t> buf) : " << base << "(buf) {}\n";
    }
    for (const auto& f : d.fields) {
        if (k != nullptr && f.name == k->name) continue;
        o << "    using " << base << "::" << f.name << ";\n";
    }
    o << "};\n\n";
}

static void emitDecoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls = structName + "Decoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — zero-copy reader over one record of any version: a field\n"
      << "// outside the record's own block (an older, shorter version) or past its end\n"
      << "// reads as 0 / empty; a longer block (a newer version) is fine (fixed offsets).\n";
    o << "class " << cls << " {\n";
    o << "public:\n";
    o << "    static constexpr uint32_t kBlockLength = " << d.blockLength << "u;\n";
    o << "    static constexpr bool     kHasKey      = " << (k != nullptr ? "true" : "false") << ";\n";
    if (k != nullptr) o << "    using KeyType = " << memberType(*k) << ";\n";
    o << "\n";
    o << "    explicit " << cls << "(std::span<const uint8_t> rec) noexcept\n"
      << "        : p_(rec.data()), size_(rec.size()),\n"
      << "          bl_(rec.size() >= 2 ? ::rune::codec::fly::load<uint16_t>(rec.data()) : 0u) {}\n\n";
    for (const auto& f : d.fields) {
        const uint32_t w = fieldWidth(f);
        if (f.type == PrimType::FixedStr) {
            o << "    [[nodiscard]] std::string_view " << f.name << "() const noexcept {\n"
              << "        return has_(" << f.offset << "u, " << w << "u)\n"
              << "            ? ::rune::codec::fly::loadFixedStr(p_ + 4u + " << f.offset << "u, " << w << "u)\n"
              << "            : std::string_view{};\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] " << cppType(f.type) << " " << f.name << "() const noexcept {\n"
              << "        return has_(" << f.offset << "u, " << w << "u)\n"
              << "            ? ::rune::codec::fly::load<" << flyType(f.type) << ">(p_ + 4u + " << f.offset << "u)\n"
              << "            : " << cppType(f.type) << "{};\n"
              << "    }\n";
        }
    }
    if (k != nullptr && k->name != "key") {
        if (k->type == PrimType::FixedStr) {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        KeyType v{};\n"
              << "        if (has_(" << k->offset << "u, " << k->fixedLen << "u)) {\n"
              << "            std::memcpy(v.data(), p_ + 4u + " << k->offset << "u, " << k->fixedLen << "u);\n"
              << "        }\n"
              << "        return v;\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] KeyType key() const noexcept { return " << k->name << "(); }\n";
        }
    }
    o << "\nprivate:\n";
    o << "    [[nodiscard]] bool has_(uint32_t off, uint32_t w) const noexcept {\n";
    o << "        return off + w <= bl_ && 4u + off + w <= size_;\n";
    o << "    }\n";
    o << "    const uint8_t* p_;\n";
    o << "    std::size_t    size_;\n";
    o << "    uint32_t       bl_;\n";
    o << "};\n\n";
}

// ─── generateHeader ───────────────────────────────────────────────────────────

std::string generateHeader(const Descriptor& d, std::string_view nspace) {
    // Compute derived values
    const uint64_t fp   = fingerprint(d);
    const std::string structName = toPascalCase(d.name);

    // Serialize the descriptor blob so we can embed it as a constexpr array
    const std::vector<uint8_t> blob = serialize(d);

    // Split namespace into components for nested open/close
    // e.g. "rune::gen" → ["rune", "gen"]
    std::vector<std::string> nsparts;
    {
        std::string ns(nspace);
        size_t start = 0;
        while (true) {
            size_t pos = ns.find("::", start);
            if (pos == std::string::npos) {
                nsparts.push_back(ns.substr(start));
                break;
            }
            nsparts.push_back(ns.substr(start, pos - start));
            start = pos + 2;
        }
    }

    std::ostringstream o;

    // ── File header ──────────────────────────────────────────────────────────
    o << "// Auto-generated by rune_codec_gen — DO NOT EDIT\n";
    o << "#pragma once\n";
    o << "#include <array>\n";
    o << "#include <cstddef>\n";
    o << "#include <cstdint>\n";
    o << "#include <cstring>\n";
    o << "#include <span>\n";
    o << "#include <stdexcept>\n";
    o << "#include <string_view>\n";
    o << "#include <vector>\n";
    o << "#include <rune/codec/Descriptor.h>\n";
    o << "#include <rune/codec/Fingerprint.h>\n";
    o << "#include <rune/codec/Flyweight.h>\n";
    o << "#include <rune/codec/Serialize.h>\n";
    o << "\n";

    // ── Namespace open ───────────────────────────────────────────────────────
    for (const auto& part : nsparts) {
        o << "namespace " << part << " {\n";
    }
    o << "\n";

    // ── 2b Part B: flyweight forward declarations (all-fixed schemas) ────────
    const bool fly = wantsFlyweights(d);
    if (fly) {
        o << "class " << structName << "Encoder;\n";
        o << "class " << structName << "ClaimEncoder;\n";
        o << "class " << structName << "Decoder;\n\n";
    }

    // ── Struct ───────────────────────────────────────────────────────────────
    o << "struct " << structName << " {\n";

    // ── Constants ───────────────────────────────────────────────────────────
    o << "    static constexpr uint16_t     kBlockLength = " << d.blockLength << "u;\n";
    o << "    static constexpr uint16_t     kVersion     = " << d.version << "u;\n";
    o << "    static constexpr uint64_t     kFingerprint = " << fp << "ull;\n";
    o << "    static constexpr std::string_view kSchemaName = \"" << d.name << "\";\n";
    if (fly) {
        o << "    using Encoder = " << structName << "Encoder;  // zero-copy writer (all-fixed schema)\n";
        o << "    using Decoder = " << structName << "Decoder;  // zero-copy reader\n";
        o << "    using ClaimEncoder = " << structName
          << "ClaimEncoder;  // Encoder minus the key field (TypedTopic::claim)\n";
    }
    o << "\n";

    // ── Members: fixed fields ────────────────────────────────────────────────
    for (const auto& f : d.fields) {
        if (!isVarLen(f.type)) {
            if (f.type == PrimType::FixedStr)
                o << "    std::array<char, " << f.fixedLen << "> " << f.name << " = {};\n";
            else
                o << "    " << cppType(f.type) << " " << f.name << " = {};\n";
        }
    }
    // ── Members: var-len fields ──────────────────────────────────────────────
    for (const auto& f : d.fields) {
        if (isVarLen(f.type)) {
            o << "    " << cppType(f.type) << " " << f.name << " = {};\n";
        }
    }
    o << "\n";

    // ── key() — Phase 2b Part C ─────────────────────────────────────────────
    // The isKey field's value in the exact form rune::client::Key consumes, so
    // TypedTopic::send(rec) routes like Producer::send(h, Key(rec.key()), ...):
    //   scalar      -> by value  (Key(int64_t)/Key(uint64_t) = 8 LE bytes;
    //                             other scalars = their raw bytes)
    //   string(N)   -> const std::array<char, N>&  (its N raw bytes)
    //   string/bytes-> std::span<const uint8_t> over the CONTENT — never a
    //                  std::string_view: that is trivially copyable, so Key
    //                  would hash the view object (pointer + length) instead.
    // No isKey field -> no key(): TypedTopic sends partition 0 + empty key.
    {
        const FieldDescriptor* kf = nullptr;
        for (const auto& f : d.fields) {
            if (!f.isKey) continue;
            if (kf != nullptr) {
                // Two key fields: which one routes the record is ambiguous —
                // reject instead of silently routing by the first.
                throw std::invalid_argument(
                    "generateHeader: schema '" + d.name + "' declares more than one key field ('" +
                    kf->name + "' and '" + f.name + "') — a typed topic routes by exactly one key");
            }
            kf = &f;
        }
        if (kf != nullptr) {
            if (d.find("key") != nullptr) {
                throw std::invalid_argument(
                    "generateHeader: schema '" + d.name + "' has a field named 'key', which "
                    "collides with the generated key() accessor of its key field '" + kf->name +
                    "' — rename the field");
            }
            if (isVarLen(kf->type)) {
                o << "    [[nodiscard]] std::span<const uint8_t> key() const noexcept {\n";
                o << "        return {reinterpret_cast<const uint8_t*>(" << kf->name << ".data()), "
                  << kf->name << ".size()};\n";
                o << "    }\n\n";
            } else if (kf->type == PrimType::FixedStr) {
                o << "    [[nodiscard]] const std::array<char, " << kf->fixedLen
                  << ">& key() const noexcept { return " << kf->name << "; }\n\n";
            } else {
                o << "    [[nodiscard]] " << cppType(kf->type)
                  << " key() const noexcept { return " << kf->name << "; }\n\n";
            }
        }
    }

    // ── encodedSize() ────────────────────────────────────────────────────────
    // total = 4 (header) + blockLength + sum(4 + len) for each var field
    o << "    [[nodiscard]] size_t encodedSize() const noexcept {\n";
    o << "        size_t n = 4 + kBlockLength;\n";
    for (const auto& f : d.fields) {
        if (isVarLen(f.type)) {
            o << "        n += 4 + " << f.name << ".size();\n";
        }
    }
    o << "        return n;\n";
    o << "    }\n\n";

    // ── encode() ─────────────────────────────────────────────────────────────
    // Layout: [blockLength:u16 LE][version:u16 LE] [fixed block] [var fields: u32len+bytes each]
    o << "    size_t encode(uint8_t* buf) const noexcept {\n";
    o << "        uint8_t* p = buf;\n";
    o << "        // Header: blockLength (u16 LE) + version (u16 LE)\n";
    o << "        auto wr16 = [&](uint16_t v) { *p++ = uint8_t(v & 0xff); *p++ = uint8_t(v >> 8); };\n";
    o << "        auto wr32 = [&](uint32_t v) { *p++ = uint8_t(v); *p++ = uint8_t(v>>8); *p++ = uint8_t(v>>16); *p++ = uint8_t(v>>24); };\n";
    o << "        auto wr64 = [&](uint64_t v) { for(int i=0;i<8;i++) *p++=uint8_t(v>>(8*i)); };\n";
    o << "        wr16(kBlockLength);\n";
    o << "        wr16(kVersion);\n";
    o << "        // Fixed block\n";

    // Emit fixed fields in offset order (they are already in declaration order by the builder)
    {
        // Collect fixed fields sorted by offset
        std::vector<const FieldDescriptor*> fixedFields;
        for (const auto& f : d.fields) {
            if (!isVarLen(f.type)) {
                fixedFields.push_back(&f);
            }
        }
        std::sort(fixedFields.begin(), fixedFields.end(),
                  [](const FieldDescriptor* a, const FieldDescriptor* b) {
                      return a->offset < b->offset;
                  });

        for (const FieldDescriptor* f : fixedFields) {
            switch (f->type) {
                case PrimType::Int32:
                case PrimType::Enum:
                    o << "        wr32(static_cast<uint32_t>(" << f->name << "));\n";
                    break;
                case PrimType::Int64:
                    o << "        wr64(static_cast<uint64_t>(" << f->name << "));\n";
                    break;
                case PrimType::UInt16:
                    o << "        wr16(" << f->name << ");\n";
                    break;
                case PrimType::UInt32:
                    o << "        wr32(" << f->name << ");\n";
                    break;
                case PrimType::UInt64:
                    o << "        wr64(" << f->name << ");\n";
                    break;
                case PrimType::Float64:
                    o << "        { uint64_t _tmp; std::memcpy(&_tmp, &" << f->name << ", 8); wr64(_tmp); }\n";
                    break;
                case PrimType::Bool:
                    o << "        *p++ = " << f->name << " ? uint8_t(1) : uint8_t(0);\n";
                    break;
                case PrimType::FixedStr:
                    o << "        std::memcpy(p, " << f->name << ".data(), " << f->fixedLen << "u); p += " << f->fixedLen << "u;\n";
                    break;
                default:
                    break;
            }
        }
    }

    o << "        // Var-len fields (in var-index order)\n";
    // Emit var fields in var-index order (their offset IS their var-index)
    {
        std::vector<const FieldDescriptor*> varFields;
        for (const auto& f : d.fields) {
            if (isVarLen(f.type)) {
                varFields.push_back(&f);
            }
        }
        std::sort(varFields.begin(), varFields.end(),
                  [](const FieldDescriptor* a, const FieldDescriptor* b) {
                      return a->offset < b->offset;
                  });
        for (const FieldDescriptor* f : varFields) {
            o << "        wr32(uint32_t(" << f->name << ".size()));\n";
            o << "        std::memcpy(p, " << f->name << ".data(), " << f->name << ".size()); p += " << f->name << ".size();\n";
        }
    }

    o << "        return static_cast<size_t>(p - buf);\n";
    o << "    }\n\n";

    // ── decode() ─────────────────────────────────────────────────────────────
    o << "    // PRECONDITION: `record` is assumed to be a pre-validated, well-formed\n";
    o << "    // record. decode() trusts the record's declared blockLength to locate the\n";
    o << "    // var-data section and does NOT reject a blockLength that disagrees with\n";
    o << "    // the descriptor — it is the zero-copy hot path and stays branch-lean.\n";
    o << "    // For untrusted input, call rune::codec::Validator::validate first; the\n";
    o << "    // Validator is the gate. All accesses here are still bounds-checked, so a\n";
    o << "    // hostile record can never read out of bounds.\n";
    o << "    void decode(std::span<const uint8_t> record) noexcept {\n";
    o << "        if (record.size() < 4) return;\n";
    o << "        const uint8_t* p = record.data();\n";
    o << "        auto rd16 = [&]() -> uint16_t { uint16_t v = uint16_t(p[0])|(uint16_t(p[1])<<8); p+=2; return v; };\n";
    o << "        auto rd32 = [&]() -> uint32_t { uint32_t v = uint32_t(p[0])|(uint32_t(p[1])<<8)|(uint32_t(p[2])<<16)|(uint32_t(p[3])<<24); p+=4; return v; };\n";
    o << "        auto rd64 = [&]() -> uint64_t { uint64_t v=0; for(int i=0;i<8;i++) v|=uint64_t(p[i])<<(8*i); p+=8; return v; };\n";
    o << "        uint16_t bl  = rd16();\n";
    o << "        uint16_t ver = rd16();\n";
    o << "        (void)ver;\n";
    o << "        const uint8_t* fixedStart = p;\n";

    // Fixed fields
    {
        std::vector<const FieldDescriptor*> fixedFields;
        for (const auto& f : d.fields) {
            if (!isVarLen(f.type)) {
                fixedFields.push_back(&f);
            }
        }
        std::sort(fixedFields.begin(), fixedFields.end(),
                  [](const FieldDescriptor* a, const FieldDescriptor* b) {
                      return a->offset < b->offset;
                  });
        for (const FieldDescriptor* f : fixedFields) {
            const uint32_t w = fieldWidth(*f);
            o << "        if (size_t(p - record.data()) + " << w << "u <= record.size()) {\n";
            o << "            p = fixedStart + " << f->offset << "u;\n";
            switch (f->type) {
                case PrimType::Int32:
                case PrimType::Enum:
                    o << "            " << f->name << " = static_cast<int32_t>(rd32());\n";
                    break;
                case PrimType::Int64:
                    o << "            " << f->name << " = static_cast<int64_t>(rd64());\n";
                    break;
                case PrimType::UInt16:
                    o << "            " << f->name << " = rd16();\n";
                    break;
                case PrimType::UInt32:
                    o << "            " << f->name << " = rd32();\n";
                    break;
                case PrimType::UInt64:
                    o << "            " << f->name << " = rd64();\n";
                    break;
                case PrimType::Float64:
                    o << "            { uint64_t _tmp = rd64(); std::memcpy(&" << f->name << ", &_tmp, 8); }\n";
                    break;
                case PrimType::Bool:
                    o << "            " << f->name << " = (*p++ != 0);\n";
                    break;
                case PrimType::FixedStr:
                    o << "            std::memcpy(" << f->name << ".data(), p, " << f->fixedLen << "u); p += " << f->fixedLen << "u;\n";
                    break;
                default:
                    break;
            }
            o << "        }\n";
        }
    }

    o << "        // Var-len fields\n";
    o << "        const uint8_t* varStart = record.data() + 4 + bl;\n";
    o << "        p = varStart;\n";
    {
        std::vector<const FieldDescriptor*> varFields;
        for (const auto& f : d.fields) {
            if (isVarLen(f.type)) {
                varFields.push_back(&f);
            }
        }
        std::sort(varFields.begin(), varFields.end(),
                  [](const FieldDescriptor* a, const FieldDescriptor* b) {
                      return a->offset < b->offset;
                  });
        for (const FieldDescriptor* f : varFields) {
            o << "        if (p + 4 <= record.data() + record.size()) {\n";
            o << "            uint32_t len = uint32_t(p[0])|(uint32_t(p[1])<<8)|(uint32_t(p[2])<<16)|(uint32_t(p[3])<<24); p+=4;\n";
            o << "            if (p + len <= record.data() + record.size()) {\n";
            o << "                " << f->name << " = std::string_view(reinterpret_cast<const char*>(p), len); p += len;\n";
            o << "            }\n";
            o << "        }\n";
        }
    }

    o << "    }\n\n";

    // ── descriptor() ────────────────────────────────────────────────────────
    // Embed the serialized blob as a constexpr uint8_t array
    o << "    static const ::rune::codec::Descriptor& descriptor() {\n";
    o << "        static constexpr uint8_t kBlob[] = {\n";
    o << "            ";
    for (size_t i = 0; i < blob.size(); ++i) {
        if (i > 0 && i % 16 == 0) o << "\n            ";
        o << "0x" << std::hex;
        if (blob[i] < 0x10) o << "0";
        o << static_cast<unsigned>(blob[i]) << std::dec;
        if (i + 1 < blob.size()) o << ",";
    }
    o << "\n        };\n";
    o << "        static const ::rune::codec::Descriptor kDescriptor =\n";
    o << "            ::rune::codec::deserialize(std::span<const uint8_t>(kBlob, sizeof kBlob));\n";
    o << "        return kDescriptor;\n";
    o << "    }\n";

    o << "}; // struct " << structName << "\n";
    if (fly) {
        o << "\n";
        emitEncoder(o, d, structName);
        emitClaimEncoder(o, d, structName);
        emitDecoder(o, d, structName);
    }

    // ── Namespace close ──────────────────────────────────────────────────────
    o << "\n";
    for (auto it = nsparts.rbegin(); it != nsparts.rend(); ++it) {
        o << "} // namespace " << *it << "\n";
    }

    return o.str();
}

} // namespace rune::codec
