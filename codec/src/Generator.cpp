// src/Generator.cpp
#include <rune/codec/Generator.h>
#include <rune/codec/FilterWire.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Lineage.h>
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
      << "// every field is stored with memcpy (a claimed buffer is not aligned).\n"
      << "// The setters chain BY VALUE (a copy is one pointer): a byte store may alias\n"
      << "// any object the encoder lives in (an escaping TypedClaim), so a chain through\n"
      << "// *this would reload the pointer after every field; through the returned copy\n"
      << "// it stays in a register.\n";
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
    o << "        uint8_t* const p = buf.data();\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p, static_cast<uint16_t>(kBlockLength));\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p + 2, kVersion);\n";
    o << "        std::memset(p + 4, 0, kBlockLength);\n";
    o << "    }\n\n";
    for (const auto& f : d.fields) {
        if (f.type == PrimType::FixedStr) {
            o << "    [[gnu::always_inline]] " << cls << " " << f.name << "(std::string_view v) noexcept {\n"
              << "        uint8_t* const p = p_;\n"
              << "        ::rune::codec::fly::storeFixedStr(p + 4u + " << f.offset << "u, v, "
              << f.fixedLen << "u);\n"
              << "        return " << cls << "(p, Rebind{});\n"
              << "    }\n";
        } else {
            o << "    [[gnu::always_inline]] " << cls << " " << f.name << "(" << cppType(f.type)
              << " v) noexcept {\n"
              << "        uint8_t* const p = p_;\n"
              << "        ::rune::codec::fly::store<" << flyType(f.type) << ">(p + 4u + " << f.offset
              << "u, v);\n"
              << "        return " << cls << "(p, Rebind{});\n"
              << "    }\n";
        }
    }
    if (k != nullptr) {
        if (k->name != "key") {  // a key field named `key` already has this setter
            if (k->type == PrimType::FixedStr) {
                o << "    [[gnu::always_inline]] " << cls << " key(const KeyType& v) noexcept {\n"
                  << "        uint8_t* const p = p_;\n"
                  << "        std::memcpy(p + 4u + " << k->offset << "u, v.data(), " << k->fixedLen << "u);\n"
                  << "        return " << cls << "(p, Rebind{});\n"
                  << "    }\n";
            } else {
                o << "    [[gnu::always_inline]] " << cls << " key(KeyType v) noexcept { return " << k->name
                  << "(v); }\n";
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
    o << "    struct Rebind {};  // a setter's returned copy: no header write, no zeroing\n";
    o << "    " << cls << "(uint8_t* p, Rebind) noexcept : p_(p) {}\n";
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
    // Own setters returning the ClaimEncoder by value (the Encoder's chain
    // optimisation, and the chain never yields the base: the key setter stays
    // unreachable through a returned value too).
    for (const auto& f : d.fields) {
        if (k != nullptr && f.name == k->name) continue;
        const std::string arg = f.type == PrimType::FixedStr ? std::string("std::string_view")
                                                             : std::string(cppType(f.type));
        o << "    [[gnu::always_inline]] " << cls << " " << f.name << "(" << arg << " v) noexcept {\n"
          << "        return " << cls << "(" << base << "::" << f.name << "(v));\n"
          << "    }\n";
    }
    o << "\nprivate:\n";
    o << "    explicit " << cls << "(const " << base << "& e) noexcept : " << base << "(e) {}\n";
    o << "};\n\n";
}

static void emitDecoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls = structName + "Decoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — zero-copy reader over one record of any version it knows (#418): a record of\n"
      << "// " << structName << "::kVersion reads at the constant offsets; any other version reads through its row of\n"
      << "// the lineage (" << structName << "::kLineage, or the run-time Lineage for a newer version).  A field the\n"
      << "// record's version does not carry, a version nobody knows (known() == false) or bytes past the record\n"
      << "// read as 0 / empty — never another field's bytes.\n";
    o << "class " << cls << " {\n";
    o << "public:\n";
    o << "    static constexpr uint32_t kBlockLength = " << d.blockLength << "u;\n";
    o << "    static constexpr bool     kHasKey      = " << (k != nullptr ? "true" : "false") << ";\n";
    if (k != nullptr) o << "    using KeyType = " << memberType(*k) << ";\n";
    o << "\n";
    o << "    explicit " << cls << "(std::span<const uint8_t> rec) noexcept : " << cls << "(rec, nullptr) {}\n";
    o << "    /// A record of a version after kVersion needs the topic's lineage (Client::lineageAndWait).\n";
    o << "    " << cls << "(std::span<const uint8_t> rec, const " << structName << "::Lineage& lineage) noexcept\n"
      << "        : " << cls << "(rec, &lineage) {}\n\n";
    o << "    /// False: the record's version is unknown to this reader (every field reads as 0 / empty).\n";
    o << "    [[nodiscard]] bool known() const noexcept { return ver_ == " << structName << "::kVersion || row_ != nullptr; }\n";
    o << "    [[nodiscard]] uint16_t version() const noexcept { return ver_; }\n\n";
    for (std::size_t i = 0; i < d.fields.size(); ++i) {
        const auto& f = d.fields[i];
        const uint32_t w = fieldWidth(f);
        const bool str = f.type == PrimType::FixedStr;
        const std::string ret = str ? std::string("std::string_view") : std::string(cppType(f.type));
        const auto load = [&](const std::string& off) {
            return str ? "::rune::codec::fly::loadFixedStr(p_ + 4u + " + off + ", " + std::to_string(w) + "u)"
                       : "::rune::codec::fly::load<" + std::string(flyType(f.type)) + ">(p_ + 4u + " + off + ")";
        };
        const std::string none = str ? std::string("std::string_view{}") : ret + "{}";
        o << "    [[nodiscard]] " << ret << " " << f.name << "() const noexcept {\n"
          << "        if (ver_ == " << structName << "::kVersion) [[likely]]\n"
          << "            return has_(" << f.offset << "u, " << w << "u) ? " << load(std::to_string(f.offset) + "u")
          << " : " << none << ";\n"
          << "        return at_(" << i << "u, " << w << "u) ? " << load("row_[" + std::to_string(i) + "]") << " : "
          << none << ";\n"
          << "    }\n";
    }
    if (k != nullptr && k->name != "key") {
        if (k->type == PrimType::FixedStr) {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        KeyType v{};\n"
              << "        const std::string_view s = " << k->name << "();\n"
              << "        if (s.size() == " << k->fixedLen << "u) std::memcpy(v.data(), s.data(), " << k->fixedLen << "u);\n"
              << "        return v;\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] KeyType key() const noexcept { return " << k->name << "(); }\n";
        }
    }
    o << "\nprivate:\n";
    o << "    " << cls << "(std::span<const uint8_t> rec, const " << structName << "::Lineage* lineage) noexcept\n"
      << "        : p_(rec.data()), size_(rec.size()),\n"
      << "          bl_(rec.size() >= 4 ? ::rune::codec::fly::load<uint16_t>(rec.data()) : 0u),\n"
      << "          ver_(rec.size() >= 4 ? ::rune::codec::fly::load<uint16_t>(rec.data() + 2) : uint16_t{0}),\n"
      << "          row_(rec.size() >= 4 && ver_ != " << structName << "::kVersion ? " << structName
      << "::layoutFor(ver_, lineage) : nullptr) {}   // kVersion reads at the constant offsets\n";
    o << "    [[nodiscard]] bool has_(uint32_t off, uint32_t w) const noexcept {\n";
    o << "        return off + w <= bl_ && 4u + off + w <= size_;\n";
    o << "    }\n";
    o << "    [[nodiscard]] bool at_(uint32_t i, uint32_t w) const noexcept {\n";
    o << "        return row_ != nullptr && row_[i] != ::rune::codec::kAbsentField && has_(row_[i], w);\n";
    o << "    }\n";
    o << "    const uint8_t*  p_;\n";
    o << "    std::size_t     size_;\n";
    o << "    uint32_t        bl_;\n";
    o << "    uint16_t        ver_;\n";
    o << "    const uint32_t* row_;   // the row of any other version (nullptr: unknown)\n";
    o << "};\n\n";
}

// ─── Phase 2c: the nested filter-by-example struct (spec §4) ──────────────────

static std::string filterTypeName(const FieldDescriptor& f) {
    switch (f.type) {
        case PrimType::Int32:    return "int32";
        case PrimType::Int64:    return "int64";
        case PrimType::UInt16:   return "uint16";
        case PrimType::UInt32:   return "uint32";
        case PrimType::UInt64:   return "uint64";
        case PrimType::Enum:     return "enum";
        case PrimType::Bool:     return "bool";
        case PrimType::Float64:  return "float64";
        case PrimType::String:   return "string";
        case PrimType::Bytes:    return "bytes";
        case PrimType::FixedStr: return "string(" + std::to_string(f.fixedLen) + ")";
    }
    return "?";
}

static std::string_view primTypeEnumName(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:    return "Int32";
        case PrimType::Int64:    return "Int64";
        case PrimType::UInt16:   return "UInt16";
        case PrimType::UInt32:   return "UInt32";
        case PrimType::UInt64:   return "UInt64";
        case PrimType::Enum:     return "Enum";
        case PrimType::Bool:     return "Bool";
        case PrimType::FixedStr: return "FixedStr";
        default:                 return "";
    }
}

static void emitFilter(std::ostringstream& o, const Descriptor& d) {
    std::vector<const FieldDescriptor*> live;      // eligible live fields, declaration order
    std::vector<const FieldDescriptor*> skipped;   // live but not filterable
    for (const auto& f : d.fields) {
        if (f.untilVersion != 0xFFFF) continue;
        (isFilterable(f) ? live : skipped).push_back(&f);
    }
    o << "    /// Phase 2c filter-by-example (spec §4).  Members follow the schema's field order (designated initializers\n";
    o << "    /// must too): ";
    for (std::size_t i = 0; i < live.size(); ++i)
        o << (i ? ", " : "") << live[i]->name << " (tag " << live[i]->id << ", " << filterTypeName(*live[i]) << ")";
    if (live.empty()) o << "(no filterable field)";
    o << ".";
    if (!skipped.empty()) {
        o << "  Not filterable: ";
        for (std::size_t i = 0; i < skipped.size(); ++i)
            o << (i ? ", " : "") << skipped[i]->name << " (" << filterTypeName(*skipped[i]) << ")";
        o << ".";
    }
    o << "\n";
    o << "    struct Filter {\n";
    for (const auto* f : live) {
        if (f->type == PrimType::FixedStr)
            o << "        ::rune::Match<std::array<char, " << f->fixedLen << ">> " << f->name << "{};\n";
        else
            o << "        ::rune::Match<" << cppType(f->type) << "> " << f->name << "{};\n";
    }
    o << "        [[nodiscard]] constexpr std::expected<std::size_t, ::rune::codec::FilterError> encodeTo(std::span<uint8_t> out) const noexcept {\n";
    o << "            ::rune::codec::FilterWriter w(out);\n";
    std::vector<const FieldDescriptor*> byTag = live;
    std::sort(byTag.begin(), byTag.end(), [](const FieldDescriptor* a, const FieldDescriptor* b) { return a->id < b->id; });
    for (const auto* f : byTag) {
        o << "            w.term(" << f->id << ", ::rune::codec::PrimType::" << primTypeEnumName(f->type) << ", "
          << static_cast<unsigned>(filterValueWidth(f->type, f->fixedLen)) << ", " << "this->" << f->name << ");\n";
    }
    o << "            return w.finish();\n";
    o << "        }\n";
    o << "        /// Throws std::invalid_argument(describe(error)) — explicit, before anything is sent.\n";
    o << "        [[nodiscard]] ::rune::codec::FilterBytes encode() const {\n";
    o << "            ::rune::codec::FilterBytes b;\n";
    o << "            auto r = encodeTo(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));\n";
    o << "            if (!r) throw std::invalid_argument(::rune::codec::describe(r.error()));\n";
    o << "            b.len = static_cast<uint16_t>(*r);\n";
    o << "            return b;\n";
    o << "        }\n";
    o << "    };\n\n";
}


// ─── #418: the lineage table and the version-dispatching decode() ────────────

/// kFieldTags / kLineage (rows for versions 1..kVersion) / lineage() / layoutFor().
static void emitLineage(std::ostringstream& o, const Descriptor& evolving, const Descriptor& d,
                        const std::string& structName) {
    std::vector<uint16_t> tags;
    tags.reserve(d.fields.size());
    for (const auto& f : d.fields) tags.push_back(f.id);
    const std::size_t n = tags.size();
    o << "    static constexpr std::size_t  kFieldCount = " << n << "u;\n";
    o << "    /// The fields' durable tags, in member order.\n";
    o << "    static constexpr uint16_t     kFieldTags[kFieldCount] = {";
    for (std::size_t i = 0; i < n; ++i) o << (i ? ", " : "") << tags[i] << "u";
    o << "};\n";
    o << "    /// The fields' types (rune::codec::PrimType), in member order.\n";
    o << "    static constexpr uint8_t      kFieldTypes[kFieldCount] = {";
    for (std::size_t i = 0; i < n; ++i) o << (i ? ", " : "") << static_cast<unsigned>(d.fields[i].type) << "u";
    o << "};\n";
    o << "    /// #418: row v-1 = where each field sits in a record of version v (fixed: byte offset in the block;\n"
      << "    /// string / bytes: var index; ::rune::codec::kAbsentField: not part of version v).\n";
    o << "    static constexpr uint32_t     kLineage[kVersion][kFieldCount] = {\n";
    std::vector<uint32_t> row(n);
    for (uint32_t v = 1; v <= d.version; ++v) {
        lineageRow(evolving, static_cast<uint16_t>(v), tags, row.data());
        o << "        {";
        for (std::size_t i = 0; i < n; ++i) {
            o << (i ? ", " : "");
            if (row[i] == kAbsentField) o << "::rune::codec::kAbsentField";
            else o << row[i] << "u";
        }
        o << "},  // v" << v << "\n";
    }
    o << "    };\n";
    o << "    using Lineage = ::rune::codec::FieldLineage;\n";
    o << "    /// COLD: the run-time lineage that also places records of versions after kVersion, from the topic's\n"
      << "    /// evolving descriptor (Client::lineageAndWait<" << structName << ">).  Throws std::invalid_argument when\n"
      << "    /// `evolving` is not this type's schema (FieldLineage::forReader).\n";
    o << "    [[nodiscard]] static Lineage lineage(const ::rune::codec::Descriptor& evolving) {\n"
      << "        return Lineage::forReader(evolving, kSchemaName, kVersion, std::span<const uint16_t>(kFieldTags, kFieldCount),\n"
      << "                                  std::span<const uint8_t>(kFieldTypes, kFieldCount), kLineage[kVersion - 1u]);\n"
      << "    }\n";
    o << "    /// The row for a record of version `ver` (nullptr: unknown — newer than kVersion and not in `lineage`).\n";
    o << "    [[nodiscard]] static const uint32_t* layoutFor(uint16_t ver, const Lineage* lineage) noexcept {\n"
      << "        if (lineage != nullptr) {\n"
      << "            if (const uint32_t* r = lineage->row(ver)) return r;\n"
      << "        }\n"
      << "        return (ver >= 1u && ver <= kVersion) ? kLineage[ver - 1u] : nullptr;\n"
      << "    }\n";
}

/// The steady state of decode_(): a whole record of kVersion reads every field at its constant offset (one
/// predicted branch, no table, no reset: every member is assigned).
static void emitDecodeFastPath(std::ostringstream& o, const Descriptor& d) {
    o << "        if (::rune::codec::fly::load<uint16_t>(r + 2) == kVersion && bl >= kBlockLength) [[likely]] {\n";
    std::vector<const FieldDescriptor*> vars;
    for (const auto& f : d.fields) {
        if (isVarLen(f.type)) { vars.push_back(&f); continue; }
        if (f.type == PrimType::FixedStr) {
            o << "            std::memcpy(" << f.name << ".data(), r + 4u + " << f.offset << "u, " << f.fixedLen << "u);\n";
        } else {
            o << "            " << f.name << " = ::rune::codec::fly::load<" << flyType(f.type) << ">(r + 4u + " << f.offset
              << "u);\n";
        }
    }
    if (!vars.empty()) {
        std::sort(vars.begin(), vars.end(), [](const FieldDescriptor* a, const FieldDescriptor* b) { return a->offset < b->offset; });
        // At kVersion the var fields are exactly the members, in var-index order: one sequential walk.
        // F1 (PR #434 review): a hostile block length past the record's end starts the walk AT the end (every var
        // field empty) - `n - pos` never wraps.
        o << "            std::size_t pos = 4u + bl <= n ? 4u + bl : n;\n";
        for (const FieldDescriptor* f : vars) {
            o << "            if (n - pos >= 4u && ::rune::codec::fly::load<uint32_t>(r + pos) <= n - pos - 4u) {\n"
              << "                const uint32_t len = ::rune::codec::fly::load<uint32_t>(r + pos);\n"
              << "                " << f->name << " = std::string_view(reinterpret_cast<const char*>(r + pos + 4u), len);\n"
              << "                pos += 4u + len;\n"
              << "            } else {\n"
              << "                " << f->name << " = {};\n"
              << "                pos = n;   // truncated: every later var field reads empty\n"
              << "            }\n";
        }
    }
    o << "            return true;\n";
    o << "        }\n";
}

/// decode(record) / decode(record, lineage): every member is reset, then read through the row of the record's
/// own version.  False (every member at its default): shorter than a header, or a version this reader does not know.
static void emitDecode(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    o << "    // #418: decode() places every field by the record's OWN version (its row of the lineage), so a record\n";
    o << "    // written before or after a DROP / ADD / RENAME reads each field right or absent (default), never\n";
    o << "    // shifted.  Returns false — every member at its default — for a record shorter than its header or of a\n";
    o << "    // version this type does not know (after kVersion: pass the topic's Lineage).  The Validator is still\n";
    o << "    // the gate for untrusted input; every access is bounds-checked, so a hostile record can never read\n";
    o << "    // out of bounds.\n";
    o << "    [[nodiscard]] bool decode(std::span<const uint8_t> record) noexcept {\n"
      << "        return decodeOwn_(record) || decodeAny_(record, nullptr);\n"
      << "    }\n";
    o << "    [[nodiscard]] bool decode(std::span<const uint8_t> record, const Lineage& lineage) noexcept {\n"
      << "        return decodeOwn_(record) || decodeAny_(record, &lineage);\n"
      << "    }\n\n";
    o << "    /// The steady state, inlined: a whole record of kVersion at the constant offsets (false: anything else).\n";
    o << "    [[nodiscard]] bool decodeOwn_(std::span<const uint8_t> record) noexcept {\n";
    o << "        if (record.size() < 4u + kBlockLength) return false;\n";
    o << "        const uint8_t* const r = record.data();\n";
    o << "        const std::size_t n = record.size();\n";
    o << "        const uint32_t bl = ::rune::codec::fly::load<uint16_t>(r);\n";
    o << "        (void)n;\n";
    emitDecodeFastPath(o, d);
    o << "        return false;\n";
    o << "    }\n";
    o << "    /// Any other record: every member reset, then each field through the row of the record's version.\n";
    o << "    [[gnu::noinline]] [[nodiscard]] bool decodeAny_(std::span<const uint8_t> record, const Lineage* lineage) noexcept {\n";
    o << "        *this = " << structName << "{};\n";
    o << "        if (record.size() < 4) return false;\n";
    o << "        const uint8_t* const r = record.data();\n";
    o << "        const std::size_t n = record.size();\n";
    o << "        const uint32_t bl = ::rune::codec::fly::load<uint16_t>(r);\n";
    o << "        const uint16_t ver = ::rune::codec::fly::load<uint16_t>(r + 2);\n";
    o << "        const uint32_t* const row = layoutFor(ver, lineage);\n";
    o << "        if (row == nullptr) return false;\n";
    o << "        (void)bl; (void)n;\n";
    bool anyVar = false;
    for (std::size_t i = 0; i < d.fields.size(); ++i) {
        const auto& f = d.fields[i];
        if (isVarLen(f.type)) { anyVar = true; continue; }
        const uint32_t w = fieldWidth(f);
        o << "        if (row[" << i << "] != ::rune::codec::kAbsentField && row[" << i << "] + " << w << "u <= bl && 4u + row["
          << i << "] + " << w << "u <= n) ";
        if (f.type == PrimType::FixedStr) {
            o << "std::memcpy(" << f.name << ".data(), r + 4u + row[" << i << "], " << w << "u);\n";
        } else {
            o << f.name << " = ::rune::codec::fly::load<" << flyType(f.type) << ">(r + 4u + row[" << i << "]);\n";
        }
    }
    if (anyVar) {
        o << "        std::size_t pos = 4u + bl <= n ? 4u + bl : n;   // never past the end (F1)\n";
        o << "        uint32_t    at  = 0;\n";
        for (std::size_t i = 0; i < d.fields.size(); ++i) {
            const auto& f = d.fields[i];
            if (!isVarLen(f.type)) continue;
            o << "        if (row[" << i << "] != ::rune::codec::kAbsentField) (void)::rune::codec::fly::readVar(r, n, pos, at, row["
              << i << "], " << f.name << ");\n";
        }
    }
    o << "        return true;\n";
    o << "    }\n\n";
}

// ─── generateHeader ───────────────────────────────────────────────────────────

std::string generateHeader(const Descriptor& d, std::string_view nspace) {
    return generateHeader(d, d.version, nspace);
}

std::string generateHeader(const Descriptor& evolving, uint16_t version, std::string_view nspace) {
    if (version == 0 || version > evolving.version) {
        throw std::invalid_argument("generateHeader: schema '" + evolving.name + "' has no version " +
                                    std::to_string(version) + " (its lineage ends at " +
                                    std::to_string(evolving.version) + ")");
    }
    // #418: the type is the schema AS OF `version`; the lineage (every field ever declared, with its lifetime) gives
    // each of its fields' place in a record of every version 1..version.
    const Descriptor d = materializeAsOf(evolving, version);
    if (d.find("encodeTo") != nullptr)
        throw std::invalid_argument("generateHeader: schema '" + d.name +
            "' has a field named 'encodeTo', which collides with the generated Filter::encodeTo — rename the field");
    if (d.find("Filter") != nullptr)
        throw std::invalid_argument("generateHeader: schema '" + d.name +
            "' has a field named 'Filter', which collides with the generated filter type — rename the field");
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
    o << "#include <expected>\n";
    o << "#include <span>\n";
    o << "#include <stdexcept>\n";
    o << "#include <string_view>\n";
    o << "#include <vector>\n";
    o << "#include <rune/codec/Descriptor.h>\n";
    o << "#include <rune/codec/Filter.h>\n";
    o << "#include <rune/codec/Fingerprint.h>\n";
    o << "#include <rune/codec/Flyweight.h>\n";
    o << "#include <rune/codec/Lineage.h>\n";
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
    emitLineage(o, evolving, d, structName);
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

    emitFilter(o, d);

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

    // ── decode() — #418: through the row of the record's OWN version ─────────
    emitDecode(o, d, structName);

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
