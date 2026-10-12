#include "rune/node/SchemaRegistry.h"
#include <rune/codec/Evolve.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <algorithm>
#include <cstring>

namespace rune::node {

// ---------------------------------------------------------------------------
// Little-endian write helpers (no heap, stack-local temporaries only)
// ---------------------------------------------------------------------------
namespace {

void writeU8(std::vector<uint8_t>& buf, uint8_t v) {
    buf.push_back(v);
}

void writeU16LE(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
}

void writeI32LE(std::vector<uint8_t>& buf, int32_t v) {
    const uint32_t u = static_cast<uint32_t>(v);
    buf.push_back(static_cast<uint8_t>(u & 0xff));
    buf.push_back(static_cast<uint8_t>((u >> 8) & 0xff));
    buf.push_back(static_cast<uint8_t>((u >> 16) & 0xff));
    buf.push_back(static_cast<uint8_t>((u >> 24) & 0xff));
}

void writeU32LE(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
}

// ---------------------------------------------------------------------------
// Little-endian read helpers — each returns false on truncation
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

bool readI32LE(std::span<const uint8_t> buf, size_t& pos, int32_t& out) {
    if (pos + 4 > buf.size()) return false;
    uint32_t u = static_cast<uint32_t>(buf[pos]) |
                 (static_cast<uint32_t>(buf[pos + 1]) << 8) |
                 (static_cast<uint32_t>(buf[pos + 2]) << 16) |
                 (static_cast<uint32_t>(buf[pos + 3]) << 24);
    std::memcpy(&out, &u, 4);
    pos += 4;
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

} // anonymous namespace

// ---------------------------------------------------------------------------
// TopicSchema::serialize
// Layout: topicId:i32, version:u16, partitionCount:i32, validate:u8,
//         keyFieldLen:u16 + keyField bytes,
//         descriptorLen:u32 + descriptorBlob bytes
// ---------------------------------------------------------------------------
std::vector<uint8_t> TopicSchema::serialize() const {
    std::vector<uint8_t> buf;
    // Reserve conservatively: fixed fields + variable data estimate
    buf.reserve(4 + 2 + 4 + 1 + 2 + keyField.size() + 4 + descriptorBlob.size());

    writeI32LE(buf, topicId);
    writeU16LE(buf, version);
    writeI32LE(buf, partitionCount);
    writeU8(buf, validate ? uint8_t{1} : uint8_t{0});

    // keyField: u16 length prefix + bytes
    writeU16LE(buf, static_cast<uint16_t>(keyField.size()));
    for (unsigned char c : keyField) {
        buf.push_back(c);
    }

    // descriptorBlob: u32 length prefix + bytes
    writeU32LE(buf, static_cast<uint32_t>(descriptorBlob.size()));
    buf.insert(buf.end(), descriptorBlob.begin(), descriptorBlob.end());

    // [schema-evolution] trailing fields appended after the original layout
    // (Task 9) — see deserialize() for the legacy-record default behavior.
    writeU16LE(buf, nextTag);
    writeU8(buf, static_cast<uint8_t>(compatMode));

    // [restart recovery] optional topicName trailer: u16 length + bytes, written
    // ONLY when a name is set, so a nameless record stays byte-identical to the
    // pre-name layout (exact 3-byte tail).
    if (!topicName.empty()) {
        writeU16LE(buf, static_cast<uint16_t>(topicName.size()));
        for (unsigned char c : topicName) buf.push_back(c);
    }

    return buf;
}

// ---------------------------------------------------------------------------
// TopicSchema::deserialize — bounds-checks every read; nullopt on truncation
// ---------------------------------------------------------------------------
std::optional<TopicSchema> TopicSchema::deserialize(std::span<const uint8_t> buf) {
    size_t pos = 0;
    TopicSchema ts;

    if (!readI32LE(buf, pos, ts.topicId))     return std::nullopt;
    if (!readU16LE(buf, pos, ts.version))      return std::nullopt;
    if (!readI32LE(buf, pos, ts.partitionCount)) return std::nullopt;

    uint8_t validateByte = 0;
    if (!readU8(buf, pos, validateByte))       return std::nullopt;
    ts.validate = (validateByte != 0);

    uint16_t keyLen = 0;
    if (!readU16LE(buf, pos, keyLen))          return std::nullopt;
    if (pos + keyLen > buf.size())             return std::nullopt;
    ts.keyField.assign(reinterpret_cast<const char*>(buf.data() + pos), keyLen);
    pos += keyLen;

    uint32_t descLen = 0;
    if (!readU32LE(buf, pos, descLen))         return std::nullopt;
    if (pos + descLen > buf.size())            return std::nullopt;
    ts.descriptorBlob.assign(buf.data() + pos, buf.data() + pos + descLen);
    pos += descLen;

    // [schema-evolution] trailing (nextTag, compatMode) pair, appended after
    // the original layout (Task 9). A legacy record (written before these
    // fields existed) has exactly 0 bytes left here: default
    // compatMode=BACKWARD and nextTag to the embedded descriptor's field
    // count, so a migrated single-version topic's next ADD allocates a
    // fresh tag past every field it already has. Any OTHER leftover byte
    // count (1 or 2 -- a partially-written/truncated tail -- or more than
    // 3 -- an unknown trailing extension) is rejected, same as every other
    // bounds check in this function: only an EXACT 3-byte tail is accepted
    // as present-and-whole.
    const size_t remaining = buf.size() - pos;
    if (remaining == 0) {
        ts.compatMode = rune::codec::CompatMode::Backward;
        try {
            const rune::codec::Descriptor d = rune::codec::deserialize(ts.descriptorBlob);
            ts.nextTag = static_cast<uint16_t>(d.fields.size());
        } catch (const std::exception&) {
            ts.nextTag = 0;
        }
    } else if (remaining == 3 || remaining >= 5) {
        uint8_t compatByte = 0;
        if (!readU16LE(buf, pos, ts.nextTag))   return std::nullopt;
        if (!readU8(buf, pos, compatByte))      return std::nullopt;
        // Defense-in-depth: compatByte is only well-formed as Backward(0) or
        // Forward(1) (rune::codec::CompatMode's only two enumerators). A byte
        // outside that range is a malformed/corrupt record — reject it the
        // same way every other bounds/shape check in this function does,
        // rather than silently static_cast-ing it into an unintended mode.
        if (compatByte != static_cast<uint8_t>(rune::codec::CompatMode::Backward) &&
            compatByte != static_cast<uint8_t>(rune::codec::CompatMode::Forward)) {
            return std::nullopt;
        }
        ts.compatMode = static_cast<rune::codec::CompatMode>(compatByte);

        // [restart recovery] optional topicName trailer.  Accepted only when the
        // length prefix and EXACTLY that many name bytes are present — a
        // partial/oversized tail is malformed, same as every other check here.
        if (remaining >= 5) {
            uint16_t nameLen = 0;
            if (!readU16LE(buf, pos, nameLen))          return std::nullopt;
            if (buf.size() - pos != nameLen)            return std::nullopt;
            ts.topicName.assign(reinterpret_cast<const char*>(buf.data() + pos), nameLen);
            pos += nameLen;
        }
    } else {
        return std::nullopt;  // truncated tail (1, 2 or 4 bytes)
    }

    return ts;
}

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------
void SchemaRegistry::applyRecord(const TopicSchema& ts) {
    RegistryEntry e;
    e.topicId        = ts.topicId;
    e.descriptorBlob = ts.descriptorBlob;

    // `ts.descriptorBlob` carries the cumulative EVOLVING descriptor (every
    // field ever declared for this topic, live or retired). The stored
    // registry entry must reflect the live field set *as of ts.version*
    // (matching offsets/blockLength a record written at that version was
    // actually encoded with) — a fresh v1 topic's evolving descriptor has
    // no retired/future fields, so materializeAsOf is an identity there.
    const rune::codec::Descriptor evolving = rune::codec::deserialize(ts.descriptorBlob);
    e.descriptor     = rune::codec::materializeAsOf(evolving, ts.version);
    e.fingerprint    = rune::codec::fingerprintAsOf(evolving, ts.version);
    e.validator      = ts.validate
                         ? std::optional<rune::codec::Validator>(std::in_place, e.descriptor)
                         : std::nullopt;
    e.keyField       = ts.keyField;
    e.version        = ts.version;
    e.partitionCount = ts.partitionCount;
    e.nextTag        = ts.nextTag;
    e.compatMode     = ts.compatMode;
    e.topicName      = ts.topicName;

    // Index by the TOPIC name — every byName() caller (GetTopicSchema,
    // DefineTopic's TopicExists check, AlterTopic) passes the name the client
    // defined the topic under, which is independent of the descriptor's name
    // (Phase 2b C regression: "typed_route" with the "characters" descriptor
    // answered UnknownTopic).  The descriptor name is only the FALLBACK for a
    // record applied with an empty topicName: a pre-name-trailer durable record
    // (where it is the only name the topic can be found under), or any in-memory
    // caller that leaves it unset.  It is NOT legacy-only by construction — every
    // in-tree caller must set topicName (DefineTopic commit + replay, ALTER's
    // carry-forward, CREATE VIEW's commitCreateView_ and view recovery all do),
    // or the entry lands under its descriptor name and can shadow a real topic.
    const std::string& indexName = e.topicName.empty() ? e.descriptor.name : e.topicName;
    nameToId_[indexName] = ts.topicId;

    // Keep ALL versions: append (never overwrite) by version, 1-based index.
    // byId_'s per-topic container is a deque (not a vector) specifically so
    // growing it here never invalidates a `const RegistryEntry*` a caller
    // already holds from a prior byId/latest/byIdVersion/byFingerprint call.
    auto& versions = byId_[ts.topicId];
    const size_t idx = (ts.version > 0) ? static_cast<size_t>(ts.version - 1) : size_t{0};
    if (idx >= versions.size()) versions.resize(idx + 1);
    versions[idx] = std::move(e);

    // Only genuine runtime ids advance the water mark. The `>= 0` guard keeps
    // negative internal ids (e.g. kSchemaTopicId = -4) from casting to a huge
    // unsigned (0xFFFFFFFC) and corrupting the water mark.
    if (ts.topicId >= 0 &&
        static_cast<uint32_t>(ts.topicId) >= RUNTIME_TOPIC_ID_BASE) {
        runtimeIdHighWater_ = std::max(runtimeIdHighWater_,
                                       static_cast<uint32_t>(ts.topicId) + 1u);
    }
}

const RegistryEntry* SchemaRegistry::byId(int32_t topicId) const {
    return latest(topicId);
}

const RegistryEntry* SchemaRegistry::byName(std::string_view name) const {
    const auto it = nameToId_.find(std::string(name));
    if (it == nameToId_.end()) return nullptr;
    return latest(it->second);
}

const RegistryEntry* SchemaRegistry::byIdVersion(int32_t topicId, uint16_t version) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end() || version == 0 || version > it->second.size()) return nullptr;
    const RegistryEntry& e = it->second[version - 1];
    return (e.topicId == topicId) ? &e : nullptr;  // guards an unfilled gap slot
}

const RegistryEntry* SchemaRegistry::latest(int32_t topicId) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end() || it->second.empty()) return nullptr;
    const RegistryEntry& e = it->second.back();
    return (e.topicId == topicId) ? &e : nullptr;  // guards an unfilled gap slot
}

const RegistryEntry* SchemaRegistry::byFingerprint(int32_t topicId, uint64_t fp) const {
    const auto it = byId_.find(topicId);
    if (it == byId_.end()) return nullptr;
    for (const auto& e : it->second) {
        if (e.topicId == topicId && e.fingerprint == fp) return &e;
    }
    return nullptr;
}

uint16_t SchemaRegistry::nextTagFor(int32_t topicId) const {
    const RegistryEntry* e = latest(topicId);
    return e ? e->nextTag : 0;
}

int32_t SchemaRegistry::allocateRuntimeId() {
    return static_cast<int32_t>(runtimeIdHighWater_++);
}

void SchemaRegistry::rebuildFromRecords(std::span<const TopicSchema> records) {
    byId_.clear();
    nameToId_.clear();
    runtimeIdHighWater_ = RUNTIME_TOPIC_ID_BASE;
    for (const auto& ts : records) {
        applyRecord(ts);
    }
}

} // namespace rune::node
