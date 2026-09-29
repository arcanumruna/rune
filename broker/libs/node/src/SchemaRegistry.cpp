#include "rune/node/SchemaRegistry.h"
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

    return ts;
}

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------
void SchemaRegistry::applyRecord(const TopicSchema& ts) {
    RegistryEntry e;
    e.topicId        = ts.topicId;
    e.descriptorBlob = ts.descriptorBlob;
    e.descriptor     = rune::codec::deserialize(ts.descriptorBlob);
    e.fingerprint    = rune::codec::fingerprint(e.descriptor);
    e.validator      = ts.validate
                         ? std::optional<rune::codec::Validator>(std::in_place, e.descriptor)
                         : std::nullopt;
    e.keyField       = ts.keyField;
    e.version        = ts.version;
    e.partitionCount = ts.partitionCount;

    const std::string& schemaName = e.descriptor.name;
    nameToId_[schemaName]  = ts.topicId;
    byId_[ts.topicId]      = std::move(e);

    if (static_cast<uint32_t>(ts.topicId) >= RUNTIME_TOPIC_ID_BASE) {
        runtimeIdHighWater_ = std::max(runtimeIdHighWater_,
                                       static_cast<uint32_t>(ts.topicId) + 1u);
    }
}

const RegistryEntry* SchemaRegistry::byId(int32_t topicId) const {
    const auto it = byId_.find(topicId);
    return (it != byId_.end()) ? &it->second : nullptr;
}

const RegistryEntry* SchemaRegistry::byName(std::string_view name) const {
    const auto it = nameToId_.find(std::string(name));
    if (it == nameToId_.end()) return nullptr;
    return byId(it->second);
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
