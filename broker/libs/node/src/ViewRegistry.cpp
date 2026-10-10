#include "rune/node/ViewRegistry.h"
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
// ViewRecord::serialize
// Layout: viewTopicId:i32,
//         viewNameLen:u16 + viewName bytes,
//         compiledPlanLen:u32 + compiledPlanBlob bytes,
//         persistence:u8
// ---------------------------------------------------------------------------
std::vector<uint8_t> ViewRecord::serialize() const {
    std::vector<uint8_t> buf;
    buf.reserve(4 + 2 + viewName.size() + 4 + compiledPlanBlob.size() + 1);

    writeI32LE(buf, viewTopicId);

    writeU16LE(buf, static_cast<uint16_t>(viewName.size()));
    for (unsigned char c : viewName) {
        buf.push_back(c);
    }

    writeU32LE(buf, static_cast<uint32_t>(compiledPlanBlob.size()));
    buf.insert(buf.end(), compiledPlanBlob.begin(), compiledPlanBlob.end());

    writeU8(buf, persistence);

    return buf;
}

// ---------------------------------------------------------------------------
// ViewRecord::deserialize — bounds-checks every read; nullopt on truncation
// or a bogus length prefix (Task 8 lesson: never an unvalidated huge
// reserve/read driven by attacker/corruption-controlled length fields —
// every length is checked against the ACTUAL remaining buffer size before
// any bytes are copied out of it).
// ---------------------------------------------------------------------------
std::optional<ViewRecord> ViewRecord::deserialize(std::span<const uint8_t> buf) {
    size_t pos = 0;
    ViewRecord v;

    if (!readI32LE(buf, pos, v.viewTopicId)) return std::nullopt;

    uint16_t nameLen = 0;
    if (!readU16LE(buf, pos, nameLen)) return std::nullopt;
    if (pos + nameLen > buf.size())    return std::nullopt;
    v.viewName.assign(reinterpret_cast<const char*>(buf.data() + pos), nameLen);
    pos += nameLen;

    uint32_t planLen = 0;
    if (!readU32LE(buf, pos, planLen)) return std::nullopt;
    if (pos + planLen > buf.size())    return std::nullopt;
    v.compiledPlanBlob.assign(buf.data() + pos, buf.data() + pos + planLen);
    pos += planLen;

    if (!readU8(buf, pos, v.persistence)) return std::nullopt;

    // Reject any unexpected trailing bytes — an exact, fully-consumed buffer
    // only (no silent acceptance of a longer-than-expected record).
    if (pos != buf.size()) return std::nullopt;

    return v;
}

// ---------------------------------------------------------------------------
// ViewRegistry
// ---------------------------------------------------------------------------
void ViewRegistry::applyRecord(const ViewRecord& v) {
    nameToId_[v.viewName] = v.viewTopicId;

    const auto it = idToIndex_.find(v.viewTopicId);
    if (it != idToIndex_.end()) {
        // Overwrite the existing slot in place — records_ (a deque) never
        // moves existing elements on assignment, so any `const ViewRecord*`
        // a caller already holds into this slot now simply observes the
        // updated definition (same stability guarantee as SchemaRegistry's
        // append-only deque, just applied to a replace-in-place update).
        records_[it->second] = v;
        return;
    }

    idToIndex_[v.viewTopicId] = records_.size();
    records_.push_back(v);
}

const ViewRecord* ViewRegistry::byName(std::string_view name) const {
    const auto it = nameToId_.find(std::string(name));
    if (it == nameToId_.end()) return nullptr;
    return byViewTopicId(it->second);
}

const ViewRecord* ViewRegistry::byViewTopicId(int32_t viewTopicId) const {
    const auto it = idToIndex_.find(viewTopicId);
    if (it == idToIndex_.end()) return nullptr;
    return &records_[it->second];
}

void ViewRegistry::rebuildFromRecords(std::span<const ViewRecord> records) {
    records_.clear();
    idToIndex_.clear();
    nameToId_.clear();
    for (const auto& v : records) {
        applyRecord(v);
    }
}

std::vector<const ViewRecord*> ViewRegistry::all() const {
    std::vector<const ViewRecord*> out;
    out.reserve(records_.size());
    for (const auto& v : records_) {
        out.push_back(&v);
    }
    return out;
}

} // namespace rune::node
