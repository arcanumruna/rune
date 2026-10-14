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

    // Task 19b: OPTIONAL trailing output-descriptor blob.  Appended ONLY when
    // present so a record WITHOUT it serializes byte-for-byte as pre-19b did
    // (keeps existing round-trip expectations intact).
    if (!outputDescriptorBlob.empty()) {
        writeU32LE(buf, static_cast<uint32_t>(outputDescriptorBlob.size()));
        buf.insert(buf.end(), outputDescriptorBlob.begin(), outputDescriptorBlob.end());
    }

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
    if (v.persistence > 1) return std::nullopt;   // append-only: 0 = persisted, 1 = transient

    // Task 19b: an OPTIONAL trailing output-descriptor blob.  A pre-19b record
    // ends exactly here (pos == size) and leaves outputDescriptorBlob empty; a
    // 19b record carries outDescLen:u32 + bytes.  Either way the buffer must be
    // EXACTLY consumed (no silent acceptance of unexpected trailing bytes).
    if (pos < buf.size()) {
        uint32_t outLen = 0;
        if (!readU32LE(buf, pos, outLen)) return std::nullopt;
        if (pos + outLen > buf.size())    return std::nullopt;
        v.outputDescriptorBlob.assign(buf.data() + pos, buf.data() + pos + outLen);
        pos += outLen;
    }
    if (pos != buf.size()) return std::nullopt;

    return v;
}

// ---------------------------------------------------------------------------
// ViewRegistry
// ---------------------------------------------------------------------------
void ViewRegistry::applyRecord(const ViewRecord& v) {
    const auto it = idToIndex_.find(v.viewTopicId);
    if (it != idToIndex_.end()) {
        // Re-apply for an EXISTING viewTopicId: if the incoming record
        // renamed the view (viewName differs from what's currently stored),
        // the OLD name's nameToId_ entry must be erased first — otherwise
        // byName(oldName) would keep resolving to this (now-renamed) slot
        // forever, a stale-mapping hole in the by-name lookup contract.
        // (Phase 1 never actually renames a view, but the registry must not
        // silently corrupt its own index if a future DDL path does.)
        const ViewRecord& existing = records_[it->second];
        if (existing.viewName != v.viewName) {
            const auto staleIt = nameToId_.find(existing.viewName);
            if (staleIt != nameToId_.end() && staleIt->second == v.viewTopicId) {
                nameToId_.erase(staleIt);
            }
        }
        nameToId_[v.viewName] = v.viewTopicId;

        // Overwrite the existing slot in place — records_ (a deque) never
        // moves existing elements on assignment, so any `const ViewRecord*`
        // a caller already holds into this slot now simply observes the
        // updated definition (same stability guarantee as SchemaRegistry's
        // append-only deque, just applied to a replace-in-place update).
        records_[it->second] = v;
        return;
    }

    nameToId_[v.viewName] = v.viewTopicId;
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
    out.reserve(idToIndex_.size());
    for (const auto& v : records_) {
        if (v.viewTopicId < 0) continue;   // a retired (erased) slot
        out.push_back(&v);
    }
    return out;
}

bool ViewRegistry::erase(int32_t viewTopicId) {
    const auto it = idToIndex_.find(viewTopicId);
    if (it == idToIndex_.end()) return false;
    ViewRecord& slot = records_[it->second];
    const auto nameIt = nameToId_.find(slot.viewName);
    if (nameIt != nameToId_.end() && nameIt->second == viewTopicId) nameToId_.erase(nameIt);
    slot = ViewRecord{};   // retired in place: viewTopicId = -1, no blobs
    idToIndex_.erase(it);
    return true;
}

} // namespace rune::node
