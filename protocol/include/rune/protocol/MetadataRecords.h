#pragma once
// MetadataRecords.h — MSM (Metadata State Machine) log record codecs.
//
// These records are appended to and replayed from the metadata log to assign
// partition leadership across nodes.  They are INTERNAL to the broker cluster
// and are NOT part of the client-facing protocol (schemaId 120).
//
// Wire format for every record (all fields little-endian):
//   [0..1]   recordType     uint16 LE  — MetadataRecordType enum value
//   [2]      recordVersion  uint8      — schema version guard (only 0 is valid)
//   [3..]    record-specific fixed fields + var fields
//
// Var strings:  uint16 LE length prefix + that many bytes (same as client Codec.h)
// Int32 arrays: uint16 LE count prefix  + count × 4-byte LE int32 elements
//
// Decode contract:
//   - Returns std::nullopt on ANY truncation (buf too short for the claimed layout).
//   - Returns std::nullopt when recordType mismatches the target struct.
//   - Returns std::nullopt when recordVersion is not 0 (unknown — clean reject, no crash).
//
// Namespace: rune::protocol::metadata  (distinct from rune::protocol::client)

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::protocol::metadata {

// ---------------------------------------------------------------------------
// MetadataRecordType — internal enum, NOT related to client templateIds.
// IDs are small stable constants; they live in this enum namespace and
// cannot collide with client protocol template-id space by construction.
// ---------------------------------------------------------------------------
enum class MetadataRecordType : uint16_t {
    CreateTopic              = 1,
    UpdatePartitionAssignment = 2,
    NodeJoin                 = 3,
    NodeLeave                = 4,
};

// ---------------------------------------------------------------------------
// Int32ArrayView — zero-copy view over a count-prefixed int32 array.
//
// Layout in wire buffer:
//   [0..1]  count  uint16 LE
//   [2..]   count × 4 bytes, each a LE int32
//
// Lifetime: valid only while the source buffer is alive.
// ---------------------------------------------------------------------------
class Int32ArrayView {
public:
    Int32ArrayView() noexcept = default;
    Int32ArrayView(const int32_t* data, uint16_t count) noexcept
        : data_(data), count_(count) {}

    std::size_t size()  const noexcept { return count_; }
    bool        empty() const noexcept { return count_ == 0; }

    int32_t operator[](std::size_t i) const noexcept {
        // Decode LE int32 from raw bytes (data_ may be unaligned, so byte-read)
        const auto* p = reinterpret_cast<const uint8_t*>(data_) + i * 4u;
        uint32_t v = uint32_t(p[0])
                   | (uint32_t(p[1]) << 8)
                   | (uint32_t(p[2]) << 16)
                   | (uint32_t(p[3]) << 24);
        return static_cast<int32_t>(v);
    }

    // Iterator — forward only
    struct Iterator {
        using iterator_category = std::forward_iterator_tag;
        using value_type        = int32_t;
        using difference_type   = std::ptrdiff_t;
        using pointer           = void;
        using reference         = int32_t;

        const uint8_t* ptr;
        uint16_t       remaining;

        int32_t operator*() const noexcept {
            uint32_t v = uint32_t(ptr[0]) | (uint32_t(ptr[1])<<8)
                       | (uint32_t(ptr[2])<<16) | (uint32_t(ptr[3])<<24);
            return static_cast<int32_t>(v);
        }
        Iterator& operator++() noexcept { ptr += 4; --remaining; return *this; }
        Iterator  operator++(int) noexcept { auto c = *this; ++(*this); return c; }
        bool operator==(const Iterator& o) const noexcept { return remaining == o.remaining; }
        bool operator!=(const Iterator& o) const noexcept { return remaining != o.remaining; }
    };

    Iterator begin() const noexcept {
        return {reinterpret_cast<const uint8_t*>(data_), count_};
    }
    Iterator end() const noexcept {
        return {reinterpret_cast<const uint8_t*>(data_) + count_ * 4u, 0u};
    }

private:
    const int32_t* data_  = nullptr;
    uint16_t       count_ = 0;
};

// ---------------------------------------------------------------------------
// Internal LE helpers (local to this header, no client namespace dependency)
// ---------------------------------------------------------------------------
namespace detail {

inline void putU16(uint8_t* buf, int off, uint16_t v) noexcept {
    buf[off]     = static_cast<uint8_t>(v);
    buf[off + 1] = static_cast<uint8_t>(v >> 8);
}
inline uint16_t getU16(const uint8_t* buf, int off) noexcept {
    return static_cast<uint16_t>(uint16_t(buf[off]) | (uint16_t(buf[off+1]) << 8));
}
inline void putS16(uint8_t* buf, int off, int16_t v) noexcept {
    putU16(buf, off, static_cast<uint16_t>(v));
}
inline int16_t getS16(const uint8_t* buf, int off) noexcept {
    return static_cast<int16_t>(getU16(buf, off));
}
inline void putS32(uint8_t* buf, int off, int32_t v) noexcept {
    uint32_t u = static_cast<uint32_t>(v);
    buf[off]     = static_cast<uint8_t>(u);
    buf[off + 1] = static_cast<uint8_t>(u >> 8);
    buf[off + 2] = static_cast<uint8_t>(u >> 16);
    buf[off + 3] = static_cast<uint8_t>(u >> 24);
}
inline int32_t getS32(const uint8_t* buf, int off) noexcept {
    uint32_t v = uint32_t(buf[off])
               | (uint32_t(buf[off+1]) << 8)
               | (uint32_t(buf[off+2]) << 16)
               | (uint32_t(buf[off+3]) << 24);
    return static_cast<int32_t>(v);
}

// Write a var-string field: uint16 LE length + bytes.  Returns bytes written.
inline std::size_t putVarStr(uint8_t* buf, std::string_view s) noexcept {
    const uint16_t len = static_cast<uint16_t>(s.size());
    putU16(buf, 0, len);
    for (uint16_t i = 0; i < len; ++i)
        buf[2 + i] = static_cast<uint8_t>(s[i]);
    return static_cast<std::size_t>(2) + len;
}

// Read a var-string field. Advances pos. Returns false if truncated.
inline bool getVarStr(const uint8_t* buf, std::size_t avail, std::size_t& pos,
                      std::string_view& out) noexcept {
    if (pos + 2u > avail) return false;
    const uint16_t len = getU16(buf, static_cast<int>(pos));
    pos += 2u;
    if (pos + len > avail) return false;
    out = std::string_view(reinterpret_cast<const char*>(buf + pos), len);
    pos += len;
    return true;
}

// Write an int32 array: uint16 LE count + count × LE int32. Returns bytes written.
inline std::size_t putInt32Array(uint8_t* buf, const Int32ArrayView& arr) noexcept {
    const uint16_t cnt = static_cast<uint16_t>(arr.size());
    putU16(buf, 0, cnt);
    std::size_t off = 2u;
    for (uint16_t i = 0; i < cnt; ++i) {
        putS32(buf, static_cast<int>(off), arr[i]);
        off += 4u;
    }
    return off;
}

// Read an int32 array from buf. Advances pos. Returns false if truncated.
// On success, view points into buf (zero-copy).
inline bool getInt32Array(const uint8_t* buf, std::size_t avail, std::size_t& pos,
                          Int32ArrayView& out) noexcept {
    if (pos + 2u > avail) return false;
    const uint16_t cnt = getU16(buf, static_cast<int>(pos));
    pos += 2u;
    const std::size_t need = static_cast<std::size_t>(cnt) * 4u;
    if (pos + need > avail) return false;
    out = Int32ArrayView(reinterpret_cast<const int32_t*>(buf + pos), cnt);
    pos += need;
    return true;
}

// Common record prefix: type(2) + version(1) = 3 bytes.
static constexpr std::size_t kPrefixSize = 3u;
static constexpr uint8_t     kCurrentVersion = 0u;

// Encode the 3-byte prefix into buf.
inline void encodePrefixAt(uint8_t* buf, MetadataRecordType type) noexcept {
    putU16(buf, 0, static_cast<uint16_t>(type));
    buf[2] = kCurrentVersion;
}

// Validate the 3-byte prefix. Returns false if truncated, type mismatch, or unknown version.
inline bool validatePrefix(const uint8_t* buf, std::size_t avail,
                            MetadataRecordType expectedType) noexcept {
    if (avail < kPrefixSize) return false;
    const uint16_t typeId = getU16(buf, 0);
    if (typeId != static_cast<uint16_t>(expectedType)) return false;
    if (buf[2] != kCurrentVersion) return false;
    return true;
}

} // namespace detail

// ---------------------------------------------------------------------------
// peekRecordType — read the type from the wire buffer without full decode.
// Returns nullopt if buffer is too short (< 2 bytes).
// ---------------------------------------------------------------------------
inline std::optional<MetadataRecordType> peekRecordType(const uint8_t* buf,
                                                         std::size_t avail) noexcept {
    if (avail < 2u) return std::nullopt;
    return static_cast<MetadataRecordType>(detail::getU16(buf, 0));
}

// ===========================================================================
// CreateTopicRecord  (recordType = 1)
//
// Wire layout (all little-endian):
//   [0..1]   recordType     uint16 LE = 1
//   [2]      recordVersion  uint8  = 0
//   [3..6]   topicId        int32
//   [7..8]   name length    uint16
//   [9..]    name bytes
//   [+0..+3] partitionCount int32
//   [+4..+5] rf             int16
//   [+6..+7] partitionKey length  uint16
//   [+8..]   partitionKey bytes
//   [+0]     placementMode  uint8
//   [+1..+2] coPartitionGroup length  uint16
//   [+3..]   coPartitionGroup bytes
// ===========================================================================
struct CreateTopicRecord {
    int32_t          topicId          = 0;
    std::string_view name;
    int32_t          partitionCount   = 1;
    int16_t          rf               = 1;
    std::string_view partitionKey;
    uint8_t          placementMode    = 0;
    std::string_view coPartitionGroup;
    uint8_t          recordVersion    = 0;
    uint8_t          durability       = 0;   // 0 = Majority (default), 1 = AllInSync. Only encoded when recordVersion >= 1.

    static constexpr uint16_t RECORD_TYPE_ID =
        static_cast<uint16_t>(MetadataRecordType::CreateTopic); // 1

    /// Encode into buf[0..bufLen-1].  Returns total bytes written, or 0 if buf too small.
    std::size_t encode(uint8_t* buf, std::size_t bufLen) const noexcept {
        const bool v1 = (recordVersion >= 1);
        // Calculate required size
        const std::size_t needed = detail::kPrefixSize
            + 4u                        // topicId
            + 2u + name.size()          // name
            + 4u                        // partitionCount
            + 2u                        // rf
            + 2u + partitionKey.size()  // partitionKey
            + 1u                        // placementMode
            + 2u + coPartitionGroup.size() // coPartitionGroup
            + (v1 ? 1u : 0u);           // durability byte (v1+ only)
        if (bufLen < needed) return 0u;

        // Write type (2 bytes) + version (1 byte) — version from the record, not kCurrentVersion.
        detail::putU16(buf, 0, static_cast<uint16_t>(MetadataRecordType::CreateTopic));
        buf[2] = recordVersion;                       // version-aware, not kCurrentVersion
        std::size_t pos = detail::kPrefixSize;

        // Fixed fields
        detail::putS32(buf, static_cast<int>(pos), topicId);    pos += 4u;

        // Var: name
        pos += detail::putVarStr(buf + pos, name);

        // Fixed: partitionCount, rf
        detail::putS32(buf, static_cast<int>(pos), partitionCount); pos += 4u;
        detail::putS16(buf, static_cast<int>(pos), rf);             pos += 2u;

        // Var: partitionKey
        pos += detail::putVarStr(buf + pos, partitionKey);

        // Fixed: placementMode
        buf[pos++] = placementMode;

        // Var: coPartitionGroup
        pos += detail::putVarStr(buf + pos, coPartitionGroup);

        // v1+: durability byte
        if (v1) buf[pos++] = durability;

        return pos;
    }

    /// Decode from buf[0..avail-1].  Returns nullopt on any error.
    /// Returned string_views are zero-copy into buf — buf must outlive the result.
    /// Accepts recordVersion 0 or 1; rejects unknown versions (>1).
    static std::optional<CreateTopicRecord> decode(const uint8_t* buf,
                                                    std::size_t avail) noexcept {
        // Inline prefix check: accept versions 0 and 1 (not via validatePrefix which only
        // accepts kCurrentVersion == 0).
        if (avail < detail::kPrefixSize) return std::nullopt;
        const uint16_t typeId = detail::getU16(buf, 0);
        if (typeId != static_cast<uint16_t>(MetadataRecordType::CreateTopic)) return std::nullopt;
        const uint8_t ver = buf[2];
        if (ver > 1u) return std::nullopt;  // unknown future version — clean reject

        std::size_t pos = detail::kPrefixSize;

        // topicId
        if (pos + 4u > avail) return std::nullopt;
        CreateTopicRecord rec;
        rec.recordVersion  = ver;
        rec.topicId        = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        // name
        if (!detail::getVarStr(buf, avail, pos, rec.name)) return std::nullopt;

        // partitionCount
        if (pos + 4u > avail) return std::nullopt;
        rec.partitionCount = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        // rf
        if (pos + 2u > avail) return std::nullopt;
        rec.rf = detail::getS16(buf, static_cast<int>(pos)); pos += 2u;

        // partitionKey
        if (!detail::getVarStr(buf, avail, pos, rec.partitionKey)) return std::nullopt;

        // placementMode
        if (pos + 1u > avail) return std::nullopt;
        rec.placementMode = buf[pos++];

        // coPartitionGroup
        if (!detail::getVarStr(buf, avail, pos, rec.coPartitionGroup)) return std::nullopt;

        // durability — present only in v1+
        if (rec.recordVersion >= 1) {
            if (pos + 1u > avail) return std::nullopt;
            rec.durability = buf[pos++];
        } else {
            rec.durability = 0; // Majority default for legacy v0 records
        }

        return rec;
    }
};

// ===========================================================================
// UpdatePartitionAssignmentRecord  (recordType = 2)
//
// Wire layout (all little-endian):
//   [0..1]   recordType     uint16 LE = 2
//   [2]      recordVersion  uint8  = 0
//   [3..6]   topicId        int32
//   [7..10]  partition      int32
//   [11..14] leaderNode     int32
//   [15..16] isr count      uint16
//   [17..]   isr elements   count × LE int32
//   [+0..+3] leaderEpoch    int32   (distinct from partitionEpoch)
//   [+4..+7] partitionEpoch int32   (distinct from leaderEpoch)
//   [+8..+9] replicas count uint16
//   [+10..]  replicas elements count × LE int32
// ===========================================================================
struct UpdatePartitionAssignmentRecord {
    int32_t       topicId         = 0;
    int32_t       partition       = 0;
    int32_t       leaderNode      = 0;
    Int32ArrayView isr;             // zero-copy view; may be empty
    int32_t       leaderEpoch     = 0;
    int32_t       partitionEpoch  = 0;
    Int32ArrayView replicas;        // zero-copy view; may be empty
    uint8_t       recordVersion   = 0;

    static constexpr uint16_t RECORD_TYPE_ID =
        static_cast<uint16_t>(MetadataRecordType::UpdatePartitionAssignment); // 2

    /// Encode into buf[0..bufLen-1].  Returns total bytes written, or 0 if buf too small.
    std::size_t encode(uint8_t* buf, std::size_t bufLen) const noexcept {
        const std::size_t needed = detail::kPrefixSize
            + 4u                                     // topicId
            + 4u                                     // partition
            + 4u                                     // leaderNode
            + 2u + isr.size() * 4u                  // isr count + elements
            + 4u                                     // leaderEpoch
            + 4u                                     // partitionEpoch
            + 2u + replicas.size() * 4u;             // replicas count + elements
        if (bufLen < needed) return 0u;

        std::size_t pos = 0;
        detail::encodePrefixAt(buf, MetadataRecordType::UpdatePartitionAssignment);
        pos = detail::kPrefixSize;

        detail::putS32(buf, static_cast<int>(pos), topicId);   pos += 4u;
        detail::putS32(buf, static_cast<int>(pos), partition);  pos += 4u;
        detail::putS32(buf, static_cast<int>(pos), leaderNode); pos += 4u;

        pos += detail::putInt32Array(buf + pos, isr);

        detail::putS32(buf, static_cast<int>(pos), leaderEpoch);    pos += 4u;
        detail::putS32(buf, static_cast<int>(pos), partitionEpoch); pos += 4u;

        pos += detail::putInt32Array(buf + pos, replicas);

        return pos;
    }

    /// Decode from buf[0..avail-1].  Returns nullopt on any error.
    /// Returned Int32ArrayViews are zero-copy into buf — buf must outlive the result.
    static std::optional<UpdatePartitionAssignmentRecord> decode(const uint8_t* buf,
                                                                  std::size_t avail) noexcept {
        if (!detail::validatePrefix(buf, avail, MetadataRecordType::UpdatePartitionAssignment))
            return std::nullopt;

        std::size_t pos = detail::kPrefixSize;

        if (pos + 4u > avail) return std::nullopt;
        UpdatePartitionAssignmentRecord rec;
        rec.recordVersion = buf[2];
        rec.topicId    = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (pos + 4u > avail) return std::nullopt;
        rec.partition  = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (pos + 4u > avail) return std::nullopt;
        rec.leaderNode = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (!detail::getInt32Array(buf, avail, pos, rec.isr)) return std::nullopt;

        if (pos + 4u > avail) return std::nullopt;
        rec.leaderEpoch    = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (pos + 4u > avail) return std::nullopt;
        rec.partitionEpoch = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (!detail::getInt32Array(buf, avail, pos, rec.replicas)) return std::nullopt;

        return rec;
    }
};

// ===========================================================================
// NodeJoinRecord  (recordType = 3)
//
// Wire layout (all little-endian):
//   [0..1]   recordType     uint16 LE = 3
//   [2]      recordVersion  uint8  = 0
//   [3..6]   nodeId         int32
//   [7..8]   endpoint length uint16
//   [9..]    endpoint bytes
// ===========================================================================
struct NodeJoinRecord {
    int32_t          nodeId        = 0;
    std::string_view endpoint;
    uint8_t          recordVersion = 0;

    static constexpr uint16_t RECORD_TYPE_ID =
        static_cast<uint16_t>(MetadataRecordType::NodeJoin); // 3

    /// Encode into buf[0..bufLen-1].  Returns total bytes written, or 0 if buf too small.
    std::size_t encode(uint8_t* buf, std::size_t bufLen) const noexcept {
        const std::size_t needed = detail::kPrefixSize
            + 4u                      // nodeId
            + 2u + endpoint.size();   // endpoint
        if (bufLen < needed) return 0u;

        std::size_t pos = 0;
        detail::encodePrefixAt(buf, MetadataRecordType::NodeJoin);
        pos = detail::kPrefixSize;

        detail::putS32(buf, static_cast<int>(pos), nodeId); pos += 4u;
        pos += detail::putVarStr(buf + pos, endpoint);

        return pos;
    }

    /// Decode from buf[0..avail-1].  Returns nullopt on any error.
    static std::optional<NodeJoinRecord> decode(const uint8_t* buf,
                                                 std::size_t avail) noexcept {
        if (!detail::validatePrefix(buf, avail, MetadataRecordType::NodeJoin))
            return std::nullopt;

        std::size_t pos = detail::kPrefixSize;

        if (pos + 4u > avail) return std::nullopt;
        NodeJoinRecord rec;
        rec.recordVersion = buf[2];
        rec.nodeId        = detail::getS32(buf, static_cast<int>(pos)); pos += 4u;

        if (!detail::getVarStr(buf, avail, pos, rec.endpoint)) return std::nullopt;

        return rec;
    }
};

// ===========================================================================
// NodeLeaveRecord  (recordType = 4)
//
// Wire layout (all little-endian):
//   [0..1]   recordType     uint16 LE = 4
//   [2]      recordVersion  uint8  = 0
//   [3..6]   nodeId         int32
// ===========================================================================
struct NodeLeaveRecord {
    int32_t nodeId        = 0;
    uint8_t recordVersion = 0;

    static constexpr uint16_t RECORD_TYPE_ID =
        static_cast<uint16_t>(MetadataRecordType::NodeLeave); // 4

    static constexpr std::size_t ENCODED_SIZE =
        detail::kPrefixSize + 4u; // prefix(3) + nodeId(4) = 7

    /// Encode into buf[0..bufLen-1].  Returns total bytes written (7), or 0 if too small.
    std::size_t encode(uint8_t* buf, std::size_t bufLen) const noexcept {
        if (bufLen < ENCODED_SIZE) return 0u;

        detail::encodePrefixAt(buf, MetadataRecordType::NodeLeave);
        detail::putS32(buf, static_cast<int>(detail::kPrefixSize), nodeId);

        return ENCODED_SIZE;
    }

    /// Decode from buf[0..avail-1].  Returns nullopt on any error.
    static std::optional<NodeLeaveRecord> decode(const uint8_t* buf,
                                                  std::size_t avail) noexcept {
        if (!detail::validatePrefix(buf, avail, MetadataRecordType::NodeLeave))
            return std::nullopt;
        if (avail < ENCODED_SIZE) return std::nullopt;

        NodeLeaveRecord rec;
        rec.recordVersion = buf[2];
        rec.nodeId        = detail::getS32(buf, static_cast<int>(detail::kPrefixSize));

        return rec;
    }
};

} // namespace rune::protocol::metadata
