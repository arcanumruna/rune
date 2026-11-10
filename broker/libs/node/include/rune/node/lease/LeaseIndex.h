#pragma once
// LeaseIndex.h -- lease topics Task 6: the lease index, a verdict member (member 0 of a lease topic's logged set).
//
// A lease topic's key index IS its lease view: a ViewTable keyed by the wire key (1 to 128 bytes) whose fixed-width row
// holds every column of the topic's EVOLVING descriptor (the four lease system columns first, then the user columns,
// dropped ones included) plus two offset columns (lease_granted_offset, lease_updated_offset).  The judge decides each
// record against the row (judgeLease, the one pure function of spec 7.2), applies the accepted ones (applyLeaseRecord)
// and hands one LeaseEvent per accepted record to the wheels (LeaseFoldEvents.h).  Deadlines are not columns (spec 7.1):
// the wheels own them.
//
// Free functions and plain structs: no virtual call, no std::function, no allocation on the judge / apply path (beyond
// ViewTable's chunk growth).  A lease record's value is [u16 blockLength][u16 version][fixed block]; the fixed block
// starts with the four u64 system columns (rune::protocol::record::kLease*ValueOffset).
#include <rune/codec/Descriptor.h>
#include <rune/node/WriteState.h>
#include <rune/node/lease/LeaseEntryStore.h>
#include <rune/node/lease/LeaseFoldEvents.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/FoldSkip.h>
#include <rune/view/RecordFold.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <cstdint>
#include <string_view>
#include <vector>

namespace rune::node {

inline constexpr uint64_t    kLeaseIndexPlanFingerprint      = 0x4C45'4153'4549'4458ull;   // "LEASEIDX"
inline constexpr std::size_t kLeaseIndexChunkBytes           = 64u << 10;
inline constexpr int64_t     kLeaseIndexReplayRecordsPerPass = 65536;

/// Row slots: 0..3 are the system columns (version, ttl, holder, remaining), then the user columns; the last two slots
/// are the offset columns.
inline constexpr uint16_t kLeaseVersionSlot   = 0;
inline constexpr uint16_t kLeaseTtlSlot       = 1;
inline constexpr uint16_t kLeaseHolderSlot    = 2;
inline constexpr uint16_t kLeaseRemainingSlot = 3;
inline constexpr uint16_t kLeaseSystemSlots   = 4;
inline constexpr uint16_t kLeaseOffsetSlots   = 2;   // lease_granted_offset, lease_updated_offset: the last two slots

/// Per-row auxiliary bytes of the lease table (ViewTableKeys::auxBytesPerRow): `lastAppended`, the offset of the last
/// update or revoke of the key that the produce path has appended (spec 6.4, R8), -1 = none.  It stays here; the entry store
/// (Task 9) does not hold it.
inline constexpr uint32_t kLeaseAuxBytes = 8;

/// Per-row aux bytes of a TRANSIENT lease topic's table (spec A.3): the anchor list.  Layout:
///   [0,8)   lastAppended (unchanged meaning)      [8,16)  anchor offset, i64 (-1 = not linked)
///   [16,20) anchor bytes, u32                     [20,24) prev row id, u32 (kLeaseAnchorNil = none)
///   [24,28) next row id, u32 (kLeaseAnchorNil)    [28,32) zero padding
/// A row's ANCHOR is the offset of the newest log record that holds its full state (a grant, an update, a Carry or a replay-created
/// checkpoint); the list is ordered by anchor, oldest at the head, so the head anchor is the oldest offset the ring must keep.
inline constexpr uint32_t kLeaseTransientAuxBytes = 32;
/// kLeaseAnchorNil (LeaseFoldEvents.h) is the nil row id of the anchor list: the lease entry store's nil.
static_assert(kLeaseAnchorNil == lease::kLeaseNil, "the anchor list and the lease entry store share one nil row id");
/// The anchor bytes of one record: kLeaseAnchorRecordOverhead + key size + value size, an ESTIMATE of the record's ring bytes
/// (the batch and record framing, rounded).  The ring's real accounting is Task 30's; this only has to be monotone and stable.
inline constexpr uint32_t kLeaseAnchorRecordOverhead = 24;

inline constexpr std::string_view kLeaseGrantedOffsetColumn = "lease_granted_offset";
inline constexpr std::string_view kLeaseUpdatedOffsetColumn = "lease_updated_offset";

/// The row's descriptor: every field of the EVOLVING descriptor (same ids, names, types, widths; dropped fields stay)
/// plus the two Int64 offset columns (ids max+1, max+2).  Cold.
[[nodiscard]] rune::codec::Descriptor leaseIndexOutDescriptor(const rune::codec::Descriptor& evolving);
/// buildRowLayout(leaseIndexOutDescriptor(evolving)).  Throws only on a var-len field (a lease topic has none).  Cold.
[[nodiscard]] rune::view::RowLayout leaseIndexRowLayout(const rune::codec::Descriptor& evolving);
/// The bytes one ScanLeases row of this lease topic takes on the wire: scanLeasesRowBytes(keyWidth, the index layout's rowLen).  The DDL
/// refuses a lease topic whose row exceeds kScanLeasesMaxPageBytes.  `evolving` holds the system columns.  Cold.
[[nodiscard]] std::size_t leaseScanRowBytes(const rune::codec::Descriptor& evolving, std::size_t keyWidth);
/// The lease view's output fingerprint (spec 7.1): mix(layoutHash, kLeaseIndexPlanFingerprint), where layoutHash is the wyhash (seed 0) over,
/// for every slot of `layout` in slot order, [field id u32 LE][column type u8][width u32 LE][offset u32 LE].  `outDesc` is
/// leaseIndexOutDescriptor(evolving) (slot i = outDesc.fields[i]).  LAYOUT ONLY: no field names, no sinceVersion, no schema name, so a
/// rename or a FilterSchema/version install that leaves the layout alone keeps it, and an AddField changes it.  Cold.
[[nodiscard]] uint64_t leaseViewLayoutFingerprint(const rune::codec::Descriptor& outDesc, const rune::view::RowLayout& layout);
/// leaseViewLayoutFingerprint over the layout of `evolving` (the index's own: leaseIndexOutDescriptor + leaseIndexRowLayout).  Cold.
[[nodiscard]] uint64_t leaseViewFingerprintOf(const rune::codec::Descriptor& evolving);
/// keyIndexPlan's twin: name __lease_index, no view topic, 64 KiB chunks.  Cold.
[[nodiscard]] rune::view::CompiledViewPlan leaseIndexPlan(int32_t srcTopicId, uint16_t keyFieldId);

/// The writer descriptor of a lease record, from the TOPIC'S SPEC: the materialized descriptor the produce hook validated the
/// record against (system columns at ids 0..3, the evolving field ids).  The lease index's own copy of the evolving descriptor
/// is never the source: it lags the spec after a Drop, and when SchemaUpdate and FilterSchema arrive in either order.  nullptr
/// while that version is not installed on this worker (the caller holds, see PrepareStatus::Pending).
[[nodiscard]] inline const rune::codec::Descriptor* leaseWriterOf(const TopicWriteSpec& spec, std::string_view value) noexcept {
    if (value.size() < 4) return nullptr;
    return spec.descriptorFor(static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8)));
}

/// Projects a record value into a row image: each out slot whose field id is live in `writer` gets the field's bytes
/// (a raw copy of the value's fixed block at `4 + writer offset`, no FieldValue decode) and a clear null bit; every other
/// slot is null.  The offset columns stay zero.  `outDesc` is the out descriptor or the evolving one: only its first
/// layout.slotType.size() - kLeaseOffsetSlots fields are read.  Malformed: the value is shorter than the writer's block,
/// or a field's type / width differs between writer and layout (evolution never changes a type: AlterOpType is
/// Add | Drop | Rename | SetCompatibility).
/// `slotOf` (nullptr: scan the layout per field, O(fields^2)): the writer's slot map from leaseSlotMapFor, which already proved
/// every writer field covered and type-compatible; the projection is then O(fields) and byte-for-byte the same.
[[nodiscard]] rune::view::PrepareStatus projectLeaseRow(uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                                                        const rune::codec::Descriptor& writer, std::string_view value, const uint16_t* slotOf = nullptr) noexcept;

/// Cold.  Builds `slotOf` (slotOf[k] = the layout slot of writer.fields[k]).  Ok; Pending: a writer field id the layout lacks (the
/// layout is older than the writer's schema); Malformed: a field's type / width differs from its slot, or it lies outside the
/// writer's block.  `slotOf` is untouched unless Ok.
[[nodiscard]] rune::view::PrepareStatus buildLeaseSlotMap(const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                                                          const rune::codec::Descriptor& writer, std::vector<uint16_t>& slotOf);
/// The writer's slot map from the cache, filling the entry (cold, once per `installSeq`) when it is missing or stale.  nullptr when
/// `installSeq` is 0, the version is past the cache, or the map cannot be built (`status` then says Pending or Malformed; Ok otherwise).
[[nodiscard]] const uint16_t* leaseSlotMapFor(LeaseCoverCache& cache, uint16_t version, uint32_t installSeq, const rune::view::RowLayout& layout,
                                              const rune::codec::Descriptor& outDesc, const rune::codec::Descriptor& writer,
                                              rune::view::PrepareStatus* status = nullptr);
/// The spec's install sequence number of the record's writer version (0: not installed).
[[nodiscard]] inline uint32_t leaseInstallSeqOf(const TopicWriteSpec& spec, std::string_view value) noexcept {
    if (value.size() < 4) return 0;
    const SpecVersion* sv = spec.versionFor(static_cast<uint16_t>(static_cast<uint8_t>(value[2]) | (static_cast<uint8_t>(value[3]) << 8)));
    return sv != nullptr ? sv->installSeq : 0;
}

/// The highest INSTALLED writer version of a topic's spec (a refused version has no descriptor; maxVersion() counts it).  The version a
/// lease revoke is re-encoded at, and the version an Expire / Checkpoint record is written at.  descriptor == nullptr: none installed.
struct LeaseWriter { const rune::codec::Descriptor* descriptor = nullptr; uint16_t version = 0; uint32_t installSeq = 0; };
[[nodiscard]] inline LeaseWriter latestLeaseWriter(const TopicWriteSpec& spec) noexcept {
    for (int v = spec.maxVersion(); v >= 0; --v)
        if (const SpecVersion* sv = spec.versionFor(static_cast<uint16_t>(v))) return {&sv->descriptor, static_cast<uint16_t>(v), sv->installSeq};
    return {};
}

struct LeaseRowState {
    bool     present = false;
    int64_t  version = 0;
    uint64_t ttlMs   = 0;   ///< the row's lease_ttl_ms
};

/// The judgment of spec 7.2.  0 = accept; else the verdict code (KeyExists, LeaseNotFound, LeaseLimitExceeded,
/// InvalidRequest, or kVerdictStaleBroker for an Expire / Checkpoint whose lease moved on).  `recordVersion` is the
/// record's lease_version (value bytes [4,12)); a grant ignores it.  `recordRemainingMs` is the record's lease_remaining_ms (bytes [28,36)).
///
/// Exact order, per record kind (the first match answers):
///   Put + Insert:    row present -> KeyExists (56); liveCount >= maxKeys -> LeaseLimitExceeded (63); remaining != 0 -> 39; accept.
///                    The grant's TTL is taken as written (the produce hook clamps it before append).
///   Put + Existing:  row absent or version differs -> LeaseNotFound (62); accept.
///   Put + Upsert(0) = Carry (a broker re-append of a live row, spec A.3): row absent or version differs -> kVerdictStaleBroker; accept
///                    (the row is not modified, only its anchor moves).
///   Revoke+Existing: row absent or version differs -> LeaseNotFound (62); accept.
///   Expire / Checkpoint (intent 0): row absent or version differs -> kVerdictStaleBroker; accept (the broker copies the row).
///   anything else -> InvalidRequest (39).
/// An update or revoke's TTL and remaining are NOT checked (spec 6.4: the judge keeps the row's TTL and remaining; the
/// write hook already refused a client remaining != 0).  Reason: the Task 8 cleaner keeps each live key's grant record, so a
/// replay never builds a row from an update image, and the grant's TTL is the row's TTL on replay as well as live.
[[nodiscard]] uint16_t judgeLease(LeaseRowState row, uint8_t attributes, int64_t recordVersion, uint64_t recordRemainingMs,
                                  uint64_t liveCount, uint32_t maxKeys) noexcept;
/// The record's lease_remaining_ms; 0 for a value too short to carry it.
[[nodiscard]] uint64_t leaseRecordRemaining(std::string_view value) noexcept;

/// The lease_version slot of a lease row image.
[[nodiscard]] int64_t leaseRowVersion(const uint8_t* row, const rune::view::RowLayout& layout) noexcept;
/// The lease_ttl_ms slot of a lease row image.
[[nodiscard]] uint64_t leaseRowTtl(const uint8_t* row, const rune::view::RowLayout& layout) noexcept;
/// The lease_remaining_ms slot of a lease row image (a checkpointed key's remaining time; 0 = none).
[[nodiscard]] uint64_t leaseRowRemaining(const uint8_t* row, const rune::view::RowLayout& layout) noexcept;
/// The lease_holder slot of a lease row image.
[[nodiscard]] uint64_t leaseRowHolder(const uint8_t* row, const rune::view::RowLayout& layout) noexcept;
/// The record's lease_version (value bytes [4,12), little-endian); 0 for a value too short to carry one.
[[nodiscard]] int64_t leaseRecordVersion(std::string_view value) noexcept;
/// A lease record's `lease_ttl_ms` (value bytes [12, 20), little-endian); 0 for a value too short to carry it.
[[nodiscard]] uint64_t leaseRecordTtl(std::string_view value) noexcept;

/// The row id's `lastAppended` (kLeaseAuxBytes of aux; -1 when the table has none).
[[nodiscard]] inline int64_t leaseLastAppended(const rune::view::ViewTable& t, uint32_t rid) noexcept {
    if (t.auxBytesPerRow() < kLeaseAuxBytes) return -1;
    int64_t v;
    std::memcpy(&v, t.aux(rid), sizeof(v));
    return v;
}
inline void setLeaseLastAppended(rune::view::ViewTable& t, uint32_t rid, int64_t v) noexcept {
    if (t.auxBytesPerRow() < kLeaseAuxBytes) return;
    std::memcpy(t.aux(rid), &v, sizeof(v));
}
/// The `lastAppended` an ERASED lease row id carries until a new row reuses it (a fresh row resets it to -1): the lease promotion pass
/// walks row ids and reads this to tell a live row from an erased one.  Never a real offset or -1.
inline constexpr int64_t kLeaseRowErased = INT64_MIN;
[[nodiscard]] inline bool leaseRowLive(const rune::view::ViewTable& t, uint32_t rid) noexcept { return leaseLastAppended(t, rid) != kLeaseRowErased; }

namespace detail {
inline constexpr std::size_t kAnchorOff = 8, kAnchorBytesOff = 16, kAnchorPrevOff = 20, kAnchorNextOff = 24;
[[nodiscard]] inline bool hasAnchorAux(const rune::view::ViewTable& t) noexcept { return t.auxBytesPerRow() >= kLeaseTransientAuxBytes; }
template <typename T> [[nodiscard]] inline T auxGet(const rune::view::ViewTable& t, uint32_t rid, std::size_t off) noexcept {
    T v;
    std::memcpy(&v, t.aux(rid) + off, sizeof(v));
    return v;
}
template <typename T> inline void auxSet(rune::view::ViewTable& t, uint32_t rid, std::size_t off, T v) noexcept { std::memcpy(t.aux(rid) + off, &v, sizeof(v)); }
}  // namespace detail

/// The row's anchor offset; -1 when it is not linked or the table has no anchor aux.
[[nodiscard]] inline int64_t leaseAnchorOf(const rune::view::ViewTable& t, uint32_t rid) noexcept {
    return detail::hasAnchorAux(t) ? detail::auxGet<int64_t>(t, rid, detail::kAnchorOff) : -1;
}
[[nodiscard]] inline uint32_t leaseAnchorBytesOf(const rune::view::ViewTable& t, uint32_t rid) noexcept {
    return detail::hasAnchorAux(t) ? detail::auxGet<uint32_t>(t, rid, detail::kAnchorBytesOff) : 0;
}
/// A fresh row: anchor -1, bytes 0, prev / next nil (a reused row id keeps its old aux bytes).
inline void initLeaseAnchor(rune::view::ViewTable& t, uint32_t rid) noexcept {
    if (!detail::hasAnchorAux(t)) return;
    detail::auxSet<int64_t>(t, rid, detail::kAnchorOff, -1);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorBytesOff, 0);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorPrevOff, kLeaseAnchorNil);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorNextOff, kLeaseAnchorNil);
    detail::auxSet<uint32_t>(t, rid, 28, 0);
}
/// Takes the row out of the list (a no-op when it is not linked).
inline void leaseAnchorUnlink(rune::view::ViewTable& t, LeaseAnchorList& l, uint32_t rid) noexcept {
    if (!detail::hasAnchorAux(t) || detail::auxGet<int64_t>(t, rid, detail::kAnchorOff) < 0) return;
    const uint32_t prev = detail::auxGet<uint32_t>(t, rid, detail::kAnchorPrevOff), next = detail::auxGet<uint32_t>(t, rid, detail::kAnchorNextOff);
    if (prev != kLeaseAnchorNil) detail::auxSet<uint32_t>(t, prev, detail::kAnchorNextOff, next); else l.head = next;
    if (next != kLeaseAnchorNil) detail::auxSet<uint32_t>(t, next, detail::kAnchorPrevOff, prev); else l.tail = prev;
    --l.count;
    l.liveBytes -= detail::auxGet<uint32_t>(t, rid, detail::kAnchorBytesOff);
    detail::auxSet<int64_t>(t, rid, detail::kAnchorOff, -1);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorBytesOff, 0);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorPrevOff, kLeaseAnchorNil);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorNextOff, kLeaseAnchorNil);
}
/// Links (or re-links) the row at the tail with this anchor.  Anchors only grow: live and replay both fold in offset order.
inline void leaseAnchorMoveToTail(rune::view::ViewTable& t, LeaseAnchorList& l, uint32_t rid, int64_t offset, uint32_t bytes) noexcept {
    if (!detail::hasAnchorAux(t)) return;
    leaseAnchorUnlink(t, l, rid);
    assert((l.tail == kLeaseAnchorNil || offset > detail::auxGet<int64_t>(t, l.tail, detail::kAnchorOff)) && "lease anchors only grow");
    detail::auxSet<int64_t>(t, rid, detail::kAnchorOff, offset);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorBytesOff, bytes);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorPrevOff, l.tail);
    detail::auxSet<uint32_t>(t, rid, detail::kAnchorNextOff, kLeaseAnchorNil);
    if (l.tail != kLeaseAnchorNil) detail::auxSet<uint32_t>(t, l.tail, detail::kAnchorNextOff, rid); else l.head = rid;
    l.tail = rid;
    ++l.count;
    l.liveBytes += bytes;
}
/// The oldest anchor (the head's), INT64_MAX when the list is empty.
[[nodiscard]] inline int64_t leaseAnchorHead(const rune::view::ViewTable& t, const LeaseAnchorList& l) noexcept {
    return l.head == kLeaseAnchorNil || !detail::hasAnchorAux(t) ? INT64_MAX : detail::auxGet<int64_t>(t, l.head, detail::kAnchorOff);
}

/// The produce path's non-virtual window on a partition's lease index (WorkerViewSet::leaseIndexOf): the table (rows and the
/// per-row `lastAppended`), its row layout and the descriptor the layout was built from.  Null `table` = no index.
struct LeaseIndexRef {
    rune::view::ViewTable*         table   = nullptr;
    const rune::view::RowLayout*   layout  = nullptr;
    const rune::codec::Descriptor* outDesc = nullptr;
    LeaseCoverCache*               cover   = nullptr;   ///< the instance's slot maps (nullptr: encode scans)
    LeaseTransientState*           transient = nullptr; ///< the instance's anchor list and fence (Task 30 reads it)
};

/// The exact inverse of projectLeaseRow: writes a lease record value for `writer` (version `writerVersion`) from a row image:
/// [u16 blockLength][u16 version], then the writer's fixed block, each field copied from the row slot with the same field id
/// (system columns included).  A field whose slot is null is written as zeros, except an Enum's first whitelisted value (see below;
/// projectLeaseRow reads either back as a present value; the record format has no null).  Returns the bytes written; 0 when `cap` is too small or a writer field has no slot in
/// the layout (cannot happen after a layout bump).  No allocation.
/// A null slot of an Enum field is written as the field's first whitelisted value (a zero would fail the version's Validator;
/// the null bit stays set in the row, the record format has no null), every other null as zeros.
/// `slotOf` (nullptr: scan): the writer's slot map (leaseSlotMapFor).
[[nodiscard]] std::size_t encodeLeaseValue(const uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                                           const rune::codec::Descriptor& writer, uint16_t writerVersion, uint8_t* out, std::size_t cap,
                                           const uint16_t* slotOf = nullptr) noexcept;

/// What the fold needs of the lease index: its table and layout, the descriptor the layout was built from and the wire
/// key width.
struct LeaseIndexFold {
    rune::view::ViewTable&            table;
    const rune::view::RowLayout&      layout;
    const rune::codec::Descriptor&    outDesc;
    uint8_t                           keyWidth = 0;
    /// Optional (nullptr: always scan): the instance's cache of per-version slot maps (proven covered by `layout`).
    LeaseCoverCache*                  cover = nullptr;
    /// Optional (nullptr: no anchors, fence records ignored uncounted): the instance's anchor list and fence.  Anchors are kept only
    /// when `transient->transient`.  LAST, so existing brace inits still compile.
    LeaseTransientState*              transient = nullptr;
};

/// Prepares one record: the key (width checked), whether its row exists, and for a Put or Checkpoint the projected row image
/// in `p.scratch`.  `rec.writer` is the spec's descriptor of the record's version (leaseWriterOf).  An empty value is
/// prepared as an Upsert (the judge answers InvalidRequest).  Malformed: a wrong key width, a value shorter than the system
/// columns, or a projection that does not fit; the prospect then holds no operation and the judge answers InvalidRequest.
/// Pending: the writer descriptor is not installed (null) or has a field id the layout lacks; nothing is judged.  When the
/// writer is null because the version was REFUSED by the spec (`refusedVersion`), the answer is definite: Malformed (the judge
/// answers InvalidRequest and moves on).  A Checkpoint's image is projected only when its row is absent (a compacted-log replay).
/// `installSeq`: the spec's install sequence number of the record's version (leaseInstallSeqOf; 0 = do not use the cover cache).
[[nodiscard]] rune::view::PrepareStatus prepareLeaseIndex(const LeaseIndexFold& f, const rune::view::SourceRecord& rec, rune::view::Prospect& p,
                                                          bool refusedVersion = false, uint32_t installSeq = 0) noexcept;

/// Applies one ACCEPTED record (the prospect `p` was prepared for it).  Fills `ev` and returns true when the record
/// changed a row; false for a record that does not apply (a replay of a log that disagrees with the table).  Every accepted
/// Put and Checkpoint carries the full row (spec 6.4), so an update or checkpoint whose row is ABSENT creates it from the
/// image: after compaction kept only the latest record of a key, a replay sees no grant.  The live judge never reaches that
/// branch (it refuses an update or checkpoint of an absent key).
bool applyLeaseRecord(LeaseIndexFold& f, const rune::view::SourceRecord& rec, const rune::view::Prospect& p, LeaseEvent& ev) noexcept;

/// foldBatch's contract (batch, bound, cursor, corrupt tail) over the accepted records of a lease partition: nothing is
/// judged and no event is pushed.  Writers come from `spec`.  A record whose layout is not there yet (Pending) stops the fold
/// AT that record (cursor = its offset, `*held` set); the caller retries.  `scratch` is the prospect's (2 * rowLen).
rune::view::FoldStats foldLeaseIndexBatch(LeaseIndexFold& f, const TopicWriteSpec& spec, rune::view::Prospect& prospect,
                                          const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor,
                                          const rune::view::FoldSkip& skip, bool* held = nullptr, uint16_t* heldVersion = nullptr) noexcept;

}  // namespace rune::node
