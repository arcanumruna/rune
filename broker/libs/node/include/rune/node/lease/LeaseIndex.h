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
#include <rune/node/lease/LeaseFoldEvents.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/FoldSkip.h>
#include <rune/view/RecordFold.h>
#include <rune/view/ViewFold.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

#include <cstddef>
#include <cstdint>
#include <string_view>

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

inline constexpr std::string_view kLeaseGrantedOffsetColumn = "lease_granted_offset";
inline constexpr std::string_view kLeaseUpdatedOffsetColumn = "lease_updated_offset";

/// The row's descriptor: every field of the EVOLVING descriptor (same ids, names, types, widths; dropped fields stay)
/// plus the two Int64 offset columns (ids max+1, max+2).  Cold.
[[nodiscard]] rune::codec::Descriptor leaseIndexOutDescriptor(const rune::codec::Descriptor& evolving);
/// buildRowLayout(leaseIndexOutDescriptor(evolving)).  Throws only on a var-len field (a lease topic has none).  Cold.
[[nodiscard]] rune::view::RowLayout leaseIndexRowLayout(const rune::codec::Descriptor& evolving);
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
[[nodiscard]] rune::view::PrepareStatus projectLeaseRow(uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                                                        const rune::codec::Descriptor& writer, std::string_view value) noexcept;

struct LeaseRowState {
    bool    present = false;
    int64_t version = 0;
};

/// The judgment of spec 7.2.  0 = accept; else the verdict code (KeyExists, LeaseNotFound, LeaseLimitExceeded,
/// InvalidRequest, or kVerdictStaleBroker for an Expire / Checkpoint whose lease moved on).  `recordVersion` is the
/// record's lease_version (value bytes [4,12)); a grant ignores it.
[[nodiscard]] uint16_t judgeLease(LeaseRowState row, uint8_t attributes, int64_t recordVersion, uint64_t liveCount, uint32_t maxKeys) noexcept;

/// The lease_version slot of a lease row image.
[[nodiscard]] int64_t leaseRowVersion(const uint8_t* row, const rune::view::RowLayout& layout) noexcept;
/// The record's lease_version (value bytes [4,12), little-endian); 0 for a value too short to carry one.
[[nodiscard]] int64_t leaseRecordVersion(std::string_view value) noexcept;

/// What the fold needs of the lease index: its table and layout, the descriptor the layout was built from and the wire
/// key width.
struct LeaseIndexFold {
    rune::view::ViewTable&            table;
    const rune::view::RowLayout&      layout;
    const rune::codec::Descriptor&    outDesc;
    uint8_t                           keyWidth = 0;
};

/// Prepares one record: the key (width checked), whether its row exists, and for a Put or Checkpoint the projected row image
/// in `p.scratch`.  `rec.writer` is the spec's descriptor of the record's version (leaseWriterOf).  An empty value is
/// prepared as an Upsert (the judge answers InvalidRequest).  Malformed: a wrong key width, a value shorter than the system
/// columns, or a projection that does not fit; the prospect then holds no operation and the judge answers InvalidRequest.
/// Pending: the writer descriptor is not installed (null) or has a field id the layout lacks; nothing is judged.
[[nodiscard]] rune::view::PrepareStatus prepareLeaseIndex(const LeaseIndexFold& f, const rune::view::SourceRecord& rec, rune::view::Prospect& p) noexcept;

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
                                          const rune::view::FoldSkip& skip, bool* held = nullptr) noexcept;

}  // namespace rune::node
