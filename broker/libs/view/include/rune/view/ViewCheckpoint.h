// libs/view/include/rune/view/ViewCheckpoint.h
#pragma once
#include <cstdint>
#include <cstddef>
#include <span>
#include <vector>
#include <optional>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Task 12: differential-checkpoint codec for a materialized ViewTable.
//
// A checkpoint is a sequence of wire records: exactly one BASE record (every
// live row at some snapshot offset O) followed by zero or more DELTA records
// (only the rows that changed since the previous record, at increasing O).
// Both shapes use the identical wire layout below -- a base is simply a delta
// whose entries happen to cover every currently-live row instead of only the
// rows dirtied since the last flush.
//
// The checkpoint keys on the uint64 MERGE key (the wire/compaction key --
// Task 10 ruling), which ViewTable::drainDirty() (Task 6) already emits as
// `key` for both upserts and deletes.
//
// Wire format (little-endian, all integers):
//   [magic u32][O i64][rowLen u32][count u32]
//   count x {
//     rid u32, flags u8 (0=upsert,1=delete), key u64, [rowLen bytes iff upsert]
//   }
//   [crc32c u32]   -- rune::protocol::crc32c() over every byte from `magic`
//                     through the last entry (i.e. everything BEFORE this
//                     trailing crc field itself).
//
// Bounds-safety (Task 8 lesson, reapplied here): applyDelta() NEVER trusts
// the wire `count` field to size an allocation or a blind OOB read. It walks
// the entries one at a time, checking remaining-bytes at every step, and
// rejects (returns std::nullopt) the instant the buffer can't back the next
// field/row -- a bogus huge `count` on a short buffer fails on its very first
// iteration, not after scanning or allocating anything count-sized.
//
// Offset-atomicity (Review-focus #4 / S9.2-S9.3): replay() returns EXACTLY
// the highest successfully-applied record's O, never adjusted by +/-1.
// Upserts are "Replace" writes (table.put(key) + memcpy the full row), so
// replaying the same delta twice, or replaying a base followed by deltas
// that re-touch some of the base's keys, reproduces the live table
// byte-for-byte -- idempotent by construction.

inline constexpr uint32_t kViewCheckpointMagic = 0x564B4331u; // "1CKV" little-endian ASCII

// One dirty-row entry destined for encodeDelta(). Shape mirrors
// ViewTable::drainDirty()'s callback arguments 1:1 ({rid, row, deleted, key}
// there; same fields here, `row` nullptr for a delete). drainDirty()'s `row`
// pointer is only valid during its callback, so callers must copy it into a
// stable buffer before building a DirtyEntry that outlives the callback.
struct DirtyEntry {
    uint32_t rid;
    bool deleted;
    uint64_t key;
    const uint8_t* row; // nullptr iff deleted; otherwise exactly rowLen bytes, valid for the encodeDelta() call
};

// {O, count} recovered from one successfully-applied delta/base record.
struct AppliedDelta {
    int64_t O;
    uint32_t count;
};

// Appends one delta (or base) wire record to `out` and returns the number of
// bytes appended (out may already contain prior records -- this only ever
// appends, never truncates/rewrites existing bytes). A base record is built
// the same way, passing every currently-live row as upsert entries.
size_t encodeDelta(std::span<const DirtyEntry> entries, int64_t O, uint32_t rowLen,
                    std::vector<uint8_t>& out);

// Validates `deltaBytes` (magic, declared rowLen vs. `layout.rowLen`, bounds
// of every entry against the actual buffer size, then the trailing CRC-32C)
// BEFORE touching `table` at all -- a malformed/corrupt/truncated record
// never partially mutates the table. Once validated, applies every entry in
// order: `deleted` -> `table.erase(key)`; otherwise `memcpy(table.put(key),
// row, rowLen)`. Returns the record's {O, count} on success; std::nullopt on
// CRC failure, truncation, or a malformed/bogus count -- never undefined
// behavior, never an allocation sized from the unvalidated wire `count`.
std::optional<AppliedDelta> applyDelta(ViewTable& table, const RowLayout& layout,
                                        std::span<const uint8_t> deltaBytes);

// Recovery entry point: applies `base` (if non-empty) then each of
// `deltasInOrder`, in order, to `table`. Returns the HIGHEST successfully
// applied record's O -- exactly that delta's O (never +/-1) -- or -1 if
// nothing applied successfully (e.g. an empty/corrupt base and no deltas).
// Stops at the first record that fails to apply (a truncated tail is the
// only way a real recovery log can be corrupt, since writes are append-only
// and fsync'd in order) rather than skipping a bad record and continuing,
// so a partially-written trailing record can never silently apply a stale
// subset of its entries or let a later, unrelated-offset record apply on top
// of an unknown base state.
int64_t replay(ViewTable& table, const RowLayout& layout, std::span<const uint8_t> base,
               std::span<const std::span<const uint8_t>> deltasInOrder);

} // namespace rune::view
