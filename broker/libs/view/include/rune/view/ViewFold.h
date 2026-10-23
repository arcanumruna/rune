// libs/view/include/rune/view/ViewFold.h
//
// Task 10: the single-source fold -- the heart of the Rune View Engine.
#pragma once
#include <cstdint>
#include <unordered_map>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/LogReadView.h>
#include <rune/view/ViewPlan.h>
#include <rune/view/ViewRow.h>
#include <rune/view/ViewTable.h>

namespace rune::view {

// Per-writer-version materialized Descriptor cache for the fold hot path. A
// writer-version change is rare (a schema ALTER); the common case is a long
// run of records all written against the SAME version, so lookups must be
// steady-state-alloc-free. Only the FIRST sight of a given writerVersion
// pays for materializeAsOf() + an unordered_map node allocation; every later
// lookup for that version is a hash + compare, no allocation.
class VersionDescriptorCache {
public:
    explicit VersionDescriptorCache(rune::codec::Descriptor evolving)
        : evolving_(std::move(evolving)) {}

    const rune::codec::Descriptor& materializedFor(uint16_t writerVersion) {
        auto it = cache_.find(writerVersion);
        if (it != cache_.end()) {
            return it->second;
        }
        auto [ins, inserted] = cache_.emplace(
            writerVersion, rune::codec::materializeAsOf(evolving_, writerVersion));
        (void)inserted;
        return ins->second;
    }

    // Fallback writer version used only when a non-tombstone record's value
    // is too short to carry a [blockLength][version] header (defensive path;
    // never hit by a well-formed codec record). Defaults to the evolving
    // descriptor's own (latest) declared version.
    uint16_t defaultVersion() const noexcept { return evolving_.version; }

private:
    rune::codec::Descriptor evolving_;
    std::unordered_map<uint16_t, rune::codec::Descriptor> cache_;
};

// Issue #537: the source partition's aborted-transaction predicate, handed to
// every fold.  A view folds only COMMITTED data: the caller's `bound` (the
// partition's LSO) keeps every open transaction out, and this predicate keeps
// out every batch of an ABORTED one — the same rule the read_committed fetch
// and the log cleaner apply (AbortedTxnIndex::isAborted(producerId, baseOffset)
// over the owning worker's RcState).  A plain function pointer + context (no
// std::function, no virtual): called once per TRANSACTIONAL batch only — a
// non-transactional batch costs one attribute-bit test.  `fn == nullptr` means
// the source holds no aborted transaction (kNoAbortedTxns: plain-data unit
// tests and benches; the worker always passes its partition's index).
struct AbortedTxnFilter {
    using Fn = bool (*)(const void* ctx, int64_t producerId, int64_t baseOffset) noexcept;
    Fn          fn  = nullptr;
    const void* ctx = nullptr;

    // True when `r` is a data batch of an aborted transaction.
    [[nodiscard]] bool skips(const rune::protocol::RecordBatchReader& r) const noexcept {
        if ((r.attributes() & rune::protocol::batch::kAttrTransactional) == 0) [[likely]] return false;
        const int64_t pid = r.producerId();
        return pid >= 0 && fn != nullptr && fn(ctx, pid, r.baseOffset());
    }
};
inline constexpr AbortedTxnFilter kNoAbortedTxns{};

struct FoldStats {
    uint64_t applied          = 0;
    uint64_t deleted          = 0;
    uint64_t skippedControl   = 0;
    // #537: data batches of aborted transactions skipped whole (never applied).
    uint64_t skippedAborted   = 0;
    // A record whose WIRE key is not exactly the view key column's byte
    // width (e.g. a truncated/corrupt producer-side key). Never silently
    // zero-padded -- counted and skipped instead, so a malformed key is
    // observable rather than silently mis-keying a row.
    uint64_t skippedMalformed = 0;
    // Phase 2a aggregate views: a live record whose group-by value is absent
    // (its old contribution, if any, is retracted; nothing is added).
    uint64_t skippedNoGroup   = 0;
    // Output values written NULL because a Sum did not fit its type (never
    // wrapped). Counted per emit, not per overflow event.
    uint64_t aggOverflow      = 0;
    uint64_t aggNonFinite     = 0;  // a NaN/+-Inf Float aggregate input was treated as NULL (counted per Float input)
    // A retraction found missing/zero state (group row absent or a count already
    // 0): counted and skipped with no mutation, never masked.
    uint64_t aggInconsistent  = 0;
    // Phase 2b: Min/Max caches rebuilt by one scan of the group's value block
    // (the cached extreme was retracted) -- the known O(group size) path.
    uint64_t aggRescans       = 0;
};

// Folds every COMMITTED record (offset < bound) in `view` -- a LogReadView
// over whole batches -- into `table`, per `plan`/`layout`. Control batches
// are skipped (counted in skippedControl), and so is every data batch of an
// aborted transaction (`aborted`, #537; counted in skippedAborted). Merge-key model: the row's
// identity (table `put`/`erase` key) is the record's WIRE/compaction key --
// the same bytes the log's own compaction (OffsetMap) already keys "one
// current row per key" on -- NEVER the value's key-field. The value's
// key-field is a plain display/projection output column, decoded and
// written like any other CompiledField on upsert, and is NOT consulted for
// merge identity. A live (non-tombstone) record is decoded against the
// writer's OWN schema version (never the latest) and upserted (Phase 1: all
// CompiledFields are Replace); a tombstone (empty value) erases its wire key.
// `cursor` is IN/OUT: on entry, records with offset < cursor are skipped
// (already folded -- `view` is whole batches, so a pass that stopped at a
// mid-batch `bound` resumes inside that batch); on exit it is the offset the
// caller should resume from next time: the highest offset actually folded,
// capped at `bound` -- NEVER past a batch this call did not fully fold (see
// ViewFold.cpp for the early-break case). foldAggregateBatch: same contract.
FoldStats foldBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                     VersionDescriptorCache& vdc, ViewTable& table,
                     const rune::storage::LogReadView& view, int64_t bound,
                     int64_t& cursor, const AbortedTxnFilter& aborted);

} // namespace rune::view
