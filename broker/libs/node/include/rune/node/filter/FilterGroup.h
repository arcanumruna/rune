#pragma once
// FilterGroup — Phase 2c: the pure broker-side filter engine for ONE (topic, partition).
//
// Members (filtered subscriptions) are a three-state machine: Behind (catching up on its own cursor, own
// predicate), InSync (served by the group's ONE shared scan: a single read, one index probe per record and
// index field, residual terms on the candidates) and VersionStall (waiting for a record schema version the
// worker does not know yet).  The engine knows nothing about Aeron or the Worker: it reads through a
// FilterSource and writes through a FilterSink (templates, no virtual calls, no std::function), so the unit
// tests and the stress drive it with fakes and the Worker instantiates it with its real read and egress.
//
// Owned by one worker thread (shared-nothing: no locks, no atomics).  Cold: init / add / remove / moveSub.
// Hot: pass() — no allocation once the FilterScratch is reserved (the rebuild buffer grows once, never shrinks).
//
// Cost model: a pass touches only members that have something to do.  Non-InSync members live on an intrusive
// list (step 1 walks it), members that may owe a position note live on another (the sweep walks it), and a
// scan batch visits only the members its index probe hit.  `slotsTouchedForTest()` counts those visits.
#include "rune/node/filter/FilterIndex.h"

#include <rune/codec/FilterPredicate.h>
#include <rune/codec/FilterSchema.h>
#include <rune/codec/FilterWire.h>
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace rune::node::filter {

enum class SendOutcome : uint8_t { Sent, Retry, TooLarge };

/// Number of distinct schema versions remembered per set (per member for the logs and the expired waits, per index
/// field for the absent log).  Eight covers any realistic rolling upgrade; past eight the oldest entry is replaced
/// round-robin, so the worst case is one repeated log line, never an allocation.
inline constexpr std::size_t kVersionSetSize = 8;
struct VersionSet {
    std::array<uint16_t, kVersionSetSize> v{};
    uint8_t n = 0, next = 0;
    [[nodiscard]] bool has(uint16_t x) const noexcept {
        for (uint8_t i = 0; i < n; ++i) if (v[i] == x) return true;
        return false;
    }
    void add(uint16_t x) noexcept {
        if (has(x)) return;
        if (n < kVersionSetSize) { v[n++] = x; return; }
        v[next] = x;
        next = static_cast<uint8_t>((next + 1) % kVersionSetSize);
    }
};

struct FilterConfig {
    uint32_t maxSubsPerPartition        = 4096;              // RUNE_FILTER_MAX_SUBS_PER_PARTITION
    uint32_t maxIndexValuesPerPartition = 4096;              // RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION
    int64_t  noteIntervalNs             = 50'000'000;        // RUNE_FILTER_NOTE_INTERVAL_MS (50)
    uint64_t noteRecords                = 65'536;            // RUNE_FILTER_NOTE_RECORDS
    int64_t  versionWaitNs              = 5'000'000'000;     // RUNE_FILTER_VERSION_WAIT_MS (5000)
    uint32_t matchScratch               = 65'536;            // RUNE_FILTER_MATCH_SCRATCH
    /// The share of a pass's budget that catch-up (Behind members) may take while the shared scan has work; the
    /// scan gets the rest and runs first.  Half: a live tail and a catching-up member each keep making progress,
    /// and neither can starve the other.  With no InSync member, catch-up takes the whole budget.  0..100.
    uint32_t catchUpSharePercent        = 50;                // RUNE_FILTER_CATCHUP_SHARE_PERCENT
};

inline constexpr std::size_t kMaxIndexFields = 8;

/// Append-only: new counters go at the end.
struct FilterCounters {
    uint64_t zeroCopy = 0, rebuilt = 0, notes = 0, absentExcluded = 0, unknownVersionExcluded = 0,
             nullValueExcluded = 0, malformed = 0, overflowBatches = 0, overCap = 0, fellBehind = 0, rejoined = 0;
    uint64_t sourceRefused = 0;   ///< add() refused: the member's scan source is not the group's (release builds)
};

struct Behind {};
struct InSync {};
struct VersionStall { uint16_t version = 0; int64_t sinceNs = 0; };
using MemberState = std::variant<Behind, InSync, VersionStall>;
struct ScanIdle {};
struct Scanning  { int64_t cursor = 0; };
/// `cursor` is the scan cursor BEFORE the stalled batch (not its base): equal to the base unless offsets have a gap,
/// and it keeps the join test (member cursor == scan cursor) and position notes valid across gaps.
struct ScanStall { int64_t cursor = 0; uint16_t version = 0; int64_t sinceNs = 0; };
using ScanState = std::variant<ScanIdle, Scanning, ScanStall>;

template <class S>
concept FilterSource = requires(S& s, int64_t from, std::size_t cap, int64_t pid, int64_t base) {
    { s.bound() } -> std::same_as<int64_t>;                          // HW (RU) / LSO (RC); < 0 = not servable now
    { s.read(from, cap) } -> std::same_as<std::span<const uint8_t>>; // whole batches from the batch holding `from`; at least one if any
    { s.readCommitted() } -> std::same_as<bool>;
    { s.isAborted(pid, base) } -> std::same_as<bool>;
    { s.skipEvicted(from) } -> std::same_as<int64_t>;                // transient: the ring start when `from` was evicted
};
template <class K>
concept FilterSink = requires(K& k, uint32_t sub, int64_t base, std::span<const uint8_t> b, int64_t c) {
    { k.sendBatch(sub, base, b) } -> std::same_as<SendOutcome>;
    { k.sendNote(sub, c) } -> std::same_as<SendOutcome>;
    { k.cursor(sub) } -> std::same_as<int64_t&>;                     // the subscription's own cursor (WorkerSubscription::nextOffset)
    { k.deliverable(sub) } -> std::same_as<bool>;                    // active, push state Live, egress present
};

/// One per worker; sized cold by reserve(cfg).  Every vector is reserved/sized up front so the record path
/// never allocates; `rebuild` and (for an oversized batch) `records` grow once and never shrink.
struct FilterScratch {
    struct MatchRef { uint32_t member; uint32_t record; };
    std::vector<rune::protocol::RecordSpan> records;
    std::vector<MatchRef> matches;
    std::vector<uint32_t> counts, starts, order, touched, keep;
    std::vector<uint8_t>  rebuild;      // grow-once, never shrinks
    std::size_t           matchCap = 0; // matches never grows past this: a full list is the overflow signal

    void reserve(const FilterConfig& cfg, std::size_t maxRecordsPerRead) {
        records.reserve(maxRecordsPerRead);
        keep.reserve(maxRecordsPerRead);
        matchCap = cfg.matchScratch;
        matches.reserve(matchCap);
        order.assign(matchCap, 0);
        counts.assign(cfg.maxSubsPerPartition, 0);
        starts.assign(cfg.maxSubsPerPartition, 0);
        touched.reserve(cfg.maxSubsPerPartition);
    }
    /// Cold-ish: only a batch with more records than reserved ever reaches this (grows once).
    void pushRecord(const rune::protocol::RecordSpan& r) {
        if (records.size() == records.capacity()) {
            const std::size_t n = std::max<std::size_t>(64, records.capacity() * 2);
            records.reserve(n);
            keep.reserve(n);
        }
        records.push_back(r);
    }
};

class FilterGroup {
public:
    /// Cold.  `sourceId` is opaque: the Worker passes the registry id plus the incarnation of the scan source.
    void init(int32_t topicId, int32_t partition, const FilterConfig& cfg, uint64_t sourceId) {
        topicId_ = topicId; partition_ = partition; cfg_ = cfg; sourceId_ = sourceId;
        nodes_.clear();
        nodes_.resize(kSentinels);
        for (uint32_t s = 0; s < kSentinels; ++s) {
            nodes_[s].stateLink = Link{s, s};
            nodes_[s].noteLink  = Link{s, s};
        }
        freeHead_ = kNil;
        count_ = 0;
        nFields_ = 0;
        for (auto& f : fields_) f = IndexField{};
        inSync_ = 0;
        scan_ = ScanIdle{};
        scanned_ = scannedAtSweep_ = 0;
        lastSweepNs_ = 0;
        loggedUnknownGroup_ = VersionSet{};
        expiredGroup_ = VersionSet{};
        group_ = FilterCounters{};
        slotsTouched_ = 0;
    }

    /// Cold.  The member id, or kNil when a cap is reached (counted overCap, never allocating past it) or the
    /// member's scan source differs from the group's (asserts in debug; release counts sourceRefused).
    [[nodiscard]] uint32_t add(uint32_t sub, rune::codec::CompiledFilter f, const rune::codec::FilterSchema& schema,
                               uint64_t sourceId) {
        assert(sourceId == sourceId_ && "FilterGroup::add: the member's scan source is not the group's");
        return addOrRefuse(sub, std::move(f), schema, sourceId);
    }
    /// add() without the debug assert: the release-build behaviour (refuse and count a foreign scan source),
    /// public so a Debug build can test the refusal.
    [[nodiscard]] uint32_t addOrRefuse(uint32_t sub, rune::codec::CompiledFilter f,
                                       const rune::codec::FilterSchema& schema, uint64_t sourceId) {
        if (sourceId != sourceId_) { ++group_.sourceRefused; return kNil; }
        if (count_ >= cfg_.maxSubsPerPartition) { ++group_.overCap; return kNil; }
        const uint8_t idxTerm = f.indexTerm();
        const auto& term = f.term(idxTerm);
        assert(term.field >= 0 && static_cast<std::size_t>(term.field) < schema.fieldCount());
        (void)schema;
        uint8_t fi = 0;
        while (fi < nFields_ && fields_[fi].schemaField != term.field) ++fi;
        bool reuse = false;
        if (fi == nFields_) {   // a new field: reclaim a slot whose last member left before refusing the 9th
            uint8_t e = 0;
            while (e < nFields_ && fields_[e].members != 0) ++e;
            if (e < nFields_) { fi = e; reuse = true; }
            else if (nFields_ == kMaxIndexFields) { ++group_.overCap; return kNil; }
        }
        const uint32_t node = freeHead_ != kNil ? freeHead_ : static_cast<uint32_t>(nodes_.size());
        const uint32_t id   = node - kSentinels;
        IndexField& F = fields_[fi];
        if (reuse) {   // its index is empty (every registration was removed): re-key, keep the table
            F.schemaField = term.field;
            F.tag         = term.tag;
            F.absent      = 0;
            F.loggedAbsent = VersionSet{};
        } else if (fi == nFields_) {
            F.schemaField = term.field;
            F.tag         = term.tag;
            F.index.init(cfg_.maxIndexValuesPerPartition, cfg_.maxSubsPerPartition);
            ++nFields_;
        }
        const auto keys = f.values(idxTerm);
        std::size_t added = 0;
        bool ok = true;
        for (const auto& k : keys) {
            assert((added == 0 || keys[added - 1] < k) && "an In term's values are canonical: sorted, unique");
            if (!F.index.add(k, id)) { ok = false; break; }
            ++added;
        }
        if (ok && distinctValues_() > cfg_.maxIndexValuesPerPartition) ok = false;
        if (!ok) {
            for (std::size_t j = 0; j < added; ++j) F.index.remove(keys[j], id);
            ++group_.overCap;
            return kNil;
        }
        if (freeHead_ != kNil) {
            freeHead_ = nodes_[node].stateLink.next;
            nodes_[node] = Member{};
        } else {
            nodes_.emplace_back();   // cold; may grow
        }
        Member& m = nodes_[node];
        m.sub = sub;
        m.filter = std::move(f);
        m.indexField = fi;
        ++F.members;
        ++count_;
        pushBack_<&Member::stateLink>(kCatchUp, node);
        return id;
    }

    /// Cold.  Unregisters every index value and frees the slot.  `member` must be live.
    void remove(uint32_t member) noexcept {
        assert(isLive_(member));
        const uint32_t node = member + kSentinels;
        Member& m = nodes_[node];
        IndexField& F = fields_[m.indexField];
        for (const auto& k : m.filter.values(m.filter.indexTerm())) F.index.remove(k, member);
        --F.members;
        if (std::holds_alternative<InSync>(m.state)) { --inSync_; --F.synced; }
        unlink_<&Member::stateLink>(node);
        unlink_<&Member::noteLink>(node);
        m = Member{};
        m.stateLink.next = freeHead_;
        freeHead_ = node;
        --count_;
        if (inSync_ == 0) scan_ = ScanIdle{};
    }

    void moveSub(uint32_t member, uint32_t newSub) noexcept { at_(member).sub = newSub; }

    [[nodiscard]] bool empty() const noexcept { return count_ == 0; }

    /// A live member's sub (the Worker builds the source from it); kNil when empty.
    [[nodiscard]] uint32_t anySub() const noexcept {
        uint32_t n = nodes_[kCatchUp].stateLink.next;
        if (n == kCatchUp) n = nodes_[kSynced].stateLink.next;
        return n < kSentinels ? kNil : nodes_[n].sub;
    }

    /// One push pass.  `budget` is the worker's remaining push-pass byte budget (read + sent bytes are charged).
    /// `freshBudget` is computed once per cycle by the Worker: true when nothing was charged to `budget` yet.  The
    /// first batch of a fresh pass may exceed the budget (progress); the live budget value is never consulted for
    /// that, because the filtered and unfiltered passes share one budget.
    template <FilterSource Src, FilterSink Snk>
    int pass(Src& src, Snk& sink, const rune::codec::FilterSchema& schema, FilterScratch& scratch, int64_t nowNs,
             std::size_t& budget, bool freshBudget) {
        const int64_t bound = src.bound();
        if (bound < 0) return 0;   // not servable this pass (RC not Ready, not owned)
        assert(scratch.counts.size() >= cfg_.maxSubsPerPartition && "FilterScratch::reserve(cfg) first");
        const std::size_t entry = budget;
        const bool scanActive = inSync_ > 0;
        const bool hasCatchUp = nodes_[kCatchUp].stateLink.next != kCatchUp;
        // The scan (the live tail of every InSync member) runs FIRST and is never starved by catch-up; catch-up gets
        // at most catchUpSharePercent of the budget while the scan has work, and all of it when there is no scan.
        const std::size_t pct = std::min<std::size_t>(cfg_.catchUpSharePercent, 100);
        const std::size_t share = (scanActive && hasCatchUp) ? entry / 100 * pct + entry % 100 * pct / 100 : 0;
        int work = 0;
        // 1. The shared scan: one read, one index probe per record and index field, residual terms on candidates.
        std::size_t scanBudget = entry - share, scanSpent = 0;
        if (scanActive && scanBudget > 0) {
            const std::size_t b0 = scanBudget;
            Cx<Src, Snk> cx{src, sink, schema, scratch, nowNs, bound, scanBudget, freshBudget};
            work += scanStep_(cx);
            scanSpent = b0 - scanBudget;
        }
        // 2. Catch-up: only the non-InSync members, each on its own cursor with its own predicate.  It has its own
        //    fresh-pass allowance, so a scan that took the first oversize batch cannot block a catch-up forever.
        std::size_t catchBudget = scanActive ? share : entry, catchSpent = 0;
        if (hasCatchUp && catchBudget > 0) {
            const std::size_t b0 = catchBudget;
            Cx<Src, Snk> cx{src, sink, schema, scratch, nowNs, bound, catchBudget, freshBudget};
            for (uint32_t n = nodes_[kCatchUp].stateLink.next; n != kCatchUp && catchBudget > 0;) {
                const uint32_t nxt = nodes_[n].stateLink.next;   // stepBehind_ may move n to the synced list
                work += stepBehind_(n - kSentinels, cx);
                n = nxt;
            }
            catchSpent = b0 - catchBudget;
        }
        budget = entry - std::min(entry, scanSpent + catchSpent);
        // 3. Position notes: a sweep at most every noteIntervalNs / 5, or as soon as noteRecords were scanned.
        if (nowNs - lastSweepNs_ >= cfg_.noteIntervalNs / 5 || scanned_ - scannedAtSweep_ >= cfg_.noteRecords)
            work += notes_(sink, nowNs);
        return work;
    }

    /// An InSync member's cursor is the scan cursor (its own cursor field is written back on every transition out).
    [[nodiscard]] int64_t cursorOf(uint32_t member, int64_t ownCursor) const noexcept {
        if (!std::holds_alternative<InSync>(at_(member).state)) return ownCursor;
        if (const auto* s = std::get_if<Scanning>(&scan_)) return s->cursor;
        if (const auto* s = std::get_if<ScanStall>(&scan_)) return s->cursor;
        return ownCursor;
    }
    [[nodiscard]] const ScanState&   scanState() const noexcept { return scan_; }
    [[nodiscard]] const MemberState& stateOf(uint32_t member) const noexcept { return at_(member).state; }
    /// The member's counters with the group-level absent counts of its index field folded in.
    [[nodiscard]] FilterCounters countersOf(uint32_t member) const noexcept {
        const Member& m = at_(member);
        FilterCounters c = m.counters;
        if (std::holds_alternative<InSync>(m.state)) c.absentExcluded += fields_[m.indexField].absent - m.absentAtJoin;
        return c;
    }
    [[nodiscard]] const FilterCounters& groupCounters() const noexcept { return group_; }
    [[nodiscard]] uint32_t memberCount() const noexcept { return count_; }
    [[nodiscard]] uint32_t inSyncCount() const noexcept { return inSync_; }
    [[nodiscard]] uint64_t scannedRecords() const noexcept { return scanned_; }
    /// Member slots visited by passes so far (catch-up steps, scan hits and deliveries, note sweeps).
    [[nodiscard]] uint64_t slotsTouchedForTest() const noexcept { return slotsTouched_; }

private:
    static constexpr int64_t  kNoTime    = std::numeric_limits<int64_t>::min();
    static constexpr uint32_t kSentinels = 4;
    static constexpr uint32_t kCatchUp = 0, kSynced = 1, kDormant = 2, kPending = 3;   // list sentinels (node indices)

    struct Link { uint32_t prev = kNil, next = kNil; };
    struct Member {
        uint32_t sub = kNil;
        rune::codec::CompiledFilter filter;
        MemberState state = Behind{};
        uint8_t  indexField = 0;           // fields_[indexField] holds its registrations
        int64_t  coveredAt = 0;            // InSync: scanned_ when the client's position last covered the scan cursor
        uint64_t pendingSkipped = 0;       // Behind: records evaluated and not delivered since the last frame / note
        uint64_t absentAtJoin = 0;         // fields_[indexField].absent when it joined (folded on leave)
        int64_t  lastFrameNs = kNoTime;
        VersionSet loggedUnknown;          // unknown-version first log per version (FD6)
        VersionSet loggedAbsent;           // absent-field first log per version (spec §8)
        VersionSet expired;                // versions whose wait ran out: excluded without waiting again
        FilterCounters counters;
        Link stateLink;                    // catch-up list (non-InSync) or synced list (InSync); a free slot's chain
        Link noteLink;                     // dormant (InSync, nothing skipped) or pending (may owe a note) list
    };
    struct IndexField {
        int32_t  schemaField = -1;
        uint16_t tag = 0;
        FilterIndex index;
        uint32_t members = 0;              // registered members
        uint32_t synced = 0;               // of those, InSync (a field with none is not probed)
        uint64_t absent = 0;
        VersionSet loggedAbsent;           // hint: versions whose members were all logged (reset when a member joins)
    };
    template <class Src, class Snk>
    struct Cx {
        Src& src; Snk& sink; const rune::codec::FilterSchema& schema; FilterScratch& sc;
        int64_t now; int64_t bound; std::size_t& budget; bool firstOk;
    };
    struct BatchHdr {
        const uint8_t* p = nullptr;
        std::size_t size = 0;
        int64_t base = 0, end = 0, lastOffset = 0;
        int32_t count = 0;
        int16_t attrs = 0;
    };
    enum class Parse : uint8_t { Ok, Partial };

    // ---- slots and intrusive lists (node index = member id + kSentinels; 0..3 are the list sentinels) ----
    [[nodiscard]] Member&       at_(uint32_t id) noexcept { return nodes_[id + kSentinels]; }
    [[nodiscard]] const Member& at_(uint32_t id) const noexcept { return nodes_[id + kSentinels]; }
    [[nodiscard]] bool isLive_(uint32_t id) const noexcept {   // debug check: a free slot is on the free chain
        const uint32_t node = id + kSentinels;
        if (node >= nodes_.size()) return false;
        for (uint32_t f = freeHead_; f != kNil; f = nodes_[f].stateLink.next) if (f == node) return false;
        return true;
    }
    template <Link Member::*L> void pushBack_(uint32_t list, uint32_t n) noexcept {   // append before sentinel `list`
        Link& ln = nodes_[n].*L;
        Link& sl = nodes_[list].*L;
        ln.prev = sl.prev;
        ln.next = list;
        (nodes_[sl.prev].*L).next = n;
        sl.prev = n;
    }
    template <Link Member::*L> void unlink_(uint32_t n) noexcept {   // idempotent: an unlinked node has next == kNil
        Link& ln = nodes_[n].*L;
        if (ln.next == kNil) return;
        (nodes_[ln.prev].*L).next = ln.next;
        (nodes_[ln.next].*L).prev = ln.prev;
        ln = Link{};
    }
    template <Link Member::*L> void spliceAll_(uint32_t from, uint32_t to) noexcept {   // O(1): append all of `from` to `to`
        Link& f = nodes_[from].*L;
        if (f.next == from) return;
        Link& t = nodes_[to].*L;
        const uint32_t first = f.next, last = f.prev;
        (nodes_[t.prev].*L).next = first;
        (nodes_[first].*L).prev = t.prev;
        (nodes_[last].*L).next = to;
        t.prev = last;
        f.next = f.prev = from;
    }
    void ensurePending_(uint32_t node) noexcept {
        if (nodes_[node].noteLink.next == kNil) pushBack_<&Member::noteLink>(kPending, node);
    }
    [[nodiscard]] uint32_t distinctValues_() const noexcept {
        uint32_t n = 0;
        for (std::size_t i = 0; i < nFields_; ++i) n += fields_[i].index.distinctValues();
        return n;
    }

    // ---- batch parsing ----
    static Parse parseBatch_(std::span<const uint8_t> view, std::size_t pos, BatchHdr& h) noexcept {
        using namespace rune::protocol::batch;
        if (pos + static_cast<std::size_t>(kRecordBatchOverhead) > view.size()) return Parse::Partial;
        rune::protocol::RecordBatchReader r(view.data() + pos, view.size() - pos);
        const int32_t total = r.totalLength();
        if (total < kRecordBatchOverhead || pos + static_cast<std::size_t>(total) > view.size()) return Parse::Partial;
        h.p     = view.data() + pos;
        h.size  = static_cast<std::size_t>(total);
        h.base  = r.baseOffset();
        h.count = r.recordCount();
        h.attrs = r.attributes();
        const int64_t last = h.base + std::max<int32_t>(r.lastOffsetDelta(), 0);
        h.lastOffset = last;
        h.end        = last + 1;
        return Parse::Ok;
    }
    /// Control, empty/corrupt and (read_committed) aborted batches are never delivered; the cursor moves past them.
    template <class Src, class Snk>
    static bool isSkipped_(Cx<Src, Snk>& cx, const BatchHdr& h) noexcept {
        using namespace rune::protocol::batch;
        if (h.count < 1) return true;
        if ((h.attrs & kAttrControl) != 0) return true;
        if ((h.attrs & kAttrTransactional) != 0 && cx.src.readCommitted()) {
            const int64_t pid = rune::protocol::RecordBatchReader(h.p, h.size).producerId();
            if (cx.src.isAborted(pid, h.base)) return true;
        }
        return false;
    }
    /// F5: never past the budget, except the first batch of a fresh pass (progress).  Charges the read.
    template <class Src, class Snk>
    static bool admit_(Cx<Src, Snk>& cx, std::size_t size) noexcept {
        if (size > cx.budget && !cx.firstOk) return false;
        cx.firstOk = false;
        charge_(cx.budget, size);
        return true;
    }
    static void charge_(std::size_t& budget, std::size_t n) noexcept { budget -= std::min(budget, n); }

    static bool collect_(FilterScratch& sc, std::span<const uint8_t> bytes, const rune::codec::FilterSchema& schema,
                         const VersionSet& expired, uint16_t& unknownVer, bool& hasUnknown) {
        sc.records.clear();
        hasUnknown = false;
        return rune::protocol::forEachRecordSpan(bytes.data(), bytes.size(), [&](const rune::protocol::RecordSpan& r) {
            if (!r.nullValue && r.value.size() >= 4) {
                const uint16_t v = rune::codec::recordVersion(reinterpret_cast<const uint8_t*>(r.value.data()));
                if (!hasUnknown && !schema.knowsVersion(v) && !expired.has(v)) { hasUnknown = true; unknownVer = v; }
            }
            sc.pushRecord(r);
        });
    }
    [[nodiscard]] static std::span<const uint8_t> valueOf_(const rune::protocol::RecordSpan& r) noexcept {
        return {reinterpret_cast<const uint8_t*>(r.value.data()), r.value.size()};
    }

    // ---- delivery ----
    template <class Src, class Snk>
    SendOutcome send_(Cx<Src, Snk>& cx, Member& m, const BatchHdr& h, std::span<const uint8_t> bytes,
                      std::span<const uint32_t> keep) {
        FilterScratch& sc = cx.sc;
        SendOutcome out;
        std::size_t frameBytes;
        bool whole;
        if (keep.size() == sc.records.size()) {
            out = cx.sink.sendBatch(m.sub, h.base, bytes);
            frameBytes = bytes.size();
            whole = true;
        } else {
            if (sc.rebuild.size() < bytes.size()) sc.rebuild.resize(bytes.size());   // grow-once
            const std::size_t n = rune::protocol::rebuildBatch(bytes, sc.records, keep, sc.rebuild);
            assert(n > 0 && "rebuild scratch is as large as the source batch");
            if (n == 0) return SendOutcome::Retry;
            out = cx.sink.sendBatch(m.sub, h.base, std::span<const uint8_t>(sc.rebuild.data(), n));
            frameBytes = n;
            whole = false;
        }
        if (out == SendOutcome::Sent) {
            charge_(cx.budget, frameBytes);
            if (whole) { ++m.counters.zeroCopy; ++group_.zeroCopy; } else { ++m.counters.rebuilt; ++group_.rebuilt; }
        }
        return out;
    }

    // ---- transitions ----
    void join_(uint32_t id, int64_t nowNs) noexcept {
        const uint32_t node = id + kSentinels;
        Member& m = nodes_[node];
        IndexField& F = fields_[m.indexField];
        unlink_<&Member::stateLink>(node);
        pushBack_<&Member::stateLink>(kSynced, node);
        m.state = InSync{};
        ++inSync_;
        ++F.synced;
        m.absentAtJoin = F.absent;
        const uint64_t carried = m.pendingSkipped;
        m.coveredAt = static_cast<int64_t>(scanned_) - static_cast<int64_t>(carried);   // skipped records carry over
        m.pendingSkipped = 0;
        if (m.lastFrameNs == kNoTime) m.lastFrameNs = nowNs;
        unlink_<&Member::noteLink>(node);
        pushBack_<&Member::noteLink>(carried > 0 ? kPending : kDormant, node);
        if (m.counters.rejoined < m.counters.fellBehind) { ++m.counters.rejoined; ++group_.rejoined; }
        F.loggedAbsent = VersionSet{};
        loggedUnknownGroup_ = VersionSet{};
    }
    /// Leave InSync at `base` (Retry, TooLarge, not deliverable).  Writes the member's own cursor back.
    template <class Src, class Snk>
    void fallBehind_(Cx<Src, Snk>& cx, uint32_t id, int64_t base) noexcept {
        const uint32_t node = id + kSentinels;
        Member& m = nodes_[node];
        IndexField& F = fields_[m.indexField];
        cx.sink.cursor(m.sub) = base;
        const int64_t skipped = static_cast<int64_t>(scanned_) - m.coveredAt;
        m.pendingSkipped = skipped > 0 ? static_cast<uint64_t>(skipped) : 0;
        m.counters.absentExcluded += F.absent - m.absentAtJoin;
        --inSync_;
        --F.synced;
        m.state = Behind{};
        unlink_<&Member::stateLink>(node);
        pushBack_<&Member::stateLink>(kCatchUp, node);
        unlink_<&Member::noteLink>(node);
        if (m.pendingSkipped > 0) pushBack_<&Member::noteLink>(kPending, node);
        ++m.counters.fellBehind;
        ++group_.fellBehind;
    }
    void advanceScanned_(uint64_t n) noexcept {
        if (n == 0) return;
        scanned_ += n;
        spliceAll_<&Member::noteLink>(kDormant, kPending);   // every dormant InSync member now owes skipped records
    }
    void noteSkipped_(uint32_t id, int32_t n) noexcept {
        if (n <= 0) return;
        Member& m = at_(id);
        m.pendingSkipped += static_cast<uint64_t>(n);
        ensurePending_(id + kSentinels);
    }
    template <class Src, class Snk>
    bool tryJoin_(uint32_t id, int64_t cur, Cx<Src, Snk>& cx) noexcept {
        if (const auto* s = std::get_if<Scanning>(&scan_)) {
            if (cur != s->cursor) return false;
        } else if (const auto* s2 = std::get_if<ScanStall>(&scan_)) {
            if (cur != s2->cursor) return false;
        } else {
            if (cur < cx.bound) return false;
            scan_ = Scanning{cur};   // idle: the first member to catch up to the bound starts the scan there
        }
        join_(id, cx.now);
        return true;
    }

    // ---- logs (cold) ----
    void logAbsent_(const Member& m, int32_t tag, uint16_t version) const {
        spdlog::info("filter: filtered field absent: sub {} topic {} partition {} tag {} schema version {} "
                     "(records of this version never match)", m.sub, topicId_, partition_, tag, version);
    }
    void logAbsentField_(uint32_t fi, uint16_t version) {
        IndexField& F = fields_[fi];
        if (F.loggedAbsent.has(version)) return;
        F.loggedAbsent.add(version);
        for (uint32_t n = nodes_[kSynced].stateLink.next; n != kSynced; n = nodes_[n].stateLink.next) {
            Member& m = nodes_[n];
            if (m.indexField != fi || m.loggedAbsent.has(version)) continue;
            m.loggedAbsent.add(version);
            logAbsent_(m, F.tag, version);
        }
    }
    void logAbsentMember_(Member& m, const rune::codec::FilterSchema& schema, uint16_t version) {
        if (m.loggedAbsent.has(version)) return;
        m.loggedAbsent.add(version);
        int32_t tag = -1;
        for (uint8_t i = 0; i < m.filter.termCount(); ++i) {
            const auto& t = m.filter.term(i);
            const auto sl = schema.slot(t.field, version);
            if (!sl.present || sl.type != t.type) { tag = t.tag; break; }
        }
        logAbsent_(m, tag, version);
    }
    void logUnknownMember_(Member& m, uint16_t version) {
        if (m.loggedUnknown.has(version)) return;
        m.loggedUnknown.add(version);
        spdlog::warn("filter: unknown record schema version: sub {} topic {} partition {} schema version {} "
                     "(records of this version are excluded)", m.sub, topicId_, partition_, version);
    }
    void logUnknownGroup_(uint16_t version) {
        if (loggedUnknownGroup_.has(version)) return;
        loggedUnknownGroup_.add(version);
        for (uint32_t n = nodes_[kSynced].stateLink.next; n != kSynced; n = nodes_[n].stateLink.next)
            logUnknownMember_(nodes_[n], version);
    }

    // ---- step 1: a Behind / VersionStall member reads its own cursor with its own predicate ----
    template <class Src, class Snk>
    int stepBehind_(uint32_t id, Cx<Src, Snk>& cx) {
        Member& m = at_(id);
        const uint32_t node = id + kSentinels;
        FilterScratch& sc = cx.sc;
        ++slotsTouched_;
        if (m.lastFrameNs == kNoTime) m.lastFrameNs = cx.now;
        if (!cx.sink.deliverable(m.sub)) return 0;
        int64_t& cur = cx.sink.cursor(m.sub);
        if (cur < cx.bound) cur = cx.src.skipEvicted(cur);
        int work = 0;
        for (;;) {
            if (tryJoin_(id, cur, cx)) return work;
            if (cur >= cx.bound || cx.budget == 0) return work;
            const std::span<const uint8_t> view = cx.src.read(cur, cx.budget);
            if (view.empty()) return work;
            std::size_t pos = 0;
            bool progressed = false;
            while (pos < view.size()) {
                BatchHdr h;
                if (parseBatch_(view, pos, h) != Parse::Ok) break;
                if (h.lastOffset >= cx.bound) return work;   // reached the HW / LSO bound
                const std::span<const uint8_t> bytes = view.subspan(pos, h.size);
                if (isSkipped_(cx, h)) {
                    noteSkipped_(id, h.count);
                    cur = std::max(cur, h.end);
                    pos += h.size;
                    progressed = true;
                    if (tryJoin_(id, cur, cx)) return work;
                    continue;
                }
                const std::size_t budgetBefore = cx.budget;
                const bool firstBefore = cx.firstOk;
                if (!admit_(cx, h.size)) return work;
                uint16_t uver = 0;
                bool unknown = false;
                if (!collect_(sc, bytes, cx.schema, m.expired, uver, unknown)) {   // a batch the codec cannot walk: skip it, counted
                    ++m.counters.malformed;
                    noteSkipped_(id, h.count);
                } else {
                    if (unknown) {
                        const auto* st = std::get_if<VersionStall>(&m.state);
                        const int64_t since = st != nullptr ? st->sinceNs : cx.now;
                        if (cx.now - since < cfg_.versionWaitNs) { m.state = VersionStall{uver, since}; return work; }
                    }
                    if (std::holds_alternative<VersionStall>(m.state)) m.state = Behind{};
                    sc.keep.clear();
                    for (uint32_t i = 0; i < sc.records.size(); ++i) {
                        const auto& r = sc.records[i];
                        switch (m.filter.evaluate(valueOf_(r), r.nullValue, cx.schema)) {
                            case rune::codec::FilterVerdict::Match:    sc.keep.push_back(i); break;
                            case rune::codec::FilterVerdict::NoMatch:  break;
                            case rune::codec::FilterVerdict::Absent:
                                ++m.counters.absentExcluded;
                                logAbsentMember_(m, cx.schema, rune::codec::recordVersion(
                                                                   reinterpret_cast<const uint8_t*>(r.value.data())));
                                break;
                            case rune::codec::FilterVerdict::UnknownVersion:
                                ++m.counters.unknownVersionExcluded;
                                {
                                    const uint16_t uv = rune::codec::recordVersion(reinterpret_cast<const uint8_t*>(r.value.data()));
                                    m.expired.add(uv);   // the wait is over for this version: no new wait on its next batch
                                    logUnknownMember_(m, uv);
                                }
                                break;
                            case rune::codec::FilterVerdict::Malformed: ++m.counters.malformed; break;
                            case rune::codec::FilterVerdict::NullValue: ++m.counters.nullValueExcluded; break;
                        }
                    }
                    if (sc.keep.empty()) {
                        noteSkipped_(id, h.count);
                    } else {
                        if (send_(cx, m, h, bytes, sc.keep) != SendOutcome::Sent) {   // stays at `cur`
                            cx.budget = budgetBefore;   // a refused send costs the pass nothing: refund the read
                            cx.firstOk = firstBefore;   // and the fresh-pass allowance it used
                            return work;
                        }
                        m.pendingSkipped = 0;
                        m.lastFrameNs = cx.now;
                        unlink_<&Member::noteLink>(node);
                        ++work;
                    }
                }
                cur = std::max(cur, h.end);
                pos += h.size;
                progressed = true;
                if (tryJoin_(id, cur, cx)) return work;
            }
            if (!progressed) return work;
        }
    }

    // ---- step 2: the shared scan ----
    [[nodiscard]] int64_t scanCursor_() const noexcept {
        if (const auto* s = std::get_if<Scanning>(&scan_)) return s->cursor;
        if (const auto* s = std::get_if<ScanStall>(&scan_)) return s->cursor;
        return 0;
    }

    template <class Src, class Snk>
    int scanStep_(Cx<Src, Snk>& cx) {
        int work = 0;
        int64_t c = scanCursor_();
        if (const int64_t c2 = cx.src.skipEvicted(c); c2 != c) { c = c2; scan_ = Scanning{c}; }   // evicted: resume at the ring start
        while (c < cx.bound && cx.budget > 0 && inSync_ > 0) {
            const std::span<const uint8_t> view = cx.src.read(c, cx.budget);
            if (view.empty()) break;
            std::size_t pos = 0;
            bool progressed = false;
            while (pos < view.size()) {
                BatchHdr h;
                if (parseBatch_(view, pos, h) != Parse::Ok) break;
                if (h.lastOffset >= cx.bound) return work;
                const std::span<const uint8_t> bytes = view.subspan(pos, h.size);
                if (isSkipped_(cx, h)) {
                    advanceScanned_(h.count > 0 ? static_cast<uint64_t>(h.count) : 0);
                    c = std::max(c, h.end);
                    scan_ = Scanning{c};
                    pos += h.size;
                    progressed = true;
                    continue;
                }
                if (!admit_(cx, h.size)) return work;
                bool stalled = false;
                work += scanBatch_(cx, h, bytes, c, stalled);
                if (stalled) return work;
                c = std::max(c, h.end);
                pos += h.size;
                progressed = true;
                if (inSync_ == 0) { scan_ = ScanIdle{}; return work; }
            }
            if (!progressed) break;
        }
        return work;
    }

    /// One data batch: probe, then deliver to the touched members only.  Sets scan_ (Scanning past the batch, or
    /// ScanStall on an unknown version, in which case nothing of the batch is delivered or counted).
    template <class Src, class Snk>
    int scanBatch_(Cx<Src, Snk>& cx, const BatchHdr& h, std::span<const uint8_t> bytes, int64_t c, bool& stalled) {
        FilterScratch& sc = cx.sc;
        stalled = false;
        uint16_t uver = 0;
        bool unknown = false;
        if (!collect_(sc, bytes, cx.schema, expiredGroup_, uver, unknown)) {   // a batch the codec cannot walk: skip it, counted
            ++group_.malformed;
            advanceScanned_(static_cast<uint64_t>(h.count));
            scan_ = Scanning{h.end};
            return 0;
        }
        if (unknown) {
            const auto* st = std::get_if<ScanStall>(&scan_);
            const int64_t since = (st != nullptr && st->cursor == c) ? st->sinceNs : cx.now;
            if (cx.now - since < cfg_.versionWaitNs) {
                scan_ = ScanStall{c, uver, since};
                stalled = true;
                return 0;
            }
        }
        sc.matches.clear();
        sc.touched.clear();
        bool overflow = false;
        const uint32_t nrec = static_cast<uint32_t>(sc.records.size());
        for (uint32_t i = 0; i < nrec; ++i) {
            const auto& r = sc.records[i];
            if (r.nullValue) { ++group_.nullValueExcluded; continue; }
            if (r.value.size() < 4) { ++group_.malformed; continue; }
            const std::span<const uint8_t> value = valueOf_(r);
            const uint16_t ver = rune::codec::recordVersion(value.data());
            if (!cx.schema.knowsVersion(ver)) {   // only reachable after the wait
                ++group_.unknownVersionExcluded;
                expiredGroup_.add(ver);   // the wait is over for this version: no new wait on its next batch
                logUnknownGroup_(ver);
                continue;
            }
            for (uint32_t f = 0; f < nFields_; ++f) {
                IndexField& F = fields_[f];
                if (F.synced == 0) continue;
                const rune::codec::FieldSlot sl = cx.schema.slot(F.schemaField, ver);
                if (!sl.present) { ++F.absent; logAbsentField_(f, ver); continue; }
                if (static_cast<std::size_t>(sl.offset) + sl.width > value.size()) { ++group_.malformed; continue; }
                const rune::codec::FilterKey key = rune::codec::loadFilterKey(value.data() + sl.offset, sl.width);
                F.index.forEach(key, [&](uint32_t member) {
                    Member& m = at_(member);
                    if (!std::holds_alternative<InSync>(m.state)) return;
                    ++slotsTouched_;
                    const auto verdict = m.filter.termCount() == 1 ? rune::codec::FilterVerdict::Match
                                                                   : m.filter.evaluateResidual(value, ver, cx.schema);
                    switch (verdict) {
                        case rune::codec::FilterVerdict::Match:
                            if (sc.matches.size() < sc.matchCap) sc.matches.push_back({member, i}); else overflow = true;
                            break;
                        case rune::codec::FilterVerdict::Absent:
                            ++m.counters.absentExcluded;
                            logAbsentMember_(m, cx.schema, ver);
                            break;
                        case rune::codec::FilterVerdict::Malformed: ++m.counters.malformed; break;
                        default: break;
                    }
                });
            }
        }
        int work = 0;
        if (overflow) {
            ++group_.overflowBatches;
            work += serveOverflow_(cx, h, bytes);
        } else {
            // Counting sort of the matches by member: counts -> starts -> order (record indices, ascending per member).
            for (const auto& mr : sc.matches)
                if (sc.counts[mr.member]++ == 0) sc.touched.push_back(mr.member);
            uint32_t run = 0;
            for (const uint32_t t : sc.touched) { sc.starts[t] = run; run += sc.counts[t]; sc.counts[t] = 0; }
            for (const auto& mr : sc.matches) sc.order[sc.starts[mr.member] + sc.counts[mr.member]++] = mr.record;
            for (const uint32_t t : sc.touched) {
                const std::span<const uint32_t> keep(sc.order.data() + sc.starts[t], sc.counts[t]);
                sc.counts[t] = 0;
                work += serveMember_(cx, t, h, bytes, keep, nrec);
            }
        }
        advanceScanned_(nrec);
        for (const uint32_t t : sc.touched) {   // members served this batch are caught up: dormant again
            const uint32_t node = t + kSentinels;
            Member& m = nodes_[node];
            if (std::holds_alternative<InSync>(m.state) && m.coveredAt == static_cast<int64_t>(scanned_)) {
                unlink_<&Member::noteLink>(node);
                pushBack_<&Member::noteLink>(kDormant, node);
            }
        }
        scan_ = Scanning{h.end};
        return work;
    }

    template <class Src, class Snk>
    int serveMember_(Cx<Src, Snk>& cx, uint32_t id, const BatchHdr& h, std::span<const uint8_t> bytes,
                     std::span<const uint32_t> keep, uint32_t nrec) {
        const uint32_t node = id + kSentinels;
        Member& m = nodes_[node];
        ++slotsTouched_;
        if (!cx.sink.deliverable(m.sub)) { fallBehind_(cx, id, h.base); return 0; }
        const SendOutcome o = send_(cx, m, h, bytes, keep);
        if (o != SendOutcome::Sent) { fallBehind_(cx, id, h.base); return 0; }   // Retry and TooLarge alike
        m.coveredAt = static_cast<int64_t>(scanned_) + nrec;
        m.lastFrameNs = cx.now;
        unlink_<&Member::noteLink>(node);
        return 1;
    }

    /// The match list overflowed: every InSync member is served this batch with its own full evaluate.  All the
    /// counters were already taken by the probe loop, so this evaluates quietly.
    template <class Src, class Snk>
    int serveOverflow_(Cx<Src, Snk>& cx, const BatchHdr& h, std::span<const uint8_t> bytes) {
        FilterScratch& sc = cx.sc;
        const uint32_t nrec = static_cast<uint32_t>(sc.records.size());
        int work = 0;
        for (uint32_t n = nodes_[kSynced].stateLink.next; n != kSynced;) {
            const uint32_t nxt = nodes_[n].stateLink.next;   // serveMember_ may move n to the catch-up list
            const uint32_t id = n - kSentinels;
            Member& m = nodes_[n];
            ++slotsTouched_;
            sc.keep.clear();
            for (uint32_t i = 0; i < nrec; ++i) {
                const auto& r = sc.records[i];
                if (m.filter.evaluate(valueOf_(r), r.nullValue, cx.schema) == rune::codec::FilterVerdict::Match)
                    sc.keep.push_back(i);
            }
            if (!sc.keep.empty()) {
                sc.touched.push_back(id);
                work += serveMember_(cx, id, h, bytes, sc.keep, nrec);
            }
            n = nxt;
        }
        return work;
    }

    // ---- step 3: position notes ----
    template <class Snk>
    int notes_(Snk& sink, int64_t nowNs) {
        lastSweepNs_ = nowNs;
        scannedAtSweep_ = scanned_;
        int work = 0;
        for (uint32_t n = nodes_[kPending].noteLink.next; n != kPending;) {
            const uint32_t nxt = nodes_[n].noteLink.next;
            const uint32_t id = n - kSentinels;
            Member& m = nodes_[n];
            ++slotsTouched_;
            const bool sync = std::holds_alternative<InSync>(m.state);
            const int64_t skipped = sync ? static_cast<int64_t>(scanned_) - m.coveredAt
                                         : static_cast<int64_t>(m.pendingSkipped);
            if (skipped <= 0) {
                unlink_<&Member::noteLink>(n);
                if (sync) pushBack_<&Member::noteLink>(kDormant, n);
            } else {
                const int64_t last = m.lastFrameNs == kNoTime ? nowNs : m.lastFrameNs;
                const bool due = static_cast<uint64_t>(skipped) >= cfg_.noteRecords || nowNs - last >= cfg_.noteIntervalNs;
                if (due && sink.deliverable(m.sub) &&
                    sink.sendNote(m.sub, cursorOf(id, sink.cursor(m.sub))) == SendOutcome::Sent) {
                    if (sync) m.coveredAt = static_cast<int64_t>(scanned_); else m.pendingSkipped = 0;
                    m.lastFrameNs = nowNs;
                    ++m.counters.notes;
                    ++group_.notes;
                    unlink_<&Member::noteLink>(n);
                    if (sync) pushBack_<&Member::noteLink>(kDormant, n);
                    ++work;
                }
            }
            n = nxt;
        }
        return work;
    }

    int32_t  topicId_ = 0, partition_ = 0;
    uint64_t sourceId_ = 0;
    FilterConfig cfg_;
    std::vector<Member> nodes_;        // [0, kSentinels) list sentinels, then the member slots
    uint32_t freeHead_ = kNil;         // free-slot chain (through stateLink.next)
    uint32_t count_ = 0;               // live members
    std::array<IndexField, kMaxIndexFields> fields_{};
    uint8_t  nFields_ = 0;
    uint32_t inSync_ = 0;
    ScanState scan_ = ScanIdle{};
    uint64_t scanned_ = 0, scannedAtSweep_ = 0;
    int64_t  lastSweepNs_ = 0;
    VersionSet loggedUnknownGroup_, expiredGroup_;
    FilterCounters group_;
    uint64_t slotsTouched_ = 0;
};

}  // namespace rune::node::filter
