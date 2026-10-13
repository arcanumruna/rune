#pragma once
// RcState.h — F15 S4 (design note §2.1-§2.4, Option B): the read_committed
// state of ONE worker-owned user partition, owned by its worker.
//
//   * LsoTracker + AbortedTxnIndex, updated in the same call as each append
//     (data batch: onTxnBatch; marker: onMarker) — never through a return, an
//     ack or a session (C1, K1, K2: every read_committed decision is made on the
//     owning worker from this state and the live HW, L5).
//   * A rich-state build machine (each state holds only its own variables):
//
//       Unbuilt                                    not serving read_committed
//       Scanning{cursor, target, epoch, gens,      building [logStart, target)
//                segment byte cursor}              (a DISK log)
//       RingScanning{cursor, target, ringStart,    building [ringStart, target)
//                    epoch, gens}                  (a memory ring; E2 final fix wave I3)
//       Ready{epoch, gens}                         serving; append-time updates live
//
//     gens = (logGeneration, leadershipGeneration) the state was built for.
//     advance() is the per-pass driver (§2.3), state-driven and hook-free:
//       1. not Unbuilt and (not Leader, or a gen changed) → Unbuilt (clears
//          tracker, index, tail list) — BecomeFollower, self-demotion,
//          truncation, reconcile append, a follower period between passes
//          (leadershipGeneration); the `!Unbuilt` guard keeps followers from
//          paying the clear per pass.  A leader-epoch bump with unchanged gens
//          keeps the state (exact).
//       2. Unbuilt and Leader (I2) → Scanning{logStart, LEO}.
//       3. Scanning (NEVER blocks on disk — fix round 1, controller cold-cache
//          bench): the scan walks each segment's untouched read-only mapping
//          sequentially by byte position (no index lookup, no header read
//          before the gate).  Each pass:
//            - clamps the cursor to logStartOffset (I-C: retention between passes);
//            - issues MADV_WILLNEED read-ahead for the next few budgets (async);
//            - asks the residency probe (mincore by default) how many bytes from
//              the byte cursor are RESIDENT, and parses only whole batches inside
//              that prefix and inside `budget` (one batch larger than the budget
//              is taken alone once fully resident);
//            - a pass with nothing resident reads nothing and returns at once.
//          Fix round 2: read-ahead is best-effort (dropped hints, evicted pages),
//          so a cursor window that is not wholly resident is RE-REQUESTED on
//          every pass (RR1-1) — unless a hint for this partition is still in
//          flight (<= 1 per partition, RR1-4): the worker issues it as an async
//          IORING_OP_FADVISE(WILLNEED) on the segment fd where io_uring is
//          available, synchronously (counted) elsewhere.  An oversized batch's
//          extent is re-requested with a doubling pass backoff (<= 64; N3).
//          N4: mincore → access is inherently racy — a page reclaimed between
//          the probe and the parse still takes a major fault (rare; the gate
//          removes the systematic cold-read class, not this window).
//          Per-pass cost is CPU-bounded; liveness is progress-driven (completed
//          read-ahead), never time-driven: target is fixed at entry, so the scan
//          ends once ceil((target - logStart) / budget) resident windows were
//          parsed, whatever the append rate.  At target the marker tail recorded
//          meanwhile is replayed in order → Ready.  A replaced segment object (a
//          compaction swap) restarts its byte walk at 0, skipping batches below
//          the cursor (already applied).
//   * The tail list holds MARKERS ONLY: a producerId >= 0 data batch is refused
//     until Ready (the produce gate), non-transactional batches carry no state.
//   * Review m4: the two "cannot happen" branches (a data batch registered while
//     not Ready; an unparseable batch inside the written bytes) are invariant-
//     violation HALTS (exit 70) — failing open would expose aborted data.
//
// Owner (worker) thread only.  No locks, no atomics, no virtual calls; the hot
// paths (onTxnBatch, lso, isAborted, ready) never allocate once reserve()d.
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/ControlMarkerLayout.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
#include <rune/storage/ProducerStateManager.h>
#include <rune/transaction/ReadCommittedView.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <variant>
#include <vector>

#include <sys/mman.h>
#include <unistd.h>

namespace rune::node {

/// F15 S4 fix round 1 (cold cache): how many bytes from `p` (up to `len`) are
/// resident in memory, contiguous from `p`.  A plain function pointer +
/// context (no std::function, no virtual).  nullptr = the default (mincore).
using RcResidencyProbe = std::size_t (*)(void* ctx, const uint8_t* p, std::size_t len);

/// F15 S4 fix round 2 (RR1-1, RR1-4): one read-ahead request — a byte range of
/// a segment, both as its mapping address and as (fd, file offset).
struct RcReadAheadReq {
    const uint8_t* addr    = nullptr;
    std::size_t    len     = 0;
    int            fd      = -1;
    int64_t        fileOff = 0;
};
/// The outcome of one read-ahead hint (fix round 4, r3-2 / m5).
enum class RcHint : uint8_t {
    Done,       ///< executed synchronously (MADV_WILLNEED): nothing in flight
    InFlight,   ///< submitted asynchronously; the issuer later calls RcState::onHintCompleted()
    Skipped,    ///< NOT issued (per-worker cap / SQ full): retried next pass, no backoff consumed
};
/// Issue a read-ahead hint.  fn-pointer + ctx (no std::function, no virtual).
/// nullptr = synchronous MADV_WILLNEED.
using RcReadAhead = RcHint (*)(void* ctx, const RcReadAheadReq& req);

/// The replica facts one advance() step reads (owner thread, plain loads).
struct RcFacts {
    bool     leader               = false;
    uint64_t logGeneration        = 0;
    uint64_t leadershipGeneration = 0;
    int32_t  epoch                = 0;
};

/// A fixed-capacity open-addressing set of producer ids (>= 0): linear probing at
/// load <= 1/2, O(1) membership, no allocation after reserve() and never past its
/// capacity — an insert into a full set is refused (the caller decides the policy).
/// Cold-path helper of the memory-backed RC rebuild (E2 Task 12, finding B).
class FixedPidSet {
public:
    enum class Insert : uint8_t { Inserted, Present, Full };
    void reserve(std::size_t maxPids) {
        std::size_t slots = 1;
        while (slots < 2 * maxPids) slots <<= 1;
        slots_.assign(slots, kEmpty);
        mask_ = slots - 1;
        max_  = maxPids;
        n_    = 0;
    }
    void clear() noexcept {
        if (n_ != 0) std::fill(slots_.begin(), slots_.end(), kEmpty);
        n_ = 0;
    }
    [[nodiscard]] bool contains(int64_t pid) const noexcept {
        if (max_ == 0) return false;
        for (std::size_t i = hash_(pid) & mask_;; i = (i + 1) & mask_) {
            if (slots_[i] == pid) return true;
            if (slots_[i] == kEmpty) return false;
        }
    }
    Insert insert(int64_t pid) noexcept {
        if (max_ == 0) return Insert::Full;
        for (std::size_t i = hash_(pid) & mask_;; i = (i + 1) & mask_) {
            if (slots_[i] == pid) return Insert::Present;
            if (slots_[i] == kEmpty) {
                if (n_ == max_) return Insert::Full;
                slots_[i] = pid;
                ++n_;
                return Insert::Inserted;
            }
        }
    }
    [[nodiscard]] std::size_t size() const noexcept { return n_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return max_; }
    [[nodiscard]] std::size_t slotBytes() const noexcept { return slots_.capacity() * sizeof(int64_t); }

private:
    static constexpr int64_t kEmpty = -1;
    static std::size_t hash_(int64_t pid) noexcept {   // splitmix64 finaliser
        uint64_t x = static_cast<uint64_t>(pid) + 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return static_cast<std::size_t>(x ^ (x >> 31));
    }
    std::vector<int64_t> slots_;
    std::size_t          mask_ = 0;
    std::size_t          max_  = 0;
    std::size_t          n_    = 0;
};

class RcState {
public:
    /// Per-pass build budget (bytes parsed per advance step).  Set from
    /// BM_RcBuildStep with a cold page cache (p99 per step <= 20 µs target).
    /// k3s-node-01, 7c814b6 (async FADVISE hints, mincore gate), cold cache:
    /// p99 16.6 µs @16 KiB, 18.4 µs @32 KiB, 21.9 µs @64 KiB → 32 KiB.
    static constexpr int32_t     kDefaultScanBytesPerPass = 32 * 1024;
    static constexpr std::size_t kTailReserve             = 256;
    /// Distinct producers with a marker in the ring one memory-backed rebuild can
    /// remember (fixed capacity; past it a producer counts as "no marker seen" —
    /// pinned at the ring start, the conservative side — and the overflow is counted).
    static constexpr std::size_t kRingMarkedPidsCapacity  = 1024;
    /// Read-ahead depth: request (async FADVISE / sync MADV_WILLNEED) this many
    /// budgets beyond the cursor (m7).
    static constexpr std::size_t kReadAheadBudgets        = 8;
    static constexpr int         kExitInvariantViolation  = 70;

    struct Unbuilt {};
    struct Scanning {
        int64_t     cursor      = 0;   ///< next offset to apply
        int64_t     target      = 0;   ///< LEO at entry
        int32_t     epoch       = 0;
        uint64_t    logGen      = 0;
        uint64_t    leadGen     = 0;
        int64_t     segBase     = 0;   ///< segment being walked
        std::size_t bytePos     = 0;   ///< byte cursor inside it
        uint64_t    segGen      = 0;   ///< N1: the segment's install generation (identity)
        std::size_t readAheadTo = 0;   ///< bytes of this segment already requested
        std::size_t oversizeAt  = SIZE_MAX;   ///< N3: byte cursor of a waiting oversized batch
        uint64_t    oversizeNext = 0;         ///< step at which its extent is re-requested
        uint64_t    oversizeGap  = 1;         ///< doubling pass backoff (<= 64)
        std::size_t waitAt   = SIZE_MAX;      ///< m2: byte cursor of a non-resident window
        uint64_t    waitNext = 0;             ///< step at which it is re-requested
        uint64_t    waitGap  = 1;             ///< doubling pass backoff (<= 64)
        bool        stalled   = false;        ///< D30: the segment under the cursor is unreadable
        uint64_t    stallNext = 0;            ///< step at which it is retried
        uint64_t    stallGap  = 1;            ///< doubling pass backoff (<= 1024)
    };
    struct Ready {
        int32_t  epoch   = 0;
        uint64_t logGen  = 0;
        uint64_t leadGen = 0;
    };
    /// E2 final fix wave (I3): the MEMORY-BACKED build, budgeted like the disk
    /// scan (kDefaultScanBytesPerPass of whole ring batches per advance step) —
    /// it used to walk the whole ring (up to RUNE_TRANSIENT_LOG_RING_BYTES,
    /// ~10^5-10^6 small batches) in ONE worker pass, for every partition whose
    /// leadership / ownership changed in that pass.  While it runs the state is
    /// not Ready: read_committed reads are held and idempotent / transactional
    /// produces refused (NotLeader, retriable) — exactly as for a disk Scanning.
    struct RingScanning {
        int64_t  cursor    = 0;   ///< next ring offset to apply
        int64_t  target    = 0;   ///< the ring's LEO at entry (fixed: markers above it ride the tail)
        int64_t  ringStart = 0;   ///< the ring start this build began at (finding-B pins sit there)
        int32_t  epoch     = 0;
        uint64_t logGen    = 0;
        uint64_t leadGen   = 0;
    };
    struct TailEvent {
        int64_t producerId = -1;
        int64_t offset     = 0;
        bool    commit     = false;
    };

    /// Cold (ownership apply, addendum m5): reserve every structure so the
    /// partition's first transactional batch / marker never allocates.
    void reserve(std::size_t openTxnCapacity) {
        tracker_.reserve(openTxnCapacity);
        aborted_.reserve(rune::transaction::AbortedTxnIndex::kDefaultReservedRanges);
        tail_.reserve(kTailReserve);
        residency_.reserve(static_cast<std::size_t>(kDefaultScanBytesPerPass) / 4096 + 2);
    }
    /// Cold (ownership apply), MEMORY-BACKED partitions only: the ring rebuild's
    /// fixed-capacity producer set (a disk partition never pays for it).  Without
    /// it every producer counts as unmarked (conservative pins, overflows counted).
    void reserveRingRebuild(std::size_t maxPids = kRingMarkedPidsCapacity) { ringMarkedPids_.reserve(maxPids); }
    void setResidencyProbe(RcResidencyProbe fn, void* ctx) noexcept { probeFn_ = fn; probeCtx_ = ctx; }
    /// F15 S5 (§2.5, I3): the owning worker's producer state for THIS partition.
    /// The build feeds it from the same pass (RcBuilder::step): data batches as
    /// sequences, control batches as epoch raises (no sequence).  It is cleared
    /// for the partition on every Unbuilt → Scanning edge, so after a truncation,
    /// a reconcile append or a follower period it holds exactly the surviving
    /// log — and producerId >= 0 batches are refused until Ready (the produce
    /// gate), so nothing updates it concurrently except a marker's fence, which
    /// only ever raises an epoch.  Cold (ownership apply).
    void bindProducerState(rune::storage::ProducerStateManager* psm, int32_t topicId, int32_t partition) noexcept {
        psm_ = psm; psmTopic_ = topicId; psmPartition_ = partition;
    }
    /// The read-ahead issuer (nullptr = synchronous MADV_WILLNEED).
    void setReadAhead(RcReadAhead fn, void* ctx) noexcept { readAheadFn_ = fn; readAheadCtx_ = ctx; }
    /// The async hint issued for this partition completed (its CQE was reaped).
    void onHintCompleted() noexcept { hintInFlight_ = false; }
    [[nodiscard]] bool hintInFlight() const noexcept { return hintInFlight_; }
    /// Bench: per-phase time split of each step (nullptr = off: no clock reads).
    struct StepProfile {
        uint64_t hintNs  = 0;
        uint64_t probeNs = 0;
        uint64_t parseNs = 0;
    };
    void setProfile(StepProfile* p) noexcept { profile_ = p; }

    [[nodiscard]] bool ready() const noexcept { return std::holds_alternative<Ready>(st_); }
    /// Building (a disk Scanning or a memory RingScanning).
    [[nodiscard]] bool scanning() const noexcept {
        return std::holds_alternative<Scanning>(st_) || std::holds_alternative<RingScanning>(st_);
    }
    /// 0 Unbuilt, 1 Scanning (disk or ring), 2 Ready.
    [[nodiscard]] int statusCode() const noexcept { return scanning() ? 1 : ready() ? 2 : 0; }

    // ---- §2.1: append-time updates (same invocation as the append) --------
    /// A TRANSACTIONAL data batch appended at `base` (not a duplicate).  Only
    /// reachable while Ready (the produce gate) — anything else halts (m4).
    void onTxnBatch(int64_t producerId, int64_t base) {
        if (!ready()) [[unlikely]]
            haltInvariant_("a producerId >= 0 data batch was appended while the RC state is not Ready");
        tracker_.onProduceBatch(producerId, base);
    }
    /// A COMMIT / ABORT control batch appended at `offset`.
    void onMarker(int64_t producerId, int64_t offset, bool commit) {
        if (ready()) {
            applyMarker_(producerId, offset, commit);
        } else if (scanning()) {
            if (tail_.size() == tail_.capacity()) [[unlikely]] ++tailGrowths_;   // cold, counted
            tail_.push_back(TailEvent{producerId, offset, commit});
        }
        // Unbuilt: nothing — the build starts at LEO > offset and scans it.
    }

    // ---- D3b: the cleaner's LSO bound ---------------------------------------
    /// The first unstable offset the LogCleanerAgent must not clean at/after
    /// (PartitionReplica::publishCleanerLso): Ready → the first offset of the
    /// earliest open transaction, or INT64_MAX when none is open; not Ready → 0
    /// (the LSO is unknown while the state is (re)built: clean nothing).
    [[nodiscard]] int64_t cleanerFirstUnstableOffset() const noexcept {
        return ready() ? tracker_.lso(std::numeric_limits<int64_t>::max()) : 0;
    }
    /// True (and remembers `out`) when the value changed since the last call —
    /// the worker publishes only changes (no per-pass store on the replica line).
    bool takeCleanerLsoChange(int64_t& out) noexcept {
        out = cleanerFirstUnstableOffset();
        if (out == cleanerLsoPublished_) return false;
        cleanerLsoPublished_ = out;
        return true;
    }

    // ---- §2.3 step 1 alone: validity against the live replica facts --------
    /// The reset rule of advance() without any build work: cheap enough for
    /// every serve and every gated produce (a role change can land between the
    /// per-pass driver and a read in the same pass).  Returns ready().
    bool revalidate(const RcFacts& f) noexcept {
        if (!std::holds_alternative<Unbuilt>(st_)) {
            const auto [logGen, leadGen] = builtFor_();
            if (!f.leader || f.logGeneration != logGen || f.leadershipGeneration != leadGen) [[unlikely]]
                reset_();
        }
        return ready();
    }

    // ---- §2.3: per-pass driver --------------------------------------------
    /// `log` null = a memory-backed partition (`memLog`, F22): its producer state
    /// AND its read_committed state (tracker + aborted index) are rebuilt from
    /// the ring by the budgeted RingScanning state (E2 final fix wave I3).
    /// Returns the bytes this step parsed (0: no scan work).
    std::size_t advance(const RcFacts& f, rune::storage::Log* log, int32_t budget,
                        rune::storage::InMemoryLog* memLog = nullptr) {
        (void)revalidate(f);
        if (Ready* r = std::get_if<Ready>(&st_)) [[likely]] {
            r->epoch = f.epoch;                            // epoch bump, same log: still exact
            if (log != nullptr && !aborted_.ranges().empty() &&
                aborted_.ranges().front().lastOffset < log->logStartOffset()) [[unlikely]]
                aborted_.pruneBelow(log->logStartOffset());   // retention (cold)
            return 0;
        }
        if (std::holds_alternative<Unbuilt>(st_)) {
            if (!f.leader) return 0;  // I2: a leader's log only
            if (log == nullptr) {
                // PSM <-> log consistency (finding 3, F22): a memory-backed
                // partition's producer state is REBUILT from its InMemoryLog on
                // this edge, exactly like a disk log's scan — never kept from an
                // earlier leadership / log generation (a retry answered
                // duplicate-OK at an offset now holding other data), and never
                // answered from while incomplete: producerId >= 0 batches are
                // refused until Ready (the produce gate), as on disk.
                // E2 Task 11 fix round 1: the read_committed state is rebuilt from
                // the SAME ring on this edge — an empty tracker answered LSO = HW
                // (an OPEN transaction delivered) and an empty aborted index
                // delivered aborted batches after every ownership / leadership
                // change.
                // E2 final fix wave (I3): both are rebuilt by the BUDGETED
                // RingScanning state (whole batches, ~budget bytes per pass) —
                // never the whole ring in one pass.  No ring (unreachable: every
                // replica has a log) → Ready with an empty state; produceCore_
                // refuses every produce of a memory-backed partition without a
                // ring anyway.
                if (psm_ != nullptr) psm_->clearPartition(psmTopic_, psmPartition_);
                if (memLog == nullptr) {
                    st_ = Ready{f.epoch, f.logGeneration, f.leadershipGeneration};
                    return 0;
                }
                beginRingScan_(f, *memLog);
            } else {
                // F15 S5 (I3): the producer state is rebuilt from the log by this
                // scan — never kept from an earlier leadership or log generation.
                if (psm_ != nullptr) psm_->clearPartition(psmTopic_, psmPartition_);
                Scanning sc;
                sc.cursor = sc.segBase = log->logStartOffset();
                sc.target = log->logEndOffset();
                sc.epoch = f.epoch; sc.logGen = f.logGeneration; sc.leadGen = f.leadershipGeneration;
                st_ = sc;
            }
        }
        if (RingScanning* rs = std::get_if<RingScanning>(&st_)) {
            rs->epoch = f.epoch;
            if (memLog == nullptr) [[unlikely]] return 0;   // never: a ring build is driven with its ring
            return ringStep_(*rs, *memLog, budget);
        }
        Scanning& s = std::get<Scanning>(st_);
        s.epoch = f.epoch;
        return step_(s, *log, budget);
    }

    // ---- §2.4: serving ------------------------------------------------------
    [[nodiscard]] int64_t lso(int64_t hw) const noexcept { return tracker_.lso(hw); }
    [[nodiscard]] bool isAborted(int64_t producerId, int64_t base) const noexcept {
        return aborted_.isAborted(producerId, base);
    }

    [[nodiscard]] const rune::transaction::LsoTracker&      tracker() const noexcept { return tracker_; }
    [[nodiscard]] const rune::transaction::AbortedTxnIndex& aborted() const noexcept { return aborted_; }
    [[nodiscard]] uint64_t    scanSteps() const noexcept { return scanSteps_; }
    [[nodiscard]] uint64_t    unreadableStalls() const noexcept { return unreadableStalls_; }
    [[nodiscard]] std::size_t maxStepBytes() const noexcept { return maxStepBytes_; }
    [[nodiscard]] uint64_t    tailGrowths() const noexcept { return tailGrowths_; }
    [[nodiscard]] uint64_t    scannedBytes() const noexcept { return scannedBytes_; }
    /// Passes that found nothing resident at the cursor (waited on read-ahead).
    [[nodiscard]] uint64_t    nonResidentSteps() const noexcept { return nonResidentSteps_; }
    [[nodiscard]] uint64_t    readAheadRequests() const noexcept { return readAheadRequests_; }
    [[nodiscard]] uint64_t    readAheadSkippedInFlight() const noexcept { return readAheadSkippedInFlight_; }
    /// r3-2: requests the issuer skipped (cap / SQ full) — not issued, retried next pass.
    [[nodiscard]] uint64_t    readAheadSkippedByIssuer() const noexcept { return readAheadSkippedByIssuer_; }
    /// N2: mincore failures (each fell back to treating the window as resident).
    [[nodiscard]] uint64_t    probeErrors() const noexcept { return probeErrors_; }
    /// N8: cold growths of the residency vector (an oversized batch).
    [[nodiscard]] uint64_t    residencyGrowths() const noexcept { return residencyGrowths_; }

private:
    [[noreturn]] [[gnu::cold]] [[gnu::noinline]] static void haltInvariant_(const char* what) noexcept {
        spdlog::critical("RcState: INVARIANT VIOLATION — {} — halting (exit {})", what, kExitInvariantViolation);
        spdlog::default_logger()->flush();
        std::_Exit(kExitInvariantViolation);
    }
    [[nodiscard]] std::pair<uint64_t, uint64_t> builtFor_() const noexcept {
        if (const auto* s = std::get_if<Scanning>(&st_)) return {s->logGen, s->leadGen};
        if (const auto* s = std::get_if<RingScanning>(&st_)) return {s->logGen, s->leadGen};
        const auto& r = std::get<Ready>(st_);
        return {r.logGen, r.leadGen};
    }
    void reset_() noexcept {
        tracker_.clear();   // N4: both indexes
        aborted_.clear();
        tail_.clear();
        st_ = Unbuilt{};
    }
    void applyMarker_(int64_t producerId, int64_t offset, bool commit) {
        const int64_t first = tracker_.onControlMarker(producerId, offset, commit);
        if (!commit && first >= 0) aborted_.onAbortMarker(producerId, first, offset);
    }
    static std::size_t pageSize_() noexcept {
        static const std::size_t p = static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
        return p;
    }
    // Bytes resident from p (contiguous), up to len.  Never touches the bytes.
    std::size_t resident_(const uint8_t* p, std::size_t len) {
        if (len == 0) return 0;
        if (probeFn_ != nullptr) return std::min(len, probeFn_(probeCtx_, p, len));
        const std::size_t page  = pageSize_();
        const uintptr_t   start = reinterpret_cast<uintptr_t>(p) & ~static_cast<uintptr_t>(page - 1);
        const uintptr_t   end   = reinterpret_cast<uintptr_t>(p) + len;
        const std::size_t pages = (end - start + page - 1) / page;
        if (residency_.size() < pages) [[unlikely]] {   // cold: a batch above the budget (N8: counted)
            ++residencyGrowths_;
            residency_.resize(pages);
        }
#if defined(__APPLE__)
        if (::mincore(reinterpret_cast<caddr_t>(start), end - start, reinterpret_cast<char*>(residency_.data())) != 0)
#else
        if (::mincore(reinterpret_cast<void*>(start), end - start, residency_.data()) != 0)
#endif
        {
            // N2: never expected on a live mapping.  Counted (and logged once):
            // the window is treated as resident — a possibly blocking read,
            // never a silent one.
            if (probeErrors_++ == 0)
                spdlog::error("RcState: mincore failed (errno {}) — RC build window treated as resident "
                              "(counted: probeErrors)", errno);
            return len;
        }
        std::size_t i = 0;
        while (i < pages && (residency_[i] & 1u) != 0) ++i;
        if (i == pages) return len;
        const std::size_t residentEnd = start + i * page;
        return residentEnd > reinterpret_cast<uintptr_t>(p) ? residentEnd - reinterpret_cast<uintptr_t>(p) : 0;
    }
public:
    /// The synchronous fallback hint (no io_uring): MADV_WILLNEED on the range.
    static void syncWillNeed(const RcReadAheadReq& r) noexcept {
        if (r.len == 0) return;
        const std::size_t page  = pageSize_();
        const uintptr_t   start = reinterpret_cast<uintptr_t>(r.addr) & ~static_cast<uintptr_t>(page - 1);
        const uintptr_t   end   = reinterpret_cast<uintptr_t>(r.addr) + r.len;
        (void)::madvise(reinterpret_cast<void*>(start), end - start, MADV_WILLNEED);
    }
private:
    // Request read-ahead of [from, to) of the segment.  At most one async hint
    // in flight per partition (RR1-4): while one is, the request is skipped —
    // its completion re-arms the next pass.  Returns true if issued.
    // Fix round 4 (r3-2 / m5): a hint the issuer SKIPPED (per-worker cap / SQ
    // full) was never issued — not counted, the front is not advanced, and
    // false is returned so no re-request backoff is consumed: it is retried on
    // the next pass.
    bool requestReadAhead_(Scanning& s, const rune::storage::Log::SegmentBytes& m, std::size_t from, std::size_t to) {
        if (to <= from) return false;
        if (hintInFlight_) { ++readAheadSkippedInFlight_; return false; }
        RcReadAheadReq req;
        req.addr    = m.data + from;
        req.len     = to - from;
        req.fd      = m.fd;
        req.fileOff = static_cast<int64_t>(from);
        const auto t0 = profile_ ? clock_() : 0;
        RcHint out = RcHint::Done;
        if (readAheadFn_ != nullptr) out = readAheadFn_(readAheadCtx_, req);
        else                         syncWillNeed(req);
        if (profile_) profile_->hintNs += clock_() - t0;
        if (out == RcHint::Skipped) { ++readAheadSkippedByIssuer_; return false; }
        hintInFlight_ = out == RcHint::InFlight;
        ++readAheadRequests_;
        if (to > s.readAheadTo) s.readAheadTo = to;
        return true;
    }
    static uint64_t clock_() noexcept {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
    }
    std::size_t step_(Scanning& s, rune::storage::Log& log, int32_t budget) {
        ++scanSteps_;
        // I-C: retention runs on this thread between passes and may delete the
        // segments under the cursor.  Batches below the new log start are gone,
        // and so is any data they could hide: skipping them is exact.
        const int64_t logStart = log.logStartOffset();
        if (s.cursor < logStart) s.cursor = logStart;
        if (s.segBase < logStart) { s.segBase = logStart; s.bytePos = 0; s.segGen = 0; s.readAheadTo = 0; }
        std::size_t bytes = 0;
        if (s.cursor < s.target && s.stalled && scanSteps_ < s.stallNext) return 0;   // backing off (count-based)
        if (s.cursor < s.target) {
            const auto m = log.segmentBytesAt(s.segBase);
            if (m.gen != s.segGen) { s.segGen = m.gen; s.bytePos = 0; s.readAheadTo = 0; }
            if (m.data == nullptr && m.written > s.bytePos) [[unlikely]] {
                // D30 review MAJOR-2: bytes exist that cannot be mapped (ENOMEM,
                // vm.max_map_count): never "walked" — skipping them would drop
                // their abort markers and serve ABORTED records to read_committed
                // consumers.  Stall (fail closed: the build stays Scanning, nothing
                // is served past it) and retry next pass.
                // Re-review M2: retried on a doubling pass-count backoff (<= 1024
                // passes), never every pass (each retry is an mmap + allocations).
                s.stalled   = true;
                s.stallNext = scanSteps_ + s.stallGap;
                s.stallGap  = std::min<uint64_t>(s.stallGap * 2, 1024);
                if (unreadableStalls_++ % 64 == 0)
                    spdlog::error("RcState: segment {} has {} B that cannot be mapped — read_committed build "
                                  "stalled (fail closed; retried with backoff, {} stall(s))",
                                  s.segBase, m.written, unreadableStalls_);
            } else if (m.data == nullptr || s.bytePos >= m.size) {
                // This segment is walked (or gone / empty): the next one.
                s.stalled = false;
                s.stallGap = 1;
                const int64_t next = log.nextSegmentBaseAfter(s.segBase);
                if (next < 0) s.cursor = s.target;   // nothing beyond: the LEO is reached
                else { s.segBase = next; s.bytePos = 0; s.segGen = 0; s.readAheadTo = 0; }
            } else {
                s.stalled = false;
                s.stallGap = 1;
                bytes = scanResident_(s, m, static_cast<std::size_t>(budget > 0 ? budget : 1));
            }
        }
        if (bytes > maxStepBytes_) maxStepBytes_ = bytes;
        scannedBytes_ += bytes;
        if (s.cursor >= s.target) {
            finish_(s);
            log.releaseLazySealedMaps();   // D30 re-review M1: the walk is done
        }
        return bytes;
    }
    std::size_t scanResident_(Scanning& s, const rune::storage::Log::SegmentBytes& m, std::size_t budget) {
        using rune::protocol::RecordBatchReader;
        constexpr std::size_t kOverhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
        const uint8_t*    p     = m.data + s.bytePos;
        const std::size_t avail = m.size - s.bytePos;
        const std::size_t want  = std::min(avail, budget);
        const std::size_t aheadTo = std::min(m.size, s.bytePos + kReadAheadBudgets * budget);
        const auto tp = profile_ ? clock_() : 0;
        std::size_t res = resident_(p, want);
        if (profile_) profile_->probeNs += clock_() - tp;
        if (res < want) {
            // RR1-1: the cursor window is not wholly resident — re-request it
            // (and the read-ahead in front of it) until it is: a dropped hint or
            // evicted pages can never stall the build.  m2: while the cursor
            // waits at the same place, re-requests double their pass gap
            // (1, 2, 4 … 64 passes) — an async hint is anyway gated to one in
            // flight; a synchronous one is never a madvise per pass.
            if (s.waitAt != s.bytePos) { s.waitAt = s.bytePos; s.waitGap = 1; s.waitNext = 0; }
            if (scanSteps_ >= s.waitNext && requestReadAhead_(s, m, s.bytePos, aheadTo)) {
                s.waitNext = scanSteps_ + s.waitGap;
                s.waitGap  = std::min<uint64_t>(s.waitGap * 2, 64);
            }
        } else if (aheadTo > s.readAheadTo) {
            // Resident window: keep the read-ahead front kReadAheadBudgets ahead.
            (void)requestReadAhead_(s, m, std::max(s.readAheadTo, s.bytePos + want), aheadTo);
        }
        if (res < kOverhead) [[unlikely]] { ++nonResidentSteps_; return 0; }   // nothing resident: never block
        struct ParseClock {
            StepProfile* prof; uint64_t t0;
            ~ParseClock() { if (prof) prof->parseNs += clock_() - t0; }
        } parseClock{profile_, profile_ ? clock_() : 0};
        rune::transaction::RcBuilder builder(&tracker_, &aborted_);
        std::size_t pos = 0;
        while (pos + kOverhead <= res) {
            RecordBatchReader r(p + pos, res - pos);
            const int32_t total = r.totalLength();
            if (total <= 0 || pos + static_cast<std::size_t>(total) > avail) [[unlikely]]
                haltInvariant_("an unparseable batch inside the log's written bytes (RC build)");
            const auto len = static_cast<std::size_t>(total);
            if (pos + len > res) {
                // One batch larger than the budget is taken alone, once it is
                // wholly resident (its read-ahead is in flight meanwhile).
                if (pos != 0 || len <= want) break;
                if (resident_(p, len) < len) {
                    // N3: re-request the oversized extent with a doubling pass
                    // backoff (<= 64 passes), reset when the cursor moves.
                    if (s.oversizeAt != s.bytePos) { s.oversizeAt = s.bytePos; s.oversizeGap = 1; s.oversizeNext = 0; }
                    if (scanSteps_ >= s.oversizeNext &&
                        requestReadAhead_(s, m, s.bytePos, std::min(m.size, s.bytePos + len))) {
                        s.oversizeNext = scanSteps_ + s.oversizeGap;
                        s.oversizeGap  = std::min<uint64_t>(s.oversizeGap * 2, 64);
                    }
                    ++nonResidentSteps_;
                    break;
                }
                res = len;
            }
            const int64_t base = r.baseOffset();
            if (base >= s.target) { s.cursor = s.target; break; }
            const int64_t last = base + r.lastOffsetDelta();
            if (last >= s.cursor) {   // below the cursor: already applied (compaction re-walk)
                const auto out = builder.step(p + pos, len);
                if (out.outcome == rune::transaction::RcBuilder::Outcome::Invalid) [[unlikely]]
                    haltInvariant_("RcBuilder refused a whole batch inside the log's written bytes");
                if (psm_ != nullptr) feedProducerState_(r, out.outcome, p + pos, len);
                s.cursor = last + 1;
            }
            pos += len;
        }
        s.bytePos += pos;
        return pos;
    }
    // F15 S5 (§2.5, I3): the producer-state half of one build step — the same
    // classification RcBuilder made.  A REAL control batch is an epoch raise
    // (no sequence); an idempotent / transactional data batch records its
    // sequence range (an epoch older than an applied fence is ignored by the
    // PSM).  A CONTROL-flagged batch that is not a valid marker is ignored.
    // `markerFromRebuild`: the ring build keeps the producer-state semantics of
    // ProducerStateManager::rebuildFromLog it replaced (E2 final fix wave I3) —
    // a marker newer than any surviving data batch marks the producer
    // marker-only (D6 L1); the disk scan keeps its own (false).
    void feedProducerState_(const rune::protocol::RecordBatchReader& r, rune::transaction::RcBuilder::Outcome o,
                            const uint8_t* bytes, std::size_t len, bool markerFromRebuild = false) {
        using O = rune::transaction::RcBuilder::Outcome;
        const int64_t pid = r.producerId();
        if (pid < 0) return;
        if (o == O::CommitMarker || o == O::AbortMarker) {
            // D9 MAJOR-2: the marker's coordinatorEpoch restores the coordinator fence.
            psm_->fenceEpoch(psmTopic_, pid, psmPartition_, r.producerEpoch(), markerFromRebuild,
                             rune::storage::control_marker::coordinatorEpoch(bytes, len));
            return;
        }
        if ((r.attributes() & rune::protocol::batch::kAttrControl) != 0) return;
        const int32_t baseSeq = r.baseSequence();
        if (baseSeq < 0) return;
        psm_->update(psmTopic_, pid, psmPartition_, r.producerEpoch(), baseSeq, baseSeq + r.lastOffsetDelta(),
                     r.baseOffset());
    }
    // E2 Task 11 fix round 1: the memory-backed build — every batch of the ring
    // [ringStart, LEO) through the same RcBuilder the disk scan uses (tracker +
    // aborted index; reset_() emptied both) and the same producer-state feed
    // (feedProducerState_).  E2 final fix wave (I3): BUDGETED — RingScanning
    // applies whole batches up to ~`budget` bytes per advance step (at least
    // one), so a K-partition failover never walks K whole rings in one worker
    // pass.  A batch evicted before the build reached it is forgotten exactly
    // like a disk segment deleted by retention — but the build restarts from
    // the new ring start when the ring evicted batches it had NOT read yet (a
    // marker among them could close a transaction this build opened): never a
    // partial view served.  An unparseable batch in the ring is the same
    // invariant violation as on disk (halt: failing open would expose aborted
    // data).  No allocation.
    //
    // E2 Task 12 fix round 1 (finding B; controller ruling "option (c) refined"):
    // the ring may have evicted the FIRST batches of a transaction that is still
    // open.  A transactional data batch whose producer is not open in this
    // build and has left no COMMIT/ABORT marker earlier in the ring is NOT
    // provably its transaction's first batch when the ring start is above 0: the
    // transaction is opened conservatively AT the ring start (LSO pinned there:
    // read_committed never sees records between the ring start and the open
    // transaction's data until its marker lands).  After an in-ring marker of
    // the same producer (or with nothing evicted) the batch provably opens a new
    // transaction and opens it at its own offset, as before.  A transaction ALL
    // of whose batches were evicted leaves nothing in the ring to rebuild from:
    // unrecoverable (its producer is unknown — UnknownProducerId on its next
    // produce — and read_committed follows the evicted-cursor rule).
    // `ringMarkedPids_` is a fixed-capacity O(1) set (reserveRingRebuild),
    // build-local (cleared on every (re)start): never allocates; a producer it
    // cannot hold counts as unmarked — pinned at the ring start, the
    // conservative side — and the overflow is counted.
    void beginRingScan_(const RcFacts& f, const rune::storage::InMemoryLog& ring) {
        RingScanning rs;
        rs.cursor = rs.ringStart = ring.logStartOffset();
        rs.target  = ring.logEndOffset();
        rs.epoch   = f.epoch;
        rs.logGen  = f.logGeneration;
        rs.leadGen = f.leadershipGeneration;
        ringMarkedPids_.clear();
        st_ = rs;
    }
    std::size_t ringStep_(RingScanning& rs, rune::storage::InMemoryLog& ring, int32_t budget) {
        using namespace rune::protocol::batch;
        ++scanSteps_;
        if (rs.cursor < ring.logStartOffset()) [[unlikely]] {
            // The ring evicted batches this build had not read: restart it from
            // the new ring start (state, tail and producer state all rebuilt).
            ++ringScanRestarts_;
            if ((ringScanRestarts_ & (ringScanRestarts_ - 1)) == 0)
                spdlog::warn("RcState: the transient ring evicted past the read_committed build's cursor {} "
                             "(ring start {}) — build restarted from the ring start (restarts={})",
                             rs.cursor, ring.logStartOffset(), ringScanRestarts_);
            tracker_.clear();
            aborted_.clear();
            tail_.clear();
            if (psm_ != nullptr) psm_->clearPartition(psmTopic_, psmPartition_);
            const RcFacts f{true, rs.logGen, rs.leadGen, rs.epoch};
            beginRingScan_(f, ring);   // the same alternative: `rs` now holds the restarted build
        }
        RingScanning& cur = rs;
        rune::transaction::RcBuilder builder(&tracker_, &aborted_);
        const std::size_t want = static_cast<std::size_t>(budget > 0 ? budget : 1);
        std::size_t bytes = 0;
        while (cur.cursor < cur.target && bytes < want) {
            const auto v = ring.readView(cur.cursor, /*maxBytes=*/1);   // exactly one whole batch
            if (v.empty() || v.nextOffset <= cur.cursor) [[unlikely]]
                haltInvariant_("the ring has no readable batch below its LEO (RC build)");
            if (v.len < static_cast<std::size_t>(kRecordBatchOverhead)) [[unlikely]]
                haltInvariant_("a ring batch shorter than a batch header (RC build)");
            const rune::protocol::RecordBatchReader r(v.data, v.len);
            if (cur.ringStart > 0) {
                const auto attrs = static_cast<uint16_t>(r.attributes());
                const int64_t pid = r.producerId();
                if (pid >= 0 && (attrs & static_cast<uint16_t>(kAttrControl)) != 0) {
                    if (ringMarkedPids_.insert(pid) == FixedPidSet::Insert::Full) [[unlikely]]
                        ++ringMarkedPidsOverflows_;
                } else if (pid >= 0 && (attrs & static_cast<uint16_t>(kAttrTransactional)) != 0 &&
                           tracker_.firstOffsetOf(pid) < 0 && !ringMarkedPids_.contains(pid)) {
                    tracker_.onProduceBatch(pid, cur.ringStart);   // conservative pin; the step below keeps it
                    ++ringPinnedTxns_;
                }
            }
            const auto out = builder.step(v.data, v.len);
            if (out.outcome == rune::transaction::RcBuilder::Outcome::Invalid) [[unlikely]]
                haltInvariant_("RcBuilder refused a whole batch of the ring (RC build)");
            if (psm_ != nullptr) feedProducerState_(r, out.outcome, v.data, v.len, /*markerFromRebuild=*/true);
            bytes     += v.len;
            cur.cursor = v.nextOffset;
        }
        scannedBytes_ += bytes;
        if (bytes > maxStepBytes_) maxStepBytes_ = bytes;
        if (cur.cursor >= cur.target) {
            // The tail lies wholly at or above target: replayed in order (§2.2).
            for (const TailEvent& e : tail_) applyMarker_(e.producerId, e.offset, e.commit);
            tail_.clear();
            st_ = Ready{cur.epoch, cur.logGen, cur.leadGen};
        }
        return bytes;
    }
    void finish_(const Scanning& s) {
        // The tail lies wholly at or above target: replaying it after the scan
        // keeps the FIFO / range order (§2.2).
        for (const TailEvent& e : tail_) applyMarker_(e.producerId, e.offset, e.commit);
        tail_.clear();
        st_ = Ready{s.epoch, s.logGen, s.leadGen};
    }

    rune::transaction::LsoTracker      tracker_;
    int64_t cleanerLsoPublished_ = std::numeric_limits<int64_t>::min();   ///< D3b: last value published
    rune::transaction::AbortedTxnIndex aborted_;
    std::vector<TailEvent>             tail_;
    FixedPidSet                        ringMarkedPids_;   ///< E2 finding B: pass-local, fixed capacity
#if defined(__APPLE__)
    std::vector<char>                  residency_;   ///< mincore vector (pre-reserved)
#else
    std::vector<unsigned char>         residency_;
#endif
    std::variant<Unbuilt, Scanning, Ready, RingScanning> st_{Unbuilt{}};
    RcResidencyProbe probeFn_  = nullptr;
    void*            probeCtx_ = nullptr;
    RcReadAhead      readAheadFn_  = nullptr;
    void*            readAheadCtx_ = nullptr;
    bool             hintInFlight_ = false;   ///< survives reset_(): the kernel still owns the hint
    StepProfile*     profile_      = nullptr;
    rune::storage::ProducerStateManager* psm_ = nullptr;   ///< F15 S5: the worker's psm_ (not owned)
    int32_t          psmTopic_     = 0;
    int32_t          psmPartition_ = 0;
    uint64_t    readAheadRequests_        = 0;
    uint64_t    readAheadSkippedInFlight_ = 0;
    uint64_t    readAheadSkippedByIssuer_ = 0;
    uint64_t    residencyGrowths_         = 0;
    uint64_t    scanSteps_        = 0;
    uint64_t    unreadableStalls_ = 0;   ///< D30 MAJOR-2: passes stalled on an unmappable segment
    std::size_t maxStepBytes_     = 0;
    uint64_t    tailGrowths_      = 0;
    uint64_t    scannedBytes_     = 0;
    uint64_t    nonResidentSteps_ = 0;
    uint64_t    probeErrors_      = 0;
    uint64_t    ringPinnedTxns_        = 0;   ///< E2 finding B: open txns pinned at the ring start
    uint64_t    ringMarkedPidsOverflows_ = 0;   ///< producers the fixed set could not hold (pinned conservatively)
    uint64_t    ringScanRestarts_        = 0;   ///< I3: ring builds restarted (the ring evicted past the cursor)
public:
    [[nodiscard]] uint64_t ringScanRestarts() const noexcept { return ringScanRestarts_; }
    [[nodiscard]] uint64_t ringPinnedTxns() const noexcept { return ringPinnedTxns_; }
    [[nodiscard]] uint64_t ringMarkedPidsOverflows() const noexcept { return ringMarkedPidsOverflows_; }
    [[nodiscard]] const FixedPidSet& ringMarkedPids() const noexcept { return ringMarkedPids_; }
};

} // namespace rune::node
