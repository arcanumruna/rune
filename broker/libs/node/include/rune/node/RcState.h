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
//                segment byte cursor}
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
//       2. Unbuilt, Leader and leaderComplete_ (I2) → Scanning{logStart, LEO}.
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
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>
#include <rune/transaction/ReadCommittedView.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
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
    bool     leaderComplete       = false;
    uint64_t logGeneration        = 0;
    uint64_t leadershipGeneration = 0;
    int32_t  epoch                = 0;
};

class RcState {
public:
    /// Per-pass build budget (bytes parsed per advance step).  Set from
    /// BM_RcBuildStep with a cold page cache (p99 per step <= 20 µs target).
    /// k3s-node-01, 7c814b6 (async FADVISE hints, mincore gate), cold cache:
    /// p99 16.6 µs @16 KiB, 18.4 µs @32 KiB, 21.9 µs @64 KiB → 32 KiB.
    static constexpr int32_t     kDefaultScanBytesPerPass = 32 * 1024;
    static constexpr std::size_t kTailReserve             = 256;
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
    };
    struct Ready {
        int32_t  epoch   = 0;
        uint64_t logGen  = 0;
        uint64_t leadGen = 0;
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
    void setResidencyProbe(RcResidencyProbe fn, void* ctx) noexcept { probeFn_ = fn; probeCtx_ = ctx; }
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
    [[nodiscard]] bool scanning() const noexcept { return std::holds_alternative<Scanning>(st_); }
    /// 0 Unbuilt, 1 Scanning, 2 Ready.
    [[nodiscard]] int statusCode() const noexcept { return static_cast<int>(st_.index()); }

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
    /// `log` null = a memory-backed partition (nothing to scan: Ready at once;
    /// see F22).  Returns the bytes this step parsed (0: no scan work).
    std::size_t advance(const RcFacts& f, rune::storage::Log* log, int32_t budget) {
        (void)revalidate(f);
        if (Ready* r = std::get_if<Ready>(&st_)) [[likely]] {
            r->epoch = f.epoch;                            // epoch bump, same log: still exact
            if (log != nullptr && !aborted_.ranges().empty() &&
                aborted_.ranges().front().lastOffset < log->logStartOffset()) [[unlikely]]
                aborted_.pruneBelow(log->logStartOffset());   // retention (cold)
            return 0;
        }
        if (std::holds_alternative<Unbuilt>(st_)) {
            if (!f.leader || !f.leaderComplete) return 0;  // I2: wait for leaderComplete_
            if (log == nullptr) {
                st_ = Ready{f.epoch, f.logGeneration, f.leadershipGeneration};
                return 0;
            }
            Scanning sc;
            sc.cursor = sc.segBase = log->logStartOffset();
            sc.target = log->logEndOffset();
            sc.epoch = f.epoch; sc.logGen = f.logGeneration; sc.leadGen = f.leadershipGeneration;
            st_ = sc;
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
        if (s.cursor < s.target) {
            const auto m = log.segmentBytesAt(s.segBase);
            if (m.gen != s.segGen) { s.segGen = m.gen; s.bytePos = 0; s.readAheadTo = 0; }
            if (m.data == nullptr || s.bytePos >= m.size) {
                // This segment is walked (or gone / empty): the next one.
                const int64_t next = log.nextSegmentBaseAfter(s.segBase);
                if (next < 0) s.cursor = s.target;   // nothing beyond: the LEO is reached
                else { s.segBase = next; s.bytePos = 0; s.segGen = 0; s.readAheadTo = 0; }
            } else {
                bytes = scanResident_(s, m, static_cast<std::size_t>(budget > 0 ? budget : 1));
            }
        }
        if (bytes > maxStepBytes_) maxStepBytes_ = bytes;
        scannedBytes_ += bytes;
        if (s.cursor >= s.target) finish_(s);
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
                s.cursor = last + 1;
            }
            pos += len;
        }
        s.bytePos += pos;
        return pos;
    }
    void finish_(const Scanning& s) {
        // The tail lies wholly at or above target: replaying it after the scan
        // keeps the FIFO / range order (§2.2).
        for (const TailEvent& e : tail_) applyMarker_(e.producerId, e.offset, e.commit);
        tail_.clear();
        st_ = Ready{s.epoch, s.logGen, s.leadGen};
    }

    rune::transaction::LsoTracker      tracker_;
    rune::transaction::AbortedTxnIndex aborted_;
    std::vector<TailEvent>             tail_;
#if defined(__APPLE__)
    std::vector<char>                  residency_;   ///< mincore vector (pre-reserved)
#else
    std::vector<unsigned char>         residency_;
#endif
    std::variant<Unbuilt, Scanning, Ready> st_{Unbuilt{}};
    RcResidencyProbe probeFn_  = nullptr;
    void*            probeCtx_ = nullptr;
    RcReadAhead      readAheadFn_  = nullptr;
    void*            readAheadCtx_ = nullptr;
    bool             hintInFlight_ = false;   ///< survives reset_(): the kernel still owns the hint
    StepProfile*     profile_      = nullptr;
    uint64_t    readAheadRequests_        = 0;
    uint64_t    readAheadSkippedInFlight_ = 0;
    uint64_t    readAheadSkippedByIssuer_ = 0;
    uint64_t    residencyGrowths_         = 0;
    uint64_t    scanSteps_        = 0;
    std::size_t maxStepBytes_     = 0;
    uint64_t    tailGrowths_      = 0;
    uint64_t    scannedBytes_     = 0;
    uint64_t    nonResidentSteps_ = 0;
    uint64_t    probeErrors_      = 0;
};

} // namespace rune::node
