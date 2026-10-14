#pragma once
// include/rune/client/catalog/CatalogReader.h — catalog history Task 13 (spec §7):
// "the client consumes [__catalog] with the existing Subscribe from offset 0, and
// replays it with the shared library … the client caches the replayed prefix."
//
// A CatalogReader subscribes TopicHandle::catalog() partition 0 from its cache end,
// replays every committed event through the shared verifying rune::catalog::
// CatalogReplayer (every fingerprint checked), and stops at the broker's caught-up
// marker.  The replayed prefix stays cached: the next refresh fetches only the tail.
//
// Threading (shared-nothing, rings only):
//   - the record / marker callbacks run on the RUNNER thread and only COPY each
//     record (offset, value) — and the marker's high watermark — into the reader's
//     own SPSC ring (detail::OneToOneRingBuffer, 4 MiB, preallocated once per
//     reader; a record too large for one ring message is split over a
//     length-prefixed sequence of frames);
//   - the APP thread that owns the reader drains that ring, owns the replayer, the
//     cached events and the state.  One owner thread: a reader is never shared.
//
// Every refresh ends: OK at the marker, Replay (naming the offset) on a
// verification failure, Timeout at the caller's deadline — a long-leaderless
// catalog correctly withholds the marker, so the wait is bounded (Task 12 F1) —
// Rejected for a broker refusal, Closed once the Client closed.  Never a hang,
// never a silently skipped event.
//
// Truncation (Task 12): a broker whose catalog replica truncated below a high
// watermark it had served ends every catalog read ONCE with the retriable
// NotLeader (SubscriptionFailReason::CatalogReadEnded).  The cached prefix may
// then no longer be the committed history: the reader re-reads from offset 0 and
// RE-VALIDATES every cached event against the new history (event equality —
// fingerprints included — at every offset up to the cut).  The cache is kept up to
// the first difference and dropped from there (the replayer is rebuilt over the
// kept prefix), and the new history is replayed on top.  A stale history is never
// served: until a re-validation completes, the next refresh re-validates again.
//
// Cold path (control plane).  Bounded: the ring is fixed; the reassembly buffer is
// bounded by the largest event; the cache by maxEvents (CacheFull past it).
#include <rune/client/Completion.h>
#include <rune/client/subscriber/Subscription.h>

#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace rune::client {

class Client;
namespace detail { class OneToOneRingBuffer; }

struct CatalogReadError {
    /// Timeout:   the deadline passed before the caught-up marker (the marker is
    ///            withheld while the serving broker's committed prefix is not
    ///            current — e.g. a long-leaderless catalog).
    /// Replay:    the shared replayer refused the event at `offset` (decode, gap,
    ///            apply or fingerprint mismatch — `detail` is replayer's describe());
    ///            also an as-of point the log does not reach (offset -1).
    /// Closed:    the Client closed.
    /// Rejected:  the broker refused the catalog Subscribe (`brokerErrorCode`,
    ///            e.g. UnknownTopic from a broker older than client version 8).
    /// CacheFull: the catalog has more events than CatalogReaderConfig::maxEvents.
    enum class Kind : uint8_t { Timeout, Replay, Closed, Rejected, CacheFull } kind = Kind::Timeout;
    std::string detail;
    int64_t     offset          = -1;
    uint16_t    brokerErrorCode = 0;   // Rejected only
};
template <class T> using CatalogResult = std::expected<T, CatalogReadError>;

struct CatalogReaderConfig {
    /// The runner → app ring (a power of two; preallocated once per reader).
    std::size_t ringBytes = std::size_t{4} << 20;
    /// The most events the cache holds (the bound on the cold-path cache).
    std::size_t maxEvents = std::size_t{1} << 20;
    /// Back-off before re-subscribing after a lost session / an unanswered
    /// Subscribe / a retriable broker refusal (inside the caller's deadline).
    int64_t resubscribeBackoffNs = 50'000'000;
};

class CatalogReader {
public:
    /// An app-thread object; `client` must outlive it.  The first thread that
    /// calls beginRefresh / refresh owns it (other threads: std::logic_error).
    explicit CatalogReader(Client& client, CatalogReaderConfig cfg = {});
    /// Unsubscribes and waits until no runner callback can touch the ring any
    /// more (Invoker mode: pumps doWork(); Thread mode: yields until the runner
    /// applied the release).  Never call it from a runner callback.
    ~CatalogReader();
    CatalogReader(const CatalogReader&)            = delete;
    CatalogReader& operator=(const CatalogReader&) = delete;

    // ---- Non-blocking pair (what the single-threaded fake drives, F7) -----------
    /// Subscribe from the cache end (from 0 when the cache awaits re-validation).
    /// Accepted: pollRefresh() will return exactly one result.  BackPressured: a
    /// refresh is already in flight, or earlier subscriptions are still being
    /// released — poll / pump and retry.
    OpResult beginRefresh();
    /// Drain the ring; nullopt = not done yet (the caller keeps pumping doWork()).
    /// `nowNs` / `deadlineNs`: the caller's clock; at `deadlineNs` the refresh
    /// ends Timeout (the cached prefix is kept as it was).
    std::optional<CatalogResult<void>> pollRefresh(int64_t nowNs, int64_t deadlineNs);

    // ---- Blocking ----------------------------------------------------------------
    /// beginRefresh + pollRefresh until done — pumps doWork() on the Invoker
    /// owner, idles on a Thread-mode app thread.  From a runner callback (or a
    /// non-owner Invoker thread): std::logic_error.
    CatalogResult<void> refresh(std::chrono::milliseconds timeout);

    /// The cached, replayed prefix [0, events().size()) in offset order.  The
    /// committed history as of the last refresh that returned OK; after a failed
    /// refresh, the verified prefix (needsRevalidation(): it is re-validated
    /// before it is served again by Client::catalogHistory / catalogAsOf / catalogDiff).
    [[nodiscard]] std::span<const rune::catalog::RecordedEvent> events() const noexcept { return events_; }
    [[nodiscard]] const rune::catalog::CatalogState& state() const noexcept { return replayer_.state(); }
    [[nodiscard]] int64_t nextOffset() const noexcept { return replayer_.nextOffset(); }
    [[nodiscard]] bool needsRevalidation() const noexcept { return mode_ == Mode::Revalidate; }

    // ---- Observability (owner thread) ---------------------------------------------
    struct Stats {
        uint64_t subscribes        = 0;   // catalog Subscribes issued
        uint64_t readsEnded        = 0;   // CatalogReadEnded (a truncation) seen
        uint64_t revalidations     = 0;   // re-validations completed
        uint64_t prefixDivergences = 0;   // re-validations that dropped a cached tail
        uint64_t ringOverflows     = 0;   // the ring could not take a record: re-subscribed from the cache end
        uint64_t markerGaps        = 0;   // a marker above the replayed prefix: re-subscribed
        uint64_t resubscribes      = 0;   // lost session / unanswered / retriable refusal
        uint64_t reassembled       = 0;   // records that crossed the ring as a length-prefixed frame sequence
    };
    [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

private:
    // Per-state variables (hard rule 3).  Idle: nothing in flight.
    struct Idle {};
    /// Appending: subscribed from the cache end; every record is replayed.
    struct Appending { uint32_t gen; std::unique_ptr<Subscription> sub; };
    /// Revalidating: subscribed from 0; records below `cacheEnd` are compared with
    /// the cached events (`verified` = the next offset to compare).
    struct Revalidating { uint32_t gen; std::unique_ptr<Subscription> sub; int64_t cacheEnd; int64_t verified; };
    /// Waiting to re-subscribe (in `resumeAs` mode) at `retryAtNs`.
    enum class Mode : uint8_t { Append, Revalidate };
    struct Backoff { int64_t retryAtNs; Mode resumeAs; };
    using Phase = std::variant<Idle, Appending, Revalidating, Backoff>;

    /// A record being reassembled from a length-prefixed frame sequence.
    struct Assembly { uint32_t gen; int64_t offset; uint32_t total; };

    enum class Step : uint8_t { Continue, Done, Restart, RestartAppend };

    void checkOwner_();
    void subscribe_(Mode m);
    void retire_(std::unique_ptr<Subscription> sub);
    void pruneRetired_();
    void finishIdle_();
    [[nodiscard]] uint32_t currentGen_() const noexcept;
    /// Drains the ring; returns the first outcome a frame produced.
    Step drain_(std::optional<CatalogResult<void>>& result);
    Step onRecord_(int64_t offset, std::span<const uint8_t> bytes, std::optional<CatalogResult<void>>& result);
    Step onMarker_(int64_t hw, std::optional<CatalogResult<void>>& result);
    Step append_(int64_t offset, std::span<const uint8_t> bytes, std::optional<CatalogResult<void>>& result);
    void truncateCache_(std::size_t keep);

    Client&                                        client_;
    CatalogReaderConfig                            cfg_;
    std::unique_ptr<uint8_t[]>                     ringMem_;
    std::unique_ptr<detail::OneToOneRingBuffer>    ring_;
    rune::catalog::CatalogReplayer                 replayer_;
    std::vector<rune::catalog::RecordedEvent>      events_;
    Phase                                          phase_ = Idle{};
    Mode                                           mode_  = Mode::Append;   // how the next refresh starts
    uint32_t                                       nextGen_ = 0;
    std::optional<Assembly>                        assembly_;
    std::vector<uint8_t>                           assemblyBuf_;            // capacity persists
    std::vector<std::unique_ptr<Subscription>>     retired_;                // unsubscribed, not yet finished
    std::optional<std::thread::id>                 owner_;
    Stats                                          stats_;
    static constexpr std::size_t                   kMaxRetired = 8;
};

} // namespace rune::client
