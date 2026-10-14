#pragma once
// include/rune/client/catalog/CatalogReader.h — catalog history Task 13 (spec §7):
// "the client consumes [__catalog] with the existing Subscribe from offset 0, and
// replays it with the shared library … the client caches the replayed prefix."
//
// A CatalogReader subscribes TopicHandle::catalog() partition 0 from its cache end
// MINUS ONE (a one-event overlap), compares that event with the cached last event
// (the fingerprint chain pins the whole prefix), replays every later committed
// event through the shared verifying rune::catalog::CatalogReplayer (every
// fingerprint checked), and stops at the broker's caught-up marker.  The replayed
// prefix stays cached: the next refresh fetches only the tail.
//
// Staleness (C1): the reader holds no subscription between refreshes, so a
// truncation that regrew past the cache end cannot be heard.  Every refresh
// therefore re-reads the cache's last event and verifies it against the broker's
// event at that offset.  A mismatch, or a caught-up marker below the cache end,
// drops the cache and re-reads the whole catalog; a stale history is never served.
//
// Threading (shared-nothing, rings only):
//   - the record / marker callbacks run on the RUNNER thread and only COPY each
//     record (offset, value) — and the marker's high watermark — into the reader's
//     own SPSC ring (detail::OneToOneRingBuffer, 4 MiB, preallocated once per
//     reader; a record too large for one ring message is split over a
//     length-prefixed sequence of frames);
//   - the APP thread that owns the reader drains that ring, owns the replayer, the
//     cached events and the state.  One owner thread, bound at CONSTRUCTION (the
//     Client builds its reader eagerly in its constructor, on its owning thread):
//     there is no lazy creation and no first-caller claim, so no unsynchronized
//     shared state.  Every other thread: std::logic_error.
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
// served: until a re-validation completes, the next refresh re-validates again —
// resuming from the last VERIFIED offset, so a large catalog always makes progress
// (a ring overflow or a marker gap never restarts it from 0).
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
#include <cstdlib>
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
    /// CacheFull: the catalog has more events than CatalogReaderConfig::maxEvents or
    ///            more bytes than maxBytes.
    /// RecordTooLarge: one event cannot cross the reader's ring (`offset`, size in
    ///            `detail`) — raise CatalogReaderConfig::ringBytes.
    enum class Kind : uint8_t { Timeout, Replay, Closed, Rejected, CacheFull, RecordTooLarge } kind = Kind::Timeout;
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
    /// The most encoded event bytes the cache holds (CacheFull past it).
    std::size_t maxBytes = std::size_t{512} << 20;
    /// Back-off before re-subscribing after a lost session / an unanswered
    /// Subscribe / a retriable broker refusal (inside the caller's deadline).
    int64_t resubscribeBackoffNs = 50'000'000;
};

class CatalogReader {
public:
    /// An app-thread object; `client` must outlive it.  The CONSTRUCTING thread
    /// owns it for life (other threads: std::logic_error).
    explicit CatalogReader(Client& client, CatalogReaderConfig cfg = {});
    /// Unsubscribes and waits until no runner callback can touch the ring any
    /// more (Invoker mode: pumps doWork(); Thread mode: yields until the runner
    /// applied the release).  Never call it from a runner callback.
    ~CatalogReader();
    CatalogReader(const CatalogReader&)            = delete;
    CatalogReader& operator=(const CatalogReader&) = delete;

    // ---- Non-blocking pair (what the single-threaded fake drives, F7) -----------
    /// Subscribe from the cache end minus one (the overlap event is verified), or
    /// from the last verified offset of an unfinished re-validation (from 0 when the
    /// cache awaits one).
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
    [[nodiscard]] bool needsRevalidation() const noexcept;

    // ---- Observability (owner thread) ---------------------------------------------
    struct Stats {
        uint64_t subscribes        = 0;   // catalog Subscribes issued
        uint64_t readsEnded        = 0;   // CatalogReadEnded (a truncation) seen
        uint64_t revalidations     = 0;   // re-validations completed
        uint64_t prefixDivergences = 0;   // re-validations / overlap checks that dropped a cached tail
        uint64_t overlapChecks     = 0;   // refreshes whose cache-end overlap event matched
        uint64_t ringOverflows     = 0;   // the ring could not take a record: re-subscribed from the cache end
        uint64_t markerGaps        = 0;   // a marker above the replayed prefix: re-subscribed
        uint64_t resubscribes      = 0;   // lost session / unanswered / retriable refusal
        uint64_t reassembled       = 0;   // records that crossed the ring as a length-prefixed frame sequence
    };
    [[nodiscard]] const Stats& stats() const noexcept { return stats_; }

private:
    // Per-state variables (hard rule 3): each phase holds exactly what it needs.
    /// How a (re)subscription starts.  Append: from the replayed end (a continuation
    /// inside one refresh).  Tail: from the cache end - 1, verifying that overlap
    /// event.  Full: from `verified`, comparing every cached event up to the cache end.
    enum class Begin : uint8_t { Append, Tail, Full };
    struct Resume { Begin begin; int64_t verified; };
    /// A record being reassembled from a length-prefixed frame sequence.
    struct Assembly { uint32_t gen; int64_t offset; uint32_t total; };
    /// One live catalog subscription and its reassembly.
    struct Stream { uint32_t gen; std::unique_ptr<Subscription> sub; std::optional<Assembly> assembly; };
    /// Idle: nothing in flight; `next` is how the next refresh starts.
    struct Idle { Resume next; };
    /// Appending: every record is replayed onto the cache.
    struct Appending : Stream {};
    /// Revalidating: records below `cacheEnd` are compared with the cached events
    /// (`verified` = the next offset to compare).  Tail: `verified` starts at
    /// cacheEnd-1 and the offsets below it are NOT vouched for; Full: [0, verified) is.
    struct Revalidating : Stream { Begin begin; int64_t cacheEnd; int64_t verified; };
    /// Waiting to re-subscribe at `retryAtNs`.
    struct Backoff { int64_t retryAtNs; Resume resume; };
    using Phase = std::variant<Idle, Appending, Revalidating, Backoff>;

    enum class Step : uint8_t { Continue, Done, Restart, RestartFull };

    void checkOwner_();
    void subscribe_(Resume r);
    [[nodiscard]] Stream* stream_() noexcept;
    /// Where the in-flight read stands, to continue the same stream (a ring restart).
    [[nodiscard]] Resume continueOf_() const noexcept;
    /// Where to start again after a failure / lost session (re-verify the overlap).
    [[nodiscard]] Resume reopenOf_() const noexcept;
    void toAppending_() noexcept;
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
    std::unique_ptr<uint8_t, void (*)(void*)>      ringMem_;                // calloc: untouched pages stay free
    std::unique_ptr<detail::OneToOneRingBuffer>    ring_;
    rune::catalog::CatalogReplayer                 replayer_;
    std::vector<rune::catalog::RecordedEvent>      events_;
    std::vector<uint32_t>                          eventBytes_;             // encoded size of each cached event
    std::size_t                                    cachedBytes_ = 0;
    Phase                                          phase_ = Idle{Resume{Begin::Tail, 0}};
    uint32_t                                       nextGen_ = 0;
    std::vector<uint8_t>                           assemblyBuf_;            // capacity persists
    std::vector<std::unique_ptr<Subscription>>     retired_;                // unsubscribed, not yet finished
    const std::thread::id                          owner_;                  // the constructing thread, immutable
    Stats                                          stats_;
    static constexpr std::size_t                   kMaxRetired = 8;
};

} // namespace rune::client
