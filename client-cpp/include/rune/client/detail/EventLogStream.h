#pragma once
// include/rune/client/detail/EventLogStream.h — the event-log reader design, shared by the catalog reader
// (catalog history Task 13, spec §7) and the __security reader (authentication Task 8).  The text below is the
// catalog reader's contract, unchanged; "catalog" reads "the event log" for either policy.
//
// "the client consumes [__catalog] with the existing Subscribe from offset 0, and
// replays it with the shared library … the client caches the replayed prefix."
//
// A CatalogReader subscribes TopicHandle::catalog() partition 0 from its cache end,
// replays every committed event through the shared verifying
// rune::catalog::CatalogReplayer (every fingerprint checked), and stops at the
// broker's caught-up marker.  The replayed prefix stays cached: the next refresh
// fetches only the tail.
//
// The cache is (prefix, HISTORY EPOCH) (Task 13 fix round 2).  The serving broker
// opens every catalog stream with its history-epoch note — before any record —
// and repeats the epoch in the caught-up marker (DataMessages.h, client version
// 8).  The epoch names one committed history of that broker's `__catalog`: it
// changes on every truncation below a high watermark the broker served, and two
// brokers never share a value.  So every record of one stream belongs to the
// history its note names, and:
//   - a stream whose epoch equals the cache's continues it (Append from the cache
//     end) — a refresh, a continuation after a ring overflow / marker gap, a
//     resume after a lost session, a refused or timed-out Subscribe, or a read the
//     broker ended (NotLeader: a truncation below a served HW), alike;
//   - any other epoch drops the whole cache and re-reads from offset 0 (the new
//     stream's note adopts its epoch).
// No event-level check is needed: the epoch covers the whole prefix.  A stale
// history is never served.
//
// Threading (shared-nothing, rings only):
//   - the record / note / marker callbacks run on the RUNNER thread and only COPY
//     each record (offset, value) — and the epoch / high watermark — into the
//     reader's own SPSC ring (detail::OneToOneRingBuffer, 4 MiB, preallocated once
//     per reader; a record too large for one ring message is split over a
//     length-prefixed sequence of frames);
//   - the APP thread that owns the reader drains that ring, owns the replayer, the
//     cached events, the epoch and the state.  One owner thread, bound at
//     CONSTRUCTION (the Client builds its reader eagerly in its constructor, on its
//     owning thread): there is no lazy creation and no first-caller claim.  EVERY
//     member — refresh, events(), state(), stats(), nextOffset(), historyEpoch() —
//     and Client::catalogReader() throw std::logic_error on any other thread, in
//     every runner mode.
//
// Every refresh ends: OK at the marker, Replay (naming the offset) on a
// verification failure, Timeout at the caller's deadline — a long-leaderless
// catalog correctly withholds the marker, so the wait is bounded (Task 12 F1) —
// Rejected for a broker refusal (or a broker that sends no epoch), Closed once the
// Client closed.  Never a hang, never a silently skipped event.
//
// Cold path (control plane).  Bounded: the ring is fixed; the reassembly buffer is
// bounded by the largest event; the cache by maxEvents / maxBytes (CacheFull).
#include <rune/client/Completion.h>
#include <rune/client/subscriber/Subscription.h>
#include <rune/client/TopicHandle.h>

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
    ///            e.g. UnknownTopic from a broker older than client version 8), or
    ///            sent catalog records / a marker with no history-epoch note.
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

namespace detail {

/// Policy `P` (CatalogLogPolicy / SecurityLogPolicy) names the log: its topic, replayer, event and state types, its
/// decode and its error texts.  Everything else — the ring, the frames, the epoch / marker rules, the phases — is
/// the one catalog-reader design, shared (the Task 8 extraction; behaviour-neutral for the catalog).
template <class P>
class EventLogStream {
public:
    /// An app-thread object; `client` must outlive it.  The CONSTRUCTING thread
    /// owns it for life (other threads: std::logic_error).
    explicit EventLogStream(Client& client, CatalogReaderConfig cfg = {});
    /// Unsubscribes and waits until no runner callback can touch the ring any
    /// more (Invoker mode: pumps doWork(); Thread mode: yields until the runner
    /// applied the release).  Never call it from a runner callback.
    ~EventLogStream();
    EventLogStream(const EventLogStream&)            = delete;
    EventLogStream& operator=(const EventLogStream&) = delete;

    // ---- Non-blocking pair (what the single-threaded fake drives, F7) -----------
    /// Subscribe from the cache end; its epoch note decides whether the cache
    /// continues (same epoch) or is dropped and re-read from 0.
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

    /// The cached, replayed prefix [0, events().size()) in offset order, all of it
    /// read under historyEpoch().  The committed history as of the last refresh
    /// that returned OK; after a failed refresh, the verified prefix (the next
    /// refresh continues it only if the broker's epoch is unchanged).  Owner
    /// thread only (std::logic_error otherwise).
    [[nodiscard]] std::span<const typename P::Recorded> events() const;
    [[nodiscard]] const typename P::State& state() const;
    [[nodiscard]] int64_t nextOffset() const;
    /// The epoch the cache was read under (nullopt: none yet — nothing cached).
    [[nodiscard]] std::optional<uint64_t> historyEpoch() const;

    // ---- Observability (owner thread) ---------------------------------------------
    struct Stats {
        uint64_t subscribes        = 0;   // catalog Subscribes issued
        uint64_t readsEnded        = 0;   // CatalogReadEnded (a truncation) seen
        uint64_t epochChanges      = 0;   // a stream's epoch differed from the cache's: cache dropped, re-read from 0
        uint64_t contradictions    = 0;   // a marker inconsistent with its own stream: cache dropped, re-read from 0
        uint64_t ringOverflows     = 0;   // the ring could not take a record: re-subscribed from the cache end
        uint64_t markerGaps        = 0;   // a marker above the replayed prefix: re-subscribed
        uint64_t resubscribes      = 0;   // lost session / unanswered / retriable refusal
        uint64_t reassembled       = 0;   // records that crossed the ring as a length-prefixed frame sequence
    };
    [[nodiscard]] const Stats& stats() const;

private:
    friend class rune::client::Client;   // Client::catalogReader() / securityReader() enforce the owner thread too

    // Per-state variables (hard rule 3): each phase holds exactly what it needs.
    /// A record being reassembled from a length-prefixed frame sequence.
    struct Assembly { uint32_t gen; int64_t offset; uint32_t total; };
    /// One live catalog subscription and its reassembly.
    struct Stream { uint32_t gen; std::unique_ptr<Subscription> sub; std::optional<Assembly> assembly; };
    /// Idle: nothing in flight.
    struct Idle {};
    /// Opening: subscribed from the cache end; the stream's epoch note is due
    /// before anything else of it.
    struct Opening : Stream {};
    /// Streaming: the note named `epoch` (== the cache's); every record is
    /// replayed onto the cache.
    struct Streaming : Stream { uint64_t epoch; };
    /// Waiting to re-subscribe at `retryAtNs`.
    struct Backoff { int64_t retryAtNs; };
    using Phase = std::variant<Idle, Opening, Streaming, Backoff>;

    enum class Step : uint8_t { Continue, Done, Restart };

    void checkOwner_() const;
    void subscribe_();
    [[nodiscard]] Stream* stream_() noexcept;
    void retire_(std::unique_ptr<Subscription> sub);
    void pruneRetired_();
    void finishIdle_();
    [[nodiscard]] uint32_t currentGen_() const noexcept;
    /// Drains the ring; returns the first outcome a frame produced.
    Step drain_(std::optional<CatalogResult<void>>& result);
    Step onNote_(uint64_t epoch);
    Step onRecord_(int64_t offset, std::span<const uint8_t> bytes, std::optional<CatalogResult<void>>& result);
    Step onMarker_(int64_t hw, uint64_t epoch, std::optional<CatalogResult<void>>& result);
    Step append_(int64_t offset, std::span<const uint8_t> bytes, std::optional<CatalogResult<void>>& result);
    Step protocolError_(const char* what, int64_t offset, std::optional<CatalogResult<void>>& result);
    /// The whole cache is not the broker's history (another epoch / a contradiction):
    /// drop it; the next stream re-reads from 0 and adopts its epoch.
    void dropCache_();
    void truncateCache_(std::size_t keep);

    Client&                                        client_;
    CatalogReaderConfig                            cfg_;
    std::unique_ptr<uint8_t, void (*)(void*)>      ringMem_;                // calloc: untouched pages stay free
    std::unique_ptr<detail::OneToOneRingBuffer>    ring_;
    typename P::Replayer                           replayer_;
    std::vector<typename P::Recorded>             events_;
    std::vector<uint32_t>                          eventBytes_;             // encoded size of each cached event
    std::size_t                                    cachedBytes_ = 0;
    Phase                                          phase_ = Idle{};
    std::optional<uint64_t>                        cacheEpoch_;             // every cached event was read under it
    uint32_t                                       nextGen_ = 0;
    std::vector<uint8_t>                           assemblyBuf_;            // capacity persists
    std::vector<std::unique_ptr<Subscription>>     retired_;                // unsubscribed, not yet finished
    const std::thread::id                          owner_;                  // the constructing thread, immutable
    Stats                                          stats_;
    static constexpr std::size_t                   kMaxRetired = 8;
};

} // namespace detail
} // namespace rune::client
