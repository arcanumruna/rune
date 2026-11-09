// include/rune/client/lease/LeaseWatch.h - LeaseWatch<T> / LeaseWatchBase: a live view of the leases under a set of key prefixes, built from a
// hash-range scan handed off to a subscription (Task 16; spec 12.4).
//
// startFromNow(prefixes) makes, per partition, the exact handoff of spec 12.4:
//   1. Scan: for each surviving prefix, ScanLeases pages 1..n.  Page i covers the hash range (h_{i-1}, h_i] at its own exclusive asOf_i.
//   2. Subscribe the partition from the asOf of the FIRST page (the minimum: asOf only grows) the moment that page answers.
//   3. Apply a record at offset o on key k only if k matches a prefix and o >= asOf(the page of that prefix's scan that covers h(k)); the rule
//      stays active for the partition's whole epoch (rule 3 is over hash ranges, never key order).
// h is the broker's scan hash (protocol/LeaseScanHash.h, seeded with the topic id).
//
// Threading.  Everything the application sees runs on the OWNER thread inside doWork(nowNs): the handler, the observers, the scans.  The
// subscription handler runs on the client's duty thread and does only one thing: it copies {partition, offset, lease op, version, key, value}
// into the watch's RECORD RING (a standalone Completions ring, the only cross-thread memory).  When the ring cannot take a record plus the
// markers reserved for every partition, the handler writes ONE Overflow{partition, generation, cursor = this record's offset} marker into the
// reserved space and drops every later record of that subscription (the drop flag is the handler's own state).  doWork drains the ring in
// order; a record of a partition that is still scanning goes to that partition's HANDOFF LIST (handoffBuffer records, handoffBuffer * 192
// bytes); a full list is an overflow at that record too.  An overflow never drops or rescans: the subscription is cancelled, the partition's
// scan finishes, the list is applied, and the partition re-subscribes from the cursor (resubscribes()).
//
// Identity is (key, lease_version).  Granted for a held (k, v) is ignored; Granted at another version emits Vanished for the old one, then
// Granted; Updated / Revoked / Expired for an unknown (k, v) are ignored.  A partition whose subscription breaks (or whose scan is answered
// with a transient non-retriable error) is re-run, after a per-partition exponential backoff (1 ms doubling to a 1 s cap, reset when the
// partition reaches Live); a DEFINITIVE answer (NotAuthorized, UnknownTopic, InvalidRequest, or the duty thread refusing the request as
// malformed) fails the watch (failed(), one Failed event): its live entries are marked unconfirmed, the partition is rescanned and re-subscribed, and when its
// scan ends every entry still unconfirmed is emitted Vanished and removed.  isReady() STAYS true during a re-run (a recipe keeps seeing the
// old set until the rescan settles; spec 3.3); Ready is emitted once.
//
// Memory.  Everything is allocated in the constructor (live set, name heaps, page bounds, handoff lists, record ring); doWork allocates
// nothing.  Only (re)subscribing builds a std::function (cold, once per subscription).  No locks, no atomics, no virtual calls.
// Moving or copying a watch is deleted (the subscription handlers hold pointers into it).  The destructor unsubscribes every subscription and
// waits (capped) for each to finish before the ring is freed; in Invoker mode it pumps client.doWork().
// Names (Task 20): a key belongs to the name N iff it starts with N + '/' and the rest has no further '/' (one level); lowest(id) is the live
// lease of that name with the smallest version.  Register names BEFORE startFromNow.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/detail/FlatIndexMap.h>
#include <rune/client/lease/LeaseTopic.h>
#include <rune/client/subscriber/Subscription.h>

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <variant>
#include <vector>

namespace rune::client {

enum class LeaseEventKind : uint8_t { Granted, Updated, Revoked, Expired, Vanished, Ready, Failed };

struct LeaseEvent {
    LeaseEventKind kind      = LeaseEventKind::Ready;
    int32_t        topicId   = -1;
    int32_t        partition = 0;
    int64_t        offset    = -1;   ///< the log offset of the record (a scanned row: its lease_granted_offset); -1 = none
    std::span<const uint8_t> key;
    int64_t        version   = 0;
    std::span<const uint8_t> bytes;  ///< fromScan: a lease view row (LeasesRow layout); else the record's value (a log row); Vanished: empty
    bool           fromScan  = false;
};
using LeaseEventFn = void (*)(void* ctx, const LeaseEvent& ev);

struct LeaseWatchOptions {
    uint32_t handoffBuffer   = 4096;   ///< records buffered per partition while it scans (and the record ring's size: handoffBuffer * 192 bytes)
    uint32_t maxLive         = 8192;   ///< the most live leases the watch holds (exceeding it fails the watch)
    uint32_t maxPagesPerScan = 1024;   ///< page bounds kept per partition (all prefixes); exceeding it fails the watch
    uint32_t names           = 0;      ///< the most names registerName accepts
    uint32_t waitersPerName  = 0;      ///< the most live leases one name holds (exceeding it fails the watch)
    uint16_t maxRows         = 1024;   ///< ScanLeases page size (1..1024)
};

class LeaseWatchBase {
public:
    static constexpr uint32_t kMaxPrefixes  = 16;
    static constexpr uint32_t kMaxPrefixLen = rune::protocol::client::kScanLeasesMaxPrefixBytes;   ///< 127: the wire's limit
    static constexpr uint32_t kMaxObservers = 8;
    static constexpr uint32_t kMaxKey       = rune::protocol::kMaxLeaseKeyBytes;

    LeaseWatchBase(Client& client, detail::LeaseTopicCore& topic, const LeaseWatchOptions& opts = {});
    ~LeaseWatchBase();
    LeaseWatchBase(const LeaseWatchBase&)            = delete;
    LeaseWatchBase& operator=(const LeaseWatchBase&) = delete;
    LeaseWatchBase(LeaseWatchBase&&)                 = delete;
    LeaseWatchBase& operator=(LeaseWatchBase&&)      = delete;

    /// Starts the watch (once): up to 16 prefixes (each up to 127 bytes; an empty prefix means every key).  The prefixes are copied in; every
    /// prefix that extends another one is dropped, exact duplicates are kept once.  InvalidArgument: no prefix, too many or too long, already
    /// started, a closed topic handle or one whose schema layout is unknown, or a topic whose widest key plus lease view row (plus the
    /// 48-byte ring record header) does not fit the 2048-byte record ring payload (kRecPayload): such a row could never be watched.
    [[nodiscard]] OpResult startFromNow(std::span<const std::span<const uint8_t>> prefixes);

    int  doWork(int64_t nowNs);
    [[nodiscard]] bool isReady() const noexcept { return ready_ && !failed_; }
    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] uint64_t resubscribes() const noexcept { return resubscribes_; }

    void setHandler(LeaseEventFn fn, void* ctx) noexcept { handler_ = Cb{fn, ctx}; }
    /// Up to 8 observers (the LeaseSession registers one).  false when full.  An observer must outlive the watch or be removed first,
    /// or give a `gone` callback: the watch calls it (owner thread) from its destructor, so the observer can forget the watch.
    using WatchGoneFn = void (*)(void* ctx, LeaseWatchBase* watch);
    [[nodiscard]] bool addObserver(LeaseEventFn fn, void* ctx, WatchGoneFn gone = nullptr) noexcept;
    bool removeObserver(void* ctx) noexcept;
    [[nodiscard]] uint32_t observerCount() const noexcept;   ///< observers registered (tests)

    /// Names: -1 when full, after startFromNow, empty/too long, or when another name collides in the index.
    [[nodiscard]] int32_t registerName(std::span<const uint8_t> name);
    /// The live lease under `id` with the smallest version and its key (valid until the next doWork); nullopt when none.
    [[nodiscard]] std::optional<std::pair<int64_t, std::span<const uint8_t>>> lowest(int32_t id) const noexcept;
    /// The live lease under `id` with the largest version below `version` and its key (valid until the next doWork); nullopt when none.  A
    /// linear pass over the name's entries: bounded by waitersPerName.
    [[nodiscard]] std::optional<std::pair<int64_t, std::span<const uint8_t>>> predecessor(int32_t id, int64_t version) const noexcept;
    /// Binds ONE callback to the name `id` (the recipes): it runs, on the owner thread inside doWork, for every Granted / Updated / Revoked /
    /// Expired / Vanished event whose key belongs to the name, AFTER the live set and the name's heap reflect the event; Ready and Failed go to
    /// every bound name once.  false: an unknown id, a name that is already bound, no callback.  `gone` (optional) is called from the watch's
    /// destructor, so the binder can forget the watch.  unbindName removes the binding made with the same `ctx`.
    [[nodiscard]] bool bindName(int32_t id, LeaseEventFn fn, void* ctx, WatchGoneFn gone = nullptr) noexcept;
    bool unbindName(int32_t id, void* ctx) noexcept;

    [[nodiscard]] int32_t topicId() const noexcept { return core_.topicId(); }
    // ---- observers (tests) ----
    [[nodiscard]] uint32_t liveCount() const noexcept { return liveCount_; }
    [[nodiscard]] bool     isLive(std::span<const uint8_t> key, int64_t version) const noexcept;
    template <class F>
    void forEachLive(F&& f) const {
        for (const LiveEntry& e : live_) if (e.used) f(std::span<const uint8_t>(e.key, e.keyLen), e.version, e.partition);
    }
    [[nodiscard]] uint32_t prefixCount() const noexcept { return nPrefixes_; }
    [[nodiscard]] uint64_t scansIssued() const noexcept { return scansIssued_; }
    [[nodiscard]] uint64_t subscriptionsOpened() const noexcept { return subsOpened_; }
    [[nodiscard]] uint64_t reruns() const noexcept { return reruns_; }
    [[nodiscard]] uint64_t ringOverflows() const noexcept { return ringOverflows_; }   ///< record-ring Overflow markers acted on

private:
    struct Cb { LeaseEventFn fn = nullptr; void* ctx = nullptr; WatchGoneFn gone = nullptr; };
    struct PageBound { uint64_t lastHash = 0; int64_t asOf = 0; };
    struct HandoffRec { uint32_t at = 0, keyLen = 0, valLen = 0; int64_t offset = 0, version = 0; uint8_t op = 0; };

    // The scan of one partition in progress (a prefix, a page).
    struct ScanProg {
        uint8_t  prefixIdx = 0;
        bool     first     = true;
        bool     inFlight  = false;
        uint32_t token     = 0;      ///< the generation of the outstanding ScanLeases (stale answers carry another)
        uint64_t resumeHash = 0;
        int64_t  retryAtNs = 0;
    };
    // Rich per-partition state: each arm holds exactly the fields it needs.
    struct Idle {};
    struct Scanning { ScanProg scan; };
    struct Live {};
    struct Overflowed { ScanProg scan; int64_t cursor = 0; };   ///< the subscription was cancelled while scanning; the scan runs on
    struct Resubscribing { int64_t cursor = 0; };               ///< waiting for the old subscription to finish, then From::offset(cursor)
    struct Broken { int64_t retryAtNs = 0; };                   ///< to be re-run (rescan + resubscribe) once the old subscription is finished
    using State = std::variant<Idle, Scanning, Live, Overflowed, Resubscribing, Broken>;

    struct Part {
        State                       st{};
        std::optional<Subscription> sub;      ///< owned by Scanning / Live / Overflowed / Resubscribing / Broken; reset only once finished()
        uint32_t                    curGen = 0;   ///< the generation of the subscription whose records are accepted (0 = none)
        uint32_t                    nextGen = 1;
        uint32_t                    scanSeq = 0;
        bool                        everLive = false;
        int64_t                     backoffNs = 0;   ///< the next Broken delay (0 = the 1 ms floor); doubles to 1 s, reset when the partition is Live
        // page bounds of this epoch: prefix j's pages are pages[pageBegin[j] .. pageBegin[j] + pageCount[j])
        std::array<uint32_t, kMaxPrefixes> pageBegin{}, pageCount{};
        uint32_t                    pagesUsed = 0;
        PageBound*                  pages = nullptr;
        HandoffRec*                 hrec = nullptr;
        uint32_t                    hn = 0, hbytes = 0;
        uint8_t*                    harena = nullptr;
    };
    struct LiveEntry {
        uint64_t hash = 0;
        int64_t  version = 0;
        uint32_t nextFree = 0;
        uint32_t heapPos = 0;
        int32_t  partition = 0;
        int32_t  nameId = -1;
        uint8_t  keyLen = 0;
        bool     used = false, confirmed = true;
        uint8_t  key[kMaxKey]{};
    };
    struct Name {
        uint8_t  bytes[kMaxKey]{};
        uint8_t  len = 0;
        uint32_t heapBegin = 0, heapSize = 0;
        Cb       bound{};   ///< the recipe bound to this name (bindName)
    };
    struct HeapItem { int64_t version = 0; uint32_t liveIdx = 0; };
    struct Prefix { uint8_t len = 0; uint8_t bytes[kMaxPrefixLen]{}; };

    // ---- construction helpers / lifecycle ----
    void fail_();
    void emit_(LeaseEventKind kind, int32_t partition, int64_t offset, std::span<const uint8_t> key, int64_t version,
               std::span<const uint8_t> bytes, bool fromScan);
    // ---- the pass ----
    int  drainRing_();
    void onRingItem_(const Completion& c, std::span<const uint8_t> payload);
    int  pollScans_();
    void onScanCompletion_(const Completion& c, std::span<const uint8_t> payload);
    int  step_(int64_t nowNs);
    void stepPart_(int32_t p, int64_t nowNs);
    bool issueScan_(int32_t p, ScanProg& sp);
    void openSubscription_(int32_t p, int64_t fromOffset);
    void onPage_(int32_t p, ScanProg& sp, int64_t asOf, uint64_t lastHash, uint32_t rowCount, bool done, std::span<const uint8_t> rows);
    bool onScanRows_(int32_t p, uint32_t rowCount, std::span<const uint8_t> rows);
    void scanDone_(int32_t p);
    void toBroken_(int32_t p, int64_t nowNs);
    void startRerun_(int32_t p);
    void checkReady_();
    ScanProg* progOf_(Part& pt) noexcept;
    // ---- records ----
    void pushHandoff_(int32_t p, int64_t offset, uint8_t op, int64_t version, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void overflowAt_(int32_t p, int64_t cursor);
    void applyRecord_(int32_t p, int64_t offset, uint8_t op, int64_t version, std::span<const uint8_t> key, std::span<const uint8_t> value);
    int  prefixOf_(std::span<const uint8_t> key) const noexcept;
    [[nodiscard]] bool coveredAsOf_(const Part& pt, int prefix, uint64_t h, int64_t& asOf) const noexcept;
    // ---- the live set ----
    [[nodiscard]] uint32_t findLive_(std::span<const uint8_t> key, uint64_t h) const noexcept;
    [[nodiscard]] uint32_t insertLive_(int32_t p, std::span<const uint8_t> key, uint64_t h, int64_t version);
    void eraseLive_(uint32_t idx) noexcept;
    /// Grant of (key, version): scanRow = confirm rather than emit when already held at that version.
    void grant_(int32_t p, int64_t offset, std::span<const uint8_t> key, int64_t version, std::span<const uint8_t> bytes, bool fromScan);
    [[nodiscard]] int32_t nameOf_(std::span<const uint8_t> key) const noexcept;
    bool heapPush_(int32_t nameId, uint32_t liveIdx, int64_t version);
    void heapRemove_(int32_t nameId, uint32_t pos) noexcept;
    void siftUp_(Name& n, uint32_t pos) noexcept;
    void siftDown_(Name& n, uint32_t pos) noexcept;

    Client&                  client_;
    detail::LeaseTopicCore&  core_;
    LeaseWatchOptions        o_;
    int32_t                  nParts_;
    TopicHandle              handle_;
    uint32_t                 markerReserve_ = 0;   ///< ring bytes kept free for the markers (2 per record check)
    Completions              recs_;                ///< the record ring (standalone): the duty thread writes, this thread reads
    Completions              scanCq_;              ///< ScanLeases answers (registered)
    std::vector<PageBound>   pageStore_;
    std::vector<HandoffRec>  hrecStore_;
    std::vector<uint8_t>     harenaStore_;
    std::vector<LiveEntry>   live_;
    std::vector<uint32_t>    slots_;               ///< open-addressing table of live_ indexes (kNone = empty)
    uint32_t                 slotMask_ = 0, freeHead_ = 0, liveCount_ = 0;
    std::vector<Name>        names_;
    std::vector<HeapItem>    heapStore_;
    detail::FlatIndexMap     nameMap_;
    uint32_t                 nNames_ = 0;
    std::array<Prefix, kMaxPrefixes> prefixes_{};
    uint32_t                 nPrefixes_ = 0;
    std::vector<Part>        parts_;
    std::array<Cb, kMaxObservers> observers_{};
    Cb                       handler_{};
    bool                     started_ = false, ready_ = false, failed_ = false;
    uint64_t                 resubscribes_ = 0, scansIssued_ = 0, subsOpened_ = 0, reruns_ = 0, ringOverflows_ = 0;
    int64_t                  lastNowNs_ = 0;
};

/// The typed form: built from a LeaseTopic<T>.  Only decoding is typed: scan rows decode as T::LeasesRow, log values with T's generated decoder.
template <class T>
class LeaseWatch : public LeaseWatchBase {
public:
    LeaseWatch(Client& client, LeaseTopic<T>& topic, const LeaseWatchOptions& opts = {}) : LeaseWatchBase(client, topic.core(), opts) {}

    /// A fromScan event's bytes: the lease view row.  nullopt = a row of another layout.
    [[nodiscard]] static auto decodeScanRow(std::span<const uint8_t> row) noexcept {
        static_assert(requires { typename T::LeasesRow; }, "LeaseWatch<T>::decodeScanRow needs a generated lease type (T::LeasesRow)");
        return T::LeasesRow::decode(row);
    }
    /// A record event's bytes: the log row, read by T's generated zero-copy decoder (valid while `bytes` is).
    [[nodiscard]] static auto decodeValue(std::span<const uint8_t> bytes) noexcept {
        static_assert(requires { typename T::Decoder; }, "LeaseWatch<T>::decodeValue needs a generated type (T::Decoder)");
        return typename T::Decoder(bytes);
    }
};

}  // namespace rune::client
