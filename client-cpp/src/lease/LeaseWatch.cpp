// src/lease/LeaseWatch.cpp - the non-template body of LeaseWatch (Task 16).  See lease/LeaseWatch.h.
#include <rune/client/lease/LeaseWatch.h>

#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/LeaseScanHash.h>
#include <rune/protocol/WyHash.h>
#include <rune/protocol/client/LeaseMessages.h>

#include <algorithm>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <thread>

namespace rune::client {
namespace {

namespace rp = rune::protocol;

constexpr int64_t  kNsPerMs     = 1'000'000;
constexpr uint32_t kNone        = 0xFFFFFFFFu;
constexpr uint32_t kRecPayload  = 2048;     // the most key + value bytes one ring record carries
constexpr int64_t  kMaxBackoffNs = 1000 * kNsPerMs;   // a broken partition re-runs after 1 ms, 2 ms ... up to 1 s
constexpr uint32_t kRecBytesAvg = 192;      // handoff arena bytes per buffered record
constexpr uint32_t kMarkerBytes = detail::CompletionRing::completionRecordBytes(0);   // 48
constexpr uint16_t kKindRecord = 1, kKindOverflow = 2, kKindFatal = 3;
constexpr uint8_t  kOpCheckpoint = static_cast<uint8_t>(LeaseRecordOp::Checkpoint);
constexpr uint8_t  kOpCarried    = static_cast<uint8_t>(LeaseRecordOp::Carried);

[[nodiscard]] uint32_t pow2AtLeast(uint64_t v, uint32_t floor) noexcept {
    uint64_t p = floor;
    while (p < v) p <<= 1;
    return static_cast<uint32_t>(p);
}
[[nodiscard]] int64_t load64(const uint8_t* p) noexcept {
    int64_t v;
    std::memcpy(&v, p, 8);
    return v;
}
[[nodiscard]] uint32_t load32(const uint8_t* p) noexcept {
    uint32_t v;
    std::memcpy(&v, p, 4);
    return v;
}
[[nodiscard]] uint64_t keyHash(std::span<const uint8_t> k) noexcept { return rp::detail::wyhash64(k.data(), k.size()); }
[[nodiscard]] uint64_t ringToken(uint32_t gen, uint32_t keyLen) noexcept { return (uint64_t{gen} << 8) | (keyLen & 0xFFu); }

/// The subscription handler: the ONLY thing it does is copy the record into the ring.  It runs on the client's duty thread; every field it
/// touches is its own (captured) or the ring's.  `ring` outlives every call (the watch's destructor waits for finished()).
struct RingWriter {
    detail::CompletionRing* ring = nullptr;
    uint32_t reserve = 0;
    uint16_t part = 0;
    uint32_t gen = 0;
    bool     dropped = false;   // duty-thread state: after an overflow every later record of this subscription is dropped

    void marker(uint16_t kind, int64_t cursor) noexcept {
        Completion c;
        c.op            = OpKind::LeaseWatchReady;
        c.token         = ringToken(gen, 0);
        c.value         = cursor;
        c.errorCode     = kind;
        c.reserved0     = part;
        c.reservedBytes = kMarkerBytes;
        (void)ring->deliver(c, {});
        dropped = true;
    }
    void operator()(const Record& r) noexcept {
        if (dropped) return;
        const auto lop = static_cast<uint8_t>(r.leaseOp());
        if (lop == kOpCheckpoint || lop == kOpCarried) return;
        const std::size_t kl = r.key.size(), vl = r.value.size();
        if (kl > 255 || kl + vl > kRecPayload) return marker(kKindFatal, r.offset);
        const uint32_t need = detail::CompletionRing::completionRecordBytes(static_cast<uint32_t>(kl + vl));
        // A record plus (twice, for wrap padding) the markers every partition may still need must fit; else this record is the cursor.
        if (!ring->hasCapacityFor(2u * need + reserve)) return marker(kKindOverflow, r.offset);
        uint8_t buf[kRecPayload];
        if (kl != 0) std::memcpy(buf, r.key.data(), kl);
        if (vl != 0) std::memcpy(buf + kl, r.value.data(), vl);
        Completion c;
        c.op            = OpKind::LeaseWatchReady;
        c.token         = ringToken(gen, static_cast<uint32_t>(kl));
        c.value         = r.offset;
        c.aux           = vl >= rp::record::kLeaseVersionValueOffset + 8 ? load64(r.value.data() + rp::record::kLeaseVersionValueOffset) : 0;
        c.errorCode     = kKindRecord;
        c.reserved0     = part | (static_cast<uint32_t>(lop) << 16);
        c.reservedBytes = need;
        if (!ring->deliver(c, std::span<const uint8_t>(buf, kl + vl))) marker(kKindOverflow, r.offset);
    }
};

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------------------------------------------------------------

namespace {
CompletionsConfig ringConfig(const LeaseWatchOptions& o, int32_t parts, uint32_t& reserveOut) {
    reserveOut = 2u * kMarkerBytes * 3u * static_cast<uint32_t>(parts);   // two bytes-worth (wrap padding) for up to three markers a partition
    CompletionsConfig c;
    c.maxResultBytes = kRecPayload;
    c.maxErrorText   = 192;
    const uint32_t maxRec = detail::CompletionRing::completionRecordBytes(kRecPayload);
    c.capacityBytes = pow2AtLeast(std::max<uint64_t>(uint64_t{o.handoffBuffer} * kRecBytesAvg, 4ull * (2ull * maxRec + reserveOut)), 32768u);
    return c;
}
CompletionsConfig scanConfig(int32_t parts) {
    CompletionsConfig c;
    c.maxResultBytes = 61440;   // a ScanLeases page (kScanLeasesMaxPageBytes) fits
    c.maxErrorText   = 192;
    const uint64_t rec = detail::CompletionRing::completionRecordBytes(c.maxResultBytes);
    c.capacityBytes = pow2AtLeast((static_cast<uint64_t>(parts) + 2) * rec, 524288u);
    return c;
}
}  // namespace

LeaseWatchBase::LeaseWatchBase(Client& client, detail::LeaseTopicCore& topic, const LeaseWatchOptions& opts)
    : client_(client)
    , core_(topic)
    , o_(opts)
    , nParts_(topic.partitionCount())
    , handle_{topic.topicId(), topic.partitionCount(), TopicKind::Typed, rp::Partitioner::Fnv1a}
    , recs_(ringConfig(opts, topic.partitionCount(), markerReserve_))
    , scanCq_(client, scanConfig(topic.partitionCount()))
    , nameMap_(pow2AtLeast(4ull * std::max<uint32_t>(opts.names, 1), 16u)) {
    if (o_.handoffBuffer == 0) o_.handoffBuffer = 1;
    if (o_.maxLive == 0) o_.maxLive = 1;
    if (o_.maxPagesPerScan == 0) o_.maxPagesPerScan = 1;
    if (o_.maxRows == 0) o_.maxRows = 1;
    if (o_.maxRows > rp::client::kScanLeasesMaxRows) o_.maxRows = static_cast<uint16_t>(rp::client::kScanLeasesMaxRows);
    const auto np = static_cast<std::size_t>(nParts_);
    pageStore_.resize(np * o_.maxPagesPerScan);
    hrecStore_.resize(np * o_.handoffBuffer);
    harenaStore_.resize(np * o_.handoffBuffer * std::size_t{kRecBytesAvg});
    live_.resize(o_.maxLive);
    for (uint32_t i = 0; i < o_.maxLive; ++i) live_[i].nextFree = i + 1 < o_.maxLive ? i + 1 : kNone;
    freeHead_ = 0;
    slots_.assign(pow2AtLeast(2ull * o_.maxLive, 16u), kNone);
    slotMask_ = static_cast<uint32_t>(slots_.size() - 1);
    names_.resize(o_.names);
    heapStore_.resize(std::size_t{o_.names} * o_.waitersPerName);
    parts_.resize(np);
    for (std::size_t p = 0; p < np; ++p) {
        Part& pt = parts_[p];
        pt.pages  = pageStore_.data() + p * o_.maxPagesPerScan;
        pt.hrec   = hrecStore_.data() + p * o_.handoffBuffer;
        pt.harena = harenaStore_.data() + p * o_.handoffBuffer * std::size_t{kRecBytesAvg};
    }
}

LeaseWatchBase::~LeaseWatchBase() {
    for (uint32_t i = 0; i < nNames_; ++i) {   // the recipes bound to a name forget this watch first
        Cb& c = names_[i].bound;
        if (c.fn != nullptr && c.gone != nullptr) { const Cb g = c; c = Cb{}; g.gone(g.ctx, this); }
        else c = Cb{};
    }
    for (Cb& o : observers_)   // the sessions observing this watch forget it first (owner thread; they never call a destroyed watch)
        if (o.fn != nullptr && o.gone != nullptr) { const Cb c = o; o = Cb{}; c.gone(c.ctx, this); }
    if (!hasUnfinishedSub_()) { parts_.clear(); return; }   // closed (or never subscribed): nothing to wait for, the client is not touched
    for (Part& pt : parts_)
        if (pt.sub) pt.sub->unsubscribe();
    // A handler of the duty thread holds a pointer to the ring, which is freed after this body: wait until every subscription is finished,
    // or the client is closed (its duty thread then runs no handler).  Never a bare spin: the wait idle strategy backs off.
    DefaultWaitIdle idle;
    for (;;) {
        bool all = true;
        for (const Part& pt : parts_)
            if (pt.sub && !pt.sub->finished()) { all = false; break; }
        if (all || client_.commandsClosed()) break;
        if (client_.invokerMode()) (void)client_.doWork();
        else idle.idle();
    }
    parts_.clear();
}

bool LeaseWatchBase::addObserver(LeaseEventFn fn, void* ctx, WatchGoneFn gone) noexcept {
    if (fn == nullptr) return false;
    for (Cb& o : observers_)
        if (o.fn == nullptr) { o = Cb{fn, ctx, gone}; return true; }
    return false;
}
uint32_t LeaseWatchBase::observerCount() const noexcept {
    uint32_t n = 0;
    for (const Cb& o : observers_) n += o.fn != nullptr;
    return n;
}
bool LeaseWatchBase::removeObserver(void* ctx) noexcept {
    for (Cb& o : observers_)
        if (o.fn != nullptr && o.ctx == ctx) { o = Cb{}; return true; }
    return false;
}

bool LeaseWatchBase::hasUnfinishedSub_() const noexcept {
    for (const Part& pt : parts_)
        if (pt.sub && !pt.sub->finished()) return true;
    return false;
}

void LeaseWatchBase::closeAsync() noexcept {
    switch (life_) {
    case Life::Fresh:
        life_ = Life::Closed;
        return;
    case Life::Running:
        ready_ = false;
        for (Part& pt : parts_) {
            pt.curGen = 0;
            ++pt.scanSeq;   // an outstanding scan answer is stale
            pt.hn = pt.hbytes = 0;
            if (pt.sub && !pt.sub->finished()) pt.sub->unsubscribe();
            pt.st = Idle{};
        }
        life_ = Life::Closing;
        return;
    case Life::Failed:   // fail_() already unsubscribed every subscription
        life_ = Life::Closing;
        return;
    case Life::Closing:
    case Life::Closed:
        return;
    }
}

// Failed and Closing: whatever the duty thread and the broker still deliver is dropped; a finished subscription is released.  Closing ends
// as Closed once no partition holds a subscription.
void LeaseWatchBase::settleClosing_() noexcept {
    (void)recs_.ring().read([](const Completion&, std::span<const uint8_t>) {}, INT_MAX);
    (void)scanCq_.poll([](const Completion&, std::span<const uint8_t>) {});
    bool any = false;
    for (Part& pt : parts_) {
        if (pt.sub && pt.sub->finished()) pt.sub.reset();
        any = any || pt.sub.has_value();
    }
    if (life_ == Life::Closing && !any) life_ = Life::Closed;
}

void LeaseWatchBase::emit_(LeaseEventKind kind, int32_t partition, int64_t offset, std::span<const uint8_t> key, int64_t version,
                           std::span<const uint8_t> bytes, bool fromScan) {
    if (life_ == Life::Closing || life_ == Life::Closed) return;
    const LeaseEvent ev{kind, core_.topicId(), partition, offset, key, version, bytes, fromScan};
    if (handler_.fn != nullptr) handler_.fn(handler_.ctx, ev);
    for (const Cb& o : observers_)
        if (o.fn != nullptr) o.fn(o.ctx, ev);
    // The recipes bound to names: Ready / Failed go to every bound name once; a key event to the name the key belongs to (the name's heap and
    // the live set already reflect the event).
    if (nNames_ == 0 || kind == LeaseEventKind::RerunBegin || kind == LeaseEventKind::RerunEnd) return;   // a re-run bracket names no key
    if (kind == LeaseEventKind::Ready || kind == LeaseEventKind::Failed) {
        for (uint32_t i = 0; i < nNames_; ++i)
            if (names_[i].bound.fn != nullptr) { const Cb c = names_[i].bound; c.fn(c.ctx, ev); }
        return;
    }
    if (const int32_t nid = nameOf_(key); nid >= 0 && names_[static_cast<uint32_t>(nid)].bound.fn != nullptr) {
        const Cb c = names_[static_cast<uint32_t>(nid)].bound;
        c.fn(c.ctx, ev);
    }
}

void LeaseWatchBase::fail_() {
    if (life_ != Life::Running) return;
    life_  = Life::Failed;
    ready_ = false;
    for (Part& pt : parts_) {
        pt.curGen = 0;
        if (pt.sub) pt.sub->unsubscribe();
    }
    emit_(LeaseEventKind::Failed, -1, -1, {}, 0, {}, false);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------------------------------------------------------------

OpResult LeaseWatchBase::startFromNow(std::span<const std::span<const uint8_t>> prefixes) {
    if (life_ != Life::Fresh) return OpResult::InvalidArgument;
    if (const OpResult r = setPrefixes_(prefixes); r != OpResult::Accepted) return r;
    life_ = Life::Running;
    for (Part& pt : parts_) pt.st = Scanning{};
    return OpResult::Accepted;
}

OpResult LeaseWatchBase::restart(std::span<const std::span<const uint8_t>> prefixes) {
    if (life_ != Life::Closed) return OpResult::InvalidArgument;   // what startFromNow answers on a second call
    if (const OpResult r = setPrefixes_(prefixes); r != OpResult::Accepted) return r;
    // Closed: no subscription is held and no scan is outstanding.  The live table, names, handlers and every buffer are kept; every partition
    // starts as a re-run, so the scan ends each lease it does not confirm with Vanished.  nextGen / scanSeq keep growing: earlier answers stay stale.
    ready_ = false;
    for (LiveEntry& e : live_)
        if (e.used) e.confirmed = false;
    for (Part& pt : parts_) {
        pt.curGen    = 0;
        pt.everLive  = false;
        pt.backoffNs = 0;
        pt.pagesUsed = 0;
        pt.pageBegin.fill(0);
        pt.pageCount.fill(0);
        pt.hn = pt.hbytes = 0;
        pt.rerunOpen = false;
        pt.st = Scanning{};
    }
    life_ = Life::Running;
    for (int32_t p = 0; p < nParts_ && life_ == Life::Running; ++p) openRerun_(p);   // every partition re-runs: one bracket each
    return OpResult::Accepted;
}

OpResult LeaseWatchBase::setPrefixes_(std::span<const std::span<const uint8_t>> prefixes) {
    if (core_.closed() || core_.viewRowLen() == 0) return OpResult::InvalidArgument;
    if (prefixes.empty() || prefixes.size() > kMaxPrefixes) return OpResult::InvalidArgument;
    // A record of the watched topic travels the ring as key + value: refuse a topic whose widest key and lease row cannot (it would fail
    // the watch as Fatal at the first record).
    if (std::size_t{core_.hasFixedKeyWidth() ? core_.revokeKeyWidth() : kMaxKey} + core_.viewRowLen() + kMarkerBytes > kRecPayload)
        return OpResult::InvalidArgument;
    for (const auto& p : prefixes)
        if (p.size() > kMaxPrefixLen) return OpResult::InvalidArgument;
    std::array<bool, kMaxPrefixes> drop{};
    const auto isPrefixOf = [&](std::size_t a, std::size_t b) {   // prefixes[a] is a prefix of prefixes[b]
        return prefixes[a].size() <= prefixes[b].size() &&
               (prefixes[a].empty() || std::memcmp(prefixes[a].data(), prefixes[b].data(), prefixes[a].size()) == 0);
    };
    for (std::size_t i = 0; i < prefixes.size(); ++i)
        for (std::size_t j = 0; j < prefixes.size(); ++j) {
            if (i == j || !isPrefixOf(j, i)) continue;
            if (prefixes[j].size() < prefixes[i].size() || j < i) { drop[i] = true; break; }   // i extends j; an exact duplicate: keep the first
        }
    nPrefixes_ = 0;
    for (std::size_t i = 0; i < prefixes.size(); ++i) {
        if (drop[i]) continue;
        Prefix& dst = prefixes_[nPrefixes_++];
        dst.len = static_cast<uint8_t>(prefixes[i].size());
        if (!prefixes[i].empty()) std::memcpy(dst.bytes, prefixes[i].data(), prefixes[i].size());
    }
    return OpResult::Accepted;
}

// ---------------------------------------------------------------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------------------------------------------------------------

LeaseWatchBase::ScanProg* LeaseWatchBase::progOf_(Part& pt) noexcept {
    if (auto* s = std::get_if<Scanning>(&pt.st)) return &s->scan;
    if (auto* o = std::get_if<Overflowed>(&pt.st)) return &o->scan;
    return nullptr;
}

int LeaseWatchBase::doWork(int64_t nowNs) {
    if (life_ == Life::Failed || life_ == Life::Closing) { settleClosing_(); return 0; }
    if (life_ != Life::Running) return 0;
    lastNowNs_ = nowNs;
    int w = drainRing_();
    if (life_ == Life::Running) w += pollScans_();
    if (life_ == Life::Running) w += step_(nowNs);
    return w;
}

int LeaseWatchBase::drainRing_() {
    return recs_.ring().read([this](const Completion& c, std::span<const uint8_t> pl) { onRingItem_(c, pl); }, INT_MAX);
}

void LeaseWatchBase::onRingItem_(const Completion& c, std::span<const uint8_t> payload) {
    if (life_ != Life::Running) return;
    const uint32_t p = c.reserved0 & 0xFFFFu;
    const auto gen = static_cast<uint32_t>(c.token >> 8);
    if (p >= static_cast<uint32_t>(nParts_)) return;
    Part& pt = parts_[p];
    if (gen == 0 || pt.curGen != gen) return;   // another (older) subscription's item: discarded
    const auto pi = static_cast<int32_t>(p);
    switch (c.errorCode) {
    case kKindRecord: {
        const std::size_t kl = c.token & 0xFFu;
        if (payload.size() < kl) return;
        const auto key = payload.first(kl);
        const auto val = payload.subspan(kl);
        const auto op  = static_cast<uint8_t>((c.reserved0 >> 16) & 0xFFu);
        if (std::holds_alternative<Scanning>(pt.st)) pushHandoff_(pi, c.value, op, c.aux, key, val);
        else if (std::holds_alternative<Live>(pt.st)) applyRecord_(pi, c.value, op, c.aux, key, val);
        return;
    }
    case kKindOverflow:
        ++ringOverflows_;
        if (std::holds_alternative<Scanning>(pt.st)) {
            overflowAt_(pi, c.value);
        } else if (std::holds_alternative<Live>(pt.st)) {   // every earlier record of this subscription was applied by this drain
            pt.curGen = 0;
            if (pt.sub) pt.sub->unsubscribe();
            pt.st = Resubscribing{c.value};
        }
        return;
    case kKindFatal:
        fail_();
        return;
    default:
        return;
    }
}

int LeaseWatchBase::pollScans_() {
    return scanCq_.poll([this](const Completion& c, std::span<const uint8_t> pl) { onScanCompletion_(c, pl); });
}

void LeaseWatchBase::onScanCompletion_(const Completion& c, std::span<const uint8_t> payload) {
    if (life_ != Life::Running || c.op != OpKind::ScanLeases) return;
    const uint32_t p = static_cast<uint32_t>(c.token & 0xFFFFu);
    if (p >= static_cast<uint32_t>(nParts_)) return;
    Part& pt = parts_[p];
    ScanProg* sp = progOf_(pt);
    if (sp == nullptr || !sp->inFlight || sp->token != static_cast<uint32_t>(c.token >> 16)) return;   // stale: another epoch's page
    sp->inFlight = false;
    const auto pi = static_cast<int32_t>(p);
    if (c.status == OpStatus::Ok) {
        onPage_(pi, *sp, c.value, static_cast<uint64_t>(c.aux), c.reserved0 & 0x7FFFFFFFu, (c.reserved0 >> 31) != 0, payload);
        return;
    }
    switch (c.status) {
    case OpStatus::Closed:
    case OpStatus::ResultTooLarge:
    case OpStatus::InvalidArgument:   // the duty thread refused the request as malformed: it will be again
        fail_();
        return;
    case OpStatus::Timeout:
    case OpStatus::NotConnected:
    case OpStatus::SessionLost:
        sp->retryAtNs = lastNowNs_ + kNsPerMs;   // the same page again
        return;
    case OpStatus::BrokerError:
        if (c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::Throttled) ||
            c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::NotLeader) ||
            c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::AuthUnavailable)) {
            sp->retryAtNs = lastNowNs_ + kNsPerMs;   // max(1 ms, retry-after): a scan answer carries no retry-after hint
            return;
        }
        if (c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::NotAuthorized) ||
            c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::UnknownTopic) ||
            c.errorCode == static_cast<uint16_t>(rp::client::ErrorCode::InvalidRequest)) {
            fail_();   // definitive: a rescan answers the same
            return;
        }
        toBroken_(pi, lastNowNs_);
        return;
    default:
        toBroken_(pi, lastNowNs_);
        return;
    }
}

bool LeaseWatchBase::issueScan_(int32_t p, ScanProg& sp) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    sp.token = ++pt.scanSeq;
    const Prefix& pre = prefixes_[sp.prefixIdx];
    const OpResult r = core_.scanLeases(p, std::span<const uint8_t>(pre.bytes, pre.len), sp.resumeHash, sp.first, o_.maxRows, LeaseOptions{},
                                        CompletionTarget(scanCq_), (uint64_t{sp.token} << 16) | static_cast<uint32_t>(p));
    if (r == OpResult::Accepted) {
        sp.inFlight = true;
        ++scansIssued_;
        return true;
    }
    if (r == OpResult::BackPressured || r == OpResult::QueueFull) return false;   // try again next pass
    fail_();
    return false;
}

int LeaseWatchBase::step_(int64_t nowNs) {
    int w = 0;
    for (int32_t p = 0; p < nParts_ && life_ == Life::Running; ++p) {
        const std::size_t before = scansIssued_ + subsOpened_ + reruns_;
        stepPart_(p, nowNs);
        w += static_cast<int>(scansIssued_ + subsOpened_ + reruns_ - before);
    }
    checkReady_();
    return w;
}

void LeaseWatchBase::stepPart_(int32_t p, int64_t nowNs) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    // A transient partition that was reborn answered the subscription with a data reset (the client restarted it at the ring start on its
    // own): the lineage this partition's keys came from is gone.  Rerun the partition (rescan; keys the new scan lacks end as Vanished).  The
    // read follows the Subscription freshness rule (#473): eventually fresh, converges on the next pass.
    if (pt.sub && !pt.sub->finished() && !std::holds_alternative<Idle>(pt.st) && !std::holds_alternative<Overflowed>(pt.st) &&
        !std::holds_alternative<Broken>(pt.st) && pt.sub->dataResets() != pt.seenResets) {
        ++dataResetReruns_;
        toBroken_(p, nowNs);
        return;
    }
    if (auto* sc = std::get_if<Scanning>(&pt.st)) {
        if (pt.sub && pt.sub->finished()) { toBroken_(p, nowNs); return; }
        if (!sc->scan.inFlight && nowNs >= sc->scan.retryAtNs) (void)issueScan_(p, sc->scan);
    } else if (auto* ov = std::get_if<Overflowed>(&pt.st)) {
        if (!ov->scan.inFlight && nowNs >= ov->scan.retryAtNs) (void)issueScan_(p, ov->scan);
    } else if (std::holds_alternative<Live>(pt.st)) {
        if (pt.sub && pt.sub->finished()) toBroken_(p, nowNs);
    } else if (auto* rs = std::get_if<Resubscribing>(&pt.st)) {
        if (!pt.sub || pt.sub->finished()) {
            const int64_t cursor = rs->cursor;
            pt.sub.reset();
            pt.st = Live{};
            ++resubscribes_;
            pt.backoffNs = 0;
            openSubscription_(p, cursor);
            pt.everLive = true;
            if (life_ == Life::Running) closeRerun_(p);   // the end of a re-run that overflowed: the resubscribed partition is Live
        }
    } else if (auto* br = std::get_if<Broken>(&pt.st)) {
        if (nowNs >= br->retryAtNs && (!pt.sub || pt.sub->finished())) {
            pt.sub.reset();
            startRerun_(p);
        }
    }
}

void LeaseWatchBase::openSubscription_(int32_t p, int64_t fromOffset) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    uint32_t gen = pt.nextGen++;
    if (gen == 0) gen = pt.nextGen++;
    SubscribeOptions so;
    so.from = From::offset(fromOffset);
    try {
        pt.sub.emplace(client_.subscribe(handle_, p, std::move(so),
                                         RecordHandler(RingWriter{&recs_.ring(), markerReserve_, static_cast<uint16_t>(p), gen, false})));
    } catch (...) {
        fail_();
        return;
    }
    pt.curGen = gen;
    pt.seenResets = pt.sub->dataResets();
    ++subsOpened_;
}

void LeaseWatchBase::onPage_(int32_t p, ScanProg& sp, int64_t asOf, uint64_t lastHash, uint32_t rowCount, bool done,
                             std::span<const uint8_t> rows) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    const uint8_t j = sp.prefixIdx;
    if (pt.pagesUsed >= o_.maxPagesPerScan) { fail_(); return; }
    if (pt.pageCount[j] == 0) pt.pageBegin[j] = pt.pagesUsed;   // the prefixes are scanned one after the other: contiguous
    pt.pages[pt.pagesUsed++] = PageBound{lastHash, asOf};
    ++pt.pageCount[j];
    if (pt.pagesUsed == 1) {   // the first page of the epoch carries the minimum asOf: subscribe from it, now
        openSubscription_(p, asOf);
        if (life_ != Life::Running) return;
    }
    if (!onScanRows_(p, rowCount, rows)) { toBroken_(p, lastNowNs_); return; }
    if (life_ != Life::Running) return;
    if (done) {
        ++sp.prefixIdx;
        sp.first      = true;
        sp.resumeHash = 0;
        sp.retryAtNs  = 0;
        if (sp.prefixIdx >= nPrefixes_) scanDone_(p);   // `sp` is dead after this
    } else {
        sp.first      = false;
        sp.resumeHash = lastHash;
        sp.retryAtNs  = 0;
    }
}

bool LeaseWatchBase::onScanRows_(int32_t p, uint32_t rowCount, std::span<const uint8_t> rows) {
    std::size_t pos = 0;
    const uint32_t rowLen = core_.viewRowLen();
    for (uint32_t i = 0; i < rowCount; ++i) {
        if (pos >= rows.size()) return false;
        const std::size_t kl = rows[pos++];
        if (kl == 0 || kl > kMaxKey || rows.size() - pos < kl + 4) return false;
        const auto key = rows.subspan(pos, kl);
        pos += kl;
        const uint32_t rl = load32(rows.data() + pos);
        pos += 4;
        if (rows.size() - pos < rl) return false;
        const auto row = rows.subspan(pos, rl);
        pos += rl;
        if (rl != rowLen) { fail_(); return true; }   // a layout this watch was not opened with (spec 8.3: detected by the row length)
        const int64_t version = (row[0] & 1u) == 0 ? load64(row.data() + core_.viewVersionOffset()) : 0;
        const uint32_t gs     = core_.viewGrantedSlot();
        const int64_t granted = (row[gs >> 3] & (1u << (gs & 7u))) == 0 ? load64(row.data() + core_.viewGrantedOffset()) : 0;
        grant_(p, granted, key, version, row, true);
        if (life_ != Life::Running) return true;
    }
    return pos == rows.size();
}

void LeaseWatchBase::scanDone_(int32_t p) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    // A re-run: whatever the scan did not confirm ended while the partition was not being watched.
    for (uint32_t i = 0; i < o_.maxLive; ++i) {
        LiveEntry& e = live_[i];
        if (!e.used || e.partition != p || e.confirmed) continue;
        uint8_t kb[kMaxKey];   // the entry leaves the live set and its name heap BEFORE the event (a bound recipe reads both)
        const std::size_t kl = e.keyLen;
        const int64_t ver = e.version;
        std::memcpy(kb, e.key, kl);
        if (e.nameId >= 0) heapRemove_(e.nameId, e.heapPos);
        eraseLive_(i);
        emit_(LeaseEventKind::Vanished, p, -1, std::span<const uint8_t>(kb, kl), ver, {}, false);
    }
    // The records that arrived during the scan, in order, through the same apply rule.
    for (uint32_t i = 0; i < pt.hn && life_ == Life::Running; ++i) {
        const HandoffRec& r = pt.hrec[i];
        applyRecord_(p, r.offset, r.op, r.version, std::span<const uint8_t>(pt.harena + r.at, r.keyLen),
                     std::span<const uint8_t>(pt.harena + r.at + r.keyLen, r.valLen));
    }
    pt.hn = pt.hbytes = 0;
    if (life_ != Life::Running) return;
    if (const auto* ov = std::get_if<Overflowed>(&pt.st)) {
        pt.st = Resubscribing{ov->cursor};
    } else {
        pt.st        = Live{};
        pt.everLive  = true;
        pt.backoffNs = 0;
        closeRerun_(p);
    }
}

void LeaseWatchBase::openRerun_(int32_t p) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    if (pt.rerunOpen) return;   // a partition that breaks again during its re-run keeps the one bracket
    pt.rerunOpen = true;
    emit_(LeaseEventKind::RerunBegin, p, -1, {}, 0, {}, false);
}

void LeaseWatchBase::closeRerun_(int32_t p) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    if (!pt.rerunOpen) return;
    pt.rerunOpen = false;
    emit_(LeaseEventKind::RerunEnd, p, -1, {}, 0, {}, false);
}

void LeaseWatchBase::toBroken_(int32_t p, int64_t nowNs) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    pt.curGen = 0;
    pt.hn = pt.hbytes = 0;
    if (pt.sub && !pt.sub->finished()) pt.sub->unsubscribe();
    const int64_t wait = pt.backoffNs < kNsPerMs ? kNsPerMs : pt.backoffNs;
    pt.backoffNs = std::min(wait * 2, kMaxBackoffNs);
    pt.st = Broken{nowNs + wait};
}

void LeaseWatchBase::startRerun_(int32_t p) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    ++reruns_;
    for (LiveEntry& e : live_)
        if (e.used && e.partition == p) e.confirmed = false;
    pt.pagesUsed = 0;
    pt.pageBegin.fill(0);
    pt.pageCount.fill(0);
    pt.hn = pt.hbytes = 0;
    pt.st = Scanning{};
    openRerun_(p);   // after the state is set: a handler that closes the watch finds a consistent partition
}

void LeaseWatchBase::checkReady_() {
    if (ready_ || life_ != Life::Running) return;
    for (const Part& pt : parts_)
        if (!pt.everLive) return;
    ready_ = true;
    emit_(LeaseEventKind::Ready, -1, -1, {}, 0, {}, false);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------------------------------------------------------------

void LeaseWatchBase::overflowAt_(int32_t p, int64_t cursor) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    const ScanProg sp = std::get<Scanning>(pt.st).scan;
    pt.curGen = 0;   // every later record of this subscription is discarded
    if (pt.sub) pt.sub->unsubscribe();
    pt.st = Overflowed{sp, cursor};
}

void LeaseWatchBase::pushHandoff_(int32_t p, int64_t offset, uint8_t op, int64_t version, std::span<const uint8_t> key,
                                  std::span<const uint8_t> value) {
    Part& pt = parts_[static_cast<std::size_t>(p)];
    const std::size_t need = key.size() + value.size();
    if (pt.hn >= o_.handoffBuffer || pt.hbytes + need > std::size_t{o_.handoffBuffer} * kRecBytesAvg) {
        overflowAt_(p, offset);   // the list is full: this record is the cursor
        return;
    }
    HandoffRec& r = pt.hrec[pt.hn++];
    r.at = pt.hbytes;
    r.keyLen = static_cast<uint32_t>(key.size());
    r.valLen = static_cast<uint32_t>(value.size());
    r.offset = offset;
    r.version = version;
    r.op = op;
    if (!key.empty()) std::memcpy(pt.harena + r.at, key.data(), key.size());
    if (!value.empty()) std::memcpy(pt.harena + r.at + key.size(), value.data(), value.size());
    pt.hbytes += static_cast<uint32_t>(need);
}

int LeaseWatchBase::prefixOf_(std::span<const uint8_t> key) const noexcept {
    for (uint32_t j = 0; j < nPrefixes_; ++j) {
        const Prefix& pr = prefixes_[j];
        if (pr.len <= key.size() && (pr.len == 0 || std::memcmp(pr.bytes, key.data(), pr.len) == 0)) return static_cast<int>(j);
    }
    return -1;
}

bool LeaseWatchBase::coveredAsOf_(const Part& pt, int prefix, uint64_t h, int64_t& asOf) const noexcept {
    const uint32_t n = pt.pageCount[static_cast<std::size_t>(prefix)];
    if (n == 0) return false;
    const PageBound* b = pt.pages + pt.pageBegin[static_cast<std::size_t>(prefix)];
    // the first page whose lastHash >= h; the last page covers everything above
    uint32_t lo = 0, hi = n;
    while (lo < hi) {
        const uint32_t mid = lo + (hi - lo) / 2;
        if (b[mid].lastHash >= h) hi = mid;
        else lo = mid + 1;
    }
    asOf = b[lo < n ? lo : n - 1].asOf;
    return true;
}

void LeaseWatchBase::applyRecord_(int32_t p, int64_t offset, uint8_t op, int64_t version, std::span<const uint8_t> key,
                                  std::span<const uint8_t> value) {
    const int pi = prefixOf_(key);
    if (pi < 0) return;
    int64_t asOf = 0;
    if (!coveredAsOf_(parts_[static_cast<std::size_t>(p)], pi, rp::scanHash(key, core_.topicId()), asOf)) return;
    if (offset < asOf) return;   // rule 3: the page that covers this key already reflects the record
    switch (static_cast<LeaseRecordOp>(op)) {
    case LeaseRecordOp::Granted:
        grant_(p, offset, key, version, value, false);
        return;
    case LeaseRecordOp::Updated: {
        const uint32_t idx = findLive_(key, keyHash(key));
        if (idx != kNone && live_[idx].version == version) emit_(LeaseEventKind::Updated, p, offset, key, version, value, false);
        return;
    }
    case LeaseRecordOp::Revoked:
    case LeaseRecordOp::Expired: {
        const uint32_t idx = findLive_(key, keyHash(key));
        if (idx == kNone || live_[idx].version != version) return;   // an end for an unknown (key, version)
        if (live_[idx].nameId >= 0) heapRemove_(live_[idx].nameId, live_[idx].heapPos);
        eraseLive_(idx);
        emit_(static_cast<LeaseRecordOp>(op) == LeaseRecordOp::Revoked ? LeaseEventKind::Revoked : LeaseEventKind::Expired, p, offset, key,
              version, value, false);
        return;
    }
    case LeaseRecordOp::Checkpoint:
    case LeaseRecordOp::Carried:   // spec A.3: the grant or the scan already gave the row; a skipped one is a reset/clamp that reruns the scan
        return;
    }
}

// ---------------------------------------------------------------------------------------------------------------------------------
// The live set
// ---------------------------------------------------------------------------------------------------------------------------------

uint32_t LeaseWatchBase::findLive_(std::span<const uint8_t> key, uint64_t h) const noexcept {
    for (uint32_t i = static_cast<uint32_t>(h) & slotMask_;; i = (i + 1) & slotMask_) {
        const uint32_t idx = slots_[i];
        if (idx == kNone) return kNone;
        const LiveEntry& e = live_[idx];
        if (e.hash == h && e.keyLen == key.size() && std::memcmp(e.key, key.data(), key.size()) == 0) return idx;
    }
}

bool LeaseWatchBase::isLive(std::span<const uint8_t> key, int64_t version) const noexcept {
    const uint32_t idx = findLive_(key, keyHash(key));
    return idx != kNone && live_[idx].version == version;
}

uint32_t LeaseWatchBase::insertLive_(int32_t p, std::span<const uint8_t> key, uint64_t h, int64_t version) {
    if (freeHead_ == kNone) return kNone;
    const uint32_t idx = freeHead_;
    LiveEntry& e = live_[idx];
    freeHead_ = e.nextFree;
    e.hash = h;
    e.version = version;
    e.partition = p;
    e.nameId = -1;
    e.heapPos = 0;
    e.keyLen = static_cast<uint8_t>(key.size());
    e.used = true;
    e.confirmed = true;
    if (!key.empty()) std::memcpy(e.key, key.data(), key.size());
    uint32_t i = static_cast<uint32_t>(h) & slotMask_;
    while (slots_[i] != kNone) i = (i + 1) & slotMask_;
    slots_[i] = idx;
    ++liveCount_;
    return idx;
}

void LeaseWatchBase::eraseLive_(uint32_t idx) noexcept {
    LiveEntry& e = live_[idx];
    uint32_t i = static_cast<uint32_t>(e.hash) & slotMask_;
    while (slots_[i] != idx) i = (i + 1) & slotMask_;
    uint32_t hole = i, j = i;
    for (;;) {   // backward-shift deletion: no tombstones
        j = (j + 1) & slotMask_;
        if (slots_[j] == kNone) break;
        const uint32_t home = static_cast<uint32_t>(live_[slots_[j]].hash) & slotMask_;
        const bool stays = hole <= j ? (home > hole && home <= j) : (home > hole || home <= j);
        if (stays) continue;
        slots_[hole] = slots_[j];
        hole = j;
    }
    slots_[hole] = kNone;
    e.used = false;
    e.nextFree = freeHead_;
    freeHead_ = idx;
    --liveCount_;
}

void LeaseWatchBase::grant_(int32_t p, int64_t offset, std::span<const uint8_t> key, int64_t version, std::span<const uint8_t> bytes,
                            bool fromScan) {
    const uint64_t h = keyHash(key);
    if (const uint32_t old = findLive_(key, h); old != kNone) {
        LiveEntry& e = live_[old];
        if (e.version == version) {   // the same lease: already held
            // A re-run's scan row confirms an entry the re-run had not confirmed yet (once: a retried page finds it confirmed).  Only while the
            // partition's bracket is open: a first scan never emits Confirmed.
            if (fromScan && !e.confirmed && parts_[static_cast<std::size_t>(p)].rerunOpen) {
                e.confirmed = true;
                emit_(LeaseEventKind::Confirmed, p, offset, key, version, bytes, true);
            } else {
                e.confirmed = true;
            }
            return;
        }
        uint8_t kb[kMaxKey];   // the old incarnation leaves the live set and its name heap BEFORE the event (a bound recipe reads both)
        const std::size_t kl = e.keyLen;
        const int64_t ver = e.version;
        std::memcpy(kb, e.key, kl);
        if (e.nameId >= 0) heapRemove_(e.nameId, e.heapPos);
        eraseLive_(old);
        emit_(LeaseEventKind::Vanished, p, offset, std::span<const uint8_t>(kb, kl), ver, {}, false);   // another incarnation replaces it
    }
    const uint32_t idx = insertLive_(p, key, h, version);
    if (idx == kNone) { fail_(); return; }   // the live set is full
    const int32_t nid = nameOf_(key);
    if (nid >= 0) {
        live_[idx].nameId = nid;
        if (!heapPush_(nid, idx, version)) { fail_(); return; }
    }
    emit_(LeaseEventKind::Granted, p, offset, std::span<const uint8_t>(live_[idx].key, live_[idx].keyLen), version, bytes, fromScan);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Names
// ---------------------------------------------------------------------------------------------------------------------------------

int32_t LeaseWatchBase::registerName(std::span<const uint8_t> name) {
    if (life_ != Life::Fresh || name.empty() || name.size() > kMaxKey) return -1;
    const auto h = static_cast<int64_t>(keyHash(name));
    if (const uint32_t at = nameMap_.find(h); at != detail::FlatIndexMap::kNone) {
        const Name& n = names_[at];
        return n.len == name.size() && std::memcmp(n.bytes, name.data(), name.size()) == 0 ? static_cast<int32_t>(at) : -1;
    }
    if (nNames_ >= o_.names) return -1;
    const uint32_t id = nNames_++;
    Name& n = names_[id];
    n.len = static_cast<uint8_t>(name.size());
    if (!name.empty()) std::memcpy(n.bytes, name.data(), name.size());
    n.heapBegin = id * o_.waitersPerName;
    n.heapSize = 0;
    nameMap_.insert(h, id);   // sized at construction for `names` entries: never grows
    return static_cast<int32_t>(id);
}

int32_t LeaseWatchBase::nameOf_(std::span<const uint8_t> key) const noexcept {
    if (nNames_ == 0) return -1;
    std::size_t slash = key.size();
    while (slash > 0 && key[slash - 1] != '/') --slash;
    if (slash < 2) return -1;   // no '/', or an empty name
    const auto parent = key.first(slash - 1);
    const uint32_t at = nameMap_.find(static_cast<int64_t>(keyHash(parent)));
    if (at == detail::FlatIndexMap::kNone) return -1;
    const Name& n = names_[at];
    return n.len == parent.size() && std::memcmp(n.bytes, parent.data(), parent.size()) == 0 ? static_cast<int32_t>(at) : -1;
}

std::optional<std::pair<int64_t, std::span<const uint8_t>>> LeaseWatchBase::lowest(int32_t id) const noexcept {
    if (id < 0 || static_cast<uint32_t>(id) >= nNames_ || names_[static_cast<uint32_t>(id)].heapSize == 0) return std::nullopt;
    const Name& n = names_[static_cast<uint32_t>(id)];
    const HeapItem& top = heapStore_[n.heapBegin];
    const LiveEntry& e = live_[top.liveIdx];
    return std::pair<int64_t, std::span<const uint8_t>>(top.version, std::span<const uint8_t>(e.key, e.keyLen));
}

bool LeaseWatchBase::bindName(int32_t id, LeaseEventFn fn, void* ctx, WatchGoneFn gone) noexcept {
    if (fn == nullptr || id < 0 || static_cast<uint32_t>(id) >= nNames_) return false;
    Cb& c = names_[static_cast<uint32_t>(id)].bound;
    if (c.fn != nullptr) return false;
    c = Cb{fn, ctx, gone};
    return true;
}

bool LeaseWatchBase::unbindName(int32_t id, void* ctx) noexcept {
    if (id < 0 || static_cast<uint32_t>(id) >= nNames_) return false;
    Cb& c = names_[static_cast<uint32_t>(id)].bound;
    if (c.fn == nullptr || c.ctx != ctx) return false;
    c = Cb{};
    return true;
}

std::optional<std::pair<int64_t, std::span<const uint8_t>>> LeaseWatchBase::predecessor(int32_t id, int64_t version) const noexcept {
    if (id < 0 || static_cast<uint32_t>(id) >= nNames_) return std::nullopt;
    const Name& n = names_[static_cast<uint32_t>(id)];
    const HeapItem* h = heapStore_.data() + n.heapBegin;
    const HeapItem* best = nullptr;   // a linear pass over the name's entries: bounded by waitersPerName
    for (uint32_t i = 0; i < n.heapSize; ++i)
        if (h[i].version < version && (best == nullptr || h[i].version > best->version)) best = &h[i];
    if (best == nullptr) return std::nullopt;
    const LiveEntry& e = live_[best->liveIdx];
    return std::pair<int64_t, std::span<const uint8_t>>(best->version, std::span<const uint8_t>(e.key, e.keyLen));
}

void LeaseWatchBase::siftUp_(Name& n, uint32_t pos) noexcept {
    HeapItem* h = heapStore_.data() + n.heapBegin;
    const HeapItem item = h[pos];
    while (pos > 0) {
        const uint32_t parent = (pos - 1) / 2;
        if (h[parent].version <= item.version) break;
        h[pos] = h[parent];
        live_[h[pos].liveIdx].heapPos = pos;
        pos = parent;
    }
    h[pos] = item;
    live_[item.liveIdx].heapPos = pos;
}

void LeaseWatchBase::siftDown_(Name& n, uint32_t pos) noexcept {
    HeapItem* h = heapStore_.data() + n.heapBegin;
    const HeapItem item = h[pos];
    for (;;) {
        uint32_t child = 2 * pos + 1;
        if (child >= n.heapSize) break;
        if (child + 1 < n.heapSize && h[child + 1].version < h[child].version) ++child;
        if (item.version <= h[child].version) break;
        h[pos] = h[child];
        live_[h[pos].liveIdx].heapPos = pos;
        pos = child;
    }
    h[pos] = item;
    live_[item.liveIdx].heapPos = pos;
}

bool LeaseWatchBase::heapPush_(int32_t nameId, uint32_t liveIdx, int64_t version) {
    Name& n = names_[static_cast<uint32_t>(nameId)];
    if (n.heapSize >= o_.waitersPerName) return false;
    heapStore_[n.heapBegin + n.heapSize] = HeapItem{version, liveIdx};
    siftUp_(n, n.heapSize++);
    return true;
}

void LeaseWatchBase::heapRemove_(int32_t nameId, uint32_t pos) noexcept {
    Name& n = names_[static_cast<uint32_t>(nameId)];
    HeapItem* h = heapStore_.data() + n.heapBegin;
    const uint32_t last = --n.heapSize;
    if (pos == last) return;
    h[pos] = h[last];
    const uint32_t moved = h[pos].liveIdx;
    live_[moved].heapPos = pos;
    siftUp_(n, pos);
    siftDown_(n, live_[moved].heapPos);
}

}  // namespace rune::client
