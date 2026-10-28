// src/detail/EventLogStreamImpl.h — the EventLogStream<P> definitions (included by the .cpp files that explicitly
// instantiate it: CatalogReader.cpp, SecurityReader.cpp).  Catalog history Task 13, shared by Task 8.
#pragma once
#include <rune/client/detail/EventLogStream.h>

#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>

namespace rune::client::detail {

namespace {

using aeron::util::index_t;

// ---- The runner → app ring's frames ------------------------------------------------
// Every frame: [gen u32][total u32][offset i64][payload].  `gen` names the catalog
// subscription that wrote it (frames of an older subscription are skipped).
enum FrameType : int32_t {
    kWhole    = 1,   // one whole record: offset, value bytes
    kFirst    = 2,   // the first chunk of a record too large for one message (total = its size)
    kMore     = 3,   // a following chunk (the record is complete when `total` bytes arrived)
    kMarker   = 4,   // the caught-up marker: offset = the broker's high watermark; payload = the 8-byte epoch
    kOverflow = 5,   // the ring could not take the record / marker at offset: nothing from there was copied
    kTooLarge = 6,   // the record at offset (total = its size) can NEVER cross this ring, even empty
    kNote     = 7,   // the stream's history-epoch note: payload = the 8-byte epoch
};
constexpr index_t kHead = 16;
// Room always left for one control frame (kOverflow / kMarker / kNote: 8 header + 16 +
// 8, aligned 32), even across an end-of-buffer wrap (padding < one frame): 64 bytes.
constexpr index_t kControlReserve = 64;

constexpr index_t aligned(index_t n) noexcept {
    return aeron::util::BitUtil::align(n + static_cast<index_t>(rune::protocol::ring::RecordDescriptor::HEADER_LENGTH),
                                       static_cast<index_t>(rune::protocol::ring::RecordDescriptor::ALIGNMENT));
}

bool writeFrame(detail::OneToOneRingBuffer& ring, int32_t type, uint32_t gen, uint32_t total, int64_t offset,
                std::span<const uint8_t> chunk) {
    const auto len = static_cast<index_t>(kHead + chunk.size());
    return ring.writeWith(type, len, [&](aeron::concurrent::AtomicBuffer& dst, index_t at) {
        uint8_t* p = dst.buffer() + at;
        std::memcpy(p, &gen, 4);
        std::memcpy(p + 4, &total, 4);
        std::memcpy(p + 8, &offset, 8);
        if (!chunk.empty()) std::memcpy(p + kHead, chunk.data(), chunk.size());
    });
}

// ---- Runner-thread side: COPY only (no replay, no decode, no allocation) -------------
/// The ring could not take what starts at `offset`: say so (fits in the reserve the
/// earlier writes left; a second overflow in a row may not fit — the first one is
/// already in the ring, and the app re-subscribes on it).
void dutyOverflow(detail::OneToOneRingBuffer& ring, uint32_t gen, int64_t offset) {
    (void)writeFrame(ring, kOverflow, gen, 0, offset, {});
}

void dutyRecord(detail::OneToOneRingBuffer& ring, uint32_t gen, int64_t offset, std::span<const uint8_t> value) {
    const index_t maxChunk = ring.maxMsgLength() - kHead;
    if (value.size() <= static_cast<std::size_t>(maxChunk)) {
        const index_t need = aligned(kHead + static_cast<index_t>(value.size()));
        if (!ring.hasCapacityFor(need + need + kControlReserve) ||
            !writeFrame(ring, kWhole, gen, static_cast<uint32_t>(value.size()), offset, value)) {
            dutyOverflow(ring, gen, offset);
        }
        return;
    }
    // A record larger than one ring message: a length-prefixed sequence of frames
    // (kFirst carries the total; kMore follow), all or none.
    const std::size_t chunks = (value.size() + static_cast<std::size_t>(maxChunk) - 1) / static_cast<std::size_t>(maxChunk);
    const index_t full = aligned(kHead + maxChunk);
    const auto need = static_cast<int64_t>(chunks) * full + full + kControlReserve;
    if (need > ring.capacity()) {   // permanent: not even an empty ring takes it
        (void)writeFrame(ring, kTooLarge, gen, static_cast<uint32_t>(value.size()), offset, {});
        return;
    }
    if (!ring.hasCapacityFor(static_cast<index_t>(need))) {
        dutyOverflow(ring, gen, offset);
        return;
    }
    const auto total = static_cast<uint32_t>(value.size());
    for (std::size_t at = 0; at < value.size(); at += static_cast<std::size_t>(maxChunk)) {
        const auto n = std::min(static_cast<std::size_t>(maxChunk), value.size() - at);
        if (!writeFrame(ring, at == 0 ? kFirst : kMore, gen, total, offset, value.subspan(at, n))) {
            dutyOverflow(ring, gen, offset);   // the app drops the partial record on it
            return;
        }
    }
}

/// The note / marker: the epoch's 8 bytes (an Overflow if the ring cannot take it —
/// the app re-subscribes; nothing is decided on a frame that did not arrive).
void dutyEpochFrame(detail::OneToOneRingBuffer& ring, int32_t type, uint32_t gen, int64_t offset, uint64_t epoch) {
    uint8_t e[8];
    std::memcpy(e, &epoch, 8);
    if (!ring.hasCapacityFor(aligned(kHead + 8) + kControlReserve) ||
        !writeFrame(ring, type, gen, 0, offset, std::span<const uint8_t>(e, 8)))
        dutyOverflow(ring, gen, offset);
}

} // namespace

// -----------------------------------------------------------------------------------
template <class P>
EventLogStream<P>::EventLogStream(Client& client, CatalogReaderConfig cfg)
    : client_(client), cfg_(cfg), ringMem_(nullptr, &std::free), owner_(std::this_thread::get_id()) {
    std::size_t cap = 1024;
    while (cap < cfg_.ringBytes) cap <<= 1;   // a power of two (the ring's rule)
    cfg_.ringBytes = cap;
    const std::size_t len = cap + static_cast<std::size_t>(detail::OneToOneRingBuffer::trailerLength());
    // calloc: zeroed once per reader, and the untouched pages stay free (a Client builds
    // its reader eagerly, whether or not it ever reads the catalog).
    ringMem_.reset(static_cast<uint8_t*>(std::calloc(len, 1)));
    if (!ringMem_) throw std::bad_alloc();
    ring_ = std::make_unique<detail::OneToOneRingBuffer>(ringMem_.get(), len);
    retired_.reserve(kMaxRetired);
}

template <class P>
EventLogStream<P>::~EventLogStream() {
    // No runner callback may touch the ring once it is freed: unsubscribe what is
    // live, then wait until every subscription this reader created is finished.
    if (Stream* st = stream_()) retire_(std::move(st->sub));
    phase_ = Idle{};
    for (;;) {
        pruneRetired_();
        if (retired_.empty()) break;
        if (client_.cfg_.runnerMode == RunnerMode::Invoker) (void)client_.doWork();
        else std::this_thread::yield();
    }
}

template <class P>
void EventLogStream<P>::checkOwner_() const {
    // owner_ is immutable (the constructing thread): there is no claim to race for.
    if (owner_ != std::this_thread::get_id())
        throw std::logic_error(std::string(P::kReaderName) + ": used from a thread other than its owner (the thread that "
                               "constructed it — for a Client's reader, the Client's constructing thread)");
}

template <class P>
std::span<const typename P::Recorded> EventLogStream<P>::events() const {
    checkOwner_();
    return events_;
}
template <class P>
const typename P::State& EventLogStream<P>::state() const {
    checkOwner_();
    return replayer_.state();
}
template <class P>
int64_t EventLogStream<P>::nextOffset() const {
    checkOwner_();
    return replayer_.nextOffset();
}
template <class P>
std::optional<uint64_t> EventLogStream<P>::historyEpoch() const {
    checkOwner_();
    return cacheEpoch_;
}
template <class P>
const typename EventLogStream<P>::Stats& EventLogStream<P>::stats() const {
    checkOwner_();
    return stats_;
}

template <class P>
typename EventLogStream<P>::Stream* EventLogStream<P>::stream_() noexcept {
    if (auto* o = std::get_if<Opening>(&phase_)) return o;
    if (auto* st = std::get_if<Streaming>(&phase_)) return st;
    return nullptr;
}

template <class P>
uint32_t EventLogStream<P>::currentGen_() const noexcept {
    if (const auto* o = std::get_if<Opening>(&phase_)) return o->gen;
    if (const auto* st = std::get_if<Streaming>(&phase_)) return st->gen;
    return 0;   // never a subscription's generation
}

template <class P>
void EventLogStream<P>::retire_(std::unique_ptr<Subscription> sub) {
    if (!sub) return;
    sub->unsubscribe();
    retired_.push_back(std::move(sub));
}

template <class P>
void EventLogStream<P>::pruneRetired_() {
    std::erase_if(retired_, [](const std::unique_ptr<Subscription>& s) { return s->finished(); });
}

// Every subscription starts at the cache end: its epoch note decides whether the
// cache continues (same epoch) or is dropped and re-read from 0.
template <class P>
void EventLogStream<P>::subscribe_() {
    if (++nextGen_ == 0) ++nextGen_;
    const uint32_t gen = nextGen_;
    detail::OneToOneRingBuffer* ring = ring_.get();
    SubscribeOptions o;
    o.from           = From::offset(replayer_.nextOffset());
    o.onHistoryEpoch = [ring, gen](uint64_t epoch) { dutyEpochFrame(*ring, kNote, gen, -1, epoch); };
    o.onCaughtUp     = [ring, gen](int64_t hw, uint64_t epoch) { dutyEpochFrame(*ring, kMarker, gen, hw, epoch); };
    auto sub = std::make_unique<Subscription>(client_.subscribe(
        P::topic(), 0, std::move(o),
        [ring, gen](const Record& r) { dutyRecord(*ring, gen, r.offset, r.value); }));
    ++stats_.subscribes;
    phase_ = Opening{{gen, std::move(sub), std::nullopt}};
}

template <class P>
void EventLogStream<P>::finishIdle_() {
    if (Stream* st = stream_()) retire_(std::move(st->sub));
    phase_ = Idle{};
}

template <class P>
void EventLogStream<P>::truncateCache_(std::size_t keep) {
    if (keep >= events_.size()) return;
    for (std::size_t i = keep; i < eventBytes_.size(); ++i) cachedBytes_ -= eventBytes_[i];
    eventBytes_.resize(keep);
    events_.resize(keep);
    // A replayer cannot rewind: rebuild it over the kept (already verified) prefix.
    replayer_ = typename P::Replayer{};
    for (const auto& e : events_) {
        if (P::feed(replayer_, e.offset, e.event)) {   // verified before: cannot fail
            truncateCache_(static_cast<std::size_t>(replayer_.nextOffset()));
            break;
        }
    }
}

template <class P>
void EventLogStream<P>::dropCache_() {
    truncateCache_(0);
    cacheEpoch_.reset();
}

template <class P>
OpResult EventLogStream<P>::beginRefresh() {
    checkOwner_();
    pruneRetired_();
    if (!std::holds_alternative<Idle>(phase_) || retired_.size() >= kMaxRetired) return OpResult::BackPressured;
    subscribe_();
    return OpResult::Accepted;
}

// ---- App-thread side: drain, check the epoch, replay ----------------------------------
template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::append_(int64_t offset, std::span<const uint8_t> bytes,
                                           std::optional<CatalogResult<void>>& result) {
    if (events_.size() >= cfg_.maxEvents) {
        result = std::unexpected(CatalogReadError{CatalogReadError::Kind::CacheFull,
                                                  std::string("the ") + P::kName + " has more than " + std::to_string(cfg_.maxEvents) +
                                                      " events (CatalogReaderConfig::maxEvents)",
                                                  offset, 0});
        return Step::Done;
    }
    if (cachedBytes_ + bytes.size() > cfg_.maxBytes) {
        result = std::unexpected(CatalogReadError{CatalogReadError::Kind::CacheFull,
                                                  std::string("the ") + P::kName + " exceeds " + std::to_string(cfg_.maxBytes) +
                                                      " encoded bytes (CatalogReaderConfig::maxBytes)",
                                                  offset, 0});
        return Step::Done;
    }
    auto ev = P::decode(bytes);
    if (!ev) {
        result = std::unexpected(P::decodeFailure(offset, ev.error()));
        return Step::Done;   // the cache keeps the verified prefix; the next refresh fails the same way
    }
    if (auto err = P::feed(replayer_, offset, *ev)) {
        result = std::unexpected(std::move(*err));
        return Step::Done;
    }
    events_.push_back(typename P::Recorded{offset, std::move(*ev)});
    eventBytes_.push_back(static_cast<uint32_t>(bytes.size()));
    cachedBytes_ += bytes.size();
    return Step::Continue;
}

template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::protocolError_(const char* what, int64_t offset,
                                                  std::optional<CatalogResult<void>>& result) {
    result = std::unexpected(CatalogReadError{
        CatalogReadError::Kind::Rejected,
        std::string(P::kName) + " read: " + what + " (a broker that sends no " + P::kName +
            " history epoch — older than this client's " + P::kName + " reads)",
        offset, 0});
    return Step::Done;
}

template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::onNote_(uint64_t epoch) {
    if (auto* st = std::get_if<Streaming>(&phase_)) {
        if (epoch == st->epoch) return Step::Continue;   // a repeated note of the same stream: idempotent
        // A stream never changes its history (a change ends it): a broker that says
        // otherwise vouches for nothing — drop the cache, re-read.
        ++stats_.contradictions;
        dropCache_();
        return Step::Restart;
    }
    auto* o = std::get_if<Opening>(&phase_);
    if (o == nullptr) return Step::Continue;
    if (cacheEpoch_ && *cacheEpoch_ != epoch && !events_.empty()) {
        // Another committed history (a truncation below a served HW, another broker,
        // a broker that lost its epoch): nothing cached is vouched for.
        ++stats_.epochChanges;
        dropCache_();
        return Step::Restart;   // re-read from 0: its note adopts the epoch
    }
    cacheEpoch_ = epoch;   // the cache (possibly empty) continues under it
    phase_ = Streaming{{o->gen, std::move(o->sub), std::move(o->assembly)}, epoch};
    return Step::Continue;
}

template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::onRecord_(int64_t offset, std::span<const uint8_t> bytes,
                                             std::optional<CatalogResult<void>>& result) {
    if (std::holds_alternative<Opening>(phase_)) return protocolError_("a record before the history-epoch note", offset, result);
    if (!std::holds_alternative<Streaming>(phase_)) return Step::Continue;
    if (offset < replayer_.nextOffset()) return Step::Continue;   // a duplicate / a batch straddling the cache end
    return append_(offset, bytes, result);   // a gap is the replayer's explicit Gap error
}

template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::onMarker_(int64_t hw, uint64_t epoch, std::optional<CatalogResult<void>>& result) {
    if (std::holds_alternative<Opening>(phase_)) return protocolError_("a caught-up marker before the history-epoch note", hw, result);
    auto* st = std::get_if<Streaming>(&phase_);
    if (st == nullptr) return Step::Continue;
    if (epoch != st->epoch || hw < replayer_.nextOffset()) {
        // The marker contradicts its own stream (another epoch, or a committed end
        // below what this history already delivered): nothing is vouched for.
        ++stats_.contradictions;
        dropCache_();
        return Step::Restart;
    }
    if (hw == replayer_.nextOffset()) {
        result = CatalogResult<void>{};
        return Step::Done;
    }
    ++stats_.markerGaps;   // records below the marker never reached the ring: continue from the cache end
    return Step::Restart;
}

template <class P>
typename EventLogStream<P>::Step EventLogStream<P>::drain_(std::optional<CatalogResult<void>>& result) {
    const uint32_t gen = currentGen_();
    if (stream_() == nullptr) return Step::Continue;
    Step step = Step::Continue;
    auto handler = [&](int32_t type, aeron::concurrent::AtomicBuffer& buf, index_t at, index_t len) {
        if (step != Step::Continue || len < kHead) return;
        const uint8_t* p = buf.buffer() + at;
        uint32_t g = 0, total = 0;
        int64_t  offset = 0;
        std::memcpy(&g, p, 4);
        std::memcpy(&total, p + 4, 4);
        std::memcpy(&offset, p + 8, 8);
        if (g != gen) return;   // an earlier subscription's frame
        const std::span<const uint8_t> payload(p + kHead, static_cast<std::size_t>(len - kHead));
        // The stream may have moved Opening -> Streaming: its assembly moves with it.
        Stream* st = stream_();
        if (st == nullptr) return;
        std::optional<Assembly>& assembly = st->assembly;
        auto epochOf = [&] {
            uint64_t e = 0;
            if (payload.size() >= 8) std::memcpy(&e, payload.data(), 8);
            return e;
        };
        switch (type) {
        case kWhole:
            assembly.reset();
            step = onRecord_(offset, payload, result);
            return;
        case kFirst:
            assembly = Assembly{g, offset, total};
            assemblyBuf_.assign(payload.begin(), payload.end());
            return;
        case kMore:
            if (!assembly || assembly->gen != g || assembly->offset != offset) return;
            assemblyBuf_.insert(assemblyBuf_.end(), payload.begin(), payload.end());
            if (assemblyBuf_.size() >= assembly->total) {
                assembly.reset();
                ++stats_.reassembled;
                step = onRecord_(offset, assemblyBuf_, result);
            }
            return;
        case kNote:
            assembly.reset();
            step = onNote_(epochOf());
            return;
        case kMarker:
            assembly.reset();
            step = onMarker_(offset, epochOf(), result);
            return;
        case kTooLarge:
            assembly.reset();
            result = std::unexpected(CatalogReadError{
                CatalogReadError::Kind::RecordTooLarge,
                "the " + std::string(P::kName) + " event at offset " + std::to_string(offset) + " is " + std::to_string(total) +
                    " bytes: it can never cross the reader's " + std::to_string(cfg_.ringBytes) +
                    "-byte ring (raise CatalogReaderConfig::ringBytes)",
                offset, 0});
            step = Step::Done;
            return;
        case kOverflow:
            assembly.reset();
            ++stats_.ringOverflows;
            step = Step::Restart;   // continue from the cache end (what was copied is kept)
            return;
        default:
            return;
        }
    };
    while (step == Step::Continue) {
        int n = ring_->read(handler, 1);
        if (n == 0) n = ring_->read(handler, 1);   // the first read may only have skipped end-of-buffer padding
        if (n == 0) break;
    }
    return step;
}

template <class P>
std::optional<CatalogResult<void>> EventLogStream<P>::pollRefresh(int64_t nowNs, int64_t deadlineNs) {
    checkOwner_();
    pruneRetired_();
    if (std::holds_alternative<Idle>(phase_)) return std::nullopt;   // no refresh in flight

    auto timeout = [&](std::string why) {
        return std::unexpected(CatalogReadError{CatalogReadError::Kind::Timeout, std::move(why), replayer_.nextOffset(), 0});
    };
    std::optional<CatalogResult<void>> result;
    if (auto* b = std::get_if<Backoff>(&phase_)) {
        if (nowNs >= deadlineNs) {
            phase_ = Idle{};
            return timeout(std::string(P::kName) + " read: no caught-up marker before the deadline "
                           "(re-subscribing after a lost / refused subscription)");
        }
        if (nowNs < b->retryAtNs) return std::nullopt;
        subscribe_();
        return std::nullopt;
    }

    Subscription& sub = *stream_()->sub;
    // finished() first: every frame its callbacks wrote precedes it (the status ring
    // publishes after the last callback), so the drain below sees all of them.
    const bool finished = sub.finished();
    const Step step = drain_(result);
    if (step == Step::Done) {
        finishIdle_();   // OK, or an explicit error (the verified prefix and its epoch are kept)
        return result;
    }
    if (step == Step::Restart) {
        finishIdle_();
        if (nowNs >= deadlineNs) return timeout(std::string(P::kName) + " read: no caught-up marker before the deadline");
        subscribe_();   // from the cache end (0 after a drop): the new stream's note decides
        return std::nullopt;
    }
    if (finished) {
        const SubscriptionFailReason why = sub.failReason();
        const uint16_t code = sub.brokerErrorCode();
        finishIdle_();
        namespace pc = rune::protocol::client;
        auto backoff = [&] {
            ++stats_.resubscribes;
            phase_ = Backoff{nowNs + cfg_.resubscribeBackoffNs};
            return std::nullopt;
        };
        switch (why) {
        case SubscriptionFailReason::CatalogReadEnded:
            // A truncation below a served HW ended the read: resubscribe from the cache
            // end — the broker's new epoch (its note) drops the cache (binding: a stale
            // prefix is never served).
            ++stats_.readsEnded;
            subscribe_();
            return std::nullopt;
        case SubscriptionFailReason::SessionLost:
        case SubscriptionFailReason::TimedOut:
            return backoff();
        case SubscriptionFailReason::RejectedByBroker:
            if (code == static_cast<uint16_t>(pc::ErrorCode::NotLeader))   // the broker's intake is busy: retriable
                return backoff();
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Rejected,
                                                    std::string(P::kName) + " Subscribe refused by the broker (error code " +
                                                        std::to_string(code) + ")",
                                                    replayer_.nextOffset(), code});
        case SubscriptionFailReason::NotAuthorized:   // authentication (D21): catalog reads need DESCRIBE ON CLUSTER
        case SubscriptionFailReason::SessionRevoked:
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Rejected,
                                                    std::string(why == SubscriptionFailReason::NotAuthorized
                                                                    ? std::string(P::kName) + " read not authorized (NotAuthorized)"
                                                                    : std::string(P::kName) + " read ended: the session was revoked") +
                                                        " (error code " + std::to_string(code) + ")",
                                                    replayer_.nextOffset(), code});
        case SubscriptionFailReason::LeaderElsewhereWhileGroupActive:
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Rejected,
                                                    std::string(P::kName) + " Subscribe could not be routed",
                                                    replayer_.nextOffset(), 0});
        case SubscriptionFailReason::None:
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Closed, "client closed",
                                                    replayer_.nextOffset(), 0});
        }
    }
    if (nowNs >= deadlineNs) {
        finishIdle_();   // the prefix read so far (all under the cache's epoch) is kept
        return timeout(std::string(P::kName) + " read: no caught-up marker before the deadline (the serving "
                       "broker's committed prefix is not current — e.g. no " + P::kName + " leader)");
    }
    return std::nullopt;
}

template <class P>
CatalogResult<void> EventLogStream<P>::refresh(std::chrono::milliseconds timeout) {
    checkOwner_();
    client_.checkBlockingCaller_(P::kRefreshName);
    const bool invoker = client_.cfg_.runnerMode == RunnerMode::Invoker;
    auto now = [] {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch()).count();
    };
    const int64_t deadline = now() + std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count();
    int idle = 0;
    auto pump = [&] {
        if (invoker) {
            if (client_.doWork() > 0) idle = 0;
            else if (++idle > 64) std::this_thread::sleep_for(std::chrono::microseconds(50));
        } else if (++idle > 64) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        } else {
            std::this_thread::yield();
        }
    };
    // A non-blocking refresh still in flight is driven to its end first (its
    // result belongs to nobody now); released subscriptions are waited out.
    for (;;) {
        if (beginRefresh() == OpResult::Accepted) break;
        if (now() >= deadline)
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Timeout,
                                                    std::string(P::kName) + " read: could not start before the deadline",
                                                    replayer_.nextOffset(), 0});
        (void)pollRefresh(now(), deadline);
        pump();
    }
    for (;;) {
        if (auto r = pollRefresh(now(), deadline)) return *r;
        pump();
    }
}

} // namespace rune::client::detail
