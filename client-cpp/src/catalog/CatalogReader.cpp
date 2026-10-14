// src/catalog/CatalogReader.cpp — catalog history Task 13 (see CatalogReader.h).
#include <rune/client/catalog/CatalogReader.h>

#include <rune/client/Client.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/detail/OneToOneRingBuffer.h>
#include <rune/protocol/client/Protocol.h>

#include <rune/catalog/CatalogEvent.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <new>
#include <stdexcept>
#include <thread>
#include <utility>

namespace rune::client {

namespace {

namespace rcat = rune::catalog;
using aeron::util::index_t;

// ---- The runner → app ring's frames ------------------------------------------------
// Every frame: [gen u32][total u32][offset i64][payload].  `gen` names the catalog
// subscription that wrote it (frames of an older subscription are skipped).
enum FrameType : int32_t {
    kWhole    = 1,   // one whole record: offset, value bytes
    kFirst    = 2,   // the first chunk of a record too large for one message (total = its size)
    kMore     = 3,   // a following chunk (the record is complete when `total` bytes arrived)
    kMarker   = 4,   // the caught-up marker: offset = the broker's high watermark
    kOverflow = 5,   // the ring could not take the record / marker at offset: nothing from there was copied
    kTooLarge = 6,   // the record at offset (total = its size) can NEVER cross this ring, even empty
};
constexpr index_t kHead = 16;
// Room always left for one control frame (kOverflow / kMarker: 8 header + 16, aligned 32),
// even across an end-of-buffer wrap (padding < one frame): 64 bytes.
constexpr index_t kControlReserve = 64;

constexpr index_t aligned(index_t n) noexcept {
    return aeron::util::BitUtil::align(n + static_cast<index_t>(aeron::concurrent::ringbuffer::RecordDescriptor::HEADER_LENGTH),
                                       static_cast<index_t>(aeron::concurrent::ringbuffer::RecordDescriptor::ALIGNMENT));
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

void dutyMarker(detail::OneToOneRingBuffer& ring, uint32_t gen, int64_t hw) {
    if (!ring.hasCapacityFor(aligned(kHead) + kControlReserve) || !writeFrame(ring, kMarker, gen, 0, hw, {}))
        dutyOverflow(ring, gen, hw);
}

CatalogReadError replayError(const rcat::ReplayError& e) {
    return CatalogReadError{CatalogReadError::Kind::Replay, rcat::describe(e), e.offset, 0};
}

} // namespace

// -----------------------------------------------------------------------------------
CatalogReader::CatalogReader(Client& client, CatalogReaderConfig cfg)
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

CatalogReader::~CatalogReader() {
    // No runner callback may touch the ring once it is freed: unsubscribe what is
    // live, then wait until every subscription this reader created is finished.
    if (Stream* st = stream_()) retire_(std::move(st->sub));
    phase_ = Idle{Resume{Begin::Tail, 0}};
    for (;;) {
        pruneRetired_();
        if (retired_.empty()) break;
        if (client_.cfg_.runnerMode == RunnerMode::Invoker) (void)client_.doWork();
        else std::this_thread::yield();
    }
}

void CatalogReader::checkOwner_() {
    // owner_ is immutable (the constructing thread): there is no claim to race for.
    if (owner_ != std::this_thread::get_id())
        throw std::logic_error("CatalogReader: used from a thread other than its owner (the thread that "
                               "constructed it — for a Client's reader, the Client's constructing thread)");
}

CatalogReader::Stream* CatalogReader::stream_() noexcept {
    if (auto* a = std::get_if<Appending>(&phase_)) return a;
    if (auto* v = std::get_if<Revalidating>(&phase_)) return v;
    return nullptr;
}

uint32_t CatalogReader::currentGen_() const noexcept {
    if (const auto* a = std::get_if<Appending>(&phase_)) return a->gen;
    if (const auto* v = std::get_if<Revalidating>(&phase_)) return v->gen;
    return 0;   // never a subscription's generation
}

bool CatalogReader::needsRevalidation() const noexcept {
    if (const auto* i = std::get_if<Idle>(&phase_)) return i->next.begin == Begin::Full;
    if (const auto* b = std::get_if<Backoff>(&phase_)) return b->resume.begin == Begin::Full;
    if (const auto* v = std::get_if<Revalidating>(&phase_)) return v->begin == Begin::Full;
    return false;
}

CatalogReader::Resume CatalogReader::continueOf_() const noexcept {
    if (const auto* v = std::get_if<Revalidating>(&phase_)) return Resume{v->begin, v->verified};
    return Resume{Begin::Append, 0};
}

CatalogReader::Resume CatalogReader::reopenOf_() const noexcept {
    if (const auto* v = std::get_if<Revalidating>(&phase_)) return Resume{v->begin, v->verified};
    return Resume{Begin::Tail, 0};   // re-verify the overlap event: the cache may have grown, the peer may differ
}

void CatalogReader::retire_(std::unique_ptr<Subscription> sub) {
    if (!sub) return;
    sub->unsubscribe();
    retired_.push_back(std::move(sub));
}

void CatalogReader::pruneRetired_() {
    std::erase_if(retired_, [](const std::unique_ptr<Subscription>& s) { return s->finished(); });
}

void CatalogReader::subscribe_(Resume r) {
    const auto cacheEnd = static_cast<int64_t>(events_.size());
    if (cacheEnd == 0) r = Resume{Begin::Append, 0};   // nothing cached: nothing to verify
    if (r.begin == Begin::Tail) r.verified = cacheEnd - 1;
    else if (r.begin == Begin::Full) r.verified = std::clamp<int64_t>(r.verified, 0, cacheEnd - 1);
    if (++nextGen_ == 0) ++nextGen_;
    const uint32_t gen = nextGen_;
    const int64_t from = r.begin == Begin::Append ? replayer_.nextOffset() : r.verified;
    detail::OneToOneRingBuffer* ring = ring_.get();
    SubscribeOptions o;
    o.from       = From::offset(from);
    o.onCaughtUp = [ring, gen](int64_t hw) { dutyMarker(*ring, gen, hw); };
    auto sub = std::make_unique<Subscription>(client_.subscribe(
        TopicHandle::catalog(), 0, std::move(o),
        [ring, gen](const Record& r) { dutyRecord(*ring, gen, r.offset, r.value); }));
    ++stats_.subscribes;
    if (r.begin == Begin::Append) phase_ = Appending{{gen, std::move(sub), std::nullopt}};
    else phase_ = Revalidating{{gen, std::move(sub), std::nullopt}, r.begin, cacheEnd, r.verified};
}

void CatalogReader::finishIdle_() {
    // Where the next refresh starts if this one ended without a verdict (the caller sets
    // phase_ itself when it has a better answer).
    const Resume next = reopenOf_();
    if (Stream* st = stream_()) retire_(std::move(st->sub));
    phase_ = Idle{next};
}

void CatalogReader::toAppending_() noexcept {
    auto* v = std::get_if<Revalidating>(&phase_);
    if (v == nullptr) return;
    phase_ = Appending{{v->gen, std::move(v->sub), std::nullopt}};
}

void CatalogReader::truncateCache_(std::size_t keep) {
    if (keep >= events_.size()) return;
    for (std::size_t i = keep; i < eventBytes_.size(); ++i) cachedBytes_ -= eventBytes_[i];
    eventBytes_.resize(keep);
    events_.resize(keep);
    // A replayer cannot rewind: rebuild it over the kept (already verified) prefix.
    replayer_ = rcat::CatalogReplayer{};
    for (const auto& e : events_) {
        if (!replayer_.feedDecoded(e.offset, e.event)) {   // verified before: cannot fail
            truncateCache_(static_cast<std::size_t>(replayer_.nextOffset()));
            break;
        }
    }
}

OpResult CatalogReader::beginRefresh() {
    checkOwner_();
    pruneRetired_();
    if (!std::holds_alternative<Idle>(phase_) || retired_.size() >= kMaxRetired) return OpResult::BackPressured;
    subscribe_(std::get<Idle>(phase_).next);
    return OpResult::Accepted;
}

// ---- App-thread side: drain, replay, re-validate ------------------------------------
CatalogReader::Step CatalogReader::append_(int64_t offset, std::span<const uint8_t> bytes,
                                           std::optional<CatalogResult<void>>& result) {
    if (events_.size() >= cfg_.maxEvents) {
        result = std::unexpected(CatalogReadError{CatalogReadError::Kind::CacheFull,
                                                  "the catalog has more than " + std::to_string(cfg_.maxEvents) +
                                                      " events (CatalogReaderConfig::maxEvents)",
                                                  offset, 0});
        return Step::Done;
    }
    if (cachedBytes_ + bytes.size() > cfg_.maxBytes) {
        result = std::unexpected(CatalogReadError{CatalogReadError::Kind::CacheFull,
                                                  "the catalog exceeds " + std::to_string(cfg_.maxBytes) +
                                                      " encoded bytes (CatalogReaderConfig::maxBytes)",
                                                  offset, 0});
        return Step::Done;
    }
    auto ev = rcat::decode(bytes);
    if (!ev) {
        rcat::ReplayError e;
        e.offset = offset;
        e.code   = rcat::ReplayError::Code::Decode;
        e.decode = ev.error();
        result   = std::unexpected(replayError(e));
        return Step::Done;   // the cache keeps the verified prefix; the next refresh fails the same way
    }
    if (auto applied = replayer_.feedDecoded(offset, *ev); !applied) {
        result = std::unexpected(replayError(applied.error()));
        return Step::Done;
    }
    events_.push_back(rcat::RecordedEvent{offset, std::move(*ev)});
    eventBytes_.push_back(static_cast<uint32_t>(bytes.size()));
    cachedBytes_ += bytes.size();
    return Step::Continue;
}

CatalogReader::Step CatalogReader::onRecord_(int64_t offset, std::span<const uint8_t> bytes,
                                             std::optional<CatalogResult<void>>& result) {
    if (std::holds_alternative<Appending>(phase_)) {
        if (offset < replayer_.nextOffset()) return Step::Continue;   // a batch straddling the cache end: replayed already
        return append_(offset, bytes, result);   // a gap is the replayer's explicit Gap error
    }
    auto* v = std::get_if<Revalidating>(&phase_);
    if (v == nullptr) return Step::Continue;
    if (offset < v->verified) return Step::Continue;   // a duplicate / straddling batch: compared already
    if (offset > v->verified) {
        rcat::ReplayError e;
        e.offset = v->verified;
        e.code   = rcat::ReplayError::Code::Gap;
        e.detail = "re-validation: the next record is at offset " + std::to_string(offset);
        result   = std::unexpected(replayError(e));
        return Step::Done;   // the cache is not served until re-validated
    }
    // offset == verified < cacheEnd: the new history's event must equal the cached one.
    const auto ev = rcat::decode(bytes);
    if (!ev) {
        // A record that does not decode is an error, not evidence of a divergence: the
        // cache is left alone (a corrupt record from a broker bug must not discard it).
        rcat::ReplayError e;
        e.offset = offset;
        e.code   = rcat::ReplayError::Code::Decode;
        e.decode = ev.error();
        result   = std::unexpected(replayError(e));
        return Step::Done;
    }
    if (*ev == events_[static_cast<std::size_t>(offset)].event) {
        if (++v->verified < v->cacheEnd) return Step::Continue;
        // The whole cached prefix (Full) / the cache-end overlap event (Tail) is the
        // committed history: append from here on.
        if (v->begin == Begin::Full) ++stats_.revalidations;
        else ++stats_.overlapChecks;
        toAppending_();
        return Step::Continue;
    }
    // The history differs at `offset`.  Full: [0, offset) was verified, keep it and drop
    // the rest.  Tail: only this one event was compared — nothing below it is vouched
    // for (the history may have been rewritten from any earlier offset): drop everything.
    const std::size_t keep = v->begin == Begin::Full ? static_cast<std::size_t>(offset) : 0;
    if (v->begin == Begin::Full) ++stats_.revalidations;
    ++stats_.prefixDivergences;
    truncateCache_(keep);
    toAppending_();
    if (keep == static_cast<std::size_t>(offset)) return append_(offset, bytes, result);   // this very record
    return Step::Restart;   // an empty cache: re-read the catalog from 0
}

CatalogReader::Step CatalogReader::onMarker_(int64_t hw, std::optional<CatalogResult<void>>& result) {
    if (std::holds_alternative<Appending>(phase_)) {
        if (hw == replayer_.nextOffset()) {
            result = CatalogResult<void>{};
            return Step::Done;
        }
        if (hw > replayer_.nextOffset()) {
            ++stats_.markerGaps;   // records below the marker never reached the ring: read them again
            return Step::Restart;
        }
        // hw < what this refresh replayed: the broker's committed end contradicts the
        // records it just delivered (or the cache): nothing is vouched for — re-read and
        // re-validate the whole cache against it.
        ++stats_.markerGaps;
        return Step::RestartFull;
    }
    auto* v = std::get_if<Revalidating>(&phase_);
    if (v == nullptr) return Step::Continue;
    if (hw > v->verified) {
        ++stats_.markerGaps;
        return Step::Restart;
    }
    // The committed history ends at hw, inside the cache: the cached tail beyond it is
    // not (any longer) committed.
    ++stats_.prefixDivergences;
    if (v->begin == Begin::Full) {
        // [0, verified) was compared equal and hw <= verified: keep [0, hw).
        ++stats_.revalidations;
        truncateCache_(static_cast<std::size_t>(std::max<int64_t>(hw, 0)));
        toAppending_();
        result = CatalogResult<void>{};
        return Step::Done;
    }
    // Tail: only the overlap offset was due — nothing below hw is vouched for. Drop all, re-read.
    truncateCache_(0);
    toAppending_();
    return Step::Restart;
}

CatalogReader::Step CatalogReader::drain_(std::optional<CatalogResult<void>>& result) {
    const uint32_t gen = currentGen_();
    Stream* st = stream_();
    if (st == nullptr) return Step::Continue;
    std::optional<Assembly>& assembly = st->assembly;
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
        case kMarker:
            assembly.reset();
            step = onMarker_(offset, result);
            return;
        case kTooLarge:
            assembly.reset();
            result = std::unexpected(CatalogReadError{
                CatalogReadError::Kind::RecordTooLarge,
                "the catalog event at offset " + std::to_string(offset) + " is " + std::to_string(total) +
                    " bytes: it can never cross the reader's " + std::to_string(cfg_.ringBytes) +
                    "-byte ring (raise CatalogReaderConfig::ringBytes)",
                offset, 0});
            step = Step::Done;
            return;
        case kOverflow:
            assembly.reset();
            ++stats_.ringOverflows;
            step = Step::Restart;   // re-read from where the copy stopped
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

std::optional<CatalogResult<void>> CatalogReader::pollRefresh(int64_t nowNs, int64_t deadlineNs) {
    checkOwner_();
    pruneRetired_();
    if (std::holds_alternative<Idle>(phase_)) return std::nullopt;   // no refresh in flight

    auto timeout = [&](const char* why) {
        return std::unexpected(CatalogReadError{CatalogReadError::Kind::Timeout, why, replayer_.nextOffset(), 0});
    };
    std::optional<CatalogResult<void>> result;
    if (auto* b = std::get_if<Backoff>(&phase_)) {
        if (nowNs >= deadlineNs) {
            phase_ = Idle{b->resume};
            return timeout("catalog read: no caught-up marker before the deadline "
                           "(re-subscribing after a lost / refused subscription)");
        }
        if (nowNs < b->retryAtNs) return std::nullopt;
        subscribe_(b->resume);
        return std::nullopt;
    }

    Subscription& sub = *stream_()->sub;
    // finished() first: every frame its callbacks wrote precedes it (the status ring
    // publishes after the last callback), so the drain below sees all of them.
    const bool finished = sub.finished();
    const Step step = drain_(result);
    if (step == Step::Done) {
        // OK, or an explicit error: where the NEXT refresh starts (the verified prefix is
        // kept; an unfinished re-validation resumes at its verified offset).
        finishIdle_();
        return result;
    }
    if (step == Step::Restart || step == Step::RestartFull) {
        const Resume again = step == Step::RestartFull ? Resume{Begin::Full, 0} : continueOf_();
        finishIdle_();
        if (nowNs >= deadlineNs) {
            if (step == Step::RestartFull) phase_ = Idle{again};   // the contradiction is not forgotten
            return timeout("catalog read: no caught-up marker before the deadline");
        }
        subscribe_(again);   // a Revalidating restart resumes at its verified offset: always progress
        return std::nullopt;
    }
    if (finished) {
        const Resume reopen = reopenOf_();
        const SubscriptionFailReason why = sub.failReason();
        const uint16_t code = sub.brokerErrorCode();
        finishIdle_();
        namespace pc = rune::protocol::client;
        auto backoff = [&] {
            ++stats_.resubscribes;
            phase_ = Backoff{nowNs + cfg_.resubscribeBackoffNs, reopen};
            return std::nullopt;
        };
        switch (why) {
        case SubscriptionFailReason::CatalogReadEnded:
            // A truncation below a served HW: the cached prefix may be stale — re-read
            // from 0 and re-validate it (binding: never served silently).
            ++stats_.readsEnded;
            subscribe_(Resume{Begin::Full, 0});
            return std::nullopt;
        case SubscriptionFailReason::SessionLost:
        case SubscriptionFailReason::TimedOut:
            return backoff();
        case SubscriptionFailReason::RejectedByBroker:
            if (code == static_cast<uint16_t>(pc::ErrorCode::NotLeader))   // the broker's intake is busy: retriable
                return backoff();
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Rejected,
                                                    "catalog Subscribe refused by the broker (error code " +
                                                        std::to_string(code) + ")",
                                                    replayer_.nextOffset(), code});
        case SubscriptionFailReason::LeaderElsewhereWhileGroupActive:
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Rejected,
                                                    "catalog Subscribe could not be routed",
                                                    replayer_.nextOffset(), 0});
        case SubscriptionFailReason::None:
            return std::unexpected(CatalogReadError{CatalogReadError::Kind::Closed, "client closed",
                                                    replayer_.nextOffset(), 0});
        }
    }
    if (nowNs >= deadlineNs) {
        finishIdle_();   // a re-validation cut short resumes at its verified offset next time
        return timeout("catalog read: no caught-up marker before the deadline (the serving "
                       "broker's committed prefix is not current — e.g. no catalog leader)");
    }
    return std::nullopt;
}

CatalogResult<void> CatalogReader::refresh(std::chrono::milliseconds timeout) {
    checkOwner_();
    client_.checkBlockingCaller_("CatalogReader::refresh()");
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
                                                    "catalog read: could not start before the deadline",
                                                    replayer_.nextOffset(), 0});
        (void)pollRefresh(now(), deadline);
        pump();
    }
    for (;;) {
        if (auto r = pollRefresh(now(), deadline)) return *r;
        pump();
    }
}

} // namespace rune::client
