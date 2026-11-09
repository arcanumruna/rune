// src/lease/LeaseRecipe.cpp - the non-template body of the lease recipes (Task 20).  See lease/LeaseRecipe.h.
#include <rune/client/lease/LeaseRecipe.h>

#include <rune/protocol/LeaseRecord.h>

#include <algorithm>
#include <cstring>

namespace rune::client::detail {
namespace {

constexpr std::size_t kMinLeaseRow = rune::protocol::record::kLeaseRemainingValueOffset + 8;   // 36
constexpr int64_t     kNoReport    = INT64_MIN;

void putHex(uint8_t* out, uint64_t v, int digits) noexcept {
    static constexpr char kHex[] = "0123456789abcdef";
    for (int i = digits - 1; i >= 0; --i, v >>= 4) out[i] = static_cast<uint8_t>(kHex[v & 0xF]);
}
[[nodiscard]] int64_t load64(const uint8_t* p) noexcept {
    int64_t v;
    std::memcpy(&v, p, 8);
    return v;
}
/// The statuses after which a grant may have been applied: the recipe reads the key.
[[nodiscard]] bool unknownOutcome(OpStatus s) noexcept {
    return s == OpStatus::VerdictUnknown || s == OpStatus::Timeout || s == OpStatus::StoredIdUnknown || s == OpStatus::DeliveryFailed ||
           s == OpStatus::SessionLost;
}
[[nodiscard]] bool isEnd(LeaseEventKind k) noexcept {
    return k == LeaseEventKind::Revoked || k == LeaseEventKind::Expired || k == LeaseEventKind::Vanished;
}

}  // namespace

// ---------------------------------------------------------------------------------------------------------------------------------
// LeaseRecipeHostCore
// ---------------------------------------------------------------------------------------------------------------------------------

CompletionsConfig LeaseRecipeHostCore::queueConfig_(uint32_t capacity, uint32_t viewRowLen) {
    CompletionsConfig c;
    uint32_t rows = viewRowLen + 64u;
    rows = std::max<uint32_t>(256u, (rows + 7u) & ~7u);
    c.maxResultBytes = rows;
    c.maxErrorText   = 192;
    // A recipe has at most one grant or key read in flight, plus a few best-effort revokes of a key it gave up: 4 records each.
    const uint64_t rec  = CompletionRing::completionRecordBytes(rows);
    const uint64_t need = (4ull * capacity + 8ull) * rec + 2ull * rec + 4096ull;
    uint32_t cap = 4096;
    while (cap < need) cap <<= 1;
    c.capacityBytes = cap;
    return c;
}

LeaseRecipeHostCore::LeaseRecipeHostCore(Client& client, LeaseSessionCore& session, LeaseWatchBase& watch, LeaseTopicCore& topic,
                                         LeaseRecipeCore** table, uint32_t capacity, int16_t* slotMap, uint32_t slotMapSize)
    : client_(client)
    , session_(session)
    , watch_(watch)
    , topic_(topic)
    , table_(table)
    , capacity_(capacity)
    , slotMap_(slotMap)
    , slotMapSize_(slotMapSize)
    , cq_(client, queueConfig_(capacity, topic.viewRowLen())) {
    for (uint32_t i = 0; i < capacity_; ++i) table_[i] = nullptr;
    for (uint32_t i = 0; i < slotMapSize_; ++i) slotMap_[i] = -1;
    usable_ = topic.hasFixedKeyWidth() && topic.viewRowLen() != 0 && session.capacity() <= slotMapSize && cq_.valid();
}

LeaseRecipeHostCore::~LeaseRecipeHostCore() {
    for (uint32_t i = 0; i < capacity_; ++i)   // the recipes still alive forget this host
        if (table_[i] != nullptr) { LeaseRecipeCore* rc = table_[i]; table_[i] = nullptr; rc->hostGone(); }
}

int LeaseRecipeHostCore::attach(LeaseRecipeCore* rc) noexcept {
    for (uint32_t i = 0; i < capacity_; ++i)
        if (table_[i] == nullptr) { table_[i] = rc; ++count_; return static_cast<int>(i); }
    return -1;
}

void LeaseRecipeHostCore::detach(int idx) noexcept {
    if (idx < 0 || static_cast<uint32_t>(idx) >= capacity_ || table_[idx] == nullptr) return;
    table_[idx] = nullptr;
    --count_;
}

void LeaseRecipeHostCore::mapRef(LeaseRef ref, int idx) noexcept {
    if (ref.slot < slotMapSize_) slotMap_[ref.slot] = static_cast<int16_t>(idx);
}

void LeaseRecipeHostCore::unmapRef(LeaseRef ref) noexcept {
    if (ref.slot < slotMapSize_) slotMap_[ref.slot] = -1;
}

bool LeaseRecipeHostCore::onLost(LeaseRef ref, LostReason reason) {
    if (ref.slot >= slotMapSize_ || slotMap_[ref.slot] < 0) return false;
    LeaseRecipeCore* rc = table_[static_cast<uint32_t>(slotMap_[ref.slot])];
    if (rc == nullptr || !rc->holdsRef(ref)) return false;
    rc->onLost(reason);
    return true;
}

bool LeaseRecipeHostCore::onAtRisk(LeaseRef ref, AtRiskEdge edge) {
    if (ref.slot >= slotMapSize_ || slotMap_[ref.slot] < 0) return false;
    LeaseRecipeCore* rc = table_[static_cast<uint32_t>(slotMap_[ref.slot])];
    if (rc == nullptr || !rc->holdsRef(ref)) return false;
    rc->onAtRisk(edge);
    return true;
}

int LeaseRecipeHostCore::doWork(int64_t /*nowNs*/) {
    int n = cq_.poll([this](const Completion& c, std::span<const uint8_t> payload) {
        const uint32_t idx = static_cast<uint32_t>((c.token >> 36) & 0xFFFu);
        const uint32_t tag = static_cast<uint32_t>((c.token >> 32) & 0xFu);
        const uint32_t no  = static_cast<uint32_t>(c.token & 0xFFFFFFFFu);
        if (idx >= capacity_ || table_[idx] == nullptr) return;   // the recipe left; the answer is dropped
        table_[idx]->onCompletion(tag, no, c, payload);
    });
    if (releasing_ != 0) {   // the revokes the session finished: the entry is gone from the session
        for (uint32_t i = 0; i < capacity_ && releasing_ != 0; ++i)
            if (table_[i] != nullptr && table_[i]->releasing()) { table_[i]->pollReleasing(); ++n; }
    }
    return n;
}

// ---------------------------------------------------------------------------------------------------------------------------------
// LeaseRecipeCore: construction
// ---------------------------------------------------------------------------------------------------------------------------------

LeaseRecipeCore::LeaseRecipeCore(LeaseRecipeHostCore& host, std::string_view name, const LeaseRecipeCallbacks& cb, uint32_t rowCapacity)
    : host_(&host), watch_(&host.watch()), cb_(cb) {
    if (!host.usable()) return;
    LeaseTopicCore& topic = host.topic();
    const uint32_t width = topic.revokeKeyWidth();
    if (name.empty() || width > rune::protocol::kMaxLeaseKeyBytes || name.size() + kKeySuffix > width || topic.partitionCount() != 1) return;
    std::memcpy(name_, name.data(), name.size());
    nameLen_ = static_cast<uint8_t>(name.size());
    nameId_  = watch_->registerName(std::span<const uint8_t>(name_, nameLen_));
    if (nameId_ < 0) return;
    hostIdx_ = host.attach(this);
    if (hostIdx_ < 0) return;
    if (!watch_->bindName(nameId_, &LeaseRecipeCore::onWatchEvent, this, &LeaseRecipeCore::onWatchGone)) {
        host.detach(hostIdx_);
        hostIdx_ = -1;
        return;
    }
    row_.assign(rowCapacity, 0);
    valid_ = true;
}

LeaseRecipeCore::~LeaseRecipeCore() {
    if (host_ == nullptr || hostIdx_ < 0) return;
    if (watch_ != nullptr) (void)watch_->unbindName(nameId_, this);
    LeaseRef ref;
    int64_t  version = 0;
    if (refOf_(ref, version)) {   // a lease still held leaves the session with a fenced revoke (best effort)
        (void)host_->session().revoke(ref);
        host_->unmapRef(ref);
    } else if (const auto* r = std::get_if<Releasing>(&st_)) {
        host_->unmapRef(r->ref);
        host_->noteReleasing(-1);
    }
    st_ = Idle{};
    if (pend_.active) complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseReleased), -1);
    host_->detach(hostIdx_);
}

void LeaseRecipeCore::hostGone() noexcept {
    if (watch_ != nullptr && nameId_ >= 0) (void)watch_->unbindName(nameId_, this);
    if (pend_.active) complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseReleased), -1);
    host_    = nullptr;
    hostIdx_ = -1;
    valid_   = false;
    st_      = Idle{};
}

void LeaseRecipeCore::onWatchGone(void* ctx, LeaseWatchBase* /*watch*/) {
    auto* rc = static_cast<LeaseRecipeCore*>(ctx);
    rc->watch_ = nullptr;
    rc->valid_ = false;
}

bool LeaseRecipeCore::refOf_(LeaseRef& ref, int64_t& version) const noexcept {
    if (const auto* j = std::get_if<Joining>(&st_)) { ref = j->ref; version = j->version; return true; }
    if (const auto* w = std::get_if<Waiting>(&st_)) { ref = w->ref; version = w->version; return true; }
    if (const auto* o = std::get_if<Owner>(&st_)) { ref = o->ref; version = o->version; return true; }
    if (const auto* s = std::get_if<Suspended>(&st_)) { ref = s->ref; version = s->version; return true; }
    return false;
}

bool LeaseRecipeCore::holdsRef(LeaseRef ref) const noexcept {
    LeaseRef r;
    int64_t  v = 0;
    if (refOf_(r, v)) return r == ref;
    if (const auto* rel = std::get_if<Releasing>(&st_)) return rel->ref == ref;
    return false;
}

std::optional<std::pair<int64_t, std::span<const uint8_t>>> LeaseRecipeCore::leader() const noexcept {
    if (watch_ == nullptr || nameId_ < 0) return std::nullopt;
    return watch_->lowest(nameId_);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Claiming
// ---------------------------------------------------------------------------------------------------------------------------------

void LeaseRecipeCore::buildKey_(uint32_t campaignNo) {
    keyLen_ = static_cast<uint8_t>(host_->topic().revokeKeyWidth());
    std::memset(key_, 0, sizeof key_);
    std::memcpy(key_, name_, nameLen_);
    uint8_t* p = key_ + nameLen_;
    *p++ = '/';
    putHex(p, host_->holderId(), 16);
    p += 16;
    *p++ = '-';
    putHex(p, campaignNo, 8);
}

OpResult LeaseRecipeCore::sendGrant_(int64_t& sentNs) {
    const uint32_t no = campaignNo_ + 1;
    buildKey_(no);
    LeaseOptions o;
    o.holder = host_->holderId();
    sentNs   = host_->client().nowNs();
    const std::span<const uint8_t> key(key_, keyLen_);
    const Token tok = LeaseRecipeHostCore::tokenOf(hostIdx_, LeaseRecipeHostCore::kTagGrant, no);
    OpResult r;
    if (rowLen_ == 0) {
        r = host_->topic().grantKey(key, o, CompletionTarget(host_->queue()), tok);
    } else {
        if (!host_->topic().patchKey(std::span<uint8_t>(row_.data(), rowLen_), key)) return OpResult::InvalidArgument;
        r = host_->topic().writeRaw(OpKind::LeaseGrant, key, std::span<const uint8_t>(row_.data(), rowLen_), 0, o,
                                    CompletionTarget(host_->queue()), tok);
    }
    if (r == OpResult::Accepted) campaignNo_ = no;   // every grant sent is a new campaign number
    return r;
}

OpResult LeaseRecipeCore::claim(Mode mode, std::span<const uint8_t> row, CompletionTarget target, Token token) {
    if (row.size() > row_.size()) return OpResult::InvalidArgument;
    if (!row.empty()) std::memmove(row_.data(), row.data(), row.size());
    return claimStored(mode, row.size(), target, token);
}

OpResult LeaseRecipeCore::claimStored(Mode mode, std::size_t rowLen, CompletionTarget target, Token token) {
    if (!valid_ || !std::holds_alternative<Idle>(st_)) return OpResult::InvalidArgument;
    if (row_.empty() ? rowLen != 0 : (rowLen < kMinLeaseRow || rowLen > row_.size())) return OpResult::InvalidArgument;
    Pending p;
    p.active  = true;
    p.token   = token;
    p.handler = target.handler;
    if (target.cq != nullptr) {
        // The completion is written from this thread: the queue's ring has ONE producer, which in Thread mode is the client's duty thread.
        if (!target.cq->valid() || !host_->client().invokerMode()) return OpResult::InvalidArgument;
        p.cq       = target.cq;
        p.reserved = target.cq->reservationFor(OpKind::LeaseCampaign);
        if (const OpResult r = target.cq->reserve(p.reserved); r != OpResult::Accepted) return r;
    } else if (target.handler.fn == nullptr) {
        return OpResult::InvalidArgument;
    }
    rowLen_  = rowLen;
    mode_    = mode;
    regrants_ = 0;
    int64_t sentNs = 0;
    const OpResult r = sendGrant_(sentNs);
    if (r != OpResult::Accepted) {
        if (p.cq != nullptr) p.cq->unreserve(p.reserved);
        return r;
    }
    pend_ = p;
    st_   = Granting{campaignNo_, sentNs};
    return OpResult::Accepted;
}

void LeaseRecipeCore::complete_(OpStatus st, uint16_t err, int64_t value) {
    if (!pend_.active) return;
    const Pending p = pend_;
    pend_ = Pending{};
    Completion c;
    c.token         = p.token;
    c.value         = value;
    c.op            = OpKind::LeaseCampaign;
    c.status        = st;
    c.errorCode     = err;
    c.reservedBytes = p.reserved;
    if (p.cq != nullptr) (void)p.cq->ring().deliver(c, {});
    else p.handler(c, {});
}

void LeaseRecipeCore::bestEffortRevoke_(int64_t version) {
    (void)host_->topic().revokeKey(std::span<const uint8_t>(key_, keyLen_), version, LeaseOptions{}, CompletionTarget(host_->queue()),
                                   LeaseRecipeHostCore::tokenOf(hostIdx_, LeaseRecipeHostCore::kTagRevoke, campaignNo_));
}

void LeaseRecipeCore::onCompletion(uint32_t tag, uint32_t no, const Completion& c, std::span<const uint8_t> payload) {
    if (tag == LeaseRecipeHostCore::kTagGrant) {
        if (const auto* g = std::get_if<Granting>(&st_); g != nullptr && g->campaignNo == no) {
            const int64_t sentNs = g->sentNs;
            if (c.status == OpStatus::Ok) { join_(c.value, static_cast<uint64_t>(c.aux), sentNs); return; }
            if (unknownOutcome(c.status)) { startRead_(c.status, c.errorCode, sentNs); return; }
            st_ = Idle{};
            complete_(c.status, c.errorCode, -1);
            return;
        }
        if (const auto* a = std::get_if<Abandoned>(&st_); a != nullptr && a->campaignNo == no) {
            st_ = Idle{};
            if (c.status == OpStatus::Ok) bestEffortRevoke_(c.value);   // granted after the caller let go: give it back
        }
        return;
    }
    if (tag != LeaseRecipeHostCore::kTagRead) return;   // a best-effort revoke's answer: nothing to do
    const LeaseTopicCore& topic = host_->topic();
    // The lease view row of our key: live with lease_holder == our holder id -> ours.
    bool     ours = false;
    int64_t  version = 0;
    uint64_t ttl = 0;
    if (c.status == OpStatus::Ok && payload.size() >= topic.viewRowLen() && topic.viewRowLen() != 0 && (payload[0] & 1u) == 0) {
        const uint32_t vo = topic.viewVersionOffset();
        version = load64(payload.data() + vo);
        ttl     = static_cast<uint64_t>(load64(payload.data() + vo + 8));
        ours    = static_cast<uint64_t>(load64(payload.data() + vo + 16)) == host_->holderId() && version >= 0;
    }
    if (const auto* r = std::get_if<Reading>(&st_); r != nullptr && r->campaignNo == no) {
        const Reading rd = *r;
        if (ours) join_(version, ttl, rd.sentNs);
        else regrant_(rd.unknown, rd.unknownCode);
        return;
    }
    if (const auto* a = std::get_if<Abandoned>(&st_); a != nullptr && a->campaignNo == no) {
        st_ = Idle{};
        if (ours) bestEffortRevoke_(version);
    }
}

void LeaseRecipeCore::startRead_(OpStatus unknown, uint16_t code, int64_t sentNs) {
    st_ = Reading{campaignNo_, sentNs, unknown, code};
    const OpResult r = host_->topic().get(std::span<const uint8_t>(key_, keyLen_), -1, CompletionTarget(host_->queue()),
                                          LeaseRecipeHostCore::tokenOf(hostIdx_, LeaseRecipeHostCore::kTagRead, campaignNo_));
    if (r != OpResult::Accepted) regrant_(unknown, code);
}

void LeaseRecipeCore::regrant_(OpStatus unknown, uint16_t code) {
    if (++regrants_ > kMaxRegrants) {
        st_ = Idle{};
        complete_(unknown, code, -1);
        return;
    }
    int64_t sentNs = 0;
    if (sendGrant_(sentNs) != OpResult::Accepted) {
        st_ = Idle{};
        complete_(OpStatus::DeliveryFailed, 0, -1);
        return;
    }
    st_ = Granting{campaignNo_, sentNs};
}

void LeaseRecipeCore::join_(int64_t version, uint64_t ttlMs, int64_t sentNs) {
    OpResult err = OpResult::Accepted;
    const LeaseRef ref = host_->session().add(host_->topic(), std::span<const uint8_t>(key_, keyLen_), version, ttlMs, sentNs, &err);
    if (ref.slot == 0xFFFFFFFFu) {   // the session cannot keep it alive: give it back
        st_ = Idle{};
        bestEffortRevoke_(version);
        complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseLost), -1);
        return;
    }
    host_->mapRef(ref, hostIdx_);
    st_ = Joining{version, ref};
    settle_();
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Evaluation (etcd waitDeletes)
// ---------------------------------------------------------------------------------------------------------------------------------

void LeaseRecipeCore::settle_() {
    const auto* j = std::get_if<Joining>(&st_);
    if (j == nullptr || watch_ == nullptr) return;
    if (!watch_->isReady() || !watch_->isLive(std::span<const uint8_t>(key_, keyLen_), j->version)) return;
    evaluate_(j->version, j->ref);
}

void LeaseRecipeCore::evaluate_(int64_t version, LeaseRef ref) {
    ++evaluations_;
    const auto low = watch_->lowest(nameId_);
    if (low && low->first == version) { becomeOwner_(version, ref); return; }
    if (pend_.active && mode_ == Mode::TryClaim) {   // not the lowest: give the key back and report the name as claimed
        (void)host_->session().revoke(ref);
        st_ = Releasing{ref};
        host_->noteReleasing(+1);
        complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseClaimed), -1);
        return;
    }
    const auto pred = watch_->predecessor(nameId_, version);
    if (!pred) { st_ = Joining{version, ref}; return; }   // the lowest left between events: evaluate again on the next one
    Waiting w;
    w.version     = version;
    w.ref         = ref;
    w.predVersion = pred->first;
    w.predKeyLen  = static_cast<uint8_t>(pred->second.size());
    std::memcpy(w.predKey, pred->second.data(), pred->second.size());
    st_ = w;
}

void LeaseRecipeCore::becomeOwner_(int64_t version, LeaseRef ref) {
    st_ = Owner{version, ref};
    complete_(OpStatus::Ok, 0, version);
}

void LeaseRecipeCore::onWatchEvent(void* ctx, const LeaseEvent& ev) {
    auto* rc = static_cast<LeaseRecipeCore*>(ctx);
    if (ev.kind == LeaseEventKind::Failed) return;
    if (ev.kind == LeaseEventKind::Ready) {
        rc->observeCheck_(&ev);
        rc->settle_();
        return;
    }
    rc->onKeyEvent_(ev);
}

void LeaseRecipeCore::onKeyEvent_(const LeaseEvent& ev) {
    observeCheck_(&ev);
    if (std::holds_alternative<Joining>(st_)) { settle_(); return; }
    const auto* w = std::get_if<Waiting>(&st_);
    // Only the END of exactly the predecessor re-evaluates: every other event under the name changes nothing for a waiter.
    if (w == nullptr || !isEnd(ev.kind) || ev.version != w->predVersion || ev.key.size() != w->predKeyLen ||
        std::memcmp(ev.key.data(), w->predKey, w->predKeyLen) != 0)
        return;
    const int64_t  version = w->version;
    const LeaseRef ref     = w->ref;
    evaluate_(version, ref);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Loss, at-risk, release
// ---------------------------------------------------------------------------------------------------------------------------------

void LeaseRecipeCore::onLost(LostReason reason) {
    LeaseRef ref;
    int64_t  version = 0;
    if (refOf_(ref, version)) {
        const bool held = std::holds_alternative<Owner>(st_) || std::holds_alternative<Suspended>(st_);
        host_->unmapRef(ref);
        st_ = Idle{};
        complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseLost), -1);
        if (held && cb_.lost != nullptr) cb_.lost(cb_.ctx, reason);
        return;
    }
    if (const auto* r = std::get_if<Releasing>(&st_)) {   // the revoke's end, not a loss
        host_->unmapRef(r->ref);
        host_->noteReleasing(-1);
        st_ = Idle{};
    }
}

void LeaseRecipeCore::onAtRisk(AtRiskEdge edge) {
    if (edge == AtRiskEdge::Entered) {
        LeaseRef ref;
        int64_t  version = 0;
        if (!refOf_(ref, version) || std::holds_alternative<Suspended>(st_)) return;
        st_ = Suspended{version, ref, std::holds_alternative<Owner>(st_)};
        if (cb_.atRisk != nullptr) cb_.atRisk(cb_.ctx, AtRiskEdge::Entered);
        return;
    }
    const auto* s = std::get_if<Suspended>(&st_);
    if (s == nullptr) return;
    st_ = Joining{s->version, s->ref};   // re-evaluate: an owner again only if its key is still the lowest
    settle_();
    if (cb_.atRisk != nullptr) cb_.atRisk(cb_.ctx, AtRiskEdge::Cleared);
}

OpResult LeaseRecipeCore::release() {
    if (host_ == nullptr || hostIdx_ < 0) return OpResult::InvalidArgument;
    if (std::holds_alternative<Granting>(st_) || std::holds_alternative<Reading>(st_)) {
        st_ = Abandoned{campaignNo_};   // the answer of the grant / read in flight is cleaned up when it arrives
        complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseReleased), -1);
        return OpResult::Accepted;
    }
    LeaseRef ref;
    int64_t  version = 0;
    if (!refOf_(ref, version)) return OpResult::InvalidArgument;   // idle, already releasing or abandoned
    const OpResult r = host_->session().revoke(ref);
    if (r != OpResult::Accepted) {   // the session no longer knows the lease
        host_->unmapRef(ref);
        st_ = Idle{};
        complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseReleased), -1);
        return r;
    }
    st_ = Releasing{ref};
    host_->noteReleasing(+1);
    complete_(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseReleased), -1);
    return OpResult::Accepted;
}

void LeaseRecipeCore::pollReleasing() {
    const auto* r = std::get_if<Releasing>(&st_);
    if (r == nullptr || host_->session().stateOf(r->ref) != LeaseEntryState::Free) return;
    host_->unmapRef(r->ref);
    host_->noteReleasing(-1);
    st_ = Idle{};
}

OpResult LeaseRecipeCore::proclaim(std::span<const uint8_t> row, CompletionTarget target, Token token) {
    if (row.size() > row_.size()) return OpResult::InvalidArgument;
    if (!row.empty()) std::memmove(row_.data(), row.data(), row.size());
    return proclaimStored(row.size(), target, token);
}

OpResult LeaseRecipeCore::proclaimStored(std::size_t rowLen, CompletionTarget target, Token token) {
    const auto* o = std::get_if<Owner>(&st_);
    if (!valid_ || o == nullptr || row_.empty() || rowLen < kMinLeaseRow || rowLen > row_.size()) return OpResult::InvalidArgument;
    if (!host_->topic().patchKey(std::span<uint8_t>(row_.data(), rowLen), std::span<const uint8_t>(key_, keyLen_))) return OpResult::InvalidArgument;
    return host_->session().update(o->ref, std::span<const uint8_t>(row_.data(), rowLen), target, token);
}

// ---------------------------------------------------------------------------------------------------------------------------------
// Observe
// ---------------------------------------------------------------------------------------------------------------------------------

bool LeaseRecipeCore::setObserver(ObserveFn fn, void* ctx) {
    if (!valid_ || fn == nullptr || observer_ != nullptr) return false;
    observer_    = fn;
    observerCtx_ = ctx;
    lastReported_ = kNoReport;
    if (watch_ != nullptr && watch_->isReady()) observeCheck_(nullptr);   // the current leader first (etcd's Observe)
    return true;
}

void LeaseRecipeCore::observeCheck_(const LeaseEvent* ev) {
    if (observer_ == nullptr || watch_ == nullptr) return;
    const auto low = watch_->lowest(nameId_);
    if (!low) { lastReported_ = kNoReport; return; }   // no leader: nothing to report until the next one
    if (low->first == lastReported_) return;
    lastReported_ = low->first;
    std::span<const uint8_t> bytes;
    if (ev != nullptr && (ev->kind == LeaseEventKind::Granted || ev->kind == LeaseEventKind::Updated) && ev->version == low->first)
        bytes = ev->bytes;
    observer_(observerCtx_, low->first, low->second, bytes);
}

}  // namespace rune::client::detail
