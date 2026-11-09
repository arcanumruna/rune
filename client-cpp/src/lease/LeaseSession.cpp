// src/lease/LeaseSession.cpp - the non-template body of LeaseSession (Task 15).  See lease/LeaseSession.h.
#include <rune/client/lease/LeaseSession.h>

#include <rune/client/detail/HubCommand.h>
#include <rune/client/detail/OpTarget.h>
#include <rune/client/lease/detail/LeaseGuards.h>

#include <algorithm>
#include <climits>
#include <cstring>

namespace rune::client::detail {
namespace {

constexpr int64_t kNsPerMs = 1'000'000;
constexpr uint64_t kKindKeepAlive = 1, kKindBestEffort = 2, kKindClose = 3;

[[nodiscard]] int64_t satAdd(int64_t a, int64_t b) noexcept {   // b >= 0
    return a > INT64_MAX - b ? INT64_MAX : a + b;
}
[[nodiscard]] uint64_t kaToken(uint32_t req, uint32_t gen) noexcept { return (kKindKeepAlive << 44) | (uint64_t{req} << 32) | gen; }
[[nodiscard]] uint64_t revokeToken(uint64_t kind, uint32_t slot, uint32_t gen) noexcept {
    return (kind << 44) | (uint64_t{slot & 0xFFFu} << 32) | gen;
}

}  // namespace

CompletionsConfig LeaseSessionCore::queueConfig_(uint32_t capacity) {
    CompletionsConfig c;
    c.maxResultBytes = 256;
    c.maxErrorText   = 192;
    // Decision C: every record the queue can hold at once is a 48-byte credit: the guard event pool (2*Capacity + 8), one credit per
    // in-flight keepalive entry (at most Capacity) and per request terminal (kMaxRequests), the best-effort and close revokes (at most
    // Capacity each).  4*Capacity + 2*kMaxRequests + 64 units covers all of it; plus one maximal record of slack.
    const uint64_t units = 4ull * capacity + 2ull * kMaxRequests + 64ull;
    const uint64_t need  = units * 48ull + 4096ull;
    uint32_t cap = 4096;
    while (cap < need) cap <<= 1;
    c.capacityBytes = cap;
    return c;
}

LeaseSessionCore::LeaseSessionCore(Client& client, Entry* entries, uint32_t* cand, uint32_t capacity, Callbacks cb,
                                   const LeaseSessionOptions& opts)
    : client_(client)
    , entries_(entries)
    , cand_(cand)
    , capacity_(capacity)
    , cb_(cb)
    , dNs_(static_cast<int64_t>(opts.maxDelayMs) * kNsPerMs)
    , num_(opts.atRiskNum)
    , den_(opts.atRiskDen)
    , cq_(client, queueConfig_(capacity)) {
    if (num_ == 0 || num_ >= den_) throw std::invalid_argument("LeaseSession: need 0 < atRiskNum < atRiskDen");
    nextWorkNs_ = INT64_MAX;
    sid_        = cq_.cqRef();
    pool_       = 2u * capacity + kPoolExtra;
    for (uint32_t i = 0; i < pool_; ++i) {
        if (cq_.reserve(48u) != OpResult::Accepted) break;
        ++poolHeld_;
    }
    if (poolHeld_ != pool_ || sid_ == kNoCqRef) {
        refused_ = true;
        releasePool_();
        return;
    }
    HubCommand cmd;
    cmd.op     = HubCommand::Op::GuardReserve;
    cmd.handle = sid_;
    cmd.a      = static_cast<int64_t>(uint64_t{capacity} | (uint64_t{pool_} << 32));
    OpTarget t;
    t.reservedBytes = 48u;
    t.cqRef         = cq_.cqRef();
    encodeTarget(t, cmd);
    if (!client_.submitGuardCommand(cmd)) {
        refused_ = true;
        releasePool_();
    }
}

LeaseSessionCore::~LeaseSessionCore() {
    if (sid_ != kNoCqRef && !refused_) {
        HubCommand rel;
        rel.op     = HubCommand::Op::GuardRelease;
        rel.handle = sid_;
        for (int i = 0; i < 4096; ++i)
            if (client_.submitGuardCommand(rel)) break;   // bounded: a full ring drains, a closed client never accepts
    }
    releasePool_();
}

void LeaseSessionCore::releasePool_() noexcept {
    for (; poolHeld_ != 0; --poolHeld_) cq_.unreserve(48u);
}

LeaseSessionCore::Timing* LeaseSessionCore::timing_(Entry& e) noexcept {
    if (auto* h = std::get_if<Held>(&e.st)) return &h->t;
    if (auto* a = std::get_if<AtRiskS>(&e.st)) return &a->t;
    return nullptr;
}
const LeaseSessionCore::Timing* LeaseSessionCore::timing_(const Entry& e) noexcept {
    if (const auto* h = std::get_if<Held>(&e.st)) return &h->t;
    if (const auto* a = std::get_if<AtRiskS>(&e.st)) return &a->t;
    return nullptr;
}

LeaseSessionCore::Entry* LeaseSessionCore::resolve_(LeaseRef ref) noexcept {
    if (ref.slot >= capacity_) return nullptr;
    Entry& e = entries_[ref.slot];
    if (e.gen != ref.gen || std::holds_alternative<Free>(e.st)) return nullptr;
    return &e;
}

LeaseEntryState LeaseSessionCore::stateOf(LeaseRef ref) const noexcept {
    if (ref.slot >= capacity_) return LeaseEntryState::Free;
    const Entry& e = entries_[ref.slot];
    if (e.gen != ref.gen) return LeaseEntryState::Free;
    if (std::holds_alternative<Held>(e.st)) return LeaseEntryState::Held;
    if (std::holds_alternative<AtRiskS>(e.st)) return LeaseEntryState::AtRisk;
    if (std::holds_alternative<Losing>(e.st)) return LeaseEntryState::Losing;
    if (std::holds_alternative<Revoking>(e.st)) return LeaseEntryState::Revoking;
    return LeaseEntryState::Free;
}

int64_t LeaseSessionCore::margin_(const Entry& e, int64_t sentNs) const noexcept {
    return satAdd(sentNs, e.ttlNs - e.ttlNs / 50) - dNs_;
}

int64_t LeaseSessionCore::atRiskAt_(const Entry& e, int64_t sentNs) const noexcept {
    const auto part = static_cast<int64_t>(static_cast<__int128>(e.ttlNs) * num_ / den_);
    return std::min(satAdd(sentNs, part), margin_(e, sentNs) - dNs_);
}

bool LeaseSessionCore::postGuard_(HubCommand::Op op, uint32_t slot, uint64_t gen, int64_t b, int64_t c) {
    HubCommand cmd;
    cmd.op     = op;
    cmd.handle = sid_;
    cmd.a      = static_cast<int64_t>(uint64_t{slot} | (gen << 32));
    cmd.b      = b;
    cmd.c      = c;
    if (!client_.submitGuardCommand(cmd)) return false;
    posted_ = true;
    return true;
}

bool LeaseSessionCore::postArm_(uint32_t slot, Entry& e, int64_t atRiskNs) {
    return postGuard_(HubCommand::Op::GuardArm, slot, e.gen, static_cast<int64_t>(++e.armSeq), atRiskNs);
}

void LeaseSessionCore::postEnd_(uint32_t slot, Entry& e) {
    if (!postGuard_(HubCommand::Op::GuardEnd, slot, e.gen, 0, 0)) setPending_(e, 2);
}

void LeaseSessionCore::freeEntry_(uint32_t slot) {
    Entry& e = entries_[slot];
    if (e.group >= 0 && groups_[static_cast<std::size_t>(e.group)].refs != 0) --groups_[static_cast<std::size_t>(e.group)].refs;
    e.group      = -1;
    e.st         = Free{};
    e.topic      = nullptr;
    e.nProducers = 0;
    e.cmdPending = 0;
    if (++e.gen == 0) e.gen = 1;
    --live_;
}

LeaseRef LeaseSessionCore::add(LeaseTopicCore& topic, std::span<const uint8_t> key, int64_t version, uint64_t ttlMs, int64_t grantSentNs,
                               OpResult* err) {
    auto fail = [&](OpResult r) {
        if (err != nullptr) *err = r;
        return LeaseRef{};
    };
    if (closed_ || refused_ || topic.closed() || &topic.client() != &client_) return fail(OpResult::InvalidArgument);
    if (key.empty() || key.size() > rune::protocol::kMaxLeaseKeyBytes || version < 0) return fail(OpResult::InvalidArgument);
    if (ttlMs == 0 || ttlMs > topic.spec().maxTtlMs || ttlMs > static_cast<uint64_t>(INT64_MAX / kNsPerMs)) return fail(OpResult::InvalidArgument);
    const auto ttlNs = static_cast<int64_t>(ttlMs) * kNsPerMs;
    if (ttlNs / 2 + dNs_ >= ttlNs - ttlNs / 50) return fail(OpResult::InvalidArgument);
    uint32_t slot = capacity_;
    for (uint32_t i = 0; i < capacity_; ++i)
        if (std::holds_alternative<Free>(entries_[i].st)) { slot = i; break; }
    if (slot == capacity_) return fail(OpResult::InvalidArgument);
    const int32_t partition = topic.partitionOf(key);
    int32_t group = -1, freeGroup = -1;
    for (uint32_t g = 0; g < kMaxGroups; ++g) {
        if (groups_[g].refs == 0) { if (freeGroup < 0) freeGroup = static_cast<int32_t>(g); continue; }
        if (groups_[g].topicId == topic.topicId() && groups_[g].partition == partition) { group = static_cast<int32_t>(g); break; }
    }
    if (group < 0) {
        if (freeGroup < 0) return fail(OpResult::InvalidArgument);
        group = freeGroup;
        groups_[static_cast<std::size_t>(group)] = Group{topic.topicId(), partition, 0};
    }
    Entry& e = entries_[slot];
    e.topic     = &topic;
    e.partition = partition;
    e.group     = group;
    e.keyLen    = static_cast<uint8_t>(key.size());
    std::memcpy(e.key, key.data(), key.size());
    e.version    = version;
    e.ttlNs      = ttlNs;
    e.armSeq     = 0;
    e.nProducers = 0;
    e.cmdPending = 0;
    Timing t;
    t.lastOkSentNs = grantSentNs;
    t.marginNs     = margin_(e, grantSentNs);
    t.atRiskNs     = atRiskAt_(e, grantSentNs);
    t.phaseNs      = satAdd(grantSentNs, ttlNs / 3);
    t.nextSendNs   = t.phaseNs;
    t.inFlightReq  = -1;
    if (!postArm_(slot, e, t.atRiskNs)) {   // not guarded: not accepted
        e.group = -1;
        e.topic = nullptr;
        return fail(OpResult::BackPressured);
    }
    ++groups_[static_cast<std::size_t>(group)].refs;
    e.st = Held{t};
    ++live_;
    noteWork_(std::min(t.nextSendNs, t.marginNs));
    return LeaseRef{slot, e.gen};
}

OpResult LeaseSessionCore::remove(LeaseRef ref) {
    Entry* e = resolve_(ref);
    if (e == nullptr) return OpResult::InvalidArgument;
    if (!postGuard_(HubCommand::Op::GuardEnd, ref.slot, e->gen, 0, 0)) return OpResult::BackPressured;
    freeEntry_(ref.slot);
    return OpResult::Accepted;
}

bool LeaseSessionCore::sendRevoke_(uint32_t slot, bool closeKind) {
    Entry& e = entries_[slot];
    const OpResult r = e.topic->revokeKey(std::span<const uint8_t>(e.key, e.keyLen), e.version, LeaseOptions{}, CompletionTarget(cq_),
                                          revokeToken(closeKind ? kKindClose : kKindBestEffort, slot, e.gen));
    if (r == OpResult::Accepted) posted_ = true;
    return r == OpResult::Accepted;
}

OpResult LeaseSessionCore::revoke(LeaseRef ref) {
    Entry* e = resolve_(ref);
    if (e == nullptr) return OpResult::InvalidArgument;
    if (auto* l = std::get_if<Losing>(&e->st)) {   // already ending: the revoke follows its onLost
        l->closeAfter = true;
        return OpResult::Accepted;
    }
    if (std::holds_alternative<Revoking>(e->st)) return OpResult::Accepted;
    e->st = Revoking{false};
    postEnd_(ref.slot, *e);
    if (sendRevoke_(ref.slot, true)) std::get<Revoking>(e->st).sent = true;
    else setPending_(*e, 4);
    return OpResult::Accepted;
}

OpResult LeaseSessionCore::update(LeaseRef ref, std::span<const uint8_t> row, CompletionTarget target, Token token) {
    Entry* e = resolve_(ref);
    if (e == nullptr || timing_(*e) == nullptr) return OpResult::InvalidArgument;
    return e->topic->writeRaw(OpKind::LeaseUpdate, std::span<const uint8_t>(e->key, e->keyLen), row, e->version, LeaseOptions{}, target, token);
}

OpResult LeaseSessionCore::bindProducer(LeaseRef ref, Producer& producer) {
    Entry* e = resolve_(ref);
    if (e == nullptr || timing_(*e) == nullptr || refused_) return OpResult::InvalidArgument;
    if (!producer.ownedBy(client_) || e->nProducers >= kMaxGuardProducers) return OpResult::InvalidArgument;
    if (!producer.bindGuard(sid_, ref.slot, e->gen)) return OpResult::InvalidArgument;
    ++e->nProducers;
    return OpResult::Accepted;
}

void LeaseSessionCore::close() {
    closed_ = true;
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        Entry& e = entries_[slot];
        if (auto* l = std::get_if<Losing>(&e.st)) { l->closeAfter = true; continue; }
        if (timing_(e) == nullptr) continue;
        e.st = Revoking{false};
        postEnd_(slot, e);
        if (sendRevoke_(slot, true)) std::get<Revoking>(e.st).sent = true;
        else setPending_(e, 4);
    }
}

void LeaseSessionCore::toLosing_(uint32_t slot, LostReason why) {
    Entry& e = entries_[slot];
    e.st = Losing{why, false};
    postEnd_(slot, e);
}

void LeaseSessionCore::onWatchedEnd(int32_t topicId, int32_t partition, std::span<const uint8_t> key, int64_t version, LostReason reason) {
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        Entry& e = entries_[slot];
        if (timing_(e) == nullptr || e.topic->topicId() != topicId || e.partition != partition || e.version != version) continue;
        if (e.keyLen != key.size() || std::memcmp(e.key, key.data(), key.size()) != 0) continue;
        toLosing_(slot, reason);
    }
}

namespace {
void watchThunk(void* ctx, const LeaseEvent& ev) {
    LostReason why;
    switch (ev.kind) {
    case LeaseEventKind::Revoked:  why = LostReason::WatchedRevoked; break;
    case LeaseEventKind::Expired:  why = LostReason::WatchedExpired; break;
    case LeaseEventKind::Vanished: why = LostReason::WatchedVanished; break;
    default: return;
    }
    static_cast<LeaseSessionCore*>(ctx)->onWatchedEnd(ev.topicId, ev.partition, ev.key, ev.version, why);   // filters by topic id, key and version
}
}  // namespace

bool LeaseSessionCore::observe(LeaseWatchBase& watch) { return watch.addObserver(&watchThunk, this); }
bool LeaseSessionCore::unobserve(LeaseWatchBase& watch) noexcept { return watch.removeObserver(this); }

// ---------------------------------------------------------------------------------------------------------------------------------
// The work pass
// ---------------------------------------------------------------------------------------------------------------------------------

int LeaseSessionCore::doWork(int64_t nowNs) {
    int work = drain_(nowNs);
    if (anyPending_) work += retryPending_();
    if (nowNs >= nextWorkNs_) work += timeWork_(nowNs);
    for (int i = 0; posted_ && i < 4; ++i) {   // a command we just posted may already have its answer queued (Invoker mode)
        posted_ = false;
        work += drain_(nowNs);
    }
    posted_ = false;
    if (consumed_ != acked_ && !refused_) {
        HubCommand ack;
        ack.op     = HubCommand::Op::GuardAck;
        ack.handle = sid_;
        ack.a      = static_cast<int64_t>(consumed_);
        if (client_.submitGuardCommand(ack)) acked_ = consumed_;
    }
    return work;
}

int LeaseSessionCore::drain_(int64_t nowNs) {
    return cq_.poll([&](const Completion& c, std::span<const uint8_t>) { onCompletion_(c, nowNs); });
}

void LeaseSessionCore::onCompletion_(const Completion& c, int64_t nowNs) {
    switch (c.op) {
    case OpKind::LeaseGuard:     ++consumed_; onGuardEvent_(c, nowNs); break;
    case OpKind::LeaseKeepAlive: onKeepAlive_(c, nowNs); break;
    case OpKind::LeaseRevoke:    onRevoked_(c); break;
    default: break;
    }
}

void LeaseSessionCore::onGuardEvent_(const Completion& c, int64_t nowNs) {
    const auto ev   = static_cast<GuardEvent>(c.reserved0 & 0xFFu);
    const auto slot = static_cast<uint32_t>(c.value);
    const auto gen  = static_cast<uint32_t>(c.aux);
    switch (ev) {
    case GuardEvent::Reserved: guardReady_ = true; return;
    case GuardEvent::ReserveFailed:
        refused_ = true;
        for (uint32_t s = 0; s < capacity_; ++s) {
            Entry& e = entries_[s];
            if (std::holds_alternative<Free>(e.st)) continue;
            const bool revoking = std::holds_alternative<Revoking>(e.st);
            const LeaseRef ref{s, e.gen};
            const uint32_t g = e.gen;
            if (!revoking) cb_.lost(cb_.ctx, ref, LostReason::SessionRefused);
            if (entries_[s].gen == g && !std::holds_alternative<Free>(entries_[s].st)) freeEntry_(s);
        }
        return;
    case GuardEvent::BindRefused: return;   // the producer was killed (blocked for good); nothing for the session to undo
    default: break;
    }
    if (slot >= capacity_ || entries_[slot].gen != gen) return;   // an event of a lease that left the session
    Entry& e = entries_[slot];
    switch (ev) {
    case GuardEvent::Tripped:
        if (auto* h = std::get_if<Held>(&e.st)) {
            e.st = AtRiskS{h->t, nowNs};
            cb_.atRisk(cb_.ctx, LeaseRef{slot, gen}, AtRiskEdge::Entered);
        }
        return;
    case GuardEvent::Rearmed:
        if (auto* a = std::get_if<AtRiskS>(&e.st)) {
            e.st = Held{a->t};
            cb_.atRisk(cb_.ctx, LeaseRef{slot, gen}, AtRiskEdge::Cleared);
        }
        return;
    case GuardEvent::Ended: onEnded_(slot); return;
    default: return;
    }
}

void LeaseSessionCore::onEnded_(uint32_t slot) {
    Entry& e = entries_[slot];
    const auto* l = std::get_if<Losing>(&e.st);
    if (l == nullptr) return;
    const LostReason why = l->reason;
    const bool closeAfter = l->closeAfter;
    LeaseTopicCore* topic = e.topic;
    uint8_t key[rune::protocol::kMaxLeaseKeyBytes];
    const uint8_t keyLen = e.keyLen;
    std::memcpy(key, e.key, keyLen);
    const int64_t version = e.version;
    const uint32_t gen = e.gen;
    cb_.lost(cb_.ctx, LeaseRef{slot, gen}, why);   // the producers are already failed and blocked: the guard ended first
    Entry& after = entries_[slot];
    if (after.gen != gen || !std::holds_alternative<Losing>(after.st)) return;   // the callback removed it: no revoke
    if (closeAfter || std::get<Losing>(after.st).closeAfter) {
        after.st = Revoking{false};
        if (sendRevoke_(slot, true)) std::get<Revoking>(after.st).sent = true;
        else setPending_(after, 4);
        return;
    }
    freeEntry_(slot);
    const bool mayBeLive = why != LostReason::NotFound && why != LostReason::TopicDropped && why != LostReason::SessionRefused;
    if (mayBeLive && client_.state() == session::SessionState::Connected) bestEffortRevoke_(topic, key, keyLen, version, slot, gen);
}

void LeaseSessionCore::bestEffortRevoke_(LeaseTopicCore* topic, const uint8_t* key, uint8_t keyLen, int64_t version, uint32_t slot,
                                         uint32_t gen) {
    // Its completion is ignored (kind 2).  A full queue or ring just skips it: the lease expires on its own.
    if (topic->revokeKey(std::span<const uint8_t>(key, keyLen), version, LeaseOptions{}, CompletionTarget(cq_),
                         revokeToken(kKindBestEffort, slot, gen)) == OpResult::Accepted)
        posted_ = true;
}

void LeaseSessionCore::onRevoked_(const Completion& c) {
    if ((c.token >> 44) != kKindClose) return;
    const auto slot = static_cast<uint32_t>((c.token >> 32) & 0xFFFu);
    const auto gen  = static_cast<uint32_t>(c.token);
    if (slot >= capacity_) return;
    Entry& e = entries_[slot];
    if (e.gen != gen || !std::holds_alternative<Revoking>(e.st)) return;
    freeEntry_(slot);
}

void LeaseSessionCore::onKeepAlive_(const Completion& c, int64_t nowNs) {
    if ((c.token >> 44) != kKindKeepAlive) return;
    const auto r   = static_cast<uint32_t>((c.token >> 32) & 0xFFu);
    const auto gen = static_cast<uint32_t>(c.token);
    if (r >= kMaxRequests || !requests_[r].used || requests_[r].gen != gen) return;   // a completion of a request that was freed
    Request& req = requests_[r];
    if (c.reserved0 == kLeaseKeepAliveTerminal) {
        for (uint16_t i = 0; i < req.count; ++i) {
            Entry& e = entries_[req.slot[i]];
            if (Timing* t = timing_(e); t != nullptr && t->inFlightReq == static_cast<int32_t>(r)) {   // missing reply
                t->inFlightReq = -1;
                noteWork_(t->nextSendNs);
            }
        }
        req.used = false;
        if (++req.gen == 0) req.gen = 1;
        return;
    }
    const uint32_t idx = c.reserved0;
    if (idx >= req.count) return;
    const uint32_t slot = req.slot[idx];
    Entry& e = entries_[slot];
    Timing* t = timing_(e);
    if (t == nullptr || t->inFlightReq != static_cast<int32_t>(r)) return;   // freed, reused or no longer kept alive
    t->inFlightReq = -1;
    noteWork_(t->nextSendNs);
    namespace rpc = rune::protocol::client;
    const auto code = static_cast<uint16_t>(c.status == OpStatus::Ok ? 0 : c.errorCode);
    if (c.status == OpStatus::Ok) {
        ++okAnswers_;
        if (req.sendNs > t->lastOkSentNs) {
            t->lastOkSentNs = req.sendNs;
            t->marginNs     = margin_(e, req.sendNs);
            t->atRiskNs     = atRiskAt_(e, req.sendNs);
            const bool held = std::holds_alternative<Held>(e.st);
            if (held || nowNs < t->atRiskNs) {   // an at-risk lease that is still past its new deadline stays at risk
                if (!postArm_(slot, e, t->atRiskNs)) setPending_(e, 1);
            }
        }
        return;
    }
    if (code == static_cast<uint16_t>(rpc::ErrorCode::LeaseNotFound)) { toLosing_(slot, LostReason::NotFound); return; }
    if (code == static_cast<uint16_t>(rpc::ErrorCode::UnknownTopic)) { toLosing_(slot, LostReason::TopicDropped); return; }
    // Any other code is retriable: soon again, within the entry's own cadence; a Throttled answer carries the retry-after (ms) in aux.
    int64_t wait = e.ttlNs / 12;
    if (code == static_cast<uint16_t>(rpc::ErrorCode::Throttled) && c.aux > 0) {
        const auto ms = static_cast<uint64_t>(c.aux);
        const int64_t ns = ms > static_cast<uint64_t>(INT64_MAX / kNsPerMs) ? INT64_MAX : static_cast<int64_t>(ms) * kNsPerMs;
        wait = std::max(wait, ns);
    }
    t->nextSendNs = std::min(t->phaseNs, satAdd(req.sendNs, wait));
    noteWork_(t->nextSendNs);
}

int LeaseSessionCore::retryPending_() {
    int work = 0;
    bool any = false;
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        Entry& e = entries_[slot];
        if (e.cmdPending == 0) continue;
        if ((e.cmdPending & 1u) != 0) {
            if (const Timing* t = timing_(e); t != nullptr && postArm_(slot, e, t->atRiskNs)) { e.cmdPending &= ~1u; ++work; }
            else if (t == nullptr) e.cmdPending &= ~1u;
        }
        if ((e.cmdPending & 2u) != 0) {
            if (postGuard_(HubCommand::Op::GuardEnd, slot, e.gen, 0, 0)) { e.cmdPending &= ~2u; ++work; }
        }
        if ((e.cmdPending & 4u) != 0) {
            if (auto* r = std::get_if<Revoking>(&e.st); r != nullptr && !r->sent && sendRevoke_(slot, true)) {
                r->sent = true;
                e.cmdPending &= ~4u;
                ++work;
            } else if (r == nullptr || r->sent) {
                e.cmdPending &= ~4u;
            }
        }
        any = any || e.cmdPending != 0;
    }
    anyPending_ = any;
    return work;
}

int LeaseSessionCore::timeWork_(int64_t nowNs) {
    int work = 0;
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        Entry& e = entries_[slot];
        const Timing* t = timing_(e);
        if (t != nullptr && nowNs >= t->marginNs) {
            toLosing_(slot, LostReason::MarginExpired);
            ++work;
        }
    }
    if (!closed_) work += sendDue_(nowNs);
    int64_t next = INT64_MAX;
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        const Timing* t = timing_(entries_[slot]);
        if (t == nullptr) continue;
        int64_t x = t->marginNs;
        if (t->inFlightReq < 0 && !closed_) x = std::min(x, t->nextSendNs);
        next = std::min(next, x);
    }
    nextWorkNs_ = next;
    return work;
}

int LeaseSessionCore::sendDue_(int64_t nowNs) {
    uint32_t n = 0;
    for (uint32_t slot = 0; slot < capacity_; ++slot) {
        Entry& e = entries_[slot];
        const Timing* t = timing_(e);
        if (t == nullptr || t->inFlightReq >= 0) continue;
        if (t->nextSendNs <= satAdd(nowNs, e.ttlNs / 12)) cand_[n++] = slot;
    }
    if (n == 0) return 0;
    std::sort(cand_, cand_ + n, [this](uint32_t a, uint32_t b) {
        const Entry& ea = entries_[a];
        const Entry& eb = entries_[b];
        if (ea.group != eb.group) return ea.group < eb.group;
        const int64_t ta = timing_(ea)->nextSendNs, tb = timing_(eb)->nextSendNs;
        return ta != tb ? ta < tb : a < b;
    });
    namespace rpc = rune::protocol::client;
    const std::size_t budget = client_.leaseKeepAliveBudgetBytes();
    constexpr std::size_t kFixed = rpc::MessageHeader::ENCODED_SIZE + rpc::LeaseKeepAlive::BODY_FIXED;
    int work = 0;
    uint32_t i = 0;
    while (i < n) {
        uint32_t j = i;
        const int32_t g = entries_[cand_[i]].group;
        bool due = false;
        while (j < n && entries_[cand_[j]].group == g) {
            due = due || timing_(entries_[cand_[j]])->nextSendNs <= nowNs;
            ++j;
        }
        if (due) {
            uint32_t k = i;
            while (k < j) {
                std::size_t bytes = kFixed;
                uint32_t m = 0;
                while (k + m < j && m < kMaxKeepAliveEntries) {
                    const std::size_t add = 1u + entries_[cand_[k + m]].keyLen + 8u;
                    if (bytes + add > budget) break;
                    bytes += add;
                    ++m;
                }
                if (m == 0) { ++k; continue; }   // a single entry the transport cannot carry: left alone
                if (!sendRequest_(g, cand_ + k, m, nowNs)) break;   // no free request: the rest waits for the next pass
                ++work;
                k += m;
            }
        }
        i = j;
    }
    return work;
}

bool LeaseSessionCore::sendRequest_(int32_t group, const uint32_t* slots, uint32_t n, int64_t nowNs) {
    uint32_t r = kMaxRequests;
    for (uint32_t x = 0; x < kMaxRequests; ++x)
        if (!requests_[x].used) { r = x; break; }
    if (r == kMaxRequests) return false;
    const Group& g = groups_[static_cast<std::size_t>(group)];
    int64_t minTtl = INT64_MAX;
    for (uint32_t k = 0; k < n; ++k) {
        const Entry& e = entries_[slots[k]];
        kaEntries_[k] = rune::protocol::client::LeaseKeepAlive::Entry{std::span<const uint8_t>(e.key, e.keyLen), e.version};
        minTtl = std::min(minTtl, e.ttlNs);
    }
    auto refuse = [&] {   // synchronous refusal: not in flight, again within ttl/12
        for (uint32_t k = 0; k < n; ++k) {
            Entry& e = entries_[slots[k]];
            Timing* t = timing_(e);
            t->nextSendNs = satAdd(nowNs, e.ttlNs / 12);
            noteWork_(t->nextSendNs);
        }
        return true;
    };
    uint32_t got = 0;
    for (; got < n + 1u; ++got)
        if (cq_.reserve(48u) != OpResult::Accepted) break;
    if (got != n + 1u) {
        for (uint32_t k = 0; k < got; ++k) cq_.unreserve(48u);
        return refuse();
    }
    Request& req = requests_[r];
    const auto timeoutMs = static_cast<uint32_t>(std::max<int64_t>(1, minTtl / 3 / kNsPerMs));
    const OpResult res = client_.leaseKeepAliveBatch(g.topicId, g.partition,
                                                     std::span<const rune::protocol::client::LeaseKeepAlive::Entry>(kaEntries_.data(), n),
                                                     timeoutMs, CompletionTarget(cq_), kaToken(r, req.gen));
    if (res != OpResult::Accepted) {
        for (uint32_t k = 0; k < got; ++k) cq_.unreserve(48u);
        return refuse();
    }
    req.used   = true;
    req.sendNs = nowNs;
    req.count  = static_cast<uint16_t>(n);
    ++requestsSent_;
    for (uint32_t k = 0; k < n; ++k) {
        req.slot[k] = static_cast<uint16_t>(slots[k]);
        Entry& e = entries_[slots[k]];
        Timing* t = timing_(e);
        // Phase-preserving: the entry's own cadence moves on by whole periods past this request (and past the early window that let it
        // ride along), it is never re-anchored to the send time - that would collapse spread entries into one burst.
        const int64_t period = e.ttlNs / 3;
        const int64_t horizon = satAdd(nowNs, e.ttlNs / 12);
        if (t->phaseNs <= horizon) t->phaseNs = satAdd(t->phaseNs, ((horizon - t->phaseNs) / period + 1) * period);
        t->nextSendNs  = t->phaseNs;
        t->inFlightReq = static_cast<int32_t>(r);
    }
    return true;
}

}  // namespace rune::client::detail
