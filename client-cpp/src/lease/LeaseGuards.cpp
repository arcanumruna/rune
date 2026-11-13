// src/lease/LeaseGuards.cpp - the duty-thread side of the lease produce guard (Task 15).  See LeaseGuards.h.
#include <rune/client/lease/detail/LeaseGuards.h>

#include <rune/client/producer/ProducerImpl.h>

#include <algorithm>
#include <new>

namespace rune::client::detail {

void LeaseGuards::configure(uint32_t maxSessions, CompletionSink sink) {
    blocks_.resize(maxSessions);   // cold, once
    sink_ = sink;
}

LeaseGuards::Block* LeaseGuards::find_(uint32_t sid) noexcept {
    for (Block& b : blocks_)
        if (b.used && b.sid == sid) return &b;
    return nullptr;
}
const LeaseGuards::Block* LeaseGuards::find_(uint32_t sid) const noexcept {
    for (const Block& b : blocks_)
        if (b.used && b.sid == sid) return &b;
    return nullptr;
}

void LeaseGuards::deliver_(Block& b, GuardEvent ev, uint32_t slot, uint32_t gen, uint32_t seq) noexcept {
    Completion c;
    c.token     = b.sid;
    c.value     = slot;
    c.aux       = gen;
    c.op        = OpKind::LeaseGuard;
    c.status    = OpStatus::Ok;
    c.reserved0 = guardEventWord(ev, seq);
    OpTarget t  = b.target;
    t.counted       = false;   // the block's pin is released by GuardRelease only
    t.producerOp    = false;
    t.reservedBytes = 48u;
    ++b.posted;
    if (sink_.fn != nullptr) sink_.fn(sink_.ctx, t, c, {});
}

void LeaseGuards::enlist_(Block& b, uint32_t slot) noexcept {
    GuardEntry& e = b.entries[slot];
    if (e.listed) return;
    e.listed = true;
    e.next   = -1;
    if (b.tail >= 0) b.entries[static_cast<uint32_t>(b.tail)].next = static_cast<int32_t>(slot);
    else b.head = static_cast<int32_t>(slot);
    b.tail = static_cast<int32_t>(slot);
}

void LeaseGuards::pushEdge_(Block& b, uint32_t slot, GuardEvent ev, uint32_t seq) noexcept {
    GuardEntry& e = b.entries[slot];
    if (e.nEdge == 2) {          // [a, b] + c (c has a's kind): the state after c is the state after a; keep the latest only
        e.edge[0]    = ev;
        e.edgeSeq[0] = seq;
        e.nEdge      = 1;
    } else {
        e.edge[e.nEdge]    = ev;
        e.edgeSeq[e.nEdge] = seq;
        ++e.nEdge;
    }
    enlist_(b, slot);
    flush_(b);
}

void LeaseGuards::flush_(Block& b) noexcept {
    while (b.head >= 0 && b.posted - b.acked < b.pool) {
        const uint32_t slot = static_cast<uint32_t>(b.head);
        GuardEntry&    e    = b.entries[slot];
        b.head = e.next;
        if (b.head < 0) b.tail = -1;
        e.listed = false;
        e.next   = -1;
        if (e.nEdge > 0) {
            const GuardEvent ev  = e.edge[0];
            const uint32_t   seq = e.edgeSeq[0];
            e.edge[0]    = e.edge[1];
            e.edgeSeq[0] = e.edgeSeq[1];
            --e.nEdge;
            deliver_(b, ev, slot, e.gen, seq);
        } else if (e.pendEnded) {
            e.pendEnded = false;
            deliver_(b, GuardEvent::Ended, slot, e.gen, e.armSeq);
        }
        if (e.nEdge > 0 || e.pendEnded) enlist_(b, slot);   // more for this slot: behind the other slots
    }
}

void LeaseGuards::reserve(uint32_t sid, uint32_t capacity, uint32_t pool, const OpTarget& target) {
    Block* free = nullptr;
    for (Block& b : blocks_) {
        if (b.used && b.sid == sid) { free = nullptr; break; }   // duplicate id: refuse
        if (!b.used && free == nullptr) free = &b;
    }
    OpTarget t = target;
    t.counted  = false;
    t.reservedBytes = 48u;
    if (free == nullptr || capacity == 0 || pool == 0) {
        Block tmp;   // no block: the event goes straight out (the pool holds at least one record)
        tmp.sid = sid; tmp.target = target;
        deliver_(tmp, GuardEvent::ReserveFailed, 0, 0, 0);
        return;
    }
    Block& b = *free;
    b.entries.reset(new (std::nothrow) GuardEntry[capacity]);   // cold, duty thread
    if (!b.entries) {
        Block tmp;
        tmp.sid = sid; tmp.target = target;
        deliver_(tmp, GuardEvent::ReserveFailed, 0, 0, 0);
        return;
    }
    b.used = true; b.sid = sid; b.capacity = capacity; b.pool = pool; b.target = target;
    b.posted = b.acked = 0; b.head = b.tail = -1;
    deliver_(b, GuardEvent::Reserved, 0, 0, 0);
}

void LeaseGuards::arm(uint32_t sid, uint32_t slot, uint32_t gen, uint32_t armSeq, int64_t atRiskNs, int64_t nowNs) {
    Block* b = find_(sid);
    if (b == nullptr || slot >= b->capacity) return;
    GuardEntry& e = b->entries[slot];
    if (e.state != GuardEntry::State::Free && newer(e.gen, gen)) return;   // an arm of a lease that already left the slot
    if (e.state == GuardEntry::State::Free || newer(gen, e.gen)) {   // a new lease in the slot: adopt it
        killProducers_(e);   // the producers of the old lease are blocked for good, never left bound to a reused block
        e.gen = gen; e.state = GuardEntry::State::Armed; e.armSeq = armSeq; e.atRiskNs = atRiskNs;
        e.nEdge = 0; e.pendEnded = false;
    } else if (e.state == GuardEntry::State::Ended) {
        return;
    } else if (!newer(armSeq, e.armSeq)) {
        return;   // the same lease: an older (or the same) arm is a no-op, the newer deadline stays
    } else if (e.state == GuardEntry::State::Tripped) {
        e.armSeq = armSeq; e.atRiskNs = atRiskNs;
        if (atRiskNs > nowNs) {
            e.state = GuardEntry::State::Armed;
            pushEdge_(*b, slot, GuardEvent::Rearmed, armSeq);
        } else {
            return;   // the new deadline is already past: stays tripped
        }
    } else {
        e.armSeq = armSeq; e.atRiskNs = atRiskNs;   // Armed: the deadline only
    }
    if (atRiskNs < minArmedNs_) minArmedNs_ = atRiskNs;
}

void LeaseGuards::killProducers_(GuardEntry& e) noexcept {
    for (uint32_t i = 0; i < e.nProducers; ++i)
        if (e.producers[i] != nullptr) {
            e.producers[i]->failGuarded(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseAtRisk));
            e.producers[i]->guardKill();
            e.producers[i] = nullptr;
        }
    e.nProducers = 0;
}

void LeaseGuards::failProducers_(GuardEntry& e) noexcept {
    for (uint32_t i = 0; i < e.nProducers; ++i)
        if (e.producers[i] != nullptr) e.producers[i]->failGuarded(OpStatus::DeliveryFailed, static_cast<uint16_t>(ClientErrorCode::LeaseAtRisk));
}

void LeaseGuards::end(uint32_t sid, uint32_t slot, uint32_t gen) {
    Block* b = find_(sid);
    if (b == nullptr || slot >= b->capacity) return;
    GuardEntry& e = b->entries[slot];
    if (e.state != GuardEntry::State::Free && newer(e.gen, gen)) return;   // the end of a lease that already left the slot
    if (e.state == GuardEntry::State::Free || newer(gen, e.gen)) {         // ended before anything of it was applied: adopt as Ended
        killProducers_(e);
        e.gen = gen; e.state = GuardEntry::State::Ended; e.armSeq = 0; e.atRiskNs = 0;
        e.nEdge = 0;
    } else if (e.state == GuardEntry::State::Ended) {
        return;
    } else {
        e.state = GuardEntry::State::Ended;   // blocks the bound producers first ...
        failProducers_(e);                    // ... then fails what they accepted
    }
    e.pendEnded = true;
    enlist_(*b, slot);
    flush_(*b);
}

OpTarget LeaseGuards::release(uint32_t sid) {
    Block* b = find_(sid);
    OpTarget none{};
    if (b == nullptr) return none;
    for (uint32_t s = 0; s < b->capacity; ++s) {
        killProducers_(b->entries[s]);
    }
    OpTarget t = b->target;
    b->entries.reset();
    *b = Block{};
    recomputeMin_();
    return t;
}

void LeaseGuards::ack(uint32_t sid, uint64_t consumedTotal) {
    Block* b = find_(sid);
    if (b == nullptr) return;
    consumedTotal = std::min(consumedTotal, b->posted);
    if (consumedTotal > b->acked) b->acked = consumedTotal;
    flush_(*b);
}

void LeaseGuards::refuseBind_(Block& b, uint32_t slot, uint32_t gen, producer::ProducerImpl& p) noexcept {
    p.guardFailOnBind();   // a transactional producer's open transaction can no longer commit (deferred out of the drain's read)
    p.guardKill();
    if (b.posted - b.acked < b.pool) deliver_(b, GuardEvent::BindRefused, slot, gen, 0);
    else ++droppedBindRefused_;
}

void LeaseGuards::bind(uint32_t sid, uint32_t slot, uint32_t gen, uint32_t armSeq, int64_t atRiskNs, producer::ProducerImpl& p,
                       int64_t nowNs) {
    Block* b = find_(sid);
    if (b == nullptr || slot >= b->capacity) { p.guardFailOnBind(); p.guardKill(); return; }   // fail closed (the session gates binds on its block)
    if (p.guardBound()) { refuseBind_(*b, slot, gen, p); return; }        // a producer is bound to at most one guard: blocked for good
    GuardEntry& e = b->entries[slot];
    if (e.state != GuardEntry::State::Free && (newer(e.gen, gen) || (e.gen == gen && e.state == GuardEntry::State::Ended))) {
        refuseBind_(*b, slot, gen, p);   // the lease already left the slot
        return;
    }
    arm(sid, slot, gen, armSeq, atRiskNs, nowNs);   // adopts a newer generation; the same lease keeps the newer of the two arms
    if (e.nProducers >= kMaxGuardProducers) { refuseBind_(*b, slot, gen, p); return; }
    e.producers[e.nProducers++] = &p;
    p.guardAttach(&e, gen);
    if (e.state == GuardEntry::State::Tripped) p.guardFailOnBind();   // the failure runs after the drain's read (I-7)
}

void LeaseGuards::unbindProducer(producer::ProducerImpl& p) noexcept {
    for (Block& b : blocks_) {
        if (!b.used) continue;
        for (uint32_t s = 0; s < b.capacity; ++s) {
            GuardEntry& e = b.entries[s];
            for (uint32_t i = 0; i < e.nProducers; ++i)
                if (e.producers[i] == &p) {
                    e.producers[i] = e.producers[--e.nProducers];
                    e.producers[e.nProducers] = nullptr;
                    break;
                }
        }
    }
    p.guardDetach();
}

void LeaseGuards::recomputeMin_() noexcept {
    // Control-plane cost: runs at most once per armed deadline passing (amortized over a keepalive period) and on release, never per
    // produce; O(sessions * capacity) there.
    int64_t m = INT64_MAX;
    for (Block& b : blocks_) {
        if (!b.used) continue;
        for (uint32_t s = 0; s < b.capacity; ++s)
            if (b.entries[s].state == GuardEntry::State::Armed && b.entries[s].atRiskNs < m) m = b.entries[s].atRiskNs;
    }
    minArmedNs_ = m;
}

int LeaseGuards::trip(int64_t nowNs) {
    if (nowNs < minArmedNs_) [[likely]] return 0;
    int n = 0;
    for (Block& b : blocks_) {
        if (!b.used) continue;
        for (uint32_t s = 0; s < b.capacity; ++s) {
            GuardEntry& e = b.entries[s];
            if (e.state != GuardEntry::State::Armed || nowNs < e.atRiskNs) continue;
            e.state = GuardEntry::State::Tripped;   // blocks the bound producers first ...
            failProducers_(e);                      // ... then fails what they accepted
            pushEdge_(b, s, GuardEvent::Tripped, e.armSeq);
            ++n;
        }
    }
    recomputeMin_();
    return n;
}

uint64_t LeaseGuards::testPosted(uint32_t sid) const noexcept { const Block* b = find_(sid); return b ? b->posted : 0; }
uint64_t LeaseGuards::testAcked(uint32_t sid) const noexcept { const Block* b = find_(sid); return b ? b->acked : 0; }
int LeaseGuards::testState(uint32_t sid, uint32_t slot) const noexcept {
    const Block* b = find_(sid);
    return (b && slot < b->capacity) ? static_cast<int>(b->entries[slot].state) : -1;
}

}  // namespace rune::client::detail
