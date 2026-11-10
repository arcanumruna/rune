#pragma once
// LeaseParkTable.h - lease topics Task 11a (spec 8.1, 9.3, 9.6): the keepalives a worker parked, waiting for the log to catch up.
//
// A keepalive whose answer depends on a record still in flight (the Expire of a due key, the Checkpoint(0) that clears a row's
// remaining time, an Emitting / Clearing / Checkpointing entry) is PARKED here instead of answered: one slot per (request entry), chained
// from its lease entry (LeaseEntry::parkHead).  The fold answers the chain (answerChain) into the outbox; the worker drains the outbox
// into WorkerReturns.  Header-only, no allocation after construction (two fixed slot / request arrays, an outbox ring, a scratch order
// array), owned by ONE worker thread (shared-nothing).  Capacity kLeaseParkTableEntries parks; a request holds at most 256 entries, so
// 16 full requests fill the table.
//
// Chain order: park() pushes at the HEAD (newest first); answerChain / forEachInChain visit OLDEST first (through the scratch order).
// final: the last answer of a REQUEST (its pending count reaches 0) carries final = 1.  A request index is taken lazily on the
// request's first park (openRequest) and released when its final answer is drained.
#include <rune/node/lease/LeaseEntryStore.h>

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace rune::node::lease {

inline constexpr uint32_t kLeaseParkTableEntries = 4096;

enum class LeaseParkState : uint8_t { Free = 0, Parked = 1, Queued = 2 };

struct LeaseParkSlot {
    int64_t  sessionId;
    int64_t  correlationId;
    int32_t  topicId;
    int32_t  partition;
    uint32_t rid;
    uint32_t next;     ///< the chain (older slot) while Parked, the free list while Free
    uint32_t req;
    uint16_t index;    ///< the entry's index in its request
    uint8_t  state;    ///< LeaseParkState
    uint8_t  pad;
};
static_assert(sizeof(LeaseParkSlot) == 40);

struct LeaseParkRequest {
    int64_t  sessionId;
    int64_t  correlationId;
    uint32_t pending;    ///< entries parked or queued, not yet queued as an answer / drained
    uint32_t nextFree;
};

struct LeaseParkAnswer {
    uint32_t slot;
    uint16_t code;
    uint8_t  final;
    uint64_t ttlMs;
};

class LeaseParkTable {
public:
    LeaseParkTable() noexcept {
        for (uint32_t i = 0; i < kLeaseParkTableEntries; ++i) {
            slots_[i]    = LeaseParkSlot{};
            slots_[i].next = i + 1 < kLeaseParkTableEntries ? i + 1 : kLeaseNil;
            requests_[i] = LeaseParkRequest{};
            requests_[i].nextFree = i + 1 < kLeaseParkTableEntries ? i + 1 : kLeaseNil;
        }
        freeSlot_ = 0;
        freeReq_  = 0;
    }
    LeaseParkTable(const LeaseParkTable&)            = delete;
    LeaseParkTable& operator=(const LeaseParkTable&) = delete;

    /// A request index for the entries of one keepalive op, or kLeaseNil when none is free.
    [[nodiscard]] uint32_t openRequest(int64_t sessionId, int64_t correlationId) noexcept {
        if (freeReq_ == kLeaseNil) return kLeaseNil;
        const uint32_t r = freeReq_;
        LeaseParkRequest& q = requests_[r];
        freeReq_ = q.nextFree;
        q.sessionId = sessionId;
        q.correlationId = correlationId;
        q.pending = 0;
        q.nextFree = kLeaseNil;
        ++requestsInUse_;
        return r;
    }

    /// Parks entry `index` of request `req` on `rid`: pushed at the head of `chainHead`.  kLeaseNil when the table is full.
    [[nodiscard]] uint32_t park(uint32_t req, int32_t topicId, int32_t partition, uint32_t rid, uint16_t index, uint32_t& chainHead) noexcept {
        assert(req < kLeaseParkTableEntries);
        if (freeSlot_ == kLeaseNil) return kLeaseNil;
        const uint32_t s = freeSlot_;
        LeaseParkSlot& p = slots_[s];
        freeSlot_ = p.next;
        const LeaseParkRequest& q = requests_[req];
        p.sessionId     = q.sessionId;
        p.correlationId = q.correlationId;
        p.topicId       = topicId;
        p.partition     = partition;
        p.rid           = rid;
        p.next          = chainHead;
        p.req           = req;
        p.index         = index;
        p.state         = static_cast<uint8_t>(LeaseParkState::Parked);
        chainHead       = s;
        ++requests_[req].pending;
        ++inUse_;
        return s;
    }

    /// Every slot of the chain, oldest first, goes to the outbox with `code` / `ttlMs`; the request's last one carries final = 1.
    void answerChain(uint32_t& chainHead, uint16_t code, uint64_t ttlMs) noexcept {
        uint32_t n = 0;
        for (uint32_t s = chainHead; s != kLeaseNil; s = slots_[s].next) order_[n++] = s;
        chainHead = kLeaseNil;
        for (uint32_t i = n; i > 0; --i) queue_(order_[i - 1], code, ttlMs);
    }

    /// Cold: every Parked slot of (topicId, partition) is queued with `code` (ttlMs 0).  Used when a partition's chains die with its
    /// runtime (demotion, drop); the caller then destroys the runtime.
    void answerWhere(int32_t topicId, int32_t partition, uint16_t code) noexcept {
        for (uint32_t s = 0; s < kLeaseParkTableEntries; ++s) {
            const LeaseParkSlot& p = slots_[s];
            if (p.state == static_cast<uint8_t>(LeaseParkState::Parked) && p.topicId == topicId && p.partition == partition) queue_(s, code, 0);
        }
    }

    /// Visits the chain oldest first: fn(slotIndex, const LeaseParkSlot&).  `fn` may call answerOne on the visited slot (only).
    template <class Fn>
    void forEachInChain(uint32_t head, Fn&& fn) noexcept {
        uint32_t n = 0;
        for (uint32_t s = head; s != kLeaseNil; s = slots_[s].next) replay_[n++] = s;
        for (uint32_t i = n; i > 0; --i) fn(replay_[i - 1], static_cast<const LeaseParkSlot&>(slots_[replay_[i - 1]]));
    }

    /// Unlinks one slot from the chain and queues it.
    void answerOne(uint32_t& chainHead, uint32_t slot, uint16_t code, uint64_t ttlMs) noexcept {
        if (chainHead == slot) {
            chainHead = slots_[slot].next;
        } else {
            uint32_t s = chainHead;
            while (s != kLeaseNil && slots_[s].next != slot) s = slots_[s].next;
            assert(s != kLeaseNil && "answerOne: the slot is not in the chain");
            if (s == kLeaseNil) return;
            slots_[s].next = slots_[slot].next;
        }
        queue_(slot, code, ttlMs);
    }

    /// Pops every outbox answer: fn(const LeaseParkSlot& (a copy), const LeaseParkAnswer&), then frees the slot (and the request with its
    /// final answer).  Returns the count.
    template <class Fn>
    uint32_t drain(Fn&& fn) {
        uint32_t n = 0;
        while (head_ != tail_) {
            const LeaseParkAnswer a = outbox_[head_ % kLeaseParkTableEntries];
            ++head_;
            const LeaseParkSlot copy = slots_[a.slot];
            fn(copy, a);
            LeaseParkSlot& p = slots_[a.slot];
            p.state = static_cast<uint8_t>(LeaseParkState::Free);
            p.next  = freeSlot_;
            freeSlot_ = a.slot;
            --inUse_;
            if (a.final != 0) {
                LeaseParkRequest& q = requests_[copy.req];
                q.nextFree = freeReq_;
                freeReq_ = copy.req;
                --requestsInUse_;
            }
            ++n;
        }
        return n;
    }

    /// Parked + queued slots (counted in the worker's reservedAckSlots_).
    [[nodiscard]] uint32_t inUse() const noexcept { return inUse_; }
    [[nodiscard]] bool full() const noexcept { return freeSlot_ == kLeaseNil; }
    [[nodiscard]] uint32_t requestsInUse() const noexcept { return requestsInUse_; }
    [[nodiscard]] uint32_t queued() const noexcept { return static_cast<uint32_t>(tail_ - head_); }
    [[nodiscard]] uint32_t pendingOf(uint32_t req) const noexcept { return requests_[req].pending; }
    [[nodiscard]] const LeaseParkSlot& slot(uint32_t s) const noexcept { return slots_[s]; }

private:
    void queue_(uint32_t s, uint16_t code, uint64_t ttlMs) noexcept {
        LeaseParkSlot& p = slots_[s];
        assert(p.state == static_cast<uint8_t>(LeaseParkState::Parked));
        p.state = static_cast<uint8_t>(LeaseParkState::Queued);
        LeaseParkRequest& q = requests_[p.req];
        assert(q.pending > 0);
        const bool final = --q.pending == 0;
        outbox_[tail_ % kLeaseParkTableEntries] = LeaseParkAnswer{s, code, static_cast<uint8_t>(final ? 1 : 0), ttlMs};
        ++tail_;
    }

    std::array<LeaseParkSlot, kLeaseParkTableEntries>    slots_;
    std::array<LeaseParkRequest, kLeaseParkTableEntries> requests_;
    std::array<LeaseParkAnswer, kLeaseParkTableEntries>  outbox_;
    std::array<uint32_t, kLeaseParkTableEntries>         order_;    ///< answerChain's oldest-first scratch
    std::array<uint32_t, kLeaseParkTableEntries>         replay_;   ///< forEachInChain's
    uint32_t freeSlot_ = kLeaseNil, freeReq_ = kLeaseNil;
    uint32_t inUse_ = 0, requestsInUse_ = 0;
    uint64_t head_ = 0, tail_ = 0;
};

}  // namespace rune::node::lease
