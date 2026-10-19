#pragma once
// ReturnFifo.h — F15 S3 (design note 2026-10-01-f15-eos-marker-lso-design.md
// §6, m5 + I8): the worker's local return FIFO in front of ret_.
//
// One FIFO in EVERY mode.  A return goes to ret_ only while this FIFO is empty;
// otherwise it is appended here and flushed front-to-back, stopping at the
// first ring refusal — so returns reach the control thread in emission order
// (§1.1 L2) and none is ever dropped.
//
// Fixed capacity (power of two), pre-allocated once at construction: push and
// pop-front are O(1) index arithmetic with no allocation, no virtual call and
// no std::function (replaces the old `std::vector::erase(begin())`).  Entries
// are tagged: a WorkerReturn (template id 2) or a TxnReturn (template id 6).
// A WorkerReturn's chunk is MOVED in and released on pop, so a slot never pins
// a PushRead chunk's memory after it is flushed.
//
// Single-threaded: owned and touched only by the worker thread.  push() on a
// full FIFO returns false — the caller treats it as an invariant violation
// (the capacity is static_assert-ed against the §6 bound in Worker.h).

#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReturnCodec.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace rune::node {

struct ReturnFifoEntry {
    enum class Tag : std::uint8_t { Worker, Txn };
    Tag          tag = Tag::Worker;
    WorkerReturn worker;
    TxnReturn    txn;
};

template <std::size_t Capacity>
class ReturnFifo {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "ReturnFifo capacity must be a power of two");
public:
    static constexpr std::size_t kCapacity = Capacity;

    ReturnFifo() : slots_(std::make_unique<ReturnFifoEntry[]>(Capacity)) {}
    ReturnFifo(const ReturnFifo&)            = delete;
    ReturnFifo& operator=(const ReturnFifo&) = delete;

    [[nodiscard]] bool        empty() const noexcept { return head_ == tail_; }
    [[nodiscard]] std::size_t size() const noexcept { return static_cast<std::size_t>(tail_ - head_); }
    [[nodiscard]] bool        full() const noexcept { return size() == Capacity; }
    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

    /// Append a WorkerReturn (copied; its chunk is copied too — callers on the
    /// chunk-carrying paths use the rvalue overload).  False when full.
    [[nodiscard]] bool push(const WorkerReturn& r) {
        if (full()) [[unlikely]] return false;
        ReturnFifoEntry& e = slots_[tail_ & kMask];
        e.tag    = ReturnFifoEntry::Tag::Worker;
        e.worker = r;
        ++tail_;
        return true;
    }
    /// Append a WorkerReturn, moving its chunk in (no copy).  False when full.
    [[nodiscard]] bool push(WorkerReturn&& r) noexcept {
        if (full()) [[unlikely]] return false;
        ReturnFifoEntry& e = slots_[tail_ & kMask];
        e.tag    = ReturnFifoEntry::Tag::Worker;
        e.worker = std::move(r);
        ++tail_;
        return true;
    }
    /// Append a TxnReturn (trivially copyable).  False when full.
    [[nodiscard]] bool push(const TxnReturn& r) noexcept {
        if (full()) [[unlikely]] return false;
        ReturnFifoEntry& e = slots_[tail_ & kMask];
        e.tag = ReturnFifoEntry::Tag::Txn;
        e.txn = r;
        ++tail_;
        return true;
    }

    /// The oldest entry.  Precondition: !empty().
    [[nodiscard]] ReturnFifoEntry& front() noexcept { return slots_[head_ & kMask]; }

    /// Drop the oldest entry, O(1).  Precondition: !empty().  A WorkerReturn
    /// chunk's buffer is released (a flushed PushRead chunk is not pinned).
    void popFront() noexcept {
        ReturnFifoEntry& e = slots_[head_ & kMask];
        if (e.tag == ReturnFifoEntry::Tag::Worker && e.worker.chunk.capacity() != 0) [[unlikely]] {
            std::vector<uint8_t>().swap(e.worker.chunk);
        }
        ++head_;
    }

private:
    static constexpr std::uint64_t kMask = Capacity - 1;
    std::unique_ptr<ReturnFifoEntry[]> slots_;
    std::uint64_t head_ = 0;   // next to pop
    std::uint64_t tail_ = 0;   // next to push
};

} // namespace rune::node
