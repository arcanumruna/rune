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
// #987: a chunk-carrying answer queued under return-ring back-pressure does not allocate: its bytes are copied into a
// preallocated chunk BYTE RING (kChunkArenaBytes, a power of two) in queue order - entries leave strictly front to
// back, so the bytes are filled and freed in the same order.  A chunk is stored contiguously: when it does not fit
// before the physical end, the tail skips to the next wrap (the skipped bytes are accounted, so the free computation
// stays exact). pushWithChunk() reports SlotsFull (the slot ring is full: the caller's invariant halt) or ChunkFull
// (the bytes do not fit: the caller answers a chunk-less Throttled refusal, or, for a return type that cannot say it,
// falls back to an owned vector).  The entry's `worker.chunk` stays empty for an arena entry; chunkOf() is its bytes.
//
// Single-threaded: owned and touched only by the worker thread.  push() on a full FIFO returns false — the caller
// treats it as an invariant violation (the capacity is static_assert-ed against the §6 bound in Worker.h).

#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerReturnCodec.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <utility>

namespace rune::node {

struct ReturnFifoEntry {
    enum class Tag : std::uint8_t { Worker, Txn };
    Tag          tag = Tag::Worker;
    WorkerReturn worker;
    TxnReturn    txn;
    std::uint64_t chunkPos   = 0;       ///< #987: monotonic arena position of the chunk's first byte (arenaChunk only)
    std::uint32_t chunkLen   = 0;       ///< #987: the chunk's length in the arena
    bool          arenaChunk = false;   ///< #987: the chunk lives in the byte ring, not in worker.chunk
};

/// #987: the outcome of ReturnFifo::pushWithChunk.
enum class ChunkPush : std::uint8_t { Ok, SlotsFull, ChunkFull };

template <std::size_t Capacity>
class ReturnFifo {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "ReturnFifo capacity must be a power of two");
public:
    static constexpr std::size_t kCapacity = Capacity;

    /// `chunkArenaBytes` (a power of two, 0 = no arena) is the chunk byte ring; allocated once, never touched until
    /// used.
    explicit ReturnFifo(std::size_t chunkArenaBytes = 0)
        : slots_(std::make_unique<ReturnFifoEntry[]>(Capacity)) { allocArena_(chunkArenaBytes); }
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
        e.arenaChunk = false;
        ++tail_;
        return true;
    }
    /// Append a WorkerReturn, moving its chunk in (no copy).  False when full.
    [[nodiscard]] bool push(WorkerReturn&& r) noexcept {
        if (full()) [[unlikely]] return false;
        ReturnFifoEntry& e = slots_[tail_ & kMask];
        e.tag    = ReturnFifoEntry::Tag::Worker;
        e.worker = std::move(r);
        e.arenaChunk = false;
        ++tail_;
        return true;
    }
    /// Append a TxnReturn (trivially copyable).  False when full.
    [[nodiscard]] bool push(const TxnReturn& r) noexcept {
        if (full()) [[unlikely]] return false;
        ReturnFifoEntry& e = slots_[tail_ & kMask];
        e.tag = ReturnFifoEntry::Tag::Txn;
        e.txn = r;
        e.arenaChunk = false;
        ++tail_;
        return true;
    }

    /// #987: append a WorkerReturn whose chunk is the concatenation of `runs` (`total` bytes), copied into the chunk
    /// byte ring (no allocation).  `r.chunk` must be empty.  SlotsFull: the slot ring is full; ChunkFull: the bytes
    /// do not fit (nothing was pushed).  A zero-length chunk takes no arena bytes.
    [[nodiscard]] ChunkPush pushWithChunk(WorkerReturn&& r, const ChunkRun* runs, std::size_t nRuns,
                                          std::uint32_t total) noexcept;

    /// #987: the entry's chunk bytes (arena or owned vector).
    [[nodiscard]] std::span<const std::uint8_t> chunkOf(const ReturnFifoEntry& e) const noexcept {
        if (e.arenaChunk) return {arena_.get() + (e.chunkPos & arenaMask_), e.chunkLen};
        return {e.worker.chunk.data(), e.worker.chunk.size()};
    }
    [[nodiscard]] std::size_t chunkArenaCapacity() const noexcept { return arenaBytes_; }
    [[nodiscard]] std::size_t chunkArenaUsed() const noexcept {
        return static_cast<std::size_t>(arenaTail_ - arenaHead_);
    }
    /// Test seam: a different arena size (a power of two); only while the FIFO is empty.
    void resetChunkArenaForTest(std::size_t bytes) { if (empty()) allocArena_(bytes); }

    /// The oldest entry.  Precondition: !empty().
    [[nodiscard]] ReturnFifoEntry& front() noexcept { return slots_[head_ & kMask]; }

    /// Drop the oldest entry, O(1).  Precondition: !empty().  A WorkerReturn
    /// chunk's buffer is released (a flushed PushRead chunk is not pinned).
    void popFront() noexcept {
        ReturnFifoEntry& e = slots_[head_ & kMask];
        if (e.arenaChunk) [[unlikely]] {
            arenaHead_ = e.chunkPos + e.chunkLen;   // entries leave in order: everything before this chunk is free too
            e.arenaChunk = false;
            e.chunkLen = 0;
        }
        if (e.tag == ReturnFifoEntry::Tag::Worker && e.worker.chunk.capacity() != 0) [[unlikely]] {
            std::vector<uint8_t>().swap(e.worker.chunk);
        }
        ++head_;
        if (head_ == tail_) arenaHead_ = arenaTail_;   // empty: no byte is held
    }

private:
    static constexpr std::uint64_t kMask = Capacity - 1;
    void allocArena_(std::size_t bytes) {
        arena_.reset(bytes != 0 ? new std::uint8_t[bytes] : nullptr);   // default-init: no page is touched until used
        arenaBytes_ = bytes;
        arenaMask_  = bytes != 0 ? bytes - 1 : 0;
        arenaHead_ = arenaTail_ = 0;
    }
    std::unique_ptr<ReturnFifoEntry[]> slots_;
    std::unique_ptr<std::uint8_t[]>    arena_;
    std::size_t   arenaBytes_ = 0;
    std::uint64_t arenaMask_  = 0;
    std::uint64_t arenaHead_  = 0;   // first byte still held (monotonic)
    std::uint64_t arenaTail_  = 0;   // next byte to write (monotonic)
    std::uint64_t head_ = 0;   // next to pop
    std::uint64_t tail_ = 0;   // next to push
};

template <std::size_t Capacity>
inline ChunkPush ReturnFifo<Capacity>::pushWithChunk(WorkerReturn&& r, const ChunkRun* runs, std::size_t nRuns,
                                                     std::uint32_t total) noexcept {
    if (full()) [[unlikely]] return ChunkPush::SlotsFull;
    std::uint64_t pos = arenaTail_;
    if (total != 0) {
        if (total > arenaBytes_) [[unlikely]] return ChunkPush::ChunkFull;
        const std::uint64_t phys = pos & arenaMask_;
        const std::uint64_t skip = total > arenaBytes_ - phys ? arenaBytes_ - phys : 0;   // contiguous: wrap to 0
        if (arenaBytes_ - (arenaTail_ - arenaHead_) < skip + total) return ChunkPush::ChunkFull;
        pos += skip;
        std::uint8_t* dst = arena_.get() + (pos & arenaMask_);
        for (std::size_t i = 0; i < nRuns; ++i) { std::memcpy(dst, runs[i].data, runs[i].len); dst += runs[i].len; }
        arenaTail_ = pos + total;
    }
    ReturnFifoEntry& e = slots_[tail_ & kMask];
    e.tag        = ReturnFifoEntry::Tag::Worker;
    e.worker     = std::move(r);
    e.arenaChunk = total != 0;
    e.chunkPos   = pos;
    e.chunkLen   = total;
    ++tail_;
    return ChunkPush::Ok;
}

} // namespace rune::node
