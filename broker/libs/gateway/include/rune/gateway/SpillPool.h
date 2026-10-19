#pragma once
// ---------------------------------------------------------------------------
// SpillPool.h — no-hang fix round 4 (N8): the gateway's egress spill as a
// preallocated pool of fixed-size blocks with per-owner intrusive chains.
//
// Replaces the round-3 byte FIFO (SpillFifo scan / discard / compact), whose
// drain walked EVERY spilled record on every duty cycle (O(spilled records),
// ~190 µs with a full spill) and whose compaction needed a twin 4 MiB buffer.
//
//   * Storage: `blockCount` blocks of kBlockBytes, allocated ONCE in the
//     constructor; an intrusive singly linked free list threads the free ones.
//     Block layout: [next:u32][len:u32][payload: kBlockPayload bytes].
//     `len` is meaningful in a record's FIRST block only.
//   * A record is a run of consecutive blocks of one Chain; a Chain (one per
//     owner — a gateway Session) links all of its records' blocks in order:
//     append at the tail, consume at the head.  Per-owner order holds by
//     construction; owners never interleave, so nothing is ever scanned.
//   * append / popHead: O(blocks of that record).  freeAll: O(1) (the whole
//     chain is spliced onto the free list).  No compaction, no memmove.
//   * A record of one block is read in place; a longer one is gathered into a
//     caller-supplied staging buffer (the gateway's preallocated stage).
//
// One owner thread (shared-nothing; no atomics, no locks).  Never allocates
// after construction.  append returns false when the free blocks do not cover
// the record — the caller then takes its EXPLICIT full path (never a drop).
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <rune/storage/ZeroedBytes.h>

namespace rune::gateway {

class SpillPool {
public:
    static constexpr uint32_t    kNil          = UINT32_MAX;
    static constexpr std::size_t kBlockBytes   = 64;     // one cache line
    static constexpr std::size_t kBlockHeader  = 8;      // next(4) + len(4)
    static constexpr std::size_t kBlockPayload = kBlockBytes - kBlockHeader;

    /// One owner's spilled records, oldest first.  Plain data owned by the
    /// caller (a gateway Session); only this pool mutates it.
    struct Chain {
        uint32_t head    = kNil;   ///< first block of the oldest record
        uint32_t tail    = kNil;   ///< last block of the newest record
        uint32_t records = 0;
        uint32_t blocks  = 0;
        [[nodiscard]] bool empty() const noexcept { return records == 0; }
    };

    explicit SpillPool(std::size_t capacityBytes)
        : blocks_(capacityBytes / kBlockBytes < 1 ? 1 : capacityBytes / kBlockBytes),
          mem_(blocks_ * kBlockBytes) {
        for (uint32_t b = 0; b < blocks_; ++b) setNext_(b, b + 1 < blocks_ ? b + 1 : kNil);
        freeHead_   = 0;
        freeBlocks_ = blocks_;
    }

    SpillPool(const SpillPool&)            = delete;
    SpillPool& operator=(const SpillPool&) = delete;
    SpillPool(SpillPool&&)                 = default;
    SpillPool& operator=(SpillPool&&)      = default;

    /// Blocks one record of `len` payload bytes occupies.
    [[nodiscard]] static constexpr std::size_t blocksFor(std::size_t len) noexcept {
        return len == 0 ? 1 : (len + kBlockPayload - 1) / kBlockPayload;
    }
    /// Pool bytes one record of `len` payload bytes occupies (quota unit).
    [[nodiscard]] static constexpr std::size_t bytesFor(std::size_t len) noexcept {
        return blocksFor(len) * kBlockBytes;
    }

    [[nodiscard]] std::size_t capacityBlocks() const noexcept { return blocks_; }
    [[nodiscard]] std::size_t capacityBytes() const noexcept { return blocks_ * kBlockBytes; }
    [[nodiscard]] std::size_t freeBlocks() const noexcept { return freeBlocks_; }
    [[nodiscard]] std::size_t usedBytes() const noexcept { return (blocks_ - freeBlocks_) * kBlockBytes; }
    [[nodiscard]] std::size_t records() const noexcept { return records_; }

    /// Append one record to `c`.  False (nothing written) when it does not fit.
    [[nodiscard]] bool append(Chain& c, const uint8_t* data, std::size_t len) noexcept {
        if (len > UINT32_MAX) return false;
        const std::size_t n = blocksFor(len);
        if (n > freeBlocks_) return false;
        const uint32_t first = freeHead_;
        uint32_t b = first, last = kNil;
        std::size_t off = 0;
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t piece = len - off < kBlockPayload ? len - off : kBlockPayload;
            if (piece != 0) std::memcpy(payload_(b), data + off, piece);
            off += piece;
            last = b;
            b = next_(b);
        }
        freeHead_ = b;                         // the free list continues after the run
        freeBlocks_ -= n;
        setLen_(first, static_cast<uint32_t>(len));
        setNext_(last, kNil);
        if (c.tail == kNil) c.head = first; else setNext_(c.tail, first);
        c.tail = last;
        ++c.records;
        c.blocks += static_cast<uint32_t>(n);
        ++records_;
        return true;
    }

    /// Payload length of `c`'s oldest record (c non-empty).
    [[nodiscard]] std::size_t headLen(const Chain& c) const noexcept { return getLen_(c.head); }

    /// `c`'s oldest record's bytes: in place when it is one block, else gathered
    /// into `stage` (capacity >= headLen(c)).  Valid until the next mutation.
    [[nodiscard]] const uint8_t* headData(const Chain& c, uint8_t* stage) const noexcept {
        const std::size_t len = getLen_(c.head);
        if (len <= kBlockPayload) return payload_(c.head);
        std::size_t off = 0;
        for (uint32_t b = c.head; off < len; b = next_(b)) {
            const std::size_t piece = len - off < kBlockPayload ? len - off : kBlockPayload;
            std::memcpy(stage + off, payload_(b), piece);
            off += piece;
        }
        return stage;
    }

    /// Release `c`'s oldest record (c non-empty).  O(its blocks).
    void popHead(Chain& c) noexcept {
        const std::size_t n = blocksFor(getLen_(c.head));
        const uint32_t first = c.head;
        uint32_t last = first;
        for (std::size_t i = 1; i < n; ++i) last = next_(last);
        c.head = next_(last);
        if (c.head == kNil) c.tail = kNil;
        setNext_(last, freeHead_);
        freeHead_ = first;
        freeBlocks_ += n;
        --c.records;
        c.blocks -= static_cast<uint32_t>(n);
        --records_;
    }

    /// Release every record of `c` (returns how many).  O(1): the chain is
    /// spliced onto the free list as a whole.
    std::size_t freeAll(Chain& c) noexcept {
        const std::size_t n = c.records;
        if (n == 0) return 0;
        setNext_(c.tail, freeHead_);
        freeHead_ = c.head;
        freeBlocks_ += c.blocks;
        records_ -= n;
        c = Chain{};
        return n;
    }

private:
    [[nodiscard]] uint8_t* block_(uint32_t b) noexcept { return mem_.data() + std::size_t{b} * kBlockBytes; }
    [[nodiscard]] const uint8_t* block_(uint32_t b) const noexcept {
        return mem_.data() + std::size_t{b} * kBlockBytes;
    }
    [[nodiscard]] uint8_t* payload_(uint32_t b) noexcept { return block_(b) + kBlockHeader; }
    [[nodiscard]] const uint8_t* payload_(uint32_t b) const noexcept { return block_(b) + kBlockHeader; }
    [[nodiscard]] uint32_t next_(uint32_t b) const noexcept {
        uint32_t v; std::memcpy(&v, block_(b), 4); return v;
    }
    void setNext_(uint32_t b, uint32_t v) noexcept { std::memcpy(block_(b), &v, 4); }
    [[nodiscard]] uint32_t getLen_(uint32_t b) const noexcept {
        uint32_t v; std::memcpy(&v, block_(b) + 4, 4); return v;
    }
    void setLen_(uint32_t b, uint32_t v) noexcept { std::memcpy(block_(b) + 4, &v, 4); }

    uint32_t             blocks_;
    rune::storage::ZeroedBytes mem_;   // fixed at construction (memset, not a per-byte vector fill — Debug-build cost)
    uint32_t             freeHead_   = kNil;
    std::size_t          freeBlocks_ = 0;
    std::size_t          records_    = 0;
};

} // namespace rune::gateway
