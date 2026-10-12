#pragma once
// ---------------------------------------------------------------------------
// SpillFifo.h — no-hang fix round 2: a preallocated, bounded, single-thread
// byte FIFO of variable-length records [key:i64][len:u32][bytes] (8-aligned).
//
// Used where an answer that cannot be delivered NOW must be kept, in order,
// instead of being dropped or sent best-effort:
//   - ClientGateway's egress spill (back-pressured client answers, any size up
//     to the capacity — the old 512-byte slot ring fell back to best-effort);
//   - AdminWorkerAgent's admin→control egress backlog (DDL answers on a full
//     ring — was an unbounded vector<vector<uint8_t>> with O(n) front erase).
//
// One owner thread (shared-nothing; no atomics).  The storage is allocated
// once in the constructor; push / front / pop never allocate.  push returns
// false when the record does not fit — the caller then takes its EXPLICIT
// full path (never a silent drop).
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace rune::gateway {

class SpillFifo {
public:
    struct Record {
        int64_t        key  = 0;
        const uint8_t* data = nullptr;
        std::size_t    len  = 0;
    };

    explicit SpillFifo(std::size_t capacityBytes) : buf_(alignUp_(capacityBytes < 64 ? 64 : capacityBytes)) {}

    SpillFifo(const SpillFifo&)            = delete;
    SpillFifo& operator=(const SpillFifo&) = delete;
    SpillFifo(SpillFifo&&)                 = default;
    SpillFifo& operator=(SpillFifo&&)      = default;

    [[nodiscard]] bool        empty() const noexcept { return count_ == 0; }
    [[nodiscard]] std::size_t count() const noexcept { return count_; }
    [[nodiscard]] std::size_t capacity() const noexcept { return buf_.size(); }
    [[nodiscard]] std::size_t usedBytes() const noexcept { return used_; }

    /// Append one record.  False (nothing written) when it does not fit.
    [[nodiscard]] bool push(int64_t key, const uint8_t* data, std::size_t len) noexcept {
        const std::size_t cap = buf_.size();
        if (len > UINT32_MAX - 1) return false;
        const std::size_t rs = recordSize_(len);
        if (rs > cap) return false;
        if (count_ == 0) { head_ = 0; tail_ = 0; used_ = 0; }
        std::size_t at;
        if (count_ == 0 || tail_ > head_) {
            if (cap - tail_ >= rs) {
                at = tail_;
            } else if (head_ >= rs) {            // wrap: the region [0, head_) is free
                if (cap - tail_ >= kHeader) putLen_(tail_, kWrap);
                used_ += cap - tail_;            // the skipped tail counts as used until the reader wraps
                at = 0;
            } else {
                return false;
            }
        } else if (tail_ < head_) {
            if (head_ - tail_ >= rs) at = tail_; else return false;
        } else {
            return false;                         // tail_ == head_ with records: full
        }
        std::memcpy(buf_.data() + at, &key, 8);
        putLen_(at, static_cast<uint32_t>(len));
        if (len != 0) std::memcpy(buf_.data() + at + kHeader, data, len);
        tail_ = at + rs;
        used_ += rs;
        ++count_;
        return true;
    }

    /// The oldest record (count() > 0).  Valid until the next pop / push.
    [[nodiscard]] Record front() noexcept {
        skipWrap_();
        Record r;
        std::memcpy(&r.key, buf_.data() + head_, 8);
        r.len  = getLen_(head_);
        r.data = buf_.data() + head_ + kHeader;
        return r;
    }

    void pop() noexcept {
        if (count_ == 0) return;
        skipWrap_();
        const std::size_t rs = recordSize_(getLen_(head_));
        head_ += rs;
        used_ -= rs;
        if (--count_ == 0) { head_ = 0; tail_ = 0; used_ = 0; }
    }

private:
    static constexpr std::size_t kHeader = 12;          // key(8) + len(4)
    static constexpr uint32_t    kWrap   = UINT32_MAX;  // "continue at offset 0"

    static constexpr std::size_t alignUp_(std::size_t n) noexcept { return (n + 7u) & ~std::size_t{7}; }
    static constexpr std::size_t recordSize_(std::size_t len) noexcept { return alignUp_(kHeader + len); }

    void putLen_(std::size_t at, uint32_t v) noexcept { std::memcpy(buf_.data() + at + 8, &v, 4); }
    [[nodiscard]] uint32_t getLen_(std::size_t at) const noexcept {
        uint32_t v; std::memcpy(&v, buf_.data() + at + 8, 4); return v;
    }
    // The writer wrapped at head_: either no room for a header remained, or a
    // wrap marker was written there.  Release the skipped tail bytes.
    void skipWrap_() noexcept {
        const std::size_t cap = buf_.size();
        if (cap - head_ < kHeader || getLen_(head_) == kWrap) {
            used_ -= cap - head_;
            head_ = 0;
        }
    }

    std::vector<uint8_t> buf_;
    std::size_t head_  = 0;
    std::size_t tail_  = 0;
    std::size_t used_  = 0;
    std::size_t count_ = 0;
};

} // namespace rune::gateway
