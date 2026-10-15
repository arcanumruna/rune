#pragma once
// ---------------------------------------------------------------------------
// SpillFifo.h — no-hang fix round 2: a preallocated, bounded, single-thread
// byte FIFO of variable-length records [key:i64][len:u32][flags:u32][bytes]
// (8-aligned).
//
// Used where an answer that cannot be delivered NOW must be kept, in strict
// FIFO order, instead of being dropped or sent best-effort: AdminWorkerAgent's
// admin→control egress backlog (DDL answers on a full ring — front() / pop()).
// (The gateway's per-session egress spill is SpillPool — fix round 4.)
//
// One owner thread (shared-nothing; no atomics).  All storage is allocated
// once in the constructor; push / front / pop never allocate.  push returns
// false when the record does not fit — the caller then takes its EXPLICIT
// full path (never a silent drop).
// ---------------------------------------------------------------------------
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include <rune/storage/ZeroedBytes.h>

namespace rune::gateway {

class SpillFifo {
public:
    struct Record {
        int64_t        key  = 0;
        const uint8_t* data = nullptr;
        std::size_t    len  = 0;
    };

    explicit SpillFifo(std::size_t capacityBytes)
        : buf_(alignUp_(capacityBytes < 64 ? 64 : capacityBytes)) {}

    SpillFifo(const SpillFifo&)            = delete;
    SpillFifo& operator=(const SpillFifo&) = delete;
    SpillFifo(SpillFifo&&)                 = default;
    SpillFifo& operator=(SpillFifo&&)      = default;

    [[nodiscard]] bool        empty() const noexcept { return live_ == 0; }
    [[nodiscard]] std::size_t count() const noexcept { return live_; }      ///< live records
    [[nodiscard]] std::size_t capacity() const noexcept { return buf_.size(); }
    [[nodiscard]] std::size_t usedBytes() const noexcept { return used_; }  ///< incl. not-yet-reclaimed done records

    /// Append one record.  False (nothing written) when it does not fit.
    [[nodiscard]] bool push(int64_t key, const uint8_t* data, std::size_t len) noexcept {
        const std::size_t cap = buf_.size();
        if (len > UINT32_MAX - 1) return false;
        const std::size_t rs = recordSize_(len);
        if (rs > cap) return false;
        if (records_ == 0) { head_ = 0; tail_ = 0; used_ = 0; }
        std::size_t at;
        if (records_ == 0 || tail_ > head_) {
            if (cap - tail_ >= rs) {
                at = tail_;
            } else if (head_ >= rs) {            // wrap: the region [0, head_) is free
                if (cap - tail_ >= kHeader) putLen_(buf_.data(), tail_, kWrap);
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
        writeRecord_(buf_.data(), at, key, data, len);
        tail_ = at + rs;
        used_ += rs;
        ++records_;
        ++live_;
        return true;
    }

    /// The oldest LIVE record (count() > 0).  Valid until the next mutation.
    [[nodiscard]] Record front() noexcept {
        reclaimPrefix_();
        std::size_t p = head_;
        for (std::size_t i = 0; i < records_; ++i) {
            p = wrapPos_(p);
            if (!isDone_(p)) return recordAt_(p);
            p += recordSize_(getLen_(buf_.data(), p));
        }
        return Record{};
    }

    /// Remove the oldest LIVE record.
    void pop() noexcept {
        std::size_t p = head_;
        for (std::size_t i = 0; i < records_; ++i) {
            p = wrapPos_(p);
            if (!isDone_(p)) { markDone_(p); break; }
            p += recordSize_(getLen_(buf_.data(), p));
        }
        reclaimPrefix_();
    }

private:
    static constexpr std::size_t kHeader = 16;          // key(8) + len(4) + flags(4)
    static constexpr uint32_t    kWrap   = UINT32_MAX;  // "continue at offset 0"
    static constexpr uint32_t    kDone   = 1u;

    static constexpr std::size_t alignUp_(std::size_t n) noexcept { return (n + 7u) & ~std::size_t{7}; }
    static constexpr std::size_t recordSize_(std::size_t len) noexcept { return alignUp_(kHeader + len); }

    static void putLen_(uint8_t* b, std::size_t at, uint32_t v) noexcept { std::memcpy(b + at + 8, &v, 4); }
    [[nodiscard]] static uint32_t getLen_(const uint8_t* b, std::size_t at) noexcept {
        uint32_t v; std::memcpy(&v, b + at + 8, 4); return v;
    }
    static void writeRecord_(uint8_t* b, std::size_t at, int64_t key, const uint8_t* data, std::size_t len) noexcept {
        const uint32_t flags = 0;
        std::memcpy(b + at, &key, 8);
        putLen_(b, at, static_cast<uint32_t>(len));
        std::memcpy(b + at + 12, &flags, 4);
        if (len != 0) std::memcpy(b + at + kHeader, data, len);
    }
    [[nodiscard]] bool isDone_(std::size_t at) const noexcept {
        uint32_t f; std::memcpy(&f, buf_.data() + at + 12, 4); return (f & kDone) != 0;
    }
    void markDone_(std::size_t at) noexcept {
        uint32_t f; std::memcpy(&f, buf_.data() + at + 12, 4);
        if (f & kDone) return;
        f |= kDone;
        std::memcpy(buf_.data() + at + 12, &f, 4);
        --live_;
    }
    [[nodiscard]] Record recordAt_(std::size_t at) const noexcept {
        Record r;
        std::memcpy(&r.key, buf_.data() + at, 8);
        r.len  = getLen_(buf_.data(), at);
        r.data = buf_.data() + at + kHeader;
        return r;
    }
    // The writer wrapped at `p`: no room for a header remained, or a wrap
    // marker was written there.
    [[nodiscard]] std::size_t wrapPos_(std::size_t p) const noexcept {
        const std::size_t cap = buf_.size();
        if (cap - p < kHeader || getLen_(buf_.data(), p) == kWrap) return 0;
        return p;
    }
    void reclaimPrefix_() noexcept {
        const std::size_t cap = buf_.size();
        while (records_ != 0) {
            const std::size_t w = wrapPos_(head_);
            if (w != head_) { used_ -= cap - head_; head_ = 0; }
            if (!isDone_(head_)) break;
            const std::size_t rs = recordSize_(getLen_(buf_.data(), head_));
            head_ += rs;
            used_ -= rs;
            --records_;
        }
        if (records_ == 0) { head_ = 0; tail_ = 0; used_ = 0; }
    }

    rune::storage::ZeroedBytes buf_;   // fixed at construction (memset, not a per-byte vector fill — Debug-build cost)
    std::size_t head_    = 0;
    std::size_t tail_    = 0;
    std::size_t used_    = 0;
    std::size_t records_ = 0;        // live + done (not yet reclaimed)
    std::size_t live_    = 0;
};

} // namespace rune::gateway
