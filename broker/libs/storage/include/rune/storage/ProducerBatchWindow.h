#pragma once
// ProducerBatchWindow.h - checked writes Part 06 (contract addition 9, P6-9): the last `depth` data batches of every idempotent
// producer in the range a cleaner pass scanned, on a partition whose producer state keeps the whole retry window (an
// autoincrement topic: depth kRetryWindowSize).
//
// A leader rebuilt from the log answers a retried key-0 batch with the ids its stored batch carries, and dedupes against the
// producer's last `depth` batches.  So the cleaner keeps those batches of a live producer WHOLE (protects): no record rule runs on
// them, a superseded or rejected record in one stays.  A superseded insert kept that way would come back once the tombstone of its
// key expired, so the tombstone of every key such a batch holds is pinned (pinsTombstone) while the batch is protected.  A key of
// 1..8 bytes (every autoincrement key width) is recorded as its keyIndexWord; a wider or null key is not.
//
// Expiry is ProducerTails': a producer whose last batch's broker append time (its segment file's last write, never the client's
// maxTimestamp) is older than nowMs - expirationMs is not protected (nowMs < 0 never expires); finish() drops it.  The ring holds each batch's position only, and finish() reads the keys of the batches it
// keeps.  Memory is bounded by a byte budget (the cleaner gives it the share of its dedupe budget ProducerTails gets).  A charge past
// it first evicts the producers that look expired so far in the scan, keeping only their ids (8 bytes each).  One of them seen again
// later is taken back with a fresh, PARTIAL ring: at finish() it is dropped if expired, exact once its ring holds `depth` batches
// (the evicted ones are older than its window), and only a live one short of `depth` overflows - an evicted batch may be in its
// window.  Past the budget the window is OVERFLOWED and conservative - every data batch protected, every tombstone pinned, the
// watermark held (lowestProtectedBase).
//
// The cleaner keeps its watermark at or below lowestProtectedBase(): a protected batch and the records superseding it stay in the
// dirty range until the pass that releases it, so a protected insert and its tombstone go in the same pass.
//
// Depth 1 (every other topic) builds no window: the cleaner then runs exactly as ProducerTails alone dictates.
// Cleaner thread only (cold; the cleaner is the one sanctioned heap user).
#include <rune/storage/ProducerTails.h>
#include <rune/storage/Timestamps.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::storage {

/// The wire key's bytes packed little-endian; the caller checked the width (at most 8).  The key index keys its table by it too.
[[nodiscard]] inline uint64_t keyIndexWord(std::string_view wireKey) noexcept {
    uint64_t w = 0;
    for (std::size_t i = 0; i < wireKey.size(); ++i) w |= static_cast<uint64_t>(static_cast<uint8_t>(wireKey[i])) << (8 * i);
    return w;
}

class ProducerBatchWindow {
public:
    /// What one recorded producer costs besides its batches (node + bucket, approximate).
    static constexpr std::size_t kBytesPerProducer = ProducerTails::kBytesPerProducer;

    explicit ProducerBatchWindow(int depth) : depth_(static_cast<std::size_t>(depth)) {}

    [[nodiscard]] int depth() const noexcept { return static_cast<int>(depth_); }

    void clear() noexcept {
        producers_.clear();
        evicted_.clear();
        batches_.clear();
        keys_.clear();
        bytes_ = 0;
        overflowed_ = false;
    }
    /// Start of a pre-pass: an empty window of `budget` bytes whose producers expire at `nowMs` (see expired()).
    void begin(std::size_t budget, int64_t nowMs, int64_t expirationMs) noexcept {
        clear();
        budget_ = budget;
        nowMs_ = nowMs;
        expirationMs_ = expirationMs;
    }

    // ---- pre-pass feed (offset order) -------------------------------------
    /// An idempotent DATA batch of `producerId` over [baseOffset, lastOffset], appended at `appendMs`, at byte `pos` of segment
    /// `segBase`.
    void noteBatch(int64_t producerId, int64_t baseOffset, int64_t lastOffset, int64_t appendMs, int64_t segBase, std::size_t pos) {
        if (overflowed_) return;
        auto it = producers_.find(producerId);
        if (it == producers_.end()) {
            const auto ev = std::lower_bound(evicted_.begin(), evicted_.end(), producerId);
            const bool partial = ev != evicted_.end() && *ev == producerId;
            if (partial) {
                evicted_.erase(ev);
                bytes_ -= sizeof(int64_t);
            }
            if (!charge(kBytesPerProducer, producerId)) return;
            it = producers_.emplace(producerId, Producer{}).first;
            it->second.partial = partial;
        }
        Producer& p = it->second;
        const Batch b{baseOffset, lastOffset, segBase, pos};
        if (p.ring.size() < depth_) {
            if (!charge(sizeof(Batch), producerId)) return;
            p.ring.push_back(b);
        } else {
            p.ring[p.head] = b;
            p.head = (p.head + 1) % depth_;
        }
        p.lastAppendMs = appendMs;
    }
    /// End of the pre-pass: drops the expired producers and reads the keys of the batches kept.  `readKeys(segBase, pos, keys)`
    /// appends the keyIndexWord of every 1..8-byte key of the batch at byte `pos` of segment `segBase` (called in offset order) and returns false if it
    /// cannot read it (the window then overflows).
    template <class ReadKeys>
    void finish(ReadKeys&& readKeys) {
        bool unrecorded = false;   // a live producer taken back short of `depth` batches
        for (const auto& [pid, p] : producers_) {
            if (expired(p.lastAppendMs)) continue;
            if (p.partial && p.ring.size() < depth_) { unrecorded = true; break; }
            batches_.insert(batches_.end(), p.ring.begin(), p.ring.end());
        }
        if (unrecorded) {
            overflow();
            return;
        }
        producers_.clear();
        evicted_.clear();
        std::sort(batches_.begin(), batches_.end(), [](const Batch& a, const Batch& b) { return a.base < b.base; });
        bytes_ = batches_.size() * sizeof(Batch);
        for (const Batch& b : batches_) {
            if (!readKeys(b.segBase, b.pos, keys_) || bytes() > budget_) {
                overflow();
                return;
            }
        }
        std::sort(keys_.begin(), keys_.end());
        keys_.erase(std::unique(keys_.begin(), keys_.end()), keys_.end());
    }

    // ---- queries ------------------------------------------------------------
    /// Is the data batch starting at `baseOffset` kept whole?
    [[nodiscard]] bool protects(int64_t baseOffset) const noexcept {
        if (overflowed_) return true;
        const auto it = std::lower_bound(batches_.begin(), batches_.end(), baseOffset, [](const Batch& b, int64_t o) { return b.base < o; });
        return it != batches_.end() && it->base == baseOffset;
    }
    /// Does a protected batch hold `offset`?  (A verdict marker listing it must stay.)
    [[nodiscard]] bool protectsOffset(int64_t offset) const noexcept {
        if (overflowed_) return true;
        auto it = std::upper_bound(batches_.begin(), batches_.end(), offset, [](int64_t o, const Batch& b) { return o < b.base; });
        return it != batches_.begin() && offset <= (--it)->last;
    }
    /// Must the tombstone of `key` outlive its delete horizon (a protected batch holds the key)?
    [[nodiscard]] bool pinsTombstone(std::string_view key) const noexcept {
        if (overflowed_) return true;
        if (key.empty() || key.size() > sizeof(uint64_t)) return false;
        return std::binary_search(keys_.begin(), keys_.end(), keyIndexWord(key));
    }
    /// The watermark bound: the base of the oldest protected batch (max when none; min when overflowed).
    [[nodiscard]] int64_t lowestProtectedBase() const noexcept {
        if (overflowed_) return std::numeric_limits<int64_t>::min();
        return batches_.empty() ? std::numeric_limits<int64_t>::max() : batches_.front().base;
    }
    [[nodiscard]] bool overflowed() const noexcept { return overflowed_; }
    /// The bytes charged against the budget so far.
    [[nodiscard]] std::size_t bytes() const noexcept { return bytes_ + keys_.size() * sizeof(uint64_t); }

private:
    struct Batch { int64_t base, last, segBase; std::size_t pos; };
    struct Producer { std::vector<Batch> ring; std::size_t head = 0; int64_t lastAppendMs = 0; bool partial = false; };

    bool expired(int64_t appendMs) const noexcept {
        return nowMs_ >= 0 && expirationMs_ >= 0 && toEpochMs(appendMs) < nowMs_ - expirationMs_;
    }
    /// Charges `n` bytes for `producerId`, evicting the other expired producers first when they do not fit.
    bool charge(std::size_t n, int64_t producerId) {
        if (bytes() + n > budget_) {
            for (auto it = producers_.begin(); it != producers_.end();) {
                if (it->first == producerId || !expired(it->second.lastAppendMs)) { ++it; continue; }
                bytes_ -= kBytesPerProducer + it->second.ring.size() * sizeof(Batch) - sizeof(int64_t);
                evicted_.push_back(it->first);
                it = producers_.erase(it);
            }
            std::sort(evicted_.begin(), evicted_.end());
        }
        if (bytes() + n > budget_) { overflow(); return false; }
        bytes_ += n;
        return true;
    }
    void overflow() {
        clear();
        overflowed_ = true;
    }

    std::size_t depth_;
    std::size_t budget_ = std::numeric_limits<std::size_t>::max();
    int64_t nowMs_ = -1;
    int64_t expirationMs_ = -1;
    std::unordered_map<int64_t, Producer> producers_;
    std::vector<int64_t> evicted_;                       // the expired producers charge() dropped, ascending
    std::vector<Batch> batches_;                         // the protected batches, ascending base (finish)
    std::vector<uint64_t> keys_;                         // their keys, ascending (finish)
    std::size_t bytes_ = 0;
    bool overflowed_ = false;
};

}  // namespace rune::storage
