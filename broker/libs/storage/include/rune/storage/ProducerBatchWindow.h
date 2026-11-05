#pragma once
// ProducerBatchWindow.h - checked writes Part 06 (contract addition 9, P6-9): the last `depth` data batches of every idempotent
// producer in the range a cleaner pass scanned, on a partition whose producer state keeps the whole retry window (an
// autoincrement topic: depth kRetryWindowSize).
//
// A leader rebuilt from the log answers a retried key-0 batch with the ids its stored batch carries, and dedupes against the
// producer's last `depth` batches.  So the cleaner keeps those batches of a live producer WHOLE (protects): no record rule runs on
// them, a superseded or rejected record in one stays.  A superseded insert kept that way would come back once the tombstone of its
// key expired, so the tombstone of every key such a batch holds is pinned (pinsTombstone) while the batch is protected.  Keys are
// read as LE uint64 (the autoincrement key); a key of another width is not recorded.
//
// Expiry and the producer cap are ProducerTails': a producer whose last batch's maxTimestamp is older than nowMs - expirationMs
// is not protected (nowMs < 0 never expires).  Past the cap a new producer is not recorded and the window is OVERFLOWED: every
// unexpired batch of an unrecorded producer is protected and every tombstone pinned (conservative).
//
// Depth 1 (every other topic) builds no window: the cleaner then runs exactly as ProducerTails alone dictates.
// Cleaner thread only (cold; the cleaner is the one sanctioned heap user).
#include <rune/storage/ProducerTails.h>
#include <rune/storage/Timestamps.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rune::storage {

class ProducerBatchWindow {
public:
    explicit ProducerBatchWindow(int depth) : depth_(static_cast<std::size_t>(depth)) {}

    [[nodiscard]] int depth() const noexcept { return static_cast<int>(depth_); }
    void setCapacity(std::size_t capacity) noexcept { capacity_ = capacity; }

    void clear() noexcept {
        producers_.clear();
        ranges_.clear();
        pins_.clear();
        overflowed_ = false;
    }

    // ---- pre-pass feed (offset order) -------------------------------------
    /// An idempotent DATA batch of `producerId` over [baseOffset, lastOffset], batch maxTimestamp `maxTimestampMs`, holding `keys`.
    void noteBatch(int64_t producerId, int64_t baseOffset, int64_t lastOffset, int64_t maxTimestampMs, std::span<const uint64_t> keys) {
        auto it = producers_.find(producerId);
        if (it == producers_.end()) {
            if (producers_.size() >= capacity_) { overflowed_ = true; return; }
            it = producers_.emplace(producerId, Producer{}).first;
            it->second.ring.resize(depth_);
        }
        Producer& p = it->second;
        Batch& b = p.ring[p.head];
        b.base = baseOffset;
        b.last = lastOffset;
        b.keys.assign(keys.begin(), keys.end());
        p.head = (p.head + 1) % depth_;
        p.count = std::min(p.count + 1, depth_);
        p.lastMaxTs = maxTimestampMs;
    }
    /// End of the pre-pass: the protected ranges and the pinned keys, each with its producer's last maxTimestamp.
    void finish() {
        for (const auto& [pid, p] : producers_) {
            for (std::size_t i = 0; i < p.count; ++i) {
                const Batch& b = p.ring[(p.head + depth_ - p.count + i) % depth_];
                ranges_.push_back(Range{b.base, b.last, p.lastMaxTs});
                for (const uint64_t k : b.keys) pins_.push_back(Pin{k, p.lastMaxTs});
            }
        }
        std::sort(ranges_.begin(), ranges_.end(), [](const Range& a, const Range& b) { return a.base < b.base; });
        std::sort(pins_.begin(), pins_.end(), [](const Pin& a, const Pin& b) { return a.key != b.key ? a.key < b.key : a.lastMaxTs > b.lastMaxTs; });
    }

    // ---- queries ------------------------------------------------------------
    /// Is the data batch of `producerId` starting at `baseOffset` (maxTimestamp `maxTimestampMs`) kept whole?
    [[nodiscard]] bool protects(int64_t producerId, int64_t baseOffset, int64_t maxTimestampMs, int64_t nowMs, int64_t expirationMs) const noexcept {
        const auto it = producers_.find(producerId);
        if (it == producers_.end()) return overflowed_ && !expired(maxTimestampMs, nowMs, expirationMs);
        const Producer& p = it->second;
        if (expired(p.lastMaxTs, nowMs, expirationMs)) return false;
        for (std::size_t i = 0; i < p.count; ++i)
            if (p.ring[(p.head + depth_ - 1 - i) % depth_].base == baseOffset) return true;
        return false;
    }
    /// Does a protected batch hold `offset`?  (A verdict marker listing it must stay.)
    [[nodiscard]] bool protectsOffset(int64_t offset, int64_t nowMs, int64_t expirationMs) const noexcept {
        if (overflowed_) return true;
        auto it = std::upper_bound(ranges_.begin(), ranges_.end(), offset, [](int64_t o, const Range& r) { return o < r.base; });
        if (it == ranges_.begin()) return false;
        --it;
        return offset <= it->last && !expired(it->lastMaxTs, nowMs, expirationMs);
    }
    /// Must the tombstone of `key` outlive its delete horizon (a protected batch holds the key)?
    [[nodiscard]] bool pinsTombstone(std::string_view key, int64_t nowMs, int64_t expirationMs) const noexcept {
        if (overflowed_) return true;
        if (key.size() != sizeof(uint64_t)) return false;
        uint64_t k = 0;
        for (std::size_t i = 0; i < sizeof(uint64_t); ++i) k |= uint64_t{static_cast<uint8_t>(key[i])} << (8 * i);
        const auto it = std::lower_bound(pins_.begin(), pins_.end(), k, [](const Pin& p, uint64_t v) { return p.key < v; });
        return it != pins_.end() && it->key == k && !expired(it->lastMaxTs, nowMs, expirationMs);   // the first: the latest producer
    }
    [[nodiscard]] bool overflowed() const noexcept { return overflowed_; }

private:
    struct Batch { int64_t base = 0, last = 0; std::vector<uint64_t> keys; };
    struct Producer { std::vector<Batch> ring; std::size_t head = 0, count = 0; int64_t lastMaxTs = 0; };
    struct Range { int64_t base, last, lastMaxTs; };
    struct Pin { uint64_t key; int64_t lastMaxTs; };

    static bool expired(int64_t maxTimestampMs, int64_t nowMs, int64_t expirationMs) noexcept {
        return nowMs >= 0 && expirationMs >= 0 && toEpochMs(maxTimestampMs) < nowMs - expirationMs;
    }

    std::size_t depth_;
    std::size_t capacity_ = ProducerTails::kUnbounded;
    std::unordered_map<int64_t, Producer> producers_;
    std::vector<Range> ranges_;                         // ascending base
    std::vector<Pin> pins_;                             // ascending key, the latest producer first
    bool overflowed_ = false;
};

}  // namespace rune::storage
