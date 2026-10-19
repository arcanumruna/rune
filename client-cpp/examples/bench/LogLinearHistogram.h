// examples/bench/LogLinearHistogram.h
//
// Compact HdrHistogram-style log-linear latency histogram (ns).  Values < 256 are
// exact; above, each power of two is split into 128 linear sub-buckets (relative
// error <= 1/128 = 0.78%) up to 2^43 ns (~2.4 h).  record() is O(1) and never
// allocates (fixed std::array); larger values clamp into the last bucket while
// max() stays exact.  Spec §5: every record is recorded, no sampling.
#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace rune::bench {

class LogLinearHistogram {
public:
    static constexpr int kSubBits = 8;   // first 256 values exact
    static constexpr int kMaxExp  = 42;  // last octave [2^42, 2^43)
    static constexpr int kHalf    = 1 << (kSubBits - 1);  // 128 sub-buckets per octave
    static constexpr int kBuckets = (1 << kSubBits) + (kMaxExp - kSubBits + 1) * kHalf;  // 4736

    void record(int64_t valueNs) noexcept {
        const uint64_t v = valueNs < 0 ? 0 : static_cast<uint64_t>(valueNs);
        ++counts_[static_cast<std::size_t>(index(v))];
        ++total_;
        if (v > max_) max_ = v;
        if (v < min_) min_ = v;
    }

    [[nodiscard]] uint64_t count() const noexcept { return total_; }
    [[nodiscard]] uint64_t max() const noexcept { return total_ != 0 ? max_ : 0; }
    [[nodiscard]] uint64_t min() const noexcept { return total_ != 0 ? min_ : 0; }

    /// Upper bound of the bucket holding the ceil(q * count)-th smallest value
    /// (HdrHistogram "highest equivalent value": conservative for tails), capped at max().
    [[nodiscard]] uint64_t valueAtQuantile(double q) const noexcept {
        if (total_ == 0) return 0;
        const double c = std::clamp(q, 0.0, 1.0);
        uint64_t rank = static_cast<uint64_t>(std::ceil(c * static_cast<double>(total_)));
        if (rank == 0) rank = 1;
        uint64_t seen = 0;
        for (int i = 0; i < kBuckets; ++i) {
            seen += counts_[static_cast<std::size_t>(i)];
            if (seen >= rank) {
                return i == kBuckets - 1 ? max_ : std::min<uint64_t>(upperBound(i), max_);
            }
        }
        return max_;
    }

    void merge(const LogLinearHistogram& o) noexcept {
        for (int i = 0; i < kBuckets; ++i) {
            counts_[static_cast<std::size_t>(i)] += o.counts_[static_cast<std::size_t>(i)];
        }
        total_ += o.total_;
        if (o.total_ != 0) {
            max_ = std::max(max_, o.max_);
            min_ = std::min(min_, o.min_);
        }
    }

    void reset() noexcept {
        counts_.fill(0);
        total_ = 0;
        max_   = 0;
        min_   = UINT64_MAX;
    }

    static constexpr int index(uint64_t v) noexcept {
        if (v < (uint64_t{1} << kSubBits)) return static_cast<int>(v);
        const int e = 63 - std::countl_zero(v);  // floor(log2 v) >= kSubBits
        if (e > kMaxExp) return kBuckets - 1;
        const int shift = e - (kSubBits - 1);     // keep kSubBits significant bits
        const int sub   = static_cast<int>(v >> shift);  // [128, 256)
        return (1 << kSubBits) + (e - kSubBits) * kHalf + (sub - kHalf);
    }

    static constexpr uint64_t upperBound(int i) noexcept {
        if (i < (1 << kSubBits)) return static_cast<uint64_t>(i);
        const int j     = i - (1 << kSubBits);
        const int e     = kSubBits + j / kHalf;
        const int sub   = kHalf + j % kHalf;
        const int shift = e - (kSubBits - 1);
        return (static_cast<uint64_t>(sub + 1) << shift) - 1;
    }

private:
    std::array<uint64_t, kBuckets> counts_{};
    uint64_t total_ = 0;
    uint64_t max_   = 0;
    uint64_t min_   = UINT64_MAX;
};

static_assert(LogLinearHistogram::kBuckets == 4736);
static_assert(LogLinearHistogram::index(LogLinearHistogram::upperBound(LogLinearHistogram::kBuckets - 1)) ==
              LogLinearHistogram::kBuckets - 1);

} // namespace rune::bench
