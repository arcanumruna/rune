#pragma once
#include <cstdint>
#include <map>
namespace rune::storage {
class LeaderEpochCache {
public:
    void assign(int32_t epoch, int64_t startOffset) { epochs_[epoch] = startOffset; }
    int64_t startOffsetFor(int32_t epoch) const { auto it = epochs_.find(epoch); return it==epochs_.end()? -1 : it->second; }
    int32_t latestEpoch() const { return epochs_.empty()? -1 : epochs_.rbegin()->first; }
    int64_t truncateOffsetForEpoch(int32_t leaderEpoch) const {
        auto it = epochs_.upper_bound(leaderEpoch);
        return it == epochs_.end() ? -1 : it->second;
    }
private:
    std::map<int32_t,int64_t> epochs_;
};
} // namespace rune::storage
