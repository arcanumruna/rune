#pragma once
#include <rune/storage/LogSegment.h>
#include <map>
#include <memory>
#include <vector>
namespace rune::storage {
class Log {
public:
    Log(std::string dir, int64_t segmentRollBytes = 64LL*1024*1024);
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset);
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t logEndOffset() const noexcept { return logEndOffset_; }
    int64_t recover();
    std::vector<int64_t> segmentBaseOffsets() const;
private:
    LogSegment& activeSegment() { return *segments_.rbegin()->second; }
    void maybeRoll();
    std::string dir_; int64_t rollBytes_; int64_t logEndOffset_ = 0;
    std::map<int64_t, std::unique_ptr<LogSegment>> segments_;
};
} // namespace rune::storage
