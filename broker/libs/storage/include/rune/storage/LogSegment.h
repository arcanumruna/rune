#pragma once
#include <rune/storage/OffsetIndex.h>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
namespace rune::storage {
class LogSegment {
public:
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096);
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); // returns nextOffset
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t recover();
    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
private:
    static std::string fname(std::string dir, int64_t base, const char* ext);
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    std::string logPath_; std::fstream log_;
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
};
} // namespace rune::storage
