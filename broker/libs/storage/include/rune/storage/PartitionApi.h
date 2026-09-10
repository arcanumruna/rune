#pragma once
#include <rune/storage/Log.h>
#include <string>
#include <string_view>
#include <vector>
namespace rune::storage {
struct ConsumedRecord { std::string key, value; int64_t offset; int64_t timestamp; };
class PartitionApi {
public:
    explicit PartitionApi(std::string dir);
    int64_t produce(std::string_view key, std::string_view value, int64_t timestamp);
    std::vector<ConsumedRecord> consume(int64_t fromOffset, int32_t maxBytes = 1 << 20);
    int64_t highWatermark() const { return log_.logEndOffset(); }
private:
    Log log_;
};
} // namespace rune::storage
