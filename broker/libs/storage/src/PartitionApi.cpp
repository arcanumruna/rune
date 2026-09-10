#include <rune/storage/PartitionApi.h>
#include <rune/protocol/RecordBatch.h>
namespace rune::storage {
PartitionApi::PartitionApi(std::string dir) : log_(std::move(dir)) { log_.recover(); }

int64_t PartitionApi::produce(std::string_view key, std::string_view value, int64_t timestamp) {
    int64_t baseOffset = log_.logEndOffset();
    rune::protocol::RecordBatchBuilder b(baseOffset, timestamp);
    b.append(key, value, timestamp);
    return log_.append(b.build(), baseOffset); // single record -> lastOffset == baseOffset
}

std::vector<ConsumedRecord> PartitionApi::consume(int64_t fromOffset, int32_t maxBytes) {
    std::vector<ConsumedRecord> out;
    int64_t cursor = fromOffset;
    int64_t hw = highWatermark();
    while (cursor < hw) {
        auto bytes = log_.read(cursor, maxBytes);
        if (bytes.empty()) break;
        size_t pos = 0;
        bool advanced = false;
        while (pos < bytes.size()) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            if (!r.ensureValid()) return out;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
                if (off >= fromOffset && off < hw) out.push_back({std::string(k), std::string(v), off, ts});
            });
            cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += r.totalLength();
            advanced = true;
        }
        if (!advanced) break;
    }
    return out;
}
} // namespace rune::storage
