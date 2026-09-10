#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <cstdint>
#include <stdexcept>
#include <iomanip>
#include <sstream>
namespace rune::storage {
std::string LogSegment::fname(std::string dir, int64_t base, const char* ext) {
    std::ostringstream os; os << dir << "/" << std::setw(20) << std::setfill('0') << base << ext;
    return os.str();
}
LogSegment::LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes)
    : baseOffset_(baseOffset), indexIntervalBytes_(indexIntervalBytes), nextOffset_(baseOffset) {
    logPath_ = fname(dir, baseOffset, ".log");
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!log_.is_open()) { std::ofstream(logPath_, std::ios::binary).close();
        log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app); }
    index_ = std::make_unique<OffsetIndex>(fname(dir, baseOffset, ".index"), baseOffset, 8 * 1024 * 1024);
}
int64_t LogSegment::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    // Guard: batch must be at least the size of record-batch overhead
    if (batch.size() < static_cast<size_t>(rune::protocol::batch::kRecordBatchOverhead))
        throw std::runtime_error("rune: batch smaller than record-batch overhead");

    if (bytesSinceIndex_ == 0 || bytesSinceIndex_ >= indexIntervalBytes_) {
        rune::protocol::RecordBatchReader r(batch.data(), batch.size());
        // Guard: Kafka-style segments cap size so relative position fits in int32 (2 GiB max)
        if (logSize_ > static_cast<int64_t>(INT32_MAX))
            throw std::runtime_error("rune: segment exceeds 2GiB index-position limit");
        index_->append(r.baseOffset(), static_cast<int32_t>(logSize_));
        bytesSinceIndex_ = 0;
    }
    log_.seekp(0, std::ios::end);
    log_.write(reinterpret_cast<const char*>(batch.data()), static_cast<std::streamsize>(batch.size()));
    log_.flush();
    logSize_ += static_cast<int64_t>(batch.size());
    bytesSinceIndex_ += static_cast<int64_t>(batch.size());
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
}
std::vector<uint8_t> LogSegment::read(int64_t startOffset, int32_t maxBytes) {
    OffsetPosition op = index_->lookup(startOffset);
    log_.seekg(op.position, std::ios::beg);
    int64_t remaining = logSize_ - op.position;
    // Guard: clamp negative remaining (if index is out of sync) to prevent huge allocation
    if (remaining < 0) remaining = 0;
    int64_t toRead = std::min<int64_t>(remaining, maxBytes);
    std::vector<uint8_t> buf(static_cast<size_t>(toRead));
    if (toRead > 0) log_.read(reinterpret_cast<char*>(buf.data()), toRead);
    // Skip batches whose lastOffset < startOffset (index is sparse -> may start earlier).
    size_t pos = 0;
    while (pos < buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
        if (batchLast >= startOffset) { return std::vector<uint8_t>(buf.begin() + pos, buf.end()); }
        pos += r.totalLength();
    }
    return {};
}
} // namespace rune::storage
