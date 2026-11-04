#pragma once
// Tests only (#839): flip one byte inside the 2nd batch of the Nth segment file of a log directory, so that the segment
// recovers/reloads as DAMAGED (a CRC failure behind a valid first batch).  The caller then makes the log re-detect it
// (reload / reopen).  The position is computed from the file (the first batch's totalLength), never hardcoded.
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

namespace rune::storage::testing {

/// @returns the first offset of the damaged batch (>= 0), or -1 when the segment has fewer than two whole batches.
inline int64_t damageSecondBatchOfSegment(const std::string& logDir, std::size_t segmentIndex) {
    namespace fs = std::filesystem;
    std::vector<fs::path> files;
    for (const auto& e : fs::directory_iterator(logDir))
        if (e.path().extension() == ".log") files.push_back(e.path());
    std::sort(files.begin(), files.end());
    if (segmentIndex >= files.size()) return -1;
    const fs::path& f = files[segmentIndex];
    std::ifstream in(f, std::ios::binary);
    std::vector<uint8_t> buf((std::istreambuf_iterator<char>(in)), {});
    in.close();
    const auto overhead = static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    if (buf.size() < overhead) return -1;
    const std::size_t second = static_cast<std::size_t>(rune::protocol::RecordBatchReader(buf.data(), buf.size()).totalLength());
    if (second + overhead >= buf.size()) return -1;
    const int64_t base = static_cast<int64_t>(
        rune::protocol::RecordBatchReader(buf.data() + second, buf.size() - second).baseOffset());
    std::fstream out(f, std::ios::in | std::ios::out | std::ios::binary);
    const std::size_t at = second + overhead;   // the batch's first record byte (CRC-covered)
    const char c = static_cast<char>(buf[at] ^ 0x5A);
    out.seekp(static_cast<std::streamoff>(at));
    out.write(&c, 1);
    return base;
}

} // namespace rune::storage::testing
