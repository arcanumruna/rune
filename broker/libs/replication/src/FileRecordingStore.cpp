// ---------------------------------------------------------------------------
// FileRecordingStore.cpp — File-backed RecordingStore implementation.
//
// Frame format (little-endian):
//   [uint64_t length][int64_t baseOffset][uint8_t bytes[length]]
//
// Recovery: on open, scan the whole file, skipping any incomplete trailing
// frame (torn tail).  Physical truncation restores a clean tail.
// ---------------------------------------------------------------------------

#include <rune/replication/FileRecordingStore.h>
#include <rune/protocol/RecordBatch.h>

#include <cassert>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <system_error>

namespace rune::replication {

// ---------------------------------------------------------------------------
// Little-endian helpers
// ---------------------------------------------------------------------------
static uint64_t readLE64u(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}

static int64_t readLE64s(const uint8_t* p) {
    return static_cast<int64_t>(readLE64u(p));
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

FileRecordingStore::FileRecordingStore(const std::string& path)
    : path_(path)
{
    namespace fs = std::filesystem;

    bool exists = fs::exists(path_);

    // Open in read+write+binary; create if not exists.
    if (exists) {
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    } else {
        // Create empty file, then reopen r+w.
        { std::ofstream create(path_, std::ios::binary); }
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!file_.is_open()) {
        throw std::runtime_error("FileRecordingStore: cannot open file: " + path_);
    }

    if (exists) {
        int64_t cleanEnd = recoverFromFile();
        // Truncate file to cleanEnd (remove any torn tail).
        file_.close();
        // Use filesystem::resize_file for truncation.
        std::error_code ec;
        fs::resize_file(path_, static_cast<uintmax_t>(cleanEnd), ec);
        if (ec) {
            throw std::runtime_error("FileRecordingStore: truncate failed: " + ec.message());
        }
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            throw std::runtime_error("FileRecordingStore: reopen after truncate failed: " + path_);
        }
    }

    // Seek write pointer to end of file.
    file_.seekp(0, std::ios::end);
}

// ---------------------------------------------------------------------------
// recoverFromFile — scan frames, return byte offset of end of last complete
// frame, and populate batchCount_ + recordingPosition_.
//
// We READ the payload bytes (not seekg past them) so that a partial payload
// is detected deterministically via gcount() < length.  seekg past end-of-file
// has implementation-defined behavior on some platforms.
// ---------------------------------------------------------------------------
int64_t FileRecordingStore::recoverFromFile()
{
    batchCount_        = 0;
    recordingPosition_ = 0;

    file_.seekg(0, std::ios::beg);
    file_.clear();

    int64_t lastCompleteEnd = 0;

    // Reusable read buffer.
    std::vector<uint8_t> payload;

    while (true) {
        // Record position before reading the header.
        std::streamoff headerPos = file_.tellg();
        if (headerPos < 0) {
            break;
        }

        // Try to read header (16 bytes).
        uint8_t hdr[kHeaderSize];
        file_.read(reinterpret_cast<char*>(hdr), static_cast<std::streamsize>(kHeaderSize));
        std::streamsize got = file_.gcount();
        if (got < static_cast<std::streamsize>(kHeaderSize)) {
            // Partial or no header — torn tail; stop.
            file_.clear();
            break;
        }

        uint64_t length     = readLE64u(hdr);
        int64_t  baseOffset = readLE64s(hdr + 8);

        // Sanity: length must be > 0 and reasonable (<= 64 MiB per batch).
        if (length == 0 || length > 64ULL * 1024 * 1024) {
            // Corrupted or torn — stop.
            file_.clear();
            break;
        }

        // Read exactly `length` payload bytes to detect torn payloads.
        payload.resize(length);
        file_.read(reinterpret_cast<char*>(payload.data()),
                   static_cast<std::streamsize>(length));
        std::streamsize payloadGot = file_.gcount();
        if (static_cast<uint64_t>(payloadGot) < length) {
            // Torn payload — this frame is incomplete; discard it.
            file_.clear();
            break;
        }

        // This frame is complete.  Record the byte position after this frame.
        std::streamoff afterFrame = file_.tellg();
        if (afterFrame < 0) {
            // eof immediately after reading — this is still a valid frame.
            // Compute position from headerPos + kHeaderSize + length.
            afterFrame = headerPos + static_cast<std::streamoff>(kHeaderSize + length);
        }
        lastCompleteEnd = static_cast<int64_t>(afterFrame);
        batchCount_     += 1;

        // Decode lastOffsetDelta from the batch header so multi-record batches
        // advance recordingPosition_ to baseOffset + lastOffsetDelta + 1.
        int32_t lastOffsetDelta = 0;
        if (length >= static_cast<uint64_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            rune::protocol::RecordBatchReader r(payload.data(), payload.size());
            lastOffsetDelta = r.lastOffsetDelta();
        }
        recordingPosition_ = baseOffset + lastOffsetDelta + 1;  // next expected offset (exclusive)

        // If eof was reached after reading the payload, we're done.
        if (file_.eof()) {
            file_.clear();
            break;
        }
    }

    return lastCompleteEnd;
}

// ---------------------------------------------------------------------------
// writeLE64 / writeLE64s helpers
// ---------------------------------------------------------------------------

void FileRecordingStore::writeLE64(uint64_t v)
{
    uint8_t buf[8];
    for (int i = 0; i < 8; ++i)
        buf[i] = static_cast<uint8_t>((v >> (8 * i)) & 0xFF);
    file_.write(reinterpret_cast<const char*>(buf), 8);
}

void FileRecordingStore::writeLE64s(int64_t v)
{
    writeLE64(static_cast<uint64_t>(v));
}

// ---------------------------------------------------------------------------
// append
// ---------------------------------------------------------------------------

void FileRecordingStore::append(int64_t baseOffset,
                                std::span<const uint8_t> bytes)
{
    assert(file_.is_open());

    // Write: [uint64_t length][int64_t baseOffset][bytes]
    writeLE64(static_cast<uint64_t>(bytes.size()));
    writeLE64s(baseOffset);
    file_.write(reinterpret_cast<const char*>(bytes.data()),
                static_cast<std::streamsize>(bytes.size()));
    file_.flush();

    batchCount_ += 1;

    // Decode lastOffsetDelta from the RecordBatch header so multi-record batches
    // advance recordingPosition_ to baseOffset + lastOffsetDelta + 1 (exclusive).
    // For single-record batches lastOffsetDelta == 0, giving baseOffset + 1.
    int32_t lastOffsetDelta = 0;
    if (bytes.size() >= static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
        rune::protocol::RecordBatchReader r(bytes.data(), bytes.size());
        lastOffsetDelta = r.lastOffsetDelta();
    }
    recordingPosition_ = baseOffset + lastOffsetDelta + 1;
}

// ---------------------------------------------------------------------------
// scan
// ---------------------------------------------------------------------------

void FileRecordingStore::scan(int64_t fromOffset, int64_t toOffset,
                               const BatchSink& sink) const
{
    // Seek to beginning and read all frames in order.
    file_.seekg(0, std::ios::beg);
    file_.clear();

    std::vector<uint8_t> payload;

    while (true) {
        uint8_t hdr[kHeaderSize];
        file_.read(reinterpret_cast<char*>(hdr), static_cast<std::streamsize>(kHeaderSize));
        if (file_.gcount() < static_cast<std::streamsize>(kHeaderSize)) {
            break;
        }

        uint64_t length     = readLE64u(hdr);
        int64_t  baseOffset = readLE64s(hdr + 8);

        if (length == 0 || length > 64ULL * 1024 * 1024) {
            break;
        }

        payload.resize(length);
        file_.read(reinterpret_cast<char*>(payload.data()),
                   static_cast<std::streamsize>(length));
        if (static_cast<uint64_t>(file_.gcount()) < length) {
            break;  // torn payload
        }

        // Filter: skip if baseOffset not in [fromOffset, toOffset).
        if (baseOffset >= fromOffset && baseOffset < toOffset) {
            bool cont = sink(baseOffset, std::span<const uint8_t>(payload.data(), length));
            if (!cont) {
                break;
            }
        }

        // If we've passed toOffset, no point continuing.
        if (baseOffset >= toOffset) {
            break;
        }
    }

    file_.clear();
}

} // namespace rune::replication
