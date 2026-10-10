// ViewSnapshotIoAgent.cpp — Task 13.
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/PortableFlushBackend.h> // detail::platformFdatasync (fdatasync; fsync on macOS)
#include <rune/view/ViewCheckpoint.h>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <spdlog/spdlog.h>

namespace rune::node {

namespace {

// ---------------------------------------------------------------------------
// Little-endian helpers (mirrors ViewCheckpoint.cpp's private helpers -- this
// agent never links against them; it only PEEKS the O field out of a Task-12
// delta record's header, it never applies/validates the record itself).
// ---------------------------------------------------------------------------
inline uint32_t readLE32(const uint8_t* p) noexcept {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}
inline uint64_t readLE64(const uint8_t* p) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (i * 8);
    return v;
}
inline void writeLE32(uint8_t* p, uint32_t v) noexcept {
    for (int i = 0; i < 4; ++i) p[i] = static_cast<uint8_t>(v >> (i * 8));
}
inline void writeLE64(uint8_t* p, uint64_t v) noexcept {
    for (int i = 0; i < 8; ++i) p[i] = static_cast<uint8_t>(v >> (i * 8));
}

constexpr std::size_t kViewHeaderLen = 4 + 8 + 4 + 4; // magic,O,rowLen,count (rune::view::ViewCheckpoint.h)
constexpr std::size_t kViewCrcLen    = 4;

// Peek the O field out of a Task-12 delta/base record's header. This does
// NOT validate the record's CRC or per-entry bounds -- this agent only
// appends raw bytes to the per-(view,partition) delta file; the format's own
// CRC + bounds checks (rune::view::applyDelta) are what prove the bytes are
// intact, and are performed later by whichever recovery path reopens and
// replays the file. A frame too short to hold the header, or whose magic
// does not match, is malformed: returns -1 (dropped by the caller) rather
// than ever being misread as a real offset.
int64_t peekDeltaOffset(const uint8_t* p, std::size_t len) noexcept {
    if (len < kViewHeaderLen + kViewCrcLen) return -1;
    if (readLE32(p) != rune::view::kViewCheckpointMagic) return -1;
    return static_cast<int64_t>(readLE64(p + 4));
}

} // namespace

ViewSnapshotIoAgent::ViewSnapshotIoAgent(ViewSnapshotChannel& channel,
                                         std::string viewDataDir,
                                         int64_t fsyncMs)
    : channel_(channel)
    , viewDataDir_(std::move(viewDataDir))
    , fsyncMs_(fsyncMs)
{
    std::error_code ec;
    std::filesystem::create_directories(viewDataDir_, ec);
    if (ec) {
        spdlog::warn("ViewSnapshotIoAgent: failed to create view data dir {}: {}",
                     viewDataDir_, ec.message());
    }
}

ViewSnapshotIoAgent::~ViewSnapshotIoAgent() {
    for (auto& [key, fs] : files_) {
        if (fs.fd >= 0) ::close(fs.fd);
    }
}

std::string ViewSnapshotIoAgent::deltaFilePath_(const Key& key) const {
    return viewDataDir_ + "/view-" + std::to_string(static_cast<uint32_t>(key.viewTopicId)) +
           "-p" + std::to_string(key.partition) + ".delta";
}

std::string ViewSnapshotIoAgent::deltaFilePathForTest(int32_t viewTopicId, int32_t partition) const {
    return deltaFilePath_(Key{viewTopicId, partition});
}

ViewSnapshotIoAgent::FileState& ViewSnapshotIoAgent::fileFor_(const Key& key) {
    auto it = files_.find(key);
    if (it != files_.end()) return it->second;

    FileState fs;
    const std::string path = deltaFilePath_(key);
    fs.fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fs.fd < 0) {
        spdlog::warn("ViewSnapshotIoAgent: failed to open delta file {} (viewTopicId={}, partition={}): {}",
                     path, key.viewTopicId, key.partition, std::strerror(errno));
    }
    auto [ins, inserted] = files_.emplace(key, fs);
    (void)inserted;
    return ins->second;
}

void ViewSnapshotIoAgent::handleDeltaFrame_(const uint8_t* p, std::size_t len) {
    if (len < ViewSnapshotChannel::kDeltaHeaderBytes) return; // malformed frame -- drop

    const int32_t viewTopicId = static_cast<int32_t>(readLE32(p));
    const int32_t partition   = static_cast<int32_t>(readLE32(p + 4));
    const uint8_t* deltaBytes  = p + ViewSnapshotChannel::kDeltaHeaderBytes;
    const std::size_t deltaLen = len - ViewSnapshotChannel::kDeltaHeaderBytes;

    const int64_t O = peekDeltaOffset(deltaBytes, deltaLen);
    if (O < 0) return; // malformed/truncated Task-12 record -- drop, never corrupt the file

    Key key{viewTopicId, partition};
    FileState& fs = fileFor_(key);
    if (fs.fd < 0) return; // open failed (already logged) -- drop

    // appendNoSync-style: a plain write(), no fsync here -- fsyncs are
    // GROUPED in flushDue_ (offset-atomic: see file header).
    const ssize_t written = ::write(fs.fd, deltaBytes, deltaLen);
    if (written < 0 || static_cast<std::size_t>(written) != deltaLen) {
        spdlog::warn("ViewSnapshotIoAgent: short/failed write for viewTopicId={} partition={}: {}",
                     viewTopicId, partition, std::strerror(errno));
        return; // do not advance pendingO on a torn write
    }

    // Deltas for a given (view,partition) arrive in increasing O order (the
    // worker drains its own ViewTable dirty set in order), so a plain
    // overwrite -- not a max() -- is correct and matches the file's
    // append-only ordering.
    fs.pendingO       = O;
    fs.dirtySinceSync = true;
}

void ViewSnapshotIoAgent::postDone_(int32_t viewTopicId, int32_t partition, int64_t O) {
    uint8_t buf[ViewSnapshotChannel::kDoneMsgBytes];
    writeLE32(buf, static_cast<uint32_t>(viewTopicId));
    writeLE32(buf + 4, static_cast<uint32_t>(partition));
    writeLE64(buf + 8, static_cast<uint64_t>(O));
    if (!channel_.done.write(buf, sizeof(buf), ViewSnapshotChannel::kDoneMsgType)) {
        // Ring momentarily full -- retry next cycle (never dropped). The
        // in-process lastDurableOffset() is already correct regardless of
        // whether this notification lands; see file header.
        pendingAcks_.push_back(PendingAck{viewTopicId, partition, O});
    }
}

void ViewSnapshotIoAgent::drainPendingAcks_() {
    if (pendingAcks_.empty()) return;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < pendingAcks_.size(); ++i) {
        const auto& ack = pendingAcks_[i];
        uint8_t buf[ViewSnapshotChannel::kDoneMsgBytes];
        writeLE32(buf, static_cast<uint32_t>(ack.viewTopicId));
        writeLE32(buf + 4, static_cast<uint32_t>(ack.partition));
        writeLE64(buf + 8, static_cast<uint64_t>(ack.O));
        if (!channel_.done.write(buf, sizeof(buf), ViewSnapshotChannel::kDoneMsgType)) {
            if (keep != i) pendingAcks_[keep] = pendingAcks_[i];
            ++keep;
        }
    }
    pendingAcks_.resize(keep);
}

void ViewSnapshotIoAgent::flushDue_(int64_t nowNs, bool idle) {
    bool anyDirty = false;
    for (auto& [key, fs] : files_) {
        if (fs.dirtySinceSync) { anyDirty = true; break; }
    }
    if (!anyDirty) return;

    // lastFsyncNs_ starts at 0 (epoch-relative to whatever nowNs clock the
    // caller uses) so the very first-ever flush is gated the same way every
    // later one is: due once fsyncMs has elapsed since the last sync (or
    // since construction, for the first one) -- no special-cased override.
    const bool timeDue = (nowNs - lastFsyncNs_) >= fsyncMs_ * 1'000'000LL;
    // "or on drain-empty with pending bytes": an idle cycle (nothing new
    // drained) with dirty bytes still pending always gets one grouped
    // fsync pass, regardless of the timer -- this is what lets the
    // AppendsDeltaAndGroupsFsync test converge within its hard iteration
    // cap instead of spinning until the wall-clock window elapses.
    if (!timeDue && !idle) return;

    for (auto& [key, fs] : files_) {
        if (!fs.dirtySinceSync) continue;
        // OFFSET-ATOMIC: fdatasync FIRST, advance lastDurableO only after it
        // returns -- a crash between append() and this point leaves
        // lastDurableOffset() at the PRIOR fsync'd O, never the pending one.
        if (rune::node::detail::platformFdatasync(fs.fd) != 0) {
            spdlog::warn("ViewSnapshotIoAgent: fdatasync failed for viewTopicId={} partition={}: {}",
                         key.viewTopicId, key.partition, std::strerror(errno));
            continue; // leave dirty -- retried next cycle, lastDurableO unchanged
        }
        fs.lastDurableO    = fs.pendingO;
        fs.dirtySinceSync  = false;
        postDone_(key.viewTopicId, key.partition, fs.lastDurableO);
    }
    lastFsyncNs_ = nowNs;
}

int ViewSnapshotIoAgent::doWork(int64_t nowNs) {
    drainPendingAcks_();

    int drained = channel_.deltas.drain(
        [this](int32_t type, aeron::concurrent::AtomicBuffer& buf,
               aeron::util::index_t off, aeron::util::index_t len) {
            if (type != ViewSnapshotChannel::kDeltaMsgType) return;
            handleDeltaFrame_(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                              static_cast<std::size_t>(len));
        },
        /*limit=*/64);

    flushDue_(nowNs, /*idle=*/drained == 0);
    drainPendingAcks_();

    return drained;
}

int64_t ViewSnapshotIoAgent::lastDurableOffset(int32_t viewTopicId, int32_t partition) const noexcept {
    auto it = files_.find(Key{viewTopicId, partition});
    return it == files_.end() ? -1 : it->second.lastDurableO;
}

} // namespace rune::node
