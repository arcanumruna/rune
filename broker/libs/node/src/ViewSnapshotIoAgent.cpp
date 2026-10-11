// ViewSnapshotIoAgent.cpp — Task 13.
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/PortableFlushBackend.h> // detail::platformFdatasync (fdatasync; fsync on macOS)
#include <rune/view/ViewCheckpoint.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
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

constexpr std::size_t kViewHeaderLenV2 = 4 + 8 + 1; // magic,O,tableCount (multi-table v2)

// Peek the O field out of a delta/base record's header: a v1 (single-table,
// Replace views) or v2 (multi-table, aggregate views) record -- O is at byte 4
// in both. This does NOT validate the record's CRC or per-entry bounds -- this
// agent only appends raw bytes to the per-(view,partition) delta file; the
// format's own CRC + bounds checks (rune::view::applyDelta / applyMultiDelta)
// are what prove the bytes are intact, and are performed later by whichever
// recovery path reopens and replays the file. A frame too short to hold its
// version's header + CRC, or whose magic is neither v1 nor v2, is malformed:
// returns -1 (dropped by the caller) rather than ever being misread as a real
// offset.
int64_t peekDeltaOffset(const uint8_t* p, std::size_t len) noexcept {
    if (len < 4) return -1;
    const uint32_t magic = readLE32(p);
    if (magic == rune::view::kViewCheckpointMagic) {
        if (len < kViewHeaderLen + kViewCrcLen) return -1;
    } else if (magic == rune::view::kViewCheckpointMagicV2) {
        if (len < kViewHeaderLenV2 + kViewCrcLen) return -1;
    } else {
        return -1;
    }
    return static_cast<int64_t>(readLE64(p + 4));
}

// Review round 1 (IMPORTANT 2): loop ::write() until every byte lands, so
// the only way this returns false is a REAL I/O error -- EINTR and a
// legitimate short write (the kernel accepted fewer bytes than requested,
// no error) just resume from the new offset. Without this loop a short
// write would leave a torn/partial record on disk with no resync marker
// between records, permanently misaligning every later record's CRC on
// replay -- see handleDeltaFrame_'s FATAL handling of a `false` return.
bool writeFull(int fd, const uint8_t* data, std::size_t len) noexcept {
    std::size_t written = 0;
    while (written < len) {
        const ssize_t n = ::write(fd, data + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false; // real error
        }
        if (n == 0) return false; // defensive: no forward progress -- never spin
        written += static_cast<std::size_t>(n);
    }
    return true;
}

// Shared 16-byte [viewTopicId i32][partition i32][O i64] ack encode (fix-on-
// sight DRY: was duplicated in postDone_ and drainPendingAcks_).
void encodeAck(uint8_t* buf, int32_t viewTopicId, int32_t partition, int64_t O) noexcept {
    writeLE32(buf, static_cast<uint32_t>(viewTopicId));
    writeLE32(buf + 4, static_cast<uint32_t>(partition));
    writeLE64(buf + 8, static_cast<uint64_t>(O));
}

} // namespace

ViewSnapshotIoAgent::ViewSnapshotIoAgent(ViewSnapshotChannel& channel,
                                         std::string viewDataDir,
                                         int64_t fsyncMs)
    : ViewSnapshotIoAgent(std::vector<ViewSnapshotChannel*>{&channel},
                          std::move(viewDataDir), fsyncMs) {}

ViewSnapshotIoAgent::ViewSnapshotIoAgent(std::vector<ViewSnapshotChannel*> channels,
                                         std::string viewDataDir,
                                         int64_t fsyncMs)
    : channels_(std::move(channels))
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

ViewSnapshotIoAgent::FileState* ViewSnapshotIoAgent::fileFor_(const Key& key) {
    auto it = files_.find(key);
    if (it != files_.end()) return &it->second;

    const std::string path = deltaFilePath_(key);
    const int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (fd < 0) {
        // Review round 1 (IMPORTANT 1): do NOT emplace a FileState for a
        // failed open -- caching fd=-1 here would permanently blackhole
        // every future delta for this key behind one log line, even for a
        // purely transient failure (dir not yet created, fd-ulimit,
        // momentary ENOSPC/EACCES). Leaving no entry means the NEXT delta
        // for this key retries open() from scratch. openFailures_ makes a
        // PERSISTENT failure observable beyond this one log line.
        ++openFailures_;
        spdlog::warn("ViewSnapshotIoAgent: failed to open delta file {} (viewTopicId={}, partition={}): {} "
                     "-- not cached, will retry on the next delta for this key",
                     path, key.viewTopicId, key.partition, std::strerror(errno));
        return nullptr;
    }
    FileState fs;
    fs.fd = fd;
    auto [ins, inserted] = files_.emplace(key, fs);
    (void)inserted;
    return &ins->second;
}

void ViewSnapshotIoAgent::handleDeltaFrame_(const uint8_t* p, std::size_t len, int channelIdx) {
    if (len < ViewSnapshotChannel::kDeltaHeaderBytes) return; // malformed frame -- drop

    const int32_t viewTopicId = static_cast<int32_t>(readLE32(p));
    const int32_t partition   = static_cast<int32_t>(readLE32(p + 4));
    const uint8_t* deltaBytes  = p + ViewSnapshotChannel::kDeltaHeaderBytes;
    const std::size_t deltaLen = len - ViewSnapshotChannel::kDeltaHeaderBytes;

    const int64_t O = peekDeltaOffset(deltaBytes, deltaLen);
    if (O < 0) return; // malformed/truncated Task-12 record -- drop, never corrupt the file

    const Key key{viewTopicId, partition};

    // Review round 1 (IMPORTANT 2): once a file has hit a FATAL write error
    // it must NEVER accept another delta -- a torn/partial record with no
    // resync marker would otherwise misalign every later record's CRC on
    // replay. fileFor_() below would happily return the SAME (already open)
    // FileState for this key, so the errored check must come first.
    if (auto it = files_.find(key); it != files_.end() && it->second.errored) return;

    FileState* fs = fileFor_(key);
    if (fs == nullptr) return; // open failed this cycle (transient, NOT cached) -- drop, retried next delta
    fs->channelIdx = channelIdx;  // Task 19: route this key's done acks back to its channel

    // appendNoSync-style: a plain write(), no fsync here -- fsyncs are
    // GROUPED in flushDue_ (offset-atomic: see file header).
    if (!writeFull(fs->fd, deltaBytes, deltaLen)) {
        // FATAL: writeFull() only fails on a real I/O error (not EINTR, not
        // a legitimate short write -- those are retried internally), which
        // means the file may now hold a torn/partial record. Stop trusting
        // it forever: never advance pendingO/lastDurableO past this point,
        // and never accept another delta for this key (the `errored` check
        // above enforces that from here on).
        fs->errored = true;
        spdlog::error("ViewSnapshotIoAgent: FATAL write failure for viewTopicId={} partition={} at O={}: {} "
                      "-- this delta stream is now stopped (hasError() will report true)",
                      viewTopicId, partition, O, std::strerror(errno));
        return;
    }

    // Deltas for a given (view,partition) arrive in increasing O order (the
    // worker drains its own ViewTable dirty set in order); std::max is a
    // cheap defense against a latent offset-regression if that invariant is
    // ever violated (e.g. a redelivery) -- never silently moves pendingO
    // backward.
    fs->pendingO       = std::max(fs->pendingO, O);
    fs->dirtySinceSync = true;
}

void ViewSnapshotIoAgent::postDone_(int32_t viewTopicId, int32_t partition, int64_t O, int channelIdx) {
    uint8_t buf[ViewSnapshotChannel::kDoneMsgBytes];
    encodeAck(buf, viewTopicId, partition, O);
    auto* ch = (channelIdx >= 0 && channelIdx < static_cast<int>(channels_.size()))
                   ? channels_[channelIdx] : nullptr;
    if (!ch || !ch->done.write(buf, sizeof(buf), ViewSnapshotChannel::kDoneMsgType)) {
        // Ring momentarily full -- retry next cycle (never dropped). The
        // in-process lastDurableOffset() is already correct regardless of
        // whether this notification lands; see file header.
        pendingAcks_.push_back(PendingAck{viewTopicId, partition, O, channelIdx});
    }
}

void ViewSnapshotIoAgent::drainPendingAcks_() {
    if (pendingAcks_.empty()) return;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < pendingAcks_.size(); ++i) {
        const auto& ack = pendingAcks_[i];
        uint8_t buf[ViewSnapshotChannel::kDoneMsgBytes];
        encodeAck(buf, ack.viewTopicId, ack.partition, ack.O);
        auto* ch = (ack.channelIdx >= 0 && ack.channelIdx < static_cast<int>(channels_.size()))
                       ? channels_[ack.channelIdx] : nullptr;
        if (!ch || !ch->done.write(buf, sizeof(buf), ViewSnapshotChannel::kDoneMsgType)) {
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
        postDone_(key.viewTopicId, key.partition, fs.lastDurableO, fs.channelIdx);
    }
    lastFsyncNs_ = nowNs;
}

int ViewSnapshotIoAgent::doWork(int64_t nowNs) {
    drainPendingAcks_();

    int drained = 0;
    for (int ci = 0; ci < static_cast<int>(channels_.size()); ++ci) {
        ViewSnapshotChannel* ch = channels_[ci];
        if (!ch) continue;
        drained += ch->deltas.drain(
            [this, ci](int32_t type, aeron::concurrent::AtomicBuffer& buf,
                       aeron::util::index_t off, aeron::util::index_t len) {
                if (type != ViewSnapshotChannel::kDeltaMsgType) return;
                handleDeltaFrame_(reinterpret_cast<const uint8_t*>(buf.buffer() + off),
                                  static_cast<std::size_t>(len), ci);
            },
            /*limit=*/64);
    }

    flushDue_(nowNs, /*idle=*/drained == 0);
    drainPendingAcks_();

    return drained;
}

int64_t ViewSnapshotIoAgent::lastDurableOffset(int32_t viewTopicId, int32_t partition) const noexcept {
    auto it = files_.find(Key{viewTopicId, partition});
    return it == files_.end() ? -1 : it->second.lastDurableO;
}

bool ViewSnapshotIoAgent::hasError(int32_t viewTopicId, int32_t partition) const noexcept {
    auto it = files_.find(Key{viewTopicId, partition});
    return it != files_.end() && it->second.errored;
}

void ViewSnapshotIoAgent::forceWriteFailureForTest(int32_t viewTopicId, int32_t partition) {
    const Key key{viewTopicId, partition};
    FileState* fs = fileFor_(key);
    if (fs == nullptr) return; // open failed -- nothing to break (already "not durable")
    if (fs->fd >= 0) ::close(fs->fd);
    // Reopen read-only: any ::write() to this fd fails with EBADF -- a
    // portable, deterministic stand-in for a real short/failed write (no
    // reliance on an OS-specific partial-write race window).
    fs->fd = ::open(deltaFilePath_(key).c_str(), O_RDONLY);
}

} // namespace rune::node
