// ViewSnapshotIoAgent.cpp — Task 13.
#include <rune/node/ViewSnapshotIoAgent.h>
#include <rune/node/PortableFlushBackend.h> // detail::platformFdatasync (fdatasync; fsync on macOS)
#include <rune/view/ViewCheckpoint.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <cstdio>
#include <cstdlib>
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
                                         int64_t fsyncMs,
                                         ViewBackupPolicy policy)
    : ViewSnapshotIoAgent(std::vector<ViewSnapshotChannel*>{&channel},
                          std::move(viewDataDir), fsyncMs, policy) {}

ViewSnapshotIoAgent::ViewSnapshotIoAgent(std::vector<ViewSnapshotChannel*> channels,
                                         std::string viewDataDir,
                                         int64_t fsyncMs,
                                         ViewBackupPolicy policy)
    : channels_(std::move(channels))
    , viewDataDir_(std::move(viewDataDir))
    , fsyncMs_(fsyncMs)
    , policy_(policy)
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
    return viewCheckpointFilePath(viewDataDir_, key.viewTopicId, key.partition);   // #537: the committed-fold file
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

namespace {
// Backup file names: view-<view>-p<part>.<10-digit seq>.<reason>.delta
std::string backupPrefix(int32_t view, int32_t part) {
    return "view-" + std::to_string(static_cast<uint32_t>(view)) + "-p" + std::to_string(part) + ".";
}
// seq of `name` when it is a backup of (view, part); false otherwise.
bool parseBackupName(const std::string& name, const std::string& prefix, uint32_t& seqOut) {
    if (name.size() < prefix.size() + 10 + 1 + 1 + 6 || name.compare(0, prefix.size(), prefix) != 0) return false;
    uint64_t seq = 0;
    for (std::size_t i = 0; i < 10; ++i) {
        const char c = name[prefix.size() + i];
        if (c < '0' || c > '9') return false;
        seq = seq * 10 + static_cast<uint64_t>(c - '0');
    }
    if (name[prefix.size() + 10] != '.') return false;
    static const char kSuffix[] = ".delta";
    if (name.size() < sizeof(kSuffix) - 1 || name.compare(name.size() - (sizeof(kSuffix) - 1), sizeof(kSuffix) - 1, kSuffix) != 0) return false;
    if (name.size() - (sizeof(kSuffix) - 1) <= prefix.size() + 11) return false;   // the reason tag must be non-empty
    if (seq > UINT32_MAX) return false;
    seqOut = static_cast<uint32_t>(seq);
    return true;
}
uint64_t seqKey(int32_t view, int32_t part) noexcept {
    return (static_cast<uint64_t>(static_cast<uint32_t>(view)) << 32) | static_cast<uint32_t>(part);
}
} // namespace

std::vector<std::string> listViewBackups(const std::string& viewDataDir, int32_t view, int32_t part) {
    std::vector<std::pair<uint32_t, std::string>> found;
    const std::string dir = viewBackupDir(viewDataDir);
    const std::string prefix = backupPrefix(view, part);
    std::error_code ec;
    for (std::filesystem::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string name = it->path().filename().string();
        uint32_t seq = 0;
        if (parseBackupName(name, prefix, seq)) found.emplace_back(seq, it->path().string());
    }
    std::sort(found.begin(), found.end());
    std::vector<std::string> out;
    out.reserve(found.size());
    for (auto& f : found) out.push_back(std::move(f.second));
    return out;
}

int ViewSnapshotIoAgent::fsyncDir_(const std::string& dir, const char* tag) {
    if (recordFsyncLog_) fsyncLog_.emplace_back(tag);
    const int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd < 0) return errno;
    int rc = 0;
    if (rune::node::detail::platformFdatasync(fd) != 0) rc = errno;
    ::close(fd);
    return rc;
}

uint32_t ViewSnapshotIoAgent::nextSeq_(int32_t view, int32_t part) {
    auto it = seqCache_.find(seqKey(view, part));
    if (it == seqCache_.end()) {
        uint32_t maxSeq = 0;
        const std::string prefix = backupPrefix(view, part);
        for (const std::string& path : listViewBackups(viewDataDir_, view, part)) {
            uint32_t seq = 0;
            if (parseBackupName(std::filesystem::path(path).filename().string(), prefix, seq)) maxSeq = std::max(maxSeq, seq);
        }
        it = seqCache_.emplace(seqKey(view, part), maxSeq).first;
    }
    return ++it->second;
}

int ViewSnapshotIoAgent::moveToBackup_(const std::string& path, int32_t view, int32_t part, ViewDiscardReason reason) {
    struct stat st{};
    if (::lstat(path.c_str(), &st) != 0) return errno == ENOENT ? 0 : errno;   // ENOENT: an idempotent replay after a crash
    // Nothing is lost by removing an empty file, and a backup of it would only spend a retention slot.
    if (!policy_.enabled || st.st_size == 0) {
        if (::unlink(path.c_str()) != 0 && errno != ENOENT) return errno;
        return fsyncDir_(viewDataDir_, "viewDir");
    }

    const std::string bdir = viewBackupDir(viewDataDir_);
    bool madeDir = false;
    if (::mkdir(bdir.c_str(), 0755) == 0) {
        madeDir = true;
    } else if (errno != EEXIST) {
        return errno;
    }
    struct stat bst{};
    if (::stat(bdir.c_str(), &bst) != 0) return errno;
    if (!S_ISDIR(bst.st_mode)) return ENOTDIR;
    if (madeDir) {
        if (const int e = fsyncDir_(viewDataDir_, "viewDir")) return e;
    }

    uint32_t seq = nextSeq_(view, part);
    std::string backup;
    char name[128];
    constexpr int kTries = 16;
    int err = EEXIST;
    for (int attempt = 0; attempt < kTries; ++attempt, ++seq) {
        std::snprintf(name, sizeof(name), "view-%u-p%d.%010u.%s.delta", static_cast<uint32_t>(view), part, seq, viewDiscardReasonName(reason));
        backup = bdir + "/" + name;
        if (::link(path.c_str(), backup.c_str()) == 0) { err = 0; break; }
        err = errno;
        if (err == EEXIST) continue;   // someone else's file at that seq: take the next one
        if (err == EPERM || err == ENOTSUP || err == EXDEV || err == EMLINK) {   // a filesystem without hard links: rename, never copy
            if (::access(backup.c_str(), F_OK) == -1 && errno == ENOENT) {
                if (::rename(path.c_str(), backup.c_str()) == 0) { err = 0; break; }
                err = errno;
            } else {
                err = EEXIST;
                continue;
            }
        }
        return err;
    }
    if (err != 0) return err;
    seqCache_[seqKey(view, part)] = seq;
    if (::unlink(path.c_str()) != 0 && errno != ENOENT) return errno;   // (after a rename fallback the file is already gone)

    if (const int e = fsyncDir_(bdir, "backupDir")) return e;
    if (const int e = fsyncDir_(viewDataDir_, "viewDir")) return e;

    // Retention: keep the newest `keep`.  A failure here never fails the discard.
    std::vector<std::string> all = listViewBackups(viewDataDir_, view, part);
    bool pruned = false;
    for (std::size_t i = 0; all.size() - i > policy_.keep; ++i) {
        if (::unlink(all[i].c_str()) != 0) spdlog::warn("ViewSnapshotIoAgent: pruning backup {} failed: {}", all[i], std::strerror(errno));
        else pruned = true;
    }
    if (pruned && fsyncDir_(bdir, "backupDir") != 0)
        spdlog::warn("ViewSnapshotIoAgent: fsync of {} after pruning failed: {}", bdir, std::strerror(errno));
    spdlog::info("VIEW-BACKUP view={} partition={} reason={} seq={} bytes={} file={}", view, part, viewDiscardReasonName(reason), seq,
                 static_cast<long long>(st.st_size), name);
    return 0;
}

void ViewSnapshotIoAgent::handleDiscardFrame_(const uint8_t* p, std::size_t len, int channelIdx) {
    if (len < static_cast<std::size_t>(kDiscardMsgBytes)) return;
    const Key key{static_cast<int32_t>(readLE32(p)), static_cast<int32_t>(readLE32(p + 4))};
    const auto reason = static_cast<ViewDiscardReason>(p[8]);
    const bool legacy = discardTargetsLegacy(reason);
    const std::string path = legacy ? legacyViewCheckpointFilePath(viewDataDir_, key.viewTopicId, key.partition)
                                    : deltaFilePath_(key);
    if (!legacy) {
        // The committed file: flush and drop its stream so the next append reopens (and re-verifies) whatever file is there then.
        // This also clears an errored stream: the discard starts a fresh one.
        if (auto it = files_.find(key); it != files_.end()) {
            if (it->second.fd >= 0) {
                (void)rune::node::detail::platformFdatasync(it->second.fd);
                ::close(it->second.fd);
            }
            files_.erase(it);
        }
    }
    const int status = moveToBackup_(path, key.viewTopicId, key.partition, reason);
    if (status != 0)
        spdlog::error("ViewSnapshotIoAgent: discard of view {} partition {} ({}) failed: {}", key.viewTopicId, key.partition,
                      viewDiscardReasonName(reason), std::strerror(status));
    if (recordFsyncLog_) fsyncLog_.emplace_back("ack");
    postDiscardDone_(channelIdx, key.viewTopicId, key.partition, status);
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

// One ack onto `channels_[channelIdx]->done`; false when the ring is full (or the channel is gone).
bool ViewSnapshotIoAgent::writeAck_(int channelIdx, int32_t msgType, int32_t view, int32_t part, int64_t value) {
    auto* ch = (channelIdx >= 0 && channelIdx < static_cast<int>(channels_.size())) ? channels_[channelIdx] : nullptr;
    if (!ch) return false;
    if (msgType == kDiscardDoneMsgType) {
        uint8_t buf[kDiscardDoneMsgBytes];
        writeLE32(buf, static_cast<uint32_t>(view));
        writeLE32(buf + 4, static_cast<uint32_t>(part));
        writeLE32(buf + 8, static_cast<uint32_t>(static_cast<int32_t>(value)));
        return ch->done.write(buf, sizeof(buf), kDiscardDoneMsgType);
    }
    uint8_t buf[ViewSnapshotChannel::kDoneMsgBytes];
    encodeAck(buf, view, part, value);
    return ch->done.write(buf, sizeof(buf), ViewSnapshotChannel::kDoneMsgType);
}

void ViewSnapshotIoAgent::postDone_(int32_t viewTopicId, int32_t partition, int64_t O, int channelIdx) {
    if (!writeAck_(channelIdx, ViewSnapshotChannel::kDoneMsgType, viewTopicId, partition, O)) {
        // Ring momentarily full -- retry next cycle (never dropped). The
        // in-process lastDurableOffset() is already correct regardless of
        // whether this notification lands; see file header.
        pendingAcks_.push_back(PendingAck{viewTopicId, partition, O, channelIdx, ViewSnapshotChannel::kDoneMsgType, 0});
    }
}

void ViewSnapshotIoAgent::postDiscardDone_(int channelIdx, int32_t view, int32_t part, int32_t status) {
    ++discards_;
    if (!writeAck_(channelIdx, kDiscardDoneMsgType, view, part, status))
        pendingAcks_.push_back(PendingAck{view, part, 0, channelIdx, kDiscardDoneMsgType, status});   // never dropped: replayed next cycle
}

void ViewSnapshotIoAgent::drainPendingAcks_() {
    if (pendingAcks_.empty()) return;
    std::size_t keep = 0;
    for (std::size_t i = 0; i < pendingAcks_.size(); ++i) {
        const auto& ack = pendingAcks_[i];
        const int64_t value = ack.msgType == kDiscardDoneMsgType ? static_cast<int64_t>(ack.status) : ack.O;
        if (!writeAck_(ack.channelIdx, ack.msgType, ack.viewTopicId, ack.partition, value)) {
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
                if (type == kDiscardMsgType) {
                    handleDiscardFrame_(reinterpret_cast<const uint8_t*>(buf.buffer() + off), static_cast<std::size_t>(len), ci);
                    return;
                }
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

void ViewSnapshotIoAgent::forceErroredForTest(int32_t viewTopicId, int32_t partition) {
    if (FileState* fs = fileFor_(Key{viewTopicId, partition})) fs->errored = true;
}

} // namespace rune::node
