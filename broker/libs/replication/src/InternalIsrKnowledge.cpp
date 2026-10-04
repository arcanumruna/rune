// ---------------------------------------------------------------------------
// InternalIsrKnowledge.cpp — durable internal-ISR knowledge (see the header).
//
// File format (little-endian, one file per node, tmp + fsync + rename + dir
// fsync — the leader-epoch checkpoint discipline):
//   u32 magic 'RIIS'  u32 version 1  u32 count  u32 pad
//   count × { i32 topicId, i32 partition, i64 leaderEpoch, i64 version,
//             i32 memberCount, i32 members[16] }
//   u64 checksum (FNV-1a over everything before it)
// ---------------------------------------------------------------------------
#include <rune/replication/InternalIsrKnowledge.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

namespace rune::replication {

namespace {
constexpr uint32_t    kMagic   = 0x53494952u;   // "RIIS"
constexpr uint32_t    kVersion = 1u;
constexpr std::size_t kHdr     = 16u;
constexpr std::size_t kEnt     = 4 + 4 + 8 + 8 + 4 + 4 * InternalIsrState::kMaxMembers;

uint64_t fnv1a(const uint8_t* p, std::size_t n) noexcept {
    uint64_t h = 1469598103934665603ULL;
    for (std::size_t i = 0; i < n; ++i) h = (h ^ p[i]) * 1099511628211ULL;
    return h;
}
template <class T> void put(uint8_t*& p, T v) noexcept { std::memcpy(p, &v, sizeof v); p += sizeof v; }
template <class T> T get(const uint8_t*& p) noexcept { T v; std::memcpy(&v, p, sizeof v); p += sizeof v; return v; }
} // namespace

InternalIsrKnowledge::InternalIsrKnowledge(std::string path) : path_(std::move(path))
{
    entries_.reserve(8);
    load_();
}

const InternalIsrState* InternalIsrKnowledge::latest(int32_t topicId, int32_t partition) const noexcept
{
    for (const auto& e : entries_)
        if (e.topicId == topicId && e.partition == partition) return &e;
    return nullptr;
}

bool InternalIsrKnowledge::eligible(int32_t candidate) const noexcept
{
    if (!votable_) return false;   // LOW-A: unprovable knowledge — refuse to vote
    // MEDIUM-B: only __cluster_metadata decides candidacy (the coordinator
    // partitions are on the controller-ISR path; any other entry — e.g. from an
    // older file — is ignored by the rule).
    const InternalIsrState* m = latest(kCandidacyTopicId, 0);
    return m == nullptr || m->contains(candidate);
}

bool InternalIsrKnowledge::offer(const InternalIsrState& s)
{
    if (s.count < 0 || s.count > InternalIsrState::kMaxMembers) return false;
    // Raft's term rule applied to the ack (re-review 2026-10-04 MEDIUM-A): a
    // state from a leadership OLDER than the term this node voted in / observed
    // is neither persisted nor acked — a deposed (zombie) leader can never
    // commit an ISR change, nor plant a stale candidacy view, through a node
    // that already moved on.
    if (const int64_t term = consensusTerm(); term >= 0 && s.leaderEpoch < term) {
        ++staleTermRefusals_;
        return false;
    }
    for (auto& e : entries_) {
        if (e.topicId != s.topicId || e.partition != s.partition) continue;
        if (!s.newerThan(e.leaderEpoch, e.version)) return true;   // holds s or newer
        const InternalIsrState prev = e;
        e = s;
        if (persist_()) { if (s.topicId == kCandidacyTopicId) votable_ = true; return true; }
        e = prev;                                                    // never claim an unpersisted state
        return false;
    }
    entries_.push_back(s);
    if (persist_()) { if (s.topicId == kCandidacyTopicId) votable_ = true; return true; }
    entries_.pop_back();
    return false;
}

bool InternalIsrKnowledge::persist_()
{
    ++persists_;
    if (path_.empty()) return true;   // memory only
    std::vector<uint8_t> buf(kHdr + entries_.size() * kEnt + 8, 0u);
    uint8_t* p = buf.data();
    put<uint32_t>(p, kMagic);
    put<uint32_t>(p, kVersion);
    put<uint32_t>(p, static_cast<uint32_t>(entries_.size()));
    put<uint32_t>(p, 0u);
    for (const auto& e : entries_) {
        put<int32_t>(p, e.topicId);
        put<int32_t>(p, e.partition);
        put<int64_t>(p, e.leaderEpoch);
        put<int64_t>(p, e.version);
        put<int32_t>(p, e.count);
        for (int32_t m : e.members) put<int32_t>(p, m);
    }
    put<uint64_t>(p, fnv1a(buf.data(), static_cast<std::size_t>(p - buf.data())));

    const std::string tmp = path_ + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;
    const bool ok = ::write(fd, buf.data(), buf.size()) == static_cast<ssize_t>(buf.size()) &&
                    ::fsync(fd) == 0;
    ::close(fd);
    if (!ok || ::rename(tmp.c_str(), path_.c_str()) != 0) {
        ::unlink(tmp.c_str());
        spdlog::error("InternalIsrKnowledge: could not persist {} — the ISR change is NOT acked", path_);
        return false;
    }
    // The rename is durable only once the directory entry is (LOW-A): a failed
    // directory fsync is a failed persist — the caller never acks it.
    const std::string dir = std::filesystem::path(path_).parent_path().string();
    const int dfd = ::open(dir.empty() ? "." : dir.c_str(), O_RDONLY | O_DIRECTORY);
    const bool dirOk = dfd >= 0 && ::fsync(dfd) == 0;
    if (dfd >= 0) ::close(dfd);
    if (!dirOk) {
        spdlog::error("InternalIsrKnowledge: directory fsync for {} failed — the ISR change is NOT acked",
                      path_);
        return false;
    }
    return true;
}

void InternalIsrKnowledge::load_()
{
    if (path_.empty()) return;
    const int fd = ::open(path_.c_str(), O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) return;   // absent: the fresh state (never acked anything)
        failClosed_("unreadable");
        return;
    }
    std::vector<uint8_t> buf;
    uint8_t chunk[4096];
    for (;;) {
        const ssize_t n = ::read(fd, chunk, sizeof chunk);
        if (n <= 0) break;
        buf.insert(buf.end(), chunk, chunk + n);
    }
    ::close(fd);
    if (buf.size() < kHdr + 8) { failClosed_("short"); return; }
    const uint8_t* p = buf.data();
    if (get<uint32_t>(p) != kMagic) { failClosed_("bad magic"); return; }
    if (get<uint32_t>(p) != kVersion) { failClosed_("unknown version"); return; }
    const uint32_t count = get<uint32_t>(p);
    (void)get<uint32_t>(p);
    if (buf.size() != kHdr + static_cast<std::size_t>(count) * kEnt + 8) { failClosed_("short"); return; }
    const uint64_t sum = fnv1a(buf.data(), buf.size() - 8);
    const uint8_t* q = buf.data() + buf.size() - 8;
    if (get<uint64_t>(q) != sum) { failClosed_("checksum mismatch"); return; }
    for (uint32_t i = 0; i < count; ++i) {
        InternalIsrState e;
        e.topicId     = get<int32_t>(p);
        e.partition   = get<int32_t>(p);
        e.leaderEpoch = get<int64_t>(p);
        e.version     = get<int64_t>(p);
        e.count       = get<int32_t>(p);
        for (auto& m : e.members) m = get<int32_t>(p);
        if (e.count < 0 || e.count > InternalIsrState::kMaxMembers) {
            entries_.clear();
            failClosed_("bad member count");
            return;
        }
        entries_.push_back(e);
    }
}

void InternalIsrKnowledge::failClosed_(const char* why)
{
    votable_ = false;
    spdlog::error("InternalIsrKnowledge: {} exists but does not validate ({}) — this node REFUSES "
                  "TO VOTE in consensus elections until it durably holds a fresh internal ISR "
                  "state from the leader (fail closed)", path_, why);
}

} // namespace rune::replication
