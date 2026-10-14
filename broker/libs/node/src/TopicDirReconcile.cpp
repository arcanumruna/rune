// libs/node/src/TopicDirReconcile.cpp — see the header for the contract.
#include <rune/node/TopicDirReconcile.h>

#include <rune/catalog/TopicName.h>   // topicNamesFoldEqual, escapeNameForLog
#include <rune/gateway/TopicDirMarker.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <string>

#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace rune::node {

using rune::catalog::topicNamesFoldEqual;

namespace {

bool allDigits(const std::string& s) {
    if (s.empty()) return false;
    for (const char c : s) if (c < '0' || c > '9') return false;
    return true;
}

/// <topic>/[topic-id] + <digits>/<regular files>: at least one partition dir, no
/// symlink and no sub-directory below a partition.  Anything else is not ours.
/// (A `topic-id` / `topic-id.tmp` regular file is a leftover of a retired build's
/// marker: tolerated, never read.)
bool isTopicDirLayout(const fs::path& topicDir) {
    std::error_code ec;
    bool sawPartition = false;
    for (fs::directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::file_status st = it->symlink_status(ec);
        if (ec) return false;
        const std::string child = it->path().filename().string();
        if ((child == "topic-id" || child == "topic-id.tmp") && fs::is_regular_file(st)) continue;
        if (!fs::is_directory(st) || !allDigits(child)) return false;
        std::error_code pec;
        for (fs::directory_iterator pit(it->path(), pec), pend; !pec && pit != pend; pit.increment(pec)) {
            const fs::file_status pst = pit->symlink_status(pec);
            if (pec || !fs::is_regular_file(pst)) return false;
        }
        if (pec) return false;
        sawPartition = true;
    }
    return !ec && sawPartition;
}

/// The id of a dropped/ entry named "<digits>.<name>"; -1 when it does not parse.
int64_t droppedEntryId(const std::string& name) {
    const std::size_t dot = name.find('.');
    if (dot == std::string::npos || dot == 0 || dot > 10) return -1;
    const std::string digits = name.substr(0, dot);
    if (!allDigits(digits)) return -1;
    if (digits.size() > 1 && digits[0] == '0') return -1;   // the writer never pads
    const int64_t id = std::stoll(digits);
    return id > INT32_MAX ? -1 : id;
}

void purgeDroppedEntries(const std::string& dataDir, std::span<const int32_t> droppedIds,
                         TopicDirReconciliation& result) {
    const fs::path root = fs::path(dataDir) / "dropped";
    std::error_code ec;
    const fs::file_status rst = fs::symlink_status(root, ec);
    if (ec || !fs::is_directory(rst)) return;   // absent, or a symlink: never followed
    std::vector<fs::path> entries;
    for (fs::directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) entries.push_back(it->path());
    if (ec) {
        spdlog::error("topic-dir reconcile: listing '{}' failed: {} — no dropped/ entry purged", root.string(),
                      ec.message());
        return;
    }
    for (const fs::path& p : entries) {
        std::error_code sec;
        const fs::file_status st = fs::symlink_status(p, sec);
        const int64_t id = droppedEntryId(p.filename().string());
        const bool marked = id >= 0 && std::find(droppedIds.begin(), droppedIds.end(), static_cast<int32_t>(id)) !=
                                           droppedIds.end();
        if (sec || fs::is_symlink(st) || !marked) {
            spdlog::warn("topic-dir reconcile: leaving '{}' in dropped/ alone — the catalog does not mark "
                         "topic id {} dropped (or it is not a plain entry); it is never deleted on a guess",
                         p.string(), id);
            continue;
        }
        std::error_code rec;
        fs::remove_all(p, rec);   // never follows symlinks
        if (rec) {
            spdlog::error("topic-dir reconcile: purging '{}' failed: {} — retried at the next boot", p.string(),
                          rec.message());
            continue;
        }
        spdlog::info("topic-dir reconcile: purged '{}' — the data of dropped topic id {} (an interrupted purge)",
                     p.string(), id);
        result.purged.push_back(p);
    }
}

} // namespace

namespace {

/// Moves `c` (a direct child of the canonical topics root) to dataDir/quarantine/<name>-<now>[-N]; never
/// deletes.  true iff moved.
bool quarantineOne(const fs::path& p, const fs::path& dataDir, const fs::path& canonicalRoot, int64_t now,
                   TopicDirReconciliation& result) {
    std::error_code cec;
    const fs::path c = fs::canonical(p, cec);
    if (cec || c.parent_path() != canonicalRoot) {   // guard: a direct child of dataDir/topics only
        spdlog::error("topic-dir reconcile: refusing to touch '{}' (outside '{}')", p.string(), canonicalRoot.string());
        return false;
    }
    const fs::path quarantineRoot = dataDir / "quarantine";
    std::error_code qec;
    const fs::file_status qst = fs::symlink_status(quarantineRoot, qec);
    if (fs::is_symlink(qst) || (fs::exists(qst) && !fs::is_directory(qst))) {
        spdlog::error("topic-dir reconcile: '{}' is not a plain directory — not quarantining '{}'",
                      quarantineRoot.string(), c.string());
        return false;
    }
    qec.clear();
    fs::create_directories(quarantineRoot, qec);
    if (qec) {
        spdlog::error("topic-dir reconcile: cannot create '{}': {}", quarantineRoot.string(), qec.message());
        return false;
    }
    const std::string base = c.filename().string() + "-" + std::to_string(now);
    fs::path dest = quarantineRoot / base;
    for (int n = 1; fs::exists(fs::symlink_status(dest, qec)); ++n)
        dest = quarantineRoot / (base + "-" + std::to_string(n));
    std::error_code mec;
    fs::rename(c, dest, mec);   // same filesystem (both under dataDir): atomic, data untouched
    if (mec) {
        spdlog::error("topic-dir reconcile: moving '{}' to '{}' failed: {}", c.string(), dest.string(), mec.message());
        return false;
    }
    spdlog::warn("topic-dir reconcile: quarantined '{}' -> '{}' — no topic in the catalog claims it (or it holds "
                 "data of an unknown incarnation); it is NOT restored automatically (never deleted)",
                 c.string(), dest.string());
    result.quarantined.push_back(dest);
    return true;
}

/// R1.3 step 0: a dir claimed by a CATALOG topic is opened only by the incarnation that made it.
void settleClaimedDirs(const fs::path& dataDir, const fs::path& topicsRoot, const fs::path& canonicalRoot,
                       const std::vector<TopicSpec>& catalogTopics, std::span<const int32_t> droppedIds,
                       int64_t now, TopicDirReconciliation& result) {
    namespace gw = rune::gateway;
    std::error_code ec;
    std::vector<fs::path> children;
    for (fs::directory_iterator it(topicsRoot, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;
        children.push_back(it->path());
    }
    for (const fs::path& c : children) {
        const std::string fname = c.filename().string();
        const TopicSpec* t = nullptr;
        for (const TopicSpec& cand : catalogTopics)
            if (cand.name == fname || topicNamesFoldEqual(cand.name, fname)) { t = &cand; break; }
        if (t == nullptr) continue;
        const gw::TopicIdMarker m = gw::readTopicIdMarker(c);
        if (!isTopicDirLayout(c) && m.kind != gw::TopicIdMarker::Kind::Valid) continue;   // not a topic dir: left alone
        const bool valid     = m.kind == gw::TopicIdMarker::Kind::Valid;
        const bool transient = t->persistence == PersistenceMode::Transient;
        if (valid && m.id == t->topicId && !transient) continue;                 // keep: this incarnation's own dir
        if (!valid && !gw::topicDirHoldsRecords(c)) continue;                    // keep: holds nothing; createTopicAt adopts it
        const bool dropped = valid && std::find(droppedIds.begin(), droppedIds.end(), m.id) != droppedIds.end();
        if (valid && (m.id != t->topicId || transient) && dropped) {
            const fs::path droppedRoot = dataDir / "dropped";
            const fs::path dest        = droppedRoot / (std::to_string(m.id) + "." + fname);
            std::error_code dec;
            fs::create_directories(droppedRoot, dec);
            const bool free = !dec && !fs::exists(fs::symlink_status(dest, dec));
            std::error_code mec;
            if (free) fs::rename(c, dest, mec);
            if (free && !mec) {
                spdlog::warn("topic-dir reconcile: older incarnation id {} of '{}' moved to {} — the live topic id {} "
                             "starts empty", m.id, fname, dest.string(), t->topicId);
                result.retired.push_back(dest);
                continue;
            }
        }
        (void)quarantineOne(c, dataDir, canonicalRoot, now, result);
    }
}

} // namespace

TopicDirReconciliation reconcileTopicDirs(const std::string& dataDirStr, const std::vector<TopicSpec>& catalogTopics,
                                          std::span<const int32_t> droppedIds, int64_t nowUnixSeconds,
                                          const std::vector<TopicSpec>& staticTopics) {
    TopicDirReconciliation result;
    const fs::path dataDir{dataDirStr};
    std::error_code ec;
    const fs::path topicsRoot = dataDir / "topics";
    const fs::file_status rootSt = fs::symlink_status(topicsRoot, ec);
    const bool haveTopics = !ec && fs::is_directory(rootSt);   // absent, or a symlink: never followed
    fs::path canonicalRoot;
    if (haveTopics) {
        canonicalRoot = fs::canonical(topicsRoot, ec);
        if (ec) { purgeDroppedEntries(dataDirStr, droppedIds, result); return result; }
        settleClaimedDirs(dataDir, topicsRoot, canonicalRoot, catalogTopics, droppedIds, nowUnixSeconds, result);
    }
    purgeDroppedEntries(dataDirStr, droppedIds, result);
    if (!haveTopics) return result;

    auto claimed = [&](const std::string& n) {
        for (const auto* list : {&catalogTopics, &staticTopics})
            for (const TopicSpec& t : *list)
                if (t.name == n || topicNamesFoldEqual(t.name, n)) return true;
        return false;
    };

    std::vector<fs::path> candidates;
    for (fs::directory_iterator it(topicsRoot, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;   // not a topic dir: left alone
        if (claimed(it->path().filename().string())) continue;
        if (!isTopicDirLayout(it->path())) {
            spdlog::info("topic-dir reconcile: leaving '{}' alone (not a recognised topic dir layout)",
                         it->path().string());
            continue;
        }
        candidates.push_back(it->path());
    }
    if (ec)
        spdlog::error("topic-dir reconcile: listing '{}' failed: {} — no further dirs considered",
                      topicsRoot.string(), ec.message());
    for (const fs::path& p : candidates) (void)quarantineOne(p, dataDir, canonicalRoot, nowUnixSeconds, result);
    return result;
}

namespace {
bool syncDir(const fs::path& dir) noexcept {
    const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}
} // namespace

CatchUpSentinel readCatchUpSentinel(const std::string& dataDir) noexcept {
    CatchUpSentinel s;
    try {
        const fs::path p = fs::path{dataDir} / kCatchUpSentinelFile;
        std::error_code ec;
        const fs::file_status st = fs::symlink_status(p, ec);
        if (ec || !fs::exists(st)) return s;
        s.kind = CatchUpSentinel::Kind::Pending;   // present but unreadable / malformed: Pending (never trusted)
        if (fs::is_symlink(st) || !fs::is_regular_file(st)) {
            spdlog::warn("catch-up sentinel '{}' is not a regular file — read as pending", p.string());
            return s;
        }
        std::ifstream in(p, std::ios::binary);
        std::string line;
        std::getline(in, line);
        if (line.rfind("caughtup ", 0) == 0) {
            const std::string digits = line.substr(9);
            if (!digits.empty() && digits.size() <= 18 &&
                digits.find_first_not_of("0123456789") == std::string::npos) {
                s.kind   = CatchUpSentinel::Kind::CaughtUp;
                s.offset = std::stoll(digits);
                return s;
            }
        }
        if (line != "pending")
            spdlog::warn("catch-up sentinel '{}' is malformed ('{}') — read as pending", p.string(), line);
    } catch (...) {
    }
    return s;
}

bool writeCatchUpSentinel(const std::string& dataDir, int64_t caughtUpAt) noexcept {
    try {
        const fs::path dir{dataDir};
        const fs::path tmp = dir / (std::string{kCatchUpSentinelFile} + ".tmp");
        const fs::path dst = dir / kCatchUpSentinelFile;
        const std::string text = caughtUpAt < 0 ? std::string{"pending\n"}
                                                : "caughtup " + std::to_string(caughtUpAt) + "\n";
        const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, 0644);
        if (fd < 0) return false;
        bool ok = true;
        for (std::size_t off = 0; ok && off < text.size();) {
            const ssize_t w = ::write(fd, text.data() + off, text.size() - off);
            if (w < 0 && errno == EINTR) continue;
            if (w <= 0) ok = false; else off += static_cast<std::size_t>(w);
        }
        if (ok && ::fsync(fd) != 0) ok = false;
        ::close(fd);
        std::error_code ec;
        if (ok) { fs::rename(tmp, dst, ec); ok = !ec; }
        if (ok) ok = syncDir(dir);
        if (!ok) { std::error_code rec; fs::remove(tmp, rec); }
        return ok;
    } catch (...) {
        return false;
    }
}

bool removeCatchUpSentinel(const std::string& dataDir) noexcept {
    try {
        std::error_code ec;
        fs::remove(fs::path{dataDir} / kCatchUpSentinelFile, ec);
        if (ec) return false;
        return syncDir(fs::path{dataDir});
    } catch (...) {
        return false;
    }
}

bool topicsHoldCatalogMarkedDir(const std::string& dataDir) noexcept {
    try {
        std::error_code ec;
        const fs::path root = fs::path{dataDir} / "topics";
        const fs::file_status rst = fs::symlink_status(root, ec);
        if (ec || !fs::is_directory(rst)) return false;
        for (fs::directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) {
            std::error_code sec;
            const fs::file_status st = it->symlink_status(sec);
            if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;
            if (rune::gateway::readTopicIdMarker(it->path()).kind == rune::gateway::TopicIdMarker::Kind::Valid)
                return true;
        }
    } catch (...) {
    }
    return false;
}

BootReconcile decideBootReconcile(bool halted, int64_t lastApplied, bool markedDirs, bool droppedHasEntries,
                                  CatchUpSentinel s) noexcept {
    if (halted) return BootReconcile::Skip;
    if (s.kind == CatchUpSentinel::Kind::CaughtUp)
        return lastApplied >= s.offset ? BootReconcile::RunNowAndClearSentinel : BootReconcile::DeferAndWritePending;
    if (s.kind == CatchUpSentinel::Kind::Pending) return BootReconcile::Defer;
    if (lastApplied < 0 && (markedDirs || droppedHasEntries)) return BootReconcile::DeferAndWritePending;
    return BootReconcile::RunNow;
}

void warnRetiredCatalogDirs(const std::string& dataDir) {
    for (const char* retired : {"__topic_schemas", "__views"}) {
        std::error_code rec;
        const fs::path rdir = fs::path{dataDir} / retired;
        bool holdsData = false;
        for (fs::directory_iterator it(rdir, rec), end; !rec && it != end; it.increment(rec)) {
            std::error_code fec;
            if (it->path().extension() == ".log" && fs::file_size(it->path(), fec) > 0 && !fec) holdsData = true;
        }
        if (holdsData)
            spdlog::warn("retired catalog dir {} ignored — __catalog is the catalog (no migration); topics "
                         "recorded only there are not recovered", rdir.string());
    }
}

std::optional<std::string> staticCatalogCollision(const std::vector<TopicSpec>& statics,
                                                  const std::vector<TopicSpec>& catalogTopics,
                                                  std::span<const int32_t> catalogDroppedIds) {
    auto staticId = [&](std::size_t i) {
        return statics[i].topicId >= 0 ? statics[i].topicId : static_cast<int32_t>(i);
    };
    auto refusal = [](const TopicSpec& s, int32_t sid, const TopicSpec& c, const char* how) {
        return "static RUNE_TOPICS topic '" + rune::catalog::escapeNameForLog(s.name) + "' (id " + std::to_string(sid) +
               ") collides " + how + " with the catalog topic '" + rune::catalog::escapeNameForLog(c.name) +
               "' (id " + std::to_string(c.topicId) +
               ") — a static topic would take the catalog topic's directory or id; refusing to start (remove it "
               "from RUNE_TOPICS: create every topic by DDL)";
    };
    for (std::size_t i = 0; i < statics.size(); ++i)
        for (const TopicSpec& c : catalogTopics)
            if (statics[i].name == c.name || topicNamesFoldEqual(statics[i].name, c.name))
                return refusal(statics[i], staticId(i), c, "by name");
    for (std::size_t i = 0; i < statics.size(); ++i)
        for (const TopicSpec& c : catalogTopics)
            if (staticId(i) == c.topicId) return refusal(statics[i], staticId(i), c, "by topic id");
    for (std::size_t i = 0; i < statics.size(); ++i)
        if (std::find(catalogDroppedIds.begin(), catalogDroppedIds.end(), staticId(i)) != catalogDroppedIds.end())
            return "static RUNE_TOPICS topic '" + rune::catalog::escapeNameForLog(statics[i].name) + "' (id " +
                   std::to_string(staticId(i)) + ") collides by topic id with a topic the catalog dropped (id " +
                   std::to_string(staticId(i)) + ") — the boot would drop the static topic; refusing to start "
                   "(remove it from RUNE_TOPICS: create every topic by DDL)";
    return std::nullopt;
}

} // namespace rune::node
