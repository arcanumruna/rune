// libs/node/src/TopicDirReconcile.cpp — see the header for the contract.
#include <rune/node/TopicDirReconcile.h>
#include <rune/node/NamespacePaths.h>

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

void purgeDroppedEntries(const fs::path& dataDir, uint32_t nsId, std::span<const int32_t> droppedIds,
                         TopicDirReconciliation& result) {
    const fs::path root = droppedRoot(dataDir, nsId);
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

/// Moves `c` (a direct child of the canonical topics root of ns `nsId`) to ns-<nsId>/quarantine/<name>-<now>[-N];
/// never deletes.  true iff moved.
bool quarantineOne(const fs::path& p, const fs::path& dataDir, uint32_t nsId, const fs::path& canonicalRoot,
                   int64_t now, TopicDirReconciliation& result) {
    std::error_code cec;
    const fs::path c = fs::canonical(p, cec);
    if (cec || c.parent_path() != canonicalRoot) {   // guard: a direct child of ns-<nsId>/topics only
        spdlog::error("topic-dir reconcile: refusing to touch '{}' (outside '{}')", p.string(), canonicalRoot.string());
        return false;
    }
    const fs::path qRoot = quarantineRoot(dataDir, nsId);
    std::error_code qec;
    const fs::file_status qst = fs::symlink_status(qRoot, qec);
    if (fs::is_symlink(qst) || (fs::exists(qst) && !fs::is_directory(qst))) {
        spdlog::error("topic-dir reconcile: '{}' is not a plain directory — not quarantining '{}'",
                      qRoot.string(), c.string());
        return false;
    }
    qec.clear();
    fs::create_directories(qRoot, qec);
    if (qec) {
        spdlog::error("topic-dir reconcile: cannot create '{}': {}", qRoot.string(), qec.message());
        return false;
    }
    const std::string base = c.filename().string() + "-" + std::to_string(now);
    fs::path dest = qRoot / base;
    for (int n = 1; fs::exists(fs::symlink_status(dest, qec)); ++n)
        dest = qRoot / (base + "-" + std::to_string(n));
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

/// Moves a topic dir `c` whose marker names the dropped id `id` to ns-<nsId>/dropped/<id>.<name> (purged right
/// after by purgeDroppedEntries).  false (nothing moved, logged) when the destination is taken or the rename
/// fails - the caller quarantines instead.  Never deletes.
bool retireToDropped(const fs::path& c, const fs::path& dataDir, uint32_t nsId, int32_t id,
                     TopicDirReconciliation& result) {
    const fs::path dRoot = droppedRoot(dataDir, nsId);
    const fs::path dest  = dRoot / (std::to_string(id) + "." + c.filename().string());
    std::error_code dec;
    fs::create_directories(dRoot, dec);
    const bool free = !dec && !fs::exists(fs::symlink_status(dest, dec));
    std::error_code mec;
    if (free) fs::rename(c, dest, mec);
    if (free && !mec) {
        spdlog::info("topic-dir reconcile: dir of dropped topic id {} moved to {} (purged below)", id, dest.string());
        result.retired.push_back(dest);
        return true;
    }
    return false;
}

/// R1.3 step 0: a dir claimed by a CATALOG topic is opened only by the incarnation that made it.
void settleClaimedDirs(const fs::path& dataDir, uint32_t nsId, const fs::path& topicsDir, const fs::path& canonicalRoot,
                       const std::vector<TopicSpec>& catalogTopics, std::span<const int32_t> droppedIds,
                       int64_t now, TopicDirReconciliation& result) {
    namespace gw = rune::gateway;
    std::error_code ec;
    std::vector<fs::path> children;
    for (fs::directory_iterator it(topicsDir, ec), end; !ec && it != end; it.increment(ec)) {
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
        if (valid && (m.id != t->topicId || transient) && dropped && retireToDropped(c, dataDir, nsId, m.id, result)) {
            spdlog::warn("topic-dir reconcile: older incarnation id {} of '{}' retired — the live topic id {} "
                         "starts empty", m.id, fname, t->topicId);
            continue;
        }
        (void)quarantineOne(c, dataDir, nsId, canonicalRoot, now, result);
    }
}

} // namespace

namespace {

/// The reconcile of ONE namespace root: `allTopics` are the catalog topics of ns `nsId` only.
void reconcileNs(const fs::path& dataDir, uint32_t nsId, const std::vector<TopicSpec>& allTopics,
                 std::span<const int32_t> droppedIds, int64_t nowUnixSeconds, TopicDirReconciliation& result) {
    std::error_code ec;
    const fs::path topicsDir = topicsRoot(dataDir, nsId);
    // VLS B.2: a view sink is catalog-only -- it owns no dir.  An old sink dir (an earlier build made a replica for
    // it) that holds no record is removed (logged); one holding records is left unclaimed, so it is quarantined.
    std::vector<TopicSpec> catalogTopics;   // cold (boot): the topics that own a dir
    for (const TopicSpec& t : allTopics) {
        if (!t.viewSink) { catalogTopics.push_back(t); continue; }
        const fs::path d = topicsDir / t.name;
        std::error_code sec;
        const fs::file_status st = fs::symlink_status(d, sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st) || rune::gateway::topicDirHoldsRecords(d)) continue;
        fs::remove_all(d, sec);
        if (!sec) {
            spdlog::info("topic-dir reconcile: removed the empty dir of view sink '{}' (id {}) -- a sink owns no log",
                         rune::catalog::escapeNameForLog(t.name), t.topicId);
            result.removedSinkDirs.push_back(d);
        }
    }
    const fs::file_status rootSt = fs::symlink_status(topicsDir, ec);
    const bool haveTopics = !ec && fs::is_directory(rootSt);   // absent, or a symlink: never followed
    fs::path canonicalRoot;
    if (haveTopics) {
        canonicalRoot = fs::canonical(topicsDir, ec);
        if (ec) { purgeDroppedEntries(dataDir, nsId, droppedIds, result); return; }
        settleClaimedDirs(dataDir, nsId, topicsDir, canonicalRoot, catalogTopics, droppedIds, nowUnixSeconds, result);
    }
    if (!haveTopics) { purgeDroppedEntries(dataDir, nsId, droppedIds, result); return; }

    auto claimed = [&](const std::string& n) {
        for (const TopicSpec& t : catalogTopics)
            if (t.name == n || topicNamesFoldEqual(t.name, n)) return true;
        return false;
    };

    std::vector<fs::path> candidates;
    for (fs::directory_iterator it(topicsDir, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;   // not a topic dir: left alone
        if (claimed(it->path().filename().string())) continue;
        if (!isTopicDirLayout(it->path()) &&
            rune::gateway::readTopicIdMarker(it->path()).kind != rune::gateway::TopicIdMarker::Kind::Valid) {
            spdlog::info("topic-dir reconcile: leaving '{}' alone (not a recognised topic dir layout)",
                         it->path().string());
            continue;
        }
        candidates.push_back(it->path());
    }
    if (ec)
        spdlog::error("topic-dir reconcile: listing '{}' failed: {} — no further dirs considered",
                      topicsDir.string(), ec.message());
    for (const fs::path& p : candidates) {
        const rune::gateway::TopicIdMarker m = rune::gateway::readTopicIdMarker(p);
        const bool dropped = m.kind == rune::gateway::TopicIdMarker::Kind::Valid &&
                             std::find(droppedIds.begin(), droppedIds.end(), m.id) != droppedIds.end();
        // An unclaimed dir of a committed DROP (a crash between the apply and the retire): retired, purged
        // below - never quarantined.  Anything else unattributed is quarantined.
        if (dropped && retireToDropped(p, dataDir, nsId, m.id, result)) continue;
        (void)quarantineOne(p, dataDir, nsId, canonicalRoot, nowUnixSeconds, result);
    }
    purgeDroppedEntries(dataDir, nsId, droppedIds, result);   // AFTER both loops: this run's retires are purged here
}

/// Every ns-<u32> dir directly under dataDir (sorted); any other non-state entry is logged and left alone.
std::vector<uint32_t> listNsDirs(const fs::path& dataDir, bool logForeign) {
    std::vector<uint32_t> ids;
    std::error_code ec;
    for (fs::directory_iterator it(dataDir, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string n = it->path().filename().string();
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (const auto id = parseNsDir(n); id && !sec && fs::is_directory(st) && !fs::is_symlink(st)) {
            ids.push_back(*id);
            continue;
        }
        if (!logForeign || !fs::is_directory(st) || n.rfind("__", 0) == 0 || n.rfind('.', 0) == 0) continue;
        spdlog::info("topic-dir reconcile: leaving '{}' alone — not a namespace dir (ns-<id>); never read, moved "
                     "or deleted (no migration of a pre-namespace layout)", it->path().string());
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

} // namespace

TopicDirReconciliation reconcileTopicDirs(const std::string& dataDirStr, const std::vector<TopicSpec>& allTopics,
                                          std::span<const int32_t> droppedIds, int64_t nowUnixSeconds) {
    TopicDirReconciliation result;
    const fs::path dataDir{dataDirStr};
    std::vector<uint32_t> nsIds = listNsDirs(dataDir, /*logForeign=*/true);
    for (const TopicSpec& t : allTopics) nsIds.push_back(t.nsId);   // a sink dir may exist without its ns listed
    std::sort(nsIds.begin(), nsIds.end());
    nsIds.erase(std::unique(nsIds.begin(), nsIds.end()), nsIds.end());
    std::vector<TopicSpec> nsTopics;   // cold (boot)
    for (const uint32_t nsId : nsIds) {
        nsTopics.clear();
        for (const TopicSpec& t : allTopics) if (t.nsId == nsId) nsTopics.push_back(t);
        reconcileNs(dataDir, nsId, nsTopics, droppedIds, nowUnixSeconds, result);
    }
    return result;
}

NsDeletingReconciliation reconcileNsDeletingDirs(const std::string& dataDirStr, std::span<const NsDirFact> facts,
                                                 uint32_t nextNsId) {
    NsDeletingReconciliation result;
    const fs::path dataDir{dataDirStr};
    constexpr std::string_view kSuffix = ".deleting";
    const auto factOf = [&](uint32_t id) -> const NsDirFact* {
        for (const NsDirFact& f : facts) if (f.nsId == id) return &f;
        return nullptr;
    };
    std::vector<fs::path> entries;
    std::error_code ec;
    for (fs::directory_iterator it(dataDir, ec), end; !ec && it != end; it.increment(ec)) entries.push_back(it->path());
    std::sort(entries.begin(), entries.end());
    for (const fs::path& p : entries) {
        const std::string n = p.filename().string();
        std::error_code sec;
        const fs::file_status st = fs::symlink_status(p, sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;   // never followed
        const bool deletingDir = n.size() > kSuffix.size() && n.ends_with(kSuffix);
        const auto id = parseNsDir(deletingDir ? std::string_view(n).substr(0, n.size() - kSuffix.size()) : std::string_view(n));
        if (!id || *id == 0) continue;
        const NsDirFact* fact = factOf(*id);
        if (fact != nullptr && !deletingDir) continue;   // a live / DELETING namespace's own dir: the topic reconcile's
        if (fact == nullptr && *id >= nextNsId) {        // an id this catalog never issued: not ours to judge
            spdlog::warn("namespace-dir reconcile: leaving '{}' alone - namespace id {} was never issued by this catalog",
                         p.string(), *id);
            continue;
        }
        if (fact != nullptr) {   // a ".deleting" dir of a namespace the registry still holds
            if (fact->deleting) {
                spdlog::info("namespace-dir reconcile: keeping '{}' - namespace {} is still DELETING", p.string(), *id);
                result.keptDeleting.push_back(p);
            } else {
                spdlog::critical("namespace-dir reconcile: '{}' exists but namespace {} is LIVE - a deleted namespace's data "
                                 "dir beside a live namespace of the same id; refusing to start (catalog or disk damage)",
                                 p.string(), *id);
                result.liveMismatch.push_back(p);
            }
            continue;
        }
        // The namespace is erased: its dir (ns-<id>, or ns-<id>.deleting) goes.
        fs::path doomed = p;
        if (!deletingDir) {
            const fs::path to = fs::path(p.string() + std::string(kSuffix));
            std::error_code rec;
            if (fs::exists(fs::symlink_status(to, rec))) fs::remove_all(to, rec);
            rec.clear();
            fs::rename(p, to, rec);
            if (rec) {
                spdlog::error("namespace-dir reconcile: renaming '{}' -> '{}' failed: {} - retried at the next boot",
                              p.string(), to.string(), rec.message());
                continue;
            }
            doomed = to;
        }
        std::error_code rem;
        fs::remove_all(doomed, rem);   // never follows symlinks
        if (rem) {
            spdlog::error("namespace-dir reconcile: removing '{}' failed: {} - retried at the next boot", doomed.string(),
                          rem.message());
            continue;
        }
        spdlog::info("namespace-dir reconcile: removed '{}' - namespace {} was deleted (an interrupted purge)",
                     doomed.string(), *id);
        (deletingDir ? result.removed : result.erasedDirs).push_back(doomed);
    }
    return result;
}

bool droppedHoldsEntries(const std::string& dataDir) noexcept {
    try {
        const fs::path d{dataDir};
        for (const uint32_t nsId : listNsDirs(d, /*logForeign=*/false)) {
            std::error_code ec;
            fs::directory_iterator it(droppedRoot(d, nsId), ec), end;
            if (!ec && it != end) return true;
        }
    } catch (...) {
    }
    return false;
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
        const fs::path d{dataDir};
        for (const uint32_t nsId : listNsDirs(d, /*logForeign=*/false)) {
            std::error_code ec;
            const fs::path root = topicsRoot(d, nsId);
            const fs::file_status rst = fs::symlink_status(root, ec);
            if (ec || !fs::is_directory(rst)) continue;
            for (fs::directory_iterator it(root, ec), end; !ec && it != end; it.increment(ec)) {
                std::error_code sec;
                const fs::file_status st = it->symlink_status(sec);
                if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;
                if (rune::gateway::readTopicIdMarker(it->path()).kind == rune::gateway::TopicIdMarker::Kind::Valid)
                    return true;
            }
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

} // namespace rune::node
