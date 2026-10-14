// libs/node/src/TopicDirReconcile.cpp — see the header for the contract.
#include <rune/node/TopicDirReconcile.h>

#include <rune/catalog/TopicName.h>   // topicNamesFoldEqual, escapeNameForLog

#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <string>

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

TopicDirReconciliation reconcileTopicDirs(const std::string& dataDir, const std::vector<TopicSpec>& liveTopics,
                                          std::span<const int32_t> droppedIds, int64_t nowUnixSeconds) {
    TopicDirReconciliation result;
    purgeDroppedEntries(dataDir, droppedIds, result);

    std::error_code ec;
    const fs::path topicsRoot = fs::path(dataDir) / "topics";
    const fs::file_status rootSt = fs::symlink_status(topicsRoot, ec);
    if (ec || !fs::is_directory(rootSt)) return result;   // absent, or a symlink: never followed
    const fs::path canonicalRoot = fs::canonical(topicsRoot, ec);
    if (ec) return result;

    auto claimed = [&](const std::string& n) {
        for (const TopicSpec& t : liveTopics)
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

    const fs::path quarantineRoot = fs::path(dataDir) / "quarantine";
    for (const fs::path& p : candidates) {
        std::error_code cec;
        const fs::path c = fs::canonical(p, cec);
        if (cec || c.parent_path() != canonicalRoot) {   // guard: a direct child of dataDir/topics only
            spdlog::error("topic-dir reconcile: refusing to touch '{}' (outside '{}')", p.string(),
                          canonicalRoot.string());
            continue;
        }
        std::error_code qec;
        const fs::file_status qst = fs::symlink_status(quarantineRoot, qec);
        if (fs::is_symlink(qst) || (fs::exists(qst) && !fs::is_directory(qst))) {
            spdlog::error("topic-dir reconcile: '{}' is not a plain directory — not quarantining '{}'",
                          quarantineRoot.string(), c.string());
            continue;
        }
        qec.clear();
        fs::create_directories(quarantineRoot, qec);
        if (qec) {
            spdlog::error("topic-dir reconcile: cannot create '{}': {}", quarantineRoot.string(), qec.message());
            continue;
        }
        const std::string base = c.filename().string() + "-" + std::to_string(nowUnixSeconds);
        fs::path dest = quarantineRoot / base;
        for (int n = 1; fs::exists(fs::symlink_status(dest, qec)); ++n)
            dest = quarantineRoot / (base + "-" + std::to_string(n));
        std::error_code mec;
        fs::rename(c, dest, mec);   // same filesystem (both under dataDir): atomic, data untouched
        if (mec) {
            spdlog::error("topic-dir reconcile: moving '{}' to '{}' failed: {}", c.string(), dest.string(),
                          mec.message());
            continue;
        }
        spdlog::warn("topic-dir reconcile: quarantined '{}' -> '{}' — no topic in the catalog claims it; it is "
                     "NOT restored automatically (never deleted)", c.string(), dest.string());
        result.quarantined.push_back(dest);
    }
    return result;
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
                                                  const std::vector<TopicSpec>& catalogTopics) {
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
    return std::nullopt;
}

} // namespace rune::node
