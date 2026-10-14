// libs/node/src/RuntimeTopicRecovery.cpp — see the header for the contract.
#include <rune/node/RuntimeTopicRecovery.h>

#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>   // TopicSchema
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/ViewRegistry.h>     // ViewRecord

#include <rune/codec/Serialize.h>       // rune::codec::deserialize
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/TopicName.h>      // topicNamesFoldEqual
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>         // deserializeCompiledViewPlan

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <climits>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

namespace rune::node {

using rune::catalog::isValidTopicName;
using rune::catalog::topicNamesFoldEqual;
using rune::catalog::escapeNameForLog;

namespace {

bool allDigits(const std::string& s) {
    if (s.empty()) return false;
    for (const char c : s) if (c < '0' || c > '9') return false;
    return true;
}

/// <topic>/[topic-id] + <digits>/<regular files>: at least one partition dir, no
/// symlink and no sub-directory below a partition.  Anything else is not ours.
bool isTopicDirLayout(const fs::path& topicDir) {
    std::error_code ec;
    bool sawPartition = false;
    for (fs::directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec)) {
        const fs::file_status st = it->symlink_status(ec);
        if (ec) return false;
        const std::string child = it->path().filename().string();
        if ((child == kTopicIdMarkerFile ||
             child == kTopicIdMarkerTmpFile) && fs::is_regular_file(st))
            continue;   // the marker, or a marker write interrupted before its rename
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

/// The parsed topic-id marker (see kTopicIdMarkerFile).
enum class MarkerKind : uint8_t { Invalid, Pending, Committed };
struct TopicIdMarker {
    MarkerKind kind = MarkerKind::Invalid;
    int32_t    id   = -1;
};

/// <topicDir>/topic-id parsed EXACTLY as "pending <digits>[\n]" or
/// "committed <digits>[\n]" (<= 10 digits, <= INT32_MAX).  Anything else —
/// absent, unreadable, a bare number, extra spaces/lines, other case — is Invalid:
/// a delete is never decided on a guess.
TopicIdMarker readTopicIdMarker(const fs::path& topicDir) {
    std::ifstream in(topicDir / kTopicIdMarkerFile, std::ios::binary);
    if (!in) return {};
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (!text.empty() && text.back() == '\n') text.pop_back();
    const std::size_t sp = text.find(' ');
    if (sp == std::string::npos) return {};
    const std::string_view state(text.data(), sp);
    const std::string digits = text.substr(sp + 1);
    MarkerKind kind = MarkerKind::Invalid;
    if (state == kTopicIdMarkerPending) kind = MarkerKind::Pending;
    else if (state == kTopicIdMarkerCommitted) kind = MarkerKind::Committed;
    else return {};
    if (!allDigits(digits) || digits.size() > 10) return {};
    if (digits.size() > 1 && digits[0] == '0') return {};   // the writer never pads: "0002" is not ours
    const int64_t id = std::stoll(digits);
    if (id > INT32_MAX) return {};
    return {kind, static_cast<int32_t>(id)};
}

/// Final review C1 (a): true when any <partition>/*.log file of `topicDir` holds a
/// byte.  Segments are not preallocated, so a log nothing was ever appended to is
/// empty.  An unreadable size counts as "holds records" (never delete on doubt).
bool anyLogBytes(const fs::path& topicDir) {
    std::error_code ec;
    for (fs::directory_iterator it(topicDir, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        if (!fs::is_directory(it->symlink_status(sec)) || sec) continue;   // the marker files
        std::error_code pec;
        for (fs::directory_iterator pit(it->path(), pec), pend; !pec && pit != pend; pit.increment(pec)) {
            if (pit->path().extension() != ".log") continue;
            std::error_code fec;
            const auto size = fs::file_size(pit->path(), fec);
            if (fec || size > 0) return true;
        }
        if (pec) return true;
    }
    return static_cast<bool>(ec);
}

} // namespace

std::string orphanPlaceholderName(int32_t topicId) {
    return "__orphan." + std::to_string(topicId);
}

TopicDirReconciliation reconcileTopicDirs(const std::string& dataDir,
                                          const std::vector<TopicSpec>& topics,
                                          const std::vector<int32_t>& placeholderIds,
                                          int64_t nowUnixSeconds,
                                          std::string_view catalogDamage) {
    TopicDirReconciliation result;
    std::error_code ec;
    const fs::path topicsRoot = fs::path(dataDir) / "topics";
    const fs::file_status rootSt = fs::symlink_status(topicsRoot, ec);
    if (ec || !fs::is_directory(rootSt)) return result;   // absent, or a symlink: never followed
    const fs::path canonicalRoot = fs::canonical(topicsRoot, ec);
    if (ec) return result;

    // Fix round 2b: on a damaged catalog NOTHING is deleted this boot.
    // Final review M1: and the damage is LATCHED (dataDir/.catalog-damaged) — this
    // boot's recovery truncated the damaged catalog (or the replica recreates a
    // missing one), so the next boot would read it intact and could delete a dir
    // this boot failed to quarantine.  Cleared once no unclaimed topic dir remains.
    const fs::path sentinel = fs::path(dataDir) / kCatalogDamagedSentinel;
    std::error_code sec0;
    const bool latched = fs::exists(fs::symlink_status(sentinel, sec0));
    if (!catalogDamage.empty() && !latched) {
        std::ofstream out(sentinel, std::ios::binary | std::ios::trunc);
        out << catalogDamage << "\n";
        out.close();
        if (!out) spdlog::error("topic-dir reconcile: cannot write '{}' — the catalog damage is not latched",
                                sentinel.string());
    }
    const bool deletesAllowed = catalogDamage.empty() && !latched;
    if (!catalogDamage.empty()) {
        spdlog::warn("topic-dir reconcile: the topic catalog is damaged ({}) — NO topic dir is "
                     "deleted this boot; every orphan-placeholder dir is quarantined instead",
                     catalogDamage);
    } else if (latched) {
        spdlog::warn("topic-dir reconcile: an earlier boot found the topic catalog damaged ('{}' exists) — "
                     "NO topic dir is deleted; every orphan-placeholder dir is quarantined instead",
                     sentinel.string());
    }
    std::size_t leftInPlace = 0;   // candidates neither deleted nor quarantined (M1)

    // I2: placeholders are what RECOVERY created (structural), never a name match.
    const std::set<int32_t> placeholders(placeholderIds.begin(), placeholderIds.end());
    auto claimed = [&](const std::string& n) {
        for (const TopicSpec& t : topics)
            if (t.name == n || topicNamesFoldEqual(t.name, n)) return true;
        return false;
    };

    // ── Fix round 1b: promote claimed dirs' markers, during the scan below — i.e.
    // BEFORE any delete decision (deletes happen only after the scan).
    // The admin thread posts kA2cTopicCommitted only after the record committed,
    // and the control thread rewrites the marker asynchronously; a crash in between
    // leaves "pending" on a topic whose record IS in the catalog.  The catalog
    // claims it now, so its record committed: promote it, atomically, so a LATER
    // loss of that record (CRC truncation, a missing catalog dir) quarantines the
    // dir instead of deleting it.  Residual (not healable here): the record is lost
    // in the SAME crash that lost the rewrite — since a DDL is acked only after its
    // record is HW-committed, that needs a committed-then-lost catalog record.
    // Guards: the on-disk entry name must EQUAL the topic's name (a fold-only match
    // may be another topic's dir — and a path lookup would fold on a
    // case-insensitive filesystem), the dir and the marker are not symlinks, and
    // the marker id equals the claiming topic's id.  Placeholders have no dir.
    auto promoteIfPending = [&](const fs::path& entry) {
        const std::string n = entry.filename().string();
        const TopicSpec* owner = nullptr;
        for (const TopicSpec& t : topics)
            if (t.name == n) { owner = &t; break; }
        if (owner == nullptr || placeholders.count(owner->topicId) != 0) return;
        std::error_code pec;
        const fs::file_status mst = fs::symlink_status(entry / kTopicIdMarkerFile, pec);
        if (pec || !fs::is_regular_file(mst)) return;   // no marker, or a symlinked one
        const fs::path c = fs::canonical(entry, pec);
        if (pec || c.parent_path() != canonicalRoot) return;
        const TopicIdMarker m = readTopicIdMarker(c);
        if (m.kind != MarkerKind::Invalid && m.id != owner->topicId) {
            // Fix round 2 (N2): the exact-claimed dir IS the owner's, but its marker
            // names another id (a stale marker left on a reused orphan dir).  Remove
            // it: a stale "pending <X>" must never arm a delete of the owner's data
            // should X be a placeholder at some later boot.  Removal only ever turns
            // a delete into a quarantine.
            if (clearTopicIdMarker(c.string())) {
                spdlog::warn("topic-dir reconcile: '{}' carried a stale topic-id marker (id {}, owner "
                             "id {}) — removed", c.string(), m.id, owner->topicId);
                result.markersCleared.push_back(c);
            }
            return;
        }
        if (m.kind != MarkerKind::Pending) return;
        if (writeTopicIdMarker(c.string(), kTopicIdMarkerCommitted, m.id)) {
            spdlog::info("topic-dir reconcile: '{}' marker promoted pending -> committed (topic id {} "
                         "is in the catalog)", c.string(), m.id);
            result.promoted.push_back(c);
        }
    };

    std::vector<fs::path> candidates;
    for (fs::directory_iterator it(topicsRoot, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        const fs::file_status st = it->symlink_status(sec);
        if (sec || fs::is_symlink(st) || !fs::is_directory(st)) continue;   // not a topic dir: left alone
        if (claimed(it->path().filename().string())) {
            promoteIfPending(it->path());   // fix round 1b (see above)
            continue;
        }
        if (!isTopicDirLayout(it->path())) {
            spdlog::info("topic-dir reconcile: leaving '{}' alone (not a recognised topic dir layout)",
                         it->path().string());
            continue;
        }
        candidates.push_back(it->path());
    }
    if (ec) {
        spdlog::error("topic-dir reconcile: listing '{}' failed: {} — no further dirs considered",
                      topicsRoot.string(), ec.message());
    }

    const fs::path quarantineRoot = fs::path(dataDir) / "quarantine";
    for (const fs::path& p : candidates) {
        std::error_code cec;
        const fs::path c = fs::canonical(p, cec);
        if (cec || c.parent_path() != canonicalRoot) {   // guard: a direct child of dataDir/topics only
            spdlog::error("topic-dir reconcile: refusing to touch '{}' (outside '{}')",
                          p.string(), canonicalRoot.string());
            ++leftInPlace;
            continue;
        }
        const TopicIdMarker marker = readTopicIdMarker(c);
        const bool isPlaceholderId = marker.id >= 0 && placeholders.count(marker.id) != 0;
        // Fix round 2 (N3): a leftover topic-id.tmp means a marker rewrite (to
        // "committed", from markTopicCommitted or a boot promotion) was interrupted —
        // on-disk evidence the record may have committed.  Never delete then.
        std::error_code tec;
        const bool tmpPresent =
            fs::exists(fs::symlink_status(c / kTopicIdMarkerTmpFile, tec));
        if (marker.kind == MarkerKind::Pending && isPlaceholderId && tmpPresent) {
            spdlog::warn("topic-dir reconcile: '{}' is a pending orphan but holds an interrupted "
                         "marker rewrite ('{}') — quarantining, NOT deleting", c.string(),
                         kTopicIdMarkerTmpFile);
        }
        // Final review C1 (a): an uncommitted topic is unwritable (C1 b), so a
        // never-committed orphan's logs are empty.  One holding bytes (acked data on
        // an older broker) is quarantined, never deleted.
        const bool holdsRecords = marker.kind == MarkerKind::Pending && isPlaceholderId && anyLogBytes(c);
        if (holdsRecords) {
            spdlog::error("topic-dir reconcile: '{}' is the dir of orphan placeholder '{}' but its log holds "
                          "records — quarantining, NOT deleting", c.string(), orphanPlaceholderName(marker.id));
        }
        if (marker.kind == MarkerKind::Pending && isPlaceholderId && !tmpPresent && deletesAllowed &&
            !holdsRecords) {
            spdlog::warn("topic-dir reconcile: deleting '{}' — the data dir of orphan placeholder '{}' "
                         "(a runtime create whose catalog record never committed)",
                         c.string(), orphanPlaceholderName(marker.id));
            std::error_code rec;
            fs::remove_all(c, rec);   // never follows symlinks; the layout check excluded them anyway
            if (!rec) {
                result.deleted.push_back(c);
                continue;
            }
            // Fix round 2 (M2/M3): a failed (possibly partial) delete must not leave the
            // orphan's dir in place for a later same-name create to reuse — quarantine
            // whatever remains (below).
            spdlog::error("topic-dir reconcile: deleting '{}' failed: {} — quarantining what remains",
                          c.string(), rec.message());
        }
        if (marker.kind == MarkerKind::Committed && isPlaceholderId) {
            // I1: the record for this id COMMITTED, yet the catalog no longer has it —
            // the catalog was damaged (CRC truncation, a missing catalog dir).  This is
            // acknowledged data: quarantine it below, never delete.
            spdlog::error("topic-dir reconcile: '{}' holds COMMITTED topic id {} but the catalogs "
                          "have no record for it (damaged / truncated / missing catalog?) — "
                          "quarantining, NOT deleting", c.string(), marker.id);
        }
        // Everything else is MOVED, never deleted (D4).
        std::error_code qec;
        const fs::file_status qst = fs::symlink_status(quarantineRoot, qec);
        if (fs::is_symlink(qst) || (fs::exists(qst) && !fs::is_directory(qst))) {
            spdlog::error("topic-dir reconcile: '{}' is not a plain directory — not quarantining '{}'",
                          quarantineRoot.string(), c.string());
            ++leftInPlace;
            continue;
        }
        qec.clear();
        fs::create_directories(quarantineRoot, qec);
        if (qec) {
            spdlog::error("topic-dir reconcile: cannot create '{}': {}", quarantineRoot.string(), qec.message());
            ++leftInPlace;
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
            ++leftInPlace;
            continue;
        }
        spdlog::warn("topic-dir reconcile: quarantined '{}' -> '{}' — no topic in the catalog claims it "
                     "(removed from RUNE_TOPICS, a typo, or another build's dir); it is NOT restored "
                     "automatically", c.string(), dest.string());
        result.quarantined.push_back(dest);
    }
    // M1: the latch protects unclaimed dirs a damaged boot could not move; once none
    // remains in topics/ there is nothing left to protect.
    if ((latched || !catalogDamage.empty()) && leftInPlace == 0 && !ec) {
        std::error_code rec;
        if (fs::remove(sentinel, rec))
            spdlog::info("topic-dir reconcile: no unclaimed topic dir remains — cleared '{}'", sentinel.string());
    }
    return result;
}

// ---------------------------------------------------------------------------
// writeTopicIdMarker / clearTopicIdMarker (B10 / D4; moved from TopicRegistry, Task 9)
// ---------------------------------------------------------------------------
namespace {
// Ensures `dir` is a real directory (never a symlink).
bool isPlainMarkerDir(const fs::path& dir) {
    std::error_code ec;
    const fs::file_status st = fs::symlink_status(dir, ec);
    return !ec && fs::is_directory(st) && !fs::is_symlink(st);
}
} // namespace

bool
writeTopicIdMarker(const std::string& topicDir, std::string_view state, int32_t id)
{
    const fs::path dir{topicDir};
    const fs::path tmp    = dir / kTopicIdMarkerTmpFile;
    const fs::path marker = dir / kTopicIdMarkerFile;
    if (!isPlainMarkerDir(dir)) {
        spdlog::warn("topic-id marker: not writing topic-id marker — '{}' is not a plain directory",
                     dir.string());
        return false;
    }
    {   // the final path must be absent or a regular file — never a symlink
        std::error_code ec;
        const fs::file_status mst = fs::symlink_status(marker, ec);
        if (fs::exists(mst) && !fs::is_regular_file(mst)) {
            spdlog::warn("topic-id marker: not writing topic-id marker — '{}' is not a regular file",
                         marker.string());
            return false;
        }
    }
    std::error_code rmEc;
    fs::remove(tmp, rmEc);   // unlinks a leftover/planted entry itself, never a symlink's target

    char buf[48];
    const int len = std::snprintf(buf, sizeof buf, "%.*s %d\n",
                                  static_cast<int>(state.size()), state.data(), id);
    if (len <= 0 || static_cast<std::size_t>(len) >= sizeof buf) return false;

    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0644);
    if (fd < 0) {
        spdlog::warn("topic-id marker: cannot create topic-id marker temp '{}': {}", tmp.string(),
                     std::strerror(errno));
        return false;
    }
    bool ok = true;
    std::size_t off = 0;
    while (off < static_cast<std::size_t>(len)) {
        const ssize_t w = ::write(fd, buf + off, static_cast<std::size_t>(len) - off);
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) { ok = false; break; }
        off += static_cast<std::size_t>(w);
    }
    if (::close(fd) != 0) ok = false;
    if (!ok) {
        spdlog::warn("topic-id marker: cannot write topic-id marker temp '{}'", tmp.string());
        std::error_code rec;
        fs::remove(tmp, rec);
        return false;
    }
    std::error_code ec;
    fs::rename(tmp, marker, ec);   // atomic replace: a reader sees the old or the new marker
    if (ec) {
        spdlog::warn("topic-id marker: cannot install topic-id marker '{}': {}", marker.string(),
                     ec.message());
        std::error_code rec;
        fs::remove(tmp, rec);
        return false;
    }
    return true;
}

bool
clearTopicIdMarker(const std::string& topicDir)
{
    const fs::path dir{topicDir};
    if (!isPlainMarkerDir(dir)) return false;
    bool removed = false;
    std::error_code ec;
    if (fs::remove(dir / kTopicIdMarkerFile, ec)) removed = true;   // unlink only
    ec.clear();
    if (fs::remove(dir / kTopicIdMarkerTmpFile, ec)) removed = true;
    return removed;
}


// ---------------------------------------------------------------------------
// catalogLiveTopicSpecs — INTERIM (Task 9): the live topics of the local catalog
// at its checkpoint, for reconcileTopicDirs' claims.
// ---------------------------------------------------------------------------
std::vector<TopicSpec> catalogLiveTopicSpecs(const std::string& dataDir) {
    namespace cat = rune::catalog;
    const fs::path dir = fs::path{dataDir} / "__catalog";
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) return {};
    const std::optional<CatalogCheckpoint> ckpt = readCatalogCheckpoint(dir.string());   // throws: refuse boot
    if (!ckpt) return {};
    rune::storage::Log log(dir.string());
    log.recover();
    cat::CatalogReplayer replayer;
    bool stop = false;
    while (!stop && replayer.nextOffset() <= ckpt->appliedOffset) {
        const int64_t from = replayer.nextOffset();
        const std::vector<uint8_t> data = log.read(from, 1 << 20);
        if (data.empty()) break;
        std::size_t pos = 0;
        while (pos < data.size() && !stop) {
            if (data.size() - pos < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader reader(data.data() + pos, data.size() - pos);
            if (!reader.ensureValid()) { stop = true; break; }
            const auto tl = static_cast<std::size_t>(reader.totalLength());
            if (!reader.isControl()) {
                reader.forEachRecord([&](std::string_view, std::string_view val, int64_t off, int64_t) {
                    if (stop || off != replayer.nextOffset() || off > ckpt->appliedOffset) return;
                    auto r = replayer.feed(off, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()),
                                                                        val.size()));
                    if (!r) {
                        spdlog::error("catalogLiveTopicSpecs: the local __catalog stops at {} — {} (the applier halts "
                                      "on it; topic dirs claimed from the prefix before it)", off,
                                      cat::describe(r.error()));
                        stop = true;
                    }
                });
            }
            if (tl == 0) { stop = true; break; }
            pos += tl;
        }
        if (replayer.nextOffset() == from) break;   // no progress: a gap / control batch past it
    }
    std::vector<TopicSpec> out;
    for (const cat::TopicObject* t : replayer.state().topics()) {
        TopicSpec s;
        s.name           = t->name;
        s.topicId        = t->topicId;
        s.partitionCount = t->partitions;
        s.persistence    = t->persistence == cat::Persistence::Transient ? PersistenceMode::Transient
                                                                         : PersistenceMode::Disk;
        out.push_back(std::move(s));
    }
    for (const cat::ViewObject* v : replayer.state().views()) {
        TopicSpec s;
        s.name           = v->name;   // the view's sink topic is named after the view
        s.topicId        = v->def.viewTopicId;
        s.partitionCount = v->def.sinkPartitions;
        s.persistence    = v->def.persistence == cat::Persistence::Transient ? PersistenceMode::Transient
                                                                             : PersistenceMode::Disk;
        out.push_back(std::move(s));
    }
    return out;
}

// ---------------------------------------------------------------------------
// staticCatalogCollision — INTERIM (Task 10 fix round 1): a static topic and a
// catalog topic never share a broker.
// ---------------------------------------------------------------------------
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
