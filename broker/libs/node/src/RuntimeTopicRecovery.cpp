// libs/node/src/RuntimeTopicRecovery.cpp — see the header for the contract.
#include <rune/node/RuntimeTopicRecovery.h>

#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>   // TopicSchema
#include <rune/node/TopicName.h>        // topicNamesFoldEqual
#include <rune/node/ViewRegistry.h>     // ViewRecord

#include <rune/codec/Serialize.h>       // rune::codec::deserialize
#include <rune/gateway/TopicRegistry.h> // kTopicIdMarkerFile
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>         // deserializeCompiledViewPlan

#include <spdlog/spdlog.h>

#include <algorithm>
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

namespace fs = std::filesystem;

namespace rune::node {

namespace {

constexpr int32_t kChunkBytes = 64 * 1024 * 1024;

[[noreturn]] void corrupt(const char* catalog, int64_t batchBase, const char* what) {
    throw std::runtime_error(std::string("runtime topic recovery: ") + catalog +
                             " record at batch baseOffset=" + std::to_string(batchBase) +
                             " — " + what + "; refusing to boot on a damaged topic catalog");
}

// Calls fn(value, batchBaseOffset) for every record value in the log under
// `dir`, oldest first.  A missing directory is an empty catalog.
template <class Fn>
void forEachCatalogValue(const fs::path& dir, const char* catalog, Fn&& fn) {
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) return;

    rune::storage::Log log(dir.string());
    log.recover();
    int64_t       off = log.logStartOffset();
    const int64_t end = log.logEndOffset();

    while (off < end) {
        const std::vector<uint8_t> data = log.read(off, kChunkBytes);
        if (data.empty()) break;

        std::size_t pos  = 0;
        int64_t     next = off;
        while (pos < data.size()) {
            const std::size_t rem = data.size() - pos;
            if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader reader(data.data() + pos, rem);
            if (!reader.ensureValid()) break;
            const int32_t tl = reader.totalLength();
            if (tl <= 0 || static_cast<std::size_t>(tl) > rem) break;

            const int64_t batchBase = reader.baseOffset();
            reader.forEachRecord([&](std::string_view /*key*/, std::string_view val,
                                     int64_t /*offset*/, int64_t /*ts*/) {
                fn(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()),
                                            val.size()),
                   batchBase);
            });
            next = std::max<int64_t>(next, batchBase + reader.lastOffsetDelta() + 1);
            pos += static_cast<std::size_t>(tl);
        }
        // recover() already truncated any torn tail, so every byte below the LEO
        // must parse; no progress means the catalog itself is damaged.
        if (next <= off) corrupt(catalog, off, "no readable batch");
        off = next;
    }
}

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
        if ((child == rune::gateway::kTopicIdMarkerFile ||
             child == rune::gateway::kTopicIdMarkerTmpFile) && fs::is_regular_file(st))
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

/// The parsed topic-id marker (see rune::gateway::kTopicIdMarkerFile).
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
    std::ifstream in(topicDir / rune::gateway::kTopicIdMarkerFile, std::ios::binary);
    if (!in) return {};
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (!text.empty() && text.back() == '\n') text.pop_back();
    const std::size_t sp = text.find(' ');
    if (sp == std::string::npos) return {};
    const std::string_view state(text.data(), sp);
    const std::string digits = text.substr(sp + 1);
    MarkerKind kind = MarkerKind::Invalid;
    if (state == rune::gateway::kTopicIdMarkerPending) kind = MarkerKind::Pending;
    else if (state == rune::gateway::kTopicIdMarkerCommitted) kind = MarkerKind::Committed;
    else return {};
    if (!allDigits(digits) || digits.size() > 10) return {};
    const int64_t id = std::stoll(digits);
    if (id > INT32_MAX) return {};
    return {kind, static_cast<int32_t>(id)};
}

struct DefinedTopic {
    std::string name;
    int32_t     partitionCount = 1;
    uint16_t    version        = 0;   // latest version seen
    PersistenceMode persistence = PersistenceMode::Disk;   // latest version's (Phase 2b E)
};

} // namespace

std::string orphanPlaceholderName(int32_t topicId) {
    return "__orphan." + std::to_string(topicId);
}

TopicDirReconciliation reconcileTopicDirs(const std::string& dataDir,
                                          const std::vector<TopicSpec>& topics,
                                          const std::vector<int32_t>& placeholderIds,
                                          int64_t nowUnixSeconds) {
    TopicDirReconciliation result;
    std::error_code ec;
    const fs::path topicsRoot = fs::path(dataDir) / "topics";
    const fs::file_status rootSt = fs::symlink_status(topicsRoot, ec);
    if (ec || !fs::is_directory(rootSt)) return result;   // absent, or a symlink: never followed
    const fs::path canonicalRoot = fs::canonical(topicsRoot, ec);
    if (ec) return result;

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
        const fs::file_status mst = fs::symlink_status(entry / rune::gateway::kTopicIdMarkerFile, pec);
        if (pec || !fs::is_regular_file(mst)) return;   // no marker, or a symlinked one
        const fs::path c = fs::canonical(entry, pec);
        if (pec || c.parent_path() != canonicalRoot) return;
        const TopicIdMarker m = readTopicIdMarker(c);
        if (m.kind != MarkerKind::Pending || m.id != owner->topicId) return;
        if (rune::gateway::writeTopicIdMarker(c.string(), rune::gateway::kTopicIdMarkerCommitted, m.id)) {
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
            continue;
        }
        const TopicIdMarker marker = readTopicIdMarker(c);
        const bool isPlaceholderId = marker.id >= 0 && placeholders.count(marker.id) != 0;
        if (marker.kind == MarkerKind::Pending && isPlaceholderId) {
            spdlog::warn("topic-dir reconcile: deleting '{}' — the data dir of orphan placeholder '{}' "
                         "(a runtime create whose catalog record never committed)",
                         c.string(), orphanPlaceholderName(marker.id));
            std::error_code rec;
            fs::remove_all(c, rec);   // never follows symlinks; the layout check excluded them anyway
            if (rec) spdlog::error("topic-dir reconcile: deleting '{}' failed: {}", c.string(), rec.message());
            else result.deleted.push_back(c);
            continue;
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
        spdlog::warn("topic-dir reconcile: quarantined '{}' -> '{}' — no topic in the catalog claims it "
                     "(removed from RUNE_TOPICS, a typo, or another build's dir); it is NOT restored "
                     "automatically", c.string(), dest.string());
        result.quarantined.push_back(dest);
    }
    return result;
}

std::vector<TopicSpec> recoverRuntimeTopicSpecs(const std::string& dataDir,
                                                int32_t firstRuntimeId) {
    return recoverRuntimeTopics(dataDir, firstRuntimeId).specs;
}

RecoveredRuntimeTopics recoverRuntimeTopics(const std::string& dataDir,
                                            int32_t firstRuntimeId) {
    const fs::path root{dataDir};

    // ── __topic_schemas: every DefineTopic topic (ALL ids — a view over a static
    //    source needs that source's partition count too). ──────────────────────
    std::map<int32_t, DefinedTopic> defined;
    forEachCatalogValue(root / "__topic_schemas", "__topic_schemas",
                        [&](std::span<const uint8_t> val, int64_t batchBase) {
        auto ts = TopicSchema::deserialize(val);
        if (!ts) corrupt("__topic_schemas", batchBase, "TopicSchema::deserialize failed");
        if (ts->topicId < 0) return;   // internal topic

        DefinedTopic& t = defined[ts->topicId];
        if (ts->version < t.version) return;   // keep the latest version's facts
        t.version        = ts->version;
        t.partitionCount = ts->partitionCount;
        t.persistence    = ts->persistence;
        if (!ts->topicName.empty()) {
            t.name = ts->topicName;
        } else if (t.name.empty()) {
            // Legacy record (written before the topicName trailer): the
            // descriptor name is the only name it carries.
            try {
                t.name = rune::codec::deserialize(ts->descriptorBlob).name;
            } catch (const std::exception& ex) {
                corrupt("__topic_schemas", batchBase, ex.what());
            }
            spdlog::warn("runtime topic recovery: topic id={} has no durable topic name "
                         "(pre-trailer record) — recovering it as '{}' (its descriptor name)",
                         ts->topicId, t.name);
        }
    });

    std::map<int32_t, TopicSpec> runtime;
    for (const auto& [id, t] : defined) {
        if (id < firstRuntimeId) continue;   // a static topic: booted from config
        TopicSpec spec;
        spec.topicId        = id;
        spec.name           = t.name;
        spec.partitionCount = t.partitionCount;
        // Phase 2b E: a transient topic comes back TRANSIENT — TopicRegistry skips
        // its disk dirs and buildReplication puts it on a fresh (empty) InMemoryLog.
        spec.persistence    = t.persistence;
        // AdminWorker DefineTopic → requestCreateTopic(name, partitions, keyField="",
        // cleanupPolicy=Delete, persistence): defaults already match otherwise.
        runtime.emplace(id, std::move(spec));
    }

    // ── __views: every view's sink topic. ────────────────────────────────────
    forEachCatalogValue(root / "__views", "__views",
                        [&](std::span<const uint8_t> val, int64_t batchBase) {
        auto rec = ViewRecord::deserialize(val);
        if (!rec) corrupt("__views", batchBase, "ViewRecord::deserialize failed");
        if (rec->viewTopicId < firstRuntimeId) return;   // sink resolved to a static topic

        // Mirror AdminWorker::startCreateView_: partitions = the source's (or 1),
        // keyed + Compact, partition key = the output descriptor's key field.
        int32_t partitions = 1;
        if (auto plan = rune::view::deserializeCompiledViewPlan(rec->compiledPlanBlob);
            plan && !plan->sourceTopicId.empty()) {
            if (auto it = defined.find(plan->sourceTopicId[0]); it != defined.end())
                partitions = it->second.partitionCount;
        } else {
            corrupt("__views", batchBase, "compiled plan missing/un-deserializable");
        }
        std::string keyField;
        if (!rec->outputDescriptorBlob.empty()) {
            try {
                const auto out = rune::codec::deserialize(rec->outputDescriptorBlob);
                for (const auto& f : out.fields) {
                    if (f.isKey) { keyField = f.name; break; }
                }
            } catch (const std::exception& ex) {
                corrupt("__views", batchBase, ex.what());
            }
        }

        TopicSpec spec;
        spec.topicId        = rec->viewTopicId;
        spec.name           = rec->viewName;
        spec.partitionCount = partitions;
        spec.cleanupPolicy  = TopicSpec::CleanupPolicy::Compact;
        spec.partitionKey   = keyField;

        // A compacted catalog may hold several records for one view (latest
        // wins); a view id also claimed by a DefineTopic record is impossible
        // unless the catalogs disagree.
        if (defined.count(rec->viewTopicId) != 0) {
            corrupt("__views", batchBase, "view sink id is also a DefineTopic topic id");
        }
        runtime.insert_or_assign(rec->viewTopicId, std::move(spec));
    });

    // ── contiguity: TopicRegistry hands out ids sequentially. ────────────────
    // B5 (DDL conformance, D1): a missing id is usually an orphan (its DDL never
    // committed a record) — but a damaged catalog (CRC truncation, a missing catalog
    // dir) also leaves gaps, so a placeholder never by itself authorises deleting
    // data: reconcileTopicDirs deletes only a "pending"-marked dir (fix round 1, I1).
    // Fill it with a hidden, inert, ZERO-partition placeholder so every later topic
    // keeps its id; log all placeholders of this boot ONCE.
    RecoveredRuntimeTopics result;
    std::vector<TopicSpec>& out = result.specs;
    out.reserve(runtime.size());
    std::string placeholders;   // "__orphan.1, __orphan.2" for the single log line
    int32_t expected = firstRuntimeId;
    for (auto& [id, spec] : runtime) {
        while (expected < id) {
            TopicSpec ph;
            ph.topicId        = expected;
            ph.name           = orphanPlaceholderName(expected);
            ph.partitionCount = 0;
            ph.persistence    = PersistenceMode::Transient;
            if (!placeholders.empty()) placeholders += ", ";
            placeholders += ph.name;
            out.push_back(std::move(ph));
            result.placeholderIds.push_back(expected);
            ++expected;
        }
        out.push_back(std::move(spec));
        ++expected;
    }
    if (!placeholders.empty()) {
        spdlog::warn("runtime topic recovery: topic id(s) with no catalog record (a DefineTopic/"
                     "CreateView that never committed left them behind) recreated as hidden "
                     "zero-partition placeholders so later topics keep their ids: {}", placeholders);
    }
    return result;
}

} // namespace rune::node
