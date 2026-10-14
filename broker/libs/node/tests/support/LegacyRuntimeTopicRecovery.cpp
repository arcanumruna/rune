// LegacyRuntimeTopicRecovery.cpp — TEST-ONLY (catalog history Task 9 fix round 1,
// Minor 5): the boot recovery of runtime topics from the retired __topic_schemas /
// __views catalogs, moved verbatim out of the production library (no production
// code reads those catalogs any more — spec §1, no migration).  It survives only so
// RuntimeTopicRecoveryTest can still drive boot's topic-dir reconcile over the dirs
// a pre-Task-9 broker left; Task 16 deletes it with RuntimeTopicRecovery.
#include "support/LegacyRuntimeTopicRecovery.h"

#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>   // TopicSchema
#include <rune/node/ViewRegistry.h>     // ViewRecord

#include <rune/catalog/TopicName.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <climits>
#include <filesystem>
#include <iterator>
#include <map>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

namespace fs = std::filesystem;

namespace rune::node {

using rune::catalog::escapeNameForLog;
using rune::catalog::topicNamesFoldEqual;

namespace {

constexpr int32_t kChunkBytes = 64 * 1024 * 1024;

[[noreturn]] void corrupt(const char* catalog, int64_t batchBase, const char* what) {
    throw std::runtime_error(std::string("runtime topic recovery: ") + catalog +
                             " record at batch baseOffset=" + std::to_string(batchBase) +
                             " — " + what + "; refusing to boot on a damaged topic catalog");
}

// Calls fn(value, batchBaseOffset) for every record value in the log under
// `dir`, oldest first.  A missing directory is an empty catalog.
//
// Fix round 2b: reports what recovery found.  Truncated = Log::recover() cut
// bytes (a CRC / torn tail, or later segments deleted on a chain gap) — measured
// as the catalog's total segment bytes before vs after recover() (LogSegment
// ftruncates to the last valid batch; segments are not preallocated).
enum class CatalogState : uint8_t { Missing, Intact, Truncated };

int64_t segmentBytes(const fs::path& dir) {
    int64_t total = 0;
    std::error_code ec;
    for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code sec;
        if (it->path().extension() != ".log" || !it->is_regular_file(sec) || sec) continue;
        const auto sz = it->file_size(sec);
        if (!sec) total += static_cast<int64_t>(sz);
    }
    return total;
}

template <class Fn>
CatalogState forEachCatalogValue(const fs::path& dir, const char* catalog, Fn&& fn) {
    std::error_code ec;
    if (!fs::is_directory(dir, ec)) return CatalogState::Missing;

    const int64_t bytesBefore = segmentBytes(dir);
    rune::storage::Log log(dir.string());
    log.recover();
    const CatalogState state =
        segmentBytes(dir) < bytesBefore ? CatalogState::Truncated : CatalogState::Intact;
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
    return state;
}

struct DefinedTopic {
    std::string name;
    int32_t     partitionCount = 1;
    uint16_t    version        = 0;   // latest version seen
    PersistenceMode persistence = PersistenceMode::Disk;   // latest version's (Phase 2b E)
};

} // namespace

void refuseStaticNamesCollidingWithRecovered(const std::vector<TopicSpec>& statics,
                                             const std::vector<TopicSpec>& catalogTopics) {
    for (const TopicSpec& r : catalogTopics) {
        for (std::size_t i = 0; i < statics.size(); ++i) {
            const TopicSpec& st = statics[i];
            // A static topic's id is its RUNE_TOPICS position (TopicRegistry assigns
            // ids sequentially), whatever the spec's topicId field says.
            const int32_t staticId = static_cast<int32_t>(i);
            const bool exact = st.name == r.name;
            if (!exact && !topicNamesFoldEqual(st.name, r.name)) continue;
            if (exact && staticId == r.topicId) continue;   // one topic: a static typed by DDL
            throw std::runtime_error(
                "RUNE_TOPICS topic '" + escapeNameForLog(st.name) + "' " +
                (st.name == r.name ? "has the same name as" : "differs only by letter case from") +
                " the runtime topic '" + escapeNameForLog(r.name) + "' (id " + std::to_string(r.topicId) +
                ", created by DefineTopic / CREATE VIEW and recorded in the admin catalogs; the RUNE_TOPICS "
                "entry is id " + std::to_string(staticId) + ") — both would own dataDir/topics/" +
                escapeNameForLog(r.name) + "/; refusing to boot. Remove or rename the RUNE_TOPICS entry (a "
                "runtime topic is managed by DDL, not RUNE_TOPICS).");
        }
    }
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
    const CatalogState schemasState = forEachCatalogValue(root / "__topic_schemas", "__topic_schemas",
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

    std::map<int32_t, std::string> catalogNames;   // I3: every id the catalogs name
    for (const auto& [id, t] : defined) catalogNames[id] = t.name;
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
    const CatalogState viewsState = forEachCatalogValue(root / "__views", "__views",
                        [&](std::span<const uint8_t> val, int64_t batchBase) {
        auto rec = ViewRecord::deserialize(val);
        if (!rec) corrupt("__views", batchBase, "ViewRecord::deserialize failed");
        if (rec->viewTopicId >= 0) catalogNames[rec->viewTopicId] = rec->viewName;   // I3
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
    // Fix round 2b: a damaged catalog (cut by recovery, or one catalog missing
    // while the other exists) means a gap may be a LOST committed record, not an
    // orphan — reconcile then deletes nothing (quarantines instead).
    auto stateText = [](CatalogState s) {
        return s == CatalogState::Missing ? "missing" : s == CatalogState::Truncated ? "truncated by recovery" : "intact";
    };
    if (schemasState == CatalogState::Truncated || viewsState == CatalogState::Truncated ||
        (schemasState == CatalogState::Missing) != (viewsState == CatalogState::Missing)) {
        result.catalogDamage = std::string("__topic_schemas ") + stateText(schemasState) +
                               ", __views " + stateText(viewsState);
    }
    for (const auto& [id, name] : catalogNames) {
        TopicSpec t;
        t.topicId = id;
        t.name    = name;
        result.catalogTopics.push_back(std::move(t));
    }
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
