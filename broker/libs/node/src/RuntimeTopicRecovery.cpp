// libs/node/src/RuntimeTopicRecovery.cpp — see the header for the contract.
#include <rune/node/RuntimeTopicRecovery.h>

#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>   // TopicSchema
#include <rune/node/ViewRegistry.h>     // ViewRecord

#include <rune/codec/Serialize.h>       // rune::codec::deserialize
#include <rune/protocol/RecordBatch.h>  // RecordBatchReader
#include <rune/storage/Log.h>
#include <rune/view/ViewPlan.h>         // deserializeCompiledViewPlan

#include <spdlog/spdlog.h>

#include <algorithm>
#include <filesystem>
#include <map>
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

std::vector<TopicSpec> recoverRuntimeTopicSpecs(const std::string& dataDir,
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
    // B5 (DDL conformance, D1): a missing id is an orphan (its DDL never committed a
    // record).  Fill it with a hidden, inert, ZERO-partition placeholder so every
    // later topic keeps its id; log all placeholders of this boot ONCE.
    std::vector<TopicSpec> out;
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
    return out;
}

} // namespace rune::node
