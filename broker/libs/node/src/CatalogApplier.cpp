// CatalogApplier.cpp — offset-ordered, verified apply of `__catalog` (catalog
// history spec §5.3, ruling 8).  See CatalogApplier.h for the step list.
#include <rune/node/CatalogApplier.h>

#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/PersistenceMode.h>
#include <rune/node/SchemaRegistry.h>
#include <rune/node/ViewRegistry.h>

#include <rune/catalog/CatalogReplayer.h>
#include <rune/codec/Serialize.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>
#include <rune/storage/LogReadView.h>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <utility>

namespace rune::node {

namespace cat = rune::catalog;

namespace {

/// The most bytes one readInto asks for: half the reused buffer, so the read's
/// index-interval slack always fits (Log::readInto answers a whole-batch prefix).
constexpr int32_t kReadMaxBytes = static_cast<int32_t>(kCatalogReadChunk / 2);
/// A single batch larger than the reused buffer (never produced by the admin
/// write path) is read once through the allocating path — cold, logged.
constexpr int32_t kOversizeReadMaxBytes = 64 * 1024 * 1024;

PersistenceMode persistenceOf(cat::Persistence p) noexcept {
    return p == cat::Persistence::Transient ? PersistenceMode::Transient : PersistenceMode::Disk;
}

TopicSchema topicSchemaOf(const cat::TopicObject& t) {
    const cat::SchemaState& s = *t.schema;
    TopicSchema ts;
    ts.topicId        = t.topicId;
    ts.version        = s.schemaVersion;
    ts.partitionCount = t.partitions;
    ts.keyField       = s.keyField;
    ts.validate       = s.validate;
    ts.descriptorBlob = s.evolvingBlob;
    ts.nextTag        = s.nextTag;
    ts.compatMode     = s.compat;
    ts.topicName      = t.name;
    ts.persistence    = persistenceOf(t.persistence);
    return ts;
}

SinkSpec sinkSpecOf(const cat::ViewObject& v) {
    return SinkSpec{v.def.viewTopicId, v.name, v.def.sinkPartitions, v.def.persistence, v.def.keyField};
}

cat::ApplyError decodeFailure(int64_t offset, cat::DecodeError e) {
    cat::ApplyError err;
    err.offset = offset;
    err.code   = e == cat::DecodeError::UnknownFormatVer ? cat::ApplyError::Code::UnsupportedFormat
                                                         : cat::ApplyError::Code::BadOps;
    err.detail = "the record does not decode: " + std::string{cat::decodeErrorName(e)};
    return err;
}

cat::ApplyError localFailure(int64_t offset, cat::ApplyError::Code code, std::string detail) {
    cat::ApplyError err;
    err.offset = offset;
    err.code   = code;
    err.detail = std::move(detail);
    return err;
}

} // namespace

CatalogApplier::CatalogApplier(std::string catalogDir, SchemaRegistry& schemas, ViewRegistry& views)
    : dir_(std::move(catalogDir)), schemas_(&schemas), views_(&views),
      st_(Applying{0}), phase_(AwaitingBoot{}), readBuf_(kCatalogReadChunk) {}

const cat::ApplyError* CatalogApplier::haltError() const noexcept {
    const Halted* h = std::get_if<Halted>(&st_);
    return h ? &h->error : nullptr;
}

int64_t CatalogApplier::nextOffset() const noexcept {
    const Applying* a = std::get_if<Applying>(&st_);
    return a ? a->nextOffset : -1;
}

std::string_view CatalogApplier::stateName() const noexcept {
    return std::holds_alternative<Applying>(st_) ? "Applying" : "Halted";
}

std::string_view CatalogApplier::phaseName() const noexcept {
    switch (phase_.index()) {
    case 0:  return "AwaitingBoot";
    case 1:  return "Booted";
    default: return "Live";
    }
}

const CatalogEffects* CatalogApplier::sink_() const noexcept {
    if (const Booted* b = std::get_if<Booted>(&phase_)) return &b->effects.sink;
    if (const Live* l = std::get_if<Live>(&phase_)) return &l->effects.sink;
    return nullptr;
}

// ─── replayLocal — the one boot replay of [0, checkpoint] ────────────────────
int CatalogApplier::replayLocal(rune::replication::PartitionReplica& r, BootEffects boot) {
    if (!std::holds_alternative<AwaitingBoot>(phase_)) {
        spdlog::error("CatalogApplier: replayLocal refused in phase {} — the boot prefix is applied exactly once "
                      "(a second replay would re-apply recorded request ids)", phaseName());
        return -1;
    }
    phase_ = Booted{std::move(boot)};

    // Throws (refuse to start) on an unreadable / corrupt checkpoint.
    const std::optional<CatalogCheckpoint> ckpt = readCatalogCheckpoint(dir_);
    const rune::storage::Log* log = r.log();
    const int64_t leo = log ? log->logEndOffset() : r.logEndOffset();
    if (auto refusal = catalogCheckpointBeyondLog(ckpt, leo, dir_)) {
        spdlog::critical("CatalogApplier: {}", *refusal);
        throw std::runtime_error(*refusal);
    }
    if (!ckpt) {
        spdlog::info("CatalogApplier: no applied checkpoint in {} — nothing to replay (log end {})", dir_, leo);
        return 0;
    }

    const int applied = applyRange_(r, ckpt->appliedOffset + 1);
    if (halted()) return applied;   // explicit already (critical + the boot sink's `halted`)
    if (nextOffset() != ckpt->appliedOffset + 1) {
        halt_(localFailure(nextOffset(), cat::ApplyError::Code::Precondition,
                           "boot replay stopped at offset " + std::to_string(nextOffset()) +
                               " before the checkpointed offset " + std::to_string(ckpt->appliedOffset) +
                               " — the local __catalog log is missing or cannot serve applied events"));
        return applied;
    }
    const uint64_t digest = state_.digest();
    if (digest != ckpt->digest) {
        cat::ApplyError err = localFailure(ckpt->appliedOffset, cat::ApplyError::Code::FingerprintMismatch,
                                           "boot replay of [0, " + std::to_string(ckpt->appliedOffset) +
                                               "] ends at a catalog digest other than the checkpoint's — local "
                                               "__catalog log damage");
        err.expected = ckpt->digest;
        err.actual   = digest;
        halt_(std::move(err));
        return applied;
    }
    spdlog::info("CatalogApplier: boot replay applied {} event(s), [0, {}], digest {:#x} verified",
                 applied, ckpt->appliedOffset, digest);
    return applied;
}

void CatalogApplier::goLive(LiveEffects live) {
    if (std::holds_alternative<Live>(phase_)) {
        spdlog::error("CatalogApplier: goLive refused — already Live");
        return;
    }
    phase_ = Live{std::move(live)};
}

// ─── applyCommitted — steady state: [nextOffset, HW) ──────────────────────────
int CatalogApplier::applyCommitted(rune::replication::PartitionReplica& r) {
    if (!std::holds_alternative<Live>(phase_)) return 0;
    const Applying* a = std::get_if<Applying>(&st_);
    if (a == nullptr) return 0;   // halted: no further apply, ever
    const int64_t hw = r.highWatermark();
    if (a->nextOffset >= hw) return 0;   // idle: one compare, no allocation
    return applyRange_(r, hw);
}

// ─── applyRange_ — read [nextOffset, bound) and apply record by record ───────
int CatalogApplier::applyRange_(rune::replication::PartitionReplica& r, int64_t bound) {
    rune::storage::Log* log = r.log();
    if (log == nullptr) {
        halt_(localFailure(nextOffset(), cat::ApplyError::Code::Precondition,
                           "the __catalog replica has no disk log (the catalog is always disk-backed)"));
        return 0;
    }
    int applied = 0;
    std::vector<uint8_t> oversize;   // stays empty (no allocation) unless a batch exceeds readBuf_
    while (const Applying* a = std::get_if<Applying>(&st_)) {
        const int64_t from = a->nextOffset;
        if (from >= bound) break;

        rune::storage::ReadIntoStatus st = rune::storage::ReadIntoStatus::Ok;
        int err = 0;
        const rune::storage::LogReadView v =
            log->readInto(from, kReadMaxBytes, readBuf_.data(), readBuf_.size(), st, bound, &err);
        std::span<const uint8_t> bytes;
        if (st == rune::storage::ReadIntoStatus::FirstTooLarge) {
            spdlog::warn("CatalogApplier: the batch at offset {} exceeds the {} B read buffer — read once "
                         "through the allocating path", from, readBuf_.size());
            oversize = log->read(from, kOversizeReadMaxBytes);
            bytes = std::span<const uint8_t>(oversize.data(), oversize.size());
        } else if (st == rune::storage::ReadIntoStatus::Unreadable || st == rune::storage::ReadIntoStatus::IoError) {
            halt_(localFailure(from, cat::ApplyError::Code::Precondition,
                               std::string{"the local __catalog log cannot be read at this offset ("} +
                                   (st == rune::storage::ReadIntoStatus::IoError
                                        ? "I/O error: " + std::string{std::strerror(err)}
                                        : std::string{"torn or corrupt bytes"}) +
                                   ")"));
            break;
        } else if (!v.empty()) {
            bytes = std::span<const uint8_t>(v.data, v.len);
        }
        if (bytes.empty()) break;   // nothing readable yet below the bound: retried next pass

        std::size_t pos = 0;
        while (pos < bytes.size() && !halted()) {
            const uint8_t*    bs  = bytes.data() + pos;
            const std::size_t rem = bytes.size() - pos;
            if (rem < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) break;
            rune::protocol::RecordBatchReader reader(bs, rem);
            if (!reader.ensureValid()) {
                halt_(localFailure(reader.baseOffset(), cat::ApplyError::Code::Precondition,
                                   "a __catalog batch fails its CRC / length check"));
                break;
            }
            const auto tl = static_cast<std::size_t>(reader.totalLength());
            if (reader.isControl()) {
                // Not a catalog event (never written by the catalog leader); its
                // offsets are consumed so the next real event is not a gap.
                const int64_t end = reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (Applying* cur = std::get_if<Applying>(&st_); cur && end > cur->nextOffset)
                    cur->nextOffset = std::min(end, bound);
                pos += tl;
                continue;
            }
            reader.forEachRecord([&](std::string_view /*key*/, std::string_view val, int64_t off, int64_t /*ts*/) {
                const Applying* cur = std::get_if<Applying>(&st_);
                if (cur == nullptr || off >= bound) return;   // halted, or past the commit bound
                if (off < cur->nextOffset) return;            // applied before (an earlier pass)
                if (off != cur->nextOffset) {
                    halt_(localFailure(cur->nextOffset, cat::ApplyError::Code::Precondition,
                                       "a gap in __catalog: the next record is at offset " + std::to_string(off)));
                    return;
                }
                if (applyRecord_(off, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(val.data()),
                                                               val.size())))
                    ++applied;
            });
            pos += tl;
        }
        const Applying* after = std::get_if<Applying>(&st_);
        if (after == nullptr || after->nextOffset == from) break;   // halted, or no progress this read
    }
    return applied;
}

// ─── applyRecord_ — steps 2–7 for ONE record ─────────────────────────────────
bool CatalogApplier::applyRecord_(int64_t offset, std::span<const uint8_t> bytes) {
    auto ev = cat::decode(bytes);
    if (!ev) {
        halt_(decodeFailure(offset, ev.error()));
        return false;
    }
    if (tamperXor_ != 0) [[unlikely]] {
        ev->fingerprint ^= tamperXor_;
        tamperXor_ = 0;
    }
    auto a = cat::apply(state_, *ev, offset);
    if (!a) {
        halt_(std::move(a.error()));
        return false;
    }
    const uint64_t digest = state_.digest();

    // Step 4 (Live only): the checkpoint precedes every side effect (ruling 8).
    if (std::holds_alternative<Live>(phase_) && !writeCatalogCheckpoint(dir_, CatalogCheckpoint{offset, digest})) {
        const int e = errno;
        cat::ApplyError err = localFailure(offset, cat::ApplyError::Code::Precondition,
                                           "the applied checkpoint could not be made durable in " + dir_ +
                                               " (errno " + std::to_string(e) + ": " + std::strerror(e) +
                                               ") — storage failure; no effect of this event was posted");
        const CatalogEffects* sink = sink_();
        if (sink && sink->storageFailed) sink->storageFailed(e, dir_);
        halt_(std::move(err));
        return false;
    }

    // Step 5: the admin-owned projections.
    try {
        project_(*ev, *a);
    } catch (const std::exception& ex) {
        halt_(localFailure(offset, cat::ApplyError::Code::BadOps,
                           std::string{"projecting "} + std::string{cat::kindName(ev->kind)} + " '" +
                               ev->objectName + "' into the registries failed: " + ex.what()));
        return false;
    }

    // Step 6: the effects, into the CURRENT phase's sink.
    std::get<Applying>(st_).nextOffset = offset + 1;
    if (const CatalogEffects* sink = sink_()) effects_(*sink, *ev, *a);

    // Step 7.
    lastApplied_ = AppliedAt{offset, *a};
    if (const CatalogEffects* sink = sink_(); sink && sink->applied) sink->applied(offset, digest);
    return true;
}

void CatalogApplier::project_(const cat::CatalogEvent& e, const cat::Applied& a) {
    switch (e.kind) {
    case cat::EventKind::CreateTopic:
    case cat::EventKind::CreateSchema:
    case cat::EventKind::AlterSchema:
    case cat::EventKind::SetCompatibility: {
        const cat::TopicObject* t = state_.topicByObjectId(e.objectId);
        if (t != nullptr && t->schema) schemas_->applyRecord(topicSchemaOf(*t));
        return;
    }
    case cat::EventKind::DropTopic:
        (void)schemas_->erase(a.droppedTopicId);
        return;
    case cat::EventKind::AlterTopicConfig:
        return;   // the registries hold no topic config
    case cat::EventKind::AlterView:
    case cat::EventKind::DropView:
        (void)views_->erase(a.droppedTopicId);
        (void)schemas_->erase(a.droppedTopicId);   // the retired sink's typed output
        if (e.kind == cat::EventKind::DropView) return;
        [[fallthrough]];
    case cat::EventKind::CreateView: {
        const cat::ViewObject* v = state_.viewByObjectId(e.objectId);
        if (v == nullptr) return;
        ViewRecord rec;
        rec.viewTopicId          = v->def.viewTopicId;
        rec.viewName             = v->name;
        rec.compiledPlanBlob     = v->def.compiledPlanBlob;
        rec.persistence          = persistenceToWire(persistenceOf(v->def.persistence));
        rec.outputDescriptorBlob = v->def.outputDescriptorBlob;
        views_->applyRecord(rec);
        // The view's OUTPUT is a typed topic (GetTopicSchema / openView), indexed by
        // the VIEW name — exactly as commitCreateView_ registers it.
        if (!v->def.outputDescriptorBlob.empty()) {
            const rune::codec::Descriptor out = rune::codec::deserialize(
                std::span<const uint8_t>(v->def.outputDescriptorBlob.data(), v->def.outputDescriptorBlob.size()));
            TopicSchema ts;
            ts.topicId        = v->def.viewTopicId;
            ts.version        = 1;
            ts.partitionCount = v->def.sinkPartitions;
            ts.keyField       = v->def.keyField;
            ts.validate       = false;
            ts.descriptorBlob = v->def.outputDescriptorBlob;
            ts.nextTag        = static_cast<uint16_t>(out.fields.size());
            ts.topicName      = v->name;
            ts.persistence    = persistenceOf(v->def.persistence);
            schemas_->applyRecord(ts);
        }
        return;
    }
    }
}

void CatalogApplier::effects_(const CatalogEffects& sink, const cat::CatalogEvent& e, const cat::Applied& a) {
    switch (e.kind) {
    case cat::EventKind::CreateTopic:
        if (const cat::TopicObject* t = state_.topicByObjectId(e.objectId); t && sink.topicApplied) sink.topicApplied(*t);
        return;
    case cat::EventKind::DropTopic:
        if (sink.topicDropped) sink.topicDropped(a.droppedTopicId, e.objectName);
        return;
    case cat::EventKind::AlterTopicConfig: {
        const cat::TopicObject* t = state_.topicByObjectId(e.objectId);
        const auto* ops = std::get_if<cat::AlterTopicConfigOps>(&e.ops);
        if (t && ops && sink.topicConfig)
            sink.topicConfig(t->topicId, std::span<const cat::ConfigChange>(ops->changes.data(), ops->changes.size()));
        return;
    }
    case cat::EventKind::CreateSchema:
    case cat::EventKind::AlterSchema:
    case cat::EventKind::SetCompatibility:
        if (const cat::TopicObject* t = state_.topicByObjectId(e.objectId); t && sink.schemaChanged) sink.schemaChanged(*t);
        return;
    case cat::EventKind::CreateView:
    case cat::EventKind::AlterView: {
        const cat::ViewObject* v = state_.viewByObjectId(e.objectId);
        if (v == nullptr) return;
        // Install before uninstall: the new sink exists before the new plan, and
        // the old plan stops before its sink is retired (F27).
        if (sink.sinkApplied) sink.sinkApplied(sinkSpecOf(*v));
        if (sink.viewInstalled) sink.viewInstalled(*v);
        if (e.kind == cat::EventKind::AlterView) {
            if (sink.viewUninstalled) sink.viewUninstalled(a.droppedTopicId);
            if (sink.topicDropped) sink.topicDropped(a.droppedTopicId, e.objectName);
        }
        return;
    }
    case cat::EventKind::DropView:
        if (sink.viewUninstalled) sink.viewUninstalled(a.droppedTopicId);
        if (sink.topicDropped) sink.topicDropped(a.droppedTopicId, e.objectName);
        return;
    }
}

void CatalogApplier::halt_(cat::ApplyError err) {
    spdlog::critical("CatalogApplier: apply HALTED at offset {} — {} {} (expected={:#x} actual={:#x}); this broker "
                     "serves what it applied and accepts no catalog change until an operator intervenes "
                     "(docs/CATALOG.md#halted)",
                     err.offset, cat::applyErrorName(err.code), err.detail, err.expected, err.actual);
    st_ = Halted{std::move(err)};
    if (const CatalogEffects* sink = sink_(); sink && sink->halted) sink->halted(std::get<Halted>(st_).error);
}

} // namespace rune::node
