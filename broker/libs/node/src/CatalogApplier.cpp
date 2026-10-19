// CatalogApplier.cpp — offset-ordered, verified apply of `__catalog` (catalog
// history spec §5.3, ruling 8).  See CatalogApplier.h for the step list.
#include <rune/node/CatalogApplier.h>

#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogEffectFrames.h>
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

#include <algorithm>
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

std::vector<int32_t> droppedTopicIds(const cat::CatalogState& s) {
    std::vector<int32_t> out;
    for (int32_t id = 0; id < s.nextTopicId(); ++id)
        if (s.topicByTopicId(id) == nullptr && s.viewByViewTopicId(id) == nullptr) out.push_back(id);
    return out;
}

CatalogApplier::CatalogApplier(std::string catalogDir, SchemaRegistry& schemas, ViewRegistry& views)
    : dir_(std::move(catalogDir)), schemas_(&schemas), views_(&views),
      st_(Applying{0}), phase_(AwaitingBoot{}), readBuf_(kCatalogReadChunk) {}

int64_t CatalogApplier::stagedOffset() const noexcept {
    const Halted* h = std::get_if<Halted>(&st_);
    return h ? h->stagedOffset : -1;
}

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
    rune::storage::Log* log = r.log();
    return replay_(log, log ? log->logEndOffset() : r.logEndOffset(), std::move(boot));
}

int CatalogApplier::replayLocal(rune::storage::Log& log, BootEffects boot) {
    return replay_(&log, log.logEndOffset(), std::move(boot));
}

int CatalogApplier::replay_(rune::storage::Log* log, int64_t leo, BootEffects boot) {
    if (!std::holds_alternative<AwaitingBoot>(phase_)) {
        spdlog::error("CatalogApplier: replayLocal refused in phase {} — the boot prefix is applied exactly once "
                      "(a second replay would re-apply recorded request ids)", phaseName());
        return -1;
    }
    phase_ = Booted{std::move(boot)};

    // Throws (refuse to start) on an unreadable / corrupt checkpoint.
    const std::optional<CatalogCheckpoint> ckpt = readCatalogCheckpoint(dir_);
    if (auto refusal = catalogCheckpointBeyondLog(ckpt, leo, dir_)) {
        spdlog::critical("CatalogApplier: {}", *refusal);
        throw std::runtime_error(*refusal);
    }
    if (!ckpt) {
        spdlog::info("CatalogApplier: no applied checkpoint in {} — nothing to replay (log end {})", dir_, leo);
        std::get<Booted>(phase_).effects = BootEffects{};   // R5
        return 0;
    }

    const int applied = applyRange_(log, ckpt->appliedOffset + 1);
    if (!halted() && nextOffset() != ckpt->appliedOffset + 1) {
        halt_(localFailure(nextOffset(), cat::ApplyError::Code::Precondition,
                           "boot replay stopped at offset " + std::to_string(nextOffset()) +
                               " before the checkpointed offset " + std::to_string(ckpt->appliedOffset) +
                               " — the local __catalog log is missing or cannot serve applied events"));
    }
    if (!halted()) {
        const uint64_t digest = state_.digest();
        if (digest != ckpt->digest) {
            cat::ApplyError err = localFailure(ckpt->appliedOffset, cat::ApplyError::Code::FingerprintMismatch,
                                               "boot replay of [0, " + std::to_string(ckpt->appliedOffset) +
                                                   "] ends at a catalog digest other than the checkpoint's — local "
                                                   "__catalog log damage");
            err.expected = ckpt->digest;
            err.actual   = digest;
            halt_(std::move(err));
        } else {
            spdlog::info("CatalogApplier: boot replay applied {} event(s), [0, {}], digest {:#x} verified",
                         applied, ckpt->appliedOffset, digest);
        }
    }
    // The coalesced boot sink (I1): what was applied is served — halted or not.
    emitFinalState_();
    // R5: the boot sink serves the replay only; release its captures (the caller's locals die with it) —
    // a later halt in Booted posts nothing, postBootStash delivers it.
    std::get<Booted>(phase_).effects = BootEffects{};
    return applied;
}

// ─── postBootStash — the boot effects that need the workers (Task 16) ────────
// The BROKER's boot sink collects the topics (it builds them before any builder
// runs); what needs a running broker — the validators, the view sinks' plan gate,
// the view installs, the metric and a halt — is posted from the applied state
// once the workers exist.
void CatalogApplier::postBootStash(const CatalogEffects& sink) const {
    if (lastApplied_.offset >= 0) {
        // Exactly what the boot accepted (R4): the objects emitFinalState_ refused as oversize are skipped
        // here too (the replay already halted on them).
        const std::size_t maxFrame = sink.maxFrameBytes;
        for (const cat::TopicObject* t : state_.topics()) {
            if (maxFrame != 0 && topicEffectFramesError(*t, maxFrame)) continue;
            if (t->schema && sink.schemaChanged) sink.schemaChanged(*t);
        }
        for (const cat::ViewObject* v : state_.views()) {
            if (maxFrame != 0 && viewEffectFramesError(*v, state_, maxFrame)) continue;
            if (sink.sinkApplied) sink.sinkApplied(sinkSpecOf(*v));
            if (sink.viewInstalled) sink.viewInstalled(*v);
        }
        if (sink.applied) sink.applied(lastApplied_.offset, state_.digest());
    }
    if (const Halted* h = std::get_if<Halted>(&st_); h != nullptr && sink.halted) sink.halted(h->error);
}

// ─── emitFinalState_ — the coalesced boot effects (fix round 1, I1) ──────────
// The FINAL state of every live object, never its history: no create/drop pair
// for a dropped object, one validator per typed topic (its current version), one
// install per live view, ONE `applied`.  Topics before views (a view's sink and
// sources exist before its plan).  A frame that cannot fit is an explicit halt
// (the other objects are still posted — the broker serves what it can).
void CatalogApplier::emitFinalState_() {
    const CatalogEffects* sink = sink_();
    if (sink == nullptr || lastApplied_.offset < 0) return;
    const std::size_t maxFrame = sink->maxFrameBytes;
    auto refuse = [&](std::string why) {
        if (!halted())
            halt_(localFailure(lastApplied_.offset, cat::ApplyError::Code::Precondition,
                               "boot: an effect of the applied catalog cannot be posted: " + std::move(why)));
        else
            spdlog::critical("CatalogApplier: boot: an effect of the applied catalog cannot be posted: {}", why);
    };
    for (const cat::TopicObject* t : state_.topics()) {
        if (maxFrame != 0)
            if (auto err = topicEffectFramesError(*t, maxFrame)) { refuse(std::move(*err)); continue; }
        if (sink->topicApplied) sink->topicApplied(*t);
        if (t->schema && sink->schemaChanged) sink->schemaChanged(*t);
    }
    for (const cat::ViewObject* v : state_.views()) {
        if (maxFrame != 0)
            if (auto err = viewEffectFramesError(*v, state_, maxFrame)) { refuse(std::move(*err)); continue; }
        if (sink->sinkApplied) sink->sinkApplied(sinkSpecOf(*v));
        if (sink->viewInstalled) sink->viewInstalled(*v);
    }
    if (sink->applied) sink->applied(lastApplied_.offset, state_.digest());
}

bool CatalogApplier::goLive(LiveEffects live) {
    if (std::holds_alternative<Live>(phase_)) {
        spdlog::error("CatalogApplier: goLive refused — already Live");
        return false;
    }
    if (std::holds_alternative<AwaitingBoot>(phase_)) {
        // M2: AwaitingBoot → Live is an EMPTY catalog's path only.  With a
        // checkpoint the boot replay must run first, or every applied event would
        // be re-applied live (commit installs, rewritten checkpoints).
        bool hasCheckpoint = true;
        try {
            hasCheckpoint = readCatalogCheckpoint(dir_).has_value();
        } catch (const std::exception& ex) {
            spdlog::error("CatalogApplier: goLive refused — the checkpoint is unreadable: {}", ex.what());
            return false;
        }
        if (hasCheckpoint) {
            spdlog::error("CatalogApplier: goLive refused in AwaitingBoot — {} holds an applied checkpoint; "
                          "replayLocal must run first", dir_);
            return false;
        }
    }
    phase_ = Live{std::move(live)};
    return true;
}

// ─── applyCommitted — steady state: [nextOffset, HW) ──────────────────────────
int CatalogApplier::applyCommitted(rune::replication::PartitionReplica& r) {
    if (!std::holds_alternative<Live>(phase_)) return 0;
    const Applying* a = std::get_if<Applying>(&st_);
    if (a == nullptr) return 0;   // halted: no further apply, ever
    const int64_t hw = r.highWatermark();
    if (a->nextOffset >= hw) return 0;   // idle: one compare, no allocation
    return applyRange_(r.log(), hw);
}

// ─── applyRange_ — read [nextOffset, bound) and apply record by record ───────
int CatalogApplier::applyRange_(rune::storage::Log* log, int64_t bound) {
    namespace st = rune::storage;
    if (log == nullptr) {
        halt_(localFailure(nextOffset(), cat::ApplyError::Code::Precondition,
                           "the __catalog replica has no disk log (the catalog is always disk-backed)"));
        return 0;
    }
    int applied = 0;
    auto cannotRead = [&](int64_t at, const std::string& cause) {
        halt_(localFailure(at, cat::ApplyError::Code::Precondition,
                           "cannot read committed offset " + std::to_string(at) + " (below " +
                               std::to_string(bound) + "): " + cause));
    };
    while (const Applying* a = std::get_if<Applying>(&st_)) {
        const int64_t from = a->nextOffset;
        if (from >= bound) break;

        st::ReadIntoStatus status = st::ReadIntoStatus::Ok;
        int err = 0;
        std::span<const uint8_t> bytes;
        if (readFault_.count != 0) [[unlikely]] {   // the I2 test seam
            status = static_cast<st::ReadIntoStatus>(readFault_.status);
            bytes  = std::span<const uint8_t>(readFault_.bytes.data(), readFault_.bytes.size());
            if (readFault_.count > 0) --readFault_.count;
        } else {
            const auto maxBytes = static_cast<int32_t>(readBuf_.size() / 2);
            const st::LogReadView v = log->readInto(from, maxBytes, readBuf_.data(), readBuf_.size(), status, bound, &err);
            if (!v.empty()) bytes = std::span<const uint8_t>(v.data, v.len);
        }
        switch (status) {
        case st::ReadIntoStatus::Ok:
            break;
        case st::ReadIntoStatus::FirstTooLarge:
            // Grow the reused buffer (capacity kept: never a per-pass allocation) and retry.
            if (readBuf_.size() >= kCatalogReadMaxBuffer) {
                cannotRead(from, "oversize — the batch does not fit the " + std::to_string(kCatalogReadMaxBuffer) +
                                     " B read buffer cap");
                return applied;
            }
            spdlog::warn("CatalogApplier: the batch at offset {} exceeds the {} B read buffer — growing it", from,
                         readBuf_.size());
            readBuf_.reset(std::min(readBuf_.size() * 2, kCatalogReadMaxBuffer));   // the retry re-reads into it
            continue;
        case st::ReadIntoStatus::Absent:
            cannotRead(from, "Absent — the log holds no batch at or after it");
            return applied;
        case st::ReadIntoStatus::Unreadable:
            cannotRead(from, "unreadable — torn or corrupt bytes");
            return applied;
        case st::ReadIntoStatus::IoError:
            cannotRead(from, "I/O error: " + std::string{std::strerror(err)});
            return applied;
        }
        if (bytes.empty()) {
            cannotRead(from, "empty — the read returned no bytes");
            return applied;
        }
        if (bytes.size() < static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead)) {
            cannotRead(from, "short — " + std::to_string(bytes.size()) + " bytes, less than one batch header");
            return applied;
        }

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
                // offsets are consumed — but only CONTIGUOUSLY (I3): a control
                // batch past nextOffset hides a gap, never skipped over.
                Applying& cur = std::get<Applying>(st_);
                if (reader.baseOffset() > cur.nextOffset) {
                    halt_(localFailure(cur.nextOffset, cat::ApplyError::Code::Precondition,
                                       "a gap in __catalog: the next batch (a control batch) starts at offset " +
                                           std::to_string(reader.baseOffset())));
                    break;
                }
                const int64_t end = reader.baseOffset() + reader.lastOffsetDelta() + 1;
                if (end > cur.nextOffset) cur.nextOffset = std::min(end, bound);
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
        if (after == nullptr) break;   // halted (explicit already)
        if (after->nextOffset == from) {
            cannotRead(from, "no record at it in the batches read (the read ended before it)");
            break;
        }
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
    // From here on the library holds the event (M1): a halt names it staged.
    const bool live = std::holds_alternative<Live>(phase_);
    const CatalogEffects* sink = sink_();

    if (live) {
        // C1: every frame the effects will post must fit the sink — sized on the
        // applied state, BEFORE the checkpoint (nothing of it is posted or durable).
        if (sink != nullptr && sink->maxFrameBytes != 0)
            if (auto err = eventEffectFramesError(state_, *ev, *a, sink->maxFrameBytes)) {
                halt_(localFailure(offset, cat::ApplyError::Code::Precondition,
                                   std::string{cat::kindName(ev->kind)} + " '" + ev->objectName +
                                       "': an effect cannot be posted: " + *err),
                      offset);
                return false;
            }
        // Step 4: the checkpoint precedes every side effect (ruling 8).
        const uint64_t digest = state_.digest();
        if (!writeCatalogCheckpoint(dir_, CatalogCheckpoint{offset, digest})) {
            const int e = errno;
            cat::ApplyError err = localFailure(offset, cat::ApplyError::Code::Precondition,
                                               "the applied checkpoint could not be made durable in " + dir_ +
                                                   " (errno " + std::to_string(e) + ": " + std::strerror(e) +
                                                   ") — storage failure; no effect of this event was posted");
            if (sink && sink->storageFailed) sink->storageFailed(e, dir_);
            halt_(std::move(err), offset);
            return false;
        }
    }

    // Step 5: the admin-owned projections.
    try {
        project_(*ev, *a);
    } catch (const std::exception& ex) {
        halt_(localFailure(offset, cat::ApplyError::Code::BadOps,
                           std::string{"projecting "} + std::string{cat::kindName(ev->kind)} + " '" +
                               ev->objectName + "' into the registries failed: " + ex.what()),
              offset);
        return false;
    }

    std::get<Applying>(st_).nextOffset = offset + 1;
    lastApplied_ = AppliedAt{offset, *a};
    // Steps 6–7, LIVE only: the boot sink is coalesced (emitFinalState_, I1).
    if (live && sink != nullptr) {
        effects_(*sink, *ev, *a);
        if (sink->applied) sink->applied(offset, state_.digest());
    }
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
        if (const cat::TopicObject* t = state_.topicByObjectId(e.objectId)) {
            if (sink.topicApplied) sink.topicApplied(*t);
            if (t->schema && sink.schemaChanged) sink.schemaChanged(*t);   // a typed topic's validators follow it
        }
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

void CatalogApplier::halt_(cat::ApplyError err, int64_t stagedOffset) {
    spdlog::critical("CatalogApplier: apply HALTED at offset {} — {} {} (expected={:#x} actual={:#x}{}); this broker "
                     "serves what it applied and accepts no catalog change until an operator intervenes "
                     "(docs/CATALOG.md#halted)",
                     err.offset, cat::applyErrorName(err.code), err.detail, err.expected, err.actual,
                     stagedOffset >= 0 ? "; the event is staged in the catalog state, its effects never ran" : "");
    st_ = Halted{std::move(err), stagedOffset};
    if (const CatalogEffects* sink = sink_(); sink && sink->halted) sink->halted(std::get<Halted>(st_).error);
}

} // namespace rune::node
