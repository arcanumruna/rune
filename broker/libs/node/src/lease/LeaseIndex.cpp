#include <rune/node/lease/LeaseIndex.h>

#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstring>

namespace rune::node {

namespace {

namespace lr = rune::protocol::record;
using rune::view::PrepareStatus;

static_assert(std::endian::native == std::endian::little, "row slots and record values are copied in memory order");

uint64_t loadU64(const uint8_t* p) noexcept {
    uint64_t v;
    std::memcpy(&v, p, 8);
    return v;
}
void storeU64(uint8_t* p, uint64_t v) noexcept { std::memcpy(p, &v, 8); }

uint64_t slotU64(const uint8_t* row, const rune::view::RowLayout& l, uint16_t slot) noexcept { return loadU64(row + l.slotOffset[slot]); }
void setSlotU64(uint8_t* row, const rune::view::RowLayout& l, uint16_t slot, uint64_t v) noexcept {
    storeU64(row + l.slotOffset[slot], v);
    rune::view::clearNull(row, l, slot);
}

uint16_t userSlots(const rune::view::RowLayout& l) noexcept { return static_cast<uint16_t>(l.slotType.size() - kLeaseOffsetSlots); }

}  // namespace

rune::codec::Descriptor leaseIndexOutDescriptor(const rune::codec::Descriptor& evolving) {
    rune::codec::Descriptor out = evolving;
    uint32_t maxId = 0;
    for (const auto& f : evolving.fields) maxId = std::max<uint32_t>(maxId, f.id);
    const std::string_view names[2] = {kLeaseGrantedOffsetColumn, kLeaseUpdatedOffsetColumn};
    for (uint32_t i = 0; i < 2; ++i) {
        rune::codec::FieldDescriptor f;
        f.name         = std::string(names[i]);
        f.id           = static_cast<uint16_t>(maxId + 1 + i);   // labels only (rows are addressed by slot)
        f.type         = rune::codec::PrimType::Int64;
        f.sinceVersion = 1;
        f.untilVersion = 0xFFFF;
        out.fields.push_back(std::move(f));
    }
    return out;
}

rune::view::RowLayout leaseIndexRowLayout(const rune::codec::Descriptor& evolving) {
    return rune::view::buildRowLayout(leaseIndexOutDescriptor(evolving));
}

rune::view::CompiledViewPlan leaseIndexPlan(int32_t srcTopicId, uint16_t keyFieldId) {
    rune::view::CompiledViewPlan plan;
    plan.viewName          = "__lease_index";
    plan.viewTopicId       = -1;
    plan.keyFieldId        = {keyFieldId};
    plan.sourceTopicId     = {srcTopicId};
    plan.outputFingerprint = kLeaseIndexPlanFingerprint;
    plan.persistence       = 1;
    plan.sizing.chunkBytes = static_cast<uint32_t>(kLeaseIndexChunkBytes);
    return plan;
}

PrepareStatus buildLeaseSlotMap(const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc, const rune::codec::Descriptor& writer,
                                std::vector<uint16_t>& slotOf) {
    if (layout.slotType.size() < kLeaseSystemSlots + kLeaseOffsetSlots || outDesc.fields.size() < userSlots(layout)) return PrepareStatus::Malformed;
    const uint16_t n = userSlots(layout);
    bool malformed = false;
    std::vector<uint16_t> map(writer.fields.size(), 0);
    for (std::size_t k = 0; k < writer.fields.size(); ++k) {
        const auto& w = writer.fields[k];
        uint16_t slot = n;
        for (uint16_t i = 0; i < n; ++i)
            if (outDesc.fields[i].id == w.id) { slot = i; break; }
        if (slot == n) return PrepareStatus::Pending;   // a field id the layout lacks: the layout is older than the writer's schema
        const uint32_t width = rune::codec::fieldWidth(w);
        if (w.type != layout.slotType[slot] || width != layout.slotWidth[slot] || w.offset + width > writer.blockLength) malformed = true;
        map[k] = slot;
    }
    if (malformed) return PrepareStatus::Malformed;
    slotOf = std::move(map);
    return PrepareStatus::Ok;
}

const uint16_t* leaseSlotMapFor(LeaseCoverCache& cache, uint16_t version, uint32_t installSeq, const rune::view::RowLayout& layout,
                                const rune::codec::Descriptor& outDesc, const rune::codec::Descriptor& writer, PrepareStatus* status) {
    if (status != nullptr) *status = PrepareStatus::Ok;
    if (installSeq == 0 || version >= kLeaseCoverCacheVersions) return nullptr;
    LeaseCoverEntry& e = cache.entries[version];
    if (e.installSeq == installSeq) return e.slotOf.data();
    const PrepareStatus ps = buildLeaseSlotMap(layout, outDesc, writer, e.slotOf);
    if (ps != PrepareStatus::Ok) {
        e.installSeq = 0;
        if (status != nullptr) *status = ps;
        return nullptr;
    }
    e.installSeq = installSeq;
    ++cache.fills;
    return e.slotOf.data();
}

PrepareStatus projectLeaseRow(uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                              const rune::codec::Descriptor& writer, std::string_view value, const uint16_t* slotOf) noexcept {
    if (layout.slotType.size() < kLeaseSystemSlots + kLeaseOffsetSlots || outDesc.fields.size() < userSlots(layout)) return PrepareStatus::Malformed;
    const uint16_t n = userSlots(layout);
    if (slotOf != nullptr) {
        // The cached map already proved every writer field covered and type-compatible inside the writer's block.
        if (value.size() < 4u + writer.blockLength) return PrepareStatus::Malformed;
        std::memset(row, 0, layout.rowLen);
        for (uint16_t i = 0; i < n; ++i) rune::view::setNull(row, layout, i);
        for (std::size_t k = 0; k < writer.fields.size(); ++k) {
            const uint16_t slot = slotOf[k];
            std::memcpy(row + layout.slotOffset[slot], value.data() + 4 + writer.fields[k].offset, layout.slotWidth[slot]);
            rune::view::clearNull(row, layout, slot);
        }
        return PrepareStatus::Ok;
    }
    for (const auto& w : writer.fields) {   // a field id the layout lacks: the layout is older than the writer's schema
        bool known = false;
        for (uint16_t i = 0; i < n && !known; ++i) known = outDesc.fields[i].id == w.id;
        if (!known) return PrepareStatus::Pending;
    }
    if (value.size() < 4u + writer.blockLength) return PrepareStatus::Malformed;
    std::memset(row, 0, layout.rowLen);
    for (uint16_t i = 0; i < n; ++i) {
        const rune::codec::FieldDescriptor& of = outDesc.fields[i];
        const rune::codec::FieldDescriptor* wf = nullptr;
        for (const auto& w : writer.fields)
            if (w.id == of.id) { wf = &w; break; }
        if (wf == nullptr) {
            rune::view::setNull(row, layout, i);
            continue;
        }
        const uint32_t width = rune::codec::fieldWidth(*wf);
        if (wf->type != layout.slotType[i] || width != layout.slotWidth[i] || 4u + wf->offset + width > value.size()) return PrepareStatus::Malformed;
        std::memcpy(row + layout.slotOffset[i], value.data() + 4 + wf->offset, width);
    }
    return PrepareStatus::Ok;
}

std::size_t encodeLeaseValue(const uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                             const rune::codec::Descriptor& writer, uint16_t writerVersion, uint8_t* out, std::size_t cap,
                             const uint16_t* slotOf) noexcept {
    const std::size_t total = 4u + writer.blockLength;
    if (total > cap || outDesc.fields.size() < userSlots(layout)) return 0;
    std::memset(out, 0, total);
    out[0] = static_cast<uint8_t>(writer.blockLength);
    out[1] = static_cast<uint8_t>(writer.blockLength >> 8);
    out[2] = static_cast<uint8_t>(writerVersion);
    out[3] = static_cast<uint8_t>(writerVersion >> 8);
    const uint16_t n = userSlots(layout);
    for (std::size_t k = 0; k < writer.fields.size(); ++k) {
        const auto& w = writer.fields[k];
        uint16_t slot = n;
        if (slotOf != nullptr) {
            slot = slotOf[k];
        } else {
            for (uint16_t i = 0; i < n; ++i)
                if (outDesc.fields[i].id == w.id) { slot = i; break; }
        }
        assert(slot != n && "a writer field without a layout slot");
        if (slot == n) return 0;
        const uint32_t width = rune::codec::fieldWidth(w);
        if (layout.slotWidth[slot] != width || 4u + w.offset + width > total) return 0;
        if (rune::view::isNull(row, layout, slot)) {
            // The record format has no null: zeros, except an Enum, whose zero may not be whitelisted by this version's Validator.
            if (w.type == rune::codec::PrimType::Enum && !w.enumValues.empty() && width >= 4) {
                const uint32_t first = static_cast<uint32_t>(w.enumValues.front());
                for (std::size_t b = 0; b < 4; ++b) out[4 + w.offset + b] = static_cast<uint8_t>(first >> (8 * b));
            }
            continue;
        }
        std::memcpy(out + 4 + w.offset, row + layout.slotOffset[slot], width);
    }
    return total;
}

uint16_t judgeLease(LeaseRowState row, uint8_t attributes, int64_t recordVersion, uint64_t recordRemainingMs,
                    uint64_t liveCount, uint32_t maxKeys) noexcept {
    using rune::protocol::client::ErrorCode;
    constexpr auto kInvalid = static_cast<uint16_t>(ErrorCode::InvalidRequest);
    const lr::LeaseOp op = lr::leaseOpOf(attributes);
    const uint8_t intent = lr::intentOf(attributes);
    constexpr auto kInsert = static_cast<uint8_t>(lr::Intent::Insert), kExisting = static_cast<uint8_t>(lr::Intent::Existing);
    const bool sameVersion = row.present && row.version == recordVersion;
    switch (op) {
    case lr::LeaseOp::Put:
        if (intent == kInsert) {
            if (row.present) return static_cast<uint16_t>(ErrorCode::KeyExists);
            if (liveCount >= maxKeys) return static_cast<uint16_t>(ErrorCode::LeaseLimitExceeded);
            return recordRemainingMs == 0 ? 0 : kInvalid;
        }
        if (intent == kExisting) {
            if (!sameVersion) return static_cast<uint16_t>(ErrorCode::LeaseNotFound);
            return 0;   // the record's TTL and remaining are not checked (spec 6.4); apply keeps the row's
        }
        break;
    case lr::LeaseOp::Revoke:
        if (intent == kExisting) {
            if (!sameVersion) return static_cast<uint16_t>(ErrorCode::LeaseNotFound);
            return 0;   // the record's TTL and remaining are not checked (spec 6.4); apply keeps the row's
        }
        break;
    case lr::LeaseOp::Expire:
    case lr::LeaseOp::Checkpoint:
        if (intent == 0) return sameVersion ? 0 : rune::view::kVerdictStaleBroker;
        break;
    }
    return kInvalid;
}

uint64_t leaseRecordRemaining(std::string_view value) noexcept {
    if (value.size() < lr::kLeaseRemainingValueOffset + 8) return 0;
    return loadU64(reinterpret_cast<const uint8_t*>(value.data()) + lr::kLeaseRemainingValueOffset);
}

int64_t leaseRowVersion(const uint8_t* row, const rune::view::RowLayout& layout) noexcept {
    return static_cast<int64_t>(slotU64(row, layout, kLeaseVersionSlot));
}

uint64_t leaseRowTtl(const uint8_t* row, const rune::view::RowLayout& layout) noexcept { return slotU64(row, layout, kLeaseTtlSlot); }
uint64_t leaseRowRemaining(const uint8_t* row, const rune::view::RowLayout& layout) noexcept { return slotU64(row, layout, kLeaseRemainingSlot); }
uint64_t leaseRowHolder(const uint8_t* row, const rune::view::RowLayout& layout) noexcept { return slotU64(row, layout, kLeaseHolderSlot); }

int64_t leaseRecordVersion(std::string_view value) noexcept {
    if (value.size() < lr::kLeaseVersionValueOffset + 8) return 0;
    return static_cast<int64_t>(loadU64(reinterpret_cast<const uint8_t*>(value.data()) + lr::kLeaseVersionValueOffset));
}

uint64_t leaseRecordTtl(std::string_view value) noexcept {
    if (value.size() < lr::kLeaseTtlValueOffset + 8) return 0;
    return loadU64(reinterpret_cast<const uint8_t*>(value.data()) + lr::kLeaseTtlValueOffset);
}

PrepareStatus prepareLeaseIndex(const LeaseIndexFold& f, const rune::view::SourceRecord& r, rune::view::Prospect& p, bool refusedVersion,
                                uint32_t installSeq) noexcept {
    p.clear();
    p.rows[0].existed = false;   // a record refused before its key is probed has no row: the verdict reads this (stale broker record)
    p.rows[0].rid     = UINT32_MAX;
    if (r.key.size() != f.keyWidth) {
        p.stats.skippedMalformed = 1;
        return PrepareStatus::Malformed;
    }
    const lr::LeaseOp op = lr::leaseOpOf(r.attributes);
    rune::view::ProspectRow& row = p.rows[0];
    row.key     = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size()});
    // The ONE key probe of this record on the judge path: the verdict and the apply reuse the row id found here.
    uint32_t existingRid = 0;
    row.existed = f.table.find(row.key, existingRid) != nullptr;
    row.rid     = row.existed ? existingRid : UINT32_MAX;
    row.removed = op == lr::LeaseOp::Revoke || op == lr::LeaseOp::Expire;
    row.image   = nullptr;
    if (!r.value.empty()) {
        if (r.value.size() < 4u + lr::kLeaseSystemBytes) {
            p.stats.skippedMalformed = 1;
            return PrepareStatus::Malformed;
        }
        if (r.writer == nullptr) {
            if (!refusedVersion) return PrepareStatus::Pending;   // the record's version is not installed yet: hold, never accept
            p.stats.skippedMalformed = 1;                         // a version the spec refused is a definite answer
            return PrepareStatus::Malformed;
        }
        // A Checkpoint's image is only needed when its row is ABSENT: a replay of a compacted log creates the row from it, and on the
        // live path an absent row means a stale broker record, which the judge refuses (a malformed projection of one included, see
        // VerdictSource::verdictOf_).  A Checkpoint of a present row never builds it.
        if (op == lr::LeaseOp::Put || (op == lr::LeaseOp::Checkpoint && !row.existed)) {
            if (p.scratch.size() < f.layout.rowLen) {
                p.stats.skippedMalformed = 1;
                return PrepareStatus::Malformed;
            }
            const uint16_t ver = static_cast<uint16_t>(static_cast<uint8_t>(r.value[2]) | (static_cast<uint8_t>(r.value[3]) << 8));
            PrepareStatus ps = PrepareStatus::Ok;
            const uint16_t* slotOf = f.cover != nullptr ? leaseSlotMapFor(*f.cover, ver, installSeq, f.layout, f.outDesc, *r.writer, &ps) : nullptr;
            if (ps == PrepareStatus::Ok) ps = projectLeaseRow(p.scratch.data(), f.layout, f.outDesc, *r.writer, r.value, slotOf);
            if (ps == PrepareStatus::Pending) return ps;
            if (ps != PrepareStatus::Ok) {
                p.stats.skippedMalformed = 1;
                return PrepareStatus::Malformed;
            }
            row.image = p.scratch.data();
        }
    }
    p.rowCount = 1;
    p.op       = row.removed ? rune::view::ProspectOp::Erase : rune::view::ProspectOp::Upsert;
    return PrepareStatus::Ok;
}

bool applyLeaseRecord(LeaseIndexFold& f, const rune::view::SourceRecord& r, const rune::view::Prospect& p, LeaseEvent& ev) noexcept {
    if (p.op == rune::view::ProspectOp::None || r.value.empty()) return false;
    const rune::view::RowLayout& l = f.layout;
    const rune::view::ViewKey& key = p.rows[0].key;
    const lr::LeaseOp op = lr::leaseOpOf(r.attributes);
    const uint16_t grantedSlot = userSlots(l), updatedSlot = static_cast<uint16_t>(grantedSlot + 1);
    ev = LeaseEvent{};
    ev.offset = r.offset;
    uint32_t rid = 0;
    switch (op) {
    case lr::LeaseOp::Put: {
        if (p.rows[0].image == nullptr) return false;
        if (lr::intentOf(r.attributes) == static_cast<uint8_t>(lr::Intent::Insert)) {
            uint8_t* row = f.table.put(key, rid);
            std::memcpy(row, p.rows[0].image, l.rowLen);
            setSlotU64(row, l, kLeaseVersionSlot, static_cast<uint64_t>(r.offset));
            setSlotU64(row, l, kLeaseRemainingSlot, 0);
            setSlotU64(row, l, grantedSlot, static_cast<uint64_t>(r.offset));
            setSlotU64(row, l, updatedSlot, static_cast<uint64_t>(r.offset));
            setLeaseLastAppended(f.table, rid, -1);   // a fresh row (a reused row id keeps its old aux bytes)
            ev.kind = LeaseEventKind::Granted;
            ev.rid = rid;
            ev.version = r.offset;
            ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
            ev.remainingMs = 0;
            return true;
        }
        if (!p.rows[0].existed) {
            // Replay of a compacted log: the grant is gone and this update carries the full row.  The live judge never gets
            // here (it refuses an update of an absent key with LeaseNotFound).
            uint8_t* created = f.table.put(key, rid);
            std::memcpy(created, p.rows[0].image, l.rowLen);
            setSlotU64(created, l, grantedSlot, slotU64(created, l, kLeaseVersionSlot));
            setSlotU64(created, l, updatedSlot, static_cast<uint64_t>(r.offset));
            setLeaseLastAppended(f.table, rid, -1);
            ev.kind = LeaseEventKind::Updated;
            ev.rid = rid;
            ev.version = static_cast<int64_t>(slotU64(created, l, kLeaseVersionSlot));
            ev.ttlMs = slotU64(created, l, kLeaseTtlSlot);
            ev.remainingMs = slotU64(created, l, kLeaseRemainingSlot);
            return true;
        }
        rid = p.rows[0].rid;
        uint8_t* row = f.table.rowOf(rid);   // the existing row (found by prepare); update replaces the holder and the user columns only
        const uint8_t* img = p.rows[0].image;
        const uint16_t n = userSlots(l);
        const auto copySlot = [&](uint16_t s) noexcept {
            std::memcpy(row + l.slotOffset[s], img + l.slotOffset[s], l.slotWidth[s]);
            if (rune::view::isNull(img, l, s)) rune::view::setNull(row, l, s);
            else rune::view::clearNull(row, l, s);
        };
        copySlot(kLeaseHolderSlot);
        for (uint16_t s = kLeaseSystemSlots; s < n; ++s) copySlot(s);   // version, ttl, remaining and the offsets stay
        setSlotU64(row, l, updatedSlot, static_cast<uint64_t>(r.offset));
        ev.kind = LeaseEventKind::Updated;
        ev.rid = rid;
        ev.version = static_cast<int64_t>(slotU64(row, l, kLeaseVersionSlot));
        ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
        ev.remainingMs = slotU64(row, l, kLeaseRemainingSlot);
        return true;
    }
    case lr::LeaseOp::Revoke:
    case lr::LeaseOp::Expire: {
        if (!p.rows[0].existed) return false;
        rid = p.rows[0].rid;
        const uint8_t* row = f.table.rowOf(rid);
        ev.kind = LeaseEventKind::Ended;
        ev.endOp = static_cast<uint8_t>(op);
        ev.rid = rid;
        ev.version = static_cast<int64_t>(slotU64(row, l, kLeaseVersionSlot));
        ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
        ev.remainingMs = slotU64(row, l, kLeaseRemainingSlot);
        setLeaseLastAppended(f.table, rid, kLeaseRowErased);   // the promotion pass walks row ids: this one holds no row now
        (void)f.table.erase(key);
        return true;
    }
    case lr::LeaseOp::Checkpoint: {
        if (r.value.size() < lr::kLeaseRemainingValueOffset + 8) return false;
        const bool absent = !p.rows[0].existed;
        if (absent && p.rows[0].image == nullptr) return false;
        uint8_t* row = absent ? f.table.put(key, rid) : f.table.rowOf(rid = p.rows[0].rid);
        if (absent) {   // replay of a compacted log (see the update above): the checkpoint carries the full row
            std::memcpy(row, p.rows[0].image, l.rowLen);
            setSlotU64(row, l, grantedSlot, slotU64(row, l, kLeaseVersionSlot));
        }
        setSlotU64(row, l, kLeaseRemainingSlot, loadU64(reinterpret_cast<const uint8_t*>(r.value.data()) + lr::kLeaseRemainingValueOffset));
        setSlotU64(row, l, updatedSlot, static_cast<uint64_t>(r.offset));
        if (absent) setLeaseLastAppended(f.table, rid, -1);
        ev.kind = LeaseEventKind::Checkpointed;
        ev.rid = rid;
        ev.version = static_cast<int64_t>(slotU64(row, l, kLeaseVersionSlot));
        ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
        ev.remainingMs = slotU64(row, l, kLeaseRemainingSlot);
        return true;
    }
    }
    return false;
}

rune::view::FoldStats foldLeaseIndexBatch(LeaseIndexFold& f, const TopicWriteSpec& spec, rune::view::Prospect& prospect,
                                          const rune::storage::LogReadView& view, int64_t bound, int64_t& cursor,
                                          const rune::view::FoldSkip& skipRef, bool* held, uint16_t* heldVersion) noexcept {
    const rune::view::FoldSkip skip = skipRef;   // #537: by value (see AbortedTxnFilter::skips)
    rune::view::FoldStats stats{};
    const int64_t from = cursor;
    bool stopped = false, corrupt = false;
    int64_t pendingAt = -1;   // a record whose layout is not installed: the fold stops AT it
    uint16_t pendingVersion = 0;
    int64_t walkedUpto = view.baseOffset;
    std::size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) {
            corrupt = true;
            break;
        }
        const int16_t attrs = r.attributes();
        const int64_t end   = r.baseOffset() + r.lastOffsetDelta() + 1;
        if ((attrs & rune::protocol::batch::kAttrControl) != 0) {
            ++stats.skippedControl;
        } else if (rune::view::isAbortedBatch(skip, r, attrs)) [[unlikely]] {
            if (end > bound) { stopped = true; break; }
            ++stats.skippedAborted;
        } else {
            const bool walked = rune::protocol::forEachRecordSpan(view.data + pos, view.len - pos, [&](const rune::protocol::RecordSpan& span) {
                if (stopped) return;
                if (span.offset >= bound) { stopped = true; return; }
                if (span.offset < from) return;
                if (rune::view::isRejectedRecord(skip, span.offset)) [[unlikely]] {
                    ++stats.skippedRejected;
                    return;
                }
                rune::view::SourceRecord rec{span.key, span.value, span.offset, lr::intentOf(span.attributes), nullptr, span.attributes};
                if (!span.value.empty()) rec.writer = leaseWriterOf(spec, span.value);
                const bool refused = rec.writer == nullptr && span.value.size() >= 4 && spec.versionRefused(detail::le16(span.value.data() + 2));
                const PrepareStatus ps = prepareLeaseIndex(f, rec, prospect, refused, leaseInstallSeqOf(spec, span.value));
                if (ps == PrepareStatus::Pending) {
                    pendingAt = span.offset;
                    pendingVersion = span.value.size() >= 4 ? detail::le16(span.value.data() + 2) : 0;
                    stopped   = true;
                    return;
                }
                if (ps == PrepareStatus::Malformed) {
                    ++stats.skippedMalformed;
                    return;
                }
                LeaseEvent ignored;
                if (!applyLeaseRecord(f, rec, prospect, ignored)) return;   // accepted but not applicable: nothing to fold
                ++(prospect.op == rune::view::ProspectOp::Erase ? stats.deleted : stats.applied);
            });
            if (!walked && !stopped) { corrupt = true; break; }
            if (stopped) break;
        }
        pos += static_cast<std::size_t>(r.totalLength());
        walkedUpto = end;
    }
    if (pendingAt >= 0) {
        if (held != nullptr) *held = true;
        if (heldVersion != nullptr) *heldVersion = pendingVersion;
        cursor = std::max(from, pendingAt);
        return stats;
    }
    cursor = std::max(from, corrupt ? walkedUpto : std::min(view.nextOffset, bound));
    return stats;
}

}  // namespace rune::node
