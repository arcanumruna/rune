#include <rune/node/lease/LeaseIndex.h>

#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/view/Verdict.h>

#include <algorithm>
#include <bit>
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

PrepareStatus projectLeaseRow(uint8_t* row, const rune::view::RowLayout& layout, const rune::codec::Descriptor& outDesc,
                              const rune::codec::Descriptor& writer, std::string_view value) noexcept {
    if (layout.slotType.size() < kLeaseSystemSlots + kLeaseOffsetSlots || outDesc.fields.size() < userSlots(layout)) return PrepareStatus::Malformed;
    const uint16_t n = userSlots(layout);
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

uint16_t judgeLease(LeaseRowState row, uint8_t attributes, int64_t recordVersion, uint64_t liveCount, uint32_t maxKeys) noexcept {
    using rune::protocol::client::ErrorCode;
    const lr::LeaseOp op = lr::leaseOpOf(attributes);
    const uint8_t intent = lr::intentOf(attributes);
    constexpr auto kInsert = static_cast<uint8_t>(lr::Intent::Insert), kExisting = static_cast<uint8_t>(lr::Intent::Existing);
    const bool sameVersion = row.present && row.version == recordVersion;
    switch (op) {
    case lr::LeaseOp::Put:
        if (intent == kInsert) {
            if (row.present) return static_cast<uint16_t>(ErrorCode::KeyExists);
            return liveCount < maxKeys ? 0 : static_cast<uint16_t>(ErrorCode::LeaseLimitExceeded);
        }
        if (intent == kExisting) return sameVersion ? 0 : static_cast<uint16_t>(ErrorCode::LeaseNotFound);
        break;
    case lr::LeaseOp::Revoke:
        if (intent == kExisting) return sameVersion ? 0 : static_cast<uint16_t>(ErrorCode::LeaseNotFound);
        break;
    case lr::LeaseOp::Expire:
    case lr::LeaseOp::Checkpoint:
        if (intent == 0) return sameVersion ? 0 : rune::view::kVerdictStaleBroker;
        break;
    }
    return static_cast<uint16_t>(ErrorCode::InvalidRequest);
}

int64_t leaseRowVersion(const uint8_t* row, const rune::view::RowLayout& layout) noexcept {
    return static_cast<int64_t>(slotU64(row, layout, kLeaseVersionSlot));
}

int64_t leaseRecordVersion(std::string_view value) noexcept {
    if (value.size() < lr::kLeaseVersionValueOffset + 8) return 0;
    return static_cast<int64_t>(loadU64(reinterpret_cast<const uint8_t*>(value.data()) + lr::kLeaseVersionValueOffset));
}

PrepareStatus prepareLeaseIndex(const LeaseIndexFold& f, const rune::view::SourceRecord& r, rune::view::Prospect& p) noexcept {
    p.clear();
    if (r.key.size() != f.keyWidth) {
        p.stats.skippedMalformed = 1;
        return PrepareStatus::Malformed;
    }
    const lr::LeaseOp op = lr::leaseOpOf(r.attributes);
    rune::view::ProspectRow& row = p.rows[0];
    row.key     = rune::view::ViewKey::fromBytes({reinterpret_cast<const uint8_t*>(r.key.data()), r.key.size()});
    row.existed = f.table.get(row.key) != nullptr;
    row.removed = op == lr::LeaseOp::Revoke || op == lr::LeaseOp::Expire;
    row.image   = nullptr;
    if (!r.value.empty()) {
        if (r.value.size() < 4u + lr::kLeaseSystemBytes) {
            p.stats.skippedMalformed = 1;
            return PrepareStatus::Malformed;
        }
        if (r.writer == nullptr) return PrepareStatus::Pending;   // the record's version is not installed yet: hold, never accept
        if (op == lr::LeaseOp::Put || op == lr::LeaseOp::Checkpoint) {
            if (p.scratch.size() < f.layout.rowLen) {
                p.stats.skippedMalformed = 1;
                return PrepareStatus::Malformed;
            }
            const PrepareStatus ps = projectLeaseRow(p.scratch.data(), f.layout, f.outDesc, *r.writer, r.value);
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
            uint8_t* row = f.table.put(key);
            std::memcpy(row, p.rows[0].image, l.rowLen);
            setSlotU64(row, l, kLeaseVersionSlot, static_cast<uint64_t>(r.offset));
            setSlotU64(row, l, kLeaseRemainingSlot, 0);
            setSlotU64(row, l, grantedSlot, static_cast<uint64_t>(r.offset));
            setSlotU64(row, l, updatedSlot, static_cast<uint64_t>(r.offset));
            if (!f.table.ridOf(key, rid)) return false;
            ev.kind = LeaseEventKind::Granted;
            ev.rid = rid;
            ev.version = r.offset;
            ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
            ev.remainingMs = 0;
            return true;
        }
        if (f.table.get(key) == nullptr) {
            // Replay of a compacted log: the grant is gone and this update carries the full row.  The live judge never gets
            // here (it refuses an update of an absent key with LeaseNotFound).
            uint8_t* created = f.table.put(key);
            std::memcpy(created, p.rows[0].image, l.rowLen);
            setSlotU64(created, l, grantedSlot, slotU64(created, l, kLeaseVersionSlot));
            setSlotU64(created, l, updatedSlot, static_cast<uint64_t>(r.offset));
            if (!f.table.ridOf(key, rid)) return false;
            ev.kind = LeaseEventKind::Updated;
            ev.rid = rid;
            ev.version = static_cast<int64_t>(slotU64(created, l, kLeaseVersionSlot));
            ev.ttlMs = slotU64(created, l, kLeaseTtlSlot);
            ev.remainingMs = slotU64(created, l, kLeaseRemainingSlot);
            return true;
        }
        uint8_t* row = f.table.put(key);   // the existing row; update replaces the holder and the user columns only
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
        if (!f.table.ridOf(key, rid)) return false;
        ev.kind = LeaseEventKind::Updated;
        ev.rid = rid;
        ev.version = static_cast<int64_t>(slotU64(row, l, kLeaseVersionSlot));
        ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
        ev.remainingMs = slotU64(row, l, kLeaseRemainingSlot);
        return true;
    }
    case lr::LeaseOp::Revoke:
    case lr::LeaseOp::Expire: {
        const uint8_t* row = f.table.get(key);
        if (row == nullptr || !f.table.ridOf(key, rid)) return false;
        ev.kind = LeaseEventKind::Ended;
        ev.endOp = static_cast<uint8_t>(op);
        ev.rid = rid;
        ev.version = static_cast<int64_t>(slotU64(row, l, kLeaseVersionSlot));
        ev.ttlMs = slotU64(row, l, kLeaseTtlSlot);
        ev.remainingMs = slotU64(row, l, kLeaseRemainingSlot);
        (void)f.table.erase(key);
        return true;
    }
    case lr::LeaseOp::Checkpoint: {
        if (r.value.size() < lr::kLeaseRemainingValueOffset + 8) return false;
        const bool absent = f.table.get(key) == nullptr;
        if (absent && p.rows[0].image == nullptr) return false;
        uint8_t* row = f.table.put(key);
        if (absent) {   // replay of a compacted log (see the update above): the checkpoint carries the full row
            std::memcpy(row, p.rows[0].image, l.rowLen);
            setSlotU64(row, l, grantedSlot, slotU64(row, l, kLeaseVersionSlot));
        }
        setSlotU64(row, l, kLeaseRemainingSlot, loadU64(reinterpret_cast<const uint8_t*>(r.value.data()) + lr::kLeaseRemainingValueOffset));
        setSlotU64(row, l, updatedSlot, static_cast<uint64_t>(r.offset));
        if (!f.table.ridOf(key, rid)) return false;
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
                                          const rune::view::FoldSkip& skipRef, bool* held) noexcept {
    const rune::view::FoldSkip skip = skipRef;   // #537: by value (see AbortedTxnFilter::skips)
    rune::view::FoldStats stats{};
    const int64_t from = cursor;
    bool stopped = false, corrupt = false;
    int64_t pendingAt = -1;   // a record whose layout is not installed: the fold stops AT it
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
                const PrepareStatus ps = prepareLeaseIndex(f, rec, prospect);
                if (ps == PrepareStatus::Pending) {
                    pendingAt = span.offset;
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
        cursor = std::max(from, pendingAt);
        return stats;
    }
    cursor = std::max(from, corrupt ? walkedUpto : std::min(view.nextOffset, bound));
    return stats;
}

}  // namespace rune::node
