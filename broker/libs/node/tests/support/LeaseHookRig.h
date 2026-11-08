// LeaseHookRig.h - lease topics Task 5a: a unit rig for the lease branch of CheckedWriteHook (no worker, no Aeron), shared by
// LeaseWriteHookTest and the alloc binary.  The spec is a lease topic with the lease index as member 0 of the logged set;
// the log is an InMemoryLog the staged batches are appended to.
#pragma once
#include <rune/codec/Descriptor.h>
#include <rune/node/CheckedWriteHook.h>
#include <rune/node/lease/LeaseIndex.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/LeaseRecord.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/view/ViewTable.h>

#include <memory>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace rune::node::leasehook {

namespace rec = rune::protocol::record;
using rune::protocol::client::ErrorCode;

inline constexpr uint8_t kGrantA   = 1;                                                              // Put | insert
inline constexpr uint8_t kUpdateA  = 2;                                                              // Put | existing
inline constexpr uint8_t kRevokeA  = 2 | (uint8_t(rec::LeaseOp::Revoke) << rec::kAttrLeaseOpShift);   // Revoke | existing
inline constexpr uint8_t kAcksViewAll = rune::protocol::client::kAcksWireView | rune::protocol::client::kAcksWireAll;

inline void putLe(std::vector<uint8_t>& b, std::size_t at, uint64_t v, int bytes) {
    for (int i = 0; i < bytes; ++i) b[at + std::size_t(i)] = uint8_t(v >> (8 * i));
}
inline uint64_t getLe(const uint8_t* p, int bytes = 8) {
    uint64_t v = 0;
    for (int i = 0; i < bytes; ++i) v |= uint64_t(p[i]) << (8 * i);
    return v;
}
inline std::string keyOf(uint64_t id) {
    std::string k(8, '\0');
    for (int i = 0; i < 8; ++i) k[std::size_t(i)] = char(id >> (8 * i));
    return k;
}

/// [u16 blockLength=56][u16 schema=1][version, ttl, holder, remaining, id, addr(16)].
inline std::vector<uint8_t> leaseValue(uint64_t id, int64_t ver, uint64_t ttl, uint64_t remaining = 0, uint64_t holder = 1) {
    std::vector<uint8_t> b(4u + 56u, 0);
    putLe(b, 0, 56, 2);
    putLe(b, 2, 1, 2);
    putLe(b, 4, uint64_t(ver), 8);
    putLe(b, 12, ttl, 8);
    putLe(b, 20, holder, 8);
    putLe(b, 28, remaining, 8);
    putLe(b, 36, id, 8);
    return b;
}

struct LRec { uint64_t id; uint8_t attrs; std::vector<uint8_t> value; };
inline LRec grantOf(uint64_t id, uint64_t ttl = 5000) { return {id, kGrantA, leaseValue(id, 0, ttl)}; }
inline LRec updateOf(uint64_t id, int64_t ver, uint64_t ttl = 5000) { return {id, kUpdateA, leaseValue(id, ver, ttl)}; }
inline LRec revokeOf(uint64_t id, int64_t ver, uint64_t ttl = 5000) { return {id, kRevokeA, leaseValue(id, ver, ttl)}; }

inline std::vector<uint8_t> batchOf(const std::vector<LRec>& recs, int64_t headerBase = 0) {
    rune::protocol::RecordBatchBuilder b(headerBase, 1'000'000);
    for (const LRec& r : recs)
        b.append(keyOf(r.id), {reinterpret_cast<const char*>(r.value.data()), r.value.size()}, 1'000'000, r.attrs);
    return b.build();
}

struct LeaseLog {
    rune::storage::InMemoryLog log{8 << 20, 4096};
    int64_t logEndOffset() const noexcept { return log.logEndOffset(); }
    int64_t appendNoSyncRestamp(const uint8_t* b, std::size_t len, int32_t lastOffsetDelta, int64_t& base) {
        return log.appendNoSyncRestamp(b, len, lastOffsetDelta, base);
    }
};

struct Outcome {
    ErrorCode            code = ErrorCode::OK;
    HookRefusal          why  = HookRefusal::None;
    int64_t              base = -1;
    int64_t              retryAfterNs = 0;
    const uint8_t*       staged = nullptr;   ///< the bytes handed to the append (the input itself when not patched)
    std::size_t          stagedLen = 0;
    std::vector<uint8_t> copy;               ///< a copy of the staged bytes (the scratch is reused by the next write)
};

/// A lease index for the hook (table with the lastAppended aux, the row layout and its descriptor).
struct IndexBox {
    rune::codec::Descriptor  desc;
    rune::view::RowLayout    layout;
    rune::view::ViewTable    table;
    explicit IndexBox(rune::codec::Descriptor d)
        : desc(std::move(d)), layout(leaseIndexRowLayout(desc)), table(layout, 4096, 64u << 10, keys()) {}
    static rune::view::ViewTableKeys keys() {
        rune::view::ViewTableKeys k;
        k.keyWidth = 8;
        k.auxBytesPerRow = kLeaseAuxBytes;
        return k;
    }
};

struct Rig {
    TopicWriteSpec       spec;
    std::unique_ptr<LeaseHookWork> work = std::make_unique<LeaseHookWork>();
    std::unique_ptr<IndexBox>      index;
    PartitionWriteState  state;
    WriteFacts           facts;
    static constexpr std::size_t kMaxIntake = 512u << 10;   ///< the intake limit the rig's scratch is sized from
    std::vector<uint8_t> scratch = std::vector<uint8_t>(leaseScratchBytes(kMaxIntake));
    LeaseLog             tlog;

    explicit Rig(rune::protocol::LeaseSpec ls = {}) {
        rune::codec::DescriptorBuilder d("inst", 1);
        d.add("lease_version", rune::codec::PrimType::Int64).add("lease_ttl_ms", rune::codec::PrimType::UInt64)
            .add("lease_holder", rune::codec::PrimType::UInt64).add("lease_remaining_ms", rune::codec::PrimType::UInt64)
            .add("id", rune::codec::PrimType::UInt64, 1, true).addFixedStr("addr", 16);
        spec.setFacts(wire(ls));   // facts first: a lease topic's installVersion checks the system columns
        spec.installVersion(1, d.build());
        state.spec = &spec;
        ConstraintSet set;
        set.id = 1;
        set.count = 1;
        set.members[0] = ConstraintSetMember{0, rune::view::VerdictViewKind::LeaseIndex, kLeaseIndexPlanFingerprint};
        state.constraintSets.onChange(7, set);
        facts.acks               = kAcksViewAll;
        facts.idempotent         = true;
        facts.levelCheckedWrites = true;
        facts.clientVersion      = rune::protocol::client::kClientVersionLeases;
        facts.levelLeases        = true;
        facts.leaseWork          = work.get();
    }

    /// Installs schema version 2 (v1 plus `port uint32`).
    void installV2() {
        rune::codec::DescriptorBuilder d("inst", 2);
        d.add("lease_version", rune::codec::PrimType::Int64).add("lease_ttl_ms", rune::codec::PrimType::UInt64)
            .add("lease_holder", rune::codec::PrimType::UInt64).add("lease_remaining_ms", rune::codec::PrimType::UInt64)
            .add("id", rune::codec::PrimType::UInt64, 1, true).addFixedStr("addr", 16).add("port", rune::codec::PrimType::UInt32, 2);
        spec.installVersion(2, d.build());
    }
    /// Installs schema version 2 with a wide fixed block: v1 plus `blob` sized so the block is `blockLength` bytes.
    void installWide(uint16_t blockLength) {
        const auto build = [&](uint16_t blobLen) {
            rune::codec::DescriptorBuilder d("inst", 2);
            d.add("lease_version", rune::codec::PrimType::Int64).add("lease_ttl_ms", rune::codec::PrimType::UInt64)
                .add("lease_holder", rune::codec::PrimType::UInt64).add("lease_remaining_ms", rune::codec::PrimType::UInt64)
                .add("id", rune::codec::PrimType::UInt64, 1, true).addFixedStr("addr", 16).addFixedStr("blob", blobLen, 2);
            return d.build();
        };
        const uint16_t probe = build(1).blockLength;
        spec.installVersion(2, build(uint16_t(blockLength - (probe - 1))));
    }
    /// Builds the lease index from the installed versions and points the hook's facts at it (ready).
    void withIndex() {
        index = std::make_unique<IndexBox>(*spec.leaseLayoutSource());
        facts.leaseIndex = LeaseIndexRef{&index->table, &index->layout, &index->desc};
        facts.leaseIndexReady = true;
    }
    /// A row for `id`: version `ver`, ttl, remaining, and the user columns by field id (slot 5 addr, slot 6 port when present).
    uint32_t addRow(uint64_t id, int64_t ver, uint64_t ttl = 5000, uint64_t remaining = 0, const std::string& addr = "", uint32_t port = 0) {
        const auto key = rune::view::ViewKey::fromU64(id);
        uint8_t* row = index->table.put(key);
        const auto& l = index->layout;
        const auto setU64 = [&](uint16_t slot, uint64_t v) { std::memcpy(row + l.slotOffset[slot], &v, 8); rune::view::clearNull(row, l, slot); };
        setU64(0, uint64_t(ver)); setU64(1, ttl); setU64(2, 7); setU64(3, remaining); setU64(4, id);
        std::memset(row + l.slotOffset[5], 0, 16);
        std::memcpy(row + l.slotOffset[5], addr.data(), std::min<std::size_t>(addr.size(), 16));
        rune::view::clearNull(row, l, 5);
        if (l.slotType.size() > 8) { std::memcpy(row + l.slotOffset[6], &port, 4); rune::view::clearNull(row, l, 6); }
        uint32_t rid = 0;
        (void)index->table.ridOf(key, rid);
        setLeaseLastAppended(index->table, rid, -1);
        return rid;
    }
    int64_t lastAppendedOf(uint64_t id) const {
        uint32_t rid = 0;
        return index->table.ridOf(rune::view::ViewKey::fromU64(id), rid) ? leaseLastAppended(index->table, rid) : -2;
    }
    static TopicWriteSpecWire wire(const rune::protocol::LeaseSpec& ls) {
        TopicWriteSpecWire w;
        w.topicId    = 1;
        w.keyFieldId = 4;   // "id" is the fifth field (tag 4), as in the worker rig
        w.keyType    = rune::codec::PrimType::UInt64;
        w.keyWidth   = 8;
        w.lease      = ls;
        return w;
    }

    /// Fills the log with one plain batch of `n` records, so the next base offset is `n`.
    void preload(int n) {
        std::vector<LRec> recs;
        for (int i = 0; i < n; ++i) recs.push_back(updateOf(uint64_t(i + 1000), 1));
        const auto b = batchOf(recs);
        int64_t base = 0;
        (void)tlog.appendNoSyncRestamp(b.data(), b.size(), n - 1, base);
    }

    static int32_t countOf(const std::vector<uint8_t>& batch) {
        int32_t n = 0;
        (void)rune::protocol::forEachRecordSpan(batch.data(), batch.size(), [&](const rune::protocol::RecordSpan&) { ++n; });
        return n;
    }

    /// check only; the log is untouched.
    Outcome check(const std::vector<uint8_t>& batch, int32_t count = -1) {
        CheckedWriteHook hook(state, facts, scratch);
        Outcome o;
        o.code = hook.check(batch.data(), batch.size(), count < 0 ? countOf(batch) : count);
        o.why  = hook.refusal();
        o.retryAfterNs = hook.retryAfterNs();
        return o;
    }

    /// check, stage and (when `append`) append (what handleProduceBatch does).  `append` false = the append fails: onAppended never runs.
    Outcome write(const std::vector<uint8_t>& batch, bool keepCopy = true, bool append = true) {
        CheckedWriteHook hook(state, facts, scratch);
        const int32_t n = countOf(batch);
        Outcome o;
        o.code = hook.check(batch.data(), batch.size(), n);
        o.why  = hook.refusal();
        o.retryAfterNs = hook.retryAfterNs();
        if (o.code != ErrorCode::OK) return o;
        const auto staged = hook.stage(tlog, batch.data(), batch.size());
        o.code = staged.error;
        if (o.code != ErrorCode::OK) return o;
        o.staged = staged.batch;
        o.stagedLen = staged.len;
        if (keepCopy) o.copy.assign(staged.batch, staged.batch + staged.len);
        if (!append) { o.code = ErrorCode::TransientLogFull; return o; }
        if (tlog.appendNoSyncRestamp(staged.batch, staged.len, n - 1, o.base) < 0) { o.code = ErrorCode::TransientLogFull; return o; }
        hook.onAppended(o.base, o.base + n - 1);
        return o;
    }
};

}  // namespace rune::node::leasehook
