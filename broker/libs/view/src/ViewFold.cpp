// libs/view/src/ViewFold.cpp
//
// Task 10: the single-source fold -- the heart of the Rune View Engine.
// Walks a committed prefix of a source topic's LogReadView batch-by-batch,
// decodes each record's codec-encoded VALUE against the WRITER's own schema
// version (never the latest), and applies Phase 1 Replace semantics into the
// view's ViewTable: upsert on a live record, erase on a tombstone. A source
// field the writer's version has dropped (or not yet added) decodes absent
// and writeSlot() leaves that output column NULL -- never garbage, never a
// stale carry-over from a previous row at that address.
//
// Hot path (sec14.0, <5us/record budget -- Task 11 benchmarks it): zero heap
// allocation in steady state. The only allocation is VersionDescriptorCache's
// first sight of a new writer version -- rare, not steady-state. No
// std::vector/std::function/std::string is built per record.
#include <rune/view/ViewFold.h>

#include <algorithm>
#include <cstring>

#include <rune/codec/Record.h>
#include <rune/protocol/RecordBatch.h>

namespace rune::view {

namespace {

using rune::codec::Descriptor;
using rune::codec::FieldValue;
using rune::codec::PrimType;

inline std::span<const uint8_t> asBytes(std::string_view sv) noexcept {
    return std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size());
}

// Folds a decoded FieldValue to the table's uint64_t key space: a signed
// integer key bit-casts through its two's-complement representation (`.i` is
// already correctly sign-extended to int64_t by readField); an unsigned key
// zero-extends (`.u`). Phase 1 keys are guaranteed integer types by the
// compiler (isIntegerKeyType), so every other PrimType is unreachable here.
inline uint64_t foldFieldValueToKey(const FieldValue& kf) noexcept {
    switch (kf.type) {
        case PrimType::Int32:
        case PrimType::Int64:
            return static_cast<uint64_t>(kf.i);
        default: // UInt16/UInt32/UInt64 (and the present=false {} default: i=u=0)
            return kf.u;
    }
}

// A tombstone's VALUE is empty by definition (no codec header, no fields) --
// so the only place the deleted key can come from is the record's WIRE key
// (the produce-routing key). This broker's producers stamp a view source's
// routing key as the raw fixed-width little-endian encoding of its integer
// merge-key field -- the same convention the compaction path (LogCleaner /
// OffsetMap) already relies on keying its map by the wire key. Decode those
// raw bytes against `keyType`'s width/signedness using the same
// signed-bitcast / unsigned-zero-extend rule as foldFieldValueToKey, just
// read directly off the wire instead of through a codec record.
uint64_t decodeWireKey(std::string_view key, PrimType keyType) noexcept {
    auto readU = [&](size_t width) -> uint64_t {
        uint64_t v = 0;
        const size_t n = std::min(width, key.size());
        for (size_t i = 0; i < n; ++i) {
            v |= static_cast<uint64_t>(static_cast<uint8_t>(key[i])) << (8 * i);
        }
        return v;
    };
    switch (keyType) {
        case PrimType::Int32: {
            const uint32_t u = static_cast<uint32_t>(readU(4));
            int32_t s;
            std::memcpy(&s, &u, sizeof s);
            return static_cast<uint64_t>(static_cast<int64_t>(s));
        }
        case PrimType::Int64: {
            const uint64_t u = readU(8);
            int64_t s;
            std::memcpy(&s, &u, sizeof s);
            return static_cast<uint64_t>(s);
        }
        case PrimType::UInt16: return readU(2);
        case PrimType::UInt32: return readU(4);
        case PrimType::UInt64: return readU(8);
        default: return 0; // unreachable: Phase 1 keys are integer types
    }
}

// The view's key column's PrimType, resolved once per foldBatch() call (NOT
// per record): the CompiledField whose srcFieldId matches the per-source key
// field id. The compiler guarantees the key appears in the output
// (compileView step 3), so this is always found for a well-formed plan.
PrimType resolveKeyType(const CompiledViewPlan& plan) noexcept {
    const uint16_t keyFieldId = plan.keyFieldId[0];
    for (const auto& f : plan.output) {
        if (f.srcFieldId == keyFieldId) {
            return f.type;
        }
    }
    return PrimType::Int64; // unreachable for a compiler-produced plan
}

} // namespace

FoldStats foldBatch(const CompiledViewPlan& plan, const RowLayout& layout,
                     VersionDescriptorCache& vdc, ViewTable& table,
                     const rune::storage::LogReadView& view, int64_t bound,
                     int64_t& cursor) {
    FoldStats stats{};
    const PrimType keyType = resolveKeyType(plan);
    bool stopped = false;

    size_t pos = 0;
    while (pos < view.len) {
        rune::protocol::RecordBatchReader r(view.data + pos, view.len - pos);
        if (!r.ensureValid()) {
            break; // torn/corrupt tail: stop, do not advance past it
        }
        if (r.isControl()) {
            ++stats.skippedControl;
            pos += static_cast<size_t>(r.totalLength());
            continue;
        }

        r.forEachRecord([&](std::string_view key, std::string_view value,
                             int64_t offset, int64_t /*timestamp*/) {
            if (stopped) {
                return; // already hit the uncommitted boundary this call
            }
            if (offset >= bound) {
                stopped = true; // don't fold uncommitted records
                return;
            }

            if (value.empty()) {
                // Tombstone: no codec payload to read a key from -- decode
                // the wire key instead (see decodeWireKey's doc above).
                const uint64_t k = decodeWireKey(key, keyType);
                table.erase(k);
                ++stats.deleted;
                return;
            }

            const uint16_t writerVersion =
                value.size() >= 4
                    ? static_cast<uint16_t>(static_cast<uint8_t>(value[2]) |
                                             (static_cast<uint8_t>(value[3]) << 8))
                    : vdc.defaultVersion();
            const Descriptor& d = vdc.materializedFor(writerVersion);

            const FieldValue kf = readField(d, asBytes(value), plan.keyFieldId[0]);
            const uint64_t k = foldFieldValueToKey(kf);

            uint8_t* row = table.put(k);
            for (const auto& f : plan.output) {
                const FieldValue in = readField(d, asBytes(value), f.srcFieldId);
                writeSlot(row, layout, f.outSlot, in);
            }
            ++stats.applied;
        });

        if (stopped) {
            break;
        }
        pos += static_cast<size_t>(r.totalLength());
    }

    cursor = std::min(view.nextOffset, bound);
    return stats;
}

} // namespace rune::view
