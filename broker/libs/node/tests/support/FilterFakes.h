#pragma once
// FilterFakes.h — Phase 2c test support for FilterGroup (Tasks 7, 14, 15): FakeSource, FakeSink, makePlayerBatch.
// Test code only: the fakes may allocate unless noted (FakeSink::recordFrames = false makes the sink alloc-free).
#include "rune/node/filter/FilterGroup.h"

#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Filter.h>
#include <rune/codec/FilterPredicate.h>
#include <rune/codec/FilterSchema.h>
#include <rune/codec/Flyweight.h>
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Varint.h>

#include "filter/PlayerEventSchema.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <span>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace rune::node::filter::fakes {

// ---------------------------------------------------------------------------------------------------------------
// player_event: tags playerId 0 u64, worldId 1 u32, channel 2 i32, zone 3 str(8), kind 4 enum, alive 5 bool,
// hp 6 i64, name 7 string, pos 8 f64; v2 ADD guild u32 (tag 9); v3 DROP channel; v4 ADD level u32 (tag 10).
// ---------------------------------------------------------------------------------------------------------------
inline rune::codec::Descriptor playerEvent(uint16_t uptoVersion = 4) {
    using namespace rune::codec;
    Descriptor d = rune::examples::playerEventDescriptor();
    uint16_t nextTag = 9;
    if (uptoVersion >= 2) {
        auto r = applyOps(d, {{AlterOpType::Add, "guild", "", PrimType::UInt32, 0, {}}}, nextTag, 2);
        if (!r) throw std::runtime_error("add guild");
        d = *r;
    }
    if (uptoVersion >= 3) {
        auto r = applyOps(d, {{AlterOpType::Drop, "channel", "", PrimType::Int32, 0, {}}}, nextTag, 3);
        if (!r) throw std::runtime_error("drop channel");
        d = *r;
    }
    if (uptoVersion >= 4) {   // v4 ADD level u32 (tag 10): a second version without channel
        auto r = applyOps(d, {{AlterOpType::Add, "level", "", PrimType::UInt32, 0, {}}}, nextTag, 4);
        if (!r) throw std::runtime_error("add level");
        d = *r;
    }
    return d;
}
inline rune::codec::FilterSchema playerSchema(uint16_t version) {
    return rune::codec::FilterSchema::build(playerEvent(version), version);
}

struct PlayerRow {
    uint64_t         playerId = 0;
    uint32_t         worldId = 0;
    int32_t          channel = 0;
    std::string_view zone;
    int32_t          kind = 1;
    bool             alive = false;
    int64_t          hp = 0;
    uint32_t         guild = 0;
    bool             tombstone = false;   // a null value
    int              shortValue = -1;     // >= 0: a value of that many bytes (malformed, shorter than the 4-byte header)
};

struct BatchOpts {
    bool    transactional = false;
    bool    control = false;
    int64_t producerId = -1;
};

inline std::vector<uint8_t> encodePlayerValue(uint16_t version, const PlayerRow& v) {
    using namespace rune::codec;
    static const Descriptor evolving = playerEvent(4);
    const Descriptor m = materializeAsOf(evolving, version);
    std::vector<uint8_t> out(4 + m.blockLength, 0);
    fly::store<uint16_t>(out.data(), static_cast<uint16_t>(m.blockLength));
    fly::store<uint16_t>(out.data() + 2, version);
    uint8_t* blk = out.data() + 4;
    auto at = [&](const char* n) -> uint8_t* { const auto* f = m.find(n); return f ? blk + f->offset : nullptr; };
    if (auto* p = at("playerId")) fly::store<uint64_t>(p, v.playerId);
    if (auto* p = at("worldId")) fly::store<uint32_t>(p, v.worldId);
    if (auto* p = at("channel")) fly::store<int32_t>(p, v.channel);
    if (auto* p = at("zone")) fly::storeFixedStr(p, v.zone, 8);
    if (auto* p = at("kind")) fly::store<int32_t>(p, v.kind);
    if (auto* p = at("alive")) fly::store<bool>(p, v.alive);
    if (auto* p = at("hp")) fly::store<int64_t>(p, v.hp);
    if (auto* p = at("guild")) fly::store<uint32_t>(p, v.guild);
    return out;
}

/// A Kafka-v2 RecordBatch at `base` with one record per row at schema `version` (tombstone rows are null values).
/// Record i has offset base + i.  Header fields are the RecordBatchBuilder's layout.
inline std::vector<uint8_t> makePlayerBatch(int64_t base, uint16_t version, const std::vector<PlayerRow>& rows,
                                            BatchOpts opts = {}) {
    using namespace rune::protocol;
    using namespace rune::protocol::batch;
    RecordBatchBuilder hb(base, 1'000);
    hb.append("", "x", 1'000);
    std::vector<uint8_t> b = hb.build();
    b.resize(kRecordBatchOverhead);   // keep the 61-byte header only
    int16_t attrs = 0;
    if (opts.control) attrs = static_cast<int16_t>(kAttrControl | kAttrTransactional);
    else if (opts.transactional) attrs = kAttrTransactional;
    b[kAttributesOffset]     = static_cast<uint8_t>(attrs);
    b[kAttributesOffset + 1] = static_cast<uint8_t>(attrs >> 8);
    for (int i = 0; i < 8; ++i) b[kProducerIdOffset + i] = static_cast<uint8_t>(static_cast<uint64_t>(opts.producerId) >> (8 * i));
    for (std::size_t i = 0; i < rows.size(); ++i) {
        std::vector<uint8_t> body;
        auto put = [&](const uint8_t* p, std::size_t n) { body.insert(body.end(), p, p + n); };
        uint8_t tmp[10];
        body.push_back(0);                                                             // attributes
        put(tmp, writeVarlong(0, tmp));                                                // tsDelta
        put(tmp, writeVarint(static_cast<int32_t>(i), tmp));                           // offsetDelta
        put(tmp, writeVarint(0, tmp));                                                 // key length
        const PlayerRow& r = rows[i];
        if (r.tombstone) {
            put(tmp, writeVarint(-1, tmp));
        } else if (r.shortValue >= 0) {
            put(tmp, writeVarint(r.shortValue, tmp));
            body.insert(body.end(), static_cast<std::size_t>(r.shortValue), uint8_t{0});
        } else {
            const auto v = encodePlayerValue(version, r);
            put(tmp, writeVarint(static_cast<int32_t>(v.size()), tmp));
            put(v.data(), v.size());
        }
        put(tmp, writeVarint(0, tmp));                                                 // header count
        uint8_t len[10];
        const std::size_t ln = writeVarint(static_cast<int32_t>(body.size()), len);
        b.insert(b.end(), len, len + ln);
        b.insert(b.end(), body.begin(), body.end());
    }
    const int32_t n = static_cast<int32_t>(rows.size());
    detail::putLE(b, kLastOffsetDeltaOffset, static_cast<uint32_t>(n - 1));
    detail::putLE(b, kRecordCountOffset, static_cast<uint32_t>(n));
    detail::putLE(b, kLengthOffset, static_cast<uint32_t>(b.size() - 12));
    detail::putLE(b, kCrcOffset, crc32c(b.data() + kAttributesOffset, b.size() - kAttributesOffset));
    return b;
}

// ---------------------------------------------------------------------------------------------------------------
class FakeSource {
public:
    struct Entry { int64_t base; int64_t lastOffset; std::size_t pos; std::size_t size; };

    /// Appends one whole batch (as makePlayerBatch built it); batches must ascend.
    void append(const std::vector<uint8_t>& b) {
        rune::protocol::RecordBatchReader r(b.data(), b.size());
        entries_.push_back({r.baseOffset(), r.baseOffset() + r.lastOffsetDelta(), bytes_.size(), b.size()});
        bytes_.insert(bytes_.end(), b.begin(), b.end());
    }
    /// Sets the bound to the end of the last appended batch.
    void publishAll() { bound_ = entries_.empty() ? 0 : entries_.back().lastOffset + 1; }
    void setBound(int64_t b) { bound_ = b; }
    void setReadCommitted(bool rc) { rc_ = rc; }
    void abort(int64_t pid, int64_t base) { aborted_.insert({pid, base}); }
    void setRingStart(int64_t s) { ringStart_ = s; }
    [[nodiscard]] const std::vector<Entry>& entries() const { return entries_; }

    // FilterSource
    [[nodiscard]] int64_t bound() const noexcept { return bound_; }
    [[nodiscard]] bool readCommitted() const noexcept { return rc_; }
    [[nodiscard]] bool isAborted(int64_t pid, int64_t base) const { return aborted_.count({pid, base}) != 0; }
    [[nodiscard]] int64_t skipEvicted(int64_t from) noexcept { ++skipEvictedCalls; return from < ringStart_ ? ringStart_ : from; }
    /// Whole batches from the batch holding `from`, up to `cap` bytes, but always at least that one batch.
    [[nodiscard]] std::span<const uint8_t> read(int64_t from, std::size_t cap) noexcept {
        ++reads;
        auto it = std::lower_bound(entries_.begin(), entries_.end(), from,
                                   [](const Entry& e, int64_t f) { return e.lastOffset < f; });
        if (it == entries_.end()) return {};
        const std::size_t start = it->pos;
        std::size_t total = 0;
        for (; it != entries_.end(); ++it) {
            if (total != 0 && total + it->size > cap) break;
            total += it->size;
        }
        return {bytes_.data() + start, total};
    }

    uint64_t skipEvictedCalls = 0;
    uint64_t reads = 0;

private:
    std::vector<Entry> entries_;
    std::vector<uint8_t> bytes_;
    std::set<std::pair<int64_t, int64_t>> aborted_;
    int64_t bound_ = 0;
    int64_t ringStart_ = 0;
    bool rc_ = false;
};
static_assert(FilterSource<FakeSource>);

struct Frame {
    uint32_t sub = 0;
    int64_t base = 0;
    std::vector<uint8_t> bytes;
    bool isNote = false;
    int64_t cursor = 0;   // notes: the position carried
};

class FakeSink {
public:
    // FilterSink
    SendOutcome sendBatch(uint32_t sub, int64_t base, std::span<const uint8_t> b) {
        need_(sub);
        ++calls;
        if (refuse_[sub] > 0) { --refuse_[sub]; return SendOutcome::Retry; }
        if (tooLarge_[sub] != 0) return SendOutcome::TooLarge;
        ++dataFrames;
        if (recordFrames) frames.push_back({sub, base, std::vector<uint8_t>(b.begin(), b.end()), false, 0});
        return SendOutcome::Sent;
    }
    SendOutcome sendNote(uint32_t sub, int64_t c) {
        need_(sub);
        ++calls;
        if (refuse_[sub] > 0) { --refuse_[sub]; return SendOutcome::Retry; }
        ++noteFrames;
        if (recordFrames) frames.push_back({sub, 0, {}, true, c});
        return SendOutcome::Sent;
    }
    int64_t& cursor(uint32_t sub) { need_(sub); return cursors_[sub]; }
    bool deliverable(uint32_t sub) { need_(sub); return blocked_[sub] == 0; }

    // Test controls
    void reserveSubs(std::size_t n) { need_(static_cast<uint32_t>(n)); }
    void refuse(uint32_t sub, uint32_t n) { need_(sub); refuse_[sub] = n; }   // the next n sends to `sub` answer Retry
    void setDeliverable(uint32_t sub, bool d) { need_(sub); blocked_[sub] = d ? 0 : 1; }
    void setTooLarge(uint32_t sub, bool t) { need_(sub); tooLarge_[sub] = t ? 1 : 0; }

    [[nodiscard]] std::vector<const Frame*> dataFor(uint32_t sub) const {
        std::vector<const Frame*> v;
        for (const auto& f : frames) if (f.sub == sub && !f.isNote) v.push_back(&f);
        return v;
    }
    [[nodiscard]] std::vector<int64_t> notesFor(uint32_t sub) const {
        std::vector<int64_t> v;
        for (const auto& f : frames) if (f.sub == sub && f.isNote) v.push_back(f.cursor);
        return v;
    }
    /// The record offsets `sub` received, in frame order (no client-side dedupe).
    [[nodiscard]] std::vector<int64_t> offsetsFor(uint32_t sub) const {
        std::vector<int64_t> v;
        for (const Frame* f : dataFor(sub)) {
            rune::protocol::RecordBatchReader r(f->bytes.data(), f->bytes.size());
            r.forEachRecord([&](std::string_view, std::string_view, int64_t o, int64_t) { v.push_back(o); });
        }
        return v;
    }
    /// What a model client with position `from` would keep: offsets >= its position, in order, advancing it.
    [[nodiscard]] std::vector<int64_t> clientView(uint32_t sub, int64_t from = 0) const {
        std::vector<int64_t> v;
        int64_t pos = from;
        for (const int64_t o : offsetsFor(sub)) if (o >= pos) { v.push_back(o); pos = o + 1; }
        return v;
    }

    std::vector<Frame> frames;
    bool recordFrames = true;
    uint64_t calls = 0, dataFrames = 0, noteFrames = 0;

private:
    void need_(uint32_t sub) {
        if (sub < cursors_.size()) return;
        cursors_.resize(sub + 1, 0);
        refuse_.resize(sub + 1, 0);
        blocked_.resize(sub + 1, 0);
        tooLarge_.resize(sub + 1, 0);
    }
    std::vector<int64_t> cursors_;
    std::vector<uint32_t> refuse_;
    std::vector<uint8_t> blocked_, tooLarge_;
};
static_assert(FilterSink<FakeSink>);

}  // namespace rune::node::filter::fakes
