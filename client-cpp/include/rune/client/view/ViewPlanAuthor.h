// include/rune/client/view/ViewPlanAuthor.h
//
// Task 21 — client-side authoring of a materialized-view `CreateView` plan
// blob.
//
// `rune-broker/libs/view/include/rune/view/ViewPlan.h` (the broker-internal
// Phase 1 view engine) defines the authored `ViewPlan` struct and its
// `serializeAuthored()` wire encoding — the exact bytes a `CreateView`
// message's `planBlob` field carries (see SchemaMessages.h CreateView, wire
// id 70). That library is broker/node-internal (libs/view, libs/node) and is
// NOT a client dependency (shared-nothing across repos: the client has no
// business linking AdminWorker/WorkerViewSet/etc.).
//
// This header is a client-side, independent reimplementation of JUST the
// authored-plan wire format (ViewPlan::serializeAuthored, verified byte-for-
// byte against libs/view/src/ViewPlan.cpp as of the view-engine-phase1
// branch): version:u8, viewName:str, key:str, sources:{topicId:i32,
// keyField:str}*, output:{name:str, type:u8, sourceIdx:u16, srcField:str,
// rule:u8}*, persistence:u8, sizing:{chunkBytes:u32, capacity:u32,
// maxRows:u64}. Any wire-format drift between the two must be caught by the
// e2e (a client-authored plan the broker cannot deserialize fails CreateView
// outright, not silently).
//
// Cold control-plane path (CreateView is a one-time, per-view DDL op) — not
// hot-path, but still disciplined: no shared mutable state, caller owns the
// ViewPlan value.
#pragma once

#include <rune/codec/PrimType.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace rune::client::view {

// Mirrors rune::view::MergeRule (libs/view/include/rune/view/ViewTypes.h).
// Phase 2a brokers accept Replace plus the invertible aggregates CountAll
// (Count(*)), Count(col), Sum and Avg (GROUP BY the view key, single source of
// one partition); Min/Max/Last are not accepted until Phase 2b. The full set is
// carried so a plan authored here round-trips byte-identically through the
// broker's deserializeAuthored.
enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll };  // CountAll=7 (Count(*))

// WITH-clause sizing knobs (rune::view::ViewSizing). 0 fields take the
// broker's own defaults.
struct ViewSizing {
    uint32_t chunkBytes = 32u << 20;
    uint32_t capacity    = 0;
    uint64_t maxRows     = 0;
};

// One authored output field (rune::view::AuthoredField).
struct AuthoredField {
    std::string          name;
    rune::codec::PrimType type;
    uint16_t             sourceIdx = 0;
    std::string          srcField;
    MergeRule            rule = MergeRule::Replace;
};

// One authored source (rune::view::SourceSpec).
struct SourceSpec {
    int32_t     topicId = -1;
    std::string keyField;
};

// The authoring form of a `CreateView` plan (rune::view::ViewPlan).
struct ViewPlan {
    std::string                viewName;
    std::string                key;
    std::vector<SourceSpec>    sources;
    std::vector<AuthoredField> output;
    uint8_t                    persistence = 0;  // 0 = persisted, 1 = transient
    ViewSizing                 sizing;
};

namespace detail {

inline void writeU8(std::vector<uint8_t>& buf, uint8_t v) { buf.push_back(v); }

inline void writeU16LE(std::vector<uint8_t>& buf, uint16_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
}

inline void writeU32LE(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 8) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 16) & 0xff));
    buf.push_back(static_cast<uint8_t>((v >> 24) & 0xff));
}

inline void writeI32LE(std::vector<uint8_t>& buf, int32_t v) {
    uint32_t u;
    std::memcpy(&u, &v, 4);
    writeU32LE(buf, u);
}

inline void writeU64LE(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) buf.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xff));
}

inline void writeString(std::vector<uint8_t>& buf, const std::string& s) {
    writeU32LE(buf, static_cast<uint32_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

inline void writeSizing(std::vector<uint8_t>& buf, const ViewSizing& s) {
    writeU32LE(buf, s.chunkBytes);
    writeU32LE(buf, s.capacity);
    writeU64LE(buf, s.maxRows);
}

} // namespace detail

// Authored-plan wire version (must track libs/view/src/ViewPlan.cpp's
// kAuthoredVersion == 1).
inline constexpr uint8_t kAuthoredViewPlanVersion = 1;

// Serialize `plan` into the exact bytes the broker's
// rune::view::deserializeAuthored() expects as a CreateView planBlob.
inline std::vector<uint8_t> serializeAuthored(const ViewPlan& plan) {
    std::vector<uint8_t> buf;
    buf.reserve(64 + plan.viewName.size() + plan.key.size());

    detail::writeU8(buf, kAuthoredViewPlanVersion);
    detail::writeString(buf, plan.viewName);
    detail::writeString(buf, plan.key);

    detail::writeU32LE(buf, static_cast<uint32_t>(plan.sources.size()));
    for (const SourceSpec& s : plan.sources) {
        detail::writeI32LE(buf, s.topicId);
        detail::writeString(buf, s.keyField);
    }

    detail::writeU32LE(buf, static_cast<uint32_t>(plan.output.size()));
    for (const AuthoredField& f : plan.output) {
        detail::writeString(buf, f.name);
        detail::writeU8(buf, static_cast<uint8_t>(f.type));
        detail::writeU16LE(buf, f.sourceIdx);
        detail::writeString(buf, f.srcField);
        detail::writeU8(buf, static_cast<uint8_t>(f.rule));
    }

    detail::writeU8(buf, plan.persistence);
    detail::writeSizing(buf, plan.sizing);

    return buf;
}

namespace detail {

// Bounds-checked little-endian readers: false on truncation, pos advances only on success.
inline bool readU8(std::span<const uint8_t> b, std::size_t& pos, uint8_t& out) {
    if (b.size() - pos < 1 || pos > b.size()) return false;
    out = b[pos++];
    return true;
}
inline bool readU16LE(std::span<const uint8_t> b, std::size_t& pos, uint16_t& out) {
    if (pos > b.size() || b.size() - pos < 2) return false;
    out = static_cast<uint16_t>(b[pos] | (b[pos + 1] << 8));
    pos += 2;
    return true;
}
inline bool readU32LE(std::span<const uint8_t> b, std::size_t& pos, uint32_t& out) {
    if (pos > b.size() || b.size() - pos < 4) return false;
    out = static_cast<uint32_t>(b[pos]) | (static_cast<uint32_t>(b[pos + 1]) << 8) |
          (static_cast<uint32_t>(b[pos + 2]) << 16) | (static_cast<uint32_t>(b[pos + 3]) << 24);
    pos += 4;
    return true;
}
inline bool readU64LE(std::span<const uint8_t> b, std::size_t& pos, uint64_t& out) {
    if (pos > b.size() || b.size() - pos < 8) return false;
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[pos + static_cast<std::size_t>(i)]) << (8 * i);
    out = v;
    pos += 8;
    return true;
}
inline bool readString(std::span<const uint8_t> b, std::size_t& pos, std::string& out) {
    uint32_t len = 0;
    if (!readU32LE(b, pos, len)) return false;
    if (b.size() - pos < len) return false;   // pos <= size here: never an overrun
    out.assign(reinterpret_cast<const char*>(b.data() + pos), len);
    pos += len;
    return true;
}

} // namespace detail

// The inverse of serializeAuthored (a `rune catalog replay` re-reads the normalized plan an
// event stores).  Strict like the broker's rune::view::deserializeAuthored: nullopt on a wrong
// version, a truncated blob, an out-of-range type / merge rule or TRAILING bytes — never a
// partial plan.  Counts are untrusted: nothing is reserved from them.
inline std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t> buf) {
    std::size_t pos = 0;
    ViewPlan p;
    uint8_t version = 0;
    if (!detail::readU8(buf, pos, version) || version != kAuthoredViewPlanVersion) return std::nullopt;
    if (!detail::readString(buf, pos, p.viewName) || !detail::readString(buf, pos, p.key)) return std::nullopt;

    uint32_t n = 0;
    if (!detail::readU32LE(buf, pos, n)) return std::nullopt;
    for (uint32_t i = 0; i < n; ++i) {
        SourceSpec s;
        uint32_t id = 0;
        if (!detail::readU32LE(buf, pos, id) || !detail::readString(buf, pos, s.keyField)) return std::nullopt;
        s.topicId = static_cast<int32_t>(id);
        p.sources.push_back(std::move(s));
    }
    if (!detail::readU32LE(buf, pos, n)) return std::nullopt;
    for (uint32_t i = 0; i < n; ++i) {
        AuthoredField f;
        uint8_t type = 0, rule = 0;
        if (!detail::readString(buf, pos, f.name) || !detail::readU8(buf, pos, type) ||
            !detail::readU16LE(buf, pos, f.sourceIdx) || !detail::readString(buf, pos, f.srcField) ||
            !detail::readU8(buf, pos, rule))
            return std::nullopt;
        if (type > static_cast<uint8_t>(rune::codec::PrimType::FixedStr)) return std::nullopt;
        if (rule > static_cast<uint8_t>(MergeRule::CountAll)) return std::nullopt;
        f.type = static_cast<rune::codec::PrimType>(type);
        f.rule = static_cast<MergeRule>(rule);
        p.output.push_back(std::move(f));
    }
    if (!detail::readU8(buf, pos, p.persistence)) return std::nullopt;
    if (!detail::readU32LE(buf, pos, p.sizing.chunkBytes) || !detail::readU32LE(buf, pos, p.sizing.capacity) ||
        !detail::readU64LE(buf, pos, p.sizing.maxRows))
        return std::nullopt;
    if (pos != buf.size()) return std::nullopt;
    return p;
}

// Sets source i's topicId to topicIds[i] (a normalized plan names its sources by ordinal,
// ruling 11; a replay maps them onto the TARGET cluster's topic ids).  False — and the plan
// untouched — when the counts differ.
inline bool remapSources(ViewPlan& plan, std::span<const int32_t> topicIds) {
    if (plan.sources.size() != topicIds.size()) return false;
    for (std::size_t i = 0; i < topicIds.size(); ++i) plan.sources[i].topicId = topicIds[i];
    return true;
}

} // namespace rune::client::view
