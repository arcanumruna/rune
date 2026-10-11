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
#include <string>
#include <vector>

namespace rune::client::view {

// Mirrors rune::view::MergeRule (libs/view/include/rune/view/ViewTypes.h).
// Phase 1 clients only ever author Replace (single-source projection; no
// aggregates until Phase 2), but the full set is carried so a plan authored
// here round-trips byte-identically through the broker's deserializeAuthored.
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

} // namespace rune::client::view
