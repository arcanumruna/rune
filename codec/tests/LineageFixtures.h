// tests/LineageFixtures.h — #418: two schema lineages that move fields between versions (shared by the
// make_lineage_blobs fixture tool, which writes the evolving blob of every version for the generated headers, and
// by tests/LineageDecodeTest.cpp, which rebuilds the same lineage at run time).
//
// evo_mix (fixed + var):
//   v1  id Int64 KEY (t0) | age Int32 (t1) | score Int32 (t2) | nick String (t3) | bio String (t4) | flag Bool (t5)
//   v2  DROP age (a mid-block fixed slot: score moves 12 -> 8), DROP nick (the first var field: bio re-indexes 1 -> 0)
//   v3  ADD level Int32 (t6), ADD motto String (t7)
//   v4  RENAME score -> points, DROP bio (motto re-indexes 1 -> 0), ADD age Int64 (t8: the name again, a new tag)
// evo_fix (all fixed, so it also gets the zero-copy Decoder):
//   v1  id Int64 KEY (t0) | age Int32 (t1) | score Int32 (t2) | x Float64 (t3) | code string(4) (t4)
//   v2  DROP age
//   v3  ADD level Int32 (t5)
//   v4  RENAME x -> y, DROP score, ADD age UInt16 (t6)
#pragma once
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace rune::codec::testing {

enum class Lineage { Mix, Fix };

inline const char* lineageBase(Lineage l) { return l == Lineage::Mix ? "evo_mix" : "evo_fix"; }

/// The evolving descriptor of `l` at `version` (1..4), named `<base>_v<version>` (the name is what tells the
/// generated per-version types apart in one test binary; it never affects a layout).
inline Descriptor evolvingAt(Lineage l, uint16_t version, std::string name = {}) {
    using P = PrimType;
    using T = AlterOpType;
    if (name.empty()) name = std::string(lineageBase(l)) + "_v" + std::to_string(version);
    DescriptorBuilder b(name, 1);
    std::vector<std::vector<AlterOp>> steps;
    if (l == Lineage::Mix) {
        b.add("id", P::Int64, 1, true).add("age", P::Int32).add("score", P::Int32)
         .add("nick", P::String).add("bio", P::String).add("flag", P::Bool);
        steps = {
            {{T::Drop, "age", "", {}, 0, {}}, {T::Drop, "nick", "", {}, 0, {}}},
            {{T::Add, "level", "", P::Int32, 0, {}}, {T::Add, "motto", "", P::String, 0, {}}},
            {{T::Rename, "score", "points", {}, 0, {}}, {T::Drop, "bio", "", {}, 0, {}},
             {T::Add, "age", "", P::Int64, 0, {}}},
        };
    } else {
        b.add("id", P::Int64, 1, true).add("age", P::Int32).add("score", P::Int32).add("x", P::Float64)
         .addFixedStr("code", 4);
        steps = {
            {{T::Drop, "age", "", {}, 0, {}}},
            {{T::Add, "level", "", P::Int32, 0, {}}},
            {{T::Rename, "x", "y", {}, 0, {}}, {T::Drop, "score", "", {}, 0, {}}, {T::Add, "age", "", P::UInt16, 0, {}}},
        };
    }
    Descriptor d = b.build();
    uint16_t nextTag = static_cast<uint16_t>(d.fields.size());
    for (uint16_t v = 2; v <= version; ++v) {
        auto next = applyOps(d, steps[static_cast<std::size_t>(v - 2)], nextTag, v);
        if (!next) throw std::logic_error("lineage fixture: " + next.error().message);
        d = std::move(*next);
    }
    return d;
}

}  // namespace rune::codec::testing
