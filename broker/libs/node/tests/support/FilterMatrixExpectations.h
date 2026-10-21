#pragma once

#include <cstdint>
#include <optional>

namespace rune::node::filtermatrix {

enum class Op : uint8_t { Unfiltered, Eq, In, EqAndIn, MixedIndexFields };
enum class Entity : uint8_t { DiskRU, DiskRC, TransientRU, TransientRC };
enum class Timing : uint8_t {
    SteadyAllMatch, SteadyPartial, SteadyNoMatch, StraddlingResume, QuietRun, OpenTxn, AbortedTxn,
    CommittedTxn, OwnershipMove, TransientEviction, AlterAddField, AlterDropFilteredField,
    VersionArrivesLate, BackPressure
};
enum class NaRule : uint8_t { NA1, NA2 };
enum class Outcome : uint8_t {
    ByteIdentical, ZeroCopyOriginal, Rebuilt, NoteOnly, WholeBatchFromPosition, NotesByCountAndTime,
    WithheldUntilCommit, Delivered, NeverDelivered, ResumeExact, SkipEvictedCounted, AbsentExcluded,
    StallThenDeliver, NoLossNoDupRejoined
};

[[nodiscard]] constexpr bool isTransient(Entity e) noexcept {
    return e == Entity::TransientRU || e == Entity::TransientRC;
}
[[nodiscard]] constexpr bool isRc(Entity e) noexcept { return e == Entity::DiskRC || e == Entity::TransientRC; }

[[nodiscard]] constexpr std::optional<NaRule> naRule(Op o, Entity e, Timing t) noexcept {
    if (o == Op::Unfiltered && t != Timing::SteadyAllMatch && t != Timing::AbortedTxn &&
        t != Timing::OwnershipMove && t != Timing::BackPressure) {
        return NaRule::NA1;
    }
    if (t == Timing::TransientEviction && !isTransient(e)) return NaRule::NA2;
    return std::nullopt;
}

[[nodiscard]] constexpr Outcome expect(Op o, Entity e, Timing t) noexcept {
    if (o == Op::Unfiltered) return Outcome::ByteIdentical;  // R1
    switch (t) {
    case Timing::SteadyAllMatch: return Outcome::ZeroCopyOriginal;         // R2
    case Timing::SteadyPartial: return Outcome::Rebuilt;                    // R3
    case Timing::SteadyNoMatch: return Outcome::NoteOnly;                   // R4
    case Timing::StraddlingResume: return Outcome::WholeBatchFromPosition;  // R5
    case Timing::QuietRun: return Outcome::NotesByCountAndTime;             // R6
    case Timing::OpenTxn: return isRc(e) ? Outcome::WithheldUntilCommit : Outcome::Delivered;  // R7
    case Timing::AbortedTxn: return isRc(e) ? Outcome::NeverDelivered : Outcome::Delivered;    // R8
    case Timing::CommittedTxn: return Outcome::Delivered;                   // R9
    case Timing::OwnershipMove: return Outcome::ResumeExact;                // R10
    case Timing::TransientEviction: return Outcome::SkipEvictedCounted;     // R11
    case Timing::AlterAddField: return Outcome::Delivered;                  // R12
    case Timing::AlterDropFilteredField:                                    // R13
        return (o == Op::Eq || o == Op::MixedIndexFields) ? Outcome::Delivered : Outcome::AbsentExcluded;
    case Timing::VersionArrivesLate: return Outcome::StallThenDeliver;      // R14
    case Timing::BackPressure: return Outcome::NoLossNoDupRejoined;         // R15
    }
    return Outcome::Delivered;
}

}  // namespace rune::node::filtermatrix
