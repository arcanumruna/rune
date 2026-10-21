#pragma once
// ProducerIdBinding.h — a producer id carries the principal it was issued to (issue #122).
//
// The broker issues every producer id in the BOUND format:
//
//   bit 63      0                       (a producer id is never negative; -1 = none)
//   bit 62      1                       (bound format)
//   bits 61..24 the allocator's counter (38 bits, monotone: allocation order = numeric order)
//   bits 23..0  the owner tag           (the issuing principal, see pidOwnerTag)
//
// A Produce (and AddPartitions / AddOffsets / TxnOffsetCommit / EndTxn) naming a bound producer id is honoured only
// for the principal whose tag it carries, so a principal can never fence, sequence-collide with or pin the LSO under
// another principal's producer.  The check is stateless: every partition leader, follower promotion, coordinator
// failover and boot-time recovery sees the owner in the id itself, with nothing to replicate, and it costs a mask and
// a compare on the produce path (no allocation, no lock, no lookup).
//
// LEGACY ids (bit 62 clear) were issued before the bound format existed (an older coordinator, a mixed-version
// cluster mid-upgrade).  They carry no owner and stay usable by anyone, exactly as before (version skew): the broker
// never issues one, and a transactional id still holding one gets a bound id at its next InitProducerId.
//
// The tag is the principal's security object id itself while it fits 23 bits (8.4 M users and roles: exact, no two
// principals share it); a larger id is folded into the upper half of the tag space.  Principal 0 = none (dev /
// in-process brokers, no authentication): its ids carry tag 0.
#include <cstdint>

namespace rune::protocol {

inline constexpr int     kPidOwnerBits   = 24;
inline constexpr int64_t kBoundPidFlag   = int64_t{1} << 62;
inline constexpr int64_t kPidOwnerMask   = (int64_t{1} << kPidOwnerBits) - 1;
inline constexpr int64_t kMaxPidCounter  = (int64_t{1} << (62 - kPidOwnerBits)) - 1;

/// The 24-bit owner tag of `principal` (its exact id below 2^23; else a fold into [2^23, 2^24)).
[[nodiscard]] constexpr uint32_t pidOwnerTag(uint64_t principal) noexcept {
    constexpr uint64_t kExact = uint64_t{1} << (kPidOwnerBits - 1);
    if (principal < kExact) return static_cast<uint32_t>(principal);
    uint64_t z = principal + 0x9E3779B97F4A7C15ull;   // splitmix64 finalizer
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    z ^= z >> 31;
    return static_cast<uint32_t>(kExact | (z & (kExact - 1)));
}

/// True for an id in the bound format.
[[nodiscard]] constexpr bool isBoundProducerId(int64_t pid) noexcept { return pid >= 0 && (pid & kBoundPidFlag) != 0; }

/// The bound id of allocator counter `counter` (0..kMaxPidCounter) issued to `principal`.
[[nodiscard]] constexpr int64_t bindProducerId(int64_t counter, uint64_t principal) noexcept {
    return kBoundPidFlag | (counter << kPidOwnerBits) | static_cast<int64_t>(pidOwnerTag(principal));
}

/// The allocator counter an id was issued from: the counter bits of a bound id, the id itself for a legacy one.
/// Monotone in the id over issued ids (every bound id is above every legacy one and is ordered by its counter), so a
/// recovered maximum id reseeds the allocator past every issued counter.
[[nodiscard]] constexpr int64_t producerIdCounter(int64_t pid) noexcept {
    return isBoundProducerId(pid) ? ((pid & ~kBoundPidFlag) >> kPidOwnerBits) : pid;
}

/// True when `pid` was issued to `principal` (a bound id carrying its tag).  A legacy id is owned by nobody.
[[nodiscard]] constexpr bool producerIdOwnedBy(int64_t pid, uint64_t principal) noexcept {
    return isBoundProducerId(pid) && (pid & kPidOwnerMask) == static_cast<int64_t>(pidOwnerTag(principal));
}

/// THE request check against a precomputed `tag` (pidOwnerTag of the session's principal): a bound id only for its
/// owner; a legacy id (version skew) and "no producer id" (negative) as before.  Branch-free: three ALU ops.
[[nodiscard]] constexpr bool producerIdUsableByTag(int64_t pid, uint32_t tag) noexcept {
    const bool bound = (pid & (kBoundPidFlag | INT64_MIN)) == kBoundPidFlag;   // bit 62 set, sign clear
    return !bound | ((pid & kPidOwnerMask) == static_cast<int64_t>(tag));
}

/// THE request check: may `principal` name `pid`?
[[nodiscard]] constexpr bool producerIdUsableBy(int64_t pid, uint64_t principal) noexcept {
    return producerIdUsableByTag(pid, pidOwnerTag(principal));
}

static_assert(bindProducerId(kMaxPidCounter, uint64_t{1} << 40) > 0, "a bound id is never negative");
static_assert(producerIdCounter(bindProducerId(12345, 7)) == 12345);
static_assert(producerIdOwnedBy(bindProducerId(1, 7), 7) && !producerIdOwnedBy(bindProducerId(1, 7), 8));
static_assert(producerIdUsableBy(42, 8) && producerIdUsableBy(-1, 8), "legacy and absent ids are not refused");
static_assert(producerIdUsableBy(-1, (uint64_t{1} << 24) - 1) && producerIdUsableBy(INT64_MIN | kBoundPidFlag, 3),
              "a negative id is never a bound one, whatever its bits");

} // namespace rune::protocol
