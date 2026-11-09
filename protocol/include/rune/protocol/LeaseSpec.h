#pragma once
// LeaseSpec.h — the per-topic lease configuration (DefineTopic / TopicSchemaResponse) and its bounds.
//
// Wire (little-endian, no padding, field order as declared), 39 bytes:
//   ttlMs u64, minTtlMs u64, maxTtlMs u64, maxKeys u32, expiryBatch u16, checkpoint u8,
//   checkpointIntervalMs u32, checkpointRate u32
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace rune::protocol {

struct LeaseSpec {
    uint64_t ttlMs                = 30'000;
    uint64_t minTtlMs             = 1'000;
    uint64_t maxTtlMs             = 9'000'000'000'000;
    uint32_t maxKeys              = 1'048'576;
    uint16_t expiryBatch          = 256;
    uint8_t  checkpoint           = 1;
    uint32_t checkpointIntervalMs = 300'000;
    uint32_t checkpointRate       = 1'000;
    friend constexpr bool operator==(const LeaseSpec&, const LeaseSpec&) = default;
};

inline constexpr std::size_t kLeaseSpecBytes      = 8 + 8 + 8 + 4 + 2 + 1 + 4 + 4;   // 39
static_assert(kLeaseSpecBytes == 39);
inline constexpr uint64_t    kLeaseMinTtlFloorMs  = 1000;
inline constexpr uint64_t    kLeaseMaxTtlMs       = 9'000'000'000'000;
inline constexpr std::size_t kMaxLeaseKeyBytes    = 128;
/// The largest lease.expiry_batch (records per Expire batch).
inline constexpr uint16_t    kMaxLeaseExpiryBatch = 4096;

namespace lease_detail {
inline void put(uint8_t* p, uint64_t v, int n) noexcept {
    for (int i = 0; i < n; ++i) p[i] = static_cast<uint8_t>(v >> (8 * i));
}
inline uint64_t get(const uint8_t* p, int n) noexcept {
    uint64_t v = 0;
    for (int i = 0; i < n; ++i) v |= static_cast<uint64_t>(p[i]) << (8 * i);
    return v;
}
} // namespace lease_detail

/// Writes exactly kLeaseSpecBytes bytes.
inline void encodeLeaseSpec(const LeaseSpec& s, uint8_t* out) noexcept {
    using lease_detail::put;
    put(out + 0, s.ttlMs, 8);
    put(out + 8, s.minTtlMs, 8);
    put(out + 16, s.maxTtlMs, 8);
    put(out + 24, s.maxKeys, 4);
    put(out + 28, s.expiryBatch, 2);
    put(out + 30, s.checkpoint, 1);
    put(out + 31, s.checkpointIntervalMs, 4);
    put(out + 35, s.checkpointRate, 4);
}

/// Reads exactly kLeaseSpecBytes bytes; false only when len is shorter.  The checkpoint byte is NOT checked here:
/// validateLeaseSpec refuses it ("lease.checkpoint must be 0 or 1") so a bad byte answers InvalidRequest, not InvalidSchema.
inline bool decodeLeaseSpec(const uint8_t* in, std::size_t len, LeaseSpec& out) noexcept {
    using lease_detail::get;
    if (len < kLeaseSpecBytes) return false;
    LeaseSpec s;
    s.ttlMs                = get(in + 0, 8);
    s.minTtlMs             = get(in + 8, 8);
    s.maxTtlMs             = get(in + 16, 8);
    s.maxKeys              = static_cast<uint32_t>(get(in + 24, 4));
    s.expiryBatch          = static_cast<uint16_t>(get(in + 28, 2));
    s.checkpoint           = in[30];
    s.checkpointIntervalMs = static_cast<uint32_t>(get(in + 31, 4));
    s.checkpointRate       = static_cast<uint32_t>(get(in + 35, 4));
    out = s;
    return true;
}

/// The detail texts of validateLeaseSpec - the ONE spelling, shared by the broker, the client and rune-ql.
inline constexpr std::string_view kLeaseMinTtlRangeText   = "lease.min_ttl must be between 1000 and lease.max_ttl ms";
inline constexpr std::string_view kLeaseMaxTtlRangeText   = "lease.max_ttl must be between lease.min_ttl and 9000000000000 ms";
inline constexpr std::string_view kLeaseTtlRangeText      = "lease.ttl must be between lease.min_ttl and lease.max_ttl ms";
inline constexpr std::string_view kLeaseMaxKeysRangeText  = "lease.max_keys must be between 1 and 16777216";
inline constexpr std::string_view kLeaseCheckpointText    = "lease.checkpoint must be 0 or 1";
inline constexpr std::string_view kLeaseCheckpointIntervalRangeText = "lease.checkpoint_interval must be between 1000 and 86400000 ms";
inline constexpr std::string_view kLeaseCheckpointRateRangeText     = "lease.checkpoint_rate must be between 1 and 100000";
inline constexpr std::string_view kLeaseExpiryBatchRangeText        = "lease.expiry_batch must be between 1 and 4096";

/// nullopt = valid; otherwise a static detail string naming the offending key.
[[nodiscard]] inline std::optional<std::string_view> validateLeaseSpec(const LeaseSpec& s) noexcept {
    if (s.minTtlMs < kLeaseMinTtlFloorMs || s.minTtlMs > s.maxTtlMs) return kLeaseMinTtlRangeText;
    if (s.maxTtlMs < s.minTtlMs || s.maxTtlMs > kLeaseMaxTtlMs) return kLeaseMaxTtlRangeText;
    if (s.ttlMs < s.minTtlMs || s.ttlMs > s.maxTtlMs) return kLeaseTtlRangeText;
    if (s.maxKeys < 1 || s.maxKeys > 16'777'216) return kLeaseMaxKeysRangeText;
    if (s.checkpoint > 1) return kLeaseCheckpointText;
    if (s.checkpointIntervalMs < 1000 || s.checkpointIntervalMs > 86'400'000) return kLeaseCheckpointIntervalRangeText;
    if (s.checkpointRate < 1 || s.checkpointRate > 100'000) return kLeaseCheckpointRateRangeText;
    if (s.expiryBatch < 1 || s.expiryBatch > kMaxLeaseExpiryBatch) return kLeaseExpiryBatchRangeText;
    return std::nullopt;
}

} // namespace rune::protocol
