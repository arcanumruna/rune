// include/rune/client/lease/LeaseResult.h - the results of lease operations (Task 14a).
#pragma once

#include <rune/client/Completion.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>

namespace rune::client {

/// What a blocking (*AndWait) lease call returns: the completion's status and the lease fields of an Ok.
struct LeaseResult {
    OpStatus status    = OpStatus::Ok;
    uint16_t errorCode = 0;    ///< the broker code when status is BrokerError or RecordsRejected
    int64_t  version   = 0;    ///< grant / update / revoke: the leaseVersion; timeToLive: the version
    int64_t  ttlMs     = 0;    ///< grant / update / revoke / keepAlive: the ttl; timeToLive: the remaining ms (-1 = none)
    [[nodiscard]] bool ok() const noexcept { return status == OpStatus::Ok; }
};

/// The payload of an OpKind::LeaseTimeToLive completion: 32 LE bytes.
struct LeaseTtlInfo {
    uint64_t grantedTtlMs = 0;
    int64_t  remainingMs  = -1;   ///< -1 = the lease has no running ttl
    int64_t  version      = 0;
    uint64_t holder       = 0;

    static constexpr std::size_t kBytes = 32;
    [[nodiscard]] static std::optional<LeaseTtlInfo> decode(std::span<const uint8_t> p) noexcept {
        if (p.size() != kBytes) return std::nullopt;
        LeaseTtlInfo i;
        std::memcpy(&i.grantedTtlMs, p.data(), 8);   // little-endian hosts only (the client's wire contract)
        std::memcpy(&i.remainingMs, p.data() + 8, 8);
        std::memcpy(&i.version, p.data() + 16, 8);
        std::memcpy(&i.holder, p.data() + 24, 8);
        return i;
    }
};

/// Walks the rows of an OpKind::ScanLeases payload: [keyLen u8][key][rowLen u32][row] ... fn(key, row).  False when the
/// payload is malformed (a row cut short); the rows before it were delivered.
template <class Fn>
[[nodiscard]] bool forEachLeaseRow(std::span<const uint8_t> payload, Fn&& fn) {
    std::size_t pos = 0;
    while (pos < payload.size()) {
        const std::size_t klen = payload[pos++];
        if (payload.size() - pos < klen + 4u) return false;
        const std::span<const uint8_t> key = payload.subspan(pos, klen);
        pos += klen;
        uint32_t rlen = 0;
        std::memcpy(&rlen, payload.data() + pos, 4);
        pos += 4;
        if (payload.size() - pos < rlen) return false;
        fn(key, payload.subspan(pos, rlen));
        pos += rlen;
    }
    return true;
}

/// A ScanLeases completion's page meta.
struct LeaseScanPage {
    int64_t  asOfOffset = -1;
    uint64_t lastHash   = 0;
    uint32_t rowCount   = 0;
    bool     done       = false;
    [[nodiscard]] static LeaseScanPage of(const Completion& c) noexcept {
        LeaseScanPage p;
        p.asOfOffset = c.value;
        p.lastHash   = static_cast<uint64_t>(c.aux);
        p.rowCount   = c.reserved0 & 0x7FFFFFFFu;
        p.done       = (c.reserved0 >> 31) != 0;
        return p;
    }
};

/// The LeaseResult of a completed lease write or request.
[[nodiscard]] inline LeaseResult leaseResultOf(const Completion& c) noexcept {
    LeaseResult r;
    r.status    = c.status;
    r.errorCode = c.errorCode;
    if (c.status == OpStatus::Ok) {
        if (c.op == OpKind::LeaseKeepAlive) r.ttlMs = c.aux;
        else if (c.op == OpKind::LeaseTimeToLive) { r.version = c.value; r.ttlMs = c.aux; }
        else { r.version = c.value; r.ttlMs = c.aux; }
    }
    return r;
}

}  // namespace rune::client
