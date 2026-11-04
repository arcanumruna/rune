// include/rune/client/WriteResult.h - the verdict of a checked typed write (TypedTopic::insertAndWait and friends).
#pragma once

#include <rune/client/Completion.h>

#include <chrono>
#include <cstdint>

namespace rune::client {

/// How long a typed *AndWait write waits for its verdict by default.
inline constexpr std::chrono::seconds kDefaultWriteWait{30};

class WriteResult {
public:
    [[nodiscard]] static WriteResult fromCompletion(const Completion& c) noexcept { return WriteResult(c); }

    [[nodiscard]] bool ok() const noexcept { return c_.status == OpStatus::Ok; }
    /// The broker's ErrorCode when status() == BrokerError (56 KeyExists, 57 KeyNotFound, ...), else 0.
    [[nodiscard]] uint16_t code() const noexcept { return c_.status == OpStatus::BrokerError ? c_.errorCode : 0; }
    [[nodiscard]] uint32_t constraintId() const noexcept { return constraintIdOf(c_); }
    /// The broker-assigned key of the record (0 = none).
    [[nodiscard]] uint64_t key() const noexcept { return assignedKeyOf(c_); }
    [[nodiscard]] OpStatus status() const noexcept { return c_.status; }
    /// The record's offset, -1 if none.
    [[nodiscard]] int64_t offset() const noexcept { return c_.value; }

private:
    explicit WriteResult(const Completion& c) noexcept : c_(c) {}
    Completion c_;
};

}  // namespace rune::client
