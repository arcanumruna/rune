#pragma once
// ---------------------------------------------------------------------------
// GroupErrorMapping.h — coordinator result codes → protocol ErrorCode.
//
// No-hang fix round 3 (fix-on-sight): an UNKNOWN coordinator result is never
// turned into a success answer (the old `default: return OK`) — it is
// Internal (a v5 client backs off on it, bounded by its retry budget).
// ---------------------------------------------------------------------------
#include <rune/coordination/GroupCoordinator.h>
#include <rune/protocol/client/Protocol.h>

#include <cstdint>

namespace rune::node {

// ---------------------------------------------------------------------------
// Helper: convert coordinator error codes → protocol ErrorCode (int16_t)
// No-hang fix round 3 (fix-on-sight): an UNKNOWN coordinator result is never
// turned into a success answer — it is Internal (a v5 client backs off on it,
// bounded by its retry budget).  Each mapper lists every known code.
// ---------------------------------------------------------------------------
[[nodiscard]] inline int16_t joinErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case 0:                           return static_cast<int16_t>(rune::protocol::client::ErrorCode::OK);
        case rune::coordination::JoinResult::kErrDead: return static_cast<int16_t>(rune::protocol::client::ErrorCode::GroupDead);
        default:                          return static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    }
}

[[nodiscard]] inline int16_t heartbeatErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case rune::coordination::HeartbeatResult::kOk:                   return static_cast<int16_t>(rune::protocol::client::ErrorCode::OK);
        case rune::coordination::HeartbeatResult::kErrUnknownMember:     return static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownMember);
        case rune::coordination::HeartbeatResult::kErrStaleGeneration:   return static_cast<int16_t>(rune::protocol::client::ErrorCode::StaleGeneration);
        case rune::coordination::HeartbeatResult::kErrRebalanceInProgress: return static_cast<int16_t>(rune::protocol::client::ErrorCode::RebalanceInProgress);
        default:                                            return static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    }
}

[[nodiscard]] inline int16_t syncErrorToProto(int coordErr) noexcept {
    switch (coordErr) {
        case rune::coordination::SyncResult::kOk:                   return static_cast<int16_t>(rune::protocol::client::ErrorCode::OK);
        case rune::coordination::SyncResult::kErrStaleGeneration:   return static_cast<int16_t>(rune::protocol::client::ErrorCode::StaleGeneration);
        case rune::coordination::SyncResult::kErrUnknownMember:     return static_cast<int16_t>(rune::protocol::client::ErrorCode::UnknownMember);
        case rune::coordination::SyncResult::kErrRebalanceInProgress: return static_cast<int16_t>(rune::protocol::client::ErrorCode::RebalanceInProgress);
        case rune::coordination::SyncResult::kErrWrongState:        return static_cast<int16_t>(rune::protocol::client::ErrorCode::RebalanceInProgress);
        default:                                       return static_cast<int16_t>(rune::protocol::client::ErrorCode::Internal);
    }
}


} // namespace rune::node
