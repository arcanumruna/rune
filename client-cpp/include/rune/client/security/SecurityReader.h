#pragma once
// include/rune/client/security/SecurityReader.h — authentication Task 8 (spec §3): the client consumes `__security`
// (topic id −7) with the existing Subscribe from its cache end and replays it with the shared verifying
// rune::security::SecurityReplayer (every fingerprint checked) up to the broker's caught-up marker — the catalog
// reader's design (rune/client/detail/EventLogStream.h), instantiated for the security log.  Reads are restricted
// to ADMIN CLUSTER holders: the broker answers anything else NotAuthorized, which surfaces as
// CatalogReadError::Kind::Rejected with brokerErrorCode 45.  Rows never carry verifier bytes (the library's rule).
#include <rune/client/detail/EventLogStream.h>

#include <rune/security/SecurityEvent.h>
#include <rune/security/SecurityQueries.h>
#include <rune/security/SecurityReplayer.h>
#include <rune/security/SecurityState.h>

namespace rune::client::detail {

struct SecurityLogPolicy {
    using Replayer = rune::security::SecurityReplayer;
    using Event    = rune::security::SecurityEvent;
    using Recorded = rune::security::RecordedEvent;
    using State    = rune::security::SecurityState;
    static constexpr const char* kName        = "security";
    static constexpr const char* kReaderName  = "SecurityReader";
    static constexpr const char* kRefreshName = "SecurityReader::refresh()";
    static constexpr TopicHandle topic() noexcept { return TopicHandle::security(); }

    static auto decode(std::span<const uint8_t> bytes) { return rune::security::decode(bytes); }
    static CatalogReadError decodeFailure(int64_t offset, rune::security::DecodeError why) {
        rune::security::ReplayError e;
        e.kind   = rune::security::ReplayError::Kind::Decode;
        e.offset = offset;
        e.decode = why;
        return CatalogReadError{CatalogReadError::Kind::Replay, rune::security::describe(e), offset, 0};
    }
    static std::optional<CatalogReadError> feed(Replayer& r, int64_t offset, const Event& e) {
        if (auto applied = r.feedDecoded(offset, e); !applied) {
            return CatalogReadError{CatalogReadError::Kind::Replay, rune::security::describe(applied.error()),
                                    applied.error().offset, 0};
        }
        return std::nullopt;
    }
};

} // namespace rune::client::detail

namespace rune::client {
using SecurityReader = detail::EventLogStream<detail::SecurityLogPolicy>;
extern template class detail::EventLogStream<detail::SecurityLogPolicy>;
} // namespace rune::client
