#pragma once
// include/rune/client/catalog/CatalogReader.h — catalog history Task 13 (spec §7): the client consumes
// `__catalog` with the existing Subscribe from its cache end, replays it with the shared verifying
// rune::catalog::CatalogReplayer and stops at the broker's caught-up marker.  The reader's whole contract (cache =
// prefix + HISTORY EPOCH, the runner -> app ring, owner-thread rule, every refresh ends OK / Replay / Timeout /
// Rejected / Closed) lives in rune/client/detail/EventLogStream.h, shared with the __security reader (authentication
// Task 8); CatalogReader is that one design instantiated for the catalog log.
#include <rune/client/detail/EventLogStream.h>

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogReplayer.h>
#include <rune/catalog/CatalogState.h>

namespace rune::client::detail {

struct CatalogLogPolicy {
    using Replayer = rune::catalog::CatalogReplayer;
    using Event    = rune::catalog::CatalogEvent;
    using Recorded = rune::catalog::RecordedEvent;
    using State    = rune::catalog::CatalogState;
    static constexpr const char* kName        = "catalog";
    static constexpr const char* kReaderName  = "CatalogReader";
    static constexpr const char* kRefreshName = "CatalogReader::refresh()";
    static constexpr TopicHandle topic() noexcept { return TopicHandle::catalog(); }

    static auto decode(std::span<const uint8_t> bytes) { return rune::catalog::decode(bytes); }
    static CatalogReadError decodeFailure(int64_t offset, rune::catalog::DecodeError why) {
        rune::catalog::ReplayError e;
        e.offset = offset;
        e.code   = rune::catalog::ReplayError::Code::Decode;
        e.decode = why;
        return CatalogReadError{CatalogReadError::Kind::Replay, rune::catalog::describe(e), e.offset, 0};
    }
    /// Replays `e` at `offset`; the failure (naming the offset), or nullopt when applied.
    static std::optional<CatalogReadError> feed(Replayer& r, int64_t offset, const Event& e) {
        if (auto applied = r.feedDecoded(offset, e); !applied) {
            return CatalogReadError{CatalogReadError::Kind::Replay, rune::catalog::describe(applied.error()),
                                    applied.error().offset, 0};
        }
        return std::nullopt;
    }
};

} // namespace rune::client::detail

namespace rune::client {
using CatalogReader = detail::EventLogStream<detail::CatalogLogPolicy>;
extern template class detail::EventLogStream<detail::CatalogLogPolicy>;
} // namespace rune::client
