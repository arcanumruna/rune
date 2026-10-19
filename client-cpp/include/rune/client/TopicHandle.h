// include/rune/client/TopicHandle.h
//
// CB-2 — Lightweight topic handle returned by Client::topic(name).
//
// A TopicHandle is a cheap value type (two integers) that the producer caches
// to avoid repeated name→id lookups on the hot path.
//
// For Milestone B topics are configured statically via ClientConfig::topics
// (std::vector<TopicSpec>).  Dynamic topic metadata fetch from the broker is a
// TODO(dynamic) for a later milestone.

#pragma once

#include <rune/protocol/client/Protocol.h>   // kCatalogTopicId

#include <cstdint>
#include <string>

namespace rune::client {

// ---------------------------------------------------------------------------
// TopicSpec — static topic configuration entry in ClientConfig.
// ---------------------------------------------------------------------------
struct TopicSpec {
    std::string name;           // logical topic name
    int32_t     topicId        = 0;
    int32_t     partitionCount = 1;
};

// ---------------------------------------------------------------------------
// TopicHandle — returned by Client::topic(name).
// Cheap value type: two ints.
// ---------------------------------------------------------------------------
struct TopicHandle {
    int32_t topicId        = -1;
    int32_t partitionCount =  1;

    [[nodiscard]] bool valid() const noexcept { return topicId >= 0; }

    /// Catalog history (Task 13): the `__catalog` log (topic id −6, one
    /// partition), readable from ANY broker with Client::subscribe(catalog(), 0,
    /// …) — committed events only, then the caught-up marker
    /// (SubscribeOptions::onCaughtUp).  Not a producible topic (valid() is false).
    static constexpr TopicHandle catalog() noexcept { return {rune::protocol::client::kCatalogTopicId, 1}; }

    /// Authentication (Task 8): the `__security` log (topic id −7, one partition) — the same stream contract as
    /// catalog() (history-epoch note, committed records, caught-up marker), readable only by ADMIN CLUSTER holders
    /// (the broker answers NotAuthorized otherwise).  Not a producible topic.
    static constexpr TopicHandle security() noexcept { return {rune::protocol::client::kSecurityTopicId, 1}; }
};

/// The replicated event logs a client reads with the epoch-note / caught-up-marker stream contract.
[[nodiscard]] constexpr bool isEventLogTopic(int32_t topicId) noexcept {
    return topicId == rune::protocol::client::kCatalogTopicId || topicId == rune::protocol::client::kSecurityTopicId;
}

} // namespace rune::client
