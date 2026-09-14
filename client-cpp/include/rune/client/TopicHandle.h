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
};

} // namespace rune::client
