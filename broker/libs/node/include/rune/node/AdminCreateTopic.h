#pragma once
// ---------------------------------------------------------------------------
// AdminCreateTopic.h — the CONTROL-thread half of the admin create-topic round
// trip (AdminWorker::requestCreateTopic → createReqRing → HERE → replyRing).
//
// Runs on the control thread, which owns TopicRegistry (single writer).  One
// function so BrokerNode and the test harnesses that play the control thread
// answer a CreateTopicReqMsg identically.
//
// Phase 2b C final review I1: TopicRegistry::createTopic is idempotent by name —
// a name that already names a topic returns THAT topic's id and ignores the
// requested spec.  The reply therefore reports the authoritative partition count
// (read back from the registry's spec) and whether the topic already existed, so
// the admin thread never records the client's requested count for a topic that
// really has a different one, and never adopts an existing topic as a view sink.
// ---------------------------------------------------------------------------
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/NodeConfig.h>

#include <spdlog/spdlog.h>

#include <exception>

namespace rune::node {

inline CreateTopicReplyMsg
createTopicForAdmin(rune::gateway::TopicRegistry& reg, const CreateTopicReqMsg& req) {
    CreateTopicReplyMsg reply;
    reply.ticket  = req.ticket;
    reply.existed = reg.topicIdForName(req.name).has_value();

    TopicSpec spec;
    spec.name           = req.name;
    spec.partitionCount = req.partitionCount;
    spec.topicId        = -1;  // registry assigns
    // Task 19: a view SINK is a keyed + Compact "table" — carry the cleanup
    // policy + key field the admin thread resolved (DefineTopic leaves them
    // default: Delete, empty).
    spec.cleanupPolicy  = static_cast<TopicSpec::CleanupPolicy>(req.cleanupPolicy);
    if (!req.keyField.empty()) spec.partitionKey = req.keyField;

    try {
        reply.topicId = reg.createTopic(spec);
    } catch (const std::exception& ex) {
        spdlog::error("createTopicForAdmin('{}') failed: {}", req.name, ex.what());
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }
    if (reply.topicId >= 0) {
        if (const TopicSpec* auth = reg.topic(reply.topicId))
            reply.partitionCount = auth->partitionCount;
    }
    return reply;
}

} // namespace rune::node
