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
// really has a different one, and never adopts an existing topic as a view sink —
// except (Task 14) a RUNTIME topic whose spec is exactly the requested sink's
// (sinkSpecMatches), which the admin thread adopts only when no catalog record
// claims it.  `firstRuntimeId` is the static (RUNE_TOPICS) topic count; the default
// never matches.
// ---------------------------------------------------------------------------
#include <rune/gateway/TopicRegistry.h>
#include <rune/node/AdminChannels.h>
#include <rune/node/NodeConfig.h>
#include <rune/node/TopicName.h>

#include <spdlog/spdlog.h>

#include <exception>
#include <limits>
#include <optional>

namespace rune::node {

inline CreateTopicReplyMsg
createTopicForAdmin(rune::gateway::TopicRegistry& reg, const CreateTopicReqMsg& req,
                    int32_t firstRuntimeId = std::numeric_limits<int32_t>::max()) {
    CreateTopicReplyMsg reply;
    reply.ticket  = req.ticket;
    reply.existed = reg.topicIdForName(req.name).has_value();

    // DDL conformance B1/B2/B6: the admin thread already refuses these; the control
    // thread — the one that builds dataDir/topics/<name>/ — never creates them either.
    if (!isValidTopicName(req.name) || req.partitionCount < 1) {
        spdlog::error("createTopicForAdmin: refusing invalid request name='{}' partitions={}",
                      escapeNameForLog(req.name), req.partitionCount);
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }

    // DDL conformance B3: names are case-sensitive identifiers, but a name that
    // differs from an existing one only in ASCII case would share its directory on a
    // case-insensitive filesystem — refuse it (TopicExists); nothing is created.
    if (!reply.existed) {
        if (const std::optional<int32_t> other = reg.topicIdForFoldedName(req.name)) {
            reply.topicId      = *other;
            reply.existed      = true;
            reply.caseConflict = true;
            if (const TopicSpec* s = reg.topic(*other)) {
                reply.partitionCount = s->partitionCount;
                reply.persistence    = persistenceToWire(s->persistence);
            }
            return reply;
        }
    }

    TopicSpec spec;
    spec.name           = req.name;
    spec.partitionCount = req.partitionCount;
    spec.topicId        = -1;  // registry assigns
    // Task 19: a view SINK is a keyed + Compact "table" — carry the cleanup
    // policy + key field the admin thread resolved (DefineTopic leaves them
    // default: Delete, empty).
    // Fix round 2: an unknown cleanup-policy byte is refused, never cast into
    // the enum (Delete 0, Compact 1, CompactDelete 2).
    if (req.cleanupPolicy > static_cast<uint8_t>(TopicSpec::CleanupPolicy::CompactDelete)) {
        spdlog::error("createTopicForAdmin('{}'): unknown cleanup-policy byte {} — create refused",
                      req.name, static_cast<unsigned>(req.cleanupPolicy));
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }
    spec.cleanupPolicy  = static_cast<TopicSpec::CleanupPolicy>(req.cleanupPolicy);
    if (!req.keyField.empty()) spec.partitionKey = req.keyField;
    // Phase 2b E: a transient runtime topic is created exactly like a static
    // persistence=transient one — TopicRegistry skips its disk dirs and
    // createRuntimeUserReplicas builds it on an InMemoryLog.
    // No-hang wave (R1): an unknown mode byte is NEVER defaulted to Disk — the
    // create fails explicitly (topicId -1; the admin thread answers Internal).
    // The ring decode already rejects a byte > 1, so this guards a hand-built /
    // future request; nothing is created.
    const auto mode = persistenceFromWire(req.persistence);
    if (!mode) {
        spdlog::error("createTopicForAdmin('{}'): unknown persistence byte {} — create refused",
                      req.name, static_cast<unsigned>(req.persistence));
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }
    spec.persistence    = *mode;

    try {
        reply.topicId = reg.createTopic(spec);
    } catch (const std::exception& ex) {
        spdlog::error("createTopicForAdmin('{}') failed: {}", req.name, ex.what());
        reply.topicId = -1;
        reply.existed = false;
        return reply;
    }
    if (reply.topicId >= 0) {
        if (const TopicSpec* auth = reg.topic(reply.topicId)) {
            reply.partitionCount = auth->partitionCount;
            // Phase 2b E: an existing topic keeps ITS mode (createTopic ignores the
            // requested spec) — report the real one so the schema record never
            // contradicts the log backend the topic actually runs on.
            reply.persistence    = persistenceToWire(auth->persistence);
        }
        // Task 14 (KL1 follow-up): an EXISTING runtime topic whose spec is exactly the
        // requested view sink's — the admin thread may adopt it as the sink when no
        // catalog record claims it (a failed CREATE VIEW's own orphan sink).  A static
        // (RUNE_TOPICS) topic — id < firstRuntimeId — never matches.
        if (reply.existed && reply.topicId >= firstRuntimeId) {
            if (const TopicSpec* s = reg.topic(reply.topicId)) {
                reply.sinkSpecMatches =
                    req.cleanupPolicy == static_cast<uint8_t>(TopicSpec::CleanupPolicy::Compact) &&
                    static_cast<uint8_t>(s->cleanupPolicy) == req.cleanupPolicy &&
                    s->partitionKey == req.keyField &&
                    s->partitionCount == req.partitionCount &&
                    persistenceToWire(s->persistence) == req.persistence;
            }
        }
    }
    return reply;
}

} // namespace rune::node
