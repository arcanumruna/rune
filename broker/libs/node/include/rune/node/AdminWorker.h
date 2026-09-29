#pragma once
#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include "rune/node/SchemaRegistry.h"

namespace rune::node {

// Error code integers (fixed by Task 1 ErrorCode enum; AdminWorker passes ints
// to stay protocol-agnostic):
//   OK                    = 0
//   Internal              = 6
//   UnknownTopic          = 7
//   InvalidSchema         = 23
//   SchemaValidationFailed= 24  (not used in this task)
//   TopicExists           = 25

struct AdminDeps {
    // CREATING: create the physical topic partitions (blocking I/O in real impl).
    std::function<int32_t(int32_t topicId, int32_t partitionCount,
                          const std::string& name)> createTopic;

    // AWAITING_COMMIT: append a durable TopicSchema record; returns true = committed.
    std::function<bool(const TopicSchema&)> appendDurable;

    // PUBLISHING: push validators to the validator registry after commit (only when
    // validate=true).
    std::function<void(const RegistryEntry&, int32_t topicId)> pushValidators;

    // Send the DefineTopic response back to the client.
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, uint16_t errorCode)> respondDefine;

    // Send the GetTopicSchema response back to the client.
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, const std::vector<uint8_t>& blob,
                       uint16_t errorCode)> respondGet;
};

struct DefineTopicArgs {
    int64_t              session;
    int64_t              corr;
    std::string          name;
    std::string          keyField;
    int32_t              partitionCount;
    bool                 validate;
    std::vector<uint8_t> blob;
};

struct GetSchemaArgs {
    int64_t     session;
    int64_t     corr;
    std::string name;
};

/// Single-threaded, AdminWorker-owned DDL state machine.
/// No locks, no atomics — caller owns the thread (shared-nothing, rule #1).
class AdminWorker {
public:
    AdminWorker(SchemaRegistry& reg, AdminDeps deps);

    /// Run the DefineTopic DDL state machine synchronously.
    /// States: PARSING → name-lookup → CREATING → AWAITING_COMMIT → on-commit.
    void onDefineTopic(const DefineTopicArgs& args);

    /// Run the GetTopicSchema lookup.
    void onGetTopicSchema(const GetSchemaArgs& args);

private:
    SchemaRegistry& reg_;
    AdminDeps       deps_;
};

} // namespace rune::node
