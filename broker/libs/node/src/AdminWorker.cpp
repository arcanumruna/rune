#include "rune/node/AdminWorker.h"
#include <rune/codec/Serialize.h>  // rune::codec::deserialize (throws on bad input)
#include <rune/codec/Fingerprint.h>

namespace rune::node {

// Error code constants (protocol-agnostic integers fixed by Task 1 ErrorCode enum):
static constexpr uint16_t kOK             =  0;
static constexpr uint16_t kInternal       =  6;
static constexpr uint16_t kUnknownTopic   =  7;
static constexpr uint16_t kInvalidSchema  = 23;
// kSchemaValidationFailed = 24 (not used in this task)
static constexpr uint16_t kTopicExists    = 25;

AdminWorker::AdminWorker(SchemaRegistry& reg, AdminDeps deps)
    : reg_(reg), deps_(std::move(deps)) {}

// ─── DefineTopic ─────────────────────────────────────────────────────────────
//
// State machine (rule #3: each state handles its own transition):
//
//  PARSING         — deserialize blob; malformed → respond InvalidSchema=23; return
//  name-lookup     — byName hit → respond TopicExists=25 with existing id/version; return
//  CREATING        — allocate runtime id; deps.createTopic (blocking I/O in real impl)
//  AWAITING_COMMIT — build TopicSchema; deps.appendDurable; false → respond Internal=6; return
//  on-commit       — reg.applyRecord; if validate → pushValidators; respond OK=0 with id/ver=1
//
void AdminWorker::onDefineTopic(const DefineTopicArgs& args) {
    // ── PARSING ──────────────────────────────────────────────────────────────
    // Deserialize to validate the blob; the result is intentionally discarded
    // (we only want to know if it's well-formed before proceeding).
    try {
        [[maybe_unused]] auto _ =
            rune::codec::deserialize(std::span<const uint8_t>(args.blob.data(), args.blob.size()));
    } catch (...) {
        deps_.respondDefine(args.session, args.corr, -1, 0, kInvalidSchema);
        return;
    }

    // ── name-lookup ──────────────────────────────────────────────────────────
    if (const auto* ex = reg_.byName(args.name)) {
        deps_.respondDefine(args.session, args.corr,
                            ex->topicId, ex->version, kTopicExists);
        return;
    }

    // ── CREATING ─────────────────────────────────────────────────────────────
    // RULING-1: createTopic returns the authoritative topicId from the registry.
    // We do NOT pre-allocate via SchemaRegistry::allocateRuntimeId().
    const int32_t id = deps_.createTopic(args.partitionCount, args.name);

    // ── AWAITING_COMMIT ──────────────────────────────────────────────────────
    TopicSchema ts;
    ts.topicId        = id;
    ts.version        = 1;
    ts.partitionCount = args.partitionCount;
    ts.keyField       = args.keyField;
    ts.validate       = args.validate;
    ts.descriptorBlob = args.blob;

    if (!deps_.appendDurable(ts)) {
        deps_.respondDefine(args.session, args.corr, -1, 0, kInternal);
        return;
    }

    // ── on-commit ────────────────────────────────────────────────────────────
    reg_.applyRecord(ts);
    if (args.validate) {
        const RegistryEntry* entry = reg_.byId(id);
        deps_.pushValidators(*entry, id);
    }
    deps_.respondDefine(args.session, args.corr, id, 1, kOK);
}

// ─── GetTopicSchema ───────────────────────────────────────────────────────────
void AdminWorker::onGetTopicSchema(const GetSchemaArgs& args) {
    const auto* e = reg_.byName(args.name);
    if (!e) {
        deps_.respondGet(args.session, args.corr, -1, 0, {}, kUnknownTopic);
        return;
    }
    deps_.respondGet(args.session, args.corr,
                     e->topicId, e->version, e->descriptorBlob, kOK);
}

} // namespace rune::node
