// CatalogEffectFrames.cpp — catalog history Task 7 fix round 1 (C1).
#include <rune/node/CatalogEffectFrames.h>
#include <rune/node/WriteState.h>

#include <rune/node/AdminChannels.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Evolve.h>
#include <rune/codec/Serialize.h>

#include <algorithm>
#include <exception>
#include <span>

namespace rune::node {

namespace cat = rune::catalog;

namespace {

std::optional<std::string> tooLarge(std::string_view what, std::string_view name, std::size_t len, std::size_t max) {
    if (len != 0 && len <= max) return std::nullopt;
    if (len == 0)
        return std::string{what} + " for '" + std::string{name} + "' cannot be encoded (a name or key longer than " +
               std::to_string(kMaxStr16) + " bytes)";
    return std::string{what} + " for '" + std::string{name} + "' is " + std::to_string(len) +
           " bytes — larger than the " + std::to_string(max) + " B admin→control frame limit";
}

rune::protocol::TopicConfig sinkConfigOf(const cat::ViewObject& v) {
    rune::protocol::TopicConfig c;   // the production sink: a keyed, compacted table
    c.cleanupPolicy = 1;
    c.partitionKey  = v.def.keyField;
    return c;
}

} // namespace

std::size_t catalogEffectMaxFrame() noexcept { return static_cast<std::size_t>(kAdminToControlRingBytes) / 8; }

std::vector<uint8_t> materializedDescriptorOf(const cat::TopicObject& t) {
    return materializedDescriptorOf(t, t.schema->schemaVersion);
}

std::vector<uint8_t> materializedDescriptorOf(const cat::TopicObject& t, uint16_t version) {
    const cat::SchemaState& s = *t.schema;
    const rune::codec::Descriptor evolving =
        rune::codec::deserialize(std::span<const uint8_t>(s.evolvingBlob.data(), s.evolvingBlob.size()));
    return rune::codec::serialize(rune::codec::materializeAsOf(evolving, version));
}

std::optional<std::string> topicEffectFramesError(const cat::TopicObject& t, std::size_t maxFrame,
                                                  uint16_t fromVersion) {
    if (auto e = tooLarge("the TopicApplied effect", t.name, catalogTopicAppliedWireLen(t.name, t.config), maxFrame))
        return e;
    if (t.schema) {
        try {
            for (uint16_t v = fromVersion; v <= t.schema->schemaVersion; ++v) {
                const std::size_t n = schemaUpdateWireLen(materializedDescriptorOf(t, v).size());
                if (auto e = tooLarge("the validator (SchemaUpdate) effect", t.name, n, maxFrame)) return e;
            }
        } catch (const std::exception& ex) {
            return "the schema of '" + t.name + "' does not materialize: " + ex.what();
        }
        if (auto e = tooLarge("the WriteSpec effect", t.name, kTopicWriteSpecWireLen, maxFrame)) return e;
    }
    // Phase 2c: every typed topic also posts its FilterSchema (the EVOLVING descriptor: retired fields stay, so it can
    // outgrow the materialized one) - validate or not.  Unsized, a frame the ring cannot take is refused on every broker
    // AFTER commit: filtered subscribers silently lose the new version's records.
    if (t.schema) {
        if (auto e = tooLarge("the FilterSchema effect", t.name, filterSchemaWireLen(t.schema->evolvingBlob.size()), maxFrame))
            return e;
    }
    return std::nullopt;
}

std::optional<std::string> viewEffectFramesError(const cat::ViewObject& v, const cat::CatalogState& state,
                                                 std::size_t maxFrame) {
    if (auto e = tooLarge("the view sink's TopicApplied effect", v.name,
                          catalogTopicAppliedWireLen(v.name, sinkConfigOf(v)), maxFrame))
        return e;
    std::size_t src = 0;
    if (!v.def.sourceNames.empty())
        if (const cat::TopicObject* s = state.topicByName(v.nsId, v.def.sourceNames.front()); s && s->schema) {
            try {
                src = materializedDescriptorOf(*s).size();
            } catch (const std::exception& ex) {
                return "the source '" + s->name + "' of view '" + v.name + "' does not materialize: " + ex.what();
            }
        }
    const std::size_t plan = 4 + 1 + 1 + 4 + v.def.compiledPlanBlob.size() + 4 + v.def.outputDescriptorBlob.size() + 4 + src;
    return tooLarge("the view plan (ViewPlanUpdate) effect", v.name, plan, maxFrame);
}

std::optional<std::string> eventEffectFramesError(const cat::CatalogState& after, const cat::CatalogEvent& e,
                                                  const cat::Applied& a, std::size_t maxFrame) {
    switch (e.kind) {
    case cat::EventKind::CreateTopic:
    case cat::EventKind::CreateSchema:
    case cat::EventKind::AlterSchema:
    case cat::EventKind::SetCompatibility:
    case cat::EventKind::AlterTopicConfig:   // its frame is fixed-size (<= 6 keys); the topic's frames are checked
        if (const cat::TopicObject* t = after.topicByObjectId(e.objectId)) return topicEffectFramesError(*t, maxFrame, t->schema ? t->schema->schemaVersion : uint16_t{1});
        return std::nullopt;
    case cat::EventKind::DropTopic:
    case cat::EventKind::DropView:
        return tooLarge("the TopicDropped effect", e.objectName, catalogTopicDroppedWireLen(e.objectName), maxFrame);
    case cat::EventKind::CreateView:
    case cat::EventKind::AlterView: {
        if (e.kind == cat::EventKind::AlterView)
            if (auto err = tooLarge("the TopicDropped effect", e.objectName,
                                    catalogTopicDroppedWireLen(e.objectName), maxFrame))
                return err;
        if (const cat::ViewObject* v = after.viewByObjectId(e.objectId)) return viewEffectFramesError(*v, after, maxFrame);
        return std::nullopt;
    }
    case cat::EventKind::NamespaceCreated:      // namespace kinds post no effect frame (Checkpoint A F2)
    case cat::EventKind::NamespaceLimitsSet:
    case cat::EventKind::NamespaceStateChanged:
    case cat::EventKind::NamespaceDeleted:
    case cat::EventKind::NamespaceDeleteStep:
        return std::nullopt;
    }
    (void)a;
    return std::nullopt;
}

std::optional<std::string> catalogEventEffectsRefusal(const cat::CatalogState& before, const cat::CatalogEvent& e,
                                                      std::size_t maxFrame) {
    cat::CatalogState dry = before;   // cold: one copy per DDL on the leader
    cat::CatalogEvent ev  = e;
    auto fp = cat::previewFingerprint(dry, ev);
    if (!fp) return std::nullopt;
    ev.fingerprint = *fp;
    auto a = cat::apply(dry, ev, dry.appliedOffset() + 1);
    if (!a) return std::nullopt;
    if (auto err = eventEffectFramesError(dry, ev, *a, maxFrame))
        return std::string{cat::kindName(e.kind)} + " '" + e.objectName + "' refused: " + *err;
    // Task 7 re-review (binding for Task 9): a source's schema change re-sizes the
    // plan of every view that reads it — a dependent view's ViewPlanUpdate carries
    // the source's materialized descriptor, and a boot (or a later install) posts
    // it.  An event that would push one past the frame is refused here, never
    // written (else every broker would halt at the next boot).
    switch (e.kind) {
    case cat::EventKind::CreateSchema:
    case cat::EventKind::AlterSchema:
    case cat::EventKind::SetCompatibility:
        for (const cat::ViewObject* v : dry.views()) {
            const auto& srcs = v->def.sourceNames;
            if (std::find(srcs.begin(), srcs.end(), e.objectName) == srcs.end()) continue;
            if (auto err = viewEffectFramesError(*v, dry, maxFrame))
                return std::string{cat::kindName(e.kind)} + " '" + e.objectName + "' refused: dependent view '" +
                       v->name + "': " + *err;
        }
        break;
    default:
        break;
    }
    return std::nullopt;
}

} // namespace rune::node
