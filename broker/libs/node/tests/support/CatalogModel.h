#pragma once
// CatalogModel.h — catalog history Task 21: the INDEPENDENT catalog model of the seeded stress.
//
// It is written from the spec's prose (docs/superpowers/specs/2026-10-13-rune-catalog-history-design.md)
// and the documented client error codes (Protocol.h ErrorCode) — it NEVER calls rune::catalog::apply, the
// AdminWorker, a CatalogState or any rune/catalog / rune/node header: a bug shared by the production rules and
// a copy of them could not be seen by a test that reuses them (C4).  Its only includes are the standard
// library, the 128-bit RequestId and the client ErrorCode enum.
//
// The model is SEQUENTIAL: accept(request) says what a DDL sent now must answer (OK, or the refusal code);
// apply(answer) moves the model on when it was accepted.  The stress feeds it the DDLs in the order their
// answers prove (a concurrent pair is tried in both orders, see the test).  Every rule cites its spec text.
//
//   Spec §2/§4  Objects are topics (CreateTopic/DropTopic/AlterTopicConfig), schemas (CreateSchema/
//               AlterSchema/SetCompatibility — an aspect of the topic) and views (CreateView/AlterView/
//               DropView).  Each event on an object bumps its version by 1; a create is 0 -> 1.
//   Spec §4     "A dropped name may be reused, but the new object gets a new objectId."  The model numbers
//               objects 1, 2, 3 ... in creation order; a drop frees the name, never the id.
//   Spec §5.1   The leader validates "name collisions, schema compatibility mode, view validity,
//               replicationFactor bounds"; the answer codes are §9's: TopicExists (25) for a name collision,
//               "the existing specific codes, otherwise InvalidRequest (39) with detail".
//   Spec §5.1   One leader, one AdminWorker: DDLs are serialized; "concurrent DDL and name collisions are
//               impossible across brokers".
#include <rune/protocol/CatalogTypes.h>
#include <rune/protocol/client/Protocol.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace rune::node::test::cmodel {

using Code = rune::protocol::client::ErrorCode;
[[nodiscard]] constexpr uint16_t num(Code c) noexcept { return static_cast<uint16_t>(c); }

/// The DDLs the stress issues (one statement each).
enum class Op : uint8_t { DefineTopic, DropTopic, AlterConfig, AlterSchema, SetCompat, CreateView, AlterView, DropView };
enum class SchemaOp : uint8_t { AddField, DropField };
enum class Compat : uint8_t { Backward, Forward };
enum class ConfigKey : uint8_t { RetentionMs, DeleteRetentionMs };

/// One DDL as the client states it.  `requestId` is filled by the harness (stable across retries).
struct Request {
    Op          op = Op::DefineTopic;
    std::string name;
    // DefineTopic
    bool typed = false;                 // a non-empty descriptor {id (key), hp}; false = an untyped topic
    int16_t replicationFactor = 0;      // 0 = every broker (the wire default)
    // DefineTopic / CreateView / AlterView: persistence
    bool transient = false;
    // AlterConfig / AlterView: the OBJECT version the client expects (0 = no check);
    // AlterSchema / SetCompat: the SCHEMA version the client expects (0 = no check)
    uint32_t expectedVersion = 0;
    // AlterConfig
    ConfigKey configKey = ConfigKey::RetentionMs;
    int64_t   configValue = 0;
    // AlterSchema
    SchemaOp    schemaOp = SchemaOp::AddField;
    std::string field;                  // the field added / dropped
    // SetCompat
    Compat      mode = Compat::Backward;
    // CreateView / AlterView
    std::string source;                 // the one source name the view reads
    bool        extraColumn = false;    // a different plan over the same source (AlterView)
    rune::protocol::RequestId requestId{};
};

/// The model's reading of one live object.
struct ModelObject {
    bool        isView = false;
    uint64_t    objectId = 0;
    uint32_t    version = 0;
    bool        transient = false;
    // topics
    bool                  typed = false;
    uint32_t              schemaVersion = 0;      // 0 = untyped
    Compat                compat = Compat::Backward;
    std::set<std::string> fields;                 // live field names of a typed topic
    // views
    std::string source;
    // Fix round 1 (I1): every field the stress varies, so "the catalog equals the model" is not vacuous.
    //  - config: the protocol's ONE definition of the TopicConfig defaults (rune/protocol/CatalogTypes.h, a protocol type);
    //  - topicId: a topic's id or a view's sink id.  The model counts ORDINALS (0, 1, 2 ...) in take order: every
    //    CreateTopic, CreateView and AlterView takes a fresh id from one dense counter (catalog ruling 4 / ApplyV1
    //    "fresh sink"; not spec prose).  snapshot() shifts them by the calibrated base (see calibrateTopicIds);
    //  - viewColumns: a view's output column names, in order.
    rune::protocol::TopicConfig config{};
    int32_t                     topicId = -1;
    int16_t                     replicationFactor = 0;
    int32_t                     partitions = 1;
    std::string                 keyField;
    std::vector<std::string>    viewColumns;
    bool operator==(const ModelObject&) const = default;
};
using Snapshot = std::map<std::string, ModelObject>;

[[nodiscard]] inline std::string describe(const Snapshot& s) {
    std::ostringstream o;
    for (const auto& [name, x] : s) {
        o << "\n  " << name << ": " << (x.isView ? "view" : "topic") << " obj=" << x.objectId << " v=" << x.version
          << (x.transient ? " transient" : " disk") << " topicId=" << x.topicId << " key='" << x.keyField << "'";
        if (!x.isView)
            o << " rf=" << x.replicationFactor << " parts=" << x.partitions << " retention.ms=" << x.config.retentionMs
              << " delete.retention.ms=" << x.config.deleteRetentionMs;
        if (x.isView) {
            o << " source=" << x.source << " columns={";
            for (const auto& c : x.viewColumns) o << c << ",";
            o << "}";
        } else if (x.typed) {
            o << " schema=" << x.schemaVersion << (x.compat == Compat::Forward ? " forward" : " backward") << " fields={";
            for (const auto& f : x.fields) o << f << ",";
            o << "}";
        } else {
            o << " untyped";
        }
    }
    return o.str();
}

[[nodiscard]] inline const char* opName(Op o) noexcept {
    constexpr const char* n[] = {"DefineTopic", "DropTopic", "AlterConfig", "AlterSchema", "SetCompat", "CreateView", "AlterView", "DropView"};
    return n[static_cast<int>(o)];
}
[[nodiscard]] inline std::string toString(const Request& r) {
    std::ostringstream o;
    o << opName(r.op) << " '" << r.name << "'";
    switch (r.op) {
    case Op::DefineTopic:  o << (r.typed ? " typed" : " untyped") << (r.transient ? " transient" : " disk"); break;
    case Op::AlterConfig:  o << " key=" << static_cast<int>(r.configKey) << " value=" << r.configValue << " expect=" << r.expectedVersion; break;
    case Op::AlterSchema:  o << (r.schemaOp == SchemaOp::AddField ? " add " : " drop ") << r.field << " expect=" << r.expectedVersion; break;
    case Op::SetCompat:    o << (r.mode == Compat::Forward ? " forward" : " backward") << " expect=" << r.expectedVersion; break;
    case Op::CreateView:   o << " source='" << r.source << "'" << (r.transient ? " transient" : " disk"); break;
    case Op::AlterView:    o << " source='" << r.source << "'" << (r.transient ? " transient" : " disk") << " expect=" << r.expectedVersion; break;
    default: break;
    }
    return o.str();
}

/// What the model says a DDL sent now must answer.
struct ModelAnswer {
    Request  request;
    bool     accepted = false;
    uint16_t refusalCode = 0;     // the ErrorCode when !accepted
};

class CatalogModel {
public:
    /// The name pool: 12 names; "B0"/"b0" and "D1"/"d1" fold onto each other (Spec: a create whose name
    /// differs only in ASCII case from a live name is refused TopicExists), "__rsv" is reserved (40).
    static inline const std::vector<std::string> kPool = {"a0", "a1", "a2", "a3", "a4", "b0", "B0", "d1", "D1", "c.x-y_z", "e-9", "__rsv"};
    static inline const std::vector<std::string> kFields = {"f0", "f1", "f2"};

    // ── the rules ────────────────────────────────────────────────────────────
    [[nodiscard]] ModelAnswer accept(const Request& r) const {
        ModelAnswer a;
        a.request = r;
        if (const auto code = refusal_(r)) {
            a.accepted = false;
            a.refusalCode = num(*code);
        } else {
            a.accepted = true;
        }
        return a;
    }

    void apply(const ModelAnswer& a) {
        if (!a.accepted) return;
        const Request& r = a.request;
        switch (r.op) {
        case Op::DefineTopic: {
            auto it = live_.find(r.name);
            if (it != live_.end()) {   // schema-less adoption: the untyped topic gains its schema
                ModelObject& t = it->second;
                t.typed = true; t.schemaVersion = 1; t.compat = Compat::Backward; t.fields = {"id", "hp"};
                t.keyField = "id";   // the adopting DefineTopic carries key "id"; the topic id is unchanged
                ++t.version;
                return;
            }
            ModelObject t;
            t.objectId = nextObjectId_++;
            t.version = 1;
            t.transient = r.transient;
            t.topicId = take_();
            t.replicationFactor = r.replicationFactor;
            t.partitions = 1;
            if (r.typed) { t.typed = true; t.schemaVersion = 1; t.compat = Compat::Backward; t.fields = {"id", "hp"}; t.keyField = "id"; }
            live_.emplace(r.name, std::move(t));
            return;
        }
        case Op::DropTopic: case Op::DropView: live_.erase(r.name); return;
        case Op::AlterConfig: {
            ModelObject& t = live_.at(r.name);
            if (r.configKey == ConfigKey::RetentionMs) t.config.retentionMs = r.configValue;
            else t.config.deleteRetentionMs = r.configValue;
            ++t.version;
            return;
        }
        case Op::AlterSchema: {
            ModelObject& t = live_.at(r.name);
            if (r.schemaOp == SchemaOp::AddField) t.fields.insert(r.field); else t.fields.erase(r.field);
            ++t.schemaVersion; ++t.version;
            return;
        }
        case Op::SetCompat: {
            ModelObject& t = live_.at(r.name);
            t.compat = r.mode;
            ++t.schemaVersion; ++t.version;
            return;
        }
        case Op::CreateView: {
            ModelObject v;
            v.isView = true; v.objectId = nextObjectId_++; v.version = 1; v.transient = r.transient; v.source = r.source;
            v.topicId = take_(); v.keyField = "id"; v.viewColumns = columnsOf_(r);
            live_.emplace(r.name, std::move(v));
            return;
        }
        case Op::AlterView: {
            ModelObject& v = live_.at(r.name);
            v.source = r.source; v.transient = r.transient; ++v.version;
            v.topicId = take_();   // a fresh sink (catalog ruling 4); the old one is retired
            v.viewColumns = columnsOf_(r);
            return;
        }
        }
    }

    /// The live objects, with topic ids shifted to the broker's numbering once calibrated (else -1: unknown).
    [[nodiscard]] Snapshot snapshot() const {
        Snapshot out = live_;
        for (auto& [name, o] : out) {
            (void)name;
            o.topicId = idBase_ ? o.topicId + *idBase_ : -1;
        }
        return out;
    }
    [[nodiscard]] const Snapshot& live() const noexcept { return live_; }
    /// The spec says nothing about the first topic id (the internal topics come first), so the model learns the
    /// base ONCE from the first object both the broker and the model hold: base = broker id - ordinal.  From then on
    /// every create and every AlterView must land on base + ordinal (dense, never reused).
    void calibrateTopicIds(const Snapshot& observed) {
        if (idBase_) return;
        for (const auto& [name, o] : observed) {
            const auto it = live_.find(name);
            if (it == live_.end() || it->second.isView != o.isView) continue;
            idBase_ = o.topicId - it->second.topicId;
            return;
        }
    }

    // ── the generator (model-driven: it reads the model's versions and live names) ────────────────────
    /// A random DDL.  `forceName` pins the contended name of a concurrent pair.
    [[nodiscard]] Request generate(std::mt19937& rng, const std::string* forceName = nullptr) const {
        Request r;
        const std::size_t liveCount = live_.size();
        // Op weights; a nearly empty catalog favours creates.
        const uint32_t createBoost = liveCount < 3 ? 3 : 1;
        const uint32_t w[] = {16 * createBoost,   // DefineTopic
                              9,                  // DropTopic
                              9,                  // AlterConfig
                              9,                  // AlterSchema
                              5,                  // SetCompat
                              11 * createBoost,   // CreateView
                              7,                  // AlterView
                              7};                 // DropView
        uint32_t total = 0;
        for (uint32_t x : w) total += x;
        uint32_t roll = rng() % total;
        std::size_t k = 0;
        while (roll >= w[k]) { roll -= w[k]; ++k; }
        r.op = static_cast<Op>(k);
        r.name = forceName != nullptr ? *forceName : chooseName_(rng, r.op);
        const auto it = live_.find(r.name);
        const ModelObject* obj = it == live_.end() ? nullptr : &it->second;
        switch (r.op) {
        case Op::DefineTopic:
            r.typed = rng() % 100 < 55;
            r.transient = rng() % 100 < 25;
            break;
        case Op::AlterConfig: {
            r.expectedVersion = versionExpectation_(rng, obj != nullptr ? obj->version : 0);
            if (rng() % 2 == 0) {
                r.configKey = ConfigKey::RetentionMs;
                const int64_t vals[] = {60'000, 120'000, -1, 60'000, 120'000, -5};   // -5 < -1: out of range
                r.configValue = vals[rng() % 6];
            } else {
                r.configKey = ConfigKey::DeleteRetentionMs;
                const int64_t vals[] = {0, 1'000, 86'400'000, 0, 1'000, -1};         // -1 < 0: out of range
                r.configValue = vals[rng() % 6];
            }
            break;
        }
        case Op::AlterSchema: {
            r.expectedVersion = versionExpectation_(rng, obj != nullptr ? obj->schemaVersion : 0);
            std::vector<std::string> liveExtra;
            if (obj != nullptr) for (const auto& f : obj->fields) if (f[0] == 'f') liveExtra.push_back(f);
            if (!liveExtra.empty() && rng() % 100 < 40) {
                r.schemaOp = SchemaOp::DropField;
                r.field = liveExtra[rng() % liveExtra.size()];
            } else if (rng() % 100 < 6) {
                r.schemaOp = SchemaOp::AddField;
                r.field = "hp";                                   // collides with a live field
            } else {
                r.schemaOp = rng() % 100 < 8 ? SchemaOp::DropField : SchemaOp::AddField;
                r.field = kFields[rng() % kFields.size()];        // may or may not be live
            }
            break;
        }
        case Op::SetCompat:
            r.expectedVersion = versionExpectation_(rng, obj != nullptr ? obj->schemaVersion : 0);
            r.mode = rng() % 2 == 0 ? Compat::Forward : Compat::Backward;
            break;
        case Op::CreateView: case Op::AlterView: {
            r.transient = rng() % 100 < 30;
            if (r.op == Op::AlterView) {
                r.expectedVersion = versionExpectation_(rng, obj != nullptr ? obj->version : 0);
                if (obj != nullptr && obj->isView && rng() % 100 < 85) r.transient = obj->transient;   // rarely flips persistence
            }
            r.extraColumn = rng() % 2 == 0;
            std::vector<std::string> typedTopics;
            for (const auto& [n, o] : live_) if (!o.isView && o.typed) typedTopics.push_back(n);
            std::vector<std::string> views;
            for (const auto& [n, o] : live_) if (o.isView) views.push_back(n);
            if (!views.empty() && rng() % 100 < 8) r.source = views[rng() % views.size()];   // a view as a source (41)
            else if (!typedTopics.empty() && rng() % 100 < 75) r.source = typedTopics[rng() % typedTopics.size()];
            else r.source = kPool[rng() % kPool.size()];
            // A concurrent pair (forceName) builds both plans before either applies: a plan that reads the
            // contended name itself would carry an id the other DDL may retire (a client-side race, not a
            // catalog rule), so a pair never reads its own name.
            while (forceName != nullptr && r.source == r.name) r.source = kPool[rng() % kPool.size()];
            break;
        }
        case Op::DropTopic: case Op::DropView: break;
        }
        return r;
    }

    /// A DDL the model accepts (for the tamper action, which needs an event to apply); falls back to the
    /// last one generated when none of 64 tries is accepted.
    [[nodiscard]] Request generateAccepted(std::mt19937& rng) const {
        Request last;
        for (int i = 0; i < 64; ++i) {
            last = generate(rng);
            if (accept(last).accepted) return last;
        }
        return last;
    }

private:
    [[nodiscard]] static bool validName_(const std::string& n) {
        // Spec §2/§8 via Protocol.h InvalidTopicName: 1..249 bytes of [A-Za-z0-9._-], not "." / "..", and no
        // "__" prefix (internal topics).
        if (n.empty() || n.size() > 249 || n == "." || n == "..") return false;
        for (const char c : n) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            if (!ok) return false;
        }
        return n.rfind("__", 0) != 0;
    }
    [[nodiscard]] static std::string lower_(std::string s) {
        for (char& c : s) if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
        return s;
    }
    /// A DIFFERENT live name that differs from `n` only in ASCII case.
    [[nodiscard]] bool foldsOntoLive_(const std::string& n) const {
        for (const auto& [name, o] : live_) {
            (void)o;
            if (name != n && lower_(name) == lower_(n)) return true;
        }
        return false;
    }
    [[nodiscard]] bool readByAView_(const std::string& topic) const {
        for (const auto& [name, o] : live_) { (void)name; if (o.isView && o.source == topic) return true; }
        return false;
    }
    [[nodiscard]] std::string chooseName_(std::mt19937& rng, Op op) const {
        const bool needsView = op == Op::DropView || op == Op::AlterView;
        const bool needsTopic = op == Op::DropTopic || op == Op::AlterConfig || op == Op::AlterSchema || op == Op::SetCompat;
        if ((needsView || needsTopic) && rng() % 100 < 75) {
            std::vector<std::string> names;
            for (const auto& [n, o] : live_) if (o.isView == needsView) names.push_back(n);
            if (!names.empty()) return names[rng() % names.size()];
        }
        return kPool[rng() % kPool.size()];
    }
    [[nodiscard]] static uint32_t versionExpectation_(std::mt19937& rng, uint32_t current) {
        const uint32_t roll = rng() % 100;
        if (roll < 45) return 0;                 // no check
        if (roll < 85) return current;           // the version the client read
        return current + 1;                      // stale / from the future: a conflict
    }

    /// The source rules of CreateView / AlterView ("view validity", §5.1): a source is a live typed TOPIC; a
    /// view's sink carries no change events (41); a persisted view over a transient source is refused (37).
    [[nodiscard]] std::optional<Code> sourceRefusal_(const Request& r) const {
        const auto it = live_.find(r.source);
        if (it == live_.end()) return Code::ViewSourceNotFound;
        if (it->second.isView) return Code::ViewSourceIsView;
        if (!it->second.typed) return Code::ViewSourceNotFound;   // an untyped topic has no schema to read
        if (it->second.transient && !r.transient) return Code::ViewSourceTransient;
        return std::nullopt;
    }

    [[nodiscard]] std::optional<Code> refusal_(const Request& r) const {
        const auto it = live_.find(r.name);
        const ModelObject* obj = it == live_.end() ? nullptr : &it->second;
        switch (r.op) {
        case Op::DefineTopic:
            if (!validName_(r.name)) return Code::InvalidTopicName;
            if (obj != nullptr) {
                if (obj->isView) return Code::TopicExists;
                if (obj->typed) return Code::TopicExists;                       // a schema already
                if (!r.typed) return Code::TopicExists;                         // untyped over untyped: nothing to adopt
                if (r.transient && !obj->transient) return Code::TopicExists;   // cannot turn a disk topic transient
                return std::nullopt;                                            // adoption: the topic gains its schema
            }
            if (foldsOntoLive_(r.name)) return Code::TopicExists;
            return std::nullopt;
        case Op::DropTopic:
            if (obj != nullptr && obj->isView) return Code::InvalidRequest;     // "is a view; use DROP VIEW"
            if (obj == nullptr) return Code::UnknownTopic;
            if (readByAView_(r.name)) return Code::InvalidRequest;              // the source of a view
            return std::nullopt;
        case Op::AlterConfig:
            if (obj != nullptr && obj->isView) return Code::ViewNotAlterable;
            if (obj == nullptr) return Code::UnknownTopic;
            if (r.expectedVersion != 0 && r.expectedVersion != obj->version) return Code::SchemaConflict;
            if (r.configKey == ConfigKey::RetentionMs && r.configValue < -1) return Code::InvalidRequest;
            if (r.configKey == ConfigKey::DeleteRetentionMs && r.configValue < 0) return Code::InvalidRequest;
            return std::nullopt;
        case Op::AlterSchema: case Op::SetCompat:
            if (obj != nullptr && obj->isView) return Code::ViewNotAlterable;
            if (obj == nullptr || !obj->typed) return Code::UnknownTopic;       // an untyped topic has no schema to alter
            if (r.expectedVersion != 0 && r.expectedVersion != obj->schemaVersion) return Code::SchemaConflict;
            if (r.op == Op::SetCompat) return std::nullopt;
            // Forward compatibility forbids dropping a field; the gate runs before the field itself is looked at.
            if (r.schemaOp == SchemaOp::DropField && obj->compat == Compat::Forward) return Code::SchemaIncompatible;
            if (r.schemaOp == SchemaOp::AddField && obj->fields.contains(r.field)) return Code::InvalidSchema;
            if (r.schemaOp == SchemaOp::DropField && !obj->fields.contains(r.field)) return Code::InvalidSchema;
            return std::nullopt;
        case Op::CreateView:
            if (!validName_(r.name)) return Code::InvalidTopicName;
            if (obj != nullptr) return obj->isView ? Code::ViewExists : Code::TopicExists;
            if (foldsOntoLive_(r.name)) return Code::TopicExists;
            return sourceRefusal_(r);
        case Op::AlterView:
            if (obj == nullptr || !obj->isView) return Code::UnknownTopic;      // there is no UnknownView code
            if (r.expectedVersion != 0 && r.expectedVersion != obj->version) return Code::SchemaConflict;
            return sourceRefusal_(r);
        case Op::DropView:
            if (obj == nullptr || !obj->isView) return Code::UnknownTopic;
            return std::nullopt;
        }
        return Code::Internal;
    }

    [[nodiscard]] int32_t take_() noexcept { return nextTopicOrdinal_++; }
    [[nodiscard]] static std::vector<std::string> columnsOf_(const Request& r) {
        return r.extraColumn ? std::vector<std::string>{"id", "hp", "hp2"} : std::vector<std::string>{"id", "hp"};
    }

    Snapshot live_;
    uint64_t nextObjectId_ = 1;
    int32_t  nextTopicOrdinal_ = 0;
    std::optional<int32_t> idBase_;
};

} // namespace rune::node::test::cmodel
