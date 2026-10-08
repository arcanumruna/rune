// src/Evolve.cpp
#include <rune/codec/Evolve.h>
#include <algorithm>
#include <cstddef>

namespace rune::codec {

Descriptor materializeAsOf(const Descriptor& evolving, uint16_t version) {
    Descriptor d;
    d.name    = evolving.name;
    d.version = version;

    d.fields.reserve(evolving.fields.size());
    for (const auto& f : evolving.fields) {
        if (!isLiveAt(f, version)) continue;
        FieldDescriptor fv = f;
        fv.untilVersion = 0xFFFF;
        d.fields.push_back(std::move(fv));
    }

    d.blockLength = computeOffsets(d.fields);
    return d;
}

namespace {

// Within the cumulative "evolving" Descriptor that applyOps builds up, a
// field is "live" (as opposed to retired) iff it has never been DROPped --
// i.e. its untilVersion is still the open-ended sentinel. This is distinct
// from isLiveAt(f, v), which asks whether a field is live at one concrete
// schema version; here there is no single version yet -- newVersion is only
// being assembled by this very call.
bool isLive(const FieldDescriptor& f) noexcept {
    return f.untilVersion == 0xFFFF;
}

FieldDescriptor* findLiveByName(std::vector<FieldDescriptor>& fields, std::string_view name) {
    for (auto& f : fields) {
        if (isLive(f) && f.name == name) return &f;
    }
    return nullptr;
}

std::size_t countLive(const std::vector<FieldDescriptor>& fields) noexcept {
    return static_cast<std::size_t>(std::count_if(
        fields.begin(), fields.end(), [](const FieldDescriptor& f) { return isLive(f); }));
}

} // namespace

std::expected<void, EvolveError> checkCompatibility(const std::vector<AlterOp>& ops,
                                                      CompatMode mode) {
    if (mode == CompatMode::Forward) {
        for (const auto& op : ops) {
            if (op.type == AlterOpType::Drop) {
                return std::unexpected(EvolveError{
                    "DROP '" + op.name + "' is not allowed under Forward compatibility"});
            }
        }
    }
    return {};
}

std::expected<Descriptor, EvolveError> applyOps(const Descriptor& currentEvolving,
                                                 const std::vector<AlterOp>& ops,
                                                 uint16_t& nextTag, uint16_t newVersion) {
    // Work on a private copy of the field list and a private copy of the
    // tag cursor; only on full success do we publish either back to the
    // caller, so a rejected call never burns a tag or leaks a partially
    // evolved Descriptor.
    std::vector<FieldDescriptor> fields = currentEvolving.fields;
    uint16_t tagCursor = nextTag;

    for (const auto& op : ops) {
        switch (op.type) {
            case AlterOpType::Add: {
                if (findLiveByName(fields, op.name) != nullptr) {
                    return std::unexpected(EvolveError{
                        "ADD: field '" + op.name + "' collides with a live field"});
                }
                FieldDescriptor f;
                f.name         = op.name;
                f.id           = tagCursor++;
                f.type         = op.primType;
                f.fixedLen     = op.fixedLen;
                f.sinceVersion = newVersion;
                f.untilVersion = 0xFFFF;
                f.isKey        = false;
                fields.push_back(std::move(f));
                break;
            }
            case AlterOpType::Drop: {
                FieldDescriptor* f = findLiveByName(fields, op.name);
                if (f == nullptr) {
                    return std::unexpected(EvolveError{
                        "DROP: field '" + op.name + "' is not live (absent or already dropped)"});
                }
                if (f->isKey) {
                    return std::unexpected(EvolveError{
                        "DROP: cannot drop the key field '" + op.name + "'"});
                }
                if (countLive(fields) <= 1) {
                    return std::unexpected(EvolveError{
                        "DROP: cannot drop the last remaining live field '" + op.name + "'"});
                }
                f->untilVersion = newVersion;
                break;
            }
            case AlterOpType::Rename: {
                FieldDescriptor* f = findLiveByName(fields, op.name);
                if (f == nullptr) {
                    return std::unexpected(EvolveError{
                        "RENAME: field '" + op.name + "' is not live (absent or already dropped)"});
                }
                FieldDescriptor* collision = findLiveByName(fields, op.newName);
                if (collision != nullptr && collision != f) {
                    return std::unexpected(EvolveError{
                        "RENAME: new name '" + op.newName + "' collides with a live field"});
                }
                f->name = op.newName;
                break;
            }
            case AlterOpType::SetCompatibility:
                // No descriptor change: the compatibility mode is tracked
                // by the caller (broker), not stored on the Descriptor.
                break;
        }
    }

    Descriptor out;
    out.name        = currentEvolving.name;
    out.version     = newVersion;
    out.fields      = std::move(fields);
    out.blockLength = computeOffsets(out.fields);

    nextTag = tagCursor;
    return out;
}

} // namespace rune::codec
