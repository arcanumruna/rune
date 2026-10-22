#include <rune/gateway/FilterAdmission.h>

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/filter/FilterGroup.h>

#include <spdlog/spdlog.h>

namespace rune::gateway {

void FilterSchemaRegistry::install(int32_t topicId, uint16_t version, std::span<const uint8_t> evolvingBlob)
{
    try {
        const rune::codec::Descriptor desc = rune::codec::deserialize(evolvingBlob);
        rune::codec::FilterSchema built    = rune::codec::FilterSchema::build(desc, version);
        const auto it = byTopic_.find(topicId);
        if (it == byTopic_.end()) {
            byTopic_.emplace(topicId, std::move(built));
        } else if (it->second.extendFrom(built) == rune::codec::FilterSchema::ExtendResult::Incompatible) {
            spdlog::warn("ClientGateway: FilterSchema for topicId={} v{} conflicts with the installed one - replacing it",
                         topicId, version);
            it->second = std::move(built);
        }
    } catch (const std::exception& ex) {
        spdlog::error("ClientGateway: FilterSchema for topicId={} version={} refused (the old schema is kept): {}",
                      topicId, version, ex.what());
    } catch (...) {
        spdlog::error("ClientGateway: FilterSchema for topicId={} version={} refused (unknown exception)", topicId, version);
    }
}

void FilterAdmission::Ticket::reset_() noexcept
{
    if (owner_ != nullptr) owner_->release_(*this);
}

std::expected<FilterAdmission::Ticket, std::string>
FilterAdmission::admit(int32_t topicId, int32_t partition, uint16_t indexTag, std::span<const rune::codec::FilterKey> keys)
{
    const uint64_t k = key_(topicId, partition);
    Counts*        c = nullptr;
    if (const auto it = byTp_.find(k); it != byTp_.end()) c = &it->second;
    const auto where = [&] { return "on topic " + std::to_string(topicId) + " partition " + std::to_string(partition); };

    const uint32_t subs = c ? c->subs : 0;
    if (subs >= limits_.maxSubs)
        return std::unexpected("filter capacity: " + std::to_string(limits_.maxSubs) + " filtered subscriptions " + where() +
                               " (RUNE_FILTER_MAX_SUBS_PER_PARTITION)");

    // P19: the worker's FilterGroup indexes at most kMaxIndexFields distinct fields; a ninth is refused there after
    // the SubscribeResponse was already OK - so refuse it here.
    if (c != nullptr && !c->tags.contains(indexTag) && c->tags.size() >= rune::node::filter::kMaxIndexFields)
        return std::unexpected("filter capacity: " + std::to_string(rune::node::filter::kMaxIndexFields) + " index fields " +
                               where() + " (a ninth distinct index field cannot be served)");

    uint32_t fresh = 0;
    for (const auto& v : keys)
        if (c == nullptr || !c->values.contains({indexTag, v})) ++fresh;
    const uint32_t have = c ? static_cast<uint32_t>(c->values.size()) : 0;
    if (static_cast<uint64_t>(have) + fresh > limits_.maxValues)
        return std::unexpected("filter capacity: " + std::to_string(limits_.maxValues) + " index values " + where() +
                               " (RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION)");

    Counts& cc = byTp_[k];
    ++cc.subs;
    ++cc.tags[indexTag];
    for (const auto& v : keys) ++cc.values[{indexTag, v}];
    Ticket t;
    t.owner_ = this;
    t.tp_    = k;
    t.tag_   = indexTag;
    t.keys_.assign(keys.begin(), keys.end());
    return t;
}

void FilterAdmission::release_(Ticket& t) noexcept
{
    const auto it = byTp_.find(t.tp_);
    if (it != byTp_.end()) {
        Counts& c = it->second;
        if (c.subs > 0) --c.subs;
        if (const auto g = c.tags.find(t.tag_); g != c.tags.end() && --g->second == 0) c.tags.erase(g);
        for (const auto& v : t.keys_) {
            const auto vi = c.values.find({t.tag_, v});
            if (vi != c.values.end() && --vi->second == 0) c.values.erase(vi);
        }
        if (c.subs == 0) byTp_.erase(it);
    }
    t.owner_ = nullptr;
    t.keys_.clear();
}

uint32_t FilterAdmission::subsFor(int32_t topicId, int32_t partition) const noexcept
{
    const auto it = byTp_.find(key_(topicId, partition));
    return it == byTp_.end() ? 0 : it->second.subs;
}

uint32_t FilterAdmission::valuesFor(int32_t topicId, int32_t partition) const noexcept
{
    const auto it = byTp_.find(key_(topicId, partition));
    return it == byTp_.end() ? 0 : static_cast<uint32_t>(it->second.values.size());
}

uint32_t FilterAdmission::indexFieldsFor(int32_t topicId, int32_t partition) const noexcept
{
    const auto it = byTp_.find(key_(topicId, partition));
    return it == byTp_.end() ? 0 : static_cast<uint32_t>(it->second.tags.size());
}

}  // namespace rune::gateway
