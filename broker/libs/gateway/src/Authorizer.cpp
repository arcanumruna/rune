// Authorizer.cpp - see rune/gateway/Authorizer.h.
#include <rune/gateway/Authorizer.h>

namespace rune::gateway {

namespace rsec = rune::security;

namespace {
constexpr std::size_t kMaxNameInDetail = 256;   // a request's name is cut identically for hidden and missing objects

const char* opName(rsec::Operation o) noexcept {
    switch (o) {
        case rsec::Operation::Read:     return "READ";
        case rsec::Operation::Write:    return "WRITE";
        case rsec::Operation::Create:   return "CREATE";
        case rsec::Operation::Alter:    return "ALTER";
        case rsec::Operation::Drop:     return "DROP";
        case rsec::Operation::Describe: return "DESCRIBE";
        case rsec::Operation::Admin:    return "ADMIN";
    }
    return "?";
}
const char* typeName(rsec::ResourceType t) noexcept {
    switch (t) {
        case rsec::ResourceType::Cluster:         return "CLUSTER";
        case rsec::ResourceType::Topic:           return "TOPIC";
        case rsec::ResourceType::View:            return "VIEW";
        case rsec::ResourceType::Group:           return "GROUP";
        case rsec::ResourceType::TransactionalId: return "TRANSACTIONAL_ID";
    }
    return "?";
}
Denial deny(rsec::Operation op, rsec::ResourceType type, std::string_view what) {
    Denial d;
    d.detail = std::string("not authorized: ") + opName(op) + " on " + typeName(type);
    if (type != rsec::ResourceType::Cluster) {
        d.detail += ' ';
        d.detail.append(what.substr(0, kMaxNameInDetail));
        if (what.size() > kMaxNameInDetail) d.detail += "...";
    }
    return d;
}
} // namespace

std::optional<Denial> authorizeTopicId(const PermissionTable& t, int32_t topicId, rsec::Operation op,
                                       const ObjectIndex& index, rsec::ResourceType shownType)
{
    if (t.allows(topicId, rsec::opBit(op))) return std::nullopt;
    if (index.at(topicId) == nullptr && t.allowsCluster(rsec::opBit(rsec::Operation::Describe))) return std::nullopt;   // D22
    return deny(op, shownType, std::to_string(topicId));
}

std::optional<Denial> authorizeViewSource(const PermissionTable& t, int32_t topicId)
{
    if (t.allows(topicId, rsec::opBit(rsec::Operation::Read))) return std::nullopt;
    return deny(rsec::Operation::Read, rsec::ResourceType::Topic, std::to_string(topicId));
}

std::optional<Denial> authorizeName(const PermissionTable& t, rsec::ResourceType type, std::string_view name, rsec::Operation op)
{
    if (t.allowsName(type, name, rsec::opBit(op))) return std::nullopt;
    return deny(op, type, name);
}

std::optional<Denial> authorizeTopicOrViewName(const PermissionTable& t, std::string_view name, rsec::Operation op)
{
    if (t.allowsName(rsec::ResourceType::Topic, name, rsec::opBit(op)) ||
        t.allowsName(rsec::ResourceType::View, name, rsec::opBit(op)))
        return std::nullopt;
    return deny(op, rsec::ResourceType::Topic, name);
}

std::optional<Denial> authorizeCluster(const PermissionTable& t, rsec::Operation op)
{
    if (t.allowsCluster(rsec::opBit(op))) return std::nullopt;
    return deny(op, rsec::ResourceType::Cluster, {});
}

} // namespace rune::gateway
