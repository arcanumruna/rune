// #147: the auth topology refusal happens at parse time, not only at start.
#include <rune/node/NodeConfig.h>

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <utility>

namespace rune::node {

namespace {
std::function<const char*(const char*)> envOf(std::map<std::string, std::string> m) {
    return [m = std::move(m)](const char* key) -> const char* {
        auto it = m.find(key);
        return it != m.end() ? it->second.c_str() : nullptr;
    };
}

std::map<std::string, std::string> oneNode() {
    return {
        {"RUNE_NODE_ID", "0"},
        {"RUNE_PEERS", "0@rune-1:40111"},
        {"RUNE_CLIENT_ENDPOINT", "rune-1:50001"},
        {"RUNE_DATA_DIR", "/data/rune"},
        {"RUNE_CLUSTER_KEY_FILE", "/etc/rune/cluster.key"},
    };
}
}  // namespace

// RED: parseNodeConfig does not call validateAuthTopology (the config parses and only start refuses it).
TEST(NodeConfigParseRegression, ParseRefusesAuthTopologyRegression) {
    auto env = oneNode();
    env["RUNE_WORKER_MODE"] = "legacy";
    env["RUNE_RUNTIME_PLACEMENT"] = "thread";
    env["RUNE_PER_WORKER_INGRESS"] = "true";
    auto r = parseNodeConfig(envOf(env));
    EXPECT_FALSE(r.config.has_value()) << "an unsupported auth topology must not parse";
    EXPECT_NE(r.error.find("RUNE_PER_WORKER_INGRESS"), std::string::npos) << r.error;

    auto shard = oneNode();
    shard["RUNE_WORKER_MODE"] = "shard";
    r = parseNodeConfig(envOf(shard));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_FALSE(r.config->validateAuthTopology().has_value());
}

}  // namespace rune::node
