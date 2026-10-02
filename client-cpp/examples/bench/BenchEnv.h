// examples/bench/BenchEnv.h — env helpers shared by rune_produce_throughput's
// legacy env-driven path and its invoker-shard bench (cold path: startup only).
#pragma once

#include <rune/client/TopicHandle.h>

#include <cstdint>
#include <cstdlib>
#include <string>
#include <utility>
#include <vector>

namespace rune::bench {

inline std::string envStr(const char* n, const std::string& d = {}) {
    const char* v = std::getenv(n);
    return (v && *v) ? std::string(v) : d;
}
inline int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v && *v) ? std::atoll(v) : d;
}

/// "name:id:partitions[,name:id:partitions...]"; malformed tokens are skipped.
inline std::vector<rune::client::TopicSpec> parseTopics(const std::string& s) {
    std::vector<rune::client::TopicSpec> out;
    std::size_t pos = 0;
    while (pos <= s.size()) {
        const std::size_t comma = s.find(',', pos);
        const std::string tok = s.substr(pos, (comma == std::string::npos ? s.size() : comma) - pos);
        pos = (comma == std::string::npos ? s.size() + 1 : comma + 1);
        if (tok.empty()) continue;
        const auto c1 = tok.find(':'); if (c1 == std::string::npos) continue;
        const auto c2 = tok.find(':', c1 + 1); if (c2 == std::string::npos) continue;
        rune::client::TopicSpec ts;
        ts.name = tok.substr(0, c1);
        ts.topicId = std::atoi(tok.c_str() + c1 + 1);
        ts.partitionCount = std::atoi(tok.c_str() + c2 + 1);
        out.push_back(std::move(ts));
    }
    return out;
}

} // namespace rune::bench
