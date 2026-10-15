#pragma once
// CatalogBootLog.h — catalog history Task 23 (spec §10): a real dataDir/__catalog Log of 10*unit
// valid formatVer-1 events + its `applied` checkpoint, written through PRODUCTION code (a leader
// PartitionReplica's append, writeCatalogCheckpoint).  Shared by CatalogGatesRegressionTest.cpp and
// benchmark/CatalogBootReplayBench.cpp.  Events are built exactly as the leader builds them
// (rune::catalog::testing::golden_detail::ScriptBuilder: previewFingerprint, then apply).
//
// Script (unit u, even; u = 1000 -> exactly 10 000 events):
//   2u  CreateTopic t<i>  typed {id i64 key, hp i32}, 4 partitions, rf 1, disk     i in [0, 2u)
//   2u  AlterSchema t<i>  ADD mana i32                                            i in [0, 2u)
//   2u  AlterTopicConfig t<i> retention.ms = 60000 + i                            i in [0, 2u)
//   1u  SetCompatibility t<i> FORWARD                                             i in [0, u)
//   1u  CreateView v<i> over t<i> (1-partition sink)                              i in [0, u)
//   1u  AlterView v<i> (a fresh 2-partition sink)                                 i in [0, u)
//   u/2 DropView v<i>                                                             i in [u/2, u)
//   u/2 DropTopic t<i>   (no view reads them)                                     i in [3u/2, 2u)
// Final: 3u/2 live topics + u/2 live views -> bootTopics = 2u; topic ids assigned = 4u -> droppedIds = 2u.

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/node/CatalogCheckpoint.h>
#include <rune/node/CatalogPartition.h>
#include <rune/node/NodeConfig.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace rune::node::test {

struct CatalogBootLogShape {
    int64_t     events     = 0;
    std::size_t bootTopics = 0;
    std::size_t droppedIds = 0;
    uint64_t    digest     = 0;    // CatalogState::digest() after the last event == the checkpoint's
};
struct CatalogBootScript { std::vector<rune::catalog::CatalogEvent> events; uint64_t digest = 0; };

inline CatalogBootScript catalogBootScript(int unit) {
    namespace cat = rune::catalog;  namespace cd = rune::codec;
    using cat::testing::golden_detail::ScriptBuilder;
    using cat::testing::golden_detail::addOp;
    if (unit <= 0 || unit % 2 != 0) throw std::logic_error("catalogBootScript: unit must be even and > 0");
    ScriptBuilder b;
    auto topicDesc = [](const std::string& n) {
        cd::DescriptorBuilder d(n);
        d.add("id", cd::PrimType::Int64, 1, /*isKey=*/true).add("hp", cd::PrimType::Int32);
        return cd::serialize(d.build());
    };
    auto viewDef = [&](const std::string& v, const std::string& src, int32_t parts, uint8_t tagByte) {
        cat::ViewDef d;
        d.sinkPartitions = parts; d.persistence = cat::Persistence::Disk; d.keyField = "id";
        d.sourceNames = {src};
        d.normalizedPlanBlob = {tagByte, 0x00, 0x00, 0x00, 0x00};
        d.compiledPlanBlob   = {static_cast<uint8_t>(0xC0 | tagByte), 0x00, 0x00, 0x00, 0x00};
        d.outputDescriptorBlob = topicDesc(v);
        return d;
    };
    const auto T = [](int i) { return "t" + std::to_string(i); };
    const auto V = [](int i) { return "v" + std::to_string(i); };
    for (int i = 0; i < 2 * unit; ++i) {
        cat::CreateTopicOps o;
        o.partitions = 4; o.replicationFactor = 1; o.persistence = cat::Persistence::Disk;
        o.schema = cat::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = topicDesc(T(i)), .nextTag = 2};
        b.createTopic(T(i), std::move(o), "CREATE TOPIC " + T(i));
    }
    for (int i = 0; i < 2 * unit; ++i) b.alterSchema(T(i), {addOp("mana", cd::PrimType::Int32)}, "ALTER TOPIC " + T(i) + " ADD mana INT32");
    for (int i = 0; i < 2 * unit; ++i) b.alterConfig(T(i), {{cat::ConfigKey::RetentionMs, 60'000 + i}}, "ALTER TOPIC " + T(i) + " SET retention.ms");
    for (int i = 0; i < unit; ++i)     b.setCompat(T(i), cd::CompatMode::Forward, "ALTER TOPIC " + T(i) + " SET COMPATIBILITY FORWARD");
    for (int i = 0; i < unit; ++i)     b.createView(V(i), viewDef(V(i), T(i), 1, 0x01), "CREATE VIEW " + V(i));
    for (int i = 0; i < unit; ++i)     b.alterView(V(i), viewDef(V(i), T(i), 2, 0x02), "ALTER VIEW " + V(i));
    for (int i = unit / 2; i < unit; ++i)         b.dropView(V(i), "DROP VIEW " + V(i));
    for (int i = 3 * unit / 2; i < 2 * unit; ++i) b.dropTopic(T(i), "DROP TOPIC " + T(i));
    const uint64_t digest = b.state().digest();
    return CatalogBootScript{std::move(b).take(), digest};
}

/// Writes dataDir/__catalog (must not exist or be empty) + its `applied` checkpoint.  Throws on any failure.
inline CatalogBootLogShape writeCatalogBootLog(const std::filesystem::path& dataDir, int unit = 1000) {
    namespace fs = std::filesystem;
    CatalogBootScript s = catalogBootScript(unit);
    const fs::path dir = dataDir / "__catalog";
    fs::create_directories(dir);
    {
        rune::storage::Log log(dir.string());
        log.recover();
        if (log.logEndOffset() != 0) throw std::logic_error("writeCatalogBootLog: " + dir.string() + " is not empty");
        NodeConfig nc;
        nc.nodeId = 1;
        rune::replication::NullPartitionTransport transport;
        rune::replication::PartitionReplica rep(makeCatalogPartitionConfig(nc, 1), rune::replication::Role::Follower, 0,
                                                std::vector<rune::replication::NodeId>{1},
                                                std::vector<rune::replication::NodeId>{1}, &log, &transport);
        rep.becomeLeader(1, {1}, /*partitionEpoch=*/0);
        for (const auto& e : s.events) {
            auto bytes = rune::catalog::encode(e);
            if (!bytes) throw std::logic_error("writeCatalogBootLog: an event does not encode");
            if (rep.append(std::span<const uint8_t>(bytes->data(), bytes->size()), /*acksAll=*/true) < 0)
                throw std::logic_error("writeCatalogBootLog: append refused");
        }
        if (log.logEndOffset() != static_cast<int64_t>(s.events.size()))
            throw std::logic_error("writeCatalogBootLog: log end != event count");
    }   // replica + Log closed before the boot step opens the dir
    const int64_t last = static_cast<int64_t>(s.events.size()) - 1;
    if (!writeCatalogCheckpoint(dir.string(), CatalogCheckpoint{last, s.digest}))
        throw std::logic_error("writeCatalogBootLog: checkpoint write failed");
    return CatalogBootLogShape{static_cast<int64_t>(s.events.size()), static_cast<std::size_t>(2 * unit),
                               static_cast<std::size_t>(2 * unit), s.digest};
}

}  // namespace rune::node::test
