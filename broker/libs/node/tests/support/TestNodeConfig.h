#pragma once
// TestNodeConfig.h — explicit runtime pinning for tests that exercise something
// ORTHOGONAL to the worker runtime (gateway-path replication, elections,
// compaction, ...).
//
// Since 2026-10-02 the NodeConfig DEFAULT is the measured 110k config: shard
// mode + fused placement, with per-worker base ports DERIVED per node id
// (HostDefaults.h).  Derived ports are fixed per node id — right for a
// deployment, but concurrent test processes (ctest -j) would collide on them,
// and these suites were written against the legacy gateway path.  They pin it
// here, explicitly, instead of relying on a default.  Shard coverage lives in
// ShardModeTest (explicit collision-free ports) and the default-config tests.
//
// Test-only: must NOT be included from production headers.
#include <rune/node/NodeConfig.h>

namespace rune::node::test {

inline void pinLegacyRuntime(NodeConfig& c) {
    c.workerMode       = NodeConfig::WorkerMode::Legacy;
    c.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
    c.workerCoreBase   = -1;
}

} // namespace rune::node::test
