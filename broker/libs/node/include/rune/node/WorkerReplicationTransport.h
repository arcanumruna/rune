#pragma once
// Forwarding header: the per-worker replication transport lives in rune_replication
// so PartitionReplica can call it concretely (de-virtualised, spec §3.5a.1).
#include <rune/replication/WorkerReplicationTransport.h>

namespace rune::node {
using rune::replication::AeronPubRegistry;
using rune::replication::WorkerReplicationTransportImpl;
using rune::replication::WorkerReplicationTransport;
} // namespace rune::node
