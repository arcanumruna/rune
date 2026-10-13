#pragma once
// AdminWorkerTestAccess.h — test-only access to AdminWorker internals (final
// review M4: no public test mutator on the production class).  AdminWorker
// befriends this struct; only test code includes this header.
#include <rune/node/AdminWorker.h>

namespace rune::node {

struct AdminWorkerTestAccess {
    /// Swap the schema / __views replica pointers (nullptr forces the
    /// missing-replica AwaitingCommit branch).  Returns the previous pair.
    static AdminWorker::ReplicasForTest swapReplicas(AdminWorker& w, AdminWorker::ReplicasForTest r) noexcept {
        const AdminWorker::ReplicasForTest old{w.replica_, w.viewsReplica_};
        w.replica_      = r.schema;
        w.viewsReplica_ = r.views;
        return old;
    }
};

} // namespace rune::node
