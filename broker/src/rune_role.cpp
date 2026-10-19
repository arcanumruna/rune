// ---------------------------------------------------------------------------
// rune_role — standalone reader for the Aeron cluster node-role counters.
//
// Reads the machine-readable leadership signal that BrokerNode publishes into
// the embedded driver's CnC file (see RoleCounter.h): the node-role counter and
// the leadership-term counter.  Prints ONE line to stdout:
//
//     ROLE nodeId=<n> role=<LEADER|FOLLOWER|CANDIDATE|NONE> term=<t>
//     STORAGE nodeId=<n> state=<CONFIRMED|UNCONFIRMED|UNDECIDED|UNKNOWN> heldLeaderRoles=<n> pendingNodes=<n> \
//             outOfSync=<n> lostCommittedReports=<n>        (ISR v3 section 3.10; one line; a counter that is
//                                                            absent prints -1, the state prints UNKNOWN)
//
// This is the Aeron-faithful replacement for scraping "is LEADER" log lines:
// the dr-e2e harness runs `docker exec <svc> rune_role` and parses this line,
// so leader detection no longer depends on RUNE_LOG_LEVEL.
//
// Usage:
//     rune_role [<aeronDir>]
//   - If <aeronDir> is given, reads <aeronDir>/cnc.dat.
//   - Else uses ${RUNE_DATA_DIR}/aeron (RUNE_DATA_DIR default /var/lib/rune),
//     matching BrokerNode::startEmbeddedDriver().
//
// Exit codes:
//   0  counter found and printed
//   1  CnC file missing / unreadable (driver not up yet)
//   2  CnC readable but the role counter is not present yet
// ---------------------------------------------------------------------------
#include <rune/node/RoleCounter.h>

#include <CncFileReader.h>
#include <concurrent/CountersReader.h>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

using rune::node::kRoleCounterTypeId;
using rune::node::kTermCounterTypeId;
using rune::node::RoleCode;
using rune::node::roleCodeFromValue;
using rune::node::roleCodeName;
using rune::node::decodeRoleKey;

namespace {

std::string resolveAeronDir(int argc, char** argv) {
    if (argc > 1 && argv[1] != nullptr && argv[1][0] != '\0') {
        return std::string(argv[1]);
    }
    const char* dataDir = std::getenv("RUNE_DATA_DIR");
    const std::string base = (dataDir && dataDir[0] != '\0')
        ? std::string(dataDir) : std::string("/var/lib/rune");
    return base + "/aeron";
}

} // namespace

int main(int argc, char** argv) {
    const std::string aeronDir = resolveAeronDir(argc, argv);

    aeron::CncFileReader reader = [&]() {
        try {
            return aeron::CncFileReader::mapExisting(aeronDir.c_str());
        } catch (const std::exception& ex) {
            std::cerr << "rune_role: cannot map CnC file in '" << aeronDir
                      << "': " << ex.what() << "\n";
            std::exit(1);
        }
    }();

    aeron::concurrent::CountersReader counters = reader.countersReader();

    // Scan for our role + term counters (both keyed by the same nodeId).  In a
    // single broker process there is exactly one of each; we take the first.
    std::int32_t nodeId = -1;
    RoleCode     role   = RoleCode::None;
    std::int64_t term   = -1;
    bool foundRole = false;
    bool foundTerm = false;

    std::int64_t gateState = -1, heldRoles = -1, pendingNodes = -1, outOfSync = -1, lostCommitted = -1;

    counters.forEach(
        [&](std::int32_t id, std::int32_t typeId,
            const aeron::concurrent::AtomicBuffer& keyBuffer,
            const std::string& /*label*/) {
            if (typeId == rune::node::kStorageGateStateCounterTypeId) gateState = counters.getCounterValue(id);
            else if (typeId == rune::node::kLeaderRolesHeldCounterTypeId) heldRoles = counters.getCounterValue(id);
            else if (typeId == rune::node::kStoragePendingNodesCounterTypeId) pendingNodes = counters.getCounterValue(id);
            else if (typeId == rune::node::kOutOfSyncCounterTypeId) outOfSync = counters.getCounterValue(id);
            else if (typeId == rune::node::kLostCommittedReportsCounterTypeId) lostCommitted = counters.getCounterValue(id);
            if (typeId == kRoleCounterTypeId) {
                std::int32_t nid = -1;
                if (decodeRoleKey(keyBuffer.buffer(),
                                  static_cast<std::size_t>(keyBuffer.capacity()),
                                  nid)) {
                    nodeId = nid;
                }
                role = roleCodeFromValue(counters.getCounterValue(id));
                foundRole = true;
            } else if (typeId == kTermCounterTypeId) {
                term = counters.getCounterValue(id);
                foundTerm = true;
            }
        });

    if (!foundRole) {
        std::cerr << "rune_role: role counter (typeId=" << kRoleCounterTypeId
                  << ") not present in '" << aeronDir << "' yet\n";
        return 2;
    }
    (void)foundTerm;  // term may legitimately be -1 pre-election

    std::cout << "ROLE nodeId=" << nodeId
              << " role=" << roleCodeName(role)
              << " term=" << term << "\n";
    const char* state = gateState == 0 ? "CONFIRMED" : gateState == 1 ? "UNCONFIRMED" : gateState == 2 ? "UNDECIDED" : "UNKNOWN";
    std::cout << "STORAGE nodeId=" << nodeId
              << " state=" << state
              << " heldLeaderRoles=" << heldRoles
              << " pendingNodes=" << pendingNodes
              << " outOfSync=" << outOfSync
              << " lostCommittedReports=" << lostCommitted << "\n";
    return 0;
}
