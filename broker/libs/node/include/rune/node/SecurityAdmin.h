#pragma once
// SecurityAdmin.h — the `__security` leadership and write path on the admin thread (authentication spec §3, plan
// D36/D39).  The AdminWorker's counterpart for `__security`: it owns neither the replica nor the applier (the
// AdminWorker does; it drives both logs' replicas and appliers each pass) but reads them: the leadership is one
// std::variant (hard rule 3), each state its own variables —
//   SecurityFollower        — not the `__security` leader (leaderId -1 = leaderless);
//   SecurityLeaderCatchingUp— the replica leads but not every record of its log is both COMMITTED and APPLIED here
//                             (a fresh promotion, a previous epoch's tail, a commit that timed out);
//   SecurityLeaderServing   — leads, HW == LEO and the applier applied up to the HW: security DDL is served;
//   SecurityHalted          — this broker's SecurityApplier halted at `offset` (D28): terminal for the process.
// Every transition logs "AdminWorker: __security role -> <roleName()> (epoch N)" at info.
//
// The write path (D36, spec §3): a SecurityDdlMsg / SecurityBootstrapMsg is answered
//   NotLeader (2)  unless SecurityLeaderServing (catching up, demoted, the commit timed out - all retriable),
//   Internal (6)   when SecurityHalted;
// otherwise the leader builds the event from its applied state (objectId / versions, the principal from DdlMeta -
// `authenticated` = the session's user, "bootstrap" for a bootstrap, D3 - the REDACTED statement, its wall clock),
// dedupes on (requestId, requestFingerprint) (a committed one answers its original result; a different fingerprint
// answers InvalidRequest "requestId reused for a different statement"), dry-runs previewApplied (a refusal answers
// InvalidRequest 39 with refusalName + detail - never appended), encodes, appends acks=all and answers ONCE the event
// is committed AND applied locally, gated on its offset (D24: the control thread releases the OK only after its own
// SecurityView applied the event and its permission refresh reached every worker).
// Hard rules: admin-thread-owned; the in-flight request is std::variant<Idle, AwaitingCommit>; cold path.

#include <rune/node/AdminChannels.h>
#include <rune/node/SecurityApplier.h>
#include <rune/security/SecurityEvent.h>

#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace rune::replication { class PartitionReplica; struct AlterIsrResponseMsg; }
namespace rune::catalog { class CatalogState; }

namespace rune::node {

/// The largest encoded event the leader writes (the applier mirrors it in one admin→control ring message, and every
/// `__security` subscriber's Records frame carries it whole).
inline constexpr std::size_t kSecurityMaxEventBytes = 64 * 1024;

struct SecurityFollower         { int32_t leaderId = -1; int32_t epoch = 0; };
struct SecurityLeaderCatchingUp { int32_t epoch = 0; };
struct SecurityLeaderServing    { int32_t epoch = 0; };
struct SecurityHalted           { int64_t offset = -1; int32_t epoch = 0; };
using SecurityRole = std::variant<SecurityFollower, SecurityLeaderCatchingUp, SecurityLeaderServing, SecurityHalted>;

/// One answer of the write path: a DdlResponse (78) for (session, corr).  `gateOffset` >= 0: an OK for the event at
/// that offset - production posts it on the egress ring as kMsgEgressFrameGated; -1: ungated (a refusal / error).
using SecurityRespond = std::function<void(int64_t session, int64_t corr, uint16_t code, uint64_t objectId,
                                           uint32_t newVersion, int64_t gateOffset, std::string_view detail)>;

/// Namespaces Task 8 (spec §4): the namespace guard's verdict.  Missing answers exactly what a namespace that does not
/// exist answers (a foreign namespace is never confirmed to a non-cluster-admin).
enum class GuardVerdict : uint8_t { Allow, NotAuthorized, Missing };
/// Who is asking, as the leader derives it from a SecurityDdlMsg.
struct CallerPerms {
    uint32_t nsId         = 0;      // the session user's namespace
    bool     clusterAdmin = false;  // effectively ADMIN ON CLUSTER (the gateway found no denial and checked no proof)
    bool     nsAdmin      = false;  // an enabled user of nsId effectively holding ADMIN ON NAMESPACE <its own name>
    bool     denied       = true;   // the gateway's verdict stands (a non-empty denial)
};
/// The guard's view of one resolved security DDL.
struct SecurityDdl {
    const rune::security::GrantSpec* spec = nullptr;     // Grant / Revoke: the grant written or removed
    bool memberRoleIsPrivileged = false;                  // GrantRole / RevokeRole: the role holds a privileged grant
    bool targetIsPrivileged     = false;                  // the target user / role holds one (directly or via a role)
    bool targetIsCaller         = false;                  // the target is the caller's own user
    bool ownCredentialUnproven  = false;                  // PR #642 F1: a change of the caller's own password, no proof
};
/// ClusterAdmin -> Allow.  A target outside the caller's namespace -> Missing.  A namespace admin in its own namespace
/// -> Allow, except NotAuthorized for a grant / revoke of anything on Cluster or of ADMIN on a Namespace, a membership
/// in a role holding one, or any change to another user / a role holding one.  Otherwise the gateway's verdict.
/// PR #642 review F1: a non-cluster-admin's change of its OWN password without the gateway's proof check is always
/// NotAuthorized (issue #114: a session alone never mints a credential, however the request names the account).
/// Pure; cold path (DDL rate).
[[nodiscard]] GuardVerdict nsGuard(const CallerPerms& caller, uint32_t targetNsId, const SecurityDdl& ddl);

class SecurityAdmin {
public:
    SecurityAdmin(SecurityApplier& applier, rune::replication::PartitionReplica* replica, SecurityRespond respond,
                  int32_t selfId);

    /// Namespaces Task 8: the admin thread's applied catalog, whose namespace registry resolves `ns/local` names and
    /// the namespace guard (spec §4).  Not owned; it must outlive this object (the AdminWorker's CatalogApplier state).
    /// nullptr (a rig without a catalog): only `default` exists.
    void bindCatalog(const rune::catalog::CatalogState* catalog) noexcept { catalog_ = catalog; }

    /// Queued, one in flight.  Beyond kMaxPending the request is answered Internal at once (never started).
    void submit(SecurityRequest r);
    static constexpr std::size_t kMaxPending = 256;
    /// An appended event not committed + applied within this bound is answered the retriable NotLeader (the retry with
    /// the same requestId dedupes to its result).  Below the client's DDL deadline (15 s).
    static constexpr int64_t kDdlTimeoutNs = 10LL * 1'000'000'000LL;

    /// The replica's role (multi-node: the committed (-7, 0) assignment; single node: the consensus leader).
    /// Demotion answers the in-flight request NotLeader (retriable).
    void onRole(const AdminRoleMsg& role);
    /// ISR v3 Step 10: the committed `__security` leadership is HELD (storage unconfirmed): never lead; ask the
    /// controller for a failover (the restart-fence bump) and serve as a follower of nobody.
    void onHoldLeadership(const AdminRoleMsg& role);
    /// control → admin: the controller's AlterIsrResponse for (-7, 0), verbatim.
    void onAlterIsrResponse(const rune::replication::AlterIsrResponseMsg& r);

    /// One pass: advance the in-flight request, derive Follower ⇄ CatchingUp ⇄ Serving ⇄ Halted, start the next.
    int doWork(int64_t nowNs);

    [[nodiscard]] const SecurityRole& role() const noexcept { return role_; }
    /// "Follower" | "LeaderCatchingUp" | "LeaderServing" | "Halted".
    [[nodiscard]] std::string_view roleName() const noexcept;
    [[nodiscard]] bool isLeader() const noexcept { return std::holds_alternative<SecurityLeaderServing>(role_); }
    [[nodiscard]] rune::replication::PartitionReplica* replica() const noexcept { return replica_; }
    [[nodiscard]] uint64_t startedCount() const noexcept { return started_; }
    [[nodiscard]] uint64_t answeredCount() const noexcept { return answered_; }
    [[nodiscard]] uint64_t eventsAppended() const noexcept { return eventsAppended_; }
    [[nodiscard]] std::size_t pendingCount() const noexcept { return pending_.size(); }
    [[nodiscard]] uint64_t pendingRefused() const noexcept { return pendingRefused_; }
    [[nodiscard]] bool hasInFlight() const noexcept { return !std::holds_alternative<Idle>(inFlight_); }
    [[nodiscard]] std::string_view inFlightStateName() const noexcept {
        return hasInFlight() ? "AwaitingCommit" : "Idle";
    }

    struct Idle {};
    struct AwaitingCommit {
        SecurityRequest              req;
        rune::security::SecurityEvent event;
        int64_t                      offset    = -1;
        int64_t                      startedNs = 0;
    };

private:
    bool updateRole_();
    void demote_(int32_t leaderId, int32_t epoch);
    void setRole_(SecurityRole next);
    [[nodiscard]] bool caughtUp_() const noexcept;
    bool advance_(int64_t nowNs);
    void startNext_(int64_t nowNs);
    void startDdl_(const SecurityDdlMsg& m, int64_t nowNs, SecurityRequest& req);
    void startBootstrap_(const SecurityBootstrapMsg& m, int64_t nowNs, SecurityRequest& req);
    /// Preview, encode and append `e`; false = answered already.
    bool appendEvent_(SecurityRequest& req, rune::security::SecurityEvent e, int64_t nowNs);
    void answer_(const SecurityRequest& req, uint16_t code, std::string_view detail = {}, uint64_t objectId = 0,
                 uint32_t newVersion = 0, int64_t gateOffset = -1);
    [[nodiscard]] static std::pair<int64_t, int64_t> sessionCorrOf_(const SecurityRequest& req) noexcept;

    SecurityApplier&                     applier_;
    rune::replication::PartitionReplica* replica_;
    SecurityRespond                      respond_;
    int32_t                              selfId_;
    const rune::catalog::CatalogState*   catalog_ = nullptr;
    SecurityRole                         role_{SecurityFollower{}};
    std::variant<Idle, AwaitingCommit>   inFlight_;
    std::deque<SecurityRequest>          pending_;
    std::vector<uint8_t>                 eventBuf_;   // the encoded event (capacity reused)
    uint64_t started_ = 0, answered_ = 0, eventsAppended_ = 0, pendingRefused_ = 0;
};

} // namespace rune::node
