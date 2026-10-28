#pragma once
// NamespaceReaper.h - namespaces Phase 2 Task 7 (D17, D18; engineering rule 16): drives every DELETING namespace to
// removal, one idempotent step at a time, on the `__catalog` leader's admin thread.
//
// A namespace enters DELETING with ONE catalog event (NamespaceDeleted); that event is the commit point.  What is
// left - the open transactions, the topics and views, the consumer-group offsets, the transaction state, the security
// records - is cleaned by the reaper.  Every step is recorded in the catalog itself (the object drops are catalog
// events; the other steps are NamespaceDeleteStep events with one bit each), so the registry IS the progress.
//
// Nothing is persisted by the reaper and no timer exists.  Whenever it is Idle (boot, a new leadership, after a
// step) it re-derives its state from the registry: the lowest-nsId DELETING namespace, then its first unfinished step
//
//     AbortingTxns (bit 1 not set) -> DroppingViews (pending views) -> DroppingTopics (pending topics; a topic's
//     schemas go with it, so there is no separate schema step) -> Tombstoning (bit 2 groups, then bit 4 transaction
//     state) -> AwaitRemoval (bit 8, SecurityPurged: recorded once this node's `__security` state holds no record of
//     the namespace; the purge itself is the `__security` leader's) -> erased by the apply of the last event -> Idle.
//
// A crash (a fresh reaper starts Idle), a leader change (onLeadershipLost -> Idle; the new leader's reaper derives the
// same state) and a refused step (retried on the next pass, never skipped) all converge from the registry alone.  A
// result is matched to the state's in-flight correlation; a stale one (a leadership ago) is ignored.
//
// Hands: ReaperPort.  The production port (AdminWorker) turns them into admin-thread work: an admin->control ring
// request (the stores and the transaction coordinator belong to the control thread), a DDL submitted to the worker's
// own queue (the same path a client DDL takes), and a NamespaceDeleteStep event.
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/NamespaceDeleteStep.h>
#include <rune/node/AdminChannels.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

namespace rune::node {

/// What the reaper can do.  Cold (admin thread, DDL rate): an interface is fine here.  Every method returns false when
/// the work could not be issued now (a full ring, a refused submit) - the reaper then retries on its next pass.
class ReaperPort {
public:
    virtual ~ReaperPort() = default;
    /// Ask the control thread to run `op` for `nsId` (`batch` = the tombstone batch).  The answer comes back through
    /// NamespaceReaper::onControlResult(correlation, ...).
    virtual bool askControl(uint64_t correlation, ReapOp op, uint32_t nsId, uint32_t batch) = 0;
    /// Submit DropView / DropTopic `name` of namespace `nsId` through the worker's DDL queue.  The answer comes back
    /// through NamespaceReaper::onDdlResult(correlation, code).
    virtual bool submitDrop(uint64_t correlation, uint32_t nsId, bool isView, const std::string& name) = 0;
    /// Submit a NamespaceDeleteStep(nsId, step) event.  The answer comes back through onDdlResult.
    virtual bool submitDeleteStep(uint64_t correlation, uint32_t nsId, uint8_t step) = 0;
    /// True when this node's applied `__security` state holds no user, role or grant of the namespace.
    virtual bool securityClean(uint32_t nsId, std::string_view name) = 0;
};

struct ReapIdle {};
struct ReapAbortingTxns   { uint32_t nsId; uint64_t inFlightCorrelation; };   // 0 = nothing in flight
struct ReapDroppingViews  { uint32_t nsId; uint64_t inFlightCorrelation; };
struct ReapDroppingTopics { uint32_t nsId; uint64_t inFlightCorrelation; };
/// `step` = the NsDeleteStepBit being cleaned: GroupsTombstoned or TxnStateTombstoned.
struct ReapTombstoning    { uint32_t nsId; uint8_t step; uint64_t inFlightCorrelation; };
/// The step's work is done; its NamespaceDeleteStep event is being recorded.
struct ReapRecordingStep  { uint32_t nsId; uint8_t step; uint64_t inFlightCorrelation; };
struct ReapAwaitRemoval   { uint32_t nsId; };
using ReapState = std::variant<ReapIdle, ReapAbortingTxns, ReapDroppingViews, ReapDroppingTopics, ReapTombstoning,
                               ReapRecordingStep, ReapAwaitRemoval>;

class NamespaceReaper {
public:
    /// `batch` = RUNE_NS_DELETE_BATCH (tombstones per control call).
    explicit NamespaceReaper(uint32_t batch = 256) noexcept : batch_(batch == 0 ? 1 : batch) {}

    /// One pass.  Derives the state from `cs` whenever Idle (or when the current target is no longer DELETING), then
    /// issues the state's next piece of work through `port`.  Returns the number of things issued / transitions made.
    /// No timer: `nowNs` is only for symmetry with every other agent's doWork.
    int doWork(int64_t nowNs, const rune::catalog::CatalogState& cs, ReaperPort& port);

    /// The answer of a DDL / event the port submitted.  A result whose correlation is not the state's in-flight one is
    /// ignored (it belongs to an earlier leadership).  Whatever the code, the registry decides what is left to do:
    /// the in-flight slot is simply cleared, so a refused submit is retried and an applied one moves on.
    void onDdlResult(uint64_t correlation, uint16_t code) noexcept;
    /// The control thread's answer to askControl (same matching rule).
    void onControlResult(uint64_t correlation, ReapStatus status) noexcept;
    /// This broker no longer leads `__catalog`: Idle; every in-flight result is stale from now on.
    void onLeadershipLost() noexcept { state_ = ReapIdle{}; }

    [[nodiscard]] const ReapState& state() const noexcept { return state_; }
    [[nodiscard]] std::string_view stateName() const noexcept;
    [[nodiscard]] uint32_t batch() const noexcept { return batch_; }

    /// Test seam: at most `n` catalog events (drops, delete steps) are issued from now on (-1 = unlimited).  A test uses
    /// it to stop the reaper after exactly k durable steps - the crash / leader-change points.
    void setIssueBudgetForTest(int64_t n) noexcept { budget_ = n; }
    [[nodiscard]] uint64_t eventsIssued() const noexcept { return eventsIssued_; }

private:
    [[nodiscard]] ReapState derive_(const rune::catalog::NamespaceRecord& rec) const;
    int step_(const rune::catalog::CatalogState& cs, ReaperPort& port);
    [[nodiscard]] bool takeBudget_() noexcept {
        if (budget_ == 0) return false;
        if (budget_ > 0) --budget_;
        ++eventsIssued_;
        return true;
    }

    ReapState state_{ReapIdle{}};
    uint32_t  batch_;
    uint64_t  nextCorrelation_ = 1;
    int64_t   budget_ = -1;
    uint64_t  eventsIssued_ = 0;
};

} // namespace rune::node
