#pragma once
// LeaseModel.h - lease topics Task 18 (spec 16.4): the reference model of the seeded lease stress.  Pure data: no broker or client types.
//
// Two views of the truth are kept per key and compared:
//   * the ACK view: what the driver was told (granted version, acknowledged deadline bounds, revokes), and
//   * the LOG fold: what the committed lease-topic log says (every grant, update, end and checkpoint record, in offset order).
// Times are nanoseconds on the lease clock (leaseClockNowNs() on a real clock, the injected clock on the single-node leg).
//
// Deadline bounds of the live version (spec 16.4 I3 / I7):
//   lo = max over acknowledged grants / keepalives of (the driver's clock reading taken BEFORE it sent the call) + the ttl the broker
//        granted.  The broker armed the lease no earlier than the send, so its true deadline is >= lo: an Expire visible before lo is early.
//   hi = max over the same calls of (the reading taken AFTER the acknowledgement) + ttl.  The true deadline is <= hi.
// A call whose answer is in doubt (a timeout, a leader change) only raises hi (the lease may have been renewed).
//
// An end record is seen by the driver at `rxNs`, a reading of the lease clock taken at or after the record's append: append <= rxNs.  An
// Expire with rxNs < lo - slack was therefore appended early (I3).
#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace rune::node::test::leasemodel {

inline constexpr int64_t kNsPerMs = 1'000'000;

enum class RecOp : uint8_t { Put = 0, Revoke = 1, Expire = 2, Checkpoint = 3 };

/// One committed record of the lease topic as the driver's tap saw it.
struct LogRec {
    int32_t     partition = 0;
    int64_t     offset    = 0;
    RecOp       op        = RecOp::Put;
    int64_t     version   = 0;     ///< the lease_version column of the record
    uint64_t    a         = 0;     ///< the user column `a` (Put only)
    int64_t     remainingMs = 0;   ///< the lease_remaining_ms column (Checkpoint only)
    std::string key;
    int64_t     rxNs      = 0;     ///< lease-clock reading at or after the append (see above)
};

struct End {
    int64_t version = 0;
    RecOp   kind    = RecOp::Expire;
    int64_t rxNs    = 0;
};

struct KeyModel {
    std::string name;
    // ---- the ack view ----
    int64_t live      = 0;          ///< the live version the driver believes (0 = none)
    std::vector<int64_t> granted;   ///< every version ever granted and acknowledged, in order (I2)
    int64_t lo        = 0;          ///< latest acknowledged deadline, lower bound (0 = none acknowledged)
    int64_t hi        = 0;          ///< latest possible deadline, upper bound
    int64_t grantLo   = 0;          ///< lo of the live version's grant (the checkpoint remaining check)
    int64_t ttlMs     = 0;
    bool    kept      = true;       ///< the driver keeps the lease alive (false: abandoned)
    int64_t abandonedAtNs = 0;
    bool    doubt     = false;      ///< a call on this key is in doubt: the log decides
    std::set<int64_t> pendingRevoke;   ///< acked revokes whose record the tap has not shown yet
    // ---- the log fold ----
    int64_t  foldLive = 0;
    uint64_t foldA    = 0;
    std::vector<End> ends;          ///< in log order
    std::set<int64_t> seenGrants;   ///< every grant record in the log
    uint64_t checkpoints = 0;
};

class Model {
public:
    explicit Model(int keys) : keys_(static_cast<std::size_t>(keys)) {
        for (int i = 0; i < keys; ++i) {
            char b[16];
            std::snprintf(b, sizeof b, "k%02d", i);
            keys_[static_cast<std::size_t>(i)].name = b;
            index_[b] = i;
        }
    }
    [[nodiscard]] int size() const noexcept { return static_cast<int>(keys_.size()); }
    [[nodiscard]] KeyModel& key(int i) { return keys_[static_cast<std::size_t>(i)]; }
    [[nodiscard]] const KeyModel& key(int i) const { return keys_[static_cast<std::size_t>(i)]; }
    [[nodiscard]] int indexOf(const std::string& name) const {
        const auto it = index_.find(name);
        return it == index_.end() ? -1 : it->second;
    }

    // ------------------------------------------------------------------ acknowledged calls

    /// I1 / I2 at the ack: a grant is acknowledged only for a free key, and its version is above every earlier one.
    [[nodiscard]] std::string grantAcked(int k, int64_t version, int64_t ttlMs, int64_t beforeNs, int64_t afterNs) {
        KeyModel& m = key(k);
        if (m.live != 0) return "I1: grant acknowledged (version " + std::to_string(version) + ") while version " + std::to_string(m.live) + " is live";
        if (!m.granted.empty() && version <= m.granted.back())
            return "I2: grant version " + std::to_string(version) + " does not exceed the previous " + std::to_string(m.granted.back());
        if (!m.seenGrants.empty() && version <= *m.seenGrants.rbegin() && !m.seenGrants.count(version))
            return "I2: grant version " + std::to_string(version) + " is below a version already in the log";
        m.granted.push_back(version);
        m.live = version;
        m.ttlMs = ttlMs;
        m.lo = m.grantLo = beforeNs + ttlMs * kNsPerMs;
        m.hi = afterNs + ttlMs * kNsPerMs;
        m.kept = true;
        m.doubt = false;
        return {};
    }
    [[nodiscard]] std::string keepAliveAcked(int k, int64_t version, int64_t ttlMs, int64_t beforeNs, int64_t afterNs) {
        KeyModel& m = key(k);
        if (m.live != version) return "I1: keepalive acknowledged for version " + std::to_string(version) + " but the live version is " + std::to_string(m.live);
        m.ttlMs = ttlMs;
        m.lo = std::max(m.lo, beforeNs + ttlMs * kNsPerMs);
        m.hi = std::max(m.hi, afterNs + ttlMs * kNsPerMs);
        return {};
    }
    [[nodiscard]] std::string revokeAcked(int k, int64_t version) {
        KeyModel& m = key(k);
        if (m.live != version) return "I1: revoke acknowledged for version " + std::to_string(version) + " but the live version is " + std::to_string(m.live);
        m.live = 0;
        m.pendingRevoke.insert(version);
        return {};
    }
    /// A call whose answer is in doubt: the key's state is whatever the log says; the deadline may have moved up.
    void inDoubt(int k, int64_t afterNs, int64_t maxTtlMs) {
        KeyModel& m = key(k);
        m.doubt = true;
        m.hi = std::max(m.hi, afterNs + maxTtlMs * kNsPerMs);
    }

    // ------------------------------------------------------------------ the log

    /// Folds one committed record; returns the violated invariant ("" = none).  `slackNs`: the I3 / checkpoint tolerance.
    [[nodiscard]] std::string onRecord(const LogRec& r, int64_t slackNs) {
        const int k = indexOf(r.key);
        if (k < 0) return "log: a record of the unknown key '" + r.key + "'";
        KeyModel& m = key(k);
        switch (r.op) {
        case RecOp::Put:
            if (r.version == r.offset) {   // a grant
                if (m.foldLive != 0) return "I1: grant record " + std::to_string(r.offset) + " of " + m.name + " while version " + std::to_string(m.foldLive) + " has no end record";
                if (!m.seenGrants.empty() && r.version <= *m.seenGrants.rbegin()) return "I2: grant record " + std::to_string(r.version) + " of " + m.name + " does not exceed an earlier one";
                m.seenGrants.insert(r.version);
                m.foldLive = r.version;
                m.foldA = r.a;
                const bool acked = std::find(m.granted.begin(), m.granted.end(), r.version) != m.granted.end();   // acked, then maybe revoked / expired before the tap showed it
                if (m.live != r.version && !m.doubt && !acked) {
                    // the log shows a grant the driver never saw acknowledged and had no call in doubt for
                    return "log: grant record " + std::to_string(r.version) + " of " + m.name + " that no acknowledged call produced (live " + std::to_string(m.live) + ")";
                }
                if (m.doubt && m.live != r.version && !acked) {   // an in-doubt grant landed: adopt it (no acknowledged bound: lo stays 0)
                    m.live = r.version;
                    m.granted.push_back(r.version);
                    m.lo = m.grantLo = 0;
                }
            } else {   // an update keeps the lease's version
                if (m.foldLive != r.version) return "log: update record " + std::to_string(r.offset) + " of " + m.name + " for version " + std::to_string(r.version) + " but the live version is " + std::to_string(m.foldLive);
                m.foldA = r.a;
            }
            return {};
        case RecOp::Revoke:
        case RecOp::Expire: {
            for (const End& e : m.ends)
                if (e.version == r.version) return "I4: a second end record (offset " + std::to_string(r.offset) + ") for " + m.name + " version " + std::to_string(r.version);
            if (m.foldLive != r.version) return "I4: end record " + std::to_string(r.offset) + " of " + m.name + " for version " + std::to_string(r.version) + " but the log's live version is " + std::to_string(m.foldLive);
            m.ends.push_back(End{r.version, r.op, r.rxNs});
            m.foldLive = 0;
            if (r.op == RecOp::Revoke) {
                if (!m.pendingRevoke.erase(r.version)) {
                    if (m.live == r.version && m.doubt) m.live = 0;   // an in-doubt revoke landed
                    else return "log: a Revoke record of " + m.name + " version " + std::to_string(r.version) + " that no acknowledged revoke produced";
                }
            } else {
                // I3 never early: the Expire cannot have been appended before the latest acknowledged deadline.
                if (m.live == r.version && m.lo != 0 && r.rxNs + slackNs < m.lo)
                    return "I3: Expire of " + m.name + " version " + std::to_string(r.version) + " seen at " + std::to_string(r.rxNs) + " ns, before the acknowledged deadline " + std::to_string(m.lo) +
                           " ns (" + std::to_string((m.lo - r.rxNs) / 1000) + " us early, slack " + std::to_string(slackNs / 1000) + " us)";
                if (m.live == r.version) m.live = 0;
                else if (m.live != 0) return "I1: Expire of version " + std::to_string(r.version) + " while the driver's live version of " + m.name + " is " + std::to_string(m.live);
                else return "I4: Expire of " + m.name + " version " + std::to_string(r.version) + " that the driver believes already ended (an acknowledged revoke: " +
                            (m.pendingRevoke.count(r.version) ? "yes" : "no") + ")";
            }
            return {};
        }
        case RecOp::Checkpoint:   // the client's subscriptions drop Checkpoint records before the handler: never seen here (checkRemaining reads them from the view)
            ++m.checkpoints;
            return {};
        }
        return {};
    }

    /// I5, the checkpoint half: a lease view row's lease_remaining_ms (the last Checkpoint(R) folded into it; 0 = none) is >= the true remaining time
    /// at the checkpoint's offset.  The true remaining there is deadline - append >= grantLo - now (deadlines only grow, the append is not after `nowNs`).
    [[nodiscard]] std::string checkRemaining(int k, int64_t version, uint64_t remainingMs, int64_t nowNs, int64_t slackNs) const {
        const KeyModel& m = key(k);
        if (remainingMs == 0 || m.live != version || m.grantLo == 0) return {};
        const int64_t floorNs = m.grantLo - nowNs;
        if (static_cast<int64_t>(remainingMs) * kNsPerMs + slackNs < floorNs)
            return "I5: the lease view row of " + m.name + " version " + std::to_string(version) + " carries lease_remaining_ms " + std::to_string(remainingMs) +
                   ", below the true remaining " + std::to_string(floorNs / kNsPerMs) + " ms";
        return {};
    }

    // ------------------------------------------------------------------ liveness

    /// Keys whose live lease must already have an end record at `nowNs`: the latest possible deadline plus `slackNs` has passed.
    [[nodiscard]] std::vector<int> due(int64_t nowNs, int64_t slackNs) const {
        std::vector<int> out;
        for (int i = 0; i < size(); ++i) {
            const KeyModel& m = key(i);
            if (m.live != 0 && !m.doubt && m.hi != 0 && m.hi + slackNs <= nowNs) out.push_back(i);
        }
        return out;
    }
    /// The smallest latest-possible deadline among live, undoubted keys that the driver does not keep (0 = none).
    [[nodiscard]] int64_t nextDeadlineOfAbandoned() const {
        int64_t best = 0;
        for (const KeyModel& m : keys_)
            if (m.live != 0 && !m.doubt && !m.kept && m.hi != 0 && (best == 0 || m.hi < best)) best = m.hi;
        return best;
    }

    /// Ends the doubt on `k`: the key's state is what the log fold says.
    void resolve(int k) {
        KeyModel& m = key(k);
        if (m.live != m.foldLive) {
            m.live = m.foldLive;
            if (m.live != 0 && (m.granted.empty() || m.granted.back() != m.live)) m.granted.push_back(m.live);
            if (m.live == 0) { m.lo = m.hi = m.grantLo = 0; }
            else { m.lo = m.grantLo = 0; }
        }
        m.doubt = false;
    }

    /// The state the log fold implies for `k`, to compare with a lease view row.
    struct Fold { int64_t live; uint64_t a; };
    [[nodiscard]] Fold fold(int k) const { return Fold{key(k).foldLive, key(k).foldA}; }

private:
    std::vector<KeyModel> keys_;
    std::map<std::string, int> index_;
};

}  // namespace rune::node::test::leasemodel
