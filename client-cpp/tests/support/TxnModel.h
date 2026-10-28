// tests/support/TxnModel.h - Phase 2b F Task 13: an INDEPENDENT model of one transactional producer, for the seeded
// stress (tests/client/TxnStressTest.cpp).
//
// It shares no code with ProducerImpl (and does not read TxnMatrixExpectations.h): plain maps of the committed and
// the pending records per partition, the group's committed offset, a transaction-state enum and the handle's produce
// gate, driven only by what an application and a broker can see:
//   * the ops the test issues (begin / data / sendOffsets / flush / commit / abort),
//   * the coordinator's decisions (which request a scripted code landed on: the fake's decision log - the model's
//     only source of non-determinism),
//   * the completions the producer delivers (checked against the set the rules allow, then applied).
// The rules are the spec's (Phase 2b F, FD2 / FD6, Kafka TransactionManager parity):
//   * the gate opens at begin and closes when commit / abort is ISSUED: a data op on a closed gate is refused
//     synchronously and has no effect.  Invoker mode (this rig): the owner thread IS the duty side, so the check also
//     refuses at once while the machine is not InTransaction (AbortableError / FatalError) - Thread mode would accept
//     and complete the record with the failure (FD2);
//   * fixed capacities (zero allocation, documented): at most 4 calls join a running commit / abort and at most 8
//     flushes are pending - a call beyond completes InvalidArgument (#344) and changes nothing;
//   * 27 ConcurrentTransactions and 14 NotCoordinator are retried: no visible effect;
//   * 18 ProducerFenced anywhere -> FatalError(Fenced); 19 InvalidTxnState on EndTxn(commit) -> FatalError(19);
//     19 on a leg (AddPartitions / AddOffsets / TxnOffsetCommit) while the transaction is open or its commit is
//     still waiting for data -> AbortableError(19): the commit fails 19, the app must abort; 19 on EndTxn(abort):
//     if the client HEARD a leg answered OK in this transaction (it exists on the coordinator - Kafka
//     transactionStarted) FatalError(19), else it was only possibly started and 19 means "nothing open": the abort
//     is Ok (m15).  (EndTxn is never sent for a transaction no leg was even sent for.);
//   * a commit completes Ok only if no failure reached the transaction: then every record of it is visible to a
//     read_committed reader exactly once, every record token of it completed Ok, and the group's committed offset
//     is the last one it sent; anything else leaves nothing visible;
//   * misuse completes BrokerError(19) (commit with no transaction, abort while committing); a second concurrent
//     sendOffsets completes InvalidArgument (#347); AbortableError ops complete with the poison, FatalError ops with the fatal status.
// Every token op completes exactly once; a completion outside the allowed set is a violation (`errors`).
#pragma once

#include <rune/client/Completion.h>   // OpStatus: the public result vocabulary

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace txnmodel {

using Status = rune::client::OpStatus;

struct Result {
    Status   status = Status::Ok;
    uint16_t code   = 0;
    /// BrokerError compares its code; every other status is its own answer.
    [[nodiscard]] bool matches(const Result& o) const noexcept {
        return status == o.status && (status != Status::BrokerError || code == o.code);
    }
};
inline constexpr Result kOk{Status::Ok, 0};
inline constexpr Result kInvalidTxnState{Status::BrokerError, 19};
/// #344 / #347: a call beyond a fixed capacity (joiners, flushes) or a second concurrent sendOffsets.
inline constexpr Result kCapRefused{Status::InvalidArgument, 0};
inline constexpr Result kFenced{Status::Fenced, 0};

enum class State : uint8_t { Uninitialized, Ready, InTxn, Committing, Aborting, Abortable, Fatal };
enum class Gate : uint8_t { Closed, Open };
enum class TokenKind : uint8_t { Data, Commit, Abort, Flush, SendOffsets };
/// The coordinator request a decision was taken on (the fake's view).
enum class Leg : uint8_t { AddPartitions, AddOffsets, TxnOffsetCommit, EndTxnCommit, EndTxnAbort };

inline const char* name(State s) {
    switch (s) {
    case State::Uninitialized: return "Uninitialized";
    case State::Ready:         return "Ready";
    case State::InTxn:         return "InTxn";
    case State::Committing:    return "Committing";
    case State::Aborting:      return "Aborting";
    case State::Abortable:     return "Abortable";
    case State::Fatal:         return "Fatal";
    }
    return "?";
}
inline const char* name(TokenKind k) {
    switch (k) {
    case TokenKind::Data:        return "data";
    case TokenKind::Commit:      return "commit";
    case TokenKind::Abort:       return "abort";
    case TokenKind::Flush:       return "flush";
    case TokenKind::SendOffsets: return "sendOffsets";
    }
    return "?";
}

class TxnModel {
public:
    using Token  = uint64_t;
    using Values = std::map<int32_t, std::multiset<std::string>>;

    // ---- what a read_committed reader must see / the group's committed offset ----
    Values  committed;
    int64_t committedOffset = 0;

    // ---- the producer ----
    State    state = State::Uninitialized;
    Gate     gate  = Gate::Closed;
    uint32_t txn   = 0;           // the current (or last) transaction's number
    Values   pending;             // the current transaction's accepted records
    std::optional<int64_t> stagedOffset;   // the last offset a sendOffsets of this transaction completed Ok with
    Token    offsetsRunning = 0;  // a sendOffsets still running (0 = none): a second one is refused InvalidArgument (#347)
    Result   poison{};            // Abortable: what the transaction failed with
    Result   fatal{};             // Fatal: the terminal status

    std::vector<std::string> errors;   // violations, in order

    static constexpr int kMaxJoiners = 4;   // calls joining a running commit / abort
    static constexpr int kMaxFlushes = 8;   // pending flushes

    // ---- lifecycle ----
    void initialized() {
        state = State::Ready;
        gate  = Gate::Closed;
    }
    /// A replacement producer (after FatalError): the old one's transaction is gone, nothing of it visible.
    void replaced() {
        state = State::Uninitialized;
        gate  = Gate::Closed;
        pending.clear();
        stagedOffset.reset();
        offsetsRunning = 0;
    }
    /// The test issues begin only in Ready (the documented contract: "call it after the previous commit / abort /
    /// init completed").
    void begin() {
        if (state != State::Ready) {
            errors.push_back("model: begin issued outside Ready (" + std::string(name(state)) + ")");
            return;
        }
        state = State::InTxn;
        gate  = Gate::Open;
        ++txn;
        pending.clear();
        stagedOffset.reset();
        startedHeard_ = false;
    }

    /// Invoker mode: a data op is accepted only on an open gate with the machine InTransaction; otherwise it is
    /// refused synchronously, with no effect.
    [[nodiscard]] bool accepts() const noexcept { return gate == Gate::Open && state == State::InTxn; }
    // ---- data: an accepted op (returns false if the gate was closed: the caller reports the divergence) ----
    bool data(int32_t partition, const std::vector<std::string>& values, Token token) {
        if (!accepts()) return false;
        for (const auto& v : values) pending[partition].insert(v);
        if (token != 0) add(token, {TokenKind::Data, txn, {kOk}, 0});
        return true;
    }

    // ---- token ops ----
    void commit(Token token) {
        gate = Gate::Closed;
        switch (state) {
        case State::InTxn:
            state    = State::Committing;
            joiners_ = 0;
            add(token, {TokenKind::Commit, txn, {kOk}, 0});
            return;
        case State::Committing:   // joins the running commit: the same outcome (capacity: kMaxJoiners)
            if (joiners_ >= kMaxJoiners) return refuse_(token, TokenKind::Commit, kCapRefused);
            ++joiners_;
            add(token, {TokenKind::Commit, txn, commitAllowed_(), 0});
            return;
        case State::Abortable: return refuse_(token, TokenKind::Commit, poison);
        case State::Fatal:     return refuse_(token, TokenKind::Commit, fatal);
        default:               return refuse_(token, TokenKind::Commit, kInvalidTxnState);
        }
    }
    void abort(Token token) {
        gate = Gate::Closed;
        switch (state) {
        case State::InTxn:
        case State::Abortable:
            state    = State::Aborting;
            joiners_ = 0;
            // An abort drops every record not sent yet: those tokens complete with the abort's outcome (19).
            widenAll_(kInvalidTxnState, /*endOps=*/false);
            add(token, {TokenKind::Abort, txn, {kOk}, 0});
            return;
        case State::Aborting:     // joins (capacity: kMaxJoiners)
            if (joiners_ >= kMaxJoiners) return refuse_(token, TokenKind::Abort, kCapRefused);
            ++joiners_;
            add(token, {TokenKind::Abort, txn, abortAllowed_(), 0});
            return;
        case State::Fatal: return refuse_(token, TokenKind::Abort, fatal);
        default:           return refuse_(token, TokenKind::Abort, kInvalidTxnState);   // Ready / Committing
        }
    }
    void flush(Token token) {
        if (state == State::Fatal) return refuse_(token, TokenKind::Flush, fatal);
        if (flushes_ >= kMaxFlushes) return refuse_(token, TokenKind::Flush, kCapRefused);
        ++flushes_;
        switch (state) {
        case State::Abortable: add(token, {TokenKind::Flush, txn, {poison}, 0}); return;
        case State::Aborting:  add(token, {TokenKind::Flush, txn, {kOk, kInvalidTxnState}, 0}); return;
        default:               add(token, {TokenKind::Flush, txn, {kOk}, 0}); return;
        }
    }
    void sendOffsets(Token token, int64_t offset) {
        switch (state) {
        case State::Fatal:     return refuse_(token, TokenKind::SendOffsets, fatal);
        case State::Abortable: return refuse_(token, TokenKind::SendOffsets, poison);
        case State::InTxn:
            // One at a time: a second one is refused, the transaction is not poisoned.
            if (offsetsRunning != 0) return refuse_(token, TokenKind::SendOffsets, kCapRefused);
            offsetsRunning = token;
            add(token, {TokenKind::SendOffsets, txn, {kOk}, offset});
            return;
        default: return refuse_(token, TokenKind::SendOffsets, kInvalidTxnState);
        }
    }

    // ---- the coordinator's decision on one request (the fake's log) ----
    void decision(Leg leg, int16_t code) {
        if (code == 0 && (leg == Leg::AddPartitions || leg == Leg::AddOffsets || leg == Leg::TxnOffsetCommit)) {
            startedHeard_ = true;   // the transaction exists on the coordinator (an OK leg the client heard)
        }
        if (code == 0 || code == 27 || code == 14) return;   // applied / retried: nothing visible
        if (code == 18 || code == 17) {
            toFatal_(kFenced);
            return;
        }
        if (code == 19) {
            switch (leg) {
            case Leg::AddPartitions:
            case Leg::AddOffsets:
            case Leg::TxnOffsetCommit:
                if (state == State::InTxn || state == State::Committing) toAbortable_(kInvalidTxnState);
                return;
            case Leg::EndTxnCommit:
                toFatal_(kInvalidTxnState);
                return;
            case Leg::EndTxnAbort:
                // Started (an OK leg heard): FatalError(19).  Only possibly started: "nothing open" - the abort is Ok.
                for (auto& [t, o] : outstanding) {
                    if (o.kind == TokenKind::Abort && !o.refused) o.allowed = {startedHeard_ ? kInvalidTxnState : kOk};
                }
                return;
            }
        }
        errors.push_back("model: unscripted coordinator code " + std::to_string(code));
    }

    // ---- #373: the partition leader rejected a batch with a sequence gap (OutOfOrderSequence 20 / UnknownProducerId 28)
    // that was the head of its partition: the transaction can only abort (its abort's epoch bump restarts the
    // sequences).  The caller injects it only where nothing else is in flight, so the batch is the head. ----
    void produceGap(int16_t code) {
        if (state != State::InTxn) {
            errors.push_back("model: a produce gap landed in state " + std::string(name(state)));
            return;
        }
        toAbortable_({Status::BrokerError, static_cast<uint16_t>(code)});
    }

    // ---- a completion; returns what changed for the caller's checks ----
    enum class Ended : uint8_t { None, Committed, Aborted, Failed };
    struct Applied {
        TokenKind kind  = TokenKind::Data;
        Ended     ended = Ended::None;   // a commit / abort completion: the transaction's end (check the RC view)
    };
    Applied complete(Token token, Result r) {
        Applied a;
        const auto it = outstanding.find(token);
        if (it == outstanding.end()) {
            errors.push_back("token " + std::to_string(token) + " completed but was not outstanding (twice, or never issued)");
            return a;
        }
        const Outstanding o = it->second;
        outstanding.erase(it);
        a.kind = o.kind;
        bool ok = false;
        for (const auto& x : o.allowed) ok = ok || x.matches(r);
        if (!ok) errors.push_back("token " + std::to_string(token) + " (" + name(o.kind) + ", txn " + std::to_string(o.txn) +
                                  ") completed " + str(r) + "; allowed " + str(o.allowed) + " (state " + name(state) + ")");
        switch (o.kind) {
        case TokenKind::Data:
            dataDone_[o.txn].push_back({token, r});
            break;
        case TokenKind::SendOffsets:
            if (offsetsRunning == token) offsetsRunning = 0;
            if (r.status == Status::Ok && !o.refused && o.txn == txn && (state == State::InTxn || state == State::Committing)) {
                stagedOffset = o.offset;
            }
            break;
        case TokenKind::Flush:
            if (!o.refused && flushes_ > 0) --flushes_;
            break;
        case TokenKind::Commit:
            if (o.refused || state != State::Committing || o.txn != txn) break;   // a refusal / a joiner of a decided commit
            if (r.status == Status::Ok) {
                for (auto& [p, vs] : pending) committed[p].insert(vs.begin(), vs.end());
                if (stagedOffset) committedOffset = *stagedOffset;
                pending.clear();
                state   = State::Ready;
                a.ended = Ended::Committed;
                checkCommittedTokens_(o.txn);
            } else if (r.status == Status::BrokerError) {   // (already reported if the rules did not allow it)
                state   = State::Abortable;   // the app must abort
                poison  = r;
                a.ended = Ended::Failed;
            } else {
                toFatal_(r);
                a.ended = Ended::Failed;
            }
            break;
        case TokenKind::Abort:
            if (o.refused || state != State::Aborting || o.txn != txn) break;
            if (r.status == Status::Ok) {
                pending.clear();
                stagedOffset.reset();
                offsetsRunning = 0;
                state   = State::Ready;
                a.ended = Ended::Aborted;
            } else {
                toFatal_(r);
                a.ended = Ended::Failed;
            }
            break;
        }
        return a;
    }

    [[nodiscard]] bool idle() const noexcept { return outstanding.empty(); }
    [[nodiscard]] std::size_t outstandingCount() const noexcept { return outstanding.size(); }
    [[nodiscard]] std::string describeOutstanding() const {
        std::ostringstream os;
        for (const auto& [t, o] : outstanding) os << " " << t << "(" << name(o.kind) << ",txn " << o.txn << ")";
        return os.str();
    }

    static std::string str(const Result& r) {
        std::ostringstream os;
        os << "{status " << static_cast<int>(r.status) << ", code " << r.code << "}";
        return os.str();
    }
    static std::string str(const std::vector<Result>& v) {
        std::string s = "[";
        for (const auto& r : v) s += str(r);
        return s + "]";
    }

private:
    struct Outstanding {
        TokenKind           kind;
        uint32_t            txn;
        std::vector<Result> allowed;
        int64_t             offset  = 0;
        bool                refused = false;   // decided (refused at issue / a commit failed by the poison): final
    };
    std::map<Token, Outstanding> outstanding;
    std::map<uint32_t, std::vector<std::pair<Token, Result>>> dataDone_;   // completed record tokens per transaction
    std::set<uint32_t> committedTxns_;
    int joiners_ = 0;   // calls that joined the running commit / abort
    bool startedHeard_ = false;   // an AddPartitions / AddOffsets / TxnOffsetCommit OK was heard in this transaction
    int flushes_ = 0;   // pending flushes

    /// Decided at issue (Invoker: the owner thread applied it): nothing later changes the outcome.
    void refuse_(Token t, TokenKind k, const Result& r) { add(t, {k, txn, {r}, 0, /*refused=*/true}); }

    void add(Token t, Outstanding o) {
        if (outstanding.count(t) != 0) errors.push_back("model: token " + std::to_string(t) + " reused while outstanding");
        // A record token of a transaction that already committed can only be Ok.
        if (o.kind == TokenKind::Data && committedTxns_.count(o.txn) != 0) o.allowed = {kOk};
        outstanding[t] = std::move(o);
    }
    static void widen_(std::vector<Result>& v, const Result& r) {
        for (const auto& x : v) if (x.matches(r)) return;
        v.push_back(r);
    }
    /// A failure reached the transaction: every open token of it may complete with it.  endOps: commit / abort
    /// tokens are decided by it (replaced, not widened).
    void widenAll_(const Result& r, bool endOps) {
        for (auto& [t, o] : outstanding) {
            if (o.refused) continue;
            if (o.kind == TokenKind::Commit || o.kind == TokenKind::Abort) {
                if (endOps) o.allowed = {r};
                continue;
            }
            widen_(o.allowed, r);
        }
    }
    std::vector<Result> commitAllowed_() const {
        for (const auto& [t, o] : outstanding) if (o.kind == TokenKind::Commit && o.txn == txn && !o.refused) return o.allowed;
        return {kOk};
    }
    std::vector<Result> abortAllowed_() const {
        for (const auto& [t, o] : outstanding) if (o.kind == TokenKind::Abort && o.txn == txn && !o.refused) return o.allowed;
        return {kOk};
    }
    void toAbortable_(const Result& r) {
        state  = State::Abortable;
        poison = r;
        widenAll_(r, /*endOps=*/true);
        // A commit waiting for its data completes with the poison AT ONCE: decided, a later fence cannot change it.
        for (auto& [t, o] : outstanding) {
            if (o.kind == TokenKind::Commit && o.txn == txn) o.refused = true;
        }
    }
    void toFatal_(const Result& r) {
        if (state == State::Fatal) return;
        state = State::Fatal;
        fatal = r;
        pending.clear();
        stagedOffset.reset();
        widenAll_(r, /*endOps=*/true);
    }
    void checkCommittedTokens_(uint32_t t) {
        committedTxns_.insert(t);
        for (const auto& [tok, r] : dataDone_[t]) {
            if (r.status != Status::Ok) {
                errors.push_back("txn " + std::to_string(t) + " committed but its record token " + std::to_string(tok) +
                                 " completed " + str(r));
            }
        }
        for (auto& [tok, o] : outstanding) {
            if (o.kind == TokenKind::Data && o.txn == t) o.allowed = {kOk};
        }
    }
};

}  // namespace txnmodel
