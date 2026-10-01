// tests/support/TestCompletions.h — F8c C3 test helper: a future-like view of
// control ops issued on a Completions queue owned by the TEST thread.
//
// The test thread issues ops (TestCq::issue), and then either pumps (Invoker:
// client.doWork() + gateway pump) or polls (Thread mode: waitReady, an idle-
// strategy poll with a wall-clock BACKSTOP only — the runner makes progress
// by itself, the deadline never decides an outcome) until the op's single
// completion arrives.  Every completion is recorded per token with a count, so
// tests can assert exactly-once.  No futures, no condvars, no sleeps.
#pragma once

#include <rune/client/Client.h>
#include <rune/client/Completions.h>

#include <concurrent/BackOffIdleStrategy.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace test_support {

struct OpOutcome {
    rune::client::Completion c{};
    std::vector<uint8_t>     payload;
};

class TestCq;

/// One op issued on a TestCq (test thread only).
class PendingOp {
public:
    PendingOp() = default;
    PendingOp(TestCq* cq, rune::client::Token t, rune::client::OpResult r) : cq_(cq), t_(t), issued_(r) {}

    [[nodiscard]] rune::client::OpResult issued() const noexcept { return issued_; }
    [[nodiscard]] bool accepted() const noexcept { return issued_ == rune::client::OpResult::Accepted; }
    [[nodiscard]] rune::client::Token token() const noexcept { return t_; }

    /// Polls the queue (never blocks): has the op completed?
    [[nodiscard]] bool ready() const;
    /// Thread mode: idle-poll until the op completes; `backstop` is a hang guard only.
    [[nodiscard]] bool waitReady(std::chrono::nanoseconds backstop = std::chrono::seconds(30)) const;

    [[nodiscard]] const OpOutcome& outcome() const;
    [[nodiscard]] rune::client::OpStatus status() const { return outcome().c.status; }
    /// Completed Ok (polls first).
    [[nodiscard]] bool ok() const { return ready() && status() == rune::client::OpStatus::Ok; }
    /// Completed with anything but Ok (polls first).
    [[nodiscard]] bool failed() const { return ready() && status() != rune::client::OpStatus::Ok; }
    [[nodiscard]] std::vector<uint8_t> bytes() const { return outcome().payload; }
    [[nodiscard]] std::string describe() const;

    // ---- future-like accessors (throw std::runtime_error like the old get()) --
    /// Throws unless completed Ok.
    void check() const {
        if (!ok()) throw std::runtime_error("op did not complete Ok: " + describe());
    }
    /// The echo / payload of an Ok completion.
    [[nodiscard]] std::vector<uint8_t> echo() const {
        check();
        return bytes();
    }
    /// DefineTopic: Ok or a broker errorCode -> the result (errorCode inside);
    /// anything else (Timeout, NotConnected, Closed, ...) throws.
    [[nodiscard]] rune::client::DefineTopicResult defineResult() const {
        if (!ready() || (status() != rune::client::OpStatus::Ok &&
                         status() != rune::client::OpStatus::BrokerError)) {
            throw std::runtime_error("defineTopic did not complete: " + describe());
        }
        rune::client::DefineTopicResult r;
        r.topicId   = static_cast<int32_t>(outcome().c.value);
        r.version   = static_cast<uint16_t>(outcome().c.aux);
        r.errorCode = outcome().c.errorCode;
        return r;
    }
    /// GetTopicSchema: as defineResult(), with the descriptor blob.
    [[nodiscard]] rune::client::TopicSchemaResult schemaResult() const {
        if (!ready() || (status() != rune::client::OpStatus::Ok &&
                         status() != rune::client::OpStatus::BrokerError)) {
            throw std::runtime_error("getTopicSchema did not complete: " + describe());
        }
        rune::client::TopicSchemaResult r;
        r.topicId   = static_cast<int32_t>(outcome().c.value);
        r.version   = static_cast<uint16_t>(outcome().c.aux);
        r.errorCode = outcome().c.errorCode;
        if (status() == rune::client::OpStatus::Ok) r.descriptorBlob = outcome().payload;
        return r;
    }

private:
    TestCq*                cq_ = nullptr;
    rune::client::Token    t_  = 0;
    rune::client::OpResult issued_ = rune::client::OpResult::InvalidArgument;
};

class TestCq {
public:
    explicit TestCq(rune::client::Client& c, rune::client::CompletionsConfig cfg = {}) : q_(c, cfg) {}

    /// f(Completions&, Token) -> OpResult issues the op.
    template <class Issue>
    PendingOp issue(Issue&& f) {
        const rune::client::Token t = ++next_;
        return PendingOp(this, t, f(q_, t));
    }

    int drain() {
        return q_.poll([&](const rune::client::Completion& c, std::span<const uint8_t> p) {
            out_[c.token] = OpOutcome{c, {p.begin(), p.end()}};
            ++count_[c.token];
        });
    }
    [[nodiscard]] bool ready(rune::client::Token t) {
        drain();
        return out_.count(t) != 0;
    }
    [[nodiscard]] const OpOutcome& get(rune::client::Token t) const { return out_.at(t); }
    /// How many completions arrived for `t` (exactly-once checks).
    [[nodiscard]] int timesCompleted(rune::client::Token t) {
        drain();
        const auto it = count_.find(t);
        return it == count_.end() ? 0 : it->second;
    }
    [[nodiscard]] std::size_t completedCount() {
        drain();
        return out_.size();
    }
    rune::client::Completions& q() noexcept { return q_; }

private:
    rune::client::Completions               q_;
    rune::client::Token                     next_ = 0;
    std::map<rune::client::Token, OpOutcome> out_;
    std::map<rune::client::Token, int>       count_;
};

inline bool PendingOp::ready() const { return cq_ != nullptr && accepted() && cq_->ready(t_); }

inline bool PendingOp::waitReady(std::chrono::nanoseconds backstop) const {
    aeron::concurrent::BackoffIdleStrategy idle(
        10, 5, std::chrono::duration<long, std::nano>(1'000), std::chrono::milliseconds(1));
    const auto deadline = std::chrono::steady_clock::now() + backstop;
    while (!ready()) {
        if (!accepted() || std::chrono::steady_clock::now() >= deadline) return false;
        idle.idle(0);
    }
    return true;
}

inline const OpOutcome& PendingOp::outcome() const { return cq_->get(t_); }

inline std::string PendingOp::describe() const {
    if (!accepted()) return "not accepted (OpResult " + std::to_string(static_cast<int>(issued_)) + ")";
    if (!ready()) return "pending";
    const auto& o = outcome();
    return "status " + std::to_string(static_cast<int>(o.c.status)) + " errorCode " +
           std::to_string(o.c.errorCode) + " payload '" +
           std::string(reinterpret_cast<const char*>(o.payload.data()), o.payload.size()) + "'";
}

} // namespace test_support
