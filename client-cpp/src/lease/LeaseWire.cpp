// src/lease/LeaseWire.cpp - the blocking (*AndWait) forms of the lease handle (Task 14a).  Cold path: tools and tests.
#include <rune/client/lease/LeaseTopic.h>

#include <algorithm>

namespace rune::client::detail {

template <class Issue>
LeaseResult LeaseTopicCore::waitOne_(std::chrono::nanoseconds timeout, Issue&& issue, LeaseTtlInfo* info) {
    CompletionsConfig qc;
    qc.maxResultBytes = 1u << 12;
    qc.capacityBytes  = 1u << 16;
    Completions q(*client_, qc);
    constexpr Token kToken = 1;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {   // the ring may be full: let the duty side drain, within the caller's budget
        const OpResult r = issue(q, kToken);
        if (r == OpResult::Accepted) break;
        if (r == OpResult::BackPressured || r == OpResult::QueueFull) {
            if (std::chrono::steady_clock::now() >= deadline) return LeaseResult{OpStatus::Timeout, 0, 0, 0};
            if (client_->invokerMode()) (void)client_->doWork();
            continue;
        }
        return LeaseResult{r == OpResult::Closed ? OpStatus::Closed : OpStatus::InvalidArgument, 0, 0, 0};
    }
    LeaseResult out{OpStatus::Timeout, 0, 0, 0};
    const auto  left = std::max(std::chrono::nanoseconds(0), deadline - std::chrono::steady_clock::now());
    auto done = [&](const Completion& c, std::span<const uint8_t> payload) {
        out = leaseResultOf(c);
        if (info != nullptr && c.status == OpStatus::Ok && c.op == OpKind::LeaseTimeToLive) {
            if (const auto i = LeaseTtlInfo::decode(payload)) *info = *i;
        }
    };
    if (client_->invokerMode()) (void)q.waitFor(kToken, left, done, [this] { return client_->doWork(); });
    else (void)q.waitFor(kToken, left, done);
    return out;
}

LeaseResult LeaseTopicCore::writeAndWait(OpKind kind, std::span<const uint8_t> key, std::size_t rowBytes, int64_t version,
                                         const LeaseOptions& opts, std::chrono::nanoseconds timeout) {
    return waitOne_(timeout, [&](Completions& q, Token t) { return write(kind, key, rowBytes, version, opts, q, t); }, nullptr);
}

LeaseResult LeaseTopicCore::keepAliveAndWait(std::span<const uint8_t> key, int64_t version, const LeaseOptions& opts,
                                             std::chrono::nanoseconds timeout) {
    return waitOne_(timeout, [&](Completions& q, Token t) { return keepAliveOnce(key, version, opts, q, t); }, nullptr);
}

LeaseResult LeaseTopicCore::timeToLiveAndWait(std::span<const uint8_t> key, const LeaseOptions& opts,
                                              std::chrono::nanoseconds timeout, LeaseTtlInfo* info) {
    return waitOne_(timeout, [&](Completions& q, Token t) { return timeToLive(key, opts, q, t); }, info);
}

}  // namespace rune::client::detail
