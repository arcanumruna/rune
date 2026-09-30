// include/rune/client/transport/Resolution.h
//
// Rich-state resolution of one Aeron registration (spec §1a.4, §3.5a.3, §3.6):
//
//   Unstarted ──poll──▶ Resolving(regId, deadlineNs) ──find()!=null──▶ Ready(handle)
//                             │  now >= deadline                         (sticky)
//                             ▼
//                       TimedOut(retryAtNs) ──now >= retryAt──▶ Resolving(fresh regId, ...)
//   add()/find() throws ─────────────────────────────────────▶ Rejected   (terminal)
//
// The owner pumps ITS OWN driver + conductor between polls (invoker mode) or
// lets the background agents run (thread mode).  This class never sleeps, never
// blocks and never reads a clock: nowNs is injected.
#pragma once

#include <cstdint>
#include <memory>
#include <variant>

namespace rune::client::transport {

enum class ResolveStatus : uint8_t { Pending, Ready, Failed };

template <class Handle>
class Resolution {
public:
    struct Unstarted {};
    struct Resolving { int64_t regId; int64_t deadlineNs; };
    struct Ready     { std::shared_ptr<Handle> handle; };
    struct TimedOut  { int64_t retryAtNs; };
    struct Rejected  {};
    using State = std::variant<Unstarted, Resolving, Ready, TimedOut, Rejected>;

    Resolution(int64_t timeoutNs, int64_t retryBackoffNs) noexcept
        : timeoutNs_(timeoutNs), retryBackoffNs_(retryBackoffNs) {}

    template <class Registry>
    ResolveStatus poll(Registry& registry, int64_t nowNs) noexcept {
        if (std::holds_alternative<Ready>(state_)) return ResolveStatus::Ready;
        if (std::holds_alternative<Unstarted>(state_)) return start(registry, nowNs);
        if (const auto* t = std::get_if<TimedOut>(&state_)) {
            return (nowNs >= t->retryAtNs) ? start(registry, nowNs) : ResolveStatus::Failed;
        }
        if (std::holds_alternative<Rejected>(state_)) return ResolveStatus::Failed;

        const Resolving r = std::get<Resolving>(state_);
        std::shared_ptr<Handle> h;
        try {
            h = registry.find(r.regId);
        } catch (...) {
            state_ = Rejected{};
            return ResolveStatus::Failed;
        }
        if (h) {
            state_ = Ready{std::move(h)};
            return ResolveStatus::Ready;
        }
        if (nowNs >= r.deadlineNs) {
            state_ = TimedOut{nowNs + retryBackoffNs_};
            return ResolveStatus::Failed;
        }
        return ResolveStatus::Pending;
    }

    /// Raw pointer to the resolved handle (hot path), nullptr unless Ready.
    [[nodiscard]] Handle* ready() const noexcept {
        const Ready* r = std::get_if<Ready>(&state_);
        return r ? r->handle.get() : nullptr;
    }
    [[nodiscard]] const std::shared_ptr<Handle>* readyShared() const noexcept {
        const Ready* r = std::get_if<Ready>(&state_);
        return r ? &r->handle : nullptr;
    }
    [[nodiscard]] bool rejected() const noexcept {
        return std::holds_alternative<Rejected>(state_);
    }
    [[nodiscard]] const State& state() const noexcept { return state_; }

    /// Drops the handle on the owning thread and returns to Unstarted.
    void reset() noexcept { state_ = Unstarted{}; }

private:
    template <class Registry>
    ResolveStatus start(Registry& registry, int64_t nowNs) noexcept {
        try {
            state_ = Resolving{registry.add(), nowNs + timeoutNs_};
        } catch (...) {
            state_ = Rejected{};
            return ResolveStatus::Failed;
        }
        return ResolveStatus::Pending;
    }

    int64_t timeoutNs_;
    int64_t retryBackoffNs_;
    State   state_{Unstarted{}};
};

} // namespace rune::client::transport
