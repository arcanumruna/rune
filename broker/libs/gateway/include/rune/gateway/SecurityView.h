#pragma once
// SecurityView.h — the CONTROL thread's mirror of `__security` (authentication plan D29).
//
// The admin thread's SecurityApplier is authoritative (verify, checkpoint, halt).  Every event it applies is mirrored,
// as the record's bytes, to the control thread (kA2cSecurityApplied = [offset:i64][bytes]); this view applies them in
// offset order with the SAME library (rune_security) to its OWN SecurityState — never a pointer read across threads.
// At boot the startup thread hands over the booted state (a copy) before any thread runs.
//
// The gateway's SCRAM server, permission-table compiler and live-session refresh read state() — all on the control
// thread.  A gap or an apply error is an Internal halt of the mirror (the D28 fail-closed path: Task 14 revokes every
// authenticated session and refuses logins); a halt reported by the admin applier (onHalted) is the same state.
//
// Per-state data (hard rule 3): std::variant<Mirroring, Halted>.  Cold path (control plane).

#include <rune/security/SecurityState.h>

#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <variant>

namespace rune::gateway {

class SecurityView {
public:
    struct Mirroring {};
    struct Halted { rune::security::ApplyError error; };

    /// `booted` = the state the startup thread replayed from the local log (appliedOffset() = the last event in it,
    /// -1 = none).  The next event this view accepts is booted.appliedOffset() + 1.
    explicit SecurityView(rune::security::SecurityState booted) : state_(std::move(booted)) {}

    /// Apply the admin applier's event at `offset` (its record bytes).  An error (a gap, an undecodable record, an
    /// apply refusal, a fingerprint mismatch) HALTS the mirror and is returned; nothing of that event is applied.
    /// Once halted every call returns the original halt error and applies nothing.
    [[nodiscard]] std::expected<rune::security::Applied, rune::security::ApplyError>
    onApplied(int64_t offset, std::span<const uint8_t> bytes);

    /// The admin applier halted (kA2cSecurityHalted): the mirror stops too (fail closed).
    void onHalted(const rune::security::ApplyError& error);

    [[nodiscard]] const rune::security::SecurityState& state() const noexcept { return state_; }
    /// The last event applied to this view (-1 = none).
    [[nodiscard]] int64_t appliedOffset() const noexcept { return state_.appliedOffset(); }
    [[nodiscard]] bool halted() const noexcept { return std::holds_alternative<Halted>(st_); }
    [[nodiscard]] const rune::security::ApplyError* haltError() const noexcept {
        const Halted* h = std::get_if<Halted>(&st_);
        return h ? &h->error : nullptr;
    }
    [[nodiscard]] std::string_view stateName() const noexcept { return halted() ? "Halted" : "Mirroring"; }

private:
    [[nodiscard]] rune::security::ApplyError halt_(rune::security::ApplyError error);

    rune::security::SecurityState   state_;
    std::variant<Mirroring, Halted> st_;
};

} // namespace rune::gateway
