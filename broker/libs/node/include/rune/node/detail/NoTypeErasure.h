#pragma once
// NoTypeErasure.h — compile-time guard for hot-path visitors (Task 11 fix 3).
// Capture a NoTypeErasure by value in a lambda and the lambda becomes MOVE-ONLY:
// it can no longer be converted to std::function (which requires copyable
// targets), so a hot path that regresses to a type-erased API stops compiling.
namespace rune::node::detail {

struct NoTypeErasure {
    NoTypeErasure() = default;
    NoTypeErasure(const NoTypeErasure&) = delete;
    NoTypeErasure& operator=(const NoTypeErasure&) = delete;
    NoTypeErasure(NoTypeErasure&&) = default;
    NoTypeErasure& operator=(NoTypeErasure&&) = default;
};

} // namespace rune::node::detail
