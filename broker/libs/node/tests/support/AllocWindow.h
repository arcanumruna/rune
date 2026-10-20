#pragma once
// AllocWindow — counts the heap allocations (global operator new) made on the CALLING thread while it is alive.
// Test support for the zero-allocation gates (Phase 2c).  Link AllocWindow.cpp into the test binary: it replaces
// the global operator new / delete for that binary.  Allocations by other threads are not counted, and a window
// does not nest.
//
//     warmUp();
//     rune::testing::AllocWindow w;
//     hotPath();
//     EXPECT_EQ(w.count(), 0u);
#include <cstdint>

namespace rune::testing {

class AllocWindow {
public:
    AllocWindow() noexcept;
    ~AllocWindow();
    AllocWindow(const AllocWindow&)            = delete;
    AllocWindow& operator=(const AllocWindow&) = delete;
    /// Allocations made by the creating thread since the window opened.
    [[nodiscard]] uint64_t count() const noexcept;
};

}  // namespace rune::testing
