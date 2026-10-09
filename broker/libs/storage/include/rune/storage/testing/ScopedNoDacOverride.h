#pragma once
// ScopedNoDacOverride.h — test-only: make permission-based fault injection
// (chmod a directory read-only / unreadable) real for a ROOT test process.
//
// Root bypasses file permission checks through CAP_DAC_OVERRIDE /
// CAP_DAC_READ_SEARCH / CAP_FOWNER, so a chmod-injected EACCES never happens
// when the suite runs as root (the Docker builder, the box ctest) and the
// injection silently tests nothing.  This drops those capabilities from the
// calling thread's EFFECTIVE set for the scope and restores them after; they
// stay in the permitted set, so restoring needs no privilege.  A non-root
// process has none of them effective: construction and destruction are no-ops.
// Linux only; elsewhere a no-op (non-root dev hosts honour permissions anyway).
#if defined(__linux__)
#include <linux/capability.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

namespace rune::storage::testing {

class ScopedNoDacOverride {
public:
    ScopedNoDacOverride() noexcept {
#if defined(__linux__)
        hdr_.version = _LINUX_CAPABILITY_VERSION_3;
        hdr_.pid     = 0;
        if (::syscall(SYS_capget, &hdr_, saved_) != 0) return;
        __user_cap_data_struct dropped[2] = {saved_[0], saved_[1]};
        for (int cap : {CAP_DAC_OVERRIDE, CAP_DAC_READ_SEARCH, CAP_FOWNER})
            dropped[cap / 32].effective &= ~(1u << (cap % 32));
        active_ = ::syscall(SYS_capset, &hdr_, dropped) == 0;
#endif
    }
    ~ScopedNoDacOverride() {
#if defined(__linux__)
        if (active_) (void)::syscall(SYS_capset, &hdr_, saved_);
#endif
    }
    ScopedNoDacOverride(const ScopedNoDacOverride&)            = delete;
    ScopedNoDacOverride& operator=(const ScopedNoDacOverride&) = delete;

private:
#if defined(__linux__)
    __user_cap_header_struct hdr_{};
    __user_cap_data_struct   saved_[2]{};
    bool                     active_ = false;
#endif
};

} // namespace rune::storage::testing
