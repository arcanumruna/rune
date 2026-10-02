// examples/bench/FrRun.h — rune-fr lifecycle for the bench / gate binaries.
//
// Construct FIRST in main (before any client / shard thread records):
// rune_fr::configureFromEnv() sizes the rings and arms recording when
// RUNE_FR_ENABLE is set (compiled in, OFF by default; see rune_fr/Report.h for
// RUNE_FR_RING_BYTES / _SLOW_SPAN_NS / _THROTTLE_NS / _DRAIN_MS / _CATEGORIES).
// On destruction — every exit path of main, including a caught error — the
// report (hist + Chrome trace + tape + meta with the cross-process anchor) is
// written to RUNE_FR_OUT (default cwd) as fr-*-client-p<pid>-<tag>.*.
#pragma once

#include <rune_fr/FlightRecorder.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>

namespace rune::bench {

class FrRun {
public:
    explicit FrRun(std::string tag) : tag_(std::move(tag)), on_(rune_fr::configureFromEnv()) {
        if (on_) std::fprintf(stderr, "rune-fr: client flight recording ENABLED (tag=%s)\n", tag_.c_str());
    }
    ~FrRun() { dump(); }
    FrRun(const FrRun&)            = delete;
    FrRun& operator=(const FrRun&) = delete;

    /// Writes the report once (idempotent); a no-op when recording is off.
    void dump() noexcept {
        if (!on_ || dumped_) return;
        dumped_ = true;
        try {
            const char*       out = std::getenv("RUNE_FR_OUT");
            const std::string dir = (out != nullptr && *out != '\0') ? out : ".";
            const std::string sfx = "-client-p" + std::to_string(static_cast<long>(::getpid())) + "-" + tag_;
            const bool ok = rune_fr::dumpAll(dir, sfx, "client", tag_);
            std::fprintf(stderr, "rune-fr: client report %s %s (suffix=%s, dropped=%llu)\n",
                         ok ? "written to" : "FAILED for", dir.c_str(), sfx.c_str(),
                         static_cast<unsigned long long>(rune_fr::droppedRecords()));
        } catch (...) {
            std::fprintf(stderr, "rune-fr: client report FAILED (exception)\n");
        }
    }

private:
    std::string tag_;
    bool        on_     = false;
    bool        dumped_ = false;
};

} // namespace rune::bench
