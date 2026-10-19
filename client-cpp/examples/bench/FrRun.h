// examples/bench/FrRun.h — flight-recorder lifecycle for the bench / gate binaries.
//
// Construct FIRST in main (before any client / shard thread records):
// flight_recorder::configureFromEnv() sizes the rings and arms recording when
// FLIGHT_RECORDER_ENABLE is set (compiled in, OFF by default; see flight_recorder/Report.h for
// FLIGHT_RECORDER_RING_BYTES / _SLOW_SPAN_NS / _THROTTLE_NS / _DRAIN_MS / _CATEGORIES).
// On destruction — every exit path of main, including a caught error — the
// report (hist + Chrome trace + tape + meta with the cross-process anchor) is
// written to FLIGHT_RECORDER_OUT (default cwd) as fr-*-client-p<pid>-<tag>.*.
#pragma once

#include <flight_recorder/FlightRecorder.h>

#include <cstdio>
#include <cstdlib>
#include <string>
#include <unistd.h>

namespace rune::bench {

class FrRun {
public:
    explicit FrRun(std::string tag) : tag_(std::move(tag)), on_(flight_recorder::configureFromEnv()) {
        if (on_) std::fprintf(stderr, "flight-recorder: client flight recording ENABLED (tag=%s)\n", tag_.c_str());
    }
    ~FrRun() { dump(); }
    FrRun(const FrRun&)            = delete;
    FrRun& operator=(const FrRun&) = delete;

    /// Writes the report once (idempotent); a no-op when recording is off.
    void dump() noexcept {
        if (!on_ || dumped_) return;
        dumped_ = true;
        try {
            const char*       out = std::getenv("FLIGHT_RECORDER_OUT");
            const std::string dir = (out != nullptr && *out != '\0') ? out : ".";
            const std::string sfx = "-client-p" + std::to_string(static_cast<long>(::getpid())) + "-" + tag_;
            const bool ok = flight_recorder::dumpAll(dir, sfx, "client", tag_);
            std::fprintf(stderr, "flight-recorder: client report %s %s (suffix=%s, dropped=%llu)\n",
                         ok ? "written to" : "FAILED for", dir.c_str(), sfx.c_str(),
                         static_cast<unsigned long long>(flight_recorder::droppedRecords()));
        } catch (...) {
            std::fprintf(stderr, "flight-recorder: client report FAILED (exception)\n");
        }
    }

private:
    std::string tag_;
    bool        on_     = false;
    bool        dumped_ = false;
};

} // namespace rune::bench
