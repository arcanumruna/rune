#include <rune/node/ShardAgent.h>

#include <rune_fr/FlightRecorder.h>

namespace rune::node {

int ShardAgent::doWork(int64_t nowNs) {
    RUNE_FR_SPAN_RUNTIME(rune_fr::Site::ShardPass, 0);
    if (!bound_) [[unlikely]] {
        driver_.bindPumpThread();          // ownership hand-off: startup thread → this thread
        bound_ = true;
    }
    // Loop-duration alarm from the injected clock only (sampled once per pass by the
    // runner): under busy-spin the gap between passes IS the previous pass's cost.
    if (lastNowNs_ != 0 && nowNs - lastNowNs_ > kSlowPassNs) ++slowPasses_;
    lastNowNs_ = nowNs;

    int work = driver_.doWork();                        // 1. own driver duty cycle
    work += client_.conductorAgentInvoker().invoke();   // 2. own client conductor
    work += worker_.doWork(nowNs);                      // 3-6. Worker (final → direct call)
    return work;
}

} // namespace rune::node
