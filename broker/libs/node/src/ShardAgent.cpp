#include <rune/node/ShardAgent.h>
#include <rune/node/NodeRuntimeAgent.h>

namespace rune::node {

namespace detail {
int fusedRuntimePass(NodeRuntimeAgent& runtime, EmbeddedDriver& driver, aeron::Aeron& client,
                     int64_t nowNs) noexcept {
    int work = driver.doWork();                          // runtime's own invoker driver
    work += client.conductorAgentInvoker().invoke();     // runtime client conductor
    work += runtime.doWork(nowNs);                       // final class → direct call
    return work;
}
} // namespace detail

template class BasicShardAgent<rune::consensus::MonotonicNanoClock>;

} // namespace rune::node
