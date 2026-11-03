#pragma once
// Test-only construction options of rune::client::Client.  They are not part of the public ClientConfig: a config
// field is API surface for every user, a test seam is not (#756).
#include <string>

namespace rune::client::detail {

struct ClientTestOptions {
    /// NAT egress test seam: when non-empty, the ConnectRequest DECLARES this egress channel while the transport still
    /// subscribes on ClientConfig::egressChannel (models a client behind NAT whose declared address is unreachable).
    /// Empty (default) = declare ClientConfig::egressChannel.
    std::string declaredEgressChannel;
};

}  // namespace rune::client::detail
