// Issue #178: ClientAgent is not a SessionIo.  The only session send path is SessionEndpointIo::sendFrame, which
// seals per destination; an agent that were itself a SessionIo would expose an unsealed transport path.
#include <gtest/gtest.h>

#include <type_traits>

#include "rune/client/agent/ClientAgent.h"

namespace {
using rune::client::agent::ClientAgent;
using rune::client::agent::SessionEndpointIo;
using rune::client::session::SessionIo;

static_assert(!std::is_base_of_v<SessionIo, ClientAgent>, "ClientAgent must not be a SessionIo (issue #178)");
static_assert(std::is_base_of_v<SessionIo, SessionEndpointIo>);

TEST(SessionIoSealRegression, ClientAgentIsNotASessionIoRegression) {
    EXPECT_FALSE((std::is_base_of_v<SessionIo, ClientAgent>));
    EXPECT_TRUE((std::is_base_of_v<SessionIo, SessionEndpointIo>));
}
}  // namespace
