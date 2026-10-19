// src/security/SecurityReader.cpp — the security instantiation of the shared event-log reader.
#include <rune/client/security/SecurityReader.h>

#include "../detail/EventLogStreamImpl.h"

namespace rune::client::detail {
template class EventLogStream<SecurityLogPolicy>;
}
