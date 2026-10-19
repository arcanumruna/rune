// src/catalog/CatalogReader.cpp — the catalog instantiation of the shared event-log reader (see
// include/rune/client/catalog/CatalogReader.h and src/detail/EventLogStreamImpl.h).
#include <rune/client/catalog/CatalogReader.h>

#include "../detail/EventLogStreamImpl.h"

namespace rune::client::detail {
template class EventLogStream<CatalogLogPolicy>;
}
