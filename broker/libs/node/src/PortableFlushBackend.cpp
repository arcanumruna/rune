// PortableFlushBackend.cpp — E3: the production instantiation of the flusher
// (BasicPortableFlushBackend<&detail::platformFdatasync>).  The implementation
// lives in PortableFlushBackend.h (template: tests instantiate it with a
// recording Sync — a compile-time seam, F9a fix round 1).

#include <rune/node/PortableFlushBackend.h>

namespace rune::node {

template class BasicPortableFlushBackend<&detail::platformFdatasync>;

} // namespace rune::node
