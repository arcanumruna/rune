#pragma once
// IoBackend.h — E3: compile-time I/O backend seam for async durability.
//
// Includes PortableFlushBackend.h (which defines the shared types
// FlushRequest, FlushCompletion, CompletionToken, kNoToken and the
// PortableFlushBackend class), then selects the concrete backend alias
// (IoBackend) at compile time:
//
//   - Linux:      UringFlushBackend  (raw io_uring SQPOLL, zero-syscall submit)
//   - All other:  PortableFlushBackend (dedicated std::thread + SPSC rings)
//
// DESIGN:
//   No vtable on the hot path ([[rune-no-virtual-calls-hot-path]]). The
//   platform choice is resolved once (compile-time alias), not per-submit.
//   submit()/reap() are non-virtual inline calls on the selected concrete type.
//
// Usage:
//   #include <rune/node/IoBackend.h>
//   rune::node::IoBackend io(submitDepth, completionDepth);
//   CompletionToken tok = io.submit(fd, boundary);
//   if (tok == kNoToken) { /* back-pressure: hold produce, retry next cycle */ }
//   io.reap([&](const FlushCompletion& c){ /* fire deferred ack */ });

// PortableFlushBackend.h also defines the shared I/O types.
#include <rune/node/PortableFlushBackend.h>

#if defined(__linux__)
#include <rune/node/UringFlushBackend.h>
namespace rune::node { using IoBackend = UringFlushBackend; }
#else
namespace rune::node { using IoBackend = PortableFlushBackend; }
#endif
