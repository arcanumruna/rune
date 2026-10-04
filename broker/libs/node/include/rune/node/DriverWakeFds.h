/* DriverWakeFds.h — A7: the readiness fds of an INVOKER media driver (park-until-work).
 *
 * An invoker driver is pumped by its owner's duty cycle, so the owner can only learn that a datagram arrived by
 * polling every transport (a recvmmsg per socket, every pass).  Aeron's C driver already keeps an epoll instance
 * per transport poller (receiver data endpoints, sender control endpoints) with every socket registered
 * (media/aeron_udp_transport_poller.c, HAVE_EPOLL).  An epoll fd is itself pollable: it is readable while any
 * registered socket is.  Parking in ppoll() on those fds therefore wakes on the next datagram instead of
 * after a fixed sleep — the owner idles for real (CPU idle, not sched_yield) without adding wake latency.
 *
 * Linux only (other platforms: 0 fds -> the caller keeps its timed park).  Every returned fd is verified to be
 * an epoll instance (/proc/self/fd/N -> anon_inode:[eventpoll]); anything else is skipped.  The fds are owned
 * by the driver: valid until aeron_driver_close.  Implementation: src/DriverWakeFds.c (a C TU — Aeron's
 * internal driver headers are C). */
#ifndef RUNE_NODE_DRIVER_WAKE_FDS_H
#define RUNE_NODE_DRIVER_WAKE_FDS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct aeron_driver_stct aeron_driver_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;

/* Writes at most `max` fds to `out`; returns how many (0 on non-Linux, NULL driver or no epoll pollers). */
int rune_driver_wake_fds(aeron_driver_t* driver, int* out, int max);

/* 1 when `fd` is an epoll instance of this process, else 0 (always 0 on non-Linux). */
int rune_fd_is_epoll(int fd);

/* A7 epoll-first transport polling (findings §4.12).  Aeron's poller RECVMMSGs EVERY transport each duty cycle
 * while a poller holds <= 5 of them (AERON_UDP_TRANSPORT_POLLER_ITERATION_THRESHOLD) and only switches to one
 * epoll_wait beyond that.  A Rune shard driver sits at 2-5 transports per poller, so an invoker pass paid ~5
 * recvmmsg per duty cycle, mostly empty (measured: ~175k recvmmsg/s per worker at 20k rec/s; recvmmsg + syscall
 * entry = the top profile entries).  These bindings poll every poller with > 1 transport through the poller's
 * own epoll instance (one epoll_wait(0), then recvmmsg only the ready sockets) — Aeron's own > threshold path,
 * byte-identical in what it delivers.  Installed through Aeron's transport-bindings extension point
 * (poller_poll_func), so nothing else in the driver changes.
 *
 * Call AFTER any other bindings were installed on `ctx` and BEFORE aeron_driver_init.  `ctx_bindings_owned` != 0:
 * the context's bindings struct belongs to the caller (rune_gso) and is patched in place; 0: the context holds
 * Aeron's shared stock bindings, which are copied into *copy_out (free it with rune_driver_epoll_poll_free AFTER
 * aeron_driver_context_close).  Returns 1 installed, 0 not (non-Linux / no epoll / no memory: stock polling). */
int rune_driver_epoll_poll_install(aeron_driver_context_t* ctx, int ctx_bindings_owned, void** copy_out);
void rune_driver_epoll_poll_free(void* copy);
/* 1 when `ctx`'s bindings poll through rune's epoll-first poller (tests / startup log). */
int rune_driver_epoll_poll_active(aeron_driver_context_t* ctx);

#ifdef __cplusplus
}
#endif

#endif /* RUNE_NODE_DRIVER_WAKE_FDS_H */
