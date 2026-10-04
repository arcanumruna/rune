/* rune_driver_wake.h — A7 (rune-broker findings §4.12): park-until-work readiness fds of an INVOKER Aeron media
 * driver, and epoll-first transport polling.  Shared by rune-broker and rune-client-cpp (one copy, like
 * rune_gso_bindings).  Implementation: src/net/rune_driver_wake.c (a C TU: Aeron's internal driver headers are C).
 *
 * Wake fds.  An invoker driver is pumped by its owner's duty cycle, so the owner only learns that a datagram
 * arrived by polling.  Aeron's C driver keeps an epoll instance per transport poller (receiver data endpoints,
 * sender control endpoints) with every socket registered (media/aeron_udp_transport_poller.c, HAVE_EPOLL).  An
 * epoll fd is itself pollable: readable while any registered socket is ready.  Parking in ppoll() on those fds
 * wakes on the next datagram instead of after a fixed sleep.
 *
 * Epoll-first polling.  Aeron's poller recvmmsg-s EVERY transport each duty cycle while a poller holds <= 5 of
 * them and only switches to one epoll_wait beyond that.  These bindings poll every poller with > 1 transport
 * through its own epoll instance: one epoll_wait(0), then recvmmsg only the ready sockets.  A socket that epoll
 * reports with EPOLLERR (a connected UDP socket whose peer port answered ICMP unreachable: a peer down, restarting
 * or not started yet) has its pending error consumed with getsockopt(SO_ERROR) in the same pass (ECONNREFUSED is
 * benign, as in Aeron's own recvmmsg/sendmmsg), so no socket is ever left ready after a pass — a parked owner is
 * never woken by a readiness the pass does not consume (review C1).
 *
 * Wake fds are only handed out when the driver polls through these bindings (rune_driver_epoll_poll_active):
 * Aeron's stock poller above 5 transports checks EPOLLIN only and would leave an EPOLLERR socket ready forever.
 *
 * Linux only.  Elsewhere: 0 fds, install returns 0 (stock polling). */
#ifndef RUNE_PROTOCOL_NET_RUNE_DRIVER_WAKE_H
#define RUNE_PROTOCOL_NET_RUNE_DRIVER_WAKE_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct aeron_driver_stct aeron_driver_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;

/* The driver's socket-readiness fds (receiver / sender transport pollers' epoll instances) into `out` (at most
 * `max`); returns how many.  0 on non-Linux, for a NULL driver, or when the driver does not poll epoll-first.
 * Every fd is verified to be an epoll instance.  Owned by the driver: valid until aeron_driver_close. */
int rune_driver_wake_fds(aeron_driver_t *driver, int *out, int max);

/* 1 when `fd` is an epoll instance of this process, else 0 (always 0 on non-Linux). */
int rune_fd_is_epoll(int fd);

/* Install epoll-first polling.  Call AFTER any other bindings were installed on `ctx` and BEFORE
 * aeron_driver_init.  ctx_bindings_owned != 0: the context's bindings struct belongs to the caller (rune_gso) and
 * is patched in place; 0: the context holds Aeron's shared stock bindings, copied into *copy_out (free with
 * rune_driver_epoll_poll_free AFTER aeron_driver_context_close).  Returns 1 installed, 0 not. */
int rune_driver_epoll_poll_install(aeron_driver_context_t *ctx, int ctx_bindings_owned, void **copy_out);
void rune_driver_epoll_poll_free(void *copy);
/* 1 when `ctx`'s bindings poll through the epoll-first poller. */
int rune_driver_epoll_poll_active(aeron_driver_context_t *ctx);

/* TEST SEAM (pump thread only): how many epoll_event slots of the driver's two transport pollers hold a socket
 * that an epoll_wait SELECTED since the last reset (Aeron's stock <= 5 path never calls epoll_wait, so 0 proves
 * the stock path ran).  reset != 0 clears the record after counting.  Zero cost on the poll path. */
int rune_driver_epoll_selected_for_test(aeron_driver_t *driver, int reset);

#ifdef __cplusplus
}
#endif

#endif /* RUNE_PROTOCOL_NET_RUNE_DRIVER_WAKE_H */
