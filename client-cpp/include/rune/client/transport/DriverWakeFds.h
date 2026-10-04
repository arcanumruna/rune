/* DriverWakeFds.h — A7 park-until-work (rune-broker findings §4.12): the readiness fds of an INVOKER media
 * driver.  The owner of an invoker driver only learns that a datagram arrived by polling every transport (a
 * recvmmsg per socket, every pass).  Aeron's C driver keeps an epoll instance per transport poller (receiver
 * data endpoints, sender control endpoints) with every socket registered; an epoll fd is itself pollable
 * (readable while any registered socket is), so parking in ppoll() on them wakes on the next datagram instead
 * of spinning or sleeping out a fixed period.
 *
 * Linux only (other platforms: 0 fds).  Every fd is verified to be an epoll instance
 * (/proc/self/fd/N -> anon_inode:[eventpoll]).  Owned by the driver: valid until aeron_driver_close.
 * Implementation: src/transport/DriverWakeFds.c (a C TU — Aeron's internal driver headers are C). */
#ifndef RUNE_CLIENT_TRANSPORT_DRIVER_WAKE_FDS_H
#define RUNE_CLIENT_TRANSPORT_DRIVER_WAKE_FDS_H

#ifdef __cplusplus
extern "C" {
#endif

typedef struct aeron_driver_stct aeron_driver_t;
typedef struct aeron_driver_context_stct aeron_driver_context_t;

/* Writes at most `max` fds to `out`; returns how many (0 on non-Linux, NULL driver or no epoll pollers). */
int rune_client_driver_wake_fds(aeron_driver_t* driver, int* out, int max);

/* A7 epoll-first transport polling — the same as rune-broker's rune_driver_epoll_poll_install (DriverWakeFds.h
 * there): every transport poller with > 1 transport takes one epoll_wait(0) per duty cycle and recvmmsg-s only
 * the ready sockets, instead of Aeron's recvmmsg-every-transport below its 5-transport threshold.  Call AFTER
 * the GSO bindings install, BEFORE aeron_driver_init.  ctx_bindings_owned != 0: patch the context's (rune_gso)
 * bindings in place; 0: copy Aeron's stock bindings into *copy_out (free with
 * rune_client_driver_epoll_poll_free AFTER aeron_driver_context_close).  Returns 1 installed, 0 not. */
int rune_client_driver_epoll_poll_install(aeron_driver_context_t* ctx, int ctx_bindings_owned, void** copy_out);
void rune_client_driver_epoll_poll_free(void* copy);
int rune_client_driver_epoll_poll_active(aeron_driver_context_t* ctx);

#ifdef __cplusplus
}
#endif

#endif /* RUNE_CLIENT_TRANSPORT_DRIVER_WAKE_FDS_H */
