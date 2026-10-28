/* rune_driver_wake.c — see rune/protocol/net/rune_driver_wake.h (A7 park-until-work + epoll-first polling).
 *
 * Threading: install / free / active run on the driver's owner before init / after close.  The poll function runs
 * on the agent that owns the poller (the invoker owner, or the sender / receiver agent of a threaded driver).
 * rune_driver_wake_fds and the test seam run on the pump thread.  No locks, no atomics, no allocation on the poll
 * path.  Depends on Aeron internals (1.51.0: poller->fd, poller->bindings_clientd holding an epoll_event array of
 * capacity >= transports.length, driver->receiver/sender.poller); re-check on any Aeron upgrade. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE   /* struct mmsghdr (Aeron's transport headers) */
#endif
#include "rune/protocol/net/rune_driver_wake.h"

#include <stddef.h>

#if defined(__linux__)
#include <aeronmd/aeron_driver.h>   /* aeron_driver_t: sender / receiver transport pollers (internal header) */
#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "media/aeron_udp_transport_poller.h"
#include "util/aeron_error.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

int rune_fd_is_epoll(int fd)
{
    if (fd < 0)
    {
        return 0;
    }
    char path[64];
    char target[64];
    snprintf(path, sizeof path, "/proc/self/fd/%d", fd);
    const ssize_t n = readlink(path, target, sizeof target - 1);
    if (n <= 0)
    {
        return 0;
    }
    target[n] = '\0';
    return strcmp(target, "anon_inode:[eventpoll]") == 0 ? 1 : 0;
}

/* Aeron's > threshold branch of aeron_udp_transport_poller_poll (media/aeron_udp_transport_poller.c), for every
 * poller with more than one transport.  The poller keeps every transport registered in its epoll instance
 * (poller->fd) and an epoll_event array of >= transports.length entries (bindings_clientd) regardless of the
 * threshold (aeron_udp_transport_poller_add), so this only changes WHEN the epoll path is taken.
 *
 * Review C1: every event epoll reports is consumed in this pass.  EPOLLIN → recvmmsg (as Aeron).  EPOLLERR (with
 * or without EPOLLIN) → getsockopt(SO_ERROR) first, which clears the socket's pending error: a connected UDP send
 * socket whose peer port answered ICMP unreachable reports EPOLLERR WITHOUT EPOLLIN, and Aeron's EPOLLIN-only check
 * left that error pending — the epoll fd stayed readable and an owner parked on it woke at once, forever, while
 * the peer was down.  ECONNREFUSED is benign (Aeron's recvmmsg / sendmmsg map it to 0).  Any other socket error, and
 * any recvmmsg failure, is handled as Aeron 1.51's poller handles it: logged to the driver's distinct error log and
 * errors counter (aeron_udp_channel_transport_log_error), and the pass continues with the remaining sockets.
 * Consuming an error is not work (0). */
static int rune_epoll_first_poll(
    aeron_udp_transport_poller_t *poller,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    aeron_udp_channel_transport_recvmmsg_func_t recvmmsg_func,
    void *clientd)
{
    const size_t length = poller->transports.length;
    if (length <= 1 || NULL == poller->bindings_clientd)
    {
        /* 0 or 1 transport: one recvmmsg costs the same single syscall as an epoll_wait, and recvmmsg itself
         * consumes a pending socket error (ECONNREFUSED → 0). */
        return aeron_udp_transport_poller_poll(poller, msgvec, vlen, bytes_rcved, recv_func, recvmmsg_func, clientd);
    }
    struct epoll_event *events = (struct epoll_event *)poller->bindings_clientd;
    const int result = epoll_wait(poller->fd, events, (int)length, 0);
    if (result < 0)
    {
        const int err = errno;
        if (EINTR == err || EAGAIN == err)
        {
            return 0;
        }
        AERON_SET_ERR(err, "%s", "epoll_wait");
        return -1;
    }
    int work_count = 0;
    for (int i = 0; i < result; i++)
    {
        const uint32_t ev = events[i].events;
        events[i].events = 0;
        aeron_udp_channel_transport_t *transport = (aeron_udp_channel_transport_t *)events[i].data.ptr;
        if (ev & EPOLLERR)
        {
            int so_error = 0;
            socklen_t so_len = sizeof so_error;
            if (getsockopt(transport->recv_fd, SOL_SOCKET, SO_ERROR, &so_error, &so_len) < 0)
            {
                so_error = errno;
            }
            if (0 != so_error && ECONNREFUSED != so_error)
            {
                AERON_SET_ERR(so_error, "socket error, fd: %d", (int)transport->recv_fd);
                aeron_udp_channel_transport_log_error(transport);
            }
        }
        if (ev & EPOLLIN)
        {
            const int recv_result = recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
            if (recv_result < 0)
            {
                AERON_APPEND_ERR("%s", "rune_epoll_first_poll");
                aeron_udp_channel_transport_log_error(transport);
            }
            else
            {
                work_count += recv_result;
            }
        }
    }
    return work_count;
}

int rune_driver_epoll_poll_install(aeron_driver_context_t *ctx, int ctx_bindings_owned, void **copy_out)
{
    if (NULL != copy_out)
    {
        *copy_out = NULL;
    }
    if (NULL == ctx)
    {
        return 0;
    }
    aeron_udp_channel_transport_bindings_t *b = aeron_driver_context_get_udp_channel_transport_bindings(ctx);
    if (NULL == b || b->poller_poll_func != aeron_udp_transport_poller_poll)
    {
        return 0;   /* someone else's poller (or none): leave it alone */
    }
    if (ctx_bindings_owned)
    {
        b->poller_poll_func = rune_epoll_first_poll;
        return 1;
    }
    if (NULL == copy_out)
    {
        return 0;
    }
    aeron_udp_channel_transport_bindings_t *copy =
        (aeron_udp_channel_transport_bindings_t *)malloc(sizeof(aeron_udp_channel_transport_bindings_t));
    if (NULL == copy)
    {
        return 0;
    }
    *copy = *b;   /* never patch Aeron's process-wide stock struct */
    copy->poller_poll_func = rune_epoll_first_poll;
    if (aeron_driver_context_set_udp_channel_transport_bindings(ctx, copy) < 0)
    {
        free(copy);
        return 0;
    }
    *copy_out = copy;
    return 1;
}

void rune_driver_epoll_poll_free(void *copy)
{
    free(copy);
}

int rune_driver_epoll_poll_active(aeron_driver_context_t *ctx)
{
    if (NULL == ctx)
    {
        return 0;
    }
    const aeron_udp_channel_transport_bindings_t *b = aeron_driver_context_get_udp_channel_transport_bindings(ctx);
    return NULL != b && b->poller_poll_func == rune_epoll_first_poll ? 1 : 0;
}

int rune_driver_wake_fds(aeron_driver_t *driver, int *out, int max)
{
    if (driver == NULL || out == NULL || max <= 0)
    {
        return 0;
    }
    /* Only a driver whose pollers consume every readiness they are woken by (review C1). */
    if (!rune_driver_epoll_poll_active(driver->context))
    {
        return 0;
    }
    const int candidates[2] = { driver->receiver.poller.fd, driver->sender.poller.fd };
    int n = 0;
    for (int i = 0; i < 2 && n < max; i++)
    {
        if (rune_fd_is_epoll(candidates[i]))
        {
            out[n++] = candidates[i];
        }
    }
    return n;
}

static int rune_poller_selected(aeron_udp_transport_poller_t *poller, int reset)
{
    struct epoll_event *events = (struct epoll_event *)poller->bindings_clientd;
    if (NULL == events)
    {
        return 0;
    }
    int n = 0;
    for (size_t i = 0; i < poller->transports.capacity; i++)
    {
        if (NULL != events[i].data.ptr)
        {
            n++;
            if (reset)
            {
                events[i].data.ptr = NULL;
            }
        }
    }
    return n;
}

int rune_driver_epoll_selected_for_test(aeron_driver_t *driver, int reset)
{
    if (NULL == driver)
    {
        return 0;
    }
    return rune_poller_selected(&driver->receiver.poller, reset) + rune_poller_selected(&driver->sender.poller, reset);
}

#else

int rune_driver_epoll_poll_install(aeron_driver_context_t *ctx, int ctx_bindings_owned, void **copy_out)
{
    (void)ctx;
    (void)ctx_bindings_owned;
    if (NULL != copy_out)
    {
        *copy_out = NULL;
    }
    return 0;
}

void rune_driver_epoll_poll_free(void *copy)
{
    (void)copy;
}

int rune_driver_epoll_poll_active(aeron_driver_context_t *ctx)
{
    (void)ctx;
    return 0;
}

int rune_fd_is_epoll(int fd)
{
    (void)fd;
    return 0;
}

int rune_driver_wake_fds(aeron_driver_t *driver, int *out, int max)
{
    (void)driver;
    (void)out;
    (void)max;
    return 0;
}

int rune_driver_epoll_selected_for_test(aeron_driver_t *driver, int reset)
{
    (void)driver;
    (void)reset;
    return 0;
}

#endif
