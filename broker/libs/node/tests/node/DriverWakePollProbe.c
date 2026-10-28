/* DriverWakePollProbe.c — test-only C helper for EmbeddedDriverTest: drives the installed epoll-first poll function
 * (rune_driver_wake.c) on a real transport poller.  A C TU because Aeron's internal driver headers are C. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#include "DriverWakePollProbe.h"

#if defined(__linux__)
#include <aeronmd/aeronmd.h>
#include "aeron_driver_context.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "media/aeron_udp_transport_poller.h"
#include "util/aeron_error.h"

#include "rune/protocol/net/rune_driver_wake.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static int64_t probe_clock(void)
{
    return 0;
}

/* Drains the socket's one datagram; the failing transport (clientd) then reports an error the way recvmmsg does. */
static int probe_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    (void)msgvec;
    (void)vlen;
    (void)bytes_rcved;
    (void)recv_func;
    char scratch[64];
    while (recv(transport->recv_fd, scratch, sizeof scratch, MSG_DONTWAIT) > 0)
    {
    }
    if (transport == (aeron_udp_channel_transport_t *)clientd)
    {
        AERON_SET_ERR(EIO, "%s", "probe: injected recvmmsg failure");
        return -1;
    }
    return 1;
}

static int probe_bound_socket(void)
{
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        return -1;
    }
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(fd, (struct sockaddr *)&a, sizeof a) < 0)
    {
        close(fd);
        return -1;
    }
    return fd;
}

static int probe_send_one(int to_fd)
{
    struct sockaddr_in a;
    socklen_t len = sizeof a;
    if (getsockname(to_fd, (struct sockaddr *)&a, &len) < 0)
    {
        return -1;
    }
    const int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0)
    {
        return -1;
    }
    const char byte = 'x';
    const ssize_t n = sendto(s, &byte, 1, 0, (struct sockaddr *)&a, len);
    close(s);
    return n == 1 ? 0 : -1;
}

int rune_probe_epoll_poll_one_failing_socket(rune_poll_probe_result_t *out)
{
    memset(out, 0, sizeof *out);
    aeron_driver_context_t *ctx = NULL;
    if (aeron_driver_context_init(&ctx) < 0)
    {
        return -1;
    }
    void *copy = NULL;
    int rc = -1;
    aeron_udp_transport_poller_t poller;
    memset(&poller, 0, sizeof poller);
    poller.fd = -1;
    aeron_udp_channel_transport_t t[2];
    memset(t, 0, sizeof t);
    t[0].fd = t[0].recv_fd = -1;
    t[1].fd = t[1].recv_fd = -1;
    uint8_t log_buffer[64 * 1024];
    memset(log_buffer, 0, sizeof log_buffer);
    aeron_distinct_error_log_t log;
    int log_inited = 0;
    int64_t errors = 0;

    out->installed = rune_driver_epoll_poll_install(ctx, 0, &copy);
    if (!out->installed)
    {
        goto done;
    }
    aeron_udp_channel_transport_bindings_t *b = aeron_driver_context_get_udp_channel_transport_bindings(ctx);
    if (aeron_distinct_error_log_init(&log, log_buffer, sizeof log_buffer, probe_clock) < 0)
    {
        goto done;
    }
    log_inited = 1;
    if (aeron_udp_transport_poller_init(&poller, ctx, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        goto done;
    }
    for (int i = 0; i < 2; i++)
    {
        t[i].fd = t[i].recv_fd = probe_bound_socket();
        t[i].error_log = &log;
        t[i].errors_counter = &errors;
        if (t[i].recv_fd < 0 || aeron_udp_transport_poller_add(&poller, &t[i]) < 0 || probe_send_one(t[i].recv_fd) < 0)
        {
            goto done;
        }
    }
    /* Both datagrams are queued on loopback before the pass: one epoll_wait(0) reports both sockets. */
    struct mmsghdr msgvec[1];
    memset(msgvec, 0, sizeof msgvec);
    int64_t bytes = 0;
    out->work = b->poller_poll_func(&poller, msgvec, 1, &bytes, NULL, probe_recvmmsg, &t[0]);
    out->errors_counter = errors;
    out->observations = (int64_t)aeron_distinct_error_log_num_observations(&log);
    rc = 0;

done:
    if (poller.fd >= 0)
    {
        aeron_udp_transport_poller_close(&poller);
    }
    for (int i = 0; i < 2; i++)
    {
        if (t[i].recv_fd >= 0)
        {
            close(t[i].recv_fd);
        }
    }
    if (log_inited)
    {
        aeron_distinct_error_log_close(&log);
    }
    aeron_driver_context_close(ctx);
    rune_driver_epoll_poll_free(copy);
    aeron_err_clear();
    return rc;
}

#else

int rune_probe_epoll_poll_one_failing_socket(rune_poll_probe_result_t *out)
{
    (void)out;
    return -1;
}

#endif
