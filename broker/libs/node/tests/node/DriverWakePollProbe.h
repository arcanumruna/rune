/* DriverWakePollProbe.h — test-only probe of the epoll-first poll function (see DriverWakePollProbe.c). */
#ifndef RUNE_NODE_TESTS_DRIVER_WAKE_POLL_PROBE_H
#define RUNE_NODE_TESTS_DRIVER_WAKE_POLL_PROBE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct rune_poll_probe_result_stct
{
    int installed;            /* 1: the epoll-first poller was installed on the probe's driver context */
    int work;                 /* the poll function's return value for the pass */
    int64_t errors_counter;   /* the transports' errors counter after the pass */
    int64_t observations;     /* distinct error log observations after the pass */
} rune_poll_probe_result_t;

/* One epoll-first pass over a poller with two ready loopback sockets whose first recvmmsg fails.
 * Returns 0 when the pass ran (results in *out), -1 when the setup failed. */
int rune_probe_epoll_poll_one_failing_socket(rune_poll_probe_result_t *out);

#ifdef __cplusplus
}
#endif

#endif
