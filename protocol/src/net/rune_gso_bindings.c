/* rune_gso_bindings.c — see rune/protocol/net/rune_gso_bindings.h.
 *
 * Threading (verified in Aeron's C driver, 1.4x):
 *  - init_func runs ONLY on the conductor (send endpoint / receive destination create).
 *  - send/recvmmsg run on the sender/receiver agent that owns the transport.
 *  - close_func runs on the conductor (endpoint delete) OR on the RECEIVER agent (multi-destination subscription
 *    remove-destination / failed add: aeron_driver_receiver.c on_add/on_remove_destination).
 * So: per-transport state is allocated and appended to owner->states by the conductor only (init); close never
 * touches the owner (it only detaches the state from the transport); per-transport counters are written only by
 * the agent driving that transport; states are freed and totals summed only after aeron_driver_close (all agents
 * stopped).  No locks, no atomics, no allocation on the send/receive path.  Memory: one small state per transport
 * ever created, freed at rune_gso_bindings_delete. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "rune/protocol/net/rune_gso_bindings.h"

#include <stdlib.h>
#include <string.h>

#if defined(__linux__)

#include <errno.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/udp.h>

#include "aeron_driver_context.h"
#include "media/aeron_udp_channel_transport.h"
#include "media/aeron_udp_channel_transport_bindings.h"
#include "util/aeron_error.h"
#include "util/aeron_netutil.h"
#include "concurrent/aeron_counters_manager.h"

#include "rune/protocol/net/rune_udp_gso.h"

#ifndef SOL_UDP
#define SOL_UDP 17
#endif
#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef UDP_GRO
#define UDP_GRO 104
#endif

#define RUNE_GSO_UNSUPPORTED (-2)
#define RUNE_GSO_PATH_MTU    (-3)   /* EMSGSIZE: a segment exceeds the path MTU (see rune_gso_sendmsg) */

/* Review A-I3 (DR-bugs fix round 1): a path-MTU refusal is a per-DESTINATION condition (one client behind a
 * smaller MTU, or a transient link-MTU dip), never per-transport: only that destination's groups go plain, for
 * RUNE_GSO_PMTU_HOLD_NS, then GSO is re-probed (recovery after an MTU restore).  Fixed-size cache, no allocation;
 * a full cache evicts the entry closest to expiry. */
#define RUNE_GSO_PMTU_CACHE   8
#define RUNE_GSO_PMTU_HOLD_NS (10LL * 1000 * 1000 * 1000)
#define RUNE_GSO_PATH_MTU_COUNTER_TYPE_ID 1000222   /* Aeron counter "rune_gso_path_mtu_fallbacks" (review B-I2) */

struct rune_gso_bindings_stct
{
    aeron_udp_channel_transport_bindings_t bindings; /* FIRST: context->udp_channel_transport_bindings points here */
    aeron_udp_channel_transport_bindings_t defaults; /* Aeron's stock bindings: every fallback goes through these */
    int gso_enabled;
    int gro_enabled;
    int inject_errno;            /* test seams: set before driver start, consumed by one agent */
    int inject_count;
    int inject_skip;             /* test seam: GSO calls let through before the injection starts */
    int inject_trunc_count;
    /* Review B-I2: live count of path-MTU groups, an Aeron counter in this driver's CnC.  Allocated on the
     * conductor at the first transport init (rune_gso_init); a transport reaches the sender agent through the
     * driver's command queue afterwards, which orders this store before any sender read.  NULL = none. */
    volatile int64_t *path_mtu_counter;
    int counter_tried;
    struct rune_gso_transport_state_stct *states; /* conductor-only list (init); freed at delete */
};

typedef union rune_gro_ctrl_un
{
    struct cmsghdr align;
    char buf[CMSG_SPACE(sizeof(int))];
}
rune_gro_ctrl_t;

typedef struct rune_gso_transport_state_stct
{
    rune_gso_bindings_t *owner;
    struct rune_gso_transport_state_stct *next;
    int gso_on;
    int gro_parse;   /* this transport receives through rune_gro_recvmmsg (GRO was enabled at init) */
    int gro_on;      /* UDP_GRO currently enabled on recv_fd (turned off after a MSG_TRUNC) */
    int gso_logged;
    int gro_logged;
    int trunc_logged;
    int pmtu_logged;
    int pmtu_count;   /* live entries in pmtu[] (0 = the fast path skips the cache) */
    struct
    {
        struct sockaddr_storage addr;
        int64_t until_ns;
        int used;
    }
    pmtu[RUNE_GSO_PMTU_CACHE];
    rune_gso_totals_t t;
    rune_gro_ctrl_t ctrl[AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX];
}
rune_gso_transport_state_t;

static void rune_gso_totals_add(rune_gso_totals_t *dst, const rune_gso_totals_t *src)
{
    dst->gso_sends += src->gso_sends;
    dst->gso_datagrams += src->gso_datagrams;
    dst->gso_fallbacks += src->gso_fallbacks;
    dst->gro_buffers += src->gro_buffers;
    dst->gro_datagrams += src->gro_datagrams;
    dst->gro_ctrunc_drops += src->gro_ctrunc_drops;
    dst->gro_trunc_drops += src->gro_trunc_drops;
    dst->gro_disabled += src->gro_disabled;
    dst->transports += src->transports;
    dst->gro_transports += src->gro_transports;
    dst->gso_path_mtu_fallbacks += src->gso_path_mtu_fallbacks;
}

static int rune_gso_init(
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *bind_addr,
    struct sockaddr_storage *multicast_if_addr,
    struct sockaddr_storage *connect_addr,
    unsigned int multicast_if_index,
    uint8_t ttl,
    size_t socket_rcvbuf,
    size_t socket_sndbuf,
    bool is_media_timestamping,
    aeron_driver_context_t *context,
    aeron_udp_channel_transport_affinity_t affinity)
{
    rune_gso_bindings_t *owner = (rune_gso_bindings_t *)context->udp_channel_transport_bindings;
    if (!owner->counter_tried && NULL != context->counters_manager) /* conductor only */
    {
        owner->counter_tried = 1;
        static const char label[] = "rune_gso_path_mtu_fallbacks";
        const int32_t id = aeron_counters_manager_allocate(
            context->counters_manager, RUNE_GSO_PATH_MTU_COUNTER_TYPE_ID, NULL, 0, label, sizeof(label) - 1);
        if (id >= 0)
        {
            owner->path_mtu_counter = aeron_counters_manager_addr(context->counters_manager, id);
        }
    }
    if (owner->defaults.init_func(
        transport, bind_addr, multicast_if_addr, connect_addr, multicast_if_index, ttl, socket_rcvbuf, socket_sndbuf,
        is_media_timestamping, context, affinity) < 0)
    {
        return -1;
    }

    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)calloc(1, sizeof(*st));
    if (NULL == st)
    {
        /* Never fail (or double-close) a transport Aeron would have created: run it as stock Aeron. */
        transport->bindings_clientd = NULL;
        fprintf(stderr, "rune-gso: WARN no memory for transport state fd=%d; stock Aeron for it\n", (int)transport->fd);
        return 0;
    }
    st->owner = owner;
    st->gso_on = owner->gso_enabled;
    st->t.transports = 1;
    /* GRO only on RECEIVER-affinity sockets: the sender agent polls its control sockets (status messages, NAKs)
     * with mtu-sized buffers, so a GRO-merged burst would be truncated there.  And only where no other receive
     * cmsg (media timestamps) competes for the control buffer. */
    int one = 1;
    st->gro_on = owner->gro_enabled && AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER == affinity &&
        0 == transport->timestamp_flags &&
        0 == setsockopt(transport->recv_fd, SOL_UDP, UDP_GRO, &one, sizeof(one));
    st->gro_parse = st->gro_on;
    st->t.gro_transports = st->gro_on ? 1 : 0;
    /* Lifetime (re-review L3): per-transport state (~0.4 KB) lives until rune_gso_bindings_delete, not until the
     * transport closes — bounded for Rune's shapes (fixed unicast endpoints + MDC egress); unbounded endpoint
     * churn in a long-running driver would grow it. */
    st->next = owner->states; /* conductor-only */
    owner->states = st;
    transport->bindings_clientd = st;
    return 0;
}

static int rune_gso_close(aeron_udp_channel_transport_t *transport)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_close(transport);
    }
    /* May run on the receiver agent (MDS): never touch the owner here; the state stays on owner->states and
     * is summed / freed after aeron_driver_close. */
    transport->bindings_clientd = NULL;
    return st->owner->defaults.close_func(transport);
}

/* One sendmsg(UDP_SEGMENT) carrying `count` datagrams of iov[0].iov_len bytes (last may be shorter).
 * Returns count, 0 (transient: Aeron re-sends the whole batch), RUNE_GSO_UNSUPPORTED, RUNE_GSO_PATH_MTU, or -1
 * (error set). */
static int rune_gso_sendmsg(
    rune_gso_transport_state_t *st,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t count,
    int64_t *bytes_sent)
{
    union
    {
        struct cmsghdr align;
        char buf[CMSG_SPACE(sizeof(uint16_t))];
    }
    ctrl;
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    memset(&ctrl, 0, sizeof(ctrl));
    if (NULL == transport->connected_address)
    {
        msg.msg_name = address;
        msg.msg_namelen = (socklen_t)AERON_ADDR_LEN(address);
    }
    msg.msg_iov = iov;
    msg.msg_iovlen = count;
    msg.msg_control = ctrl.buf;
    msg.msg_controllen = sizeof(ctrl.buf);
    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    cm->cmsg_level = SOL_UDP;
    cm->cmsg_type = UDP_SEGMENT;
    cm->cmsg_len = CMSG_LEN(sizeof(uint16_t));
    const uint16_t seg = (uint16_t)iov[0].iov_len;
    memcpy(CMSG_DATA(cm), &seg, sizeof(seg));

    ssize_t sent;
    rune_gso_bindings_t *owner = st->owner;
    if (owner->inject_skip > 0) /* test seam only */
    {
        owner->inject_skip--;
        sent = sendmsg(transport->fd, &msg, 0);
    }
    else if (owner->inject_count > 0) /* test seam only: production never sets it */
    {
        owner->inject_count--;
        errno = owner->inject_errno;
        sent = -1;
    }
    else
    {
        sent = sendmsg(transport->fd, &msg, 0);
    }

    if (sent < 0)
    {
        const int err = errno;
        /* Same classification as Aeron's stock sendmmsg (aeron_udp_channel_transport_sendv): these four are a
         * short send; everything else — ENOBUFS included — is a send error.  Either way the network
         * publication does not advance snd-pos and re-sends the batch on its next duty cycle. */
        if (EAGAIN == err || EWOULDBLOCK == err || EINTR == err || ECONNREFUSED == err)
        {
            return 0;
        }
        if (EINVAL == err)
        {
            /* Review B-M3: kernels whose udp_send_skb rejects gso_size > fragsize answer EINVAL, not EMSGSIZE —
             * diagnose it against the route MTU (cold: GSO fails at most once per destination per hold period). */
            const struct sockaddr_storage *dst = NULL != transport->connected_address ? transport->connected_address : address;
            int pmtu = 0;
            socklen_t l = sizeof(pmtu);
            const int v6 = NULL != dst && AF_INET6 == dst->ss_family;
            const int got = v6 ? getsockopt(transport->fd, IPPROTO_IPV6, IPV6_MTU, &pmtu, &l)
                               : getsockopt(transport->fd, IPPROTO_IP, IP_MTU, &pmtu, &l);
            if (0 == got && pmtu > 0 && (int)seg + (v6 ? 48 : 28) > pmtu)
            {
                return RUNE_GSO_PATH_MTU;
            }
        }
        if (EIO == err || EINVAL == err || EOPNOTSUPP == err || ENOPROTOOPT == err)
        {
            return RUNE_GSO_UNSUPPORTED;
        }
        /* DR-BUG-MTU: the kernel refuses a GSO segment larger than the route's MTU (EMSGSIZE; measured on 5.15
         * after `ip link set mtu 1200` under a 1472-byte Aeron MTU), while the same datagram sent plainly is
         * IP-fragmented and delivered.  As a send error Aeron re-sent the identical batch forever: every
         * multi-fragment burst on this transport stalled while its heartbeats kept the session alive (acks flowed,
         * subscribers got nothing, no error).  It is a per-path condition, not a hard failure: fall back. */
        if (EMSGSIZE == err)
        {
            return RUNE_GSO_PATH_MTU;
        }
        AERON_SET_ERR(err, "rune-gso: sendmsg(UDP_SEGMENT) failed, segments=%zu seg=%u", count, (unsigned)seg);
        return -1;
    }
    *bytes_sent += sent;
    return (int)count;
}

static int64_t rune_gso_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static int rune_gso_same_addr(const struct sockaddr_storage *a, const struct sockaddr_storage *b)
{
    return a->ss_family == b->ss_family && 0 == memcmp(a, b, AERON_ADDR_LEN(a));
}

/* 1 when `dest` refused a GSO segment within the hold period (its groups go plain); an expired entry is
 * dropped (GSO re-probed).  Called only while st->pmtu_count > 0. */
static int rune_gso_pmtu_held(rune_gso_transport_state_t *st, const struct sockaddr_storage *dest, int64_t now)
{
    for (int i = 0; i < RUNE_GSO_PMTU_CACHE; i++)
    {
        if (!st->pmtu[i].used || !rune_gso_same_addr(&st->pmtu[i].addr, dest))
        {
            continue;
        }
        if (now < st->pmtu[i].until_ns)
        {
            return 1;
        }
        st->pmtu[i].used = 0;
        st->pmtu_count--;
        return 0;
    }
    return 0;
}

static void rune_gso_pmtu_hold(rune_gso_transport_state_t *st, const struct sockaddr_storage *dest, int64_t now)
{
    int slot = -1;
    for (int i = 0; i < RUNE_GSO_PMTU_CACHE && slot < 0; i++)
    {
        if (st->pmtu[i].used && rune_gso_same_addr(&st->pmtu[i].addr, dest)) slot = i;
    }
    for (int i = 0; i < RUNE_GSO_PMTU_CACHE && slot < 0; i++)
    {
        if (!st->pmtu[i].used) slot = i;
    }
    if (slot < 0) /* full: evict the entry closest to expiry */
    {
        slot = 0;
        for (int i = 1; i < RUNE_GSO_PMTU_CACHE; i++)
        {
            if (st->pmtu[i].until_ns < st->pmtu[slot].until_ns) slot = i;
        }
        st->pmtu[slot].used = 0;
        st->pmtu_count--;
    }
    if (!st->pmtu[slot].used)
    {
        st->pmtu[slot].used = 1;
        st->pmtu_count++;
    }
    memcpy(&st->pmtu[slot].addr, dest, sizeof(*dest));
    st->pmtu[slot].until_ns = now + RUNE_GSO_PMTU_HOLD_NS;
}

/* Returns the number of iov datagrams sent; a value < iov_length is a short send: Aeron's network publication
 * does not advance snd-pos and re-sends from it on the next duty cycle (the receiver treats the re-sent prefix
 * as duplicates by term offset). */
static int rune_gso_send(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_send(data_paths, transport, address, iov, iov_length, bytes_sent);
    }
    if (!st->gso_on || iov_length < 2)
    {
        return st->owner->defaults.send_func(data_paths, transport, address, iov, iov_length, bytes_sent);
    }

    size_t done = 0;
    while (done < iov_length)
    {
        const size_t k = rune_gso_group(iov + done, iov_length - done, RUNE_GSO_MAX_SEGMENTS, RUNE_GSO_MAX_BYTES);
        int r;
        if (1 == k)
        {
            r = st->owner->defaults.send_func(data_paths, transport, address, iov + done, 1, bytes_sent);
            if (r <= 0)
            {
                return done > 0 ? (int)done : r;   /* partial batch = short send (stock sendmmsg semantics) */
            }
            done += 1;
            continue;
        }

        const struct sockaddr_storage *dest = NULL != transport->connected_address ? transport->connected_address : address;
        int plain = 0;
        int64_t now = 0;
        if (st->pmtu_count > 0) /* only while some destination is held: the steady state never reads the clock */
        {
            now = rune_gso_now_ns();
            plain = rune_gso_pmtu_held(st, dest, now);
        }
        if (!plain)
        {
            r = rune_gso_sendmsg(st, transport, address, iov + done, k, bytes_sent);
            if (RUNE_GSO_UNSUPPORTED == r)
            {
                st->gso_on = 0; /* a kernel capability: per transport, for good */
                st->t.gso_fallbacks++;
                fprintf(stderr, "rune-gso: WARN UDP GSO refused on fd=%d; this transport falls back to sendmmsg\n",
                    (int)transport->fd);
                r = st->owner->defaults.send_func(
                    data_paths, transport, address, iov + done, iov_length - done, bytes_sent);
                return r < 0 ? r : (int)(done + (size_t)r);
            }
            if (RUNE_GSO_PATH_MTU == r)
            {
                /* DR-BUG-MTU + review A-I3: this destination's path is below the segment size.  This group goes
                 * plain (IP-fragmented by the kernel); the destination is held plain for the hold period, then
                 * re-probed; GSO stays on for every other destination. */
                st->t.gso_path_mtu_fallbacks++;
                if (NULL != st->owner->path_mtu_counter)
                {
                    aeron_counter_ordered_increment(st->owner->path_mtu_counter, 1);
                }
                if (0 == now)
                {
                    now = rune_gso_now_ns();
                }
                rune_gso_pmtu_hold(st, dest, now);
                if (!st->pmtu_logged)
                {
                    st->pmtu_logged = 1;
                    const int hdr = AF_INET6 == dest->ss_family ? 48 : 28;
                    fprintf(stderr, "rune-gso: WARN UDP GSO segment of %zu bytes exceeds the path MTU on fd=%d "
                        "(the link MTU is below the Aeron MTU); groups to that destination are sent plainly "
                        "(IP-fragmented) and GSO is re-probed every %lld s — lower RUNE_AERON_MTU to the link MTU "
                        "minus %d (counter rune_gso_path_mtu_fallbacks)\n",
                        iov[done].iov_len, (int)transport->fd, (long long)(RUNE_GSO_PMTU_HOLD_NS / 1000000000LL), hdr);
                }
                plain = 1;
            }
        }
        if (plain)
        {
            r = st->owner->defaults.send_func(data_paths, transport, address, iov + done, k, bytes_sent);
            if (r <= 0)
            {
                return done > 0 ? (int)done : r;   /* partial batch = short send (stock sendmmsg semantics) */
            }
            done += (size_t)r;
            if ((size_t)r < k)
            {
                return (int)done;
            }
            continue;
        }
        if (r <= 0)
        {
            return done > 0 ? (int)done : r;   /* partial batch = short send (stock sendmmsg semantics) */
        }
        st->t.gso_sends++;
        st->t.gso_datagrams += k;
        if (!st->gso_logged)
        {
            st->gso_logged = 1;
            fprintf(stderr, "rune-gso: UDP GSO active fd=%d seg=%zu segments=%zu\n",
                (int)transport->fd, iov[done].iov_len, k);
        }
        done += k;
    }
    return (int)done;
}

static int rune_gro_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    rune_gso_transport_state_t *st = (rune_gso_transport_state_t *)transport->bindings_clientd;
    if (NULL == st)
    {
        return aeron_udp_channel_transport_recvmmsg(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    }
    if (!st->gro_parse)
    {
        return st->owner->defaults.recvmmsg_func(transport, msgvec, vlen, bytes_rcved, recv_func, clientd);
    }
    if (vlen > AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX)
    {
        vlen = AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX;
    }
    for (size_t i = 0; i < vlen; i++)
    {
        msgvec[i].msg_hdr.msg_control = st->ctrl[i].buf;
        msgvec[i].msg_hdr.msg_controllen = sizeof(st->ctrl[i].buf);
        msgvec[i].msg_hdr.msg_flags = 0;
    }

    struct timespec tv = { .tv_nsec = 0, .tv_sec = 0 };
    const int result = recvmmsg(transport->recv_fd, msgvec, (unsigned int)vlen, 0, &tv);
    if (result < 0)
    {
        const int err = errno;
        for (size_t i = 0; i < vlen; i++)
        {
            msgvec[i].msg_hdr.msg_control = NULL;
            msgvec[i].msg_hdr.msg_controllen = 0;
        }
        if (EINTR == err || EAGAIN == err || ECONNREFUSED == err)
        {
            return 0;
        }
        AERON_SET_ERR(err, "rune-gso: recvmmsg failed, fd=%d, recv_fd=%d", (int)transport->fd, (int)transport->recv_fd);
        return -1;
    }

    if (result > 0 && st->owner->inject_trunc_count > 0) /* test seam only: production never sets it */
    {
        st->owner->inject_trunc_count--;
        msgvec[0].msg_hdr.msg_flags |= MSG_TRUNC;
    }

    int delivered = 0;
    for (int i = 0; i < result; i++)
    {
        struct msghdr *h = &msgvec[i].msg_hdr;
        uint8_t *base = (uint8_t *)h->msg_iov[0].iov_base;
        const size_t len = msgvec[i].msg_len;
        size_t seg = 0;
        for (struct cmsghdr *cm = CMSG_FIRSTHDR(h); NULL != cm; cm = CMSG_NXTHDR(h, cm))
        {
            if (SOL_UDP == cm->cmsg_level && UDP_GRO == cm->cmsg_type)
            {
                int v;
                memcpy(&v, CMSG_DATA(cm), sizeof(v));
                seg = (size_t)v;
            }
        }
        *bytes_rcved += (int64_t)len;
        if (0 != (h->msg_flags & MSG_CTRUNC))
        {
            st->t.gro_ctrunc_drops++; /* cannot know the segment size: drop = loss, Aeron NAKs recover it */
            continue;
        }
        size_t use = len;
        if (0 != (h->msg_flags & MSG_TRUNC))
        {
            /* The coalesced buffer did not fit: its tail is gone.  Deliver the COMPLETE leading segments and drop
             * only the damaged tail (= loss, Aeron NAKs recover it).  GRO is NEVER turned off here (re-review M1):
             * once UDP_GRO is disabled the kernel stops attaching the segment-size cmsg to buffers already
             * coalesced in the queue, so each would reach Aeron as ONE datagram — frames of other sessions written
             * into the first one's term (silent corruption). */
            st->t.gro_trunc_drops++;
            if (!st->trunc_logged)
            {
                st->trunc_logged = 1;
                fprintf(stderr, "rune-gso: WARN truncated GRO buffer (len=%zu seg=%zu) on recv_fd=%d; tail dropped\n",
                    len, seg, (int)transport->recv_fd);
            }
            if (0 == seg || len < seg)
            {
                continue;
            }
            use = (len / seg) * seg;
        }

        const size_t n = rune_gro_count(use, seg);
        if (1 == n)
        {
            recv_func(transport->data_paths, transport, clientd, transport->dispatch_clientd,
                transport->destination_clientd, base, use, (struct sockaddr_storage *)h->msg_name, NULL);
            delivered++;
            continue;
        }
        st->t.gro_buffers++;
        st->t.gro_datagrams += n;
        if (!st->gro_logged)
        {
            st->gro_logged = 1;
            fprintf(stderr, "rune-gso: UDP GRO active recv_fd=%d seg=%zu datagrams=%zu\n",
                (int)transport->recv_fd, seg, n);
        }
        for (size_t j = 0; j < n; j++)
        {
            recv_func(transport->data_paths, transport, clientd, transport->dispatch_clientd,
                transport->destination_clientd, base + j * seg, rune_gro_segment_len(use, seg, j),
                (struct sockaddr_storage *)h->msg_name, NULL);
        }
        delivered += (int)n;
    }
    /* The receiver/sender share one msgvec across every transport they poll: never leave our per-transport
     * control buffers attached for the next transport's (possibly stock) recvmmsg. */
    for (size_t i = 0; i < vlen; i++)
    {
        msgvec[i].msg_hdr.msg_control = NULL;
        msgvec[i].msg_hdr.msg_controllen = 0;
    }
    return delivered;
}

int rune_gso_platform_supported(void)
{
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        return 0;
    }
    int seg = 1400;
    int one = 1;
    int mask = 0;
    if (0 == setsockopt(fd, SOL_UDP, UDP_SEGMENT, &seg, sizeof(seg)))
    {
        mask |= 1;
    }
    if (0 == setsockopt(fd, SOL_UDP, UDP_GRO, &one, sizeof(one)))
    {
        mask |= 2;
    }
    close(fd);
    return mask;
}

rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro)
{
    const int supported = rune_gso_platform_supported();
    const int gso = want_gso && (supported & 1);
    const int gro = want_gro && (supported & 2);
    if (!gso && !gro)
    {
        return NULL;
    }
    aeron_udp_channel_transport_bindings_t *stock = aeron_udp_channel_transport_bindings_load_media("default");
    if (NULL == stock)
    {
        return NULL;
    }
    rune_gso_bindings_t *b = (rune_gso_bindings_t *)calloc(1, sizeof(*b));
    if (NULL == b)
    {
        return NULL;
    }
    b->defaults = *stock;
    b->bindings = *stock;
    b->bindings.init_func = rune_gso_init;
    b->bindings.close_func = rune_gso_close;
    b->bindings.send_func = rune_gso_send;
    b->bindings.recvmmsg_func = rune_gro_recvmmsg;
    b->bindings.meta_info.name = "rune_gso";
    b->bindings.meta_info.type = "bindings";
    b->bindings.meta_info.source_symbol = b;
    b->gso_enabled = gso;
    b->gro_enabled = gro;
    return b;
}

int rune_gso_bindings_active(const rune_gso_bindings_t *b)
{
    return NULL == b ? 0 : ((b->gso_enabled ? 1 : 0) | (b->gro_enabled ? 2 : 0));
}

int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b)
{
    if (NULL == ctx || NULL == b)
    {
        return -1;
    }
    return aeron_driver_context_set_udp_channel_transport_bindings(ctx, &b->bindings) < 0 ? -1 : 0;
}

void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out)
{
    memset(out, 0, sizeof(*out));
    for (const rune_gso_transport_state_t *st = NULL != b ? b->states : NULL; NULL != st; st = st->next)
    {
        rune_gso_totals_add(out, &st->t);
    }
}

void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count)
{
    if (NULL != b)
    {
        b->inject_errno = err;
        b->inject_count = count;
        b->inject_skip = 0;
    }
}

void rune_gso_bindings_test_inject_after(rune_gso_bindings_t *b, int err, int count, int skip)
{
    if (NULL != b)
    {
        b->inject_errno = err;
        b->inject_count = count;
        b->inject_skip = skip;
    }
}

void rune_gso_bindings_test_inject_recv_trunc(rune_gso_bindings_t *b, int count)
{
    if (NULL != b)
    {
        b->inject_trunc_count = count;
    }
}

void rune_gso_bindings_delete(rune_gso_bindings_t *b)
{
    if (NULL == b)
    {
        return;
    }
    rune_gso_transport_state_t *st = b->states;
    while (NULL != st)
    {
        rune_gso_transport_state_t *next = st->next;
        free(st);
        st = next;
    }
    free(b);
}

#else /* !__linux__ : stock Aeron everywhere */

struct rune_gso_bindings_stct
{
    int unused;
};

int rune_gso_platform_supported(void) { return 0; }
rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro) { (void)want_gso; (void)want_gro; return NULL; }
int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b) { (void)ctx; (void)b; return -1; }
void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out) { (void)b; memset(out, 0, sizeof(*out)); }
void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count) { (void)b; (void)err; (void)count; }
void rune_gso_bindings_test_inject_after(rune_gso_bindings_t *b, int err, int count, int skip)
{ (void)b; (void)err; (void)count; (void)skip; }
void rune_gso_bindings_test_inject_recv_trunc(rune_gso_bindings_t *b, int count) { (void)b; (void)count; }
int rune_gso_bindings_active(const rune_gso_bindings_t *b) { (void)b; return 0; }
void rune_gso_bindings_delete(rune_gso_bindings_t *b) { free(b); }

#endif
