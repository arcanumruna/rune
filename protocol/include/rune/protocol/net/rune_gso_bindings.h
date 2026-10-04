/* rune_gso_bindings.h — Aeron UDP transport bindings with Linux UDP GSO (send) and UDP GRO (receive).
 * Install into an aeron_driver_context_t BEFORE aeron_driver_init; delete AFTER aeron_driver_context_close.
 * Non-Linux builds: create() returns NULL (stock Aeron path).  Implementation: src/net/rune_gso_bindings.c.
 * Spec: rune-broker docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md */
#ifndef RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H
#define RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct rune_gso_bindings_stct rune_gso_bindings_t;

typedef struct rune_gso_totals_stct
{
    uint64_t gso_sends;        /* successful sendmsg(UDP_SEGMENT) calls */
    uint64_t gso_datagrams;    /* wire datagrams carried by them */
    uint64_t gso_fallbacks;    /* transports that turned GSO off (EIO/EINVAL/EOPNOTSUPP/ENOPROTOOPT) */
    uint64_t gro_buffers;      /* received buffers that held > 1 coalesced datagram */
    uint64_t gro_datagrams;    /* datagrams split out of them */
    uint64_t gro_ctrunc_drops; /* received buffers dropped: control data truncated (cannot split safely) */
    uint64_t gro_trunc_drops;  /* received buffers dropped: payload truncated (MSG_TRUNC) */
    uint64_t gro_disabled;     /* always 0: GRO is never turned off (a MSG_TRUNC drops only the damaged tail) */
    uint64_t transports;       /* transports created through these bindings */
    uint64_t gro_transports;   /* of which GRO was enabled on (receiver-affinity sockets only) */
}
rune_gso_totals_t;

/* Bitmask of what this host's kernel accepts: 1 = UDP_SEGMENT, 2 = UDP_GRO.  0 on non-Linux. */
int rune_gso_platform_supported(void);

/* NULL when neither (want_gso && supported) nor (want_gro && supported), or on allocation failure. */
rune_gso_bindings_t *rune_gso_bindings_create(int want_gso, int want_gro);

/* What these bindings do: 1 = GSO send, 2 = GRO receive (0 for NULL).  Runtime per-transport fallbacks are in
 * the totals (gso_fallbacks / gro_disabled). */
int rune_gso_bindings_active(const rune_gso_bindings_t *b);

/* aeron_driver_context_set_udp_channel_transport_bindings(ctx, ...); 0 on success, -1 otherwise. */
int rune_gso_bindings_install(aeron_driver_context_t *ctx, rune_gso_bindings_t *b);

/* Sum over every transport these bindings created.  Call ONLY after aeron_driver_close (every driver agent has
 * stopped): the per-transport counters are written by the sender/receiver agents without synchronisation. */
void rune_gso_bindings_totals(const rune_gso_bindings_t *b, rune_gso_totals_t *out);

void rune_gso_bindings_delete(rune_gso_bindings_t *b);

/* TEST SEAM — call before aeron_driver_start: the next `count` GSO sendmsg attempts fail with errno `err`. */
void rune_gso_bindings_test_inject(rune_gso_bindings_t *b, int err, int count);

/* TEST SEAM — call before aeron_driver_start: the next `count` received GRO buffers are flagged MSG_TRUNC. */
void rune_gso_bindings_test_inject_recv_trunc(rune_gso_bindings_t *b, int count);

#ifdef __cplusplus
}
#endif

#endif /* RUNE_PROTOCOL_NET_RUNE_GSO_BINDINGS_H */
