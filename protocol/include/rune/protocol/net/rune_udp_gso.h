/* rune_udp_gso.h — pure UDP GSO/GRO math shared by the Aeron transport bindings (rune_gso_bindings.c).
 * No syscalls; C and C++.  Spec: rune-broker docs/superpowers/specs/2026-10-03-aeron-udp-gso-gro-design.md */
#ifndef RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H
#define RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H

#include <stddef.h>
#include <sys/uio.h>

/* Conservative kernel limit (UDP_MAX_SEGMENTS is 64 on older kernels, 128 on newer). */
#define RUNE_GSO_MAX_SEGMENTS 64
/* One GSO super-datagram is one IPv4 UDP datagram before segmentation: <= 65,535 - 20 - 8. */
#define RUNE_GSO_MAX_BYTES 65507

#ifdef __cplusplus
extern "C" {
#endif

/* Number of LEADING iovs (each one wire datagram) that may go out as ONE UDP_SEGMENT send with segment size
 * iov[0].iov_len: every iov equals iov[0].iov_len except that the LAST may be shorter (and then ends the group);
 * count <= max_segs; total bytes <= max_bytes.  Returns 0 for n == 0, else >= 1 (1 = send it plainly). */
static inline size_t rune_gso_group(const struct iovec *iov, size_t n, size_t max_segs, size_t max_bytes)
{
    if (0 == n) return 0;
    const size_t seg = iov[0].iov_len;
    if (0 == seg || seg > max_bytes) return 1;
    size_t total = seg;
    size_t k = 1;
    while (k < n && k < max_segs)
    {
        const size_t len = iov[k].iov_len;
        if (0 == len || len > seg || total + len > max_bytes) break;
        total += len;
        k++;
        if (len < seg) break; /* a shorter segment can only be the last one */
    }
    return k;
}

/* Wire datagrams inside one received buffer of len bytes whose UDP_GRO segment size is seg (0 = no GRO cmsg). */
static inline size_t rune_gro_count(size_t len, size_t seg)
{
    if (0 == seg || seg >= len) return 1;
    return (len + seg - 1) / seg;
}

/* Length of datagram `index` (0-based) in that buffer; only meaningful when rune_gro_count(len, seg) > 1. */
static inline size_t rune_gro_segment_len(size_t len, size_t seg, size_t index)
{
    const size_t off = index * seg;
    return (len - off < seg) ? (len - off) : seg;
}

/* Aeron network_publication_max_messages_per_send to use with these bindings: 16 (Aeron's max) only when GSO
 * send is active AND two MTU datagrams fit one GSO send (else every group is a single datagram and a deeper
 * batch only adds burstiness); 0 = keep Aeron's default (2). */
static inline unsigned rune_gso_msgs_per_send(int gso_active, size_t mtu)
{
    return (gso_active && 0 != mtu && 2 * mtu <= RUNE_GSO_MAX_BYTES) ? 16u : 0u;
}

#ifdef __cplusplus
}
#endif

#endif /* RUNE_PROTOCOL_NET_RUNE_UDP_GSO_H */
