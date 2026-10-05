/* rune_peer_name_resolver.h — see rune_peer_name_resolver.c. */
#ifndef RUNE_PEER_NAME_RESOLVER_H
#define RUNE_PEER_NAME_RESOLVER_H

#include <stddef.h>
#include <stdint.h>

#include <aeronmd/aeronmd.h>

#ifdef __cplusplus
extern "C" {
#endif

/* host == NULL: drain only (conductor duty cycle).  Otherwise:
 *   1 = managed + cached (ipv4 set, network order), 0 = managed, not resolved yet,
 *  -1 = not a managed peer host (Aeron's default resolver answers). */
typedef int (*rune_peer_name_lookup_fn)(void *state, const char *host, uint32_t *ipv4);

typedef struct rune_peer_name_lookup_stct
{
    rune_peer_name_lookup_fn fn;
    void *state;
}
rune_peer_name_lookup_t;

/* `lookup` must outlive the driver.  Returns 0, or -1 (aeron_errmsg()). */
int rune_peer_name_resolver_install(aeron_driver_context_t *ctx, rune_peer_name_lookup_t *lookup);

#ifdef __cplusplus
}
#endif

#endif
