/* rune_peer_name_resolver.c — DR-15 fix round 1 (2026-10-05): an Aeron name
 * resolver that answers MANAGED peer hostnames from a cache fed off-thread
 * (rune::node::PeerResolverHub), so no driver conductor — which runs on a
 * worker / runtime thread for an invoker driver — ever calls getaddrinfo for
 * a peer.  Unmanaged names (numeric literals never reach a resolver; client-
 * supplied response hosts) keep Aeron's default resolver.
 *
 * The C side only adapts Aeron's resolver vtable to a C callback the C++ owner
 * (EmbeddedDriver) provides; the cache lives in C++ (PeerResolutionInbox). */
#include <rune/node/rune_peer_name_resolver.h>

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aeron_name_resolver.h"
#include "util/aeron_error.h"


static int rune_peer_resolve(
    aeron_name_resolver_t *resolver, const char *name, const char *uri_param_name, bool is_re_resolution,
    struct sockaddr_storage *address)
{
    rune_peer_name_lookup_t *lookup = (rune_peer_name_lookup_t *)resolver->state;
    uint32_t ipv4 = 0;
    const int r = lookup->fn(lookup->state, name, &ipv4);
    if (r < 0)
    {
        return aeron_default_name_resolver_resolve(resolver, name, uri_param_name, is_re_resolution, address);
    }
    if (0 == r)
    {
        AERON_SET_ERR(EINVAL, "peer host %s not resolved yet (rune peer resolver cache)", name);
        return -1;
    }
    struct sockaddr_in *in = (struct sockaddr_in *)address;
    memset(address, 0, sizeof(*address));
    in->sin_family = AF_INET;
    in->sin_addr.s_addr = ipv4;
    return 0;
}

static int rune_peer_start(aeron_name_resolver_t *resolver)
{
    (void)resolver;
    return 0;
}

static int rune_peer_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    (void)now_ms;
    rune_peer_name_lookup_t *lookup = (rune_peer_name_lookup_t *)resolver->state;
    (void)lookup->fn(lookup->state, NULL, NULL);   /* drain the cache ring */
    return 0;
}

static int rune_peer_close(aeron_name_resolver_t *resolver)
{
    (void)resolver;
    return 0;
}

static int rune_peer_supplier(aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
{
    (void)context;
    uintptr_t p = 0;
    if (NULL == args || 0 != strncmp(args, RUNE_PEER_RESOLVER_ARGS_PREFIX, strlen(RUNE_PEER_RESOLVER_ARGS_PREFIX)))
    {
        AERON_SET_ERR(EINVAL, "%s", "rune peer resolver: missing init args");
        return -1;
    }
    p = (uintptr_t)strtoull(args + strlen(RUNE_PEER_RESOLVER_ARGS_PREFIX), NULL, 16);
    if (0 == p)
    {
        AERON_SET_ERR(EINVAL, "%s", "rune peer resolver: bad init args");
        return -1;
    }
    resolver->name = "rune-peer";
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->resolve_func = rune_peer_resolve;
    resolver->start_func = rune_peer_start;      /* Aeron 1.51 calls it unconditionally */
    resolver->do_work_func = rune_peer_do_work;
    resolver->close_func = rune_peer_close;
    resolver->state = (void *)p;
    return 0;
}

int rune_peer_name_resolver_install(aeron_driver_context_t *ctx, rune_peer_name_lookup_t *lookup)
{
    snprintf(lookup->init_args, sizeof(lookup->init_args), RUNE_PEER_RESOLVER_ARGS_PREFIX "%" PRIxPTR,
             (uintptr_t)lookup);
    if (aeron_driver_context_set_name_resolver_init_args(ctx, lookup->init_args) < 0)
    {
        return -1;
    }
    return aeron_driver_context_set_name_resolver_supplier(ctx, rune_peer_supplier);
}
