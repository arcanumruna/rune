#!/usr/bin/env bash
# Stage c of the regression checkpoint on a Linux host (re-check M5): the Linux-only tests, for real.
#
#   scripts/linux-stage-c.sh [build-dir]      (build-dir default build-fast; called by run-regression.sh)
#
# Runs, each in a private network namespace (`unshare -n`, loopback brought up) so the host's own interfaces and
# sysctls are never touched:
#   1. RUNE_LINUX_GATE=1 ctest -L linux-only  (UdpGsoBindings*: they lower the loopback MTU, so never in the host netns)
#   2. the bind-order proof, RUNE_BIND_ORDER_GATE=1, with the netns ephemeral range narrowed to 40 ports
#      (BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression)
# Needs CAP_NET_ADMIN and a namespace it can create and configure (root on the host, or a --privileged container;
# CAP_NET_ADMIN alone does not allow `unshare -n`).  Without that it prints NOT GREEN and exits 3 — the stage is
# never silently green.  Exit 0 only when both runs pass.
set -u
B="${1:-build-fast}"
RANGE="45000 45039"

# loopback up inside the new netns: `ip` when installed, else one SIOCSIFFLAGS via python3 (the builder image has
# python3 but no iproute2).
LO_UP='if command -v ip >/dev/null 2>&1; then ip link set lo up; else python3 -c "
import fcntl, socket, struct
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
f = struct.unpack(\"16sH14s\", fcntl.ioctl(s, 0x8913, struct.pack(\"16sH14s\", b\"lo\", 0, b\"\")))[1]
fcntl.ioctl(s, 0x8914, struct.pack(\"16sH14s\", b\"lo\", f | 1, b\"\"))"; fi'

capeff="$(awk '/^CapEff:/ {print $2}' /proc/self/status 2>/dev/null)"
net_admin=0
[ -n "$capeff" ] && [ $(( (16#$capeff >> 12) & 1 )) -eq 1 ] && net_admin=1
if [ "$net_admin" -ne 1 ] || \
   ! unshare -n sh -c "$LO_UP && echo '$RANGE' > /proc/sys/net/ipv4/ip_local_port_range" >/dev/null 2>&1; then
    echo "== NOT GREEN: the Linux-only stage needs CAP_NET_ADMIN and a network namespace it can configure" \
         "(unshare -n + a writable netns ip_local_port_range: root on the host or a --privileged container); CapEff=${capeff:-?}"
    exit 3
fi

echo "== stage c (Linux): linux-only, RUNE_LINUX_GATE=1, private netns"
unshare -n sh -c "$LO_UP && RUNE_LINUX_GATE=1 ctest --test-dir '$B' -L linux-only --output-on-failure"; r1=$?
echo "== stage c (Linux): bind order, private netns, ip_local_port_range $RANGE, RUNE_BIND_ORDER_GATE=1"
unshare -n sh -c "$LO_UP && echo '$RANGE' > /proc/sys/net/ipv4/ip_local_port_range && RUNE_BIND_ORDER_GATE=1 \
    '$B/libs/node/rune_catalog_replication_tests' \
    --gtest_filter=BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression"; r2=$?
echo "== stage c (Linux): linux-only rc $r1, bind-order rc $r2"
[ "$r1" -eq 0 ] && [ "$r2" -eq 0 ]
