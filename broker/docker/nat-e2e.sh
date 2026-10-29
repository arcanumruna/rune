#!/usr/bin/env bash
# docker/nat-e2e.sh — NAT-safe client egress: real clients behind a masquerading router, against 3 authenticated brokers.
#
# What it proves: a client whose declared address the brokers cannot reach (it sits in a private network that only a
# NAT router connects to the brokers) still connects, produces, consumes, joins groups and runs transactions, because
# every reply returns over the client's own flow (Aeron response channels).  Two clients share the one NAT (the same
# public address, different source ports), and a client that does NOT use the response path fails (the negative control:
# it proves the NAT really isolates the brokers from the private network).
#
#   topology (docker/docker-compose.nat.yml, an override on the DEFAULT docker-compose.yml: 3 nodes, auth + TLS):
#     nat_public   rune-1..3 + the router's outside leg           (plain bridge)
#     nat_private  the router's inside leg + nat-client-a/-b      (internal: no route out; the clients' default route is
#                                                                  the router; the router MASQUERADEs onto nat_public)
#   Nothing is written down about addresses: every address is discovered with `docker inspect`.
#
#   1. build:    the broker, the rune CLI (client) and the router images
#   2. up:       key + TLS, the 3 brokers and the router, the MASQUERADE rule, a common consensus leader
#   3. ports:    the seed's (rune-1, node id 0) gateway response-control port, derived (HostDefaults.h)
#   4. clients:  nat-client-a and nat-client-b on nat_private; route via the router; dr_client built in the image;
#                the first admin + the e2e user are bootstrapped FROM nat-client-a (the bootstrap rides the NAT path)
#   5. DDL:      topics nat_a, nat_b, nat_tx_in (rf 3, 6 partitions) from nat-client-a
#   6. produce + consume: dr_client produce_subscribe acks=all, a on nat_a and b on nat_b CONCURRENTLY, N records each
#   7. group:    nat-client-b consumes nat_a as group nat-g: every record of step 6
#   8. txn:      nat-client-a txn_produce (nat-tx) into nat_tx_in, nat-client-b txn_consume (group nat-txg, read_committed)
#   9. NEGATIVE CONTROL: an old-path client (RUNE_GATEWAY_RESPONSE_PORT unset) behind the same NAT must FAIL to connect
#  10. leader change mid-run: produce_subscribe on nat_a while the consensus leader is `docker kill`ed and restarted
#  11. print NAT-E2E: OK
#
# Usage:  bash broker/docker/nat-e2e.sh      (from any directory; the monorepo is found from this file's location)
# Environment (all optional):
#   FLIGHT_RECORDER_DIR   the flight-recorder checkout (default: <monorepo>/../flight-recorder)
#   ARCANUM_METRICS_DIR   the arcanum-metrics checkout (default: <monorepo>/../arcanum-metrics)
#   NAT_E2E_RECORDS       records per produce_subscribe client (default 20000)
#   NAT_E2E_TX_RECORDS    records of the transaction (default 500)
#   NAT_E2E_RATE_HZ       produce rate of the leader-change run, so the run outlasts the kill (default 1000)
#   NAT_E2E_GROUP_WINDOW_MS  how long the group / txn consumers collect (default 45000; dr_client always waits it out)
#   E2E_LOG_DIR           when set, the brokers' and the clients' logs are kept there at teardown
#   E2E_WORK_DIR          scratch dir shared with the client containers as /work (default: a mktemp dir under <broker>,
#                         removed at teardown; yours is kept)
#
# House style: bounded waits (never a fixed sleep as a gate), trap teardown, every failure names its step.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib/auth.sh
. "${SCRIPT_DIR}/lib/auth.sh"
BROKER_REPO="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECTS="$(cd "${BROKER_REPO}/.." && pwd)"
# flight-recorder is its own repo, checked out next to the monorepo (compose additional_contexts read FLIGHT_RECORDER_DIR).
export FLIGHT_RECORDER_DIR="${FLIGHT_RECORDER_DIR:-${PROJECTS}/../flight-recorder}"
# arcanum-metrics (header-only) is its own repo too; compose additional_contexts read ARCANUM_METRICS_DIR.
export ARCANUM_METRICS_DIR="${ARCANUM_METRICS_DIR:-${PROJECTS}/../arcanum-metrics}"
CLIENT_REPO="${PROJECTS}/client-cpp"
COMPOSE_PROJECT="rune-nat-e2e"
PUB_NET="${COMPOSE_PROJECT}_nat_public"
PRIV_NET="${COMPOSE_PROJECT}_nat_private"
# A work dir this script made (mktemp) is removed at teardown; a caller-supplied E2E_WORK_DIR never is.
if [ -n "${E2E_WORK_DIR:-}" ]; then WORK_DIR="${E2E_WORK_DIR}"; OWN_WORK=""
else WORK_DIR="$(mktemp -d "${BROKER_REPO}/.nat-e2e-work.XXXXXX")"; OWN_WORK=1; fi

N="${NAT_E2E_RECORDS:-20000}"
TX_N="${NAT_E2E_TX_RECORDS:-500}"
RATE_HZ="${NAT_E2E_RATE_HZ:-1000}"
GROUP_WINDOW_MS="${NAT_E2E_GROUP_WINDOW_MS:-45000}"
LEADER_WAIT_S=180      # a common consensus leader after `up`
OP_TIMEOUT_S=150       # one rune CLI command
RUN_S=600              # one produce_subscribe client
NEG_CONNECT_MS=20000   # the negative control's connect timeout
NEG_S=120              # hard cap of the negative control
PROGRESS_WAIT_S=300    # the leader-change run reaches its kill threshold
RESUME_WAIT_S=180      # progress resumes after the kill
RESUME_RECORDS=500     # "resumed" = this many more acked records since the kill
LINK_WAIT_S=120        # a restarted node serving again
BROKER_PORT=40200
EGRESS_BASE=40600      # the clients' DECLARED egress ports (unreachable from the brokers; one per dr_client run)
PARTS=6
TOPIC_TABLE_A="nat_a:0:${PARTS}"        # creation order in docker/topics/nat-e2e.rql
TOPIC_TABLE_B="nat_b:1:${PARTS}"
TOPIC_TABLE_TX="nat_tx_in:2:${PARTS}"
ROUTER="rune-nat-e2e-router"            # container name resolved with `dc ps` below; this is only a label
CLIENT_A="nat-client-a"
CLIENT_B="nat-client-b"
DR="/build/build/dr_client"             # built in the rune-ql-probe image's retained build tree (step 4)

# Derived gateway response-control port of node id 0 (the seed, rune-1): HostDefaults.h
#   block base = kDerivedShardPortBase (8200) + node id * kDerivedShardPortStride (3 * 64 = 192)
#   egress control base = block base + 128;  gateway response = egress control base + 63
SEED_NODE_ID=0
DERIVED_SHARD_PORT_BASE=8200
DERIVED_SHARD_PORT_STRIDE=$(( 3 * 64 ))
EGRESS_CONTROL_OFFSET=128
GATEWAY_RESPONSE_OFFSET=63
GW_RESP=$(( DERIVED_SHARD_PORT_BASE + DERIVED_SHARD_PORT_STRIDE * SEED_NODE_ID + EGRESS_CONTROL_OFFSET + GATEWAY_RESPONSE_OFFSET ))

export CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
export RUNE_AUTH_WORK_DIR="${WORK_DIR}"      # the client containers' /work is also where the passwords live
export RUNE_AUTH_TOOLS="${CLIENT_A}"         # the CLI runs inside nat-client-a: the bootstrap itself rides the NAT path

STEP="init"
log()  { echo "[nat-e2e] $*" >&2; }
step() { STEP="$1"; echo "NAT-E2E: $1"; }
fail() { echo "[nat-e2e] FAIL (${STEP}): $*" >&2; exit 1; }

sha256_stdin() { if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi; }
repo_tag() {
    local d="$1"
    if [ -e "${d}/.git" ]; then git -C "${d}" rev-parse --short HEAD; return; fi
    (cd "${d}" && find . -type f -not -path './.nat-e2e-work*' -not -path './.auth.*' -print0 | LC_ALL=C sort -z | xargs -0 cat | sha256_stdin) | cut -c1-12
}
mkdir -p "${WORK_DIR}"
rune_auth_init   # RUNE_AUTH_DIR (mode 700) holds the cluster key; RUNE_SECRETS_DIR is exported for the compose files
export RUNE_IMAGE_TAG="$(repo_tag "${BROKER_REPO}")-nat-e2e"
export QL_IMAGE="rune-ql-probe:$(repo_tag "${CLIENT_REPO}")"

dc() { docker compose -p "${COMPOSE_PROJECT}" -f "${SCRIPT_DIR}/docker-compose.yml" -f "${SCRIPT_DIR}/docker-compose.nat.yml" "$@"; }
now_s() { date +%s; }

[ -d "${FLIGHT_RECORDER_DIR}" ] || fail "missing flight-recorder checkout (set FLIGHT_RECORDER_DIR)"
[ -d "${ARCANUM_METRICS_DIR}" ] || fail "missing arcanum-metrics checkout (set ARCANUM_METRICS_DIR)"
for d in protocol codec crypto ql client-cpp; do
    [ -d "${PROJECTS}/${d}" ] || fail "missing ../${d}"
done
[ -f "${SCRIPT_DIR}/topics/nat-e2e.rql" ] || fail "missing docker/topics/nat-e2e.rql"

cname() { dc ps -a -q "$1" | head -1; }      # the container id of a compose service (rune-1..3, nat-router)
nodectr() { cname "rune-$1"; }                # node <n> (1-3)

# Only containers this script started are ever removed (never a prune, never another project's).
STARTED=()
teardown() {
    local rc=$?
    log "Tearing down (exit ${rc})..."
    if [ -n "${E2E_LOG_DIR:-}" ]; then
        mkdir -p "${E2E_LOG_DIR}"
        for n in 1 2 3; do docker logs --timestamps "$(nodectr "${n}")" > "${E2E_LOG_DIR}/rune-${n}.log" 2>&1 || true; done
        cp -f "${WORK_DIR}"/*.out "${WORK_DIR}"/*.err "${E2E_LOG_DIR}/" 2>/dev/null || true
    fi
    local c
    for c in ${STARTED[@]+"${STARTED[@]}"}; do docker rm -f "${c}" >/dev/null 2>&1 || true; done
    dc down -v --remove-orphans >/dev/null 2>&1 || true
    if [ -n "${OWN_WORK}" ]; then rm -rf "${WORK_DIR}" 2>/dev/null || true; fi
    rune_auth_cleanup        # the cluster key and the generated passwords
    return "${rc}"
}
trap teardown EXIT

# wait_until <budget_s> <description> <command...> : polls the command (0 = done) every 1 s.
wait_until() {
    local budget="$1" what="$2"; shift 2
    local deadline=$(( $(now_s) + budget ))
    while [ "$(now_s)" -lt "${deadline}" ]; do
        if "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    log "timed out after ${budget}s waiting for ${what}"
    return 1
}

# ip_on <container> <network> : the container's address on that network (docker inspect; nothing is written down).
ip_on() { docker inspect -f "{{with index .NetworkSettings.Networks \"$2\"}}{{.IPAddress}}{{end}}" "$1"; }

# The consensus leader every node agrees on (node id 0-2, printed); the transaction coordinator is the leader.
consensus_leader() {
    local ids="" n id
    for n in 1 2 3; do
        id=$(docker logs "$(nodectr "${n}")" 2>&1 | grep -oE 'node [0-9]+ is LEADER' | tail -1 | grep -oE '[0-9]+') || return 1
        [ -n "${id}" ] || return 1
        ids="${ids} ${id}"
    done
    [ "$(echo "${ids}" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l | tr -d ' ')" = "1" ] || return 1
    echo "${ids}" | awk '{print $1}'
}
cluster_has_leader() { consensus_leader >/dev/null; }

# kill_node <node id 0-2> / restart_node <node id 0-2>: docker kill, and docker start + both peer links back (logs since the start).
kill_node() { docker kill "$(nodectr $(( $1 + 1 )))" >/dev/null || fail "docker kill of rune-$(( $1 + 1 )) failed"; }
SINCE=""
links_since() {
    local n="$1" id
    for id in 0 1 2; do
        [ "${id}" = "$(( n - 1 ))" ] && continue
        docker logs --since "${SINCE}" "$(nodectr "${n}")" 2>&1 | grep -q "peer link with node ${id} established" || return 1
    done
}
restart_node() {
    local n=$(( $1 + 1 ))
    SINCE="$(date +%s)"   # epoch seconds: no timezone ambiguity
    docker start "$(nodectr "${n}")" >/dev/null || fail "docker start of rune-${n} failed"
    wait_until "${LINK_WAIT_S}" "rune-${n}'s peer links" links_since "${n}" \
        || { docker logs --tail 60 "$(nodectr "${n}")" >&2; fail "rune-${n} did not re-establish both peer links within ${LINK_WAIT_S}s"; }
}

# ---- dr_client runs ---------------------------------------------------------------------------------------------------
RUN_N=0
DR_PRE=()          # a command prefix in the client (the negative control: env -u RUNE_GATEWAY_RESPONSE_PORT)
DR_ENV=()
# dr_env <client> <tag> <topic table> [KEY=VALUE ...] : sets DR_ENV, the `docker exec -e ...` arguments.  The DECLARED
# egress is the client's private address on a port of its own per run: unreachable from the brokers by construction.
dr_env() {
    local client="$1" tag="$2" table="$3"; shift 3
    local ip kv
    ip="$(ip_on "${client}" "${PRIV_NET}")"
    [ -n "${ip}" ] || fail "no address of ${client} on ${PRIV_NET}"
    RUN_N=$(( RUN_N + 1 ))
    DR_ENV=(-e "RUNE_TARGET=aeron:udp?endpoint=rune-1:${BROKER_PORT}"
            -e "RUNE_EGRESS=aeron:udp?endpoint=${ip}:$(( EGRESS_BASE + RUN_N ))"
            -e "RUNE_DATA_DIR=/tmp/nat-${tag}" -e "RUNE_CLIENT_TOPIC_TABLE=${table}"
            -e RUNE_TIMEOUT_MS=20000 -e RUNE_ACKS=all)
    for kv in "$@"; do DR_ENV+=(-e "${kv}"); done
}
# dr_run <client> <tag> <timeout_s> <topic table> [KEY=VALUE ...] : foreground; sets DR_RC and DR_OUT; /work/<tag>.out|.err.
DR_RC=0; DR_OUT=""
dr_run() {
    local client="$1" tag="$2" t="$3" table="$4"; shift 4
    dr_env "${client}" "${tag}" "${table}" "$@"
    set +e
    docker exec "${DR_ENV[@]}" "${client}" ${DR_PRE[@]+"${DR_PRE[@]}"} timeout "${t}" "${DR}" > "${WORK_DIR}/${tag}.out" 2> "${WORK_DIR}/${tag}.err"
    DR_RC=$?
    set -e
    DR_OUT="$(cat "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err" 2>/dev/null)"
}
# dr_start <client> <tag> <timeout_s> <topic table> [KEY=VALUE ...] : background; the exit status lands in /work/<tag>.rc.
dr_start() {
    local client="$1" tag="$2" t="$3" table="$4"; shift 4
    dr_env "${client}" "${tag}" "${table}" "$@"
    rm -f "${WORK_DIR}/${tag}.rc" "${WORK_DIR}/${tag}.out" "${WORK_DIR}/${tag}.err"
    docker exec -d "${DR_ENV[@]}" "${client}" sh -c \
        "timeout ${t} ${DR} > /work/${tag}.out 2> /work/${tag}.err; echo \$? > /work/${tag}.rc"
}
bg_out()  { cat "${WORK_DIR}/$1.out" "${WORK_DIR}/$1.err" 2>/dev/null || true; }
bg_done() { [ -s "${WORK_DIR}/$1.rc" ]; }
bg_rc()   { cat "${WORK_DIR}/$1.rc"; }
acked()   { local c; c=$(grep -c '^PRODUCED ' "${WORK_DIR}/$1.out" 2>/dev/null) || c=0; echo "${c}"; }
# uniq_seqs <tag> <DELIVERED|GROUPREC> <cid> : the number of distinct seqs of client <cid> the run delivered.
uniq_seqs() {
    local tag="$1" kind="$2" cid="$3"
    case "${kind}" in
        DELIVERED) grep "^DELIVERED cid=${cid} " "${WORK_DIR}/${tag}.out" | sed -E 's/.* seq=([0-9]+) .*/\1/' | sort -u | wc -l | tr -d ' ' ;;
        GROUPREC)  grep -E "^GROUPREC .* rec_cid=${cid} " "${WORK_DIR}/${tag}.out" | sed -E 's/.* rec_seq=([0-9]+).*/\1/' | sort -u | wc -l | tr -d ' ' ;;
    esac
}
# bg_ok <tag> : exited 0, no CLIENT: FAIL line (else prints the tail and fails the current step).
bg_ok() {
    bg_done "$1" || fail "$1 did not finish"
    if [ "$(bg_rc "$1")" != "0" ] || bg_out "$1" | grep -q 'CLIENT: FAIL'; then
        bg_out "$1" | grep -E 'CLIENT:|SUMMARY' | tail -20 >&2
        fail "$1 exited $(bg_rc "$1") (want 0 with no 'CLIENT: FAIL')"
    fi
}

# client_route <client> <router address on nat_private> : default route via the router.  The rune-ql-probe image may
# carry no `ip`: then a throwaway container of the router image (iproute2) joins the client's network namespace.
client_route() {
    local client="$1" via="$2" helper="nat-route-$$-${RANDOM}"
    if docker exec "${client}" ip route replace default via "${via}" >/dev/null 2>&1; then return 0; fi
    STARTED+=("${helper}")
    docker run --rm --name "${helper}" --network "container:${client}" --cap-add NET_ADMIN \
        "rune-nat-router:${RUNE_IMAGE_TAG}" ip route replace default via "${via}" >/dev/null \
        || fail "could not set ${client}'s default route via the router"
}

# start_client <name> : a client on nat_private ONLY, brokers' advertised names pointing at their public addresses.
start_client() {
    local name="$1"
    docker rm -f "${name}" >/dev/null 2>&1 || true
    STARTED+=("${name}")
    docker run -d --name "${name}" --hostname "${name}" --network "${PRIV_NET}" --platform linux/amd64 --shm-size=512m \
        --security-opt seccomp=unconfined --cap-add NET_ADMIN \
        "${ADD_HOSTS[@]}" \
        -v "${WORK_DIR}:/work" -v "${SCRIPT_DIR}/topics:/topics:ro" -v "${RUNE_SECRETS_DIR}:/run/secrets:ro" \
        -e RUNE_USER="${RUNE_AUTH_E2E_USER}" -e RUNE_PASSWORD_FILE=/work/e2e.pw -e RUNE_TLS_CA_FILE=/run/secrets/ca.pem \
        -e RUNE_GATEWAY_RESPONSE_PORT="${GW_RESP}" \
        "${QL_IMAGE}" sleep infinity >/dev/null || fail "could not start ${name}"
    client_route "${name}" "${ROUTER_PRIV_IP}"
}

# ---------------------------------------------------------------------------
# Step 1: build.
# ---------------------------------------------------------------------------
step "1 build"
log "building the broker image (tag ${RUNE_IMAGE_TAG}) and the router image..."
set +e
out=$(dc build rune-1 nat-router 2>&1); rc=$?
set -e
echo "${out}" | grep -E "^( Image|ERROR|#[0-9]+ (DONE|ERROR))" || true
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -60 >&2; fail "image build failed (exit ${rc})"; }
docker image inspect "rune-broker-node:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "broker image not built: rune-broker-node:${RUNE_IMAGE_TAG}"
docker image inspect "rune-nat-router:${RUNE_IMAGE_TAG}" >/dev/null 2>&1 || fail "router image not built: rune-nat-router:${RUNE_IMAGE_TAG}"
log "building the rune CLI / client image (${QL_IMAGE})..."
bash "${SCRIPT_DIR}/create-topics.sh" --build-image || fail "could not build the rune CLI image"

# ---------------------------------------------------------------------------
# Step 2: up, the NAT rule, a leader.
# ---------------------------------------------------------------------------
step "2 up"
rune_gen_cluster_key "${RUNE_SECRETS_DIR}" || fail "could not generate the cluster key"
dc up -d --no-build rune-1 rune-2 rune-3 nat-router || fail "compose up failed"
ROUTER_CTR="$(cname nat-router)"
[ -n "${ROUTER_CTR}" ] || fail "the router container is missing"
ROUTER_PUBLIC_IP="$(ip_on "${ROUTER_CTR}" "${PUB_NET}")"
ROUTER_PRIV_IP="$(ip_on "${ROUTER_CTR}" "${PRIV_NET}")"
[ -n "${ROUTER_PUBLIC_IP}" ] && [ -n "${ROUTER_PRIV_IP}" ] || fail "the router is not on both networks"
PUB_IF=$(docker exec "${ROUTER_CTR}" ip -o -4 addr show | awk -v ip="${ROUTER_PUBLIC_IP}" '$4 ~ "^"ip"/" {print $2}')
[ -n "${PUB_IF}" ] || fail "could not find the router's interface on ${PUB_NET}"
# iptables (nft backend in alpine 3.20); the legacy binary is the fallback for a host kernel without nf_tables.
router_ipt() { docker exec "${ROUTER_CTR}" iptables "$@" || docker exec "${ROUTER_CTR}" iptables-legacy "$@"; }
router_ipt -t nat -A POSTROUTING -o "${PUB_IF}" -j MASQUERADE || fail "could not add the MASQUERADE rule on ${PUB_IF}"
router_ipt -A FORWARD -j ACCEPT || fail "could not add the FORWARD rule"
log "waiting for a common consensus leader (up to ${LEADER_WAIT_S}s)..."
wait_until "${LEADER_WAIT_S}" "a common consensus leader" cluster_has_leader \
    || { for n in 1 2 3; do docker logs --tail 40 "$(nodectr "${n}")" >&2 || true; done; fail "no common consensus leader within ${LEADER_WAIT_S}s"; }

# ---------------------------------------------------------------------------
# Step 3: the derived gateway response-control port of the seed.
# ---------------------------------------------------------------------------
step "3 derived ports"
log "seed rune-1 (node id ${SEED_NODE_ID}): gateway response-control port ${GW_RESP} (HostDefaults.h derivation)"
ADD_HOSTS=()
for n in 1 2 3; do
    ip="$(ip_on "$(nodectr "${n}")" "${PUB_NET}")"
    [ -n "${ip}" ] || fail "no address of rune-${n} on ${PUB_NET}"
    ADD_HOSTS+=(--add-host "rune-${n}:${ip}")
done

# ---------------------------------------------------------------------------
# Step 4: the clients behind the NAT; bootstrap through it.
# ---------------------------------------------------------------------------
step "4 clients"
start_client "${CLIENT_A}"
start_client "${CLIENT_B}"
log "building dr_client in ${CLIENT_A} (the image's retained build tree), copying it to ${CLIENT_B}..."
set +e
out=$(docker exec "${CLIENT_A}" sh -c 'cmake --build /build/build --target dr_client -j2' 2>&1); rc=$?
set -e
[ "${rc}" -eq 0 ] || { echo "${out}" | tail -40 >&2; fail "building dr_client failed (exit ${rc})"; }
docker cp "${CLIENT_A}:${DR}" "${WORK_DIR}/dr_client" || fail "docker cp of dr_client from ${CLIENT_A} failed"
docker cp "${WORK_DIR}/dr_client" "${CLIENT_B}:${DR}" || fail "docker cp of dr_client to ${CLIENT_B} failed"
rm -f "${WORK_DIR}/dr_client"
log "bootstrapping the first admin + the e2e user from ${CLIENT_A} (through the NAT)..."
rune_auth_prepare "${PRIV_NET}" "rune-1:${BROKER_PORT}" || fail "bootstrap / e2e user"

# ---------------------------------------------------------------------------
# Step 5: DDL.
# ---------------------------------------------------------------------------
step "5 DDL"
ddl_ok=0
for attempt in 1 2 3 4 5; do
    if docker exec "${CLIENT_A}" timeout "${OP_TIMEOUT_S}" rune ql -f /topics/nat-e2e.rql --broker "rune-2:${BROKER_PORT}"; then ddl_ok=1; break; fi
    log "nat-e2e.rql attempt ${attempt} failed; retrying"; sleep 3
done
[ "${ddl_ok}" = "1" ] || fail "nat-e2e.rql through rune-2 from ${CLIENT_A} failed"

# ---------------------------------------------------------------------------
# Step 6: produce + consume, two clients behind ONE NAT, concurrently.
# ---------------------------------------------------------------------------
step "6 produce+consume"
COMMON=(RUNE_MODE=produce_subscribe "RUNE_RECORDS=${N}" RUNE_KEYS=64 "RUNE_SUB_PARTITIONS=${PARTS}")
dr_start "${CLIENT_A}" ps-a "${RUN_S}" "${TOPIC_TABLE_A}" "${COMMON[@]}" RUNE_CLIENT_ID=1 RUNE_EXPECT_CID_EXACT=1
dr_start "${CLIENT_B}" ps-b "${RUN_S}" "${TOPIC_TABLE_B}" "${COMMON[@]}" RUNE_CLIENT_ID=2 RUNE_EXPECT_CID_EXACT=2
wait_until $(( RUN_S + 30 )) "both produce_subscribe clients" sh -c "[ -s '${WORK_DIR}/ps-a.rc' ] && [ -s '${WORK_DIR}/ps-b.rc' ]" \
    || fail "the produce_subscribe clients did not finish within ${RUN_S}s"
bg_ok ps-a
bg_ok ps-b
for pair in "ps-a:1:2" "ps-b:2:1"; do
    IFS=: read -r tag own other <<< "${pair}"
    bg_out "${tag}" | grep -qE "^SUMMARY produced=${N} delivered=[0-9]+ mode=produce_subscribe rc=0" \
        || { bg_out "${tag}" | tail -5 >&2; fail "${tag}: no 'SUMMARY produced=${N}'"; }
    [ "$(uniq_seqs "${tag}" DELIVERED "${own}")" = "${N}" ] || fail "${tag}: delivered != produced (${N}); distinct seqs $(uniq_seqs "${tag}" DELIVERED "${own}")"
    [ "$(uniq_seqs "${tag}" DELIVERED "${other}")" = "0" ] || fail "${tag}: received the other client's records (cid ${other})"
done
log "two clients behind one NAT: ${N} records each produced (acks=all) and consumed, none crossed"

# ---------------------------------------------------------------------------
# Step 7: a consumer group.
# ---------------------------------------------------------------------------
step "7 group"
dr_run "${CLIENT_B}" group "$(( GROUP_WINDOW_MS / 1000 + 120 ))" "${TOPIC_TABLE_A}" \
    RUNE_MODE=group RUNE_GROUP_ID=nat-g RUNE_CLIENT_ID=3 "RUNE_SUBSCRIBE_MS=${GROUP_WINDOW_MS}"
[ "${DR_RC}" -eq 0 ] && ! echo "${DR_OUT}" | grep -q 'CLIENT: FAIL' || { echo "${DR_OUT}" | tail -20 >&2; fail "group exited ${DR_RC}"; }
[ "$(uniq_seqs group GROUPREC 1)" = "${N}" ] || fail "group nat-g saw $(uniq_seqs group GROUPREC 1) of the ${N} records of nat_a"

# ---------------------------------------------------------------------------
# Step 8: transactions.
# ---------------------------------------------------------------------------
step "8 transactions"
dr_run "${CLIENT_A}" txp 300 "${TOPIC_TABLE_TX}" \
    RUNE_MODE=txn_produce RUNE_TXN_ID=nat-tx "RUNE_RECORDS=${TX_N}" RUNE_KEYS=16 RUNE_CLIENT_ID=4
[ "${DR_RC}" -eq 0 ] && ! echo "${DR_OUT}" | grep -q 'CLIENT: FAIL' || { echo "${DR_OUT}" | tail -20 >&2; fail "txn_produce exited ${DR_RC}"; }
echo "${DR_OUT}" | grep -q "^TXNRESULT txn_id=nat-tx cid=4 outcome=committed" || fail "the transaction nat-tx was not committed"
dr_run "${CLIENT_B}" txc "$(( GROUP_WINDOW_MS / 1000 + 120 ))" "${TOPIC_TABLE_TX}" \
    RUNE_MODE=txn_consume RUNE_GROUP_ID=nat-txg RUNE_ISOLATION=read_committed RUNE_CLIENT_ID=5 "RUNE_SUBSCRIBE_MS=${GROUP_WINDOW_MS}"
[ "${DR_RC}" -eq 0 ] && ! echo "${DR_OUT}" | grep -q 'CLIENT: FAIL' || { echo "${DR_OUT}" | tail -20 >&2; fail "txn_consume exited ${DR_RC}"; }
[ "$(uniq_seqs txc DELIVERED 4)" = "${TX_N}" ] || fail "txn_consume (read_committed) saw $(uniq_seqs txc DELIVERED 4) of the ${TX_N} committed records"

# ---------------------------------------------------------------------------
# Step 9: NEGATIVE CONTROL - the old declared-address path must fail behind this NAT.
# ---------------------------------------------------------------------------
step "9 negative control"
DR_PRE=(env -u RUNE_GATEWAY_RESPONSE_PORT)
dr_run "${CLIENT_A}" neg "${NEG_S}" "${TOPIC_TABLE_A}" \
    RUNE_MODE=produce RUNE_RECORDS=5 RUNE_CLIENT_ID=9 "RUNE_TIMEOUT_MS=${NEG_CONNECT_MS}"
DR_PRE=()
if [ "${DR_RC}" -eq 0 ] && ! echo "${DR_OUT}" | grep -q 'CLIENT: FAIL'; then
    echo "${DR_OUT}" | tail -10 >&2
    fail "the NAT does not isolate the brokers from the private network: the e2e proves nothing"
fi
echo "${DR_OUT}" | grep -q '^PRODUCED ' && fail "the declared-address client produced behind the NAT: the e2e proves nothing"
log "the old-path client failed behind the NAT as it must (exit ${DR_RC}); the response path is what makes steps 6-8 work"

# ---------------------------------------------------------------------------
# Step 10: leader change mid-run.
# ---------------------------------------------------------------------------
step "10 leader change"
dr_start "${CLIENT_A}" lc "${RUN_S}" "${TOPIC_TABLE_A}" \
    RUNE_MODE=produce_subscribe "RUNE_RECORDS=${N}" RUNE_KEYS=64 "RUNE_SUB_PARTITIONS=${PARTS}" "RUNE_RATE_HZ=${RATE_HZ}" \
    RUNE_CLIENT_ID=6 RUNE_EXPECT_CID_EXACT=6
deadline=$(( $(now_s) + PROGRESS_WAIT_S ))
while [ "$(acked lc)" -lt $(( N / 4 )) ]; do
    bg_done lc && { bg_out lc | tail -20 >&2; fail "the run ended at $(acked lc) acked records, before the kill threshold $(( N / 4 ))"; }
    [ "$(now_s)" -lt "${deadline}" ] || { bg_out lc | tail -20 >&2; fail "the run is stuck at $(acked lc) acked records for ${PROGRESS_WAIT_S}s"; }
    sleep 1
done
leader=$(consensus_leader) || fail "no common consensus leader to kill"
at=$(acked lc)
log "consensus leader = node ${leader} (rune-$(( leader + 1 ))) at ${at} acked records; docker kill"
kill_node "${leader}"
deadline=$(( $(now_s) + RESUME_WAIT_S ))
while [ "$(acked lc)" -lt $(( at + RESUME_RECORDS )) ] && ! bg_done lc; do
    [ "$(now_s)" -lt "${deadline}" ] || { bg_out lc | tail -20 >&2; fail "no progress for ${RESUME_WAIT_S}s after the kill (stuck at $(acked lc), killed at ${at})"; }
    sleep 1
done
log "progress resumed without it ($(acked lc) acked records); restarting rune-$(( leader + 1 ))"
restart_node "${leader}"
wait_until "${RUN_S}" "the leader-change client" bg_done lc || { bg_out lc | tail -20 >&2; fail "the client did not finish within ${RUN_S}s"; }
bg_ok lc
bg_out lc | grep -qE "^SUMMARY produced=${N} delivered=[0-9]+ mode=produce_subscribe rc=0" || { bg_out lc | tail -5 >&2; fail "the client did not produce all ${N} records"; }
[ "$(uniq_seqs lc DELIVERED 6)" = "${N}" ] || fail "delivered != produced across the leader change ($(uniq_seqs lc DELIVERED 6) of ${N})"

echo "NAT-E2E: OK"
