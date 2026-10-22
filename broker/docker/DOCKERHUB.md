<p align="center"><img src="https://raw.githubusercontent.com/arcanumruna/rune/main/docs/assets/rune-mark.png" alt="Rune" width="128"></p>

# Quick reference

- **Maintained by:** [Arcanum Realms](https://github.com/arcanumruna/rune)
- **Where to get help:** [GitHub issues](https://github.com/arcanumruna/rune/issues)
- **Source and full documentation:** [github.com/arcanumruna/rune](https://github.com/arcanumruna/rune) ·
  [Self-hosting guide](https://github.com/arcanumruna/rune/blob/main/broker/docs/SELF-HOSTING.md) ·
  [Configuration reference](https://github.com/arcanumruna/rune/blob/main/docs/configuration.md)
- **Supported architectures:** `linux/amd64`

# Supported tags

- `latest`: the newest release
- `X.Y.Z` (for example `0.2.0`): a specific release; pin this in production

# What is Rune?

Rune is an ultra-low-latency durable log for MMORPG backends, and it also acts as a database: materialized views,
typed schemas and compacted tables over the same log. Brokers talk to clients and to each other over UDP (Aeron),
replicate every partition, and fail over on their own.

The image holds the broker (`rune_broker`) and the `rune` CLI. The CLI makes cluster keys, bootstraps the first
admin and runs Rune QL.

# How to use this image

Run Rune with **`--network host`**. It is the low-latency mode: Docker's bridge adds tens of microseconds to p50,
and published ports (`-p`) make p99 about 10x worse. Host networking measures the same as bare metal. It is Linux
only in practice; on Docker Desktop (macOS, Windows) it goes through Desktop's VM.

Every example uses these settings:

- `--network host`: the container shares the host's network stack, so clients reach the broker on the host's UDP ports.
- `--shm-size=256m`: the broker's embedded media driver maps its buffers into `/dev/shm`, and Docker's 64 MB
  default is too small.
- `-v rune-data:/var/lib/rune`: the broker's data directory. Keep it on a volume.

The default ports `40111`, `40200` and `40222` lie in Linux's ephemeral range, so the broker logs a startup warning.
Reserve them on the host once:

```console
$ sudo sysctl -w net.ipv4.ip_local_reserved_ports=40111,40200,40222
```

## Single node

### Start a development broker

No authentication, single node, data formatted on first start. Never use this mode for anything you want to keep:

```console
$ docker run -d --name rune --network host --shm-size=256m \
    -e RUNE_AUTH=disabled -e RUNE_STORAGE_FORMAT=auto \
    -e RUNE_NODE_ID=0 -e RUNE_PEERS=0@127.0.0.1:40111 -e RUNE_CLIENT_ENDPOINT=0.0.0.0:40200 \
    -v rune-data:/var/lib/rune \
    arcanumruna/rune
```

Within a few seconds the log shows `node 0 is LEADER`. Open a Rune QL console inside the container:

```console
$ docker exec -it rune rune console --broker 127.0.0.1:40200 --insecure-plaintext
rune> topic events partitions 1;
```

### Start a secured broker

Authentication is on by default. It needs a cluster key, a TLS certificate and a first admin.

**1. Make the secrets once**, in `./secrets` (the same files go to every node of a cluster):

```console
$ mkdir -p secrets
$ docker run --rm -v "$PWD/secrets:/out" arcanumruna/rune rune security gen-cluster-key --out /out/cluster.key
$ openssl ecparam -name prime256v1 -genkey -noout -out secrets/ca.key
$ openssl req -x509 -new -key secrets/ca.key -sha256 -days 365 -subj "/CN=rune CA" -out secrets/ca.pem
$ openssl ecparam -name prime256v1 -genkey -noout -out secrets/tls.key
$ openssl req -new -key secrets/tls.key -subj "/CN=localhost" -out secrets/tls.csr
$ printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > secrets/san.ext
$ openssl x509 -req -in secrets/tls.csr -CA secrets/ca.pem -CAkey secrets/ca.key -CAcreateserial \
    -sha256 -days 365 -extfile secrets/san.ext -out secrets/tls.pem
```

The certificate must name the host clients use to reach the broker. Use a certificate from your own or a
public CA in production.

**2. Format the data volume once:**

```console
$ docker run --rm -v rune-data:/var/lib/rune arcanumruna/rune rune_broker storage format --new
```

**3. Start the broker:**

```console
$ docker run -d --name rune --network host --shm-size=256m \
    -v "$PWD/secrets:/run/secrets:ro" \
    -e RUNE_CLUSTER_KEY_FILE=/run/secrets/cluster.key \
    -e RUNE_TLS_CERT_FILE=/run/secrets/tls.pem -e RUNE_TLS_KEY_FILE=/run/secrets/tls.key \
    -e RUNE_NODE_ID=0 -e RUNE_PEERS=0@127.0.0.1:40111 -e RUNE_CLIENT_ENDPOINT=0.0.0.0:40200 \
    -v rune-data:/var/lib/rune \
    arcanumruna/rune
```

**4. Bootstrap the first admin.** The one-time password is printed once:

```console
$ docker exec rune rune security bootstrap --admin admin --cluster-key-file /run/secrets/cluster.key \
    --broker 127.0.0.1:40200 --tls-ca /run/secrets/ca.pem
```

Log in, change the password, then create roles, users and grants for your services:

```console
$ docker exec -it rune sh -c 'umask 077; cat > /tmp/admin.pw'     # paste the password, then Ctrl-D
$ docker exec -it rune rune console --broker 127.0.0.1:40200 --tls-ca /run/secrets/ca.pem \
    --user admin --password-file /tmp/admin.pw
rune> ALTER USER admin SET PASSWORD '...';
```

The admin manages security only; it has no data access. Give your services their own users (log in again
after the password change):

```sql
CREATE ROLE game_server;
GRANT CREATE ON CLUSTER TO ROLE game_server;
GRANT READ  ON TOPIC PREFIX 'world_' TO ROLE game_server;
GRANT WRITE ON TOPIC PREFIX 'world_' TO ROLE game_server;
CREATE USER gs1 WITH PASSWORD '...';
GRANT ROLE game_server TO USER gs1;
```

Clients pass `--user` and `--password-file` (or `RUNE_USER` and `RUNE_PASSWORD_FILE`) and trust the CA with
`--tls-ca` (or `RUNE_TLS_CA_FILE`). A password is never accepted on the command line or from the environment.

## Three-node cluster

Three nodes on one host, each in its own container on the host network. Each node gets its own ports, and `RUNE_PEERS`
is identical on every node. Make `./secrets` as in step 1 of the secured broker, then:

```yaml
x-rune: &rune
  image: arcanumruna/rune
  network_mode: host
  shm_size: 256m
  ulimits: { nofile: { soft: 1048576, hard: 1048576 } }
  environment: &env
    RUNE_PEERS: "0@127.0.0.1:40111:40200:40222,1@127.0.0.1:40112:40201:40223,2@127.0.0.1:40113:40202:40224"
    RUNE_STORAGE_FORMAT: new           # first start only; remove it afterwards
    RUNE_CLUSTER_KEY_FILE: /run/secrets/cluster.key
    RUNE_TLS_CERT_FILE: /run/secrets/tls.pem
    RUNE_TLS_KEY_FILE: /run/secrets/tls.key

services:
  rune-1:
    <<: *rune
    environment: { <<: *env, RUNE_NODE_ID: "0", RUNE_CLIENT_ENDPOINT: "0.0.0.0:40200" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-1:/var/lib/rune" ]
  rune-2:
    <<: *rune
    environment: { <<: *env, RUNE_NODE_ID: "1", RUNE_CLIENT_ENDPOINT: "0.0.0.0:40201" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-2:/var/lib/rune" ]
  rune-3:
    <<: *rune
    environment: { <<: *env, RUNE_NODE_ID: "2", RUNE_CLIENT_ENDPOINT: "0.0.0.0:40202" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-3:/var/lib/rune" ]

volumes: { rune-data-1: {}, rune-data-2: {}, rune-data-3: {} }
```

```console
$ sudo sysctl -w net.ipv4.ip_local_reserved_ports=40111-40113,40200-40202,40222-40224
$ docker compose up -d
$ docker compose logs -f        # exactly one node logs "is LEADER"
$ docker compose exec rune-1 rune security bootstrap --admin admin \
    --cluster-key-file /run/secrets/cluster.key --broker 127.0.0.1:40200 --tls-ca /run/secrets/ca.pem
```

`RUNE_PEERS` is identical on every node; only `RUNE_NODE_ID` and the client port differ. Clients may connect to any
node (`127.0.0.1:40200`, `:40201`, `:40202`). When the leader dies, the survivors elect a new one. For one node per
host, use the same ports on every host and real addresses in `RUNE_PEERS`; see
[Multi-host](https://github.com/arcanumruna/rune/blob/main/broker/docs/SELF-HOSTING.md#multi-host-nodes-on-separate-machines).

**Bridge or published ports** work but add latency. If you use them, publish the client port **and** the node's
shard port block (`8200 + 192n` to `8200 + 192n + 191` for node id `n`), because clients reach the shard ports
too.

# Environment variables

| Variable | Required | Description |
|---|---|---|
| `RUNE_NODE_ID` | yes | This node's id (0-based), one of the ids in `RUNE_PEERS`. |
| `RUNE_PEERS` | yes | Every node, self included: `id@host:consensusPort[:clientPort:replicationPort]`, comma-separated. Single node: `0@127.0.0.1:40111`. |
| `RUNE_CLIENT_ENDPOINT` | yes | Raw `host:port` the client gateway binds, usually `0.0.0.0:40200`. Not an `aeron:udp?...` URI. |
| `RUNE_DATA_DIR` | no | Data directory. Default `/var/lib/rune` (the image's volume). |
| `RUNE_STORAGE_FORMAT` | no | `new` (founding a cluster), `join` (new or replaced disk in a running cluster) or `auto` (development). Read only when the data directory is empty. |
| `RUNE_CLUSTER_KEY_FILE` | yes, with authentication | Shared cluster key, the same on every node. Mode `0600` or `0400`. |
| `RUNE_TLS_CERT_FILE`, `RUNE_TLS_KEY_FILE` | yes, with authentication | The broker's TLS certificate and key. Every client session of a secured broker is TLS. |
| `RUNE_AUTH` | no | `enabled` (default) or `disabled`. `disabled` is single-node development only. |

The complete list, including transport and tuning variables, is in the
[configuration reference](https://github.com/arcanumruna/rune/blob/main/docs/configuration.md).

# Ports

All UDP. With `--network host` they are the host's ports.

| Port | Use |
|---|---|
| `40200` | Clients (`RUNE_CLIENT_ENDPOINT`) |
| `40111` | Consensus between nodes |
| `40222` | Partition replication between nodes |
| `8200 + 192n` to `8200 + 192n + 191` | Shard block of node id `n`: ingress at the base, replication at base + 64, egress control at base + 128. Clients reach the ingress ports. |

Nodes on one host use distinct ports (see the cluster example) and never collide on shard blocks. Under bridge
networking, publish the client port and the node's whole shard block.

# Where to store data

The broker keeps everything under `/var/lib/rune`. Mount a named volume or a host directory there.

- **The broker never formats an empty data directory on its own.** Format it once with
  `rune_broker storage format --new` (or `--join` for a new disk in a running cluster), or set
  `RUNE_STORAGE_FORMAT` for that first start. An unformatted directory makes the broker exit with code 78 and
  print the command to run.
- Remove `RUNE_STORAGE_FORMAT=new` after the first start, so an emptied volume is never silently re-formatted.
- Never copy or delete `storage.meta` on its own. See
  [Formatting the data directory](https://github.com/arcanumruna/rune/blob/main/broker/docs/SELF-HOSTING.md#formatting-the-data-directory)
  for disk replacement and backups.

# Caveats

- **Open files:** every partition keeps files open. The broker raises its soft limit to the hard limit and refuses
  to start when the hard limit is too low. Run with `--ulimit nofile=1048576:1048576`.
- **Stopping:** `docker stop` sends SIGTERM, and the broker shuts down cleanly.
- **Membership is static:** the node list is fixed at start.

# License

Rune is released under the
[Arcanum Realms Community License](https://github.com/arcanumruna/rune/blob/main/LICENSE). As with every Docker
image, the image also contains other software under its own licenses (Debian base packages and bundled libraries).
