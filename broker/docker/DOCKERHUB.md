# Quick reference

- **Maintained by:** [Arcanum](https://github.com/arcanumruna/rune)
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

Every example uses three settings that every Rune container needs:

- `--shm-size=256m`: the broker's embedded media driver maps its buffers into `/dev/shm`, and Docker's 64 MB
  default is too small.
- `-p 40200:40200/udp`: clients reach the broker over **UDP**.
- `-v rune-data:/var/lib/rune`: the broker's data directory. Keep it on a volume.

## Start a development broker

No authentication, single node, data formatted on first start. Never use this mode for anything you want to keep:

```console
$ docker run -d --name rune --shm-size=256m -p 40200:40200/udp \
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

## Start a secured broker

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
$ docker run -d --name rune --shm-size=256m -p 40200:40200/udp \
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

## Run a three-node cluster with `docker compose`

Make `./secrets` as in step 1, with every node's name in the certificate
(`subjectAltName=DNS:rune-1,DNS:rune-2,DNS:rune-3,DNS:localhost,IP:127.0.0.1`), then:

```yaml
x-rune: &rune
  image: arcanumruna/rune
  shm_size: 256m
  ulimits: { nofile: { soft: 1048576, hard: 1048576 } }
  volumes: [ "./secrets:/run/secrets:ro" ]
  environment: &env
    RUNE_PEERS: "0@rune-1:40111:40200:40222,1@rune-2:40111:40200:40222,2@rune-3:40111:40200:40222"
    RUNE_CLIENT_ENDPOINT: "0.0.0.0:40200"
    RUNE_STORAGE_FORMAT: new           # first start only; remove it afterwards
    RUNE_CLUSTER_KEY_FILE: /run/secrets/cluster.key
    RUNE_TLS_CERT_FILE: /run/secrets/tls.pem
    RUNE_TLS_KEY_FILE: /run/secrets/tls.key

services:
  rune-1:
    <<: *rune
    hostname: rune-1
    environment: { <<: *env, RUNE_NODE_ID: "0" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-1:/var/lib/rune" ]
  rune-2:
    <<: *rune
    hostname: rune-2
    environment: { <<: *env, RUNE_NODE_ID: "1" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-2:/var/lib/rune" ]
  rune-3:
    <<: *rune
    hostname: rune-3
    environment: { <<: *env, RUNE_NODE_ID: "2" }
    volumes: [ "./secrets:/run/secrets:ro", "rune-data-3:/var/lib/rune" ]

volumes: { rune-data-1: {}, rune-data-2: {}, rune-data-3: {} }
```

```console
$ docker compose up -d
$ docker compose logs -f        # exactly one node logs "is LEADER"
$ docker compose exec rune-1 rune security bootstrap --admin admin \
    --cluster-key-file /run/secrets/cluster.key --broker rune-1:40200 --tls-ca /run/secrets/ca.pem
```

`RUNE_PEERS` is identical on every node; only `RUNE_NODE_ID` differs. Clients may connect to any node. When the
leader dies, the survivors elect a new one. For nodes on separate hosts, see
[Multi-host](https://github.com/arcanumruna/rune/blob/main/broker/docs/SELF-HOSTING.md#multi-host-nodes-on-separate-machines).

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

| Port | Protocol | Use |
|---|---|---|
| `40200` | UDP | Clients |
| `40111` | UDP | Consensus between nodes |
| `40222` | UDP | Partition replication between nodes |

Publish `40200/udp` to reach clients outside Docker. Nodes reach each other's consensus and replication ports
over a shared network.

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
