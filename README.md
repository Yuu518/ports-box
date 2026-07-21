# ports-box

A TCP/UDP port forwarder for Linux with per-user traffic quotas.

- **Users as groups**: each username is a group that can hold multiple port-forwarding rules, and every rule can be enabled/disabled individually; for single-user setups, just write `rules` at the top level
- **Failover (fallback)**: each rule can have multiple backup targets — when the primary target is unavailable it automatically switches by priority and switches back once the primary recovers; rules that include TCP detect recovery via background health checks (every 10 seconds by default)
- **Domain name resolution**: forwarding targets can be domain names, resolved using the host's DNS configuration (`/etc/resolv.conf`, `/etc/hosts`) and cached according to record TTL — after a domain's records change, new connections automatically use the new address without a restart
- **Traffic quotas (optional)**: quotas can be set per user, or a single `total_quota` can be split evenly among users without an individual quota; if neither is set, that user is **unlimited** (usage is recorded but not enforced); usage follows the carrier billing model — each **local wall-clock hour** is a billing period charged as the **larger of inbound and outbound** in that hour (`max(upload, download)`), and billed usage is the sum over hours
- **Hard stop on quota exhaustion**: new connections are rejected, in-flight connections are closed immediately, UDP packets are dropped; service resumes after raising the quota and restarting
- **Usage persistence (optional)**: with `state_file` configured, used traffic is stored in a SQLite database (flushed every 10 seconds by default and on exit), so counters survive restarts; without it, usage is kept in memory only and resets to zero on restart
- **Query API**: an HTTP interface returns each user's total/used/remaining traffic; compatible with Sub-Store (`subscription-userinfo` response header)

## Build and run

```sh
cargo build --release
./target/release/ports-box -c /etc/ports-box/config.json -d /var/lib/ports-box
```

| Option | Description |
|---|---|
| `-c, --config <FILE>` | Path to the config file, defaults to `./config.json`; parsed as YAML when the extension is `.yaml`/`.yml`, otherwise as JSON |
| `-d, --dir <DIR>` | Working directory, changed into at startup (relative paths in the config such as `state_file.path` are resolved against it) |

The log level is controlled via `RUST_LOG` (default `ports_box=info`). On SIGTERM / SIGINT, usage is written to the database before exiting.

## Docker image

The image is built by GitHub Actions and pushed to Docker Hub. The repository needs the following Secrets:

| Secret | Description |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token |

After pushing to `main` / `master`, or manually running the `Docker` workflow, two tags are built and pushed:

```text
<DOCKERHUB_USERNAME>/ports-box:<version from Cargo.toml>
<DOCKERHUB_USERNAME>/ports-box:latest
```

The current version number comes from `package.version` in `Cargo.toml`.

## Docker Compose

First prepare the config file and data directory:

```sh
cp config.example.json config.json
mkdir -p data
```

Create `compose.yaml`:

```yaml
services:
  ports-box:
    image: yuu518/ports-box:latest
    container_name: ports-box
    restart: unless-stopped
    network_mode: host
    environment:
      RUST_LOG: ports_box=info
    volumes:
      - ./config.json:/etc/ports-box/config.json:ro
      - ./data:/var/lib/ports-box
    command: ["-c", "/etc/ports-box/config.json", "-d", "/var/lib/ports-box"]
```

Start, follow logs, and stop:

```sh
docker compose up -d
docker compose logs -f
docker compose down
```

Restart after upgrading the image:

```sh
docker compose pull
docker compose up -d
```

`network_mode: host` suits Linux server deployments: the `listen` ports in the config bind directly on the host, so there is no need to write a `ports` mapping for every TCP/UDP rule.
If you don't use host networking, every forwarding port and the API port must be mapped explicitly, for example:

```yaml
ports:
  - "8080:8080/tcp"
  - "5353:5353/udp"
  - "7070:7070/tcp"
```

To build the image locally from source, replace `image` with:

```yaml
build: .
image: ports-box:local
```

## Configuration

Both JSON and YAML formats are supported, distinguished by file extension (`.yaml`/`.yml` for YAML, anything else for JSON), with identical fields. JSON is parsed as [JSON5](https://json5.org/) (a superset of strict JSON): comments (`//`, `/* */`), trailing commas, unquoted keys, and single-quoted strings are all allowed.

Minimal config — single user, plain forwarding, no traffic limit: just write `rules` at the top level (internally equivalent to a user named `default`; mutually exclusive with `users`; if `total_quota` is set, it becomes that user's quota):

```json
{
  "rules": [
    { "listen": "0.0.0.0:8080", "target": "10.0.0.2:80" },
    { "listen": "0.0.0.0:9000", "target": "192.168.1.5:9000", "protocol": "tcp" }
  ]
}
```

Full example:

```json
{
  "state_file": {
    "enabled": true,
    "path": "state.db",
    "flush_secs": 10
  },
  "api": {
    "listen": "127.0.0.1:7070",
    "token": "changeme"
  },
  "total_quota": "100GB",
  "users": [
    {
      "name": "alice",
      "quota": "30GB",
      "rules": [
        { "listen": "0.0.0.0:8080", "target": "10.0.0.2:80", "fallback": "10.0.0.3:80", "tag": "web" },
        { "listen": "0.0.0.0:5353", "target": "10.0.0.2:53", "protocol": "udp" }
      ]
    },
    {
      "name": "bob",
      "rules": [
        { "listen": "0.0.0.0:9000", "target": "192.168.1.5:9000", "protocol": "tcp" },
        { "listen": "0.0.0.0:9001", "target": "192.168.1.5:9001", "enabled": false }
      ]
    }
  ]
}
```

The equivalent YAML (`config.yaml`):

```yaml
state_file:
  enabled: true
  path: state.db
  flush_secs: 10
api:
  listen: 127.0.0.1:7070
  token: changeme
total_quota: 100GB
users:
  - name: alice
    quota: 30GB
    rules:
      - listen: 0.0.0.0:8080
        target: 10.0.0.2:80
        fallback: 10.0.0.3:80
        tag: web
      - listen: 0.0.0.0:5353
        target: 10.0.0.2:53
        protocol: udp
  - name: bob
    rules:
      - listen: 0.0.0.0:9000
        target: 192.168.1.5:9000
        protocol: tcp
      - listen: 0.0.0.0:9001
        target: 192.168.1.5:9001
        enabled: false
```

| Field | Description |
|---|---|
| `state_file` | Optional; omit the whole section to disable usage persistence (counters reset on restart). Subfields: `enabled` (default `true`, set `false` to disable temporarily), `path` (SQLite database path, default `state.db`, relative to the working directory), `flush_secs` (flush interval in seconds, default `10`) |
| `api` | Optional; omit to not start the query API. `token` is also optional; when set, every request must carry the token |
| `total_quota` | Optional; users without their own `quota` split this total evenly |
| `rules` | Single-user shorthand: a top-level rule list, equivalent to a user named `default`; mutually exclusive with `users` |
| `users[].quota` | Optional; this user's quota, taking precedence over the `total_quota` share. When neither `quota` nor `total_quota` is set, the user is unlimited |
| `rules[].listen` | Listen address (`IP:port`) |
| `rules[].target` | Forwarding target (`host:port`, domain names supported — see "Domain name resolution" below) |
| `rules[].fallback` | Optional; backup target(s), a single string or a list (in order as second, third priority, …); switches over automatically when `target` is unavailable and switches back on recovery |
| `rules[].check_secs` | Optional, default `10`; health check interval in seconds. Rules that include TCP probe each target with a TCP connection; `udp` rules cannot be probed, so a failed target instead cools down for this duration before being retried |
| `rules[].protocol` | `both` (default, forwards TCP and UDP on the same port) / `tcp` / `udp` |
| `rules[].enabled` | Default `true`; set `false` to disable the port |
| `rules[].tag` | Optional label (e.g. `"web"`) for annotating and categorizing rules |

Traffic values can be written as integers (bytes) or strings: `"500MB"`, `"10GB"`, `"1.5TB"` (base 1024, case-insensitive).

**Fallback behavior**: targets are tried in priority order `target` → `fallback[0]` → `fallback[1]` …, and the first healthy one is used.
A connection failure (including the 5-second timeout) immediately moves to the next target; once a higher-priority target recovers (detected by health checks), **new** connections automatically switch back — established TCP connections are not interrupted and finish naturally, while UDP sessions are actively terminated so the client's next packet goes back to the primary target. When all targets are down, the primary target keeps being retried.
Pure `udp` rules cannot be probed (the target may not listen on TCP), so they fall back to a passive mode: a failed target cools down for `check_secs` seconds and is retried by new sessions, making the switch-back granularity roughly equal to that cooldown.

```json
{ "listen": "0.0.0.0:8080", "target": "10.0.0.2:80", "fallback": ["10.0.0.3:80", "10.0.0.4:80"] }
```

**Domain name resolution**: the host part of `target` / `fallback` may be a domain name, resolved on every new TCP connection, UDP session, and health check.
A built-in resolver reads the host's DNS configuration (nameservers from `/etc/resolv.conf` and `/etc/hosts`), with no glibc dependency — it works the same in musl static builds.
Results are cached according to the DNS record's **TTL**: repeated lookups within the TTL hit the cache, and expired entries are re-queried automatically — after a domain's records change,
**new** connections/sessions use the new address once the old TTL expires, no restart needed; established connections are unaffected and finish naturally.
IP literals (including IPv6 in the `[::1]:80` form) bypass DNS. Invalid domain names or ports are reported as errors at startup.

Quota changes (including adding traffic) take effect **after a restart**; with `state_file` enabled, used traffic is restored from the database, so restarting does not reset the counters.

## Query API

The token is carried via the `Authorization: Bearer <token>` header or the `?token=<token>` query parameter.

| Endpoint | Description |
|---|---|
| `GET /api/users` | Usage for all users |
| `GET /api/users/{name}` | Usage for a single user |
| `GET /sub/{name}` | Sub-Store-compatible endpoint |

```sh
$ curl "http://127.0.0.1:7070/api/users/alice?token=changeme"
{"name":"alice","total":"30.00GB","used":"1.00MB","hour_used":"120.00KB","remaining":"29.99GB"}
```

`used` is the billed total (sum of each hour's larger direction); `hour_used` is the current hour's billed traffic so far.

For unlimited users, `total` / `remaining` return `"unlimited"`; the Sub-Store endpoint omits the `total=` field.

**Sub-Store usage**: use `http://<host>:7070/sub/alice?token=changeme` as the subscription URL; Sub-Store reads traffic information from the
`subscription-userinfo: upload=…; download=…; total=…` response header.

## Deploying to Linux (/opt/forwarder)

Cross-compile a fully static Linux binary on Windows/macOS (requires [zig](https://ziglang.org/)):

```sh
rustup target add x86_64-unknown-linux-musl
cargo install cargo-zigbuild
cargo zigbuild --release --target x86_64-unknown-linux-musl
# Output: target/x86_64-unknown-linux-musl/release/ports-box
```

On Linux, a plain `cargo build --release` works. Copy the binary and config to the server:

```sh
scp target/x86_64-unknown-linux-musl/release/ports-box root@server:/opt/forwarder/
scp config.json root@server:/opt/forwarder/
scp deploy/ports-box.service root@server:/etc/systemd/system/
```

Enable it on the server (see [deploy/ports-box.service](deploy/ports-box.service) for the systemd unit;
the working directory is `/opt/forwarder`, and `state.db` lives there too):

```sh
chmod +x /opt/forwarder/ports-box
systemctl daemon-reload
systemctl enable --now ports-box
systemctl status ports-box      # check status
journalctl -u ports-box -f      # follow logs
```

After changing the config (e.g. adjusting quotas), apply it with `systemctl restart ports-box`;
stopping/restarting writes usage to `state.db` first, so no counters are lost.

---

> 🤖 **AI-Assisted Development**
>
> This project is an exploration of AI's project-level coding capabilities. Code generated using **Claude Code(Fable 5)** models via **Visual Studio Code**.
>
