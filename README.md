# ports-box

A TCP/UDP port forwarder for Linux with per-user traffic quotas.

- **Users as groups**: each username is a group that can hold multiple port-forwarding rules, and every rule can be enabled/disabled individually; for single-user setups, just write `rules` at the top level
- **Failover (fallback)**: each rule can have multiple backup targets — when the primary target is unavailable it automatically switches by priority and switches back once the primary recovers; rules that include TCP detect recovery via background health checks (every 10 seconds by default)
- **Domain name resolution**: forwarding targets can be domain names, resolved using the host's DNS configuration (`/etc/resolv.conf`, `/etc/hosts`) and cached according to record TTL — after a domain's records change, new connections automatically use the new address without a restart
- **Traffic quotas (optional)**: quotas can be set per user, or a single `total_quota` can be split evenly among users without an individual quota; if neither is set, that user is **unlimited** (usage is recorded but not enforced); usage follows the carrier billing model — each **local wall-clock hour** is a billing period charged as the **larger of inbound and outbound** in that hour (`max(upload, download)`), and billed usage is the sum over hours
- **Hard stop on quota exhaustion**: new connections are rejected, in-flight connections are closed immediately, UDP packets are dropped; service resumes as soon as the quota is raised in the config (no restart needed)
- **Usage persistence (optional)**: with `state_file` configured, used traffic is stored in a SQLite database (flushed every 10 seconds by default and on exit), so counters survive restarts; without it, usage is kept in memory only and resets to zero on restart
- **Config auto-reload**: edits to the config file are picked up automatically without a restart; an invalid config is rejected and the running one is kept (see "Config auto-reload" below)
- **Web panel (optional, off by default)**: a built-in dashboard at `/dashboard` shows usage, rules, and target health, and edits users, rules, and quotas; it sits behind an entry token plus a username and password (see "Web panel" below)
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
| `--no-reload` | Do not watch the config file; changes then take effect only after a restart |

The log level is controlled via `RUST_LOG` (default `ports_box=info`). On SIGTERM / SIGINT, usage is written to the database before exiting.

Every forwarded TCP connection holds two file descriptors and every UDP session one, so at startup the process raises its soft open-file limit (`RLIMIT_NOFILE`) to the hard limit. The bundled systemd unit sets `LimitNOFILE=1048576`.

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

The current version number comes from `package.version` in `Cargo.toml`; when the workflow runs on a `v*` tag, the tag must match that version.

## Docker Compose

First prepare the config file and data directory:

```sh
cp config.example.json config.json
mkdir -p data
sudo chown 10001:10001 data
```

The container runs as the unprivileged user `ports-box` (UID/GID `10001`), so the data directory must be writable by that UID, otherwise `state.db` cannot be created. The binary carries the `cap_net_bind_service` file capability, so it can bind ports below 1024 even under host networking; if you run the container with `--cap-drop ALL`, add `--cap-add NET_BIND_SERVICE` back or the binary will fail to start.

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
  "panel": {
    "enabled": true,
    "listen": "127.0.0.1:8888",
    "token": "changeme",
    "username": "admin",
    "password": "changeme"
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
panel:
  enabled: true
  listen: 127.0.0.1:8888
  token: changeme
  username: admin
  password: changeme
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
| `panel` | Optional; the web panel only starts when `enabled` is explicitly `true` (default `false`). Subfields: `listen` (default `127.0.0.1:8888`), `token` (entry token), `username`, `password` — these three are required once the panel is enabled — and `path` (where the panel stores its changes, default `panel.json`, relative to the working directory). See "Web panel" below |
| `total_quota` | Optional; users without their own `quota` split this total evenly |
| `rules` | Single-user shorthand: a top-level rule list, equivalent to a user named `default`; mutually exclusive with `users`. `rules` or `users` is required unless the panel is enabled |
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
Health checks for all rules run in parallel, with at most 16 probes in flight at once.

**UDP sessions**: each client address gets its own upstream session, closed after 60 seconds without traffic. Sessions are established in the background, so a slow DNS lookup or dead target never delays other clients; packets arriving meanwhile are queued (up to 32) and sent in order once connected. Each `listen` address keeps at most 4096 concurrent sessions; packets from new clients beyond that are dropped.

```json
{ "listen": "0.0.0.0:8080", "target": "10.0.0.2:80", "fallback": ["10.0.0.3:80", "10.0.0.4:80"] }
```

**Domain name resolution**: the host part of `target` / `fallback` may be a domain name, resolved on every new TCP connection, UDP session, and health check.
A built-in resolver reads the host's DNS configuration (nameservers from `/etc/resolv.conf` and `/etc/hosts`), with no glibc dependency — it works the same in musl static builds.
Results are cached according to the DNS record's **TTL**: repeated lookups within the TTL hit the cache, and expired entries are re-queried automatically — after a domain's records change,
**new** connections/sessions use the new address once the old TTL expires, no restart needed; established connections are unaffected and finish naturally.
IP literals (including IPv6 in the `[::1]:80` form) bypass DNS. Invalid domain names or ports are reported as errors at startup.

Quota changes (including adding traffic) take effect as soon as the config file is saved (see "Config auto-reload"); used traffic is kept. With `state_file` enabled, used traffic is also restored from the database after a restart, so restarting does not reset the counters.

## Config auto-reload

The config file is checked every 3 seconds; when its content changes (and stays the same for another 0.5 seconds, so a half-written file is never picked up), it is reloaded without a restart:

- The new config is fully parsed and validated, and every newly needed port is bound, **before** anything changes. If any step fails (syntax error, invalid field, duplicate listen address, port already in use, …), the reload is rejected with an `ERROR` log line and the running config stays in effect. Fixing the file triggers the next reload automatically.
- Unchanged rules keep running untouched, including their established connections.
- Removed or disabled rules stop listening, and their TCP connections and UDP sessions are closed.
- A changed rule (different `target`, `fallback`, `protocol`, `check_secs`, or owner) is restarted: its existing connections are closed and new ones use the new settings. The listening socket is handed over, not rebound, so the port never goes away.
- Quota changes (`quota`, `total_quota`) apply immediately and keep used traffic: raising a quota re-enables an exhausted user, lowering it below the used amount stops that user's traffic at once.
- New users start from the usage stored in `state_file` (if any); removed users have their usage flushed to it first.
- Changes to `api`, `panel`, and `state_file` are not applied live; a warning is logged and they take effect after a restart.

With Docker, a single-file bind mount (`./config.json:/etc/ports-box/config.json`) only sees edits that overwrite the file in place (e.g. `cp new.json config.json`); editors that save by writing a new file and renaming it replace the host file, which the container no longer sees. Mount the directory instead if you edit the config that way. Pass `--no-reload` to turn auto-reload off.

## Web panel

The panel is off unless the config says `"panel": { "enabled": true, ... }`. It listens on `127.0.0.1:8888` by default and needs a `token`, a `username`, and a `password`; startup fails if any of the three is missing.

1. Open `http://127.0.0.1:8888/?token=<token>`. A correct token redirects to `/dashboard`; without it every path on the panel port answers `404`, including `/dashboard` and the sign-in endpoint.
2. Sign in with the username and password.
3. For the next 7 days the browser can open `/dashboard` directly. Sessions live in memory, so a restart requires the token link and a new sign-in.

**Overview** shows each user's quota, billed usage, current-hour usage, and raw upload/download, plus every rule with its targets: which one is in use, which are on standby, and which are down. It refreshes every 3 seconds.

**Users and rules** edits `total_quota`, users, quotas, and rules. Saving validates the result, binds any new ports, and applies it immediately (it works with `--no-reload` too); if anything fails, nothing changes and the error is shown. Things to know:

- With the panel enabled, the config file may leave out `users` and `rules` entirely: the process starts with nothing forwarded, and users and rules are added from the panel.
- Saving **never touches the config file**. The panel keeps its changes in its own store, `panel.path` (default `panel.json`, relative to the working directory), and they survive restarts.
- **Editing the config file takes over again.** Whoever changed things last wins: as soon as the config file's content changes — while running, or while the process was stopped — the file is used as written, the panel's saved changes are dropped, and `panel.json` is deleted. Any change to the file counts, even one outside `users` and `total_quota`.
- If a changed config file is rejected (syntax error, port in use, …), the running config, including the panel's changes, stays in effect until the file is fixed.
- The `api`, `state_file`, and `panel` sections always come from the config file and cannot be edited from the panel.
- The working directory must be writable (as it already is for `state.db`); the config file itself can stay read-only, so a Docker `:ro` mount keeps working. Without host networking, set `panel.listen` to `0.0.0.0:8888` and map the port.

The panel speaks plain HTTP and the token and password are stored in the config file as written. To reach it from another machine, use an SSH tunnel or put it behind an HTTPS reverse proxy; a warning is logged when it listens on a non-loopback address.

## Query API

The token is carried via the `Authorization: Bearer <token>` header or the `?token=<token>` query parameter. If the API listens on a non-loopback address without a token, a warning is logged at startup.

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
Sub-Store treats `upload + download` as used traffic, so the endpoint reports the **billed** usage split between the two directions in proportion to the raw traffic: `upload + download` always equals `used`, and the remaining amount Sub-Store shows matches the API.

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

Config changes (rules, quotas) are applied automatically, see "Config auto-reload"; changes to `api` / `panel` / `state_file` need `systemctl restart ports-box`;
stopping/restarting writes usage to `state.db` first, so no counters are lost.

---

> 🤖 **AI-Assisted Development**
>
> This project is an exploration of AI's project-level coding capabilities. Code generated using **Claude Code(Fable 5)** models via **Visual Studio Code**.
>
