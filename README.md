# ports-box

面向 Linux 的 TCP/UDP 端口转发器，带用户级流量配额。

- **用户即组**：每个用户名是一个组，组下可挂多条端口转发规则，每条规则可单独启用/禁用；单人使用时顶层直接写 `rules` 即可
- **故障转移（fallback）**：每条规则可配多个备用目标，主目标不可用时自动按优先级切换，恢复后自动切回；含 TCP 的规则通过后台探测（默认每 10 秒）感知恢复
- **域名解析**：转发目标支持域名，使用本机 DNS 配置（`/etc/resolv.conf`、`/etc/hosts`）解析，结果按记录 TTL 缓存——域名指向变更后新连接自动用新地址，无需重启
- **流量配额（可选）**：可为用户单独设置配额，也可设一个 `total_quota` 由未单独设置的用户静态平分；两者都不设则该用户**不限量**（只记录用量，不做限制）；用量按**入出取大**计费（`max(上行, 下行)`）
- **配额耗尽即停**：新连接被拒绝、传输中的连接立即断开、UDP 包丢弃；调大配额并重启后恢复
- **用量持久化（可选）**：配置 `state_file` 后已用流量保存在 SQLite 数据库中（默认每 10 秒写入、退出时写入），重启不清零；不配置则只在内存中计数，重启归零
- **查询 API**：HTTP 接口返回每个用户的总量/已用/剩余；兼容 Sub-Store（`subscription-userinfo` 响应头）

## 构建与运行

```sh
cargo build --release
./target/release/ports-box -c /etc/ports-box/config.json -d /var/lib/ports-box
```

| 参数 | 说明 |
|---|---|
| `-c, --config <FILE>` | 配置文件路径，默认 `./config.json`；扩展名为 `.yaml`/`.yml` 时按 YAML 解析，其余按 JSON |
| `-d, --dir <DIR>` | 运行目录，启动时先切换到该目录（配置中的相对路径如 `state_file.path` 以此为基准） |

日志级别通过 `RUST_LOG` 控制（默认 `ports_box=info`）。收到 SIGTERM / SIGINT 时会先把用量写入数据库再退出。

## Docker 镜像

镜像由 GitHub Actions 构建并推送到 Docker Hub。仓库需要配置以下 Secrets：

| Secret | 说明 |
|---|---|
| `DOCKERHUB_USERNAME` | Docker Hub 用户名 |
| `DOCKERHUB_TOKEN` | Docker Hub access token |

推送到 `main` / `master`，或手动运行 `Docker` workflow 后，会构建并推送两个 tag：

```text
<DOCKERHUB_USERNAME>/ports-box:<Cargo.toml 中的版本号>
<DOCKERHUB_USERNAME>/ports-box:latest
```

当前版本号来自 `Cargo.toml` 的 `package.version`。

## Docker Compose

先准备配置文件和数据目录：

```sh
cp config.example.json config.json
mkdir -p data
```

创建 `compose.yaml`：

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

启动、查看日志和停止：

```sh
docker compose up -d
docker compose logs -f
docker compose down
```

升级镜像后重启：

```sh
docker compose pull
docker compose up -d
```

`network_mode: host` 适合 Linux 服务器部署：配置里的 `listen` 端口会直接监听在宿主机上，不需要为每条 TCP/UDP 规则单独写 `ports` 映射。
如果不用 host 网络，需要把每个转发端口和 API 端口都显式映射出来，例如：

```yaml
ports:
  - "8080:8080/tcp"
  - "5353:5353/udp"
  - "7070:7070/tcp"
```

如果要从源码本地构建镜像，把 `image` 改成：

```yaml
build: .
image: ports-box:local
```

## 配置文件

支持 JSON 和 YAML 两种格式，按扩展名区分（`.yaml`/`.yml` 为 YAML，其余为 JSON），字段完全相同。JSON 按 [JSON5](https://json5.org/) 解析（严格 JSON 的超集）：允许注释（`//`、`/* */`）、尾随逗号、不带引号的键名、单引号字符串。

最简配置——单人使用、单纯转发、不限流量，顶层直接写 `rules` 即可（内部等价于一个名为 `default` 的用户，与 `users` 字段互斥；设了 `total_quota` 就是它的配额）：

```json
{
  "rules": [
    { "listen": "0.0.0.0:8080", "target": "10.0.0.2:80" },
    { "listen": "0.0.0.0:9000", "target": "192.168.1.5:9000", "protocol": "tcp" }
  ]
}
```

完整示例：

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

等价的 YAML 写法（`config.yaml`）：

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

| 字段 | 说明 |
|---|---|
| `state_file` | 可选；省略整节则不持久化用量（重启归零）。子字段：`enabled`（默认 `true`，设 `false` 临时关闭）、`path`（SQLite 数据库路径，默认 `state.db`，相对运行目录）、`flush_secs`（落盘间隔秒数，默认 `10`） |
| `api` | 可选；省略则不启动查询 API。`token` 也可选，设置后所有请求都要带 token |
| `total_quota` | 可选；未设置 `quota` 的用户平分这个总量 |
| `rules` | 单用户简写：顶层规则列表，等价于一个名为 `default` 的用户；与 `users` 互斥 |
| `users[].quota` | 可选；该用户的配额，设置后优先于 `total_quota` 平分值。`quota` 和 `total_quota` 都未设置时该用户不限量 |
| `rules[].listen` | 监听地址（`IP:端口`） |
| `rules[].target` | 转发目标（`主机:端口`，支持域名，见下文「域名解析」） |
| `rules[].fallback` | 可选；备用目标，单个字符串或列表（按顺序作为第二、第三优先级），`target` 不可用时自动切换、恢复后自动切回 |
| `rules[].check_secs` | 可选，默认 `10`；健康探测间隔（秒）。含 TCP 的规则用 TCP 连接探测各目标；`udp` 规则无法探测，改为故障目标冷却该时长后重试 |
| `rules[].protocol` | `both`（默认，同端口同时转发 TCP 和 UDP）/ `tcp` / `udp` |
| `rules[].enabled` | 默认 `true`；设为 `false` 停用该端口 |
| `rules[].tag` | 可选的标签（如 `"web"`），用于标注和分类规则 |

流量值可写整数（字节）或字符串：`"500MB"`、`"10GB"`、`"1.5TB"`（1024 进制，大小写不敏感）。

**fallback 行为**：目标按 `target` → `fallback[0]` → `fallback[1]` … 的优先级取第一个健康的。
连接失败（含 5 秒超时）立即切下一个；更高优先级目标恢复后（探测发现），**新**连接自动切回——已建立的 TCP 连接不打断、自然走完，UDP 会话则主动终止，客户端下个包即回到主目标。全部目标都不可用时持续硬试主目标。
纯 `udp` 规则探测不了（目标未必开 TCP），退化为被动模式：出错的目标冷却 `check_secs` 秒后由新会话重试，回切粒度约等于该冷却期。

```json
{ "listen": "0.0.0.0:8080", "target": "10.0.0.2:80", "fallback": ["10.0.0.3:80", "10.0.0.4:80"] }
```

**域名解析**：`target` / `fallback` 的主机部分可写域名，每次新建 TCP 连接、UDP 会话及健康探测时解析。
使用内置解析器读取本机 DNS 配置（`/etc/resolv.conf` 的 nameserver 与 `/etc/hosts`），不依赖 glibc，musl 静态编译同样生效。
解析结果按 DNS 记录的 **TTL** 缓存：TTL 内的重复解析直接命中缓存，过期后自动重新查询——域名指向变更后，
**新**连接/会话在旧 TTL 过期后即用新地址，无需重启；已建立的连接不受影响，自然走完。
IP 字面量（含 `[::1]:80` 形式的 IPv6）不经过 DNS。域名或端口格式非法会在启动时报错。

配额修改（包括增加流量）在**重启后生效**；启用 `state_file` 时已用量从数据库恢复，重启不会重置计数。

## 查询 API

token 通过 `Authorization: Bearer <token>` 头或 `?token=<token>` 查询参数携带。

| 端点 | 说明 |
|---|---|
| `GET /api/users` | 所有用户的用量 |
| `GET /api/users/{name}` | 单个用户的用量 |
| `GET /sub/{name}` | Sub-Store 兼容端点 |

```sh
$ curl "http://127.0.0.1:7070/api/users/alice?token=changeme"
{"name":"alice","total":"30.00GB","used":"1.00MB","remaining":"29.99GB"}
```

不限量用户的 `total` / `remaining` 返回 `"unlimited"`；Sub-Store 端点则省略 `total=` 字段。

**Sub-Store 用法**：把 `http://<主机>:7070/sub/alice?token=changeme` 填为订阅链接，Sub-Store 会从响应的
`subscription-userinfo: upload=…; download=…; total=…` 头中读取流量信息。

## 部署到 Linux（/opt/forwarder）

在 Windows/macOS 上交叉编译全静态 Linux 二进制（需要 [zig](https://ziglang.org/)）：

```sh
rustup target add x86_64-unknown-linux-musl
cargo install cargo-zigbuild
cargo zigbuild --release --target x86_64-unknown-linux-musl
# 产物：target/x86_64-unknown-linux-musl/release/ports-box
```

在 Linux 上直接 `cargo build --release` 即可。将二进制和配置放到服务器：

```sh
scp target/x86_64-unknown-linux-musl/release/ports-box root@server:/opt/forwarder/
scp config.json root@server:/opt/forwarder/
scp deploy/ports-box.service root@server:/etc/systemd/system/
```

服务器上启用（systemd 单元见 [deploy/ports-box.service](deploy/ports-box.service)，
工作目录为 `/opt/forwarder`，state.db 也落在该目录）：

```sh
chmod +x /opt/forwarder/ports-box
systemctl daemon-reload
systemctl enable --now ports-box
systemctl status ports-box      # 查看状态
journalctl -u ports-box -f      # 跟踪日志
```

修改配置（如调整配额）后 `systemctl restart ports-box` 生效；
停止/重启会先把用量写入 state.db，不会丢计数。
