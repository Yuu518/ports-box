# ports-box

面向 Linux 的 TCP/UDP 端口转发器，带用户级流量配额。

- **用户即组**：每个用户名是一个组，组下可挂多条端口转发规则，每条规则可单独启用/禁用
- **流量配额**：可为用户单独设置配额，也可设一个 `total_quota` 由未单独设置的用户静态平分；用量按**入出取大**计费（`max(上行, 下行)`）
- **配额耗尽即停**：新连接被拒绝、传输中的连接立即断开、UDP 包丢弃；调大配额并重启后恢复
- **用量持久化**：已用流量保存在 SQLite 数据库中（默认每 10 秒写入、退出时写入），重启不清零
- **查询 API**：HTTP 接口返回每个用户的总量/已用/剩余；兼容 Sub-Store（`subscription-userinfo` 响应头）

## 构建与运行

```sh
cargo build --release
./target/release/ports-box -c /etc/ports-box/config.json -d /var/lib/ports-box
```

| 参数 | 说明 |
|---|---|
| `-c, --config <FILE>` | 配置文件路径，默认 `./config.json` |
| `-d, --dir <DIR>` | 运行目录，启动时先切换到该目录（配置中的相对路径如 `state_db` 以此为基准） |

日志级别通过 `RUST_LOG` 控制（默认 `ports_box=info`）。收到 SIGTERM / SIGINT 时会先把用量写入数据库再退出。

## 配置文件

```json
{
  "state_db": "state.db",
  "state_flush_secs": 10,
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
        { "listen": "0.0.0.0:8080", "target": "10.0.0.2:80", "tag": "web" },
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

| 字段 | 说明 |
|---|---|
| `state_db` | SQLite 用量数据库路径，默认 `state.db` |
| `state_flush_secs` | 用量落盘间隔（秒），默认 `10` |
| `api` | 可选；省略则不启动查询 API。`token` 也可选，设置后所有请求都要带 token |
| `total_quota` | 可选；未设置 `quota` 的用户平分这个总量 |
| `users[].quota` | 该用户的配额，设置后优先于 `total_quota` 平分值 |
| `rules[].listen` | 监听地址（`IP:端口`） |
| `rules[].target` | 转发目标（`主机:端口`，支持域名） |
| `rules[].protocol` | `both`（默认，同端口同时转发 TCP 和 UDP）/ `tcp` / `udp` |
| `rules[].enabled` | 默认 `true`；设为 `false` 停用该端口 |
| `rules[].tag` | 可选的标签（如 `"web"`），用于标注和分类规则 |

流量值可写整数（字节）或字符串：`"500MB"`、`"10GB"`、`"1.5TB"`（1024 进制，大小写不敏感）。

配额修改（包括增加流量）在**重启后生效**；已用量从数据库恢复，所以重启不会重置配额。

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
