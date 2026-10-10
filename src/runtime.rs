use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{Semaphore, mpsc, watch};
use tracing::{info, warn};

use crate::config::{self, Config, Protocol, format_size};
use crate::pool::{self, TargetPool};
use crate::quota::UserQuota;
use crate::state::StateDb;
use crate::{tcp, udp};

pub const RELOAD_POLL: Duration = Duration::from_secs(3);
const RELOAD_SETTLE: Duration = Duration::from_millis(500);

pub async fn stopped(mut stop: watch::Receiver<()>) {
    while stop.changed().await.is_ok() {}
}

#[cfg(test)]
pub fn never_stop() -> watch::Receiver<()> {
    let (tx, rx) = watch::channel(());
    std::mem::forget(tx);
    rx
}

#[derive(Default)]
pub struct UserSet {
    ordered: Vec<Arc<UserQuota>>,
    by_name: HashMap<String, Arc<UserQuota>>,
}

impl UserSet {
    pub fn new(ordered: Vec<Arc<UserQuota>>) -> Self {
        let by_name = ordered
            .iter()
            .map(|u| (u.name.clone(), u.clone()))
            .collect();
        Self { ordered, by_name }
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<UserQuota>> {
        self.ordered.iter()
    }

    pub fn as_slice(&self) -> &[Arc<UserQuota>] {
        &self.ordered
    }

    pub fn get(&self, name: &str) -> Option<&Arc<UserQuota>> {
        self.by_name.get(name)
    }

    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct RuleSpec {
    user: String,
    listen: SocketAddr,
    protocol: Protocol,
    targets: Vec<String>,
    check_secs: u64,
}

impl RuleSpec {
    fn describe(&self) -> String {
        let (target, fallback) = self.targets.split_first().expect("rule has a target");
        if fallback.is_empty() {
            format!("{} {} -> {}", self.protocol, self.listen, target)
        } else {
            format!(
                "{} {} -> {} (fallback: {})",
                self.protocol,
                self.listen,
                target,
                fallback.join(", "),
            )
        }
    }
}

pub type PoolMap = HashMap<(SocketAddr, Protocol), Arc<TargetPool>>;

struct RunningRule {
    _stop: watch::Sender<()>,
    pool: Arc<TargetPool>,
    tcp: Option<Arc<TcpListener>>,
    udp: Option<Arc<UdpSocket>>,
}

struct PreparedRule {
    spec: RuleSpec,
    tcp: Option<Arc<TcpListener>>,
    udp: Option<Arc<UdpSocket>>,
}

pub struct Runtime {
    rules: HashMap<RuleSpec, RunningRule>,
    users: watch::Sender<Arc<UserSet>>,
    pools: watch::Sender<Arc<PoolMap>>,
    db: Option<Arc<StateDb>>,
    permits: Arc<Semaphore>,
}

impl Runtime {
    pub fn new(db: Option<Arc<StateDb>>) -> Self {
        Self {
            rules: HashMap::new(),
            users: watch::Sender::new(Arc::default()),
            pools: watch::Sender::new(Arc::default()),
            db,
            permits: pool::probe_permits(),
        }
    }

    pub fn users(&self) -> watch::Receiver<Arc<UserSet>> {
        self.users.subscribe()
    }

    pub fn pools(&self) -> watch::Receiver<Arc<PoolMap>> {
        self.pools.subscribe()
    }

    pub async fn apply(&mut self, config: &Config) -> Result<(), String> {
        let current = self.users.borrow().clone();
        let first = current.is_empty() && self.rules.is_empty();
        let quotas = config::resolve_quotas(config);

        let mut specs = Vec::new();
        for user in &config.users {
            for rule in &user.rules {
                if !rule.enabled {
                    if first {
                        info!(user = %user.name, listen = %rule.listen, "rule disabled, skipping");
                    }
                    continue;
                }
                specs.push(RuleSpec {
                    user: user.name.clone(),
                    listen: rule.listen,
                    protocol: rule.protocol,
                    targets: std::iter::once(&rule.target)
                        .chain(&rule.fallback)
                        .cloned()
                        .collect(),
                    check_secs: rule.check_secs,
                });
            }
        }
        let wanted: HashSet<&RuleSpec> = specs.iter().collect();
        let removed: Vec<RuleSpec> = self
            .rules
            .keys()
            .filter(|spec| !wanted.contains(spec))
            .cloned()
            .collect();

        let mut spare_tcp = HashMap::new();
        let mut spare_udp = HashMap::new();
        for spec in &removed {
            let rule = &self.rules[spec];
            if let Some(listener) = &rule.tcp {
                spare_tcp.insert(spec.listen, listener.clone());
            }
            if let Some(socket) = &rule.udp {
                spare_udp.insert(spec.listen, socket.clone());
            }
        }
        let mut added = Vec::new();
        for spec in specs {
            if self.rules.contains_key(&spec) {
                continue;
            }
            let tcp = match (spec.protocol.tcp(), spare_tcp.remove(&spec.listen)) {
                (false, _) => None,
                (true, Some(listener)) => Some(listener),
                (true, None) => {
                    Some(Arc::new(TcpListener::bind(spec.listen).await.map_err(
                        |e| format!("cannot bind tcp {}: {e}", spec.listen),
                    )?))
                }
            };
            let udp = match (spec.protocol.udp(), spare_udp.remove(&spec.listen)) {
                (false, _) => None,
                (true, Some(socket)) => Some(socket),
                (true, None) => {
                    Some(Arc::new(UdpSocket::bind(spec.listen).await.map_err(
                        |e| format!("cannot bind udp {}: {e}", spec.listen),
                    )?))
                }
            };
            added.push(PreparedRule { spec, tcp, udp });
        }
        drop((spare_tcp, spare_udp));

        let new_users = config
            .users
            .iter()
            .any(|user| current.get(&user.name).is_none());
        let saved = match &self.db {
            Some(db) if new_users => {
                let db = db.clone();
                tokio::task::spawn_blocking(move || db.load())
                    .await
                    .map_err(|e| format!("state load task failed: {e}"))??
            }
            _ => HashMap::new(),
        };

        let mut ordered = Vec::with_capacity(config.users.len());
        for user in &config.users {
            let limit = quotas[&user.name];
            let quota = match current.get(&user.name) {
                Some(quota) => {
                    if quota.limit() != limit {
                        info!(
                            user = %user.name,
                            "quota changed from {} to {}",
                            describe_limit(quota.limit()),
                            describe_limit(limit),
                        );
                        quota.set_limit(limit);
                        log_quota(quota);
                    }
                    quota.clone()
                }
                None => {
                    let usage = saved.get(&user.name).copied().unwrap_or_default();
                    let quota = Arc::new(UserQuota::new(user.name.clone(), limit, usage));
                    log_quota(&quota);
                    quota
                }
            };
            ordered.push(quota);
        }
        let next = Arc::new(UserSet::new(ordered));

        for spec in &removed {
            self.rules.remove(spec);
            info!(user = %spec.user, "rule removed: {}", spec.describe());
        }
        let added_count = added.len();
        for rule in added {
            let quota = next
                .get(&rule.spec.user)
                .expect("rule owner exists")
                .clone();
            self.start(rule, quota);
        }

        let gone: Vec<Arc<UserQuota>> = current
            .iter()
            .filter(|user| next.get(&user.name).is_none())
            .cloned()
            .collect();
        if !gone.is_empty() {
            if let Some(db) = &self.db {
                let db = db.clone();
                let users = gone.clone();
                match tokio::task::spawn_blocking(move || db.flush(&users)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!("cannot flush usage of removed users: {e}"),
                    Err(e) => warn!("flush of removed users panicked: {e}"),
                }
            }
            for user in &gone {
                info!(user = %user.name, "user removed");
            }
        }
        self.users.send_replace(next);
        self.pools.send_replace(Arc::new(
            self.rules
                .iter()
                .map(|(spec, rule)| ((spec.listen, spec.protocol), rule.pool.clone()))
                .collect(),
        ));

        if !first {
            info!(
                "config reloaded: {} rules added, {} removed, {} unchanged",
                added_count,
                removed.len(),
                self.rules.len() - added_count,
            );
        }
        Ok(())
    }

    fn start(&mut self, rule: PreparedRule, quota: Arc<UserQuota>) {
        let PreparedRule { spec, tcp, udp } = rule;
        let (stop_tx, stop) = watch::channel(());
        let has_fallback = spec.targets.len() > 1;
        let check_interval = Duration::from_secs(spec.check_secs);
        // UDP-only targets cannot be probed over TCP; they recover via
        // a retry cooldown instead of the probe task.
        let cooldown = (has_fallback && !spec.protocol.tcp()).then_some(check_interval);
        let pool = TargetPool::new(spec.user.clone(), spec.targets.clone(), cooldown);
        if has_fallback && spec.protocol.tcp() {
            tokio::spawn(pool::probe(
                pool.clone(),
                check_interval,
                self.permits.clone(),
                stop.clone(),
            ));
        }
        if let Some(listener) = &tcp {
            tokio::spawn(tcp::serve(
                listener.clone(),
                pool.clone(),
                quota.clone(),
                stop.clone(),
            ));
        }
        if let Some(socket) = &udp {
            tokio::spawn(udp::serve(socket.clone(), pool.clone(), quota, stop));
        }
        info!(user = %spec.user, "{}", spec.describe());
        self.rules.insert(
            spec,
            RunningRule {
                _stop: stop_tx,
                pool,
                tcp,
                udp,
            },
        );
    }
}

fn describe_limit(limit: Option<u64>) -> String {
    limit.map_or_else(|| "unlimited".into(), format_size)
}

fn log_quota(quota: &UserQuota) {
    match quota.limit() {
        None => info!(
            user = %quota.name,
            "quota unlimited ({} used)",
            format_size(quota.used()),
        ),
        Some(limit) if quota.is_exhausted() => warn!(
            user = %quota.name,
            "quota already exhausted ({} used of {})",
            format_size(quota.used()),
            format_size(limit),
        ),
        Some(limit) => info!(
            user = %quota.name,
            "quota {} ({} used, {} remaining)",
            format_size(limit),
            format_size(quota.used()),
            format_size(quota.remaining().unwrap_or(0)),
        ),
    }
}

pub async fn watch_config(
    path: PathBuf,
    mut last: String,
    interval: Duration,
    changes: mpsc::Sender<String>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    let mut read_failed = false;
    loop {
        ticker.tick().await;
        let raw = match tokio::fs::read_to_string(&path).await {
            Ok(raw) => raw,
            Err(e) => {
                if !read_failed {
                    warn!("cannot read config {}: {e}", path.display());
                    read_failed = true;
                }
                continue;
            }
        };
        read_failed = false;
        if raw == last {
            continue;
        }
        tokio::time::sleep(RELOAD_SETTLE.min(interval)).await;
        match tokio::fs::read_to_string(&path).await {
            Ok(again) if again == raw => {}
            _ => continue,
        }
        last = raw.clone();
        if changes.send(raw).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::*;
    use crate::quota::Direction;

    const WAIT: Duration = Duration::from_secs(5);

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    async fn tagged_server(tag: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    stream.write_all(tag).await.unwrap();
                    let (mut r, mut w) = stream.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });
        addr
    }

    async fn greeting(port: u16) -> std::io::Result<(TcpStream, Vec<u8>)> {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let mut buf = [0u8; 1];
        timeout(WAIT, stream.read_exact(&mut buf))
            .await
            .map_err(|_| std::io::Error::other("timed out"))??;
        Ok((stream, buf.to_vec()))
    }

    fn parse(json: &str) -> Config {
        config::parse(json, Path::new("config.json")).unwrap()
    }

    fn rules(rules: &[(u16, &str)]) -> Config {
        let rules: Vec<String> = rules
            .iter()
            .map(|(port, target)| {
                format!(
                    r#"{{"listen": "127.0.0.1:{port}", "target": "{target}", "protocol": "tcp"}}"#
                )
            })
            .collect();
        parse(&format!(r#"{{"rules": [{}]}}"#, rules.join(",")))
    }

    #[tokio::test]
    async fn changed_target_takes_over_listening_socket() {
        let (a, b) = (tagged_server(b"a").await, tagged_server(b"b").await);
        let port = free_port();
        let mut runtime = Runtime::new(None);
        runtime.apply(&rules(&[(port, &a)])).await.unwrap();
        assert_eq!(greeting(port).await.unwrap().1, b"a");

        runtime.apply(&rules(&[(port, &b)])).await.unwrap();

        assert_eq!(greeting(port).await.unwrap().1, b"b");
    }

    #[tokio::test]
    async fn removed_rule_closes_port_and_connections() {
        let a = tagged_server(b"a").await;
        let (kept, dropped) = (free_port(), free_port());
        let mut runtime = Runtime::new(None);
        runtime
            .apply(&rules(&[(kept, &a), (dropped, &a)]))
            .await
            .unwrap();
        let (mut live, _) = greeting(dropped).await.unwrap();

        runtime.apply(&rules(&[(kept, &a)])).await.unwrap();

        let mut buf = [0u8; 8];
        let read = timeout(WAIT, live.read(&mut buf)).await.unwrap();
        assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");
        timeout(WAIT, async {
            while greeting(dropped).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(greeting(kept).await.unwrap().1, b"a");
    }

    #[tokio::test]
    async fn bind_failure_rejects_whole_reload() {
        let (a, b) = (tagged_server(b"a").await, tagged_server(b"b").await);
        let port = free_port();
        let busy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let busy_port = busy.local_addr().unwrap().port();
        let mut runtime = Runtime::new(None);
        runtime.apply(&rules(&[(port, &a)])).await.unwrap();

        let err = runtime
            .apply(&rules(&[(port, &b), (busy_port, &b)]))
            .await
            .unwrap_err();

        assert!(err.contains("cannot bind tcp"), "{err}");
        assert_eq!(greeting(port).await.unwrap().1, b"a");
        let (mut stream, _) = greeting(port).await.unwrap();
        stream.write_all(b"x").await.unwrap();
        let mut buf = [0u8; 1];
        timeout(WAIT, stream.read_exact(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf, b"x");
    }

    #[tokio::test]
    async fn quota_changes_apply_to_live_users() {
        let users = |quota: u64| {
            parse(&format!(
                r#"{{"users": [{{"name": "a", "quota": {quota}, "rules": [{{"listen": "127.0.0.1:{}", "target": "x:1"}}]}}]}}"#,
                free_port(),
            ))
        };
        let mut runtime = Runtime::new(None);
        runtime.apply(&users(10)).await.unwrap();
        let before = runtime.users().borrow().get("a").unwrap().clone();
        assert!(!before.try_consume(10, Direction::Upload));
        assert!(before.is_exhausted());

        runtime.apply(&users(1000)).await.unwrap();

        let after = runtime.users().borrow().get("a").unwrap().clone();
        assert!(Arc::ptr_eq(&before, &after));
        assert!(!after.is_exhausted());
        assert_eq!(after.remaining(), Some(990));
    }

    #[tokio::test]
    async fn removed_user_usage_survives_readd() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(StateDb::open(&dir.path().join("state.db")).unwrap());
        let (pa, pb) = (free_port(), free_port());
        let both = parse(&format!(
            r#"{{"users": [
                {{"name": "a", "rules": [{{"listen": "127.0.0.1:{pa}", "target": "x:1"}}]}},
                {{"name": "b", "rules": [{{"listen": "127.0.0.1:{pb}", "target": "x:1"}}]}}
            ]}}"#
        ));
        let only_a = parse(&format!(
            r#"{{"users": [{{"name": "a", "rules": [{{"listen": "127.0.0.1:{pa}", "target": "x:1"}}]}}]}}"#
        ));
        let mut runtime = Runtime::new(Some(db));
        runtime.apply(&both).await.unwrap();
        runtime
            .users()
            .borrow()
            .get("b")
            .unwrap()
            .try_consume(5, Direction::Upload);

        runtime.apply(&only_a).await.unwrap();
        assert!(runtime.users().borrow().get("b").is_none());
        runtime.apply(&both).await.unwrap();

        assert_eq!(runtime.users().borrow().get("b").unwrap().upload(), 5);
        let names: Vec<String> = runtime
            .users()
            .borrow()
            .iter()
            .map(|u| u.name.clone())
            .collect();
        assert_eq!(names, ["a", "b"]);
    }

    #[tokio::test]
    async fn watcher_reports_only_changed_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(&path, "one").unwrap();
        let (tx, mut rx) = mpsc::channel(1);
        let watcher = tokio::spawn(watch_config(
            path.clone(),
            "one".into(),
            Duration::from_millis(20),
            tx,
        ));
        assert!(
            timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_err()
        );

        std::fs::write(&path, "two").unwrap();

        assert_eq!(timeout(WAIT, rx.recv()).await.unwrap().unwrap(), "two");
        assert!(
            timeout(Duration::from_millis(200), rx.recv())
                .await
                .is_err()
        );
        watcher.abort();
    }
}
