use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::config::{Accounting, Config, Mode, Provider, RemoteConfig, format_size};
use crate::ledger::{Assignments, Book, Entry, Member, PackageRecord};
use crate::panel::Live;
use crate::provider::{self, Fetched, Package};
use crate::runtime::UserSet;
use crate::state::StateDb;

pub const RECENT_ENTRIES: usize = 500;
const RETRY: Duration = Duration::from_secs(60);

type Fetch = Arc<
    dyn Fn(RemoteConfig) -> Pin<Box<dyn Future<Output = Result<Fetched, String>> + Send>>
        + Send
        + Sync,
>;

fn provider_fetch() -> Fetch {
    Arc::new(|remote| Box::pin(async move { provider::fetch(&remote).await }))
}

#[derive(Default)]
pub struct RemoteLimits(RwLock<Option<HashMap<String, u64>>>);

impl RemoteLimits {
    pub fn new(book: &Book) -> Arc<Self> {
        let limits = Self::default();
        if book.migrated {
            limits.set(book.limits());
        }
        Arc::new(limits)
    }

    pub fn snapshot(&self) -> Option<HashMap<String, u64>> {
        self.0.read().unwrap().clone()
    }

    fn set(&self, limits: HashMap<String, u64>) {
        *self.0.write().unwrap() = Some(limits);
    }
}

pub enum Command {
    Sync(oneshot::Sender<Result<(), String>>),
    Transfer {
        from: String,
        to: String,
        amount: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    Allocate {
        to: Option<String>,
        amount: Option<u64>,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

#[derive(Clone, Default, Serialize)]
pub struct Status {
    pub mode: Mode,
    pub provider: Option<Provider>,
    pub regions: Vec<String>,
    pub migrated: bool,
    pub last_attempt: Option<i64>,
    pub last_error: Option<String>,
    pub synced_at: Option<i64>,
    pub remaining: u64,
    pub unallocated: i64,
    pub consumed: u64,
    pub metered: u64,
    pub balances: BTreeMap<String, i64>,
    pub packages: Vec<PackageRecord>,
    pub ignored: Vec<Package>,
    #[serde(skip)]
    pub entries: Arc<Vec<Entry>>,
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn assignments(config: &Config) -> Assignments {
    Assignments {
        split: config
            .accounting
            .remote
            .iter()
            .flat_map(|remote| remote.packages.iter().cloned())
            .collect(),
        owners: config
            .users
            .iter()
            .flat_map(|user| {
                user.packages
                    .iter()
                    .map(|id| (id.clone(), user.name.clone()))
            })
            .collect(),
        exclusive: config.single_user,
    }
}

#[derive(PartialEq)]
struct Watched {
    accounting: Accounting,
    single_user: bool,
    owners: Vec<(String, Vec<String>)>,
}

impl Watched {
    fn of(config: &Config) -> Self {
        Self {
            accounting: config.accounting.clone(),
            single_user: config.single_user,
            owners: config
                .users
                .iter()
                .map(|user| (user.name.clone(), user.packages.clone()))
                .collect(),
        }
    }
}

pub struct Engine {
    db: Option<Arc<StateDb>>,
    live: watch::Receiver<Arc<Live>>,
    users: watch::Receiver<Arc<UserSet>>,
    limits: Arc<RemoteLimits>,
    status: watch::Sender<Arc<Status>>,
    book: Book,
    entries: VecDeque<Entry>,
    last_attempt: Option<i64>,
    last_error: Option<String>,
    ignored: Vec<Package>,
    fetch: Fetch,
}

impl Engine {
    pub fn new(
        db: Option<Arc<StateDb>>,
        live: watch::Receiver<Arc<Live>>,
        users: watch::Receiver<Arc<UserSet>>,
        limits: Arc<RemoteLimits>,
        book: Book,
        entries: Vec<Entry>,
    ) -> Self {
        let engine = Self {
            db,
            live,
            users,
            limits,
            status: watch::Sender::new(Arc::default()),
            book,
            entries: entries.into(),
            last_attempt: None,
            last_error: None,
            ignored: Vec::new(),
            fetch: provider_fetch(),
        };
        engine.publish();
        engine
    }

    #[cfg(test)]
    fn with_fetch(mut self, fetch: Fetch) -> Self {
        self.fetch = fetch;
        self
    }

    pub fn status(&self) -> watch::Receiver<Arc<Status>> {
        self.status.subscribe()
    }

    fn config(&self) -> Config {
        self.live.borrow().config.clone()
    }

    fn members(&self, config: &Config) -> Vec<Member> {
        let users = self.users.borrow().clone();
        config
            .users
            .iter()
            .filter_map(|user| users.get(&user.name))
            .map(|quota| Member {
                name: quota.name.clone(),
                used: quota.used(),
                offline_remaining: quota.remaining(),
            })
            .collect()
    }

    fn publish(&self) {
        let config = self.config();
        let remote = config.accounting.remote.as_ref();
        self.status.send_replace(Arc::new(Status {
            mode: config.accounting.mode,
            provider: remote.map(|remote| remote.provider),
            regions: remote
                .map(|remote| remote.regions.clone())
                .unwrap_or_default(),
            migrated: self.book.migrated,
            last_attempt: self.last_attempt,
            last_error: self.last_error.clone(),
            synced_at: self.book.migrated.then_some(self.book.synced_at),
            remaining: self.book.remaining,
            unallocated: self.book.unallocated(),
            consumed: self.book.consumed,
            metered: self.book.metered,
            balances: self.book.balances.clone(),
            packages: self.book.packages.values().cloned().collect(),
            ignored: self.ignored.clone(),
            entries: Arc::new(self.entries.iter().rev().cloned().collect()),
        }));
    }

    fn apply_limits(&self, config: &Config) {
        if !self.book.migrated {
            return;
        }
        self.limits.set(self.book.limits());
        if config.accounting.active_remote().is_none() {
            return;
        }
        for quota in self.users.borrow().iter() {
            let limit = self.book.limit(&quota.name).unwrap_or_else(|| quota.used());
            if quota.limit() != Some(limit) {
                quota.set_limit(Some(limit));
            }
        }
    }

    async fn commit(&mut self, book: Book, entries: Vec<Entry>) -> Result<(), String> {
        if let Some(db) = &self.db {
            let db = db.clone();
            let saved = book.clone();
            let batch = entries.clone();
            tokio::task::spawn_blocking(move || db.save_book(&saved, &batch))
                .await
                .map_err(|e| format!("ledger save task failed: {e}"))?
                .map_err(|e| format!("cannot save ledger: {e}"))?;
        }
        self.book = book;
        for entry in entries {
            if self.entries.len() == RECENT_ENTRIES {
                self.entries.pop_front();
            }
            self.entries.push_back(entry);
        }
        Ok(())
    }

    async fn sync(&mut self) -> Result<(), String> {
        let config = self.config();
        let result = match config.accounting.active_remote() {
            Some(remote) => {
                self.last_attempt = Some(unix_now());
                let remote = remote.clone();
                self.sync_with(&config, &remote).await
            }
            None => Err("remote accounting is off".into()),
        };
        match &result {
            Ok(()) => self.last_error = None,
            Err(e) => {
                warn!("remote sync failed: {e}");
                self.last_error = Some(e.clone());
            }
        }
        self.publish();
        result
    }

    async fn sync_with(&mut self, config: &Config, remote: &RemoteConfig) -> Result<(), String> {
        if !self.book.migrated && self.members(config).is_empty() {
            return Err("waiting for a user before remote accounting can start".into());
        }
        let fetched = (self.fetch)(remote.clone()).await?;
        if fetched.ignored != self.ignored && !fetched.ignored.is_empty() {
            info!(
                "ignoring {} traffic packages outside {}",
                fetched.ignored.len(),
                remote.regions.join(", ")
            );
        }
        self.ignored = fetched.ignored;
        if !self.book.migrated && !fetched.packages.iter().any(Package::effective) {
            return Err(format!(
                "no effective traffic packages in {}",
                remote.regions.join(", ")
            ));
        }
        let members = self.members(config);
        let mut book = self.book.clone();
        let entries = book.reconcile(fetched.packages, &members, &assignments(config), unix_now());
        let started = !self.book.migrated;
        self.commit(book, entries).await?;
        if started {
            info!(
                "remote accounting started with {} across {} users",
                format_size(self.book.remaining),
                members.len()
            );
        }
        self.apply_limits(config);
        Ok(())
    }

    async fn transfer(&mut self, from: &str, to: &str, amount: u64) -> Result<(), String> {
        let config = self.config();
        let members = self.members(&config);
        let mut book = self.book.clone();
        let entries = book.transfer(&members, from, to, amount, unix_now())?;
        self.commit(book, entries).await?;
        info!("transferred {} from {from} to {to}", format_size(amount));
        self.apply_limits(&config);
        self.publish();
        Ok(())
    }

    async fn allocate(&mut self, to: Option<&str>, amount: Option<u64>) -> Result<(), String> {
        let config = self.config();
        let members = self.members(&config);
        let mut book = self.book.clone();
        let entries = book.allocate(&members, to, amount, unix_now())?;
        let total: i64 = entries.iter().map(|entry| entry.amount).sum();
        self.commit(book, entries).await?;
        info!(
            "allocated {} of unallocated traffic to {}",
            format_size(total.max(0) as u64),
            to.unwrap_or("all users")
        );
        self.apply_limits(&config);
        self.publish();
        Ok(())
    }

    fn next_sync(&self, result: &Result<(), String>) -> Option<Instant> {
        let config = self.config();
        let remote = config.accounting.active_remote()?;
        let wait = match result {
            Ok(()) => Duration::from_secs(remote.interval_secs),
            Err(_) => RETRY.min(Duration::from_secs(remote.interval_secs)),
        };
        Some(Instant::now() + wait)
    }

    pub async fn run(mut self, mut commands: mpsc::Receiver<Command>) {
        let mut watched = Watched::of(&self.config());
        let mut next = self
            .config()
            .accounting
            .active_remote()
            .map(|_| Instant::now());
        loop {
            let at = next;
            let due = async move {
                match at {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = due => {
                    let result = self.sync().await;
                    next = self.next_sync(&result);
                }
                changed = self.live.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let config = self.config();
                    let now = Watched::of(&config);
                    if now != watched {
                        watched = now;
                        next = config.accounting.active_remote().map(|_| Instant::now());
                    }
                    self.apply_limits(&config);
                    self.publish();
                }
                command = commands.recv() => match command {
                    Some(Command::Sync(reply)) => {
                        let result = self.sync().await;
                        next = self.next_sync(&result);
                        let _ = reply.send(result);
                    }
                    Some(Command::Transfer { from, to, amount, reply }) => {
                        let _ = reply.send(self.transfer(&from, &to, amount).await);
                    }
                    Some(Command::Allocate { to, amount, reply }) => {
                        let _ = reply.send(self.allocate(to.as_deref(), amount).await);
                    }
                    None => return,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::config::Metering;
    use crate::quota::{Direction, SavedUsage, UserQuota};

    fn live(json: &str) -> watch::Sender<Arc<Live>> {
        let config = crate::config::parse(json, Path::new("config.json")).unwrap();
        watch::Sender::new(Arc::new(Live {
            config,
            revision: 0,
            from_panel: false,
        }))
    }

    const REMOTE: &str = r#"{
        "accounting": {"mode": "remote", "remote": {"provider": "volcengine", "access_key": "ak", "secret_key": "sk", "regions": ["cn-shanghai"], "packages": ["s"]}},
        "users": [
            {"name": "a", "packages": ["o"], "rules": [{"listen": "127.0.0.1:1", "target": "x:1"}]},
            {"name": "b", "rules": [{"listen": "127.0.0.1:2", "target": "x:1"}]}
        ]
    }"#;

    fn quota(name: &str) -> Arc<UserQuota> {
        Arc::new(UserQuota::new(
            name.into(),
            Some(1000),
            SavedUsage::default(),
            Metering::Max,
        ))
    }

    #[test]
    fn assignments_follow_config() {
        let config = live(REMOTE).borrow().config.clone();
        let assignments = assignments(&config);
        assert!(assignments.split.contains("s"));
        assert_eq!(assignments.owners["o"], "a");
        assert_eq!(assignments.owners.len(), 1);
    }

    #[tokio::test]
    async fn transfer_updates_limits_status_and_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(StateDb::open(&dir.path().join("state.db")).unwrap());
        let live = live(REMOTE);
        let (a, b) = (quota("a"), quota("b"));
        a.try_consume(100, Direction::Upload);
        let users = watch::Sender::new(Arc::new(UserSet::new(vec![a.clone(), b.clone()])));
        let mut book = Book {
            migrated: true,
            remaining: 600,
            ..Default::default()
        };
        book.balances.insert("a".into(), 500);
        book.balances.insert("b".into(), 100);
        book.baselines.insert("a".into(), 0);
        book.baselines.insert("b".into(), 0);
        let limits = RemoteLimits::new(&book);
        let engine = Engine::new(
            Some(db.clone()),
            live.subscribe(),
            users.subscribe(),
            limits.clone(),
            book,
            Vec::new(),
        )
        .with_fetch(Arc::new(|_| Box::pin(async { Err("unreachable".into()) })));
        let status = engine.status();
        let (commands, rx) = mpsc::channel(1);
        let task = tokio::spawn(engine.run(rx));

        let (reply, result) = oneshot::channel();
        commands
            .send(Command::Transfer {
                from: "a".into(),
                to: "b".into(),
                amount: 450,
                reply,
            })
            .await
            .ok()
            .unwrap();
        let err = result.await.unwrap().unwrap_err();
        assert!(err.contains("only"), "{err}");

        let (reply, result) = oneshot::channel();
        commands
            .send(Command::Transfer {
                from: "a".into(),
                to: "b".into(),
                amount: 300,
                reply,
            })
            .await
            .ok()
            .unwrap();
        result.await.unwrap().unwrap();

        assert_eq!(a.limit(), Some(200));
        assert_eq!(b.limit(), Some(400));
        assert_eq!(limits.snapshot().unwrap()["b"], 400);
        let status = status.borrow().clone();
        assert_eq!(status.balances["a"], 200);
        assert_eq!(status.entries.len(), 2);
        assert_eq!(status.entries[0].user, "b");
        assert_eq!(db.load_book().unwrap().unwrap().balances["b"], 400);
        assert_eq!(db.recent_entries(10).unwrap().len(), 2);
        task.abort();
    }

    #[tokio::test]
    async fn first_sync_migrates_offline_remaining() {
        let live = live(REMOTE);
        let (a, b) = (quota("a"), quota("b"));
        a.try_consume(100, Direction::Upload);
        let users = watch::Sender::new(Arc::new(UserSet::new(vec![a.clone(), b.clone()])));
        let fetch: Fetch = Arc::new(|remote: RemoteConfig| {
            Box::pin(async move {
                assert_eq!(remote.regions, ["cn-shanghai"]);
                let package = |id: &str, region: &str| Package {
                    id: id.into(),
                    name: id.into(),
                    region: region.into(),
                    total: 2048,
                    available: 950,
                    status: "Effective".into(),
                    expiry: String::new(),
                };
                Ok(Fetched {
                    packages: vec![package("p", "cn-shanghai")],
                    ignored: vec![package("q", "cn-beijing")],
                })
            })
        });
        let limits = RemoteLimits::new(&Book::default());
        assert!(limits.snapshot().is_none());
        let engine = Engine::new(
            None,
            live.subscribe(),
            users.subscribe(),
            limits.clone(),
            Book::default(),
            Vec::new(),
        )
        .with_fetch(fetch);
        let status = engine.status();
        let (commands, rx) = mpsc::channel(1);
        let task = tokio::spawn(engine.run(rx));

        let (reply, result) = oneshot::channel();
        commands.send(Command::Sync(reply)).await.ok().unwrap();
        result.await.unwrap().unwrap();

        assert_eq!(a.limit(), Some(550));
        assert_eq!(b.limit(), Some(500));
        assert_eq!(limits.snapshot().unwrap()["a"], 550);
        let status = status.borrow().clone();
        assert!(status.migrated);
        assert_eq!(status.remaining, 950);
        assert_eq!(status.unallocated, 0);
        assert_eq!(status.packages.len(), 1);
        assert_eq!(status.ignored.len(), 1);
        assert_eq!(status.last_error, None);
        task.abort();
    }

    #[tokio::test]
    async fn migration_waits_for_a_user() {
        let live = live(
            r#"{
                "panel": {"enabled": true},
                "accounting": {"mode": "remote", "remote": {"provider": "volcengine", "access_key": "ak", "secret_key": "sk", "regions": ["cn-shanghai"]}}
            }"#,
        );
        let users = watch::Sender::new(Arc::new(UserSet::default()));
        let fetched = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let engine = Engine::new(
            None,
            live.subscribe(),
            users.subscribe(),
            RemoteLimits::new(&Book::default()),
            Book::default(),
            Vec::new(),
        )
        .with_fetch({
            let fetched = fetched.clone();
            Arc::new(move |_| {
                fetched.store(true, std::sync::atomic::Ordering::Relaxed);
                Box::pin(async { Err("fetched".into()) })
            })
        });
        let status = engine.status();
        let (commands, rx) = mpsc::channel(1);
        let task = tokio::spawn(engine.run(rx));

        let (reply, result) = oneshot::channel();
        commands.send(Command::Sync(reply)).await.ok().unwrap();
        let err = result.await.unwrap().unwrap_err();

        assert!(err.contains("waiting for a user"), "{err}");
        assert!(!fetched.load(std::sync::atomic::Ordering::Relaxed));
        assert!(!status.borrow().migrated);
        task.abort();
    }

    #[tokio::test]
    async fn offline_mode_refuses_sync_and_keeps_limits() {
        let live = live(
            r#"{"users": [{"name": "a", "rules": [{"listen": "127.0.0.1:1", "target": "x:1"}]}]}"#,
        );
        let a = quota("a");
        let users = watch::Sender::new(Arc::new(UserSet::new(vec![a.clone()])));
        let engine = Engine::new(
            None,
            live.subscribe(),
            users.subscribe(),
            RemoteLimits::new(&Book::default()),
            Book::default(),
            Vec::new(),
        );
        let status = engine.status();
        let (commands, rx) = mpsc::channel(1);
        let task = tokio::spawn(engine.run(rx));

        let (reply, result) = oneshot::channel();
        commands.send(Command::Sync(reply)).await.ok().unwrap();
        let err = result.await.unwrap().unwrap_err();

        assert!(err.contains("off"), "{err}");
        assert_eq!(a.limit(), Some(1000));
        assert_eq!(status.borrow().mode, Mode::Offline);
        assert!(!status.borrow().migrated);
        task.abort();
    }
}
