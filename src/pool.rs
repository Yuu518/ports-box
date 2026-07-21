use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{info, warn};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Prioritized targets for one rule, shared by its TCP and UDP sides.
///
/// The active target is the highest-priority healthy one; changes are
/// broadcast over a watch channel. Real traffic is the primary health
/// signal: a delivery confirmed by `confirm` marks the target up, a
/// connection failure marks it down. For rules with fallbacks, a shared
/// probe task backstops targets traffic cannot vouch for — downed ones
/// and ones without a recent confirmation — while UDP-only rules, which
/// cannot be probed, re-arm a downed target after `cooldown` instead.
pub struct TargetPool {
    user: String,
    targets: Vec<Arc<str>>,
    healthy: Mutex<Vec<bool>>,
    active: watch::Sender<usize>,
    cooldown: Option<Duration>,
    /// Milliseconds since `epoch` of each target's last confirmed delivery;
    /// pool creation counts as the initial confirmation.
    confirmed: Vec<AtomicU64>,
    epoch: Instant,
}

impl TargetPool {
    pub fn new(user: String, targets: Vec<String>, cooldown: Option<Duration>) -> Arc<Self> {
        assert!(!targets.is_empty());
        let healthy = vec![true; targets.len()];
        let (active, _) = watch::channel(0);
        let confirmed = targets.iter().map(|_| AtomicU64::new(0)).collect();
        Arc::new(Self {
            user,
            targets: targets.into_iter().map(Into::into).collect(),
            healthy: Mutex::new(healthy),
            active,
            cooldown,
            confirmed,
            epoch: Instant::now(),
        })
    }

    pub fn len(&self) -> usize {
        self.targets.len()
    }

    /// The current active target: highest-priority healthy, or the primary
    /// when everything is down (keep hammering the front door).
    pub fn pick(&self) -> (usize, Arc<str>) {
        let i = *self.active.borrow();
        (i, self.targets[i].clone())
    }

    pub fn subscribe(&self) -> watch::Receiver<usize> {
        self.active.subscribe()
    }

    pub fn mark_down(self: &Arc<Self>, i: usize) {
        {
            let mut healthy = self.healthy.lock().unwrap();
            if !healthy[i] {
                return;
            }
            healthy[i] = false;
            self.update_active(&healthy);
        }
        if let Some(cooldown) = self.cooldown {
            let pool = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(cooldown).await;
                pool.mark_up(i);
            });
        }
    }

    pub fn mark_up(&self, i: usize) {
        let mut healthy = self.healthy.lock().unwrap();
        if healthy[i] {
            return;
        }
        healthy[i] = true;
        self.update_active(&healthy);
    }

    /// Records a confirmed delivery to target `i` — real traffic reached it
    /// (TCP connect succeeded, UDP reply came back) or a probe did. This is
    /// the primary health signal; a fresh confirmation also exempts the
    /// target from probing.
    pub fn confirm(&self, i: usize) {
        self.confirmed[i].store(self.epoch.elapsed().as_millis() as u64, Ordering::Relaxed);
        self.mark_up(i);
    }

    /// Whether traffic can't vouch for target `i`: it is down (waiting for
    /// recovery) or hasn't had a confirmed delivery within `interval`.
    fn needs_probe(&self, i: usize, interval: Duration) -> bool {
        if !self.healthy.lock().unwrap()[i] {
            return true;
        }
        let elapsed = (self.epoch.elapsed().as_millis() as u64)
            .saturating_sub(self.confirmed[i].load(Ordering::Relaxed));
        elapsed >= interval.as_millis() as u64
    }

    #[cfg(test)]
    pub(crate) fn is_healthy(&self, i: usize) -> bool {
        self.healthy.lock().unwrap()[i]
    }

    /// Recomputes the active index while holding the health lock, so
    /// concurrent flips serialize and the watch value never goes stale.
    fn update_active(&self, healthy: &[bool]) {
        let next = healthy.iter().position(|&h| h).unwrap_or(0);
        let prev = *self.active.borrow();
        if next == prev {
            return;
        }
        self.active.send_replace(next);
        if !healthy[next] {
            warn!(
                user = %self.user,
                "all targets down, retrying primary {}",
                self.targets[next],
            );
        } else if next > prev {
            warn!(
                user = %self.user,
                "target {} down, switching to {}",
                self.targets[prev], self.targets[next],
            );
        } else {
            info!(
                user = %self.user,
                "target {} recovered, switching back from {}",
                self.targets[next], self.targets[prev],
            );
        }
    }
}

/// Backstop prober shared by every rule with fallbacks: when a pool's
/// interval elapses, TCP-connects to the targets its traffic cannot vouch
/// for — downed ones (so the active target recovers) and idle ones (so a
/// dead target is noticed even with nothing flowing). Probes run strictly
/// one at a time so a large rule count never bursts connections.
pub async fn probe_task(pools: Vec<(Arc<TargetPool>, Duration)>) {
    if pools.is_empty() {
        return;
    }
    let now = tokio::time::Instant::now();
    let mut due: Vec<_> = pools.iter().map(|(_, interval)| now + *interval).collect();
    loop {
        let next = due.iter().copied().min().unwrap();
        tokio::time::sleep_until(next).await;
        for (k, (pool, interval)) in pools.iter().enumerate() {
            if due[k] > tokio::time::Instant::now() {
                continue;
            }
            for i in 0..pool.len() {
                if !pool.needs_probe(i, *interval) {
                    continue;
                }
                let target = pool.targets[i].clone();
                let connect =
                    async { TcpStream::connect(crate::dns::resolve(&target).await?).await };
                match timeout(CONNECT_TIMEOUT, connect).await {
                    Ok(Ok(_)) => pool.confirm(i),
                    _ => pool.mark_down(i),
                }
            }
            due[k] = tokio::time::Instant::now() + *interval;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(cooldown: Option<Duration>) -> Arc<TargetPool> {
        TargetPool::new(
            "a".into(),
            vec!["p:1".into(), "f1:1".into(), "f2:1".into()],
            cooldown,
        )
    }

    #[tokio::test]
    async fn failover_and_failback_follow_priority() {
        let pool = pool(None);
        assert_eq!(pool.pick().0, 0);

        pool.mark_down(0);
        assert_eq!(&*pool.pick().1, "f1:1");
        pool.mark_down(1);
        assert_eq!(&*pool.pick().1, "f2:1");

        // A lower-priority recovery doesn't move the active target back up.
        pool.mark_up(1);
        assert_eq!(&*pool.pick().1, "f1:1");
        // The primary recovering does.
        pool.mark_up(0);
        assert_eq!(&*pool.pick().1, "p:1");
    }

    #[tokio::test]
    async fn all_down_falls_back_to_primary() {
        let pool = pool(None);
        for i in 0..pool.len() {
            pool.mark_down(i);
        }
        assert_eq!(pool.pick().0, 0);
    }

    #[tokio::test]
    async fn watch_broadcasts_changes() {
        let pool = pool(None);
        let mut rx = pool.subscribe();
        pool.mark_down(0);
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow_and_update(), 1);
        // Marking an already-down target again doesn't re-notify.
        pool.mark_down(0);
        assert!(!rx.has_changed().unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn cooldown_rearms_downed_target() {
        let pool = pool(Some(Duration::from_secs(30)));
        pool.mark_down(0);
        assert_eq!(pool.pick().0, 1);
        tokio::time::sleep(Duration::from_secs(31)).await;
        assert_eq!(pool.pick().0, 0);
    }

    #[test]
    fn confirm_freshness_gates_probing() {
        let pool = pool(None);
        let interval = Duration::from_millis(50);
        // Pool creation counts as the initial confirmation.
        assert!(!pool.needs_probe(0, interval));
        std::thread::sleep(Duration::from_millis(60));
        assert!(pool.needs_probe(0, interval));
        pool.confirm(0);
        assert!(!pool.needs_probe(0, interval));
        // A downed target always needs probing, however fresh.
        pool.mark_down(0);
        assert!(pool.needs_probe(0, interval));
    }

    #[tokio::test]
    async fn probe_recovers_downed_target() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary = listener.local_addr().unwrap().to_string();
        let pool = TargetPool::new("a".into(), vec![primary, "127.0.0.1:1".into()], None);
        let mut rx = pool.subscribe();
        pool.mark_down(0);
        assert_eq!(pool.pick().0, 1);

        let probe = tokio::spawn(probe_task(vec![(pool.clone(), Duration::from_millis(50))]));
        timeout(Duration::from_secs(5), async {
            while pool.pick().0 != 0 {
                rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        assert!(pool.is_healthy(0));
        probe.abort();
    }

    #[tokio::test]
    async fn probe_marks_idle_unreachable_target_down() {
        // No traffic ever confirms this dead target, so the prober is the
        // only thing that can notice it.
        let pool = TargetPool::new("a".into(), vec!["127.0.0.1:1".into()], None);
        let probe = tokio::spawn(probe_task(vec![(pool.clone(), Duration::from_millis(50))]));
        timeout(Duration::from_secs(5), async {
            while pool.is_healthy(0) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        probe.abort();
    }

    #[tokio::test]
    async fn fresh_traffic_confirmation_skips_probe() {
        // The target is unreachable, but traffic keeps confirming it, so
        // the prober must leave it alone.
        let pool = TargetPool::new("a".into(), vec!["127.0.0.1:1".into()], None);
        let probe = tokio::spawn(probe_task(vec![(pool.clone(), Duration::from_millis(100))]));
        for _ in 0..15 {
            pool.confirm(0);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(pool.is_healthy(0));
        probe.abort();
    }
}
