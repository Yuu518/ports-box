use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{info, warn};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Prioritized targets for one rule, shared by its TCP and UDP sides.
///
/// The active target is the highest-priority healthy one; changes are
/// broadcast over a watch channel. Health flips come from connection
/// failures (passive) and, for rules with fallbacks, either a TCP probe
/// task or — for UDP-only rules, which cannot be probed — a retry
/// cooldown that re-arms a downed target after `cooldown`.
pub struct TargetPool {
    user: String,
    targets: Vec<Arc<str>>,
    healthy: Mutex<Vec<bool>>,
    active: watch::Sender<usize>,
    cooldown: Option<Duration>,
}

impl TargetPool {
    pub fn new(user: String, targets: Vec<String>, cooldown: Option<Duration>) -> Arc<Self> {
        assert!(!targets.is_empty());
        let healthy = vec![true; targets.len()];
        let (active, _) = watch::channel(0);
        Arc::new(Self {
            user,
            targets: targets.into_iter().map(Into::into).collect(),
            healthy: Mutex::new(healthy),
            active,
            cooldown,
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

/// Probes every target with a TCP connect each `interval`, keeping the
/// pool's health flags fresh so the active target falls back and — the part
/// passive failures alone cannot do — recovers.
pub async fn probe_task(pool: Arc<TargetPool>, interval: Duration) {
    loop {
        for i in 0..pool.len() {
            let target = pool.targets[i].clone();
            let connect = async {
                TcpStream::connect(crate::dns::resolve(&target).await?).await
            };
            match timeout(CONNECT_TIMEOUT, connect).await {
                Ok(Ok(_)) => pool.mark_up(i),
                _ => pool.mark_down(i),
            }
        }
        tokio::time::sleep(interval).await;
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
}
