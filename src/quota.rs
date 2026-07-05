use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Upload,
    Download,
}

/// Per-user traffic quota, shared by every rule of that user.
///
/// The hot path only touches relaxed atomics; the `exhausted` watch channel
/// broadcasts to all live connections the moment the quota runs out.
pub struct UserQuota {
    pub name: String,
    /// `None` means unlimited: usage is still tracked, never enforced.
    pub limit: Option<u64>,
    upload: AtomicU64,
    download: AtomicU64,
    exhausted: watch::Sender<bool>,
}

impl UserQuota {
    pub fn new(name: String, limit: Option<u64>, upload: u64, download: u64) -> Self {
        let used = upload.max(download);
        let (exhausted, _) = watch::channel(limit.is_some_and(|l| used >= l));
        Self {
            name,
            limit,
            upload: AtomicU64::new(upload),
            download: AtomicU64::new(download),
            exhausted,
        }
    }

    /// Records `n` transferred bytes. Returns `false` if the quota is (now)
    /// exhausted, in which case all of this user's traffic must stop.
    pub fn try_consume(&self, n: u64, direction: Direction) -> bool {
        if self.is_exhausted() {
            return false;
        }
        let counter = match direction {
            Direction::Upload => &self.upload,
            Direction::Download => &self.download,
        };
        counter.fetch_add(n, Ordering::Relaxed);
        if let Some(limit) = self.limit
            && self.used() >= limit
        {
            self.exhausted.send_replace(true);
            return false;
        }
        true
    }

    pub fn is_exhausted(&self) -> bool {
        *self.exhausted.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.exhausted.subscribe()
    }

    pub fn upload(&self) -> u64 {
        self.upload.load(Ordering::Relaxed)
    }

    pub fn download(&self) -> u64 {
        self.download.load(Ordering::Relaxed)
    }

    /// Billed usage: the larger of the two directions (入出取大), so a
    /// download-heavy workload is charged only for its download side.
    pub fn used(&self) -> u64 {
        self.upload().max(self.download())
    }

    /// Remaining bytes before the limit; `None` when unlimited.
    pub fn remaining(&self) -> Option<u64> {
        self.limit.map(|l| l.saturating_sub(self.used()))
    }
}

/// Resolves once the user's quota is exhausted; pends forever otherwise.
pub async fn exhausted(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender dropped (shutdown); pend forever and let siblings win.
            std::future::pending::<()>().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_is_max_of_directions() {
        let quota = UserQuota::new("a".into(), Some(100), 0, 0);
        assert!(quota.try_consume(60, Direction::Upload));
        // Download up to the upload level doesn't add billed usage.
        assert!(quota.try_consume(60, Direction::Download));
        assert_eq!(quota.used(), 60);
        assert_eq!(quota.remaining(), Some(40));
    }

    #[test]
    fn consume_until_exhausted() {
        let quota = UserQuota::new("a".into(), Some(100), 0, 0);
        assert!(quota.try_consume(60, Direction::Download));
        assert!(!quota.is_exhausted());
        // Download reaches the limit: exhausted, upload level irrelevant.
        assert!(!quota.try_consume(40, Direction::Download));
        assert!(quota.is_exhausted());
        assert_eq!(quota.used(), 100);
        assert_eq!(quota.remaining(), Some(0));
        // Once exhausted nothing more is counted.
        assert!(!quota.try_consume(1, Direction::Upload));
        assert_eq!(quota.used(), 100);
    }

    #[test]
    fn starts_exhausted_when_restored_over_limit() {
        let quota = UserQuota::new("a".into(), Some(100), 120, 30);
        assert!(quota.is_exhausted());
        assert_eq!(quota.remaining(), Some(0));
    }

    #[test]
    fn raised_limit_reenables() {
        // Simulates a config edit + restart: same usage, bigger limit.
        let quota = UserQuota::new("a".into(), Some(200), 80, 30);
        assert!(!quota.is_exhausted());
        assert!(quota.try_consume(10, Direction::Upload));
        assert_eq!(quota.used(), 90);
        assert_eq!(quota.remaining(), Some(110));
    }

    #[test]
    fn unlimited_tracks_but_never_exhausts() {
        let quota = UserQuota::new("a".into(), None, u64::MAX / 2, 0);
        assert!(!quota.is_exhausted());
        assert!(quota.try_consume(1 << 40, Direction::Download));
        assert!(!quota.is_exhausted());
        assert_eq!(quota.used(), u64::MAX / 2);
        assert_eq!(quota.remaining(), None);
    }

    #[tokio::test]
    async fn exhausted_future_resolves() {
        let quota = UserQuota::new("a".into(), Some(10), 0, 0);
        let rx = quota.subscribe();
        let waiter = tokio::spawn(exhausted(rx));
        assert!(!waiter.is_finished());
        quota.try_consume(20, Direction::Upload);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("exhausted future should resolve")
            .unwrap();
    }
}
