use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use tokio::sync::watch;

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Upload,
    Download,
}

/// Local wall-clock time used for billing-period boundaries.
pub(crate) mod clock {
    use std::sync::Once;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Local-timezone offset in seconds, cached so the hot path never asks
    /// the OS for it. Refreshed at startup and on every hourly settlement,
    /// which bounds DST drift to a single transition hour.
    static TZ_OFFSET_SECS: AtomicI64 = AtomicI64::new(0);
    static INIT: Once = Once::new();

    pub(super) fn refresh_offset() {
        let offset = chrono::Local::now().offset().local_minus_utc();
        TZ_OFFSET_SECS.store(offset as i64, Ordering::Relaxed);
    }

    fn local_now() -> i64 {
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        INIT.call_once(refresh_offset);
        unix + TZ_OFFSET_SECS.load(Ordering::Relaxed)
    }

    /// Hour number of the local wall clock since the epoch; the billing
    /// bucket key.
    pub(crate) fn current_hour_id() -> i64 {
        local_now().div_euclid(3600)
    }

    /// Seconds until the next local hour boundary (at least 1).
    pub(crate) fn secs_until_next_hour() -> u64 {
        (3600 - local_now().rem_euclid(3600)).max(1) as u64
    }
}

/// Point-in-time snapshot of every persisted counter, shared between the
/// in-memory quota and the SQLite state file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SavedUsage {
    pub upload: u64,
    pub download: u64,
    pub settled: u64,
    pub hour_upload: u64,
    pub hour_download: u64,
    pub hour_id: i64,
}

/// Per-user traffic quota, shared by every rule of that user.
///
/// Billing follows the carrier model: each local wall-clock hour is one
/// billing period, charged the larger of its two directions (入出取大);
/// `used()` is the sum over settled hours plus the current hour's max.
///
/// The hot path only touches relaxed atomics; the hourly rollover takes
/// `settle_lock` at most once per user per hour. The `exhausted` watch
/// channel broadcasts to all live connections the moment the quota runs
/// out.
pub struct UserQuota {
    pub name: String,
    /// `None` means unlimited: usage is still tracked, never enforced.
    pub limit: Option<u64>,
    /// Lifetime totals, reported raw by the /sub endpoint.
    total_upload: AtomicU64,
    total_download: AtomicU64,
    /// Sum of max(upload, download) over all completed hours.
    settled: AtomicU64,
    /// Bytes moved during the hour identified by `bucket_hour`.
    hour_upload: AtomicU64,
    hour_download: AtomicU64,
    bucket_hour: AtomicI64,
    /// Serializes rollover and snapshotting; never held across an await.
    settle_lock: Mutex<()>,
    exhausted: watch::Sender<bool>,
}

impl UserQuota {
    pub fn new(name: String, limit: Option<u64>, saved: SavedUsage) -> Self {
        Self::new_at(name, limit, saved, clock::current_hour_id())
    }

    /// `new` with the hour injected; pub(crate) so tests control time.
    pub(crate) fn new_at(
        name: String,
        limit: Option<u64>,
        saved: SavedUsage,
        now_hour: i64,
    ) -> Self {
        // A snapshot from an earlier hour settles before use; one from the
        // current hour resumes its bucket so a quick restart loses nothing.
        let (settled, hour_up, hour_down) = if saved.hour_id == now_hour {
            (saved.settled, saved.hour_upload, saved.hour_download)
        } else {
            (
                saved.settled + saved.hour_upload.max(saved.hour_download),
                0,
                0,
            )
        };
        let used = settled + hour_up.max(hour_down);
        let (exhausted, _) = watch::channel(limit.is_some_and(|l| used >= l));
        Self {
            name,
            limit,
            total_upload: AtomicU64::new(saved.upload),
            total_download: AtomicU64::new(saved.download),
            settled: AtomicU64::new(settled),
            hour_upload: AtomicU64::new(hour_up),
            hour_download: AtomicU64::new(hour_down),
            bucket_hour: AtomicI64::new(now_hour),
            settle_lock: Mutex::new(()),
            exhausted,
        }
    }

    /// Records `n` transferred bytes. Returns `false` if the quota is (now)
    /// exhausted, in which case all of this user's traffic must stop.
    pub fn try_consume(&self, n: u64, direction: Direction) -> bool {
        self.try_consume_at(n, direction, clock::current_hour_id())
    }

    /// `try_consume` with the hour injected; pub(crate) so tests control
    /// time.
    pub(crate) fn try_consume_at(&self, n: u64, direction: Direction, now_hour: i64) -> bool {
        if self.is_exhausted() {
            return false;
        }
        if self.bucket_hour.load(Ordering::Relaxed) != now_hour {
            self.settle_to(now_hour);
        }
        let (total, bucket) = match direction {
            Direction::Upload => (&self.total_upload, &self.hour_upload),
            Direction::Download => (&self.total_download, &self.hour_download),
        };
        total.fetch_add(n, Ordering::Relaxed);
        bucket.fetch_add(n, Ordering::Relaxed);
        if let Some(limit) = self.limit
            && self.used() >= limit
        {
            self.exhausted.send_replace(true);
            return false;
        }
        true
    }

    /// Folds the finished hour's bucket into `settled`. `used()` reads the
    /// same value before and after, so correctness never depends on how
    /// promptly the rollover happens.
    fn settle_to(&self, now_hour: i64) {
        let _guard = self.settle_lock.lock().unwrap();
        if self.bucket_hour.load(Ordering::Relaxed) == now_hour {
            return; // another task already rolled over
        }
        // Empty the buckets before bumping `settled` so a `used()` that
        // sees the new sum (Acquire) cannot also see the old buckets and
        // double-count. Writes racing with the swaps migrate at most one
        // buffer into the new hour; bytes are never lost.
        let up = self.hour_upload.swap(0, Ordering::Relaxed);
        let down = self.hour_download.swap(0, Ordering::Relaxed);
        self.settled.fetch_add(up.max(down), Ordering::Release);
        self.bucket_hour.store(now_hour, Ordering::Relaxed);
        clock::refresh_offset();
    }

    pub fn is_exhausted(&self) -> bool {
        *self.exhausted.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<bool> {
        self.exhausted.subscribe()
    }

    pub fn upload(&self) -> u64 {
        self.total_upload.load(Ordering::Relaxed)
    }

    pub fn download(&self) -> u64 {
        self.total_download.load(Ordering::Relaxed)
    }

    /// The current hour's billed traffic so far: the larger direction of
    /// its bucket.
    pub fn hour_used(&self) -> u64 {
        self.hour_upload
            .load(Ordering::Relaxed)
            .max(self.hour_download.load(Ordering::Relaxed))
    }

    /// Billed usage: every settled hour's larger direction summed, plus
    /// the current hour's (入出取大 per billing period).
    pub fn used(&self) -> u64 {
        // Acquire pairs with the Release in `settle_to`: seeing the
        // post-rollover sum implies the buckets read next are already
        // empty, so an hour is never counted twice.
        self.settled.load(Ordering::Acquire) + self.hour_used()
    }

    /// Remaining bytes before the limit; `None` when unlimited.
    pub fn remaining(&self) -> Option<u64> {
        self.limit.map(|l| l.saturating_sub(self.used()))
    }

    /// Consistent snapshot for persistence. Holding `settle_lock` keeps a
    /// concurrent rollover from splitting the bucket across two hours,
    /// which a crash restore would then double-count.
    pub fn snapshot(&self) -> SavedUsage {
        let _guard = self.settle_lock.lock().unwrap();
        SavedUsage {
            upload: self.upload(),
            download: self.download(),
            settled: self.settled.load(Ordering::Acquire),
            hour_upload: self.hour_upload.load(Ordering::Relaxed),
            hour_download: self.hour_download.load(Ordering::Relaxed),
            hour_id: self.bucket_hour.load(Ordering::Relaxed),
        }
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

    /// Arbitrary but fixed hour id so tests never straddle a real boundary.
    const H: i64 = 500_000;

    fn quota(limit: Option<u64>) -> UserQuota {
        UserQuota::new_at("a".into(), limit, SavedUsage::default(), H)
    }

    #[test]
    fn usage_is_max_of_directions() {
        let quota = quota(Some(100));
        assert!(quota.try_consume_at(60, Direction::Upload, H));
        // Download up to the upload level doesn't add billed usage.
        assert!(quota.try_consume_at(60, Direction::Download, H));
        assert_eq!(quota.used(), 60);
        assert_eq!(quota.remaining(), Some(40));
    }

    #[test]
    fn consume_until_exhausted() {
        let quota = quota(Some(100));
        assert!(quota.try_consume_at(60, Direction::Download, H));
        assert!(!quota.is_exhausted());
        // Download reaches the limit: exhausted, upload level irrelevant.
        assert!(!quota.try_consume_at(40, Direction::Download, H));
        assert!(quota.is_exhausted());
        assert_eq!(quota.used(), 100);
        assert_eq!(quota.remaining(), Some(0));
        // Once exhausted nothing more is counted.
        assert!(!quota.try_consume_at(1, Direction::Upload, H));
        assert_eq!(quota.used(), 100);
    }

    #[test]
    fn starts_exhausted_when_restored_over_limit() {
        let saved = SavedUsage {
            upload: 120,
            download: 30,
            settled: 120,
            ..Default::default()
        };
        let quota = UserQuota::new_at("a".into(), Some(100), saved, H);
        assert!(quota.is_exhausted());
        assert_eq!(quota.remaining(), Some(0));
    }

    #[test]
    fn raised_limit_reenables() {
        // Simulates a config edit + restart: same usage, bigger limit.
        let saved = SavedUsage {
            upload: 80,
            download: 30,
            settled: 80,
            ..Default::default()
        };
        let quota = UserQuota::new_at("a".into(), Some(200), saved, H);
        assert!(!quota.is_exhausted());
        assert!(quota.try_consume_at(10, Direction::Upload, H));
        assert_eq!(quota.used(), 90);
        assert_eq!(quota.remaining(), Some(110));
    }

    #[test]
    fn unlimited_tracks_but_never_exhausts() {
        let saved = SavedUsage {
            upload: u64::MAX / 2,
            settled: u64::MAX / 2,
            ..Default::default()
        };
        let quota = UserQuota::new_at("a".into(), None, saved, H);
        assert!(!quota.is_exhausted());
        assert!(quota.try_consume_at(1 << 40, Direction::Download, H));
        assert!(!quota.is_exhausted());
        assert_eq!(quota.used(), u64::MAX / 2 + (1 << 40));
        assert_eq!(quota.remaining(), None);
    }

    #[test]
    fn rollover_settles_hourly_max() {
        let quota = quota(None);
        assert!(quota.try_consume_at(60, Direction::Upload, H));
        assert!(quota.try_consume_at(40, Direction::Download, H));
        assert_eq!(quota.used(), 60);
        // Next hour: the previous bucket settles at its max, the new one
        // accumulates independently.
        assert!(quota.try_consume_at(10, Direction::Upload, H + 1));
        assert!(quota.try_consume_at(30, Direction::Download, H + 1));
        assert_eq!(quota.hour_used(), 30);
        assert_eq!(quota.used(), 90); // 60 settled + 30 current
        // Lifetime totals are unaffected by settlement.
        assert_eq!((quota.upload(), quota.download()), (70, 70));
    }

    #[test]
    fn rollover_never_decreases_used() {
        let quota = quota(Some(100));
        assert!(quota.try_consume_at(70, Direction::Download, H));
        let before = quota.used();
        // A zero-byte consume in the next hour forces the rollover.
        assert!(quota.try_consume_at(0, Direction::Upload, H + 1));
        assert_eq!(quota.used(), before);
        assert_eq!(quota.hour_used(), 0);
        // Exhaustion carries across hours: settled only ever grows.
        assert!(!quota.try_consume_at(30, Direction::Upload, H + 1));
        assert!(quota.is_exhausted());
        assert!(!quota.try_consume_at(1, Direction::Upload, H + 2));
    }

    #[test]
    fn restore_same_hour_resumes_bucket() {
        let saved = SavedUsage {
            upload: 100,
            download: 40,
            settled: 60,
            hour_upload: 40,
            hour_download: 20,
            hour_id: H,
        };
        let quota = UserQuota::new_at("a".into(), Some(1000), saved, H);
        assert_eq!(quota.used(), 100); // 60 settled + max(40, 20)
        assert_eq!(quota.hour_used(), 40);
        assert_eq!(quota.snapshot(), saved);
    }

    #[test]
    fn restore_cross_hour_settles() {
        let saved = SavedUsage {
            upload: 100,
            download: 40,
            settled: 60,
            hour_upload: 40,
            hour_download: 20,
            hour_id: H - 3,
        };
        let quota = UserQuota::new_at("a".into(), Some(1000), saved, H);
        assert_eq!(quota.used(), 100); // identical value, now fully settled
        assert_eq!(quota.hour_used(), 0);
        assert_eq!(quota.snapshot().hour_id, H);
    }

    #[tokio::test]
    async fn exhausted_future_resolves() {
        let quota = UserQuota::new("a".into(), Some(10), SavedUsage::default());
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
