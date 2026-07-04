use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::watch;
use tracing::{error, info};

use crate::quota::UserQuota;

/// SQLite-backed persistence for per-user usage counters.
///
/// The forwarding hot path never touches the database: counters live in
/// memory and a background task flushes them here periodically.
pub struct StateDb {
    conn: Mutex<Connection>,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .map_err(|e| format!("cannot open state db {}: {e}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("cannot enable WAL: {e}"))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usage (
                user           TEXT PRIMARY KEY,
                upload_bytes   INTEGER NOT NULL DEFAULT 0,
                download_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at     TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| format!("cannot create usage table: {e}"))?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Loads persisted usage as `user -> (upload_bytes, download_bytes)`.
    pub fn load(&self) -> Result<HashMap<String, (u64, u64)>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT user, upload_bytes, download_bytes FROM usage")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    (row.get::<_, u64>(1)?, row.get::<_, u64>(2)?),
                ))
            })
            .map_err(|e| e.to_string())?;
        let mut map = HashMap::new();
        for row in rows {
            let (user, usage) = row.map_err(|e| e.to_string())?;
            map.insert(user, usage);
        }
        Ok(map)
    }

    /// Writes all counters back in one transaction.
    pub fn flush(&self, users: &[Arc<UserQuota>]) -> Result<(), String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO usage (user, upload_bytes, download_bytes, updated_at)
                     VALUES (?1, ?2, ?3, datetime('now'))
                     ON CONFLICT(user) DO UPDATE SET
                         upload_bytes = excluded.upload_bytes,
                         download_bytes = excluded.download_bytes,
                         updated_at = excluded.updated_at",
                )
                .map_err(|e| e.to_string())?;
            for user in users {
                stmt.execute(rusqlite::params![
                    user.name,
                    user.upload(),
                    user.download()
                ])
                .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())
    }
}

/// Flushes counters every `interval` until `shutdown` flips to true, then
/// performs a final flush. rusqlite is synchronous, so each flush runs in
/// `spawn_blocking` to keep it off the async workers.
pub async fn run_flush_task(
    db: Arc<StateDb>,
    users: Arc<Vec<Arc<UserQuota>>>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick fires immediately; skip it
    loop {
        let stopping = tokio::select! {
            _ = ticker.tick() => false,
            _ = shutdown.wait_for(|&s| s) => true,
        };
        let db = db.clone();
        let users = users.clone();
        let result = tokio::task::spawn_blocking(move || db.flush(&users)).await;
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => error!("state flush failed: {e}"),
            Err(e) => error!("state flush task panicked: {e}"),
        }
        if stopping {
            info!("final state flush complete");
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::Direction;

    #[test]
    fn roundtrip_and_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");

        let db = StateDb::open(&path).unwrap();
        assert!(db.load().unwrap().is_empty());

        let alice = Arc::new(UserQuota::new("alice".into(), 1000, 0, 0));
        let bob = Arc::new(UserQuota::new("bob".into(), 1000, 0, 0));
        alice.try_consume(123, Direction::Upload);
        alice.try_consume(456, Direction::Download);
        bob.try_consume(7, Direction::Download);
        let users = vec![alice.clone(), bob];
        db.flush(&users).unwrap();
        // Second flush upserts rather than duplicating.
        alice.try_consume(1, Direction::Upload);
        db.flush(&users).unwrap();
        drop(db);

        // Reopen as a fresh process would.
        let db = StateDb::open(&path).unwrap();
        let loaded = db.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded["alice"], (124, 456));
        assert_eq!(loaded["bob"], (0, 7));
    }
}
