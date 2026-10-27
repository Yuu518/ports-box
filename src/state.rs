use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::{Connection, OptionalExtension};
use tokio::sync::watch;
use tracing::{error, info};

use crate::ledger::{Book, Entry, EntryKind};
use crate::quota::{SavedUsage, UserQuota, clock};
use crate::runtime::UserSet;

/// SQLite-backed persistence for per-user usage counters.
///
/// The forwarding hot path never touches the database: counters live in
/// memory and a background task flushes them here periodically. Commits
/// run with `synchronous=NORMAL`, so a flush is a page-cache write that
/// never stalls forwarding; the hourly checkpoint makes settled hours
/// durable even across power loss.
pub struct StateDb {
    conn: Mutex<Connection>,
    /// Last persisted snapshot per user; lets `flush` skip untouched rows.
    last_flushed: Mutex<HashMap<String, SavedUsage>>,
}

impl StateDb {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        let mut conn = Connection::open(path)
            .map_err(|e| format!("cannot open state db {}: {e}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("cannot enable WAL: {e}"))?;
        // NORMAL under WAL drops the per-commit fsync (the disk stall that
        // measurably throttled single-stream forwarding); durability against
        // power loss comes from the hourly checkpoint instead.
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(|e| format!("cannot set synchronous=NORMAL: {e}"))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS usage (
                user           TEXT PRIMARY KEY,
                upload_bytes   INTEGER NOT NULL DEFAULT 0,
                download_bytes INTEGER NOT NULL DEFAULT 0,
                settled_bytes  INTEGER NOT NULL DEFAULT 0,
                hour_upload    INTEGER NOT NULL DEFAULT 0,
                hour_download  INTEGER NOT NULL DEFAULT 0,
                hour_id        INTEGER NOT NULL DEFAULT 0,
                updated_at     TEXT NOT NULL
            )",
            [],
        )
        .map_err(|e| format!("cannot create usage table: {e}"))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ledger_book (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                book       TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ledger_entries (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                time    INTEGER NOT NULL,
                kind    TEXT NOT NULL,
                user    TEXT NOT NULL,
                amount  INTEGER NOT NULL,
                package TEXT,
                note    TEXT NOT NULL DEFAULT ''
            );",
        )
        .map_err(|e| format!("cannot create ledger tables: {e}"))?;
        migrate(&mut conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            last_flushed: Mutex::new(HashMap::new()),
        })
    }

    /// Loads persisted usage per user.
    pub fn load(&self) -> Result<HashMap<String, SavedUsage>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT user, upload_bytes, download_bytes, settled_bytes,
                        hour_upload, hour_download, hour_id
                 FROM usage",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    SavedUsage {
                        upload: row.get(1)?,
                        download: row.get(2)?,
                        settled: row.get(3)?,
                        hour_upload: row.get(4)?,
                        hour_download: row.get(5)?,
                        hour_id: row.get(6)?,
                    },
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

    /// Writes changed counters back in one transaction. Users whose
    /// snapshot matches the last flush are skipped, so an idle server
    /// performs no disk writes at all.
    pub fn flush(&self, users: &[Arc<UserQuota>]) -> Result<(), String> {
        let mut last = self.last_flushed.lock().unwrap();
        let dirty: Vec<(String, SavedUsage)> = users
            .iter()
            .map(|user| (user.name.clone(), user.snapshot()))
            .filter(|(name, snap)| last.get(name) != Some(snap))
            .collect();
        if dirty.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO usage (user, upload_bytes, download_bytes, settled_bytes,
                                        hour_upload, hour_download, hour_id, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now'))
                     ON CONFLICT(user) DO UPDATE SET
                         upload_bytes = excluded.upload_bytes,
                         download_bytes = excluded.download_bytes,
                         settled_bytes = excluded.settled_bytes,
                         hour_upload = excluded.hour_upload,
                         hour_download = excluded.hour_download,
                         hour_id = excluded.hour_id,
                         updated_at = excluded.updated_at",
                )
                .map_err(|e| e.to_string())?;
            for (name, s) in &dirty {
                stmt.execute(rusqlite::params![
                    name,
                    s.upload,
                    s.download,
                    s.settled,
                    s.hour_upload,
                    s.hour_download,
                    s.hour_id,
                ])
                .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())?;
        last.extend(dirty);
        Ok(())
    }

    pub fn load_book(&self) -> Result<Option<Book>, String> {
        let conn = self.conn.lock().unwrap();
        let raw: Option<String> = conn
            .query_row("SELECT book FROM ledger_book WHERE id = 1", [], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| format!("cannot read ledger: {e}"))?;
        raw.map(|raw| serde_json::from_str(&raw).map_err(|e| format!("invalid ledger: {e}")))
            .transpose()
    }

    pub fn save_book(&self, book: &Book, entries: &[Entry]) -> Result<(), String> {
        let json = serde_json::to_string(book).map_err(|e| format!("cannot encode ledger: {e}"))?;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO ledger_book (id, book, updated_at) VALUES (1, ?1, datetime('now'))
             ON CONFLICT(id) DO UPDATE SET book = excluded.book, updated_at = excluded.updated_at",
            [&json],
        )
        .map_err(|e| e.to_string())?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT INTO ledger_entries (time, kind, user, amount, package, note)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                )
                .map_err(|e| e.to_string())?;
            for entry in entries {
                stmt.execute(rusqlite::params![
                    entry.time,
                    entry.kind.as_str(),
                    entry.user,
                    entry.amount,
                    entry.package,
                    entry.note,
                ])
                .map_err(|e| e.to_string())?;
            }
        }
        tx.commit().map_err(|e| e.to_string())
    }

    pub fn recent_entries(&self, limit: usize) -> Result<Vec<Entry>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT time, kind, user, amount, package, note FROM ledger_entries
                 ORDER BY id DESC LIMIT ?1",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        let mut entries = Vec::new();
        for row in rows {
            let (time, kind, user, amount, package, note) = row.map_err(|e| e.to_string())?;
            let Some(kind) = EntryKind::parse(&kind) else {
                continue;
            };
            entries.push(Entry {
                time,
                kind,
                user,
                amount,
                package,
                note,
            });
        }
        entries.reverse();
        Ok(entries)
    }

    /// Merges the WAL into the main database file with an fsync, making
    /// everything flushed so far durable across power loss, and truncates
    /// the WAL so it never grows unbounded.
    pub fn checkpoint(&self) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .map_err(|e| format!("wal checkpoint failed: {e}"))
    }
}

/// Upgrades a database from the pre-hourly-billing four-column schema in
/// place. Legacy rows settle their lifetime max(upload, download) once;
/// hour_id 0 is an ancient hour, so the empty bucket settles as +0 on
/// load without special-casing.
fn migrate(conn: &mut Connection) -> Result<(), String> {
    let has_settled = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(usage)")
            .map_err(|e| e.to_string())?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| e.to_string())?;
        columns
            .filter_map(Result::ok)
            .any(|name| name == "settled_bytes")
    };
    if has_settled {
        return Ok(());
    }
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    tx.execute_batch(
        "ALTER TABLE usage ADD COLUMN settled_bytes  INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE usage ADD COLUMN hour_upload    INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE usage ADD COLUMN hour_download  INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE usage ADD COLUMN hour_id        INTEGER NOT NULL DEFAULT 0;
         UPDATE usage SET settled_bytes = MAX(upload_bytes, download_bytes);",
    )
    .map_err(|e| format!("cannot migrate usage table: {e}"))?;
    tx.commit().map_err(|e| e.to_string())?;
    info!("state db migrated to hourly billing schema");
    Ok(())
}

/// Flushes counters every `interval` until `shutdown` flips to true, then
/// performs a final flush. Shortly after each local hour boundary (and on
/// shutdown) the flush is followed by a WAL checkpoint, so freshly settled
/// hours become durable against power loss. rusqlite is synchronous, so
/// each flush runs in `spawn_blocking` to keep it off the async workers.
pub async fn run_flush_task(
    db: Arc<StateDb>,
    users: watch::Receiver<Arc<UserSet>>,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick fires immediately; skip it
    let mut hour_deadline = next_checkpoint_deadline();
    loop {
        let (checkpoint, stopping) = tokio::select! {
            _ = ticker.tick() => (false, false),
            _ = tokio::time::sleep_until(hour_deadline) => {
                hour_deadline = next_checkpoint_deadline();
                (true, false)
            }
            _ = shutdown.wait_for(|&s| s) => (true, true),
        };
        let db = db.clone();
        let users = users.borrow().clone();
        let result = tokio::task::spawn_blocking(move || {
            db.flush(users.as_slice())?;
            if checkpoint {
                db.checkpoint()?;
            }
            Ok::<(), String>(())
        })
        .await;
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

/// A few seconds past the next local hour boundary, leaving the lazy
/// rollover in `try_consume` time to settle active users first.
fn next_checkpoint_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(clock::secs_until_next_hour() + 3)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::Direction;

    /// Fixed hour id so tests never straddle a real boundary.
    const H: i64 = 500_000;

    #[test]
    fn roundtrip_and_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");

        let db = StateDb::open(&path).unwrap();
        assert!(db.load().unwrap().is_empty());

        let alice = Arc::new(UserQuota::new_at(
            "alice".into(),
            Some(1000),
            SavedUsage::default(),
            H,
        ));
        let bob = Arc::new(UserQuota::new_at(
            "bob".into(),
            Some(1000),
            SavedUsage::default(),
            H,
        ));
        alice.try_consume_at(123, Direction::Upload, H);
        alice.try_consume_at(456, Direction::Download, H);
        // A rollover mid-run: settled and the new bucket both persist.
        alice.try_consume_at(50, Direction::Upload, H + 1);
        bob.try_consume_at(7, Direction::Download, H);
        let users = vec![alice.clone(), bob];
        db.flush(&users).unwrap();
        // Second flush upserts rather than duplicating.
        alice.try_consume_at(1, Direction::Upload, H + 1);
        db.flush(&users).unwrap();
        drop(db);

        // Reopen as a fresh process would.
        let db = StateDb::open(&path).unwrap();
        let loaded = db.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded["alice"],
            SavedUsage {
                upload: 174,
                download: 456,
                settled: 456,
                hour_upload: 51,
                hour_download: 0,
                hour_id: H + 1,
            }
        );
        assert_eq!(
            loaded["bob"],
            SavedUsage {
                download: 7,
                hour_download: 7,
                hour_id: H,
                ..Default::default()
            }
        );
    }

    #[test]
    fn legacy_schema_migrates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");

        // Hand-build the old four-column table as a previous version would.
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE usage (
                user           TEXT PRIMARY KEY,
                upload_bytes   INTEGER NOT NULL DEFAULT 0,
                download_bytes INTEGER NOT NULL DEFAULT 0,
                updated_at     TEXT NOT NULL
            );
            INSERT INTO usage VALUES ('alice', 123, 456, datetime('now'));",
        )
        .unwrap();
        drop(conn);

        let db = StateDb::open(&path).unwrap();
        let loaded = db.load().unwrap();
        // Lifetime usage settles once under the old max() reading.
        assert_eq!(
            loaded["alice"],
            SavedUsage {
                upload: 123,
                download: 456,
                settled: 456,
                hour_upload: 0,
                hour_download: 0,
                hour_id: 0,
            }
        );
        drop(db);

        // Migration is a one-shot: reopening must not reset settled_bytes.
        let db = StateDb::open(&path).unwrap();
        let alice = Arc::new(UserQuota::new_at("alice".into(), None, loaded["alice"], H));
        alice.try_consume_at(10, Direction::Upload, H);
        db.flush(&[alice]).unwrap();
        drop(db);
        let db = StateDb::open(&path).unwrap();
        assert_eq!(db.load().unwrap()["alice"].settled, 456);
    }

    #[test]
    fn flush_skips_unchanged_users() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let db = StateDb::open(&path).unwrap();

        let alice = Arc::new(UserQuota::new_at(
            "alice".into(),
            None,
            SavedUsage::default(),
            H,
        ));
        alice.try_consume_at(5, Direction::Upload, H);
        let users = vec![alice.clone()];
        db.flush(&users).unwrap();

        // Plant a sentinel; an idle flush must not touch the row.
        let probe = Connection::open(&path).unwrap();
        probe
            .execute("UPDATE usage SET updated_at = 'sentinel'", [])
            .unwrap();
        db.flush(&users).unwrap();
        let unchanged: String = probe
            .query_row("SELECT updated_at FROM usage WHERE user='alice'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(unchanged, "sentinel");

        // New traffic makes the row dirty again.
        alice.try_consume_at(1, Direction::Upload, H);
        db.flush(&users).unwrap();
        let updated: String = probe
            .query_row("SELECT updated_at FROM usage WHERE user='alice'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_ne!(updated, "sentinel");
    }

    #[test]
    fn ledger_persists_book_and_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let db = StateDb::open(&path).unwrap();
        assert_eq!(db.load_book().unwrap(), None);
        assert!(db.recent_entries(10).unwrap().is_empty());

        let mut book = Book {
            migrated: true,
            remaining: 42,
            ..Default::default()
        };
        book.balances.insert("alice".into(), 42);
        let entry = |amount: i64| Entry {
            time: 9,
            kind: EntryKind::Transfer,
            user: "alice".into(),
            amount,
            package: None,
            note: "bob".into(),
        };
        db.save_book(&book, &[entry(1), entry(2)]).unwrap();
        book.remaining = 40;
        db.save_book(&book, &[entry(3)]).unwrap();
        drop(db);

        let db = StateDb::open(&path).unwrap();
        assert_eq!(db.load_book().unwrap(), Some(book));
        let amounts: Vec<i64> = db
            .recent_entries(2)
            .unwrap()
            .iter()
            .map(|entry| entry.amount)
            .collect();
        assert_eq!(amounts, [2, 3]);
    }

    #[test]
    fn checkpoint_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let db = StateDb::open(&dir.path().join("state.db")).unwrap();
        db.checkpoint().unwrap();
    }
}
