use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::pool::TargetPool;
use crate::quota::{exhausted, Direction, UserQuota};

const RECV_BUF: usize = 64 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One NAT-style session: a dedicated upstream socket per client address.
struct Session {
    upstream: Arc<UdpSocket>,
    /// Which pool target this session is connected to; the session ends
    /// when that target stops being the active one.
    target_index: usize,
    /// Milliseconds since `Epoch` (process start) of the last packet in
    /// either direction; used for idle collection.
    last_active: AtomicU64,
}

struct Epoch(Instant);

impl Epoch {
    fn now_ms(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }
}

pub async fn serve(socket: UdpSocket, pool: Arc<TargetPool>, quota: Arc<UserQuota>) {
    let socket = Arc::new(socket);
    let sessions: Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>> = Arc::default();
    let epoch = Arc::new(Epoch(Instant::now()));
    let mut buf = vec![0u8; RECV_BUF];

    loop {
        let (n, peer) = match socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                warn!(user = %quota.name, "udp recv failed: {e}");
                continue;
            }
        };
        if !quota.try_consume(n as u64, Direction::Upload) {
            continue; // quota exhausted: drop the packet
        }

        let session = sessions.lock().unwrap().get(&peer).cloned();
        let session = match session {
            Some(s) => s,
            None => match open_session(&socket, &sessions, &epoch, peer, &pool, &quota).await {
                Ok(s) => s,
                Err(e) => {
                    debug!(user = %quota.name, %peer, "udp session failed: {e}");
                    continue;
                }
            },
        };
        session.last_active.store(epoch.now_ms(), Ordering::Relaxed);
        if let Err(e) = session.upstream.send(&buf[..n]).await {
            // An ICMP unreachable surfaces here on connected sockets: treat
            // it as the target being down and shed the session so the next
            // packet reopens against the new active target.
            debug!(user = %quota.name, %peer, "udp send failed: {e}");
            pool.mark_down(session.target_index);
            let mut map = sessions.lock().unwrap();
            if map.get(&peer).is_some_and(|s| Arc::ptr_eq(s, &session)) {
                map.remove(&peer);
            }
        }
    }
}

/// Opens a session to the pool's active target, falling through the
/// priority list on failure, same as the TCP side.
async fn open_session(
    socket: &Arc<UdpSocket>,
    sessions: &Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>>,
    epoch: &Arc<Epoch>,
    peer: SocketAddr,
    pool: &Arc<TargetPool>,
    quota: &Arc<UserQuota>,
) -> io::Result<Arc<Session>> {
    let mut last_err = None;
    for _ in 0..pool.len() {
        let (i, target) = pool.pick();
        let upstream = match connect_upstream(&target).await {
            Ok(upstream) => upstream,
            Err(e) => {
                last_err = Some(io::Error::new(
                    e.kind(),
                    format!("session to {target} failed: {e}"),
                ));
                pool.mark_down(i);
                if pool.pick().0 == i {
                    break; // everything is down; no point cycling
                }
                continue;
            }
        };

        let session = Arc::new(Session {
            upstream: Arc::new(upstream),
            target_index: i,
            last_active: AtomicU64::new(epoch.now_ms()),
        });
        sessions.lock().unwrap().insert(peer, session.clone());

        tokio::spawn(relay_downstream(
            socket.clone(),
            sessions.clone(),
            epoch.clone(),
            session.clone(),
            peer,
            pool.clone(),
            quota.clone(),
        ));
        return Ok(session);
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("no targets")))
}

async fn connect_upstream(target: &str) -> io::Result<UdpSocket> {
    let target_addr = tokio::net::lookup_host(target)
        .await?
        .next()
        .ok_or_else(|| io::Error::other(format!("cannot resolve {target}")))?;
    let local: SocketAddr = if target_addr.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let upstream = UdpSocket::bind(local).await?;
    upstream.connect(target_addr).await?;
    Ok(upstream)
}

/// Copies upstream replies back to the client, counting them as download
/// traffic, until the session idles out, the quota is exhausted, or the
/// pool's active target moves away (failover or failback): ending the
/// session then makes the client's next packet reopen on the right target.
async fn relay_downstream(
    socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>>,
    epoch: Arc<Epoch>,
    session: Arc<Session>,
    peer: SocketAddr,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
) {
    let mut active_rx = pool.subscribe();
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        let received = tokio::select! {
            r = timeout(IDLE_TIMEOUT, session.upstream.recv(&mut buf)) => r,
            _ = exhausted(quota.subscribe()) => break,
            _ = active_moved(&mut active_rx, session.target_index) => {
                debug!(user = %quota.name, %peer, "udp session ended: active target changed");
                break;
            }
        };
        match received {
            Err(_) => {
                // No reply within the window; the client side may still be
                // active (its packets flow through the main loop).
                let idle = epoch.now_ms().saturating_sub(session.last_active.load(Ordering::Relaxed));
                if idle >= IDLE_TIMEOUT.as_millis() as u64 {
                    break;
                }
            }
            Ok(Err(e)) => {
                debug!(user = %quota.name, %peer, "udp session closed: {e}");
                break;
            }
            Ok(Ok(n)) => {
                if !quota.try_consume(n as u64, Direction::Download) {
                    break;
                }
                session.last_active.store(epoch.now_ms(), Ordering::Relaxed);
                if socket.send_to(&buf[..n], peer).await.is_err() {
                    break;
                }
            }
        }
    }
    let mut map = sessions.lock().unwrap();
    // Only remove our own entry: the main loop may already have replaced it.
    if map.get(&peer).is_some_and(|s| Arc::ptr_eq(s, &session)) {
        map.remove(&peer);
    }
}

/// Resolves once the pool's active target differs from `index`; pends
/// forever otherwise. Cancel-safe: it compares values, not change events.
async fn active_moved(rx: &mut watch::Receiver<usize>, index: usize) {
    loop {
        if *rx.borrow_and_update() != index {
            return;
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
