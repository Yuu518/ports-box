use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::quota::{exhausted, Direction, UserQuota};

const RECV_BUF: usize = 64 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One NAT-style session: a dedicated upstream socket per client address.
struct Session {
    upstream: Arc<UdpSocket>,
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

pub async fn serve(socket: UdpSocket, target: Arc<str>, quota: Arc<UserQuota>) {
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
            None => match open_session(&socket, &sessions, &epoch, peer, &target, &quota).await {
                Ok(s) => s,
                Err(e) => {
                    debug!(user = %quota.name, %peer, "udp session to {target} failed: {e}");
                    continue;
                }
            },
        };
        session.last_active.store(epoch.now_ms(), Ordering::Relaxed);
        if let Err(e) = session.upstream.send(&buf[..n]).await {
            debug!(user = %quota.name, %peer, "udp send to {target} failed: {e}");
        }
    }
}

async fn open_session(
    socket: &Arc<UdpSocket>,
    sessions: &Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>>,
    epoch: &Arc<Epoch>,
    peer: SocketAddr,
    target: &str,
    quota: &Arc<UserQuota>,
) -> io::Result<Arc<Session>> {
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

    let session = Arc::new(Session {
        upstream: Arc::new(upstream),
        last_active: AtomicU64::new(epoch.now_ms()),
    });
    sessions.lock().unwrap().insert(peer, session.clone());

    tokio::spawn(relay_downstream(
        socket.clone(),
        sessions.clone(),
        epoch.clone(),
        session.clone(),
        peer,
        quota.clone(),
    ));
    Ok(session)
}

/// Copies upstream replies back to the client, counting them as download
/// traffic, until the session idles out or the quota is exhausted.
async fn relay_downstream(
    socket: Arc<UdpSocket>,
    sessions: Arc<Mutex<HashMap<SocketAddr, Arc<Session>>>>,
    epoch: Arc<Epoch>,
    session: Arc<Session>,
    peer: SocketAddr,
    quota: Arc<UserQuota>,
) {
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        let received = tokio::select! {
            r = timeout(IDLE_TIMEOUT, session.upstream.recv(&mut buf)) => r,
            _ = exhausted(quota.subscribe()) => break,
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
