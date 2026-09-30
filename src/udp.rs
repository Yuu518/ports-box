use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::pool::{CONNECT_TIMEOUT, TargetPool};
use crate::quota::{Direction, UserQuota, exhausted};

const RECV_BUF: usize = 64 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_SESSIONS: usize = 4096;
const MAX_PENDING_PACKETS: usize = 32;

/// One NAT-style session: a dedicated upstream socket per client address.
struct Session {
    upstream: UdpSocket,
    /// Which pool target this session is connected to; the session ends
    /// when that target stops being the active one.
    target_index: usize,
    /// Milliseconds since the relay's `epoch` of the last packet in either
    /// direction; used for idle collection.
    last_active: AtomicU64,
}

enum Slot {
    Opening(Vec<Box<[u8]>>),
    Open(Arc<Session>),
}

struct Relay {
    socket: UdpSocket,
    sessions: Mutex<HashMap<SocketAddr, Slot>>,
    epoch: Instant,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
    max_sessions: usize,
}

impl Relay {
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    async fn forward(&self, session: &Session, packet: &[u8]) -> io::Result<()> {
        if !self
            .quota
            .try_consume(packet.len() as u64, Direction::Upload)
        {
            return Ok(());
        }
        session.last_active.store(self.now_ms(), Ordering::Relaxed);
        if let Err(e) = session.upstream.send(packet).await {
            // An ICMP unreachable surfaces here on connected sockets: treat
            // it as the target being down and shed the session so the next
            // packet reopens against the new active target.
            self.pool.mark_down(session.target_index);
            return Err(e);
        }
        Ok(())
    }

    fn remove_open(&self, peer: SocketAddr, session: &Arc<Session>) {
        let mut map = self.sessions.lock().unwrap();
        if matches!(map.get(&peer), Some(Slot::Open(s)) if Arc::ptr_eq(s, session)) {
            map.remove(&peer);
        }
    }
}

pub async fn serve(socket: UdpSocket, pool: Arc<TargetPool>, quota: Arc<UserQuota>) {
    serve_with_limit(socket, pool, quota, MAX_SESSIONS).await
}

async fn serve_with_limit(
    socket: UdpSocket,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
    max_sessions: usize,
) {
    let relay = Arc::new(Relay {
        socket,
        sessions: Mutex::default(),
        epoch: Instant::now(),
        pool,
        quota,
        max_sessions,
    });
    let mut buf = vec![0u8; RECV_BUF];

    loop {
        let (n, peer) = match relay.socket.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                warn!(user = %relay.quota.name, "udp recv failed: {e}");
                continue;
            }
        };
        if relay.quota.is_exhausted() {
            continue;
        }
        let packet = &buf[..n];

        let session = {
            let mut map = relay.sessions.lock().unwrap();
            let len = map.len();
            match map.entry(peer) {
                Entry::Occupied(mut slot) => match slot.get_mut() {
                    Slot::Open(session) => Some(session.clone()),
                    Slot::Opening(pending) => {
                        if pending.len() < MAX_PENDING_PACKETS {
                            pending.push(packet.into());
                        }
                        None
                    }
                },
                Entry::Vacant(slot) => {
                    if len >= relay.max_sessions {
                        debug!(user = %relay.quota.name, %peer, "udp session limit reached, dropping packet");
                    } else {
                        slot.insert(Slot::Opening(vec![packet.into()]));
                        tokio::spawn(open_session(relay.clone(), peer));
                    }
                    None
                }
            }
        };
        if let Some(session) = session
            && let Err(e) = relay.forward(&session, packet).await
        {
            debug!(user = %relay.quota.name, %peer, "udp send failed: {e}");
            relay.remove_open(peer, &session);
        }
    }
}

async fn open_session(relay: Arc<Relay>, peer: SocketAddr) {
    let session = match connect_active(&relay).await {
        Ok(session) => Arc::new(session),
        Err(e) => {
            debug!(user = %relay.quota.name, %peer, "udp session failed: {e}");
            relay.sessions.lock().unwrap().remove(&peer);
            return;
        }
    };
    loop {
        let pending = {
            let mut map = relay.sessions.lock().unwrap();
            match map.get_mut(&peer) {
                Some(Slot::Opening(pending)) if !pending.is_empty() => std::mem::take(pending),
                _ => {
                    map.insert(peer, Slot::Open(session.clone()));
                    break;
                }
            }
        };
        for packet in pending {
            if let Err(e) = relay.forward(&session, &packet).await {
                debug!(user = %relay.quota.name, %peer, "udp send failed: {e}");
                relay.sessions.lock().unwrap().remove(&peer);
                return;
            }
        }
    }
    relay_downstream(relay, session, peer).await;
}

/// Opens a session to the pool's active target, falling through the
/// priority list on failure, same as the TCP side.
async fn connect_active(relay: &Relay) -> io::Result<Session> {
    let pool = &relay.pool;
    let mut last_err = None;
    for _ in 0..pool.len() {
        let (i, target) = pool.pick();
        match timeout(CONNECT_TIMEOUT, connect_upstream(&target)).await {
            Ok(Ok(upstream)) => {
                return Ok(Session {
                    upstream,
                    target_index: i,
                    last_active: AtomicU64::new(relay.now_ms()),
                });
            }
            Ok(Err(e)) => {
                last_err = Some(io::Error::new(
                    e.kind(),
                    format!("session to {target} failed: {e}"),
                ));
            }
            Err(_) => {
                last_err = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("session to {target} timed out"),
                ));
            }
        }
        pool.mark_down(i);
        if pool.pick().0 == i {
            break; // everything is down; no point cycling
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("no targets")))
}

async fn connect_upstream(target: &str) -> io::Result<UdpSocket> {
    let target_addr = crate::dns::resolve(target).await?[0];
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
async fn relay_downstream(relay: Arc<Relay>, session: Arc<Session>, peer: SocketAddr) {
    let quota = &relay.quota;
    let mut active_rx = relay.pool.subscribe();
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
                let idle = relay
                    .now_ms()
                    .saturating_sub(session.last_active.load(Ordering::Relaxed));
                if idle >= IDLE_TIMEOUT.as_millis() as u64 {
                    break;
                }
            }
            Ok(Err(e)) => {
                debug!(user = %quota.name, %peer, "udp session closed: {e}");
                break;
            }
            Ok(Ok(n)) => {
                // A reply from upstream is proof the target is alive: the
                // strongest health signal UDP can produce.
                relay.pool.confirm(session.target_index);
                if !quota.try_consume(n as u64, Direction::Download) {
                    break;
                }
                session.last_active.store(relay.now_ms(), Ordering::Relaxed);
                if relay.socket.send_to(&buf[..n], peer).await.is_err() {
                    break;
                }
            }
        }
    }
    // Only remove our own entry: the main loop may already have replaced it.
    relay.remove_open(peer, &session);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::SavedUsage;

    async fn tagged_echo(tag: &'static [u8]) -> String {
        limited_echo(tag, usize::MAX).await
    }

    async fn limited_echo(tag: &'static [u8], max_replies: usize) -> String {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut buf = [0u8; 1500];
            for _ in 0..max_replies {
                let Ok((n, peer)) = socket.recv_from(&mut buf).await else {
                    continue;
                };
                let reply = [tag, &buf[..n]].concat();
                let _ = socket.send_to(&reply, peer).await;
            }
            std::future::pending::<()>().await;
        });
        addr
    }

    async fn forwarder(
        pool: Arc<TargetPool>,
        quota: Arc<UserQuota>,
        max_sessions: usize,
    ) -> SocketAddr {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        tokio::spawn(serve_with_limit(socket, pool, quota, max_sessions));
        addr
    }

    async fn client(forwarder: SocketAddr) -> UdpSocket {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.connect(forwarder).await.unwrap();
        socket
    }

    async fn recv(socket: &UdpSocket) -> Option<Vec<u8>> {
        let mut buf = [0u8; 1500];
        let n = timeout(Duration::from_millis(500), socket.recv(&mut buf))
            .await
            .ok()?
            .ok()?;
        Some(buf[..n].to_vec())
    }

    fn unlimited() -> Arc<UserQuota> {
        Arc::new(UserQuota::new("a".into(), None, SavedUsage::default()))
    }

    #[tokio::test]
    async fn forwards_and_counts_both_directions() {
        let quota = unlimited();
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let client = client(forwarder(pool, quota.clone(), MAX_SESSIONS).await).await;

        client.send(b"ping").await.unwrap();

        assert_eq!(recv(&client).await.as_deref(), Some(&b"ping"[..]));
        assert_eq!((quota.upload(), quota.download()), (4, 4));
    }

    #[tokio::test]
    async fn packets_queued_while_opening_arrive_in_order() {
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let client = client(forwarder(pool, unlimited(), MAX_SESSIONS).await).await;

        for i in 0..5u8 {
            client.send(&[i]).await.unwrap();
        }

        for i in 0..5u8 {
            assert_eq!(recv(&client).await, Some(vec![i]));
        }
    }

    #[tokio::test]
    async fn failover_reopens_session_on_new_target() {
        let pool = TargetPool::new(
            "a".into(),
            vec![limited_echo(b"a", 1).await, tagged_echo(b"b").await],
            None,
        );
        let client = client(forwarder(pool.clone(), unlimited(), MAX_SESSIONS).await).await;
        client.send(b"1").await.unwrap();
        assert_eq!(recv(&client).await.as_deref(), Some(&b"a1"[..]));

        pool.mark_down(0);

        let mut reply = None;
        for _ in 0..20 {
            client.send(b"2").await.unwrap();
            reply = recv(&client).await;
            if reply.is_some() {
                break;
            }
        }
        assert_eq!(reply.as_deref(), Some(&b"b2"[..]));
    }

    #[tokio::test]
    async fn session_limit_drops_new_clients() {
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let addr = forwarder(pool, unlimited(), 1).await;
        let first = client(addr).await;
        first.send(b"x").await.unwrap();
        assert!(recv(&first).await.is_some());

        let second = client(addr).await;
        second.send(b"y").await.unwrap();
        assert!(recv(&second).await.is_none());

        first.send(b"z").await.unwrap();
        assert_eq!(recv(&first).await.as_deref(), Some(&b"z"[..]));
    }

    #[tokio::test]
    async fn exhausted_quota_drops_packets() {
        let quota = Arc::new(UserQuota::new("a".into(), Some(3), SavedUsage::default()));
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let client = client(forwarder(pool, quota.clone(), MAX_SESSIONS).await).await;

        client.send(b"four").await.unwrap();
        assert!(recv(&client).await.is_none());
        assert!(quota.is_exhausted());

        client.send(b"more").await.unwrap();
        assert!(recv(&client).await.is_none());
        assert_eq!(quota.upload(), 4);
    }
}
