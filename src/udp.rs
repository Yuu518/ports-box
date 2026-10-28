use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quinn_udp::{RecvMeta, Transmit, UdpSocketState};
use socket2::SockRef;
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tracing::{debug, warn};

use crate::pool::{CONNECT_TIMEOUT, TargetPool};
use crate::quota::{Direction, UserQuota, exhausted};
use crate::runtime::stopped;

const RECV_BUF: usize = 64 * 1024;
const RECV_BATCH: usize = 16;
const SOCKET_BUF: usize = 4 * 1024 * 1024;
const MAX_SHARDS: usize = 8;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_SESSIONS: usize = 4096;
const MAX_PENDING_PACKETS: usize = 32;

pub struct Endpoint {
    socket: UdpSocket,
    state: UdpSocketState,
    connected: bool,
}

impl Endpoint {
    fn new(socket: UdpSocket, connected: bool) -> io::Result<Self> {
        let state = fragmentation::preserve(&socket, || UdpSocketState::new((&socket).into()))?;
        let sock = SockRef::from(&socket);
        let _ = sock.set_recv_buffer_size(SOCKET_BUF);
        let _ = sock.set_send_buffer_size(SOCKET_BUF);
        Ok(Self {
            socket,
            state,
            connected,
        })
    }

    async fn recv(&self, bufs: &mut [IoSliceMut<'_>], meta: &mut [RecvMeta]) -> io::Result<usize> {
        self.socket
            .async_io(Interest::READABLE, || {
                self.state.recv((&self.socket).into(), bufs, meta)
            })
            .await
    }

    async fn send(&self, destination: SocketAddr, data: &[u8], stride: usize) -> io::Result<()> {
        if stride == 0 || data.len() <= stride {
            return self.send_one(destination, data).await;
        }
        let batch = stride * self.state.max_gso_segments().max(1);
        for chunk in data.chunks(batch) {
            if chunk.len() <= stride {
                self.send_one(destination, chunk).await?;
            } else if self
                .send_segmented(destination, chunk, stride)
                .await
                .is_err()
            {
                for segment in chunk.chunks(stride) {
                    self.send_one(destination, segment).await?;
                }
            }
        }
        Ok(())
    }

    async fn send_one(&self, destination: SocketAddr, data: &[u8]) -> io::Result<()> {
        if self.connected {
            self.socket.send(data).await?;
        } else {
            self.socket.send_to(data, destination).await?;
        }
        Ok(())
    }

    async fn send_segmented(
        &self,
        destination: SocketAddr,
        contents: &[u8],
        stride: usize,
    ) -> io::Result<()> {
        let transmit = Transmit {
            destination,
            ecn: None,
            contents,
            segment_size: Some(stride),
            src_ip: None,
        };
        self.socket
            .async_io(Interest::WRITABLE, || {
                self.state.try_send((&self.socket).into(), &transmit)
            })
            .await
    }
}

#[cfg(target_os = "linux")]
mod fragmentation {
    use std::os::fd::AsRawFd;

    use libc::{c_int, socklen_t};

    const OPTIONS: [(c_int, c_int); 3] = [
        (libc::IPPROTO_IP, libc::IP_MTU_DISCOVER),
        (libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER),
        (libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG),
    ];

    pub fn preserve<R>(socket: &impl AsRawFd, f: impl FnOnce() -> R) -> R {
        let fd = socket.as_raw_fd();
        let saved = OPTIONS.map(|(level, name)| {
            let mut value: c_int = 0;
            let mut len = size_of::<c_int>() as socklen_t;
            let ok =
                unsafe { libc::getsockopt(fd, level, name, (&raw mut value).cast(), &mut len) }
                    == 0;
            ok.then_some(value)
        });
        let result = f();
        for ((level, name), value) in OPTIONS.into_iter().zip(saved) {
            if let Some(value) = value {
                unsafe {
                    libc::setsockopt(
                        fd,
                        level,
                        name,
                        (&raw const value).cast(),
                        size_of::<c_int>() as socklen_t,
                    );
                }
            }
        }
        result
    }
}

#[cfg(not(target_os = "linux"))]
mod fragmentation {
    pub fn preserve<T, R>(_socket: &T, f: impl FnOnce() -> R) -> R {
        f()
    }
}

pub fn bind(addr: SocketAddr) -> io::Result<Vec<Arc<Endpoint>>> {
    let shards = std::thread::available_parallelism()
        .map_or(1, NonZero::get)
        .min(MAX_SHARDS);
    bind_shards(addr, shards)
}

fn bind_shards(addr: SocketAddr, shards: usize) -> io::Result<Vec<Arc<Endpoint>>> {
    bind_sockets(addr, shards)?
        .into_iter()
        .map(|socket| Endpoint::new(socket, false).map(Arc::new))
        .collect()
}

#[cfg(target_os = "linux")]
fn bind_sockets(mut addr: SocketAddr, shards: usize) -> io::Result<Vec<UdpSocket>> {
    use socket2::{Domain, Protocol, Socket, Type};

    let mut sockets = Vec::with_capacity(shards);
    for _ in 0..shards.max(1) {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        if shards > 1 {
            socket.set_reuse_port(true)?;
        }
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        if addr.port() == 0 {
            addr = socket
                .local_addr()?
                .as_socket()
                .ok_or_else(|| io::Error::other("bound socket has no inet address"))?;
        }
        sockets.push(UdpSocket::from_std(socket.into())?);
    }
    Ok(sockets)
}

#[cfg(not(target_os = "linux"))]
fn bind_sockets(addr: SocketAddr, _shards: usize) -> io::Result<Vec<UdpSocket>> {
    let socket = std::net::UdpSocket::bind(addr)?;
    socket.set_nonblocking(true)?;
    Ok(vec![UdpSocket::from_std(socket)?])
}

/// One NAT-style session: a dedicated upstream socket per client address.
struct Session {
    upstream: Endpoint,
    target: SocketAddr,
    /// Which pool target this session is connected to; the session ends
    /// when that target stops being the active one.
    target_index: usize,
    /// Milliseconds since the relay's `epoch` of the last packet in either
    /// direction; used for idle collection.
    last_active: AtomicU64,
}

struct Pending {
    data: Box<[u8]>,
    stride: usize,
}

enum Slot {
    Opening(Vec<Pending>),
    Open(Arc<Session>),
}

struct Relay {
    endpoint: Arc<Endpoint>,
    sessions: Mutex<HashMap<SocketAddr, Slot>>,
    epoch: Instant,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
    max_sessions: usize,
    idle_timeout: Duration,
    stop: watch::Receiver<()>,
}

impl Relay {
    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    fn idle_deadline(&self, session: &Session) -> Instant {
        self.epoch
            + Duration::from_millis(session.last_active.load(Ordering::Relaxed))
            + self.idle_timeout
    }

    async fn forward(&self, session: &Session, data: &[u8], stride: usize) -> io::Result<()> {
        if !self.quota.try_consume(data.len() as u64, Direction::Upload) {
            return Ok(());
        }
        session.last_active.store(self.now_ms(), Ordering::Relaxed);
        if let Err(e) = session.upstream.send(session.target, data, stride).await {
            // An ICMP unreachable surfaces here on connected sockets: treat
            // it as the target being down and shed the session so the next
            // packet reopens against the new active target.
            self.pool.mark_down(session.target_index);
            return Err(e);
        }
        Ok(())
    }

    async fn dispatch(self: &Arc<Self>, peer: SocketAddr, data: &[u8], stride: usize) {
        let session = {
            let mut map = self.sessions.lock().unwrap();
            let len = map.len();
            match map.entry(peer) {
                Entry::Occupied(mut slot) => match slot.get_mut() {
                    Slot::Open(session) => Some(session.clone()),
                    Slot::Opening(pending) => {
                        if pending.len() < MAX_PENDING_PACKETS {
                            pending.push(Pending {
                                data: data.into(),
                                stride,
                            });
                        }
                        None
                    }
                },
                Entry::Vacant(slot) => {
                    if len >= self.max_sessions {
                        debug!(user = %self.quota.name, %peer, "udp session limit reached, dropping packet");
                    } else {
                        slot.insert(Slot::Opening(vec![Pending {
                            data: data.into(),
                            stride,
                        }]));
                        tokio::spawn(open_session(self.clone(), peer));
                    }
                    None
                }
            }
        };
        if let Some(session) = session
            && let Err(e) = self.forward(&session, data, stride).await
        {
            debug!(user = %self.quota.name, %peer, "udp send failed: {e}");
            self.remove_open(peer, &session);
        }
    }

    fn remove_open(&self, peer: SocketAddr, session: &Arc<Session>) {
        let mut map = self.sessions.lock().unwrap();
        if matches!(map.get(&peer), Some(Slot::Open(s)) if Arc::ptr_eq(s, session)) {
            map.remove(&peer);
        }
    }
}

pub async fn serve(
    endpoints: Vec<Arc<Endpoint>>,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
    stop: watch::Receiver<()>,
) {
    serve_with_limit(endpoints, pool, quota, stop, MAX_SESSIONS, IDLE_TIMEOUT).await
}

async fn serve_with_limit(
    endpoints: Vec<Arc<Endpoint>>,
    pool: Arc<TargetPool>,
    quota: Arc<UserQuota>,
    stop: watch::Receiver<()>,
    max_sessions: usize,
    idle_timeout: Duration,
) {
    let per_shard = max_sessions.div_ceil(endpoints.len().max(1));
    let mut shards = JoinSet::new();
    for endpoint in endpoints {
        shards.spawn(serve_shard(Arc::new(Relay {
            endpoint,
            sessions: Mutex::default(),
            epoch: Instant::now(),
            pool: pool.clone(),
            quota: quota.clone(),
            max_sessions: per_shard,
            idle_timeout,
            stop: stop.clone(),
        })));
    }
    while shards.join_next().await.is_some() {}
}

async fn serve_shard(relay: Arc<Relay>) {
    let shutdown = stopped(relay.stop.clone());
    tokio::pin!(shutdown);
    let mut storage = vec![0u8; RECV_BATCH * RECV_BUF];
    let mut bufs: Vec<IoSliceMut> = storage.chunks_mut(RECV_BUF).map(IoSliceMut::new).collect();
    let mut meta = [RecvMeta::default(); RECV_BATCH];

    loop {
        let received = tokio::select! {
            biased;
            _ = &mut shutdown => return,
            received = relay.endpoint.recv(&mut bufs, &mut meta) => received,
        };
        let count = match received {
            Ok(count) => count,
            Err(e) => {
                warn!(user = %relay.quota.name, "udp recv failed: {e}");
                continue;
            }
        };
        if relay.quota.is_exhausted() {
            continue;
        }
        for (buf, meta) in bufs.iter().zip(&meta).take(count) {
            relay
                .dispatch(meta.addr, &buf[..meta.len], meta.stride)
                .await;
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
            if let Err(e) = relay.forward(&session, &packet.data, packet.stride).await {
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
            Ok(Ok((upstream, target))) => {
                return Ok(Session {
                    upstream,
                    target,
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

async fn connect_upstream(target: &str) -> io::Result<(Endpoint, SocketAddr)> {
    let target_addr = crate::dns::resolve(target).await?[0];
    let local: SocketAddr = if target_addr.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let upstream = UdpSocket::bind(local).await?;
    upstream.connect(target_addr).await?;
    Ok((Endpoint::new(upstream, true)?, target_addr))
}

/// Copies upstream replies back to the client, counting them as download
/// traffic, until the session idles out, the quota is exhausted, or the
/// pool's active target moves away (failover or failback): ending the
/// session then makes the client's next packet reopen on the right target.
async fn relay_downstream(relay: Arc<Relay>, session: Arc<Session>, peer: SocketAddr) {
    let quota = &relay.quota;
    let mut active_rx = relay.pool.subscribe();
    let mut storage = vec![0u8; RECV_BUF];
    let mut bufs = [IoSliceMut::new(&mut storage)];
    let mut meta = [RecvMeta::default()];
    let idle = tokio::time::sleep_until(relay.idle_deadline(&session));
    let drained = exhausted(quota.subscribe());
    let removed = stopped(relay.stop.clone());
    let moved = active_moved(&mut active_rx, session.target_index);
    tokio::pin!(idle, drained, removed, moved);
    loop {
        let received = tokio::select! {
            r = session.upstream.recv(&mut bufs, &mut meta) => r,
            () = &mut idle => {
                let deadline = relay.idle_deadline(&session);
                if deadline <= Instant::now() {
                    break;
                }
                idle.as_mut().reset(deadline);
                continue;
            }
            () = &mut drained => break,
            () = &mut removed => break,
            () = &mut moved => {
                debug!(user = %quota.name, %peer, "udp session ended: active target changed");
                break;
            }
        };
        match received {
            Err(e) => {
                debug!(user = %quota.name, %peer, "udp session closed: {e}");
                break;
            }
            Ok(_) => {
                // A reply from upstream is proof the target is alive: the
                // strongest health signal UDP can produce.
                relay.pool.confirm(session.target_index);
                let RecvMeta { len, stride, .. } = meta[0];
                if !quota.try_consume(len as u64, Direction::Download) {
                    break;
                }
                session.last_active.store(relay.now_ms(), Ordering::Relaxed);
                if relay
                    .endpoint
                    .send(peer, &bufs[0][..len], stride)
                    .await
                    .is_err()
                {
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
    use crate::config::Metering;
    use crate::quota::SavedUsage;
    use crate::runtime::never_stop;

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
        sharded_forwarder(pool, quota, max_sessions, 1, IDLE_TIMEOUT).await
    }

    async fn sharded_forwarder(
        pool: Arc<TargetPool>,
        quota: Arc<UserQuota>,
        max_sessions: usize,
        shards: usize,
        idle_timeout: Duration,
    ) -> SocketAddr {
        let endpoints = bind_shards("127.0.0.1:0".parse().unwrap(), shards).unwrap();
        let addr = endpoints[0].socket.local_addr().unwrap();
        tokio::spawn(serve_with_limit(
            endpoints,
            pool,
            quota,
            never_stop(),
            max_sessions,
            idle_timeout,
        ));
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
        Arc::new(UserQuota::new(
            "a".into(),
            None,
            SavedUsage::default(),
            Metering::Max,
        ))
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
    async fn sharded_listener_serves_every_client() {
        let quota = unlimited();
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let addr = sharded_forwarder(pool, quota.clone(), MAX_SESSIONS, 4, IDLE_TIMEOUT).await;
        let mut clients = Vec::new();
        for _ in 0..16 {
            clients.push(client(addr).await);
        }

        for (i, client) in clients.iter().enumerate() {
            let payload = [i as u8; 8];
            client.send(&payload).await.unwrap();
            assert_eq!(recv(client).await, Some(payload.to_vec()));
        }
        assert_eq!((quota.upload(), quota.download()), (128, 128));
    }

    #[tokio::test]
    async fn segmented_send_keeps_datagram_boundaries() {
        let quota = unlimited();
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let addr = forwarder(pool, quota.clone(), MAX_SESSIONS).await;
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.connect(addr).await.unwrap();
        let sender = Endpoint::new(socket, true).unwrap();
        let data: Vec<u8> = (0..10u8).flat_map(|i| [i; 1000]).collect();

        sender.send(addr, &data, 1000).await.unwrap();

        let mut replies = Vec::new();
        while let Some(reply) = recv(&sender.socket).await {
            replies.push(reply);
            if replies.len() == 10 {
                break;
            }
        }
        replies.sort();
        let expected: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 1000]).collect();
        assert_eq!(replies, expected);
        assert_eq!((quota.upload(), quota.download()), (10_000, 10_000));
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
    async fn idle_session_is_reclaimed() {
        let pool = TargetPool::new("a".into(), vec![tagged_echo(b"").await], None);
        let addr = sharded_forwarder(pool, unlimited(), 1, 1, Duration::from_millis(200)).await;
        let first = client(addr).await;
        first.send(b"x").await.unwrap();
        assert!(recv(&first).await.is_some());

        let second = client(addr).await;
        second.send(b"y").await.unwrap();
        assert!(recv(&second).await.is_none());

        second.send(b"z").await.unwrap();
        assert_eq!(recv(&second).await.as_deref(), Some(&b"z"[..]));
    }

    #[tokio::test]
    async fn client_traffic_keeps_session_open() {
        let pool = TargetPool::new("a".into(), vec![limited_echo(b"", 1).await], None);
        let addr = sharded_forwarder(pool, unlimited(), 1, 1, Duration::from_millis(300)).await;
        let first = client(addr).await;
        first.send(b"x").await.unwrap();
        assert!(recv(&first).await.is_some());

        for _ in 0..10 {
            first.send(b"x").await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let second = client(addr).await;
        second.send(b"y").await.unwrap();
        assert!(recv(&second).await.is_none());
    }

    #[tokio::test]
    async fn exhausted_quota_drops_packets() {
        let quota = Arc::new(UserQuota::new(
            "a".into(),
            Some(3),
            SavedUsage::default(),
            Metering::Max,
        ));
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
