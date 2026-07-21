use std::io;
use std::sync::Arc;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::pool::{CONNECT_TIMEOUT, TargetPool};
use crate::quota::{Direction, UserQuota, exhausted};

const COPY_BUF: usize = 64 * 1024;
// Probe after 60s of silence, then every 10s; the kernel's default retry
// count reaps dead peers in roughly two minutes without touching
// legitimately idle connections.
const KEEPALIVE_TIME: Duration = Duration::from_secs(60);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

fn set_keepalive(stream: &TcpStream) {
    let ka = TcpKeepalive::new()
        .with_time(KEEPALIVE_TIME)
        .with_interval(KEEPALIVE_INTERVAL);
    let _ = SockRef::from(stream).set_tcp_keepalive(&ka);
}

pub async fn serve(listener: TcpListener, pool: Arc<TargetPool>, quota: Arc<UserQuota>) {
    loop {
        let (client, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(user = %quota.name, "tcp accept failed: {e}");
                continue;
            }
        };
        if quota.is_exhausted() {
            debug!(user = %quota.name, %peer, "quota exhausted, rejecting tcp connection");
            continue; // dropping the stream closes it
        }
        let pool = pool.clone();
        let quota = quota.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(client, &pool, &quota).await {
                debug!(user = %quota.name, %peer, "tcp connection closed: {e}");
            }
        });
    }
}

/// Connects to the pool's active target, falling through the priority list
/// on failure. Gives up once the active index stops moving (all down).
async fn connect_active(pool: &Arc<TargetPool>) -> io::Result<TcpStream> {
    let mut last_err = None;
    for _ in 0..pool.len() {
        let (i, target) = pool.pick();
        // The timeout covers DNS resolution plus the connect itself.
        let connect = async { TcpStream::connect(crate::dns::resolve(&target).await?).await };
        match timeout(CONNECT_TIMEOUT, connect).await {
            Ok(Ok(stream)) => {
                pool.confirm(i);
                return Ok(stream);
            }
            Ok(Err(e)) => {
                last_err = Some(io::Error::new(
                    e.kind(),
                    format!("connect to {target} failed: {e}"),
                ));
            }
            Err(_) => {
                last_err = Some(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connect to {target} timed out"),
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

async fn handle(client: TcpStream, pool: &Arc<TargetPool>, quota: &UserQuota) -> io::Result<()> {
    let upstream = connect_active(pool).await?;
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);
    set_keepalive(&client);
    set_keepalive(&upstream);

    let (client_r, client_w) = client.into_split();
    let (upstream_r, upstream_w) = upstream.into_split();

    tokio::select! {
        r = async {
            tokio::try_join!(
                copy_counted(client_r, upstream_w, quota, Direction::Upload),
                copy_counted(upstream_r, client_w, quota, Direction::Download),
            )
        } => r.map(|_| ()),
        // Kill in-flight connections the moment the quota runs out.
        _ = exhausted(quota.subscribe()) => {
            info!(user = %quota.name, "quota exhausted, dropping tcp connection");
            Ok(())
        }
    }
}

async fn copy_counted<R, W>(
    mut reader: R,
    mut writer: W,
    quota: &UserQuota,
    direction: Direction,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; COPY_BUF];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            // Propagate EOF as a half-close and let the other direction run on.
            let _ = writer.shutdown().await;
            return Ok(());
        }
        if !quota.try_consume(n as u64, direction) {
            return Err(io::Error::other("quota exhausted"));
        }
        writer.write_all(&buf[..n]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keepalive_is_enabled_on_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });
        let stream = TcpStream::connect(addr).await.unwrap();
        assert!(!SockRef::from(&stream).keepalive().unwrap());

        set_keepalive(&stream);

        assert!(SockRef::from(&stream).keepalive().unwrap());
        accept.await.unwrap();
    }

    #[tokio::test]
    async fn connect_success_recovers_pinned_primary() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary = listener.local_addr().unwrap().to_string();
        let accept = tokio::spawn(async move {
            let _ = listener.accept().await.unwrap();
        });
        let pool = TargetPool::new("a".into(), vec![primary, "127.0.0.1:1".into()], None);
        // All targets down pins the active index to the primary; a real
        // connection succeeding must mark it healthy again.
        pool.mark_down(0);
        pool.mark_down(1);
        assert_eq!(pool.pick().0, 0);
        assert!(!pool.is_healthy(0));

        let stream = connect_active(&pool).await.unwrap();

        assert_eq!(pool.pick().0, 0);
        assert!(pool.is_healthy(0));
        drop(stream);
        accept.await.unwrap();
    }
}
