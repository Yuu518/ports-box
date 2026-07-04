use std::io;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::quota::{exhausted, Direction, UserQuota};

const COPY_BUF: usize = 64 * 1024;

pub async fn serve(listener: TcpListener, target: Arc<str>, quota: Arc<UserQuota>) {
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
        let target = target.clone();
        let quota = quota.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(client, &target, &quota).await {
                debug!(user = %quota.name, %peer, "tcp connection closed: {e}");
            }
        });
    }
}

async fn handle(client: TcpStream, target: &str, quota: &UserQuota) -> io::Result<()> {
    let upstream = TcpStream::connect(target).await.map_err(|e| {
        io::Error::new(e.kind(), format!("connect to {target} failed: {e}"))
    })?;
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);

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
