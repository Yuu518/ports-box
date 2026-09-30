use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

use hickory_resolver::config::ResolverConfig;
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::{Resolver, TokioResolver};
use tokio::net::TcpStream;
use tracing::warn;

/// Process-wide resolver built from the host's DNS configuration
/// (/etc/resolv.conf and /etc/hosts on Linux). Its cache expires entries
/// by record TTL, so a changed DNS answer takes effect once the old TTL
/// runs out — no restart needed.
fn resolver() -> &'static TokioResolver {
    static RESOLVER: OnceLock<TokioResolver> = OnceLock::new();
    RESOLVER.get_or_init(|| {
        Resolver::builder_tokio()
            .unwrap_or_else(|e| {
                warn!("cannot read system DNS config, using fallback resolver: {e}");
                Resolver::builder_with_config(
                    ResolverConfig::default(),
                    TokioConnectionProvider::default(),
                )
            })
            .build()
    })
}

/// Resolves a `host:port` target to its socket addresses. IP literals
/// (including bracketed IPv6 like `[::1]:80`) pass through without touching
/// DNS.
pub async fn resolve(target: &str) -> io::Result<Vec<SocketAddr>> {
    if let Ok(addr) = target.parse() {
        return Ok(vec![addr]);
    }
    let (host, port) = split_target(target).map_err(io::Error::other)?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    let addrs: Vec<SocketAddr> = resolver()
        .lookup_ip(host)
        .await
        .map_err(|e| io::Error::other(format!("cannot resolve {host}: {e}")))?
        .iter()
        .map(|ip| SocketAddr::new(ip, port))
        .collect();
    if addrs.is_empty() {
        return Err(io::Error::other(format!("no addresses for {host}")));
    }
    Ok(addrs)
}

pub async fn connect_tcp(target: &str) -> io::Result<TcpStream> {
    let addrs = resolve(target).await?;
    TcpStream::connect(addrs.as_slice()).await
}

/// Splits a `host:port` target. Only validates shape, not resolvability, so
/// config loading can reject malformed targets up front.
pub fn split_target(target: &str) -> Result<(&str, u16), String> {
    let (host, port) = target
        .rsplit_once(':')
        .ok_or_else(|| format!("target {target:?} is missing a port"))?;
    let port = port
        .parse()
        .map_err(|_| format!("target {target:?} has an invalid port"))?;
    if host.is_empty() {
        return Err(format!("target {target:?} has an empty host"));
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ip_literals_skip_dns() {
        assert_eq!(
            resolve("10.0.0.2:80").await.unwrap(),
            vec!["10.0.0.2:80".parse::<SocketAddr>().unwrap()],
        );
        assert_eq!(
            resolve("[::1]:80").await.unwrap(),
            vec!["[::1]:80".parse::<SocketAddr>().unwrap()],
        );
    }

    #[tokio::test]
    async fn localhost_resolves_via_hosts() {
        let addrs = resolve("localhost:8080").await.unwrap();
        assert!(!addrs.is_empty());
        for addr in addrs {
            assert_eq!(addr.port(), 8080);
            assert!(addr.ip().is_loopback());
        }
    }

    #[test]
    fn split_target_validates_shape() {
        assert_eq!(split_target("example.com:80").unwrap(), ("example.com", 80));
        assert_eq!(split_target("10.0.0.2:443").unwrap(), ("10.0.0.2", 443));
        assert!(split_target("example.com").is_err());
        assert!(split_target("example.com:99999").is_err());
        assert!(split_target("example.com:http").is_err());
        assert!(split_target(":80").is_err());
    }
}
