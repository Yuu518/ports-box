mod api;
mod config;
mod quota;
mod state;
mod tcp;
mod udp;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::config::format_size;
use crate::quota::UserQuota;
use crate::state::StateDb;

#[derive(Parser)]
#[command(version, about = "Port forwarder with per-user traffic quotas")]
struct Args {
    /// Config file path (.json, or YAML with a .yaml/.yml extension)
    #[arg(short, long, default_value = "config.json")]
    config: PathBuf,

    /// Working directory to chdir into before loading anything
    #[arg(short, long)]
    dir: Option<PathBuf>,
}

fn main() -> ExitCode {
    let env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "ports_box=info".into())
    };
    // Under systemd, journald already prefixes every line with a timestamp.
    if std::env::var_os("JOURNAL_STREAM").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter())
            .with_target(false)
            .without_time()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter())
            .with_target(false)
            .with_timer(tracing_subscriber::fmt::time::ChronoLocal::new(
                "%Y-%m-%d %H:%M:%S%.3f".into(),
            ))
            .init();
    }

    let args = Args::parse();
    if let Some(dir) = &args.dir
        && let Err(e) = std::env::set_current_dir(dir)
    {
        error!("cannot change to directory {}: {e}", dir.display());
        return ExitCode::FAILURE;
    }

    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
        .block_on(run(args))
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    let config = config::load(&args.config)?;
    let quotas = config::resolve_quotas(&config);

    let db = Arc::new(StateDb::open(&config.state_db)?);
    let saved = db.load()?;

    let mut users: Vec<Arc<UserQuota>> = Vec::new();
    for user in &config.users {
        let limit = quotas[&user.name];
        let (upload, download) = saved.get(&user.name).copied().unwrap_or((0, 0));
        let quota = Arc::new(UserQuota::new(user.name.clone(), limit, upload, download));
        match limit {
            None => info!(
                user = %user.name,
                "quota unlimited ({} used)",
                format_size(quota.used()),
            ),
            Some(limit) if quota.is_exhausted() => warn!(
                user = %user.name,
                "quota already exhausted ({} used of {})",
                format_size(quota.used()),
                format_size(limit),
            ),
            Some(limit) => info!(
                user = %user.name,
                "quota {} ({} used, {} remaining)",
                format_size(limit),
                format_size(quota.used()),
                format_size(quota.remaining().unwrap_or(0)),
            ),
        }
        users.push(quota);
    }
    let users = Arc::new(users);

    // Bind everything up front so a bad config fails fast, before any
    // forwarding starts.
    for (user, quota) in config.users.iter().zip(users.iter()) {
        for rule in &user.rules {
            if !rule.enabled {
                info!(user = %user.name, listen = %rule.listen, "rule disabled, skipping");
                continue;
            }
            let target: Arc<str> = rule.target.as_str().into();
            if rule.protocol.tcp() {
                let listener = TcpListener::bind(rule.listen)
                    .await
                    .map_err(|e| format!("cannot bind tcp {}: {e}", rule.listen))?;
                tokio::spawn(tcp::serve(listener, target.clone(), quota.clone()));
            }
            if rule.protocol.udp() {
                let socket = UdpSocket::bind(rule.listen)
                    .await
                    .map_err(|e| format!("cannot bind udp {}: {e}", rule.listen))?;
                tokio::spawn(udp::serve(socket, target.clone(), quota.clone()));
            }
            info!(user = %user.name, "{} {} -> {}", rule.protocol, rule.listen, rule.target);
        }
    }

    if let Some(api) = &config.api {
        let listener = TcpListener::bind(api.listen)
            .await
            .map_err(|e| format!("cannot bind api {}: {e}", api.listen))?;
        info!("api listening on http://{}", api.listen);
        let router = api::router(users.clone(), api.token.clone());
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                error!("api server failed: {e}");
            }
        });
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let flusher = tokio::spawn(state::run_flush_task(
        db,
        users,
        Duration::from_secs(config.state_flush_secs),
        shutdown_rx,
    ));

    wait_for_shutdown().await;
    info!("shutting down, flushing state");
    let _ = shutdown_tx.send(true);
    if let Err(e) = flusher.await {
        error!("flush task failed on shutdown: {e}");
    }
    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut sigterm = signal(SignalKind::terminate()).expect("cannot install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}
