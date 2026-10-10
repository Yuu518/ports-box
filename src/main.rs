mod api;
mod config;
mod dns;
mod pool;
mod quota;
mod runtime;
mod state;
mod tcp;
mod udp;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::runtime::Runtime;
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

    #[arg(long, help = "Do not watch the config file and reload it on change")]
    no_reload: bool,
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

#[cfg(unix)]
fn raise_nofile_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0
        || limit.rlim_cur >= limit.rlim_max
    {
        return;
    }
    let raised = libc::rlimit {
        rlim_cur: limit.rlim_max,
        rlim_max: limit.rlim_max,
    };
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
        info!(
            "open file limit raised from {} to {}",
            limit.rlim_cur, raised.rlim_cur
        );
    } else {
        warn!(
            "cannot raise open file limit from {}: {}",
            limit.rlim_cur,
            std::io::Error::last_os_error()
        );
    }
}

#[cfg(not(unix))]
fn raise_nofile_limit() {}

async fn run(args: Args) -> Result<(), String> {
    raise_nofile_limit();
    let raw = std::fs::read_to_string(&args.config)
        .map_err(|e| format!("cannot read config {}: {e}", args.config.display()))?;
    let config = config::parse(&raw, &args.config)?;

    let db = match &config.state_file {
        Some(state) if state.enabled => Some((
            Arc::new(StateDb::open(&state.path)?),
            Duration::from_secs(state.flush_secs),
        )),
        _ => None,
    };
    if db.is_none() {
        info!("state persistence disabled; usage resets on restart");
    }

    // Bind everything up front so a bad config fails fast, before any
    // forwarding starts.
    let mut runtime = Runtime::new(db.as_ref().map(|(db, _)| db.clone()));
    runtime.apply(&config).await?;

    if let Some(api) = &config.api {
        let listener = TcpListener::bind(api.listen)
            .await
            .map_err(|e| format!("cannot bind api {}: {e}", api.listen))?;
        info!("api listening on http://{}", api.listen);
        if api.token.is_none() && !api.listen.ip().is_loopback() {
            warn!(
                "api on {} has no token: anyone who can reach it can read usage",
                api.listen
            );
        }
        let router = api::router(runtime.users(), api.token.clone());
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                error!("api server failed: {e}");
            }
        });
    }

    let flusher = db.map(|(db, flush_interval)| {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(state::run_flush_task(
            db,
            runtime.users(),
            flush_interval,
            shutdown_rx,
        ));
        (shutdown_tx, task)
    });

    let (reload_tx, mut reload_rx) = mpsc::channel(1);
    if args.no_reload {
        info!("config auto-reload disabled");
    } else {
        tokio::spawn(runtime::watch_config(
            args.config.clone(),
            raw,
            runtime::RELOAD_POLL,
            reload_tx,
        ));
    }

    let shutdown = wait_for_shutdown();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            Some(raw) = reload_rx.recv() => {
                reload(&mut runtime, &config, &args.config, &raw).await;
            }
        }
    }

    let Some((shutdown_tx, flusher)) = flusher else {
        info!("shutting down");
        return Ok(());
    };
    info!("shutting down, flushing state");
    let _ = shutdown_tx.send(true);
    if let Err(e) = flusher.await {
        error!("flush task failed on shutdown: {e}");
    }
    Ok(())
}

async fn reload(runtime: &mut Runtime, initial: &Config, path: &Path, raw: &str) {
    info!("config file changed, reloading");
    let next = match config::parse(raw, path) {
        Ok(next) => next,
        Err(e) => {
            error!("config reload rejected, keeping current config: {e}");
            return;
        }
    };
    if let Err(e) = runtime.apply(&next).await {
        error!("config reload rejected, keeping current config: {e}");
        return;
    }
    if next.api != initial.api {
        warn!("api settings changed; restart to apply them");
    }
    if next.state_file != initial.state_file {
        warn!("state_file settings changed; restart to apply them");
    }
}

#[cfg(unix)]
async fn wait_for_shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
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
