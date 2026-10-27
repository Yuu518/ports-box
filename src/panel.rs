use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::rejection::JsonRejection;
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tracing::warn;

use crate::api::{UserUsage, token_matches};
use crate::config::{ByteSize, Config, PanelConfig, Protocol, Rule, UserConfig};
use crate::quota::clock;
use crate::runtime::{PoolMap, UserSet};

const PAGE: &str = include_str!("panel.html");
const COOKIE: &str = "ports_box_session";
const SESSION_TTL: Duration = Duration::from_secs(7 * 24 * 3600);
const LOGIN_DELAY: Duration = Duration::from_secs(1);
const DASHBOARD: &str = "/dashboard";
const LOGIN: &str = "/dashboard/api/login";
const LOGOUT: &str = "/dashboard/api/logout";

pub struct Live {
    pub config: Config,
    pub revision: u64,
    pub from_panel: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Store {
    pub base: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_quota: Option<ByteSize>,
    pub users: Vec<UserConfig>,
}

pub fn load_store(path: &Path) -> Option<Store> {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => {
            warn!("cannot read panel store {}: {e}", path.display());
            return None;
        }
    };
    match serde_json::from_str(&raw) {
        Ok(store) => Some(store),
        Err(e) => {
            warn!("ignoring invalid panel store {}: {e}", path.display());
            None
        }
    }
}

pub async fn write_store(path: &Path, store: &Store) -> Result<(), String> {
    let failed = |e: std::io::Error| format!("cannot write panel store {}: {e}", path.display());
    let json = serde_json::to_string_pretty(store)
        .map_err(|e| format!("cannot encode panel store: {e}"))?;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(failed)?;
    }
    let mut partial = path.as_os_str().to_owned();
    partial.push(".tmp");
    tokio::fs::write(&partial, json).await.map_err(failed)?;
    tokio::fs::rename(&partial, path).await.map_err(failed)
}

pub fn remove_store(path: &Path) {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!("cannot remove panel store {}: {e}", path.display()),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigEdit {
    pub revision: u64,
    #[serde(default)]
    pub total_quota: Option<ByteSize>,
    pub users: Vec<UserConfig>,
}

#[derive(Debug)]
pub enum SaveError {
    Conflict(String),
    Invalid(String),
    Failed(String),
}

pub struct SaveRequest {
    pub edit: ConfigEdit,
    pub reply: oneshot::Sender<Result<u64, SaveError>>,
}

struct Session {
    expires: Instant,
    logged_in: bool,
}

#[derive(Clone)]
struct PanelState {
    live: watch::Receiver<Arc<Live>>,
    users: watch::Receiver<Arc<UserSet>>,
    pools: watch::Receiver<Arc<PoolMap>>,
    credentials: Arc<PanelConfig>,
    sessions: Arc<Mutex<HashMap<String, Session>>>,
    saves: mpsc::Sender<SaveRequest>,
    now_hour: fn() -> i64,
}

impl PanelState {
    fn session(&self, headers: &HeaderMap) -> Option<(String, bool)> {
        let id = headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(';'))
            .find_map(|pair| pair.trim().strip_prefix(COOKIE)?.strip_prefix('='))?;
        let now = Instant::now();
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, session| session.expires > now);
        sessions
            .get(id)
            .map(|session| (id.to_owned(), session.logged_in))
    }

    fn passes_gate(&self, session: Option<&(String, bool)>) -> bool {
        session.is_some() || !self.credentials.has_token()
    }

    fn open_session(&self, logged_in: bool) -> String {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes).expect("os random source unavailable");
        let id: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        self.sessions.lock().unwrap().insert(
            id.clone(),
            Session {
                expires: Instant::now() + SESSION_TTL,
                logged_in,
            },
        );
        format!(
            "{COOKIE}={id}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}",
            SESSION_TTL.as_secs()
        )
    }
}

#[derive(Serialize)]
struct UserStatus {
    #[serde(flatten)]
    usage: UserUsage,
    limit_bytes: Option<u64>,
    used_bytes: u64,
    upload_bytes: u64,
    download_bytes: u64,
    exhausted: bool,
    rules: Vec<RuleStatus>,
}

#[derive(Serialize)]
struct RuleStatus {
    listen: SocketAddr,
    protocol: Protocol,
    tag: Option<String>,
    enabled: bool,
    targets: Vec<TargetStatus>,
}

#[derive(Serialize)]
struct TargetStatus {
    addr: String,
    healthy: bool,
    active: bool,
}

#[derive(Serialize)]
struct ConfigView<'a> {
    revision: u64,
    source: &'static str,
    total_quota: Option<ByteSize>,
    users: &'a [UserConfig],
}

#[derive(Deserialize)]
struct Login {
    username: String,
    password: String,
}

pub fn router(
    credentials: &PanelConfig,
    live: watch::Receiver<Arc<Live>>,
    users: watch::Receiver<Arc<UserSet>>,
    pools: watch::Receiver<Arc<PoolMap>>,
    saves: mpsc::Sender<SaveRequest>,
) -> Router {
    router_with_clock(
        credentials,
        live,
        users,
        pools,
        saves,
        clock::current_hour_id,
    )
}

fn router_with_clock(
    credentials: &PanelConfig,
    live: watch::Receiver<Arc<Live>>,
    users: watch::Receiver<Arc<UserSet>>,
    pools: watch::Receiver<Arc<PoolMap>>,
    saves: mpsc::Sender<SaveRequest>,
    now_hour: fn() -> i64,
) -> Router {
    let state = PanelState {
        live,
        users,
        pools,
        credentials: Arc::new(credentials.clone()),
        sessions: Arc::default(),
        saves,
        now_hour,
    };
    let api = Router::new()
        .route(LOGIN, post(login))
        .route(LOGOUT, post(logout))
        .route("/dashboard/api/status", get(status))
        .route("/dashboard/api/config", get(get_config).put(put_config))
        .route_layer(middleware::from_fn_with_state(state.clone(), guard));
    Router::new()
        .route("/", get(root))
        .route(DASHBOARD, get(dashboard))
        .merge(api)
        .with_state(state)
}

fn redirect(cookie: Option<String>) -> Response {
    let mut response = (StatusCode::FOUND, [(header::LOCATION, DASHBOARD)]).into_response();
    if let Some(cookie) = cookie.and_then(|cookie| cookie.parse().ok()) {
        response.headers_mut().insert(header::SET_COOKIE, cookie);
    }
    response
}

fn enter(
    state: &PanelState,
    session: Option<&(String, bool)>,
    query: &HashMap<String, String>,
) -> Option<Response> {
    let given = query
        .get("token")
        .filter(|_| state.credentials.has_token())?;
    if !token_matches(given, &state.credentials.token) {
        return None;
    }
    Some(redirect(
        session
            .is_none()
            .then(|| state.open_session(!state.credentials.has_login())),
    ))
}

async fn root(
    State(state): State<PanelState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let session = state.session(&headers);
    if let Some(response) = enter(&state, session.as_ref(), &query) {
        return response;
    }
    if state.passes_gate(session.as_ref()) {
        redirect(None)
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn dashboard(
    State(state): State<PanelState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let session = state.session(&headers);
    if let Some(response) = enter(&state, session.as_ref(), &query) {
        return response;
    }
    if state.passes_gate(session.as_ref()) {
        ([(header::CACHE_CONTROL, "no-store")], Html(PAGE)).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn guard(State(state): State<PanelState>, request: Request, next: Next) -> Response {
    let open = matches!(request.uri().path(), LOGIN | LOGOUT);
    let session = state.session(request.headers());
    if !state.passes_gate(session.as_ref()) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let logged_in =
        !state.credentials.has_login() || session.is_some_and(|(_, logged_in)| logged_in);
    if !logged_in && !open {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

async fn login(
    State(state): State<PanelState>,
    headers: HeaderMap,
    Json(login): Json<Login>,
) -> Response {
    if !state.credentials.has_login() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let username = token_matches(&login.username, &state.credentials.username);
    let password = token_matches(&login.password, &state.credentials.password);
    if !(username & password) {
        tokio::time::sleep(LOGIN_DELAY).await;
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Some((id, _)) = state.session(&headers) {
        state.sessions.lock().unwrap().remove(&id);
    }
    (
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, state.open_session(true))],
    )
        .into_response()
}

async fn logout(State(state): State<PanelState>, headers: HeaderMap) -> Response {
    if let Some((id, _)) = state.session(&headers) {
        let mut sessions = state.sessions.lock().unwrap();
        if !state.credentials.has_login() {
            sessions.remove(&id);
        } else if let Some(session) = sessions.get_mut(&id) {
            session.logged_in = false;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

fn rule_status(rule: &Rule, pools: &PoolMap) -> RuleStatus {
    let pool = pools
        .get(&(rule.listen, rule.protocol))
        .filter(|_| rule.enabled);
    let targets = match pool {
        Some(pool) => {
            let active = pool.pick().0;
            pool.targets()
                .iter()
                .enumerate()
                .map(|(i, addr)| TargetStatus {
                    addr: addr.to_string(),
                    healthy: pool.is_healthy(i),
                    active: i == active,
                })
                .collect()
        }
        None => std::iter::once(&rule.target)
            .chain(&rule.fallback)
            .map(|addr| TargetStatus {
                addr: addr.clone(),
                healthy: true,
                active: false,
            })
            .collect(),
    };
    RuleStatus {
        listen: rule.listen,
        protocol: rule.protocol,
        tag: rule.tag.clone(),
        enabled: rule.enabled,
        targets,
    }
}

async fn status(State(state): State<PanelState>) -> Response {
    let live = state.live.borrow().clone();
    let quotas = state.users.borrow().clone();
    let pools = state.pools.borrow().clone();
    let now_hour = (state.now_hour)();
    let users: Vec<UserStatus> = live
        .config
        .users
        .iter()
        .filter_map(|user| {
            let quota = quotas.get(&user.name)?;
            Some(UserStatus {
                usage: UserUsage::new(quota, now_hour),
                limit_bytes: quota.limit(),
                used_bytes: quota.used(),
                upload_bytes: quota.upload(),
                download_bytes: quota.download(),
                exhausted: quota.is_exhausted(),
                rules: user
                    .rules
                    .iter()
                    .map(|rule| rule_status(rule, &pools))
                    .collect(),
            })
        })
        .collect();
    let can_sign_out = state.credentials.has_token() || state.credentials.has_login();
    Json(json!({ "users": users, "can_sign_out": can_sign_out })).into_response()
}

async fn get_config(State(state): State<PanelState>) -> Response {
    let live = state.live.borrow().clone();
    Json(ConfigView {
        revision: live.revision,
        source: if live.from_panel { "panel" } else { "file" },
        total_quota: live.config.total_quota,
        users: &live.config.users,
    })
    .into_response()
}

fn failure(status: StatusCode, error: String) -> Response {
    (status, Json(json!({ "error": error }))).into_response()
}

async fn put_config(
    State(state): State<PanelState>,
    edit: Result<Json<ConfigEdit>, JsonRejection>,
) -> Response {
    let edit = match edit {
        Ok(Json(edit)) => edit,
        Err(rejection) => return failure(StatusCode::BAD_REQUEST, rejection.body_text()),
    };
    let (reply, result) = oneshot::channel();
    if state.saves.send(SaveRequest { edit, reply }).await.is_err() {
        return failure(StatusCode::SERVICE_UNAVAILABLE, "shutting down".into());
    }
    match result.await {
        Ok(Ok(revision)) => Json(json!({ "revision": revision })).into_response(),
        Ok(Err(SaveError::Conflict(e))) => failure(StatusCode::CONFLICT, e),
        Ok(Err(SaveError::Invalid(e))) => failure(StatusCode::BAD_REQUEST, e),
        Ok(Err(SaveError::Failed(e))) => failure(StatusCode::INTERNAL_SERVER_ERROR, e),
        Err(_) => failure(StatusCode::SERVICE_UNAVAILABLE, "shutting down".into()),
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use tower::ServiceExt as _;

    use super::*;
    use crate::pool::TargetPool;
    use crate::quota::{Direction, SavedUsage, UserQuota};

    const H: i64 = 500_000;

    #[tokio::test]
    async fn store_round_trips_and_tolerates_damage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("panel.json");
        assert!(load_store(&path).is_none());
        remove_store(&path);

        let users: Vec<UserConfig> = serde_json::from_value(json!([
            {"name": "a", "quota": "30GB", "rules": [{"listen": "0.0.0.0:1", "target": "x:1", "tag": "web"}]}
        ]))
        .unwrap();
        let store = Store {
            base: "// file text\n{}".into(),
            total_quota: Some(ByteSize(1 << 30)),
            users,
        };
        write_store(&path, &store).await.unwrap();
        write_store(&path, &store).await.unwrap();

        let loaded = load_store(&path).unwrap();
        assert_eq!(loaded.base, store.base);
        assert_eq!(loaded.total_quota, store.total_quota);
        assert_eq!(loaded.users[0].quota, Some(ByteSize(30 << 30)));
        assert_eq!(loaded.users[0].rules[0].tag.as_deref(), Some("web"));
        let names: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names, ["panel.json"]);

        std::fs::write(&path, "{ not json").unwrap();
        assert!(load_store(&path).is_none());
        remove_store(&path);
        assert!(!path.exists());
    }

    const CONFIG: &str = r#"{
        "api": {"listen": "127.0.0.1:7070", "token": "apisecret"},
        "panel": {"enabled": true, "token": "gate", "username": "admin", "password": "hunter2"},
        "total_quota": "1GB",
        "users": [
            {"name": "alice", "quota": 1000, "rules": [
                {"listen": "127.0.0.1:1", "target": "p:1", "fallback": "f:1", "protocol": "tcp", "tag": "web"},
                {"listen": "127.0.0.1:2", "target": "q:1", "enabled": false}
            ]}
        ]
    }"#;

    const AUTH: &str = r#""token": "gate", "username": "admin", "password": "hunter2""#;

    fn test_app() -> (Router, mpsc::Receiver<SaveRequest>) {
        test_app_with(AUTH)
    }

    fn test_app_with(auth: &str) -> (Router, mpsc::Receiver<SaveRequest>) {
        let config =
            crate::config::parse(&CONFIG.replace(AUTH, auth), Path::new("config.json")).unwrap();
        let alice = Arc::new(UserQuota::new_at(
            "alice".into(),
            Some(1000),
            SavedUsage::default(),
            H,
        ));
        alice.try_consume_at(100, Direction::Upload, H);
        alice.try_consume_at(200, Direction::Download, H);
        let pool = TargetPool::new("alice".into(), vec!["p:1".into(), "f:1".into()], None);
        pool.mark_down(0);
        let mut pools = PoolMap::new();
        pools.insert(("127.0.0.1:1".parse().unwrap(), Protocol::Tcp), pool);
        let credentials = config.panel().unwrap().clone();
        let (saves, save_rx) = mpsc::channel(1);
        let app = router_with_clock(
            &credentials,
            watch::Sender::new(Arc::new(Live {
                config,
                revision: 7,
                from_panel: true,
            }))
            .subscribe(),
            watch::Sender::new(Arc::new(UserSet::new(vec![alice]))).subscribe(),
            watch::Sender::new(Arc::new(pools)).subscribe(),
            saves,
            || H,
        );
        (app, save_rx)
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        cookie: Option<&str>,
        body: Option<serde_json::Value>,
    ) -> Response {
        let mut request = axum::http::Request::builder().method(method).uri(uri);
        if let Some(cookie) = cookie {
            request = request.header(header::COOKIE, cookie);
        }
        let body = match body {
            Some(body) => {
                request = request.header(header::CONTENT_TYPE, "application/json");
                Body::from(body.to_string())
            }
            None => Body::empty(),
        };
        app.clone()
            .oneshot(request.body(body).unwrap())
            .await
            .unwrap()
    }

    fn cookie_of(response: &Response) -> String {
        let cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
        assert!(cookie.contains("HttpOnly") && cookie.contains("SameSite=Lax"));
        cookie.split(';').next().unwrap().to_owned()
    }

    async fn body_text(response: Response) -> String {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    async fn body_json(response: Response) -> serde_json::Value {
        serde_json::from_str(&body_text(response).await).unwrap()
    }

    async fn enter_gate(app: &Router) -> String {
        let response = send(app, "GET", "/?token=gate", None, None).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(response.headers()[header::LOCATION], DASHBOARD);
        cookie_of(&response)
    }

    async fn log_in(app: &Router) -> String {
        let gate = enter_gate(app).await;
        let response = send(
            app,
            "POST",
            LOGIN,
            Some(&gate),
            Some(json!({"username": "admin", "password": "hunter2"})),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let session = cookie_of(&response);
        assert_ne!(session, gate);
        let stale = send(app, "GET", DASHBOARD, Some(&gate), None).await;
        assert_eq!(stale.status(), StatusCode::NOT_FOUND);
        session
    }

    #[tokio::test]
    async fn everything_is_hidden_without_the_token() {
        let (app, _saves) = test_app();
        let login = json!({"username": "admin", "password": "hunter2"});
        for (method, uri, body) in [
            ("GET", "/", None),
            ("GET", "/?token=wrong", None),
            ("GET", DASHBOARD, None),
            ("GET", "/dashboard?token=", None),
            ("GET", "/dashboard/api/status", None),
            ("GET", "/dashboard/api/config", None),
            ("PUT", "/dashboard/api/config", Some(json!({}))),
            ("POST", LOGIN, Some(login)),
            ("POST", LOGOUT, None),
        ] {
            let stale = Some("ports_box_session=nope");
            for cookie in [None, stale] {
                let response = send(&app, method, uri, cookie, body.clone()).await;
                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{method} {uri}");
                assert!(!response.headers().contains_key(header::SET_COOKIE));
            }
        }
    }

    #[tokio::test]
    async fn token_opens_the_dashboard_but_not_the_data() {
        let (app, _saves) = test_app();
        let gate = enter_gate(&app).await;

        let response = send(&app, "GET", DASHBOARD, Some(&gate), None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_text(response).await.contains("<html"));

        for uri in ["/", "/dashboard?token=gate"] {
            let response = send(&app, "GET", uri, Some(&gate), None).await;
            assert_eq!(response.status(), StatusCode::FOUND, "{uri}");
            assert!(!response.headers().contains_key(header::SET_COOKIE));
        }
        for (method, uri) in [
            ("GET", "/dashboard/api/status"),
            ("GET", "/dashboard/api/config"),
            ("PUT", "/dashboard/api/config"),
        ] {
            let response = send(&app, method, uri, Some(&gate), None).await;
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {uri}"
            );
        }

        let response = send(&app, "GET", "/dashboard?token=gate", None, None).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_ne!(cookie_of(&response), gate);
    }

    #[tokio::test(start_paused = true)]
    async fn login_requires_both_credentials() {
        let (app, _saves) = test_app();
        let gate = enter_gate(&app).await;
        for (username, password) in [("admin", "wrong"), ("root", "hunter2"), ("", "")] {
            let body = json!({"username": username, "password": password});
            let response = send(&app, "POST", LOGIN, Some(&gate), Some(body)).await;
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
            assert!(!response.headers().contains_key(header::SET_COOKIE));
        }
        let response = send(&app, "GET", "/dashboard/api/status", Some(&gate), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn logout_returns_to_the_login_form() {
        let (app, _saves) = test_app();
        let session = log_in(&app).await;
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = send(&app, "POST", LOGOUT, Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = send(&app, "GET", DASHBOARD, Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn token_alone_signs_in() {
        let (app, _saves) = test_app_with(r#""token": "gate""#);
        for uri in ["/", DASHBOARD, "/dashboard/api/status"] {
            let response = send(&app, "GET", uri, None, None).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        }
        let session = enter_gate(&app).await;
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let login = json!({"username": "", "password": ""});
        let response = send(&app, "POST", LOGIN, Some(&session), Some(login)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let response = send(&app, "POST", LOGOUT, Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn no_token_or_login_leaves_the_panel_open() {
        let (app, _saves) = test_app_with("");
        let response = send(&app, "GET", "/", None, None).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        let response = send(&app, "GET", DASHBOARD, None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = send(&app, "GET", "/dashboard/api/status", None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await["can_sign_out"], false);
        let response = send(&app, "GET", "/dashboard/api/config", None, None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let login = json!({"username": "", "password": ""});
        let response = send(&app, "POST", LOGIN, None, Some(login)).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(start_paused = true)]
    async fn credentials_alone_need_no_token() {
        let (app, _saves) = test_app_with(r#""username": "admin", "password": "hunter2""#);
        let response = send(&app, "GET", "/", None, None).await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(!response.headers().contains_key(header::SET_COOKIE));
        let response = send(&app, "GET", DASHBOARD, None, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = send(&app, "GET", "/dashboard/api/status", None, None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let response = send(&app, "GET", "/?token=", None, None).await;
        assert!(!response.headers().contains_key(header::SET_COOKIE));

        let wrong = json!({"username": "admin", "password": "wrong"});
        let response = send(&app, "POST", LOGIN, None, Some(wrong)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let right = json!({"username": "admin", "password": "hunter2"});
        let response = send(&app, "POST", LOGIN, None, Some(right)).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let session = cookie_of(&response);
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = send(&app, "POST", LOGOUT, Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test(start_paused = true)]
    async fn sessions_expire() {
        let (app, _saves) = test_app();
        let session = log_in(&app).await;
        tokio::time::advance(SESSION_TTL - Duration::from_secs(1)).await;
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(response.status(), StatusCode::OK);

        tokio::time::advance(Duration::from_secs(1)).await;

        for uri in ["/", DASHBOARD, "/dashboard/api/status"] {
            let response = send(&app, "GET", uri, Some(&session), None).await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        }
    }

    #[tokio::test]
    async fn status_reports_usage_rules_and_target_health() {
        let (app, _saves) = test_app();
        let session = log_in(&app).await;
        let response = send(&app, "GET", "/dashboard/api/status", Some(&session), None).await;
        assert_eq!(
            body_json(response).await,
            json!({"can_sign_out": true, "users": [{
                "name": "alice",
                "total": "1000B",
                "used": "200B",
                "hour_used": "200B",
                "remaining": "800B",
                "limit_bytes": 1000,
                "used_bytes": 200,
                "upload_bytes": 100,
                "download_bytes": 200,
                "exhausted": false,
                "rules": [
                    {"listen": "127.0.0.1:1", "protocol": "tcp", "tag": "web", "enabled": true, "targets": [
                        {"addr": "p:1", "healthy": false, "active": false},
                        {"addr": "f:1", "healthy": true, "active": true}
                    ]},
                    {"listen": "127.0.0.1:2", "protocol": "both", "tag": null, "enabled": false, "targets": [
                        {"addr": "q:1", "healthy": true, "active": false}
                    ]}
                ]
            }]})
        );
    }

    #[tokio::test]
    async fn config_view_omits_secrets() {
        let (app, _saves) = test_app();
        let session = log_in(&app).await;
        let response = send(&app, "GET", "/dashboard/api/config", Some(&session), None).await;
        let text = body_text(response).await;
        for secret in ["apisecret", "gate", "hunter2", "admin"] {
            assert!(!text.contains(secret), "{text}");
        }
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["revision"], 7);
        assert_eq!(json["source"], "panel");
        assert_eq!(json["total_quota"], "1GB");
        assert_eq!(json["users"][0]["quota"], 1000);
        assert_eq!(json["users"][0]["rules"][0]["fallback"], json!(["f:1"]));
    }

    #[tokio::test]
    async fn save_results_map_to_statuses() {
        let (app, mut saves) = test_app();
        let session = log_in(&app).await;
        tokio::spawn(async move {
            while let Some(request) = saves.recv().await {
                let result = match request.edit.revision {
                    7 => {
                        assert_eq!(request.edit.total_quota, Some(ByteSize(2 << 30)));
                        assert_eq!(request.edit.users[0].rules[0].target, "x:1");
                        Ok(8)
                    }
                    1 => Err(SaveError::Conflict("stale".into())),
                    2 => Err(SaveError::Invalid("duplicate".into())),
                    _ => Err(SaveError::Failed("disk".into())),
                };
                let _ = request.reply.send(result);
            }
        });
        let edit = |revision: u64| {
            json!({
                "revision": revision,
                "total_quota": "2GB",
                "users": [{"name": "a", "quota": null, "rules": [{"listen": "0.0.0.0:1", "target": "x:1"}]}]
            })
        };
        let uri = "/dashboard/api/config";

        let response = send(&app, "PUT", uri, Some(&session), Some(edit(7))).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_json(response).await, json!({"revision": 8}));

        for (revision, status, error) in [
            (1, StatusCode::CONFLICT, "stale"),
            (2, StatusCode::BAD_REQUEST, "duplicate"),
            (3, StatusCode::INTERNAL_SERVER_ERROR, "disk"),
        ] {
            let response = send(&app, "PUT", uri, Some(&session), Some(edit(revision))).await;
            assert_eq!(response.status(), status);
            assert_eq!(body_json(response).await, json!({"error": error}));
        }

        let bad = json!({"revision": 7, "users": [{"name": "a", "rules": [{"listen": "nope"}]}]});
        let response = send(&app, "PUT", uri, Some(&session), Some(bad)).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(body_json(response).await["error"].is_string());
    }
}
