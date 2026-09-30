use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::config::format_size;
use crate::quota::{UserQuota, clock};

#[derive(Clone)]
pub struct ApiState {
    users: Arc<HashMap<String, Arc<UserQuota>>>,
    /// Kept in insertion (config) order for stable /api/users output.
    ordered: Arc<Vec<Arc<UserQuota>>>,
    token: Option<Arc<str>>,
    now_hour: fn() -> i64,
}

#[derive(Serialize)]
struct UserUsage {
    name: String,
    total: String,
    used: String,
    hour_used: String,
    remaining: String,
}

impl UserUsage {
    fn new(q: &UserQuota, now_hour: i64) -> Self {
        q.roll_over_at(now_hour);
        Self {
            name: q.name.clone(),
            total: q.limit.map_or_else(|| "unlimited".into(), format_size),
            used: format_size(q.used()),
            hour_used: format_size(q.hour_used()),
            remaining: q
                .remaining()
                .map_or_else(|| "unlimited".into(), format_size),
        }
    }
}

fn billed_split(q: &UserQuota) -> (u64, u64) {
    let (up, down) = (q.upload(), q.download());
    let total = up as u128 + down as u128;
    if total == 0 {
        return (0, 0);
    }
    let used = (q.used() as u128).min(total);
    let billed_up = (used * up as u128 / total) as u64;
    (billed_up, used as u64 - billed_up)
}

fn token_matches(given: &str, expected: &str) -> bool {
    let (given, expected) = (given.as_bytes(), expected.as_bytes());
    given.len() == expected.len()
        && std::hint::black_box(
            given
                .iter()
                .zip(expected)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b)),
        ) == 0
}

pub fn router(users: Arc<Vec<Arc<UserQuota>>>, token: Option<String>) -> Router {
    router_with_clock(users, token, clock::current_hour_id)
}

fn router_with_clock(
    users: Arc<Vec<Arc<UserQuota>>>,
    token: Option<String>,
    now_hour: fn() -> i64,
) -> Router {
    let state = ApiState {
        users: Arc::new(users.iter().map(|u| (u.name.clone(), u.clone())).collect()),
        ordered: users,
        token: token.map(Into::into),
        now_hour,
    };
    Router::new()
        .route("/api/users", get(list_users))
        .route("/api/users/{name}", get(get_user))
        .route("/sub/{name}", get(sub_store))
        .with_state(state)
}

/// Accepts the token as `Authorization: Bearer <token>` or `?token=<token>`
/// (the latter lets Sub-Store use a plain URL).
fn authorize(state: &ApiState, headers: &HeaderMap, query: &HashMap<String, String>) -> bool {
    let Some(expected) = &state.token else {
        return true;
    };
    if let Some(auth) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        && auth
            .strip_prefix("Bearer ")
            .is_some_and(|given| token_matches(given, expected))
    {
        return true;
    }
    query
        .get("token")
        .is_some_and(|given| token_matches(given, expected))
}

async fn list_users(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !authorize(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let now_hour = (state.now_hour)();
    let usage: Vec<UserUsage> = state
        .ordered
        .iter()
        .map(|u| UserUsage::new(u, now_hour))
        .collect();
    Json(usage).into_response()
}

async fn get_user(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !authorize(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.users.get(&name) {
        Some(user) => Json(UserUsage::new(user, (state.now_hour)())).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Sub-Store compatible endpoint: it reads traffic info from the
/// `subscription-userinfo` response header of a subscription URL.
async fn sub_store(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !authorize(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(user) = state.users.get(&name) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    user.roll_over_at((state.now_hour)());
    let (upload, download) = billed_split(user);
    // Unlimited users omit `total=`, which Sub-Store reads as no cap.
    let mut userinfo = format!("upload={upload}; download={download}");
    if let Some(limit) = user.limit {
        userinfo.push_str(&format!("; total={limit}"));
    }
    (
        [
            ("subscription-userinfo", userinfo),
            ("content-type", "text/plain; charset=utf-8".to_string()),
        ],
        "",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quota::{Direction, SavedUsage};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    /// Fixed hour id so tests never straddle a real boundary.
    const H: i64 = 500_000;

    fn test_router(token: Option<String>) -> Router {
        router_with_clock(test_users(), token, || H)
    }

    fn test_users() -> Arc<Vec<Arc<UserQuota>>> {
        let alice = Arc::new(UserQuota::new_at(
            "alice".into(),
            Some(1000),
            SavedUsage::default(),
            H,
        ));
        alice.try_consume_at(100, Direction::Upload, H);
        alice.try_consume_at(200, Direction::Download, H);
        let bob = Arc::new(UserQuota::new_at(
            "bob".into(),
            None,
            SavedUsage::default(),
            H,
        ));
        bob.try_consume_at(50, Direction::Upload, H);
        Arc::new(vec![alice, bob])
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn list_and_get_report_usage() {
        let app = test_router(None);
        let response = app
            .clone()
            .oneshot(Request::get("/api/users").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let json = body_json(response).await;
        // used = max(upload, download) within the single billing hour
        assert_eq!(
            json,
            serde_json::json!([
                {"name": "alice", "total": "1000B", "used": "200B", "hour_used": "200B", "remaining": "800B"},
                {"name": "bob", "total": "unlimited", "used": "50B", "hour_used": "50B", "remaining": "unlimited"}
            ])
        );

        let response = app
            .oneshot(
                Request::get("/api/users/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn sub_store_header() {
        let app = test_router(None);
        let response = app
            .oneshot(Request::get("/sub/alice").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["subscription-userinfo"],
            "upload=66; download=134; total=1000"
        );

        // Unlimited users omit total entirely.
        let app = test_router(None);
        let response = app
            .oneshot(Request::get("/sub/bob").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.headers()["subscription-userinfo"],
            "upload=50; download=0"
        );
    }

    #[test]
    fn billed_split_sums_to_used() {
        let quota = UserQuota::new_at("a".into(), None, SavedUsage::default(), H);
        assert_eq!(billed_split(&quota), (0, 0));
        quota.try_consume_at(300, Direction::Upload, H);
        quota.try_consume_at(100, Direction::Download, H);
        quota.try_consume_at(50, Direction::Download, H + 1);
        assert_eq!(quota.used(), 350);
        let (up, down) = billed_split(&quota);
        assert_eq!(up + down, 350);
        assert_eq!((up, down), (233, 117));
    }

    #[tokio::test]
    async fn stale_hour_rolls_over_on_read() {
        let app = router_with_clock(test_users(), None, || H + 1);
        let response = app
            .oneshot(
                Request::get("/api/users/alice")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = body_json(response).await;
        assert_eq!(json["used"], "200B");
        assert_eq!(json["hour_used"], "0B");
    }

    #[test]
    fn token_comparison() {
        assert!(token_matches("secret", "secret"));
        assert!(!token_matches("secreT", "secret"));
        assert!(!token_matches("secret1", "secret"));
        assert!(!token_matches("", "secret"));
    }

    #[tokio::test]
    async fn token_via_bearer_or_query() {
        let app = test_router(Some("secret".into()));
        for (uri, auth, expected) in [
            ("/api/users", None, StatusCode::UNAUTHORIZED),
            ("/api/users?token=wrong", None, StatusCode::UNAUTHORIZED),
            ("/api/users?token=secret", None, StatusCode::OK),
            ("/sub/alice?token=secret", None, StatusCode::OK),
            ("/api/users", Some("Bearer secret"), StatusCode::OK),
            ("/api/users", Some("Bearer nope"), StatusCode::UNAUTHORIZED),
        ] {
            let mut request = Request::get(uri);
            if let Some(auth) = auth {
                request = request.header(header::AUTHORIZATION, auth);
            }
            let response = app
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected, "{uri} {auth:?}");
        }
    }
}
