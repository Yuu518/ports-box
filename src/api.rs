use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::config::format_size;
use crate::quota::UserQuota;

#[derive(Clone)]
pub struct ApiState {
    users: Arc<HashMap<String, Arc<UserQuota>>>,
    /// Kept in insertion (config) order for stable /api/users output.
    ordered: Arc<Vec<Arc<UserQuota>>>,
    token: Option<Arc<str>>,
}

#[derive(Serialize)]
struct UserUsage {
    name: String,
    total: String,
    used: String,
    hour_used: String,
    remaining: String,
}

impl From<&UserQuota> for UserUsage {
    fn from(q: &UserQuota) -> Self {
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

pub fn router(users: Arc<Vec<Arc<UserQuota>>>, token: Option<String>) -> Router {
    let state = ApiState {
        users: Arc::new(users.iter().map(|u| (u.name.clone(), u.clone())).collect()),
        ordered: users,
        token: token.map(Into::into),
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
        && auth.strip_prefix("Bearer ") == Some(expected)
    {
        return true;
    }
    query.get("token").map(String::as_str) == Some(expected)
}

async fn list_users(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !authorize(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let usage: Vec<UserUsage> = state.ordered.iter().map(|u| u.as_ref().into()).collect();
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
        Some(user) => Json(UserUsage::from(user.as_ref())).into_response(),
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
    // Unlimited users omit `total=`, which Sub-Store reads as no cap.
    let mut userinfo = format!("upload={}; download={}", user.upload(), user.download());
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
        router(Arc::new(vec![alice, bob]), token)
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
            "upload=100; download=200; total=1000"
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
