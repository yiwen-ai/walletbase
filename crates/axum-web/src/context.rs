use axum::{
    http::{header, HeaderMap, Request},
    middleware::Next,
    response::Response,
};
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

pub use structured_logger::unix_ms;

pub struct ReqContext {
    pub rid: String,   // from x-request-id header
    pub user: xid::Id, // from x-auth-user header
    pub rating: i8,    // from x-auth-user-rating header, 0 if not present
    pub unix_ms: u64,
    pub start: Instant,
    pub kv: RwLock<BTreeMap<String, Value>>,
}

impl ReqContext {
    pub fn new(rid: &str, user: xid::Id, rating: i8) -> Self {
        Self {
            rid: rid.to_string(),
            user,
            rating,
            unix_ms: unix_ms(),
            start: Instant::now(),
            kv: RwLock::new(BTreeMap::new()),
        }
    }

    pub async fn set(&self, key: &str, value: Value) {
        let mut kv = self.kv.write().await;
        kv.insert(key.to_string(), value);
    }

    pub async fn set_kvs(&self, kvs: Vec<(&str, Value)>) {
        let mut kv = self.kv.write().await;
        for item in kvs {
            kv.insert(item.0.to_string(), item.1);
        }
    }
}

pub async fn middleware<B>(mut req: Request<B>, next: Next<B>) -> Response {
    let method = req.method().to_string();
    let uri = req.uri().to_string();
    let rid = extract_header(req.headers(), "x-request-id", || Uuid::new_v4().to_string());
    let user = extract_header(req.headers(), "x-auth-user", || "".to_string());
    let app = extract_header(req.headers(), "x-auth-app", || "".to_string());
    let rating = extract_header(req.headers(), "x-auth-user-rating", || "0".to_string());
    let rating = i8::from_str(&rating).unwrap_or(0);

    let uid = xid::Id::from_str(&user).unwrap_or_default();

    let ctx = Arc::new(ReqContext::new(&rid, uid, rating));
    req.extensions_mut().insert(ctx.clone());

    let res = next.run(req).await;
    let kv = ctx.kv.read().await;
    let status = res.status().as_u16();
    let headers = res.headers();
    let ct = headers
        .get(header::CONTENT_TYPE)
        .map_or("", |v| v.to_str().unwrap_or_default());
    let ce = headers
        .get(header::CONTENT_ENCODING)
        .map_or("", |v| v.to_str().unwrap_or_default());
    log::info!(target: "api",
        method = method,
        uri = uri,
        rid = rid,
        user = user,
        app = app,
        rating = rating,
        status = status,
        start = ctx.unix_ms,
        elapsed = ctx.start.elapsed().as_millis() as u64,
        ctype = ct,
        encoding = ce,
        kv = log::as_serde!(*kv);
        "",
    );

    res
}

pub fn extract_header(hm: &HeaderMap, key: &str, or: impl FnOnce() -> String) -> String {
    match hm.get(key) {
        None => or(),
        Some(v) => match v.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => or(),
        },
    }
}
