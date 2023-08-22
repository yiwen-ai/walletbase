use axum::extract::State;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::object::{cbor_from_slice, cbor_to_vec, PackObject};

use crate::db::{self};

pub mod wallet;

pub const APP_NAME: &str = env!("CARGO_PKG_NAME");
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub struct AppState {
    pub scylla: Arc<db::scylladb::ScyllaDB>,
}

#[derive(Serialize, Deserialize)]
pub struct AppVersion {
    pub name: String,
    pub version: String,
}

#[derive(Serialize, Deserialize)]
pub struct AppInfo {
    // https://docs.rs/scylla/latest/scylla/struct.Metrics.html
    pub scylla_latency_avg_ms: u64,
    pub scylla_latency_p99_ms: u64,
    pub scylla_latency_p90_ms: u64,
    pub scylla_errors_num: u64,
    pub scylla_queries_num: u64,
    pub scylla_errors_iter_num: u64,
    pub scylla_queries_iter_num: u64,
    pub scylla_retries_num: u64,
}

pub async fn version(to: PackObject<()>, State(_): State<Arc<AppState>>) -> PackObject<AppVersion> {
    to.with(AppVersion {
        name: APP_NAME.to_string(),
        version: APP_VERSION.to_string(),
    })
}

pub async fn healthz(to: PackObject<()>, State(app): State<Arc<AppState>>) -> PackObject<AppInfo> {
    let m = app.scylla.metrics();
    to.with(AppInfo {
        scylla_latency_avg_ms: m.get_latency_avg_ms().unwrap_or(0),
        scylla_latency_p99_ms: m.get_latency_percentile_ms(99.0f64).unwrap_or(0),
        scylla_latency_p90_ms: m.get_latency_percentile_ms(90.0f64).unwrap_or(0),
        scylla_errors_num: m.get_errors_num(),
        scylla_queries_num: m.get_queries_num(),
        scylla_errors_iter_num: m.get_errors_iter_num(),
        scylla_queries_iter_num: m.get_queries_iter_num(),
        scylla_retries_num: m.get_retries_num(),
    })
}

pub fn get_fields(fields: Option<String>) -> Vec<String> {
    if fields.is_none() {
        return vec![];
    }
    let fields = fields.unwrap();
    let fields = fields.trim();
    if fields.is_empty() {
        return vec![];
    }
    fields.split(',').map(|s| s.trim().to_string()).collect()
}

#[derive(Debug, Deserialize, Validate)]
pub struct Pagination {
    pub uid: PackObject<xid::Id>,
    pub page_token: Option<PackObject<Vec<u8>>>,
    #[validate(range(min = 2, max = 1000))]
    pub page_size: Option<u16>,
    #[validate(range(min = -1, max = 2))]
    pub status: Option<i8>,
    pub fields: Option<Vec<String>>,
}

pub fn token_to_xid(page_token: &Option<PackObject<Vec<u8>>>) -> Option<xid::Id> {
    match page_token.as_ref().map(|v| v.unwrap_ref()) {
        Some(v) => cbor_from_slice::<PackObject<xid::Id>>(v)
            .ok()
            .map(|v| v.unwrap()),
        _ => None,
    }
}

pub fn token_from_xid(id: xid::Id) -> Option<Vec<u8>> {
    cbor_to_vec(&PackObject::Cbor(id)).ok()
}
