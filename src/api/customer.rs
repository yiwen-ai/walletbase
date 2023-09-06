use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, vec};
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::api::{get_fields, validate_provider, AppState};
use crate::db;

#[derive(Debug, Deserialize, Validate)]
pub struct CustomerInput {
    pub uid: PackObject<xid::Id>,
    #[validate(length(min = 1), custom = "validate_provider")]
    pub provider: String, // stripe
    pub customer: String,
    pub payload: PackObject<Vec<u8>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CustomerOutput {
    pub uid: PackObject<xid::Id>,
    pub provider: String,
    pub customer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<PackObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customers: Option<Vec<String>>,
}

impl CustomerOutput {
    pub fn from<T>(val: db::Customer, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            uid: to.with(val.uid),
            provider: val.provider,
            customer: val.customer,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "created_at" => rt.created_at = Some(val.created_at),
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "payload" => rt.payload = Some(to.with(val.payload.to_owned())),
                "customers" => rt.customers = Some(val.customers.iter().cloned().collect()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn upsert(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CustomerInput>,
) -> Result<PackObject<SuccessResponse<CustomerOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    ctx.set_kvs(vec![
        ("action", "upsert_customer".into()),
        ("uid", uid.to_string().into()),
        ("provider", input.provider.to_string().into()),
        ("customer", input.customer.clone().into()),
    ])
    .await;

    let mut doc = db::Customer::with_pk(uid, input.provider);

    doc.upsert(&app.scylla, input.customer, input.payload.unwrap())
        .await?;

    Ok(to.with(SuccessResponse::new(CustomerOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct QueryCustomer {
    pub uid: PackObject<xid::Id>,
    pub provider: String,
    pub fields: Option<String>,
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryCustomer>,
) -> Result<PackObject<SuccessResponse<CustomerOutput>>, HTTPError> {
    input.validate()?;
    let uid = *input.uid.to_owned();
    let provider = input.provider.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_customer".into()),
        ("uid", uid.to_string().into()),
        ("provider", provider.clone().into()),
    ])
    .await;

    let mut doc = db::Customer::with_pk(uid, provider);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(CustomerOutput::from(doc, &to))))
}
