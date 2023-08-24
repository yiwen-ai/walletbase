use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc, vec};
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::{cbor_to_vec, PackObject};
use scylla_orm::ColumnsMap;

use crate::api::{
    currency::Currency, get_fields, token_from_xid, token_to_xid, AppState, Pagination, QueryUidId,
    TransactionPayload,
};
use crate::db;

#[derive(Debug, Deserialize, Validate)]
pub struct ChargeInput {
    pub uid: PackObject<xid::Id>,
    #[validate(range(min = 0, max = 1))]
    pub status: i8,
    pub currency: String,
    #[validate(range(min = 10))]
    pub amount: i64,
    #[validate(range(min = 10))]
    pub quantity: i64,
    pub provider: String, // stripe
    pub charge_id: Option<String>,
    pub charge_payload: Option<PackObject<Vec<u8>>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ChargeOutput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub status: i8,
    pub updated_at: i64,
    pub expire_at: i64,
    pub quantity: i64,
    pub currency: String,
    pub amount: i64,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_refunded: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charge_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub charge_payload: Option<PackObject<Vec<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_refunded: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_msg: Option<String>,
}

impl ChargeOutput {
    pub fn from<T>(val: db::Charge, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            uid: to.with(val.uid),
            id: to.with(val.id),
            status: val.status,
            updated_at: val.updated_at,
            expire_at: val.expire_at,
            quantity: val.quantity,
            currency: val.currency,
            amount: val.amount,
            provider: val.provider,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "amount_refunded" => rt.amount_refunded = Some(val.amount_refunded),
                "charge_id" => rt.charge_id = Some(val.charge_id.to_owned()),
                "charge_payload" => {
                    rt.charge_payload = Some(to.with(val.charge_payload.to_owned()))
                }
                "txn" => rt.txn = to.with_option(val.txn),
                "txn_refunded" => rt.txn_refunded = to.with_option(val.txn_refunded),
                "failure_code" => rt.failure_code = Some(val.failure_code.to_owned()),
                "failure_msg" => rt.failure_msg = Some(val.failure_msg.to_owned()),
                _ => {}
            }
        }

        rt
    }
}

pub async fn create(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<ChargeInput>,
) -> Result<PackObject<SuccessResponse<ChargeOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;
    let cur = Currency::from_str(&input.currency)?;
    cur.valid_amount(input.amount)?;

    let uid = input.uid.unwrap();
    ctx.set_kvs(vec![
        ("action", "create_charge".into()),
        ("uid", uid.to_string().into()),
        ("provider", input.provider.clone().into()),
        ("currency", input.currency.clone().into()),
        ("amount", input.amount.into()),
        ("quantity", input.quantity.into()),
    ])
    .await;

    let mut doc = db::Charge {
        uid,
        status: input.status,
        quantity: input.quantity,
        currency: input.currency.to_lowercase(),
        amount: input.amount,
        provider: input.provider,
        ..Default::default()
    };

    if let Some(charge_id) = input.charge_id {
        ctx.set("charge_id", charge_id.clone().into()).await;
        doc.charge_id = charge_id;
    }
    if let Some(charge_payload) = input.charge_payload {
        doc.charge_payload = charge_payload.unwrap();
    }

    doc.save(&app.scylla).await?;
    // todo

    Ok(to.with(SuccessResponse::new(ChargeOutput::from(doc, &to))))
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryUidId>,
) -> Result<PackObject<SuccessResponse<ChargeOutput>>, HTTPError> {
    input.validate()?;
    let uid = *input.uid.to_owned();
    let id = *input.id.to_owned();

    ctx.set_kvs(vec![
        ("action", "get_charge".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Charge::with_pk(uid, id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(ChargeOutput::from(doc, &to))))
}

pub async fn list(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<ChargeOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_charge".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Charge::list(
        &app.scylla,
        input.uid.unwrap(),
        fields,
        page_size,
        token_to_xid(&input.page_token),
        input.status,
    )
    .await?;
    let next_page_token = if res.len() >= page_size as usize {
        to.with_option(token_from_xid(res.last().unwrap().id))
    } else {
        None
    };

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: res
            .iter()
            .map(|r| ChargeOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct UpdateChargeInput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    #[validate(range(min = 0, max = 2))]
    pub current_status: i8,
    #[validate(range(min = -2, max = 1))]
    pub status: Option<i8>,
    pub charge_id: Option<String>,
    pub charge_payload: Option<PackObject<Vec<u8>>>,
    pub failure_code: Option<String>,
    pub failure_msg: Option<String>,
}

impl UpdateChargeInput {
    fn into(self) -> anyhow::Result<ColumnsMap> {
        let mut cols = ColumnsMap::new();
        if let Some(status) = self.status {
            if status == -1 || status > 1 {
                return Err(HTTPError::new(400, format!("Invalid status: {}", status)).into());
            }
            cols.set_as("status", &status);
        }
        if let Some(charge_id) = self.charge_id {
            cols.set_as("charge_id", &charge_id);
        }
        if let Some(charge_payload) = self.charge_payload {
            cols.set_as("charge_payload", &charge_payload.unwrap());
        }
        if let Some(failure_code) = self.failure_code {
            cols.set_as("failure_code", &failure_code);
        }
        if let Some(failure_msg) = self.failure_msg {
            cols.set_as("failure_msg", &failure_msg);
        }

        if cols.is_empty() {
            return Err(HTTPError::new(400, "No fields to update".to_string()).into());
        }

        Ok(cols)
    }
}

pub async fn update(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateChargeInput>,
) -> Result<PackObject<SuccessResponse<ChargeOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = *input.uid.to_owned();
    let id = *input.id.to_owned();
    let status = input.current_status;
    let cols = input.into()?;
    ctx.set_kvs(vec![
        ("action", "update_charge".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Charge::with_pk(uid, id);
    doc.update(&app.scylla, cols, status).await?;
    Ok(to.with(SuccessResponse::new(ChargeOutput::from(doc, &to))))
}

pub async fn complete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<UpdateChargeInput>,
) -> Result<PackObject<SuccessResponse<bool>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    let id = input.id.unwrap();
    ctx.set_kvs(vec![
        ("action", "complete_charge".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Charge::with_pk(uid, id);
    let ok = doc.set_status(&app.scylla, 1, 2).await?;
    if !ok {
        if doc.status >= 2 {
            return Ok(to.with(SuccessResponse::new(false)));
        }

        return Err(HTTPError::new(
            500,
            format!("Invalid status {} for completing charge", doc.status),
        ));
    }
    doc.get_one(&app.scylla, vec!["quantity".to_string()])
        .await?;
    let mut txn: db::Transaction = Default::default();
    txn.description = "complete_charge".to_string();
    let payload = TransactionPayload {
        kind: "charge".to_string(),
        id: PackObject::Cbor(doc.id),
    };
    txn.payload = cbor_to_vec(&payload).unwrap_or_default();

    txn.prepare(
        &app.scylla,
        &app.mac,
        uid,
        db::TransactionKind::Topup,
        doc.quantity,
    )
    .await?;
    txn.commit(&app.scylla, &app.mac).await?;

    let mut cols = ColumnsMap::with_capacity(2);
    cols.set_as("status", &3i8);
    cols.set_as("txn", &txn.id);
    doc.update(&app.scylla, cols, 2i8).await?;

    Ok(to.with(SuccessResponse::new(true)))
}
