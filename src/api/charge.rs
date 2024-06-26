use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc, vec};
use validator::Validate;

use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::{cbor_to_vec, PackObject};
use axum_web::{
    context::{unix_ms, ReqContext},
    object::cbor_from_slice,
};
use scylla_orm::ColumnsMap;

use crate::api::{
    currency::Currency, get_fields, token_from_xid, token_to_xid, validate_provider, AppState,
    Pagination, QueryUidId, TransactionPayload,
};
use crate::db;

#[derive(Debug, Deserialize, Validate)]
pub struct ChargeInput {
    pub uid: PackObject<xid::Id>,
    #[validate(length(min = 1), custom = "validate_provider")]
    pub provider: String, // stripe
    #[validate(range(min = 50, max = 1_000_000))]
    pub quantity: i64,
    pub currency: Option<String>,
    #[validate(range(min = 1))]
    pub amount: Option<i64>,
    pub charge_id: Option<String>,
    pub charge_payload: Option<PackObject<Vec<u8>>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ChargeOutput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub status: i8,
    pub quantity: i64,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expire_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount: Option<i64>,
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
            quantity: val.quantity,
            provider: val.provider,
            ..Default::default()
        };

        for v in val._fields {
            match v.as_str() {
                "updated_at" => rt.updated_at = Some(val.updated_at),
                "expire_at" => rt.expire_at = Some(val.expire_at),
                "currency" => rt.currency = Some(val.currency.to_owned()),
                "amount" => rt.amount = Some(val.amount),
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
        quantity: input.quantity,
        provider: input.provider,
        ..Default::default()
    };

    if let Some(amount) = input.amount {
        let cur = Currency::from_str(
            &input
                .currency
                .ok_or(HTTPError::new(400, "currency required".to_string()))?,
        )?;
        doc.amount = amount;
        doc.currency = cur.alpha.to_lowercase();
    }

    if let Some(charge_id) = input.charge_id {
        ctx.set("charge_id", charge_id.clone().into()).await;
        doc.status = 1;
        doc.charge_id = charge_id;
        doc.charge_payload = input
            .charge_payload
            .ok_or(HTTPError::new(400, "invalid charge_payload".to_string()))?
            .unwrap();
    }

    doc.save(&app.scylla).await?;
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
    let now = unix_ms() as i64;
    if (doc.status == 0 || doc.status == 1) && doc.expire_at > 0 && doc.expire_at <= now {
        doc.status = -2;
        doc.failure_msg = "checkout.expired".to_string();
    }
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
    let mut res = db::Charge::list(
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

    let now = unix_ms() as i64;
    for doc in res.iter_mut() {
        if (doc.status == 0 || doc.status == 1) && doc.expire_at > 0 && doc.expire_at <= now {
            doc.status = -2;
            doc.failure_msg = "checkout.expired".to_string();
        }
    }

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
    #[validate(range(min = 0, max = 1))]
    pub current_status: i8,
    #[validate(range(min = -2, max = 1))]
    pub status: Option<i8>,
    pub currency: Option<String>,
    #[validate(range(min = 1))]
    pub amount: Option<i64>,
    #[validate(range(min = 1))]
    pub amount_refunded: Option<i64>,
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
        if let Some(currency) = self.currency {
            cols.set_as("currency", &currency);
        }
        if let Some(amount) = self.amount {
            cols.set_as("amount", &amount);
        }
        if let Some(amount_refunded) = self.amount_refunded {
            cols.set_as("amount_refunded", &amount_refunded);
        }
        if let Some(charge_id) = self.charge_id {
            cols.set_as("charge_id", &charge_id);
        }
        if let Some(charge_payload) = self.charge_payload {
            cols.set_as("charge_payload", &charge_payload.unwrap());
        }
        if let Some(failure_code) = self.failure_code {
            if self.status != Some(-2) {
                return Err(HTTPError::new(
                    400,
                    "failure_code can only be set with status -2".to_string(),
                )
                .into());
            }
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

#[derive(Debug, Deserialize, Validate)]
pub struct CompleteChargeInput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
    pub currency: String,
    #[validate(range(min = 1))]
    pub amount: i64,
    pub charge_id: String,
    pub charge_payload: PackObject<Vec<u8>>,
}

pub async fn complete(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<CompleteChargeInput>,
) -> Result<PackObject<SuccessResponse<ChargeOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    let id = input.id.unwrap();
    ctx.set_kvs(vec![
        ("action", "complete_charge".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
        ("currency", input.currency.clone().into()),
        ("amount", input.amount.into()),
    ])
    .await;

    let mut doc = db::Charge::with_pk(uid, id);
    doc.get_one(
        &app.scylla,
        vec![
            "quantity".to_string(),
            "provider".to_string(),
            "currency".to_string(),
            "amount".to_string(),
            "charge_id".to_string(),
        ],
    )
    .await?;

    if doc.charge_id != input.charge_id {
        return Err(HTTPError::new(
            400,
            format!(
                "charge_id mismatch, expected {}, got {}",
                doc.charge_id, input.charge_id
            ),
        ));
    }

    let mut cols = ColumnsMap::new();
    cols.set_as("status", &2i8);
    cols.set_as("currency", &input.currency);
    cols.set_as("amount", &input.amount);
    cols.set_as("charge_payload", &input.charge_payload.unwrap());

    let ok = doc.update(&app.scylla, cols, 1).await?;
    if !ok {
        if doc.status >= 2 {
            return Ok(to.with(SuccessResponse::new(ChargeOutput::from(doc, &to))));
        }

        return Err(HTTPError::new(
            500,
            format!("Invalid status {} for completing charge", doc.status),
        ));
    }

    let mut txn = db::Transaction {
        description: format!("{}.topup", doc.provider),
        payload: cbor_to_vec(&TransactionPayload {
            kind: "charge".to_string(),
            id: PackObject::Cbor(doc.id),
            provider: Some(doc.provider.clone()),
            currency: Some(input.currency.clone()),
            amount: Some(input.amount),
        })
        .unwrap_or_default(),
        ..Default::default()
    };

    txn.prepare(
        &app.scylla,
        &app.mac,
        uid,
        db::TransactionKind::Topup,
        doc.quantity,
    )
    .await?;
    let wallet = txn.commit(&app.scylla, &app.mac).await?;

    let mut cols = ColumnsMap::with_capacity(2);
    cols.set_as("status", &3i8);
    cols.set_as("txn", &txn.id);
    doc.update(&app.scylla, cols, 2i8).await?;

    if wallet.map(|w| w.credits == 0) == Some(true) {
        tokio::spawn(award_first_topup(
            app,
            ReqContext::new(ctx.rid.clone(), uid, 0),
            txn.id,
        ));
    }

    if let Ok(cur) = Currency::from_str(&input.currency) {
        let amount = input.amount as f32 / 10f32.powi(cur.decimals as i32);
        ctx.set(
            "message",
            format!("{:.2} {}", amount.to_string(), cur.name).into(),
        )
        .await;
    }

    Ok(to.with(SuccessResponse::new(ChargeOutput::from(doc, &to))))
}

#[derive(Deserialize)]
struct AwardPayload {
    pub referrer: Option<PackObject<xid::Id>>,
}

async fn award_first_topup(app: Arc<AppState>, ctx: ReqContext, txn: xid::Id) {
    let res: anyhow::Result<()> = async {
        let mut credit = db::Credit::with_pk(ctx.user, txn);
        credit.kind = db::CreditKind::Award.to_string();
        credit.amount = 10;
        credit.description = "member.active".to_string();
        let res = credit.save(&app.scylla).await;
        ctx.set("init_credits", res.is_ok().into()).await;

        if res.is_ok() {
            let tx0 = db::Transaction::first_from_system(&app.scylla, ctx.user).await?;
            if let Ok(payload) = cbor_from_slice::<AwardPayload>(&tx0.payload) {
                if let Some(referrer) = payload.referrer {
                    ctx.set("referrer", referrer.to_string().into()).await;
                    let mut wallet = db::Wallet::with_pk(referrer.unwrap());
                    wallet.get_one(&app.scylla).await?;
                    if wallet.credits > 0 {
                        let mut txn = db::Transaction {
                            description: "Referral reward".to_string(),
                            payload: cbor_to_vec(&TransactionPayload {
                                kind: "transaction".to_string(),
                                id: PackObject::Cbor(txn),
                                provider: None,
                                currency: None,
                                amount: None,
                            })
                            .unwrap_or_default(),
                            ..Default::default()
                        };

                        txn.prepare(
                            &app.scylla,
                            &app.mac,
                            wallet.uid,
                            db::TransactionKind::Award,
                            50,
                        )
                        .await?;
                        txn.commit(&app.scylla, &app.mac).await?;
                        ctx.set("award_txn", txn.id.to_string().into()).await;
                    }
                }
            }
        }
        Ok(())
    }
    .await;

    let kv = ctx.get_kv().await;
    let elapsed = ctx.start.elapsed().as_millis() as u64;
    match res {
        Ok(_) => {
            log::info!(target: "async_jobs",
                action = "award_first_topup",
                rid = ctx.rid,
                uid = ctx.user.to_string(),
                start = ctx.unix_ms,
                elapsed = elapsed,
                kv = log::as_serde!(kv);
                "",
            );
        }
        Err(err) => {
            log::error!(target: "async_jobs",
                action = "award_first_topup",
                rid = ctx.rid,
                uid = ctx.user.to_string(),
                start = ctx.unix_ms,
                elapsed= elapsed,
                kv = log::as_serde!(kv);
                "{}", err.to_string(),
            );
        }
    }
}
