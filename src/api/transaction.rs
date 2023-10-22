use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, sync::Arc};
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::db;
use crate::{
    api::{get_fields, token_from_xid, token_to_xid, AppState, Pagination, QueryUidId},
    db::TransactionKind,
};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct TransactionOutput {
    pub id: PackObject<xid::Id>,
    pub sequence: i64,
    pub payee: PackObject<xid::Id>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub_payee: Option<PackObject<xid::Id>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payer: Option<PackObject<xid::Id>>,
    pub status: i8,
    pub kind: String,
    pub amount: i64,
    pub sys_fee: i64,
    pub sub_shares: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<PackObject<Vec<u8>>>,
}

impl TransactionOutput {
    pub fn from<T>(val: db::Transaction, to: &PackObject<T>) -> Self {
        let mut rt = Self {
            id: to.with(val.id),
            sequence: val.sequence,
            payee: to.with(val.payee),
            status: val.status,
            kind: val.kind.clone(),
            amount: val.amount,
            sys_fee: val.sys_fee,
            sub_shares: val.sub_shares,
            ..Default::default()
        };

        match TransactionKind::from_str(&val.kind) {
            Ok(TransactionKind::Award)
            | Ok(TransactionKind::Topup)
            | Ok(TransactionKind::Sponsor)
            | Ok(TransactionKind::Subscribe) => rt.payer = to.with_option(Some(val.uid)),
            _ => {}
        }

        for v in val._fields {
            match v.as_str() {
                "sub_payee" => rt.sub_payee = to.with_option(val.sub_payee),
                "description" => rt.description = Some(val.description.to_owned()),
                "payload" => rt.payload = Some(to.with(val.payload.to_owned())),
                _ => {}
            }
        }

        rt
    }
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    input: Query<QueryUidId>,
) -> Result<PackObject<SuccessResponse<TransactionOutput>>, HTTPError> {
    input.validate()?;

    let uid = *input.uid.to_owned();
    let id = *input.id.to_owned();
    ctx.set_kvs(vec![
        ("action", "get_transaction".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Transaction::with_pk(uid, id);
    doc.get_one(&app.scylla, get_fields(input.fields.clone()))
        .await?;
    Ok(to.with(SuccessResponse::new(TransactionOutput::from(doc, &to))))
}

pub async fn list_outgo(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<TransactionOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_outgo".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let kind = if input.kind.is_some() {
        Some(
            db::TransactionKind::from_str(&input.kind.unwrap())
                .map_err(|e| HTTPError::new(400, format!("Invalid kind: {}", e)))?,
        )
    } else {
        None
    };

    let res = db::Transaction::list(
        &app.scylla,
        input.uid.unwrap(),
        fields,
        page_size,
        token_to_xid(&input.page_token),
        kind,
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
            .map(|r| TransactionOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

pub async fn list_income(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<TransactionOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_income".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let kind = if input.kind.is_some() {
        Some(
            db::TransactionKind::from_str(&input.kind.unwrap())
                .map_err(|e| HTTPError::new(400, format!("Invalid kind: {}", e)))?,
        )
    } else {
        None
    };

    let res = db::Transaction::list_by_payee(
        &app.scylla,
        input.uid.unwrap(),
        fields,
        page_size,
        token_to_xid(&input.page_token),
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
            .map(|r| TransactionOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct TransactionInput {
    pub uid: PackObject<xid::Id>,
    pub id: PackObject<xid::Id>,
}

pub async fn commit(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<TransactionInput>,
) -> Result<PackObject<SuccessResponse<TransactionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    let id = input.id.unwrap();
    ctx.set_kvs(vec![
        ("action", "commit_transaction".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Transaction::with_pk(uid, id);
    doc.get_one(
        &app.scylla,
        vec![
            "sequence".to_string(),
            "payee".to_string(),
            "sub_payee".to_string(),
            "status".to_string(),
            "kind".to_string(),
            "amount".to_string(),
            "sys_fee".to_string(),
            "sub_shares".to_string(),
        ],
    )
    .await?;

    doc.commit(&app.scylla, &app.mac).await?;
    let mut credits = doc.credits();
    db::Credit::save_all(&app.scylla, &mut credits).await?;
    Ok(to.with(SuccessResponse::new(TransactionOutput::from(doc, &to))))
}

pub async fn cancel(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<TransactionInput>,
) -> Result<PackObject<SuccessResponse<TransactionOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let id = input.id.unwrap();
    let uid = input.uid.unwrap();
    ctx.set_kvs(vec![
        ("action", "commit_transaction".into()),
        ("uid", uid.to_string().into()),
        ("id", id.to_string().into()),
    ])
    .await;

    let mut doc = db::Transaction::with_pk(uid, id);
    doc.get_one(
        &app.scylla,
        vec![
            "sequence".to_string(),
            "payee".to_string(),
            "sub_payee".to_string(),
            "status".to_string(),
            "kind".to_string(),
            "amount".to_string(),
            "sys_fee".to_string(),
            "sub_shares".to_string(),
        ],
    )
    .await?;

    doc.cancel(&app.scylla, &app.mac).await?;
    Ok(to.with(SuccessResponse::new(TransactionOutput::from(doc, &to))))
}
