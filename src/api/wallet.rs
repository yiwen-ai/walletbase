use axum::{
    extract::{Query, State},
    Extension,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use validator::Validate;

use axum_web::context::ReqContext;
use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::db;
use crate::{
    api::{AppState, QueryUid},
    db::SYS_ID,
};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct WalletOutput {
    pub sequence: i64,
    pub award: i64,
    pub topup: i64,
    pub income: i64,
    pub credits: i64,
    pub txn: Option<PackObject<xid::Id>>,
}

impl WalletOutput {
    pub fn from<T>(val: db::Wallet, to: &PackObject<T>) -> Self {
        Self {
            sequence: val.sequence,
            award: val.award,
            topup: val.topup,
            income: val.income,
            credits: val.credits,
            txn: if val.txn.is_zero() {
                None
            } else {
                Some(to.with(val.txn))
            },
        }
    }
}

pub async fn get(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<()>,
    Query(input): Query<QueryUid>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    input.validate()?;

    ctx.set_kvs(vec![
        ("action", "get_wallet".into()),
        ("uid", input.uid.to_string().into()),
    ])
    .await;

    let mut doc = db::Wallet::with_pk(input.uid.unwrap());
    let res = doc.get_one(&app.scylla).await;
    ctx.set("exists", res.is_ok().into()).await;

    Ok(to.with(SuccessResponse::new(WalletOutput::from(doc, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct AwardInput {
    pub payee: PackObject<xid::Id>,
    #[validate(range(min = 1, max = 1000000))]
    pub amount: i64,
    #[validate(range(min = 1, max = 1000000))]
    pub credits: Option<i64>,
    pub description: Option<String>,
    pub payload: Option<PackObject<Vec<u8>>>,
}

// the txn is committed.
// returns payee's wallet
pub async fn award(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<AwardInput>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let payee = input.payee.unwrap();
    ctx.set_kvs(vec![
        ("action", "award".into()),
        ("payee", payee.to_string().into()),
        ("amount", input.amount.into()),
    ])
    .await;

    let mut txn: db::Transaction = Default::default();
    if let Some(description) = input.description {
        txn.description = description;
    }
    if let Some(payload) = input.payload {
        txn.payload = payload.unwrap();
    }

    txn.prepare(
        &app.scylla,
        &app.mac,
        payee,
        db::TransactionKind::Award,
        input.amount,
    )
    .await?;
    txn.commit(&app.scylla, &app.mac).await?;

    if let Some(amount) = input.credits {
        let mut credit = db::Credit::with_pk(payee, txn.id);
        credit.kind = db::CreditKind::Award.to_string();
        credit.amount = amount;
        credit.save(&app.scylla).await?;
        ctx.set("credits", amount.into()).await;
    }

    let mut wallet = db::Wallet::with_pk(payee);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct ExpendInput {
    pub uid: PackObject<xid::Id>,
    pub payee: Option<PackObject<xid::Id>>,
    pub sub_payee: Option<PackObject<xid::Id>>,
    #[validate(range(min = 1, max = 1000000))]
    pub amount: i64,
    pub description: Option<String>,
    pub payload: Option<PackObject<Vec<u8>>>,
}

// the txn is not committed, it should be committed or cancelled by the caller
// returns payer's wallet
pub async fn expend(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<ExpendInput>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    ctx.set_kvs(vec![
        ("action", "expend".into()),
        ("payer", uid.to_string().into()),
        ("amount", input.amount.into()),
    ])
    .await;

    let mut txn = db::Transaction::with_uid(uid);
    if let Some(description) = input.description {
        txn.description = description;
    }
    if let Some(payload) = input.payload {
        txn.payload = payload.unwrap();
    }

    txn.prepare(
        &app.scylla,
        &app.mac,
        SYS_ID,
        db::TransactionKind::Expenditure,
        input.amount,
    )
    .await?;

    let mut wallet = db::Wallet::with_pk(uid);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}

// the txn is committed.
// returns payer's wallet
pub async fn sponsor(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<ExpendInput>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    if input.payee.is_none() {
        return Err(HTTPError::new(400, "payee is required".to_string()));
    }
    let payee = *input.payee.unwrap();
    ctx.set_kvs(vec![
        ("action", "sponsor".into()),
        ("payer", uid.to_string().into()),
        ("payee", payee.to_string().into()),
        ("amount", input.amount.into()),
    ])
    .await;

    let mut txn = db::Transaction::with_uid(uid);
    if let Some(description) = input.description {
        txn.description = description;
    }
    if let Some(payload) = input.payload {
        txn.payload = payload.unwrap();
    }
    if let Some(sub_payee) = input.sub_payee {
        ctx.set("sub_payee", sub_payee.to_string().into()).await;
        txn.sub_payee = Some(sub_payee.unwrap());
    }

    txn.prepare(
        &app.scylla,
        &app.mac,
        payee,
        db::TransactionKind::Sponsor,
        input.amount,
    )
    .await?;
    txn.commit(&app.scylla, &app.mac).await?;

    let mut credits = txn.credits();
    db::Credit::save_all(&app.scylla, &mut credits).await?;

    let mut wallet = db::Wallet::with_pk(uid);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}

// the txn is committed.
// returns payer's wallet
pub async fn subscribe(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<ExpendInput>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    if input.payee.is_none() {
        return Err(HTTPError::new(400, "payee is required".to_string()));
    }
    let payee = *input.payee.unwrap();
    ctx.set_kvs(vec![
        ("action", "subscribe".into()),
        ("payer", uid.to_string().into()),
        ("payee", payee.to_string().into()),
        ("amount", input.amount.into()),
    ])
    .await;

    let mut txn = db::Transaction::with_uid(uid);
    if let Some(description) = input.description {
        txn.description = description;
    }
    if let Some(payload) = input.payload {
        txn.payload = payload.unwrap();
    }
    if let Some(sub_payee) = input.sub_payee {
        ctx.set("sub_payee", sub_payee.to_string().into()).await;
        txn.sub_payee = Some(sub_payee.unwrap());
    }

    txn.prepare(
        &app.scylla,
        &app.mac,
        payee,
        db::TransactionKind::Subscribe,
        input.amount,
    )
    .await?;
    txn.commit(&app.scylla, &app.mac).await?;

    let mut credits = txn.credits();
    db::Credit::save_all(&app.scylla, &mut credits).await?;

    let mut wallet = db::Wallet::with_pk(uid);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}
