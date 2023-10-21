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
    api::{token_from_xid, token_to_xid, AppState, Pagination, QueryUid},
    db::SYS_ID,
};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct WalletOutput {
    pub sequence: i64,
    pub award: i64,
    pub topup: i64,
    pub income: i64,
    pub credits: i64,
    pub txn: PackObject<xid::Id>,
}

impl WalletOutput {
    pub fn from<T>(val: db::Wallet, to: &PackObject<T>) -> Self {
        Self {
            sequence: val.sequence,
            award: val.award,
            topup: val.topup,
            income: val.income,
            credits: val.credits,
            txn: to.with(val.txn),
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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CreditOutput {
    pub txn: PackObject<xid::Id>,
    pub kind: String,
    pub amount: i64,
    pub description: String,
}

impl CreditOutput {
    pub fn from<T>(val: db::Credit, to: &PackObject<T>) -> Self {
        Self {
            txn: to.with(val.txn),
            kind: val.kind,
            amount: val.amount,
            description: val.description,
        }
    }
}

pub async fn list_credits(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<Pagination>,
) -> Result<PackObject<SuccessResponse<Vec<CreditOutput>>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let page_size = input.page_size.unwrap_or(10);
    ctx.set_kvs(vec![
        ("action", "list_credit".into()),
        ("uid", input.uid.to_string().into()),
        ("page_size", page_size.into()),
    ])
    .await;

    let fields = input.fields.unwrap_or_default();
    let res = db::Credit::list(
        &app.scylla,
        input.uid.unwrap(),
        fields,
        page_size,
        token_to_xid(&input.page_token),
        None,
    )
    .await?;
    let next_page_token = if res.len() >= page_size as usize {
        to.with_option(token_from_xid(res.last().unwrap().txn))
    } else {
        None
    };

    Ok(to.with(SuccessResponse {
        total_size: None,
        next_page_token,
        result: res
            .iter()
            .map(|r| CreditOutput::from(r.to_owned(), &to))
            .collect(),
    }))
}

#[derive(Debug, Deserialize, Validate)]
pub struct AwardInput {
    pub payee: PackObject<xid::Id>,
    #[validate(range(min = 1, max = 1000000))]
    pub amount: i64,
    #[validate(range(min = 0, max = 1000000))]
    pub credits: u64,
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
    } else {
        txn.description = "payee.award".to_string();
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

    if input.credits > 0 {
        let mut credit = db::Credit::with_pk(payee, txn.id);
        credit.kind = db::CreditKind::Award.to_string();
        credit.amount = input.credits as i64;
        credit.description = txn.description;
        credit.save(&app.scylla).await?;
        ctx.set("credits", input.credits.into()).await;
    }

    let mut wallet = db::Wallet::with_pk(payee);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}

#[derive(Debug, Deserialize, Validate)]
pub struct SpendInput {
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
pub async fn spend(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<SpendInput>,
) -> Result<PackObject<SuccessResponse<WalletOutput>>, HTTPError> {
    let (to, input) = to.unpack();
    input.validate()?;

    let uid = input.uid.unwrap();
    ctx.set_kvs(vec![
        ("action", "spend".into()),
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
        db::TransactionKind::Spend,
        input.amount,
    )
    .await?;

    let mut wallet = db::Wallet::with_pk(uid);
    wallet.get_one(&app.scylla).await?;
    wallet.txn = txn.id; // txn.id may be not the walllet.txn, return the txn.id to the caller
    Ok(to.with(SuccessResponse::new(WalletOutput::from(wallet, &to))))
}

// the txn is not committed, it should be committed or cancelled by the caller
// returns payer's wallet
pub async fn subscribe(
    State(app): State<Arc<AppState>>,
    Extension(ctx): Extension<Arc<ReqContext>>,
    to: PackObject<SpendInput>,
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
    to: PackObject<SpendInput>,
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
