use std::collections::HashSet;
use std::str::FromStr;
use strum_macros::{AsRefStr, EnumString};

use axum_web::{context::unix_ms, erring::HTTPError};
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use super::{Wallet, SYS_ID};
use crate::db::scylladb::{self, extract_applied};

#[derive(AsRefStr, Debug, EnumString, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum CreditKind {
    Init,
    Award,
    Expenditure,
    Income,
}

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Credit {
    pub uid: xid::Id,
    pub txn: xid::Id,
    pub kind: String,
    pub amount: i64,
    pub description: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Credit {
    pub fn with_pk(uid: xid::Id, txn: xid::Id) -> Self {
        Self {
            uid,
            txn,
            ..Default::default()
        }
    }

    pub fn select_fields(select_fields: Vec<String>, with_pk: bool) -> anyhow::Result<Vec<String>> {
        if select_fields.is_empty() {
            return Ok(Self::fields());
        }

        let fields = Self::fields();
        for field in &select_fields {
            if !fields.contains(field) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        let mut select_fields = select_fields;
        let field = "kind".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        if with_pk {
            let field = "uid".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "txn".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
        }

        Ok(select_fields)
    }

    pub async fn create(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        txn: xid::Id,
        kind: CreditKind,
        amount: i64,
        description: String,
    ) -> anyhow::Result<()> {
        if amount <= 0 {
            return Err(HTTPError::new(400, format!("Invalid amount {}", amount)).into());
        }

        if uid == SYS_ID {
            return Ok(());
        }

        let mut wallet = Wallet::with_pk(uid);
        wallet.get_one(db).await?;

        if wallet.credits == 0 && kind != CreditKind::Init {
            // credits is not initialized, skip
            return Ok(());
        }

        let mut log: Self = Credit {
            uid,
            txn,
            kind: kind.as_ref().to_string(),
            amount,
            description,
            ..Default::default()
        };

        let fields = Self::fields();
        log._fields = fields.iter().map(|f| f.to_string()).collect();
        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut insert_params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = log.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            insert_params.push(cols.get(field).unwrap());
        }

        let insert_query = format!(
            "INSERT INTO credit ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(","),
        );

        let res = db.execute(insert_query, insert_params).await?;
        if extract_applied(res) {
            let update_wallet_query = "UPDATE wallet SET credits=credits+? WHERE uid=?";
            let update_wallet_params = (amount, uid.to_cql());
            db.execute(update_wallet_query.to_string(), update_wallet_params)
                .await?;
        } else {
            log::warn!(target: "scylladb",
                action = "create_credit",
                uid = log.uid.to_string(),
                txn = log.txn.to_string(),
                kind = log.kind,
                amount = log.amount,
                result = false;
                "add credits to walllet on other node, skip",
            );
        }

        Ok(())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        kind: Option<String>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if kind.is_none() {
                let query = format!(
                    "SELECT {} FROM credit WHERE uid=? AND txn<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                );
                let params = (uid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_iter(query, params).await?
            } else {
                let query = format!(
                    "SELECT {} FROM credit WHERE uid=? AND kind=? AND txn<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","));
                let params = (uid.to_cql(), id.to_cql(), kind.unwrap(), page_size as i32);
                db.execute_iter(query, params).await?
            }
        } else if kind.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM credit WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM credit WHERE uid=? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.as_bytes(), kind.unwrap(), page_size as i32);
            db.execute_iter(query, params).await?
        };

        let mut res: Vec<Self> = Vec::with_capacity(rows.len());
        for row in rows {
            let mut doc = Self::default();
            let mut cols = ColumnsMap::with_capacity(fields.len());
            cols.fill(row, &fields)?;
            doc.fill(&cols);
            doc._fields = fields.clone();
            res.push(doc);
        }

        Ok(res)
    }
}
