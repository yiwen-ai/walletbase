use futures::future::join_all;

use std::str::FromStr;
use strum_macros::{AsRefStr, EnumString};

use axum_web::erring::HTTPError;
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

impl ToString for CreditKind {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
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

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM credit WHERE uid=? AND txn=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.txn.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        if self.amount <= 0 {
            return Err(HTTPError::new(400, format!("Invalid amount {}", self.amount)).into());
        }

        if self.uid == SYS_ID {
            return Ok(());
        }

        let mut wallet = Wallet::with_pk(self.uid);
        wallet.get_one(db).await?;

        let is_init = self.kind == CreditKind::Init.as_ref();
        if is_init && wallet.credits > 0 {
            return Err(
                HTTPError::new(400, format!("Wallet {} already initialized", self.uid)).into(),
            );
        }

        if wallet.credits == 0 && !is_init {
            // credits is not initialized, skip
            return Ok(());
        }
        if is_init {
            self.amount = 10;
        }

        let fields = Self::fields();
        self._fields = fields.iter().map(|f| f.to_string()).collect();
        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut insert_params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

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
            let query = "UPDATE wallet SET credits=? WHERE uid=? IF credits=?";
            for _ in 0..5 {
                wallet.get_one(db).await?;
                let params = (
                    self.amount + wallet.credits,
                    wallet.uid.to_cql(),
                    wallet.credits,
                );
                let res = db.execute(query, params).await?;
                if extract_applied(res) {
                    return Ok(());
                }
            }

            log::error!(target: "scylladb",
                action = "add_credit",
                uid = self.uid.to_string(),
                txn = self.txn.to_string(),
                wallet = self.uid.to_string();
                "add_credit failed",
            );

            return Err(HTTPError::new(
                500,
                format!("add_credit failed: {}, {}", self.uid, self.txn),
            )
            .into());
        } else {
            log::warn!(target: "scylladb",
                action = "add_credit",
                uid = self.uid.to_string(),
                txn = self.txn.to_string(),
                kind = self.kind,
                amount = self.amount,
                result = false;
                "add credits to walllet on other node, skip",
            );
        }

        Ok(())
    }

    pub async fn save_all(
        db: &scylladb::ScyllaDB,
        credits: &mut Vec<Credit>,
    ) -> anyhow::Result<()> {
        match credits.len() {
            0 => return Ok(()),
            1 => return credits[0].save(db).await,
            _ => {}
        }

        let res = join_all(credits.iter_mut().map(|credit| credit.save(db))).await;
        let errs: Vec<String> = res
            .into_iter()
            .filter_map(|r| r.err())
            .map(|e| e.to_string())
            .collect();
        if errs.is_empty() {
            return Ok(());
        }

        Err(HTTPError::new(
            500,
            format!("Credit::save_all partly applied, errors: {:?}", errs),
        )
        .into())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        kind: Option<CreditKind>,
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
                let params = (
                    uid.to_cql(),
                    id.to_cql(),
                    kind.unwrap().to_string(),
                    page_size as i32,
                );
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
            let params = (uid.as_bytes(), kind.unwrap().to_string(), page_size as i32);
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

#[cfg(test)]
mod tests {
    use crate::conf;

    use super::*;

    async fn get_db() -> scylladb::ScyllaDB {
        let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
        let res = scylladb::ScyllaDB::new(cfg.scylla, "walletbase_test").await;
        res.unwrap()
    }

    #[test]
    fn credit_kind_works() {
        {
            assert_eq!("init", CreditKind::Init.as_ref());
            assert_eq!("award", CreditKind::Award.as_ref());
            assert_eq!("expenditure", CreditKind::Expenditure.as_ref());
            assert_eq!("income", CreditKind::Income.as_ref());

            assert_eq!(CreditKind::Init, CreditKind::from_str("init").unwrap());
            assert_eq!(CreditKind::Award, CreditKind::from_str("award").unwrap());
            assert_eq!(
                CreditKind::Expenditure,
                CreditKind::from_str("expenditure").unwrap()
            );
            assert_eq!(CreditKind::Income, CreditKind::from_str("income").unwrap());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn credit_model_works() {
        let db = get_db().await;

        let mut wallet = Wallet::with_pk(xid::new());
        wallet.save(&db).await.unwrap();

        let mut credit: Credit = Default::default();
        let res = credit.save(&db).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Invalid amount 0"));
        credit.amount = 10;
        credit.save(&db).await.unwrap();
        assert!(credit.get_one(&db, vec![]).await.is_err());

        let mut credit = Credit::with_pk(wallet.uid, xid::new());
        credit.amount = 10;
        credit.kind = CreditKind::Award.to_string();
        credit.save(&db).await.unwrap();
        assert!(credit.get_one(&db, vec![]).await.is_err());

        credit.kind = CreditKind::Init.to_string();
        credit.save(&db).await.unwrap();
        credit.get_one(&db, vec![]).await.unwrap();
        wallet.get_one(&db).await.unwrap();
        assert_eq!(10, wallet.credits);

        let mut credit = Credit::with_pk(wallet.uid, xid::new());
        credit.amount = 10;
        credit.kind = CreditKind::Init.to_string();
        let res = credit.save(&db).await;
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("already initialized"));

        credit.kind = CreditKind::Award.to_string();
        credit.save(&db).await.unwrap();
        wallet.get_one(&db).await.unwrap();
        assert_eq!(20, wallet.credits);

        credit.kind = CreditKind::Award.to_string();
        credit.save(&db).await.unwrap();
        wallet.get_one(&db).await.unwrap();
        assert_eq!(20, wallet.credits);

        let mut credit = Credit::with_pk(wallet.uid, xid::new());
        credit.amount = 100;
        credit.kind = CreditKind::Expenditure.to_string();
        credit.save(&db).await.unwrap();
        wallet.get_one(&db).await.unwrap();
        assert_eq!(120, wallet.credits);

        credit.save(&db).await.unwrap();
        wallet.get_one(&db).await.unwrap();
        assert_eq!(120, wallet.credits);

        let logs = Credit::list(&db, wallet.uid, vec![], 10, None, None)
            .await
            .unwrap();
        assert_eq!(3, logs.len());
        assert_eq!(CreditKind::Expenditure.to_string(), logs[0].kind);
        assert_eq!(100i64, logs[0].amount);
        assert_eq!(CreditKind::Award.to_string(), logs[1].kind);
        assert_eq!(10i64, logs[1].amount);
        assert_eq!(CreditKind::Init.to_string(), logs[2].kind);
        assert_eq!(10i64, logs[2].amount);
    }
}
