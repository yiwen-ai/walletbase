use anyhow::anyhow;
use futures::{future::BoxFuture, join};
use futures_util::FutureExt;
use std::str::FromStr;
use strum_macros::{AsRefStr, EnumString};

use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use super::{income_fee_rate, Credit, CreditKind, HMacTag, Wallet, SYS_FEE_RATE, SYS_ID};
use crate::db::scylladb::{self, extract_applied};

// user's wallet.topup can be negative to MAX_OVERDRAW.
const MAX_OVERDRAW: i64 = 100;

#[derive(AsRefStr, Debug, EnumString, PartialEq)]
#[strum(serialize_all = "lowercase")]
pub enum TransactionKind {
    Award,
    Topup,
    Refund,
    Withdraw,
    Spend,
    Sponsor,
    Subscribe,
    // Redpacket, // TODO
}

impl ToString for TransactionKind {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl TransactionKind {
    pub fn check_payer(&self, uid: xid::Id) -> anyhow::Result<()> {
        match self {
            TransactionKind::Award | TransactionKind::Topup => {
                if uid != SYS_ID {
                    return Err(HTTPError::new(
                        400,
                        format!("Invalid payer {} for {} transaction", uid, self.as_ref()),
                    )
                    .into());
                }

                Ok(())
            }
            _ => {
                if uid == SYS_ID {
                    return Err(HTTPError::new(
                        400,
                        format!("Invalid payer {} for {} transaction", uid, self.as_ref()),
                    )
                    .into());
                }

                Ok(())
            }
        }
    }

    pub fn check_payee(&self, uid: xid::Id) -> anyhow::Result<()> {
        match self {
            TransactionKind::Spend | TransactionKind::Withdraw | TransactionKind::Refund => {
                if uid != SYS_ID {
                    return Err(HTTPError::new(
                        400,
                        format!("Invalid payee {} for {} transaction", uid, self.as_ref()),
                    )
                    .into());
                }

                Ok(())
            }
            _ => {
                if uid == SYS_ID {
                    return Err(HTTPError::new(
                        400,
                        format!("Invalid payee {} for {} transaction", uid, self.as_ref()),
                    )
                    .into());
                }

                Ok(())
            }
        }
    }

    pub fn check_sub_payee(&self, uid: xid::Id) -> anyhow::Result<()> {
        match self {
            TransactionKind::Sponsor | TransactionKind::Subscribe => Ok(()),
            _ => Err(HTTPError::new(
                400,
                format!(
                    "Invalid sub_payee {} for {} transaction",
                    uid,
                    self.as_ref()
                ),
            )
            .into()),
        }
    }

    pub fn sub_payer_balance(&self, wallet: &mut Wallet, amount: i64) -> anyhow::Result<()> {
        assert!(amount > 0);
        if wallet.is_system() {
            match self {
                TransactionKind::Award => {
                    wallet.award -= amount;
                }
                TransactionKind::Topup => {
                    wallet.topup -= amount;
                }
                _ => {
                    return Err(HTTPError::new(
                        400,
                        format!("Invalid {} transaction", self.as_ref()),
                    )
                    .into());
                }
            }

            return Ok(());
        }

        if wallet.credits == 0 && *self != TransactionKind::Spend {
            return Err(HTTPError::new(
                400,
                format!("Require credits for {} transaction", self.as_ref()),
            )
            .into());
        }

        let quota = match self {
            TransactionKind::Withdraw => wallet.income,
            TransactionKind::Refund => wallet.topup,
            TransactionKind::Spend => wallet.balance() + MAX_OVERDRAW,
            _ => wallet.balance(),
        };

        let b = wallet.balance();
        if b <= 0 || quota < amount {
            return Err(HTTPError::new(
                400,
                format!(
                    "Insufficient balance for {} transaction, expected {}, got {}",
                    self.as_ref(),
                    amount,
                    b
                ),
            )
            .into());
        }

        match self {
            TransactionKind::Withdraw => {
                wallet.income -= amount;
            }
            TransactionKind::Refund => {
                wallet.topup -= amount;
            }
            TransactionKind::Spend | TransactionKind::Sponsor | TransactionKind::Subscribe => {
                wallet.award -= amount;
                if wallet.award < 0 {
                    wallet.topup -= -wallet.award;
                    wallet.award = 0;
                    if wallet.topup < 0 {
                        wallet.income -= -wallet.topup;
                        wallet.topup = 0;

                        if wallet.income < 0 {
                            // overdraw will be recorded on topup
                            (wallet.topup, wallet.income) = (wallet.income, 0);
                        }
                    }
                }
            }
            _ => {
                return Err(HTTPError::new(
                    400,
                    format!(
                        "Invalid payer {} for {} transaction",
                        wallet.uid,
                        self.as_ref()
                    ),
                )
                .into());
            }
        }

        Ok(())
    }

    pub fn rollback_payer_balance(&self, wallet: &mut Wallet, amount: i64) -> anyhow::Result<()> {
        match self {
            TransactionKind::Award => {
                wallet.award += amount;
            }
            TransactionKind::Topup | TransactionKind::Refund => {
                wallet.topup += amount;
            }
            TransactionKind::Withdraw => {
                wallet.income += amount;
            }
            TransactionKind::Spend | TransactionKind::Sponsor | TransactionKind::Subscribe => {
                // can not rollback to award or income balance.
                wallet.topup += amount;
            }
        }

        Ok(())
    }

    pub fn add_payee_balance(&self, wallet: &mut Wallet, amount: i64) -> anyhow::Result<()> {
        match self {
            TransactionKind::Award => {
                wallet.award += amount;
            }
            TransactionKind::Topup | TransactionKind::Refund | TransactionKind::Withdraw => {
                wallet.topup += amount;
            }
            TransactionKind::Spend | TransactionKind::Sponsor | TransactionKind::Subscribe => {
                wallet.income += amount;
            }
        }

        Ok(())
    }

    pub fn fee_and_shares(&self, amount: i64, credits: i64, has_sub_payee: bool) -> (i64, i64) {
        match self {
            TransactionKind::Withdraw => {
                let mut sys_fee = (amount as f32 * SYS_FEE_RATE) as i64;
                if sys_fee < 1 {
                    sys_fee = 1;
                }
                (sys_fee, 0)
            }

            TransactionKind::Sponsor | TransactionKind::Subscribe => {
                let rate = income_fee_rate(credits);
                let mut sys_fee = (amount as f32 * rate) as i64;
                if sys_fee < 1 {
                    sys_fee = 1;
                }

                let sub_shares = if has_sub_payee {
                    (amount - sys_fee) / 2
                } else {
                    0
                };
                (sys_fee, sub_shares)
            }
            _ => (0i64, 0i64),
        }
    }
}

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Transaction {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub sequence: i64,
    pub payee: xid::Id,
    pub sub_payee: Option<xid::Id>,
    pub status: i8,
    pub kind: String,
    pub amount: i64,
    pub sys_fee: i64,
    pub sub_shares: i64,
    pub description: String,
    pub payload: Vec<u8>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Transaction {
    pub fn with_pk(uid: xid::Id, id: xid::Id) -> Self {
        Self {
            uid,
            id,
            ..Default::default()
        }
    }

    pub fn with_uid(uid: xid::Id) -> Self {
        Self {
            uid,
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
        let field = "status".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "kind".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        if with_pk {
            let field = "uid".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "id".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
        }

        Ok(select_fields)
    }

    // do it after transaction commited.
    pub fn credits(&self) -> Vec<Credit> {
        let kind = TransactionKind::from_str(&self.kind);
        if self.status != 3 || self.uid == SYS_ID || kind.is_err() {
            return Vec::new();
        }

        let kind = kind.unwrap();
        let mut logs: Vec<Credit> = Vec::with_capacity(3);
        match kind {
            TransactionKind::Spend | TransactionKind::Sponsor | TransactionKind::Subscribe => {
                logs.push(Credit {
                    uid: self.uid,
                    txn: self.id,
                    kind: CreditKind::Payout.to_string(),
                    amount: self.amount,
                    description: self.description.clone(),
                    ..Default::default()
                });
            }
            _ => {}
        }

        match kind {
            TransactionKind::Sponsor | TransactionKind::Subscribe => {
                logs.push(Credit {
                    uid: self.payee,
                    txn: self.id,
                    kind: CreditKind::Income.to_string(),
                    amount: self.amount - self.sys_fee - self.sub_shares,
                    description: self.description.clone(),
                    ..Default::default()
                });

                if self.sub_shares > 0 && self.sub_payee.is_some() {
                    logs.push(Credit {
                        uid: self.sub_payee.unwrap(),
                        txn: self.id,
                        kind: CreditKind::Income.to_string(),
                        amount: self.sub_shares,
                        description: self.description.clone(),
                        ..Default::default()
                    });
                }
            }
            _ => {}
        }

        logs
    }

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM transaction WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    async fn delete(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let query = "DELETE FROM transaction WHERE uid=? AND id=?";
        let params = (self.uid.to_cql(), self.id.to_cql());
        let _ = db.execute(query.to_string(), params).await?;
        Ok(())
    }

    async fn set_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        from: i8,
        to: i8,
    ) -> anyhow::Result<bool> {
        let query = "UPDATE transaction SET status=? WHERE uid=? AND id=? IF status=?";
        let params = (to, self.uid.to_cql(), self.id.to_cql(), from);
        let res = db.execute(query.to_string(), params).await?;
        let res = extract_applied(res);
        if res {
            self.status = to;
        } else {
            // get the current status
            self.get_one(db, vec!["status".to_string()]).await?;
        }
        Ok(res)
    }

    pub async fn prepare(
        &mut self,
        db: &scylladb::ScyllaDB,
        mac: &HMacTag,
        payee: xid::Id,
        kind: TransactionKind,
        amount: i64,
    ) -> anyhow::Result<()> {
        if amount <= 0 {
            return Err(HTTPError::new(
                400,
                format!(
                    "Invalid amount {} for {} transaction",
                    amount,
                    kind.as_ref()
                ),
            )
            .into());
        }

        kind.check_payer(self.uid)?;
        kind.check_payee(payee)?;
        if let Some(id) = self.sub_payee {
            kind.check_sub_payee(id)?;
            if id == payee || id == SYS_ID || id == self.uid {
                return Err(HTTPError::new(
                    400,
                    format!("Invalid sub_payee {} for {} transaction", id, kind.as_ref()),
                )
                .into());
            }
        }

        let mut payer_wallet = Wallet::with_pk(self.uid);
        payer_wallet.get_one(db).await?;
        payer_wallet.verify_checksum(mac)?;

        let (sys_fee, sub_shares) =
            kind.fee_and_shares(amount, payer_wallet.credits, self.sub_payee.is_some());
        kind.sub_payer_balance(&mut payer_wallet, amount)?;

        self.id = xid::new();
        self.sequence = payer_wallet.sequence;
        self.payee = payee;
        self.status = 0;
        self.kind = kind.as_ref().to_string();
        self.amount = amount;
        self.sys_fee = sys_fee;
        self.sub_shares = sub_shares;

        let fields = Self::fields();
        self._fields = fields.iter().map(|f| f.to_string()).collect();
        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut insert_params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

        for field in &fields {
            let val = cols.get(field).unwrap();
            if val == &CqlValue::Empty {
                continue;
            }

            cols_name.push(field);
            vals_name.push("?");
            insert_params.push(val);
        }

        let insert_query = format!(
            "INSERT INTO transaction ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(","),
        );

        // can not use: BATCH with conditions cannot span multiple tables
        let res = db.execute(insert_query, insert_params).await?;
        if extract_applied(res) {
            payer_wallet.next_checksum(mac, self.id);
            let res = payer_wallet.update_balance(db).await?;
            if res {
                self.set_status(db, 0, 1).await?;
                return Ok(());
            }
        }

        self.delete(db).await?;
        Err(HTTPError::new(
            429,
            format!("Failed to prepare {} transaction", kind.as_ref()),
        )
        .into())
    }

    // do it after prepared.
    pub async fn cancel(&mut self, db: &scylladb::ScyllaDB, mac: &HMacTag) -> anyhow::Result<()> {
        let kind = TransactionKind::from_str(&self.kind)?;
        if self.status != 1 {
            return Err(HTTPError::new(
                429,
                format!("Invalid status {} for canceling transaction", self.status),
            )
            .into());
        }
        if self.amount <= 0 {
            return Err(HTTPError::new(
                429,
                format!("Invalid amount {} for canceling transaction", self.amount),
            )
            .into());
        }

        let ok = self.set_status(db, 1, -1).await?;
        if !ok {
            if self.status < 0 {
                // canceling or canceled
                return Ok(());
            }

            return Err(HTTPError::new(
                500,
                format!("Invalid status {} for canceling transaction", self.status),
            )
            .into());
        }

        let mut ok = false;
        let mut payer_wallet = Wallet::with_pk(self.uid);
        for _ in 0..5 {
            payer_wallet.get_one(db).await?;
            payer_wallet.verify_checksum(mac)?;
            kind.rollback_payer_balance(&mut payer_wallet, self.amount)?;
            payer_wallet.next_checksum(mac, self.id);
            ok = payer_wallet.update_balance(db).await?;
            if ok {
                break;
            }
        }

        if ok {
            self.set_status(db, -1, -2).await?;
            return Ok(());
        }

        log::error!(target: "scylladb",
            action = "cancel_transaction",
            uid = self.uid.to_string(),
            id = self.id.to_string(),
            wallet = payer_wallet.uid.to_string();
            "payer_wallet canceling failed",
        );

        Err(HTTPError::new(
            500,
            format!("canceling transaction failed: {}, {}", self.uid, self.id),
        )
        .into())
    }

    // do it after prepared.
    pub async fn commit(&mut self, db: &scylladb::ScyllaDB, mac: &HMacTag) -> anyhow::Result<()> {
        let kind = TransactionKind::from_str(&self.kind)?;
        kind.check_payee(self.payee)?;

        if self.sub_shares > 0 && self.sub_payee.is_none() {
            panic!("No sub_payee with sub_shares");
        }

        let ok = self.set_status(db, 1, 2).await?;
        if !ok {
            if self.status == 3 {
                // already committed
                return Ok(());
            }

            return Err(HTTPError::new(
                500,
                format!("Invalid status {} for committing transaction", self.status),
            )
            .into());
        }

        let mut payee_wallet = Wallet::with_pk(self.payee);
        let res = payee_wallet.get_one(db).await;
        if res.is_err() {
            // create payee wallet if not exists
            let res = payee_wallet.save(db).await?;
            log::info!(target: "scylladb",
                action = "create_wallet",
                uid = payee_wallet.uid.to_string(),
                txn_uid = self.uid.to_string(),
                txn_id = self.id.to_string(),
                txn_kind = self.kind,
                result = res;
                "",
            );
        }

        let payee_wallet_is_sys = payee_wallet.is_system();
        let fut_payee: BoxFuture<'_, anyhow::Result<()>> = async {
            let mut ok = false;
            for _ in 0..5 {
                payee_wallet.verify_checksum(mac)?;
                kind.add_payee_balance(
                    &mut payee_wallet,
                    self.amount - self.sys_fee - self.sub_shares,
                )?;
                if payee_wallet.is_system() {
                    payee_wallet.income += self.sys_fee;
                }
                payee_wallet.next_checksum(mac, self.id);
                ok = payee_wallet.update_balance(db).await?;
                if ok {
                    break;
                }
                payee_wallet.get_one(db).await?;
            }

            if !ok {
                log::error!(target: "scylladb",
                    action = "commit_transaction",
                    uid = self.uid.to_string(),
                    id = self.id.to_string(),
                    wallet = payee_wallet.uid.to_string();
                    "payee_wallet committing failed",
                );
                return Err(anyhow!(
                    "payee_wallet committing failed, {}",
                    payee_wallet.uid.to_string()
                ));
            }
            Ok(())
        }
        .boxed();

        let fut_sys: BoxFuture<'_, anyhow::Result<()>> = async {
            if self.sys_fee > 0 && !payee_wallet_is_sys {
                let mut ok = false;
                let mut sys_wallet = Wallet::with_pk(SYS_ID);
                for _ in 0..5 {
                    sys_wallet.get_one(db).await?;
                    sys_wallet.verify_checksum(mac)?;
                    sys_wallet.income += self.sys_fee;
                    sys_wallet.next_checksum(mac, self.id);

                    ok = sys_wallet.update_balance(db).await?;
                    if ok {
                        break;
                    }
                }

                if !ok {
                    log::error!(target: "scylladb",
                        action = "commit_transaction",
                        uid = self.uid.to_string(),
                        id = self.id.to_string(),
                        wallet = sys_wallet.uid.to_string();
                        "sys_wallet committing failed",
                    );
                    return Err(anyhow!(
                        "sys_wallet committing failed, {}",
                        sys_wallet.uid.to_string()
                    ));
                }
            }
            Ok(())
        }
        .boxed();

        let fut_sub: BoxFuture<'_, anyhow::Result<()>> = async {
            if self.sub_shares > 0 {
                let mut ok = false;
                let mut sub_wallet = Wallet::with_pk(self.sub_payee.unwrap());
                let res = sub_wallet.get_one(db).await;
                if res.is_err() {
                    // create payee wallet if not exists
                    let res = sub_wallet.save(db).await?;
                    log::info!(target: "scylladb",
                        action = "create_wallet",
                        uid = sub_wallet.uid.to_string(),
                        txn_uid = self.uid.to_string(),
                        txn_id = self.id.to_string(),
                        txn_kind = self.kind,
                        result = res;
                        "",
                    );
                }

                for _ in 0..5 {
                    sub_wallet.verify_checksum(mac)?;
                    sub_wallet.income += self.sub_shares;
                    sub_wallet.next_checksum(mac, self.id);

                    ok = sub_wallet.update_balance(db).await?;
                    if ok {
                        break;
                    }
                    sub_wallet.get_one(db).await?;
                }

                if !ok {
                    log::error!(target: "scylladb",
                        action = "commit_transaction",
                        uid = self.uid.to_string(),
                        id = self.id.to_string(),
                        wallet = sub_wallet.uid.to_string();
                        "sub_wallet committing failed",
                    );
                    return Err(anyhow!(
                        "sub_wallet committing failed, {}",
                        sub_wallet.uid.to_string()
                    ));
                }
            }
            Ok(())
        }
        .boxed();

        let (a, b, c) = join!(fut_payee, fut_sys, fut_sub);
        let mut errs: Vec<String> = Vec::new();
        if a.is_err() {
            errs.push(a.unwrap_err().to_string());
        }
        if b.is_err() {
            errs.push(b.unwrap_err().to_string());
        }
        if c.is_err() {
            errs.push(c.unwrap_err().to_string());
        }

        if errs.is_empty() {
            self.set_status(db, 2, 3).await?;
            return Ok(());
        }

        Err(HTTPError::new(
            500,
            format!("committing transaction partly applied, errors: {:?}", errs),
        )
        .into())
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        kind: Option<TransactionKind>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if kind.is_none() {
                let query = format!(
                    "SELECT {} FROM transaction WHERE uid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                );
                let params = (uid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_iter(query, params).await?
            } else {
                let query = format!(
                    "SELECT {} FROM transaction WHERE uid=? AND kind=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
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
                "SELECT {} FROM transaction WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM transaction WHERE uid=? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
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

    pub async fn list_by_payee(
        db: &scylladb::ScyllaDB,
        payee: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        kind: Option<TransactionKind>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if kind.is_none() {
                let query = format!(
                    "SELECT {} FROM transaction WHERE payee=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                );
                let params = (payee.to_cql(), id.to_cql(), page_size as i32);
                db.execute_iter(query, params).await?
            } else {
                let query = format!(
                    "SELECT {} FROM transaction WHERE payee=? AND id<? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","));
                let params = (
                    payee.to_cql(),
                    id.to_cql(),
                    kind.unwrap().to_string(),
                    page_size as i32,
                );
                db.execute_iter(query, params).await?
            }
        } else if kind.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM transaction WHERE payee=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (payee.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM transaction WHERE payee=? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (
                payee.as_bytes(),
                kind.unwrap().to_string(),
                page_size as i32,
            );
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

    pub async fn list_by_sub_payee(
        db: &scylladb::ScyllaDB,
        sub_payee: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        kind: Option<TransactionKind>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let rows = if let Some(id) = page_token {
            if kind.is_none() {
                let query = format!(
                    "SELECT {} FROM transaction WHERE sub_payee=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                );
                let params = (sub_payee.to_cql(), id.to_cql(), page_size as i32);
                db.execute_iter(query, params).await?
            } else {
                let query = format!(
                    "SELECT {} FROM transaction WHERE sub_payee=? AND id<? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","));
                let params = (
                    sub_payee.to_cql(),
                    id.to_cql(),
                    kind.unwrap().to_string(),
                    page_size as i32,
                );
                db.execute_iter(query, params).await?
            }
        } else if kind.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM transaction WHERE sub_payee=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (sub_payee.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM transaction WHERE sub_payee=? AND kind=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (
                sub_payee.as_bytes(),
                kind.unwrap().to_string(),
                page_size as i32,
            );
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
    fn transaction_kind_works() {
        {
            assert_eq!("award", TransactionKind::Award.as_ref());
            assert_eq!("topup", TransactionKind::Topup.as_ref());
            assert_eq!("spend", TransactionKind::Spend.as_ref());
            assert_eq!("sponsor", TransactionKind::Sponsor.as_ref());
            assert_eq!("subscribe", TransactionKind::Subscribe.as_ref());
            assert_eq!("withdraw", TransactionKind::Withdraw.as_ref());
            assert_eq!("refund", TransactionKind::Refund.as_ref());
            assert_eq!(
                TransactionKind::Award,
                TransactionKind::from_str("award").unwrap()
            );
            assert_eq!(
                TransactionKind::Refund,
                TransactionKind::from_str("refund").unwrap()
            );
        }

        let uid = xid::new();
        // check_payer
        {
            assert!(TransactionKind::Award.check_payer(SYS_ID).is_ok());
            assert!(TransactionKind::Topup.check_payer(SYS_ID).is_ok());

            assert!(TransactionKind::Award.check_payer(uid).is_err());
            assert!(TransactionKind::Topup.check_payer(uid).is_err());

            assert!(TransactionKind::Spend.check_payer(uid).is_ok());
            assert!(TransactionKind::Sponsor.check_payer(uid).is_ok());
            assert!(TransactionKind::Subscribe.check_payer(uid).is_ok());
            assert!(TransactionKind::Withdraw.check_payer(uid).is_ok());
            assert!(TransactionKind::Refund.check_payer(uid).is_ok());

            assert!(TransactionKind::Spend.check_payer(SYS_ID).is_err());
            assert!(TransactionKind::Sponsor.check_payer(SYS_ID).is_err());
            assert!(TransactionKind::Subscribe.check_payer(SYS_ID).is_err());
            assert!(TransactionKind::Withdraw.check_payer(SYS_ID).is_err());
            assert!(TransactionKind::Refund.check_payer(SYS_ID).is_err());
        }

        // check_payee
        {
            assert!(TransactionKind::Spend.check_payee(SYS_ID).is_ok());
            assert!(TransactionKind::Withdraw.check_payee(SYS_ID).is_ok());
            assert!(TransactionKind::Refund.check_payee(SYS_ID).is_ok());

            assert!(TransactionKind::Spend.check_payee(uid).is_err());
            assert!(TransactionKind::Withdraw.check_payee(uid).is_err());
            assert!(TransactionKind::Refund.check_payee(uid).is_err());

            assert!(TransactionKind::Award.check_payee(uid).is_ok());
            assert!(TransactionKind::Topup.check_payee(uid).is_ok());
            assert!(TransactionKind::Sponsor.check_payee(uid).is_ok());
            assert!(TransactionKind::Subscribe.check_payee(uid).is_ok());

            assert!(TransactionKind::Award.check_payee(SYS_ID).is_err());
            assert!(TransactionKind::Topup.check_payee(SYS_ID).is_err());
            assert!(TransactionKind::Sponsor.check_payee(SYS_ID).is_err());
            assert!(TransactionKind::Subscribe.check_payee(SYS_ID).is_err());
        }

        // check_sub_payee
        {
            assert!(TransactionKind::Award.check_sub_payee(uid).is_err());
            assert!(TransactionKind::Topup.check_sub_payee(uid).is_err());
            assert!(TransactionKind::Spend.check_sub_payee(uid).is_err());
            assert!(TransactionKind::Sponsor.check_sub_payee(uid).is_ok());
            assert!(TransactionKind::Subscribe.check_sub_payee(uid).is_ok());
            assert!(TransactionKind::Withdraw.check_sub_payee(uid).is_err());
            assert!(TransactionKind::Refund.check_sub_payee(uid).is_err());
        }

        // sub_payer_balance
        {
            // system wallet
            let mut sys_wallet = Wallet::with_pk(SYS_ID);
            assert!(TransactionKind::Award
                .sub_payer_balance(&mut sys_wallet, 100)
                .is_ok());
            assert_eq!(-100, sys_wallet.award);
            assert!(TransactionKind::Award
                .sub_payer_balance(&mut sys_wallet, 100)
                .is_ok());
            assert_eq!(-200, sys_wallet.award);
            assert!(TransactionKind::Topup
                .sub_payer_balance(&mut sys_wallet, 100)
                .is_ok());
            assert_eq!(-100, sys_wallet.topup);
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut sys_wallet, 100)
                .is_err());
            assert_eq!(-300, sys_wallet.balance());

            // user wallet without credits
            let mut wallet = Wallet::with_pk(xid::new());
            wallet.award = 100;
            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 100)
                .is_err());

            // user wallet with credits
            wallet.credits = 10;
            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 100)
                .is_ok());
            assert_eq!(0, wallet.balance());
            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 1000)
                .is_err());

            wallet.award = 100;
            wallet.topup = 100;
            wallet.income = 100;
            assert_eq!(300, wallet.balance());

            assert!(TransactionKind::Refund
                .sub_payer_balance(&mut wallet, 200)
                .is_err());
            assert!(TransactionKind::Withdraw
                .sub_payer_balance(&mut wallet, 200)
                .is_err());

            assert!(TransactionKind::Refund
                .sub_payer_balance(&mut wallet, 50)
                .is_ok());
            assert_eq!(50, wallet.topup);

            assert!(TransactionKind::Withdraw
                .sub_payer_balance(&mut wallet, 50)
                .is_ok());
            assert_eq!(50, wallet.income);

            assert_eq!(200, wallet.balance());
            assert!(TransactionKind::Award
                .sub_payer_balance(&mut wallet, 110)
                .is_err());
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 110)
                .is_ok());
            assert_eq!(90, wallet.balance());
            assert_eq!(40, wallet.topup);
            assert_eq!(50, wallet.income);

            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 50)
                .is_ok());
            assert_eq!(40, wallet.balance());
            assert_eq!(0, wallet.topup);
            assert_eq!(40, wallet.income);

            assert!(TransactionKind::Refund
                .sub_payer_balance(&mut wallet, 40)
                .is_err());
            assert!(TransactionKind::Withdraw
                .sub_payer_balance(&mut wallet, 50)
                .is_err());
            assert!(TransactionKind::Withdraw
                .sub_payer_balance(&mut wallet, 40)
                .is_ok());
            assert_eq!(0, wallet.balance());

            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 50)
                .is_err());

            wallet.award = 10;
            wallet.topup = 10;
            wallet.income = 10;
            assert!(TransactionKind::Sponsor
                .sub_payer_balance(&mut wallet, 50)
                .is_err());
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 50)
                .is_ok());
            assert_eq!(-20, wallet.balance());
            assert_eq!(0, wallet.award);
            assert_eq!(-20, wallet.topup);
            assert_eq!(0, wallet.income);

            wallet.award = 20;
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 110)
                .is_err());

            wallet.award = 30;
            assert!(TransactionKind::Subscribe
                .sub_payer_balance(&mut wallet, 110)
                .is_err());
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 110)
                .is_ok());
            assert_eq!(-100, wallet.balance());
            assert_eq!(0, wallet.award);
            assert_eq!(-100, wallet.topup);
            assert_eq!(0, wallet.income);

            wallet.topup = 10;
            assert_eq!(10, wallet.balance());
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 120)
                .is_err());
            assert!(TransactionKind::Spend
                .sub_payer_balance(&mut wallet, 110)
                .is_ok());
            assert_eq!(-100, wallet.balance());
            assert_eq!(0, wallet.award);
            assert_eq!(-100, wallet.topup);
            assert_eq!(0, wallet.income);
        }

        // rollback_payer_balance
        {
            let mut wallet = Wallet::with_pk(xid::new());
            assert!(TransactionKind::Award
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.award);
            assert!(TransactionKind::Topup
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.topup);
            assert!(TransactionKind::Refund
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(2, wallet.topup);
            assert!(TransactionKind::Withdraw
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.income);

            assert!(TransactionKind::Spend
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(3, wallet.topup);
            assert!(TransactionKind::Sponsor
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(4, wallet.topup);
            assert!(TransactionKind::Subscribe
                .rollback_payer_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(5, wallet.topup);
        }

        // add_payee_balance
        {
            let mut wallet = Wallet::with_pk(xid::new());
            assert!(TransactionKind::Award
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.award);
            assert!(TransactionKind::Award
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(2, wallet.award);

            assert!(TransactionKind::Topup
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.topup);
            assert!(TransactionKind::Refund
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(2, wallet.topup);
            assert!(TransactionKind::Withdraw
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(3, wallet.topup);

            assert!(TransactionKind::Spend
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(1, wallet.income);
            assert!(TransactionKind::Sponsor
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(2, wallet.income);
            assert!(TransactionKind::Subscribe
                .add_payee_balance(&mut wallet, 1)
                .is_ok());
            assert_eq!(3, wallet.income);
        }

        // fee_and_shares
        {
            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Withdraw.fee_and_shares(1, 0, false)
            );
            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Withdraw.fee_and_shares(1000, 0, false)
            );
            assert_eq!(
                (10i64, 0i64),
                TransactionKind::Withdraw.fee_and_shares(10000, 10000, false)
            );

            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(1, 0, false)
            );
            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(1, 10000, false)
            );
            assert_eq!(
                (30i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(100, 9999, false)
            );
            assert_eq!(
                (27i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(100, 10000, false)
            );
            assert_eq!(
                (24i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(100, 100000, false)
            );
            assert_eq!(
                (15i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(100, 100000000, false)
            );
            assert_eq!(
                (15i64, 0i64),
                TransactionKind::Sponsor.fee_and_shares(101, 100000000, false)
            );

            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Subscribe.fee_and_shares(1, 0, true)
            );
            assert_eq!(
                (1i64, 0i64),
                TransactionKind::Subscribe.fee_and_shares(1, 10000, true)
            );
            assert_eq!(
                (30i64, 35i64),
                TransactionKind::Sponsor.fee_and_shares(100, 9999, true)
            );
            assert_eq!(
                (27i64, 36i64),
                TransactionKind::Subscribe.fee_and_shares(100, 10000, true)
            );
            assert_eq!(
                (24i64, 38i64),
                TransactionKind::Subscribe.fee_and_shares(100, 100000, true)
            );
            assert_eq!(
                (15i64, 42i64),
                TransactionKind::Subscribe.fee_and_shares(100, 100000000, true)
            );
            assert_eq!(
                (15i64, 43i64),
                TransactionKind::Subscribe.fee_and_shares(101, 100000000, true)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn transaction_model_works() {
        let db = get_db().await;
        let mac = HMacTag::new([1u8; 32]);
        let payee = xid::new();
        let mut sys_wallet: Wallet = Default::default();
        // make sure system wallet exists.
        {
            let mut wallet: Wallet = Default::default();
            wallet.save(&db).await.unwrap();
        }

        // invalid args
        {
            let mut txn: Transaction = Default::default();
            let res = txn
                .prepare(&db, &mac, payee, TransactionKind::Award, -1)
                .await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid amount"));
            let res = txn
                .prepare(&db, &mac, payee, TransactionKind::Award, 0)
                .await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid amount"));

            txn.sub_payee = Some(payee);
            let res = txn
                .prepare(&db, &mac, payee, TransactionKind::Award, 1)
                .await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid sub_payee"));

            txn.sub_payee = Some(xid::new());
            let res = txn
                .prepare(&db, &mac, payee, TransactionKind::Award, 1)
                .await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid sub_payee"));
        }

        // prepare and commit
        {
            let mut payee_wallet = Wallet::with_pk(payee);
            assert!(payee_wallet.get_one(&db).await.is_err());

            sys_wallet.get_one(&db).await.unwrap();
            sys_wallet.verify_checksum(&mac).unwrap();
            let prev_balance = sys_wallet.balance();
            let prev_amount = sys_wallet.award;

            let mut txn: Transaction = Default::default();
            txn.prepare(&db, &mac, payee, TransactionKind::Award, 100)
                .await
                .unwrap();
            assert_eq!(1, txn.status);
            assert!(txn.credits().is_empty());

            sys_wallet.get_one(&db).await.unwrap();
            sys_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(prev_balance - 100, sys_wallet.balance());
            assert_eq!(prev_amount - 100, sys_wallet.award);
            assert_eq!(txn.sequence + 1, sys_wallet.sequence);
            assert_eq!(txn.id, sys_wallet.txn);
            assert!(!sys_wallet.txn.is_zero());

            assert_eq!(payee, txn.payee);
            assert_eq!(1, txn.status);
            assert_eq!("award", txn.kind);
            assert_eq!(100, txn.amount);
            assert_eq!(0, txn.sys_fee);
            assert_eq!(0, txn.sub_shares);

            assert!(payee_wallet.get_one(&db).await.is_err());

            txn.commit(&db, &mac).await.unwrap();
            assert_eq!(3, txn.status);
            assert!(txn.credits().is_empty());
            assert!(payee_wallet.get_one(&db).await.is_ok());
            assert_eq!(100, payee_wallet.award);
            assert_eq!(100, payee_wallet.balance());
            assert_eq!(1, payee_wallet.sequence);
        }

        // prepare and cancel
        {
            let mut payer_wallet = Wallet::with_pk(xid::new());
            assert!(payer_wallet.get_one(&db).await.is_err());

            sys_wallet.get_one(&db).await.unwrap();
            sys_wallet.verify_checksum(&mac).unwrap();

            let mut txn: Transaction = Transaction::with_uid(SYS_ID);
            txn.prepare(&db, &mac, payer_wallet.uid, TransactionKind::Award, 1000)
                .await
                .unwrap();
            txn.commit(&db, &mac).await.unwrap();
            assert!(payer_wallet.get_one(&db).await.is_ok());
            assert_eq!(1000, payer_wallet.award);
            assert_eq!(1000, payer_wallet.balance());
            assert_eq!(1, payer_wallet.sequence);

            let mut txn: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn.kind = "award".to_string();
            let res = txn.cancel(&db, &mac).await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid status 0"));

            txn.status = 1;
            let res = txn.cancel(&db, &mac).await;
            assert!(res.is_err());
            assert!(res.unwrap_err().to_string().contains("Invalid amount 0"));

            let mut txn: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn.prepare(&db, &mac, SYS_ID, TransactionKind::Spend, 400)
                .await
                .unwrap();

            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(600, payer_wallet.award);
            assert_eq!(0, payer_wallet.topup);
            assert_eq!(2, payer_wallet.sequence);
            assert_eq!(txn.id, payer_wallet.txn);
            assert_eq!(1, txn.status);

            txn.cancel(&db, &mac).await.unwrap();
            assert_eq!(-2, txn.status);

            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(600, payer_wallet.award);
            assert_eq!(400, payer_wallet.topup);
            assert_eq!(3, payer_wallet.sequence);
            assert_eq!(txn.id, payer_wallet.txn);

            txn.get_one(&db, vec![]).await.unwrap();
            assert_eq!(-2, txn.status);

            let mut txn: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn.prepare(&db, &mac, SYS_ID, TransactionKind::Spend, 100)
                .await
                .unwrap();
            txn.commit(&db, &mac).await.unwrap();
            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(500, payer_wallet.award);
            assert_eq!(400, payer_wallet.topup);
            assert_eq!(4, payer_wallet.sequence);
            assert_eq!(txn.id, payer_wallet.txn);
            assert_eq!(3, txn.status);

            let mut txn1: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn1.prepare(&db, &mac, SYS_ID, TransactionKind::Spend, 600)
                .await
                .unwrap();
            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(0, payer_wallet.award);
            assert_eq!(300, payer_wallet.topup);
            assert_eq!(5, payer_wallet.sequence);
            assert_eq!(txn1.id, payer_wallet.txn);
            assert_eq!(1, txn1.status);

            let mut txn2: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn2.prepare(&db, &mac, SYS_ID, TransactionKind::Spend, 100)
                .await
                .unwrap();
            txn2.commit(&db, &mac).await.unwrap();
            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(0, payer_wallet.award);
            assert_eq!(200, payer_wallet.topup);
            assert_eq!(6, payer_wallet.sequence);
            assert_eq!(txn2.id, payer_wallet.txn);
            assert_eq!(3, txn2.status);

            txn1.cancel(&db, &mac).await.unwrap();
            assert_eq!(-2, txn1.status);

            payer_wallet.get_one(&db).await.unwrap();
            payer_wallet.verify_checksum(&mac).unwrap();
            assert_eq!(0, payer_wallet.award);
            assert_eq!(800, payer_wallet.topup);
            assert_eq!(7, payer_wallet.sequence);
            assert_eq!(txn1.id, payer_wallet.txn);

            txn1.get_one(&db, vec![]).await.unwrap();
            assert_eq!(-2, txn1.status);
        }

        // commit and credits
        {
            let mut payer_wallet = Wallet::with_pk(xid::new());
            let mut payee_wallet = Wallet::with_pk(xid::new());
            let mut sub_payee_wallet = Wallet::with_pk(xid::new());
            assert!(payer_wallet.get_one(&db).await.is_err());
            assert!(payee_wallet.get_one(&db).await.is_err());
            assert!(sub_payee_wallet.get_one(&db).await.is_err());

            sys_wallet.get_one(&db).await.unwrap();
            sys_wallet.verify_checksum(&mac).unwrap();

            let mut txn: Transaction = Default::default();
            txn.prepare(&db, &mac, payer_wallet.uid, TransactionKind::Award, 1000)
                .await
                .unwrap();
            txn.commit(&db, &mac).await.unwrap();
            assert!(txn.credits().is_empty());

            let mut credit = Credit::with_pk(payer_wallet.uid, txn.id);
            credit.kind = CreditKind::Award.to_string();
            credit.amount = 10; // will be ignored.
            credit.save(&db).await.unwrap();

            assert!(payer_wallet.get_one(&db).await.is_ok());
            assert_eq!(1000, payer_wallet.award);
            assert_eq!(1000, payer_wallet.balance());
            assert_eq!(10, payer_wallet.credits);
            assert_eq!(1, payer_wallet.sequence);

            let mut txn: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn.prepare(&db, &mac, payee_wallet.uid, TransactionKind::Sponsor, 100)
                .await
                .unwrap();
            txn.commit(&db, &mac).await.unwrap();

            let mut credits = txn.credits();
            assert_eq!(2, credits.len());
            assert_eq!(100, credits[0].amount);
            assert_eq!("payout", credits[0].kind);
            assert_eq!(70, credits[1].amount);
            assert_eq!("income", credits[1].kind);
            Credit::save_all(&db, &mut credits).await.unwrap();

            assert!(payer_wallet.get_one(&db).await.is_ok());
            assert_eq!(900, payer_wallet.award);
            assert_eq!(900, payer_wallet.balance());
            assert_eq!(110, payer_wallet.credits);
            assert_eq!(2, payer_wallet.sequence);

            assert!(payee_wallet.get_one(&db).await.is_ok());
            assert_eq!(70, payee_wallet.income);
            assert_eq!(70, payee_wallet.balance());
            assert_eq!(0, payee_wallet.credits);
            assert_eq!(1, payee_wallet.sequence);

            let mut credit = Credit::with_pk(payee_wallet.uid, txn.id);
            credit.kind = CreditKind::Award.to_string();
            credit.amount = 10;
            credit.save(&db).await.unwrap();
            assert!(payee_wallet.get_one(&db).await.is_ok());
            assert_eq!(70, payee_wallet.income);
            assert_eq!(70, payee_wallet.balance());
            assert_eq!(10, payee_wallet.credits);
            assert_eq!(1, payee_wallet.sequence);

            let mut txn: Transaction = Transaction::with_uid(payer_wallet.uid);
            txn.sub_payee = Some(sub_payee_wallet.uid);
            txn.prepare(&db, &mac, payee_wallet.uid, TransactionKind::Subscribe, 200)
                .await
                .unwrap();
            txn.commit(&db, &mac).await.unwrap();

            let mut credit = Credit::with_pk(sub_payee_wallet.uid, xid::new());
            credit.kind = CreditKind::Award.to_string();
            credit.amount = 1; // will be ignored.
            credit.save(&db).await.unwrap();

            let mut credits = txn.credits();
            assert_eq!(3, credits.len());
            assert_eq!(200, credits[0].amount);
            assert_eq!("payout", credits[0].kind);
            assert_eq!(70, credits[1].amount);
            assert_eq!("income", credits[1].kind);
            assert_eq!(70, credits[2].amount);
            assert_eq!("income", credits[2].kind);
            Credit::save_all(&db, &mut credits).await.unwrap();

            assert!(payer_wallet.get_one(&db).await.is_ok());
            assert_eq!(700, payer_wallet.award);
            assert_eq!(700, payer_wallet.balance());
            assert_eq!(310, payer_wallet.credits);
            assert_eq!(3, payer_wallet.sequence);

            assert!(payee_wallet.get_one(&db).await.is_ok());
            assert_eq!(140, payee_wallet.income);
            assert_eq!(140, payee_wallet.balance());
            assert_eq!(80, payee_wallet.credits);
            assert_eq!(2, payee_wallet.sequence);

            assert!(sub_payee_wallet.get_one(&db).await.is_ok());
            assert_eq!(70, sub_payee_wallet.income);
            assert_eq!(70, sub_payee_wallet.balance());
            assert_eq!(71, sub_payee_wallet.credits);
            assert_eq!(1, sub_payee_wallet.sequence);
        }
    }
}
