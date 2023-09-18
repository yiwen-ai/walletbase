use axum_web::{context::unix_ms, erring::HTTPError};
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use super::MAX_ID;
use crate::db::scylladb::{self, extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Charge {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub status: i8,
    pub updated_at: i64,
    pub expire_at: i64,
    pub quantity: i64,
    pub currency: String,
    pub amount: i64,
    pub amount_refunded: i64,
    pub provider: String,
    pub charge_id: String,
    pub charge_payload: Vec<u8>,
    pub txn: Option<xid::Id>,
    pub txn_refunded: Option<xid::Id>,
    pub failure_code: String,
    pub failure_msg: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Charge {
    pub fn with_pk(uid: xid::Id, id: xid::Id) -> Self {
        Self {
            uid,
            id,
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
        let field = "quantity".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "provider".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        let field = "expire_at".to_string();
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

    pub async fn get_one(
        &mut self,
        db: &scylladb::ScyllaDB,
        select_fields: Vec<String>,
    ) -> anyhow::Result<()> {
        let fields = Self::select_fields(select_fields, false)?;
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM charge WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn set_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        from: i8,
        to: i8,
    ) -> anyhow::Result<bool> {
        let query = "UPDATE charge SET status=? WHERE uid=? AND id=? IF status=?";
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

    pub async fn update(
        &mut self,
        db: &scylladb::ScyllaDB,
        cols: ColumnsMap,
        status: i8,
    ) -> anyhow::Result<bool> {
        let valid_fields = [
            "status",
            "currency",
            "amount",
            "amount_refunded",
            "charge_id",
            "charge_payload",
            "txn",
            "txn_refunded",
            "failure_code",
            "failure_msg",
        ];
        let update_fields = cols.keys();
        for field in &update_fields {
            if !valid_fields.contains(&field.as_str()) {
                return Err(HTTPError::new(400, format!("Invalid field: {}", field)).into());
            }
        }

        self.get_one(db, vec!["status".to_string()]).await?;
        if self.status != status {
            return Err(HTTPError::new(
                409,
                format!(
                    "Charge status conflict, expected {}, got {}",
                    self.status, status
                ),
            )
            .into());
        }

        let mut set_fields: Vec<String> = Vec::with_capacity(update_fields.len() + 1);
        let mut params: Vec<CqlValue> = Vec::with_capacity(update_fields.len() + 1 + 3);

        let new_updated_at = unix_ms() as i64;
        set_fields.push("updated_at=?".to_string());
        params.push(new_updated_at.to_cql());

        for field in &update_fields {
            set_fields.push(format!("{}=?", field));
            params.push(cols.get(field).unwrap().to_owned());
        }

        let query = format!(
            "UPDATE charge SET {} WHERE uid=? AND id=? IF status=?",
            set_fields.join(",")
        );
        params.push(self.uid.to_cql());
        params.push(self.id.to_cql());
        params.push(status.to_cql());

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Charge update failed, please try again".to_string()).into(),
            );
        }

        self.fill(&cols); // fill for meilisearch update
        self.updated_at = new_updated_at;
        Ok(true)
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        if self.status != 0 && self.status != 1 {
            return Err(HTTPError::new(400, format!("Invalid status {}", self.status)).into());
        }

        self.id = xid::new();
        self.updated_at = unix_ms() as i64;
        self.expire_at = self.updated_at + 24 * 3600 * 1000;

        let fields = Self::fields();
        self._fields = fields.clone();

        let mut cols_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut vals_name: Vec<&str> = Vec::with_capacity(fields.len());
        let mut params: Vec<&CqlValue> = Vec::with_capacity(fields.len());
        let cols = self.to();

        for field in &fields {
            cols_name.push(field);
            vals_name.push("?");
            params.push(cols.get(field).unwrap());
        }

        let query = format!(
            "INSERT INTO charge ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(
                HTTPError::new(409, "Charge save failed, please try again".to_string()).into(),
            );
        }

        Ok(true)
    }

    pub async fn list(
        db: &scylladb::ScyllaDB,
        uid: xid::Id,
        select_fields: Vec<String>,
        page_size: u16,
        page_token: Option<xid::Id>,
        status: Option<i8>,
    ) -> anyhow::Result<Vec<Self>> {
        let fields = Self::select_fields(select_fields, true)?;

        let token = match page_token {
            Some(id) => id,
            None => MAX_ID,
        };

        let rows = if status.is_none() {
            let query = format!(
                "SELECT {} FROM charge WHERE uid=? AND id<? LIMIT ? USING TIMEOUT 3s",
                fields.clone().join(",")
            );
            let params = (uid.to_cql(), token.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = format!(
                "SELECT {} FROM charge WHERE uid=? AND status=? AND id<? LIMIT ? USING TIMEOUT 3s",
                fields.clone().join(",")
            );
            let params = (
                uid.to_cql(),
                status.unwrap(),
                token.to_cql(),
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
