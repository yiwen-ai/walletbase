use axum_web::{context::unix_ms, erring::HTTPError};
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::scylladb::{self, extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Topup {
    pub uid: xid::Id,
    pub id: xid::Id,
    pub status: i8,
    pub updated_at: i64,
    pub expire_at: i64,
    pub quantity: i64,
    pub currency: String,
    pub amount: i64,
    pub amount_refunded: i64,
    pub exchange: i64,
    pub provider: String,
    pub charge_id: String,
    pub charge_payload: Vec<u8>,
    pub txn: Option<xid::Id>,
    pub txn_refunded: Option<xid::Id>,
    pub failure_code: String,
    pub failure_msg: String,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Topup {
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
            "SELECT {} FROM topup WHERE uid=? AND id=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.id.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    async fn set_status(
        &mut self,
        db: &scylladb::ScyllaDB,
        from: i8,
        to: i8,
    ) -> anyhow::Result<bool> {
        let query = "UPDATE topup SET status=? WHERE uid=? AND id=? IF status=?";
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

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        if self.status != 0 && self.status != 1 {
            return Err(HTTPError::new(400, format!("Invalid status {}", self.status)).into());
        }

        self.id = xid::new();
        self.updated_at = unix_ms() as i64;
        self.expire_at = self.updated_at + 3600 * 1000;

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
            "INSERT INTO topup ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
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

        let rows = if let Some(id) = page_token {
            if status.is_none() {
                let query = format!(
                    "SELECT {} FROM topup WHERE uid=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(",")
                );
                let params = (uid.to_cql(), id.to_cql(), page_size as i32);
                db.execute_iter(query, params).await?
            } else {
                let query = format!(
                    "SELECT {} FROM topup WHERE uid=? AND status=? AND id<? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                    fields.clone().join(","));
                let params = (uid.to_cql(), id.to_cql(), status.unwrap(), page_size as i32);
                db.execute_iter(query, params).await?
            }
        } else if status.is_none() {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM topup WHERE uid=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.to_cql(), page_size as i32);
            db.execute_iter(query, params).await?
        } else {
            let query = scylladb::Query::new(format!(
                "SELECT {} FROM topup WHERE uid=? AND status=? LIMIT ? BYPASS CACHE USING TIMEOUT 3s",
                fields.clone().join(",")
            ))
            .with_page_size(page_size as i32);
            let params = (uid.as_bytes(), status.unwrap(), page_size as i32);
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
