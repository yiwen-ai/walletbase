use axum_web::{context::unix_ms, erring::HTTPError};
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;
use std::collections::HashSet;

use crate::db::scylladb::{self, extract_applied};

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Customer {
    pub uid: xid::Id,
    pub provider: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub customer: String,
    pub payload: Vec<u8>,
    pub customers: HashSet<String>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

impl Customer {
    pub fn with_pk(uid: xid::Id, provider: String) -> Self {
        Self {
            uid,
            provider,
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
        let field = "customer".to_string();
        if !select_fields.contains(&field) {
            select_fields.push(field);
        }
        if with_pk {
            let field = "uid".to_string();
            if !select_fields.contains(&field) {
                select_fields.push(field);
            }
            let field = "provider".to_string();
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
            "SELECT {} FROM customer WHERE uid=? AND provider=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(), self.provider.to_cql());
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    pub async fn upsert(
        &mut self,
        db: &scylladb::ScyllaDB,
        customer: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<bool> {
        if self
            .get_one(db, vec!["customer".to_string()])
            .await
            .is_err()
        {
            self.created_at = unix_ms() as i64;
            self.updated_at = self.created_at;
            self.customer = customer.clone();
            self.payload = payload.clone();

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
                "INSERT INTO customer ({}) VALUES ({}) IF NOT EXISTS",
                cols_name.join(","),
                vals_name.join(",")
            );

            let res = db.execute(query, params).await?;
            if extract_applied(res) {
                return Ok(true);
            }

            // data exists, we try to update it
            self.get_one(db, vec!["customer".to_string()]).await?;
        }

        if self.customer == customer {
            return Ok(false);
        }

        let new_updated_at = unix_ms() as i64;
        let query = "UPDATE customer SET updated_at=?,customer=?,payload=?,customers=customers+{?} WHERE uid=? AND provider=? IF customer=?";
        let params = (
            new_updated_at,
            customer.to_cql(),
            payload.to_cql(),
            self.customer.to_cql(),
            self.uid.to_cql(),
            self.provider.to_cql(),
            self.customer.to_cql(),
        );

        let res = db.execute(query, params).await?;
        if !extract_applied(res) {
            return Err(HTTPError::new(
                409,
                "Customer update failed, please try again".to_string(),
            )
            .into());
        }

        self._fields.push("updated_at".to_string());
        self.updated_at = new_updated_at;
        Ok(true)
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

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn customer_model_works() {
        let db = get_db().await;
        let uid = xid::new();
        let provider = "stripe".to_string();

        let mut customer = Customer::with_pk(uid, provider.clone());
        let res = customer.get_one(&db, vec![]).await;
        assert!(res.is_err());
        let err: HTTPError = res.unwrap_err().into();
        assert_eq!(err.code, 404);

        let res = customer
            .upsert(&db, "cus_123".to_string(), vec![0xa0])
            .await
            .unwrap();
        assert!(res);

        customer.get_one(&db, vec![]).await.unwrap();
        assert!(customer.created_at > 0);
        assert_eq!(customer.created_at, customer.updated_at);
        assert_eq!(customer.customer, "cus_123");
        assert_eq!(customer.payload, vec![0xa0]);
        assert_eq!(customer.customers.len(), 0);

        let mut c2 = Customer::with_pk(uid, provider);

        let res = c2
            .upsert(
                &db,
                "cus_456".to_string(),
                vec![0xa2, 0x01, 0x02, 0x03, 0x04],
            )
            .await
            .unwrap();
        assert!(res);

        c2.get_one(&db, vec![]).await.unwrap();
        assert!(c2.updated_at > customer.updated_at);
        assert_eq!(c2.customer, "cus_456");
        assert_eq!(c2.payload, vec![0xa2, 0x01, 0x02, 0x03, 0x04]);
        assert_eq!(c2.customers.len(), 1);
        assert!(c2.customers.contains("cus_123"));
    }
}
