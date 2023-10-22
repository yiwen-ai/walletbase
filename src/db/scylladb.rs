use futures::{stream::StreamExt, Stream};
use scylla::{
    frame::value::{BatchValues, ValueList},
    statement::{Consistency, SerialConsistency},
    transport::{iterator::RowIterator, query_result::QueryResult, Compression, ExecutionProfile},
    CachingSession, Metrics, Session, SessionBuilder,
};
use std::{sync::Arc, time::Duration};

pub use scylla::{
    batch::Batch,
    frame::response::result::{ColumnType, Row},
    query::Query,
    Bytes,
};

use crate::conf;

pub struct ScyllaDB {
    session: CachingSession,
}

impl ScyllaDB {
    pub async fn new(cfg: conf::ScyllaDB, keyspace: &str) -> anyhow::Result<Self> {
        // use tls https://github.com/scylladb/scylla-rust-driver/blob/main/examples/tls.rs

        let handle = ExecutionProfile::builder()
            .consistency(Consistency::Quorum)
            .serial_consistency(Some(SerialConsistency::Serial))
            .request_timeout(Some(Duration::from_secs(5)))
            .build()
            .into_handle();

        let session: Session = SessionBuilder::new()
            .known_nodes(&cfg.nodes)
            .user(cfg.username, cfg.password)
            .compression(Some(Compression::Lz4))
            .default_execution_profile_handle(handle)
            .build()
            .await?;

        if !keyspace.is_empty() {
            session.use_keyspace(keyspace, false).await?;
        }

        Ok(Self {
            session: CachingSession::from(session, 100000),
        })
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.session.get_session().get_metrics()
    }

    pub async fn execute(
        &self,
        query: impl Into<Query>,
        params: impl ValueList,
    ) -> anyhow::Result<QueryResult> {
        let res = self.session.execute(query, params).await?;
        Ok(res)
    }

    pub async fn execute_iter(
        &self,
        query: impl Into<Query>,
        params: impl ValueList,
    ) -> anyhow::Result<Vec<Row>> {
        let mut rows_stream = self.session.execute_iter(query, params).await?;

        let (capacity, _) = rows_stream.size_hint();
        let mut rows: Vec<Row> = Vec::with_capacity(capacity);
        while let Some(next_row) = rows_stream.next().await {
            rows.push(next_row?);
        }
        Ok(rows)
    }

    pub async fn stream(
        &self,
        query: impl Into<Query>,
        params: impl ValueList,
    ) -> anyhow::Result<RowIterator> {
        let stream = self.session.execute_iter(query, params).await?;
        Ok(stream)
    }

    // https://opensource.docs.scylladb.com/master/cql/dml.html#batch-statement
    // BATCH operations are only isolated within a single partition.
    // BATCH with conditions cannot span multiple tables
    pub async fn batch(
        &self,
        statements: Vec<&str>,
        values: impl BatchValues,
    ) -> anyhow::Result<QueryResult> {
        let mut batch: Batch = Default::default();
        for statement in statements {
            batch.append_statement(statement);
        }
        let res = self.session.batch(&batch, values).await?;
        Ok(res)
    }
}

pub fn extract_applied(res: QueryResult) -> bool {
    let res = res
        .single_row()
        .map(|r| r.columns[0].as_ref().and_then(|r| r.as_boolean()))
        .unwrap_or(Some(false));
    res == Some(true)
}

pub async fn exec_cqls(db: &ScyllaDB, cqls: &str) -> anyhow::Result<()> {
    let lines = cqls.lines();
    let mut cql = String::new();
    let mut cqls: Vec<String> = Vec::new();
    for line in lines {
        let line = line.split("--").collect::<Vec<&str>>()[0].trim();
        if line.is_empty() {
            continue;
        }
        cql.push(' ');
        cql.push_str(line);
        if cql.ends_with(';') {
            cqls.push(cql.trim().trim_end_matches(';').to_string());
            cql.clear();
        }
    }

    for cql in cqls {
        let res = db
            .execute(cql.clone(), &[])
            .await
            .map_err(|err| anyhow::anyhow!("\ncql: {}\nerror: {}", &cql, &err));
        if let Err(err) = res {
            if err.to_string().contains("Index already exists") {
                println!("WARN: {}", err);
            } else {
                return Err(err);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conf;
    use crate::db;
    use tokio::sync::OnceCell;

    static DB: OnceCell<db::scylladb::ScyllaDB> = OnceCell::const_new();

    async fn get_db() -> &'static db::scylladb::ScyllaDB {
        DB.get_or_init(|| async {
            let cfg = conf::Conf::new().unwrap_or_else(|err| panic!("config error: {}", err));
            let res = db::scylladb::ScyllaDB::new(cfg.scylla, "").await;
            res.unwrap()
        })
        .await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn exec_cqls_works() {
        let db = get_db().await;

        let schema = std::include_str!("../../cql/schema_keyspace_test.cql");
        exec_cqls(db, schema).await.unwrap();

        let schema = std::include_str!("../../cql/schema_table.cql");
        exec_cqls(db, schema).await.unwrap();
    }
}
