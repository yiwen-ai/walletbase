use futures::stream::StreamExt;
use scylla_orm::{ColumnsMap, ToCqlVal};
use structured_logger::{async_json::new_writer, Builder};
use tokio::io;
use walletbase::{conf, db};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    Builder::with_level("debug")
        .with_target_writer("*", new_writer(io::stdout()))
        .init();

    let nodes = std::env::var("SCYLLA_NODES").expect(
        "env SCYLLA_NODES required:\nSCYLLA_NODES=127.0.0.1:9042 ./sync-to-payee-transaction",
    );

    let cfg = conf::ScyllaDB {
        nodes: nodes.split(',').map(|s| s.to_string()).collect(),
        username: "".to_string(),
        password: "".to_string(),
    };

    let sess = db::scylladb::ScyllaDB::new(cfg, "walletbase").await?;
    let fields = vec![
        "uid".to_string(),
        "id".to_string(),
        "payee".to_string(),
        "sub_payee".to_string(),
        "status".to_string(),
    ];
    let query = format!("SELECT {} FROM transaction", fields.join(","));
    let mut stream = sess.stream(query, ()).await?;
    let mut total: usize = 0;
    let mut synced: usize = 0;

    while let Some(row) = stream.next().await {
        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(row?, &fields)?;
        let mut doc = db::Transaction::default();
        doc.fill(&cols);
        total += 1;

        if doc.status == 3 {
            let ok = db::PayeeTransaction::new(doc.payee, doc.id, doc.uid)
                .save(&sess)
                .await?;
            if ok {
                synced += 1;
            }
            if let Some(sub_payee) = doc.sub_payee {
                let ok = db::PayeeTransaction::new(sub_payee, doc.id, doc.uid)
                    .save(&sess)
                    .await?;
                if ok {
                    synced += 1;
                }
            }
        }
    }

    println!("total: {}, synced: {}", total, synced);

    Ok(())
}
