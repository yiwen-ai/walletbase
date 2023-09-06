use axum::{middleware, routing, Router};
use std::{fs, sync::Arc};
use tower::ServiceBuilder;
use tower_http::{
    catch_panic::CatchPanicLayer,
    compression::{predicate::SizeAbove, CompressionLayer},
};

use axum_web::context;
use axum_web::encoding;

use crate::api;
use crate::conf;
use crate::crypto;
use crate::db;

pub async fn new(cfg: conf::Conf) -> anyhow::Result<(Arc<api::AppState>, Router)> {
    let app_state = Arc::new(new_app_state(cfg).await?);

    let mds = ServiceBuilder::new()
        .layer(CatchPanicLayer::new())
        .layer(middleware::from_fn(context::middleware))
        .layer(CompressionLayer::new().compress_when(SizeAbove::new(encoding::MIN_ENCODING_SIZE)));

    let app = Router::new()
        .route("/", routing::get(api::version))
        .route("/healthz", routing::get(api::healthz))
        .route("/currencies", routing::get(api::currency::currencies))
        .nest(
            "/v1/wallet",
            Router::new()
                .route("/", routing::get(api::wallet::get))
                .route("/award", routing::post(api::wallet::award))
                .route("/expend", routing::post(api::wallet::expend))
                .route("/sponsor", routing::post(api::wallet::sponsor))
                .route("/subscribe", routing::post(api::wallet::subscribe)),
        )
        .nest(
            "/v1/charge",
            Router::new()
                .route(
                    "/",
                    routing::post(api::charge::create)
                        .get(api::charge::get)
                        .patch(api::charge::update),
                )
                .route("/list", routing::post(api::charge::list))
                // .route("/refund", routing::post(api::charge::refund))
                .route("/complete", routing::post(api::charge::complete)),
        )
        .nest(
            "/v1/transaction",
            Router::new()
                .route("/", routing::get(api::transaction::get))
                .route("/list_outgo", routing::post(api::transaction::list_outgo))
                .route("/list_income", routing::post(api::transaction::list_income))
                .route("/list_shares", routing::post(api::transaction::list_shares))
                .route("/commit", routing::post(api::transaction::commit))
                .route("/cancel", routing::post(api::transaction::cancel)),
        )
        .nest(
            "/v1/customer",
            Router::new().route(
                "/",
                routing::post(api::customer::upsert).get(api::customer::get),
            ),
        )
        .route_layer(mds)
        .with_state(app_state.clone());

    Ok((app_state, app))
}

async fn new_app_state(cfg: conf::Conf) -> anyhow::Result<api::AppState> {
    let aad = cfg.keys.aad.as_bytes();

    let decryptor = {
        // Should use KMS on production.
        let mkek = std::env::var("YIWEN_MKEK")
            .unwrap_or("YiWenAI-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-LLc".to_string()); // default to test key
        let mkek = crypto::base64url_decode(&mkek)?;
        let decryptor = crypto::Encrypt0::new(mkek.try_into().unwrap(), b"");

        let kek = read_key(&decryptor, aad, &cfg.keys.kek)?;
        crypto::Encrypt0::new(kek.get_private()?, b"")
    };

    let mac = {
        let wallet_key = read_key(
            &decryptor,
            aad,
            &fs::read_to_string(cfg.keys.wallet_key_file)?,
        )?;
        db::HMacTag::new(wallet_key.get_private()?)
    };

    let keyspace = if cfg.env == "test" {
        "walletbase_test"
    } else {
        "walletbase"
    };
    let scylla = db::scylladb::ScyllaDB::new(cfg.scylla, keyspace).await?;

    Ok(api::AppState {
        scylla: Arc::new(scylla),
        mac: Arc::new(mac),
    })
}

fn read_key(
    decryptor: &crypto::Encrypt0,
    aad: &[u8],
    ciphertext: &str,
) -> anyhow::Result<crypto::Key> {
    let key = crypto::base64url_decode(ciphertext.trim())?;
    let key = decryptor.decrypt(crypto::unwrap_cbor_tag(&key), aad)?;
    crypto::Key::from_slice(&key)
}
