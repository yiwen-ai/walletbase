use hmac::{Hmac, Mac};
use sha3::Sha3_256;
use subtle::ConstantTimeEq;

use axum_web::erring::HTTPError;
use scylla_orm::{ColumnsMap, CqlValue, ToCqlVal};
use scylla_orm_macros::CqlOrm;

use crate::db::scylladb::{self, extract_applied};

pub const SYS_ID: xid::Id = xid::Id([0u8; 12]);
pub const SYS_FEE_RATE: f32 = 0.001; // 1%

#[derive(Debug, Default, Clone, CqlOrm)]
pub struct Wallet {
    pub uid: xid::Id,
    pub sequence: i64,
    pub award: i64,
    pub topup: i64,
    pub income: i64,
    pub credits: i64,
    pub txn: xid::Id,
    pub checksum: Vec<u8>,

    pub _fields: Vec<String>, // selected fields，`_` 前缀字段会被 CqlOrm 忽略
}

pub fn income_fee_rate(credits: i64) -> f32 {
    match credits {
        ..=9999 => 0.3,
        10000..=99999 => 0.27,             // LV4
        100000..=999999 => 0.24,           // LV5
        1000000..=9999999 => 0.21,         // LV6
        10000000..=99999999 => 0.18,       // LV7
        100000000..=999999999 => 0.15,     // LV8
        1000000000..=9999999999 => 0.12,   // LV9
        10000000000..=99999999999 => 0.09, // LV10
        _ => 0.09,
    }
}

impl Wallet {
    pub fn with_pk(uid: xid::Id) -> Self {
        Self {
            uid,
            ..Default::default()
        }
    }

    pub fn is_system(&self) -> bool {
        self.uid.is_zero()
    }

    pub fn balance(&self) -> i64 {
        self.award + self.topup + self.income
    }

    pub fn verify_checksum(&self, mac: &HMacTag) -> anyhow::Result<()> {
        if self.sequence == 0 {
            return Ok(());
        }
        let tag = mac.tag64(self);
        if tag.ct_eq(&self.checksum).unwrap_u8() != 1 {
            return Err(
                HTTPError::new(400, format!("wallet {} checksum mismatch", self.uid)).into(),
            );
        }
        Ok(())
    }

    pub fn next_checksum(&mut self, mac: &HMacTag, txn: xid::Id) {
        self.sequence += 1;
        self.txn = txn;
        self.checksum = mac.tag64(self);
    }

    pub async fn get_one(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<()> {
        let fields = Self::fields();
        self._fields = fields.clone();

        let query = format!(
            "SELECT {} FROM wallet WHERE uid=? LIMIT 1",
            fields.join(",")
        );
        let params = (self.uid.to_cql(),);
        let res = db.execute(query, params).await?.single_row()?;

        let mut cols = ColumnsMap::with_capacity(fields.len());
        cols.fill(res, &fields)?;
        self.fill(&cols);

        Ok(())
    }

    // should be call after next_checksum
    pub async fn update_balance(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
        let query = "UPDATE wallet SET sequence=?,award=?,topup=?,income=?,txn=?,checksum=? WHERE uid=? IF sequence=?";
        let params = (
            self.sequence,
            self.award,
            self.topup,
            self.income,
            self.txn.to_cql(),
            self.checksum.to_cql(),
            self.uid.to_cql(),
            self.sequence - 1,
        );

        let res = db.execute(query.to_string(), params).await?;
        Ok(extract_applied(res))
    }

    pub async fn save(&mut self, db: &scylladb::ScyllaDB) -> anyhow::Result<bool> {
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
            "INSERT INTO wallet ({}) VALUES ({}) IF NOT EXISTS",
            cols_name.join(","),
            vals_name.join(",")
        );

        let res = db.execute(query, params).await?;
        Ok(extract_applied(res))
    }
}

pub struct HMacTag {
    hmac: Hmac<Sha3_256>,
}

impl HMacTag {
    pub fn new(key: [u8; 32]) -> Self {
        let hmac: Hmac<Sha3_256> = Hmac::new_from_slice(&key).unwrap();
        HMacTag { hmac }
    }

    // HMAC(uid, sequence, award, balance_charge, income, balance_ywd, updated_by)
    pub fn tag64(&self, wallet: &Wallet) -> Vec<u8> {
        let digest = self
            .hmac
            .clone()
            .chain_update(wallet.uid.as_bytes())
            .chain_update(wallet.sequence.to_be_bytes())
            .chain_update(wallet.award.to_be_bytes())
            .chain_update(wallet.topup.to_be_bytes())
            .chain_update(wallet.income.to_be_bytes())
            .chain_update(wallet.txn.as_bytes())
            .finalize()
            .into_bytes();

        let mut tag: Vec<u8> = Vec::with_capacity(8);
        tag.extend_from_slice(&digest[..8]);
        tag
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
    fn income_fee_rate_works() {
        assert_eq!(0.3f32, income_fee_rate(-1));
        assert_eq!(0.3f32, income_fee_rate(0));
        assert_eq!(0.3f32, income_fee_rate(9999));
        assert_eq!(0.27f32, income_fee_rate(9999 + 1));
        assert_eq!(0.27f32, income_fee_rate(99999));
        assert_eq!(0.24f32, income_fee_rate(99999 + 1));
        assert_eq!(0.24f32, income_fee_rate(999999));
        assert_eq!(0.21f32, income_fee_rate(999999 + 1));
        assert_eq!(0.21f32, income_fee_rate(9999999));
        assert_eq!(0.18f32, income_fee_rate(9999999 + 1));
        assert_eq!(0.18f32, income_fee_rate(99999999));
        assert_eq!(0.15f32, income_fee_rate(99999999 + 1));
        assert_eq!(0.15f32, income_fee_rate(999999999));
        assert_eq!(0.12f32, income_fee_rate(999999999 + 1));
        assert_eq!(0.12f32, income_fee_rate(9999999999));
        assert_eq!(0.09f32, income_fee_rate(9999999999 + 1));
        assert_eq!(0.09f32, income_fee_rate(99999999999));
        assert_eq!(0.09f32, income_fee_rate(99999999999 + 1));
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn wallet_model_works() {
        let db = get_db().await;

        let mac = HMacTag::new([1u8; 32]);
        let mut wallet: Wallet = Default::default();

        assert!(wallet.is_system());
        assert_eq!(0, wallet.balance());
        assert!(wallet.verify_checksum(&mac).is_ok());

        wallet.save(&db).await.unwrap();
        wallet.get_one(&db).await.unwrap();

        assert!(wallet.verify_checksum(&mac).is_ok());

        let txn = xid::new();
        wallet.award -= 100;
        wallet.topup -= 100;
        wallet.next_checksum(&mac, txn);
        assert!(wallet.verify_checksum(&mac).is_ok());

        wallet.uid = xid::new();
        assert!(wallet.verify_checksum(&mac).is_err());
    }
}
