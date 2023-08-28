use std::str::FromStr;

use axum::extract::State;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use axum_web::erring::{HTTPError, SuccessResponse};
use axum_web::object::PackObject;

use crate::api::AppState;

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Currency {
    pub name: &'static str,
    pub alpha: &'static str,
    pub decimals: u8, // 0..3
    pub code: u16,
    pub min_amount: u32, // min change amount
    pub max_amount: u32, // max change amount
}

impl FromStr for Currency {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for currency in CURRENCIES.iter() {
            if currency.alpha == s.to_ascii_uppercase() {
                return Ok(currency.clone());
            }
        }
        Err(anyhow::anyhow!("Invalid currency: {}", s))
    }
}

impl Currency {
    pub fn valid_amount(&self, amount: i64) -> Result<(), anyhow::Error> {
        if amount < self.min_amount as i64 || amount > self.max_amount as i64 {
            return Err(anyhow::anyhow!(
                "Invalid amount {} ({}..{}) for {}",
                amount,
                self.min_amount,
                self.max_amount,
                self.name
            ));
        }
        Ok(())
    }
}

// https://www.iban.com/currency-codes
// https://github.com/yiwen-ai/countries
pub const CURRENCIES: [Currency; 12] = [
    Currency {
        name: "港幣",
        alpha: "HKD",
        decimals: 2,
        code: 344,
        min_amount: 1000, // stripe: $4.00
        max_amount: 1000000,
    },
    Currency {
        name: "US Dollar",
        alpha: "USD",
        decimals: 2,
        code: 840,
        min_amount: 200, // stripe: $0.50
        max_amount: 200000,
    },
    Currency {
        name: "人民币",
        alpha: "CNY",
        decimals: 2,
        code: 156,
        min_amount: 1000, // stripe: -
        max_amount: 1000000,
    },
    Currency {
        name: "Euro",
        alpha: "EUR",
        decimals: 2,
        code: 978,
        min_amount: 200, // stripe: €0.50
        max_amount: 200000,
    },
    Currency {
        name: "日本円",
        alpha: "JPY",
        decimals: 0,
        code: 392,
        min_amount: 200, // stripe: ¥50
        max_amount: 200000,
    },
    Currency {
        name: "Pound Sterling",
        alpha: "GBP",
        decimals: 2,
        code: 826,
        min_amount: 100, // stripe: £0.30
        max_amount: 100000,
    },
    Currency {
        name: "Canadian Dollar",
        alpha: "CAD",
        decimals: 2,
        code: 124,
        min_amount: 200, // stripe: $0.50
        max_amount: 200000,
    },
    Currency {
        name: "Singapore Dollar",
        alpha: "SGD",
        decimals: 2,
        code: 702,
        min_amount: 200, // stripe: $0.50
        max_amount: 200000,
    },
    Currency {
        name: "Australian Dollar",
        alpha: "AUD",
        decimals: 2,
        code: 36,
        min_amount: 200, // stripe: $0.50
        max_amount: 200000,
    },
    Currency {
        name: "درهم إماراتي",
        alpha: "AED",
        decimals: 2,
        code: 784,
        min_amount: 500, // stripe: 2.00 د.إ
        max_amount: 500000,
    },
    Currency {
        name: "원",
        alpha: "KRW",
        decimals: 0,
        code: 410,
        min_amount: 2000, // stripe: -
        max_amount: 2000000,
    },
    Currency {
        name: "рубль",
        alpha: "RUB",
        decimals: 2,
        code: 643,
        min_amount: 10000, // stripe: -
        max_amount: 10000000,
    },
];

pub async fn currencies(
    to: PackObject<()>,
    State(_app): State<Arc<AppState>>,
) -> Result<PackObject<SuccessResponse<Vec<Currency>>>, HTTPError> {
    Ok(to.with(SuccessResponse::new(CURRENCIES.into())))
}
