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
}

impl FromStr for Currency {
    type Err = HTTPError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for currency in CURRENCIES.iter() {
            if currency.alpha == s.to_ascii_uppercase() {
                return Ok(currency.clone());
            }
        }
        Err(HTTPError::new(400, format!("Invalid currency: {}", s)))
    }
}

// https://en.wikipedia.org/wiki/Template:Most_traded_currencies
// https://www.iban.com/currency-codes
// https://github.com/yiwen-ai/countries
pub const CURRENCIES: [Currency; 12] = [
    Currency {
        name: "港幣",
        alpha: "HKD",
        decimals: 2,
        code: 344,
    },
    Currency {
        name: "US Dollar",
        alpha: "USD",
        decimals: 2,
        code: 840,
    },
    Currency {
        name: "人民币",
        alpha: "CNY",
        decimals: 2,
        code: 156,
    },
    Currency {
        name: "Euro",
        alpha: "EUR",
        decimals: 2,
        code: 978,
    },
    Currency {
        name: "日本円",
        alpha: "JPY",
        decimals: 0,
        code: 392,
    },
    Currency {
        name: "Pound Sterling",
        alpha: "GBP",
        decimals: 2,
        code: 826,
    },
    Currency {
        name: "Canadian Dollar",
        alpha: "CAD",
        decimals: 2,
        code: 124,
    },
    Currency {
        name: "Singapore Dollar",
        alpha: "SGD",
        decimals: 2,
        code: 702,
    },
    Currency {
        name: "Australian Dollar",
        alpha: "AUD",
        decimals: 2,
        code: 36,
    },
    Currency {
        name: "درهم إماراتي",
        alpha: "AED",
        decimals: 2,
        code: 784,
    },
    Currency {
        name: "원",
        alpha: "KRW",
        decimals: 0,
        code: 410,
    },
    Currency {
        name: "рубль",
        alpha: "RUB",
        decimals: 2,
        code: 643,
    },
];

pub async fn currencies(
    to: PackObject<()>,
    State(_app): State<Arc<AppState>>,
) -> Result<PackObject<SuccessResponse<Vec<Currency>>>, HTTPError> {
    Ok(to.with(SuccessResponse::new(CURRENCIES.into())))
}
