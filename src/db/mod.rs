mod model_charge;
mod model_credit;
mod model_customer;
mod model_transaction;
mod model_wallet;

pub mod scylladb;

pub use model_charge::Charge;
pub use model_credit::{Credit, CreditKind};
pub use model_customer::Customer;
pub use model_transaction::{Transaction, TransactionKind, PayeeTransaction};
pub use model_wallet::{income_fee_rate, HMacTag, Wallet, SYS_FEE_RATE, SYS_ID};

pub static MAX_ID: xid::Id = xid::Id([255; 12]);
pub static MIN_ID: xid::Id = xid::Id([0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255]);
