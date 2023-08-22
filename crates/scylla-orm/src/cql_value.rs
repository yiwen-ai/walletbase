use scylla::cql_to_rust;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

pub use scylla::cql_to_rust::FromCqlValError;
pub use scylla::frame::response::result::CqlValue;

pub trait FromCqlVal: Sized {
    // Required method
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError>;
}

pub trait ToCqlVal: Sized {
    fn to_cql(&self) -> CqlValue;
}

impl FromCqlVal for String {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for String {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Text(self.to_owned())
    }
}

impl FromCqlVal for i8 {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for i8 {
    fn to_cql(&self) -> CqlValue {
        CqlValue::TinyInt(self.to_owned())
    }
}

impl FromCqlVal for i16 {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for i16 {
    fn to_cql(&self) -> CqlValue {
        CqlValue::SmallInt(self.to_owned())
    }
}

impl FromCqlVal for i32 {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for i32 {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Int(self.to_owned())
    }
}

impl FromCqlVal for i64 {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for i64 {
    fn to_cql(&self) -> CqlValue {
        CqlValue::BigInt(self.to_owned())
    }
}

impl FromCqlVal for f32 {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for f32 {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Float(self.to_owned())
    }
}

impl FromCqlVal for xid::Id {
    fn from_cql(val: &CqlValue) -> Result<Self, FromCqlValError> {
        match val {
            CqlValue::Blob(val) => {
                if val.len() != 12 {
                    Err(FromCqlValError::BadVal)
                } else {
                    let mut bytes = [0u8; 12];
                    bytes.copy_from_slice(val);
                    Ok(xid::Id(bytes))
                }
            }
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl ToCqlVal for xid::Id {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Blob(self.as_bytes().to_vec())
    }
}

impl FromCqlVal for Option<xid::Id> {
    fn from_cql(val: &CqlValue) -> Result<Self, FromCqlValError> {
        match val {
            CqlValue::Blob(val) => {
                if val.len() != 12 {
                    Err(FromCqlValError::BadVal)
                } else {
                    let mut bytes = [0u8; 12];
                    bytes.copy_from_slice(val);
                    Ok(Some(xid::Id(bytes)))
                }
            }
            CqlValue::Empty => Ok(None),
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl ToCqlVal for Option<xid::Id> {
    fn to_cql(&self) -> CqlValue {
        match self {
            Some(id) => CqlValue::Blob(id.as_bytes().to_vec()),
            None => CqlValue::Empty,
        }
    }
}

impl FromCqlVal for isolang::Language {
    fn from_cql(val: &CqlValue) -> Result<Self, FromCqlValError> {
        match val {
            CqlValue::Text(val) => match isolang::Language::from_str(val) {
                Ok(lang) => Ok(lang),
                Err(_) => Err(FromCqlValError::BadVal),
            },
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl ToCqlVal for isolang::Language {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Text(self.to_639_3().to_string())
    }
}

impl FromCqlVal for Vec<u8> {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for Vec<u8> {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Blob(self.to_owned())
    }
}

impl FromCqlVal for uuid::Uuid {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        cql_to_rust::FromCqlVal::from_cql(cql_val.to_owned())
    }
}

impl ToCqlVal for uuid::Uuid {
    fn to_cql(&self) -> CqlValue {
        CqlValue::Uuid(self.to_owned())
    }
}

impl FromCqlVal for CqlValue {
    fn from_cql(val: &CqlValue) -> Result<Self, FromCqlValError> {
        Ok(val.to_owned())
    }
}

impl ToCqlVal for CqlValue {
    fn to_cql(&self) -> CqlValue {
        self.to_owned()
    }
}

impl<T: FromCqlVal> FromCqlVal for Vec<T> {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::List(list) => {
                let mut rt = Vec::with_capacity(list.len());
                for item in list {
                    rt.push(T::from_cql(item)?);
                }
                Ok(rt)
            }
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl<T: ToCqlVal> ToCqlVal for Vec<T> {
    fn to_cql(&self) -> CqlValue {
        let mut rt: Vec<CqlValue> = Vec::with_capacity(self.len());
        for item in self {
            rt.push(item.to_cql());
        }
        CqlValue::List(rt)
    }
}

impl<T: FromCqlVal + std::cmp::Eq + std::hash::Hash> FromCqlVal for HashSet<T> {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Set(list) => {
                let mut rt = HashSet::with_capacity(list.len());
                for item in list {
                    rt.insert(T::from_cql(item)?);
                }
                Ok(rt)
            }
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl<T: ToCqlVal> ToCqlVal for HashSet<T> {
    fn to_cql(&self) -> CqlValue {
        let mut rt: Vec<CqlValue> = Vec::with_capacity(self.len());
        for item in self {
            rt.push(item.to_cql());
        }
        CqlValue::Set(rt)
    }
}

impl<T: FromCqlVal> FromCqlVal for HashMap<String, T> {
    fn from_cql(cql_val: &CqlValue) -> Result<Self, FromCqlValError> {
        match cql_val {
            CqlValue::Map(list) => {
                let mut rt = HashMap::with_capacity(list.len());
                for item in list {
                    rt.insert(String::from_cql(&item.0)?, T::from_cql(&item.1)?);
                }
                Ok(rt)
            }
            _ => Err(FromCqlValError::BadCqlType),
        }
    }
}

impl<T: ToCqlVal> ToCqlVal for HashMap<String, T> {
    fn to_cql(&self) -> CqlValue {
        let mut rt: Vec<(CqlValue, CqlValue)> = Vec::with_capacity(self.len());
        for item in self {
            rt.push((item.0.to_cql(), item.1.to_cql()));
        }
        CqlValue::Map(rt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_cql_val_works() {
        assert_eq!(
            "hello".to_string().to_cql(),
            CqlValue::Text("hello".to_string())
        );
    }
}
