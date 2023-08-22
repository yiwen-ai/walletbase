use async_trait::async_trait;
use axum::{
    body::HttpBody,
    extract::{FromRequest, FromRequestParts},
    http::{
        header::{self, HeaderMap, HeaderValue},
        request::{Parts, Request},
        StatusCode,
    },
    response::{IntoResponse, Response},
    BoxError,
};
use base64::{engine::general_purpose, Engine as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{
    de::{self, DeserializeOwned},
    ser::Serializer,
    Deserialize, Serialize,
};
use std::{collections::HashSet, error::Error, fmt, ops::Deref, str::FromStr};

use crate::{encoding::Encoding, erring::HTTPError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackObject<T> {
    Json(T),
    Cbor(T),
}

impl<S> PackObject<S> {
    pub fn unwrap(self) -> S {
        match self {
            PackObject::Json(v) => v,
            PackObject::Cbor(v) => v,
        }
    }

    pub fn unwrap_ref(&self) -> &S {
        match self {
            PackObject::Json(v) => v,
            PackObject::Cbor(v) => v,
        }
    }

    pub fn unpack(self) -> (PackObject<()>, S) {
        match self {
            PackObject::Json(v) => (PackObject::Json(()), v),
            PackObject::Cbor(v) => (PackObject::Cbor(()), v),
        }
    }

    pub fn unit(&self) -> PackObject<()> {
        match self {
            PackObject::Json(_) => PackObject::Json(()),
            PackObject::Cbor(_) => PackObject::Cbor(()),
        }
    }

    pub fn with<T>(&self, v: T) -> PackObject<T> {
        match self {
            PackObject::Json(_) => PackObject::Json(v),
            PackObject::Cbor(_) => PackObject::Cbor(v),
        }
    }

    pub fn with_option<T>(&self, v: Option<T>) -> Option<PackObject<T>> {
        match self {
            PackObject::Json(_) => v.map(PackObject::Json),
            PackObject::Cbor(_) => v.map(PackObject::Cbor),
        }
    }

    pub fn with_vec<T>(&self, vv: Vec<T>) -> Vec<PackObject<T>> {
        match self {
            PackObject::Json(_) => vv.into_iter().map(PackObject::Json).collect(),
            PackObject::Cbor(_) => vv.into_iter().map(PackObject::Cbor).collect(),
        }
    }

    pub fn with_set<T>(&self, vv: HashSet<T>) -> Vec<PackObject<T>> {
        match self {
            PackObject::Json(_) => vv.into_iter().map(PackObject::Json).collect(),
            PackObject::Cbor(_) => vv.into_iter().map(PackObject::Cbor).collect(),
        }
    }
}

impl<T: Default> Default for PackObject<T> {
    fn default() -> Self {
        PackObject::Json(T::default())
    }
}

impl<T> AsRef<T> for PackObject<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        match self {
            PackObject::Json(ref v) => v,
            PackObject::Cbor(ref v) => v,
        }
    }
}

impl<T> Deref for PackObject<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            PackObject::Json(ref v) => v,
            PackObject::Cbor(ref v) => v,
        }
    }
}

impl Serialize for PackObject<()> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_unit()
    }
}

impl Serialize for PackObject<&[u8]> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PackObject::Json(v) => {
                serializer.serialize_str(general_purpose::URL_SAFE_NO_PAD.encode(v).as_str())
            }
            PackObject::Cbor(v) => serializer.serialize_bytes(v),
        }
    }
}

impl Serialize for PackObject<Vec<u8>> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PackObject::Json(v) => {
                serializer.serialize_str(general_purpose::URL_SAFE_NO_PAD.encode(v).as_str())
            }
            PackObject::Cbor(v) => serializer.serialize_bytes(v),
        }
    }
}

impl Serialize for PackObject<xid::Id> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PackObject::Json(v) => serializer.serialize_str(v.to_string().as_str()),
            PackObject::Cbor(v) => serializer.serialize_bytes(v.as_bytes()),
        }
    }
}

impl Serialize for PackObject<isolang::Language> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PackObject::Json(v) => {
                serializer.serialize_str(v.to_autonym().unwrap_or_else(|| v.to_name()))
            }
            PackObject::Cbor(v) => serializer.serialize_str(v.to_639_3()),
        }
    }
}

impl Serialize for PackObject<uuid::Uuid> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PackObject::Json(v) => serializer.serialize_str(v.to_string().as_str()),
            PackObject::Cbor(v) => serializer.serialize_bytes(v.as_bytes()),
        }
    }
}

struct PackObjectBytesVisitor;

impl<'de> de::Visitor<'de> for PackObjectBytesVisitor {
    type Value = PackObject<Vec<u8>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a byte array or a no pad base64url string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(PackObject::Cbor(v.to_vec()))
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(PackObject::Cbor(v.to_vec()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(PackObject::Cbor(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(PackObject::Json(v))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(PackObject::Json(v))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let v = general_purpose::URL_SAFE_NO_PAD
            .decode(v)
            .map_err(de::Error::custom)?;
        Ok(PackObject::Json(v))
    }
}

impl<'de> Deserialize<'de> for PackObject<Vec<u8>> {
    fn deserialize<D>(deserializer: D) -> Result<PackObject<Vec<u8>>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(PackObjectBytesVisitor)
    }
}

struct PackObjectLanguageVisitor;

impl<'de> de::Visitor<'de> for PackObjectLanguageVisitor {
    type Value = PackObject<isolang::Language>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a ISO 639-1, ISO 639-3 or English language name string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(v.to_ascii_lowercase().as_str())
            .map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(PackObject::Cbor(id)),
            _ => Ok(PackObject::Json(id)),
        }
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(v.to_ascii_lowercase().as_str())
            .map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(PackObject::Cbor(id)),
            _ => Ok(PackObject::Json(id)),
        }
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = isolang::Language::from_str(v.to_ascii_lowercase().as_str())
            .map_err(de::Error::custom)?;
        match v.len() {
            3 => Ok(PackObject::Cbor(id)),
            _ => Ok(PackObject::Json(id)),
        }
    }
}

impl<'de> Deserialize<'de> for PackObject<isolang::Language> {
    fn deserialize<D>(deserializer: D) -> Result<PackObject<isolang::Language>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_str(PackObjectLanguageVisitor)
    }
}

struct PackObjectXidVisitor;

impl<'de> de::Visitor<'de> for PackObjectXidVisitor {
    type Value = PackObject<xid::Id>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 12 bytes array or a 20 length xid string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match xid::Id::from_bytes(v) {
            Ok(id) => Ok(PackObject::Cbor(id)),
            Err(err) => Err(de::Error::custom(err)),
        }
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match xid::Id::from_bytes(v) {
            Ok(id) => Ok(PackObject::Cbor(id)),
            Err(err) => Err(de::Error::custom(err)),
        }
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        match xid::Id::from_bytes(&v) {
            Ok(id) => Ok(PackObject::Cbor(id)),
            Err(err) => Err(de::Error::custom(err)),
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = xid::Id::from_str(&v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }
}

impl<'de> Deserialize<'de> for PackObject<xid::Id> {
    fn deserialize<D>(deserializer: D) -> Result<PackObject<xid::Id>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(PackObjectXidVisitor)
    }
}

struct PackObjectUuidVisitor;

impl<'de> de::Visitor<'de> for PackObjectUuidVisitor {
    type Value = PackObject<uuid::Uuid>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a 16 bytes array or a uuid string")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 16 {
            Err(de::Error::custom(format!(
                "expected value length 16, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(v);
            Ok(PackObject::Cbor(uuid::Uuid::from_bytes(bytes)))
        }
    }

    fn visit_borrowed_bytes<E>(self, v: &'de [u8]) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 16 {
            Err(de::Error::custom(format!(
                "expected value length 16, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(v);
            Ok(PackObject::Cbor(uuid::Uuid::from_bytes(bytes)))
        }
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v.len() != 16 {
            Err(de::Error::custom(format!(
                "expected value length 16, got {:?}",
                v.len()
            )))
        } else {
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&v);
            Ok(PackObject::Cbor(uuid::Uuid::from_bytes(bytes)))
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = uuid::Uuid::parse_str(v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = uuid::Uuid::parse_str(v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        let id = uuid::Uuid::parse_str(&v).map_err(de::Error::custom)?;
        Ok(PackObject::Json(id))
    }
}

impl<'de> Deserialize<'de> for PackObject<uuid::Uuid> {
    fn deserialize<D>(deserializer: D) -> Result<PackObject<uuid::Uuid>, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(PackObjectUuidVisitor)
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for PackObject<()>
where
    S: Send + Sync,
{
    type Rejection = HTTPError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        match get_content_type(&parts.headers) {
            Ok(ot) => Ok(ot),
            Err(mut ct) => {
                if let Some(accept) = parts.headers.get(header::ACCEPT) {
                    if let Ok(accept) = accept.to_str() {
                        if accept.contains("application/cbor") {
                            return Ok(PackObject::Cbor(()));
                        }
                        if accept.contains("application/json") {
                            return Ok(PackObject::Json(()));
                        }
                        ct = accept.to_string();
                    }
                }

                Err(HTTPError::new(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
                    format!("Unsupported media type, {}", ct),
                ))
            }
        }
    }
}

#[async_trait]
impl<T, S, B> FromRequest<S, B> for PackObject<T>
where
    T: DeserializeOwned + Send + Sync,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = HTTPError;

    async fn from_request(req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers();
        let ct = get_content_type(headers).map_err(|ct| {
            HTTPError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE.as_u16(),
                format!("Unsupported media type, {}", ct),
            )
        })?;

        let enc = Encoding::from_header_value(headers.get(header::CONTENT_ENCODING));
        let mut bytes = Bytes::from_request(req, state).await.map_err(|err| {
            HTTPError::new(
                StatusCode::BAD_REQUEST.as_u16(),
                format!("Invalid body, {}", err),
            )
        })?;

        if !enc.identity() {
            bytes = enc
                .decode_all(bytes.reader())
                .map_err(|err| {
                    HTTPError::new(
                        StatusCode::BAD_REQUEST.as_u16(),
                        format!("Invalid body, {}", err),
                    )
                })?
                .into();
        }

        match ct {
            PackObject::Json(_) => {
                let value: T = serde_json::from_slice(&bytes).map_err(|err| HTTPError {
                    code: StatusCode::BAD_REQUEST.as_u16(),
                    message: format!("Invalid JSON body, {}", err),
                    data: None,
                })?;
                Ok(PackObject::Json(value))
            }
            PackObject::Cbor(_) => {
                let value: T = ciborium::from_reader(&bytes[..]).map_err(|err| HTTPError {
                    code: StatusCode::BAD_REQUEST.as_u16(),
                    message: format!("Invalid CBOR body, {}", err),
                    data: None,
                })?;
                Ok(PackObject::Cbor(value))
            }
        }
    }
}

fn get_content_type(headers: &HeaderMap) -> Result<PackObject<()>, String> {
    let content_type = if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        content_type
    } else {
        return Err("".to_string());
    };

    let content_type = if let Ok(content_type) = content_type.to_str() {
        content_type
    } else {
        return Err("".to_string());
    };

    if let Ok(mime) = content_type.parse::<mime::Mime>() {
        if mime.type_() == "application" {
            if mime.subtype() == "cbor" || mime.suffix().map_or(false, |name| name == "cbor") {
                return Ok(PackObject::Cbor(()));
            } else if mime.subtype() == "json" || mime.suffix().map_or(false, |name| name == "json")
            {
                return Ok(PackObject::Json(()));
            }
        }
    }

    Err(content_type.to_string())
}

impl<T> IntoResponse for PackObject<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        // Use a small initial capacity of 128 bytes like serde_json::to_vec
        // https://docs.rs/serde_json/1.0.82/src/serde_json/ser.rs.html#2189
        let mut buf = BytesMut::with_capacity(128).writer();
        let res: Result<Response, Box<dyn Error>> = match self {
            PackObject::Json(v) => match serde_json::to_writer(&mut buf, &v) {
                Ok(()) => Ok((
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                    )],
                    buf.into_inner().freeze(),
                )
                    .into_response()),
                Err(err) => Err(Box::new(err)),
            },
            PackObject::Cbor(v) => match ciborium::into_writer(&v, &mut buf) {
                Ok(()) => Ok((
                    [(
                        header::CONTENT_TYPE,
                        HeaderValue::from_static("application/cbor"),
                    )],
                    buf.into_inner().freeze(),
                )
                    .into_response()),
                Err(err) => Err(Box::new(err)),
            },
        };

        match res {
            Ok(res) => res,
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                err.to_string(),
            )
                .into_response(),
        }
    }
}

pub fn cbor_from_slice<T>(bytes: &[u8]) -> Result<T, HTTPError>
where
    T: DeserializeOwned,
{
    let value: T = ciborium::from_reader(bytes).map_err(|err| HTTPError {
        code: StatusCode::BAD_REQUEST.as_u16(),
        message: format!("Invalid CBOR bytes, {}", err),
        data: None,
    })?;
    Ok(value)
}

pub fn cbor_to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, HTTPError> {
    let mut buf: Vec<u8> = Vec::new();
    ciborium::into_writer(value, &mut buf).map_err(|err| HTTPError {
        code: StatusCode::BAD_REQUEST.as_u16(),
        message: format!("Failed to serialize CBOR, {}", err),
        data: None,
    })?;
    Ok(buf)
}
