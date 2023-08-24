// use hex_literal::hex;
use base64ct::{Base64UrlUnpadded, Encoding};
mod cose_key;
mod encrypt;

pub use cose_key::Key;
pub use coset::iana;
pub use encrypt::Encrypt0;

// https://www.rfc-editor.org/rfc/rfc8949.html#name-self-described-cbor
pub const CBOR_TAG: [u8; 3] = [0xd9, 0xd9, 0xf7];

pub fn base64url_encode(data: &[u8]) -> String {
    Base64UrlUnpadded::encode_string(data)
}

pub fn base64url_decode(data: &str) -> anyhow::Result<Vec<u8>> {
    Base64UrlUnpadded::decode_vec(data).map_err(anyhow::Error::msg)
}

pub fn wrap_cbor_tag(data: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(data.len() + 3);
    buf.extend_from_slice(&CBOR_TAG);
    buf.extend_from_slice(data);
    buf
}

pub fn unwrap_cbor_tag(data: &[u8]) -> &[u8] {
    if data.len() > 3 && data[..3] == CBOR_TAG {
        return &data[3..];
    }
    data
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::{env, fs};

    use super::*;

    /// Check that the generated files are up to date.
    #[test]
    #[ignore]
    fn generated_keys_if_not_exists() -> anyhow::Result<()> {
        let aad = b"yiwen.ai".as_slice();
        let keys_path = Path::new(&env::var("CARGO_MANIFEST_DIR")?).join("tests/keys");
        // https://en.wikipedia.org/wiki/Glossary_of_cryptographic_keys

        let mkek = base64url_decode("YiWenAI-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-LLc")?;
        println!("mkek: {}, {:?}", mkek.len(), mkek);

        let kek = fs::read_to_string(keys_path.join("encrypted-a256gcm-kek.key"));
        if kek.is_ok() {
            println!("encrypted-a256gcm-kek.key exists, skipping key generation");
            return Ok(());
        }

        let kek = Key::new_sym(iana::Algorithm::A256GCM, b"20230511")?;
        let encryptor = Encrypt0::new(kek.get_private()?, kek.key_id().as_slice());

        let wallet_key = Key::new_sym(iana::Algorithm::Direct, b"42")?;
        let data = encryptor.encrypt(&wallet_key.to_vec()?, aad)?;
        let data = wrap_cbor_tag(&data);
        fs::write(keys_path.join("encrypted-direct-wallet.key.cbor"), &data)?;
        fs::write(
            keys_path.join("encrypted-direct-wallet.key"),
            base64url_encode(&data),
        )?;

        println!("Generate keys successfully");
        Ok(())
    }
}
