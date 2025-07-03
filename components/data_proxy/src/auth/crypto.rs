use anyhow::{bail, Result};
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::pkcs8::DecodePublicKey;
use sha2::Digest;

pub fn ed25519_to_x25519_pubkey(pubkey: &str) -> Result<[u8; 32]> {
    let key_pem = format!("-----BEGIN PUBLIC KEY-----\n{pubkey}\n-----END PUBLIC KEY-----");
    let pk = ed25519_dalek::VerifyingKey::from_public_key_pem(&key_pem)?;
    use curve25519_dalek::edwards::CompressedEdwardsY;
    let cey = CompressedEdwardsY::from_slice(pk.as_bytes())?;
    match cey.decompress() {
        Some(ep) => Ok(*ep.to_montgomery().as_bytes()),
        None => bail!("Invalid public key"),
    }
}

pub fn ed25519_to_x25519_privatekey(privatekey: &str) -> Result<[u8; 32]> {
    let key_pem = format!("-----BEGIN PRIVATE KEY-----\n{privatekey}\n-----END PRIVATE KEY-----");
    let sk = ed25519_dalek::SigningKey::from_pkcs8_pem(&key_pem)?;
    if sk.as_bytes().len() < 32 {
        bail!("Invalid private key length")
    }
    // hash secret
    let hash = sha2::Sha512::digest(&sk.as_bytes()[..32]);
    let mut output = [0u8; 32];
    output.copy_from_slice(&hash[..32]);
    Ok(output)
}
