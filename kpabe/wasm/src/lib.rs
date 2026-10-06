//! Browser binding of `aruna-kpabe` for the portal's key worker.
//! Imports opened grants, opens object envelopes and issues scoped keys from a bucket key.
//! Secret inputs are taken as mutable slices and cleared, which also clears the caller's buffer.

use aruna_kpabe::{Attribute, Envelope, MasterSecret, Policy, PublicParameters, UserKey};
use wasm_bindgen::prelude::wasm_bindgen;
use zeroize::{Zeroize, Zeroizing};

const REFUSED: &str = "invalid KP-ABE input";
const SEED_SALT: &[u8] = b"aruna bucket ABE seed v1";

/// An imported user key with the admitted parameters it belongs to.
#[wasm_bindgen]
pub struct ScopedKey {
    parameters: PublicParameters,
    key: UserKey,
}

fn refused<T>(_: T) -> String {
    REFUSED.into()
}

fn admitted(
    parameters: &[u8],
    context: &[u8],
    fingerprint: &[u8],
) -> Result<PublicParameters, String> {
    let fingerprint: &[u8; 32] = fingerprint.try_into().map_err(refused)?;
    PublicParameters::from_bytes(parameters, context, fingerprint).map_err(refused)
}

/// Imports an opened grant key under the admitted parameters and clears `plain`.
#[wasm_bindgen]
pub fn import_key(
    parameters: &[u8],
    context: &[u8],
    fingerprint: &[u8],
    plain: &mut [u8],
) -> Result<ScopedKey, String> {
    let result = admitted(parameters, context, fingerprint).and_then(|parameters| {
        let key = UserKey::open(&parameters, plain, |value| {
            Ok(Zeroizing::new(value.to_vec()))
        })
        .map_err(refused)?;
        Ok(ScopedKey { parameters, key })
    });
    plain.zeroize();
    result
}

#[wasm_bindgen]
impl ScopedKey {
    /// Opens the 32-byte object private key of one envelope with its context bytes.
    pub fn open_object(&self, envelope: &[u8], context: &[u8]) -> Result<Vec<u8>, String> {
        let envelope = Envelope::from_bytes(&self.parameters, envelope).map_err(refused)?;
        let key =
            aruna_kpabe::open(&self.parameters, &self.key, &envelope, context).map_err(refused)?;
        Ok(key.as_bytes().to_vec())
    }
}

/// Issues the encoded key for a scope from the bucket private key and clears `bucket_key`.
/// Refuses parameters that the bucket key does not derive, like the node's issuer.
#[wasm_bindgen]
pub fn issue_key(
    bucket_key: &mut [u8],
    parameters: &[u8],
    context: &[u8],
    fingerprint: &[u8],
    kind: &str,
    scope: &str,
    epochs: &[u64],
) -> Result<Vec<u8>, String> {
    let result = issue_scope(
        bucket_key,
        parameters,
        context,
        fingerprint,
        kind,
        scope,
        epochs,
    );
    bucket_key.zeroize();
    result
}

/// Clamps the bucket key with RFC 7748 masks, then derives the setup seed like the node.
fn derive_master(
    bucket_key: &[u8],
    context: &[u8],
) -> Result<(PublicParameters, MasterSecret), String> {
    let mut scalar = Zeroizing::new(<[u8; 32]>::try_from(bucket_key).map_err(refused)?);
    scalar[0] &= 248;
    scalar[31] &= 127;
    scalar[31] |= 64;
    let seed = aruna_kpabe::derive_seed(&scalar[..], SEED_SALT, context).map_err(refused)?;
    aruna_kpabe::setup_from_seed(seed.as_bytes(), context).map_err(refused)
}

fn issue_scope(
    bucket_key: &[u8],
    parameters: &[u8],
    context: &[u8],
    fingerprint: &[u8],
    kind: &str,
    scope: &str,
    epochs: &[u64],
) -> Result<Vec<u8>, String> {
    let admitted = admitted(parameters, context, fingerprint)?;
    let (derived, master) = derive_master(bucket_key, context)?;
    if derived.fingerprint() != admitted.fingerprint() || derived.to_bytes() != parameters {
        return Err(REFUSED.into());
    }
    let alternatives = match (kind, scope) {
        ("subtree", "") => Vec::new(),
        ("subtree", prefix) if prefix.ends_with('/') => vec![Attribute::Prefix(prefix.into())],
        ("exact", key) if !key.is_empty() => vec![Attribute::Key(key.into())],
        _ => return Err(REFUSED.into()),
    };
    if scope.contains('\0') {
        return Err(REFUSED.into());
    }
    let domain = blake3::hash(context);
    let policy = Policy::new(domain.as_bytes(), epochs, &alternatives).map_err(refused)?;
    let key =
        aruna_kpabe::issue(&derived, &master, &policy, &mut getrandom::SysRng).map_err(refused)?;
    key.seal(|bytes| Ok(bytes.to_vec())).map_err(refused)
}

#[cfg(test)]
mod tests;
