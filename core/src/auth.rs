pub const TRUSTED_REALMS_LIST_KEY: &[u8] = b"trusted_realms_list";

/// Maximum bearer-token lifetime accepted by replicated revocation admission.
/// 366 days covers every 12-calendar-month token, including leap years.
pub const MAX_BEARER_TOKEN_LIFETIME_SECS: u64 = 366 * 24 * 60 * 60;

/// Bounded grace for revocation retention across pairwise clock skew. Also the
/// only skew a replicated timestamp may be ahead of the local clock before it
/// is treated as a rollback rather than as skew.
pub const REVOCATION_GRACE_SECS: u64 = 5 * 60;

pub fn credential_hash(value: impl AsRef<[u8]>) -> String {
    blake3::hash(value.as_ref()).to_string()
}

pub fn bearer_token_hash(token: &str) -> String {
    credential_hash(token)
}

/// Shape check for a replicated revocation entry, so a realm-wide revocation
/// set can never be filled with arbitrary strings.
pub fn valid_token_hash(hash: &str) -> bool {
    hash.len() == blake3::OUT_LEN * 2
        && hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn revocation_live(expires_at: u64, now: u64) -> bool {
    expires_at >= now
}

pub fn revocation_retained(expires_at: u64, now: u64) -> bool {
    expires_at.saturating_add(REVOCATION_GRACE_SECS) >= now
}

pub fn valid_revocation_expiry(expires_at: u64, now: u64) -> bool {
    expires_at.saturating_sub(now)
        <= MAX_BEARER_TOKEN_LIFETIME_SECS.saturating_add(REVOCATION_GRACE_SECS)
}

/// Whether the signed lifetime of a token stays revocable. Issuance and
/// validation share it so a minted token can never outlive what the replicated
/// revocation set is able to hold.
pub fn valid_token_lifetime(iat: u64, exp: u64) -> bool {
    exp.checked_sub(iat)
        .is_some_and(|lifetime| lifetime <= MAX_BEARER_TOKEN_LIFETIME_SECS)
}

#[cfg(test)]
mod tests {
    use super::{
        REVOCATION_GRACE_SECS, TRUSTED_REALMS_LIST_KEY, bearer_token_hash, credential_hash,
        revocation_live, revocation_retained, valid_revocation_expiry,
    };

    #[test]
    fn bearer_token_hash_matches_existing_blake3_hex() {
        let token = "bearer-token";
        let expected = blake3::hash(b"bearer-token").to_string();

        assert_eq!(bearer_token_hash(token), expected);
    }

    #[test]
    fn credential_hash_matches_existing_blake3_hex() {
        let secret = b"credential-secret";

        assert_eq!(credential_hash(secret), blake3::hash(secret).to_string());
    }

    #[test]
    fn auth_state_keys_preserve_persisted_names() {
        assert_eq!(TRUSTED_REALMS_LIST_KEY, b"trusted_realms_list");
    }

    #[test]
    fn bounded_revocation_window() {
        let now = 1_000;
        assert!(revocation_live(now, now));
        assert!(!revocation_live(now - 1, now));
        assert!(revocation_retained(now - REVOCATION_GRACE_SECS, now));
        assert!(!revocation_retained(now - REVOCATION_GRACE_SECS - 1, now));
        assert!(valid_revocation_expiry(
            now + super::MAX_BEARER_TOKEN_LIFETIME_SECS,
            now
        ));
    }
}
