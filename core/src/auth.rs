pub const TOKEN_REVOCATION_LIST_KEY: &[u8] = b"token_revocation_list";
pub const TRUSTED_REALMS_LIST_KEY: &[u8] = b"trusted_realms_list";

pub fn credential_hash(value: impl AsRef<[u8]>) -> String {
    blake3::hash(value.as_ref()).to_string()
}

pub fn bearer_token_hash(token: &str) -> String {
    credential_hash(token)
}

#[cfg(test)]
mod tests {
    use super::{
        TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash, credential_hash,
    };

    #[test]
    fn bearer_token_hash_matches_existing_blake3_hex() {
        let token = "bearer-token-for-compatibility";
        let expected = blake3::hash(b"bearer-token-for-compatibility").to_string();

        assert_eq!(bearer_token_hash(token), expected);
    }

    #[test]
    fn credential_hash_matches_existing_blake3_hex() {
        let secret = b"credential-secret-for-compatibility";

        assert_eq!(credential_hash(secret), blake3::hash(secret).to_string());
    }

    #[test]
    fn auth_state_keys_preserve_persisted_names() {
        assert_eq!(TOKEN_REVOCATION_LIST_KEY, b"token_revocation_list");
        assert_eq!(TRUSTED_REALMS_LIST_KEY, b"trusted_realms_list");
    }
}
