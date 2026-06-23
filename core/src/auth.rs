pub const TOKEN_REVOCATION_LIST_KEY: &[u8] = b"token_revocation_list";
pub const TRUSTED_REALMS_LIST_KEY: &[u8] = b"trusted_realms_list";

pub fn bearer_token_hash(token: &str) -> String {
    blake3::hash(token.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::{TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY, bearer_token_hash};

    #[test]
    fn bearer_token_hash_matches_existing_blake3_hex() {
        let token = "bearer-token-for-compatibility";

        assert_eq!(
            bearer_token_hash(token),
            blake3::hash(token.as_bytes()).to_string()
        );
    }

    #[test]
    fn auth_state_keys_preserve_persisted_names() {
        assert_eq!(TOKEN_REVOCATION_LIST_KEY, b"token_revocation_list");
        assert_eq!(TRUSTED_REALMS_LIST_KEY, b"trusted_realms_list");
    }
}
