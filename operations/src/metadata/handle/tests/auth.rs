use super::super::peer_auth::{authorize_peer, remote_auth_context};
use super::*;
use crate::auth::bearer_token::decode_bearer_token;
use crate::auth::bearer_token::validate_bearer_token;
use crate::metadata::handle::peer_auth::bucket_search_auth;
use crate::metadata::protocol::MetadataAuthToken;
use crate::metadata::protocol::MetadataReadError;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::API_STATE_KEYSPACE;
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::structs::AuthContext;
use aruna_core::structs::Permission;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::structs::TokenClaims;
use byteview::ByteView;
pub(super) async fn assert_auth_rejected(
    state: &MetadataAuthValidationState,
    token: &str,
    expected: &str,
) {
    let error = remote_auth_context(state, Some(MetadataAuthToken::bearer(token).unwrap()))
        .await
        .unwrap_err();

    match error {
        MetadataError::Backend(message) => assert!(
            message.contains(expected),
            "expected {message:?} to contain {expected:?}"
        ),
        other => panic!("unexpected metadata auth error: {other:?}"),
    }
}

pub(super) fn auth_storage() -> (TempDir, StorageHandle) {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    (dir, storage)
}

pub(super) async fn persist_auth_state<T: Serialize>(
    storage: &StorageHandle,
    key: &[u8],
    value: &T,
) {
    let bytes = postcard::to_allocvec(value).expect("auth state serializes");
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected auth state write result: {other:?}"),
    }
}

pub(super) async fn persist_revoked_config(
    storage: &StorageHandle,
    realm_id: RealmId,
    token: &str,
) {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.revoked_tokens.push(TokenRevocation {
        token_hash: bearer_token_hash(token),
        expires_at: aruna_core::time::unix_timestamp_secs() + 600,
    });
    write_realm_config(storage, realm_id, &config).await;
}

pub(super) async fn persist_realm_config(
    storage: &StorageHandle,
    realm_id: RealmId,
    node_ids: &[NodeId],
) {
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    for node_id in node_ids {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    write_realm_config(storage, realm_id, &config).await;
}

async fn write_realm_config(
    storage: &StorageHandle,
    realm_id: RealmId,
    config: &RealmConfigDocument,
) {
    let bytes = postcard::to_allocvec(config).expect("realm config serializes");

    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: (*realm_id.as_bytes()).into(),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected realm config write result: {other:?}"),
    }
}

pub(super) fn realm_fixture() -> (SigningKey, RealmId, UserId) {
    let signing_key = signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::generate(), realm_id);
    (signing_key, realm_id, user_id)
}

fn signing_key() -> SigningKey {
    generate_signing_key()
}

pub(super) fn node_id_seed(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn token_claims(realm_id: RealmId, user_id: UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    }
}

pub(super) fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn member_peer_accepted() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let (_dir, storage) = auth_storage();
    let configured_peer = node_id_seed(12);
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_realm_config(&storage, realm_id, &[configured_peer]).await;
    let state = MetadataAuthValidationState::new(storage.clone(), Some(realm_id));

    let auth = authorize_peer(
        &state,
        &storage,
        configured_peer,
        Some(RealmId([99u8; 32])),
        Some(MetadataAuthToken::bearer(token).unwrap()),
        false,
    )
    .await
    .expect("configured peer accepted");

    let auth = auth.expect("authenticated request has auth context");
    assert_eq!(auth.user_id, user_id);
    assert_eq!(auth.realm_id, realm_id);
}

#[tokio::test]
async fn internal_auth_preserves() {
    let realm_id = RealmId([13; 32]);
    let peer = node_id_seed(14);
    let user_id = UserId::new(Ulid::from_bytes([15; 16]), realm_id);
    let restrictions = vec![PathRestriction {
        pattern: format!("/{realm_id}/g/**"),
        permission: Permission::READ,
    }];
    let expected = AuthContext {
        user_id,
        realm_id,
        path_restrictions: Some(restrictions),
        session: None,
    };
    let (_dir, storage) = auth_storage();
    persist_realm_config(&storage, realm_id, &[peer]).await;

    let auth = authorize_peer(
        &MetadataAuthValidationState::new(storage.clone(), Some(realm_id)),
        &storage,
        peer,
        Some(realm_id),
        Some(MetadataAuthToken::internal(expected.clone())),
        true,
    )
    .await
    .expect("internal peer accepted");

    assert_eq!(auth, Some(expected));
}

#[tokio::test]
async fn bad_bucket_token() {
    let (_dir, storage) = auth_storage();
    let realm_id = RealmId([17u8; 32]);
    let auth = bucket_search_auth(
        &MetadataAuthValidationState::new(storage.clone(), Some(realm_id)),
        &storage,
        node_id_seed(18),
        Some(realm_id),
        Some(MetadataAuthToken::bearer("invalid-token").unwrap()),
    )
    .await;

    assert_eq!(auth, Err(MetadataReadError::Unauthorized));
}

#[tokio::test]
async fn bucket_realm_mismatch() {
    // The same valid token authorizes its own realm and is denied for
    // another one, so the denial is the realm boundary and not a bad token.
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let peer = node_id_seed(31);
    let (_dir, storage) = auth_storage();
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_realm_config(&storage, realm_id, &[peer]).await;
    let state = MetadataAuthValidationState::new(storage.clone(), Some(realm_id));
    let auth_token = MetadataAuthToken::bearer(token).unwrap();

    let allowed = bucket_search_auth(
        &state,
        &storage,
        peer,
        Some(realm_id),
        Some(auth_token.clone()),
    )
    .await;
    let denied = bucket_search_auth(
        &state,
        &storage,
        peer,
        Some(RealmId([21u8; 32])),
        Some(auth_token),
    )
    .await;

    assert_eq!(allowed.map(|auth| auth.realm_id), Ok(realm_id));
    assert_eq!(denied, Err(MetadataReadError::Forbidden));
}

#[tokio::test]
async fn revocation_blind_decode() {
    // Revoking a token must still decode its claims, while every other
    // check the validator runs stays in force.
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let (_dir, storage) = auth_storage();
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_revoked_config(&storage, realm_id, &token).await;
    let state = MetadataAuthValidationState::new(storage, Some(realm_id));

    assert!(matches!(
        validate_bearer_token(&state, &token).await,
        Err(ArunaBearerTokenError::TokenRevoked)
    ));
    let claims = decode_bearer_token(&RevocationBlindValidation(&state), &token)
        .await
        .expect("revoked token still decodes for revocation");
    assert_eq!(claims.sub, user_id.to_string());

    let (_untrusted_dir, untrusted_storage) = auth_storage();
    let untrusted = MetadataAuthValidationState::new(untrusted_storage, Some(realm_id));
    assert!(
        decode_bearer_token(&RevocationBlindValidation(&untrusted), &token)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn foreign_peer_rejected() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let (_dir, storage) = auth_storage();
    let wrong_realm_id = RealmId([21u8; 32]);
    let wrong_realm_peer = node_id_seed(22);
    let auth_realm_peer = node_id_seed(23);
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_realm_config(&storage, wrong_realm_id, &[wrong_realm_peer]).await;
    persist_realm_config(&storage, realm_id, &[auth_realm_peer]).await;
    let state = MetadataAuthValidationState::new(storage.clone(), Some(realm_id));

    let error = authorize_peer(
        &state,
        &storage,
        wrong_realm_peer,
        Some(wrong_realm_id),
        Some(MetadataAuthToken::bearer(token).unwrap()),
        false,
    )
    .await
    .expect_err("wrong realm peer rejected");

    assert_eq!(
        error,
        MetadataError::InvalidInput(format!(
            "remote metadata peer `{wrong_realm_peer}` is not configured in realm `{realm_id}`"
        ))
    );
}

#[tokio::test]
async fn anonymous_peer_accepted() {
    let local_realm_id = RealmId([25u8; 32]);
    let (_dir, storage) = auth_storage();
    let local_peer = node_id_seed(26);
    persist_realm_config(&storage, local_realm_id, &[local_peer]).await;
    let state = MetadataAuthValidationState::new(storage.clone(), Some(local_realm_id));

    let auth = authorize_peer(
        &state,
        &storage,
        local_peer,
        Some(local_realm_id),
        None,
        false,
    )
    .await
    .expect("anonymous local-realm peer accepted");

    assert_eq!(auth, None);
}

#[tokio::test]
async fn anonymous_peer_rejected() {
    let local_realm_id = RealmId([27u8; 32]);
    let wrong_realm_id = RealmId([28u8; 32]);
    let (_dir, storage) = auth_storage();
    let wrong_realm_peer = node_id_seed(29);
    persist_realm_config(&storage, wrong_realm_id, &[wrong_realm_peer]).await;
    let state = MetadataAuthValidationState::new(storage.clone(), Some(local_realm_id));

    let error = authorize_peer(
        &state,
        &storage,
        wrong_realm_peer,
        Some(local_realm_id),
        None,
        false,
    )
    .await
    .expect_err("anonymous wrong-realm peer rejected");

    assert_eq!(
        error,
        MetadataError::InvalidInput(format!(
            "remote metadata peer `{wrong_realm_peer}` is not configured in realm `{local_realm_id}`"
        ))
    );
}

#[tokio::test]
async fn auth_validates_token() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let (_dir, storage) = auth_storage();
    // Revocation fails closed without the realm config revocation set.
    persist_realm_config(&storage, realm_id, &[]).await;
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    let state = MetadataAuthValidationState::new(storage, Some(realm_id));

    let auth = remote_auth_context(&state, Some(MetadataAuthToken::bearer(token).unwrap()))
        .await
        .unwrap()
        .expect("token produces auth context");

    assert_eq!(auth.user_id, user_id);
    assert_eq!(auth.realm_id, realm_id);
}

#[tokio::test]
async fn auth_preserves_path() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let restrictions = vec![PathRestriction {
        pattern: format!("/{realm_id}/g/{}/meta/**", Ulid::generate()),
        permission: Permission::READ,
    }];
    let mut claims = token_claims(realm_id, user_id);
    claims.restrictions = Some(restrictions.clone());
    let token = sign_token(&realm_signing_key, &claims);
    let (_dir, storage) = auth_storage();
    // Revocation fails closed without the realm config revocation set.
    persist_realm_config(&storage, realm_id, &[]).await;
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    let state = MetadataAuthValidationState::new(storage, Some(realm_id));

    let auth = remote_auth_context(&state, Some(MetadataAuthToken::bearer(token).unwrap()))
        .await
        .unwrap()
        .expect("token produces auth context");

    assert_eq!(auth.user_id, user_id);
    assert_eq!(auth.realm_id, realm_id);
    assert_eq!(auth.path_restrictions, Some(restrictions));
}

#[tokio::test]
async fn auth_rejects_revoked() {
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));

    let (_revoked_dir, revoked_storage) = auth_storage();
    persist_auth_state(
        &revoked_storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_revoked_config(&revoked_storage, realm_id, &token).await;
    let revoked_state = MetadataAuthValidationState::new(revoked_storage, Some(realm_id));
    assert_auth_rejected(&revoked_state, &token, "Token is revoked").await;

    let (_untrusted_dir, untrusted_storage) = auth_storage();
    let untrusted_state = MetadataAuthValidationState::new(untrusted_storage, Some(realm_id));
    assert_auth_rejected(&untrusted_state, &token, "Realm is not trusted").await;

    let (_invalid_dir, invalid_storage) = auth_storage();
    let invalid_state = MetadataAuthValidationState::new(invalid_storage, Some(realm_id));
    assert_auth_rejected(&invalid_state, "not-a-jwt", "invalid metadata auth token").await;
}

#[tokio::test]
async fn replicated_revocation_rejects() {
    // A revocation that arrived through the replicated realm config must
    // deny the token on this node too.
    let (realm_signing_key, realm_id, user_id) = realm_fixture();
    let token = sign_token(&realm_signing_key, &token_claims(realm_id, user_id));
    let (_dir, storage) = auth_storage();
    persist_auth_state(
        &storage,
        TRUSTED_REALMS_LIST_KEY,
        &HashSet::from([realm_id]),
    )
    .await;
    persist_revoked_config(&storage, realm_id, &token).await;
    let state = MetadataAuthValidationState::new(storage, Some(realm_id));

    assert_auth_rejected(&state, &token, "Token is revoked").await;
}

#[tokio::test]
async fn auth_allows_missing() {
    let (_dir, storage) = auth_storage();
    let state = MetadataAuthValidationState::new(storage, None);

    assert_eq!(
        remote_auth_context(&state, None)
            .await
            .expect("missing token is anonymous"),
        None
    );
}
