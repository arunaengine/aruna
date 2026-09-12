use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{API_STATE_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{AuthContext, RealmConfigDocument, RealmId, TokenClaims};
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::StorageHandle;
use byteview::ByteView;
use serde::de::DeserializeOwned;

use super::{MetadataHandle, MetadataWritePeerError, RevocationBlindValidation};
use crate::auth::bearer_token::{
    ArunaBearerTokenError, ArunaBearerTokenValidationState, decode_bearer_token,
    validate_bearer_token,
};
use crate::driver::DriverContext;
use crate::metadata::contact::PeerContacts;
use crate::metadata::protocol::{MetadataAuthToken, MetadataReadError};
use crate::realm::peer_trust::{PeerTrust, RealmPeerError, ensure_peer_trust};

impl MetadataHandle {
    /// Validates a forwarded caller's bearer token and confirms the forwarding
    /// peer belongs to the token's realm, exactly as the query/search paths do.
    pub(crate) async fn authorize_remote_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
    ) -> Result<Option<AuthContext>, MetadataError> {
        authorize_peer(
            &self.inner.auth_validation,
            &self.inner.storage_handle,
            peer,
            self.inner.net_handle.as_ref().map(|net| *net.realm_id()),
            auth_token,
            true,
        )
        .await
    }

    pub(crate) async fn authorize_read_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
        require_trusted: bool,
    ) -> Result<Option<AuthContext>, MetadataReadError> {
        let auth = match auth_token {
            Some(token @ MetadataAuthToken::Bearer(_)) => {
                Some(self.authorize_write_peer(peer, Some(token)).await.map_err(
                    |error| match error {
                        MetadataWritePeerError::Unauthorized => MetadataReadError::Unauthorized,
                        MetadataWritePeerError::Unavailable(_) => MetadataReadError::Unavailable,
                    },
                )?)
            }
            token => self
                .authorize_remote_peer(peer, token)
                .await
                .map_err(|_| MetadataReadError::Unavailable)?,
        };
        let realm_id = self
            .inner
            .net_handle
            .as_ref()
            .map(|net| *net.realm_id())
            .ok_or(MetadataReadError::Unavailable)?;
        if auth.as_ref().is_some_and(|auth| auth.realm_id != realm_id) {
            return Err(MetadataReadError::Forbidden);
        }
        if require_trusted {
            ensure_peer_configured(
                &self.inner.storage_handle,
                peer,
                realm_id,
                PeerTrust::Vouched(None),
            )
            .await
            .map_err(|_| MetadataReadError::Unavailable)?;
        }
        self.note_peer_contact(peer);
        Ok(auth)
    }

    pub(crate) async fn authorize_write_peer(
        &self,
        peer: NodeId,
        auth_token: Option<MetadataAuthToken>,
    ) -> Result<AuthContext, MetadataWritePeerError> {
        let Some(auth_token) = auth_token else {
            return Err(MetadataWritePeerError::Unauthorized);
        };
        let MetadataAuthToken::Bearer(token) = auth_token else {
            let auth = self
                .authorize_remote_peer(peer, Some(auth_token))
                .await
                .map_err(MetadataWritePeerError::Unavailable)?
                .ok_or(MetadataWritePeerError::Unauthorized)?;
            self.note_peer_contact(peer);
            return Ok(auth);
        };
        let auth = validate_bearer_token(&self.inner.auth_validation, token.as_str())
            .await
            .map_err(|_| MetadataWritePeerError::Unauthorized)?;
        let local_realm_id = self
            .inner
            .net_handle
            .as_ref()
            .map(|net| *net.realm_id())
            .ok_or_else(|| {
                MetadataWritePeerError::Unavailable(MetadataError::InvalidInput(
                    "forwarded metadata auth requires a local serving realm".to_string(),
                ))
            })?;
        if auth.realm_id == local_realm_id {
            ensure_peer_configured(
                &self.inner.storage_handle,
                peer,
                auth.realm_id,
                PeerTrust::Member,
            )
            .await
            .map_err(MetadataWritePeerError::Unavailable)?;
        }
        self.note_peer_contact(peer);
        Ok(auth)
    }

    /// This node's own liveness observation, taken where the peer identity is
    /// authorized. Never realm state: it is neither replicated nor published.
    fn note_peer_contact(&self, peer: NodeId) {
        self.inner.peer_contacts.note(peer, unix_timestamp_millis());
    }

    /// When this node last saw each authorized peer.
    pub fn peer_contacts(&self) -> PeerContacts {
        self.inner.peer_contacts.clone()
    }

    pub(crate) async fn claims_for_revocation(
        &self,
        token: &str,
    ) -> Result<TokenClaims, ArunaBearerTokenError> {
        decode_bearer_token(
            &RevocationBlindValidation(&self.inner.auth_validation),
            token,
        )
        .await
    }
}

pub(super) async fn remote_auth_context<S>(
    state: &S,
    auth_token: Option<MetadataAuthToken>,
) -> Result<Option<AuthContext>, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Some(auth_token) = auth_token else {
        return Ok(None);
    };
    let MetadataAuthToken::Bearer(token) = auth_token else {
        return Err(MetadataError::Backend(
            "internal metadata auth requires the remote peer gate".to_string(),
        ));
    };
    validate_bearer_token(state, token.as_str())
        .await
        .map(Some)
        .map_err(|error| MetadataError::Backend(format!("invalid metadata auth token: {error}")))
}

pub(super) async fn bucket_search_auth<S>(
    state: &S,
    storage_handle: &StorageHandle,
    peer: NodeId,
    local_realm_id: Option<RealmId>,
    auth_token: Option<MetadataAuthToken>,
) -> Result<AuthContext, MetadataReadError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let Some(MetadataAuthToken::Bearer(token)) = auth_token else {
        return Err(MetadataReadError::Unauthorized);
    };
    let auth = validate_bearer_token(state, token.as_str())
        .await
        .map_err(|_| MetadataReadError::Unauthorized)?;
    let Some(local_realm_id) = local_realm_id else {
        return Err(MetadataReadError::Unavailable);
    };
    if auth.realm_id != local_realm_id {
        return Err(MetadataReadError::Forbidden);
    }
    ensure_peer_configured(storage_handle, peer, auth.realm_id, PeerTrust::Member)
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    Ok(auth)
}

pub(super) async fn authorize_peer<S>(
    state: &S,
    storage_handle: &StorageHandle,
    peer: NodeId,
    local_realm_id: Option<RealmId>,
    auth_token: Option<MetadataAuthToken>,
    allow_internal: bool,
) -> Result<Option<AuthContext>, MetadataError>
where
    S: ArunaBearerTokenValidationState + ?Sized,
{
    let internal_auth = matches!(&auth_token, Some(MetadataAuthToken::Internal(_)));
    let auth_context = match auth_token {
        Some(MetadataAuthToken::Internal(auth)) if allow_internal => {
            let local_realm_id = local_realm_id.ok_or_else(|| {
                MetadataError::InvalidInput(
                    "internal metadata auth requires a local serving realm".to_string(),
                )
            })?;
            if auth.realm_id != local_realm_id {
                return Err(MetadataError::InvalidInput(format!(
                    "internal metadata auth realm `{}` does not match local realm `{local_realm_id}`",
                    auth.realm_id
                )));
            }
            if auth.user_id.realm_id != auth.realm_id {
                return Err(MetadataError::InvalidInput(format!(
                    "internal metadata auth user realm `{}` does not match token realm `{}`",
                    auth.user_id.realm_id, auth.realm_id
                )));
            }
            Some(auth)
        }
        Some(MetadataAuthToken::Internal(_)) => {
            return Err(MetadataError::Backend(
                "internal metadata auth is limited to forwarded requests".to_string(),
            ));
        }
        auth_token => remote_auth_context(state, auth_token).await?,
    };
    // Authenticated metadata requests are bound to the token's realm.
    let peer_realm_id = match auth_context.as_ref().map(|auth| auth.realm_id) {
        Some(realm_id) => realm_id,
        None => local_realm_id.ok_or_else(|| {
            MetadataError::InvalidInput(
                "remote metadata anonymous peer gate requires a local serving realm".to_string(),
            )
        })?,
    };
    // Internal auth is node-vouched, so the gate is told which user the peer
    // vouches for: an owner-bound device passes only for its own owner.
    let trust = match internal_auth {
        true => PeerTrust::Vouched(auth_context.as_ref().map(|auth| auth.user_id)),
        false => PeerTrust::Member,
    };
    ensure_peer_configured(storage_handle, peer, peer_realm_id, trust).await?;
    Ok(auth_context)
}

async fn ensure_peer_configured(
    storage_handle: &StorageHandle,
    peer: NodeId,
    realm_id: RealmId,
    trust: PeerTrust,
) -> Result<(), MetadataError> {
    match storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(realm_id.as_bytes().to_vec()),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => {
            let document = RealmConfigDocument::from_bytes(&bytes)
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
            ensure_peer_trust(&document, peer, realm_id, trust)
                .map_err(|error| {
                    MetadataError::InvalidInput(match error {
                        RealmPeerError::RealmMismatch { configured, .. } => format!(
                            "realm config `{configured}` does not match remote metadata realm `{realm_id}`"
                        ),
                        RealmPeerError::NotConfigured { .. } => format!(
                            "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
                        ),
                        RealmPeerError::NotTrusted { .. } => format!(
                            "remote metadata peer `{peer}` is not trusted for internal auth in realm `{realm_id}`"
                        ),
                    })
                })
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => {
            Err(MetadataError::InvalidInput(format!(
                "remote metadata peer `{peer}` is not configured in realm `{realm_id}`"
            )))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(MetadataError::Storage(error)),
        other => Err(MetadataError::Backend(format!(
            "unexpected realm config read result for `{realm_id}`: {other:?}"
        ))),
    }
}

pub(super) async fn load_auth_state<T>(
    storage_handle: &StorageHandle,
    key: &[u8],
) -> Result<T, MetadataError>
where
    T: DeserializeOwned + Default,
{
    match storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: API_STATE_KEYSPACE.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult {
            value: Some(bytes), ..
        }) => {
            postcard::from_bytes(&bytes).map_err(|error| MetadataError::Backend(error.to_string()))
        }
        Event::Storage(StorageEvent::ReadResult { value: None, .. }) => Ok(T::default()),
        Event::Storage(StorageEvent::Error { error }) => Err(MetadataError::Storage(error)),
        other => Err(MetadataError::Backend(format!(
            "unexpected metadata auth state read result: {other:?}"
        ))),
    }
}

pub(super) async fn config_digest_matches(
    context: &DriverContext,
    realm_id: RealmId,
    expected: &[u8; 32],
) -> bool {
    super::super::api::load_realm_config(context, realm_id)
        .await
        .and_then(|config| config.digest().ok())
        .as_ref()
        == Some(expected)
}
