//! Checks a forwarded caller's bearer token and that the sending peer joined the same realm.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
use aruna_core::metadata::MetadataError;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_storage::StorageHandle;
use byteview::ByteView;

use super::AuthValidationState;
use crate::realm::peer_trust::{PeerTrust, RealmPeerError, ensure_peer_trust};
use aruna_core::metadata::AuthToken;

pub(super) async fn remote_auth_context(
    state: &AuthValidationState,
    auth_token: Option<AuthToken>,
) -> Result<Option<AuthContext>, MetadataError> {
    let Some(auth_token) = auth_token else {
        return Ok(None);
    };
    let AuthToken::Bearer(token) = auth_token else {
        return Err(MetadataError::Backend(
            "internal metadata auth requires the remote peer gate".to_string(),
        ));
    };
    state
        .validate(token.as_str())
        .await
        .map(Some)
        .map_err(|error| MetadataError::Backend(format!("invalid metadata auth token: {error}")))
}

pub(super) async fn authorize_peer(
    state: &AuthValidationState,
    storage_handle: &StorageHandle,
    peer: NodeId,
    local_realm_id: Option<RealmId>,
    auth_token: Option<AuthToken>,
    allow_internal: bool,
) -> Result<Option<AuthContext>, MetadataError> {
    let internal_auth = matches!(&auth_token, Some(AuthToken::Internal(_)));
    let auth_context = match auth_token {
        Some(AuthToken::Internal(auth)) if allow_internal => {
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
            // A user of another realm may sign in here; an anonymous one never.
            if auth.user_id.realm_id != auth.realm_id && auth.user_id.is_nil() {
                return Err(MetadataError::InvalidInput(format!(
                    "internal metadata auth user realm `{}` does not match token realm `{}`",
                    auth.user_id.realm_id, auth.realm_id
                )));
            }
            Some(auth)
        }
        Some(AuthToken::Internal(_)) => {
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
            "unexpected realm config read result for `{realm_id}`: {}",
            other.label()
        ))),
    }
}
