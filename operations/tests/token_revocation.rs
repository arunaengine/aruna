//! Tests that a token revoked on one node is denied on a peer once the realm config converges.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};

use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentOperation, AdminDocumentTarget};
use aruna_core::auth::{bearer_token_hash, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::document::{DocumentSyncPublish, DocumentTarget};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::handle::Handle;
use aruna_core::keys::generate_signing_key;
use aruna_core::reducer::AdminDocumentState;
use aruna_core::structs::identity::auth::{Actor, TokenClaims};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::placement::record::NodePlacementEntry;
use aruna_core::{DocumentEffect, DocumentNetEvent};
use aruna_operations::auth::bearer_token::{
    DecodeTokenOperation, IssuerTrust, RevocationCheck, TokenChecks, TokenRevokedOperation,
    UserCutoffOperation,
};
use aruna_operations::auth::error::ArunaBearerError;
use aruna_operations::auth::revoke_token::{
    RevokeTokenAdmission, RevokeTokenConfig, RevokeTokenOperation,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_storage::StorageHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::store_realm_config;
use crate::topology::{TestNode, spawn_replicas};
type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Mirrors how production wires revocation: enforcement reads the replicated
/// realm config of the node that is validating, never a node-local list.
struct PeerAuthState {
    storage: StorageHandle,
    realm_id: RealmId,
}

impl PeerAuthState {
    async fn decode(&self, token: &str) -> Result<TokenClaims, ArunaBearerError> {
        let checks = TokenChecks {
            trust: IssuerTrust::Listed(HashSet::from([self.realm_id])),
            revocation: RevocationCheck::Realm,
            linked_realm: None,
            keys: None,
        };
        let context = DriverContext::for_storage(self.storage.clone());
        drive(DecodeTokenOperation::new(token, checks), &context).await
    }
}

#[tokio::test]
async fn peer_denies_token() -> TestResult<()> {
    // A token revoked on node A must be rejected on node B once the realm
    // config converges, without B ever seeing the raw token.
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let nodes = build_realm_nodes(realm_id, 2).await?;
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let (token, expires_at) = mint_token(&signing_key, realm_id, user_id);
    let token_hash = bearer_token_hash(&token);

    let peer = peer_auth(&nodes[1], realm_id);
    peer.decode(&token)
        .await
        .expect("peer accepts the token before it is revoked");

    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id,
                realm_id,
            },
            token_hash: token_hash.clone(),
            expires_at,
            token_owner: user_id,
            admission: RevokeTokenAdmission::SelfService,
            now: aruna_core::time::unix_timestamp_secs(),
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the revocation never reached the peer node",
        || async {
            Ok(usize::from(
                !drive(TokenRevokedOperation::new(realm_id, &token_hash), &nodes[1].context)
                    .await?,
            ))
        },
    )
    .await?;

    let error = peer.decode(&token)
        .await
        .expect_err("peer rejects the revoked token");
    assert!(matches!(error, ArunaBearerError::TokenRevoked));

    // The revocation is durable realm state, not process state: a validation
    // state built fresh over the same storage still denies the token.
    let restarted = peer_auth(&nodes[1], realm_id);
    assert!(matches!(
        restarted.decode(&token).await,
        Err(ArunaBearerError::TokenRevoked)
    ));

    // Only the revoked token is denied; the realm stays usable.
    let (other, _) = mint_token(&signing_key, realm_id, user_id);
    peer.decode(&other)
        .await
        .expect("peer still accepts a token that was never revoked");

    shutdown_nodes(nodes).await;
    Ok(())
}

#[tokio::test]
async fn peer_applies_cutoff() -> TestResult<()> {
    // A user cutoff written on node A denies that user's older tokens on node B,
    // while tokens issued at or after the cutoff and other users stay valid.
    let signing_key = generate_signing_key();
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let nodes = build_realm_nodes(realm_id, 2).await?;
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let other_id = UserId::local(Ulid::generate(), realm_id);
    let now = aruna_core::time::unix_timestamp_secs();
    let cutoff = now + 1;
    let (older, _) = mint_token_at(&signing_key, realm_id, user_id, now);
    let (newer, _) = mint_token_at(&signing_key, realm_id, user_id, cutoff);
    let (other, _) = mint_token_at(&signing_key, realm_id, other_id, now);

    drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: other_id,
                realm_id,
            },
            token_hash: user_cutoff_hash(&user_id),
            expires_at: user_cutoff_expiry(cutoff),
            token_owner: user_id,
            admission: RevokeTokenAdmission::Privileged,
            now,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(
        "the user cutoff never reached the peer node",
        || async {
            Ok(usize::from(
                drive(UserCutoffOperation::new(realm_id, &user_id), &nodes[1].context)
                    .await?
                    .is_none(),
            ))
        },
    )
    .await?;

    let peer = peer_auth(&nodes[1], realm_id);
    assert!(matches!(
        peer.decode(&older).await,
        Err(ArunaBearerError::TokenRevoked)
    ));
    peer.decode(&newer)
        .await
        .expect("a token issued at the cutoff stays valid");
    peer.decode(&other)
        .await
        .expect("another user's token stays valid");

    shutdown_nodes(nodes).await;
    Ok(())
}

fn peer_auth(node: &TestNode, realm_id: RealmId) -> PeerAuthState {
    PeerAuthState {
        storage: node.context.storage_handle.clone(),
        realm_id,
    }
}

/// Returns the signed token and its expiry, which bounds the revocation entry.
fn mint_token(signing_key: &SigningKey, realm_id: RealmId, user_id: UserId) -> (String, u64) {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    mint_token_at(signing_key, realm_id, user_id, now)
}

fn mint_token_at(
    signing_key: &SigningKey,
    realm_id: RealmId,
    user_id: UserId,
    now: u64,
) -> (String, u64) {
    let expires_at = now + 600;
    let claims = TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: expires_at,
        jti: Ulid::generate().to_string(),
        sid: None,
        session_kind: None,
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
        name: None,
        via: None,
        auth_time: None,
    };
    let key_pem = signing_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("realm key encodes");
    let token = encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).expect("realm key is an ed25519 key"),
    )
    .expect("token signs");
    (token, expires_at)
}

async fn build_realm_nodes(realm_id: RealmId, count: usize) -> TestResult<Vec<TestNode>> {
    let nodes = spawn_replicas(realm_id, count, false, true).await?;
    install_realm_config(&nodes, realm_id).await?;
    Ok(nodes)
}

fn base_config(nodes: &[TestNode], realm_id: RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        let node_id = node.net.node_id();
        config.ensure_node(node_id, RealmNodeKind::Management);
        config.placement_map.push(NodePlacementEntry {
            node_id,
            location: "eu".to_string(),
            weight: 100,
            full: false,
            draining: false,
            labels: BTreeMap::new(),
        });
    }
    config
}

async fn install_realm_config(nodes: &[TestNode], realm_id: RealmId) -> TestResult<()> {
    let config = base_config(nodes, realm_id);
    for node in nodes {
        store_realm_config(&node.net, &node.context, realm_id, &config).await?;
    }
    seed_sync_topic(nodes, realm_id, &config).await?;
    for node in nodes {
        aruna_operations::driver::drive(aruna_operations::placement::process_placements::ReconcilePlacementsOperation::new(realm_id, node.net.node_id(), true), &node.context).await.unwrap_or_else(|_| aruna_operations::placement::process_placements::PlacementReconcileOutcome::storage_failure());
    }
    Ok(())
}

/// Creates the realm config sync topic on both nodes, as the production realm
/// config apply path does, so a later admin event has a topic to ride.
async fn seed_sync_topic(
    nodes: &[TestNode],
    realm_id: RealmId,
    config: &RealmConfigDocument,
) -> TestResult<()> {
    let target = DocumentTarget::RealmConfig { realm_id };
    let placement =
        aruna_core::placement::target_placement_ref(config, &target, Default::default());
    let topic = target.sync_topic_id(realm_id, &placement);
    let actor = Actor {
        node_id: nodes[1].net.node_id(),
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let mut reducer_state = AdminDocumentState::new(AdminDocumentTarget::RealmConfig { realm_id });
    let event = reducer_state.apply_operation(
        &actor,
        AdminDocumentOperation::NodePlacementSet {
            entry: config
                .placement_map
                .first()
                .ok_or("realm config has no placement entry")?
                .clone(),
        },
    )?;

    match nodes[1]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentEffect::PublishDocuments {
                documents: vec![DocumentSyncPublish::AdminOperation {
                    target: target.clone(),
                    event: Box::new(event),
                    placement,
                    allow_genesis: true,
                    origin_signature: None,
                }],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsPublished { .. })) => {}
        other => return Err(format!("unexpected config seed publish event: {other:?}").into()),
    }

    match nodes[0]
        .net
        .send_effect(Effect::Net(NetEffect::DocumentSync(
            DocumentEffect::SyncDocuments {
                topics: vec![topic],
                peers: Vec::new(),
            },
        )))
        .await
    {
        Event::Net(NetEvent::DocumentSync(DocumentNetEvent::DocumentsReconciled { .. })) => Ok(()),
        other => Err(format!("unexpected config seed sync event: {other:?}").into()),
    }
}

async fn shutdown_nodes(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
