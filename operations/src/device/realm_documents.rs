//! A device's copy of the realm-wide documents.
//!
//! A device runs no document sync, so nothing pushes the realm configuration to
//! it. It fetches the documents from a realm node as an ordinary routed read
//! and installs the copies into the same keyspaces a realm node uses, so every
//! local read and permission check runs unchanged. The copies are never
//! published on: a device originates no realm administration.
//!
//! What it installs never regresses. A copy is refused unless its realm-config
//! clock covers the installed one, and every revocation the device already
//! holds survives whatever the answer says, so neither a lagging node nor one
//! the realm evicted can hand a device back a revoked token. Only the realm's
//! own nodes count in that clock, and a marker every peer disagrees with is
//! re-based rather than left to lock the device out of its realm.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentClock;
use aruna_core::auth::revocation_live;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    DEVICE_MANAGEMENT_URL_KEYSPACE, DEVICE_REALM_MARKER_KEYSPACE, GROUP_KEYSPACE,
};
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::structs::{
    Actor, AuthContext, Group, GroupAuthorizationDocument, NodeInfoDocument, RealmConfigDocument,
    RealmId, SyncRefusal,
};
use aruna_core::types::{Key, UserId, Value};
use aruna_core::util::unix_timestamp_secs;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::driver::DriverContext;
use crate::metadata::api::load_realm_config;
use crate::metadata::protocol::{
    DeviceGroupDocuments, MAX_DEVICE_GROUPS, MetadataTransportMessage, RealmDocuments,
};
use crate::mutate_realm_placement::node_kind;
use crate::node_info::{read_node_info_document, write_node_info_document};

/// Attempts in which every answering peer served a copy the marker does not
/// cover before the marker itself is treated as the wrong one. A marker can
/// only be too high through a peer that lied, and the realm agreeing against it
/// is the evidence that it did.
const REBASE_ATTEMPTS: u32 = 3;

/// How many cached groups one reconciliation looks at. Above
/// [`MAX_DEVICE_GROUPS`] so a group installed outside a fetch is still seen.
const GROUP_SCAN_LIMIT: usize = MAX_DEVICE_GROUPS + 8;

/// What the device accepted last, and how often the realm has disagreed with it
/// since. Device-local: it is the memory that keeps a copy from going backwards.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct RealmMarker {
    clock: AdminDocumentClock,
    behind: u32,
}

/// One fetch attempt: who this device is, whom it may ask, and what it holds.
struct FetchPlan {
    realm_id: RealmId,
    node_id: NodeId,
    owner: UserId,
    auth: AuthContext,
    peers: Vec<NodeId>,
    marker: RealmMarker,
}

/// One answer worth keeping, with the configuration it carries decoded once.
struct Accepted {
    documents: RealmDocuments,
    config: RealmConfigDocument,
    /// The answer's clock, counting only origins its own configuration names.
    clock: AdminDocumentClock,
}

/// The best of what the peers answered: one copy that covers the marker, or a
/// copy two peers agreed on below it.
#[derive(Default)]
struct Selection {
    current: Option<Accepted>,
    behind: Option<Accepted>,
    candidates: Vec<Accepted>,
}

impl Selection {
    /// Offers one answer, and says whether the search may stop: it may as soon
    /// as a copy covers the marker.
    fn offer(&mut self, accepted: Accepted, installed: &AdminDocumentClock) -> bool {
        let origins = realm_origins(&accepted.config);
        if covers(&accepted.clock, installed, &origins) {
            self.current = Some(accepted);
            return true;
        }
        let agreed = self.candidates.iter().any(|candidate| {
            candidate.clock == accepted.clock && candidate.documents == accepted.documents
        });
        if agreed {
            let further = self
                .behind
                .as_ref()
                .is_none_or(|best| covers(&accepted.clock, &best.clock, &origins));
            if further {
                self.behind = Some(accepted);
            }
        } else {
            self.candidates.push(accepted);
        }
        false
    }
}

/// Fetches the realm-wide documents and installs them on this device. `budget`
/// bounds the whole exchange, so a realm nobody answers for costs that much
/// once and the stored copy keeps serving until the next attempt.
pub async fn fetch_realm_documents(context: &Arc<DriverContext>, budget: Duration) -> bool {
    let Some(plan) = fetch_plan(context).await else {
        return false;
    };
    fetch_with_plan(context, plan, budget).await
}

/// The same fetch for a device that holds no realm configuration yet: the owner
/// and the peers to ask are given instead of read back from a stored copy. This
/// is the onboarding path, where the realm documents are what the device is
/// still missing.
pub async fn fetch_from_peers(
    context: &Arc<DriverContext>,
    owner: UserId,
    peers: Vec<NodeId>,
    budget: Duration,
) -> bool {
    let Some(plan) = build_plan(context, owner, peers).await else {
        return false;
    };
    fetch_with_plan(context, plan, budget).await
}

async fn fetch_with_plan(context: &Arc<DriverContext>, plan: FetchPlan, budget: Duration) -> bool {
    let Ok(selection) = tokio::time::timeout(budget, ask_realm(context, &plan)).await else {
        return false;
    };
    // The install is deliberately outside the budget: half of it would leave the
    // device with a configuration and an authorization document from two ages.
    match (selection.current, selection.behind) {
        (Some(accepted), _) => install_documents(context, &plan, accepted, 0).await,
        (None, Some(behind)) => rebase_or_wait(context, &plan, behind).await,
        (None, None) => false,
    }
}

/// Takes a copy the marker does not cover only once the realm has answered the
/// same way often enough. Until then the attempt is counted and nothing moves.
async fn rebase_or_wait(context: &Arc<DriverContext>, plan: &FetchPlan, behind: Accepted) -> bool {
    let (rebase, attempts) = rebase_after(plan.marker.behind);
    if !rebase {
        let marker = RealmMarker {
            clock: plan.marker.clock.clone(),
            behind: attempts,
        };
        store_marker(context, plan.realm_id, &marker).await;
        debug!(
            attempts,
            "Every realm node answered below this device's marker"
        );
        return false;
    }
    warn!("Re-basing this device's realm marker on what its realm answers");
    install_documents(context, plan, behind, 0).await
}

/// Whether an attempt that found only copies below the marker may re-base it,
/// and what the attempt count becomes.
fn rebase_after(behind: u32) -> (bool, u32) {
    let attempts = behind.saturating_add(1);
    match attempts >= REBASE_ATTEMPTS {
        true => (true, 0),
        false => (false, attempts),
    }
}

async fn fetch_plan(context: &Arc<DriverContext>) -> Option<FetchPlan> {
    let net_handle = context.net_handle.as_ref()?;
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let config = load_realm_config(context, realm_id).await?;
    let owner = node_kind(&config, node_id).and_then(|kind| kind.owner())?;
    let mut peers = realm_peers(&config, node_id);
    // A different node answers first every time, so one lagging peer never owns
    // this device's view of the realm.
    peers.shuffle(&mut rand::rng());
    build_plan(context, owner, peers).await
}

async fn build_plan(
    context: &Arc<DriverContext>,
    owner: UserId,
    peers: Vec<NodeId>,
) -> Option<FetchPlan> {
    let net_handle = context.net_handle.as_ref()?;
    context.metadata_handle.as_ref()?;
    let realm_id = *net_handle.realm_id();
    Some(FetchPlan {
        realm_id,
        node_id: net_handle.node_id(),
        owner,
        auth: AuthContext {
            user_id: owner,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        peers,
        marker: installed_marker(context, realm_id).await,
    })
}

/// Asks the realm's nodes in turn, stopping at the first copy that covers the
/// installed marker.
async fn ask_realm(context: &Arc<DriverContext>, plan: &FetchPlan) -> Selection {
    let mut selection = Selection::default();
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return selection;
    };
    for peer in &plan.peers {
        let message = MetadataTransportMessage::FetchRealmDocuments {
            auth_token: MetadataAuthToken::internal(plan.auth.clone()),
        };
        match metadata.request_forwarded_write(*peer, message).await {
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Ok(documents),
            }) => {
                let Some(accepted) = accept(documents, plan.realm_id) else {
                    warn!(peer = %peer, "A realm node served a configuration this device refuses");
                    continue;
                };
                if selection.offer(accepted, &plan.marker.clock) {
                    return selection;
                }
                debug!(peer = %peer, "A realm node offered an older realm configuration");
            }
            Ok(MetadataTransportMessage::FetchedRealmDocuments {
                result: Err(refusal),
            }) => {
                debug!(peer = %peer, refusal = ?refusal, "A realm node refused the document fetch");
                // An authorization verdict is the same at every peer.
                if matches!(refusal, SyncRefusal::Unauthorized | SyncRefusal::Forbidden) {
                    return Selection::default();
                }
            }
            Ok(other) => {
                debug!(
                    peer = %peer,
                    message = %crate::metadata::transport_message_kind(&other),
                    "Unexpected answer to a document fetch"
                );
            }
            Err(error) => {
                debug!(peer = %peer, error = %error, "Could not reach a realm node for the documents");
            }
        }
    }
    selection
}

/// Decodes one answer and keeps it only if it is this realm's configuration.
/// The clock it carries is trimmed to the nodes that configuration names, so a
/// peer cannot inflate the marker with origins the realm does not have.
fn accept(mut documents: RealmDocuments, realm_id: RealmId) -> Option<Accepted> {
    let config = RealmConfigDocument::from_bytes(&documents.realm_config).ok()?;
    if config.realm_id != realm_id {
        return None;
    }
    documents.groups.truncate(MAX_DEVICE_GROUPS);
    let clock = trim_clock(&documents.clock, &realm_origins(&config));
    Some(Accepted {
        documents,
        config,
        clock,
    })
}

/// Whether `offered` has seen everything `installed` has, counting the realm's
/// own nodes alone. An origin the realm does not name is one a peer invented,
/// and it never keeps a legitimate copy out.
fn covers(
    offered: &AdminDocumentClock,
    installed: &AdminDocumentClock,
    origins: &BTreeSet<NodeId>,
) -> bool {
    installed
        .origins
        .iter()
        .filter(|(origin, _)| origins.contains(*origin))
        .all(|(origin, seq)| offered.sequence_for(origin) >= *seq)
}

/// The same clock without the origins the realm does not name.
fn trim_clock(clock: &AdminDocumentClock, origins: &BTreeSet<NodeId>) -> AdminDocumentClock {
    AdminDocumentClock {
        origins: clock
            .origins
            .iter()
            .filter(|(origin, _)| origins.contains(*origin))
            .map(|(origin, seq)| (*origin, *seq))
            .collect(),
    }
}

/// The nodes that may author the realm's documents, as one configuration names
/// them.
fn realm_origins(config: &RealmConfigDocument) -> BTreeSet<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .collect()
}

/// The realm's own nodes, in configuration order. A device asks infrastructure
/// only: another device holds nothing it could serve.
fn realm_peers(config: &RealmConfigDocument, node_id: NodeId) -> Vec<NodeId> {
    config
        .nodes
        .iter()
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| NodeId::from_str(&node.node_id).ok())
        .filter(|peer| *peer != node_id)
        .collect()
}

/// Writes the fetched copies where every local read already looks for them, in
/// one batch with the marker they were accepted at: a half-installed copy would
/// leave the device judging itself by two ages of the realm at once.
async fn install_documents(
    context: &Arc<DriverContext>,
    plan: &FetchPlan,
    accepted: Accepted,
    behind: u32,
) -> bool {
    let mut config = accepted.config;
    let stored_config = read_bytes(
        context,
        DocumentSyncTarget::RealmConfig {
            realm_id: plan.realm_id,
        },
    )
    .await;
    if let Some(installed) = stored_config
        .as_deref()
        .and_then(|bytes| RealmConfigDocument::from_bytes(bytes).ok())
    {
        keep_revocations(&mut config, &installed, unix_timestamp_secs());
    }
    let actor = Actor {
        node_id: plan.node_id,
        user_id: plan.owner,
        realm_id: plan.realm_id,
    };
    let Ok(bytes) = config.to_bytes(&actor) else {
        warn!("A fetched realm configuration could not be stored");
        return false;
    };
    let marker = RealmMarker {
        clock: accepted.clock,
        behind,
    };
    let Ok(marker_bytes) = postcard::to_allocvec(&marker) else {
        return false;
    };
    let mut writes = vec![(
        DEVICE_REALM_MARKER_KEYSPACE.to_string(),
        Key::from(plan.realm_id.as_bytes().to_vec()),
        Value::from(marker_bytes),
    )];

    let stored_authorization = read_bytes(
        context,
        DocumentSyncTarget::RealmAuthorization {
            realm_id: plan.realm_id,
        },
    )
    .await;
    let owner_target = DocumentSyncTarget::User {
        user_id: plan.owner,
    };
    let stored_owner = read_bytes(context, owner_target.clone()).await;
    let stored_groups = installed_group_docs(context).await;
    let stored_urls = installed_management_urls(context, plan.realm_id).await;
    if !install_node_infos(context, &accepted.documents.node_infos).await {
        return false;
    }
    let unchanged = stored_config.as_deref() == Some(bytes.as_slice())
        && stored_authorization.as_deref() == accepted.documents.realm_authorization.as_deref()
        && stored_owner.as_deref() == accepted.documents.owner.as_deref()
        && stored_groups == accepted.documents.groups
        && stored_urls == accepted.documents.management_urls;
    if unchanged {
        // Nothing but the marker moves: writing the documents again would
        // re-register every realm peer on every beat for a copy this device
        // already holds.
        if marker == plan.marker {
            return true;
        }
        debug!("This device already holds the realm documents it fetched");
        return write_batch(context, writes).await;
    }

    writes.push((
        DocumentSyncTarget::RealmConfig {
            realm_id: plan.realm_id,
        }
        .storage_keyspace()
        .to_string(),
        DocumentSyncTarget::RealmConfig {
            realm_id: plan.realm_id,
        }
        .storage_key(),
        Value::from(bytes),
    ));
    if let Some(authorization) = accepted.documents.realm_authorization {
        writes.push((
            DocumentSyncTarget::RealmAuthorization {
                realm_id: plan.realm_id,
            }
            .storage_keyspace()
            .to_string(),
            DocumentSyncTarget::RealmAuthorization {
                realm_id: plan.realm_id,
            }
            .storage_key(),
            Value::from(authorization),
        ));
    }
    if let Some(owner) = accepted.documents.owner {
        writes.push((
            owner_target.storage_keyspace().to_string(),
            owner_target.storage_key(),
            Value::from(owner),
        ));
    }
    for documents in &accepted.documents.groups {
        let Some(group_writes) = group_doc_writes(documents, &actor) else {
            warn!(group_id = %documents.group.group_id, "A fetched group could not be stored");
            return false;
        };
        writes.extend(group_writes);
    }
    let Ok(url_bytes) = postcard::to_allocvec(&accepted.documents.management_urls) else {
        warn!("The fetched management urls could not be stored");
        return false;
    };
    writes.push((
        DEVICE_MANAGEMENT_URL_KEYSPACE.to_string(),
        Key::from(plan.realm_id.as_bytes().to_vec()),
        Value::from(url_bytes),
    ));
    if !write_batch(context, writes).await {
        return false;
    }
    // The peer set and the node kinds this device enforces follow the copy it
    // just installed, exactly as they follow a synced one on a realm node.
    if let Some(net_handle) = context.net_handle.as_ref()
        && let Err(error) = net_handle.refresh_realm_peers_from_document(&config).await
    {
        warn!(error = %error, "Failed to apply the fetched realm configuration");
    }
    debug!(owner = %plan.owner, "Installed the realm documents on this device");
    true
}

/// Carries the device's own revocations into the copy it is about to install.
/// A revoked token stays revoked here whatever the answer forgot, and the floor
/// only ever rises.
fn keep_revocations(fetched: &mut RealmConfigDocument, installed: &RealmConfigDocument, now: u64) {
    fetched.revocation_floor = fetched.revocation_floor.max(installed.revocation_floor);
    for entry in &installed.revoked_tokens {
        if !revocation_live(entry.expires_at, now) {
            continue;
        }
        if !fetched
            .revoked_tokens
            .iter()
            .any(|kept| kept.token_hash == entry.token_hash)
        {
            fetched.revoked_tokens.push(entry.clone());
        }
    }
}

/// The group documents this device holds, in the group-id order both the scan
/// and a serving node's listing produce, so a fetched set compares directly.
/// A group whose two rows disagree is treated as absent and reinstalled.
async fn installed_group_docs(context: &Arc<DriverContext>) -> Vec<DeviceGroupDocuments> {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: GROUP_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: GROUP_SCAN_LIMIT,
            txn_id: None,
        })
        .await
    else {
        return Vec::new();
    };
    let mut documents = Vec::with_capacity(values.len());
    for (_, value) in values {
        let Ok(group) = Group::from_bytes(&value) else {
            continue;
        };
        let read = read_bytes(
            context,
            DocumentSyncTarget::GroupAuthorization {
                group_id: group.group_id,
            },
        )
        .await;
        let Some(authorization) = read
            .as_deref()
            .and_then(|bytes| GroupAuthorizationDocument::from_bytes(bytes).ok())
        else {
            continue;
        };
        documents.push(DeviceGroupDocuments {
            group,
            authorization,
        });
    }
    documents
}

/// The two rows one group occupies, encoded as a realm node encodes them.
fn group_doc_writes(
    documents: &DeviceGroupDocuments,
    actor: &Actor,
) -> Option<[(String, Key, Value); 2]> {
    let group_id = documents.group.group_id;
    let group = DocumentSyncTarget::Group { group_id };
    let authorization = DocumentSyncTarget::GroupAuthorization { group_id };
    Some([
        (
            group.storage_keyspace().to_string(),
            group.storage_key(),
            Value::from(documents.group.to_bytes(actor).ok()?),
        ),
        (
            authorization.storage_keyspace().to_string(),
            authorization.storage_key(),
            Value::from(documents.authorization.to_bytes(actor).ok()?),
        ),
    ])
}

/// Writes one group's documents where every local read already looks for them,
/// so a group the owner just created answers before the next fetch.
pub async fn install_group_docs(
    context: &Arc<DriverContext>,
    actor: &Actor,
    group: &Group,
    authorization: &GroupAuthorizationDocument,
) -> bool {
    let documents = DeviceGroupDocuments {
        group: group.clone(),
        authorization: authorization.clone(),
    };
    let Some(writes) = group_doc_writes(&documents, actor) else {
        return false;
    };
    write_batch(context, writes.to_vec()).await
}

async fn install_node_infos(context: &Arc<DriverContext>, documents: &[NodeInfoDocument]) -> bool {
    for document in documents {
        match read_node_info_document(&context.storage_handle, document.node_id).await {
            Ok(Some(stored)) if stored == *document => continue,
            Ok(_) => {}
            Err(error) => {
                warn!(
                    node_id = %document.node_id,
                    %error,
                    "Failed to read a fetched node info document"
                );
                return false;
            }
        }
        if let Err(error) = write_node_info_document(&context.storage_handle, document).await {
            warn!(
                node_id = %document.node_id,
                %error,
                "Failed to install a fetched node info document"
            );
            return false;
        }
    }
    true
}

/// The management api urls a realm node last served this device, in the order
/// it served them. A device holds no peer node-info document, so relaying a
/// management-only route has no other target.
pub async fn installed_management_urls(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
) -> Vec<String> {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_MANAGEMENT_URL_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        return Vec::new();
    };
    postcard::from_bytes(&bytes).unwrap_or_default()
}

/// What the copy this device holds was accepted at. An absent marker reads as
/// nothing seen, which accepts the first copy and refuses nothing.
async fn installed_marker(context: &Arc<DriverContext>, realm_id: RealmId) -> RealmMarker {
    let Event::Storage(StorageEvent::ReadResult {
        value: Some(bytes), ..
    }) = context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: DEVICE_REALM_MARKER_KEYSPACE.to_string(),
            key: realm_id.as_bytes().to_vec().into(),
            txn_id: None,
        })
        .await
    else {
        return RealmMarker::default();
    };
    postcard::from_bytes(&bytes).unwrap_or_default()
}

async fn store_marker(context: &Arc<DriverContext>, realm_id: RealmId, marker: &RealmMarker) {
    let Ok(bytes) = postcard::to_allocvec(marker) else {
        return;
    };
    write_batch(
        context,
        vec![(
            DEVICE_REALM_MARKER_KEYSPACE.to_string(),
            Key::from(realm_id.as_bytes().to_vec()),
            Value::from(bytes),
        )],
    )
    .await;
}

/// One stored document, or `None` when this device holds it not (yet).
async fn read_bytes(context: &Arc<DriverContext>, target: DocumentSyncTarget) -> Option<Vec<u8>> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            value.map(|bytes| bytes.as_ref().to_vec())
        }
        other => {
            warn!(event = ?other, "Failed to read an installed realm document");
            None
        }
    }
}

async fn write_batch(context: &Arc<DriverContext>, writes: Vec<(String, Key, Value)>) -> bool {
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    if matches!(event, Event::Storage(StorageEvent::BatchWriteResult { .. })) {
        return true;
    }
    warn!(event = ?event, "Failed to install the fetched realm documents");
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::{RealmNodeKind, TokenRevocation};
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9u8; 32])
    }

    fn clock(entries: &[(u8, u64)]) -> AdminDocumentClock {
        let mut clock = AdminDocumentClock::default();
        for (seed, seq) in entries {
            clock.advance(node(*seed), *seq);
        }
        clock
    }

    fn config(nodes: &[u8]) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(realm(), Vec::new(), 1);
        for seed in nodes {
            config.ensure_node(node(*seed), RealmNodeKind::Server);
        }
        config
    }

    fn answer(nodes: &[u8], seen: &[(u8, u64)]) -> Accepted {
        let config = config(nodes);
        let documents = RealmDocuments {
            realm_config: config
                .to_bytes(&Actor {
                    node_id: node(1),
                    user_id: UserId::nil(realm()),
                    realm_id: realm(),
                })
                .expect("config encodes"),
            realm_authorization: None,
            owner: None,
            groups: Vec::new(),
            node_infos: Vec::new(),
            management_urls: Vec::new(),
            clock: clock(seen),
        };
        accept(documents, realm()).expect("the answer is this realm's")
    }

    fn answer_groups(nodes: &[u8], groups: Vec<DeviceGroupDocuments>) -> Accepted {
        let mut accepted = answer(nodes, &[]);
        accepted.documents.groups = groups;
        accepted
    }

    // A copy that has seen less than the installed one is a rollback, whoever
    // serves it: the realm's own lagging node and an evicted one look the same.
    #[test]
    fn refuses_older_copy() {
        let origins = realm_origins(&config(&[1, 2, 3]));
        let installed = clock(&[(1, 7), (2, 3)]);
        assert!(covers(&installed, &installed, &origins));
        assert!(covers(
            &clock(&[(1, 8), (2, 3), (3, 1)]),
            &installed,
            &origins
        ));
        assert!(!covers(&clock(&[(1, 6), (2, 3)]), &installed, &origins));
        assert!(!covers(&clock(&[(1, 7)]), &installed, &origins));
        // Nothing installed yet accepts the first answer.
        assert!(covers(
            &AdminDocumentClock::default(),
            &AdminDocumentClock::default(),
            &origins
        ));
    }

    // Only the realm's own nodes may appear in a marker: a peer that invents an
    // origin, or one the realm has since evicted, must not be able to lock this
    // device out of every legitimate copy.
    #[test]
    fn ignores_forged_origins() {
        let realm_nodes = config(&[1, 2]);
        let origins = realm_origins(&realm_nodes);
        let inflated = clock(&[(1, 4), (7, u64::MAX)]);
        assert!(covers(&clock(&[(1, 4)]), &inflated, &origins));
        assert!(
            !trim_clock(&inflated, &origins)
                .origins
                .contains_key(&node(7))
        );

        // A copy is stored under the trimmed clock, so nothing forged is kept.
        let accepted = answer(&[1, 2], &[(1, 4), (7, u64::MAX)]);
        assert_eq!(accepted.clock, clock(&[(1, 4)]));
    }

    // A marker can only be too high because a peer lied, so the realm agreeing
    // against it is what makes it wrong - and only after it keeps saying so.
    #[test]
    fn rebases_after_agreement() {
        assert_eq!(rebase_after(0), (false, 1));
        assert_eq!(rebase_after(1), (false, 2));
        assert_eq!(rebase_after(2), (true, 0));
    }

    // One lagging node among fresh ones is not the realm disagreeing: the fresh
    // copy is taken and nothing is ever re-based on the lagging one.
    #[test]
    fn keeps_fresh_over_lagging() {
        let installed = clock(&[(1, 5)]);
        let mut selection = Selection::default();

        assert!(!selection.offer(answer(&[1, 2], &[(1, 3)]), &installed));
        assert!(selection.offer(answer(&[1, 2], &[(1, 6)]), &installed));
        assert_eq!(
            selection.current.expect("the fresh copy is taken").clock,
            clock(&[(1, 6)])
        );

        // Different older answers do not corroborate one another.
        let mut agreed = Selection::default();
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 2)]), &installed));
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 4)]), &installed));
        assert!(agreed.current.is_none());
        assert!(agreed.behind.is_none());

        // Two peers returning the same bundle may repair an inflated marker.
        assert!(!agreed.offer(answer(&[1, 2], &[(1, 4)]), &installed));
        assert_eq!(
            agreed.behind.expect("a candidate to re-base on").clock,
            clock(&[(1, 4)])
        );
    }

    async fn device(config: &RealmConfigDocument) -> (tempfile::TempDir, Arc<DriverContext>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().expect("path"))
            .expect("storage opens");
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let target = DocumentSyncTarget::RealmConfig { realm_id: realm() };
        let bytes = config
            .to_bytes(&Actor {
                node_id: node(1),
                user_id: UserId::nil(realm()),
                realm_id: realm(),
            })
            .expect("config encodes");
        write_batch(
            &context,
            vec![(
                target.storage_keyspace().to_string(),
                target.storage_key(),
                Value::from(bytes),
            )],
        )
        .await;
        (dir, context)
    }

    // A copy that changed nothing still moves the marker: the device saw more
    // of the realm than the marker said, and the next answer is judged by that.
    #[tokio::test]
    async fn advances_unchanged_marker() {
        let (_dir, context) = device(&config(&[1, 2])).await;
        let plan = FetchPlan {
            realm_id: realm(),
            node_id: node(1),
            owner: UserId::nil(realm()),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            peers: Vec::new(),
            marker: RealmMarker {
                clock: clock(&[(1, 4)]),
                behind: 2,
            },
        };
        let stored = read_bytes(
            &context,
            DocumentSyncTarget::RealmConfig { realm_id: realm() },
        )
        .await
        .expect("the device holds a configuration");

        assert!(install_documents(&context, &plan, answer(&[1, 2], &[(1, 6)]), 0).await);

        assert_eq!(
            installed_marker(&context, realm()).await,
            RealmMarker {
                clock: clock(&[(1, 6)]),
                behind: 0
            }
        );
        assert_eq!(
            read_bytes(
                &context,
                DocumentSyncTarget::RealmConfig { realm_id: realm() }
            )
            .await,
            Some(stored),
            "the documents themselves are not rewritten"
        );
    }

    fn group_docs(seed: u128, display_name: &str) -> DeviceGroupDocuments {
        let group_id = Ulid::from(seed);
        let owner = UserId::nil(realm());
        DeviceGroupDocuments {
            group: Group {
                display_name: display_name.to_string(),
                group_id,
                realm_id: realm(),
                roles: Default::default(),
                owner,
            },
            authorization: GroupAuthorizationDocument::new_default_group_doc(
                owner,
                realm(),
                group_id,
            ),
        }
    }

    fn owner_actor() -> Actor {
        Actor {
            node_id: node(1),
            user_id: UserId::nil(realm()),
            realm_id: realm(),
        }
    }

    // A group the owner created on this device must answer at once, from the
    // same two rows a realm node holds, and a repeat must not duplicate it.
    #[tokio::test]
    async fn installs_created_group() {
        let (_dir, context) = device(&config(&[1])).await;
        let actor = owner_actor();

        let second = group_docs(2, "second");
        assert!(install_group_docs(&context, &actor, &second.group, &second.authorization).await);
        let first = group_docs(1, "first");
        assert!(install_group_docs(&context, &actor, &first.group, &first.authorization).await);
        let renamed = group_docs(2, "renamed");
        assert!(install_group_docs(&context, &actor, &renamed.group, &renamed.authorization).await);

        assert_eq!(
            installed_group_docs(&context).await,
            vec![first, renamed],
            "one pair of rows per group, in group-id order"
        );
    }

    // An unversioned group snapshot cannot prove that an absent group was
    // removed rather than still replicating to the serving node.
    #[tokio::test]
    async fn keeps_unlisted_groups() {
        let (_dir, context) = device(&config(&[1])).await;
        let actor = owner_actor();
        let kept = group_docs(1, "kept");
        let dropped = group_docs(2, "dropped");
        let realm_auth = DocumentSyncTarget::RealmAuthorization { realm_id: realm() };
        assert!(
            write_batch(
                &context,
                vec![(
                    realm_auth.storage_keyspace().to_string(),
                    realm_auth.storage_key(),
                    Value::from(vec![7u8; 8]),
                )],
            )
            .await
        );
        assert!(install_group_docs(&context, &actor, &kept.group, &kept.authorization).await);
        assert!(install_group_docs(&context, &actor, &dropped.group, &dropped.authorization).await);
        let plan = FetchPlan {
            realm_id: realm(),
            node_id: node(1),
            owner: UserId::nil(realm()),
            auth: AuthContext {
                user_id: UserId::nil(realm()),
                realm_id: realm(),
                path_restrictions: None,
                session: None,
            },
            peers: Vec::new(),
            marker: RealmMarker::default(),
        };

        assert!(
            install_documents(&context, &plan, answer_groups(&[1], vec![kept.clone()]), 0).await
        );

        assert_eq!(
            installed_group_docs(&context).await,
            vec![kept, dropped.clone()]
        );
        assert!(
            read_bytes(
                &context,
                DocumentSyncTarget::Group {
                    group_id: dropped.group.group_id
                }
            )
            .await
            .is_some()
        );
        assert_eq!(
            read_bytes(&context, realm_auth).await,
            Some(vec![7u8; 8]),
            "the realm authorization row is not a group row"
        );
    }

    // The explicit plan still needs the handles that name this device and carry
    // the read: an owner and a peer list alone never make one.
    #[tokio::test]
    async fn plan_needs_handles() {
        let (_dir, context) = device(&config(&[1])).await;

        assert!(
            build_plan(&context, UserId::nil(realm()), vec![node(2)])
                .await
                .is_none()
        );
    }

    fn revoked(revoked: &[(&str, u64)], floor: u64) -> RealmConfigDocument {
        let mut config = config(&[1]);
        config.revocation_floor = floor;
        config.revoked_tokens = revoked
            .iter()
            .map(|(hash, expires_at)| TokenRevocation {
                token_hash: (*hash).to_string(),
                expires_at: *expires_at,
            })
            .collect();
        config
    }

    // A revocation this device already holds must survive a copy that lost it,
    // and an expired one must not come back with it.
    #[test]
    fn keeps_local_revocations() {
        let now = 1_000;
        let installed = revoked(&[("live", now + 60), ("gone", now - 60)], 900);
        let mut fetched = revoked(&[("other", now + 60)], 800);

        keep_revocations(&mut fetched, &installed, now);

        let hashes: Vec<&str> = fetched
            .revoked_tokens
            .iter()
            .map(|entry| entry.token_hash.as_str())
            .collect();
        assert!(
            hashes.contains(&"live"),
            "a live revocation may not be lost"
        );
        assert!(hashes.contains(&"other"));
        assert!(!hashes.contains(&"gone"), "an expired entry stays pruned");
        assert_eq!(fetched.revocation_floor, 900);
    }
}
