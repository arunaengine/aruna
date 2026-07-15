use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::document::{
    DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncOutboxEvent, DocumentSyncRevision,
    DocumentSyncTarget,
};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::metadata::MetadataError;
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, PlacementRef, RealmAuthorizationDocument, RealmConfigDocument,
    RealmId, RealmNodeKind, TokenClaims,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
    mint_forward_document_id, mint_local_document_id,
};
use aruna_operations::document_sync_outbox::{
    new_outbox_record, outbox_key, read_outbox_record, schedule_outbox_drain_effect,
    write_outbox_effect,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::forward::{
    MetadataWriteError, create_metadata_document_routed, update_metadata_document_routed,
};
use aruna_operations::metadata::{MetadataAuthToken, MetadataHandle};
use aruna_operations::placement::resolve_shard_holders;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_operations::update_metadata_document::{
    UpdateMetadataDocumentError, UpdateMetadataDocumentMutation,
};
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

// Every wait below polls to a condition; the ceiling only bounds a genuine
// hang. Convergence measures single-digit seconds, but a loaded CI runner can
// stall consecutive peer syncs for the full 30s peer-sync timeout each, so the
// backstop is 2-3x that timeout, not the expected latency.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);

fn actor_of(realm: &Realm, node: &TestNode) -> Actor {
    Actor {
        node_id: node.net.node_id(),
        user_id: realm.user_id,
        realm_id: realm.realm_id,
    }
}

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
    sync_eligible: bool,
}

/// A create from a User-kind node lands on a holder, over the wire.
///
/// User nodes are never sync-eligible, so they hold no bucket and can publish
/// nothing: every write they take must be forwarded. That makes them the case the
/// forwarding path exists for, and the case a sync-eligibility gate on the receive
/// side would reject. The holder applies the create under the caller's own bearer
/// token and re-runs the same permission check the origin's HTTP handler would.
#[tokio::test]
async fn user_node_forwards_create() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");

    let group_id = seed_group(&realm, &nodes).await?;
    let document_id = mint_forward_document_id(
        &config,
        &actor_of(&realm, user_node),
        group_id,
        "datasets/forwarded",
    )?;
    let created = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;

    // The user node holds nothing, so the record it got back was written by a
    // holder, not locally: the holder set must exclude the forwarder.
    let holders = resolve_shard_holders(&config, &created.placement);
    assert!(!holders.contains(&user_node.net.node_id()));
    assert!(!holders.is_empty());

    // Whichever holder answered the forward publishes the create onto the bucket's
    // topic, so it reaches every holder of that bucket.
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut pending = false;
        for node in nodes
            .iter()
            .filter(|node| holders.contains(&node.net.node_id()))
        {
            match registry_record(node, document_id).await? {
                Some(stored) => {
                    assert_eq!(stored.document_id, document_id);
                    assert_eq!(stored.group_id, group_id);
                    assert_eq!(stored.placement, created.placement);
                }
                None => pending = true,
            }
        }
        if !pending {
            break;
        }
        if Instant::now() >= deadline {
            return Err("the forwarded create never reached every holder".into());
        }
        sleep(Duration::from_millis(50)).await;
    }

    shutdown(nodes).await;
    Ok(())
}

#[tokio::test]
async fn forwarded_invalid_terminal() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");
    let group_id = seed_group(&realm, &nodes).await?;
    let document_id = mint_forward_document_id(
        &config,
        &actor_of(&realm, user_node),
        group_id,
        "datasets/forwarded",
    )?;
    let record = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;
    let holders = resolve_shard_holders(&config, &record.placement);
    wait_for_record_on_holders(&nodes, &holders, document_id).await?;

    let error = update_metadata_document_routed(
        &user_node.context,
        Actor {
            node_id: user_node.net.node_id(),
            user_id: realm.user_id,
            realm_id: realm.realm_id,
        },
        &record,
        None,
        UpdateMetadataDocumentMutation::UpsertDataEntity {
            jsonld: "{}".to_string(),
        },
        Some(realm.bearer_token()),
    )
    .await
    .expect_err("invalid forwarded update must fail");

    assert!(
        matches!(
            &error,
            MetadataWriteError::Update(UpdateMetadataDocumentError::MetadataError(
                MetadataError::InvalidInput(_)
            ))
        ),
        "unexpected forwarded update error: {error:?}"
    );

    shutdown(nodes).await;
    Ok(())
}

/// A group created on a User-kind node is never silently lost.
///
/// A User node holds no bucket of any group, so its admin-operation outbox
/// records can never publish from here. There is no outbox relay to hand them to
/// a holder either: a peer-relayed publish carries no proof it was
/// permission-checked, so the tokenless relay was removed. The record therefore
/// stays in the outbox, retried and loud, instead of being deleted after the
/// caller was told the group exists. Creating groups from a User node needs a
/// signed-origin admin path in the net layer; until then this test pins the
/// invariant that matters — the write is never thrown away.
#[tokio::test]
async fn user_node_group_survives() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, _config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");

    let (group, _auth) = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: user_node.net.node_id(),
                user_id: realm.user_id,
                realm_id: realm.realm_id,
            },
            display_name: "group from a user node".to_string(),
            owner_cap: None,
        }),
        user_node.context.as_ref(),
    )
    .await?;

    // Give the drain time to run: it must not have deleted the records.
    sleep(Duration::from_secs(2)).await;
    assert!(read_group_auth(user_node, group.group_id).await?.is_some());
    assert!(
        outbox_len(user_node).await? > 0,
        "the user node's unpublishable admin records must be retained, not dropped"
    );

    shutdown(nodes).await;
    Ok(())
}

async fn outbox_len(node: &TestNode) -> Result<usize, Box<dyn std::error::Error>> {
    match node
        .context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Iter {
            key_space: aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE.to_string(),
            prefix: None,
            start: None,
            limit: 64,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(values.len()),
        other => Err(format!("unexpected outbox iter event: {other:?}").into()),
    }
}

/// Re-forwarding a create that already applied returns the existing record,
/// even when the retry lands on a different holder.
///
/// The forward always offers the create to the bucket's rank-0 holder first, so
/// a plain second attempt would just hit that same holder again. Taking rank-0
/// down after the first create forces the retry onto a co-holder that never
/// created the document itself but received it by sync; it must replay the record
/// rather than fork a second one onto another topic. Node count is beside the
/// point — skipping rank-0 is what makes the holder differ — so this stays at the
/// minimum realm that leaves a holder to fail over to.
#[tokio::test]
async fn forwarded_create_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");

    let group_id = seed_group(&realm, &nodes).await?;
    let document_id = mint_forward_document_id(
        &config,
        &actor_of(&realm, user_node),
        group_id,
        "datasets/forwarded",
    )?;

    let first = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;
    let holders = resolve_shard_holders(&config, &first.placement);
    assert_eq!(holders.len(), 3);

    // Every holder must carry the row before the retry, so a co-holder that never
    // created the document can still replay it.
    wait_for_record_on_holders(&nodes, &holders, document_id).await?;

    // Rank-0 answered the first forward; taking it down routes the retry to a
    // different holder.
    let rank0 = nodes
        .iter()
        .find(|node| node.net.node_id() == holders[0])
        .expect("rank-0 holder is a node");
    rank0.net.shutdown().await;

    let second = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;

    assert_eq!(second.document_id, first.document_id);
    assert_eq!(second.placement, first.placement);
    assert_eq!(second.last_event_id, first.last_event_id);
    assert_eq!(second.created_at_ms, first.created_at_ms);

    shutdown(nodes).await;
    Ok(())
}

#[tokio::test]
async fn create_replay_rejects() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");
    let first_group = seed_group(&realm, &nodes).await?;
    let other_group = seed_group(&realm, &nodes).await?;
    let document_id = mint_forward_document_id(
        &config,
        &actor_of(&realm, user_node),
        first_group,
        "datasets/forwarded",
    )?;

    let created = drive_forwarded_create(&realm, user_node, first_group, document_id).await?;
    let holders = resolve_shard_holders(&config, &created.placement);
    wait_for_record_on_holders(&nodes, &holders, document_id).await?;

    let group_error = drive_forwarded_create_at(
        &realm,
        user_node,
        other_group,
        document_id,
        "datasets/forwarded",
    )
    .await
    .expect_err("a document id cannot replay a record from another group");
    assert!(matches!(
        group_error.downcast_ref::<MetadataWriteError>(),
        Some(MetadataWriteError::Undeliverable(_))
    ));

    let path_error = drive_forwarded_create_at(
        &realm,
        user_node,
        first_group,
        document_id,
        "datasets/other",
    )
    .await
    .expect_err("a document id cannot replay a record from another path");
    assert!(matches!(
        path_error.downcast_ref::<MetadataWriteError>(),
        Some(MetadataWriteError::Undeliverable(_))
    ));

    shutdown(nodes).await;
    Ok(())
}

fn forged_delete_change(placement: PlacementRef, actor: NodeId) -> DocumentSyncChange {
    DocumentSyncChange {
        base: None,
        current: DocumentSyncRevision {
            generation: 1,
            event_id: Ulid::r#gen(),
            actor,
            updated_at_ms: 1,
        },
        kind: DocumentSyncChangeKind::Delete,
        placement,
    }
}

async fn wait_for_record_on_holders(
    nodes: &[TestNode],
    holders: &[NodeId],
    document_id: MetaResourceId,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut pending = false;
        for node in nodes
            .iter()
            .filter(|node| holders.contains(&node.net.node_id()))
        {
            if registry_record(node, document_id).await?.is_none() {
                pending = true;
            }
        }
        if !pending {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("the create never reached every holder".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// A node holding no bucket of a document can resolve it, and cannot be tricked
/// into relaying a forged delete for it.
///
/// Five nodes at replication factor three leaves two nodes outside every
/// document's bucket. Registry rows are the only thing that tells such a node the
/// document exists at all, and they ride the registry class's own everywhere-bound
/// topic rather than the document's capped bucket. Without them a read through a
/// non-holder 404s forever and an update through it cannot even load the record it
/// needs in order to forward. The same non-holder must never publish a delete for
/// that bucket: the tokenless outbox relay is gone, so a forged delete it plants
/// in its own outbox stays there, unpublished, and the document survives.
#[tokio::test]
async fn nonholder_resolves_document() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 5, 0).await?;
    let group_id = Ulid::r#gen();
    let document_id = mint_local_document_id(
        &config,
        &actor_of(&realm, &nodes[0]),
        group_id,
        "datasets/nonholder",
    )?;

    let created = drive(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: realm.user_id,
                realm_id: realm.realm_id,
            },
            group_id,
            document_id,
            document_path: "datasets/nonholder".to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Non Holder".to_string(),
                description: "Resolvable from outside its bucket".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        nodes[0].context.as_ref(),
    )
    .await?
    .record;

    let holders = resolve_shard_holders(&config, &created.placement);
    assert_eq!(holders.len(), 3);
    let outsider = nodes
        .iter()
        .find(|node| !holders.contains(&node.net.node_id()))
        .expect("replica 3 of 5 nodes leaves a non-holder");

    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        match registry_record(outsider, document_id).await? {
            Some(record) => {
                assert_eq!(record.document_id, document_id);
                assert_eq!(record.placement, created.placement);
                // The point of the row: the non-holder can route to the holders.
                assert_eq!(resolve_shard_holders(&config, &record.placement), holders);
                break;
            }
            None if Instant::now() >= deadline => {
                return Err("the registry row never reached a non-holder".into());
            }
            None => sleep(Duration::from_millis(50)).await,
        }
    }

    // Plant a delete for the document's bucket into the non-holder's outbox: a
    // record it can never publish and that the removed relay used to hand to a
    // holder to publish under the holder's signature.
    let forged = new_outbox_record(
        outsider.net.node_id(),
        DocumentSyncTarget::MetadataDocumentLifecycle { document_id },
        Vec::new(),
        DocumentSyncOutboxEvent::Delete {
            change: forged_delete_change(created.placement, outsider.net.node_id()),
        },
        created.placement,
        false,
    );
    let key = outbox_key(&forged).to_vec();
    match outsider
        .context
        .storage_handle
        .send_effect(write_outbox_effect(&forged)?)
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => return Err(format!("unexpected forged outbox write event: {other:?}").into()),
    }
    outsider
        .context
        .task_handle
        .as_ref()
        .expect("task handle")
        .send_effect(schedule_outbox_drain_effect())
        .await;
    sleep(Duration::from_secs(2)).await;

    // Neither relayed nor deleted: the forged record is still in the outbox.
    assert_eq!(
        read_outbox_record(&outsider.context.storage_handle, &key).await?,
        Some(forged),
        "the forged delete must stay in the non-holder's outbox, unpublished"
    );
    // And the document survives on every holder: the forged delete never reached a
    // topic.
    for node in nodes
        .iter()
        .filter(|node| holders.contains(&node.net.node_id()))
    {
        assert!(
            registry_record(node, document_id).await?.is_some(),
            "a forged delete from a non-holder must not remove the document"
        );
    }

    shutdown(nodes).await;
    Ok(())
}

/// A realm whose id is its token-signing key, so tests can mint bearer tokens the
/// forwarded-write path validates exactly as it validates a real caller's.
struct Realm {
    signing_key: SigningKey,
    realm_id: RealmId,
    user_id: UserId,
}

impl Realm {
    fn new() -> Self {
        let signing_key = SigningKey::generate(&mut jsonwebtoken::signature::rand_core::OsRng);
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        Self {
            realm_id,
            user_id: UserId::local(Ulid::r#gen(), realm_id),
            signing_key,
        }
    }

    fn bearer_token(&self) -> MetadataAuthToken {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let claims = TokenClaims {
            sub: self.user_id.to_string(),
            iss: self.realm_id.to_string(),
            iat: now,
            exp: now + 600,
            jti: Ulid::r#gen().to_string(),
            restrictions: None,
            issuer_pubkey: None,
            delegation_signature: None,
        };
        let key_pem = self
            .signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("realm key encodes");
        let token = encode(
            &Header::new(Algorithm::EdDSA),
            &claims,
            &EncodingKey::from_ed_pem(key_pem.as_bytes()).expect("realm key is an ed25519 key"),
        )
        .expect("token signs");
        MetadataAuthToken::bearer(token).expect("token is within the length bound")
    }
}

/// A group owned by the realm's user, replicated to every node. The holder of a
/// forwarded write re-runs the caller's permission check against this group's
/// authorization document, read from its own `AUTH_KEYSPACE`.
async fn seed_group(realm: &Realm, nodes: &[TestNode]) -> Result<Ulid, Box<dyn std::error::Error>> {
    let (group, _auth) = drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: nodes[0].net.node_id(),
                user_id: realm.user_id,
                realm_id: realm.realm_id,
            },
            display_name: "forwarding group".to_string(),
            owner_cap: None,
        }),
        nodes[0].context.as_ref(),
    )
    .await?;

    wait_for_group(nodes, group.group_id).await?;
    Ok(group.group_id)
}

/// Waits for the group's authorization document — what the permission check
/// reads — on every sync-eligible node. A User node is never a sync target, so it
/// never receives the group it may itself have created.
async fn wait_for_group(
    nodes: &[TestNode],
    group_id: Ulid,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let mut pending = false;
        for node in nodes.iter().filter(|node| node.sync_eligible) {
            if read_group_auth(node, group_id).await?.is_none() {
                pending = true;
            }
        }
        if !pending {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("the group's authorization document never reached every holder".into());
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn registry_record(
    node: &TestNode,
    document_id: MetaResourceId,
) -> Result<Option<MetadataRegistryRecord>, Box<dyn std::error::Error>> {
    load_metadata_record_by_document(node.context.as_ref(), document_id)
        .await
        .map_err(|error| format!("metadata registry read failed: {error:?}").into())
}

async fn read_group_auth(
    node: &TestNode,
    group_id: Ulid,
) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    match node
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: AUTH_KEYSPACE.to_string(),
            key: group_id.to_bytes().into(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.map(|bytes| bytes.to_vec()))
        }
        other => Err(format!("unexpected group auth read event: {other:?}").into()),
    }
}

async fn drive_forwarded_create(
    realm: &Realm,
    node: &TestNode,
    group_id: Ulid,
    document_id: MetaResourceId,
) -> Result<MetadataRegistryRecord, Box<dyn std::error::Error>> {
    drive_forwarded_create_at(realm, node, group_id, document_id, "datasets/forwarded").await
}

async fn drive_forwarded_create_at(
    realm: &Realm,
    node: &TestNode,
    group_id: Ulid,
    document_id: MetaResourceId,
    document_path: &str,
) -> Result<MetadataRegistryRecord, Box<dyn std::error::Error>> {
    let created = create_metadata_document_routed(
        CreateMetadataDocumentOperation::new(CreateMetadataDocumentConfig {
            actor: Actor {
                node_id: node.net.node_id(),
                user_id: realm.user_id,
                realm_id: realm.realm_id,
            },
            group_id,
            document_id,
            document_path: document_path.to_string(),
            public: true,
            payload: CreateMetadataDocumentPayload::Scaffold {
                name: "Forwarded".to_string(),
                description: "Placed by a holder".to_string(),
                date_published: "2026-01-01".to_string(),
                license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
            },
        }),
        node.context.clone(),
        Some(realm.bearer_token()),
    )
    .await?;
    Ok(created.record)
}

async fn build_realm(
    realm: &Realm,
    servers: usize,
    users: usize,
) -> Result<(Vec<TestNode>, RealmConfigDocument), Box<dyn std::error::Error>> {
    let mut nodes = Vec::with_capacity(servers + users);
    for index in 0..(servers + users) {
        nodes.push(spawn_node(realm.realm_id, index < servers).await?);
    }

    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            nodes[i]
                .net
                .add_peer_addr(nodes[j].net.endpoint_addr())
                .await;
            nodes[j]
                .net
                .add_peer_addr(nodes[i].net.endpoint_addr())
                .await;
        }
    }

    let config = install_realm_config(realm, &nodes).await?;
    Ok((nodes, config))
}

async fn spawn_node(
    realm_id: RealmId,
    sync_eligible: bool,
) -> Result<TestNode, Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let storage = FjallStorage::open(temp_dir.path().to_str().ok_or("invalid temp path")?)?;
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("valid bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await?;
    let task_handle = TaskHandle::new();
    let metadata_handle = MetadataHandle::new(
        temp_dir.path().join("metadata"),
        net.node_id(),
        storage.clone(),
        Some(net.clone()),
        Some(net.document_sync_node()),
        Some(net.document_sync_database()),
    )?;

    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: Some(metadata_handle),
        task_handle: Some(task_handle.clone()),
    });

    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;

    Ok(TestNode {
        _temp_dir: temp_dir,
        net,
        context,
        sync_eligible,
    })
}

async fn install_realm_config(
    realm: &Realm,
    nodes: &[TestNode],
) -> Result<RealmConfigDocument, Box<dyn std::error::Error>> {
    let realm_id = realm.realm_id;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    for node in nodes {
        let kind = if node.sync_eligible {
            RealmNodeKind::Management
        } else {
            RealmNodeKind::User
        };
        config.ensure_node(node.net.node_id(), kind);
    }

    let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    let trusted = HashSet::from([realm_id]);
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        write(
            node,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            config.to_bytes(&actor)?,
        )
        .await?;
        // The realm authorization document the permission check reads first, and
        // the trusted-realm list the forwarded caller's bearer token validates
        // against: both are per-node local state in production too.
        write(
            node,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm_auth.to_bytes(&actor)?,
        )
        .await?;
        write(
            node,
            API_STATE_KEYSPACE,
            TRUSTED_REALMS_LIST_KEY.to_vec(),
            postcard::to_allocvec(&trusted)?,
        )
        .await?;
        node.net.refresh_realm_peers_from_document(&config).await?;
    }

    for _ in 0..5 {
        for node in nodes {
            aruna_operations::startup::restore_shard_subscriptions(
                &node.context,
                node.net.node_id(),
                realm_id,
            )
            .await;
        }
        let mut retry = false;
        for node in nodes {
            retry |= aruna_operations::process_placements::process_shard_placements(
                &node.context,
                realm_id,
                node.net.node_id(),
            )
            .await
            .retry_scheduled;
        }
        if !retry {
            break;
        }
    }

    Ok(config)
}

async fn write(
    node: &TestNode,
    key_space: &str,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    match node
        .context
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        }))
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(format!("unexpected write event in `{key_space}`: {other:?}").into()),
    }
}

async fn shutdown(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
