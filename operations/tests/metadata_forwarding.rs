use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use aruna_core::UserId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{API_STATE_KEYSPACE, AUTH_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::structs::{
    Actor, MetadataRegistryRecord, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    RealmNodeKind, TokenClaims,
};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_operations::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentOperation, CreateMetadataDocumentPayload,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_metadata_document::load_metadata_record_by_document;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::metadata::forward::create_metadata_document_routed;
use aruna_operations::metadata::{MetadataAuthToken, MetadataHandle};
use aruna_operations::placement::resolve_shard_holders;
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use ulid::Ulid;

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(30);

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
    let document_id = Ulid::r#gen();
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

/// A group created on a User-kind node is never silently lost.
///
/// A User node holds no bucket of any group, so its admin-operation outbox
/// records can never publish from here. They also cannot be relayed: receivers
/// authenticate an admin event by requiring its signed publisher to be its
/// `origin_node_id`, so a holder publishing on the emitter's behalf is rejected
/// realm-wide (`validate_replicated_admin_event`). The record therefore stays in
/// the outbox, retried and loud, instead of being deleted after the caller was
/// told the group exists. Creating groups from a User node needs a signed-origin
/// admin relay in the net layer; until then this test pins the invariant that
/// matters — the write is never thrown away.
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

/// Re-forwarding a create that already applied returns the existing record.
///
/// A forward whose response is lost is retried, possibly against a different
/// holder. Every holder of the document's blind-hashed bucket stamps that same
/// bucket, and a holder that already has the document replays it instead of
/// creating a second one, so a retry cannot fork one document across two topics.
#[tokio::test]
async fn forwarded_create_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, _config) = build_realm(&realm, 3, 1).await?;
    let user_node = nodes.last().expect("user node");

    let group_id = seed_group(&realm, &nodes).await?;
    let document_id = Ulid::r#gen();

    let first = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;
    let second = drive_forwarded_create(&realm, user_node, group_id, document_id).await?;

    assert_eq!(second.document_id, first.document_id);
    assert_eq!(second.placement, first.placement);
    assert_eq!(second.last_event_id, first.last_event_id);
    assert_eq!(second.created_at_ms, first.created_at_ms);

    shutdown(nodes).await;
    Ok(())
}

/// A node holding no bucket of a document can still resolve it.
///
/// Five nodes at replication factor three leaves two nodes outside every
/// document's bucket. Registry rows are the only thing that tells such a node the
/// document exists at all, and they ride the registry class's own everywhere-bound
/// topic rather than the document's capped bucket. Without them a read through a
/// non-holder 404s forever and an update through it cannot even load the record it
/// needs in order to forward.
#[tokio::test]
async fn nonholder_resolves_document() -> Result<(), Box<dyn std::error::Error>> {
    let realm = Realm::new();
    let (nodes, config) = build_realm(&realm, 5, 0).await?;
    let group_id = Ulid::r#gen();
    let document_id = Ulid::r#gen();

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
    document_id: Ulid,
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
    document_id: Ulid,
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
            document_path: "datasets/forwarded".to_string(),
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
