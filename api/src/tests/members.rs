//! Builds a distributed bucket over several in-process members, each with its own storage,
//! blob plane, realm transport and S3 server on an ephemeral port, for forwarding tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aruna_blob::blob::BlobHandler;
use aruna_core::credential_encryption::{CredentialEncryptionKey, EncryptedS3Secret};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, NODE_INFO_KEYSPACE};
use aruna_core::metrics::NodeMetrics;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::storage::blob::{Backend, BackendConfig, BucketInfo, UserAccess};
use aruna_core::structs::storage::distribution::presets::shard_layout;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionConfig, DistributionRequest, DistributionSettings,
    DistributionSnapshot, DistributionView, SetRoot, SignedSnapshot, next_snapshot,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization, node_info_key,
};
use aruna_core::{NodeId, UserId};
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::distribution::admit::{AdmitConfig, AdmitOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::bucket::create::CreateBucketOperation;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::cors::CorsConfig;
use crate::s3::server::{S3Server, S3ServerHandle};
use crate::s3::service::ArunaS3Service;
use crate::tests::routes::{seed_group_docs, seed_realm_auth, write_doc};

/// The bucket name every member binds to the set.
pub(crate) const BUCKET: &str = "spire";

/// One member node with its S3 service, called directly as a client's own node.
pub(crate) struct Node {
    _dir: TempDir,
    pub secret: iroh::SecretKey,
    pub context: Arc<DriverContext>,
    pub service: ArunaS3Service,
    pub url: String,
    server: Option<S3ServerHandle>,
}

impl Node {
    pub fn node_id(&self) -> NodeId {
        self.secret.public()
    }

    /// Stops this member's S3 server, so other members cannot reach it.
    pub async fn stop(&mut self) {
        if let Some(mut server) = self.server.take() {
            server.abort();
            server.wait_until_released().await;
        }
    }

    /// This member's bucket as the access hook would hand it to a request.
    pub async fn bucket(&self) -> BucketInfo {
        drive(GetBucketOperation::new(BUCKET.to_string()), &self.context)
            .await
            .expect("member bucket exists")
    }
}

/// A distributed bucket whose ranges, split at `boundaries`, go to the members in turn.
pub(crate) struct Members {
    pub realm_id: RealmId,
    pub group_id: Ulid,
    pub user: UserAccess,
    pub nodes: Vec<Node>,
    pub snapshot: DistributionSnapshot,
    _shutdown: CancellationToken,
}

impl Members {
    /// Lets Everyone read the bucket on every member, by a node glob such as operators set.
    pub async fn publish(&self) {
        use aruna_core::structs::identity::auth::{Permission, Role};
        use aruna_core::structs::identity::group::GroupAuthorizationDocument;
        use aruna_core::structs::storage::blob::bucket_permission_path;
        let owner = self.user.user_identity;
        let mut document =
            GroupAuthorizationDocument::default_group_doc(owner, self.realm_id, self.group_id);
        let first = self.nodes[0].node_id();
        let path = bucket_permission_path(self.realm_id, self.group_id, first, BUCKET)
            .replace(&first.to_string(), "*");
        let role_id = Ulid::from_bytes([76; 16]);
        let role = Role {
            role_id,
            name: "public".to_string(),
            permissions: HashMap::from([
                (path.clone(), Permission::READ),
                (format!("{path}/**"), Permission::READ),
            ]),
            assigned_users: std::collections::HashSet::from([UserId::nil(self.realm_id)]),
        };
        document.roles.insert(role_id, role);
        for node in &self.nodes {
            let actor = Actor {
                node_id: node.node_id(),
                user_id: owner,
                realm_id: self.realm_id,
            };
            let value = document.to_bytes(&actor).unwrap();
            let key = aruna_core::keyspaces::AUTH_KEYSPACE;
            write_doc(
                &node.context,
                key,
                self.group_id.to_bytes().into(),
                value.into(),
            )
            .await;
        }
    }

    /// The caller's credential as the node it is used on issued it.
    pub fn user_on(&self, node: usize) -> UserAccess {
        UserAccess {
            issued_by: *self.nodes[node].node_id().as_bytes(),
            ..self.user.clone()
        }
    }
}

struct Started {
    dir: TempDir,
    secret: iroh::SecretKey,
    context: Arc<DriverContext>,
    listener: TcpListener,
}

async fn start(realm_id: RealmId, seed: u8) -> Started {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_str().unwrap().to_string();
    let storage = aruna_storage::FjallStorage::open(&root).unwrap();
    storage.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    let secret = iroh::SecretKey::from_bytes(&[seed; 32]);
    let config = NetConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        secret_key: Some(secret.clone()),
        realm_id,
        discovery_method: DiscoveryMethod::None,
        relay_method: RelayMethod::None,
        ..NetConfig::default()
    };
    let net = NetHandle::new(config, storage.clone()).await.unwrap();
    let backend = BackendConfig {
        backend_type: Backend::FileSystem,
        root: format!("{root}/blobs"),
        service_config: HashMap::new(),
        bucket_prefix: Some("aruna-".to_string()),
        max_bucket_size: Some(1_000_000),
        multipart_bucket: Some("multipart".to_string()),
        timeouts: Default::default(),
    };
    let blob = BlobHandler::new(backend, storage.clone(), net.clone())
        .await
        .unwrap();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net),
        blob_handle: Some(blob),
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    Started {
        dir,
        secret,
        context,
        listener,
    }
}

fn info_document(node_id: NodeId, url: &str) -> NodeInfoDocument {
    NodeInfoDocument {
        node_id,
        executors: Vec::new(),
        labels: Default::default(),
        urls: NodeUrls {
            api: None,
            s3: Some(url.to_string()),
        },
        utilization: NodeUtilization {
            storage_bytes_used: 0,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1,
            download_slots: None,
            storage_capacity_bytes: None,
        },
        updated_at_ms: 1,
        epoch: AdvertisementEpoch::default(),
        compute_draining: false,
        leaving: false,
        demand: Default::default(),
        reservation: Default::default(),
    }
}

/// Starts `count` members of one set split at `boundaries`, each range with `replicas` copies,
/// every member admitted and the set active.
pub(crate) async fn members(count: u8, boundaries: &[&str], replicas: usize) -> Members {
    let realm_id = RealmId::from_bytes([3; 32]);
    let mut started = Vec::new();
    for seed in 0..count {
        started.push(start(realm_id, 71 + seed).await);
    }
    let owner = UserId::local(Ulid::from_bytes([72; 16]), realm_id);
    let group_id = Ulid::from_bytes([73; 16]);
    let mut realm = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    for node in &started {
        realm.ensure_node(node.secret.public(), RealmNodeKind::Server);
    }
    let urls: Vec<String> = (started.iter())
        .map(|node| {
            format!(
                "http://127.0.0.1:{}",
                node.listener.local_addr().unwrap().port()
            )
        })
        .collect();
    for node in &started {
        let actor = Actor {
            node_id: node.secret.public(),
            user_id: owner,
            realm_id,
        };
        let config = realm.to_bytes(&actor).unwrap();
        let key = aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
        write_doc(
            &node.context,
            key,
            (*realm_id.as_bytes()).into(),
            config.into(),
        )
        .await;
        seed_realm_auth(&node.context, realm_id, &actor).await;
        seed_group_docs(&node.context, realm_id, &actor, group_id, "spire", owner).await;
        for (other, url) in started.iter().zip(&urls) {
            let id = other.secret.public();
            let document = info_document(id, url).to_bytes().unwrap();
            write_doc(
                &node.context,
                NODE_INFO_KEYSPACE,
                node_info_key(id).into(),
                document.into(),
            )
            .await;
        }
    }
    let snapshot = admitted_set(realm_id, group_id, owner, &started, boundaries, replicas).await;
    let shutdown = CancellationToken::new();
    let mut nodes = Vec::new();
    for (node, url) in started.into_iter().zip(urls) {
        let port = node.listener.local_addr().unwrap().port();
        let server = S3Server::new(
            "127.0.0.1:0",
            format!("127.0.0.1:{port}"),
            node.context.clone(),
            realm_id,
            node.secret.public(),
            CredentialEncryptionKey::random(),
            RoCrateLimits::default(),
            CorsConfig::default(),
            Arc::new(NodeMetrics::new()),
        )
        .await
        .unwrap();
        let (_, handle) = server
            .run_with_listener(node.listener, shutdown.clone())
            .unwrap();
        let service =
            ArunaS3Service::new(node.context.clone(), realm_id, node.secret.public()).await;
        nodes.push(Node {
            _dir: node.dir,
            secret: node.secret,
            context: node.context,
            service,
            url,
            server: Some(handle),
        });
    }
    let user = UserAccess {
        access_key: "AKFORWARD".to_string(),
        user_identity: owner,
        group_id,
        secret: EncryptedS3Secret::empty(),
        expiry: SystemTime::now() + Duration::from_secs(3600),
        path_restrictions: None,
        issued_by: [0; 32],
        revoked_at: None,
    };
    Members {
        realm_id,
        group_id,
        user,
        nodes,
        snapshot,
        _shutdown: shutdown,
    }
}

/// Creates the set with the first node as publisher, creates and admits each member bucket on
/// its node, activates the set and stores the signed record on every node.
async fn admitted_set(
    realm_id: RealmId,
    group_id: Ulid,
    owner: UserId,
    started: &[Started],
    boundaries: &[&str],
    replicas: usize,
) -> DistributionSnapshot {
    let root = SetRoot {
        publisher: started[0].secret.public(),
        owner_group: group_id,
        nonce: [74; 16],
    };
    let nodes: Vec<(Ulid, NodeId)> = (started.iter().enumerate())
        .map(|(index, node)| (Ulid::from_parts(75, index as u128), node.secret.public()))
        .collect();
    let boundaries: Vec<String> = boundaries.iter().map(ToString::to_string).collect();
    let layout = shard_layout(root.set_id(), &nodes, &boundaries, replicas).unwrap();
    let create = DistributionRequest::Create {
        root: root.clone(),
        config: DistributionConfig {
            bucket: BUCKET.to_string(),
            owner_group: group_id,
            node_set: None,
            members: layout.members,
            ranges: layout.ranges,
            settings: DistributionSettings::default(),
        },
    };
    let mut head = next_snapshot(realm_id, None, &create).unwrap();
    let created = head.clone();
    for (node, (member_id, node_id)) in started.iter().zip(&nodes) {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::now(),
            created_by: owner,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: Some(DistributionBinding {
                set_id: created.set_id(),
                member_id: *member_id,
                incarnation: 0,
                revision: created.revision(),
            }),
        };
        let create = CreateBucketOperation::new(BUCKET.to_string(), info).mapped();
        drive(create, &node.context).await.unwrap();
        let member = created.member(*member_id).unwrap();
        let admit = AdmitConfig {
            realm_id,
            node_id: *node_id,
            bucket: BUCKET.to_string(),
            set_id: created.set_id(),
            member_id: *member_id,
            intent: member.intent,
            ranges: created
                .held_ranges(*member_id)
                .into_iter()
                .cloned()
                .collect(),
            strategy: member.strategy,
            coverage: member.coverage,
            sizes: Vec::new(),
            capacity: None,
        };
        let statement = drive(AdmitOperation::new(admit), &node.context)
            .await
            .unwrap();
        let request = DistributionRequest::Statement(Box::new(statement));
        head = next_snapshot(realm_id, Some(&head), &request).unwrap();
    }
    head = next_snapshot(realm_id, Some(&head), &DistributionRequest::Activate).unwrap();
    let view = DistributionView {
        head: SignedSnapshot::sign(head.clone(), realm_id, &started[0].secret).unwrap(),
        conflict: None,
    };
    let record = view.to_bytes().unwrap();
    for node in started {
        let key = head.set_id().to_bytes().to_vec();
        write_doc(
            &node.context,
            DISTRIBUTION_KEYSPACE,
            key.into(),
            record.clone().into(),
        )
        .await;
    }
    head
}
