//! Builds the links, scopes and answers the repository operation tests feed their operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::fmt::Debug;
use std::time::SystemTime;

use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoResult};
use aruna_core::credential_encryption::CredentialEncryptionKey;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::repository::remote::InvenioAccess;
use aruna_core::repository::remote::{JobStop, RemoteAnswer, RemoteFailure};
use aruna_core::repository::{LinkDirection, LinkRemote, LinkStatus, RepositoryLink};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ArtifactRef, JobId, JobProgress, RoCrateLimits};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::{FIRST_GRANTABLE_HANDLE, PlacementRef};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::Value;
use byteview::ByteView;
use serde_json::Value as Json;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::invenio::Session;
use super::{JobScope, NodeScope};
use crate::jobs::executor::ProgressReporter;

pub(crate) const NOW_MS: u64 = 1_700_000_000_000;

pub(crate) fn realm() -> RealmId {
    RealmId::from_bytes([3; 32])
}

pub(crate) fn owner_node() -> NodeId {
    iroh::SecretKey::from_bytes(&[7; 32]).public()
}

pub(crate) fn other_node() -> NodeId {
    iroh::SecretKey::from_bytes(&[8; 32]).public()
}

pub(crate) fn credential_key() -> CredentialEncryptionKey {
    CredentialEncryptionKey::derive(&[1; 32])
}

/// A node with every plane, the owner of [`link`].
pub(crate) fn scope() -> NodeScope {
    NodeScope {
        node: Some((owner_node(), realm())),
        credential_key: Some(credential_key()),
        has_blob: true,
        has_tasks: true,
    }
}

/// A node without a net transport.
pub(crate) fn local_scope() -> NodeScope {
    NodeScope {
        node: None,
        credential_key: None,
        has_blob: true,
        has_tasks: false,
    }
}

pub(crate) fn job_id(nonce: u64) -> JobId {
    JobId::from_parts(
        NOW_MS,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        nonce,
    )
    .unwrap()
}

pub(crate) fn stop() -> JobStop {
    aruna_blob::remote::job_stop(CancellationToken::new(), CancellationToken::new())
}

pub(crate) fn job_scope() -> JobScope {
    JobScope {
        job_id: job_id(1),
        owner_node: owner_node(),
        claim_token: Ulid::from_bytes([9; 16]),
        stop: stop(),
        progress: ProgressReporter::from_progress(&JobProgress {
            current: 0,
            total: None,
            unit: "files".into(),
            paused_until: None,
        }),
    }
}

pub(crate) fn link() -> RepositoryLink {
    RepositoryLink {
        link_id: Ulid::from_bytes([1; 16]),
        document_id: Ulid::from_bytes([2; 16]),
        group_id: Ulid::from_bytes([4; 16]),
        connector_id: Ulid::from_bytes([5; 16]),
        endpoint: "https://zenodo.org/api/".into(),
        owner_node: owner_node(),
        owner_node_url: "https://node.example/api/v1".into(),
        created_by: UserId::local(Ulid::from_bytes([6; 16]), realm()),
        status: LinkStatus::Enabled,
        auto_publish: false,
        public_files: false,
        metadata_json: "{}".into(),
        remote: LinkRemote::default(),
        last_push: None,
        active_job: None,
        sequence: 0,
        limits: RoCrateLimits::default(),
        created_at: SystemTime::UNIX_EPOCH,
        updated_at: SystemTime::UNIX_EPOCH,
        generation: 0,
        warning: None,
        direction: LinkDirection::Push,
        kind: RepositoryConnectorKind::Invenio,
    }
}

/// A session on the endpoint of [`link`] whose calls do not watch a job.
pub(crate) fn session() -> Session {
    Session::new(
        InvenioAccess::new(&link().endpoint, Some("token".into()), 1024 * 1024).unwrap(),
        None,
    )
}

pub(crate) fn artifact() -> ArtifactRef {
    ArtifactRef {
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".into(),
            storage_bucket: "bucket".into(),
            backend_path: "artifact".into(),
            ulid: Ulid::from_bytes([3; 16]),
            format: Default::default(),
            created_by: link().created_by,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 100,
            hashes: Default::default(),
        },
        blake3: [0; 32],
        size: 100,
        expires_at_ms: 0,
    }
}

/// The registry row of the linked dataset.
pub(crate) fn registry_record() -> MetadataRegistryRecord {
    let link = link();
    MetadataRegistryRecord {
        realm_id: realm(),
        group_id: link.group_id,
        document_id: link.document_id,
        document_path: "doc".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(link.document_id),
        public: false,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm(),
            link.group_id,
            "doc",
            link.document_id,
        ),
        placement: PlacementRef::NIL,
        holder_node_ids: Vec::new(),
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: Ulid::from_bytes([1; 16]),
        last_event_id: Ulid::from_bytes([1; 16]),
    }
}

pub(crate) fn link_bytes(link: &RepositoryLink) -> Value {
    ByteView::from(link.to_bytes().unwrap())
}

/// The answer of a typed child.
pub(crate) fn child<T, E>(result: Result<T, E>) -> IoResult
where
    T: Send + Debug + PartialEq + 'static,
    E: Send + Debug + PartialEq + 'static,
{
    IoResult::Child(ChildOutcome::new(result))
}

pub(crate) fn event(event: Event) -> IoResult {
    IoResult::Event(Box::new(event))
}

pub(crate) fn remote(answer: Result<RemoteAnswer, RemoteFailure>) -> IoResult {
    event(Event::Blob(BlobEvent::Remote(answer)))
}

pub(crate) fn body(value: Json) -> IoResult {
    remote(Ok(RemoteAnswer::Json(value)))
}
