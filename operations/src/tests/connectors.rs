//! Builds connectors, reference versions and reference pages for the connector tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::SystemTime;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::{SourceConnector, SourceConnectorKind};
use aruna_core::structs::execution::staging::VersionSourceBinding;
use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
use aruna_core::types::Key;
use aruna_storage::storage;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
use crate::connectors::repository::{connector_reference_key, connector_reference_write};
use crate::driver::{DriverContext, drive};
use crate::testing::test_context;

/// A storage-only context with an open node vault for connector secrets.
pub(crate) fn vault_context() -> (TempDir, DriverContext) {
    let tempdir = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
    (tempdir, test_context(storage_handle))
}

pub(crate) fn source_metadata() -> SourceMetadata {
    SourceMetadata {
        content_length: 42,
        content_type: Some("text/plain".to_string()),
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    }
}

/// Stores a WebDAV connector with a username and password secret.
pub(crate) async fn create_connector(context: &DriverContext) -> SourceConnector {
    drive(
        SourceConnectorOperation::new(SourceConnectorInput {
            group_id: Ulid::generate(),
            created_by: Default::default(),
            name: "dav-source".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::from([
                (
                    "endpoint".to_string(),
                    "https://dav.example.org".to_string(),
                ),
                ("root".to_string(), "/datasets".to_string()),
            ]),
            secret_config: HashMap::from([
                ("username".to_string(), "alice".to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
        }),
        context,
    )
    .await
    .unwrap()
    .connector
}

/// Writes one object version that references `source`, with its connector reference row.
pub(crate) async fn write_reference(context: &DriverContext, source: VersionSourceBinding) {
    let version_key = VersionKey::new("bucket", "key", Ulid::generate());
    let version = BlobVersion::reference(
        source,
        source_metadata(),
        SystemTime::UNIX_EPOCH,
        Default::default(),
        SystemTime::UNIX_EPOCH,
    );
    let mut writes = vec![(
        BLOB_VERSIONS_KEYSPACE.to_string(),
        version_key.to_bytes().unwrap().into(),
        version.to_bytes().unwrap().into(),
    )];
    writes.extend(connector_reference_write(&version, &version_key).unwrap());

    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::BatchWriteResult { .. })
    ));
}

/// A reference scan page with one row for `connector_id`.
pub(crate) fn reference_rows(connector_id: Ulid, next: Option<Key>) -> IoResult {
    let version_key = VersionKey::new("bucket", "key", Ulid::nil());
    IoResult::Page {
        rows: vec![(
            connector_reference_key(connector_id, &version_key).unwrap(),
            Vec::new().into(),
        )],
        next,
    }
}

/// Checks that a stale page with more rows behind it commits its pruning and the queued
/// background work, then asks the caller to retry instead of scanning on.
pub(crate) fn defers_pending<O, T>(mut operation: O, connector_id: Ulid)
where
    O: Operation<Output = Option<T>>,
    T: Debug + Send,
{
    operation
        .next(reference_rows(connector_id, Some(vec![9u8].into())))
        .unwrap();
    let missing = IoResult::Values(vec![None]);
    assert!(matches!(
        operation.next(missing),
        Ok(Next::Call(IoCall::Delete(deletes)))
            if deletes[0].0 == CONNECTOR_REFERENCE_KEYSPACE
    ));
    assert!(matches!(
        operation.next(IoResult::Done),
        Ok(Next::Call(IoCall::Write(writes)))
            if writes[0].0 == CONNECTOR_PRUNE_KEYSPACE
    ));
    let Ok(Next::Then {
        output: None,
        calls,
    }) = operation.next(IoResult::Done)
    else {
        panic!("expected the pending outcome");
    };
    assert!(
        matches!(calls.as_slice(), [IoCall::Effect(wake)] if matches!(**wake, Effect::Task(_))),
        "the pruning is woken after the commit"
    );
}
