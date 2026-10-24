//! Replaces the config of a source connector unless an object version still uses it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_connector::{
    SourceConnector, SourceConnectorKind, SourceConnectorSecret,
};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::connectors::reference_prune::schedule_prune;
use crate::connectors::reference_scan::{ReferenceScan, ScanStep};
use crate::connectors::repository::{
    StorageReadError, delete_secret_effect, parse_connector_read, parse_secret_read,
    read_connector_effect, read_secret_effect, source_connector_key, write_secret_effect,
};
use crate::connectors::validation::{ValidationError, validate_connector_input};

#[derive(Clone, PartialEq, Eq)]
pub struct ReplaceSourceInput {
    pub group_id: GroupId,
    pub connector_id: Ulid,
    pub name: String,
    pub kind: SourceConnectorKind,
    pub public_config: HashMap<String, String>,
    pub secret_config: HashMap<String, String>,
}

/// Shows only the secret keys; the values are live credentials.
impl std::fmt::Debug for ReplaceSourceInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplaceSourceInput")
            .field("group_id", &self.group_id)
            .field("connector_id", &self.connector_id)
            .field("name", &self.name)
            .field("kind", &self.kind)
            .field("public_config", &self.public_config)
            .field(
                "secret_keys",
                &self.secret_config.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceSourceResult {
    pub connector: SourceConnector,
    pub has_secret_config: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaceSourceState {
    Init,
    ReadCurrent,
    ReadSecret,
    StartTransaction,
    ScanReferenceVersions,
    /// More reference rows follow than one request checks; their pruning is being queued.
    CommitDeferred,
    ScheduleDeferred,
    WriteRecords,
    WriteSecret,
    CommitTransaction,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum ReplaceSourceError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    #[error("Connector not found")]
    NotFound,
    #[error("Connector credentials are referenced by object versions")]
    ReferencedObjectVersion,
    #[error("Connector references are still being checked; retry later")]
    ReferencesPending,
    #[error("ReplaceSourceConnector failed")]
    ReplaceConnectorFailed,
    #[error("State [{state:?}] invalid: expected [{expected}] - received [{received:?}]")]
    InvalidStateEvent {
        state: ReplaceSourceState,
        expected: &'static str,
        received: Event,
    },
}

impl From<StorageReadError> for ReplaceSourceError {
    fn from(value: StorageReadError) -> Self {
        match value {
            StorageReadError::Storage(error) => Self::StorageError(error),
            StorageReadError::Conversion(error) => Self::ConversionError(error),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ReplaceSourceOperation {
    input: ReplaceSourceInput,
    state: ReplaceSourceState,
    replacement: Option<SourceConnector>,
    replacement_secret: Option<SourceConnectorSecret>,
    txn_id: Option<TxnId>,
    scan: Option<ReferenceScan>,
    output: Option<Result<ReplaceSourceResult, ReplaceSourceError>>,
}

impl ReplaceSourceOperation {
    pub fn new(input: ReplaceSourceInput) -> Self {
        Self {
            input,
            state: ReplaceSourceState::Init,
            replacement: None,
            replacement_secret: None,
            txn_id: None,
            scan: None,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        if let Err(error) = validate_connector_input(
            &self.input.name,
            self.input.kind,
            &self.input.public_config,
            &self.input.secret_config,
        ) {
            return self.emit_error(error.into());
        }

        self.state = ReplaceSourceState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn handle_current_read(&mut self, event: Event) -> Effects {
        match parse_connector_read(event) {
            Ok(Some(existing)) => {
                self.prepare_replacement(existing);
                self.state = ReplaceSourceState::ReadSecret;
                smallvec![read_secret_effect(self.input.connector_id, self.txn_id)]
            }
            Ok(None) => self.fail_or_abort(ReplaceSourceError::NotFound),
            Err(error) => self.fail_or_abort(error.into()),
        }
    }

    fn prepare_replacement(&mut self, existing: SourceConnector) {
        let now = SystemTime::now();
        self.replacement = Some(
            SourceConnector::new(
                existing.connector_id,
                existing.group_id,
                self.input.name.clone(),
                self.input.kind,
                self.input.public_config.clone(),
                existing.created_at,
                now,
                existing.created_by,
            )
            .with_namespace(&self.input.secret_config),
        );
        self.replacement_secret = SourceConnectorSecret::new(
            existing.connector_id,
            self.input.secret_config.clone(),
            now,
        );
    }

    fn handle_secret_read(&mut self, event: Event) -> Effects {
        let current_secret = match parse_secret_read(event, self.input.connector_id) {
            Ok(secret) => secret,
            Err(error) => return self.fail_or_abort(error.into()),
        };

        if secret_config_changed(current_secret.as_ref(), self.replacement_secret.as_ref()) {
            return self.scan_reference_versions();
        }

        self.write_records()
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => {
                self.txn_id = Some(txn_id);
                self.state = ReplaceSourceState::ReadCurrent;
                smallvec![read_connector_effect(
                    self.input.group_id,
                    self.input.connector_id,
                    Some(txn_id),
                )]
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(ReplaceSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionStarted)",
                received,
            }),
        }
    }

    fn emit_error(&mut self, error: ReplaceSourceError) -> Effects {
        self.state = ReplaceSourceState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn abort_with_error(&mut self, error: ReplaceSourceError) -> Effects {
        let Some(txn_id) = self.txn_id.take() else {
            return self.emit_error(error);
        };

        self.state = ReplaceSourceState::AbortTransaction;
        self.output = Some(Err(error));
        smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
    }

    fn fail_or_abort(&mut self, error: ReplaceSourceError) -> Effects {
        if self.txn_id.is_some() {
            self.abort_with_error(error)
        } else {
            self.emit_error(error)
        }
    }

    fn scan_reference_versions(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.emit_error(StorageError::TransactionNotFound.into());
        };

        self.state = ReplaceSourceState::ScanReferenceVersions;
        let mut scan = ReferenceScan::new(self.input.connector_id);
        let effects = scan.start(txn_id);
        self.scan = Some(scan);
        effects
    }

    fn handle_scan_page(&mut self, event: Event) -> Effects {
        let (Some(scan), Some(txn_id)) = (self.scan.as_mut(), self.txn_id) else {
            return self.abort_with_error(StorageError::TransactionNotFound.into());
        };
        match scan.step(event, txn_id) {
            Ok(ScanStep::Referenced) => {
                self.abort_with_error(ReplaceSourceError::ReferencedObjectVersion)
            }
            Ok(ScanStep::Next(effects)) => effects,
            Ok(ScanStep::Deferred) => {
                // The stale page and the pruning queue row commit; the caller retries later.
                self.state = ReplaceSourceState::CommitDeferred;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            Ok(ScanStep::Complete) => self.write_records(),
            Err(error) => self.abort_with_error(error.into()),
        }
    }

    fn handle_deferred_commit(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.state = ReplaceSourceState::ScheduleDeferred;
                smallvec![schedule_prune()]
            }
            Event::Storage(StorageEvent::Error { error }) => self.emit_error(error.into()),
            received => self.emit_error(ReplaceSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.txn_id = None;
                self.finish()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.txn_id = None;
                self.emit_error(error.into())
            }
            received => self.fail_or_abort(ReplaceSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                received,
            }),
        }
    }

    fn handle_transaction_aborted(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::TransactionAborted { .. })
            | Event::Storage(StorageEvent::Error { .. }) => {
                self.state = ReplaceSourceState::Error;
                smallvec![]
            }
            received => self.emit_error(ReplaceSourceError::InvalidStateEvent {
                state: self.state.clone(),
                expected: "Event::Storage(StorageEvent::TransactionAborted)",
                received,
            }),
        }
    }

    fn write_records(&mut self) -> Effects {
        let Some(replacement) = self.replacement.clone() else {
            return self.fail_or_abort(ReplaceSourceError::ReplaceConnectorFailed);
        };

        self.state = ReplaceSourceState::WriteRecords;
        let connector_bytes = match replacement.to_bytes() {
            Ok(bytes) => bytes,
            Err(error) => return self.fail_or_abort(error.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE.to_string(),
            key: source_connector_key(replacement.group_id, replacement.connector_id),
            value: connector_bytes.into(),
            txn_id: self.txn_id,
        })]
    }

    fn handle_records_written(&mut self, event: Event) -> Effects {
        match event {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            Event::Storage(StorageEvent::Error { error }) if self.txn_id.is_some() => {
                return self.abort_with_error(error.into());
            }
            Event::Storage(StorageEvent::Error { error }) => return self.emit_error(error.into()),
            received => {
                return self.fail_or_abort(ReplaceSourceError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::WriteResult)",
                    received,
                });
            }
        }

        self.state = ReplaceSourceState::WriteSecret;
        let effect = match self.replacement_secret.as_ref() {
            Some(secret) => match write_secret_effect(secret, self.txn_id) {
                Ok(effect) => effect,
                Err(error) => return self.fail_or_abort(error.into()),
            },
            None => delete_secret_effect(self.input.connector_id, self.txn_id),
        };
        smallvec![effect]
    }

    /// A replacement with secrets writes them; one without removes the old record.
    fn handle_secret_written(&mut self, event: Event) -> Effects {
        match (&self.replacement_secret, event) {
            (Some(_), Event::Storage(StorageEvent::WriteResult { .. }))
            | (None, Event::Storage(StorageEvent::DeleteResult { .. })) => {}
            (_, Event::Storage(StorageEvent::Error { error })) if self.txn_id.is_some() => {
                return self.abort_with_error(error.into());
            }
            (_, Event::Storage(StorageEvent::Error { error })) => {
                return self.emit_error(error.into());
            }
            (_, received) => {
                return self.fail_or_abort(ReplaceSourceError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Storage(StorageEvent::WriteResult | DeleteResult)",
                    received,
                });
            }
        }

        self.commit_or_finish()
    }

    fn commit_or_finish(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id {
            self.state = ReplaceSourceState::CommitTransaction;
            return smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })];
        }

        self.finish()
    }

    fn finish(&mut self) -> Effects {
        let Some(connector) = self.replacement.clone() else {
            return self.emit_error(ReplaceSourceError::ReplaceConnectorFailed);
        };

        self.state = ReplaceSourceState::Finish;
        self.output = Some(Ok(ReplaceSourceResult {
            connector,
            has_secret_config: self.replacement_secret.is_some(),
        }));
        smallvec![]
    }
}

fn secret_config_changed(
    current: Option<&SourceConnectorSecret>,
    replacement: Option<&SourceConnectorSecret>,
) -> bool {
    current.map(|secret| &secret.secret_config) != replacement.map(|secret| &secret.secret_config)
}

impl Operation for ReplaceSourceOperation {
    type Output = ReplaceSourceResult;
    type Error = ReplaceSourceError;

    fn start(&mut self) -> Effects {
        self.handle_init()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReplaceSourceState::Init => self.handle_init(),
            ReplaceSourceState::ReadCurrent => self.handle_current_read(event),
            ReplaceSourceState::ReadSecret => self.handle_secret_read(event),
            ReplaceSourceState::StartTransaction => self.handle_transaction_started(event),
            ReplaceSourceState::ScanReferenceVersions => self.handle_scan_page(event),
            ReplaceSourceState::CommitDeferred => self.handle_deferred_commit(event),
            ReplaceSourceState::ScheduleDeferred => match event {
                Event::Task(_) => self.emit_error(ReplaceSourceError::ReferencesPending),
                received => self.emit_error(ReplaceSourceError::InvalidStateEvent {
                    state: self.state.clone(),
                    expected: "Event::Task",
                    received,
                }),
            },
            ReplaceSourceState::WriteRecords => self.handle_records_written(event),
            ReplaceSourceState::WriteSecret => self.handle_secret_written(event),
            ReplaceSourceState::CommitTransaction => self.handle_transaction_committed(event),
            ReplaceSourceState::AbortTransaction => self.handle_transaction_aborted(event),
            ReplaceSourceState::Finish => smallvec![],
            ReplaceSourceState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            ReplaceSourceState::Finish | ReplaceSourceState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == ReplaceSourceState::Error {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(ReplaceSourceError::ReplaceConnectorFailed);
        }

        self.output
            .ok_or(ReplaceSourceError::ReplaceConnectorFailed)?
    }

    fn abort(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = ReplaceSourceState::AbortTransaction;
            return smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })];
        }
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::repository::{connector_reference_key, connector_reference_write};
    use crate::connectors::resolver::{ResolveBindingInput, ResolveBindingOperation};
    use crate::driver::{DriverContext, drive};
    use crate::staging::descriptor::build_source_binding;
    use aruna_core::keyspaces::{
        BLOB_VERSIONS_KEYSPACE, CONNECTOR_PRUNE_KEYSPACE, CONNECTOR_REFERENCE_KEYSPACE,
    };
    use aruna_core::structs::execution::source_access::{ResolvedSourceAccess, SourceMetadata};
    use aruna_core::structs::execution::staging::{StagingStrategy, VersionSourceBinding};
    use aruna_core::structs::storage::blob::{BlobVersion, VersionKey};
    use aruna_core::types::Key;
    use aruna_storage::storage;
    use tempfile::{TempDir, tempdir};

    fn test_context() -> (TempDir, DriverContext) {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (tempdir, context)
    }

    fn source_metadata() -> SourceMetadata {
        SourceMetadata {
            content_length: 42,
            content_type: Some("text/plain".to_string()),
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }
    }

    async fn create_connector(context: &DriverContext) -> SourceConnector {
        drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
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

    async fn write_reference_version(context: &DriverContext, source: VersionSourceBinding) {
        let version_key = VersionKey::new("bucket", "key", ulid::Ulid::generate());
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

    fn connector_secret(connector_id: Ulid, user: &str) -> SourceConnectorSecret {
        SourceConnectorSecret::new(
            connector_id,
            HashMap::from([
                ("username".to_string(), user.to_string()),
                ("password".to_string(), "secret".to_string()),
            ]),
            SystemTime::UNIX_EPOCH,
        )
        .unwrap()
    }

    fn replacement_connector(group_id: Ulid, connector_id: Ulid) -> SourceConnector {
        SourceConnector::new(
            connector_id,
            group_id,
            "new".to_string(),
            SourceConnectorKind::Webdav,
            HashMap::from([(
                "endpoint".to_string(),
                "https://dav.example.org".to_string(),
            )]),
            SystemTime::UNIX_EPOCH,
            SystemTime::UNIX_EPOCH,
            Default::default(),
        )
    }

    fn reference_blob_version(connector_id: Ulid) -> BlobVersion {
        let connector = replacement_connector(Ulid::generate(), connector_id);
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector_id),
        );

        BlobVersion::reference(
            source,
            source_metadata(),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            SystemTime::UNIX_EPOCH,
        )
    }

    #[test]
    fn scan_write_transaction() {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id,
            connector_id,
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceState::ReadSecret;
        operation.txn_id = Some(txn_id);
        operation.replacement = Some(replacement_connector(group_id, connector_id));
        operation.replacement_secret = Some(connector_secret(connector_id, "bob"));

        let effects = operation.step(Event::Storage(StorageEvent::VaultResult {
            entry: SourceConnectorSecret::vault_entry(connector_id),
            secret: Some(connector_secret(connector_id, "alice").to_secret().unwrap()),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Iter { txn_id: Some(scan_txn), .. })]
                if *scan_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![],
            next_start_after: None,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::Write { txn_id: Some(write_txn), .. })]
                if *write_txn == txn_id
        ));

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::VaultWrite { entry, txn_id: Some(write_txn), .. })]
                if entry.id == connector_id && *write_txn == txn_id
        ));
        let rendered = format!("{effects:?} {operation:?}");
        assert!(!rendered.contains("bob"), "{rendered}");

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
    }

    /// An operation inside its transaction with the reference scan started.
    fn scanning(connector_id: Ulid, txn_id: Ulid) -> ReplaceSourceOperation {
        let group_id = Ulid::from_bytes([1u8; 16]);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id,
            connector_id,
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceState::ReadSecret;
        operation.txn_id = Some(txn_id);
        operation.replacement = Some(replacement_connector(group_id, connector_id));
        operation.replacement_secret = Some(connector_secret(connector_id, "bob"));
        operation.step(Event::Storage(StorageEvent::VaultResult {
            entry: SourceConnectorSecret::vault_entry(connector_id),
            secret: Some(connector_secret(connector_id, "alice").to_secret().unwrap()),
        }));
        operation
    }

    fn reference_rows(connector_id: Ulid, next: Option<Key>) -> Event {
        let version_key = VersionKey::new("bucket", "key", Ulid::nil());
        Event::Storage(StorageEvent::IterResult {
            values: vec![(
                connector_reference_key(connector_id, &version_key).unwrap(),
                Vec::new().into(),
            )],
            next_start_after: next,
        })
    }

    fn version_read(version: Option<BlobVersion>) -> Event {
        Event::Storage(StorageEvent::BatchReadResult {
            values: vec![(
                vec![1].into(),
                version.map(|version| version.to_bytes().unwrap().into()),
            )],
        })
    }

    fn pruned() -> Event {
        Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    #[test]
    fn reference_aborts_replace() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(connector_id, txn_id);

        assert!(matches!(
            operation.step(reference_rows(connector_id, None)).as_slice(),
            [Effect::Storage(StorageEffect::BatchRead { txn_id: Some(read_txn), .. })]
                if *read_txn == txn_id
        ));
        let effects = operation.step(version_read(Some(reference_blob_version(connector_id))));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            operation.finalize(),
            Err(ReplaceSourceError::ReferencedObjectVersion)
        );
    }

    // A stale page with more rows behind it commits its pruning and the queued background work,
    // then asks the caller to retry instead of scanning on.
    #[test]
    fn pending_defers_check() {
        let connector_id = Ulid::from_bytes([2u8; 16]);
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(connector_id, txn_id);

        operation.step(reference_rows(connector_id, Some(vec![9u8].into())));
        assert!(matches!(
            operation.step(version_read(None)).as_slice(),
            [Effect::Storage(StorageEffect::BatchDelete { deletes, .. })]
                if deletes[0].0 == CONNECTOR_REFERENCE_KEYSPACE
        ));
        assert!(matches!(
            operation.step(pruned()).as_slice(),
            [Effect::Storage(StorageEffect::Write { key_space, .. })]
                if key_space == CONNECTOR_PRUNE_KEYSPACE
        ));
        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { txn_id: commit_txn })]
                if *commit_txn == txn_id
        ));
        let effects = operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id,
        }));
        assert!(matches!(effects.as_slice(), [Effect::Task(_)]));
        operation.step(Event::Task(aruna_core::task::TaskEvent::TimerScheduled {
            key: aruna_core::task::TaskKey::PruneConnectorReferences,
            after: std::time::Duration::ZERO,
        }));

        assert_eq!(
            operation.finalize(),
            Err(ReplaceSourceError::ReferencesPending)
        );
    }

    #[test]
    fn scan_failure_aborts() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = scanning(Ulid::from_bytes([2u8; 16]), txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(matches!(
            operation.finalize(),
            Err(ReplaceSourceError::StorageError(StorageError::ReadError(_)))
        ));
    }

    #[test]
    fn commit_failure_reports() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceState::CommitTransaction;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::Error {
            error: StorageError::ReadError("commit failed".to_string()),
        }));
        assert!(effects.is_empty());
        assert!(operation.txn_id.is_none());
        assert_eq!(
            operation.finalize(),
            Err(ReplaceSourceError::StorageError(StorageError::ReadError(
                "commit failed".to_string()
            )))
        );
    }

    #[test]
    fn invalid_commit_aborts() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.state = ReplaceSourceState::CommitTransaction;
        operation.txn_id = Some(txn_id);

        let effects = operation.step(Event::Storage(StorageEvent::WriteResult {
            key: vec![].into(),
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));

        operation.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert!(matches!(
            operation.finalize(),
            Err(ReplaceSourceError::InvalidStateEvent {
                expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                ..
            })
        ));
    }

    #[test]
    fn abort_emits_transaction() {
        let txn_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id: Ulid::from_bytes([1u8; 16]),
            connector_id: Ulid::from_bytes([2u8; 16]),
            name: "new".to_string(),
            kind: SourceConnectorKind::Webdav,
            public_config: HashMap::new(),
            secret_config: HashMap::new(),
        });
        operation.txn_id = Some(txn_id);

        let effects = operation.abort();
        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { txn_id: abort_txn })]
                if *abort_txn == txn_id
        ));
        assert!(operation.txn_id.is_none());
    }

    #[tokio::test]
    async fn removes_secret_config() {
        let tempdir = tempdir().unwrap();
        let storage_handle = storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
        storage_handle.open_vault(aruna_core::node_vault::NodeVaultKey::random());
        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };

        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: ulid::Ulid::generate(),
                created_by: Default::default(),
                name: "old".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org".to_string(),
                )]),
                secret_config: HashMap::from([("token".to_string(), "secret".to_string())]),
            }),
            &context,
        )
        .await
        .unwrap();

        let replaced = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: created.connector.group_id,
                connector_id: created.connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([(
                    "endpoint".to_string(),
                    "https://example.org/v2".to_string(),
                )]),
                secret_config: HashMap::new(),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(replaced.connector.name, "new");
        assert!(!replaced.has_secret_config);
    }

    #[tokio::test]
    async fn secret_change_rejected() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source).await;

        let result = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: connector.public_config.clone(),
                secret_config: HashMap::from([
                    ("username".to_string(), "bob".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await;

        assert_eq!(result, Err(ReplaceSourceError::ReferencedObjectVersion));
    }

    #[tokio::test]
    async fn public_change_allowed() {
        let (_tempdir, context) = test_context();
        let connector = create_connector(&context).await;
        let source = build_source_binding(
            StagingStrategy::Reference,
            &connector,
            &source_metadata(),
            "run-1/data.txt".to_string(),
            None,
            Some(connector.connector_id),
        );
        write_reference_version(&context, source.clone()).await;

        let replaced = drive(
            ReplaceSourceOperation::new(ReplaceSourceInput {
                group_id: connector.group_id,
                connector_id: connector.connector_id,
                name: "new".to_string(),
                kind: SourceConnectorKind::Webdav,
                public_config: HashMap::from([
                    (
                        "endpoint".to_string(),
                        "https://dav.example.org/v2".to_string(),
                    ),
                    ("root".to_string(), "/datasets-v2".to_string()),
                ]),
                secret_config: HashMap::from([
                    ("username".to_string(), "alice".to_string()),
                    ("password".to_string(), "secret".to_string()),
                ]),
            }),
            &context,
        )
        .await
        .unwrap();

        assert_eq!(replaced.connector.name, "new");

        let access = drive(
            ResolveBindingOperation::new(ResolveBindingInput { source }),
            &context,
        )
        .await
        .unwrap();
        let ResolvedSourceAccess::OpenDal { config, .. } = access;
        assert_eq!(
            config.get("endpoint").map(String::as_str),
            Some("https://dav.example.org")
        );
        assert_eq!(config.get("root").map(String::as_str), Some("/datasets"));
        assert_eq!(config.get("username").map(String::as_str), Some("alice"));
    }
}
