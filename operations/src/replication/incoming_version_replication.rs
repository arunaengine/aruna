use crate::replication::error::ReplicationError;
use crate::replication::protocol::{VersionReplicationManifest, VersionReplicationMessage};
use crate::replication::util::{dht_registration_effect, hash_lookup_write_effect};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, DhtEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::{
    S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_MULTIPART_OBJECT_METADATA_KEYSPACE,
    S3_VERSION_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BackendLocation, Location, LookupKey, MultipartObjectMetadataKey, RealmId, ReplicationItemKind,
    ReplicationNegotiationResult, VersionKey, VersionMetadata,
};
use aruna_core::types::{Effects, NodeId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
enum IncomingVersionReplicationState {
    Init,
    ReadExistingVersion,
    ReadExistingBlob,
    SendNegotiation,
    ReceiveBlob,
    StartTransaction,
    EnsureBucket,
    WriteBucket,
    WriteHashLookup,
    WriteObjectLookup,
    WriteVersion,
    WriteMultipartMetadata,
    CommitTransaction,
    SendApplyRejected,
    AbortTransaction,
    CleanupReceivedBlob,
    RegisterBlobInDht,
    SendApplyComplete,
    CloseConnection,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum IncomingVersionReplicationError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    ReplicationError(#[from] ReplicationError),
    #[error("Replication is only allowed within the same realm")]
    RealmMismatch,
    #[error("Materialized replication manifest is missing blob info")]
    MissingBlobInfo,
    #[error("Materialized replication manifest is missing local blob location")]
    MissingBlobLocation,
    #[error("Unexpected event in state {state}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

#[derive(Debug, PartialEq)]
pub struct IncomingVersionReplicationOperation {
    state: IncomingVersionReplicationState,
    stream_id: Ulid,
    local_node_id: NodeId,
    local_realm_id: RealmId,
    manifest: VersionReplicationManifest,
    txn_id: Option<Ulid>,
    negotiation_result: Option<ReplicationNegotiationResult>,
    existing_blob_location: Option<BackendLocation>,
    received_blob_location: Option<BackendLocation>,
    cleanup_blob_location: Option<BackendLocation>,
    apply_committed: bool,
    output: Option<Result<(), IncomingVersionReplicationError>>,
}

impl IncomingVersionReplicationOperation {
    pub fn new(
        stream_id: Ulid,
        local_node_id: NodeId,
        local_realm_id: RealmId,
        manifest: VersionReplicationManifest,
    ) -> Self {
        Self {
            state: IncomingVersionReplicationState::Init,
            stream_id,
            local_node_id,
            local_realm_id,
            manifest,
            txn_id: None,
            negotiation_result: None,
            existing_blob_location: None,
            received_blob_location: None,
            cleanup_blob_location: None,
            apply_committed: false,
            output: None,
        }
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingVersionReplicationState::Init => "Init",
            IncomingVersionReplicationState::ReadExistingVersion => "ReadExistingVersion",
            IncomingVersionReplicationState::ReadExistingBlob => "ReadExistingBlob",
            IncomingVersionReplicationState::SendNegotiation => "SendNegotiation",
            IncomingVersionReplicationState::ReceiveBlob => "ReceiveBlob",
            IncomingVersionReplicationState::StartTransaction => "StartTransaction",
            IncomingVersionReplicationState::EnsureBucket => "EnsureBucket",
            IncomingVersionReplicationState::WriteBucket => "WriteBucket",
            IncomingVersionReplicationState::WriteHashLookup => "WriteHashLookup",
            IncomingVersionReplicationState::WriteObjectLookup => "WriteObjectLookup",
            IncomingVersionReplicationState::WriteVersion => "WriteVersion",
            IncomingVersionReplicationState::WriteMultipartMetadata => "WriteMultipartMetadata",
            IncomingVersionReplicationState::CommitTransaction => "CommitTransaction",
            IncomingVersionReplicationState::SendApplyRejected => "SendApplyRejected",
            IncomingVersionReplicationState::AbortTransaction => "AbortTransaction",
            IncomingVersionReplicationState::CleanupReceivedBlob => "CleanupReceivedBlob",
            IncomingVersionReplicationState::RegisterBlobInDht => "RegisterBlobInDht",
            IncomingVersionReplicationState::SendApplyComplete => "SendApplyComplete",
            IncomingVersionReplicationState::CloseConnection => "CloseConnection",
            IncomingVersionReplicationState::Finish => "Finish",
            IncomingVersionReplicationState::Error => "Error",
        }
    }

    fn fail(&mut self, err: IncomingVersionReplicationError) -> Effects {
        let should_reject = matches!(
            self.negotiation_result,
            Some(
                ReplicationNegotiationResult::NeedVersionOnly
                    | ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ) && !self.apply_committed
            && !matches!(
                self.state,
                IncomingVersionReplicationState::SendApplyRejected
                    | IncomingVersionReplicationState::AbortTransaction
                    | IncomingVersionReplicationState::CleanupReceivedBlob
                    | IncomingVersionReplicationState::CloseConnection
                    | IncomingVersionReplicationState::Error
            );
        self.output = Some(Err(err));
        if should_reject {
            self.send_apply_rejected()
        } else {
            self.state = IncomingVersionReplicationState::Error;
            self.abort()
        }
    }

    fn version_key_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        )
        .to_bytes()
    }

    fn read_existing_version(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReadExistingVersion;
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn read_existing_blob(&mut self) -> Effects {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionReplicationError::MissingBlobInfo);
        };
        self.state = IncomingVersionReplicationState::ReadExistingBlob;
        let key = match LookupKey::from_blake3_hash(&blob.hash).and_then(|key| key.to_bytes()) {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn send_negotiation(&mut self, result: ReplicationNegotiationResult) -> Effects {
        self.negotiation_result = Some(result.clone());
        self.state = IncomingVersionReplicationState::SendNegotiation;
        let payload = match VersionReplicationMessage::VersionNegotiationResponse(result).to_bytes()
        {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn receive_blob(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::ReceiveBlob;
        smallvec![Effect::Blob(BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            keep_alive: true,
        })]
    }

    fn start_transaction(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false,
        })]
    }

    fn ensure_bucket(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::EnsureBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.manifest.bucket.as_bytes().to_vec().into(),
            txn_id: self.txn_id,
        })]
    }

    fn write_bucket(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::WriteBucket;
        let value = match self.manifest.bucket_info.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.manifest.bucket.as_bytes().to_vec().into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn effective_materialized_location(
        &self,
    ) -> Result<BackendLocation, IncomingVersionReplicationError> {
        self.received_blob_location
            .clone()
            .or_else(|| self.existing_blob_location.clone())
            .ok_or(IncomingVersionReplicationError::MissingBlobLocation)
    }

    fn version_location(&self) -> Result<Location, IncomingVersionReplicationError> {
        match self.manifest.kind {
            ReplicationItemKind::Materialized => {
                Ok(Location::Real(self.effective_materialized_location()?))
            }
            ReplicationItemKind::DeleteMarker => Ok(Location::Deleted),
        }
    }

    fn write_hash_lookup_or_continue(&mut self) -> Effects {
        if self.received_blob_location.is_none() {
            return self.write_object_lookup_or_continue();
        }

        let location = match self.effective_materialized_location() {
            Ok(location) => location,
            Err(err) => return self.fail(err),
        };
        let blake3 = location
            .get_blake3()
            .map(|hash| hash.to_vec())
            .unwrap_or_default();
        if blake3.is_empty() {
            return self.fail(IncomingVersionReplicationError::MissingBlobLocation);
        }

        self.state = IncomingVersionReplicationState::WriteHashLookup;
        let effect = match hash_lookup_write_effect(location, &blake3, self.txn_id) {
            Ok(effect) => effect,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![effect]
    }

    fn write_object_lookup_or_continue(&mut self) -> Effects {
        if !self.manifest.current_version {
            return self.write_version();
        }

        self.state = IncomingVersionReplicationState::WriteObjectLookup;
        let key = match LookupKey::object(&self.manifest.bucket, &self.manifest.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        let value = match self.version_location() {
            Ok(location) => match location.to_bytes() {
                Ok(bytes) => bytes,
                Err(err) => return self.fail(err.into()),
            },
            Err(err) => return self.fail(err),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_LOOKUP_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn write_version(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::WriteVersion;
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        let value = match (VersionMetadata {
            version_id: self.manifest.version_id,
            location: match self.version_location() {
                Ok(location) => location,
                Err(err) => return self.fail(err),
            },
            created_at: self.manifest.created_at,
            created_by: self.manifest.created_by,
        })
        .to_bytes()
        {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };

        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_VERSION_KEYSPACE.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }

    fn write_multipart_metadata_or_continue(&mut self) -> Effects {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.commit_transaction();
        };

        self.state = IncomingVersionReplicationState::WriteMultipartMetadata;
        let mut writes = Vec::with_capacity(multipart.parts.len() + 1);

        let summary_key =
            match MultipartObjectMetadataKey::summary(self.manifest.version_id).to_bytes() {
                Ok(key) => key,
                Err(err) => return self.fail(err.into()),
            };
        let summary_value = match multipart.summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        writes.push((
            S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for part in &multipart.parts {
            let key =
                match MultipartObjectMetadataKey::part(self.manifest.version_id, part.part_number)
                    .to_bytes()
                {
                    Ok(key) => key,
                    Err(err) => return self.fail(err.into()),
                };
            let value = match part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.fail(err.into()),
            };
            writes.push((
                S3_MULTIPART_OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: self.txn_id,
        })]
    }

    fn commit_transaction(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(IncomingVersionReplicationError::StorageError(
                StorageError::TransactionNotFound,
            ));
        };
        self.state = IncomingVersionReplicationState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn register_blob_in_dht_or_continue(&mut self) -> Effects {
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        self.state = IncomingVersionReplicationState::RegisterBlobInDht;
        let effect = match dht_registration_effect(
            blake3_hash,
            self.local_realm_id.clone(),
            self.local_node_id,
        ) {
            Ok(effect) => effect,
            Err(_) => return self.send_apply_complete(),
        };
        smallvec![effect]
    }

    fn send_apply_complete(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::SendApplyComplete;
        let payload = match VersionReplicationMessage::VersionApplyComplete.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn send_apply_rejected(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::SendApplyRejected;
        let reason = self
            .output
            .as_ref()
            .and_then(|result| result.as_ref().err())
            .map(ToString::to_string)
            .unwrap_or_else(|| "version replication apply failed".to_string());
        let payload = match VersionReplicationMessage::VersionApplyRejected(reason).to_bytes() {
            Ok(payload) => payload,
            Err(_) => {
                self.state = IncomingVersionReplicationState::Error;
                return self.abort();
            }
        };
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn abort_transaction_or_close(&mut self) -> Effects {
        if let Some(txn_id) = self.txn_id.take() {
            self.state = IncomingVersionReplicationState::AbortTransaction;
            smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
        } else {
            self.cleanup_received_blob_or_close()
        }
    }

    fn cleanup_received_blob_or_close(&mut self) -> Effects {
        if let Some(location) = self.cleanup_blob_location.take() {
            self.state = IncomingVersionReplicationState::CleanupReceivedBlob;
            smallvec![Effect::Blob(BlobEffect::Delete { location })]
        } else {
            self.state = IncomingVersionReplicationState::Error;
            self.close_connection()
        }
    }

    fn close_connection(&mut self) -> Effects {
        self.state = IncomingVersionReplicationState::CloseConnection;
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }
}

impl Operation for IncomingVersionReplicationOperation {
    type Output = Result<(), IncomingVersionReplicationError>;
    type Error = IncomingVersionReplicationError;

    fn start(&mut self) -> Effects {
        if self.manifest.user_identity.realm_key != self.local_realm_id {
            return self.send_negotiation(ReplicationNegotiationResult::Rejected(
                IncomingVersionReplicationError::RealmMismatch.to_string(),
            ));
        }

        self.read_existing_version()
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            IncomingVersionReplicationState::Init => self.start(),
            IncomingVersionReplicationState::ReadExistingVersion => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                if value.is_some() {
                    return self
                        .send_negotiation(ReplicationNegotiationResult::AlreadyReplicatedVersion);
                }

                match self.manifest.kind {
                    ReplicationItemKind::DeleteMarker => {
                        self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                    }
                    ReplicationItemKind::Materialized => self.read_existing_blob(),
                }
            }
            IncomingVersionReplicationState::ReadExistingBlob => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };

                if let Some(value) = value {
                    match Location::from_bytes(value.as_ref()) {
                        Ok(Location::Real(location)) => {
                            self.existing_blob_location = Some(location);
                            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                        }
                        Ok(Location::Deleted) | Err(_) => {
                            self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion)
                        }
                    }
                } else {
                    self.send_negotiation(ReplicationNegotiationResult::NeedBlobAndVersion)
                }
            }
            IncomingVersionReplicationState::SendNegotiation => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };

                match self.negotiation_result.clone() {
                    Some(ReplicationNegotiationResult::AlreadyReplicatedVersion)
                    | Some(ReplicationNegotiationResult::Rejected(_)) => self.close_connection(),
                    Some(ReplicationNegotiationResult::NeedVersionOnly) => self.start_transaction(),
                    Some(ReplicationNegotiationResult::NeedBlobAndVersion) => self.receive_blob(),
                    None => self.fail(IncomingVersionReplicationError::ReplicationError(
                        ReplicationError::ReplicationFailed,
                    )),
                }
            }
            IncomingVersionReplicationState::ReceiveBlob => {
                let Event::Blob(BlobEvent::ReplicationFinished { location }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ReplicationFinished)",
                        received: event,
                    });
                };
                self.received_blob_location = Some(location.clone());
                self.cleanup_blob_location = Some(location);
                self.start_transaction()
            }
            IncomingVersionReplicationState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionStarted)",
                        received: event,
                    });
                };
                self.txn_id = Some(txn_id);
                self.ensure_bucket()
            }
            IncomingVersionReplicationState::EnsureBucket => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::ReadResult)",
                        received: event,
                    });
                };
                if value.is_some() {
                    self.write_hash_lookup_or_continue()
                } else {
                    self.write_bucket()
                }
            }
            IncomingVersionReplicationState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_hash_lookup_or_continue()
            }
            IncomingVersionReplicationState::WriteHashLookup => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_object_lookup_or_continue()
            }
            IncomingVersionReplicationState::WriteObjectLookup => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_version()
            }
            IncomingVersionReplicationState::WriteVersion => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::WriteResult)",
                        received: event,
                    });
                };
                self.write_multipart_metadata_or_continue()
            }
            IncomingVersionReplicationState::WriteMultipartMetadata => {
                let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::BatchWriteResult)",
                        received: event,
                    });
                };
                self.commit_transaction()
            }
            IncomingVersionReplicationState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionCommitted)",
                        received: event,
                    });
                };
                self.txn_id = None;
                self.cleanup_blob_location = None;
                self.apply_committed = true;
                match self.manifest.kind {
                    ReplicationItemKind::Materialized => self.register_blob_in_dht_or_continue(),
                    ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
                }
            }
            IncomingVersionReplicationState::SendApplyRejected => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };
                self.abort_transaction_or_close()
            }
            IncomingVersionReplicationState::AbortTransaction => {
                let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Storage(StorageEvent::TransactionAborted)",
                        received: event,
                    });
                };
                self.cleanup_received_blob_or_close()
            }
            IncomingVersionReplicationState::CleanupReceivedBlob => match event {
                Event::Blob(BlobEvent::DeleteFinished) | Event::Blob(BlobEvent::Error(_)) => {
                    self.state = IncomingVersionReplicationState::Error;
                    self.close_connection()
                }
                _ => {
                    self.state = IncomingVersionReplicationState::Error;
                    self.close_connection()
                }
            },
            IncomingVersionReplicationState::RegisterBlobInDht => match event {
                Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
                | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
                | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
                _ => self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                    state: self.state_name(),
                    expected: "Event::Net(NetEvent::Dht(DhtEvent::*))",
                    received: event,
                }),
            },
            IncomingVersionReplicationState::SendApplyComplete => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::MessageSent)",
                        received: event,
                    });
                };
                self.close_connection()
            }
            IncomingVersionReplicationState::CloseConnection => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.fail(IncomingVersionReplicationError::InvalidStateEvent {
                        state: self.state_name(),
                        expected: "Event::Blob(BlobEvent::ConnectionClosed)",
                        received: event,
                    });
                };
                if self.output.as_ref().is_some_and(Result::is_err) {
                    self.state = IncomingVersionReplicationState::Error;
                } else {
                    self.state = IncomingVersionReplicationState::Finish;
                }
                if self.output.is_none() {
                    self.output = Some(Ok(()));
                }
                smallvec![]
            }
            IncomingVersionReplicationState::Finish => smallvec![],
            IncomingVersionReplicationState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingVersionReplicationState::Finish | IncomingVersionReplicationState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if self.state == IncomingVersionReplicationState::Error
            && let Some(Err(err)) = self.output
        {
            return Err(err);
        }
        Ok(self.output.unwrap_or(Ok(())))
    }

    fn abort(&mut self) -> Effects {
        let mut effects = smallvec![];

        if let Some(location) = self.cleanup_blob_location.take() {
            effects.push(Effect::Blob(BlobEffect::Delete { location }));
        }
        if let Some(txn_id) = self.txn_id.take() {
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
        }
        effects.push(Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        }));

        effects
    }
}

#[cfg(test)]
mod tests {
    use super::{IncomingVersionReplicationOperation, IncomingVersionReplicationState};
    use crate::replication::protocol::{
        MaterializedBlobInfo, VersionReplicationManifest, VersionReplicationMessage,
    };
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        BackendLocation, BucketInfo, Location, RealmId, ReplicationItemKind, UserIdentity,
        VersionMetadata,
    };
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn make_location() -> BackendLocation {
        let mut hashes = HashMap::new();
        hashes.insert("blake3".to_string(), vec![1u8; 32]);
        BackendLocation {
            root: "/tmp".to_string(),
            storage_bucket: "blob-bucket".to_string(),
            backend_path: format!("bucket/key_{}", Ulid::new()),
            ulid: Ulid::new(),
            compressed: false,
            encrypted: false,
            created_by: Ulid::new(),
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 42,
            hashes,
        }
    }

    fn make_manifest(kind: ReplicationItemKind) -> VersionReplicationManifest {
        let blob = match kind {
            ReplicationItemKind::Materialized => {
                let location = make_location();
                Some(MaterializedBlobInfo {
                    hash: [1u8; 32],
                    size: location.blob_size,
                    compressed: location.compressed,
                    encrypted: location.encrypted,
                    location,
                })
            }
            ReplicationItemKind::DeleteMarker => None,
        };

        VersionReplicationManifest {
            bucket: "bucket".to_string(),
            key: "dir/file.txt".to_string(),
            version_id: Ulid::new(),
            kind,
            bucket_info: BucketInfo {
                group_id: Ulid::new(),
                created_at: SystemTime::now(),
                created_by: Ulid::new(),
            },
            created_at: SystemTime::now(),
            created_by: Ulid::new(),
            current_version: true,
            user_identity: UserIdentity {
                user_id: Ulid::new(),
                realm_key: RealmId::from_bytes([7u8; 32]),
            },
            blob,
            multipart: None,
        }
    }

    fn message_from_effect(effect: &Effect) -> VersionReplicationMessage {
        let Effect::Blob(BlobEffect::SendMessage { payload, .. }) = effect else {
            panic!("expected blob send message effect")
        };
        VersionReplicationMessage::from_bytes(payload).unwrap()
    }

    #[test]
    fn existing_version_short_circuits_with_already_replicated_response() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest.clone(),
        );

        let effects = op.start();
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::ReadExistingVersion
        );
        assert_eq!(effects.len(), 1);

        let version = VersionMetadata {
            version_id: manifest.version_id,
            location: Location::Real(make_location()),
            created_at: manifest.created_at,
            created_by: manifest.created_by,
        };
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: Some(version.to_bytes().unwrap().into()),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::AlreadyReplicatedVersion
            )
        ));
    }

    #[test]
    fn delete_marker_requests_version_only() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));

        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly
            )
        ));
    }

    #[test]
    fn missing_blob_requests_blob_transfer() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let mut op = IncomingVersionReplicationOperation::new(
            Ulid::new(),
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::ReadExistingBlob);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendNegotiation);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionNegotiationResponse(
                aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion
            )
        ));
    }

    #[test]
    fn missing_bucket_is_created_during_apply() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::StartTransaction);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::StartTransaction { read: false })
        ));

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::EnsureBucket);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Read { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::WriteBucket);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Write { .. })
        ));
    }

    #[test]
    fn apply_failures_send_explicit_rejection_before_abort() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );

        op.start();
        op.step(Event::Storage(StorageEvent::ReadResult {
            key: vec![0u8; 4].into(),
            value: None,
        }));
        op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::new(),
        }));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { .. })
        ));

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted {
            txn_id: Ulid::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn received_blobs_are_deleted_after_apply_failure_before_commit() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::new();
        let received = make_location();
        let txn_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::EnsureBucket;
        op.txn_id = Some(txn_id);
        op.received_blob_location = Some(received.clone());
        op.cleanup_blob_location = Some(received.clone());

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(
            op.state,
            IncomingVersionReplicationState::CleanupReceivedBlob
        );
        assert_eq!(
            effects[0],
            Effect::Blob(BlobEffect::Delete { location: received })
        );

        let effects = op.step(Event::Blob(BlobEvent::DeleteFinished));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn failures_without_received_blob_close_without_delete() {
        let manifest = make_manifest(ReplicationItemKind::DeleteMarker);
        let stream_id = Ulid::new();
        let txn_id = Ulid::new();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedVersionOnly);
        op.state = IncomingVersionReplicationState::EnsureBucket;
        op.txn_id = Some(txn_id);

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::SendApplyRejected);
        assert!(matches!(
            message_from_effect(&effects[0]),
            VersionReplicationMessage::VersionApplyRejected(_)
        ));

        let effects = op.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::AbortTransaction);
        assert_eq!(
            effects[0],
            Effect::Storage(StorageEffect::AbortTransaction { txn_id })
        );

        let effects = op.step(Event::Storage(StorageEvent::TransactionAborted { txn_id }));
        assert_eq!(op.state, IncomingVersionReplicationState::CloseConnection);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }

    #[test]
    fn committed_replication_does_not_delete_received_blob_on_late_failure() {
        let manifest = make_manifest(ReplicationItemKind::Materialized);
        let stream_id = Ulid::new();
        let received = make_location();
        let mut op = IncomingVersionReplicationOperation::new(
            stream_id,
            iroh::SecretKey::generate(&mut rand::rng()).public(),
            RealmId::from_bytes([7u8; 32]),
            manifest,
        );
        op.negotiation_result =
            Some(aruna_core::structs::ReplicationNegotiationResult::NeedBlobAndVersion);
        op.state = IncomingVersionReplicationState::RegisterBlobInDht;
        op.received_blob_location = Some(received);
        op.apply_committed = true;

        let effects = op.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::new(),
        }));
        assert_eq!(op.state, IncomingVersionReplicationState::Error);
        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Blob(BlobEffect::CloseConnection { .. })
        ));
    }
}
