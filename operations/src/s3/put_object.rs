use aruna_blob::blob::{BLOB_LOCATION_DB, BLOB_PATH_DB};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, BoxError};
use aruna_core::structs::BlobInfo;
use aruna_core::types::{Effects, GroupId, UserId};
use byteview::ByteView;
use opendal::Operator;
use smallvec::smallvec;
use std::time::SystemTime;
use bytes::Bytes;
use thiserror::Error;

//TODO:
//  - Backend bucket management
//    - Stats db
//    - Create bucket on demand (No bucket available or all full)
//    - bucket: <prefix>_<uuid> | key: <key>.<ulid>
//  - Path creation/handling

#[derive(Debug, Eq, PartialEq)]
pub enum PutObjectState {
    Init,
    EvalBackendBucket, // Has to be done in the state machine as the io modules do not communicate directly
    GetOperator,       // Need to be generated individually
    WriteBlob,         // Gives the request body handle (custom stream necessary?) to blob io module
    StartTransaction,
    CreateBlob,
    CreatePermissions,
    CreatePath,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error)]
pub enum PutObjectError {
    #[error(transparent)]
    StorageError(#[from] StorageError),
    #[error("Invalid operation state")]
    InvalidOperationState,
    #[error("No transaction found")]
    NoTransactionFound,
    #[error("output is missing")]
    MissingOutput,
    #[error("hash missing: {0}")]
    MissingHash(String),
    #[error("request body missing")]
    MissingBody,
    #[error("body size did not match Content-Length header")]
    IncompleteBody,
    #[error("Something went wrong ...")]
    PutObjectFailed,
}

#[derive(Debug)]
pub struct PutObjectInput {
    bucket: String,
    key: String,
    content_length: Option<u64>,
    body: BackendStream<Result<Bytes, BoxError>>,
}

#[derive(Debug)]
pub struct PutObjectConfig {
    pub user_id: UserId,
    pub group_id: GroupId,
    pub request: PutObjectInput,
    pub exists: bool, //Note: For version shenanigans which will be implemented later
}

#[derive(Debug)]
pub struct PutObjectOperation {
    state: PutObjectState,
    config: PutObjectConfig,
    operator: Option<Operator>,
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BlobInfo, PutObjectError>>,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            operator: None,
            txn_id: None,
            output: None,
        }
    }

    fn emit_get_bucket_stats(&mut self) -> Effects {
        //Note: Bucket evaluation takes place in the blob io module
        todo!()
    }

    fn handle_init(&mut self) -> Effects {
        self.state = PutObjectState::GetOperator;
        smallvec![Effect::Blob(BlobEffect::GetOperator {
            bucket: Some(self.config.request.bucket.clone())
        })]
    }

    fn handle_operator_created(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::OperatorCreated { operator }) = event {
            self.operator = Some(operator.clone());
            self.state = PutObjectState::WriteBlob;

            if let Some(blob) = self.config.request.body.take() {
                smallvec![Effect::Blob(BlobEffect::Write {
                    operator,
                    path: "TODO".to_string(),
                    blob//: BackendStream::new(blob),
                })]
            } else {
                self.emit_error(PutObjectError::MissingBody)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn hande_write_finished(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::WriteFinished {
            backend_path,
            bytes_written,
            hashes,
        }) = event
        {
            // Check if the body was fully written
            if bytes_written != self.config.request.content_length.unwrap_or(0) as u64 {
                return self.emit_error(PutObjectError::IncompleteBody);
            }

            // Update output
            self.output = Some(Ok(BlobInfo {
                bucket: self.config.request.bucket.clone(),
                key: self.config.request.key.clone(),
                created_by: self.config.user_id.clone(),
                created_at: SystemTime::now(),
                staging: false,
                compressed: false,
                encrypted: false,
                partial: false,
                storage_path: backend_path,
                blob_size: bytes_written,
                hashes,
            }));

            self.state = PutObjectState::StartTransaction;
            smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: false
            })]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_transaction_started(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event {
            self.txn_id = Some(txn_id);
            self.state = PutObjectState::CreateBlob;

            if let Some(output) = self.get_output() {
                if let Some(blake3_hash) = output.get_blake3() {
                    let key = blake3_hash.into();
                    let value: ByteView = match output.to_bytes() {
                        Ok(bytes) => bytes.into(),
                        Err(_) => return self.emit_error(PutObjectError::PutObjectFailed),
                    };

                    smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: BLOB_LOCATION_DB.to_string(),
                        key,
                        value,
                        txn_id: self.txn_id,
                    })]
                } else {
                    self.emit_error(PutObjectError::MissingHash("blake3".to_string()))
                }
            } else {
                self.emit_error(PutObjectError::PutObjectFailed)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.state = PutObjectState::CommitTransaction;

            smallvec![Effect::Storage(StorageEffect::CommitTransaction {
                txn_id: self.txn_id.unwrap()
            })]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn emit_create_blob_path(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            let frontend_path = format!(
                "{}/{}/{}",
                self.config.group_id, self.config.request.bucket, self.config.request.key
            );
            self.state = PutObjectState::CreatePath;

            match self.get_blake3() {
                None => self.emit_error(PutObjectError::MissingHash("blake3".to_string())),
                Some(hash) => {
                    let key = frontend_path.into();
                    let value = hash.into();

                    smallvec![Effect::Storage(StorageEffect::Write {
                        key_space: BLOB_PATH_DB.to_string(),
                        key,
                        value,
                        txn_id: self.txn_id,
                    })]
                }
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_path_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            self.state = PutObjectState::CreatePermissions;

            //TODO: Create permissions
            smallvec![Effect::Storage(StorageEffect::CommitTransaction {txn_id: })]
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn emit_finish(&mut self) -> Effects {
        self.state = PutObjectState::Finish;
        smallvec![]
    }

    fn emit_error(&mut self, error: PutObjectError) -> Effects {
        self.state = PutObjectState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn get_blake3(&self) -> Option<&Vec<u8>> {
        self.output.as_ref()?.as_ref().ok()?.hashes.get("blake3")
    }

    fn get_output(&self) -> Option<&BlobInfo> {
        self.output.as_ref()?.as_ref().ok()
    }
}

impl Operation for PutObjectOperation {
    type Output = Option<Result<BlobInfo, PutObjectError>>;
    type Error = PutObjectError;

    fn start(&mut self) -> Effects {
        if self.state != PutObjectState::Init {
            self.state = PutObjectState::Error;
            smallvec![]
        } else {
            self.state = PutObjectState::GetOperator;
            smallvec![Effect::Blob(BlobEffect::GetOperator {
                bucket: Some(self.config.request.bucket.clone())
            })]
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            PutObjectState::Init => self.emit_get_bucket_stats(),
            PutObjectState::EvalBackendBucket => self.handle_init(),
            PutObjectState::GetOperator => self.handle_operator_created(event),
            PutObjectState::WriteBlob => self.hande_write_finished(event),
            PutObjectState::StartTransaction => self.handle_transaction_started(event),
            PutObjectState::CreateBlob => self.emit_create_blob_path(event),
            PutObjectState::CreatePath => self.handle_blob_created(event), //TODO: Create permissions as next step
            PutObjectState::CreatePermissions => todo!(),
            PutObjectState::CommitTransaction => self.emit_finish(),
            PutObjectState::Finish => smallvec![],
            PutObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutObjectState::Finish | PutObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if PutObjectState::Error == self.state {
            return Err(PutObjectError::PutObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        // Rollback blob io and transaction
        let mut actions = smallvec![];
        if let Some(operator) = &self.operator {
            actions.insert(
                0,
                Effect::Blob(BlobEffect::Delete {
                    operator: operator.clone(),
                    path: "".to_string(),
                }),
            )
        }
        if let Some(txn_id) = self.txn_id {
            actions.insert(
                1,
                Effect::Storage(StorageEffect::AbortTransaction { txn_id }),
            )
        }
        actions
    }
}
