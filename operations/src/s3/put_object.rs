use aruna_blob::blob::{BLOB_LOCATION_DB, BLOB_PATH_DB};
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::operation::Operation;
use aruna_core::stream::{BackendStream, BoxError};
use aruna_core::structs::BlobInfo;
use aruna_core::types::{Effects, GroupId, UserId};
use bytes::Bytes;
use byteview::ByteView;
use smallvec::smallvec;
use std::time::SystemTime;
use thiserror::Error;

#[derive(Debug, Eq, PartialEq)]
pub enum PutObjectState {
    Init,
    WriteBlob,
    StartTransaction,
    CreateBlob,
    CreatePathMapping,
    CreatePermissions,
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
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error("Something went wrong ...")]
    PutObjectFailed,
}

#[derive(Debug)]
pub struct PutObjectInput {
    bucket: String,
    key: String,
    content_length: Option<u64>,
    body: Option<BackendStream<Result<Bytes, BoxError>>>,
    //TODO: tbc
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
    txn_id: Option<ulid::Ulid>,
    output: Option<Result<BlobInfo, PutObjectError>>,
}

impl PutObjectOperation {
    pub fn new(config: PutObjectConfig) -> Self {
        PutObjectOperation {
            state: PutObjectState::Init,
            config,
            txn_id: None,
            output: None,
        }
    }

    fn handle_init(&mut self) -> Effects {
        self.state = PutObjectState::WriteBlob;
        if let Some(blob) = self.config.request.body.take() {
            smallvec![Effect::Blob(BlobEffect::Write {
                bucket: self.config.request.bucket.clone(),
                key: self.config.request.key.clone(),
                blob
            })]
        } else {
            self.emit_error(PutObjectError::MissingBody)
        }
    }

    fn handle_write_finished(&mut self, event: Event) -> Effects {
        if let Event::Blob(BlobEvent::WriteFinished {
            location,
            bytes_written,
            hashes,
        }) = event
        {
            // Check if the body was fully written
            if bytes_written != self.config.request.content_length.unwrap_or(0) {
                return self.emit_error(PutObjectError::IncompleteBody);
            }

            // Update output
            self.output = Some(Ok(BlobInfo {
                location,
                created_by: self.config.user_id,
                created_at: SystemTime::now(),
                staging: false,
                partial: false,
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
                    let key = blake3_hash.as_slice().into();
                    let value: ByteView = match output.to_bytes() {
                        Ok(bytes) => bytes.into(),
                        Err(e) => return self.emit_error(PutObjectError::ConversionError(e)),
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
                self.emit_error(PutObjectError::MissingOutput)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_blob_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            let frontend_path = format!(
                "{}/{}/{}",
                self.config.group_id, self.config.request.bucket, self.config.request.key
            );
            self.state = PutObjectState::CreatePermissions;

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

    fn handle_path_mapping_created(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::WriteResult { .. }) = event {
            //TODO: Create permissions
            // self.state = PutObjectState::CreatePermissions;
            // [...]

            if let Some(txn_id) = self.txn_id {
                self.state = PutObjectState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            } else {
                self.emit_error(PutObjectError::NoTransactionFound)
            }
        } else {
            self.emit_error(PutObjectError::InvalidOperationState)
        }
    }

    fn handle_transaction_committed(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event {
            self.state = PutObjectState::Finish;
            self.txn_id = None;
            smallvec![]
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

    fn get_blake3(&self) -> Option<Vec<u8>> {
        self.output
            .as_ref()?
            .as_ref()
            .ok()?
            .hashes
            .get("blake3")
            .cloned()
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
            self.emit_error(PutObjectError::InvalidOperationState)
        } else {
            self.handle_init()
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        match &self.state {
            PutObjectState::Init => self.handle_init(),
            PutObjectState::WriteBlob => self.handle_write_finished(event),
            PutObjectState::StartTransaction => self.handle_transaction_started(event),
            PutObjectState::CreateBlob => self.handle_blob_created(event),
            PutObjectState::CreatePathMapping => self.handle_blob_created(event),
            PutObjectState::CreatePermissions => self.handle_path_mapping_created(event),
            PutObjectState::CommitTransaction => self.handle_transaction_committed(event),
            PutObjectState::Finish => self.emit_finish(),
            PutObjectState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, PutObjectState::Finish | PutObjectState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        if PutObjectState::Error == self.state {
            if let Some(Err(error)) = self.output {
                return Err(error);
            }
            return Err(PutObjectError::PutObjectFailed);
        }
        Ok(self.output)
    }

    fn abort(&mut self) -> Effects {
        // Rollback blob io and transaction
        let mut actions = smallvec![];
        if let Some(output) = self.get_output() {
            actions.insert(
                0,
                Effect::Blob(BlobEffect::Delete {
                    location: output.location.clone(),
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

#[cfg(test)]
mod test {
    use crate::driver::{DriverContext, drive};
    use crate::s3::put_object::{PutObjectConfig, PutObjectInput, PutObjectOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::{Backend, BackendConfig};
    use aruna_storage::storage;
    use std::collections::HashMap;
    use std::fs::{exists, read_to_string};
    use tempfile::tempdir;
    use ulid::Ulid;

    #[tokio::test]
    pub async fn test_put_object() {
        let temp_handle = tempdir().unwrap();
        let temp_root = temp_handle.path().to_str().unwrap();
        let storage_handle = storage::FjallStorage::open(temp_root).unwrap();
        let blob_handle = BlobHandler::new(
            BackendConfig {
                backend_type: Backend::FileSystem,
                bucket_prefix: Some("aruna_".to_string()),
                max_bucket_size: Some(100000),
                root: temp_root.to_string(),
                service_config: HashMap::new(),
            },
            storage_handle.clone(),
        )
        .await
        .unwrap();

        let data = b"hello, world!";
        let stream = tokio_util::io::ReaderStream::new(&data[..]);
        let put_config = PutObjectConfig {
            user_id: Ulid::new(),
            group_id: Ulid::new(),
            request: PutObjectInput {
                bucket: "mybucket".to_string(),
                key: "some-file.txt".to_string(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(stream)),
            },
            exists: false,
        };
        let put_operation = PutObjectOperation::new(put_config);

        let context = DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: Some(blob_handle),
        };
        // Jesus, Take the Wheel!
        let result = drive(put_operation, &context)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert!(exists(result.location.get_full_path().unwrap()).unwrap());
        assert_eq!(
            read_to_string(result.location.get_full_path().unwrap()).unwrap(),
            String::from_utf8_lossy(&data[..]).to_string()
        );
    }
}
