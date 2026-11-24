//! The storage provider of `aruna://` and `s3://` addresses: every stat, listing, read and write
//! is a call to the host, which checks it as the run's creator. Bytes move in bounded chunks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

use aruna_workflow_contract::host::StorageEntry;
use aruna_workflow_contract::{ContractError, DATA_CHUNK_BYTES, HostRpcRequest, HostRpcResult};
use executor::BoxFuture;
use storage::{Details, DirectRef, Entry, Metadata, StorageError, StorageProvider, UploadReceipt};

use crate::protocol::RpcClient;

/// The address schemes the host serves.
pub const SCHEMES: [&str; 2] = ["aruna://", "s3://"];

pub struct CoordinatorStorage {
    rpc: RpcClient,
}

impl CoordinatorStorage {
    pub fn new(rpc: RpcClient) -> CoordinatorStorage {
        CoordinatorStorage { rpc }
    }
}

/// Whether the host serves an address.
pub fn served(address: &str) -> bool {
    SCHEMES.iter().any(|scheme| address.starts_with(scheme))
}

impl StorageProvider for CoordinatorStorage {
    fn name(&self) -> &str {
        "aruna"
    }

    fn matches(&self, address: &str) -> bool {
        served(address)
    }

    fn stat<'a>(
        &'a self,
        address: &'a str,
    ) -> BoxFuture<'a, Result<Option<Details>, StorageError>> {
        Box::pin(async move {
            let request = HostRpcRequest::StorageStat {
                address: address.to_owned(),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageStat { entry }) => {
                    Ok(entry.map(|entry| details(address, entry)))
                }
                Ok(other) => Err(unexpected("stat", address, &other)),
                Err(ContractError::NotFound { .. }) => Ok(None),
                Err(error) => Err(failed("stat", address, error)),
            }
        })
    }

    fn list<'a>(&'a self, address: &'a str) -> BoxFuture<'a, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            let request = HostRpcRequest::StorageList {
                address: address.to_owned(),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageList { entries }) => Ok((entries.into_iter())
                    .map(|entry| Entry {
                        name: entry.name,
                        directory: entry.directory,
                    })
                    .collect()),
                Ok(other) => Err(unexpected("list", address, &other)),
                Err(error) => Err(failed("list", address, error)),
            }
        })
    }

    fn download<'a>(
        &'a self,
        address: &'a str,
        target: &'a Path,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move {
            download_file(&self.rpc, address, target)
                .await
                .map_err(|error| failed("read", address, error))
        })
    }

    fn upload<'a>(
        &'a self,
        source: &'a Path,
        address: &'a str,
    ) -> BoxFuture<'a, Result<UploadReceipt, StorageError>> {
        Box::pin(async move {
            let receipt = upload_file(&self.rpc, source, address)
                .await
                .map_err(|error| failed("write", address, error))?;
            Ok(UploadReceipt {
                address: receipt.address,
                size: receipt.size,
                version: receipt.version,
                digest: receipt.sha256,
            })
        })
    }

    fn upload_absent<'a>(
        &'a self,
        source: &'a Path,
        address: &'a str,
    ) -> BoxFuture<'a, Result<Option<UploadReceipt>, StorageError>> {
        Box::pin(async move {
            if self.stat(address).await?.is_some() {
                return Ok(None);
            }
            self.upload(source, address).await.map(Some)
        })
    }
}

fn details(address: &str, entry: StorageEntry) -> Details {
    let modified = (entry.modified_ms)
        .and_then(|milliseconds| u64::try_from(milliseconds).ok())
        .and_then(|milliseconds| {
            SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(milliseconds))
        });
    let direct = (!entry.directory).then(|| DirectRef {
        url: address.to_owned(),
        aruna_id: entry.aruna_id.clone(),
        endpoint: entry.endpoints.first().cloned(),
        ..DirectRef::default()
    });
    Details {
        metadata: Metadata {
            directory: entry.directory,
            modified,
            size: entry.size,
            version: entry.version,
        },
        digest: entry.sha256,
        endpoints: entry.endpoints,
        direct,
    }
}

/// Reads a file of the host into `target` chunk by chunk, and gives its version.
pub async fn download_file(
    rpc: &RpcClient,
    address: &str,
    target: &Path,
) -> Result<Option<String>, ContractError> {
    let mut file = std::fs::File::create(target).map_err(local_error)?;
    let mut offset = 0u64;
    loop {
        let request = HostRpcRequest::StorageDownload {
            address: address.to_owned(),
            offset,
        };
        let (bytes, eof, version) = match rpc.call(request).await? {
            HostRpcResult::StorageChunk {
                bytes,
                eof,
                version,
            } => (bytes, eof, version),
            other => {
                return Err(ContractError::invalid(format!(
                    "unexpected answer {other:?}"
                )));
            }
        };
        if bytes.len() > DATA_CHUNK_BYTES || (bytes.is_empty() && !eof) {
            return Err(ContractError::invalid(format!(
                "a chunk of {} bytes of {address} is out of bounds",
                bytes.len()
            )));
        }
        file.write_all(&bytes).map_err(local_error)?;
        offset += bytes.len() as u64;
        if eof {
            file.flush().map_err(local_error)?;
            return Ok(version);
        }
    }
}

/// Writes a local file to an address of the host chunk by chunk, and gives the receipt.
pub async fn upload_file(
    rpc: &RpcClient,
    source: &Path,
    address: &str,
) -> Result<aruna_workflow_contract::host::UploadReceipt, ContractError> {
    let mut file = std::fs::File::open(source).map_err(local_error)?;
    let size = file.metadata().map_err(local_error)?.len();
    let start = HostRpcRequest::StorageUploadStart {
        address: address.to_owned(),
        size,
        digest: None,
    };
    let upload_id = match rpc.call(start).await? {
        HostRpcResult::UploadStarted { upload_id } => upload_id,
        other => {
            return Err(ContractError::invalid(format!(
                "unexpected answer {other:?}"
            )));
        }
    };
    let mut buffer = vec![0u8; DATA_CHUNK_BYTES];
    let mut seq = 0u64;
    let mut sent = 0u64;
    loop {
        let read = read_full(&mut file, &mut buffer)?;
        sent += read as u64;
        let eof = read < buffer.len() || sent == size;
        let chunk = HostRpcRequest::StorageUploadChunk {
            upload_id: upload_id.clone(),
            seq,
            bytes: buffer[..read].to_vec(),
            eof,
        };
        match rpc.call(chunk).await? {
            HostRpcResult::UploadAck {
                seq: acked,
                receipt,
            } if acked == seq => {
                if eof {
                    return receipt.ok_or_else(|| {
                        ContractError::invalid("the last chunk got no upload receipt")
                    });
                }
            }
            other => {
                return Err(ContractError::invalid(format!(
                    "unexpected answer {other:?}"
                )));
            }
        }
        seq += 1;
    }
}

/// Fills `buffer` as far as the file goes.
fn read_full(file: &mut std::fs::File, buffer: &mut [u8]) -> Result<usize, ContractError> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]).map_err(local_error)? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

fn local_error(error: std::io::Error) -> ContractError {
    ContractError::internal(format!("a local file failed: {error}"))
}

fn unexpected(action: &'static str, address: &str, answer: &HostRpcResult) -> StorageError {
    failed(
        action,
        address,
        ContractError::invalid(format!("unexpected answer {answer:?}")),
    )
}

fn failed(action: &'static str, address: &str, error: ContractError) -> StorageError {
    match error {
        ContractError::NotFound { .. } => StorageError::Missing {
            address: storage::address::redacted(address),
            provider: "aruna".into(),
        },
        error => StorageError::Failed {
            action,
            address: storage::address::redacted(address),
            provider: "aruna".into(),
            retryable: matches!(error, ContractError::Internal { .. }),
            message: error.to_string(),
        },
    }
}
