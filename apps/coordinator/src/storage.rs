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
use sha2::{Digest as _, Sha256};
use storage::{
    Details, DirectRef, Entry, Metadata, StorageError, StorageProvider, TreeFile, TreeManifest,
    UploadReceipt,
};

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
            let normalized = normalize_address(address);
            let request = HostRpcRequest::StorageStat {
                address: normalized.clone(),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageStat { entry }) => {
                    Ok(entry.map(|entry| details(&normalized, entry)))
                }
                Ok(other) => Err(unexpected("stat", &normalized, &other)),
                Err(ContractError::NotFound { .. }) => Ok(None),
                Err(error) => Err(failed("stat", &normalized, error)),
            }
        })
    }

    fn list<'a>(&'a self, address: &'a str) -> BoxFuture<'a, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            let normalized = normalize_address(address);
            let request = HostRpcRequest::StorageList {
                address: normalized.clone(),
            };
            match self.rpc.call(request).await {
                Ok(HostRpcResult::StorageList { entries }) => Ok((entries.into_iter())
                    .map(|entry| Entry {
                        name: entry.name,
                        directory: entry.directory,
                    })
                    .collect()),
                Ok(other) => Err(unexpected("list", &normalized, &other)),
                Err(error) => Err(failed("list", &normalized, error)),
            }
        })
    }

    fn tree_manifest<'a>(
        &'a self,
        address: &'a str,
    ) -> BoxFuture<'a, Result<TreeManifest, StorageError>> {
        Box::pin(async move {
            let root = normalize_address(address);
            let mut manifest = TreeManifest::default();
            let mut pending = vec![String::new()];
            while let Some(relative) = pending.pop() {
                let folder = join_address(&root, &relative, true);
                let entries = self.list(&folder).await?;
                if entries.is_empty() && !relative.is_empty() {
                    manifest.empty.push(relative.clone());
                }
                for entry in entries {
                    let name = entry.name.trim_end_matches('/');
                    if name.is_empty()
                        || name == "."
                        || name == ".."
                        || name.contains(['/', '\\'])
                        || name.chars().any(char::is_control)
                    {
                        return Err(StorageError::Failed {
                            action: "list",
                            address: storage::address::redacted(&folder),
                            provider: self.name().to_owned(),
                            message: format!("the listing names an invalid entry {:?}", entry.name),
                            retryable: false,
                        });
                    }
                    let path = match relative.is_empty() {
                        true => name.to_owned(),
                        false => format!("{relative}/{name}"),
                    };
                    if entry.directory {
                        pending.push(path);
                        continue;
                    }
                    let child = join_address(&root, &path, false);
                    let found = self
                        .stat(&child)
                        .await?
                        .ok_or_else(|| StorageError::Missing {
                            address: storage::address::redacted(&child),
                            provider: self.name().to_owned(),
                        })?;
                    manifest.files.push(TreeFile {
                        path,
                        size: found.metadata.size,
                        executable: false,
                        digest: found.verified().map(str::to_owned),
                        direct: found.direct,
                    });
                }
            }
            manifest
                .files
                .sort_by(|first, second| first.path.cmp(&second.path));
            manifest.empty.sort();
            Ok(manifest)
        })
    }

    fn download<'a>(
        &'a self,
        address: &'a str,
        target: &'a Path,
    ) -> BoxFuture<'a, Result<Option<String>, StorageError>> {
        Box::pin(async move {
            let normalized = normalize_address(address);
            download_file(&self.rpc, &normalized, target)
                .await
                .map_err(|error| failed("read", &normalized, error))
        })
    }

    fn upload<'a>(
        &'a self,
        source: &'a Path,
        address: &'a str,
    ) -> BoxFuture<'a, Result<UploadReceipt, StorageError>> {
        Box::pin(async move {
            let normalized = normalize_address(address);
            let receipt = upload_file(&self.rpc, source, &normalized)
                .await
                .map_err(|error| failed("write", &normalized, error))?;
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
            let normalized = normalize_address(address);
            if self.stat(&normalized).await?.is_some() {
                return Ok(None);
            }
            self.upload(source, &normalized).await.map(Some)
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

/// Whether `query` contains only Aruna `version=` or `node=` query pairs.
fn is_aruna_query(query: &str) -> bool {
    !query.is_empty()
        && query.split('&').all(|pair| {
            matches!(
                pair.split_once('='),
                Some(("version" | "node", val)) if !val.is_empty()
            )
        })
}

/// Moves child segments appended after an Aruna `?query` back into the object key.
pub(crate) fn normalize_address(address: &str) -> String {
    if !served(address) {
        return address.to_owned();
    }
    let Some((head, tail)) = address.split_once('?') else {
        return address.to_owned();
    };
    let Some((query, suffix)) = tail.split_once('/') else {
        return address.to_owned();
    };
    if !is_aruna_query(query) {
        return address.to_owned();
    }
    let base = head.trim_end_matches('/');
    if suffix.is_empty() {
        return format!("{base}/?{query}");
    }
    let encoded = aruna_workflow_contract::host::encode_key(suffix);
    format!("{base}/{encoded}?{query}")
}

/// Joins a relative child path onto a root address while keeping any Aruna `?query` at the end.
pub(crate) fn join_address(root: &str, relative: &str, folder: bool) -> String {
    let (head, query) = match root.split_once('?') {
        Some((head, q)) if is_aruna_query(q) => (head, Some(q)),
        _ => (root, None),
    };
    let base = head.trim_end_matches('/');
    let rel = relative.trim_matches('/');
    let mut joined = match rel.is_empty() {
        true => base.to_owned(),
        false => format!("{base}/{}", aruna_workflow_contract::host::encode_key(rel)),
    };
    if folder {
        joined.push('/');
    }
    match query {
        Some(q) => format!("{joined}?{q}"),
        None => joined,
    }
}

/// Whether `address` already pins an explicit `version=` query parameter.
pub(crate) fn has_version(address: &str) -> bool {
    address.split_once('?').is_some_and(|(_, query)| {
        query.split('&').any(|pair| {
            pair.split_once('=')
                .is_some_and(|(key, val)| key == "version" && !val.is_empty())
        })
    })
}

/// Pins an unversioned `aruna://` or `s3://` address to `version` for later chunks.
pub(crate) fn pin_version(address: &str, version: &str) -> String {
    if !served(address) || version.is_empty() || version.contains(['&', '?', '#']) {
        return address.to_owned();
    }
    let Some((_, query)) = address.split_once('?') else {
        return format!("{address}?version={version}");
    };
    match has_version(address) {
        true => address.to_owned(),
        false if query.is_empty() => format!("{address}version={version}"),
        false => format!("{address}&version={version}"),
    }
}

/// Metadata collected while streaming a file down from the host.
pub struct Downloaded {
    pub version: Option<String>,
    pub size: u64,
    pub sha256: [u8; 32],
}

/// Reads a file of the host into `target` chunk by chunk, and gives its version.
pub async fn download_file(
    rpc: &RpcClient,
    address: &str,
    target: &Path,
) -> Result<Option<String>, ContractError> {
    download_hashed(rpc, address, target)
        .await
        .map(|done| done.version)
}

/// Reads a file of the host into `target` chunk by chunk, returning its version, size and SHA-256.
pub async fn download_hashed(
    rpc: &RpcClient,
    address: &str,
    target: &Path,
) -> Result<Downloaded, ContractError> {
    let mut file = std::fs::File::create(target).map_err(local_error)?;
    let result = download_chunks(rpc, address, &mut file).await;
    if result.is_err() {
        drop(file);
        let _ = std::fs::remove_file(target);
    }
    result
}

async fn download_chunks(
    rpc: &RpcClient,
    address: &str,
    file: &mut std::fs::File,
) -> Result<Downloaded, ContractError> {
    let mut offset = 0u64;
    let mut pinned: Option<String> = None;
    let mut current = address.to_owned();
    let mut hasher = Sha256::new();
    loop {
        let request = HostRpcRequest::StorageDownload {
            address: current.clone(),
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
        if offset == 0 {
            if let Some(ref ver) = version {
                pinned = Some(ver.clone());
                current = pin_version(address, ver);
            }
        } else if pinned.is_some() && version != pinned {
            return Err(ContractError::invalid(format!(
                "object version of {address} changed during download"
            )));
        }
        hasher.update(&bytes);
        file.write_all(&bytes).map_err(local_error)?;
        offset += bytes.len() as u64;
        if eof {
            file.flush().map_err(local_error)?;
            return Ok(Downloaded {
                version: pinned.or(version),
                size: offset,
                sha256: hasher.finalize().into(),
            });
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

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_workflow_contract::{
        CoordinatorToHost, HostToCoordinator, decode_frame_len, decode_message, encode_message,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn pins_unversioned_addresses() {
        assert_eq!(pin_version("s3://b/k", "v1"), "s3://b/k?version=v1");
        assert_eq!(
            pin_version("aruna://b/k?node=n1", "v1"),
            "aruna://b/k?node=n1&version=v1"
        );
        assert_eq!(
            pin_version("aruna://b/k?version=v0", "v1"),
            "aruna://b/k?version=v0"
        );
        assert_eq!(
            pin_version("aruna-staging://x/0", "v1"),
            "aruna-staging://x/0"
        );
    }

    #[test]
    fn normalizes_query_paths() {
        assert_eq!(
            normalize_address("s3://results/folder/?node=abc/"),
            "s3://results/folder/?node=abc"
        );
        assert_eq!(
            normalize_address("s3://results/folder?node=abc/"),
            "s3://results/folder/?node=abc"
        );
        assert_eq!(
            normalize_address("s3://results/folder/?node=abc/sub/"),
            "s3://results/folder/sub/?node=abc"
        );
        assert_eq!(
            normalize_address("s3://results/folder/?node=abc/sub/b.txt"),
            "s3://results/folder/sub/b.txt?node=abc"
        );
        assert_eq!(
            normalize_address("s3://results/folder/?node=abc/a?1.txt"),
            "s3://results/folder/a%3F1.txt?node=abc"
        );
        // Non-query literal `?` in a local directory path is left untouched.
        assert_eq!(
            normalize_address("s3://results/dir?1/a.txt"),
            "s3://results/dir?1/a.txt"
        );
        assert_eq!(
            join_address("s3://results/folder/?node=abc", "", true),
            "s3://results/folder/?node=abc"
        );
        assert_eq!(
            join_address("s3://results/folder/?node=abc", "sub/b.txt", false),
            "s3://results/folder/sub/b.txt?node=abc"
        );
        assert_eq!(
            join_address("s3://results/folder/?node=abc", "sub", true),
            "s3://results/folder/sub/?node=abc"
        );
    }

    async fn read_rpc(
        reader: &mut tokio::io::ReadHalf<tokio::io::DuplexStream>,
    ) -> (u64, HostRpcRequest) {
        let mut prefix = [0u8; 4];
        reader.read_exact(&mut prefix).await.unwrap();
        let len = decode_frame_len(prefix).unwrap();
        let mut body = vec![0u8; len];
        reader.read_exact(&mut body).await.unwrap();
        match decode_message(&body).unwrap() {
            CoordinatorToHost::RpcRequest { call_id, request } => (call_id, request),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn send_rpc(
        writer: &mut tokio::io::WriteHalf<tokio::io::DuplexStream>,
        call_id: u64,
        result: HostRpcResult,
    ) {
        let frame = encode_message(&HostToCoordinator::RpcResponse { call_id, result }).unwrap();
        writer.write_all(&frame).await.unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn pins_multichunk_version() {
        let (host, coord) = tokio::io::duplex(1 << 20);
        let (coord_r, coord_w) = tokio::io::split(coord);
        let (mut host_r, mut host_w) = tokio::io::split(host);
        let (rpc, writer, mut inbound) = crate::protocol::start(coord_r, coord_w);
        let pump = tokio::spawn({
            let rpc = rpc.clone();
            async move {
                while let Some(crate::protocol::Inbound::Message(msg)) = inbound.recv().await {
                    if let HostToCoordinator::RpcResponse { call_id, result } = *msg {
                        rpc.answer(call_id, result);
                    }
                }
            }
        });
        let server = tokio::spawn(async move {
            let (id1, req1) = read_rpc(&mut host_r).await;
            assert_eq!(
                req1,
                HostRpcRequest::StorageDownload {
                    address: "s3://data/big.bin".into(),
                    offset: 0,
                }
            );
            send_rpc(
                &mut host_w,
                id1,
                HostRpcResult::StorageChunk {
                    bytes: b"hello ".to_vec(),
                    eof: false,
                    version: Some("v1".into()),
                },
            )
            .await;
            let (id2, req2) = read_rpc(&mut host_r).await;
            assert_eq!(
                req2,
                HostRpcRequest::StorageDownload {
                    address: "s3://data/big.bin?version=v1".into(),
                    offset: 6,
                }
            );
            send_rpc(
                &mut host_w,
                id2,
                HostRpcResult::StorageChunk {
                    bytes: b"world".to_vec(),
                    eof: true,
                    version: Some("v1".into()),
                },
            )
            .await;
            // Second download: host returns mismatched version on chunk 2.
            let (id3, _) = read_rpc(&mut host_r).await;
            send_rpc(
                &mut host_w,
                id3,
                HostRpcResult::StorageChunk {
                    bytes: b"part1".to_vec(),
                    eof: false,
                    version: Some("v1".into()),
                },
            )
            .await;
            let (id4, _) = read_rpc(&mut host_r).await;
            send_rpc(
                &mut host_w,
                id4,
                HostRpcResult::StorageChunk {
                    bytes: b"part2".to_vec(),
                    eof: true,
                    version: Some("v2".into()),
                },
            )
            .await;
        });

        let dir = tempfile::tempdir().unwrap();
        let ok_path = dir.path().join("ok.bin");
        let version = download_file(&rpc, "s3://data/big.bin", &ok_path)
            .await
            .unwrap();
        assert_eq!(version.as_deref(), Some("v1"));
        assert_eq!(std::fs::read(&ok_path).unwrap(), b"hello world");

        let bad_path = dir.path().join("bad.bin");
        let err = download_file(&rpc, "s3://data/big.bin", &bad_path)
            .await
            .unwrap_err();
        assert!(matches!(err, ContractError::Invalid { .. }), "{err:?}");
        assert!(!bad_path.exists());

        server.await.unwrap();
        rpc.close_calls("done");
        writer.close().await.unwrap();
        pump.abort();
    }
}
