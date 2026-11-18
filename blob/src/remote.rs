//! Runs repository, provider and stored artifact calls, each ending when its job must stop.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::effects::BlobEffect;
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::repository::ImportMode;
use aruna_core::repository::invenio::{file_path, record_id};
use aruna_core::repository::remote::{
    ArchiveSpool, FileDigests, InvenioAccess, InvenioMethod, JobStop, RemoteAnswer, RemoteCall,
    RemoteFailure, RemoteRequest,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::Hasher;
use aruna_core::structs::execution::job::{ArtifactRef, RoCrateLimits};
use aruna_core::structs::storage::blob::{BackendLocation, HiddenBlobKey};
use async_zip::base::write::ZipFileWriter;
use async_zip::error::ZipError;
use async_zip::{Compression, ZipEntryBuilder};
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::io::AsyncWriteExt;
use reqwest::{Method, Url};
use serde_json::{Value, json};
use tokio::io::DuplexStream;

use crate::blob::BlobHandle;
use crate::invenio::InvenioClient;

/// Hard cap on one decoded OAI-PMH response body.
const OAI_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Total wall clock one OAI-PMH fetch may take, connect through last byte.
const OAI_FETCH_DEADLINE: Duration = Duration::from_secs(120);
/// How long removing a failed archive may take.
const ARCHIVE_DELETE_BUDGET: Duration = Duration::from_secs(30);

type Answer = Result<RemoteAnswer, RemoteFailure>;

/// Runs `work`, unless the job must stop first; a stop ends it at once.
async fn until_stop<T>(
    stop: Option<&JobStop>,
    work: impl Future<Output = Result<T, RemoteFailure>>,
) -> Result<T, RemoteFailure> {
    let Some(stop) = stop else {
        return work.await;
    };
    if let Some(reason) = stop.reason() {
        return Err(RemoteFailure::Stopped(reason));
    }
    tokio::select! {
        biased;
        reason = stop.stopped() => Err(RemoteFailure::Stopped(reason)),
        result = work => result,
    }
}

fn permanent(message: &str) -> RemoteFailure {
    RemoteFailure::Permanent(message.to_string())
}

fn retryable(message: impl Into<String>) -> RemoteFailure {
    RemoteFailure::Retryable(message.into())
}

fn zip_failure(error: ZipError) -> RemoteFailure {
    retryable(error.to_string())
}

fn write_failure(error: std::io::Error) -> RemoteFailure {
    retryable(error.to_string())
}

impl BlobHandle {
    /// A blob effect of a call; boxed because the call itself runs as a blob effect.
    fn nested(&self, effect: BlobEffect) -> BoxFuture<'_, Event> {
        Box::pin(self.send_blob_effect(effect))
    }

    /// Runs one call for an operation; the answer travels back as `BlobEvent::Remote`.
    pub(crate) async fn run_remote(&self, request: RemoteRequest) -> Answer {
        let RemoteRequest { stop, call } = request;
        match call {
            // The spool keeps running after a stop, so the partial archive is removed.
            RemoteCall::SpoolArchive(spool) => self.spool_archive(*spool, stop.as_ref()).await,
            call => until_stop(stop.as_ref(), self.remote_call(call)).await,
        }
    }

    async fn remote_call(&self, call: RemoteCall) -> Answer {
        match call {
            RemoteCall::Checkpoint => Ok(RemoteAnswer::Done),
            RemoteCall::InvenioJson {
                access,
                method,
                url,
                body,
                if_match,
            } => {
                let client = InvenioClient::new(self.egress(), access);
                let answer = match (if_match, body.as_ref()) {
                    (Some(revision), Some(body)) => client.update(url, body, revision).await,
                    _ => client.json(http_method(method), url, body.as_ref()).await,
                };
                answer
                    .map(RemoteAnswer::Json)
                    .map_err(RemoteFailure::Invenio)
            }
            RemoteCall::InvenioDelete { access, url } => InvenioClient::new(self.egress(), access)
                .delete(url)
                .await
                .map(|()| RemoteAnswer::Done)
                .map_err(RemoteFailure::Invenio),
            RemoteCall::InvenioHead { access, url } => InvenioClient::new(self.egress(), access)
                .head(url)
                .await
                .map(RemoteAnswer::Metadata)
                .map_err(RemoteFailure::Invenio),
            RemoteCall::InvenioUpload {
                access,
                url,
                size,
                source,
                range,
            } => self.upload_range(access, url, size, source, range).await,
            RemoteCall::HashRange {
                source,
                range,
                limit,
            } => self.hash_range(source, range, limit).await,
            RemoteCall::ReadRange { source, range } => self.read_range(source, range).await,
            RemoteCall::OaiFetch { url } => self.fetch_oai(url).await.map(RemoteAnswer::Text),
            RemoteCall::SpoolArchive(_) => Err(permanent("the archive spool runs on its own")),
        }
    }

    async fn stored_range(
        &self,
        location: BackendLocation,
        range: Range<u64>,
    ) -> Result<BackendStream<Result<Bytes, StreamError>>, RemoteFailure> {
        let effect = BlobEffect::ReadHiddenRange { location, range };
        match self.nested(effect).await {
            Event::Blob(BlobEvent::HiddenRead { blob, .. }) => Ok(blob),
            Event::Blob(BlobEvent::Error(error)) => Err(retryable(error.to_string())),
            event => Err(retryable(format!(
                "unexpected hidden artifact read event: {}",
                event.label()
            ))),
        }
    }

    async fn hash_range(&self, source: BackendLocation, range: Range<u64>, limit: u64) -> Answer {
        let mut blob = self.stored_range(source, range).await?;
        let mut hasher = Hasher::new();
        let mut size = 0u64;
        while let Some(chunk) = blob.next().await {
            let chunk = chunk.map_err(|_| retryable("crate file read failed"))?;
            size = size
                .checked_add(chunk.len() as u64)
                .filter(|size| *size <= limit)
                .ok_or_else(|| permanent("crate file exceeds snapshot size"))?;
            hasher.update(&chunk);
        }
        Ok(RemoteAnswer::Digests(FileDigests::of(&hasher, size)))
    }

    async fn read_range(&self, source: BackendLocation, range: Range<u64>) -> Answer {
        let mut blob = self.stored_range(source, range).await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = blob.next().await {
            let chunk = chunk.map_err(|_| retryable("crate file read failed"))?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(RemoteAnswer::Bytes(bytes))
    }

    async fn upload_range(
        &self,
        access: InvenioAccess,
        url: Url,
        size: u64,
        source: BackendLocation,
        range: Range<u64>,
    ) -> Answer {
        let blob = self.stored_range(source, range).await?;
        let hasher = Arc::new(Mutex::new(Hasher::new()));
        let sent = hasher.clone();
        let stream = blob.map(move |chunk| {
            if let Ok(bytes) = &chunk {
                sent.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .update(bytes);
            }
            chunk
        });
        InvenioClient::new(self.egress(), access)
            .upload(url, size, BackendStream::new(stream))
            .await
            .map_err(RemoteFailure::Invenio)?;
        let hasher = hasher.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(RemoteAnswer::Digests(FileDigests::of(&hasher, size)))
    }

    /// Writes the archive into hidden storage while it downloads the files. A failed archive
    /// is removed again; a stop ends the download, then the spool sees the end of the stream.
    async fn spool_archive(&self, spool: ArchiveSpool, stop: Option<&JobStop>) -> Answer {
        let client = InvenioClient::new(self.egress(), spool.access.clone());
        let (writer, reader) = tokio::io::duplex(128 * 1024);
        let write = until_stop(
            stop,
            write_archive(
                &client,
                writer,
                &spool.metadata,
                &spool.records,
                &spool.limits,
                spool.mode,
            ),
        );
        let store = self.nested(BlobEffect::SpoolHidden {
            namespace: spool.namespace,
            name: "input".into(),
            created_by: spool.created_by,
            max_bytes: Some(spool.limits.import_source_bytes),
            deadline: None,
            blob: BackendStream::new(tokio_util::io::ReaderStream::new(reader)),
        });
        let (written, event) = tokio::join!(write, store);
        match event {
            Event::Blob(BlobEvent::HiddenSpooled {
                location,
                size,
                blake3,
            }) => {
                if let Err(error) = written {
                    self.discard_archive(&location).await;
                    return Err(error);
                }
                Ok(RemoteAnswer::Artifact(ArtifactRef {
                    location,
                    size,
                    blake3,
                    expires_at_ms: 0,
                }))
            }
            Event::Blob(BlobEvent::Error(BlobError::SizeLimitExceeded { .. })) => {
                Err(permanent("repository archive exceeds import source limit"))
            }
            _ => {
                written?;
                Err(retryable("repository archive spool failed"))
            }
        }
    }

    async fn discard_archive(&self, location: &BackendLocation) {
        let Ok(key) = HiddenBlobKey::try_from(location) else {
            return;
        };
        let delete = self.nested(BlobEffect::DeleteHidden { key });
        let _ = tokio::time::timeout(ARCHIVE_DELETE_BUDGET, delete).await;
    }

    /// Fetches one OAI-PMH response under a byte cap and a wall-clock deadline. Egress bounds
    /// only connect and read inactivity, so a slow-drip response would otherwise run on.
    async fn fetch_oai(&self, url: Url) -> Result<String, RemoteFailure> {
        let deadline = tokio::time::Instant::now() + OAI_FETCH_DEADLINE;
        let request = self
            .egress_request(url)
            .map_err(|error| RemoteFailure::Permanent(format!("egress screen: {error}")))?;
        let mut response = tokio::time::timeout_at(deadline, request.send())
            .await
            .map_err(|_| retryable("OAI fetch exceeded the total deadline"))?
            .map_err(|error| retryable(format!("OAI fetch: {error}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let hint = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(|value| format!(", retry after {value}"))
                .unwrap_or_default();
            return Err(retryable(format!("OAI HTTP status {status}{hint}")));
        }
        if let Some(declared) = response.content_length()
            && declared > OAI_BODY_BYTES as u64
        {
            return Err(RemoteFailure::Permanent(format!(
                "OAI response declares {declared} bytes, over the {OAI_BODY_BYTES} byte cap"
            )));
        }
        let mut body: Vec<u8> = Vec::new();
        loop {
            let chunk = tokio::time::timeout_at(deadline, response.chunk())
                .await
                .map_err(|_| retryable("OAI fetch exceeded the total deadline"))?
                .map_err(|error| retryable(format!("OAI body: {error}")))?;
            let Some(chunk) = chunk else {
                break;
            };
            if body.len() + chunk.len() > OAI_BODY_BYTES {
                return Err(RemoteFailure::Permanent(format!(
                    "OAI response body exceeds the {OAI_BODY_BYTES} byte cap"
                )));
            }
            body.extend_from_slice(&chunk);
        }
        String::from_utf8(body)
            .map_err(|error| RemoteFailure::Permanent(format!("OAI body is not UTF-8: {error}")))
    }
}

fn http_method(method: InvenioMethod) -> Method {
    match method {
        InvenioMethod::Get => Method::GET,
        InvenioMethod::Post => Method::POST,
        InvenioMethod::Put => Method::PUT,
    }
}

async fn write_archive(
    client: &InvenioClient<'_>,
    writer: DuplexStream,
    metadata: &str,
    records: &[(Value, Value)],
    limits: &RoCrateLimits,
    mode: ImportMode,
) -> Result<(), RemoteFailure> {
    let mut archive = ZipFileWriter::with_tokio(writer);
    archive
        .write_entry_whole(entry("ro-crate-metadata.json"), metadata.as_bytes())
        .await
        .map_err(zip_failure)?;
    let mut size = metadata.len() as u64;
    let mut paths = HashSet::new();
    for (record, files) in records {
        let id = record_id(record).map_err(|error| RemoteFailure::Permanent(error.to_string()))?;
        let provenance = json!({"record": record, "files": files}).to_string();
        size = checked_size(size, provenance.len() as u64, limits.expanded_import_bytes)?;
        archive
            .write_entry_whole(
                entry(&format!("versions/{id}/invenio-record.json")),
                provenance.as_bytes(),
            )
            .await
            .map_err(zip_failure)?;
        for file in files["entries"]
            .as_array()
            .ok_or_else(|| permanent("missing files"))?
        {
            let key = file["key"]
                .as_str()
                .ok_or_else(|| permanent("missing file key"))?;
            let path =
                file_path(id, key).map_err(|error| RemoteFailure::Permanent(error.to_string()))?;
            if path.len() as u64 > limits.key_bytes || !paths.insert(path.clone()) {
                return Err(permanent("duplicate or oversized file path"));
            }
            if mode == ImportMode::Reference {
                let descriptor = json!({"record_id": id, "file": file}).to_string();
                size = checked_size(size, descriptor.len() as u64, limits.expanded_import_bytes)?;
                archive
                    .write_entry_whole(entry(&path), descriptor.as_bytes())
                    .await
                    .map_err(zip_failure)?;
                continue;
            }
            let expected = file["size"]
                .as_u64()
                .ok_or_else(|| permanent("missing file size"))?;
            size = checked_size(size, expected, limits.expanded_import_bytes)?;
            let checksum = file["checksum"]
                .as_str()
                .ok_or_else(|| permanent("missing file checksum"))?;
            let (algorithm, digest) = checksum
                .split_once(':')
                .ok_or_else(|| permanent("invalid checksum"))?;
            if !matches!(algorithm, "md5" | "sha1" | "sha256" | "blake3") {
                return Err(permanent("unsupported repository checksum"));
            }
            let url = client
                .url(&["records", id, "files", key, "content"])
                .map_err(RemoteFailure::Invenio)?;
            let mut response = client.download(url).await.map_err(RemoteFailure::Invenio)?;
            let mut target = archive
                .write_entry_stream(entry(&path))
                .await
                .map_err(zip_failure)?;
            let mut actual = 0u64;
            let mut hasher = Hasher::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| retryable("repository file read failed"))?
            {
                actual = checked_size(actual, chunk.len() as u64, expected)?;
                hasher.update(&chunk);
                target.write_all(&chunk).await.map_err(write_failure)?;
            }
            let hashes = hasher.to_map();
            if actual != expected
                || hashes.get(algorithm).map(hex::encode).as_deref() != Some(digest)
            {
                return Err(permanent("repository file size or checksum mismatch"));
            }
            target.close().await.map_err(zip_failure)?;
        }
    }
    archive.close().await.map_err(zip_failure)?;
    Ok(())
}

fn entry(path: &str) -> ZipEntryBuilder {
    ZipEntryBuilder::new(path.into(), Compression::Stored)
}

fn checked_size(size: u64, added: u64, limit: u64) -> Result<u64, RemoteFailure> {
    size.checked_add(added)
        .filter(|size| *size <= limit)
        .ok_or_else(|| permanent("repository data exceeds size limit"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::repository::remote::StopReason;
    use tokio_util::sync::CancellationToken;

    fn stop() -> (JobStop, CancellationToken, CancellationToken) {
        let (cancel, shutdown) = (CancellationToken::new(), CancellationToken::new());
        (
            JobStop::new(cancel.clone(), shutdown.clone()),
            cancel,
            shutdown,
        )
    }

    #[tokio::test]
    async fn stopped_job_refused() {
        let (job, cancel, shutdown) = stop();
        shutdown.cancel();
        cancel.cancel();
        let work = async { panic!("the call must not start") };
        let result: Result<(), _> = until_stop(Some(&job), work).await;
        assert_eq!(result, Err(RemoteFailure::Stopped(StopReason::Cancelled)));
    }

    #[tokio::test]
    async fn stop_ends_call() {
        let (job, _cancel, shutdown) = stop();
        let work = std::future::pending::<Result<(), RemoteFailure>>();
        let (result, ()) = tokio::join!(until_stop(Some(&job), work), async { shutdown.cancel() });
        assert_eq!(result, Err(RemoteFailure::Stopped(StopReason::Interrupted)));
    }

    #[tokio::test]
    async fn unwatched_call_runs() {
        let result = until_stop(None, async { Ok::<_, RemoteFailure>(7) }).await;
        assert_eq!(result, Ok(7));
    }

    #[test]
    fn size_limit_holds() {
        assert_eq!(checked_size(1, 2, 3), Ok(3));
        assert!(checked_size(2, 2, 3).is_err());
        assert!(checked_size(u64::MAX, 1, u64::MAX).is_err());
    }
}
