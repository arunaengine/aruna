//! Exports the provenance of a file version or dataset as one RO-Crate, streamed as a ZIP while
//! the links are followed. Nothing is stored.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Each hop is read with the caller's credential on the node that holds it, exactly as the
// origin and run routes answer it; the crate shows only what those reads show.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::bucket_metadata::RELEASE_TAG;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::release::{
    CrateChunk, CrateRead, CrateReadError, HolderAnswer, HolderAsk,
};
use aruna_core::structs::storage::replication::W3idIdentifier;
use aruna_operations::distribution::resolve::{GlobalResolution, RESOLVE_BUDGET, resolve_version};
use aruna_operations::metadata::MetadataReadError;
use aruna_operations::metadata::provenance::{
    CrateFile, Hop, ProvenanceCrate, Target, scratch_dir, scratch_file, scratch_name,
};
use aruna_operations::s3::bucket::release::publish::read_crate;
use async_zip::tokio::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use axum::{Extension, Json};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::io::AsyncWriteExt;
use serde::Deserialize;
use tokio::io::DuplexStream;
use tokio::time::Instant;
use tokio_util::io::ReaderStream;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ValidatedBearer, bearer_auth, ensure_permission, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::metadata::{MetadataRoCrateResponse, RoCrateExportParams};
use crate::routes::execution::runs::run_answer;
use crate::routes::metadata::rocrate::export_metadata_rocrate;
use crate::routes::storage::deletion::bucket_info;
use crate::routes::storage::origin::version_answer;
use crate::server::state::ServerState;

/// The technical time limit of one export; the crate is marked incomplete when it is reached.
const EXPORT_DEADLINE: Duration = Duration::from_secs(15 * 60);
/// The most time kept back for writing the root and closing the ZIP.
const FINISH_WINDOW: Duration = Duration::from_secs(60);
/// Why a walk ends when the caller's token no longer holds.
const REVOKED: &str = "the token expired or was revoked";
/// Bytes buffered between the walk and the client.
const PIPE_BYTES: usize = 64 * 1024;
/// A regular file readable by everyone, so an unpacked crate opens without a chmod.
const FILE_MODE: u16 = 0o100_644;

#[derive(OpenApi)]
#[openapi()]
pub struct ProvenanceApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(ProvenanceApiDoc::openapi()).routes(routes!(export_provenance))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ExportParams {
    /// Start at this exact version: a global id URL or a versioned object w3id.
    pub version: Option<String>,
    /// Start at this dataset document, from its file parts with exact versions.
    pub dataset: Option<String>,
}

#[utoipa::path(
    get,
    path = "/data/provenance/crate",
    tag = "data/storage",
    summary = "Export where data came from as a crate",
    description = r#"Streams one RO-Crate ZIP with the provenance of a file version or dataset, assembled while the links are followed.

**Authentication**: realm bearer token. Each hop is read with this token on the node that holds
it, as the origin and run routes do.

**Behavior**
- The root `ro-crate-metadata.json` lists versions, runs, sources, database identifiers and
  their links. Each readable run crate is a nested crate under `runs/{run_id}/`; a dataset
  start is nested under `dataset/`. File bytes are referenced, not included.
- Each release that pins a version is nested under `releases/`, read from its exact tagged
  crate version with this token, on the node that holds it. A holder that does not answer
  marks the crate incomplete.
- A hop the caller may not read is named by its global id or the digest that referred to it,
  never by bucket or key. A node that does not answer is listed, and the crate is incomplete.
- Each version and run is read once per export, so cycles end.
- The token is checked again before each hop; the walk stops when it expires or is revoked,
  when the client disconnects or at the time limit. The root then says `Incomplete` in
  `creativeWorkStatus`.

**Limits**
- No count limit; one export takes at most 15 minutes, and never longer than the token lives."#,
    params(ExportParams),
    responses(
        (status = 200, description = "RO-Crate ZIP stream", content_type = "application/zip"),
        (status = 400, description = "Neither or both of version and dataset given", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn export_provenance(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    Query(params): Query<ExportParams>,
) -> ServerResult<Response> {
    require_realm_auth(&state, auth)?;
    // Every hop checks this token again, so an export needs one.
    let bearer = bearer.ok_or(ServerError::Unauthorized)?;
    let start = match (params.version, params.dataset) {
        (Some(version), None) if is_version_id(&version) => Start::Version(version),
        (Some(_), None) => {
            return Err(ServerError::BadRequestReason(
                "not a version id".to_string(),
            ));
        }
        (None, Some(dataset)) => Start::Dataset(dataset),
        _ => {
            return Err(ServerError::BadRequestReason(
                "give either version or dataset".to_string(),
            ));
        }
    };
    let (now, unix_now) = (Instant::now(), aruna_core::time::unix_timestamp_secs());
    let finish_by = export_deadline(now, unix_now, bearer.expires_at_secs());
    let caller = Caller {
        state,
        deadline: walk_deadline(now, finish_by),
        finish_by,
        bearer,
        recheck: Arc::new(|state, bearer| {
            Box::pin(async move {
                bearer_auth(&state, bearer.as_str())
                    .await
                    .map(|(auth, _)| auth)
            })
        }),
    };
    start_export(caller, start)
}

/// The export ends at its time limit or when the token expires, whichever comes first.
fn export_deadline(now: Instant, unix_now: u64, expires_at_secs: u64) -> Instant {
    let left = Duration::from_secs(expires_at_secs.saturating_sub(unix_now));
    now + left.min(EXPORT_DEADLINE)
}

/// The walk ends early enough to leave a finishing window: a quarter of the time, at most
/// `FINISH_WINDOW`.
fn walk_deadline(now: Instant, finish_by: Instant) -> Instant {
    let left = finish_by.saturating_duration_since(now);
    finish_by - (left / 4).min(FINISH_WINDOW)
}

/// Starts the walk and answers with the ZIP stream it writes.
fn start_export(caller: Caller, start: Start) -> ServerResult<Response> {
    let published = chrono::Utc::now().to_rfc3339();
    let named = match &start {
        Start::Version(id) | Start::Dataset(id) => id,
    };
    // The export keeps its working state on disk; without room for it nothing starts.
    let built = ProvenanceCrate::new(named, &published)
        .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))?;
    let (writer, reader) = tokio::io::duplex(PIPE_BYTES);
    let gone = CancellationToken::new();
    let guard = gone.clone().drop_guard();
    tokio::spawn(async move { walk(caller, start, built, writer, gone).await });
    // Dropping the body, as a disconnect does, stops the walk.
    let body = ReaderStream::new(reader).map(move |chunk| {
        let _ = &guard;
        chunk
    });
    Response::builder()
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            "attachment; filename=\"provenance.zip\"",
        )
        .body(Body::from_stream(body))
        .map_err(|error| ServerError::InternalError(error.to_string()))
}

enum Start {
    Version(String),
    Dataset(String),
}

/// Checks the caller's token again and answers who it names now; `None` once it is expired or
/// revoked.
type Recheck = Arc<
    dyn Fn(Arc<ServerState>, ValidatedBearer) -> BoxFuture<'static, Option<AuthContext>>
        + Send
        + Sync,
>;

struct Caller {
    state: Arc<ServerState>,
    bearer: ValidatedBearer,
    recheck: Recheck,
    /// The walk ends here, leaving the finishing window for the root and the ZIP close.
    deadline: Instant,
    /// The hard end of the export.
    finish_by: Instant,
}

impl Caller {
    /// The caller as the token names it now, checked before each hop.
    async fn current(&self, gone: &CancellationToken) -> Option<AuthContext> {
        let check = (self.recheck)(self.state.clone(), self.bearer.clone());
        bounded(self, gone, check).await.flatten()
    }
}

/// What one hop read gave.
enum Read {
    Answer(serde_json::Value),
    Hidden,
    Silent,
}

async fn walk(
    caller: Caller,
    start: Start,
    mut built: ProvenanceCrate,
    writer: DuplexStream,
    gone: CancellationToken,
) {
    let mut zip = ZipFileWriter::with_tokio(writer);
    match &start {
        Start::Version(_) => {
            let first = built.version_start();
            built.push([first]);
        }
        Start::Dataset(document) => {
            let Some(auth) = caller.current(&gone).await else {
                built.stop(REVOKED);
                return finish(&caller, &gone, zip, built).await;
            };
            match bounded(&caller, &gone, crate_document(&caller, &auth, document)).await {
                Some(Read::Answer(dataset)) => {
                    let (hops, file) = built.dataset(&dataset);
                    if !write(&caller, &gone, &mut zip, file).await {
                        return;
                    }
                    built.push(hops);
                }
                Some(Read::Hidden) => built.stop("the dataset is not readable"),
                Some(Read::Silent) => built.stop("the dataset did not answer"),
                None => built.stop("stopped before the dataset was read"),
            }
        }
    }
    while let Some(hop) = built.pop() {
        if built.known(&hop) {
            continue;
        }
        let Some(auth) = caller.current(&gone).await else {
            built.stop(REVOKED);
            break;
        };
        let Some(read) = bounded(&caller, &gone, read_hop(&caller, &auth, &hop)).await else {
            built.stop("the export stopped at its time limit or on disconnect");
            break;
        };
        match (&hop.target, read) {
            (Target::Version(_), Read::Answer(origin)) => {
                let next = built.version(&hop, &origin);
                built.push(next);
            }
            (Target::Run(_), Read::Answer(run)) => {
                let read = match run["run_crate"].as_str() {
                    Some(document) => match crate_read(&caller, &gone, document).await {
                        Some(read) => read,
                        None => {
                            built.run(&hop, None);
                            built.stop(REVOKED);
                            break;
                        }
                    },
                    None => {
                        let status = &run["run_crate_status"];
                        if let Some(state) = status["status"].as_str()
                            && built.unwritten(&hop, state, status["message"].as_str())
                        {
                            continue;
                        }
                        Some(Read::Hidden)
                    }
                };
                let (hops, file, go_on) = record_run(&mut built, &hop, read);
                if let Some(file) = file
                    && !write(&caller, &gone, &mut zip, file).await
                {
                    return;
                }
                built.push(hops);
                if !go_on {
                    break;
                }
            }
            (Target::Run(_), Read::Hidden) => {
                built.run(&hop, None);
            }
            (Target::Release(_), Read::Answer(release)) => {
                if let Some(file) = built.release(&hop, Some(&release))
                    && !write(&caller, &gone, &mut zip, file).await
                {
                    return;
                }
            }
            (Target::Release(_), read) => {
                built.release(&hop, None);
                if matches!(read, Read::Silent) {
                    built.incomplete("a release crate did not answer");
                }
            }
            (_, Read::Hidden) => built.hidden(&hop),
            (_, Read::Silent) => built.unreachable(&hop),
        }
    }
    finish(&caller, &gone, zip, built).await
}

/// Whether `id` names one exact version, as `/data/origin` accepts it.
fn is_version_id(id: &str) -> bool {
    matches!(
        W3idIdentifier::parse(id),
        Ok(W3idIdentifier::VersionedObject(_) | W3idIdentifier::Global(GlobalId::Version { .. }))
    )
}

/// Writes the root metadata and closes the ZIP within the finishing window. The root goes to a
/// private temporary file first and is copied into the ZIP in pieces, never held whole.
async fn finish(
    caller: &Caller,
    gone: &CancellationToken,
    mut zip: ZipFileWriter<DuplexStream>,
    built: ProvenanceCrate,
) {
    let stop = Arc::new(AtomicBool::new(false));
    let halt = stop.clone();
    let worker = tokio::task::spawn_blocking(move || {
        let path = scratch_dir()?.join(format!("{}.json", scratch_name()));
        spool_root(built, path, &|| halt.load(Relaxed))
    });
    let spooled = match until(caller.finish_by, gone, worker).await {
        Some(Ok(Ok(spooled))) => spooled,
        // The worker owns its file and removes it once it sees the stop.
        _ => {
            stop.store(true, Relaxed);
            return;
        }
    };
    let copied = async {
        let mut file = tokio::fs::File::open(&spooled.0).await?;
        let entry = ZipEntryBuilder::new(
            "ro-crate-metadata.json".to_string().into(),
            Compression::Stored,
        )
        .unix_permissions(FILE_MODE);
        let mut entry = zip
            .write_entry_stream(entry)
            .await
            .map_err(std::io::Error::other)?;
        let mut chunk = vec![0u8; PIPE_BYTES];
        loop {
            let read = tokio::io::AsyncReadExt::read(&mut file, &mut chunk).await?;
            if read == 0 {
                break;
            }
            entry.write_all(&chunk[..read]).await?;
        }
        entry.close().await.map_err(std::io::Error::other)
    };
    if matches!(until(caller.finish_by, gone, copied).await, Some(Ok(()))) {
        let _ = until(caller.finish_by, gone, zip.close()).await;
    }
}

/// Writes the root to `path`, which only this user may read, and hands back the file. On any
/// failure or stop the file is removed here, by the worker that made it.
fn spool_root(
    built: ProvenanceCrate,
    path: std::path::PathBuf,
    stop: &dyn Fn() -> bool,
) -> std::io::Result<Spooled> {
    let spooled = Spooled(path);
    let mut out = std::io::BufWriter::new(scratch_file(&spooled.0)?);
    built.write_root(&mut out, stop)?;
    std::io::Write::flush(&mut out)?;
    Ok(spooled)
}

/// A temporary file removed when the export ends, however it ends.
struct Spooled(std::path::PathBuf);

impl Drop for Spooled {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Records a run with what its crate read gave. A crate that did not answer in time leaves the
/// export incomplete, never "not visible"; `false` ends the walk at the time limit.
fn record_run(
    built: &mut ProvenanceCrate,
    hop: &Hop,
    read: Option<Read>,
) -> (Vec<Hop>, Option<CrateFile>, bool) {
    match read {
        Some(Read::Answer(run_crate)) => {
            let (hops, file) = built.run(hop, Some(&run_crate));
            (hops, file, true)
        }
        Some(Read::Hidden) => {
            built.run(hop, None);
            (Vec::new(), None, true)
        }
        Some(Read::Silent) => {
            built.run(hop, None);
            built.incomplete("a run record did not answer");
            (Vec::new(), None, true)
        }
        None => {
            built.run(hop, None);
            built.stop("the export stopped at its time limit or on disconnect");
            (Vec::new(), None, false)
        }
    }
}

/// Runs `read` until the walk's deadline or a disconnect; `None` when either came first.
async fn bounded<T>(
    caller: &Caller,
    gone: &CancellationToken,
    read: impl Future<Output = T>,
) -> Option<T> {
    until(caller.deadline, gone, read).await
}

/// Runs `work` until `deadline` or a disconnect; `None` when either came first.
async fn until<T>(
    deadline: Instant,
    gone: &CancellationToken,
    work: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        _ = gone.cancelled() => None,
        answer = tokio::time::timeout_at(deadline, work) => answer.ok(),
    }
}

async fn read_hop(caller: &Caller, auth: &AuthContext, hop: &Hop) -> Read {
    let state = &caller.state;
    let bearer = Some(caller.bearer.clone());
    let headers = HeaderMap::new();
    let answer = match &hop.target {
        Target::Version(version) => {
            let ask = (version.as_str(), caller.deadline);
            version_answer(state, auth, bearer, &headers, ask)
                .await
                .and_then(|Json(origin)| to_value(&origin))
        }
        Target::Run(run_id) => {
            let ask = (run_id.as_str(), caller.deadline);
            run_answer(state, auth, bearer, &headers, ask)
                .await
                .and_then(|Json(run)| to_value(&run))
        }
        Target::Release(version) => release_crate(state, (auth, &caller.bearer), version).await,
    };
    classify(answer)
}

/// A release crate's exact version, read with the caller's identity on this node or, through
/// a holder request with the caller's token, on the node that holds it; it has to carry the
/// release tag still.
async fn release_crate(
    state: &Arc<ServerState>,
    (auth, bearer): (&AuthContext, &ValidatedBearer),
    version: &str,
) -> ServerResult<serde_json::Value> {
    let context = state.get_ctx();
    let unavailable = || ServerError::ServiceUnavailableReason("no holder is known".to_string());
    let local = state.get_node_id();
    let (holder, bucket, key, version_id) = match W3idIdentifier::parse(version) {
        Ok(W3idIdentifier::VersionedObject(arn)) if arn.realm_id == state.get_realm_id() => {
            (arn.node_id, arn.bucket, arn.key, arn.version)
        }
        Ok(W3idIdentifier::Global(GlobalId::Version {
            realm_id,
            version_id,
        })) if realm_id == state.get_realm_id() => {
            let deadline = Instant::now() + RESOLVE_BUDGET;
            match resolve_version(context.as_ref(), version_id, deadline).await {
                GlobalResolution::Found(found) => {
                    // Holders come coordinator first.
                    let holder = match found.local {
                        true => local,
                        false => *found.holders.first().ok_or_else(unavailable)?,
                    };
                    (holder, found.bucket, found.key, version_id)
                }
                GlobalResolution::NotFound => return Err(ServerError::NotFound),
                _ => return Err(unavailable()),
            }
        }
        _ => return Err(ServerError::NotFound),
    };
    if holder != local {
        return remote_crate(state, bearer, (holder, bucket, key, version_id)).await;
    }
    let info = bucket_info(state, &bucket).await?;
    let (realm_id, node_id) = (state.get_realm_id(), state.get_node_id());
    let path = object_permission_path(realm_id, info.group_id, node_id, &bucket, &key);
    ensure_permission(state, auth, path, Permission::READ).await?;
    let read = CrateRead {
        bucket,
        key,
        version_id,
        group_id: info.group_id,
        auth: auth.clone(),
        realm_id,
        node_id,
    };
    match read_crate(context.as_ref(), read).await {
        Ok(found) if found.metadata.contains_key(RELEASE_TAG) => {
            serde_json::from_slice(&found.bytes)
                .map_err(|error| ServerError::InternalError(error.to_string()))
        }
        Ok(_) | Err(CrateReadError::Missing) => Err(ServerError::NotFound),
        Err(CrateReadError::Failed(reason)) => Err(ServerError::ServiceUnavailableReason(reason)),
    }
}

/// Asks `holder` for a release crate chunk by chunk; it validates the token and checks READ
/// and the release tag on each ask.
async fn remote_crate(
    state: &Arc<ServerState>,
    bearer: &ValidatedBearer,
    (holder, bucket, key, version_id): (NodeId, String, String, Ulid),
) -> ServerResult<serde_json::Value> {
    let context = state.get_ctx();
    let handle = context.metadata_handle.as_ref().ok_or_else(holder_silent)?;
    let token = AuthToken::bearer(bearer.as_str().to_string()).map_err(|_| holder_silent())?;
    let mut bytes = Vec::new();
    loop {
        let ask = HolderAsk::Crate {
            key: key.clone(),
            version_id,
            offset: bytes.len() as u64,
        };
        let answer = handle
            .request_release_holder(holder, Some(token.clone()), bucket.clone(), ask)
            .await;
        let chunk = holder_chunk(answer)?;
        // A chunk that adds nothing before the end would never finish.
        if chunk.bytes.is_empty() && (bytes.len() as u64) < chunk.size {
            return Err(holder_silent());
        }
        bytes.extend(chunk.bytes);
        if bytes.len() as u64 >= chunk.size {
            break;
        }
    }
    serde_json::from_slice(&bytes).map_err(|error| ServerError::InternalError(error.to_string()))
}

fn holder_silent() -> ServerError {
    ServerError::ServiceUnavailableReason("the holder did not answer".to_string())
}

/// A holder's crate reply: a chunk, a gone or untagged version, a refusal, or no answer.
fn holder_chunk(answer: Result<HolderAnswer, MetadataReadError>) -> ServerResult<CrateChunk> {
    match answer {
        Ok(HolderAnswer::Crate(Some(chunk))) => Ok(chunk),
        Ok(HolderAnswer::Crate(None)) | Err(MetadataReadError::NotFound) => {
            Err(ServerError::NotFound)
        }
        Err(MetadataReadError::Forbidden | MetadataReadError::Unauthorized) => {
            Err(ServerError::Forbidden)
        }
        _ => Err(holder_silent()),
    }
}

/// A refusal hides a hop; any other failure means it did not answer.
fn classify(answer: ServerResult<serde_json::Value>) -> Read {
    match answer {
        Ok(answer) => Read::Answer(answer),
        Err(
            ServerError::Forbidden
            | ServerError::NotFound
            | ServerError::Unauthorized
            | ServerError::BadRequestReason(_),
        ) => Read::Hidden,
        Err(_) => Read::Silent,
    }
}

fn to_value(answer: &impl serde::Serialize) -> ServerResult<serde_json::Value> {
    serde_json::to_value(answer).map_err(|error| ServerError::InternalError(error.to_string()))
}

/// A run crate read under the token as it holds now, checked again after the run read;
/// `None` when the token expired or was revoked in between.
async fn crate_read(
    caller: &Caller,
    gone: &CancellationToken,
    document: &str,
) -> Option<Option<Read>> {
    let auth = caller.current(gone).await?;
    Some(bounded(caller, gone, crate_document(caller, &auth, document)).await)
}

/// A metadata document's crate as the caller may read it.
async fn crate_document(caller: &Caller, auth: &AuthContext, document_id: &str) -> Read {
    let answer = export_metadata_rocrate(
        State(caller.state.clone()),
        Extension(Some(auth.clone())),
        Extension(Some(caller.bearer.clone())),
        Path(document_id.to_string()),
        Query(RoCrateExportParams::default()),
    )
    .await;
    classify(answer.and_then(|(_, Json(answer))| match answer {
        MetadataRoCrateResponse::Projected(projected) => Ok(projected.rocrate),
        MetadataRoCrateResponse::Raw(_) => Err(ServerError::InternalError("raw view".to_string())),
    }))
}

/// Writes one file into the ZIP; `false` when the client is gone, stopped reading or the
/// export reached its time limit.
async fn write(
    caller: &Caller,
    gone: &CancellationToken,
    zip: &mut ZipFileWriter<DuplexStream>,
    file: CrateFile,
) -> bool {
    let entry =
        ZipEntryBuilder::new(file.path.into(), Compression::Stored).unix_permissions(FILE_MODE);
    // A started entry may use the finishing window, so the ZIP never ends inside an entry.
    let written = until(
        caller.finish_by,
        gone,
        zip.write_entry_whole(entry, &file.bytes),
    )
    .await;
    matches!(written, Some(Ok(_)))
}

#[cfg(test)]
#[path = "provenance_tests.rs"]
mod tests;
