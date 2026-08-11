use std::collections::HashSet;
use std::future::Future;
use std::time::{Duration, SystemTime};

use aruna_blob::blob::BlobHandle;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_PENDING_PROJECTION_KEYSPACE;
use aruna_core::structs::{
    Actor, AuthContext, HarvestCursor, HarvestGranularity, HarvestJobSpec, HarvestProvenance,
    HarvestRecordState, HarvestSource, IncomingRecord, JobError, JobResultPayload,
    MetadataRegistryRecord, ProvenanceDecision, RealmId, RepositoryConnector, provenance_decision,
};
use aruna_core::structured_id::StructuredId;
use aruna_core::types::GroupId;
use byteview::ByteView;
use ulid::Ulid;

use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_job_document,
};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::harvest::oai::mapping::dc_to_jsonld;
use crate::harvest::oai::parse::{
    OaiParseError, OaiRecord, parse_datestamp_ms, parse_granularity, parse_list_page,
};
use crate::harvest::oai::request::{format_window, identify_url, list_records_url};
use crate::harvest::repository::{
    StorageReadError, parse_connector_read, parse_provenance_read, parse_source_read,
    read_connector_effect, read_provenance_effect, read_source_effect, write_provenance_effect,
    write_source_effect,
};
use crate::harvest::target_path::{HARVEST_PATH_BYTES, normalize_target_prefix};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::jobs::metadata_class::{MetadataFailure, classify_metadata};
use crate::metadata::MetadataAuthToken;
use crate::metadata::forward::{
    MetadataWriteError, create_metadata_document_routed, delete_metadata_document_routed,
    update_metadata_document_routed,
};
use crate::update_metadata_document::UpdateMetadataDocumentMutation;

/// Bound on resumption-token paging so a broken provider cannot loop forever.
/// Operationally generous at a typical page size, and small enough that a
/// pathological provider cannot hold a worker for the life of the node.
const MAX_HARVEST_PAGES: u32 = 10_000;
/// Total wall clock one harvest run may spend paging.
const HARVEST_RUN_DEADLINE: Duration = Duration::from_secs(1800);
/// Hard cap on one decoded OAI-PMH response body.
const HARVEST_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Total wall clock one fetch may take, connect through last byte.
const HARVEST_FETCH_DEADLINE: Duration = Duration::from_secs(120);

#[derive(Default)]
struct HarvestCounts {
    minted: u64,
    updated: u64,
    tombstoned: u64,
    skipped: u64,
}

#[derive(Debug)]
enum HarvestFailure {
    Cancelled,
    Interrupted,
    Job(JobError),
}

/// Run one harvest of a repository source: page through OAI-PMH ListRecords,
/// apply each record through the metadata write seam with the source's group
/// authority, and advance the cursor. Idempotent by harvest provenance, so a
/// fenced re-run re-applies nothing.
pub async fn run_harvest_job(ctx: &JobContext, spec: &HarvestJobSpec) -> JobRunOutcome {
    match harvest(ctx, spec).await {
        Ok(counts) => JobRunOutcome::Succeeded(JobResultPayload::Harvest {
            minted: counts.minted,
            updated: counts.updated,
            tombstoned: counts.tombstoned,
            skipped: counts.skipped,
        }),
        Err(HarvestFailure::Cancelled) => JobRunOutcome::Cancelled,
        Err(HarvestFailure::Interrupted) => JobRunOutcome::Interrupted,
        Err(HarvestFailure::Job(error)) => JobRunOutcome::Failed(error),
    }
}

async fn harvest(ctx: &JobContext, spec: &HarvestJobSpec) -> Result<HarvestCounts, HarvestFailure> {
    let mut source = read_source(ctx, spec.group_id, spec.source_id)
        .await?
        .ok_or_else(|| permanent("harvest source not found"))?;
    let connector = read_connector(ctx, source.group_id, source.connector_id)
        .await?
        .ok_or_else(|| permanent("repository connector not found"))?;

    let net = ctx
        .driver
        .net_handle
        .as_ref()
        .ok_or_else(|| permanent("net plane unavailable for harvest"))?;
    let realm_id = *net.realm_id();
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| permanent("blob handle unavailable for harvest"))?;
    let actor = Actor {
        node_id: ctx.owner_node_id,
        user_id: source.created_by,
        realm_id,
    };

    let original_last = source
        .cursor
        .as_ref()
        .map(|cursor| cursor.last_datestamp_ms)
        .unwrap_or(0);
    if source.granularity.is_none() {
        source.granularity = discover_granularity(ctx, blob, &connector.endpoint).await;
        if source.granularity.is_some() {
            persist_source(ctx, &source).await?;
        }
    }
    let from = format_window(original_last, source.granularity.unwrap_or_default());
    let mut resumption_token = source
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.resumption_token.clone());
    let mut overall_max = original_last;
    let mut counts = HarvestCounts::default();

    // One restart per run at most: a provider that keeps rejecting its own
    // tokens must not be able to loop the listing back to the start forever.
    let mut restarted = false;
    let mut seen_tokens: HashSet<[u8; 32]> = HashSet::new();
    let started = tokio::time::Instant::now();

    for page_index in 0..MAX_HARVEST_PAGES {
        check_signals(ctx)?;
        if page_index + 1 == MAX_HARVEST_PAGES || started.elapsed() > HARVEST_RUN_DEADLINE {
            // Budget exhaustion is not a defect in the source: keep the position
            // so the next run continues where this one stopped.
            persist_cursor(ctx, &source, original_last, resumption_token).await?;
            return Err(retryable("harvest exhausted its paging or time budget"));
        }
        let resumed = resumption_token.clone();
        let url = list_records_url(
            &connector.endpoint,
            &source.selector,
            from.as_deref(),
            resumed.as_deref(),
        )
        .map_err(|error| permanent(format!("invalid OAI endpoint URL: {error}")))?;

        let body = fetch(ctx, blob, url).await?;
        let page = match parse_list_page(&body) {
            Ok(page) => page,
            Err(error) if resumed.is_some() => {
                persist_cursor(ctx, &source, original_last, None).await?;
                // The provider disowned its own token. Restart the same window
                // once in this run; a second rejection is the provider's fault.
                if !restarted && is_bad_token(&error) {
                    restarted = true;
                    resumption_token = None;
                    // A restart is a fresh listing: the provider may legitimately
                    // hand back the same token names again.
                    seen_tokens.clear();
                    continue;
                }
                return Err(retryable(format!(
                    "OAI resumption token rejected, listing restarts next run: {error}"
                )));
            }
            Err(error) => return Err(classify_parse(error)),
        };

        for record in &page.records {
            check_signals(ctx)?;
            let datestamp_ms = parse_datestamp_ms(&record.header.datestamp).unwrap_or(0);
            overall_max = overall_max.max(datestamp_ms);
            apply_record(
                ctx,
                &source,
                &actor,
                realm_id,
                record,
                datestamp_ms,
                &mut counts,
            )
            .await?;
        }
        ctx.progress.advance(page.records.len() as u64);

        resumption_token = page.resumption_token.clone();
        let Some(token) = resumption_token.clone() else {
            // Complete: advance the window so the next harvest is incremental.
            persist_cursor(ctx, &source, overall_max, None).await?;
            return Ok(counts);
        };
        if !seen_tokens.insert(*blake3::hash(token.as_bytes()).as_bytes()) {
            persist_cursor(ctx, &source, original_last, None).await?;
            return Err(retryable("OAI provider returned a resumption token twice"));
        }
        // Mid-list: keep the window fixed and store the token so a re-run resumes.
        persist_cursor(ctx, &source, original_last, Some(token)).await?;
    }

    Err(retryable("harvest exhausted its paging budget"))
}

fn is_bad_token(error: &OaiParseError) -> bool {
    matches!(error, OaiParseError::Protocol { code, .. } if code == "badResumptionToken")
}

/// A malformed body is upstream junk that a retry can clear; an OAI protocol
/// rejection of a well-formed request is a configuration fault that will not.
fn classify_parse(error: OaiParseError) -> HarvestFailure {
    match error {
        OaiParseError::Xml(_) => retryable(format!("OAI response: {error}")),
        OaiParseError::Protocol { .. } => permanent(format!("OAI response: {error}")),
    }
}

async fn apply_record(
    ctx: &JobContext,
    source: &HarvestSource,
    actor: &Actor,
    realm_id: RealmId,
    record: &OaiRecord,
    datestamp_ms: u64,
    counts: &mut HarvestCounts,
) -> Result<(), HarvestFailure> {
    let identifier = &record.header.identifier;
    if identifier.is_empty() {
        counts.skipped += 1;
        return Ok(());
    }
    let existing = read_provenance(ctx, source.group_id, &source.namespace, identifier).await?;
    let incoming = IncomingRecord {
        datestamp_ms,
        deleted: record.header.deleted,
    };

    let mut row = HarvestProvenance {
        group_id: source.group_id,
        namespace: source.namespace.clone(),
        source_record_id: identifier.to_string(),
        meta_resource_id: Ulid::nil(),
        version: next_version(existing.as_ref()),
        source_datestamp_ms: datestamp_ms,
        state: HarvestRecordState::PendingCreate,
        predecessors: existing
            .as_ref()
            .map(|provenance| provenance.predecessors.clone())
            .unwrap_or_default(),
    };

    match provenance_decision(existing.as_ref(), &incoming) {
        ProvenanceDecision::Skip => counts.skipped += 1,
        ProvenanceDecision::Mint => {
            mint_and_create(ctx, source, actor, realm_id, record, &mut row).await?;
            counts.minted += 1;
        }
        ProvenanceDecision::Revive { predecessor } => {
            row.predecessors.push(predecessor);
            mint_and_create(ctx, source, actor, realm_id, record, &mut row).await?;
            counts.minted += 1;
        }
        ProvenanceDecision::ResumeCreate { meta_resource_id } => {
            row.meta_resource_id = meta_resource_id;
            row.source_datestamp_ms = datestamp_ms.max(
                existing
                    .as_ref()
                    .map(|provenance| provenance.source_datestamp_ms)
                    .unwrap_or(0),
            );
            create_document(ctx, source, actor, realm_id, meta_resource_id, record).await?;
            row.state = HarvestRecordState::Live;
            write_provenance(ctx, &row).await?;
            counts.minted += 1;
        }
        ProvenanceDecision::Update { meta_resource_id } => {
            match read_stored(ctx, meta_resource_id).await? {
                Some(stored) => {
                    update_document(
                        ctx,
                        source,
                        actor,
                        realm_id,
                        meta_resource_id,
                        Some(&stored),
                        record,
                    )
                    .await?;
                    row.meta_resource_id = meta_resource_id;
                    row.state = HarvestRecordState::Live;
                    write_provenance(ctx, &row).await?;
                    counts.updated += 1;
                }
                // Deleted out of band while the provenance stayed live: an
                // update can never land on a document that is gone, so the
                // identity is retired and a successor allocated, exactly as a
                // revival does.
                None => {
                    confirm_absent(ctx, meta_resource_id).await?;
                    row.predecessors.push(meta_resource_id);
                    mint_and_create(ctx, source, actor, realm_id, record, &mut row).await?;
                    counts.minted += 1;
                }
            }
        }
        ProvenanceDecision::Tombstone { meta_resource_id } => {
            confirm_withdrawn(ctx, source, actor, realm_id, meta_resource_id).await?;
            row.meta_resource_id = meta_resource_id;
            row.state = HarvestRecordState::Tombstoned;
            write_provenance(ctx, &row).await?;
            counts.tombstoned += 1;
        }
    }
    Ok(())
}

/// Prove the harvested document is gone before its provenance may go terminal.
///
/// An empty local registry read is not evidence of absence: the row is a
/// projection of a create that may still be queued here, and a `Tombstoned` row
/// written over that race would leave the document live forever. Only a routed
/// delete that succeeds, or one every holder answers as already absent, retires
/// the identity; anything else is retryable and keeps the prior state.
async fn confirm_withdrawn(
    ctx: &JobContext,
    source: &HarvestSource,
    actor: &Actor,
    realm_id: RealmId,
    document_id: Ulid,
) -> Result<(), HarvestFailure> {
    let stored = read_stored(ctx, document_id).await?;
    if stored.is_none() {
        confirm_absent(ctx, document_id).await?;
    }
    match delete_metadata_document_routed(
        &ctx.driver,
        actor.clone(),
        stored.as_ref(),
        document_id,
        Some(internal_token(source.created_by, realm_id)),
    )
    .await
    {
        // Every holder reporting the document absent is the same evidence as a
        // delete this run performed.
        Ok(()) | Err(MetadataWriteError::NotFound) => Ok(()),
        Err(error) => Err(apply_failure(error)),
    }
}

async fn read_stored(
    ctx: &JobContext,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, HarvestFailure> {
    load_metadata_record_by_document(&ctx.driver, document_id)
        .await
        .map_err(|error| retryable(format!("harvest record read: {error:?}")))
}

/// An empty registry read is only evidence of absence once nothing committed
/// for the document is still queued for projection here.
async fn confirm_absent(ctx: &JobContext, document_id: Ulid) -> Result<(), HarvestFailure> {
    if projection_pending(ctx, document_id).await? {
        return Err(retryable(format!(
            "harvest waits for the pending registry projection of {document_id}"
        )));
    }
    Ok(())
}

/// Whether a committed metadata event for this document is still waiting to be
/// projected into the local registry.
async fn projection_pending(ctx: &JobContext, document_id: Ulid) -> Result<bool, HarvestFailure> {
    let event = ctx
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: METADATA_PENDING_PROJECTION_KEYSPACE.to_string(),
            prefix: Some(ByteView::from(document_id.to_bytes().to_vec())),
            start: None,
            limit: 1,
            txn_id: None,
        })
        .await;
    match event {
        Event::Storage(StorageEvent::IterResult { values, .. }) => Ok(!values.is_empty()),
        Event::Storage(StorageEvent::Error { error }) => Err(retryable(format!(
            "harvest pending projection scan: {error}"
        ))),
        other => Err(retryable(format!(
            "harvest pending projection scan: unexpected {other:?}"
        ))),
    }
}

/// Allocate a structured document id, record it as `PendingCreate` before the
/// create runs, then confirm it. A crash anywhere in between leaves a retry the
/// same id, so a replay converges on one document instead of orphaning one per
/// attempt.
async fn mint_and_create(
    ctx: &JobContext,
    source: &HarvestSource,
    actor: &Actor,
    realm_id: RealmId,
    record: &OaiRecord,
    row: &mut HarvestProvenance,
) -> Result<(), HarvestFailure> {
    let document_path = harvest_document_path(&source.target_prefix, &record.header.identifier)?;
    let document_id = mint_job_document(&ctx.driver, actor, source.group_id, &document_path)
        .await
        .map_err(|error| retryable(format!("harvest mint document id: {error}")))?
        .as_ulid();
    row.meta_resource_id = document_id;
    row.state = HarvestRecordState::PendingCreate;
    write_provenance(ctx, row).await?;
    create_document(ctx, source, actor, realm_id, document_id, record).await?;
    row.state = HarvestRecordState::Live;
    write_provenance(ctx, row).await
}

async fn create_document(
    ctx: &JobContext,
    source: &HarvestSource,
    actor: &Actor,
    realm_id: RealmId,
    document_id: Ulid,
    record: &OaiRecord,
) -> Result<(), HarvestFailure> {
    let document_path = harvest_document_path(&source.target_prefix, &record.header.identifier)?;
    let created = create_metadata_document_routed(
        CreateMetadataDocumentOperation::new_for_generated_document_id(
            CreateMetadataDocumentConfig {
                actor: actor.clone(),
                group_id: source.group_id,
                document_id,
                document_path: document_path.clone(),
                public: false,
                payload: CreateMetadataDocumentPayload::RoCrate {
                    jsonld: dc_to_jsonld(record),
                },
            },
        ),
        ctx.driver.clone(),
        Some(internal_token(source.created_by, realm_id)),
    )
    .await;
    match created {
        Ok(_) => Ok(()),
        // A create the pending identity already committed under a different
        // payload: the id is resolved, and the newer content lands as an update.
        Err(MetadataWriteError::Create(CreateMetadataDocumentError::DocumentAlreadyExists)) => {
            let stored = read_stored(ctx, document_id).await?;
            update_document(
                ctx,
                source,
                actor,
                realm_id,
                document_id,
                stored.as_ref(),
                record,
            )
            .await
        }
        Err(error) => Err(apply_failure(error)),
    }
}

async fn update_document(
    ctx: &JobContext,
    source: &HarvestSource,
    actor: &Actor,
    realm_id: RealmId,
    document_id: Ulid,
    stored: Option<&MetadataRegistryRecord>,
    record: &OaiRecord,
) -> Result<(), HarvestFailure> {
    update_metadata_document_routed(
        &ctx.driver,
        actor.clone(),
        stored,
        document_id,
        None,
        UpdateMetadataDocumentMutation::ReplaceRoCrate {
            jsonld: dc_to_jsonld(record),
        },
        Some(internal_token(source.created_by, realm_id)),
    )
    .await
    .map_err(apply_failure)?;
    Ok(())
}

/// Harvest writes run as the source owner, unrestricted: the source record is
/// the authorization decision, made when an operator created it.
fn internal_token(created_by: aruna_core::types::UserId, realm_id: RealmId) -> MetadataAuthToken {
    MetadataAuthToken::internal(AuthContext {
        user_id: created_by,
        realm_id,
        path_restrictions: None,
    })
}

fn next_version(existing: Option<&HarvestProvenance>) -> u64 {
    existing
        .map(|provenance| provenance.version + 1)
        .unwrap_or(1)
}

/// Land a source record under its target prefix at a stable, path-safe segment
/// derived from the OAI identifier.
///
/// Every identifier is encoded into one of two disjoint domains so no two raw
/// identifiers can ever share a segment: `b64-` carries the exact identifier as
/// URL-safe unpadded base64 whenever it fits the path budget, `b3-` carries the
/// full 256-bit BLAKE3 digest of anything longer. Provenance keeps the raw
/// identifier, so the encoding never has to be reversed.
fn harvest_document_path(prefix: &str, identifier: &str) -> Result<String, HarvestFailure> {
    let Some(prefix) = normalize_target_prefix(prefix) else {
        return Err(permanent(format!(
            "harvest target prefix leaves no room for an encoded identifier: {prefix}"
        )));
    };
    let budget = HARVEST_PATH_BYTES - prefix.len() - 1;
    let encoded = format!(
        "b64-{}",
        base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            identifier
        )
    );
    let segment = if encoded.len() <= budget {
        encoded
    } else {
        format!("b3-{}", blake3::hash(identifier.as_bytes()).to_hex())
    };
    Ok(format!("{prefix}/{segment}"))
}

async fn write_provenance(
    ctx: &JobContext,
    provenance: &HarvestProvenance,
) -> Result<(), HarvestFailure> {
    let effect = write_provenance_effect(provenance, None)
        .map_err(|error| permanent(format!("provenance encode: {error}")))?;
    expect_write(ctx.driver.storage_handle.send_effect(effect).await)
}

async fn persist_cursor(
    ctx: &JobContext,
    source: &HarvestSource,
    last_datestamp_ms: u64,
    resumption_token: Option<String>,
) -> Result<(), HarvestFailure> {
    let mut updated = source.clone();
    updated.cursor = Some(HarvestCursor {
        last_datestamp_ms,
        resumption_token,
    });
    persist_source(ctx, &updated).await
}

async fn persist_source(ctx: &JobContext, source: &HarvestSource) -> Result<(), HarvestFailure> {
    let mut updated = source.clone();
    updated.updated_at = SystemTime::now();
    let effect = write_source_effect(&updated, None)
        .map_err(|error| permanent(format!("harvest source encode: {error}")))?;
    expect_write(ctx.driver.storage_handle.send_effect(effect).await)
}

/// Ask the provider once which datestamp precision it accepts. `Identify` is a
/// hint, not a gate: an unreachable or unparseable answer leaves this run on the
/// baseline and is rediscovered next run.
async fn discover_granularity(
    ctx: &JobContext,
    blob: &BlobHandle,
    endpoint: &str,
) -> Option<HarvestGranularity> {
    let url = identify_url(endpoint).ok()?;
    let body = fetch(ctx, blob, url).await.ok()?;
    parse_granularity(&body)
}

/// Fetch one OAI-PMH response under a hard byte cap and a total wall-clock
/// deadline.
///
/// The egress client only bounds connect and read *inactivity*, so a slow-drip
/// or endless response would otherwise run until the node stops. Chunks are
/// counted after decoding, which is what a compressed body expands to.
async fn fetch(
    ctx: &JobContext,
    blob: &BlobHandle,
    url: url::Url,
) -> Result<String, HarvestFailure> {
    let deadline = tokio::time::Instant::now() + HARVEST_FETCH_DEADLINE;
    let request = blob
        .egress_request(url)
        .map_err(|error| permanent(format!("egress screen: {error}")))?;
    let mut response = bounded(ctx, deadline, request.send())
        .await?
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
        && declared > HARVEST_BODY_BYTES as u64
    {
        return Err(permanent(format!(
            "OAI response declares {declared} bytes, over the {HARVEST_BODY_BYTES} byte cap"
        )));
    }

    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = bounded(ctx, deadline, response.chunk())
        .await?
        .map_err(|error| retryable(format!("OAI body: {error}")))?
    {
        if body.len() + chunk.len() > HARVEST_BODY_BYTES {
            return Err(permanent(format!(
                "OAI response body exceeds the {HARVEST_BODY_BYTES} byte cap"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|error| permanent(format!("OAI body is not UTF-8: {error}")))
}

/// Await one network step under the fetch deadline while staying responsive to
/// cancel and shutdown, so the job drain budget holds against a slow provider.
async fn bounded<T>(
    ctx: &JobContext,
    deadline: tokio::time::Instant,
    future: impl Future<Output = T>,
) -> Result<T, HarvestFailure> {
    tokio::select! {
        result = future => Ok(result),
        () = ctx.cancel.cancelled() => Err(HarvestFailure::Cancelled),
        () = ctx.shutdown.cancelled() => Err(HarvestFailure::Interrupted),
        () = tokio::time::sleep_until(deadline) => {
            Err(retryable("OAI fetch exceeded the total deadline"))
        }
    }
}

async fn read_source(
    ctx: &JobContext,
    group_id: GroupId,
    source_id: Ulid,
) -> Result<Option<HarvestSource>, HarvestFailure> {
    let event = ctx
        .driver
        .storage_handle
        .send_effect(read_source_effect(group_id, source_id, None))
        .await;
    parse_source_read(event).map_err(read_failure)
}

async fn read_connector(
    ctx: &JobContext,
    group_id: GroupId,
    connector_id: Ulid,
) -> Result<Option<RepositoryConnector>, HarvestFailure> {
    let event = ctx
        .driver
        .storage_handle
        .send_effect(read_connector_effect(group_id, connector_id, None))
        .await;
    parse_connector_read(event).map_err(read_failure)
}

async fn read_provenance(
    ctx: &JobContext,
    group_id: GroupId,
    namespace: &str,
    identifier: &str,
) -> Result<Option<HarvestProvenance>, HarvestFailure> {
    let event = ctx
        .driver
        .storage_handle
        .send_effect(read_provenance_effect(
            group_id, namespace, identifier, None,
        ))
        .await;
    parse_provenance_read(event).map_err(read_failure)
}

fn check_signals(ctx: &JobContext) -> Result<(), HarvestFailure> {
    if ctx.cancel.is_cancelled() {
        return Err(HarvestFailure::Cancelled);
    }
    if ctx.shutdown.is_cancelled() {
        return Err(HarvestFailure::Interrupted);
    }
    Ok(())
}

fn expect_write(event: Event) -> Result<(), HarvestFailure> {
    match event {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        Event::Storage(StorageEvent::Error { error }) => {
            Err(retryable(format!("harvest write: {error}")))
        }
        _ => Err(retryable("harvest write: unexpected event")),
    }
}

fn permanent(message: impl Into<String>) -> HarvestFailure {
    HarvestFailure::Job(JobError::permanent(message))
}

fn retryable(message: impl Into<String>) -> HarvestFailure {
    HarvestFailure::Job(JobError::retryable(message))
}

fn read_failure(error: StorageReadError) -> HarvestFailure {
    retryable(format!("harvest storage read: {error}"))
}

fn apply_failure(error: MetadataWriteError) -> HarvestFailure {
    match classify_metadata(error) {
        MetadataFailure::Retryable(message) => retryable(format!("harvest apply: {message}")),
        MetadataFailure::Permanent(message) => {
            permanent(format!("harvest apply failed: {message}"))
        }
        MetadataFailure::Validation(violations) => permanent(format!(
            "harvest record rejected by validation: {violations:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harvest::target_path::DIGEST_SEGMENT_BYTES;

    fn path(identifier: &str) -> String {
        harvest_document_path("/imported/zenodo/", identifier).unwrap()
    }

    #[test]
    fn path_encodes_identifier() {
        let encoded = path("oai:example.org:123/v2");
        assert!(encoded.starts_with("imported/zenodo/b64-"));
        assert!(!encoded["imported/zenodo/".len()..].contains('/'));
    }

    #[test]
    fn separators_never_collide() {
        // ':' and '/' both collapsed to '-' under the old sanitizer.
        let paths = [
            path("oai:a:b"),
            path("oai/a/b"),
            path("oai-a-b"),
            path("oai:a/b"),
        ];
        for (index, left) in paths.iter().enumerate() {
            for right in &paths[index + 1..] {
                assert_ne!(left, right);
            }
        }
    }

    // a clean digest-shaped identifier stays distinct
    #[test]
    fn digest_shape_distinct() {
        // A raw identifier that looks like digest output lands in the b64 domain.
        let digest = blake3::hash(b"oai:example.org:1").to_hex().to_string();
        assert_ne!(path(&format!("b3-{digest}")), path("oai:example.org:1"));
        assert!(path(&format!("b3-{digest}")).contains("/b64-"));
    }

    // a long identifier falls back to a digest
    #[test]
    fn long_identifier_digests() {
        let long = "oai:example.org:".to_string() + &"x".repeat(600);
        let encoded = path(&long);
        assert!(encoded.starts_with("imported/zenodo/b3-"));
        assert_eq!(encoded, path(&long));
        assert!(encoded.len() <= HARVEST_PATH_BYTES);
    }

    // a unicode identifier round-trips distinctly
    #[test]
    fn unicode_stays_distinct() {
        assert_ne!(path("oai:例:1"), path("oai:例:2"));
        assert!(path("oai:例:1").contains("/b64-"));
    }

    #[test]
    fn oversized_prefix_rejected() {
        assert!(harvest_document_path(&"p".repeat(HARVEST_PATH_BYTES), "x").is_err());
    }

    /// Every prefix source creation accepts must yield a bounded path, and
    /// padding must never move a record to a different one.
    #[test]
    fn accepted_prefixes_yield() {
        let longest = "p".repeat(HARVEST_PATH_BYTES - DIGEST_SEGMENT_BYTES - 1);
        for prefix in ["imported/zenodo", " /imported/zenodo/ ", longest.as_str()] {
            assert!(normalize_target_prefix(prefix).is_some());
            let encoded = harvest_document_path(prefix, "oai:example.org:1").unwrap();
            assert!(encoded.len() <= HARVEST_PATH_BYTES);
        }
        assert_eq!(
            path("oai:example.org:1"),
            harvest_document_path(" /imported/zenodo/ ", "oai:example.org:1").unwrap()
        );
    }

    // the next version increments, or starts at one
    #[test]
    fn next_version_increments() {
        assert_eq!(next_version(None), 1);
        let provenance = HarvestProvenance {
            group_id: Ulid::nil(),
            namespace: "ns".to_string(),
            source_record_id: "r".to_string(),
            meta_resource_id: Ulid::nil(),
            version: 4,
            source_datestamp_ms: 0,
            state: HarvestRecordState::Live,
            predecessors: Vec::new(),
        };
        assert_eq!(next_version(Some(&provenance)), 5);
    }
}
