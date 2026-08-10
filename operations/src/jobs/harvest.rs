use std::time::SystemTime;

use aruna_blob::blob::BlobHandle;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::structured_id::StructuredId;
use aruna_core::structs::{
    Actor, AuthContext, HarvestCursor, HarvestJobSpec, HarvestProvenance, HarvestRecordState,
    HarvestSource, IncomingRecord, JobError, JobResultPayload, ProvenanceDecision, RealmId,
    RepositoryConnector, provenance_decision,
};
use aruna_core::types::GroupId;
use ulid::Ulid;

use crate::create_metadata_document::{
    CreateMetadataDocumentConfig, CreateMetadataDocumentError, CreateMetadataDocumentOperation,
    CreateMetadataDocumentPayload, mint_job_document,
};
use crate::get_metadata_document::load_metadata_record_by_document;
use crate::harvest::oai::mapping::oai_dc_to_jsonld;
use crate::harvest::oai::parse::{OaiRecord, parse_datestamp_ms, parse_list_page};
use crate::harvest::oai::request::{format_from, list_records_url};
use crate::harvest::repository::{
    StorageReadError, parse_connector_read, parse_provenance_read, parse_source_read,
    read_connector_effect, read_provenance_effect, read_source_effect, write_provenance_effect,
    write_source_effect,
};
use crate::jobs::executor::{JobContext, JobRunOutcome};
use crate::metadata::MetadataAuthToken;
use crate::metadata::forward::{
    MetadataWriteError, create_metadata_document_routed, delete_metadata_document_routed,
    update_metadata_document_routed,
};
use crate::update_metadata_document::UpdateMetadataDocumentMutation;

/// Bound on resumption-token paging so a broken provider cannot loop forever.
const MAX_HARVEST_PAGES: u32 = 100_000;
/// Budget for a harvested document's normalized metadata path.
const HARVEST_PATH_BYTES: usize = 512;
/// `b3-` plus 64 hex characters: the shortest segment any identifier can take.
const DIGEST_SEGMENT_BYTES: usize = 67;

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
    let source = read_source(ctx, spec.group_id, spec.source_id)
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
    let from = format_from(original_last);
    let mut resumption_token = source
        .cursor
        .as_ref()
        .and_then(|cursor| cursor.resumption_token.clone());
    let mut overall_max = original_last;
    let mut counts = HarvestCounts::default();

    for _ in 0..MAX_HARVEST_PAGES {
        check_signals(ctx)?;
        let resumed = resumption_token.clone();
        let url = list_records_url(
            &connector.endpoint,
            &source.selector,
            from.as_deref(),
            resumed.as_deref(),
        )
        .map_err(|error| permanent(format!("invalid OAI endpoint URL: {error}")))?;

        let body = fetch(blob, url).await?;
        let page = match parse_list_page(&body) {
            Ok(page) => page,
            // A token the provider no longer honours would be replayed by every
            // later run, so drop it and let the next run restart the window.
            Err(error) if resumed.is_some() => {
                persist_cursor(ctx, &source, original_last, None).await?;
                return Err(retryable(format!(
                    "OAI resumption token rejected, listing restarts next run: {error}"
                )));
            }
            Err(error) => return Err(permanent(format!("OAI response: {error}"))),
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
        if resumption_token.is_none() {
            // Complete: advance the window so the next harvest is incremental.
            persist_cursor(ctx, &source, overall_max, None).await?;
            return Ok(counts);
        }
        // Mid-list: keep the window fixed and store the token so a re-run resumes.
        persist_cursor(ctx, &source, original_last, resumption_token.clone()).await?;
    }

    Err(permanent("harvest exceeded resumption page bound"))
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
            update_document(ctx, source, actor, realm_id, meta_resource_id, record).await?;
            row.meta_resource_id = meta_resource_id;
            row.state = HarvestRecordState::Live;
            write_provenance(ctx, &row).await?;
            counts.updated += 1;
        }
        ProvenanceDecision::Tombstone { meta_resource_id } => {
            if let Some(stored) = load_metadata_record_by_document(&ctx.driver, meta_resource_id)
                .await
                .map_err(|error| retryable(format!("harvest record read: {error:?}")))?
            {
                delete_metadata_document_routed(
                    &ctx.driver,
                    actor.clone(),
                    Some(&stored),
                    meta_resource_id,
                    Some(internal_token(source.created_by, realm_id)),
                )
                .await
                .map_err(apply_failure)?;
            }
            row.meta_resource_id = meta_resource_id;
            row.state = HarvestRecordState::Tombstoned;
            write_provenance(ctx, &row).await?;
            counts.tombstoned += 1;
        }
    }
    Ok(())
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
                    jsonld: oai_dc_to_jsonld(record),
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
            update_document(ctx, source, actor, realm_id, document_id, record).await
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
    record: &OaiRecord,
) -> Result<(), HarvestFailure> {
    let stored = load_metadata_record_by_document(&ctx.driver, document_id)
        .await
        .map_err(|error| retryable(format!("harvest record read: {error:?}")))?;
    update_metadata_document_routed(
        &ctx.driver,
        actor.clone(),
        stored.as_ref(),
        document_id,
        None,
        UpdateMetadataDocumentMutation::ReplaceRoCrate {
            jsonld: oai_dc_to_jsonld(record),
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
    let prefix = prefix.trim_matches('/');
    let budget = HARVEST_PATH_BYTES.saturating_sub(prefix.len() + 1);
    if budget < DIGEST_SEGMENT_BYTES {
        return Err(permanent(format!(
            "harvest target prefix leaves no room for an encoded identifier: {prefix}"
        )));
    }
    let encoded = format!(
        "b64-{}",
        base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, identifier)
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
    updated.updated_at = SystemTime::now();
    let effect = write_source_effect(&updated, None)
        .map_err(|error| permanent(format!("harvest source encode: {error}")))?;
    expect_write(ctx.driver.storage_handle.send_effect(effect).await)
}

async fn fetch(blob: &BlobHandle, url: url::Url) -> Result<String, HarvestFailure> {
    let request = blob
        .egress_request(url)
        .map_err(|error| permanent(format!("egress screen: {error}")))?;
    let response = request
        .send()
        .await
        .map_err(|error| retryable(format!("OAI fetch: {error}")))?;
    if !response.status().is_success() {
        return Err(retryable(format!("OAI HTTP status {}", response.status())));
    }
    response
        .text()
        .await
        .map_err(|error| retryable(format!("OAI body: {error}")))
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
        .send_effect(read_provenance_effect(group_id, namespace, identifier, None))
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
    match error {
        MetadataWriteError::Undeliverable(message) => {
            retryable(format!("harvest apply undeliverable: {message}"))
        }
        other => permanent(format!("harvest apply failed: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(identifier: &str) -> String {
        harvest_document_path("/imported/zenodo/", identifier).unwrap()
    }

    #[test]
    fn document_path_encodes_identifier() {
        let encoded = path("oai:example.org:123/v2");
        assert!(encoded.starts_with("imported/zenodo/b64-"));
        assert!(!encoded["imported/zenodo/".len()..].contains('/'));
    }

    #[test]
    fn separator_variants_never_collide() {
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

    #[test]
    fn clean_digest_shaped_identifier_stays_distinct() {
        // A raw identifier that looks like digest output lands in the b64 domain.
        let digest = blake3::hash(b"oai:example.org:1").to_hex().to_string();
        assert_ne!(path(&format!("b3-{digest}")), path("oai:example.org:1"));
        assert!(path(&format!("b3-{digest}")).contains("/b64-"));
    }

    #[test]
    fn long_identifier_falls_back_to_digest() {
        let long = "oai:example.org:".to_string() + &"x".repeat(600);
        let encoded = path(&long);
        assert!(encoded.starts_with("imported/zenodo/b3-"));
        assert_eq!(encoded, path(&long));
        assert!(encoded.len() <= HARVEST_PATH_BYTES);
    }

    #[test]
    fn unicode_identifier_round_trips_distinctly() {
        assert_ne!(path("oai:例:1"), path("oai:例:2"));
        assert!(path("oai:例:1").contains("/b64-"));
    }

    #[test]
    fn oversized_prefix_is_rejected() {
        assert!(harvest_document_path(&"p".repeat(HARVEST_PATH_BYTES), "x").is_err());
    }

    #[test]
    fn next_version_increments_or_starts_at_one() {
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
