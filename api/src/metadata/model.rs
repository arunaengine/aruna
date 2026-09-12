use super::format_timestamp_ms;
use crate::error::{ProfileValidationFindingResponse, ValidationViolationResponse};
use aruna_core::metadata::{
    MetadataProfileValidationCompleteness, MetadataProfileValidationState,
    MetadataProfileValidationStatus,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_operations::metadata::MetadataPathWinner;
use aruna_operations::metadata::profile_validation::MetadataProfilePreview;
use aruna_operations::metadata::public_preview::RestrictedFilesPreview;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use utoipa::ToSchema;

/// Public registry summary for a write accepted by the durable projection pipeline.
/// Graph visibility and remote replicas may still be catching up.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataDocumentSummary {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub replicas: usize,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProfileValidationCapabilitiesResponse {
    pub evaluator: String,
    pub supported_constraints: Vec<String>,
    pub unsupported_constraint_policy: String,
    pub public_profile_iri_template: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ProfileValidationPreviewRequest {
    /// RO-Crate JSON-LD draft. Nothing is stored.
    #[schema(value_type = Object)]
    pub rocrate: Value,
    /// Group the draft would be saved in. A Profile of that group resolves
    /// even while it is not public; without it only public Profiles do.
    #[serde(default)]
    pub group_id: Option<String>,
    /// Whether the draft would be saved as a public dataset. Only then are
    /// restricted files reported.
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProfileValidationPreviewResponse {
    /// Whether a create or replace of this draft would be accepted.
    pub accepted: bool,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_iri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_revision: Option<String>,
    pub evaluator: String,
    pub findings: Vec<ProfileValidationFindingResponse>,
    pub completeness: String,
    pub structural_violations: Vec<ValidationViolationResponse>,
    /// Data entities the anonymous principal may not read. Empty unless the
    /// request set `public`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub restricted_files: Vec<RestrictedFileResponse>,
    /// Whether every relevant file could be checked. Present for public drafts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restricted_files_complete: Option<bool>,
}

/// A draft data entity that a public dataset would not expose. The location
/// fields are omitted when the caller may not read the object either.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RestrictedFileResponse {
    pub entity_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    /// The object's permission path, ready to grant READ on exactly this object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub permission_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

impl From<MetadataProfilePreview> for ProfileValidationPreviewResponse {
    fn from(preview: MetadataProfilePreview) -> Self {
        let accepted = preview.accepted();
        let status = ProfileValidationStatusResponse::from(preview.status);
        Self {
            accepted,
            state: status.state,
            profile_id: status.profile_id,
            profile_iri: status.profile_iri,
            profile_revision: status.profile_revision,
            evaluator: status.evaluator,
            findings: status.findings,
            completeness: status.completeness,
            structural_violations: preview
                .structural_violations
                .into_iter()
                .map(Into::into)
                .collect(),
            restricted_files: Vec::new(),
            restricted_files_complete: None,
        }
    }
}

impl ProfileValidationPreviewResponse {
    pub(crate) fn set_restricted(&mut self, preview: RestrictedFilesPreview) {
        self.restricted_files_complete = Some(preview.complete);
        self.restricted_files = preview
            .files
            .into_iter()
            .map(|file| RestrictedFileResponse {
                entity_id: file.entity_id,
                group_id: file.group_id.map(|id| id.to_string()),
                permission_path: file.permission_path,
                bucket: file.bucket,
                key: file.key,
            })
            .collect();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProfileValidationStatusResponse {
    pub document_id: String,
    pub dataset_revision: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_iri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_revision: Option<String>,
    pub evaluator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validated_at_ms: Option<u64>,
    pub findings: Vec<ProfileValidationFindingResponse>,
    pub completeness: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale_reason: Option<String>,
}

impl From<MetadataProfileValidationStatus> for ProfileValidationStatusResponse {
    fn from(status: MetadataProfileValidationStatus) -> Self {
        Self {
            document_id: status.document_id.to_string(),
            dataset_revision: status.dataset_revision.to_string(),
            state: match status.state {
                MetadataProfileValidationState::NotProfiled => "not_profiled",
                MetadataProfileValidationState::Valid => "valid",
                MetadataProfileValidationState::Invalid => "invalid",
                MetadataProfileValidationState::Stale => "stale",
            }
            .to_string(),
            profile_id: status.profile_id.map(|id| id.to_string()),
            profile_iri: status.profile_iri,
            profile_revision: status.profile_revision,
            evaluator: status.evaluator,
            validated_at_ms: status.validated_at_ms,
            findings: status.findings.into_iter().map(Into::into).collect(),
            completeness: match status.completeness {
                MetadataProfileValidationCompleteness::Complete => "complete",
                MetadataProfileValidationCompleteness::Incomplete => "incomplete",
            }
            .to_string(),
            stale_reason: status.stale_reason,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateMetadataScaffoldRequest {
    pub group_id: String,
    pub path: String,
    pub name: String,
    pub description: String,
    pub date_published: String,
    #[serde(default)]
    pub license: Option<String>,
    #[serde(default)]
    pub public: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateMetadataRoCrateRequest {
    pub group_id: String,
    pub path: String,
    #[serde(default)]
    pub public: bool,
    #[schema(value_type = Object)]
    pub rocrate: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum CreateMetadataRequest {
    Scaffold(CreateMetadataScaffoldRequest),
    RoCrate(CreateMetadataRoCrateRequest),
}

/// Response for a metadata create request accepted into the durable
/// event/projection pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateMetadataResponse {
    /// Accepted registry summary. It does not guarantee the graph is fully
    /// materialized, queryable, searchable, or replicated yet.
    #[serde(flatten)]
    pub summary: MetadataDocumentSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListMetadataResponse {
    pub documents: Vec<MetadataDocumentListItem>,
    pub limit: usize,
    pub offset: usize,
    pub total_returned: usize,
    /// Exact total of documents visible to the caller across all pages,
    /// evaluated per document against the caller's permission rules. Absent
    /// on targeted lookups, whose limit is too small to be worth the scan.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_estimate: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataDocumentListItem {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub public: bool,
    pub replicas: usize,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub rocrate_summary: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPathResponse {
    pub winner: MetadataDocumentSummary,
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataPathQuery {
    pub path: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ListMetadataQuery {
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub path_prefix: Option<String>,
    #[serde(default)]
    pub include: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub order: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReplaceMetadataRoCrateRequest {
    #[schema(value_type = Object)]
    pub rocrate: Value,
    #[serde(default)]
    pub public: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum MetadataRoCrateResponse {
    Projected(ProjectedRoCrateResponse),
    Raw(MetadataRawRoCrateResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ProjectedRoCrateResponse {
    #[schema(value_type = Object)]
    pub rocrate: Value,
    pub total_data_entities: Option<usize>,
    pub returned_data_entities: Option<usize>,
    pub next_offset: Option<usize>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataRawRoCrateResponse {
    #[schema(value_type = Object)]
    pub raw: Value,
    pub winning_event_id: String,
    pub projection_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projected_event_id: Option<String>,
    pub context_digest: String,
    pub dataset_digest: Option<String>,
    /// Present only while the merged graph fails profile validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged: Option<MetadataMergedRoCrateResponse>,
}

/// The merged graph while it is invalid: what the editor opens to fix it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataMergedRoCrateResponse {
    #[schema(value_type = Object)]
    pub rocrate: Value,
    pub findings: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(value_type = Object)]
pub struct JsonLdObject(pub Value);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetadataRoCrateView {
    Full,
    Summary,
    Page,
    Raw,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataRoCrateExportParams {
    #[serde(default)]
    pub view: Option<MetadataRoCrateView>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub after: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SubmitRoCrateExportRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SubmitRoCrateExportResponse {
    pub job_id: String,
    pub created: bool,
    pub owner_node_url: String,
    pub status_url: String,
    pub report_url: String,
    pub artifact_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchParams {
    #[serde(default)]
    pub q: String,
    #[serde(default)]
    pub conforms_to: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque continuation token from a previous response's `next_cursor`. Bound
    /// to the original query; a changed query is rejected with `400`.
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchHitResponse {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    pub subject_iri: String,
    pub score: f32,
    /// Human-readable title for the hit, populated by the answering node from
    /// the resource's `schema:name` with a path-based fallback.
    pub title: String,
    /// Query-relevant text excerpt, populated by the answering node. Absent when
    /// the resource has no indexed literals to window.
    pub snippet: Option<String>,
    /// `rdf:type` IRIs of the matched subject, at most eight, so a file entity
    /// can be told apart from the dataset it belongs to. Empty when the
    /// answering node knows no named type for it.
    pub subject_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataSearchResponse {
    pub hits: Vec<MetadataSearchHitResponse>,
    /// Continuation token for the next page, or `null` when the results are
    /// exhausted. Pass it back as `cursor` to fetch the next page.
    pub next_cursor: Option<String>,
    /// Number of node partitions this search was executed against.
    pub nodes_queried: usize,
    /// Number of node partitions that failed or timed out; a non-zero value
    /// means the result is partial.
    pub nodes_failed: usize,
    /// True when pagination stopped at the server-side depth cap before the
    /// result set was exhausted.
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct MetadataReferencesParams {
    /// Referenced object IRI to find backlinks for, such as a document graph IRI
    /// or any IRI that appears as a triple object.
    pub iri: String,
    /// Optional exact predicate IRI filter, such as http://schema.org/conformsTo.
    #[serde(default)]
    pub predicate: Option<String>,
    /// Page size (default 25, silently clamped to a maximum of 100).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Resolve `iri` as a document graph IRI and return that document's summary
    /// as a single predicate-less entry, skipping the backlink scan.
    #[serde(default)]
    pub resolve: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataReferenceItem {
    pub document_id: String,
    pub group_id: String,
    pub document_path: String,
    pub graph_iri: String,
    /// Predicate that names the queried IRI, or absent for a resolved graph-IRI
    /// entry.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    /// Subjects in the referencing document that name the queried IRI; empty for
    /// a resolved graph-IRI entry.
    pub subject_iris: Vec<String>,
    /// Human-readable title for the referencing document, from its root
    /// schema:name with a document-path fallback.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataReferencesResponse {
    pub references: Vec<MetadataReferenceItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MetadataReferencePreflightTargetBody {
    ContentW3ids {
        content_w3ids: Vec<String>,
        #[serde(default)]
        remove_all_resolvable_locations: bool,
    },
    BucketPrefix {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        operation: MetadataPreflightStorageOperationBody,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetadataPreflightStorageOperationBody {
    #[default]
    LatestVersionTombstone,
    AllVersionsPurge,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataReferencePreflightBody {
    pub target: MetadataReferencePreflightTargetBody,
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
    #[serde(default = "default_allow_partial")]
    pub allow_partial: bool,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightLocationResponse {
    pub node_id: String,
    pub bucket: String,
    pub key: String,
    pub version_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightVisibleReferenceResponse {
    pub document_id: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightTargetResponse {
    pub content_w3id: String,
    pub targeted_versions: Vec<MetadataPreflightLocationResponse>,
    pub visible_references: Vec<MetadataPreflightVisibleReferenceResponse>,
    pub hidden_references_exist: bool,
    pub would_remove_last_resolvable_aruna_location: bool,
    pub location_impact_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightExcludedFormResponse {
    pub form: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightNodeFreshnessResponse {
    pub node_id: String,
    pub index_state: String,
    pub oldest_status_updated_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataPreflightCoverageResponse {
    pub queried_scope: String,
    pub queried_forms: Vec<String>,
    pub excluded_forms: Vec<MetadataPreflightExcludedFormResponse>,
    pub node_freshness: Vec<MetadataPreflightNodeFreshnessResponse>,
    pub target_resolution_complete: bool,
    pub path_style_endpoint_coverage_complete: bool,
    pub realm_coverage_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataReferencePreflightResponse {
    pub targets: Vec<MetadataPreflightTargetResponse>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
    pub complete: bool,
    pub failed_partitions: Vec<String>,
    pub coverage: MetadataPreflightCoverageResponse,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct MetadataIncludeFlags {
    summary: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SparqlQueryRequest {
    /// SPARQL query string. Only `SELECT` and `ASK` queries are supported.
    pub query: String,
    /// Query scope; omitted means distributed. `local` uses only this node.
    /// `distributed` fans out all-metadata queries, but document queries try
    /// replicas sequentially until one complete result succeeds.
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
    /// Keeps successful all-metadata partitions, and permits document-local fallback
    /// only when holder discovery fails. Replica failover never returns partial data.
    #[serde(default = "default_allow_partial")]
    pub allow_partial: bool,
}

fn default_allow_partial() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum MetadataQueryMode {
    /// Run the query only on the current node.
    Local,
    /// Fan out all-metadata queries or try document replicas sequentially.
    /// Only fanout and document discovery fallback may return partial data.
    Distributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MetadataQueryResponse {
    #[serde(flatten)]
    pub result: MetadataQueryResult,
    /// Number of node attempts made while producing this result.
    pub nodes_queried: usize,
    /// Number of selected partitions absent from the result; non-zero means partial.
    /// Failed document replica attempts followed by success are not partitions.
    pub nodes_failed: usize,
    /// Whether every selected partition completed successfully.
    pub complete: bool,
    /// Node partitions that failed, timed out, or exceeded the fan-out bound.
    pub failed_partitions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", content = "value")]
pub enum MetadataQueryResult {
    Solutions(Vec<HashMap<String, String>>),
    Boolean(bool),
}

impl From<&MetadataRegistryRecord> for MetadataDocumentSummary {
    fn from(record: &MetadataRegistryRecord) -> Self {
        Self {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            public: record.public,
            replicas: record.holder_node_ids.len(),
            created_at: format_timestamp_ms(record.created_at_ms),
            updated_at: format_timestamp_ms(record.updated_at_ms),
        }
    }
}

impl From<&MetadataPathWinner> for MetadataDocumentSummary {
    fn from(winner: &MetadataPathWinner) -> Self {
        Self {
            document_id: winner.document_id.to_string(),
            group_id: winner.group_id.to_string(),
            document_path: winner.document_path.clone(),
            graph_iri: winner.graph_iri.clone(),
            public: winner.public,
            replicas: winner.replicas,
            created_at: format_timestamp_ms(winner.created_at_ms),
            updated_at: format_timestamp_ms(winner.updated_at_ms),
        }
    }
}

impl MetadataDocumentListItem {
    pub(super) fn from_record(
        record: &MetadataRegistryRecord,
        rocrate_summary: Option<Value>,
    ) -> Self {
        Self {
            document_id: record.document_id.to_string(),
            group_id: record.group_id.to_string(),
            document_path: record.document_path.clone(),
            graph_iri: record.graph_iri.clone(),
            public: record.public,
            replicas: record.holder_node_ids.len(),
            created_at: format_timestamp_ms(record.created_at_ms),
            updated_at: format_timestamp_ms(record.updated_at_ms),
            rocrate_summary,
        }
    }
}
