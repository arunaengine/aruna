//! Paginated audit of one external job's immutable records.
//!
//! Every alternative execution, output and cancellation observation stays
//! visible after convergence, so a caller can see exactly what ran, not only
//! what the canonical projection selected. The projection redacts the record
//! internals: identities, signatures and raw envelopes never leave the node,
//! and a caller that did not submit the job is answered 404 like any other
//! unknown id.

use std::sync::Arc;

use aruna_core::structs::{AuthContext, JobFamilyRecord, JobId, JobRecordEnvelope};
use aruna_operations::jobs::lifecycle::{AuditPaging, AuditRange, family_audit, family_report};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::jobs::{JobOutputResponse, map_job_route, output_response, parse_job_id};
use crate::auth::require_unrestricted_realm_auth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

/// The audit route joins the `jobs` tag the jobs module already declares.
#[derive(OpenApi)]
#[openapi()]
pub struct JobAuditApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(JobAuditApiDoc::openapi()).routes(routes!(get_job_audit))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AuditQuery {
    pub scope: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

/// One immutable record, projected without its envelope, signature or the node
/// identities it names.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobAuditRecord {
    /// `spec`, `claim`, `budget`, `launch`, `receipt`, `update`, `output` or
    /// `cancel`.
    pub kind: String,
    /// Digest of the canonical record bytes.
    pub digest: String,
    /// Request family the record belongs to. A value other than the requested
    /// family is an idempotency conflict of the same submission.
    pub request_digest: String,
    pub conflicting_family: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_alias: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<JobOutputResponse>,
    pub at_ms: u64,
}

/// A record refused under a key another record already holds. Both stay
/// addressable; neither overwrites the other.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobAuditConflict {
    pub kind: String,
    /// Digest of the refused record.
    pub digest: String,
    /// Digest of the record already stored under the same key.
    pub retained: String,
    pub observed_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct JobAuditResponse {
    pub submission_id: String,
    pub request_digest: String,
    pub scope: String,
    pub records: Vec<JobAuditRecord>,
    /// Same-key/different-digest records, reported with the first page only.
    pub conflicts: Vec<JobAuditConflict>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Projection digest the responder reduced while answering, so a client can
    /// tell that its view moved between pages.
    pub projection_digest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub responder_node_id: Option<String>,
    /// This responder could not reduce every record of the family.
    pub partial: bool,
}

fn hex32(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_range(scope: Option<&str>) -> ServerResult<(AuditRange, &'static str)> {
    match scope {
        None | Some("family") => Ok((AuditRange::Family, "family")),
        Some("submission") => Ok((AuditRange::Submission, "submission")),
        Some(_) => Err(ServerError::BadRequest),
    }
}

/// Both bounds are the record store's own, validated before any read starts.
fn parse_paging(query: &AuditQuery) -> ServerResult<AuditPaging> {
    let cursor = query
        .cursor
        .as_deref()
        .map(|cursor| URL_SAFE_NO_PAD.decode(cursor))
        .transpose()
        .map_err(|_| ServerError::BadRequest)?;
    AuditPaging::new(cursor, query.limit)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))
}

/// Projects one record. Publishers, executors, schedulers and signatures are
/// dropped: the audit answers what happened, never who may be reached.
fn audit_record(
    envelope: &JobRecordEnvelope,
    family_digest: &[u8; 32],
    canonical: JobId,
) -> Option<JobAuditRecord> {
    let digest = hex32(&envelope.record.digest().ok()?);
    let record_family = envelope.family();
    let mut record = JobAuditRecord {
        kind: kind_name(&envelope.record).to_string(),
        digest,
        request_digest: hex32(&record_family.request_digest),
        conflicting_family: &record_family.request_digest != family_digest,
        job_id: None,
        canonical_alias: None,
        execution_id: None,
        sequence: None,
        state: None,
        spec_digest: None,
        plan_digest: None,
        outputs: Vec::new(),
        at_ms: 0,
    };
    match &envelope.record {
        JobFamilyRecord::Spec(spec) => {
            record.job_id = Some(spec.job_id.to_string());
            record.spec_digest = Some(hex32(&spec.spec_digest));
            record.at_ms = spec.created_at_ms;
        }
        JobFamilyRecord::Claim(claim) => {
            record.job_id = Some(claim.job_id.to_string());
            record.canonical_alias = Some(claim.job_id == canonical);
            record.spec_digest = Some(hex32(&claim.spec_digest));
            record.at_ms = claim.accepted_at_ms;
        }
        JobFamilyRecord::Budget(budget) => {
            record.spec_digest = Some(hex32(&budget.source_spec_digest));
            record.sequence = Some(u64::from(budget.max_launches));
        }
        JobFamilyRecord::Launch(launch) => {
            record.job_id = Some(launch.job_id.to_string());
            record.sequence = Some(u64::from(launch.scheduler_seq));
            record.plan_digest = Some(hex32(&launch.plan_digest));
            record.spec_digest = Some(hex32(&launch.spec_digest));
            record.at_ms = launch.created_at_ms;
        }
        JobFamilyRecord::Receipt(receipt) => {
            record.job_id = Some(receipt.job_id.to_string());
            record.execution_id = Some(receipt.execution_id.to_string());
            record.spec_digest = Some(hex32(&receipt.spec_digest));
            record.at_ms = receipt.accepted_at_ms;
        }
        JobFamilyRecord::Update(update) => {
            record.execution_id = Some(update.execution_id.to_string());
            record.sequence = Some(update.sequence);
            record.state = Some(update.state.name().to_string());
            record.at_ms = update.observed_at_ms;
        }
        JobFamilyRecord::Output(output) => {
            record.job_id = Some(output.job_id.to_string());
            record.execution_id = Some(output.execution_id.to_string());
            record.outputs = output
                .outputs
                .as_slice()
                .iter()
                .map(output_response)
                .collect();
            record.at_ms = output.committed_at_ms;
        }
        JobFamilyRecord::Cancel(cancel) => {
            record.job_id = Some(cancel.job_id.to_string());
            record.spec_digest = Some(hex32(&cancel.spec_digest));
            record.at_ms = cancel.requested_at_ms;
        }
    }
    Some(record)
}

fn kind_name(record: &JobFamilyRecord) -> &'static str {
    match record {
        JobFamilyRecord::Spec(_) => "spec",
        JobFamilyRecord::Claim(_) => "claim",
        JobFamilyRecord::Budget(_) => "budget",
        JobFamilyRecord::Launch(_) => "launch",
        JobFamilyRecord::Receipt(_) => "receipt",
        JobFamilyRecord::Update(_) => "update",
        JobFamilyRecord::Output(_) => "output",
        JobFamilyRecord::Cancel(_) => "cancel",
    }
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/audit",
    tag = "jobs",
    summary = "Page the immutable records of one external job",
    description = "Requires a realm bearer token; a path-restricted (delegated) token is refused. Reads are self-scoped: only the submitter of the request may audit it, and any other id answers 404, so the surface never confirms that somebody else's job exists. The page is ordered by stable record key, never by arrival, and it exposes every alternative execution and every output that any partition produced, not only the canonical one: at-least-once execution means duplicates are normal and stay auditable forever. `scope=family` pages the request family the alias resolves to; `scope=submission` also pages the idempotency conflicts of the same submission, each record marked with `conflicting_family`. Records are projected, never returned raw: signatures, envelopes and the node identities of publishers, schedulers and executors are omitted. `conflicts` lists records refused under a key another record already held and is reported with the first page only. `partial` means this responder holds more records than one projection reduces, and the answer is this node's local view of a replicated log, so a later page may reveal records an earlier one could not.",
    params(
        ("job_id" = String, Path, description = "Job identifier as returned by submission, or any accepted alias of the same request: a 26-character ULID-shaped id. An unparseable id is 404"),
        ("scope" = Option<String>, Query, description = "`family` (the default) pages the request family only; `submission` also pages other request families of the same submission. Any other value is 400"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from a previous page's `next_cursor`, base64url without padding. Anything that is not a record key of this log is rejected with 400"),
        ("limit" = Option<usize>, Query, description = "Records per page, 1 to 64; the default is 64 and any other value is rejected with 400")
    ),
    responses(
        (
            status = 200,
            description = "One page of the immutable log, oldest record key first; a missing `next_cursor` means the scope is exhausted at this responder",
            body = JobAuditResponse,
            example = json!({
                "submission_id": "6b1f8c9d0e2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4",
                "request_digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                "scope": "family",
                "records": [
                    {
                        "kind": "claim",
                        "digest": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f80910",
                        "request_digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                        "conflicting_family": false,
                        "job_id": "01JJRSTVWXYZ0123456789ABCD",
                        "canonical_alias": true,
                        "spec_digest": "7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e",
                        "outputs": [],
                        "at_ms": 1755500000000u64
                    },
                    {
                        "kind": "output",
                        "digest": "4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c",
                        "request_digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                        "conflicting_family": false,
                        "job_id": "01JJRSTVWXYZ0123456789ABCD",
                        "execution_id": "01JJRSEXEC0123456789ABCDEF",
                        "outputs": [{
                            "bucket": "ws-01jjrstvwxyz0123456789abcd",
                            "key": "reports/reads_fastqc.html",
                            "version_id": "01JJRSVERSION0123456789ABC",
                            "execution_id": "01JJRSEXEC0123456789ABCDEF",
                            "container_path": "/outputs/reads_fastqc.html",
                            "size": 20480,
                            "digest": "fa2c8cc4f28176bbeed4b736df569a34c79cd3723e9ec42f9674b4d46ac6b8b8"
                        }],
                        "at_ms": 1755500009000u64
                    }
                ],
                "conflicts": [],
                "next_cursor": "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHw",
                "projection_digest": "1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f",
                "responder_node_id": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                "partial": false
            })
        ),
        (status = 400, description = "Unknown `scope`, a cursor that is not a record key of this log, or a `limit` outside 1..=64", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "The token is path-restricted or belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No external job with that id is known at this responder, or it was submitted by somebody else; absence and foreign ownership are deliberately indistinguishable, and a responder that never held the family answers the same way, so page the node that accepted the submission", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_job_audit(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(job_id): Path<String>,
    Query(query): Query<AuditQuery>,
) -> ServerResult<(StatusCode, Json<JobAuditResponse>)> {
    let auth: AuthContext = require_unrestricted_realm_auth(&state, auth)?;
    let job_id = parse_job_id(&job_id)?;
    let (range, scope) = parse_range(query.scope.as_deref())?;
    let paging = parse_paging(&query)?;
    let context = state.get_ctx();

    let report = family_report(&context, &auth, job_id)
        .await
        .ok_or(ServerError::NotFound)?
        .map_err(map_job_route)?;
    let page = family_audit(&context, &auth, job_id, range, paging)
        .await
        .ok_or(ServerError::NotFound)?
        .map_err(map_job_route)?;

    let records = page
        .records
        .iter()
        .filter_map(|envelope| {
            audit_record(envelope, &report.request_digest, report.canonical_job_id)
        })
        .collect();
    let conflicts = page
        .conflicts
        .iter()
        .filter_map(|conflict| {
            Some(JobAuditConflict {
                kind: kind_name(&conflict.envelope.record).to_string(),
                digest: hex32(&conflict.envelope.record.digest().ok()?),
                retained: hex32(&conflict.retained),
                observed_at_ms: conflict.observed_at_ms,
            })
        })
        .collect();
    Ok((
        StatusCode::OK,
        Json(JobAuditResponse {
            submission_id: hex32(&report.submission_id.0),
            request_digest: hex32(&report.request_digest),
            scope: scope.to_string(),
            records,
            conflicts,
            next_cursor: page
                .next
                .as_ref()
                .map(|cursor| URL_SAFE_NO_PAD.encode(cursor.as_slice())),
            projection_digest: hex32(&report.digest),
            responder_node_id: report.responder.map(|node| node.to_string()),
            partial: report.partial,
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_operations::jobs::lifecycle::MAX_AUDIT_PAGE;

    fn query(cursor: Option<&str>, limit: Option<usize>) -> AuditQuery {
        AuditQuery {
            scope: None,
            cursor: cursor.map(str::to_string),
            limit,
        }
    }

    #[test]
    fn rejects_bad_paging() {
        // A cursor must be a bounded record key and a limit must fit the page.
        assert!(parse_paging(&query(Some("not base64 !"), None)).is_err());
        assert!(parse_paging(&query(Some(&URL_SAFE_NO_PAD.encode([1u8; 200])), None)).is_err());
        assert!(parse_paging(&query(Some(&URL_SAFE_NO_PAD.encode([1u8; 32])), None)).is_ok());
        assert!(parse_paging(&query(None, Some(0))).is_err());
        assert!(parse_paging(&query(None, Some(MAX_AUDIT_PAGE + 1))).is_err());
        assert!(parse_paging(&query(None, None)).is_ok());
        assert!(parse_range(Some("everything")).is_err());
        assert_eq!(parse_range(None).unwrap().1, "family");
        assert_eq!(parse_range(Some("submission")).unwrap().1, "submission");
    }

    #[test]
    fn redacts_record_internals() {
        // The projection must never carry identities, signatures or envelopes.
        let value = serde_json::to_value(JobAuditRecord {
            kind: "receipt".to_string(),
            digest: "aa".to_string(),
            request_digest: "bb".to_string(),
            conflicting_family: false,
            job_id: Some(JobId::from_bytes([1u8; 16]).to_string()),
            canonical_alias: None,
            execution_id: Some("01JJRSEXEC0123456789ABCDEF".to_string()),
            sequence: None,
            state: None,
            spec_digest: None,
            plan_digest: None,
            outputs: Vec::new(),
            at_ms: 7,
        })
        .expect("record serializes");

        let fields: Vec<&String> = value.as_object().expect("object").keys().collect();
        for forbidden in [
            "signature",
            "published_by",
            "executor_node_id",
            "scheduler_node_id",
            "envelope",
            "auth_token",
        ] {
            assert!(
                !fields.iter().any(|field| field.as_str() == forbidden),
                "{forbidden} must not reach the audit surface"
            );
        }
    }
}
