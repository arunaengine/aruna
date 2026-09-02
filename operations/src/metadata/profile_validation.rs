use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::METADATA_PROFILE_VALIDATION_STATUS_KEYSPACE;
use aruna_core::metadata::{
    MetadataError, MetadataProfileValidationCompleteness, MetadataProfileValidationFinding,
    MetadataProfileValidationSeverity, MetadataProfileValidationState,
    MetadataProfileValidationStatus, MetadataRawRevision, MetadataValidationViolation,
    is_rocrate_specification,
};
use aruna_core::storage_entries::{
    metadata_profile_validation_status_key, metadata_profile_validation_status_write_entry,
};
use aruna_core::structs::MetadataRegistryRecord;
use aruna_core::types::{GroupId, TxnId};
use aruna_core::util::unix_timestamp_millis as now_ms;
use craqle::{CrateViolation, ShaclValidationResult};
use oxrdf::{Dataset, GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use oxttl::NQuadsParser;
use ulid::Ulid;

use crate::driver::DriverContext;
use crate::metadata::MetadataHandle;
use crate::metadata::api::ExportMetadataRoCrateResult;
use crate::metadata::forward::export_profile_routed;
use crate::metadata::profile_shacl::{
    ProfileShaclError, ProfileShaclReport, ProfileShapes, VALIDATION_GRAPH_IRI,
};
use crate::metadata::raw::load_raw_revision;
use crate::metadata::repository::{
    StorageReadError, parse_registry_read, read_registry_by_document_effect,
};

const SH: &str = "http://www.w3.org/ns/shacl#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const DCTERMS_CONFORMS_TO: &str = "http://purl.org/dc/terms/conformsTo";
const SCHEMA_CONFORMS_TO: &str = "http://schema.org/conformsTo";
const SCHEMA_HTTPS_CONFORMS_TO: &str = "https://schema.org/conformsTo";
const SCHEMA_ABOUT: &str = "http://schema.org/about";
const SCHEMA_HTTPS_ABOUT: &str = "https://schema.org/about";
const SCHEMA_ENCODING_FORMAT: &str = "http://schema.org/encodingFormat";
const SCHEMA_HTTPS_ENCODING_FORMAT: &str = "https://schema.org/encodingFormat";
const SCHEMA_TEXT: &str = "http://schema.org/text";
const SCHEMA_HTTPS_TEXT: &str = "https://schema.org/text";
const DX_PROFILE: &str = "http://www.w3.org/ns/dx/prof/Profile";
const PROFILE_PUBLIC_PREFIX: &str = "https://w3id.org/aruna/profile/";
const EVALUATOR_NAME: &str = "craqle-shacl-core/0.2";

/// Authoritative backend SHACL support for Profile validation.
///
/// Shapes are compiled and executed by craqle's native SHACL Core Subset v1
/// engine. Every construct outside this set, including SHACL-SPARQL, SHACL-JS,
/// SHACL-AF, custom components and targets, recursive shapes, RDF-star, and
/// remote `owl:imports`, fails closed with an `unsupported_constraint` finding.
pub const SUPPORTED_PROFILE_CONSTRAINTS: &[&str] = &[
    "sh:targetClass",
    "sh:targetNode",
    "sh:targetSubjectsOf",
    "sh:targetObjectsOf",
    "implicit class target",
    "sh:property",
    "sh:path (predicate)",
    "sh:path (sh:inversePath)",
    "sh:path (sequence)",
    "sh:path (sh:alternativePath)",
    "sh:path (sh:zeroOrOnePath)",
    "sh:path (sh:zeroOrMorePath)",
    "sh:path (sh:oneOrMorePath)",
    "sh:class",
    "sh:datatype",
    "sh:nodeKind",
    "sh:minCount",
    "sh:maxCount",
    "sh:minExclusive",
    "sh:minInclusive",
    "sh:maxExclusive",
    "sh:maxInclusive",
    "sh:minLength",
    "sh:maxLength",
    "sh:pattern",
    "sh:flags",
    "sh:uniqueLang",
    "sh:languageIn",
    "sh:equals",
    "sh:disjoint",
    "sh:lessThan",
    "sh:lessThanOrEquals",
    "sh:or",
    "sh:and",
    "sh:not",
    "sh:xone",
    "sh:node",
    "sh:hasValue",
    "sh:in",
    "sh:qualifiedValueShape",
    "sh:qualifiedMinCount",
    "sh:qualifiedMaxCount",
    "sh:qualifiedValueShapesDisjoint",
    "sh:closed",
    "sh:ignoredProperties",
    "sh:severity",
    "sh:deactivated",
    "sh:message (annotation)",
    "sh:name (annotation)",
    "sh:description (annotation)",
    "sh:order (annotation)",
    "sh:group (annotation)",
];

#[derive(Debug)]
struct ResolvedProfile {
    id: Ulid,
    requested_iri: String,
    revision: Ulid,
    shapes: Vec<String>,
}

/// Which registered Profiles a validation may resolve.
///
/// Usability is a property of the registry row alone: a Profile serves the
/// Datasets of its own group, and everyone once it is public. Whoever reaches
/// validation already proved WRITE on the Dataset's path in that group, or READ
/// on the group's metadata for a preview, so no caller identity takes part in
/// the decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProfileScope {
    /// Datasets of this group, so its own Profiles resolve as well.
    Group(GroupId),
    /// No group is known, so only public Profiles resolve.
    PublicOnly,
}

impl ProfileScope {
    fn may_use(self, record: &MetadataRegistryRecord) -> bool {
        record.public
            || matches!(self, ProfileScope::Group(group_id) if group_id == record.group_id)
    }
}

impl From<Option<GroupId>> for ProfileScope {
    fn from(group_id: Option<GroupId>) -> Self {
        group_id.map_or(ProfileScope::PublicOnly, ProfileScope::Group)
    }
}

pub fn evaluator_name() -> &'static str {
    EVALUATOR_NAME
}

pub fn profile_public_iri(profile_id: Ulid) -> String {
    format!("{PROFILE_PUBLIC_PREFIX}{profile_id}")
}

pub fn equivalent_profile_iris(iri: &str) -> Vec<String> {
    profile_id_from_iri(iri).map_or_else(
        || vec![iri.to_string()],
        |profile_id| vec![profile_public_iri(profile_id)],
    )
}

pub(crate) fn submission_has_profile_tag(jsonld: &str) -> bool {
    data_graph(jsonld)
        .map(|(data, root)| !profile_tags(&data, &root).is_empty())
        .unwrap_or(true)
}

pub async fn validate_submission(
    context: &DriverContext,
    document_id: Ulid,
    group_id: GroupId,
    jsonld: &str,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let status =
        assess_submission(context, document_id, ProfileScope::Group(group_id), jsonld).await?;
    if status.state == MetadataProfileValidationState::Invalid {
        return Err(MetadataError::ProfileValidation(status.findings));
    }
    Ok(status)
}

async fn assess_submission(
    context: &DriverContext,
    document_id: Ulid,
    scope: ProfileScope,
    jsonld: &str,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let (data, root) = data_graph(jsonld)?;
    let Some(requested_iri) = single_profile_tag(&data, &root)? else {
        return Ok(not_profiled_status(document_id));
    };
    let profile = resolve_registered_profile(context, &requested_iri, scope).await?;
    let metadata = evaluator_handle(context, Some(profile.revision))?;
    let assessment = evaluate_profile(metadata, &profile, jsonld).await?;
    Ok(profiled_status(document_id, &profile, assessment.findings))
}

/// Profile verdict for a merged render. A merge is never rejected, so an
/// evaluator that cannot answer reports stale rather than failing the job.
pub(crate) async fn assess_render(
    context: &DriverContext,
    document_id: Ulid,
    group_id: GroupId,
    jsonld: &str,
) -> MetadataProfileValidationStatus {
    match assess_submission(context, document_id, ProfileScope::Group(group_id), jsonld).await {
        Ok(status) => status,
        Err(MetadataError::ProfileValidation(findings)) => {
            let mut status = stale_status(document_id, "profile_unavailable");
            status.findings = findings;
            status
        }
        Err(error) => stale_status(document_id, &error.to_string()),
    }
}

/// Findings that make a document invalid, and so keep the render undisplayed.
pub(crate) fn violation_count(status: &MetadataProfileValidationStatus) -> u32 {
    if status.state != MetadataProfileValidationState::Invalid {
        return 0;
    }
    u32::try_from(
        status
            .findings
            .iter()
            .filter(|finding| finding.severity == MetadataProfileValidationSeverity::Violation)
            .count(),
    )
    .unwrap_or(u32::MAX)
}

/// The verdict a create or replace would enforce for an unsaved draft.
#[derive(Debug)]
pub struct MetadataProfilePreview {
    pub status: MetadataProfileValidationStatus,
    pub structural_violations: Vec<MetadataValidationViolation>,
}

impl MetadataProfilePreview {
    pub fn accepted(&self) -> bool {
        self.structural_violations.is_empty()
            && self.status.state != MetadataProfileValidationState::Invalid
    }
}

/// Validates a draft without reading or writing any stored document. Without a
/// group only public Profiles resolve, exactly as a foreign group's write would.
pub async fn preview_submission(
    context: &DriverContext,
    group_id: Option<GroupId>,
    jsonld: &str,
) -> Result<MetadataProfilePreview, MetadataError> {
    let (data, root) = data_graph(jsonld)?;
    let Some(requested_iri) = single_profile_tag(&data, &root)? else {
        let metadata = evaluator_handle(context, None)?;
        let structural = metadata
            .preview_crate_structure(jsonld.to_string())
            .await
            .map_err(|error| shacl_failure(error, None))?;
        return Ok(MetadataProfilePreview {
            status: not_profiled_status(Ulid::nil()),
            structural_violations: structural.into_iter().map(structural_violation).collect(),
        });
    };
    let profile = resolve_registered_profile(context, &requested_iri, group_id.into()).await?;
    let metadata = evaluator_handle(context, Some(profile.revision))?;
    let assessment = evaluate_profile(metadata, &profile, jsonld).await?;
    Ok(MetadataProfilePreview {
        status: profiled_status(Ulid::nil(), &profile, assessment.findings),
        structural_violations: assessment.structural,
    })
}

struct ProfileAssessment {
    findings: Vec<MetadataProfileValidationFinding>,
    structural: Vec<MetadataValidationViolation>,
}

async fn evaluate_profile(
    metadata: &MetadataHandle,
    profile: &ResolvedProfile,
    jsonld: &str,
) -> Result<ProfileAssessment, MetadataError> {
    let shapes = ProfileShapes {
        profile_id: profile.id,
        revision: profile.revision,
        graph_iri: MetadataRegistryRecord::graph_iri_for(profile.id),
        sources: profile.shapes.clone(),
    };
    match metadata
        .evaluate_profile_shapes(shapes, jsonld.to_string())
        .await
    {
        Ok(report) => Ok(shacl_assessment(report, profile.revision)),
        Err(ProfileShaclError::Unsupported { rule, message }) => Ok(ProfileAssessment {
            findings: vec![unsupported_finding(&rule, message, profile.revision)],
            structural: Vec::new(),
        }),
        Err(ProfileShaclError::Limit { message }) => Ok(ProfileAssessment {
            findings: vec![limit_finding(message, profile.revision)],
            structural: Vec::new(),
        }),
        Err(error) => Err(shacl_failure(error, Some(profile.revision))),
    }
}

fn shacl_assessment(report: ProfileShaclReport, profile_revision: Ulid) -> ProfileAssessment {
    ProfileAssessment {
        findings: report
            .results
            .iter()
            .map(|result| constraint_finding(result, profile_revision))
            .collect(),
        structural: report
            .structural
            .into_iter()
            .map(structural_violation)
            .collect(),
    }
}

fn constraint_finding(
    result: &ShaclValidationResult,
    profile_revision: Ulid,
) -> MetadataProfileValidationFinding {
    let component = constraint_term(&result.source_constraint_component);
    MetadataProfileValidationFinding {
        code: "constraint_violation".to_string(),
        severity: finding_severity(&result.severity.0),
        focus_node: Some(crate_local(&result.focus_node.0)),
        path: result.result_path.as_deref().map(crate_local),
        rule: component.as_ref().map_or_else(
            || result.source_constraint_component.clone(),
            |component| format!("{SH}{component}"),
        ),
        message: result.messages.first().map_or_else(
            || default_message(component.as_deref().unwrap_or_default()).to_string(),
            |message| message.text.clone(),
        ),
        profile_revision: Some(profile_revision),
        completeness: MetadataProfileValidationCompleteness::Complete,
    }
}

/// `sh:MinCountConstraintComponent` reports as the `sh:minCount` rule.
fn constraint_term(component: &str) -> Option<String> {
    let local = component
        .strip_prefix(SH)?
        .strip_suffix("ConstraintComponent")?;
    let mut characters = local.chars();
    let first = characters.next()?;
    Some(format!("{}{}", first.to_lowercase(), characters.as_str()))
}

fn finding_severity(severity: &str) -> MetadataProfileValidationSeverity {
    match decode_term(severity).strip_prefix(SH) {
        Some("Warning") => MetadataProfileValidationSeverity::Warning,
        Some("Info" | "Debug" | "Trace") => MetadataProfileValidationSeverity::Info,
        _ => MetadataProfileValidationSeverity::Violation,
    }
}

/// Craqle reports terms in N-Triples form; IRIs arrive in angle brackets.
fn decode_term(term: &str) -> String {
    term.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .unwrap_or(term)
        .to_string()
}

/// Reports the crate root as `./` so a caller can locate the entity in the
/// document it submitted rather than in the validation store.
fn crate_local(term: &str) -> String {
    let decoded = decode_term(term);
    if decoded == VALIDATION_GRAPH_IRI {
        "./".to_string()
    } else {
        decoded
    }
}

fn structural_violation(violation: CrateViolation) -> MetadataValidationViolation {
    MetadataValidationViolation {
        code: violation.code.to_string(),
        message: violation.message,
        pointer: violation.pointer,
        entity_id: violation.entity_id,
    }
}

fn single_profile_tag(
    data: &Dataset,
    root: &NamedOrBlankNode,
) -> Result<Option<String>, MetadataError> {
    let mut tags = profile_tags(data, root);
    match tags.len() {
        0 => Ok(None),
        1 => Ok(tags.pop()),
        _ => Err(MetadataError::ProfileValidation(vec![unsupported_finding(
            "multiple_profile_tags",
            "multiple root conformsTo Profile tags are not supported by the revision-bound status contract"
                .to_string(),
            Ulid::nil(),
        )])),
    }
}

fn evaluator_handle(
    context: &DriverContext,
    profile_revision: Option<Ulid>,
) -> Result<&MetadataHandle, MetadataError> {
    match context.metadata_handle.as_ref() {
        Some(metadata) if metadata.profile_validation_available() => Ok(metadata),
        _ => Err(unavailable_error(
            "validator_unavailable",
            "the profile evaluator is unavailable; retry or remove the Profile tag",
            profile_revision,
        )),
    }
}

fn shacl_failure(error: ProfileShaclError, profile_revision: Option<Ulid>) -> MetadataError {
    match error {
        ProfileShaclError::InvalidInput { message } => MetadataError::InvalidInput(message),
        other => unavailable_error(
            "validator_unavailable",
            &other.to_string(),
            profile_revision,
        ),
    }
}

fn profiled_status(
    document_id: Ulid,
    profile: &ResolvedProfile,
    findings: Vec<MetadataProfileValidationFinding>,
) -> MetadataProfileValidationStatus {
    let invalid = findings
        .iter()
        .any(|finding| finding.severity == MetadataProfileValidationSeverity::Violation);
    let completeness = if findings
        .iter()
        .any(|finding| finding.completeness == MetadataProfileValidationCompleteness::Incomplete)
    {
        MetadataProfileValidationCompleteness::Incomplete
    } else {
        MetadataProfileValidationCompleteness::Complete
    };
    MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: if invalid {
            MetadataProfileValidationState::Invalid
        } else {
            MetadataProfileValidationState::Valid
        },
        profile_id: Some(profile.id),
        profile_iri: Some(profile.requested_iri.clone()),
        profile_revision: Some(profile.revision),
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: Some(now_ms()),
        findings,
        completeness,
        stale_reason: None,
        dataset_digest: None,
    }
}

pub fn not_profiled_status(document_id: Ulid) -> MetadataProfileValidationStatus {
    MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: MetadataProfileValidationState::NotProfiled,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: Some(now_ms()),
        findings: Vec::new(),
        completeness: MetadataProfileValidationCompleteness::Complete,
        stale_reason: None,
        dataset_digest: None,
    }
}

pub fn stale_status(document_id: Ulid, reason: &str) -> MetadataProfileValidationStatus {
    MetadataProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: MetadataProfileValidationState::Stale,
        profile_id: None,
        profile_iri: None,
        profile_revision: None,
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: None,
        findings: Vec::new(),
        completeness: MetadataProfileValidationCompleteness::Incomplete,
        stale_reason: Some(reason.to_string()),
        dataset_digest: None,
    }
}

pub async fn load_validation_status(
    context: &DriverContext,
    document_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Option<MetadataProfileValidationStatus>, MetadataError> {
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Read {
            key_space: METADATA_PROFILE_VALIDATION_STATUS_KEYSPACE.to_string(),
            key: metadata_profile_validation_status_key(document_id),
            txn_id,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| {
                postcard::from_bytes(&value)
                    .map_err(aruna_core::errors::ConversionError::from)
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
            .transpose(),
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataError::Backend(format!(
            "unexpected profile validation status read: {other:?}"
        ))),
    }
}

pub async fn current_validation_status(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let Some(mut status) = load_validation_status(context, record.document_id, None).await? else {
        return Ok(stale_status(
            record.document_id,
            "validation_status_missing",
        ));
    };
    if !validation_is_current(context, record, &status).await? {
        status.state = MetadataProfileValidationState::Stale;
        status.completeness = MetadataProfileValidationCompleteness::Incomplete;
        status.stale_reason = Some("dataset_revision_changed".to_string());
        return Ok(status);
    }
    if let (Some(profile_id), Some(validated_revision)) =
        (status.profile_id, status.profile_revision)
    {
        match read_registry(context, profile_id).await {
            Ok(Some(profile)) if profile.last_event_id == validated_revision => {}
            Ok(Some(_)) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_revision_changed".to_string());
            }
            Ok(None) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_not_registered".to_string());
            }
            Err(_) => {
                status.state = MetadataProfileValidationState::Stale;
                status.completeness = MetadataProfileValidationCompleteness::Incomplete;
                status.stale_reason = Some("profile_unavailable".to_string());
            }
        }
    }
    Ok(status)
}

/// A merged document's status is bound to the render it validated, because a
/// merge can leave the displayed revision behind the newest event. A status
/// written before the first merge still carries only its event id.
async fn validation_is_current(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
    status: &MetadataProfileValidationStatus,
) -> Result<bool, MetadataError> {
    let Some(digest) = status.dataset_digest else {
        return Ok(status.dataset_revision == record.last_event_id);
    };
    let current = crate::metadata::raw::load_raw_digest(context, record.document_id)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    Ok(current == Some(digest))
}

pub async fn revalidate_current(
    context: &DriverContext,
    record: &MetadataRegistryRecord,
) -> Result<MetadataProfileValidationStatus, MetadataError> {
    let raw = load_raw_revision(context, record.document_id, None)
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?
        .ok_or(MetadataError::GraphNotFound)?;
    let Some(digest) = raw.dataset_digest else {
        return Err(MetadataError::Backend(
            "metadata raw revision has no dataset digest to fence on".to_string(),
        ));
    };
    let merged = merged_jsonld(&raw).map(str::to_owned);
    let mut status = match merged.as_deref() {
        Some(jsonld) => assess_render(context, record.document_id, record.group_id, jsonld).await,
        None => {
            assess_submission(
                context,
                record.document_id,
                ProfileScope::Group(record.group_id),
                &raw.jsonld,
            )
            .await?
        }
    };
    status.dataset_revision = raw.winning_event_id;
    status.dataset_digest = Some(digest);
    let mut owner = context
        .storage_handle
        .start_transaction(false)
        .await
        .map_err(MetadataError::Storage)?;
    let txn_id = owner.id().ok_or_else(|| {
        MetadataError::Backend("profile revalidation transaction is missing".to_string())
    })?;
    let fenced = parse_registry_read(
        context
            .storage_handle
            .send_effect(read_registry_by_document_effect(
                record.document_id,
                Some(txn_id),
            ))
            .await,
    )
    .map_err(map_registry_error)?;
    let fenced_raw = load_raw_revision(context, record.document_id, Some(txn_id))
        .await
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    let fenced_digest = fenced_raw.as_ref().and_then(|raw| raw.dataset_digest);
    let fenced_merged = fenced_raw.as_ref().and_then(merged_jsonld);
    if fenced.is_none() || fenced_digest != Some(digest) || fenced_merged != merged.as_deref() {
        let _ = context
            .storage_handle
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        owner.finish();
        return Err(MetadataError::Backend(
            "metadata revision changed during profile revalidation; retry".to_string(),
        ));
    }
    let (key_space, key, value) = metadata_profile_validation_status_write_entry(&status)
        .map_err(|error| MetadataError::Backend(error.to_string()))?;
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space,
            key,
            value,
            txn_id: Some(txn_id),
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        Event::Storage(StorageEvent::Error { error }) => return Err(error.into()),
        other => {
            return Err(MetadataError::Backend(format!(
                "unexpected profile validation status write: {other:?}"
            )));
        }
    }
    owner.unknown();
    match context
        .storage_handle
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
            owner.finish();
            Ok(status)
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error.into()),
        other => Err(MetadataError::Backend(format!(
            "unexpected profile revalidation commit: {other:?}"
        ))),
    }
}

fn merged_jsonld(raw: &MetadataRawRevision) -> Option<&str> {
    raw.merged.as_ref().map(|merged| merged.jsonld.as_str())
}

async fn resolve_registered_profile(
    context: &DriverContext,
    requested_iri: &str,
    scope: ProfileScope,
) -> Result<ResolvedProfile, MetadataError> {
    let Some(profile_id) = profile_id_from_iri(requested_iri) else {
        return Err(profile_not_registered(requested_iri));
    };
    let record = match read_registry(context, profile_id).await {
        Ok(Some(record)) => record,
        Ok(None) => return Err(profile_not_registered(requested_iri)),
        Err(_) => {
            return Err(unavailable_error(
                "profile_unavailable",
                "the registered Profile is temporarily unavailable; retry or remove the Profile tag",
                None,
            ));
        }
    };
    if record.graph_iri != MetadataRegistryRecord::graph_iri_for(profile_id)
        || !scope.may_use(&record)
    {
        return Err(profile_not_registered(requested_iri));
    }
    let revision = record.last_event_id;
    if let Some(shapes) = cached_shapes(context, profile_id, revision) {
        return Ok(ResolvedProfile {
            id: profile_id,
            requested_iri: requested_iri.to_string(),
            revision,
            shapes: shapes.as_ref().clone(),
        });
    }
    let exported = export_profile_routed(&Arc::new(context.clone()), record.realm_id, profile_id, revision)
        .await
        .map_err(|_| {
            unavailable_error(
                "profile_unavailable",
                "the registered Profile revision is temporarily unavailable; retry or remove the Profile tag",
                Some(revision),
            )
        })?;
    let ExportMetadataRoCrateResult::Raw {
        record: holder_record,
        raw,
        ..
    } = exported
    else {
        return Err(unavailable_error(
            "profile_unavailable",
            "the registered Profile revision cannot be read; retry or remove the Profile tag",
            Some(revision),
        ));
    };
    // Usability was decided from the registry row above, so the holder copy is
    // only fenced on identity and revision here.
    if holder_record.document_id != record.document_id
        || holder_record.last_event_id != revision
        || raw.revision.winning_event_id != revision
    {
        return Err(unavailable_error(
            "profile_unavailable",
            "the registered Profile revision is changing; retry or remove the Profile tag",
            Some(revision),
        ));
    }
    let raw = raw.revision;
    let (profile_data, profile_root) = data_graph(&raw.jsonld).map_err(|_| {
        unavailable_error(
            "profile_unavailable",
            "the registered Profile cannot be read; retry or remove the Profile tag",
            Some(revision),
        )
    })?;
    if !has_type(&profile_data, &profile_root, DX_PROFILE) {
        return Err(profile_not_registered(requested_iri));
    }
    let shapes = profile_shapes(&profile_data)
        .map_err(|message| unavailable_error("profile_unavailable", &message, Some(revision)))?;
    cache_shapes(context, profile_id, revision, &shapes);
    Ok(ResolvedProfile {
        id: profile_id,
        requested_iri: requested_iri.to_string(),
        revision,
        shapes,
    })
}

fn cached_shapes(
    context: &DriverContext,
    profile_id: Ulid,
    revision: Ulid,
) -> Option<Arc<Vec<String>>> {
    context
        .metadata_handle
        .as_ref()?
        .profile_cache()
        .get(profile_id, revision)
}

fn cache_shapes(context: &DriverContext, profile_id: Ulid, revision: Ulid, shapes: &[String]) {
    if let Some(metadata) = context.metadata_handle.as_ref() {
        metadata
            .profile_cache()
            .insert(profile_id, revision, Arc::new(shapes.to_vec()));
    }
}

async fn read_registry(
    context: &DriverContext,
    document_id: Ulid,
) -> Result<Option<MetadataRegistryRecord>, MetadataError> {
    parse_registry_read(
        context
            .storage_handle
            .send_effect(read_registry_by_document_effect(document_id, None))
            .await,
    )
    .map_err(map_registry_error)
}

fn map_registry_error(error: StorageReadError) -> MetadataError {
    match error {
        StorageReadError::Storage(error) => error.into(),
        StorageReadError::Conversion(error) => MetadataError::Backend(error.to_string()),
    }
}

fn profile_id_from_iri(iri: &str) -> Option<Ulid> {
    let value = iri.strip_prefix(PROFILE_PUBLIC_PREFIX)?;
    if value.is_empty() || value.contains('/') {
        return None;
    }
    let id = Ulid::from_string(value).ok()?;
    aruna_core::MetaResourceId::from_bytes(id.to_bytes()).ok()?;
    Some(id)
}

fn data_graph(jsonld: &str) -> Result<(Dataset, NamedOrBlankNode), MetadataError> {
    let canonical = craqle::canonicalize_jsonld(jsonld)
        .map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
    let mut dataset = Dataset::new();
    for quad in NQuadsParser::new().for_slice(canonical.nquads.as_bytes()) {
        let quad = quad.map_err(|error| MetadataError::InvalidInput(error.to_string()))?;
        dataset.insert(&quad);
        if !quad.graph_name.is_default_graph() {
            dataset.insert(&Quad::new(
                quad.subject,
                quad.predicate,
                quad.object,
                GraphName::DefaultGraph,
            ));
        }
    }
    let root = crate_root(&dataset).ok_or_else(|| {
        MetadataError::InvalidInput(
            "RO-Crate descriptor does not identify a root entity".to_string(),
        )
    })?;
    Ok((dataset, root))
}

fn crate_root(dataset: &Dataset) -> Option<NamedOrBlankNode> {
    for predicate in [SCHEMA_ABOUT, SCHEMA_HTTPS_ABOUT] {
        let predicate = NamedNode::new_unchecked(predicate);
        for quad in dataset.quads_for_predicate(&predicate) {
            if !quad.graph_name.is_default_graph() {
                continue;
            }
            if let Some(root) = term_as_node(quad.object.into_owned()) {
                return Some(root);
            }
        }
    }
    None
}

fn profile_tags(dataset: &Dataset, root: &NamedOrBlankNode) -> Vec<String> {
    let mut tags = BTreeSet::new();
    for predicate in [
        DCTERMS_CONFORMS_TO,
        SCHEMA_CONFORMS_TO,
        SCHEMA_HTTPS_CONFORMS_TO,
    ] {
        for object in objects(dataset, root, predicate) {
            if let Term::NamedNode(iri) = object
                && !is_rocrate_specification(iri.as_str())
            {
                tags.insert(iri.as_str().to_string());
            }
        }
    }
    tags.into_iter().collect()
}

fn profile_shapes(dataset: &Dataset) -> Result<Vec<String>, String> {
    let mut candidates = HashSet::new();
    for predicate in [SCHEMA_ENCODING_FORMAT, SCHEMA_HTTPS_ENCODING_FORMAT] {
        let predicate = NamedNode::new_unchecked(predicate);
        for quad in dataset.quads_for_predicate(&predicate) {
            if quad.graph_name.is_default_graph()
                && matches!(quad.object, oxrdf::TermRef::Literal(value) if value.value() == "text/turtle")
            {
                candidates.insert(quad.subject.into_owned());
            }
        }
    }
    let mut shapes = Vec::new();
    for candidate in candidates {
        let mut text = None;
        for predicate in [SCHEMA_TEXT, SCHEMA_HTTPS_TEXT] {
            for object in objects(dataset, &candidate, predicate) {
                if let Term::Literal(value) = object {
                    text = Some(value.value().to_string());
                    break;
                }
            }
        }
        match text {
            Some(text) => shapes.push(text),
            None => {
                return Err(
                    "the registered Profile's SHACL artifact is not locally available; retry or remove the Profile tag"
                        .to_string(),
                );
            }
        }
    }
    Ok(shapes)
}

fn objects(dataset: &Dataset, subject: &NamedOrBlankNode, predicate: &str) -> Vec<Term> {
    let predicate = NamedNode::new_unchecked(predicate);
    dataset
        .quads_for_subject(subject)
        .filter(|quad| quad.graph_name.is_default_graph() && quad.predicate == predicate.as_ref())
        .map(|quad| quad.object.into_owned())
        .collect()
}

fn has_type(dataset: &Dataset, subject: &NamedOrBlankNode, class: &str) -> bool {
    objects(dataset, subject, RDF_TYPE)
        .iter()
        .any(|term| matches!(term, Term::NamedNode(value) if value.as_str() == class))
}

fn term_as_node(term: Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(node) => Some(NamedOrBlankNode::NamedNode(node)),
        Term::BlankNode(node) => Some(NamedOrBlankNode::BlankNode(node)),
        _ => None,
    }
}

fn default_message(component: &str) -> &'static str {
    match component {
        "minCount" => "fewer values are present than the Profile requires",
        "maxCount" => "more values are present than the Profile allows",
        "datatype" => "a value has the wrong RDF datatype",
        "class" => "a value is not an instance of the required class",
        "nodeKind" => "a value has the wrong RDF node kind",
        "pattern" => "a value does not match the required pattern",
        "in" => "a value is outside the allowed set",
        "hasValue" => "the required value is missing",
        "closed" => "a closed shape contains a property that is not allowed",
        _ => "the submitted crate does not satisfy the Profile constraint",
    }
}

fn limit_finding(message: String, profile_revision: Ulid) -> MetadataProfileValidationFinding {
    MetadataProfileValidationFinding {
        code: "validation_limit".to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: "validation_limit".to_string(),
        message,
        profile_revision: Some(profile_revision),
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }
}

fn unsupported_finding(
    rule: &str,
    message: String,
    profile_revision: Ulid,
) -> MetadataProfileValidationFinding {
    MetadataProfileValidationFinding {
        code: "unsupported_constraint".to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: rule.to_string(),
        message,
        profile_revision: (profile_revision != Ulid::nil()).then_some(profile_revision),
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }
}

fn profile_not_registered(iri: &str) -> MetadataError {
    MetadataError::ProfileValidation(vec![MetadataProfileValidationFinding {
        code: "profile_not_registered".to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: DCTERMS_CONFORMS_TO.to_string(),
        message: format!("Profile `{iri}` is not registered; remove the Profile tag before saving"),
        profile_revision: None,
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }])
}

fn unavailable_error(code: &str, message: &str, revision: Option<Ulid>) -> MetadataError {
    MetadataError::ProfileValidation(vec![MetadataProfileValidationFinding {
        code: code.to_string(),
        severity: MetadataProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: code.to_string(),
        message: message.to_string(),
        profile_revision: revision,
        completeness: MetadataProfileValidationCompleteness::Incomplete,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::{BucketId, MetaResourceId, PlacementHandle, StructuredId};
    use craqle::{EncodedTerm, ShaclMessage};

    fn result(component: &str, severity: &str, path: Option<&str>) -> ShaclValidationResult {
        ShaclValidationResult {
            focus_node: EncodedTerm("<https://example.test/dataset>".to_string()),
            value: None,
            result_path: path.map(str::to_string),
            source_shape: EncodedTerm("<urn:shape>".to_string()),
            source_constraint_component: component.to_string(),
            severity: EncodedTerm(format!("<{SH}{severity}>")),
            messages: Vec::new(),
        }
    }

    #[test]
    fn selects_merged_candidate() {
        let mut raw = MetadataRawRevision {
            jsonld: "displayed".to_string(),
            winning_event_id: Ulid::nil(),
            context_digest: [0u8; 32],
            dataset_digest: Some([1u8; 32]),
            merged: None,
        };
        assert!(merged_jsonld(&raw).is_none());
        raw.merged = Some(aruna_core::metadata::MetadataMergedRevision {
            jsonld: "candidate".to_string(),
            findings: 1,
        });
        assert_eq!(merged_jsonld(&raw), Some("candidate"));
    }

    #[test]
    fn maps_component_rules() {
        assert_eq!(
            constraint_term(&format!("{SH}MinCountConstraintComponent")).as_deref(),
            Some("minCount")
        );
        assert_eq!(
            constraint_term(&format!("{SH}QualifiedMinCountConstraintComponent")).as_deref(),
            Some("qualifiedMinCount")
        );
        assert_eq!(constraint_term("urn:custom:component"), None);
    }

    #[test]
    fn decodes_term_forms() {
        assert_eq!(
            decode_term("<https://example.test/a>"),
            "https://example.test/a"
        );
        assert_eq!(decode_term("_:b0"), "_:b0");
        assert_eq!(decode_term("\"value\""), "\"value\"");
    }

    #[test]
    fn maps_shacl_severities() {
        let revision = Ulid::from_parts(1, 1);
        let finding = constraint_finding(
            &result(&format!("{SH}MinCountConstraintComponent"), "Warning", None),
            revision,
        );
        assert_eq!(finding.severity, MetadataProfileValidationSeverity::Warning);
        assert_eq!(finding.rule, format!("{SH}minCount"));
        assert_eq!(
            finding.message,
            "fewer values are present than the Profile requires"
        );
        for (severity, expected) in [
            ("Trace", MetadataProfileValidationSeverity::Info),
            ("Debug", MetadataProfileValidationSeverity::Info),
            ("Info", MetadataProfileValidationSeverity::Info),
            ("Violation", MetadataProfileValidationSeverity::Violation),
            ("Custom", MetadataProfileValidationSeverity::Violation),
        ] {
            let finding = constraint_finding(
                &result(&format!("{SH}MinCountConstraintComponent"), severity, None),
                revision,
            );
            assert_eq!(finding.severity, expected);
        }
    }

    #[test]
    fn prefers_shape_message() {
        let mut input = result(
            &format!("{SH}DatatypeConstraintComponent"),
            "Violation",
            Some("<http://schema.org/name>"),
        );
        input.messages.push(ShaclMessage {
            language: None,
            text: "name must be a string".to_string(),
        });
        let finding = constraint_finding(&input, Ulid::from_parts(1, 1));
        assert_eq!(finding.message, "name must be a string");
        assert_eq!(finding.path.as_deref(), Some("http://schema.org/name"));
        assert_eq!(
            finding.focus_node.as_deref(),
            Some("https://example.test/dataset")
        );
    }

    #[test]
    fn limits_are_incomplete() {
        let finding = limit_finding("budget exhausted".to_string(), Ulid::from_parts(1, 1));
        assert_eq!(finding.code, "validation_limit");
        assert_eq!(
            finding.completeness,
            MetadataProfileValidationCompleteness::Incomplete
        );
    }

    #[test]
    fn canonical_profile_iri() {
        let id = MetaResourceId::from_parts(
            1,
            PlacementHandle::new(1).unwrap(),
            BucketId::new(1).unwrap(),
            1,
        )
        .unwrap()
        .as_ulid();
        assert_eq!(profile_id_from_iri(&profile_public_iri(id)), Some(id));
        assert_eq!(
            profile_id_from_iri(&MetadataRegistryRecord::graph_iri_for(id)),
            None
        );
    }

    #[tokio::test]
    async fn keys_status_digest() {
        // A merge can leave the displayed revision behind the newest event, so
        // freshness follows the render's digest, not the event id.
        use aruna_core::metadata::{MetadataCreateEventPayload, MetadataCreateEventRecord};
        use aruna_core::structs::{PlacementRef, RealmId};
        use aruna_storage::FjallStorage;

        let dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage");
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let group_id = Ulid::from_parts(1, 1);
        let document_id = Ulid::from_parts(1, 2);
        let node_id = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let record = MetadataRegistryRecord {
            realm_id,
            group_id,
            document_id,
            document_path: "datasets/digest".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: MetadataRegistryRecord::permission_path_for(
                &realm_id,
                group_id,
                "datasets/digest",
                document_id,
            ),
            placement: PlacementRef::NIL,
            holder_node_ids: vec![node_id],
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 3),
            last_event_id: Ulid::from_parts(2, 3),
        };
        let event = MetadataCreateEventRecord {
            event_id: record.last_event_id,
            record: record.clone(),
            user_id: aruna_core::UserId::local(Ulid::from_parts(1, 4), realm_id),
            node_id,
            payload: MetadataCreateEventPayload::UpsertDataEntity {
                jsonld: "{}".to_string(),
            },
            occurred_at_ms: 1,
        };
        let render = serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [{ "@id": "./", "@type": "Dataset", "name": "merged" }]
        })
        .to_string();

        // A status written before the first merge still keys on its event id.
        let mut status = not_profiled_status(document_id);
        status.dataset_revision = record.last_event_id;
        assert!(
            validation_is_current(&context, &record, &status)
                .await
                .unwrap()
        );
        status.dataset_digest = Some([9u8; 32]);
        assert!(
            !validation_is_current(&context, &record, &status)
                .await
                .unwrap()
        );

        let plan = crate::metadata::raw::prepare_merged_event(
            &context,
            &event,
            render,
            0,
            &mut crate::metadata::raw::RawStateCache::default(),
        )
        .await
        .expect("merged raw state");
        let (key_space, key, value) = plan.state_write.clone();
        assert!(matches!(
            storage
                .send_storage_effect(StorageEffect::Write {
                    key_space,
                    key,
                    value,
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));

        status.dataset_digest = plan
            .revision
            .as_ref()
            .and_then(|revision| revision.dataset_digest);
        assert!(status.dataset_digest.is_some());
        assert!(
            validation_is_current(&context, &record, &status)
                .await
                .unwrap()
        );
    }

    #[test]
    fn spec_tags_ignored() {
        for version in ["1.2", "1.3"] {
            let specification = format!("https://w3id.org/ro/crate/{version}");
            let document = serde_json::json!({
                "@context": format!("{specification}/context"),
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": {"@id": specification},
                        "about": {"@id": "https://example.test/dataset"}
                    },
                    {
                        "@id": "https://example.test/dataset",
                        "@type": "Dataset",
                        "name": "Versioned crate",
                        "description": "Specification IRIs are not Profiles",
                        "datePublished": "2026-08-19",
                        "conformsTo": [
                            {"@id": specification},
                            {"@id": "https://w3id.org/ro/wfrun/process/0.5"}
                        ]
                    }
                ]
            })
            .to_string();
            let (data, root) = data_graph(&document).unwrap();
            assert!(profile_tags(&data, &root).is_empty());
        }
    }
}
