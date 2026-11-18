//! Validates documents against their Profile and records the validation verdict.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::{DOCUMENT_INDEX_KEYSPACE, VALIDATION_STATUS_KEYSPACE};
use aruna_core::metadata::{
    MetadataEffect, MetadataError, MetadataEvent, MetadataRawRevision, MetadataValidationViolation,
    ProfileValidationCompleteness, ProfileValidationFinding, ProfileValidationSeverity,
    ProfileValidationState, ProfileValidationStatus,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::profile_shapes::{
    ProfileShaclError, ProfileShaclReport, ProfileShapes, VALIDATION_GRAPH_IRI,
};
use aruna_core::rows::{done, read_row, value};
use aruna_core::storage_entries::{
    metadata_document_key, profile_validation_entry, profile_validation_key,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use craqle::ShaclValidationResult;
use oxrdf::{Dataset, NamedNode, NamedOrBlankNode, Term};
use ulid::Ulid;

use super::rules::{
    DCTERMS_CONFORMS_TO, DX_PROFILE, EVALUATOR_NAME, data_graph, has_type, not_profiled_status,
    objects, profile_tags, stale_status, submission_profile_tag,
};
use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::builtin::{BUILTIN_REVISION, builtin_shapes};
use crate::metadata::call_results::effect_event;
use crate::metadata::forward::profile_export::ProfileExportOperation;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::raw_events::{RawReadError, RawRevisionState};
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::revision_read::{RawStateOperation, RawViewOperation, RawViewOutput};
use crate::storage_read::StorageReadError;

const SH: &str = "http://www.w3.org/ns/shacl#";
const SCHEMA_ENCODING_FORMAT: &str = "http://schema.org/encodingFormat";
const HTTPS_ENCODING_FORMAT: &str = "https://schema.org/encodingFormat";
const SCHEMA_TEXT: &str = "http://schema.org/text";
const SCHEMA_HTTPS_TEXT: &str = "https://schema.org/text";
const PROFILE_PUBLIC_PREFIX: &str = "https://w3id.org/aruna/profile/";
/// Authoritative backend SHACL support for Profile validation: craqle's native
/// SHACL Core Subset v1 engine. Anything outside it (SHACL-SPARQL/JS/AF, custom
/// targets, recursion, RDF-star, remote `owl:imports`) fails closed with `unsupported_constraint`.
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

#[derive(Debug, PartialEq)]
struct ResolvedProfile {
    /// Absent for a built-in Profile, which no registry row defines.
    id: Option<Ulid>,
    requested_iri: String,
    revision: String,
    /// Graph the shapes are installed under, unique per Profile revision.
    shapes_graph_iri: String,
    shapes: Vec<String>,
}

/// Which registered Profiles a validation may resolve.
/// Usability follows the registry row alone: group-mates, then everyone once
/// public. The caller already proved WRITE or READ, so identity takes no part.
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

pub(crate) fn equivalent_profile_iris(iri: &str) -> Vec<String> {
    profile_from_iri(iri).map_or_else(
        || vec![iri.to_string()],
        |profile_id| vec![profile_public_iri(profile_id)],
    )
}

fn untagged_without_evaluator(error: &MetadataError, jsonld: &str) -> bool {
    matches!(
        error,
        MetadataError::ProfileValidation(findings)
            if findings
                .iter()
                .any(|finding| finding.code == "validator_unavailable")
    ) && !submission_profile_tag(jsonld)
}

/// A write refuses exactly what the preview reports: structural violations
/// first, since a malformed crate makes the Profile findings secondary.
fn write_verdict(
    verdict: MetadataProfilePreview,
) -> Result<ProfileValidationStatus, MetadataError> {
    if !verdict.structural_violations.is_empty() {
        return Err(MetadataError::Validation(verdict.structural_violations));
    }
    if verdict.status.state == ProfileValidationState::Invalid {
        return Err(MetadataError::ProfileValidation(verdict.status.findings));
    }
    Ok(verdict.status)
}

/// Findings that make a document invalid, and so keep the render undisplayed.
pub(crate) fn violation_count(status: &ProfileValidationStatus) -> u32 {
    if status.state != ProfileValidationState::Invalid {
        return 0;
    }
    u32::try_from(
        status
            .findings
            .iter()
            .filter(|finding| finding.severity == ProfileValidationSeverity::Violation)
            .count(),
    )
    .unwrap_or(u32::MAX)
}

/// The verdict a create or replace would enforce for an unsaved draft.
#[derive(Debug, PartialEq)]
pub struct MetadataProfilePreview {
    pub status: ProfileValidationStatus,
    pub structural_violations: Vec<MetadataValidationViolation>,
}

impl MetadataProfilePreview {
    pub fn accepted(&self) -> bool {
        self.structural_violations.is_empty()
            && self.status.state != ProfileValidationState::Invalid
    }
}

struct ProfileAssessment {
    findings: Vec<ProfileValidationFinding>,
    structural: Vec<MetadataValidationViolation>,
}

fn shacl_assessment(report: ProfileShaclReport, profile_revision: &str) -> ProfileAssessment {
    ProfileAssessment {
        findings: report
            .results
            .iter()
            .map(|result| constraint_finding(result, profile_revision))
            .collect(),
        structural: report
            .structural
            .into_iter()
            .map(MetadataValidationViolation::from)
            .collect(),
    }
}

fn constraint_finding(
    result: &ShaclValidationResult,
    profile_revision: &str,
) -> ProfileValidationFinding {
    let component = constraint_term(&result.source_constraint_component);
    ProfileValidationFinding {
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
        profile_revision: Some(profile_revision.to_string()),
        completeness: ProfileValidationCompleteness::Complete,
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

fn finding_severity(severity: &str) -> ProfileValidationSeverity {
    match decode_term(severity).strip_prefix(SH) {
        Some("Warning") => ProfileValidationSeverity::Warning,
        Some("Info" | "Debug" | "Trace") => ProfileValidationSeverity::Info,
        _ => ProfileValidationSeverity::Violation,
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
            None,
        )])),
    }
}

fn shacl_failure(error: ProfileShaclError, profile_revision: Option<&str>) -> MetadataError {
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
    findings: Vec<ProfileValidationFinding>,
    validated_at_ms: u64,
) -> ProfileValidationStatus {
    let invalid = findings
        .iter()
        .any(|finding| finding.severity == ProfileValidationSeverity::Violation);
    let completeness = if findings
        .iter()
        .any(|finding| finding.completeness == ProfileValidationCompleteness::Incomplete)
    {
        ProfileValidationCompleteness::Incomplete
    } else {
        ProfileValidationCompleteness::Complete
    };
    ProfileValidationStatus {
        document_id,
        dataset_revision: Ulid::nil(),
        state: if invalid {
            ProfileValidationState::Invalid
        } else {
            ProfileValidationState::Valid
        },
        profile_id: profile.id,
        profile_iri: Some(profile.requested_iri.clone()),
        profile_revision: Some(profile.revision.clone()),
        evaluator: EVALUATOR_NAME.to_string(),
        validated_at_ms: Some(validated_at_ms),
        findings,
        completeness,
        stale_reason: None,
        dataset_digest: None,
    }
}

/// The verdict a create or replace enforces. An untagged crate never depended on the evaluator,
/// so a node without one keeps accepting it instead of refusing every write.
pub(crate) fn submission_verdict(
    assessed: Result<MetadataProfilePreview, MetadataError>,
    document_id: Ulid,
    jsonld: &str,
) -> Result<ProfileValidationStatus, MetadataError> {
    match assessed {
        Ok(verdict) => write_verdict(verdict),
        Err(error) if untagged_without_evaluator(&error, jsonld) => {
            Ok(not_profiled_status(document_id))
        }
        Err(error) => Err(error),
    }
}

/// Profile verdict for a merged render. A merge is never rejected, so an evaluator that cannot
/// answer reports stale rather than failing the job.
pub(crate) fn render_verdict(
    assessed: Result<MetadataProfilePreview, MetadataError>,
    document_id: Ulid,
) -> ProfileValidationStatus {
    match assessed {
        Ok(preview) => preview.status,
        Err(MetadataError::ProfileValidation(findings)) => {
            let mut status = stale_status(document_id, "profile_unavailable");
            status.findings = findings;
            status
        }
        Err(error) => stale_status(document_id, &error.to_string()),
    }
}

/// The status of an explicit Profile check; structural crate violations are findings too.
pub fn profile_verdict(preview: MetadataProfilePreview) -> ProfileValidationStatus {
    let mut status = preview.status;
    if !preview.structural_violations.is_empty() {
        status.state = ProfileValidationState::Invalid;
    }
    status
        .findings
        .extend(preview.structural_violations.into_iter().map(|violation| {
            ProfileValidationFinding {
                code: violation.code,
                severity: ProfileValidationSeverity::Violation,
                focus_node: Some(violation.entity_id.unwrap_or_else(|| "./".into())),
                path: Some(violation.pointer).filter(|pointer| !pointer.is_empty()),
                rule: "structural".into(),
                message: violation.message,
                profile_revision: status.profile_revision.clone(),
                completeness: ProfileValidationCompleteness::Complete,
            }
        }));
    status
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum AssessStage {
    /// Running the structural pass of an untagged crate.
    Structure,
    /// Reading the registry row of a registered Profile.
    Registry,
    /// Looking for the Profile revision's shapes in this node's cache.
    Cached,
    /// Fetching the Profile revision from a holder.
    Export,
    /// Keeping the fetched shapes.
    Store,
    /// Evaluating the crate against the shapes.
    Evaluate,
}

/// Assesses one crate against the Profile it names, or a given Profile: the Profile findings,
/// plus the structural findings a write enforces. Built-in Profiles answer from embedded
/// shapes; a registered one is fetched from a holder once per revision and cached.
#[derive(Debug, PartialEq)]
pub struct ProfileAssessOperation {
    local_node: Option<NodeId>,
    document_id: Ulid,
    scope: ProfileScope,
    jsonld: String,
    /// The Profile the crate is checked against even when it names none.
    forced: Option<String>,
    /// Whether an untagged crate gets the bare structural pass a write enforces.
    structural: bool,
    stage: AssessStage,
    requested_iri: String,
    registered: Option<MetadataRegistryRecord>,
    profile: Option<ResolvedProfile>,
    now_ms: u64,
}

type AssessTurn = Result<Next<MetadataProfilePreview>, MetadataError>;

impl ProfileAssessOperation {
    fn new(
        local_node: Option<NodeId>,
        document_id: Ulid,
        scope: ProfileScope,
        jsonld: String,
        structural: bool,
    ) -> Self {
        Self {
            local_node,
            document_id,
            scope,
            jsonld,
            forced: None,
            structural,
            stage: AssessStage::Evaluate,
            requested_iri: String::new(),
            registered: None,
            profile: None,
            now_ms: 0,
        }
    }

    /// The verdict a create or replace of `document_id` in `group_id` enforces.
    pub fn write(
        local_node: Option<NodeId>,
        document_id: Ulid,
        group_id: GroupId,
        jsonld: String,
    ) -> Self {
        Self::new(
            local_node,
            document_id,
            ProfileScope::Group(group_id),
            jsonld,
            true,
        )
    }

    /// The verdict a write would enforce for an unsaved draft. Without a group only public
    /// Profiles resolve, exactly as a foreign group's write would.
    pub fn preview(local_node: Option<NodeId>, group_id: Option<GroupId>, jsonld: String) -> Self {
        Self::new(local_node, Ulid::nil(), group_id.into(), jsonld, true)
    }

    /// The Profile verdict of a merged render, which skips the structural findings.
    pub(crate) fn render(
        local_node: Option<NodeId>,
        document_id: Ulid,
        group_id: GroupId,
        jsonld: String,
    ) -> Self {
        Self::new(
            local_node,
            document_id,
            ProfileScope::Group(group_id),
            jsonld,
            false,
        )
    }

    /// Checks a crate against the Profile `iri` whether or not the crate names it. Built-in and
    /// public Profiles resolve; nothing is stored.
    pub fn against(local_node: Option<NodeId>, iri: &str, jsonld: String) -> Self {
        let mut operation = Self::new(
            local_node,
            Ulid::nil(),
            ProfileScope::PublicOnly,
            jsonld,
            true,
        );
        operation.forced = Some(iri.to_string());
        operation
    }

    fn resolve(&mut self, requested_iri: String) -> AssessTurn {
        if let Some(shapes) = builtin_shapes(&requested_iri) {
            let profile = ResolvedProfile {
                id: None,
                requested_iri: requested_iri.clone(),
                revision: BUILTIN_REVISION.to_string(),
                shapes_graph_iri: format!("{requested_iri}#shapes/{BUILTIN_REVISION}"),
                shapes: shapes.iter().map(|shapes| shapes.to_string()).collect(),
            };
            return self.evaluate(profile);
        }
        let Some(profile_id) = profile_from_iri(&requested_iri) else {
            return Err(profile_not_registered(&requested_iri));
        };
        self.requested_iri = requested_iri;
        self.stage = AssessStage::Registry;
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            profile_id,
        ))))
    }

    fn registry_read(&mut self, result: IoResult) -> AssessTurn {
        let record = match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
            Ok(Some(record)) => record,
            Ok(None) => return Err(profile_not_registered(&self.requested_iri)),
            Err(_) => {
                return Err(unavailable_error(
                    "profile_unavailable",
                    "the registered Profile is temporarily unavailable; retry or remove the Profile tag",
                    None,
                ));
            }
        };
        if record.graph_iri != MetadataRegistryRecord::graph_iri_for(record.document_id)
            || !self.scope.may_use(&record)
        {
            return Err(profile_not_registered(&self.requested_iri));
        }
        let cached = MetadataEffect::CachedShapes {
            profile_id: record.document_id,
            revision: record.last_event_id,
        };
        self.registered = Some(record);
        self.stage = AssessStage::Cached;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            cached,
        )))))
    }

    fn registered(&self) -> Result<&MetadataRegistryRecord, MetadataError> {
        self.registered
            .as_ref()
            .ok_or_else(|| MetadataError::Backend("the Profile row is missing".to_string()))
    }

    fn registered_profile(&self, shapes: Vec<String>) -> Result<ResolvedProfile, MetadataError> {
        let record = self.registered()?;
        let revision = record.last_event_id;
        Ok(ResolvedProfile {
            id: Some(record.document_id),
            requested_iri: self.requested_iri.clone(),
            revision: revision.to_string(),
            shapes_graph_iri: format!("{}#shapes/{revision}", record.graph_iri),
            shapes,
        })
    }

    fn cached_read(&mut self, result: IoResult) -> AssessTurn {
        // A node without the graph store keeps no shapes and fetches every time.
        if let Event::Metadata(MetadataEvent::ProfileShapes {
            shapes: Some(shapes),
        }) = effect_event(result)?
        {
            let profile = self.registered_profile(shapes)?;
            return self.evaluate(profile);
        }
        let record = self.registered()?;
        let export = ProfileExportOperation::new(
            self.local_node,
            record.realm_id,
            record.document_id,
            record.last_event_id,
        );
        self.stage = AssessStage::Export;
        Ok(Next::Call(IoCall::child(export)))
    }

    fn export_read(&mut self, result: IoResult) -> AssessTurn {
        let record = self.registered()?.clone();
        let revision = record.last_event_id;
        let pinned = revision.to_string();
        let exported = result
            .child::<ExportMetadataResult, MetadataReadError>()?
            .map_err(|_| {
                unavailable_error(
                    "profile_unavailable",
                    "the registered Profile revision is temporarily unavailable; retry or remove the Profile tag",
                    Some(&pinned),
                )
            })?;
        let ExportMetadataResult::Raw {
            record: holder_record,
            raw,
            ..
        } = exported
        else {
            return Err(unavailable_error(
                "profile_unavailable",
                "the registered Profile revision cannot be read; retry or remove the Profile tag",
                Some(&pinned),
            ));
        };
        // Usability was decided from the registry row, so the holder copy is only fenced on
        // identity and revision here.
        if holder_record.document_id != record.document_id
            || holder_record.last_event_id != revision
            || raw.revision.winning_event_id != revision
        {
            return Err(unavailable_error(
                "profile_unavailable",
                "the registered Profile revision is changing; retry or remove the Profile tag",
                Some(&pinned),
            ));
        }
        let (profile_data, profile_root) = data_graph(&raw.revision.jsonld).map_err(|_| {
            unavailable_error(
                "profile_unavailable",
                "the registered Profile cannot be read; retry or remove the Profile tag",
                Some(&pinned),
            )
        })?;
        if !has_type(&profile_data, &profile_root, DX_PROFILE) {
            return Err(profile_not_registered(&self.requested_iri));
        }
        let shapes = profile_shapes(&profile_data)
            .map_err(|message| unavailable_error("profile_unavailable", &message, Some(&pinned)))?;
        self.profile = Some(self.registered_profile(shapes.clone())?);
        self.stage = AssessStage::Store;
        let store = MetadataEffect::CacheShapes {
            profile_id: record.document_id,
            revision,
            shapes,
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            store,
        )))))
    }

    fn evaluate(&mut self, profile: ResolvedProfile) -> AssessTurn {
        let check = MetadataEffect::CheckProfile {
            shapes: ProfileShapes {
                graph_iri: profile.shapes_graph_iri.clone(),
                sources: profile.shapes.clone(),
            },
            jsonld: self.jsonld.clone(),
        };
        self.profile = Some(profile);
        self.stage = AssessStage::Evaluate;
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            check,
        )))))
    }

    fn evaluated(&mut self, result: IoResult) -> AssessTurn {
        let profile = self
            .profile
            .take()
            .ok_or_else(|| MetadataError::Backend("the Profile is missing".to_string()))?;
        let assessment = match checked(effect_event(result)?, Some(&profile.revision))? {
            Ok(report) => shacl_assessment(report, &profile.revision),
            Err(ProfileShaclError::Unsupported { rule, message }) => ProfileAssessment {
                findings: vec![unsupported_finding(&rule, message, Some(&profile.revision))],
                structural: Vec::new(),
            },
            Err(ProfileShaclError::Limit { message }) => ProfileAssessment {
                findings: vec![limit_finding(message, &profile.revision)],
                structural: Vec::new(),
            },
            Err(error) => return Err(shacl_failure(error, Some(&profile.revision))),
        };
        Ok(Next::Done(MetadataProfilePreview {
            status: profiled_status(self.document_id, &profile, assessment.findings, self.now_ms),
            structural_violations: assessment.structural,
        }))
    }

    fn structure_checked(&mut self, result: IoResult) -> AssessTurn {
        let report =
            checked(effect_event(result)?, None)?.map_err(|error| shacl_failure(error, None))?;
        Ok(Next::Done(MetadataProfilePreview {
            status: not_profiled_status(self.document_id),
            structural_violations: report
                .structural
                .into_iter()
                .map(MetadataValidationViolation::from)
                .collect(),
        }))
    }
}

/// The report of a structure or Profile check. A node without the graph store has no evaluator.
fn checked(
    event: Event,
    profile_revision: Option<&str>,
) -> Result<Result<ProfileShaclReport, ProfileShaclError>, MetadataError> {
    match event {
        Event::Metadata(MetadataEvent::ProfileChecked { report }) => {
            Ok(report.map(|report| *report))
        }
        Event::Metadata(MetadataEvent::Error { .. }) => Err(unavailable_error(
            "validator_unavailable",
            "the profile evaluator is unavailable; retry or remove the Profile tag",
            profile_revision,
        )),
        other => Err(MetadataError::Backend(format!(
            "unexpected profile check answer: {}",
            other.label()
        ))),
    }
}

impl Operation for ProfileAssessOperation {
    type Output = MetadataProfilePreview;
    type Error = MetadataError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> AssessTurn {
        if let Some(iri) = self.forced.clone() {
            return self.resolve(iri);
        }
        let (data, root) = data_graph(&self.jsonld)?;
        if let Some(requested_iri) = single_profile_tag(&data, &root)? {
            return self.resolve(requested_iri);
        }
        if !self.structural {
            return Ok(Next::Done(MetadataProfilePreview {
                status: not_profiled_status(self.document_id),
                structural_violations: Vec::new(),
            }));
        }
        self.stage = AssessStage::Structure;
        let check = MetadataEffect::CheckStructure {
            jsonld: self.jsonld.clone(),
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            check,
        )))))
    }

    fn next(&mut self, result: IoResult) -> AssessTurn {
        match self.stage {
            AssessStage::Structure => self.structure_checked(result),
            AssessStage::Registry => self.registry_read(result),
            AssessStage::Cached => self.cached_read(result),
            AssessStage::Export => self.export_read(result),
            AssessStage::Store => {
                effect_event(result)?;
                let profile = self
                    .profile
                    .take()
                    .ok_or_else(|| MetadataError::Backend("the Profile is missing".to_string()))?;
                self.evaluate(profile)
            }
            AssessStage::Evaluate => self.evaluated(result),
        }
    }
}

/// Reads the stored validation status of one document.
#[derive(Debug, PartialEq)]
pub struct StoredStatusOperation {
    document_id: Ulid,
}

impl StoredStatusOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self { document_id }
    }
}

impl Operation for StoredStatusOperation {
    type Output = Option<ProfileValidationStatus>;
    type Error = MetadataError;

    fn first(&mut self) -> Result<Next<Self::Output>, MetadataError> {
        let key = profile_validation_key(self.document_id);
        Ok(Next::Call(read_row(VALIDATION_STATUS_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, MetadataError> {
        let status = value(result)?
            .map(|value| {
                postcard::from_bytes(&value)
                    .map_err(aruna_core::errors::ConversionError::from)
                    .map_err(|error| MetadataError::Backend(error.to_string()))
            })
            .transpose()?;
        Ok(Next::Done(status))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StatusStage {
    Stored,
    Digest,
    Profile,
}

/// The document's validation status, marked stale when the document moved on since, or when its
/// registered Profile changed or is gone. A built-in Profile carries no id, so nothing ages it.
#[derive(Debug, PartialEq)]
pub struct ValidationStatusOperation {
    record: MetadataRegistryRecord,
    stage: StatusStage,
    status: Option<ProfileValidationStatus>,
}

type StatusTurn = Result<Next<ProfileValidationStatus>, MetadataError>;

impl ValidationStatusOperation {
    pub fn new(record: MetadataRegistryRecord) -> Self {
        Self {
            record,
            stage: StatusStage::Stored,
            status: None,
        }
    }

    fn take_status(&mut self) -> Result<ProfileValidationStatus, MetadataError> {
        self.status
            .take()
            .ok_or_else(|| MetadataError::Backend("the validation status is missing".to_string()))
    }

    /// A merged document's status is bound to the render it validated, because a merge can
    /// leave the displayed revision behind the newest event. A status written before the first
    /// merge still carries only its event id.
    fn stored_read(&mut self, result: IoResult) -> StatusTurn {
        let stored = result.child::<Option<ProfileValidationStatus>, MetadataError>()??;
        let Some(status) = stored else {
            return Ok(Next::Done(stale_status(
                self.record.document_id,
                "validation_status_missing",
            )));
        };
        let has_digest = status.dataset_digest.is_some();
        let current = status.dataset_revision == self.record.last_event_id;
        self.status = Some(status);
        if has_digest {
            self.stage = StatusStage::Digest;
            return Ok(Next::Call(IoCall::child(RawStateOperation::new(
                self.record.document_id,
            ))));
        }
        self.after_current(current)
    }

    fn digest_read(&mut self, result: IoResult) -> StatusTurn {
        let state = result
            .child::<Option<RawRevisionState>, RawReadError>()?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let digest = state
            .and_then(|state| state.revision)
            .and_then(|revision| revision.dataset_digest);
        let current = self.status.as_ref().is_some_and(|status| {
            status.dataset_digest.is_some() && status.dataset_digest == digest
        });
        self.after_current(current)
    }

    fn after_current(&mut self, current: bool) -> StatusTurn {
        let mut status = self.take_status()?;
        if !current {
            mark_stale(&mut status, "dataset_revision_changed");
            return Ok(Next::Done(status));
        }
        let Some(profile_id) = status
            .profile_id
            .filter(|_| status.profile_revision.is_some())
        else {
            return Ok(Next::Done(status));
        };
        self.status = Some(status);
        self.stage = StatusStage::Profile;
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            profile_id,
        ))))
    }

    fn profile_read(&mut self, result: IoResult) -> StatusTurn {
        let mut status = self.take_status()?;
        let validated = status.profile_revision.clone().unwrap_or_default();
        match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
            Ok(Some(profile)) if profile.last_event_id.to_string() == validated => {}
            Ok(Some(_)) => mark_stale(&mut status, "profile_revision_changed"),
            Ok(None) => mark_stale(&mut status, "profile_not_registered"),
            Err(_) => mark_stale(&mut status, "profile_unavailable"),
        }
        Ok(Next::Done(status))
    }
}

fn mark_stale(status: &mut ProfileValidationStatus, reason: &str) {
    status.state = ProfileValidationState::Stale;
    status.completeness = ProfileValidationCompleteness::Incomplete;
    status.stale_reason = Some(reason.to_string());
}

impl Operation for ValidationStatusOperation {
    type Output = ProfileValidationStatus;
    type Error = MetadataError;

    fn first(&mut self) -> StatusTurn {
        Ok(Next::Call(IoCall::child(StoredStatusOperation::new(
            self.record.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match self.stage {
            StatusStage::Stored => self.stored_read(result),
            StatusStage::Digest => self.digest_read(result),
            StatusStage::Profile => self.profile_read(result),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RevalidateStage {
    Raw,
    Assess,
    Commit,
}

/// Validates the document's displayed revision again and stores the verdict, fenced on that
/// revision: a document that moved on meanwhile is refused for a retry.
#[derive(Debug, PartialEq)]
pub struct RevalidateOperation {
    local_node: Option<NodeId>,
    record: MetadataRegistryRecord,
    stage: RevalidateStage,
    raw: Option<MetadataRawRevision>,
}

impl RevalidateOperation {
    pub fn new(local_node: Option<NodeId>, record: MetadataRegistryRecord) -> Self {
        Self {
            local_node,
            record,
            stage: RevalidateStage::Raw,
            raw: None,
        }
    }

    fn raw_read(&mut self, result: IoResult) -> StatusTurn {
        let (_, raw) = result
            .child::<RawViewOutput, RawReadError>()?
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        let raw = raw.ok_or(MetadataError::GraphNotFound)?;
        if raw.dataset_digest.is_none() {
            return Err(MetadataError::Backend(
                "metadata raw revision has no dataset digest to fence on".to_string(),
            ));
        }
        let assess = match merged_jsonld(&raw) {
            Some(jsonld) => ProfileAssessOperation::render(
                self.local_node,
                self.record.document_id,
                self.record.group_id,
                jsonld.to_string(),
            ),
            None => ProfileAssessOperation::new(
                self.local_node,
                self.record.document_id,
                ProfileScope::Group(self.record.group_id),
                raw.jsonld.clone(),
                false,
            ),
        };
        self.raw = Some(raw);
        self.stage = RevalidateStage::Assess;
        Ok(Next::Call(IoCall::child(assess)))
    }

    fn assessed(&mut self, result: IoResult) -> StatusTurn {
        let raw = self
            .raw
            .take()
            .ok_or_else(|| MetadataError::Backend("the raw revision is missing".to_string()))?;
        let assessed = result.child::<MetadataProfilePreview, MetadataError>()?;
        let mut status = match merged_jsonld(&raw) {
            Some(_) => render_verdict(assessed, self.record.document_id),
            None => assessed?.status,
        };
        status.dataset_revision = raw.winning_event_id;
        status.dataset_digest = raw.dataset_digest;
        let commit = RevalidateCommitOperation {
            document_id: self.record.document_id,
            merged: merged_jsonld(&raw).map(str::to_owned),
            digest: raw.dataset_digest,
            status,
            stage: CommitStage::Fence,
        };
        self.stage = RevalidateStage::Commit;
        Ok(Next::Call(IoCall::child(commit)))
    }
}

impl Operation for RevalidateOperation {
    type Output = ProfileValidationStatus;
    type Error = MetadataError;

    fn first(&mut self) -> StatusTurn {
        Ok(Next::Call(IoCall::child(RawViewOperation::revision(
            self.record.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match self.stage {
            RevalidateStage::Raw => self.raw_read(result),
            RevalidateStage::Assess => self.assessed(result),
            RevalidateStage::Commit => Ok(Next::Done(
                result.child::<ProfileValidationStatus, MetadataError>()??,
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
enum CommitStage {
    Fence,
    /// Reading the raw revision again inside the transaction, the way the first read did.
    Raw(RawViewOperation),
    Write,
}

/// Stores a revalidated status in one write transaction, after re-reading the registry row and
/// the raw revision it was computed for.
#[derive(Debug, PartialEq)]
pub struct RevalidateCommitOperation {
    document_id: Ulid,
    merged: Option<String>,
    digest: Option<[u8; 32]>,
    status: ProfileValidationStatus,
    stage: CommitStage,
}

impl RevalidateCommitOperation {
    fn raw_turn(&mut self, turn: Next<RawViewOutput>) -> StatusTurn {
        let raw = match turn {
            Next::Call(call) => return Ok(Next::Call(call)),
            Next::Wait => return Ok(Next::Wait),
            Next::Done((_, raw))
            | Next::Then {
                output: (_, raw), ..
            } => raw,
        };
        let digest = raw.as_ref().and_then(|raw| raw.dataset_digest);
        let merged = raw.as_ref().and_then(merged_jsonld);
        if digest != self.digest || merged != self.merged.as_deref() {
            return Err(revision_changed());
        }
        let row = profile_validation_entry(&self.status)
            .map_err(|error| MetadataError::Backend(error.to_string()))?;
        self.stage = CommitStage::Write;
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for RevalidateCommitOperation {
    type Output = ProfileValidationStatus;
    type Error = MetadataError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> StatusTurn {
        let key = metadata_document_key(self.document_id);
        Ok(Next::Call(read_row(DOCUMENT_INDEX_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match &mut self.stage {
            CommitStage::Fence => {
                if value(result)?.is_none() {
                    return Err(revision_changed());
                }
                let mut view = RawViewOperation::revision(self.document_id);
                let turn = view
                    .first()
                    .map_err(|error| MetadataError::Backend(error.to_string()))?;
                self.stage = CommitStage::Raw(view);
                self.raw_turn(turn)
            }
            CommitStage::Raw(view) => {
                let turn = view
                    .next(result)
                    .map_err(|error| MetadataError::Backend(error.to_string()))?;
                self.raw_turn(turn)
            }
            CommitStage::Write => {
                done(result)?;
                Ok(Next::Done(self.status.clone()))
            }
        }
    }
}

fn revision_changed() -> MetadataError {
    MetadataError::Backend(
        "metadata revision changed during profile revalidation; retry".to_string(),
    )
}

fn merged_jsonld(raw: &MetadataRawRevision) -> Option<&str> {
    raw.merged.as_ref().map(|merged| merged.jsonld.as_str())
}

fn profile_from_iri(iri: &str) -> Option<Ulid> {
    let value = iri.strip_prefix(PROFILE_PUBLIC_PREFIX)?;
    if value.is_empty() || value.contains('/') {
        return None;
    }
    let id = Ulid::from_string(value).ok()?;
    aruna_core::MetaResourceId::from_bytes(id.to_bytes()).ok()?;
    Some(id)
}

fn profile_shapes(dataset: &Dataset) -> Result<Vec<String>, String> {
    let mut candidates = HashSet::new();
    for predicate in [SCHEMA_ENCODING_FORMAT, HTTPS_ENCODING_FORMAT] {
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

fn limit_finding(message: String, profile_revision: &str) -> ProfileValidationFinding {
    ProfileValidationFinding {
        code: "validation_limit".to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: "validation_limit".to_string(),
        message,
        profile_revision: Some(profile_revision.to_string()),
        completeness: ProfileValidationCompleteness::Incomplete,
    }
}

fn unsupported_finding(
    rule: &str,
    message: String,
    profile_revision: Option<&str>,
) -> ProfileValidationFinding {
    ProfileValidationFinding {
        code: "unsupported_constraint".to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node: None,
        path: None,
        rule: rule.to_string(),
        message,
        profile_revision: profile_revision.map(str::to_string),
        completeness: ProfileValidationCompleteness::Incomplete,
    }
}

fn profile_not_registered(iri: &str) -> MetadataError {
    MetadataError::ProfileValidation(vec![ProfileValidationFinding {
        code: "profile_not_registered".to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: DCTERMS_CONFORMS_TO.to_string(),
        message: format!("Profile `{iri}` is not registered; remove the Profile tag before saving"),
        profile_revision: None,
        completeness: ProfileValidationCompleteness::Incomplete,
    }])
}

fn unavailable_error(code: &str, message: &str, revision: Option<&str>) -> MetadataError {
    MetadataError::ProfileValidation(vec![ProfileValidationFinding {
        code: code.to_string(),
        severity: ProfileValidationSeverity::Violation,
        focus_node: None,
        path: Some(DCTERMS_CONFORMS_TO.to_string()),
        rule: code.to_string(),
        message: message.to_string(),
        profile_revision: revision.map(str::to_string),
        completeness: ProfileValidationCompleteness::Incomplete,
    }])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::profile::rules::crate_is_profile;
    use aruna_core::metadata::CRATE_PROFILE_IRI;
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

    fn violation() -> MetadataValidationViolation {
        MetadataValidationViolation {
            code: "incomplete_root".to_string(),
            message: "the root data entity needs a description".to_string(),
            pointer: "/@graph/1".to_string(),
            entity_id: Some("./".to_string()),
        }
    }

    #[test]
    fn refuses_untagged_structure() {
        // The preview reports these for an untagged crate; the write must too.
        let error = write_verdict(MetadataProfilePreview {
            status: not_profiled_status(Ulid::nil()),
            structural_violations: vec![violation()],
        })
        .expect_err("a structural violation must refuse the write");
        let MetadataError::Validation(violations) = error else {
            panic!("expected structural violations");
        };
        assert_eq!(violations, vec![violation()]);
    }

    #[test]
    fn refuses_profiled_structure() {
        // A Profile-scoped evaluation reports structure alongside its findings.
        let mut status = not_profiled_status(Ulid::nil());
        status.state = ProfileValidationState::Valid;
        let error = write_verdict(MetadataProfilePreview {
            status,
            structural_violations: vec![violation()],
        })
        .expect_err("a valid Profile verdict must not hide broken structure");
        assert!(matches!(error, MetadataError::Validation(_)));
    }

    #[test]
    fn refuses_invalid_profile() {
        let mut status = not_profiled_status(Ulid::nil());
        status.state = ProfileValidationState::Invalid;
        status.findings = vec![limit_finding("budget".to_string(), "01REV")];
        let error = write_verdict(MetadataProfilePreview {
            status,
            structural_violations: Vec::new(),
        })
        .expect_err("an invalid Profile verdict must refuse the write");
        assert!(matches!(error, MetadataError::ProfileValidation(_)));
        let accepted = write_verdict(MetadataProfilePreview {
            status: not_profiled_status(Ulid::nil()),
            structural_violations: Vec::new(),
        })
        .expect("a clean verdict is accepted");
        assert_eq!(accepted.state, ProfileValidationState::NotProfiled);
    }

    fn crate_with_tag(tag: Option<&str>) -> String {
        let mut root = serde_json::json!({
            "@id": "https://example.test/dataset",
            "@type": "Dataset",
            "name": "Fixture"
        });
        if let Some(tag) = tag {
            root["conformsTo"] = serde_json::json!({"@id": tag});
        }
        serde_json::json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
                {
                    "@id": "ro-crate-metadata.json",
                    "@type": "CreativeWork",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                    "about": {"@id": "https://example.test/dataset"}
                },
                root
            ]
        })
        .to_string()
    }

    #[test]
    fn root_from_descriptor() {
        // An `about` of another entity never picks the root, so a Profile root stays a Profile.
        let mut graph = vec![
            serde_json::json!({
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
                "about": {"@id": "https://example.test/profile"}
            }),
            serde_json::json!({"@id": "https://example.test/profile", "@type": DX_PROFILE}),
        ];
        for index in 0..16 {
            let note = format!("https://example.test/note/{index}");
            let other = format!("https://example.test/other/{index}");
            graph.push(serde_json::json!({"@id": note, "about": {"@id": other.clone()}}));
            graph.push(serde_json::json!({"@id": other, "@type": "Thing"}));
        }
        let context = "https://w3id.org/ro/crate/1.2/context";
        let jsonld = serde_json::json!({"@context": context, "@graph": graph}).to_string();
        assert!(crate_is_profile(&jsonld));
        // A subject whose name only ends like the descriptor's names no root either.
        let mut graph =
            vec![serde_json::json!({"@id": "https://example.test/profile", "@type": DX_PROFILE})];
        for index in 0..16 {
            let other = format!("https://example.test/other/{index}");
            graph.push(serde_json::json!({
                "@id": format!("note-{index}-ro-crate-metadata.json"),
                "about": {"@id": other.clone()}
            }));
            graph.push(serde_json::json!({"@id": other, "@type": "Thing"}));
        }
        graph.push(serde_json::json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "about": {"@id": "https://example.test/profile"}
        }));
        let jsonld = serde_json::json!({"@context": context, "@graph": graph}).to_string();
        assert!(crate_is_profile(&jsonld));
    }

    #[test]
    fn keeps_untagged_writable() {
        // A node without an evaluator must still accept an untagged crate.
        let untagged = crate_with_tag(None);
        let tagged = crate_with_tag(Some(
            "https://w3id.org/aruna/profile/01J00000000000000000000000",
        ));
        let unavailable = unavailable_error("validator_unavailable", "no evaluator", None);
        assert!(untagged_without_evaluator(&unavailable, &untagged));
        assert!(!untagged_without_evaluator(&unavailable, &tagged));
        assert!(!untagged_without_evaluator(
            &MetadataError::InvalidInput("broken".to_string()),
            &untagged
        ));
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
        let revision = "01REV";
        let finding = constraint_finding(
            &result(&format!("{SH}MinCountConstraintComponent"), "Warning", None),
            revision,
        );
        assert_eq!(finding.severity, ProfileValidationSeverity::Warning);
        assert_eq!(finding.rule, format!("{SH}minCount"));
        assert_eq!(
            finding.message,
            "fewer values are present than the Profile requires"
        );
        for (severity, expected) in [
            ("Trace", ProfileValidationSeverity::Info),
            ("Debug", ProfileValidationSeverity::Info),
            ("Info", ProfileValidationSeverity::Info),
            ("Violation", ProfileValidationSeverity::Violation),
            ("Custom", ProfileValidationSeverity::Violation),
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
        let finding = constraint_finding(&input, "01REV");
        assert_eq!(finding.message, "name must be a string");
        assert_eq!(finding.path.as_deref(), Some("http://schema.org/name"));
        assert_eq!(
            finding.focus_node.as_deref(),
            Some("https://example.test/dataset")
        );
    }

    #[test]
    fn limits_are_incomplete() {
        let finding = limit_finding("budget exhausted".to_string(), "01REV");
        assert_eq!(finding.code, "validation_limit");
        assert_eq!(
            finding.completeness,
            ProfileValidationCompleteness::Incomplete
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
        assert_eq!(profile_from_iri(&profile_public_iri(id)), Some(id));
        assert_eq!(
            profile_from_iri(&MetadataRegistryRecord::graph_iri_for(id)),
            None
        );
    }

    fn digest_record() -> MetadataRegistryRecord {
        use aruna_core::structs::identity::realm::RealmId;
        let realm_id = RealmId::from_bytes([4u8; 32]);
        let document_id = Ulid::from_parts(1, 2);
        MetadataRegistryRecord {
            realm_id,
            group_id: Ulid::from_parts(1, 1),
            document_id,
            document_path: "datasets/digest".to_string(),
            graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
            public: true,
            permission_path: String::new(),
            placement: Default::default(),
            holder_node_ids: Vec::new(),
            created_at_ms: 1,
            updated_at_ms: 1,
            establishing_event_id: Ulid::from_parts(1, 3),
            last_event_id: Ulid::from_parts(2, 3),
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn event(event: Event) -> IoResult {
        IoResult::Event(Box::new(event))
    }

    fn raw_state(digest: [u8; 32]) -> RawRevisionState {
        RawRevisionState {
            base: None,
            last_event_id: Ulid::from_parts(2, 3),
            revision: Some(MetadataRawRevision {
                jsonld: "{}".to_string(),
                winning_event_id: Ulid::from_parts(2, 3),
                context_digest: [1; 32],
                dataset_digest: Some(digest),
                merged: None,
            }),
            merged: true,
        }
    }

    #[test]
    fn keys_status_digest() {
        // A merge can leave the displayed revision behind the newest event, so freshness
        // follows the render's digest, not the event id.
        let record = digest_record();
        let status_of = |digest: Option<[u8; 32]>, revision: Ulid| {
            let mut status = not_profiled_status(record.document_id);
            status.dataset_revision = revision;
            status.dataset_digest = digest;
            status
        };
        let run = |stored: ProfileValidationStatus, state: Option<RawRevisionState>| {
            let mut operation = ValidationStatusOperation::new(record.clone());
            assert!(matches!(
                operation.first(),
                Ok(Next::Call(IoCall::Child(_)))
            ));
            let turn = operation.next(child(Ok::<_, MetadataError>(Some(stored))));
            match turn {
                Ok(Next::Call(IoCall::Child(_))) => {
                    operation.next(child(Ok::<_, RawReadError>(state)))
                }
                other => other,
            }
        };
        // A status written before the first merge still keys on its event id.
        let Ok(Next::Done(current)) = run(status_of(None, record.last_event_id), None) else {
            panic!("expected the status");
        };
        assert_eq!(current.state, ProfileValidationState::NotProfiled);
        let Ok(Next::Done(stale)) = run(status_of(Some([9; 32]), record.last_event_id), None)
        else {
            panic!("expected the status");
        };
        assert_eq!(
            stale.stale_reason.as_deref(),
            Some("dataset_revision_changed")
        );
        let Ok(Next::Done(merged)) = run(
            status_of(Some([9; 32]), Ulid::from_parts(1, 3)),
            Some(raw_state([9; 32])),
        ) else {
            panic!("expected the status");
        };
        assert_eq!(merged.state, ProfileValidationState::NotProfiled);
    }

    #[test]
    fn missing_status_stale() {
        let mut operation = ValidationStatusOperation::new(digest_record());
        operation.first().unwrap();
        let Ok(Next::Done(status)) = operation.next(child(Ok::<_, MetadataError>(None))) else {
            panic!("expected the status");
        };
        assert_eq!(
            status.stale_reason.as_deref(),
            Some("validation_status_missing")
        );
    }

    #[test]
    fn profile_ages_status() {
        let record = digest_record();
        let profile_id = Ulid::from_parts(8, 8);
        let mut stored = not_profiled_status(record.document_id);
        stored.dataset_revision = record.last_event_id;
        stored.profile_id = Some(profile_id);
        stored.profile_revision = Some(Ulid::from_parts(9, 9).to_string());
        let run = |profile: Result<Option<MetadataRegistryRecord>, StorageReadError>| {
            let mut operation = ValidationStatusOperation::new(record.clone());
            operation.first().unwrap();
            operation
                .next(child(Ok::<_, MetadataError>(Some(stored.clone()))))
                .unwrap();
            match operation.next(child(profile)) {
                Ok(Next::Done(status)) => status.stale_reason,
                other => panic!("expected the status, got {other:?}"),
            }
        };
        let mut profile = digest_record();
        profile.last_event_id = Ulid::from_parts(9, 9);
        assert_eq!(run(Ok(Some(profile.clone()))), None);
        profile.last_event_id = Ulid::from_parts(10, 10);
        assert_eq!(
            run(Ok(Some(profile))).as_deref(),
            Some("profile_revision_changed")
        );
        assert_eq!(run(Ok(None)).as_deref(), Some("profile_not_registered"));
        let failed = StorageReadError::Storage(aruna_core::errors::StorageError::Timeout);
        assert_eq!(run(Err(failed)).as_deref(), Some("profile_unavailable"));
    }

    #[test]
    fn untagged_checks_structure() {
        let jsonld = crate_with_tag(None);
        let mut write =
            ProfileAssessOperation::write(None, Ulid::nil(), Ulid::nil(), jsonld.clone());
        let Ok(Next::Call(IoCall::Effect(check))) = write.first() else {
            panic!("expected the structural pass");
        };
        assert_eq!(
            *check,
            Effect::Metadata(MetadataEffect::CheckStructure {
                jsonld: jsonld.clone()
            })
        );
        let report = ProfileShaclReport {
            results: Vec::new(),
            structural: Vec::new(),
        };
        let answer = event(Event::Metadata(MetadataEvent::ProfileChecked {
            report: Ok(Box::new(report)),
        }));
        let Ok(Next::Done(preview)) = write.next(answer) else {
            panic!("expected the verdict");
        };
        assert!(preview.accepted());

        // A render skips the structural pass of an untagged crate.
        let mut render = ProfileAssessOperation::render(None, Ulid::nil(), Ulid::nil(), jsonld);
        assert!(matches!(render.first(), Ok(Next::Done(_))));
    }

    #[test]
    fn missing_evaluator_unavailable() {
        let jsonld = crate_with_tag(None);
        let mut write =
            ProfileAssessOperation::write(None, Ulid::nil(), Ulid::nil(), jsonld.clone());
        write.first().unwrap();
        let missing = event(Event::Metadata(MetadataEvent::Error {
            graph_iri: None,
            error: MetadataError::HandleMissing,
        }));
        let refused = write.next(missing);
        assert!(untagged_without_evaluator(
            refused.as_ref().unwrap_err(),
            &jsonld
        ));
        let status = submission_verdict(refused.map(|_| unreachable!()), Ulid::nil(), &jsonld);
        assert_eq!(status, Ok(not_profiled_status(Ulid::nil())));
    }

    #[test]
    fn builtin_evaluates_shapes() {
        let mut check =
            ProfileAssessOperation::against(None, CRATE_PROFILE_IRI, crate_with_tag(None));
        check.stamp(42);
        let Ok(Next::Call(IoCall::Effect(effect))) = check.first() else {
            panic!("expected the Profile check");
        };
        assert!(matches!(
            *effect,
            Effect::Metadata(MetadataEffect::CheckProfile { .. })
        ));
        let report = ProfileShaclReport {
            results: vec![result(
                "MinCountConstraintComponent",
                "Violation",
                Some("name"),
            )],
            structural: vec![craqle::CrateViolation {
                code: "missing_root_data_entity",
                message: "missing root".to_string(),
                pointer: String::new(),
                entity_id: None,
            }],
        };
        let answer = event(Event::Metadata(MetadataEvent::ProfileChecked {
            report: Ok(Box::new(report)),
        }));
        let Ok(Next::Done(preview)) = check.next(answer) else {
            panic!("expected the verdict");
        };
        assert_eq!(preview.status.validated_at_ms, Some(42));
        let status = profile_verdict(preview);
        assert_eq!(status.state, ProfileValidationState::Invalid);
        assert_eq!(status.findings.len(), 2);
    }

    #[test]
    fn registered_uses_cache() {
        // A cached revision skips the fetch; a miss fetches the Profile and keeps its shapes.
        let mut profile = digest_record();
        let profile_id = profile_id();
        profile.document_id = profile_id;
        profile.graph_iri = MetadataRegistryRecord::graph_iri_for(profile_id);
        let iri = profile_public_iri(profile_id);
        let start = || {
            let mut check = ProfileAssessOperation::preview(None, None, crate_with_tag(Some(&iri)));
            assert!(matches!(check.first(), Ok(Next::Call(IoCall::Child(_)))));
            let cached = check.next(child(Ok::<_, StorageReadError>(Some(profile.clone()))));
            let Ok(Next::Call(IoCall::Effect(effect))) = cached else {
                panic!("expected the cache lookup");
            };
            assert_eq!(
                *effect,
                Effect::Metadata(MetadataEffect::CachedShapes {
                    profile_id,
                    revision: profile.last_event_id,
                })
            );
            check
        };
        let mut hit = start();
        let shapes = event(Event::Metadata(MetadataEvent::ProfileShapes {
            shapes: Some(vec!["shape".to_string()]),
        }));
        assert!(matches!(
            hit.next(shapes),
            Ok(Next::Call(IoCall::Effect(effect))) if matches!(*effect, Effect::Metadata(MetadataEffect::CheckProfile { .. }))
        ));
        let mut miss = start();
        let none = event(Event::Metadata(MetadataEvent::ProfileShapes {
            shapes: None,
        }));
        assert!(matches!(miss.next(none), Ok(Next::Call(IoCall::Child(_)))));
        let refused = child(Err::<ExportMetadataResult, _>(
            MetadataReadError::Unavailable,
        ));
        let Err(MetadataError::ProfileValidation(findings)) = miss.next(refused) else {
            panic!("expected the unavailable Profile");
        };
        assert_eq!(findings[0].code, "profile_unavailable");
    }

    #[test]
    fn private_profile_unregistered() {
        let mut profile = digest_record();
        profile.public = false;
        profile.document_id = profile_id();
        profile.graph_iri = MetadataRegistryRecord::graph_iri_for(profile_id());
        let iri = profile_public_iri(profile_id());
        let mut check = ProfileAssessOperation::preview(None, None, crate_with_tag(Some(&iri)));
        check.first().unwrap();
        let Err(MetadataError::ProfileValidation(findings)) =
            check.next(child(Ok::<_, StorageReadError>(Some(profile))))
        else {
            panic!("expected the refusal");
        };
        assert_eq!(findings[0].code, "profile_not_registered");
    }

    #[test]
    fn revalidate_fences_revision() {
        let record = digest_record();
        let mut status = not_profiled_status(record.document_id);
        status.dataset_digest = Some([9; 32]);
        let commit = || RevalidateCommitOperation {
            document_id: record.document_id,
            merged: None,
            digest: Some([9; 32]),
            status: status.clone(),
            stage: CommitStage::Fence,
        };
        let mut gone = commit();
        assert_eq!(gone.transaction(), TransactionMode::Write);
        gone.first().unwrap();
        assert_eq!(
            gone.next(IoResult::Values(vec![None])),
            Err(revision_changed())
        );

        let mut moved = commit();
        moved.first().unwrap();
        let row = Some(postcard::to_allocvec(&record).unwrap().into());
        let Ok(Next::Call(IoCall::Read(_))) = moved.next(IoResult::Values(vec![row])) else {
            panic!("expected the raw revision read");
        };
    }

    fn profile_id() -> Ulid {
        let handle = PlacementHandle::new(1).unwrap();
        MetaResourceId::from_parts(2, handle, BucketId::new(1).unwrap(), 2)
            .unwrap()
            .as_ulid()
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
                            {"@id": "https://w3id.org/ro/wfrun/workflow/0.5"}
                        ]
                    }
                ]
            })
            .to_string();
            let (data, root) = data_graph(&document).unwrap();
            assert!(profile_tags(&data, &root).is_empty());
        }
    }

    #[test]
    fn builtin_tag_resolves() {
        // The built-in Profile must survive the specification-marker filter.
        let document = crate_with_tag(Some(CRATE_PROFILE_IRI));
        let (data, root) = data_graph(&document).unwrap();
        assert_eq!(
            single_profile_tag(&data, &root).unwrap().as_deref(),
            Some(CRATE_PROFILE_IRI)
        );
        assert!(builtin_shapes(CRATE_PROFILE_IRI).is_some());
    }
}
