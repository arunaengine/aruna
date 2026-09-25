//! Checks a dataset crate against the requirements of a repository kind before any transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::metadata::{MetadataError, ProfileValidationFinding, ProfileValidationSeverity};
use aruna_core::repository::rules::{Mapped, finding, preview, rules};
use aruna_core::repository::{LinkFailure, MAX_LINK_FINDINGS};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::AuthContext;
use serde_json::Value;
use ulid::Ulid;

use super::{TransferError, not_supported, repository, requirement_profile};
use crate::driver::DriverContext;
use crate::metadata::profile::validation::check_profile;

/// What a repository of `kind` needs from a crate: the requirement Profile findings, then the
/// mapping rule findings, and what each crate entity becomes. Only the crate counts, so
/// repository metadata overrides never satisfy a requirement.
#[derive(Clone, Debug)]
pub struct RequirementCheck {
    pub kind: RepositoryConnectorKind,
    pub profile_iri: &'static str,
    pub profile_revision: Option<String>,
    /// No finding is a violation.
    pub ready: bool,
    pub findings: Vec<ProfileValidationFinding>,
    pub mapping: Vec<Mapped>,
}

/// Checks the dataset's current crate against the connector's repository.
pub async fn check_requirements(
    context: &Arc<DriverContext>,
    auth: &AuthContext,
    document_id: Ulid,
    group_id: Ulid,
    connector_id: Ulid,
    metadata_bytes: u64,
) -> Result<RequirementCheck, TransferError> {
    let view = repository(context, group_id, connector_id).await?;
    let (jsonld, _) = Box::pin(crate::jobs::export::crate_jsonld(
        context,
        auth,
        document_id,
        metadata_bytes,
    ))
    .await?;
    let kind = view.connector.kind;
    Box::pin(check_crate(
        context,
        kind,
        &view.connector.endpoint,
        &jsonld,
    ))
    .await
}

/// Checks a crate against the requirements of a repository of `kind` at `endpoint`.
pub(crate) async fn check_crate(
    context: &DriverContext,
    kind: RepositoryConnectorKind,
    endpoint: &str,
    jsonld: &str,
) -> Result<RequirementCheck, TransferError> {
    let (Some(profile_iri), Some(rules)) = (requirement_profile(kind, endpoint), rules(kind)?)
    else {
        return Err(not_supported("publishing"));
    };
    let document: Value = serde_json::from_str(jsonld)
        .map_err(|_| TransferError::Permanent("invalid source crate".into()))?;
    let status = Box::pin(check_profile(context, profile_iri, jsonld))
        .await
        .map_err(|error| match error {
            MetadataError::InvalidInput(message) => TransferError::Permanent(message),
            error => TransferError::Retryable(format!("checking the requirements failed: {error}")),
        })?;
    let (mapping, rule_findings) = preview(rules, &document);
    let mut findings = status.findings;
    findings.extend(rule_findings);
    Ok(RequirementCheck {
        kind,
        profile_iri,
        profile_revision: status.profile_revision,
        ready: !findings
            .iter()
            .any(|finding| finding.severity == ProfileValidationSeverity::Violation),
        findings,
        mapping,
    })
}

/// The content rules of a kind checked against the files an export would upload, as paths
/// with sizes. File formats and MD5 checksums are not checked yet.
pub(crate) fn check_content(
    kind: RepositoryConnectorKind,
    files: &[(&str, u64)],
) -> Result<Vec<ProfileValidationFinding>, TransferError> {
    let Some(rules) = rules(kind)? else {
        return Ok(Vec::new());
    };
    let violation = |focus: &str, rule: String, message: String| {
        finding("content_violation", Some(focus.into()), None, rule, message)
    };
    let mut findings = Vec::new();
    for target in &rules.targets {
        let content = &target.content;
        if let Some(max) = content.max_files.filter(|max| files.len() > *max) {
            let message = format!("The export has {} files, more than {max}.", files.len());
            findings.push(violation(
                "./",
                format!("{}/max_files", target.name),
                message,
            ));
        }
        if let Some(max) = content.max_file_bytes {
            for (path, size) in files.iter().filter(|(_, size)| *size > max) {
                let message = format!("{path} has {size} bytes, more than {max}.");
                findings.push(violation(
                    path,
                    format!("{}/max_file_bytes", target.name),
                    message,
                ));
            }
        }
        let total = files.iter().map(|(_, size)| size).sum::<u64>();
        if let Some(max) = content.max_total_bytes.filter(|max| total > *max) {
            let message = format!("The export has {total} bytes, more than {max}.");
            findings.push(violation(
                "./",
                format!("{}/max_total_bytes", target.name),
                message,
            ));
        }
    }
    Ok(findings)
}

/// A refusal a link records: violations first, at most `MAX_LINK_FINDINGS` findings.
pub(crate) fn unmet(mut findings: Vec<ProfileValidationFinding>) -> TransferError {
    findings.sort_by_key(|finding| finding.severity != ProfileValidationSeverity::Violation);
    findings.truncate(MAX_LINK_FINDINGS);
    TransferError::Refused(LinkFailure::RequirementsUnmet(findings))
}
