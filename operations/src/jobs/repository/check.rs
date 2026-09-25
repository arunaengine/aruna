//! Checks a dataset crate against the requirements of a repository kind before any transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::Arc;

use aruna_core::metadata::{MetadataError, ProfileValidationFinding, ProfileValidationSeverity};
use aruna_core::repository::rules::{
    Content, FORMAT_PREFIX, FileFacts, Mapped, content_findings, preview, rules, target_files,
};
use aruna_core::repository::{LinkFailure, MAX_LINK_FINDINGS, descriptor};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ArtifactRef, ExportRoCrateSpec};
use aruna_core::structs::identity::auth::AuthContext;
use futures_util::StreamExt;
use serde_json::Value;
use ulid::Ulid;

use super::{Action, TransferError, ensure_supported, not_supported};
use crate::driver::DriverContext;
use crate::harvest::read_connector::ConnectorView;
use crate::jobs::executor::JobContext;
use crate::jobs::export::ExportCheckpoint;
use crate::jobs::import::archive::{
    ArchiveCompression, ArchiveEntry, ArchiveInspection, inspect_reader,
};
use crate::jobs::service::read_artifact_range;
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
    view: &ConnectorView,
    metadata_bytes: u64,
) -> Result<RequirementCheck, TransferError> {
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
    ensure_supported(kind, Action::Publish)?;
    let descriptor = descriptor(kind).ok_or_else(|| not_supported("publishing"))?;
    let profile_iri = (descriptor.profile)(endpoint);
    let rules = descriptor.rules()?;
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

/// The archive paths an export uploads for `target` of the kind's rules: the one definition
/// the content checks and the adapter's upload share.
pub(crate) fn uploads(
    kind: RepositoryConnectorKind,
    target: &str,
    checkpoint: &ExportCheckpoint,
) -> Result<Vec<String>, TransferError> {
    let rules = descriptor(kind)
        .ok_or_else(|| not_supported("publishing"))?
        .rules()?;
    let (mapping, _) = preview(rules, &snapshot(checkpoint)?);
    let paths = checkpoint.archive_paths();
    Ok(target_files(target, &mapping, |id| {
        paths.get(id).map(|path| (*path).to_string())
    }))
}

/// The content rules of a kind checked against the files the export uploads for each target,
/// with sizes from the archive and, for format rules, a bounded prefix of each file.
pub(crate) async fn check_content(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    kind: RepositoryConnectorKind,
    checkpoint: &ExportCheckpoint,
) -> Result<Vec<ProfileValidationFinding>, TransferError> {
    let Some(rules) = rules(kind)? else {
        return Ok(Vec::new());
    };
    if rules
        .targets
        .iter()
        .all(|target| target.content == Content::default())
    {
        return Ok(Vec::new());
    }
    let artifact = checkpoint
        .artifact
        .as_ref()
        .ok_or_else(|| TransferError::Permanent("export artifact missing".into()))?;
    let inspection = inspect_artifact(ctx, spec, artifact).await?;
    let entries = inspection
        .entries
        .iter()
        .filter(|entry| !entry.directory)
        .map(|entry| (entry.path.as_str(), entry))
        .collect::<HashMap<_, _>>();
    let mut findings = Vec::new();
    for target in rules
        .targets
        .iter()
        .filter(|t| t.content != Content::default())
    {
        let paths = uploads(kind, &target.name, checkpoint)?;
        let mut prefixes = Vec::new();
        for path in &paths {
            let entry = entries.get(path.as_str());
            let prefix = match (entry, target.content.format.is_some()) {
                (Some(entry), true) => Some(read_prefix(ctx, artifact, entry).await?),
                _ => None,
            };
            prefixes.push(prefix);
        }
        let facts = paths
            .iter()
            .zip(&prefixes)
            .map(|(path, prefix)| FileFacts {
                path,
                size: entries
                    .get(path.as_str())
                    .map(|entry| entry.uncompressed_size),
                prefix: prefix.as_deref(),
            })
            .collect::<Vec<_>>();
        findings.extend(content_findings(target, &facts));
    }
    Ok(findings)
}

/// The first `FORMAT_PREFIX` bytes of an archive entry stored verbatim.
async fn read_prefix(
    ctx: &JobContext,
    artifact: &ArtifactRef,
    entry: &ArchiveEntry,
) -> Result<Vec<u8>, TransferError> {
    if entry.compression != ArchiveCompression::Stored {
        return Err(TransferError::Permanent(
            "repository snapshot entry is not stored verbatim".into(),
        ));
    }
    let end = entry.data_offset + entry.uncompressed_size.min(FORMAT_PREFIX);
    let mut read = read_artifact_range(&ctx.driver, artifact, entry.data_offset..end)
        .await
        .map_err(TransferError::Retryable)?;
    let mut prefix = Vec::new();
    while let Some(chunk) = read.blob.next().await {
        let chunk = chunk.map_err(|_| TransferError::Retryable("crate file read failed".into()))?;
        prefix.extend_from_slice(&chunk);
    }
    prefix.truncate(FORMAT_PREFIX as usize);
    Ok(prefix)
}

/// The export artifact's entries, with the limits an export archive may reach.
pub(crate) async fn inspect_artifact(
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    artifact: &ArtifactRef,
) -> Result<ArchiveInspection, TransferError> {
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    let mut limits = spec.limits.clone();
    limits.import_source_bytes = spec.limits.export_artifact_bytes;
    limits.expanded_import_bytes = spec.limits.export_artifact_bytes;
    limits.max_entries = limits.max_entries.saturating_add(2);
    let (inspection, _) = inspect_reader(
        blob.clone(),
        artifact.location.clone(),
        artifact.size,
        false,
        &limits,
    )
    .await
    .map_err(TransferError::Permanent)?;
    Ok(inspection)
}

fn snapshot(checkpoint: &ExportCheckpoint) -> Result<Value, TransferError> {
    let jsonld = checkpoint
        .raw_jsonld
        .as_deref()
        .ok_or_else(|| TransferError::Permanent("source crate metadata missing".into()))?;
    serde_json::from_str(jsonld)
        .map_err(|_| TransferError::Permanent("invalid source crate".into()))
}

/// A refusal a link records: violations first, at most `MAX_LINK_FINDINGS` findings.
pub(crate) fn unmet(mut findings: Vec<ProfileValidationFinding>) -> TransferError {
    findings.sort_by_key(|finding| finding.severity != ProfileValidationSeverity::Violation);
    findings.truncate(MAX_LINK_FINDINGS);
    TransferError::Refused(LinkFailure::RequirementsUnmet(findings))
}
