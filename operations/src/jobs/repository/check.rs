//! Checks a dataset crate against the requirements of a repository kind before any transfer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};

use aruna_core::calls::{IoResult, Next};
use aruna_core::metadata::{
    MetadataError, ProfileValidationFinding, ProfileValidationSeverity, ProfileValidationStatus,
};
use aruna_core::operation::Operation;
use aruna_core::repository::remote::{JobStop, RemoteAnswer, RemoteCall};
use aruna_core::repository::rules::{
    Content, FORMAT_PREFIX, FileFacts, Mapped, Target, content_findings, preview, rules,
    target_files,
};
use aruna_core::repository::{LinkFailure, MAX_LINK_FINDINGS, descriptor};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::ArtifactRef;
use serde_json::Value;

use super::{Action, TransferError, ensure_supported, not_supported, remote_answer, remote_call};
use crate::jobs::export::ExportCheckpoint;
use crate::jobs::import::archive::{ArchiveCompression, ArchiveEntry, ArchiveInspection};

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
    /// At most `MAX_CHECK_FINDINGS`, violations first.
    pub findings: Vec<ProfileValidationFinding>,
    /// How many findings the capped list leaves out.
    pub omitted: usize,
    pub mapping: Vec<Mapped>,
}

/// The most findings a check returns.
const MAX_CHECK_FINDINGS: usize = 100;

/// A crate ready to be checked against the Profile of its repository kind.
#[derive(Debug)]
pub struct CrateCheck {
    kind: RepositoryConnectorKind,
    /// The Profile the caller validates the crate against.
    pub profile_iri: &'static str,
    document: Value,
}

impl CrateCheck {
    pub fn new(
        kind: RepositoryConnectorKind,
        endpoint: &str,
        jsonld: &str,
    ) -> Result<Self, TransferError> {
        ensure_supported(kind, Action::Publish)?;
        let descriptor = descriptor(kind).ok_or_else(|| not_supported("publishing"))?;
        let profile_iri = (descriptor.profile)(endpoint);
        descriptor.rules()?;
        let document = serde_json::from_str(jsonld)
            .map_err(|_| TransferError::Permanent("invalid source crate".into()))?;
        Ok(Self {
            kind,
            profile_iri,
            document,
        })
    }

    /// The check result from the Profile's verdict and the kind's mapping rules.
    pub fn finish(self, status: ProfileValidationStatus) -> Result<RequirementCheck, TransferError> {
        let rules = descriptor(self.kind)
            .ok_or_else(|| not_supported("publishing"))?
            .rules()?;
        let (mapping, rule_findings) = preview(rules, &self.document);
        let mut findings = status.findings;
        findings.extend(rule_findings);
        let ready = !findings
            .iter()
            .any(|finding| finding.severity == ProfileValidationSeverity::Violation);
        findings.sort_by_key(|finding| finding.severity != ProfileValidationSeverity::Violation);
        let omitted = findings.len().saturating_sub(MAX_CHECK_FINDINGS);
        findings.truncate(MAX_CHECK_FINDINGS);
        Ok(RequirementCheck {
            kind: self.kind,
            profile_iri: self.profile_iri,
            profile_revision: status.profile_revision,
            ready,
            findings,
            omitted,
            mapping,
        })
    }
}

/// How a failed Profile validation ends a requirement check.
pub fn profile_failure(error: MetadataError) -> TransferError {
    match error {
        MetadataError::InvalidInput(message) => TransferError::Permanent(message),
        error => TransferError::Retryable(format!("checking the requirements failed: {error}")),
    }
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

/// Whether the kind's rules check the content of any uploaded file.
pub(crate) fn checks_content(kind: RepositoryConnectorKind) -> Result<bool, TransferError> {
    Ok(rules(kind)?.is_some_and(|rules| {
        rules
            .targets
            .iter()
            .any(|target| target.content != Content::default())
    }))
}

/// One content target with the paths its check still has to look at.
#[derive(Debug, PartialEq)]
struct TargetCheck {
    target: Target,
    paths: Vec<String>,
    prefixes: Vec<Option<Vec<u8>>>,
}

/// The content rules of a kind checked against the files the export uploads for each target,
/// with sizes from the archive and, for format rules, a bounded prefix of each file.
#[derive(Debug, PartialEq)]
pub(crate) struct CheckContentOperation {
    artifact: ArtifactRef,
    entries: HashMap<String, ArchiveEntry>,
    targets: VecDeque<TargetCheck>,
    findings: Vec<ProfileValidationFinding>,
    stop: JobStop,
}

impl CheckContentOperation {
    pub(crate) fn new(
        kind: RepositoryConnectorKind,
        checkpoint: &ExportCheckpoint,
        artifact: ArtifactRef,
        inspection: ArchiveInspection,
        stop: JobStop,
    ) -> Result<Self, TransferError> {
        let mut targets = VecDeque::new();
        if let Some(rules) = rules(kind)? {
            for target in rules
                .targets
                .iter()
                .filter(|target| target.content != Content::default())
            {
                targets.push_back(TargetCheck {
                    target: target.clone(),
                    paths: uploads(kind, &target.name, checkpoint)?,
                    prefixes: Vec::new(),
                });
            }
        }
        Ok(Self {
            artifact,
            entries: inspection
                .entries
                .into_iter()
                .filter(|entry| !entry.directory)
                .map(|entry| (entry.path.clone(), entry))
                .collect(),
            targets,
            findings: Vec::new(),
            stop,
        })
    }

    /// Reads the next prefix a format rule needs, or checks each target whose files are known.
    fn advance(&mut self) -> Result<Next<Vec<ProfileValidationFinding>>, TransferError> {
        while let Some(check) = self.targets.front_mut() {
            while check.prefixes.len() < check.paths.len() {
                let entry = self.entries.get(&check.paths[check.prefixes.len()]);
                match (entry, check.target.content.format.is_some()) {
                    (Some(entry), true) => {
                        if entry.compression != ArchiveCompression::Stored {
                            return Err(TransferError::Permanent(
                                "repository snapshot entry is not stored verbatim".into(),
                            ));
                        }
                        let end = entry.data_offset + entry.uncompressed_size.min(FORMAT_PREFIX);
                        return Ok(Next::Call(remote_call(
                            RemoteCall::ReadRange {
                                source: self.artifact.location.clone(),
                                range: entry.data_offset..end,
                            },
                            Some(self.stop.clone()),
                        )));
                    }
                    _ => check.prefixes.push(None),
                }
            }
            let facts = check
                .paths
                .iter()
                .zip(&check.prefixes)
                .map(|(path, prefix)| FileFacts {
                    path,
                    size: self.entries.get(path).map(|entry| entry.uncompressed_size),
                    prefix: prefix.as_deref(),
                })
                .collect::<Vec<_>>();
            self.findings.extend(content_findings(&check.target, &facts));
            self.targets.pop_front();
        }
        Ok(Next::Done(std::mem::take(&mut self.findings)))
    }
}

impl Operation for CheckContentOperation {
    type Output = Vec<ProfileValidationFinding>;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Self::Output>, TransferError> {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, TransferError> {
        let answer = remote_answer(result)?.map_err(TransferError::from)?;
        let RemoteAnswer::Bytes(mut prefix) = answer else {
            return Err(TransferError::Retryable("unexpected crate file answer".into()));
        };
        prefix.truncate(FORMAT_PREFIX as usize);
        let check = self
            .targets
            .front_mut()
            .ok_or_else(|| TransferError::Retryable("no content check is waiting".into()))?;
        check.prefixes.push(Some(prefix));
        self.advance()
    }
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

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;
