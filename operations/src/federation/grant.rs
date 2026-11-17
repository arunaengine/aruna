//! Decides the grant of a finished export: the stored grant while it is valid, or a new one
//! signed from the finished export checkpoint. Nothing is written here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::federation::Signed;
use aruna_core::keyspaces::{FEDERATION_KEYSPACE, JOB_STATE_KEYSPACE};
use aruna_core::operation::EffectOperation;
use aruna_core::structs::execution::job::{ExportSelection, JobId};
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::transfer::{
    ExportGrant, GrantError, GrantRecord, MAX_TRANSFER_SECS, check_issued, selection_digest,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
use ulid::Ulid;
use url::Url;

use crate::jobs::export::ExportCheckpoint;

pub(crate) fn record_key(job_id: JobId) -> Vec<u8> {
    [&b"grant/"[..], &job_id.to_bytes()].concat()
}

/// Owned inputs of [`IssueGrantOperation`].
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IssueGrantConfig {
    pub realm_id: RealmId,
    pub principal: UserId,
    pub job_id: JobId,
    pub document_id: Ulid,
    pub document_path: String,
    pub selection: ExportSelection,
    pub artifact_url: Url,
    pub capabilities: NodeCapabilities,
    pub now: u64,
}

/// Reads the grant record and checkpoint of an export, then returns the stored grant while it
/// is valid, or signs a new one from the finished checkpoint. Nothing is written here.
#[derive(Debug, PartialEq)]
pub(crate) struct IssueGrantOperation {
    config: IssueGrantConfig,
    output: Option<Result<(bool, GrantRecord), GrantError>>,
}

impl IssueGrantOperation {
    pub(crate) fn new(config: IssueGrantConfig) -> Self {
        Self {
            config,
            output: None,
        }
    }

    fn stored(&self, record: GrantRecord) -> Result<(bool, GrantRecord), GrantError> {
        if record.revoked {
            return Err(GrantError::Revoked);
        }
        let config = &self.config;
        check_issued(
            &record.grant,
            &config.realm_id,
            config.job_id.as_ulid(),
            config.now,
        )?;
        Ok((true, record))
    }

    fn sign(&self, checkpoint: ExportCheckpoint) -> Result<(bool, GrantRecord), GrantError> {
        let config = &self.config;
        let selection = &config.selection;
        let facts = checkpoint
            .export_facts(&selection.files)
            .ok_or(GrantError::Unfinished)?;
        let grant = ExportGrant {
            source: config.realm_id,
            audience: selection.audience,
            intent_digest: selection.intent_digest.clone(),
            export_job_id: config.job_id.as_ulid(),
            document_id: config.document_id,
            source_revision: facts.revision,
            dataset_digest: hex::encode(facts.dataset_digest),
            selection_digest: selection_digest(facts.revision, &facts.versions)?,
            artifact_url: config.artifact_url.clone(),
            artifact_blake3: hex::encode(facts.artifact.blake3),
            artifact_size: facts.artifact.size,
            issued_at: config.now,
            expires_at: config.now.saturating_add(MAX_TRANSFER_SECS),
        };
        let record = GrantRecord {
            grant: Signed::sign(grant, &config.capabilities)?,
            principal: config.principal,
            document_path: config.document_path.clone(),
            with_files: !selection.files.is_empty(),
            sources: facts.sources,
            revoked: false,
        };
        Ok((false, record))
    }

    fn decide(&self, event: Event) -> Result<(bool, GrantRecord), GrantError> {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return Err(GrantError::Storage(format!(
                "unexpected event {}",
                event.label()
            )));
        };
        let [(_, record), (_, checkpoint)] = values.as_slice() else {
            return Err(GrantError::Storage("unexpected read result".to_string()));
        };
        let decode = |error: postcard::Error| GrantError::Storage(error.to_string());
        if let Some(record) = record {
            return self.stored(postcard::from_bytes(record).map_err(decode)?);
        }
        let checkpoint = checkpoint.as_ref().ok_or(GrantError::Unfinished)?;
        self.sign(postcard::from_bytes(checkpoint).map_err(decode)?)
    }
}

impl EffectOperation for IssueGrantOperation {
    /// Whether the record was stored before, and the record.
    type Output = (bool, GrantRecord);
    type Error = GrantError;

    fn start(&mut self) -> Effects {
        let job_id = self.config.job_id;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (FEDERATION_KEYSPACE.to_string(), record_key(job_id).into()),
                (
                    JOB_STATE_KEYSPACE.to_string(),
                    job_id.to_bytes().to_vec().into()
                ),
            ],
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        // Only the one read answers; a decided grant takes no further event.
        self.output = Some(match self.output {
            Some(_) => Err(GrantError::Storage(
                "unexpected event after the grant".to_string(),
            )),
            None => self.decide(event),
        });
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.output.is_some()
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(GrantError::Storage("not finished".to_string())))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}
