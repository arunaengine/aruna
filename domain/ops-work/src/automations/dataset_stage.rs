//! Intent-first keyed-create `dataset` stage publishing an RO-Crate below `scope.metadata`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::automation::firing::Firing;
use aruna_core::automation::record::{Automation, StageKind};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{AUTOMATION_CREATES_KEYSPACE, AUTOMATION_INTENTS_KEYSPACE};
use aruna_core::metadata::AuthToken;
use aruna_core::metadata::api::CreateDocumentPayload;
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::CallIntent;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_ops_data::metadata::api::MetadataApiError;
use aruna_ops_data::metadata::api::export_types::GetVisibleRequest;
use aruna_ops_data::metadata::forward::authorized_create::{
    CreateAuthorizedError, PinnedCreateOperation,
};
use aruna_ops_data::metadata::forward::pinned::{PinnedCreate, PinnedMintOperation};
use aruna_ops_data::metadata::forward::routed::read::RoutedReadOperation;
use serde_json::json;
use ulid::Ulid;

use super::copy_stage::{StageOutcome, call_intent, stage_intent_id};
use super::run_stage::render_text;
use super::{AutoError, automation_auth_for};
use crate::plugins::creates::{
    Claim, Lookup, Step as CreateStep, claim_key_in, created_text, next_step, settle_key_in,
};
use crate::plugins::intents::{Begun, begin_in, row_key_in, settled_row_in, unknown_row_in};
use crate::plugins::rows::{StoreError, delete_call};

#[derive(Debug, PartialEq)]
enum DatasetPhase {
    Begin,
    Mint,
    Claim,
    Lookup,
    Create,
    SettleKey(Result<Ulid, String>),
    RecordIntent(StageOutcome),
}

/// Creates a keyed RO-Crate dataset document below `scope.metadata` describing delivered files.
#[derive(Debug, PartialEq)]
pub struct DatasetStageOp {
    auto: Automation,
    firing: Firing,
    stage_name: String,
    stage_id: Ulid,
    from_stage: String,
    metadata_path: String,
    name: String,
    description: String,
    license: Option<String>,
    local: (NodeId, RealmId),
    auth: Option<AuthContext>,
    intent: CallIntent,
    pinned: Option<Box<PinnedCreate>>,
    claim: Claim,
    lookup: Option<Lookup>,
    phase: DatasetPhase,
}

impl DatasetStageOp {
    pub fn new(
        auto: Automation,
        firing: Firing,
        stage_idx: usize,
        local: (NodeId, RealmId),
    ) -> Result<Self, AutoError> {
        let (stage_name, from_stage, metadata_path, name_template, description, license) =
            match firing.spec.stages.get(stage_idx) {
                Some(stage) => match &stage.kind {
                    StageKind::Dataset {
                        from_stage,
                        metadata_path,
                        name_template,
                        description,
                        license,
                        ..
                    } => (
                        stage.name.clone(),
                        from_stage.clone(),
                        metadata_path.clone(),
                        name_template.clone(),
                        description.clone(),
                        license.clone(),
                    ),
                    _ => return Err(AutoError::Invalid("stage is not a dataset stage".into())),
                },
                None => return Err(AutoError::Invalid("missing dataset stage".into())),
            };
        let name = render_text(&name_template, &firing);
        let stage_id = stage_intent_id(firing.firing_id, &stage_name);
        let intent = call_intent(0, &[metadata_path.as_bytes(), name.as_bytes()]);
        Ok(Self {
            auto,
            firing,
            stage_name,
            stage_id,
            from_stage,
            metadata_path,
            name,
            description,
            license,
            local,
            auth: None,
            intent,
            pinned: None,
            claim: Claim::Fresh,
            lookup: None,
            phase: DatasetPhase::Begin,
        })
    }

    fn key_name(&self) -> String {
        format!("{}:{}", self.firing.firing_id, self.stage_name)
    }

    fn rocrate_payload(&self) -> Result<CreateDocumentPayload, AutoError> {
        let mut parts = Vec::new();
        let mut graph = vec![json!({
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.2"},
            "about": {"@id": "./"}
        })];
        if let Some(stage) = self
            .firing
            .stages
            .iter()
            .find(|s| s.name == self.from_stage)
        {
            for file in &stage.receipt.copied {
                let id = format!(
                    "aruna://{}/{}?version={}",
                    file.dest_bucket, file.dest_key, file.dest_version
                );
                parts.push(json!({"@id": id}));
                graph.push(json!({"@id": id, "@type": "File", "contentSize": file.bytes}));
            }
            if parts.is_empty() {
                for file in &stage.receipt.outputs {
                    if let (Some(bucket), Some(key)) = (&file.bucket, &file.key) {
                        let id = match &file.version_id {
                            Some(v) => format!("aruna://{bucket}/{key}?version={v}"),
                            None => format!("aruna://{bucket}/{key}"),
                        };
                        parts.push(json!({"@id": id}));
                        graph.push(json!({"@id": id, "@type": "File", "name": file.name}));
                    }
                }
            }
        }
        let mut root = json!({
            "@id": "./",
            "@type": "Dataset",
            "name": self.name,
            "description": self.description,
            "hasPart": parts,
        });
        if let Some(lic) = &self.license {
            root["license"] = json!(lic);
        }
        if let Some(run_id) = self
            .firing
            .stages
            .iter()
            .find_map(|s| s.receipt.workflow_run_id)
        {
            root["isBasedOn"] = json!({"@id": format!("/compute/workflow-runs/{run_id}")});
        }
        graph.push(root);
        let crate_json = json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": graph,
        });
        let jsonld =
            serde_json::to_string(&crate_json).map_err(|e| AutoError::Storage(e.to_string()))?;
        Ok(CreateDocumentPayload::RoCrate { jsonld })
    }

    fn dispatch(&mut self) -> Result<Next<StageOutcome>, AutoError> {
        let pinned = self.pinned.as_ref().ok_or(IoError::Unfinished)?;
        let auth = self.auth.clone().ok_or(IoError::Unfinished)?;
        let actor = Actor {
            node_id: self.local.0,
            user_id: auth.user_id,
            realm_id: self.local.1,
        };
        match next_step(self.claim, self.lookup) {
            CreateStep::Dispatch => {
                let extras = PolicyRequestExtras::operation("automation.metadata.create");
                let op = PinnedCreateOperation::new(
                    (actor, Some(self.local.0)),
                    (auth, extras),
                    (**pinned).clone(),
                );
                self.phase = DatasetPhase::Create;
                Ok(Next::Call(IoCall::child(op)))
            }
            CreateStep::Lookup => {
                let req = GetVisibleRequest {
                    document_id: pinned.document_id,
                    auth: Some(auth.clone()),
                };
                let read = RoutedReadOperation::new(
                    Some(self.local.0),
                    self.local.1,
                    req,
                    Some(AuthToken::Internal(auth)),
                );
                self.phase = DatasetPhase::Lookup;
                Ok(Next::Call(IoCall::child(read)))
            }
            CreateStep::Adopt(_) => {
                let doc_id = pinned.document_id;
                let settle = settle_key_in(
                    AUTOMATION_CREATES_KEYSPACE,
                    (self.auto.automation_id, auth.user_id, &self.key_name()),
                    false,
                );
                self.phase = DatasetPhase::SettleKey(Ok(doc_id));
                Ok(Next::Call(IoCall::child(settle)))
            }
            CreateStep::Unknown => {
                let msg = "earlier dataset create could not be verified".to_string();
                let row = unknown_row_in(
                    AUTOMATION_INTENTS_KEYSPACE,
                    self.stage_id,
                    &self.intent,
                    &msg,
                )
                .map_err(|e| AutoError::Storage(e.detail()))?;
                self.phase = DatasetPhase::RecordIntent(StageOutcome::NeedsAttention(msg, None));
                Ok(Next::Call(IoCall::Write(vec![row])))
            }
        }
    }
}

impl Operation for DatasetStageOp {
    type Output = StageOutcome;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<StageOutcome>, AutoError> {
        let auth = automation_auth_for(&self.auto, &self.firing.spec, self.local.1)
            .ok_or_else(|| AutoError::Conflict("automation has no active account".into()))?;
        self.auth = Some(auth);
        let begin = begin_in(
            AUTOMATION_INTENTS_KEYSPACE,
            self.stage_id,
            self.intent.clone(),
        );
        Ok(Next::Call(IoCall::child(begin)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<StageOutcome>, AutoError> {
        match std::mem::replace(&mut self.phase, DatasetPhase::Begin) {
            DatasetPhase::Begin => match result
                .child::<Begun, StoreError>()?
                .map_err(|e| AutoError::Storage(e.detail()))?
            {
                Begun::Fresh => {
                    let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                    let actor = Actor {
                        node_id: self.local.0,
                        user_id: auth.user_id,
                        realm_id: self.local.1,
                    };
                    let payload = self.rocrate_payload()?;
                    let mint = PinnedMintOperation::new(
                        actor,
                        (self.auto.group_id, &self.metadata_path),
                        (payload, Some(format!("automation {}", self.auto.name))),
                    );
                    self.phase = DatasetPhase::Mint;
                    Ok(Next::Call(IoCall::child(mint)))
                }
                Begun::Settled(reply) => {
                    let doc_id = reply
                        .split_whitespace()
                        .next()
                        .and_then(|s| Ulid::from_string(s).ok())
                        .ok_or_else(|| AutoError::Storage("invalid dataset receipt".into()))?;
                    Ok(Next::Done(StageOutcome::Dataset(doc_id)))
                }
                Begun::Changed | Begun::Unknown(_) => Ok(Next::Done(StageOutcome::NeedsAttention(
                    "dataset create outcome unknown".into(),
                    None,
                ))),
            },
            DatasetPhase::Mint => {
                let minted = match result.child::<PinnedCreate, CreateAuthorizedError>()? {
                    Ok(minted) => minted,
                    Err(err) => {
                        let key =
                            row_key_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, &self.intent);
                        self.phase =
                            DatasetPhase::RecordIntent(StageOutcome::Failed(err.to_string()));
                        return Ok(Next::Call(delete_call(vec![key])));
                    }
                };
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                let claim = claim_key_in(
                    AUTOMATION_CREATES_KEYSPACE,
                    (self.auto.automation_id, auth.user_id, &self.key_name()),
                    minted,
                );
                self.phase = DatasetPhase::Claim;
                Ok(Next::Call(IoCall::child(claim)))
            }
            DatasetPhase::Claim => {
                let (pinned, claim) = result
                    .child::<(PinnedCreate, Claim), StoreError>()?
                    .map_err(|e| AutoError::Storage(e.detail()))?;
                self.pinned = Some(Box::new(pinned));
                self.claim = claim;
                self.dispatch()
            }
            DatasetPhase::Lookup => {
                let found = result.child::<MetadataRegistryRecord, MetadataApiError>()?;
                self.lookup = Some(match found {
                    Ok(rec) => Lookup::Found(rec.establishing_event_id),
                    Err(MetadataApiError::NotFound) => Lookup::Missing,
                    Err(_) => Lookup::Unreachable,
                });
                self.dispatch()
            }
            DatasetPhase::Create => {
                let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
                match result.child::<MetadataRegistryRecord, CreateAuthorizedError>()? {
                    Ok(rec) => {
                        let settle = settle_key_in(
                            AUTOMATION_CREATES_KEYSPACE,
                            (self.auto.automation_id, auth.user_id, &self.key_name()),
                            false,
                        );
                        self.phase = DatasetPhase::SettleKey(Ok(rec.document_id));
                        Ok(Next::Call(IoCall::child(settle)))
                    }
                    Err(
                        err @ (CreateAuthorizedError::EmptyPath | CreateAuthorizedError::Forbidden),
                    ) => {
                        let settle = settle_key_in(
                            AUTOMATION_CREATES_KEYSPACE,
                            (self.auto.automation_id, auth.user_id, &self.key_name()),
                            true,
                        );
                        self.phase = DatasetPhase::SettleKey(Err(err.to_string()));
                        Ok(Next::Call(IoCall::child(settle)))
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        let row = unknown_row_in(
                            AUTOMATION_INTENTS_KEYSPACE,
                            self.stage_id,
                            &self.intent,
                            &msg,
                        )
                        .map_err(|e| AutoError::Storage(e.detail()))?;
                        self.phase =
                            DatasetPhase::RecordIntent(StageOutcome::NeedsAttention(msg, None));
                        Ok(Next::Call(IoCall::Write(vec![row])))
                    }
                }
            }
            DatasetPhase::SettleKey(outcome) => {
                let _ = result.child::<(), StoreError>()?;
                match outcome {
                    Ok(doc_id) => {
                        let row = settled_row_in(
                            AUTOMATION_INTENTS_KEYSPACE,
                            self.stage_id,
                            &self.intent,
                            created_text(&doc_id, &Ulid::nil()),
                        )
                        .map_err(|e| AutoError::Storage(e.detail()))?;
                        self.phase = DatasetPhase::RecordIntent(StageOutcome::Dataset(doc_id));
                        Ok(Next::Call(IoCall::Write(vec![row])))
                    }
                    Err(msg) => {
                        let key =
                            row_key_in(AUTOMATION_INTENTS_KEYSPACE, self.stage_id, &self.intent);
                        self.phase = DatasetPhase::RecordIntent(StageOutcome::Failed(msg));
                        Ok(Next::Call(delete_call(vec![key])))
                    }
                }
            }
            DatasetPhase::RecordIntent(outcome) => {
                result.done()?;
                Ok(Next::Done(outcome))
            }
        }
    }
}
