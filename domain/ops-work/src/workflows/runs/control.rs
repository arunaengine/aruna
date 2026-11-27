//! Resumes and repeats an ended run as a new run of its lineage: a resume replays the lineage's
//! run log so finished tasks are not run again, a repeat runs everything again in its own cache.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{
    CacheScope, WorkflowRunRecord, WorkflowSecretScope,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use serde_json::Value;
use ulid::Ulid;

use aruna_ops_identity::auth::check_permissions::{
    CheckPermissionsConfig, CheckPermissionsOperation,
};
use aruna_ops_identity::groups::admin_check::realm_groups_path;

use super::admit::{AdmittedRun, Lineage, PinnedInput, RunRequest, StartRunOperation};
use super::{RunAccessOperation, access_of};
use crate::workflows::{CatalogError, Need};

/// What a new run of a lineage does with the run it follows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Follow {
    /// Replays the lineage's run log; tasks that finished there are not run again.
    Resume,
    /// Runs the same version, values, inputs and images again, reusing no earlier result.
    Repeat,
}

/// Starts a new run that resumes or repeats an ended run. The caller needs WRITE on the group's
/// workflows and every right a start needs; a run that has not ended is a conflict.
#[derive(Debug, PartialEq)]
pub struct FollowRunOperation {
    auth: AuthContext,
    token: Option<AuthToken>,
    node: (NodeId, RealmId),
    source: Ulid,
    follow: Follow,
    step: FollowStep,
}

#[derive(Debug, PartialEq)]
enum FollowStep {
    Access,
    /// A copy of a run another node coordinates waits for the realm admin check.
    Admin(Box<WorkflowRunRecord>),
    Start,
}

impl FollowRunOperation {
    pub fn new(auth: &AuthContext, node: (NodeId, RealmId), source: Ulid, follow: Follow) -> Self {
        Self {
            auth: auth.clone(),
            token: None,
            node,
            source,
            follow,
            step: FollowStep::Access,
        }
    }

    /// Supplies the caller's bearer token for resolving remote pinned inputs on repeat or resume.
    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }

    fn admit(
        &mut self,
        source: &WorkflowRunRecord,
    ) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let start = self.start(source)?;
        self.step = FollowStep::Start;
        Ok(Next::Call(IoCall::child(start)))
    }

    /// Decides on a run that has not ended. Only a copy of a run another node coordinates is
    /// resumed here, and only by its owner or a realm admin; the copy itself never moves.
    fn active(
        &mut self,
        source: WorkflowRunRecord,
    ) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        if source.node_id == self.node.0 || self.follow != Follow::Resume {
            return Err(CatalogError::Conflict(format!(
                "the run is still {}",
                source.state.as_str()
            )));
        }
        if source.created_by == self.auth.user_id {
            return self.admit(&source);
        }
        let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: realm_groups_path(&self.auth),
            required_permission: Permission::WRITE,
        });
        self.step = FollowStep::Admin(Box::new(source));
        Ok(Next::Call(IoCall::child(check)))
    }

    fn start(&self, source: &WorkflowRunRecord) -> Result<StartRunOperation, CatalogError> {
        let mut values: Value = crate::workflows::from_text(&source.values_json)?;
        if source.node_id != self.node.0 {
            values_held_by(&mut values, source.node_id);
        }
        let mut inputs_json = source.inputs_json.clone();
        // Pins the run's own node held name it once the run continues on another node.
        if let Some(text) = inputs_json
            .as_deref()
            .filter(|_| source.node_id != self.node.0)
        {
            let mut inputs: Value = crate::workflows::from_text(text)?;
            held_by(&mut inputs, source.node_id);
            inputs_json = Some(crate::workflows::to_text(&inputs)?);
        }
        let inputs = match inputs_json.as_deref() {
            Some(text) => pins_of(&crate::workflows::from_text(text)?)?,
            None => Vec::new(),
        };
        let has_user_secrets = source
            .secrets
            .values()
            .any(|binding| binding.scope == WorkflowSecretScope::User);
        if has_user_secrets && source.created_by != self.auth.user_id {
            return Err(CatalogError::Forbidden);
        }
        let mut output = source.output.clone();
        // A cross-node failover resume rebinds an output targeting the original coordinator to this
        // surviving holder; admission still checks the bucket exists and is writable here.
        if self.follow == Follow::Resume
            && source.node_id != self.node.0
            && let Some(out) = &mut output
            && out.node_id == Some(source.node_id)
        {
            out.node_id = Some(self.node.0);
        }
        let request = RunRequest {
            group_id: source.group_id,
            workflow_id: source.workflow_id,
            version: source.version,
            values,
            inputs,
            inputs_json,
            output,
            provenance_crate: source.provenance_crate,
            idempotency_key: None,
            placement_site: source.placement_site.clone(),
            secrets: source.secrets.clone(),
        };
        let lineage = match self.follow {
            Follow::Resume => Lineage {
                first: Some(source.lineage),
                resumes: Some(source.run_id),
                repeats: None,
                cache_scope: source.cache_scope,
                protected: source.protected,
                used_tools: source.used_tools.clone(),
            },
            Follow::Repeat => Lineage {
                first: Some(source.lineage),
                resumes: None,
                repeats: Some(source.run_id),
                cache_scope: CacheScope::Run,
                protected: source.protected,
                used_tools: Vec::new(),
            },
        };
        let mut start = StartRunOperation::new(&self.auth, self.node, request)
            .in_lineage(lineage)
            .with_token(self.token.clone());
        let mut directory_pins = Vec::new();
        for mut pin in source.directory_pins.clone() {
            if source.node_id != self.node.0 {
                pin.source_node_id.get_or_insert(source.node_id);
            }
            if pin.source_node_id == Some(self.node.0) {
                pin.source_node_id = None;
            }
            directory_pins.push(pin);
        }
        let mut directory_roots = Vec::new();
        for mut root in source.directory_roots.clone() {
            if source.node_id != self.node.0 {
                root.source_node_id.get_or_insert(source.node_id);
            }
            if root.source_node_id == Some(self.node.0) {
                root.source_node_id = None;
            }
            directory_roots.push(root);
        }
        if !directory_pins.is_empty() || !directory_roots.is_empty() {
            start = start.with_directories(directory_pins, directory_roots);
        }
        if source.created_by == self.auth.user_id && self.auth.path_restrictions.is_none() {
            start = start.with_captured(source.captured_remote.clone());
        }
        Ok(start)
    }
}

/// Names `node` on every `aruna://` or `s3://` file address in `values` that names no node.
pub(super) fn values_held_by(value: &mut Value, node: NodeId) {
    match value {
        Value::String(text) => {
            let scheme = if text.starts_with("aruna://") {
                Some("aruna://")
            } else if text.starts_with("s3://") {
                Some("s3://")
            } else {
                None
            };
            if let Some(scheme) = scheme
                && let Ok((bucket, key, version, None)) = super::data::parse_address(text)
            {
                *text = super::data::format_address(scheme, &bucket, &key, version, Some(node));
            }
        }
        Value::Array(items) => {
            for item in items {
                values_held_by(item, node);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                values_held_by(item, node);
            }
        }
        _ => {}
    }
}

/// Names `node` as the holder of every pin that names none.
fn held_by(inputs: &mut Value, node: NodeId) {
    let pins = (inputs.as_object_mut().into_iter()).flat_map(|inputs| inputs.values_mut());
    for value in pins {
        let listed: Vec<&mut Value> = match value {
            Value::Array(items) => items.iter_mut().collect(),
            other => vec![other],
        };
        for pin in listed.into_iter().filter_map(Value::as_object_mut) {
            if !pin.get("source_node_id").is_some_and(Value::is_string) {
                pin.insert("source_node_id".into(), Value::String(node.to_string()));
            }
        }
    }
}

/// The pinned inputs of a stored request: one pin or a list of pins per input name.
pub(crate) fn pins_of(inputs: &Value) -> Result<Vec<PinnedInput>, CatalogError> {
    let invalid = || CatalogError::Storage("the run's stored inputs do not read".into());
    let mut pins = Vec::new();
    for (name, value) in inputs.as_object().ok_or_else(invalid)? {
        let listed = match value {
            Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        for pin in listed {
            let text = |field: &str| pin.get(field).and_then(Value::as_str);
            let version = text("version_id").and_then(|version| version.parse().ok());
            let (Some(bucket), Some(key), Some(version_id)) =
                (text("bucket"), text("key"), version)
            else {
                return Err(invalid());
            };
            let source_node_id = match text("source_node_id") {
                Some(node) => Some(node.parse().map_err(|_| invalid())?),
                None => None,
            };
            pins.push(PinnedInput {
                name: name.clone(),
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                version_id,
                source_node_id,
            });
        }
    }
    Ok(pins)
}

impl Operation for FollowRunOperation {
    type Output = WorkflowRunRecord;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        let access = RunAccessOperation::new(&self.auth, self.source, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkflowRunRecord>, CatalogError> {
        match std::mem::replace(&mut self.step, FollowStep::Start) {
            FollowStep::Access => {
                let (source, _) = access_of(result)?;
                match source.state.is_active() {
                    true => self.active(source),
                    false => self.admit(&source),
                }
            }
            FollowStep::Admin(source) => {
                let allowed = result.child::<bool, AuthorizationError>()?;
                match allowed {
                    Ok(true) => self.admit(&source),
                    Ok(false) => Err(CatalogError::Forbidden),
                    Err(error) => Err(CatalogError::Storage(error.to_string())),
                }
            }
            FollowStep::Start => {
                let admitted = result.child::<AdmittedRun, CatalogError>()??;
                Ok(Next::Done(admitted.record))
            }
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(
            error,
            CatalogError::NotFound
                | CatalogError::Forbidden
                | CatalogError::Conflict(_)
                | CatalogError::Invalid(_)
        )
    }
}
