//! Rereads an import rule and everything it depends on before each listing page, so a removed
//! rule, a disabled backend or a lost permission stops the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BUCKET_CREATION_KEYSPACE, IMPORT_DISCOVERY_KEYSPACE,
    IMPORT_PATTERN_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::{PrefixPages, values};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::backend_import::{
    BackendImport, import_key, object_prefix, read_patterns,
};
use aruna_core::structs::storage::blob::{BucketInfo, bucket_permission_path, incarnation};
use aruna_core::structs::storage::discovery::{DiscoveryRule, DiscoveryRules};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::Value;

use super::reads::read_call;
use super::{ImportJob, ImportRun, PageReads, permanent, retryable};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::backends::query::{GetBackendOperation, GroupQueryError};
use crate::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use crate::jobs::executor::JobRunOutcome;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::imports::shared::{SharedRule, shares_folder};

/// Rule rows one page of the group's rules reads.
const RULE_PAGE: usize = 256;

/// Why a run cannot go on: the outcome the job ends with.
#[derive(Debug, PartialEq)]
pub(super) struct RunEnd(pub(super) JobRunOutcome);

impl From<IoError> for RunEnd {
    fn from(error: IoError) -> Self {
        Self(retryable(error))
    }
}

fn stop(error: impl ToString) -> RunEnd {
    RunEnd(retryable(error))
}

fn lost() -> RunEnd {
    stop("the import run lost its state")
}

/// The earlier write patterns and the discovery rules stored for one rule.
fn rule_extras(
    earlier: Option<Value>,
    discovery: Option<Value>,
) -> Result<(Vec<String>, Vec<DiscoveryRule>), RunEnd> {
    let earlier = read_patterns(earlier.as_deref()).map_err(stop)?;
    let discovery = discovery
        .map(|value| DiscoveryRules::from_bytes(&value))
        .transpose()
        .map_err(stop)?;
    Ok((earlier, discovery.map_or_else(Vec::new, |rules| rules.rules)))
}

fn extras_call(rule: &BackendImport) -> IoCall {
    read_call([
        (IMPORT_PATTERN_KEYSPACE, object_prefix(rule.import_id)),
        (
            IMPORT_DISCOVERY_KEYSPACE,
            import_key(rule.group_id, rule.import_id),
        ),
    ])
}

#[derive(Debug, PartialEq)]
enum Step {
    Rule,
    Extras,
    Backend,
    Bucket,
    Creation,
    Allowed,
    Routing,
    Quota,
    Gate,
    Rules,
    Shared,
}

/// Everything a page of the run is checked against, read again for each page.
#[derive(Debug, PartialEq)]
pub(super) struct PrepareRunOperation {
    job: ImportJob,
    now_ms: u64,
    rule: Option<BackendImport>,
    earlier: Vec<String>,
    discovery: Vec<DiscoveryRule>,
    backend: Option<GroupStorage>,
    bucket: Option<BucketInfo>,
    incarnation: u128,
    routing: Option<RoutingSnapshot>,
    quota: Option<u64>,
    gate: Option<GateContext>,
    pages: Option<PrefixPages>,
    others: Vec<BackendImport>,
    shared: Vec<SharedRule>,
    step: Step,
}

impl PrepareRunOperation {
    pub(super) fn new(job: ImportJob) -> Self {
        Self {
            job,
            now_ms: 0,
            rule: None,
            earlier: Vec::new(),
            discovery: Vec::new(),
            backend: None,
            bucket: None,
            incarnation: 0,
            routing: None,
            quota: None,
            gate: None,
            pages: None,
            others: Vec::new(),
            shared: Vec::new(),
            step: Step::Rule,
        }
    }

    fn rule(&self) -> Result<&BackendImport, RunEnd> {
        self.rule.as_ref().ok_or_else(lost)
    }

    fn next_shared(&mut self) -> Result<Next<ImportRun>, RunEnd> {
        match self.others.last() {
            Some(other) => {
                self.step = Step::Shared;
                Ok(Next::Call(extras_call(other)))
            }
            None => self.finish(),
        }
    }

    fn finish(&mut self) -> Result<Next<ImportRun>, RunEnd> {
        Ok(Next::Done(ImportRun {
            rule: self.rule.take().ok_or_else(lost)?,
            earlier: std::mem::take(&mut self.earlier),
            backend: self.backend.take().ok_or_else(lost)?,
            bucket: self.bucket.take().ok_or_else(lost)?,
            incarnation: self.incarnation,
            routing: self.routing.take().ok_or_else(lost)?,
            quota: self.quota,
            gate: self.gate.take(),
            intents: BTreeMap::new(),
            discovery: std::mem::take(&mut self.discovery),
            shared: std::mem::take(&mut self.shared),
            page: PageReads::default(),
        }))
    }
}

impl Operation for PrepareRunOperation {
    type Output = ImportRun;
    type Error = RunEnd;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<ImportRun>, RunEnd> {
        let spec = &self.job.spec;
        let key = import_key(spec.group_id, spec.import_id);
        Ok(Next::Call(read_call([(BACKEND_IMPORT_KEYSPACE, key)])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ImportRun>, RunEnd> {
        let spec = self.job.spec.clone();
        let realm_id = spec.auth_context.realm_id;
        match self.step {
            Step::Rule => {
                let [row] = values::<1>(result)?;
                let Some(row) = row else {
                    return Err(RunEnd(permanent("the import rule was removed")));
                };
                let rule = BackendImport::from_bytes(&row).map_err(stop)?;
                let call = extras_call(&rule);
                self.rule = Some(rule);
                self.step = Step::Extras;
                Ok(Next::Call(call))
            }
            Step::Extras => {
                let [earlier, discovery] = values::<2>(result)?;
                (self.earlier, self.discovery) = rule_extras(earlier, discovery)?;
                self.step = Step::Backend;
                let backend = GetBackendOperation::new(self.rule()?.backend_id);
                Ok(Next::Call(IoCall::child(backend)))
            }
            Step::Backend => {
                let found = result.child::<Option<GroupStorage>, GroupQueryError>()?;
                let group_id = self.rule()?.group_id;
                match found.map_err(stop)? {
                    Some(backend) if backend.group_id == group_id && !backend.disabled => {
                        self.backend = Some(backend)
                    }
                    _ => {
                        let message = "the storage backend is missing or disabled";
                        return Err(RunEnd(permanent(message)));
                    }
                }
                self.step = Step::Bucket;
                let bucket = GetBucketOperation::new(self.rule()?.bucket.clone());
                Ok(Next::Call(IoCall::child(bucket)))
            }
            Step::Bucket => {
                let rule = self.rule()?;
                let group_id = rule.group_id;
                let name = rule.bucket.as_bytes().to_vec();
                match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(bucket) if bucket.group_id == group_id => self.bucket = Some(bucket),
                    Ok(_) | Err(GetBucketError::NotFound) => {
                        let message = "the bucket no longer exists in this group";
                        return Err(RunEnd(permanent(message)));
                    }
                    Err(error) => return Err(stop(error)),
                }
                self.step = Step::Creation;
                Ok(Next::Call(read_call([(
                    BUCKET_CREATION_KEYSPACE,
                    name.into(),
                )])))
            }
            Step::Creation => {
                let [creation] = values::<1>(result)?;
                self.incarnation = incarnation(creation.as_deref());
                let rule = self.rule()?;
                let path =
                    bucket_permission_path(realm_id, rule.group_id, spec.node_id, &rule.bucket);
                let check = CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context: spec.auth_context.clone(),
                    path,
                    required_permission: Permission::WRITE,
                });
                self.step = Step::Allowed;
                Ok(Next::Call(IoCall::child(check)))
            }
            Step::Allowed => {
                let allowed = result.child::<bool, AuthorizationError>()?.map_err(stop)?;
                if !allowed {
                    let message = "the importing user may not write to the bucket";
                    return Err(RunEnd(permanent(message)));
                }
                let rule = self.rule()?;
                let routing = SnapshotOperation::new(
                    &self.job.routing,
                    rule.group_id,
                    BucketRules::Named(rule.bucket.to_string()),
                );
                self.step = Step::Routing;
                Ok(Next::Call(IoCall::child(routing)))
            }
            Step::Routing => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>()?;
                self.routing = Some(routing.map_err(stop)?);
                self.step = Step::Quota;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
            }
            Step::Quota => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(stop)?;
                let group_id = self.rule()?.group_id;
                self.quota = config.quota.effective_group_ceiling(&group_id);
                self.step = Step::Gate;
                let gate = GateOperation::new(realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            Step::Gate => {
                let gate = result.child::<Option<GateContext>, GateContextError>()?;
                self.gate = gate.map_err(stop)?;
                let prefix = self.rule()?.group_id.to_bytes().to_vec();
                let pages = PrefixPages::new(BACKEND_IMPORT_KEYSPACE, prefix, RULE_PAGE);
                let call = pages.call();
                self.pages = Some(pages);
                self.step = Step::Rules;
                Ok(Next::Call(call))
            }
            Step::Rules => {
                let rule = self.rule()?.clone();
                let pages = self.pages.as_mut().ok_or_else(lost)?;
                for (_, value) in pages.take(result)? {
                    let other = BackendImport::from_bytes(&value).map_err(stop)?;
                    if shares_folder(&rule, &other) {
                        self.others.push(other);
                    }
                }
                if !pages.done() {
                    return Ok(Next::Call(pages.call()));
                }
                // Taken from the end: the rules stay in key order.
                self.others.reverse();
                self.next_shared()
            }
            Step::Shared => {
                let [earlier, discovery] = values::<2>(result)?;
                let rule = self.others.pop().ok_or_else(lost)?;
                let (earlier, discovery) = rule_extras(earlier, discovery)?;
                self.shared.push(SharedRule {
                    rule,
                    earlier,
                    discovery,
                });
                self.next_shared()
            }
        }
    }
}
