//! Authorized lifecycle transitions for an automation, checking feature switches and syncing
//! its service account.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::automation::record::Automation;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::workflow_permission_path;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::groups::admin_check::AdminCheckOperation;
use ulid::Ulid;

use super::change::{AutoChange, WriteAutoOperation};
use super::identity::SyncIdentityOperation;
use super::{AutoAccessOperation, AutoError, Need, allowed, authorize_path, check_feature};

#[derive(Debug, PartialEq)]
enum Step {
    Access,
    Config(Box<Automation>),
    AuthWorkflow(Box<Automation>),
    Admin,
    Write,
    Sync,
}

/// Checks caller rights, feature switches and human group admin for approval/rejection, writes
/// the automation change atomically, and syncs its service account and role when needed.
#[derive(Debug, PartialEq)]
pub struct ChangeAutoOperation {
    auth: AuthContext,
    id: Ulid,
    expected: Option<u64>,
    change: AutoChange,
    local: NodeId,
    disabled_features: BTreeSet<Feature>,
    step: Step,
}

impl ChangeAutoOperation {
    pub fn new(
        auth: &AuthContext,
        (id, expected): (Ulid, Option<u64>),
        change: AutoChange,
        local: NodeId,
    ) -> Self {
        Self {
            auth: auth.clone(),
            id,
            expected,
            change,
            local,
            disabled_features: BTreeSet::new(),
            step: Step::Access,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn uses_applets(&self, auto: &Automation) -> bool {
        match &self.change {
            AutoChange::Update {
                spec: Some(next_spec),
                ..
            } => next_spec.uses_applets(),
            _ => auto
                .pending
                .as_ref()
                .map_or(&auto.spec, |pending| &pending.spec)
                .uses_applets(),
        }
    }

    fn after_config(&mut self, auto: Automation) -> Next<Automation> {
        if self.change.edits_workflow_link() {
            let path = workflow_permission_path(self.auth.realm_id, auto.group_id);
            self.step = Step::AuthWorkflow(Box::new(auto));
            return Next::Call(authorize_path(&self.auth, &path, Permission::WRITE));
        }
        self.after_workflow_auth(&auto)
    }

    fn after_workflow_auth(&mut self, auto: &Automation) -> Next<Automation> {
        if self.change.needs_admin() {
            self.step = Step::Admin;
            let check = AdminCheckOperation::new(self.auth.clone(), auto.group_id);
            return Next::Call(IoCall::child(check));
        }
        self.write_call()
    }

    fn write_call(&mut self) -> Next<Automation> {
        self.step = Step::Write;
        let write = WriteAutoOperation::new(
            (self.id, self.expected),
            self.change.clone(),
            self.auth.user_id,
            self.auth.realm_id,
        );
        Next::Call(IoCall::retry(write, &self.id.to_bytes()))
    }
}

impl Operation for ChangeAutoOperation {
    type Output = Automation;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Automation>, AutoError> {
        self.step = Step::Access;
        let access = AutoAccessOperation::new(&self.auth, self.id, self.local, Need::Write);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Automation>, AutoError> {
        match std::mem::replace(&mut self.step, Step::Sync) {
            Step::Access => {
                let auto = result.child::<Automation, AutoError>()??;
                if self.change.requires_features() {
                    self.step = Step::Config(Box::new(auto));
                    return Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                        self.auth.realm_id,
                    ))));
                }
                Ok(self.after_config(auto))
            }
            Step::Config(auto) => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Automations,
                    auto.group_id,
                )?;
                if self.uses_applets(&auto) {
                    check_feature(
                        &config,
                        &self.disabled_features,
                        Feature::Applets,
                        auto.group_id,
                    )?;
                }
                Ok(self.after_config(*auto))
            }
            Step::AuthWorkflow(auto) => {
                if !allowed(result)? {
                    return Err(AutoError::Forbidden);
                }
                Ok(self.after_workflow_auth(&auto))
            }
            Step::Admin => {
                let is_admin = result
                    .child::<bool, AuthorizationError>()?
                    .map_err(|err| AutoError::Storage(err.to_string()))?;
                if !is_admin {
                    return Err(AutoError::Forbidden);
                }
                Ok(self.write_call())
            }
            Step::Write => {
                let auto = result.child::<Automation, AutoError>()??;
                if self.change.moves_identity() && (auto.wants_account() || auto.account.is_some())
                {
                    self.step = Step::Sync;
                    let sync = SyncIdentityOperation::new(&auto, (self.local, self.auth.realm_id));
                    return Ok(Next::Call(IoCall::child(sync)));
                }
                Ok(Next::Done(auto))
            }
            Step::Sync => Ok(Next::Done(result.child::<Automation, AutoError>()??)),
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}
