//! Links an applet to the group automations started when visitors submit it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::{Access, Applet, Lifecycle as AppletLifecycle, applet_key};
use aruna_core::automation::keys::record_key;
use aruna_core::automation::record::{Automation, Lifecycle, Trigger};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{APPLETS_KEYSPACE, AUTOMATIONS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::features::Feature;
use aruna_ops_identity::document_repository::GetConfigOperation;
use ulid::Ulid;

use super::{AutoError, check_features};
use crate::applets::{AppletAccessOperation, AppletError, Need as AppletNeed, record_rows};

/// Target applet and optional optimistic revision for [`LinkAppletOperation`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppletLinkTarget {
    pub applet_id: Ulid,
    pub expected_revision: Option<u64>,
}

#[derive(Debug, PartialEq)]
enum LinkStep {
    Access,
    Config(Box<Applet>),
    Write,
}

fn map_applet(error: AppletError) -> AutoError {
    match error {
        AppletError::NotFound => AutoError::NotFound,
        AppletError::Forbidden => AutoError::Forbidden,
        AppletError::Invalid(reason) => AutoError::Invalid(reason),
        AppletError::Conflict(reason) => AutoError::Conflict(reason),
        AppletError::Limited(reason) => AutoError::limited(reason, 60),
        AppletError::Elsewhere(node) => AutoError::Conflict(format!(
            "the applet is hosted on node {node}; send changes there"
        )),
        AppletError::Storage(reason) => AutoError::Storage(reason),
    }
}

/// Validates current applet exposure and target automations and writes `applet.automations` in
/// one transaction so concurrent exposure or automation approvals cannot make validation stale.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct WriteLinkOperation {
    target: AppletLinkTarget,
    automation_ids: Vec<Ulid>,
    local: NodeId,
    updated: Option<Box<Applet>>,
}

impl WriteLinkOperation {
    pub(crate) fn new(target: AppletLinkTarget, automation_ids: Vec<Ulid>, local: NodeId) -> Self {
        Self {
            target,
            automation_ids,
            local,
            updated: None,
        }
    }
}

impl Operation for WriteLinkOperation {
    type Output = Applet;
    type Error = AutoError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn first(&mut self) -> Result<Next<Applet>, AutoError> {
        let mut reads = Vec::with_capacity(self.automation_ids.len() + 1);
        reads.push((
            APPLETS_KEYSPACE.to_string(),
            applet_key(self.target.applet_id),
        ));
        for automation_id in &self.automation_ids {
            reads.push((AUTOMATIONS_KEYSPACE.to_string(), record_key(*automation_id)));
        }
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AutoError> {
        if let Some(applet) = self.updated.take() {
            result.done()?;
            return Ok(Next::Done(*applet));
        }
        let values = result.values()?;
        let Some(applet_bytes) = values.first().and_then(Option::as_deref) else {
            return Err(AutoError::NotFound);
        };
        let mut applet = Applet::from_bytes(applet_bytes)?;
        if applet.host_node != self.local {
            return Err(AutoError::Conflict(format!(
                "the applet is hosted on node {}; send changes there",
                applet.host_node
            )));
        }
        if self
            .target
            .expected_revision
            .is_some_and(|expected| expected != applet.revision)
        {
            return Err(AutoError::Conflict(
                "the applet changed meanwhile; read it again".into(),
            ));
        }
        if applet.lifecycle == AppletLifecycle::TakenDown {
            return Err(AutoError::Conflict("the applet is taken down".into()));
        }
        let applet_outside = applet.access == Access::Public || applet.guests.is_some();
        for (index, automation_id) in self.automation_ids.iter().enumerate() {
            let Some(bytes) = values.get(index + 1).and_then(Option::as_deref) else {
                return Err(AutoError::NotFound);
            };
            let automation = Automation::from_bytes(bytes)?;
            if automation.lifecycle == Lifecycle::Removed {
                return Err(AutoError::NotFound);
            }
            if automation.group_id != applet.group_id {
                return Err(AutoError::Invalid(format!(
                    "automation `{automation_id}` belongs to a different group"
                )));
            }
            let wildcard =
                automation.spec.triggers.iter().any(|trigger| {
                    matches!(trigger, Trigger::AppletSubmission { applet_id: None })
                });
            if applet_outside
                && wildcard
                && automation.approved_by.is_some()
                && !automation.spec.scope.allow_outside_input
            {
                return Err(AutoError::Conflict(format!(
                    "automation `{automation_id}` is not approved for outside input; approve scope.allow_outside_input first"
                )));
            }
        }
        applet.automations = self.automation_ids.clone();
        applet.revision += 1;
        let writes = record_rows(&applet).map_err(map_applet)?;
        self.updated = Some(Box::new(applet));
        Ok(Next::Call(IoCall::Write(writes)))
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

/// Validates feature switches and applet write rights, then atomically validates target
/// automations and updates the applet's linked automation list in one write transaction.
#[derive(Debug, PartialEq)]
pub struct LinkAppletOperation {
    auth: AuthContext,
    target: AppletLinkTarget,
    automation_ids: Vec<Ulid>,
    local: NodeId,
    disabled_features: BTreeSet<Feature>,
    step: LinkStep,
}

impl LinkAppletOperation {
    pub fn new(
        auth: &AuthContext,
        target: AppletLinkTarget,
        mut automation_ids: Vec<Ulid>,
        local: NodeId,
    ) -> Self {
        let mut seen = BTreeSet::new();
        automation_ids.retain(|id| seen.insert(*id));
        Self {
            auth: auth.clone(),
            target,
            automation_ids,
            local,
            disabled_features: BTreeSet::new(),
            step: LinkStep::Access,
        }
    }

    pub fn with_disabled_features(mut self, disabled: BTreeSet<Feature>) -> Self {
        self.disabled_features = disabled;
        self
    }

    fn write_links(&mut self) -> Next<Applet> {
        let write = WriteLinkOperation::new(self.target, self.automation_ids.clone(), self.local);
        self.step = LinkStep::Write;
        Next::Call(IoCall::retry(write, &self.target.applet_id.to_bytes()))
    }
}

impl Operation for LinkAppletOperation {
    type Output = Applet;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Applet>, AutoError> {
        let access =
            AppletAccessOperation::new(&self.auth, self.target.applet_id, AppletNeed::Write);
        self.step = LinkStep::Access;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AutoError> {
        match std::mem::replace(&mut self.step, LinkStep::Write) {
            LinkStep::Access => {
                let applet = result.child::<Applet, AppletError>()?.map_err(map_applet)?;
                if applet.host_node != self.local {
                    return Err(AutoError::Conflict(format!(
                        "the applet is hosted on node {}; send changes there",
                        applet.host_node
                    )));
                }
                self.step = LinkStep::Config(Box::new(applet));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.auth.realm_id,
                ))))
            }
            LinkStep::Config(applet) => {
                let features: &[Feature] = if self.automation_ids.is_empty() {
                    &[Feature::Applets]
                } else {
                    &[Feature::Applets, Feature::Automations]
                };
                check_features(result, &self.disabled_features, applet.group_id, features)?;
                Ok(self.write_links())
            }
            LinkStep::Write => {
                let updated = result.child::<Applet, AutoError>()??;
                Ok(Next::Done(updated))
            }
        }
    }

    fn expected_error(error: &AutoError) -> bool {
        !matches!(error, AutoError::Storage(_))
    }
}

#[cfg(test)]
#[path = "applet_link_tests.rs"]
mod tests;
