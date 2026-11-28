//! Links an applet to the group automations started when visitors submit it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::applet::record::Applet;
use aruna_core::automation::keys::record_key;
use aruna_core::automation::record::{Automation, Lifecycle};
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::AUTOMATIONS_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::features::Feature;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use ulid::Ulid;

use super::{AutoError, check_feature};
use crate::applets::change::{AppletChange, ChangeAppletOperation};
use crate::applets::{AppletAccessOperation, AppletError, Need as AppletNeed};

#[derive(Debug, PartialEq)]
enum LinkStep {
    Access,
    Config(Box<Applet>),
    ReadAutomations(Box<Applet>),
    Change,
}

fn map_applet(error: AppletError) -> AutoError {
    match error {
        AppletError::NotFound => AutoError::NotFound,
        AppletError::Forbidden => AutoError::Forbidden,
        AppletError::Invalid(reason) => AutoError::Invalid(reason),
        AppletError::Conflict(reason) => AutoError::Conflict(reason),
        AppletError::Limited(reason) => AutoError::Limited(reason),
        AppletError::Elsewhere(node) => AutoError::Conflict(format!(
            "the applet is hosted on node {node}; send changes there"
        )),
        AppletError::Storage(reason) => AutoError::Storage(reason),
    }
}

/// Validates feature switches, applet write rights and same-group automations, then updates the
/// applet's linked automation list in one atomic domain operation.
#[derive(Debug, PartialEq)]
pub struct LinkAppletOperation {
    auth: AuthContext,
    applet_id: Ulid,
    expected_revision: Option<u64>,
    automation_ids: Vec<Ulid>,
    local: NodeId,
    disabled_features: BTreeSet<Feature>,
    step: LinkStep,
}

impl LinkAppletOperation {
    pub fn new(
        auth: &AuthContext,
        (applet_id, expected_revision): (Ulid, Option<u64>),
        mut automation_ids: Vec<Ulid>,
        local: NodeId,
    ) -> Self {
        let mut seen = BTreeSet::new();
        automation_ids.retain(|id| seen.insert(*id));
        Self {
            auth: auth.clone(),
            applet_id,
            expected_revision,
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

    fn apply_change(&mut self) -> Next<Applet> {
        let op = ChangeAppletOperation::new(
            &self.auth,
            (self.applet_id, self.expected_revision),
            AppletChange::Automations(self.automation_ids.clone()),
            self.local,
        );
        self.step = LinkStep::Change;
        Next::Call(IoCall::child(op))
    }
}

impl Operation for LinkAppletOperation {
    type Output = Applet;
    type Error = AutoError;

    fn first(&mut self) -> Result<Next<Applet>, AutoError> {
        let access = AppletAccessOperation::new(&self.auth, self.applet_id, AppletNeed::Write);
        self.step = LinkStep::Access;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AutoError> {
        match std::mem::replace(&mut self.step, LinkStep::Change) {
            LinkStep::Access => {
                let applet = result.child::<Applet, AppletError>()?.map_err(map_applet)?;
                self.step = LinkStep::Config(Box::new(applet));
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.auth.realm_id,
                ))))
            }
            LinkStep::Config(applet) => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| {
                        AutoError::Unavailable("the realm feature policy cannot be read".into())
                    })?;
                check_feature(
                    &config,
                    &self.disabled_features,
                    Feature::Applets,
                    applet.group_id,
                )?;
                if !self.automation_ids.is_empty() {
                    check_feature(
                        &config,
                        &self.disabled_features,
                        Feature::Automations,
                        applet.group_id,
                    )?;
                    let reads = self
                        .automation_ids
                        .iter()
                        .map(|id| (AUTOMATIONS_KEYSPACE.to_string(), record_key(*id)))
                        .collect();
                    self.step = LinkStep::ReadAutomations(applet);
                    return Ok(Next::Call(IoCall::Read(reads)));
                }
                Ok(self.apply_change())
            }
            LinkStep::ReadAutomations(applet) => {
                let values = result.values()?;
                for (index, automation_id) in self.automation_ids.iter().enumerate() {
                    let Some(bytes) = values.get(index).and_then(Option::as_deref) else {
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
                }
                Ok(self.apply_change())
            }
            LinkStep::Change => {
                let updated = result.child::<Applet, AppletError>()?.map_err(map_applet)?;
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
