//! Starts the applet's granted workflow for a visitor or guest as the applet's own account, and
//! decides which runs a visitor may read: those writing below the visitor's own folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{Applet, StartGrant};
use aruna_core::applet::usage::caps;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow_run::{RunOutput, WorkflowRunRecord};
use aruna_core::structs::identity::realm::RealmId;
use serde_json::Value;
use ulid::Ulid;

use super::AppletError;
use super::intake::{Visit, applet_auth};
use super::visits::CountVisitOperation;
use crate::workflows::CatalogError;
use crate::workflows::runs::admit::{AdmittedRun, PinnedInput, RunRequest, StartRunOperation};

/// One hand-in of the visitor that fills a workflow input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInput {
    pub name: String,
    pub key: String,
    pub version_id: Ulid,
}

/// The visitor's own folder in the submission folder, ending in `/`.
fn visitor_folder(applet: &Applet, visit: &Visit) -> Result<(String, String), AppletError> {
    let folder = (applet.grants.submissions.as_ref()).ok_or_else(|| {
        AppletError::Conflict("runs need a submission folder for their results".into())
    })?;
    let prefix = match folder.prefix.trim_end_matches('/') {
        "" => format!("{}/", visit.pseudonym),
        prefix => format!("{prefix}/{}/", visit.pseudonym),
    };
    Ok((folder.bucket.clone(), prefix))
}

/// The run a visitor's start asks for: the granted workflow version, inputs only from the
/// visitor's own hand-ins, and results below the visitor's folder.
pub fn run_request(
    applet: &Applet,
    visit: &Visit,
    values: Value,
    inputs: Vec<RunInput>,
) -> Result<RunRequest, AppletError> {
    let (workflow_id, version) = match &applet.grants.start {
        Some(StartGrant::Workflow {
            workflow_id,
            version,
        }) => (*workflow_id, *version),
        Some(StartGrant::Tool { .. }) => {
            return Err(AppletError::Conflict(
                "a tool grant cannot start runs yet; grant a workflow instead".into(),
            ));
        }
        None => return Err(AppletError::Forbidden),
    };
    let (bucket, folder) = visitor_folder(applet, visit)?;
    let mut pinned = Vec::with_capacity(inputs.len());
    for input in inputs {
        if !input.key.starts_with(&folder) || input.key.contains("/../") {
            return Err(AppletError::Forbidden);
        }
        pinned.push(PinnedInput {
            name: input.name,
            bucket: bucket.clone(),
            key: input.key,
            version_id: input.version_id,
            source_node_id: Some(applet.host_node),
        });
    }
    Ok(RunRequest {
        group_id: applet.group_id,
        workflow_id,
        version,
        values,
        inputs: pinned,
        inputs_json: None,
        output: Some(RunOutput {
            bucket,
            prefix: format!("{folder}runs"),
            node_id: Some(applet.host_node),
            dataset_json: None,
        }),
        provenance_crate: false,
        idempotency_key: None,
        placement_site: None,
        secrets: std::collections::BTreeMap::new(),
    })
}

/// Whether `record` is a run of this applet that writes below the visitor's own folder.
pub fn visitor_run(applet: &Applet, visit: &Visit, record: &WorkflowRunRecord) -> bool {
    let Ok((bucket, folder)) = visitor_folder(applet, visit) else {
        return false;
    };
    let ours = applet.account == Some(record.created_by) && record.group_id == applet.group_id;
    ours && (record.output.as_ref())
        .is_some_and(|output| output.bucket == bucket && output.prefix.starts_with(&folder))
}

pub(crate) fn catalog_error(error: CatalogError) -> AppletError {
    match error {
        CatalogError::NotFound => AppletError::NotFound,
        CatalogError::Forbidden => AppletError::Forbidden,
        CatalogError::Invalid(reason) | CatalogError::Unsupported(reason) => {
            AppletError::Invalid(reason)
        }
        CatalogError::Conflict(reason) => AppletError::Conflict(reason),
        other => AppletError::Storage(other.to_string()),
    }
}

/// Counts the start against the visitor's caps, then admits the run as the applet's account.
#[derive(Debug, PartialEq)]
pub struct VisitRunOperation {
    applet: Applet,
    visit: Visit,
    node: (NodeId, RealmId),
    request: Option<RunRequest>,
}

impl VisitRunOperation {
    pub fn new(
        applet: &Applet,
        visit: Visit,
        node: (NodeId, RealmId),
        request: RunRequest,
    ) -> Self {
        Self {
            applet: applet.clone(),
            visit,
            node,
            request: Some(request),
        }
    }
}

impl Operation for VisitRunOperation {
    type Output = AdmittedRun;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<AdmittedRun>, AppletError> {
        applet_auth(&self.applet, self.node.1).ok_or(AppletError::Forbidden)?;
        let caps = caps(&self.applet, self.visit.guest);
        let id = self.applet.applet_id;
        let count = CountVisitOperation::new(id, &self.visit.pseudonym, caps);
        Ok(Next::Call(IoCall::child(count)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdmittedRun>, AppletError> {
        let Some(request) = self.request.take() else {
            let admitted = result.child::<AdmittedRun, CatalogError>()?;
            return admitted.map(Next::Done).map_err(catalog_error);
        };
        result.child::<(), AppletError>()??;
        let auth = applet_auth(&self.applet, self.node.1).ok_or(AppletError::Forbidden)?;
        let start = StartRunOperation::new(&auth, self.node, request);
        Ok(Next::Call(IoCall::child(start)))
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::applets::change::tests::applet;
    use aruna_core::applet::record::Folder;

    // A visitor starts only the granted workflow with its own hand-ins and results below its
    // folder; a tool grant or another visitor's file is refused.
    #[test]
    fn runs_stay_inside() {
        let mut applet = applet();
        let visit = Visit {
            pseudonym: "ab".into(),
            guest: true,
        };
        let input = |key: &str| RunInput {
            name: "reads".into(),
            key: key.into(),
            version_id: Ulid::from_bytes([1; 16]),
        };
        let start =
            |applet: &Applet, key: &str| run_request(applet, &visit, Value::Null, vec![input(key)]);
        assert!(matches!(start(&applet, "x"), Err(AppletError::Forbidden)));
        applet.grants.start = Some(StartGrant::Workflow {
            workflow_id: Ulid::from_bytes([2; 16]),
            version: 3,
        });
        assert!(matches!(start(&applet, "x"), Err(AppletError::Conflict(_))));
        applet.grants.submissions = Some(Folder {
            bucket: "uploads".into(),
            prefix: "bakta/".into(),
        });
        let request = start(&applet, "bakta/ab/1-reads.fa").unwrap();
        assert_eq!((request.version, request.inputs.len()), (3, 1));
        assert_eq!(request.output.unwrap().prefix, "bakta/ab/runs");
        for refused in ["bakta/cd/1-reads.fa", "bakta/ab/../cd/1-reads.fa", "other"] {
            assert!(matches!(
                start(&applet, refused),
                Err(AppletError::Forbidden)
            ));
        }
        applet.grants.start = Some(StartGrant::Tool {
            tool_id: "bakta".into(),
            version: "1".into(),
        });
        assert!(matches!(
            start(&applet, "bakta/ab/x"),
            Err(AppletError::Conflict(_))
        ));
    }
}
