//! Starts the applet's granted workflow for a visitor or guest as the applet's own account, and
//! decides which runs a visitor may read: those writing below the visitor's own folder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{Applet, StartGrant};
use aruna_core::applet::usage::caps;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::permission_path::pattern_overlaps_subtree;
use aruna_core::structs::execution::workflow::workflow_permission_path;
use aruna_core::structs::execution::workflow_run::{RunOutput, WorkflowRunRecord};
use aruna_core::structs::identity::auth::{AuthContext, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
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
    let inputs_json = if pinned.is_empty() {
        None
    } else {
        let mut by_name: std::collections::BTreeMap<String, Vec<Value>> =
            std::collections::BTreeMap::new();
        for pin in &pinned {
            by_name
                .entry(pin.name.clone())
                .or_default()
                .push(serde_json::json!({
                    "bucket": pin.bucket,
                    "key": pin.key,
                    "version_id": pin.version_id.to_string(),
                    "source_node_id": pin.source_node_id.map(|n| n.to_string()),
                }));
        }
        let map: serde_json::Map<String, Value> = by_name
            .into_iter()
            .map(|(name, mut items)| {
                let value = match items.len() {
                    1 => items.pop().unwrap_or(Value::Null),
                    _ => Value::Array(items),
                };
                (name, value)
            })
            .collect();
        serde_json::to_string(&Value::Object(map)).ok()
    };
    Ok(RunRequest {
        group_id: applet.group_id,
        workflow_id,
        version,
        values,
        inputs: pinned,
        inputs_json,
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
    /// Narrows the applet account to this visitor's submission folder plus disjoint reference paths
    /// so file-valued workflow parameters cannot read another visitor's uploads.
    fn visitor_auth(&self) -> Result<AuthContext, AppletError> {
        let mut auth = applet_auth(&self.applet, self.node.1).ok_or(AppletError::Forbidden)?;
        let (bucket, folder) = visitor_folder(&self.applet, &self.visit)?;
        let sub_root = self
            .applet
            .grants
            .submissions
            .as_ref()
            .map(|sub| sub.permission_root(self.node.1, self.applet.group_id, self.node.0))
            .unwrap_or_else(|| {
                bucket_permission_path(self.node.1, self.applet.group_id, self.node.0, &bucket)
            });
        let mut restrictions: Vec<PathRestriction> = self
            .applet
            .grants
            .read
            .iter()
            .chain(&self.applet.grants.references)
            .filter(|pattern| !pattern_overlaps_subtree(pattern, &sub_root))
            .map(|pattern| PathRestriction {
                pattern: pattern.clone(),
                permission: Permission::READ,
            })
            .collect();
        let prefix = folder.trim_end_matches('/');
        let path = object_permission_path(
            self.node.1,
            self.applet.group_id,
            self.node.0,
            &bucket,
            prefix,
        );
        restrictions.push(PathRestriction {
            pattern: format!("{path}/**"),
            permission: Permission::WRITE,
        });
        restrictions.push(PathRestriction {
            pattern: workflow_permission_path(self.node.1, self.applet.group_id),
            permission: Permission::WRITE,
        });
        auth.path_restrictions = Some(restrictions);
        Ok(auth)
    }
}

impl Operation for VisitRunOperation {
    type Output = AdmittedRun;
    type Error = AppletError;

    fn first(&mut self) -> Result<Next<AdmittedRun>, AppletError> {
        self.visitor_auth()?;
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
        let auth = self.visitor_auth()?;
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

        // Visitor auth drops read/reference patterns overlapping the submission folder.
        let realm = aruna_core::structs::identity::realm::RealmId::from_bytes([7; 32]);
        let root = aruna_core::permission_path::role_subtree_root(realm, applet.group_id);
        let node = applet.host_node;
        applet.account = Some(aruna_core::user::id::UserId::local(
            Ulid::from_bytes([9; 16]),
            realm,
        ));
        applet.role_id = Some(Ulid::from_bytes([8; 16]));
        applet.grants.read = vec![
            format!("{root}/data/{node}/uploads/**"),
            format!("{root}/data/{node}/uploads/b\\akta/**"),
            format!("{root}/data/{node}/uploads/ref/**"),
        ];
        applet.grants.references = vec![
            format!("{root}/data/*/uploads/bakta/**"),
            format!("{root}/data/{node}/other/**"),
        ];
        let op = VisitRunOperation::new(
            &applet,
            visit,
            (node, realm),
            RunRequest {
                group_id: applet.group_id,
                workflow_id: Ulid::from_bytes([2; 16]),
                version: 3,
                values: Value::Object(Default::default()),
                inputs: Vec::new(),
                inputs_json: None,
                output: None,
                provenance_crate: false,
                idempotency_key: None,
                placement_site: None,
                secrets: Default::default(),
            },
        );
        let auth = op.visitor_auth().unwrap();
        let patterns: Vec<&str> = auth
            .path_restrictions
            .as_deref()
            .unwrap()
            .iter()
            .map(|r| r.pattern.as_str())
            .collect();
        assert!(patterns.contains(&format!("{root}/data/{node}/uploads/ref/**").as_str()));
        assert!(patterns.contains(&format!("{root}/data/{node}/other/**").as_str()));
        assert!(!patterns.contains(&format!("{root}/data/{node}/uploads/**").as_str()));
        assert!(!patterns.contains(&format!("{root}/data/{node}/uploads/b\\akta/**").as_str()));
        assert!(!patterns.contains(&format!("{root}/data/*/uploads/bakta/**").as_str()));

        // A leading slash in the submission prefix matches intake_key rather than being trimmed.
        applet.grants.submissions = Some(Folder {
            bucket: "uploads".into(),
            prefix: "/bakta/".into(),
        });
        applet.grants.read = vec![format!("{root}/data/{node}/uploads//bakta/**")];
        applet.grants.references = Vec::new();
        let leading = VisitRunOperation::new(
            &applet,
            Visit {
                pseudonym: "ab".into(),
                guest: false,
            },
            (node, realm),
            RunRequest {
                group_id: applet.group_id,
                workflow_id: Ulid::from_bytes([2; 16]),
                version: 3,
                values: Value::Object(Default::default()),
                inputs: Vec::new(),
                inputs_json: None,
                output: None,
                provenance_crate: false,
                idempotency_key: None,
                placement_site: None,
                secrets: Default::default(),
            },
        )
        .visitor_auth()
        .unwrap();
        let leading_pats: Vec<&str> = leading
            .path_restrictions
            .as_deref()
            .unwrap()
            .iter()
            .map(|r| r.pattern.as_str())
            .collect();
        assert!(!leading_pats.contains(&format!("{root}/data/{node}/uploads//bakta/**").as_str()));
    }
}
