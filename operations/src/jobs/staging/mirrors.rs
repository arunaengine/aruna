//! Gives each imported file its further sources: the job's mappings of its source path and the
//! templates of its table row, each read through a connector of the job's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use aruna_core::structs::execution::job::{StagingJobCheckpoint, StagingJobSpec};
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::SourceMirror;
use aruna_core::structs::execution::table::source_path;
use aruna_core::structs::identity::auth::Permission;
use ulid::Ulid;

use super::ItemFailure;
use super::walk::{PageFailure, access_rules, connector_permission_path, record_failure};
use crate::auth::permission_rules::PermissionRules;
use crate::connectors::get_connector::{GetSourceError, GetSourceInput, GetSourceOperation};
use crate::driver::drive;
use crate::jobs::executor::JobContext;
use crate::staging::descriptor::build_source_descriptor;
use crate::staging::page::rows::PageEntry;

/// Adds the further sources of each entry. An entry whose further source is invalid or not
/// readable by the submitter is recorded as failed and dropped; a connector that is gone fails
/// the job. The connectors come back for the page to check again when it commits.
pub(super) async fn add_mirrors(
    ctx: &JobContext,
    (spec, rules): (&StagingJobSpec, &PermissionRules),
    next: &mut StagingJobCheckpoint,
    entries: Vec<(PageEntry, Vec<(Ulid, String)>)>,
) -> Result<(Vec<PageEntry>, Vec<SourceConnector>), PageFailure> {
    let mut connectors: HashMap<Ulid, SourceConnector> = HashMap::new();
    let mut kept = Vec::with_capacity(entries.len());
    'entries: for (mut entry, rendered) in entries {
        let mapped = (spec.mirrors.iter())
            .filter_map(|mapping| Some((mapping.connector_id, mapping.map(&entry.source_path)?)));
        for (connector_id, value) in mapped.chain(rendered) {
            let connector = match connectors.entry(connector_id) {
                Entry::Occupied(found) => found.into_mut(),
                Entry::Vacant(entry) => {
                    let found = group_connector(ctx, spec, connector_id).await;
                    entry.insert(found.map_err(|failure| match failure {
                        ItemFailure::Lasting(message) => PageFailure::Permanent(message),
                        ItemFailure::Denied(message)
                        | ItemFailure::Stage(message)
                        | ItemFailure::System(message) => PageFailure::Retry(message),
                    })?)
                }
            };
            let path = source_path(&value, connector.base_url().as_deref()).and_then(|path| {
                let allowed = connector_permission_path(spec, connector_id, &path);
                match rules.allows(&allowed, &Permission::READ) {
                    true => Ok(path),
                    false => Err("permission denied".to_string()),
                }
            });
            let path = match path {
                Ok(path) => path,
                Err(reason) => {
                    record_failure(next, &entry.source_path, &entry.target_key, reason);
                    continue 'entries;
                }
            };
            let first = connector_id == spec.connector_id && path == entry.source_path;
            let known = (entry.mirrors.iter()).any(|mirror| {
                mirror.connector_id == Some(connector_id) && mirror.descriptor.source_path == path
            });
            if first || known {
                continue;
            }
            let node = Some(spec.node_id);
            let mut descriptor = build_source_descriptor(connector, &entry.observation, path, node);
            // A version id names one server's copy, never another's.
            descriptor.version_selector = None;
            entry.mirrors.push(SourceMirror {
                descriptor,
                connector_id: Some(connector_id),
            });
        }
        kept.push(entry);
    }
    Ok((kept, connectors.into_values().collect()))
}

/// The further sources of one listed file, each readable by the submitter, with their
/// connectors for the write to check again when it commits.
pub(super) async fn item_mirrors(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    file_path: &str,
) -> Result<Vec<(SourceConnector, String)>, ItemFailure> {
    if spec.mirrors.is_empty() {
        return Ok(Vec::new());
    }
    let rules = access_rules(ctx, spec).await.map_err(ItemFailure::System)?;
    let mut mirrors = Vec::with_capacity(spec.mirrors.len());
    for mapping in &spec.mirrors {
        let Some(value) = mapping.map(file_path) else {
            continue;
        };
        let connector = group_connector(ctx, spec, mapping.connector_id).await?;
        let path =
            source_path(&value, connector.base_url().as_deref()).map_err(ItemFailure::Stage)?;
        let allowed = connector_permission_path(spec, mapping.connector_id, &path);
        if !rules.allows(&allowed, &Permission::READ) {
            return Err(ItemFailure::Denied("permission denied".to_string()));
        }
        mirrors.push((connector, path));
    }
    Ok(mirrors)
}

/// A connector of the job's group, so a job never reads through another group's connector.
async fn group_connector(
    ctx: &JobContext,
    spec: &StagingJobSpec,
    connector_id: Ulid,
) -> Result<SourceConnector, ItemFailure> {
    let input = GetSourceInput {
        group_id: spec.group_id,
        connector_id,
    };
    match drive(GetSourceOperation::new(input), &ctx.driver).await {
        Ok(found) => Ok(found.connector),
        Err(GetSourceError::NotFound) => Err(ItemFailure::Lasting(format!(
            "the mirror connector {connector_id} is gone"
        ))),
        Err(error) => Err(ItemFailure::System(error.to_string())),
    }
}
