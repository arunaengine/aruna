//! Gives each imported file its further sources: the job's mappings of its source path and the
//! templates of its table row, each read through a connector of the job's group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::StagingJobCheckpoint;
use aruna_core::structs::execution::source_connector::SourceConnector;
use aruna_core::structs::execution::staging::SourceMirror;
use aruna_core::structs::execution::table::mirror_path;
use aruna_core::structs::identity::auth::Permission;
use ulid::Ulid;

use super::walk::{connector_permission_path, record_failure, rules_call, rules_result};
use super::{ItemFailure, PageFailure, StagingRun};
use crate::auth::permission_rules::PermissionRules;
use crate::connectors::descriptor::build_source_descriptor;
use crate::connectors::get_connector::{
    GetSourceError, GetSourceInput, GetSourceOperation, GetSourceResult,
};
use crate::staging::page::rows::PageEntry;

fn connector_call(run: &StagingRun, connector_id: Ulid) -> IoCall {
    IoCall::child(GetSourceOperation::new(GetSourceInput {
        group_id: run.spec.group_id,
        connector_id,
    }))
}

/// A connector of the job's group, so a job never reads through another group's connector.
fn connector_result(result: IoResult, connector_id: Ulid) -> Result<SourceConnector, ItemFailure> {
    match result.child::<GetSourceResult, GetSourceError>()? {
        Ok(found) => Ok(found.connector),
        Err(GetSourceError::NotFound) => Err(ItemFailure::Lasting(format!(
            "the mirror connector {connector_id} is gone"
        ))),
        Err(error) => Err(ItemFailure::System(error.to_string())),
    }
}

/// A page's entries with their further sources, and the connectors its commit checks again.
#[derive(Debug, PartialEq)]
pub(super) struct MirroredPage {
    pub(super) next: StagingJobCheckpoint,
    pub(super) entries: Vec<PageEntry>,
    pub(super) connectors: Vec<SourceConnector>,
}

/// The entry being worked on and the further sources it still has to add.
#[derive(Debug, PartialEq)]
struct OpenEntry {
    entry: PageEntry,
    sources: VecDeque<(Ulid, String)>,
}

/// Adds the further sources of each entry. An entry whose further source is invalid or not
/// readable by the submitter is recorded as failed and dropped; a connector that is gone fails
/// the job. The connectors come back for the page to check again when it commits.
#[derive(Debug, PartialEq)]
pub(super) struct MirrorsOperation {
    run: StagingRun,
    rules: PermissionRules,
    next: StagingJobCheckpoint,
    entries: VecDeque<(PageEntry, Vec<(Ulid, String)>)>,
    connectors: HashMap<Ulid, SourceConnector>,
    open: Option<OpenEntry>,
    kept: Vec<PageEntry>,
    asked: Option<Ulid>,
}

impl MirrorsOperation {
    pub(super) fn new(
        run: StagingRun,
        rules: PermissionRules,
        next: StagingJobCheckpoint,
        entries: Vec<(PageEntry, Vec<(Ulid, String)>)>,
    ) -> Self {
        Self {
            run,
            rules,
            next,
            entries: entries.into(),
            connectors: HashMap::new(),
            open: None,
            kept: Vec::new(),
            asked: None,
        }
    }

    /// Works through the entries until a connector must be read or the page is done.
    fn advance(&mut self) -> Next<MirroredPage> {
        loop {
            let Some(mut open) = self.open.take().or_else(|| self.open_next()) else {
                return Next::Done(MirroredPage {
                    next: self.next.clone(),
                    entries: std::mem::take(&mut self.kept),
                    connectors: std::mem::take(&mut self.connectors).into_values().collect(),
                });
            };
            let Some((connector_id, value)) = open.sources.front().cloned() else {
                self.kept.push(open.entry);
                continue;
            };
            let Some(connector) = self.connectors.get(&connector_id) else {
                self.open = Some(open);
                self.asked = Some(connector_id);
                return Next::Call(connector_call(&self.run, connector_id));
            };
            open.sources.pop_front();
            let spec = &self.run.spec;
            let path = mirror_path(&value, connector.base_url().as_deref()).and_then(|path| {
                let allowed = connector_permission_path(spec, connector_id, &path);
                match self.rules.allows(&allowed, &Permission::READ) {
                    true => Ok(path),
                    false => Err("permission denied".to_string()),
                }
            });
            let path = match path {
                Ok(path) => path,
                Err(reason) => {
                    let (source, target) = (&open.entry.source_path, &open.entry.target_key);
                    record_failure(&mut self.next, source, target, reason);
                    continue;
                }
            };
            let entry = &mut open.entry;
            let first = connector_id == spec.connector_id && path == entry.source_path;
            let known = (entry.mirrors.iter()).any(|mirror| {
                mirror.connector_id == Some(connector_id) && mirror.descriptor.source_path == path
            });
            if !first && !known {
                let node = Some(spec.node_id);
                let mut descriptor =
                    build_source_descriptor(connector, &entry.observation, path, node);
                // A version id names one server's copy, never another's.
                descriptor.version_selector = None;
                entry.mirrors.push(SourceMirror {
                    descriptor,
                    connector_id: Some(connector_id),
                });
            }
            self.open = Some(open);
        }
    }

    fn open_next(&mut self) -> Option<OpenEntry> {
        let (entry, rendered) = self.entries.pop_front()?;
        let mapped = (self.run.spec.mirrors.iter())
            .filter_map(|mapping| Some((mapping.connector_id, mapping.map(&entry.source_path)?)));
        let sources = mapped.chain(rendered).collect();
        Some(OpenEntry { entry, sources })
    }
}

impl Operation for MirrorsOperation {
    type Output = MirroredPage;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<MirroredPage>, PageFailure> {
        Ok(self.advance())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MirroredPage>, PageFailure> {
        let connector_id = (self.asked.take())
            .ok_or_else(|| PageFailure::Retry("no connector was asked for".to_string()))?;
        let connector = connector_result(result, connector_id).map_err(|failure| match failure {
            ItemFailure::Lasting(message) => PageFailure::Permanent(message),
            ItemFailure::Denied(message)
            | ItemFailure::Stage(message)
            | ItemFailure::System(message) => PageFailure::Retry(message),
        })?;
        self.connectors.insert(connector_id, connector);
        Ok(self.advance())
    }
}

/// The further sources of one listed file, each readable by the submitter, with their
/// connectors for the write to check again when it commits.
#[derive(Debug, PartialEq)]
pub(super) struct ItemMirrorsOperation {
    run: StagingRun,
    file_path: String,
    rules: Option<PermissionRules>,
    index: usize,
    mirrors: Vec<(SourceConnector, String)>,
    value: Option<String>,
}

impl ItemMirrorsOperation {
    pub(super) fn new(run: StagingRun, file_path: String) -> Self {
        Self {
            run,
            file_path,
            rules: None,
            index: 0,
            mirrors: Vec::new(),
            value: None,
        }
    }

    fn advance(&mut self) -> Next<Vec<(SourceConnector, String)>> {
        let spec = self.run.spec.clone();
        while let Some(mapping) = spec.mirrors.get(self.index) {
            if let Some(value) = mapping.map(&self.file_path) {
                self.value = Some(value);
                return Next::Call(connector_call(&self.run, mapping.connector_id));
            }
            self.index += 1;
        }
        Next::Done(std::mem::take(&mut self.mirrors))
    }
}

impl Operation for ItemMirrorsOperation {
    type Output = Vec<(SourceConnector, String)>;
    type Error = ItemFailure;

    fn first(&mut self) -> Result<Next<Self::Output>, ItemFailure> {
        Ok(Next::Call(rules_call(&self.run.spec)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, ItemFailure> {
        let Some(rules) = self.rules.as_ref() else {
            self.rules = Some(rules_result(result).map_err(ItemFailure::System)?);
            return Ok(self.advance());
        };
        let spec = self.run.spec.clone();
        let mapping = spec.mirrors.get(self.index);
        let (Some(mapping), Some(value)) = (mapping, self.value.take()) else {
            return Err(ItemFailure::System("no mirror was asked for".to_string()));
        };
        let connector = connector_result(result, mapping.connector_id)?;
        let path =
            mirror_path(&value, connector.base_url().as_deref()).map_err(ItemFailure::Stage)?;
        let allowed = connector_permission_path(&spec, mapping.connector_id, &path);
        if !rules.allows(&allowed, &Permission::READ) {
            return Err(ItemFailure::Denied("permission denied".to_string()));
        }
        self.mirrors.push((connector, path));
        self.index += 1;
        Ok(self.advance())
    }
}

#[cfg(test)]
#[path = "mirrors_tests.rs"]
mod tests;
