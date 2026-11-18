//! Builds an import archive from every accessible published version and checked file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::repository::invenio::{
    crate_versions, import_crate, pull_crate, record_id, record_identifiers, validate_id,
};
use aruna_core::repository::remote::{ArchiveSpool, NodeScope, RemoteAnswer, RemoteCall};
use aruna_core::repository::{ImportMode, ImportOptions, RepositoryPull};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ArtifactRef, ImportRoCrateSpec, RoCrateLimits};
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::secondary_id::{IdentifierOrigin, SecondaryIdentifier};
use serde_json::{Value, json};
use ulid::Ulid;
use url::Url;

use super::export::record_from;
use super::{Session, invalid, json as json_body, reply};
use crate::jobs::repository::connector::{ConnectOperation, ConnectRequest};
use crate::jobs::repository::pull::{PullProgress, PullRunningOperation};
use crate::jobs::repository::{JobScope, TransferError, not_supported, remote_call};

/// Record files sit under `versions/{id}/files/`; in reference mode they are descriptors.
pub(crate) fn is_reference(path: &str) -> bool {
    path.starts_with("versions/") && path.contains("/files/")
}

/// What a repository import acquires.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AcquireRequest {
    pub(crate) kind: RepositoryConnectorKind,
    pub(crate) scope: NodeScope,
    pub(crate) job: JobScope,
    pub(crate) spec: ImportRoCrateSpec,
    pub(crate) group_id: Ulid,
    pub(crate) connector_id: Ulid,
    pub(crate) record_id: String,
    pub(crate) options: ImportOptions,
    pub(crate) pull: Option<RepositoryPull>,
    /// The dataset's current crate and revision, which a pull update merges into.
    pub(crate) current: Option<(String, Ulid)>,
}

pub(crate) type Acquired = (ArtifactRef, Vec<SecondaryIdentifier>, Option<PullProgress>);

type VersionRecords = Vec<(Value, Value)>;

#[derive(Debug, PartialEq)]
enum AcquireStep {
    Running,
    Connect,
    History(Session),
    Spool(Vec<SecondaryIdentifier>, Option<PullProgress>),
}

/// Downloads a repository record into an import artifact with the identifiers it found.
#[derive(Debug, PartialEq)]
pub(crate) struct AcquireOperation {
    request: AcquireRequest,
    step: AcquireStep,
}

impl AcquireOperation {
    pub(crate) fn new(request: AcquireRequest) -> Self {
        Self {
            request,
            step: AcquireStep::Running,
        }
    }

    fn connect(&mut self) -> Result<Next<Acquired>, TransferError> {
        let request = &self.request;
        self.step = AcquireStep::Connect;
        Ok(Next::Call(IoCall::child(ConnectOperation::new(
            ConnectRequest {
                scope: request.scope.clone(),
                auth: request.spec.auth_context.clone(),
                group_id: request.group_id,
                connector_id: request.connector_id,
                permission: Permission::READ,
                limit: request.spec.limits.metadata_bytes,
                credential: None,
                stop: Some(request.job.stop.clone()),
            },
        ))))
    }

    fn spool(
        &mut self,
        session: &Session,
        selected: String,
        records: VersionRecords,
    ) -> Result<Next<Acquired>, TransferError> {
        let request = &self.request;
        let limits = &request.spec.limits;
        let latest = records
            .iter()
            .find(|(record, _)| record["id"] == selected.as_str())
            .map(|(record, _)| record.clone())
            .ok_or_else(|| invalid("requested record absent from history"))?;
        let endpoint = session.endpoint();
        let (document, records, base) = match &request.pull {
            Some(RepositoryPull::Update { .. }) => {
                let (jsonld, base) = request
                    .current
                    .as_ref()
                    .ok_or_else(|| invalid("invalid dataset crate"))?;
                let current: Value =
                    serde_json::from_str(jsonld).map_err(|_| invalid("invalid dataset crate"))?;
                let known = crate_versions(&current);
                let added = records
                    .into_iter()
                    .filter(|(record, _)| {
                        record_id(record).is_ok_and(|id| !known.iter().any(|k| k == id))
                    })
                    .collect::<Vec<_>>();
                let document = pull_crate(&current, endpoint, &latest, &added)?;
                (document, added, Some(*base))
            }
            _ => (import_crate(endpoint, &selected, &records)?, records, None),
        };
        let progress = match &request.pull {
            Some(_) => Some(PullProgress {
                record: record_from(session, &latest)?,
                endpoint: endpoint.to_string(),
                base,
                revision: None,
                kind: RepositoryConnectorKind::Invenio,
            }),
            None => None,
        };
        let mut identifiers = Vec::new();
        for record in records.iter().map(|(record, _)| record).chain([&latest]) {
            for id in record_identifiers(endpoint, record, IdentifierOrigin::Imported) {
                if !identifiers.contains(&id) {
                    identifiers.push(id);
                }
            }
        }
        let metadata = document.to_string();
        if metadata.len() as u64 > limits.metadata_bytes {
            return Err(invalid("generated crate exceeds metadata limit"));
        }
        let call = remote_call(
            RemoteCall::SpoolArchive(Box::new(ArchiveSpool {
                access: session.access().clone(),
                namespace: request.job.job_id.as_ulid(),
                created_by: request.spec.auth_context.user_id,
                metadata,
                records,
                limits: limits.clone(),
                mode: request.options.mode,
            })),
            Some(request.job.stop.clone()),
        );
        self.step = AcquireStep::Spool(identifiers, progress);
        Ok(Next::Call(call))
    }
}

impl Operation for AcquireOperation {
    type Output = Acquired;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Acquired>, TransferError> {
        let request = &self.request;
        if request.kind != RepositoryConnectorKind::Invenio {
            return Err(not_supported("import"));
        }
        validate_id(&request.record_id)?;
        if let Some(RepositoryPull::Update { link_id }) = request.pull {
            return Ok(Next::Call(IoCall::child(PullRunningOperation::new(
                request.job.job_id,
                request.spec.document_id,
                link_id,
            ))));
        }
        self.connect()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Acquired>, TransferError> {
        match std::mem::replace(&mut self.step, AcquireStep::Connect) {
            AcquireStep::Running => {
                result.child::<(), TransferError>()??;
                self.connect()
            }
            AcquireStep::Connect => {
                let session = result.child::<Session, TransferError>()??;
                let history = HistoryOperation::new(
                    session.clone(),
                    self.request.record_id.clone(),
                    self.request.spec.limits.clone(),
                    self.request.options.clone(),
                );
                self.step = AcquireStep::History(session);
                Ok(Next::Call(IoCall::child(history)))
            }
            AcquireStep::History(session) => {
                let (selected, records) =
                    result.child::<(String, VersionRecords), TransferError>()??;
                self.spool(&session, selected, records)
            }
            AcquireStep::Spool(identifiers, progress) => match reply(result)?? {
                RemoteAnswer::Artifact(artifact) => {
                    Ok(Next::Done((artifact, identifiers, progress)))
                }
                _ => Err(invalid("unexpected archive spool answer")),
            },
        }
    }
}

#[derive(Debug, PartialEq)]
enum HistoryStep {
    Seed,
    Page,
    Record(String),
    Files(String, Value),
}

/// Reads every requested version of a record with its file list, within the crate limits.
#[derive(Debug, PartialEq)]
struct HistoryOperation {
    session: Session,
    requested: String,
    limits: RoCrateLimits,
    options: ImportOptions,
    selected: String,
    parent: String,
    next_page: Option<Url>,
    pages: HashSet<String>,
    hits: std::collections::VecDeque<Value>,
    ids: HashSet<String>,
    records: VersionRecords,
    total: Option<u64>,
    bytes: u64,
    entries: u64,
    step: HistoryStep,
}

impl HistoryOperation {
    fn new(
        session: Session,
        requested: String,
        limits: RoCrateLimits,
        options: ImportOptions,
    ) -> Self {
        Self {
            session,
            requested,
            limits,
            options,
            selected: String::new(),
            parent: String::new(),
            next_page: None,
            pages: HashSet::new(),
            hits: Default::default(),
            ids: HashSet::new(),
            records: Vec::new(),
            total: None,
            bytes: 0,
            entries: 1,
            step: HistoryStep::Seed,
        }
    }

    fn take_page(&mut self, page: &Value) -> Result<(), TransferError> {
        let count = page["hits"]["total"]
            .as_u64()
            .or_else(|| page["hits"]["total"]["value"].as_u64())
            .ok_or_else(|| invalid("missing version total"))?;
        if count > self.limits.max_entries
            || self.total.is_some_and(|total| total != count)
            || page["hits"]["total"]["relation"]
                .as_str()
                .is_some_and(|relation| relation != "eq")
        {
            return Err(invalid("version listing changed or exceeds limits"));
        }
        self.total = Some(count);
        self.hits = page["hits"]["hits"]
            .as_array()
            .ok_or_else(|| invalid("missing version hits"))?
            .iter()
            .cloned()
            .collect();
        self.next_page = page["links"]["next"]
            .as_str()
            .filter(|link| !link.is_empty())
            .map(|link| self.session.link(link))
            .transpose()?;
        Ok(())
    }

    fn read_page(&mut self) -> Result<Next<(String, VersionRecords)>, TransferError> {
        let url = self
            .next_page
            .take()
            .ok_or_else(|| invalid("missing version page"))?;
        if self.pages.len() >= 10_000 || !self.pages.insert(url.as_str().to_string()) {
            return Err(invalid("version pagination loop or limit"));
        }
        self.step = HistoryStep::Page;
        Ok(Next::Call(self.session.get(url)))
    }

    fn next_hit(&mut self) -> Result<Next<(String, VersionRecords)>, TransferError> {
        let Some(hit) = self.hits.pop_front() else {
            if self.next_page.is_some() {
                return self.read_page();
            }
            return self.finish();
        };
        let id = record_id(&hit)?.to_string();
        if !self.ids.insert(id.clone()) || self.ids.len() as u64 > self.limits.max_entries {
            return Err(invalid("duplicate version or version limit"));
        }
        let url = self.session.url(&["records", &id])?;
        self.step = HistoryStep::Record(id);
        Ok(Next::Call(self.session.get(url)))
    }

    fn keep_version(
        &mut self,
        record: Value,
        files: Value,
    ) -> Result<Next<(String, VersionRecords)>, TransferError> {
        if files["links"]["next"]
            .as_str()
            .is_some_and(|link| !link.is_empty())
        {
            return Err(invalid("paginated file listing is not complete"));
        }
        let list = files["entries"]
            .as_array()
            .ok_or_else(|| invalid("missing file entries"))?;
        self.entries = self
            .entries
            .checked_add(list.len() as u64 + 1)
            .ok_or_else(|| invalid("entry overflow"))?;
        self.bytes = self
            .bytes
            .checked_add(record.to_string().len() as u64 + files.to_string().len() as u64)
            .ok_or_else(|| invalid("metadata overflow"))?;
        if self.entries > self.limits.max_entries || self.bytes > self.limits.metadata_bytes {
            return Err(invalid("repository history exceeds crate limits"));
        }
        self.records.push((record, files));
        self.next_hit()
    }

    fn finish(&mut self) -> Result<Next<(String, VersionRecords)>, TransferError> {
        if Some(self.records.len() as u64) != self.total || !self.ids.contains(&self.selected) {
            return Err(invalid("incomplete repository version history"));
        }
        let mut records = std::mem::take(&mut self.records);
        records.sort_by_key(|(record, _)| record["versions"]["index"].as_u64().unwrap_or(0));
        Ok(Next::Done((self.selected.clone(), records)))
    }
}

impl Operation for HistoryOperation {
    type Output = (String, VersionRecords);
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Self::Output>, TransferError> {
        let url = self.session.url(&["records", &self.requested])?;
        Ok(Next::Call(self.session.get(url)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, TransferError> {
        let body = json_body(result)?;
        match std::mem::replace(&mut self.step, HistoryStep::Page) {
            HistoryStep::Seed => {
                self.selected = record_id(&body)?.to_string();
                if body["is_published"] != true {
                    return Err(invalid("record is not published"));
                }
                self.parent = body["parent"]["id"]
                    .as_str()
                    .ok_or_else(|| invalid("missing parent identity"))?
                    .to_string();
                if !self.options.all_versions {
                    let single = json!({"hits": {"total": 1, "hits": [{"id": self.selected}]}});
                    self.take_page(&single)?;
                    return self.next_hit();
                }
                let mut url = self.session.url(&["records", &self.selected, "versions"])?;
                url.query_pairs_mut()
                    .append_pair("allversions", "true")
                    .append_pair("size", "25");
                self.next_page = Some(url);
                self.read_page()
            }
            HistoryStep::Page => {
                self.take_page(&body)?;
                self.next_hit()
            }
            HistoryStep::Record(id) => {
                if record_id(&body)? != id
                    || body["parent"]["id"] != self.parent
                    || body["is_published"] != true
                {
                    return Err(invalid("version identity or publication state changed"));
                }
                if self.options.mode == ImportMode::Metadata {
                    let files = json!({"entries": [], "listing_requested": false});
                    return self.keep_version(body, files);
                }
                let url = self.session.url(&["records", &id, "files"])?;
                self.step = HistoryStep::Files(id, body);
                Ok(Next::Call(self.session.get(url)))
            }
            HistoryStep::Files(_, record) => self.keep_version(record, body),
        }
    }
}

#[cfg(test)]
#[path = "import_tests.rs"]
mod tests;
