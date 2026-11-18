//! Reads stored repository links and applies one link change on the link's owner node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::{Duration, SystemTime};

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{LINK_QUEUE_KEYSPACE, LINK_SECRET_KEYSPACE, REPOSITORY_LINK_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::repository::remote::NodeScope;
use aruna_core::repository::{
    LinkStatus, RepositoryCredential, RepositoryLink, link_key, link_prefix,
};
use aruna_core::rows::{PrefixPages, all_values, value};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::secondary_id::RegisterIdentifiersSpec;
use aruna_core::types::{Key, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::links::{ChangeLinkOperation, LinkChange, LinkError, id_key};
use super::push::creator_auth;
use super::submit::{LinkJob, LinkJobOperation};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::metadata::id_mapping::{MappingRoute, PersistentIdError, mapping_route_for};

const LINK_PAGE: usize = 256;

type Turn<T> = Result<Next<T>, LinkError>;

/// The stored time of a runner clock reading.
pub(crate) fn time_of(now_ms: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(now_ms)
}

/// The row of one stored link.
pub(crate) fn link_row(document_id: Ulid, link_id: Ulid) -> (String, Key) {
    (
        REPOSITORY_LINK_KEYSPACE.to_string(),
        ByteView::from(link_key(document_id, link_id)),
    )
}

/// The stored link of a row read with [`link_row`].
pub(crate) fn parse_link(row: Option<Value>) -> Result<Option<RepositoryLink>, LinkError> {
    Ok(row
        .map(|bytes| RepositoryLink::from_bytes(&bytes))
        .transpose()?)
}

/// The row of a link's sealed token.
pub(crate) fn secret_row(link_id: Ulid) -> (String, Key) {
    (LINK_SECRET_KEYSPACE.to_string(), id_key(link_id))
}

/// The sealed token of a row read with [`secret_row`].
pub(crate) fn parse_secret(row: Option<Value>) -> Result<Option<RepositoryCredential>, LinkError> {
    Ok(row
        .map(|bytes| postcard::from_bytes(&bytes).map_err(ConversionError::from))
        .transpose()?)
}

/// Reads one stored link.
#[derive(Debug, PartialEq)]
pub struct ReadLinkOperation {
    document_id: Ulid,
    link_id: Ulid,
}

impl ReadLinkOperation {
    pub fn new(document_id: Ulid, link_id: Ulid) -> Self {
        Self {
            document_id,
            link_id,
        }
    }
}

impl Operation for ReadLinkOperation {
    type Output = Option<RepositoryLink>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Option<RepositoryLink>> {
        let row = link_row(self.document_id, self.link_id);
        Ok(Next::Call(IoCall::Read(vec![row])))
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<RepositoryLink>> {
        Ok(Next::Done(parse_link(value(result)?)?))
    }
}

/// All links of a document, read page by page.
#[derive(Debug, PartialEq)]
pub(crate) struct DocumentLinksOperation {
    pages: PrefixPages,
    links: Vec<RepositoryLink>,
}

impl DocumentLinksOperation {
    pub(crate) fn new(document_id: Ulid) -> Self {
        Self {
            pages: PrefixPages::new(
                REPOSITORY_LINK_KEYSPACE,
                ByteView::from(link_prefix(document_id)),
                LINK_PAGE,
            ),
            links: Vec::new(),
        }
    }
}

impl Operation for DocumentLinksOperation {
    type Output = Vec<RepositoryLink>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Vec<RepositoryLink>> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Turn<Vec<RepositoryLink>> {
        for (_, bytes) in self.pages.take(result)? {
            self.links.push(RepositoryLink::from_bytes(&bytes)?);
        }
        Ok(match self.pages.done() {
            true => Next::Done(std::mem::take(&mut self.links)),
            false => Next::Call(self.pages.call()),
        })
    }
}

/// The document's links with whether each still has a push check queued.
#[derive(Debug, PartialEq)]
pub struct ListLinksOperation {
    document_id: Ulid,
    links: Option<Vec<RepositoryLink>>,
}

impl ListLinksOperation {
    pub fn new(document_id: Ulid) -> Self {
        Self {
            document_id,
            links: None,
        }
    }
}

impl Operation for ListLinksOperation {
    type Output = Vec<(RepositoryLink, bool)>;
    type Error = LinkError;

    fn first(&mut self) -> Turn<Self::Output> {
        Ok(Next::Call(IoCall::child(DocumentLinksOperation::new(
            self.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<Self::Output> {
        let Some(links) = self.links.take() else {
            let links = result.child::<Vec<RepositoryLink>, LinkError>()??;
            if links.is_empty() {
                return Ok(Next::Done(Vec::new()));
            }
            let reads = links
                .iter()
                .map(|link| (LINK_QUEUE_KEYSPACE.to_string(), id_key(link.link_id)))
                .collect();
            self.links = Some(links);
            return Ok(Next::Call(IoCall::Read(reads)));
        };
        let queued = all_values(result)?;
        Ok(Next::Done(
            links
                .into_iter()
                .zip(queued)
                .map(|(link, queued)| (link, queued.is_some()))
                .collect(),
        ))
    }
}

/// One lineage cannot have an enabled push link and an enabled pull link on one dataset.
#[derive(Debug, PartialEq)]
pub(crate) struct EnsureLineageOperation {
    link: RepositoryLink,
}

impl EnsureLineageOperation {
    pub(crate) fn new(link: RepositoryLink) -> Self {
        Self { link }
    }
}

impl Operation for EnsureLineageOperation {
    type Output = ();
    type Error = LinkError;

    fn first(&mut self) -> Turn<()> {
        Ok(Next::Call(IoCall::child(DocumentLinksOperation::new(
            self.link.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Turn<()> {
        let pulls = self.link.pull().is_some();
        let conflict = result
            .child::<Vec<RepositoryLink>, LinkError>()??
            .into_iter()
            .any(|other| {
                other.link_id != self.link.link_id
                    && other.status == LinkStatus::Enabled
                    && other.pull().is_some() != pulls
                    && other.same_lineage(&self.link)
            });
        if conflict {
            return Err(LinkError::Lineage);
        }
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum ChangeStep {
    Config,
    Lineage,
    Register,
    Write,
}

/// Applies a change on the link's owner node and replicates it to the document's holders. A
/// change that enables the link checks its lineage first, and an accepted published record
/// queues its identifiers.
#[derive(Debug, PartialEq)]
pub struct ApplyChangeOperation {
    scope: NodeScope,
    link: RepositoryLink,
    change: Option<LinkChange>,
    route: Option<MappingRoute>,
    now_ms: u64,
    step: ChangeStep,
}

impl ApplyChangeOperation {
    pub fn new(scope: NodeScope, link: RepositoryLink, change: LinkChange) -> Self {
        Self {
            scope,
            link,
            change: Some(change),
            route: None,
            now_ms: 0,
            step: ChangeStep::Config,
        }
    }

    fn change(&self) -> Result<&LinkChange, LinkError> {
        self.change
            .as_ref()
            .ok_or_else(|| LinkError::Unexpected("the link change already ran".into()))
    }

    /// The link this change enables, which must not cross another link's lineage.
    fn enabled(&self) -> Result<Option<RepositoryLink>, LinkError> {
        // These changes can end a failure and enable the link again.
        let change = self.change()?;
        let revives = matches!(self.link.status, LinkStatus::Failed { .. })
            && matches!(
                change,
                LinkChange::Rotate(_) | LinkChange::Begin(_) | LinkChange::Accept(_)
            );
        Ok(match change {
            LinkChange::Create { link, .. } => Some(link.as_ref().clone()),
            LinkChange::Patch(patch) if patch.paused == Some(false) => Some(self.link.clone()),
            _ if revives => Some(self.link.clone()),
            _ => None,
        })
    }

    fn check_lineage(&mut self) -> Turn<Option<RepositoryLink>> {
        match self.enabled()? {
            Some(enabled) => {
                self.step = ChangeStep::Lineage;
                Ok(Next::Call(IoCall::child(EnsureLineageOperation::new(
                    enabled,
                ))))
            }
            None => self.register(),
        }
    }

    /// Queues a published record the link adopts outside a push, such as after a community
    /// review, as `Published` identifiers of the dataset.
    fn register(&mut self) -> Turn<Option<RepositoryLink>> {
        let record = match self.change()? {
            LinkChange::Accept(state) => state
                .latest
                .as_ref()
                .filter(|record| record.published && !record.identifiers.is_empty()),
            _ => None,
        };
        let Some(record) = record.cloned() else {
            return self.write();
        };
        let spec = RegisterIdentifiersSpec {
            document_id: self.link.document_id,
            identifiers: record.identifiers,
            auth_context: creator_auth(&self.scope, &self.link),
        };
        let key = format!("identifiers/{}/{}", self.link.link_id, record.id);
        self.step = ChangeStep::Register;
        Ok(Next::Call(IoCall::child(LinkJobOperation::new(
            self.scope.clone(),
            self.link.owner_node,
            LinkJob::Identifiers(spec),
            key,
        ))))
    }

    fn write(&mut self) -> Turn<Option<RepositoryLink>> {
        let change = self
            .change
            .take()
            .ok_or_else(|| LinkError::Unexpected("the link change already ran".into()))?;
        self.step = ChangeStep::Write;
        let operation = ChangeLinkOperation::new(
            self.link.document_id,
            self.link.link_id,
            change,
            time_of(self.now_ms),
        )
        .routed(self.route.take());
        Ok(Next::Call(IoCall::child(operation)))
    }

    fn route_of(&self, config: &RealmConfigDocument) -> Result<Option<MappingRoute>, LinkError> {
        let Some((node, realm)) = self.scope.node else {
            return Ok(None);
        };
        mapping_route_for(config, realm, self.link.document_id, node)
            .map(Some)
            .ok_or_else(|| unavailable("persistent id placement is unavailable"))
    }
}

fn unavailable(message: &str) -> LinkError {
    LinkError::Unexpected(PersistentIdError::Unavailable(message.to_string()).to_string())
}

impl Operation for ApplyChangeOperation {
    type Output = Option<RepositoryLink>;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn<Option<RepositoryLink>> {
        match self.scope.node {
            Some((node, _)) if node != self.link.owner_node => {
                Err(LinkError::NotOwner(self.link.owner_node_url.clone()))
            }
            Some((_, realm)) => Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm)))),
            None => self.check_lineage(),
        }
    }

    fn next(&mut self, result: IoResult) -> Turn<Option<RepositoryLink>> {
        match self.step {
            ChangeStep::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|error| {
                        tracing::warn!(%error, "Realm config unavailable for a link change");
                        unavailable("realm placement config is unavailable")
                    })?;
                self.route = self.route_of(&config)?;
                self.check_lineage()
            }
            ChangeStep::Lineage => {
                result.child::<(), LinkError>()??;
                self.register()
            }
            ChangeStep::Register => {
                result
                    .child::<SubmitJobResult, SubmitJobError>()?
                    .map_err(|error| LinkError::Submit(error.to_string()))?;
                self.write()
            }
            ChangeStep::Write => {
                let changed = result.child::<Option<RepositoryLink>, LinkError>()??;
                Ok(Next::Done(changed))
            }
        }
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
