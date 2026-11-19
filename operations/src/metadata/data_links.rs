//! Keeps the node-local data link postings of materialized documents: identifier first, so a
//! lookup is a seek, and per document, so a materialization replaces exactly its own rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Prefix lookups probe only the prefix lengths a bucket's postings use.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};

use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::keyspaces::{
    DATA_LINK_KEYSPACE, DISTRIBUTION_KEYSPACE, DOCUMENT_LINK_KEYSPACE, S3_BUCKET_KEYSPACE,
    VERSION_INDEX_KEYSPACE,
};
use aruna_core::metadata::MetadataError;
use aruna_core::operation::{Operation, TransactionMode, boxed_suboperation};
use aruna_core::rows::all_values;
use aruna_core::structs::identity::realm::NodeRealm;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data::link::unverified_key;
use aruna_core::structs::storage::data::link::{
    DataLink, LinkPosting, MAX_DOCUMENT_LINKS, MAX_LINK_SUBJECTS, document_key, posting_document,
    posting_key,
};
use aruna_core::structs::storage::distribution::inventory::index_key;
use aruna_core::structs::storage::distribution::{DistributionView, SetHint};
use aruna_core::types::Key;
use byteview::ByteView;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::distribution::resolve::{GlobalResolution, RESOLVE_BUDGET, resolve_bounded};
use crate::metadata::call_results::page_rows;

const PAGE_SIZE: usize = 256;
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SCHEMA_BASES: [&str; 2] = ["http://schema.org/", "https://schema.org/"];

#[derive(Debug, Error, PartialEq)]
pub(crate) enum DataLinkError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected data link storage event: {0}")]
    UnexpectedEvent(String),
}

impl From<DataLinkError> for MetadataError {
    fn from(error: DataLinkError) -> Self {
        MetadataError::Backend(error.to_string())
    }
}

impl From<IoError> for DataLinkError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Conflict => Self::Storage(StorageError::TransactionConflict),
            error => Self::UnexpectedEvent(error.to_string()),
        }
    }
}

/// The rows one materialization writes and the stale rows of the document it deletes.
#[derive(Debug, Default, PartialEq)]
pub(crate) struct LinkRows {
    pub writes: Vec<(String, ByteView, ByteView)>,
    pub deletes: Vec<(String, ByteView)>,
}

fn schema_term(iri: &str, term: &str) -> bool {
    SCHEMA_BASES
        .iter()
        .any(|base| iri.strip_prefix(base) == Some(term))
}

/// The links a document's references name, from `(subject, predicate, object)` triples. A
/// Dataset-typed `hasPart` of the root is a linked part; the root is the graph IRI or the
/// entity the metadata descriptor is `about`.
pub(crate) fn project_links(
    graph_iri: &str,
    cursor: Ulid,
    references: &[(String, String, String)],
) -> BTreeMap<DataLink, LinkPosting> {
    let mut roots = BTreeSet::from([graph_iri.to_string()]);
    let mut datasets = HashSet::new();
    for (subject, predicate, object) in references {
        if schema_term(predicate, "about") && subject.ends_with("ro-crate-metadata.json") {
            roots.insert(object.clone());
        }
        if predicate == RDF_TYPE && schema_term(object, "Dataset") {
            datasets.insert(subject.as_str());
        }
    }
    let mut links = BTreeMap::<DataLink, LinkPosting>::new();
    for (subject, predicate, object) in references {
        let Some(link) = DataLink::parse(object) else {
            continue;
        };
        if links.len() >= MAX_DOCUMENT_LINKS && !links.contains_key(&link) {
            warn!(
                graph_iri,
                "Document names more data links than one projection keeps"
            );
            // The marker posting makes every lookup on this node report itself incomplete.
            let marker = LinkPosting {
                cursor,
                ..LinkPosting::default()
            };
            links.insert(DataLink::Truncated, marker);
            break;
        }
        let part = schema_term(predicate, "hasPart")
            && roots.contains(subject)
            && datasets.contains(object.as_str())
            && link.scope().is_some();
        let posting = links.entry(link).or_insert_with(|| LinkPosting {
            cursor,
            ..LinkPosting::default()
        });
        posting.part |= part;
        if posting.subjects.len() < MAX_LINK_SUBJECTS && !posting.subjects.contains(subject) {
            posting.subjects.push(subject.clone());
        }
    }
    links
}

/// One lookup's shared allowance of storage seeks and posting rows; an exhausted budget
/// makes the answer incomplete, never a scan.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct LinkBudget {
    pub seeks: usize,
    pub rows: usize,
}

impl LinkBudget {
    pub(crate) fn exhausted(&self) -> bool {
        self.seeks == 0 || self.rows == 0
    }

    pub(crate) fn take_seek(&mut self) -> bool {
        if self.seeks == 0 {
            return false;
        }
        self.seeks -= 1;
        true
    }
}

/// The realm, version and document of an unverified posting key.
fn unverified_parts(key: &[u8]) -> Option<(RealmId, Ulid, Ulid)> {
    let rest = key.strip_prefix(unverified_key().as_slice())?;
    let realm: [u8; 32] = rest.get(..32)?.try_into().ok()?;
    let version: [u8; 16] = rest.get(32..48)?.try_into().ok()?;
    let document = posting_document(key).filter(|_| rest.len() == 64)?;
    Some((
        RealmId::from_bytes(realm),
        Ulid::from_bytes(version),
        document,
    ))
}

fn page_call(key_space: &str, prefix: Vec<u8>, after: Option<Key>) -> IoCall {
    IoCall::Page {
        space: key_space.to_string(),
        prefix: Key::from(prefix),
        after,
        limit: PAGE_SIZE,
    }
}

/// The posting keys a document has rows for, from its per-document rows in `key_space`.
#[derive(Debug, PartialEq)]
pub(crate) struct DocumentRowsOperation {
    key_space: &'static str,
    document_id: Ulid,
    keys: Vec<Vec<u8>>,
}

impl DocumentRowsOperation {
    pub(crate) fn new(key_space: &'static str, document_id: Ulid) -> Self {
        Self {
            key_space,
            document_id,
            keys: Vec::new(),
        }
    }

    fn page(&self, after: Option<Key>) -> IoCall {
        page_call(self.key_space, self.document_id.to_bytes().to_vec(), after)
    }
}

impl Operation for DocumentRowsOperation {
    type Output = Vec<Vec<u8>>;
    type Error = DataLinkError;

    fn first(&mut self) -> Result<Next<Vec<Vec<u8>>>, DataLinkError> {
        Ok(Next::Call(self.page(None)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<Vec<u8>>>, DataLinkError> {
        let (rows, next) = page_rows(result)?;
        let keys = rows
            .into_iter()
            .filter_map(|(key, _)| key.get(16..).map(<[u8]>::to_vec));
        self.keys.extend(keys);
        match next {
            Some(next) => Ok(Next::Call(self.page(Some(next)))),
            None => Ok(Next::Done(std::mem::take(&mut self.keys))),
        }
    }
}

/// Replaces the document's postings: writes the projection and deletes every older row of the
/// document that the projection does not write again.
#[derive(Debug, PartialEq)]
pub(crate) struct LinkRowsOperation {
    document_id: Ulid,
    links: BTreeMap<DataLink, LinkPosting>,
}

impl LinkRowsOperation {
    pub(crate) fn new(document_id: Ulid, links: BTreeMap<DataLink, LinkPosting>) -> Self {
        Self { document_id, links }
    }
}

impl Operation for LinkRowsOperation {
    type Output = LinkRows;
    type Error = DataLinkError;

    fn first(&mut self) -> Result<Next<LinkRows>, DataLinkError> {
        let rows = DocumentRowsOperation::new(DOCUMENT_LINK_KEYSPACE, self.document_id);
        Ok(Next::Call(IoCall::child(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LinkRows>, DataLinkError> {
        let existing = result.child::<Vec<Vec<u8>>, DataLinkError>()??;
        let document_id = self.document_id;
        let mut rows = LinkRows::default();
        let mut written = HashSet::new();
        for (link, posting) in &self.links {
            let key = posting_key(&link.key(), document_id);
            rows.writes.push((
                DATA_LINK_KEYSPACE.to_string(),
                key.clone().into(),
                posting.to_bytes()?.into(),
            ));
            rows.writes.push((
                DOCUMENT_LINK_KEYSPACE.to_string(),
                document_key(document_id, &key).into(),
                ByteView::from(Vec::new()),
            ));
            written.insert(key);
        }
        for key in existing {
            if !written.contains(&key) {
                rows.deletes.push((
                    DOCUMENT_LINK_KEYSPACE.to_string(),
                    document_key(document_id, &key).into(),
                ));
                rows.deletes
                    .push((DATA_LINK_KEYSPACE.to_string(), key.into()));
            }
        }
        Ok(Next::Done(rows))
    }
}

/// At most this many distribution records are read to verify one document's versions.
const MAX_SETS: usize = 4096;

/// The sets this node holds a bound member bucket of, by hint.
#[derive(Debug, PartialEq, Default)]
pub(crate) struct LocalMembersOperation {
    seen: usize,
    next: Option<Key>,
    sets: Vec<(Ulid, String)>,
    members: BTreeMap<SetHint, Vec<Ulid>>,
}

impl LocalMembersOperation {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

impl Operation for LocalMembersOperation {
    type Output = BTreeMap<SetHint, Vec<Ulid>>;
    type Error = DataLinkError;

    fn first(&mut self) -> Result<Next<Self::Output>, DataLinkError> {
        Ok(Next::Call(page_call(
            DISTRIBUTION_KEYSPACE,
            Vec::new(),
            None,
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, DataLinkError> {
        if self.sets.is_empty() {
            let (rows, next) = page_rows(result)?;
            self.seen += rows.len();
            self.next = next;
            for (_, value) in rows {
                let Ok(view) = DistributionView::from_bytes(&value) else {
                    continue;
                };
                let snapshot = view.snapshot();
                self.sets
                    .push((snapshot.set_id(), snapshot.config.bucket.clone()));
            }
            if self.sets.is_empty() {
                return self.paged();
            }
            let reads = (self.sets.iter())
                .map(|(_, bucket)| {
                    (
                        S3_BUCKET_KEYSPACE.to_string(),
                        Key::from(bucket.as_bytes().to_vec()),
                    )
                })
                .collect();
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        let values = all_values(result)?;
        for ((set_id, _), value) in std::mem::take(&mut self.sets).into_iter().zip(values) {
            let bucket = value
                .map(|value| BucketInfo::from_bytes(&value))
                .transpose()?;
            let bound = (bucket.and_then(|info| info.distribution))
                .is_some_and(|binding| binding.set_id == set_id);
            if bound {
                self.members
                    .entry(SetHint::of_set(set_id))
                    .or_default()
                    .push(set_id);
            }
        }
        self.paged()
    }
}

impl LocalMembersOperation {
    fn paged(&mut self) -> Result<Next<BTreeMap<SetHint, Vec<Ulid>>>, DataLinkError> {
        match self.next.take() {
            Some(next) if self.seen < MAX_SETS => Ok(Next::Call(page_call(
                DISTRIBUTION_KEYSPACE,
                Vec::new(),
                Some(next),
            ))),
            _ => Ok(Next::Done(std::mem::take(&mut self.members))),
        }
    }
}

/// The version index rows of `version_id` in each candidate set of `members`.
fn set_reads(
    members: &BTreeMap<SetHint, Vec<Ulid>>,
    version_id: Ulid,
) -> Vec<(Ulid, (String, Key))> {
    let candidates = members
        .get(&SetHint::of_version(version_id))
        .into_iter()
        .flatten();
    candidates
        .map(|set_id| {
            let key = Key::from(index_key(*set_id, version_id));
            (*set_id, (VERSION_INDEX_KEYSPACE.to_string(), key))
        })
        .collect()
}

/// Adds a derived posting per version link: verified in its set when a local member's version
/// id index holds it (only then does it carry a set), otherwise unverified and counted.
#[derive(Debug, PartialEq)]
pub(crate) struct AssociateVersionsOperation {
    links: BTreeMap<DataLink, LinkPosting>,
    /// The candidate set of each version index row read, in read order.
    candidates: Vec<(RealmId, Ulid, Ulid)>,
}

impl AssociateVersionsOperation {
    pub(crate) fn new(links: BTreeMap<DataLink, LinkPosting>) -> Self {
        Self {
            links,
            candidates: Vec::new(),
        }
    }

    fn versions(&self) -> Vec<(RealmId, Ulid)> {
        (self.links.keys())
            .filter_map(|link| match link {
                DataLink::Version {
                    realm_id,
                    version_id,
                } => Some((*realm_id, *version_id)),
                _ => None,
            })
            .collect()
    }
}

type Links = BTreeMap<DataLink, LinkPosting>;

impl Operation for AssociateVersionsOperation {
    type Output = Links;
    type Error = DataLinkError;

    fn first(&mut self) -> Result<Next<Links>, DataLinkError> {
        if self.versions().is_empty() {
            return Ok(Next::Done(std::mem::take(&mut self.links)));
        }
        Ok(Next::Call(IoCall::child(LocalMembersOperation::new())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Links>, DataLinkError> {
        if self.candidates.is_empty() {
            let members = result.child::<BTreeMap<SetHint, Vec<Ulid>>, DataLinkError>()??;
            let mut reads = Vec::new();
            for (realm_id, version_id) in self.versions() {
                for (set_id, read) in set_reads(&members, version_id) {
                    self.candidates.push((realm_id, version_id, set_id));
                    reads.push(read);
                }
            }
            if reads.is_empty() {
                return Ok(Next::Done(self.associated(&[])));
            }
            return Ok(Next::Call(IoCall::Read(reads)));
        }
        let values = all_values(result)?;
        let candidates = std::mem::take(&mut self.candidates);
        let found: Vec<_> = (candidates.into_iter().zip(values))
            .filter(|(_, value)| value.is_some())
            .map(|(candidate, _)| candidate)
            .collect();
        Ok(Next::Done(self.associated(&found)))
    }
}

impl AssociateVersionsOperation {
    fn associated(&mut self, found: &[(RealmId, Ulid, Ulid)]) -> Links {
        let mut links = std::mem::take(&mut self.links);
        for (realm_id, version_id) in self.versions_of(&links) {
            let Some(posting) = links
                .get(&DataLink::Version {
                    realm_id,
                    version_id,
                })
                .cloned()
            else {
                continue;
            };
            let sets: Vec<_> = (found.iter())
                .filter(|(realm, version, _)| *realm == realm_id && *version == version_id)
                .map(|(_, _, set_id)| *set_id)
                .collect();
            for set_id in &sets {
                let link = DataLink::SetVersion {
                    realm_id,
                    set_id: *set_id,
                    version_id,
                };
                links.insert(link, posting.clone());
            }
            if sets.is_empty() {
                links.insert(
                    DataLink::Unverified {
                        realm_id,
                        version_id,
                    },
                    posting,
                );
            }
        }
        links
    }

    fn versions_of(&self, links: &Links) -> Vec<(RealmId, Ulid)> {
        (links.keys())
            .filter_map(|link| match link {
                DataLink::Version {
                    realm_id,
                    version_id,
                } => Some((*realm_id, *version_id)),
                _ => None,
            })
            .collect()
    }
}

/// Unverified version links one background pass tries to verify.
const RETRY_PAGE: usize = 16;

/// One unverified posting a pass tries to settle.
#[derive(Debug, PartialEq)]
struct Unverified {
    key: ByteView,
    value: ByteView,
    realm_id: RealmId,
    version_id: Ulid,
    document_id: Ulid,
}

#[derive(Debug, PartialEq)]
enum RetryStage {
    Page,
    Members,
    Local(Unverified),
    Remote(Unverified),
    Settle,
}

/// Tries a page of the version links no member could verify at materialization: through a local
/// member's index, else by asking the set's members. A verified link gains its set posting; a
/// version no set holds stops counting. Answers the posting to continue after, if more follow.
#[derive(Debug, PartialEq)]
pub struct RetryUnverifiedOperation {
    ids: NodeRealm,
    after: Option<Vec<u8>>,
    next: Option<Vec<u8>>,
    pending: VecDeque<Unverified>,
    members: BTreeMap<SetHint, Vec<Ulid>>,
    stage: RetryStage,
}

impl RetryUnverifiedOperation {
    /// `after` is where the previous pass stopped; `None` starts at the first posting.
    pub fn new(ids: NodeRealm, after: Option<Vec<u8>>) -> Self {
        Self {
            ids,
            after,
            next: None,
            pending: VecDeque::new(),
            members: BTreeMap::new(),
            stage: RetryStage::Page,
        }
    }

    /// Checks the next posting against this node's member indexes.
    fn check(&mut self) -> Result<Next<Option<Vec<u8>>>, DataLinkError> {
        let Some(posting) = self.pending.pop_front() else {
            return Ok(Next::Done(self.next.take()));
        };
        let reads: Vec<_> = (set_reads(&self.members, posting.version_id).into_iter())
            .map(|(_, read)| read)
            .collect();
        if reads.is_empty() {
            return self.ask(posting);
        }
        self.stage = RetryStage::Local(posting);
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Asks the set's members, bounded by the resolve budget.
    fn ask(&mut self, posting: Unverified) -> Result<Next<Option<Vec<u8>>>, DataLinkError> {
        let resolve = resolve_bounded(self.ids, posting.version_id);
        let member = Effect::SubOperation(boxed_suboperation(resolve, |result| {
            Event::Child(ChildOutcome::new(result))
        }));
        self.stage = RetryStage::Remote(posting);
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members: vec![member],
            limit: 1,
            form: GroupForm::All {
                deadline: Some(RESOLVE_BUDGET),
            },
        })))
    }

    fn settle(
        &mut self,
        posting: Unverified,
        set_id: Option<Ulid>,
    ) -> Result<Next<Option<Vec<u8>>>, DataLinkError> {
        let link = set_id.map(|set_id| DataLink::SetVersion {
            realm_id: posting.realm_id,
            set_id,
            version_id: posting.version_id,
        });
        let settle =
            SettleUnverifiedOperation::new((posting.key, posting.value), posting.document_id, link);
        self.stage = RetryStage::Settle;
        Ok(Next::Call(IoCall::child(settle)))
    }
}

impl Operation for RetryUnverifiedOperation {
    type Output = Option<Vec<u8>>;
    type Error = DataLinkError;

    fn first(&mut self) -> Result<Next<Option<Vec<u8>>>, DataLinkError> {
        Ok(Next::Call(IoCall::Page {
            space: DATA_LINK_KEYSPACE.to_string(),
            prefix: Key::from(unverified_key()),
            after: self.after.take().map(Key::from),
            limit: RETRY_PAGE,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<Vec<u8>>>, DataLinkError> {
        match std::mem::replace(&mut self.stage, RetryStage::Settle) {
            RetryStage::Page => {
                let (rows, next) = page_rows(result)?;
                self.next = next.map(|next| next.to_vec());
                for (key, value) in rows {
                    if let Some((realm_id, version_id, document_id)) = unverified_parts(&key) {
                        self.pending.push_back(Unverified {
                            key,
                            value,
                            realm_id,
                            version_id,
                            document_id,
                        });
                    }
                }
                if self.pending.is_empty() {
                    return Ok(Next::Done(self.next.take()));
                }
                self.stage = RetryStage::Members;
                Ok(Next::Call(IoCall::child(LocalMembersOperation::new())))
            }
            RetryStage::Members => {
                self.members = result.child::<BTreeMap<SetHint, Vec<Ulid>>, DataLinkError>()??;
                self.check()
            }
            RetryStage::Local(posting) => {
                let reads = set_reads(&self.members, posting.version_id);
                let values = all_values(result)?;
                let local = (reads.into_iter().zip(values))
                    .find(|(_, value)| value.is_some())
                    .map(|((set_id, _), _)| set_id);
                match local {
                    Some(set_id) => self.settle(posting, Some(set_id)),
                    None => self.ask(posting),
                }
            }
            RetryStage::Remote(posting) => {
                let found = match result {
                    IoResult::Group(GroupEvent::Finished { results }) => (results
                        .into_iter()
                        .next())
                    .and_then(|member| match member {
                        GroupResult::Done(event) => match *event {
                            Event::Child(outcome) => outcome.take::<GlobalResolution, IoError>(),
                            _ => None,
                        },
                        _ => None,
                    }),
                    _ => None,
                };
                match found.and_then(Result::ok) {
                    Some(GlobalResolution::Found(found)) => {
                        self.settle(posting, Some(found.set_id))
                    }
                    Some(GlobalResolution::NotFound) => self.settle(posting, None),
                    _ => self.check(),
                }
            }
            RetryStage::Settle => {
                match result.child::<(), DataLinkError>()? {
                    Ok(()) | Err(DataLinkError::Storage(StorageError::TransactionConflict)) => {}
                    Err(error) => return Err(error),
                }
                self.check()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum SettleStage {
    Read,
    Written(Vec<(String, ByteView)>),
    Deleted,
}

/// Replaces an unverified posting by its set posting, or drops it, while the document's
/// materialization has not replaced it meanwhile, all in one transaction.
#[derive(Debug, PartialEq)]
pub(crate) struct SettleUnverifiedOperation {
    key: ByteView,
    value: ByteView,
    document_id: Ulid,
    link: Option<DataLink>,
    stage: SettleStage,
}

impl SettleUnverifiedOperation {
    pub(crate) fn new(
        (key, value): (ByteView, ByteView),
        document_id: Ulid,
        link: Option<DataLink>,
    ) -> Self {
        Self {
            key,
            value,
            document_id,
            link,
            stage: SettleStage::Read,
        }
    }
}

impl Operation for SettleUnverifiedOperation {
    type Output = ();
    type Error = DataLinkError;

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    // A materialization that won the race keeps its own rows; the caller treats the lost
    // transaction as settled instead of running this again.
    fn is_conflict(_error: &DataLinkError) -> bool {
        false
    }

    fn first(&mut self) -> Result<Next<()>, DataLinkError> {
        let read = (DATA_LINK_KEYSPACE.to_string(), self.key.clone());
        Ok(Next::Call(IoCall::Read(vec![read])))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, DataLinkError> {
        match std::mem::replace(&mut self.stage, SettleStage::Deleted) {
            SettleStage::Read => {
                let current = result.value()?;
                if current.as_ref() != Some(&self.value) {
                    return Ok(Next::Done(()));
                }
                let document_id = self.document_id;
                let mut writes = Vec::new();
                if let Some(link) = &self.link {
                    let posting = posting_key(&link.key(), document_id);
                    let row = document_key(document_id, &posting);
                    let empty = ByteView::from(Vec::new());
                    writes.push((DOCUMENT_LINK_KEYSPACE.to_string(), row.into(), empty));
                    writes.push((
                        DATA_LINK_KEYSPACE.to_string(),
                        posting.into(),
                        self.value.clone(),
                    ));
                }
                let document_row = document_key(document_id, &self.key).into();
                let deletes = vec![
                    (DATA_LINK_KEYSPACE.to_string(), self.key.clone()),
                    (DOCUMENT_LINK_KEYSPACE.to_string(), document_row),
                ];
                self.stage = SettleStage::Written(deletes);
                Ok(Next::Call(IoCall::Write(writes)))
            }
            SettleStage::Written(deletes) => {
                result.done()?;
                self.stage = SettleStage::Deleted;
                Ok(Next::Call(IoCall::Delete(deletes)))
            }
            SettleStage::Deleted => {
                result.done()?;
                Ok(Next::Done(()))
            }
        }
    }
}

#[cfg(test)]
#[path = "data_links_tests.rs"]
pub(super) mod tests;
