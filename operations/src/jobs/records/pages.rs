//! Stores, fetches and serves the input pages of one submission. Pages live outside the
//! family records, so they count toward no family read bound and are never lifecycle evidence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, MetadataAnswer, NetEvent};
use aruna_core::keyspaces::INPUT_PAGE_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::rows::{done, read_row, value};
use aruna_core::scheduling::PlannedInput;
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_core::structs::execution::job::{
    CapturedInput, InputPage, InputSelection, JobFamilyRecord, LaunchIntent, LogicalJobSpec,
    PageEntries, PageError, PageManifest, SubmissionId, launch_entries, needs_pages, paginate,
    spec_entries, verify_chain,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::{debug, warn};

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::{MetadataTransportMessage, decode_message, encode_message};
use crate::realm::peer_trust::is_sync_eligible;

/// Upper bound of one page request; the transport's own step limits end it earlier.
const FETCH_DEADLINE: Duration = Duration::from_secs(60);

/// This node and the realm it serves, when it has a network.
pub type ServingNode = Option<(NodeId, RealmId)>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PageReadError {
    #[error("input page {index} is not available from any holder")]
    Unavailable { index: u32 },
    #[error(transparent)]
    Page(#[from] PageError),
    #[error("input page storage failed: {0}")]
    Storage(String),
}

impl From<IoError> for PageReadError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

fn page_key(submission: &SubmissionId, digest: &[u8; 32]) -> Key {
    let mut key = submission_prefix(*submission).to_vec();
    key.extend_from_slice(digest);
    Key::from(key.as_slice())
}

/// The rows that keep `pages` under their digests. A page is immutable, so a rewrite is a no-op.
pub(crate) fn page_rows(
    submission: &SubmissionId,
    pages: &[InputPage],
) -> Result<Vec<(KeySpace, Key, Value)>, PageReadError> {
    let mut writes = Vec::with_capacity(pages.len());
    for page in pages {
        let digest = page.digest().map_err(PageError::from)?;
        let value = postcard::to_allocvec(page)
            .map_err(|error| PageReadError::Storage(error.to_string()))?;
        writes.push((
            INPUT_PAGE_KEYSPACE.to_string(),
            page_key(submission, &digest),
            value.into(),
        ));
    }
    Ok(writes)
}

/// The page a stored row holds, checked against the digest it is kept under.
fn stored_page(row: &[u8], digest: &[u8; 32]) -> Result<InputPage, PageReadError> {
    let page: InputPage =
        postcard::from_bytes(row).map_err(|error| PageReadError::Storage(error.to_string()))?;
    match page.digest().map_err(PageError::from)? == *digest {
        true => Ok(page),
        false => Err(PageError::Digest { index: page.index }.into()),
    }
}

#[derive(Debug, PartialEq)]
enum ChainStep {
    Local([u8; 32]),
    Fetch([u8; 32], usize),
    Keep(Box<InputPage>),
}

/// Every page of `manifest`, read here or fetched from `peers` in order and kept here.
/// Each page must hash to the digest its predecessor or the manifest names.
#[derive(Debug, PartialEq)]
pub struct LoadChainOperation {
    submission: SubmissionId,
    manifest: PageManifest,
    peers: Vec<NodeId>,
    pages: Vec<InputPage>,
    step: Option<ChainStep>,
}

type ChainNext = Result<Next<Vec<InputPage>>, PageReadError>;

impl LoadChainOperation {
    pub fn new(submission: SubmissionId, manifest: PageManifest, peers: Vec<NodeId>) -> Self {
        Self {
            submission,
            manifest,
            peers,
            pages: Vec::with_capacity(manifest.pages as usize),
            step: None,
        }
    }

    fn index(&self) -> u32 {
        self.pages.len() as u32
    }

    fn read_next(&mut self) -> ChainNext {
        let digest = match self.pages.last() {
            Some(page) => page.next,
            None => (self.manifest.pages > 0).then_some(self.manifest.first),
        };
        let Some(digest) = digest else {
            verify_chain(&self.manifest, &self.pages)?;
            return Ok(Next::Done(std::mem::take(&mut self.pages)));
        };
        if self.index() >= self.manifest.pages {
            return Err(PageError::Chain.into());
        }
        self.step = Some(ChainStep::Local(digest));
        let key = page_key(&self.submission, &digest);
        Ok(Next::Call(read_row(INPUT_PAGE_KEYSPACE, key)))
    }

    /// Asks the next peer that can still be asked; a frame that cannot be built skips it.
    fn fetch(&mut self, digest: [u8; 32], mut peer: usize) -> ChainNext {
        let message = MetadataTransportMessage::JobInputPage {
            submission: self.submission,
            digest,
        };
        while let Some(node) = self.peers.get(peer) {
            match encode_message(&message) {
                Ok(frame) => {
                    self.step = Some(ChainStep::Fetch(digest, peer));
                    let request = MetadataRequestEffect {
                        peer: *node,
                        frame,
                        timeout: FETCH_DEADLINE,
                    };
                    let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
                    return Ok(Next::Call(IoCall::Effect(Box::new(effect))));
                }
                Err(error) => debug!(peer = %node, error = %error, "Input page request not built"),
            }
            peer += 1;
        }
        let index = self.index();
        Err(PageReadError::Unavailable { index })
    }

    fn fetched(&mut self, digest: [u8; 32], peer: usize, result: IoResult) -> ChainNext {
        let expected = "Event::Net(NetEvent::MetadataAnswer)";
        let IoResult::Event(event) = result else {
            let got = result.label();
            return Err(IoError::Unexpected { expected, got }.into());
        };
        let Event::Net(NetEvent::MetadataAnswer(answer)) = *event else {
            let got = "another event";
            return Err(IoError::Unexpected { expected, got }.into());
        };
        match answered_page(*answer, &digest) {
            Ok(Some(page)) => {
                let rows = page_rows(&self.submission, std::slice::from_ref(&page))?;
                self.step = Some(ChainStep::Keep(Box::new(page)));
                Ok(Next::Call(IoCall::Write(rows)))
            }
            Ok(None) => self.fetch(digest, peer + 1),
            Err(error) => {
                debug!(peer = %self.peers[peer], error = %error, "Input page fetch failed");
                self.fetch(digest, peer + 1)
            }
        }
    }
}

/// The page a peer answered, when it is the page asked for.
fn answered_page(answer: MetadataAnswer, digest: &[u8; 32]) -> Result<Option<InputPage>, String> {
    let bytes = answer.result.map_err(|error| format!("{error:?}"))?;
    match decode_message(&bytes).map_err(|error| error.to_string())? {
        MetadataTransportMessage::InputPageResult { page } => Ok(page
            .map(|page| *page)
            .filter(|page| page.digest().ok().as_ref() == Some(digest))),
        other => Err(format!(
            "the node answered {}",
            crate::metadata::transport_message_kind(&other)
        )),
    }
}

impl Operation for LoadChainOperation {
    type Output = Vec<InputPage>;
    type Error = PageReadError;

    fn first(&mut self) -> ChainNext {
        self.read_next()
    }

    fn next(&mut self, result: IoResult) -> ChainNext {
        match self.step.take() {
            Some(ChainStep::Local(digest)) => match value(result)? {
                Some(row) => {
                    self.pages.push(stored_page(&row, &digest)?);
                    self.read_next()
                }
                None => self.fetch(digest, 0),
            },
            Some(ChainStep::Fetch(digest, peer)) => self.fetched(digest, peer, result),
            Some(ChainStep::Keep(page)) => {
                done(result)?;
                self.pages.push(*page);
                self.read_next()
            }
            None => Err(IoError::Unfinished.into()),
        }
    }
}

/// The chain of `manifest`, read from `peers` and then from the other holders of the family,
/// which keep its pages with its records.
#[derive(Debug, PartialEq)]
struct FamilyChainOperation {
    submission: SubmissionId,
    manifest: PageManifest,
    peers: Vec<NodeId>,
    node: ServingNode,
    loading: bool,
}

impl FamilyChainOperation {
    fn new(
        submission: SubmissionId,
        manifest: PageManifest,
        peers: Vec<NodeId>,
        node: ServingNode,
    ) -> Self {
        Self {
            submission,
            manifest,
            peers,
            node,
            loading: false,
        }
    }

    fn load(&mut self) -> ChainNext {
        self.loading = true;
        let peers = std::mem::take(&mut self.peers);
        let load = LoadChainOperation::new(self.submission, self.manifest, peers);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn add_holders(&mut self, config: &RealmConfigDocument) {
        let local = self.node.map(|(local, _)| local);
        let holders = (config.family_placement(self.submission))
            .map(|placement| resolve_shard_holders(config, &placement))
            .unwrap_or_default();
        let others = holders.into_iter().filter(|holder| Some(*holder) != local);
        self.peers.extend(others);
    }
}

impl Operation for FamilyChainOperation {
    type Output = Vec<InputPage>;
    type Error = PageReadError;

    fn first(&mut self) -> ChainNext {
        match self.node {
            Some((_, realm_id)) => Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id)))),
            None => self.load(),
        }
    }

    fn next(&mut self, result: IoResult) -> ChainNext {
        if self.loading {
            return Ok(Next::Done(
                result.child::<Vec<InputPage>, PageReadError>()??,
            ));
        }
        // Without the realm config only the named peers can be asked.
        match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => self.add_holders(&config),
            Err(error) => warn!(error = %error, "Realm config unavailable for input pages"),
        }
        self.load()
    }
}

/// The spec with its paged inputs filled in, read here, from the nodes that admitted it or
/// from the family holders.
#[derive(Debug, PartialEq)]
pub struct SpecInputsOperation {
    spec: LogicalJobSpec,
    node: ServingNode,
}

impl SpecInputsOperation {
    pub fn new(spec: &LogicalJobSpec, node: ServingNode) -> Self {
        Self {
            spec: spec.clone(),
            node,
        }
    }
}

impl Operation for SpecInputsOperation {
    type Output = LogicalJobSpec;
    type Error = PageReadError;

    fn first(&mut self) -> Result<Next<LogicalJobSpec>, PageReadError> {
        let spec = &self.spec;
        let Some(manifest) = spec.input_pages else {
            return Ok(Next::Done(spec.clone()));
        };
        let peers = vec![
            spec.admission.admitting_node_id,
            spec.ingress_node_id,
            spec.origin_node_id,
        ];
        let chain = FamilyChainOperation::new(spec.submission_id, manifest, peers, self.node);
        Ok(Next::Call(IoCall::child(chain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LogicalJobSpec>, PageReadError> {
        let pages = result.child::<Vec<InputPage>, PageReadError>()??;
        let (inputs, captured): (Vec<_>, Vec<_>) = spec_entries(pages)?.into_iter().unzip();
        let mut filled = self.spec.clone();
        filled.payload.inputs = inputs;
        filled.captured_inputs = captured;
        Ok(Next::Done(filled))
    }
}

/// The launch's planned inputs, inline or read from its scheduler or the family holders.
#[derive(Debug, PartialEq)]
pub struct PlannedInputsOperation {
    launch: LaunchIntent,
    node: ServingNode,
}

impl PlannedInputsOperation {
    pub fn new(launch: &LaunchIntent, node: ServingNode) -> Self {
        Self {
            launch: launch.clone(),
            node,
        }
    }
}

impl Operation for PlannedInputsOperation {
    type Output = Vec<PlannedInput>;
    type Error = PageReadError;

    fn first(&mut self) -> Result<Next<Vec<PlannedInput>>, PageReadError> {
        let launch = &self.launch;
        let Some(manifest) = launch.input_pages else {
            return Ok(Next::Done(launch.inputs.clone()));
        };
        let peers = vec![launch.scheduler_node_id];
        let chain = FamilyChainOperation::new(launch.submission_id, manifest, peers, self.node);
        Ok(Next::Call(IoCall::child(chain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<PlannedInput>>, PageReadError> {
        let pages = result.child::<Vec<InputPage>, PageReadError>()??;
        Ok(Next::Done(launch_entries(pages)?))
    }
}

/// Keeps the pages a record binds, fetched from the peer that sent it, the nodes it names or
/// the family holders. A holder acknowledges such a record only once this succeeds.
#[derive(Debug, PartialEq)]
pub struct KeepPagesOperation {
    chain: Option<(SubmissionId, PageManifest, Vec<NodeId>)>,
    node: ServingNode,
}

impl KeepPagesOperation {
    pub fn new(record: &JobFamilyRecord, peer: NodeId, node: ServingNode) -> Self {
        let bound = match record {
            JobFamilyRecord::Spec(spec) => spec.input_pages.map(|manifest| {
                let peers = vec![
                    peer,
                    spec.admission.admitting_node_id,
                    spec.ingress_node_id,
                    spec.origin_node_id,
                ];
                (spec.submission_id, manifest, peers)
            }),
            JobFamilyRecord::Launch(launch) => launch.input_pages.map(|manifest| {
                let peers = vec![peer, launch.scheduler_node_id];
                (launch.submission_id, manifest, peers)
            }),
            _ => None,
        };
        Self { chain: bound, node }
    }
}

impl Operation for KeepPagesOperation {
    type Output = ();
    type Error = PageReadError;

    fn first(&mut self) -> Result<Next<()>, PageReadError> {
        let Some((submission, manifest, peers)) = self.chain.take() else {
            return Ok(Next::Done(()));
        };
        let chain = FamilyChainOperation::new(submission, manifest, peers, self.node);
        Ok(Next::Call(IoCall::child(chain)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PageReadError> {
        result.child::<Vec<InputPage>, PageReadError>()??;
        Ok(Next::Done(()))
    }
}

/// Keeps entries too large for their record in pages here and answers the manifest, or `None`
/// when they fit inline.
#[derive(Debug, PartialEq)]
pub struct StoreEntriesOperation {
    submission: SubmissionId,
    paged: Result<Option<(PageManifest, Vec<InputPage>)>, PageError>,
    manifest: Option<PageManifest>,
}

impl StoreEntriesOperation {
    /// The inputs of a request with their captures; every input must have one.
    pub fn request(
        submission: SubmissionId,
        inputs: &[InputSelection],
        captured: &[CapturedInput],
    ) -> Self {
        let pairs: Option<Vec<_>> = inputs
            .iter()
            .map(|input| {
                let capture = captured
                    .iter()
                    .find(|capture| capture.destination_key == input.dest_key)?;
                Some((input.clone(), capture.clone()))
            })
            .collect();
        let paged = pairs
            .ok_or(PageError::Chain)
            .and_then(|pairs| paged(&pairs, PageEntries::Spec));
        Self::with(submission, paged)
    }

    /// The planned inputs of a launch.
    pub fn launch(launch: &LaunchIntent) -> Self {
        let paged = paged(&launch.inputs, PageEntries::Launch);
        Self::with(launch.submission_id, paged)
    }

    fn with(
        submission: SubmissionId,
        paged: Result<Option<(PageManifest, Vec<InputPage>)>, PageError>,
    ) -> Self {
        Self {
            submission,
            paged,
            manifest: None,
        }
    }
}

fn paged<T: serde::Serialize + Clone>(
    entries: &[T],
    wrap: fn(Vec<T>) -> PageEntries,
) -> Result<Option<(PageManifest, Vec<InputPage>)>, PageError> {
    if !needs_pages(&entries).map_err(PageError::from)? {
        return Ok(None);
    }
    paginate(entries, wrap).map(Some).map_err(PageError::from)
}

impl Operation for StoreEntriesOperation {
    type Output = Option<PageManifest>;
    type Error = PageReadError;

    fn first(&mut self) -> Result<Next<Option<PageManifest>>, PageReadError> {
        let Some((manifest, pages)) = std::mem::replace(&mut self.paged, Ok(None))? else {
            return Ok(Next::Done(None));
        };
        self.manifest = Some(manifest);
        let rows = page_rows(&self.submission, &pages)?;
        Ok(Next::Call(IoCall::Write(rows)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<PageManifest>>, PageReadError> {
        done(result)?;
        Ok(Next::Done(self.manifest.take()))
    }
}

/// One page for a realm infrastructure node; a device never reads another's inputs.
#[derive(Debug, PartialEq)]
pub struct ServePageOperation {
    realm_id: RealmId,
    peer: NodeId,
    submission: SubmissionId,
    digest: [u8; 32],
    admitted: bool,
}

type ServedPage = Option<Box<InputPage>>;

impl ServePageOperation {
    pub fn new(
        realm_id: RealmId,
        peer: NodeId,
        submission: SubmissionId,
        digest: [u8; 32],
    ) -> Self {
        Self {
            realm_id,
            peer,
            submission,
            digest,
            admitted: false,
        }
    }
}

impl Operation for ServePageOperation {
    type Output = ServedPage;
    type Error = PageReadError;

    fn first(&mut self) -> Result<Next<ServedPage>, PageReadError> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ServedPage>, PageReadError> {
        if self.admitted {
            let page = value(result)?
                .map(|row| stored_page(&row, &self.digest))
                .transpose()?;
            return Ok(Next::Done(page.map(Box::new)));
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        if !config.is_ok_and(|config| is_sync_eligible(&config, self.peer)) {
            return Ok(Next::Done(None));
        }
        self.admitted = true;
        let key = page_key(&self.submission, &self.digest);
        Ok(Next::Call(read_row(INPUT_PAGE_KEYSPACE, key)))
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::execution::job::InputContent;
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::testing::test_context;
    use crate::tests::records::node;
    use crate::tests::s3::test_storage;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::PeerRequestError;

    fn submission() -> SubmissionId {
        SubmissionId([3; 32])
    }

    fn pins() -> Vec<PlannedInput> {
        (0..40u32)
            .map(|index| PlannedInput {
                destination_key: format!("{index:08}{}", "x".repeat(40 * 1024)),
                version_id: Ulid::from_parts(u64::from(index), 1),
                content: InputContent::Blake3([index as u8; 32]),
                bytes: 1,
                policies: Vec::new(),
                source_node_id: None,
                transfer_ms: 0,
                known_link: false,
            })
            .collect()
    }

    async fn store(context: &DriverContext, pages: &[InputPage]) {
        let writes = page_rows(&submission(), pages).unwrap();
        let txn_id = None;
        let effect = StorageEffect::BatchWrite { writes, txn_id };
        context.storage_handle.send_storage_effect(effect).await;
    }

    fn answer(peer: NodeId, result: Result<Vec<u8>, PeerRequestError>) -> IoResult {
        let answer = MetadataAnswer {
            peer,
            elapsed: Duration::ZERO,
            result,
        };
        IoResult::Event(Box::new(Event::Net(NetEvent::MetadataAnswer(Box::new(
            answer,
        )))))
    }

    fn page_answer(page: Option<InputPage>) -> Vec<u8> {
        let page = page.map(Box::new);
        encode_message(&MetadataTransportMessage::InputPageResult { page }).unwrap()
    }

    fn asked_peer(next: ChainNext) -> NodeId {
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("expected a page request: {next:?}");
        };
        let Effect::Net(NetEffect::MetadataRequest(request)) = *effect else {
            panic!("expected a metadata request");
        };
        request.peer
    }

    // A missing page is asked of each peer in turn; a wrong page or failure moves on, and the
    // page that matches is kept here before the chain continues.
    #[test]
    fn fetch_tries_peers() {
        let (manifest, pages) = paginate(&pins(), PageEntries::Launch).unwrap();
        let peers = vec![node(1), node(2), node(3)];
        let mut load = LoadChainOperation::new(submission(), manifest, peers);
        assert!(matches!(load.first(), Ok(Next::Call(IoCall::Read(_)))));
        let asked = load.next(IoResult::Values(vec![None]));
        assert_eq!(asked_peer(asked), node(1));
        let failed = answer(node(1), Err(PeerRequestError::TimedOut));
        assert_eq!(asked_peer(load.next(failed)), node(2));
        let wrong = answer(node(2), Ok(page_answer(Some(pages[1].clone()))));
        assert_eq!(asked_peer(load.next(wrong)), node(3));
        let right = answer(node(3), Ok(page_answer(Some(pages[0].clone()))));
        assert!(matches!(load.next(right), Ok(Next::Call(IoCall::Write(_)))));
        assert!(matches!(
            load.next(IoResult::Done),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        assert_eq!(load.pages, pages[..1]);
    }

    // When no peer has the page, the chain names the page it could not get.
    #[test]
    fn exhausted_peers_unavailable() {
        let (manifest, _) = paginate(&pins(), PageEntries::Launch).unwrap();
        let mut load = LoadChainOperation::new(submission(), manifest, vec![node(1)]);
        load.first().expect("the page is read here first");
        load.next(IoResult::Values(vec![None])).expect("one peer");
        let empty = answer(node(1), Ok(page_answer(None)));
        assert_eq!(
            load.next(empty),
            Err(PageReadError::Unavailable { index: 0 })
        );
    }

    // A stored page under the wrong digest is refused rather than used.
    #[test]
    fn stored_page_checked() {
        let (manifest, pages) = paginate(&pins(), PageEntries::Launch).unwrap();
        let mut load = LoadChainOperation::new(submission(), manifest, Vec::new());
        load.first().expect("the page is read here first");
        let row = postcard::to_allocvec(&pages[1]).unwrap();
        let read = load.next(IoResult::Values(vec![Some(Value::from(row))]));
        assert!(matches!(
            read,
            Err(PageReadError::Page(PageError::Digest { .. }))
        ));
    }

    // Without a network only the named peers can be asked; with one, the config comes first.
    #[test]
    fn family_chain_config() {
        let (manifest, _) = paginate(&pins(), PageEntries::Launch).unwrap();
        let mut alone = FamilyChainOperation::new(submission(), manifest, vec![node(1)], None);
        assert!(matches!(alone.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert!(alone.loading);
        let node_id = Some((node(2), crate::tests::records::REALM));
        let mut served = FamilyChainOperation::new(submission(), manifest, Vec::new(), node_id);
        assert!(matches!(served.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert!(!served.loading);
    }

    // Only a realm infrastructure node is served a page.
    #[test]
    fn device_gets_nothing() {
        let realm = crate::tests::records::REALM;
        let mut serve = ServePageOperation::new(realm, node(1), submission(), [1; 32]);
        serve.first().expect("the config is read");
        let config = RealmConfigDocument::new(realm, Vec::new(), 1);
        let answer = IoResult::Child(aruna_core::calls::ChildOutcome::new(
            Ok::<_, GetConfigError>(config),
        ));
        assert_eq!(serve.next(answer), Ok(Next::Done(None)));
    }

    // A launch that fits its record stores nothing.
    #[test]
    fn small_entries_inline() {
        let mut store = StoreEntriesOperation {
            submission: submission(),
            paged: Ok(None),
            manifest: None,
        };
        assert_eq!(store.first(), Ok(Next::Done(None)));
    }

    #[tokio::test]
    async fn pages_large_requests() {
        // Inputs past the inline budget leave the request for pages, and the stored spec gets
        // them back in order; a small request stays inline.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let family = crate::tests::records::Family::new([6; 32]);
        let mut spec = family.spec();
        let mut inputs = Vec::new();
        let mut captured = Vec::new();
        for index in 0..400u32 {
            let key = format!("db/{index:05}-{}", "x".repeat(800));
            inputs.push(InputSelection {
                source: aruna_core::structs::execution::job::InputSource::S3 {
                    bucket: "bakta".to_string(),
                    key: key.clone(),
                    version_id: Some(Ulid::from_parts(u64::from(index), 1).to_string()),
                },
                source_node_id: None,
                dest_key: key.clone(),
                mode: aruna_core::structs::execution::job::InputMode::Mount,
                container_path: Some(format!("/{key}")),
                name: None,
                description: None,
            });
            captured.push(CapturedInput {
                destination_key: key,
                source_node_id: family.holder.public(),
                version_id: Ulid::from_parts(u64::from(index), 1),
                content: InputContent::Blake3([1; 32]),
                bytes: 1,
                policies: Vec::new(),
                source_group_id: Ulid::from_parts(2, 2),
                public: false,
                link: None,
                source_pin: None,
                database: None,
                releases: Vec::new(),
                releases_complete: false,
                grant: None,
                served_by: None,
            });
        }
        let (full_inputs, full_captured) = (inputs.clone(), captured.clone());
        let store = StoreEntriesOperation::request(spec.submission_id, &inputs, &captured);
        let manifest = drive(store, &context).await.unwrap().expect("paged");
        assert!(manifest.pages > 1);
        spec.input_pages = Some(manifest);
        let filled = drive(SpecInputsOperation::new(&spec, None), &context)
            .await
            .unwrap();
        assert_eq!(filled.payload.inputs, full_inputs);
        assert_eq!(filled.captured_inputs, full_captured);
        let small = StoreEntriesOperation::request(
            spec.submission_id,
            &full_inputs[..2],
            &full_captured[..2],
        );
        assert_eq!(drive(small, &context).await.unwrap(), None);
    }

    #[tokio::test]
    async fn loads_local_chain() {
        // Stored pages load back in order; a page missing everywhere is unavailable.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (manifest, pages) = paginate(&pins(), PageEntries::Launch).unwrap();
        let load = |submission| LoadChainOperation::new(submission, manifest, Vec::new());
        store(&context, &pages[1..]).await;
        let missing = drive(load(submission()), &context).await;
        assert_eq!(missing, Err(PageReadError::Unavailable { index: 0 }));
        store(&context, &pages[..1]).await;
        let loaded = drive(load(submission()), &context).await.unwrap();
        assert_eq!(loaded, pages);
        let foreign = drive(load(SubmissionId([5; 32])), &context).await;
        assert_eq!(foreign, Err(PageReadError::Unavailable { index: 0 }));
    }
}
