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

#[cfg(test)]
mod tests {
    use aruna_core::structs::execution::job::InputContent;
    use ulid::Ulid;

    use super::*;
    use crate::testing::test_context;
    use crate::tests::s3::test_storage;

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
        let manifest = page_request(&context, &spec.submission_id, &mut inputs, &mut captured)
            .await
            .unwrap()
            .expect("paged");
        assert!(inputs.is_empty() && captured.is_empty() && manifest.pages > 1);
        spec.input_pages = Some(manifest);
        let filled = with_inputs(&context, &spec).await.unwrap();
        assert_eq!(filled.payload.inputs, full_inputs);
        assert_eq!(filled.captured_inputs, full_captured);
        let (mut small, mut small_captured) =
            (full_inputs[..2].to_vec(), full_captured[..2].to_vec());
        let inline = page_request(
            &context,
            &spec.submission_id,
            &mut small,
            &mut small_captured,
        );
        assert_eq!(inline.await.unwrap(), None);
        assert_eq!(small.len(), 2);
    }

    #[tokio::test]
    async fn loads_local_chain() {
        // Stored pages load back in order; a page missing everywhere is unavailable.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (manifest, pages) = paginate(&pins(), PageEntries::Launch).unwrap();
        store_pages(&context, &submission(), &pages[1..])
            .await
            .unwrap();
        let missing = load_chain(&context, &submission(), &manifest, &[]).await;
        assert_eq!(missing, Err(PageReadError::Unavailable { index: 0 }));
        store_pages(&context, &submission(), &pages[..1])
            .await
            .unwrap();
        let loaded = load_chain(&context, &submission(), &manifest, &[])
            .await
            .unwrap();
        assert_eq!(loaded, pages);
        let other = SubmissionId([5; 32]);
        let foreign = load_chain(&context, &other, &manifest, &[]).await;
        assert_eq!(foreign, Err(PageReadError::Unavailable { index: 0 }));
    }
}
