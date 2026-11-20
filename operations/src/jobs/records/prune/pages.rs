//! Finds the input pages a pruned family names that no other family of its submission names, so
//! they go before the records that name them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{FAMILY_RECORD_KEYSPACE, INPUT_PAGE_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::execution::job::family::{
    JobFamilyId, JobFamilyRecord, JobRecordEnvelope,
};
use aruna_core::structs::execution::job::pages::PageManifest;
use aruna_core::structs::execution::job::record_keys::submission_prefix;
use aruna_core::structs::execution::job::record_rows::from_bytes;
use aruna_core::structs::execution::job::rows::JobDeletes;
use aruna_core::types::Key;

use super::{FAMILY_PRUNE_PAGE, PruneError};
use crate::jobs::records::pages::{page_key, stored_page};

/// One chain of pages being walked, and whether the pruned family names it.
#[derive(Debug, PartialEq)]
struct Chain {
    own: bool,
    manifest: PageManifest,
    digests: Vec<[u8; 32]>,
    next: Option<[u8; 32]>,
}

/// The page deletes of one pruned family, at most a page of them. Each own chain is listed from
/// its tail to its head, so the pages a batch leaves stay reachable from their manifest.
#[derive(Debug, PartialEq)]
pub struct PrunePagesOperation {
    family: JobFamilyId,
    chains: VecDeque<PageManifest>,
    owners: VecDeque<bool>,
    walking: Option<Chain>,
    own: Vec<[u8; 32]>,
    kept: BTreeSet<[u8; 32]>,
    scanned: bool,
}

type PagesNext = Result<Next<JobDeletes>, PruneError>;

impl PrunePagesOperation {
    pub fn new(family: JobFamilyId) -> Self {
        Self {
            family,
            chains: VecDeque::new(),
            owners: VecDeque::new(),
            walking: None,
            own: Vec::new(),
            kept: BTreeSet::new(),
            scanned: false,
        }
    }

    fn scan(&self, after: Option<Key>) -> PagesNext {
        Ok(Next::Call(IoCall::Page {
            space: FAMILY_RECORD_KEYSPACE.to_string(),
            prefix: submission_prefix(self.family.submission_id),
            after,
            limit: FAMILY_PRUNE_PAGE,
        }))
    }

    /// Reads the next page of the chain being walked, or starts the next chain.
    fn walk(&mut self) -> PagesNext {
        if let Some(chain) = self.walking.as_mut() {
            let left = chain.digests.len() < chain.manifest.pages as usize;
            if let Some(digest) = chain.next.filter(|_| left) {
                let key = page_key(&self.family.submission_id, &digest);
                return Ok(Next::Call(read_row(INPUT_PAGE_KEYSPACE, key)));
            }
            let chain = self.walking.take().ok_or(IoError::Unfinished)?;
            match chain.own {
                true => self.own.extend(chain.digests.into_iter().rev()),
                false => self.kept.extend(chain.digests),
            }
        }
        if let (Some(manifest), Some(own)) = (self.chains.pop_front(), self.owners.pop_front()) {
            let first = (manifest.pages > 0).then_some(manifest.first);
            self.walking = Some(Chain {
                own,
                manifest,
                digests: Vec::new(),
                next: first,
            });
            return self.walk();
        }
        Ok(Next::Done(self.deletes()))
    }

    fn deletes(&self) -> JobDeletes {
        let mut seen = BTreeSet::new();
        (self.own.iter())
            .filter(|digest| !self.kept.contains(*digest) && seen.insert(**digest))
            .take(FAMILY_PRUNE_PAGE)
            .map(|digest| {
                let key = page_key(&self.family.submission_id, digest);
                (INPUT_PAGE_KEYSPACE.to_string(), key)
            })
            .collect()
    }
}

impl Operation for PrunePagesOperation {
    type Output = JobDeletes;
    type Error = PruneError;

    fn first(&mut self) -> PagesNext {
        self.scan(None)
    }

    fn next(&mut self, result: IoResult) -> PagesNext {
        if self.scanned {
            let chain = self.walking.as_mut().ok_or(IoError::Unfinished)?;
            let digest = chain.next.take().ok_or(IoError::Unfinished)?;
            // A missing page ends its chain; what is left of it is unreachable already.
            if let Some(row) = result.value()? {
                let page =
                    stored_page(&row, &digest).map_err(|e| PruneError::Read(e.to_string()))?;
                chain.digests.push(digest);
                chain.next = page.next;
            }
            return self.walk();
        }
        let IoResult::Page { rows, next } = result else {
            return Err(IoError::Unfinished.into());
        };
        for (_, value) in rows {
            let envelope = from_bytes::<JobRecordEnvelope>(&value)?;
            let manifest = match &envelope.record {
                JobFamilyRecord::Spec(spec) => spec.input_pages,
                JobFamilyRecord::Launch(launch) => launch.input_pages,
                _ => None,
            };
            if let Some(manifest) = manifest {
                self.chains.push_back(manifest);
                self.owners.push_back(envelope.family() == self.family);
            }
        }
        if let Some(after) = next {
            return self.scan(Some(after));
        }
        self.scanned = true;
        self.walk()
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::execution::job::family::SubmissionId;
    use aruna_core::structs::execution::job::pages::{InputPage, PageEntries};
    use aruna_core::types::Value;

    use super::*;

    fn family(request: u8) -> JobFamilyId {
        JobFamilyId {
            submission_id: SubmissionId([5; 32]),
            request_digest: [request; 32],
        }
    }

    fn page(index: u32, next: Option<[u8; 32]>) -> (InputPage, [u8; 32]) {
        let page = InputPage {
            index,
            entries: PageEntries::Launch(Vec::new()),
            next,
        };
        let digest = page.digest().expect("page digest");
        (page, digest)
    }

    fn stored(page: &InputPage) -> IoResult {
        IoResult::Values(vec![Some(Value::from(
            postcard::to_allocvec(page).unwrap(),
        ))])
    }

    // An own chain goes tail first; a page another family names stays.
    #[test]
    fn chain_tail_first() {
        let (tail, tail_digest) = page(1, None);
        let (head, head_digest) = page(0, Some(tail_digest));
        let mut pages = PrunePagesOperation::new(family(1));
        assert!(matches!(pages.first(), Ok(Next::Call(IoCall::Page { .. }))));
        pages.chains = VecDeque::from([
            PageManifest {
                count: 0,
                pages: 2,
                first: head_digest,
            },
            PageManifest {
                count: 0,
                pages: 1,
                first: tail_digest,
            },
        ]);
        pages.owners = VecDeque::from([true, false]);
        let scanned = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert!(matches!(
            pages.next(scanned),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        assert!(matches!(
            pages.next(stored(&head)),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        assert!(matches!(
            pages.next(stored(&tail)),
            Ok(Next::Call(IoCall::Read(_)))
        ));
        let Ok(Next::Done(deletes)) = pages.next(stored(&tail)) else {
            panic!("the deletes are answered");
        };
        // The tail is kept by the other family, so only the head goes.
        let head_key = page_key(&family(1).submission_id, &head_digest);
        assert_eq!(deletes, vec![(INPUT_PAGE_KEYSPACE.to_string(), head_key)]);
    }
}
