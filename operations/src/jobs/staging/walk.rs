//! Walks one source listing page of a staging job: lists it, maps entries to keys and queue
//! rows, and records the page through the page import or the snapshot copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::path::{Component, Path};
use std::time::SystemTime;

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm};
use aruna_core::errors::{AuthorizationError, StagingSourceError};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::keyspaces::STAGING_QUEUE_KEYSPACE;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::rows::values;
use aruna_core::structs::execution::job::{
    JobId, StagingJobCheckpoint, StagingJobDirectory, StagingJobError, StagingJobPhase,
    StagingJobSpec, StagingWalk,
};
use aruna_core::structs::execution::source_access::{
    DigestOrigin, DigestScope, ListRequest, SourceDigest, SourceEntry, SourceEntryKind,
    SourceMetadata,
};
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, WriteRoute,
};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::limiter::{retried_call, retried_result};
use super::manifest::{
    ManifestDigest, digest_reads, lasting_resolution, lasting_source, parse_digests,
};
use super::page::{ImportPageOperation, PageSource, SnapshotPageOperation};
use super::{ItemFailure, PageFailure, StagingRun, committed, pinned, queue_key};
use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::auth::permission_rules::{
    PermissionRules, PermissionRulesConfig, PermissionRulesOperation,
};
use crate::distribution::guard::{GuardError, authority_reads, write_target, write_targets};
use crate::staging::list_source::{
    ListStagingError, ListStagingInput, ListStagingOperation, ListStagingResult,
};
use crate::staging::page::rows::MAX_ERROR_SAMPLE;

/// Entries one listing request reads.
const STAGING_PAGE_SIZE: usize = 1000;
/// Entries a flat reference walk gathers from several listings into one page transaction.
/// Its commit holds the single storage writer, so other requests wait for it: keep it short.
const COMMIT_ENTRIES: usize = 2000;
/// Bytes of listed source paths one gathered page holds at most.
const COMMIT_PATH_BYTES: usize = 4 << 20;
/// Snapshot pages copy every file before their checkpoint, so they stay smaller.
const SNAPSHOT_PAGE_SIZE: usize = 100;

/// The next walk of `checkpoint` from the spec's prefixes, with its manifest, when no directory
/// waits in the queue; `false` once nothing is left to list.
pub(super) fn start_prefix_walk(
    spec: &StagingJobSpec,
    checkpoint: &mut StagingJobCheckpoint,
) -> bool {
    let Some(prefix) = usize::try_from(checkpoint.prefixes_done)
        .ok()
        .and_then(|done| spec.prefixes.get(done))
    else {
        return false;
    };
    checkpoint.walk = Some(StagingWalk {
        source_path: directory_list_path(&prefix.source_prefix),
        target_prefix: prefix.target_prefix.clone(),
        cursor: None,
        queue_row: None,
        from: None,
    });
    // The prefix's manifest is read before its first page, so every page can look it up.
    checkpoint.manifest = spec.checksum_manifest.as_ref().map(|name| {
        aruna_core::structs::execution::job::ManifestRead {
            source_path: format!("{}{name}", directory_list_path(&prefix.source_prefix)),
            offset: 0,
            observation: None,
        }
    });
    checkpoint.prefixes_done += 1;
    true
}

/// Takes the walk of a queued directory, or passes over a row that is gone.
pub(super) fn start_queued_walk(
    checkpoint: &mut StagingJobCheckpoint,
    directory: Option<StagingJobDirectory>,
) {
    match directory {
        Some(directory) => {
            checkpoint.walk = Some(StagingWalk {
                source_path: directory.source_path,
                target_prefix: directory.target_prefix,
                cursor: None,
                queue_row: Some(checkpoint.queue_head),
                from: None,
            });
        }
        None => checkpoint.queue_head += 1,
    }
}

/// The queue row the job reads next.
pub(super) fn queue_read(job_id: JobId, position: u64) -> IoCall {
    let row = (
        STAGING_QUEUE_KEYSPACE.to_string(),
        queue_key(job_id, position),
    );
    IoCall::Read(vec![row])
}

/// Decodes a queued directory.
pub(super) fn parse_queued(result: IoResult) -> Result<Option<StagingJobDirectory>, String> {
    let [row] = values::<1>(result).map_err(|error| error.to_string())?;
    row.map(|value| postcard::from_bytes(&value))
        .transpose()
        .map_err(|error| error.to_string())
}

pub(super) fn item_phase(spec: &StagingJobSpec) -> StagingJobPhase {
    match spec.strategy {
        StagingStrategy::Snapshot => StagingJobPhase::Downloading,
        StagingStrategy::Reference | StagingStrategy::Sync => StagingJobPhase::Registering,
    }
}

/// Counts one failed item and keeps its error while the sample has room.
pub(super) fn record_failure(
    checkpoint: &mut StagingJobCheckpoint,
    source_path: &str,
    target_key: &str,
    error: String,
) {
    checkpoint.items_current = checkpoint.items_current.saturating_add(1);
    checkpoint.items_failed = checkpoint.items_failed.saturating_add(1);
    if checkpoint.errors.len() < MAX_ERROR_SAMPLE {
        checkpoint.errors.push(StagingJobError {
            source_path: source_path.to_string(),
            target_key: target_key.to_string(),
            error,
        });
    }
}

/// A file a listing page or a table row found, mapped to its destination key.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct StagedFile {
    pub(super) source_path: String,
    pub(super) target_key: String,
    pub(super) size: Option<u64>,
    pub(super) observation: Option<SourceMetadata>,
    /// Shipped digests, from a checksum manifest or a table row.
    pub(super) manifest: Vec<ManifestDigest>,
    pub(super) metadata: BTreeMap<String, String>,
    /// A table row's modification time; it replaces the one a HEAD reports.
    pub(super) modified: Option<SystemTime>,
    /// Further sources a table row names, each with its connector, as the row renders them.
    pub(super) mirrors: Vec<(Ulid, String)>,
}

/// A walk's position in the job: the checkpoint row it writes, the key it stops before and
/// whether its page transactions take turns with other lanes.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct WalkPlace {
    pub(super) slot: Option<u64>,
    pub(super) upper: Option<String>,
    pub(super) lane: bool,
}

/// What a walked page leaves: the checkpoint and the next listing, when one was made ahead.
#[derive(Debug, PartialEq)]
pub(super) struct WalkedPage {
    pub(super) checkpoint: StagingJobCheckpoint,
    pub(super) ahead: Option<Ahead>,
}

/// A listing made ahead for the walk position it was made for.
#[derive(Debug, PartialEq)]
pub(super) struct Ahead {
    walk: StagingWalk,
    listing: Listing,
}

/// What one listing of a walk position found.
#[derive(Debug, PartialEq)]
pub(super) enum Listing {
    /// No coordinated range of the walk is left.
    Done,
    Page(Option<Segment>, Result<ListStagingResult, ItemFailure>),
}

#[derive(Debug, PartialEq)]
enum WalkStep {
    Listing,
    Digests,
    Commit,
    Import,
    Both,
    Snapshot,
}

/// Lists one page of the current walk and records it, with the walk's next position and the
/// queue changes, in one write. The next page of a flat reference walk is listed meanwhile.
#[derive(Debug, PartialEq)]
pub(super) struct WalkPageOperation {
    run: StagingRun,
    place: WalkPlace,
    checkpoint: StagingJobCheckpoint,
    next: StagingJobCheckpoint,
    walk: Option<StagingWalk>,
    ahead: Option<Ahead>,
    following: Option<StagingWalk>,
    page: Option<ListStagingResult>,
    files: Vec<StagedFile>,
    writes: Vec<(KeySpace, Key, Value)>,
    deletes: Vec<(KeySpace, Key)>,
    step: WalkStep,
}

impl WalkPageOperation {
    pub(super) fn new(
        run: StagingRun,
        place: WalkPlace,
        mut checkpoint: StagingJobCheckpoint,
        ahead: Option<Ahead>,
    ) -> Self {
        let walk = checkpoint.walk.clone();
        if let Some(walk) = &walk {
            checkpoint.phase = StagingJobPhase::Discovering;
            checkpoint.current_path = Some(walk.source_path.clone());
        }
        Self {
            run,
            place,
            next: checkpoint.clone(),
            checkpoint,
            walk,
            ahead,
            following: None,
            page: None,
            files: Vec::new(),
            writes: Vec::new(),
            deletes: Vec::new(),
            step: WalkStep::Listing,
        }
    }

    fn flat(&self) -> bool {
        self.checkpoint.flat == Some(true)
    }

    fn done(&mut self, checkpoint: StagingJobCheckpoint) -> Next<WalkedPage> {
        let ahead = self.ahead.take();
        Next::Done(WalkedPage { checkpoint, ahead })
    }

    fn commit(&mut self) -> Result<Next<WalkedPage>, PageFailure> {
        self.step = WalkStep::Commit;
        let (writes, deletes) = (
            std::mem::take(&mut self.writes),
            std::mem::take(&mut self.deletes),
        );
        let call = (self.run)
            .commit_call(self.place.slot, &self.next, writes, deletes)
            .map_err(PageFailure::Retry)?;
        Ok(Next::Call(call))
    }

    fn listed(&mut self, listing: Listing) -> Result<Next<WalkedPage>, PageFailure> {
        let Some(walk) = self.walk.clone() else {
            return Ok(self.done(self.checkpoint.clone()));
        };
        let (segment, listed) = match listing {
            Listing::Done => {
                self.next.walk = None;
                return self.commit();
            }
            Listing::Page(segment, listed) => (segment, listed),
        };
        let mut page = match listed {
            Ok(page) => Some(page),
            Err(ItemFailure::Denied(message) | ItemFailure::Stage(message)) => {
                record_failure(
                    &mut self.next,
                    &walk.source_path,
                    &walk.target_prefix,
                    message,
                );
                None
            }
            Err(failure) => return Err(failure.into()),
        };
        let next_cursor = page.as_ref().and_then(|page| page.next_cursor.clone());
        if next_cursor.is_some() && next_cursor == walk.cursor {
            return Err(PageFailure::Permanent(
                "source listing pagination made no progress".to_string(),
            ));
        }
        match next_cursor {
            Some(cursor) => {
                self.next.walk = Some(StagingWalk {
                    cursor: Some(cursor),
                    ..walk.clone()
                })
            }
            None => {
                // The next coordinated range continues where this one ends.
                self.next.walk = (segment.as_ref())
                    .and_then(|segment| segment.upper.clone())
                    .map(|upper| StagingWalk {
                        cursor: None,
                        from: Some(upper),
                        ..walk.clone()
                    });
                if let Some(row) = walk.queue_row {
                    self.deletes.push((
                        STAGING_QUEUE_KEYSPACE.to_string(),
                        queue_key(self.run.job_id, row),
                    ));
                    self.next.queue_head = row + 1;
                }
            }
        }
        // HTTP index pages round sizes and print local minutes, so each file gets a HEAD (step 0).
        let exact =
            (page.as_ref()).is_some_and(|page| page.connector.kind != SourceConnectorKind::Http);
        let entries = page
            .as_mut()
            .map(|page| std::mem::take(&mut page.entries))
            .unwrap_or_default();
        let flat = self.flat();
        for entry in entries {
            match map_source_entry(&walk, entry, (flat, exact)) {
                Ok(MappedEntry::File(file)) => {
                    self.next.bytes_discovered = self
                        .next
                        .bytes_discovered
                        .saturating_add(file.size.unwrap_or(0));
                    self.files.push(file);
                }
                Ok(MappedEntry::Directory(directory)) => {
                    let value = postcard::to_allocvec(&directory)
                        .map_err(|error| PageFailure::Permanent(error.to_string()))?;
                    let key = queue_key(self.run.job_id, self.next.queue_tail);
                    self.writes.push((
                        STAGING_QUEUE_KEYSPACE.to_string(),
                        key,
                        ByteView::from(value),
                    ));
                    self.next.queue_tail += 1;
                }
                Err(message) => record_failure(
                    &mut self.next,
                    &walk.source_path,
                    &walk.target_prefix,
                    message,
                ),
            }
        }
        self.page = page;
        if self.run.spec.checksum_manifest.is_some() {
            self.step = WalkStep::Digests;
            let paths: Vec<&str> = (self.files.iter())
                .map(|file| file.source_path.as_str())
                .collect();
            return Ok(Next::Call(digest_reads(self.run.job_id, &paths)));
        }
        self.record()
    }

    /// Records the listed page: the listing failed for good, or its files go on.
    fn record(&mut self) -> Result<Next<WalkedPage>, PageFailure> {
        self.next.phase = item_phase(&self.run.spec);
        let Some(page) = self.page.take() else {
            return self.commit();
        };
        let rows = (
            std::mem::take(&mut self.files),
            std::mem::take(&mut self.writes),
            std::mem::take(&mut self.deletes),
        );
        let next = self.next.clone();
        match self.run.spec.strategy {
            StagingStrategy::Reference => {
                let source: PageSource = (page.connector, page.secret_fingerprint);
                let import = ImportPageOperation::new(
                    self.run.clone(),
                    self.place.clone(),
                    next,
                    rows,
                    Some(source),
                );
                // The next page of a flat walk is listed while this one commits; one at most.
                let flat = self.flat();
                let following =
                    (self.next.walk.clone()).filter(|walk| flat && walk.queue_row.is_none());
                let Some(walk) = following else {
                    self.step = WalkStep::Import;
                    return Ok(Next::Call(IoCall::child(import)));
                };
                self.step = WalkStep::Both;
                let upper = self.place.upper.clone();
                let listing = ListingOperation::new(self.run.clone(), walk.clone(), flat, upper);
                self.following = Some(walk);
                let members = vec![member(import), member(listing)];
                Ok(Next::Call(IoCall::Group(EffectGroup {
                    members,
                    limit: 2,
                    form: GroupForm::All { deadline: None },
                })))
            }
            StagingStrategy::Snapshot | StagingStrategy::Sync => {
                self.step = WalkStep::Snapshot;
                let copy = SnapshotPageOperation::new(
                    self.run.clone(),
                    self.checkpoint.clone(),
                    next,
                    rows,
                );
                Ok(Next::Call(IoCall::child(copy)))
            }
        }
    }
}

/// A child for an effect group, whose typed outcome the group answers with.
pub(super) fn member<O>(operation: O) -> Effect
where
    O: Operation + 'static,
    O::Output: PartialEq + 'static,
    O::Error: 'static,
{
    Effect::SubOperation(boxed_suboperation(operation, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// The typed outcome of a group member.
pub(super) fn member_outcome<T: 'static, E: 'static>(
    result: GroupResult,
) -> Result<Result<T, E>, aruna_core::calls::IoError> {
    match result {
        GroupResult::Done(event) => super::limiter::child_of::<T, E>(*event),
        GroupResult::Unfinished | GroupResult::Refused => {
            Err(aruna_core::calls::IoError::Unexpected {
                expected: "a finished group member",
                got: "an unfinished or refused member",
            })
        }
    }
}

impl Operation for WalkPageOperation {
    type Output = WalkedPage;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<WalkedPage>, PageFailure> {
        let Some(walk) = self.walk.clone() else {
            return Ok(self.done(self.checkpoint.clone()));
        };
        match self.ahead.take().filter(|ahead| ahead.walk == walk) {
            Some(ahead) => self.listed(ahead.listing),
            None => {
                let upper = self.place.upper.clone();
                let listing = ListingOperation::new(self.run.clone(), walk, self.flat(), upper);
                Ok(Next::Call(IoCall::child(listing)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WalkedPage>, PageFailure> {
        match self.step {
            WalkStep::Listing => {
                let listing = result.child::<Listing, PageFailure>()??;
                self.listed(listing)
            }
            WalkStep::Digests => {
                let digests = parse_digests(result).map_err(PageFailure::Retry)?;
                for (file, digest) in self.files.iter_mut().zip(digests) {
                    file.manifest = digest.into_iter().collect();
                }
                self.record()
            }
            WalkStep::Commit => {
                committed(result).map_err(PageFailure::Retry)?;
                Ok(self.done(self.next.clone()))
            }
            WalkStep::Import | WalkStep::Snapshot => {
                let checkpoint = result.child::<StagingJobCheckpoint, PageFailure>()??;
                Ok(self.done(checkpoint))
            }
            WalkStep::Both => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(aruna_core::calls::IoError::Unexpected {
                        expected: "the finished page and listing",
                        got: result.label(),
                    }
                    .into());
                };
                let mut results = results.into_iter();
                let (Some(imported), Some(fetched)) = (results.next(), results.next()) else {
                    return Err(PageFailure::Retry("a page member is missing".to_string()));
                };
                let checkpoint = member_outcome::<StagingJobCheckpoint, PageFailure>(imported)??;
                let listing = member_outcome::<Listing, PageFailure>(fetched)?.ok();
                self.ahead = (self.following.take())
                    .zip(listing)
                    .map(|(walk, listing)| Ahead { walk, listing });
                Ok(self.done(checkpoint))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ListingStep {
    Bounds,
    First,
    More,
}

/// Lists the page at a walk position; a flat walk into a distributed bucket lists only the key
/// ranges this node coordinates, and a flat reference walk gathers several listings.
#[derive(Debug, PartialEq)]
pub(super) struct ListingOperation {
    run: StagingRun,
    walk: StagingWalk,
    flat: bool,
    upper: Option<String>,
    segment: Option<Segment>,
    page: Option<ListStagingResult>,
    step: ListingStep,
}

impl ListingOperation {
    pub(super) fn new(
        run: StagingRun,
        walk: StagingWalk,
        flat: bool,
        upper: Option<String>,
    ) -> Self {
        Self {
            run,
            walk,
            flat,
            upper,
            segment: None,
            page: None,
            step: ListingStep::Bounds,
        }
    }

    fn list(&self, walk: StagingWalk) -> IoCall {
        let segment = self.segment.clone();
        IoCall::child(ListPageOperation::new(
            self.run.clone(),
            walk,
            self.flat,
            segment,
        ))
    }

    fn list_first(&mut self) -> Next<Listing> {
        self.step = ListingStep::First;
        Next::Call(self.list(self.walk.clone()))
    }

    fn end(&mut self, page: Result<ListStagingResult, ItemFailure>) -> Next<Listing> {
        Next::Done(Listing::Page(self.segment.take(), page))
    }

    /// Lists on from the gathered page, or ends it: it is full or the listing is exhausted.
    fn gather(&mut self, page: ListStagingResult) -> Next<Listing> {
        let spec = &self.run.spec;
        // A flat reference walk is an ordered stream, so its next pages join the same transaction.
        let gathers = self.flat
            && self.walk.queue_row.is_none()
            && spec.checksum_manifest.is_none()
            && spec.strategy == StagingStrategy::Reference;
        let bytes: usize = (page.entries.iter()).map(|entry| entry.path.len()).sum();
        let full = page.entries.len() + STAGING_PAGE_SIZE > COMMIT_ENTRIES;
        let cursor = page.next_cursor.clone().filter(|_| !full);
        match cursor.filter(|_| gathers && bytes <= COMMIT_PATH_BYTES) {
            Some(cursor) => {
                self.step = ListingStep::More;
                self.page = Some(page);
                Next::Call(self.list(StagingWalk {
                    cursor: Some(cursor),
                    ..self.walk.clone()
                }))
            }
            None => self.end(Ok(page)),
        }
    }
}

impl Operation for ListingOperation {
    type Output = Listing;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<Listing>, PageFailure> {
        if !self.flat || self.walk.queue_row.is_some() {
            return Ok(self.list_first());
        }
        Ok(Next::Call(IoCall::child(SegmentOperation::new(
            self.run.clone(),
            self.walk.clone(),
            self.upper.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Listing>, PageFailure> {
        match self.step {
            ListingStep::Bounds => {
                match result.child::<Bounds, PageFailure>()?? {
                    Bounds::Whole => {}
                    Bounds::Segment(segment) => self.segment = Some(segment),
                    Bounds::Done => return Ok(Next::Done(Listing::Done)),
                }
                Ok(self.list_first())
            }
            ListingStep::First => match result.child::<ListStagingResult, ItemFailure>()? {
                Ok(page) => Ok(self.gather(page)),
                Err(failure) => Ok(self.end(Err(failure))),
            },
            ListingStep::More => {
                let more = result.child::<ListStagingResult, ItemFailure>()?;
                let Some(mut page) = self.page.take() else {
                    return Err(PageFailure::Retry("the gathered page is gone".to_string()));
                };
                // A failed listing ends the gathered page; the walk lists it again from its cursor.
                let Ok(more) = more else {
                    return Ok(self.end(Ok(page)));
                };
                if more.connector != page.connector
                    || more.secret_fingerprint != page.secret_fingerprint
                {
                    return Ok(self.end(Ok(page)));
                }
                page.entries.extend(more.entries);
                page.next_cursor = more.next_cursor;
                Ok(self.gather(page))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ListPageStep {
    Allowed,
    Listed,
}

/// One listing request, after the submitter's READ on the walk's path is checked.
#[derive(Debug, PartialEq)]
struct ListPageOperation {
    run: StagingRun,
    walk: StagingWalk,
    flat: bool,
    segment: Option<Segment>,
    step: ListPageStep,
}

impl ListPageOperation {
    fn new(run: StagingRun, walk: StagingWalk, flat: bool, segment: Option<Segment>) -> Self {
        Self {
            run,
            walk,
            flat,
            segment,
            step: ListPageStep::Allowed,
        }
    }
}

impl Operation for ListPageOperation {
    type Output = ListStagingResult;
    type Error = ItemFailure;

    fn first(&mut self) -> Result<Next<ListStagingResult>, ItemFailure> {
        let spec = &self.run.spec;
        Ok(Next::Call(read_check_call(
            spec,
            source_permission_path(spec, &self.walk.source_path),
        )))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ListStagingResult>, ItemFailure> {
        match self.step {
            ListPageStep::Allowed => {
                let allowed = (result.child::<bool, AuthorizationError>()?)
                    .map_err(|error| ItemFailure::System(error.to_string()))?;
                if !allowed {
                    return Err(ItemFailure::Denied("permission denied".to_string()));
                }
                self.step = ListPageStep::Listed;
                let spec = &self.run.spec;
                let limit = match spec.strategy {
                    StagingStrategy::Snapshot => SNAPSHOT_PAGE_SIZE,
                    StagingStrategy::Reference | StagingStrategy::Sync => STAGING_PAGE_SIZE,
                };
                let segment = self.segment.as_ref();
                let input = ListStagingInput {
                    group_id: spec.group_id,
                    connector_id: spec.connector_id,
                    source_path: self.walk.source_path.clone(),
                    request: ListRequest {
                        cursor: self.walk.cursor.clone(),
                        lower: segment.and_then(|segment| segment.lower.clone()),
                        stop: segment.and_then(|segment| segment.stop.clone()),
                        limit,
                        recursive: self.flat,
                        files_only: self.flat,
                    },
                };
                let transient: fn(&ListStagingError) -> bool = |error| !lasting_list(error);
                Ok(Next::Call(retried_call(
                    &self.run,
                    input,
                    ListStagingOperation::new,
                    transient,
                )))
            }
            ListPageStep::Listed => {
                let listed = retried_result::<ListStagingResult, ListStagingError>(result)?;
                listed.map(Next::Done).map_err(list_failure)
            }
        }
    }
}

/// READ on `path` for the job's submitter.
pub(super) fn read_check_call(spec: &StagingJobSpec, path: String) -> IoCall {
    IoCall::child(CheckPermissionsOperation::new(CheckPermissionsConfig {
        auth_context: spec.auth_context.clone(),
        path,
        required_permission: Permission::READ,
    }))
}

/// The submitter's permission rules in the job's group, which holds the source and target paths.
pub(super) fn rules_call(spec: &StagingJobSpec) -> IoCall {
    IoCall::child(PermissionRulesOperation::new(PermissionRulesConfig {
        auth_context: spec.auth_context.clone(),
        path: source_permission_path(spec, ""),
    }))
}

/// The answer to [`rules_call`].
pub(super) fn rules_result(result: IoResult) -> Result<PermissionRules, String> {
    (result.child::<PermissionRules, AuthorizationError>())
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())
}

/// Reads the record and fence row of a distributed bucket's set.
pub(super) fn authority_call(binding: &DistributionBinding) -> IoCall {
    IoCall::Read(authority_reads(binding.set_id))
}

/// A source that refuses the path skips it; any other listing error retries the page, so one
/// transient failure never abandons the rest of a large walk.
fn list_failure(error: ListStagingError) -> ItemFailure {
    match lasting_list(&error) {
        true => ItemFailure::Stage(error.to_string()),
        false => ItemFailure::System(error.to_string()),
    }
}

fn lasting_list(error: &ListStagingError) -> bool {
    match error {
        ListStagingError::Resolve(error) => lasting_resolution(error),
        ListStagingError::Staging(StagingSourceError::InvalidListing(_)) => true,
        ListStagingError::Staging(error) => lasting_source(error),
        _ => false,
    }
}

/// The range of each of `keys` if this node coordinates it, else `None`. A range this node
/// fenced for a transfer is skipped; its new coordinator imports it on its next run.
pub(super) fn local_ranges(
    binding: &DistributionBinding,
    keys: &[&str],
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<Vec<Option<Ulid>>, String> {
    let routes = match write_targets(binding, keys.iter().copied(), record, fence) {
        Ok(routes) => routes,
        Err(error) if error.moved_away() => return Ok(vec![None; keys.len()]),
        Err(error) => return Err(error.to_string()),
    };
    (routes.into_iter())
        .map(|route| match route {
            Ok(WriteRoute::Local { range_id, .. }) => Ok(Some(range_id)),
            Ok(WriteRoute::Forward { .. }) => Ok(None),
            Err(error) if error.moved_away() => Ok(None),
            Err(error) => Err(error.to_string()),
        })
        .collect()
}

/// The range turn each of `keys` is written in, `None` for a key this node does not import; a
/// bucket without ranges needs no turn.
pub(super) fn key_turns(
    binding: &DistributionBinding,
    keys: &[&str],
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<Vec<Option<Turns>>, String> {
    let ranges = local_ranges(binding, keys, record, fence)?;
    let turn = |range| Turns::from([(binding.set_id, range)]);
    Ok(ranges.into_iter().map(|range| range.map(turn)).collect())
}

/// Range turns, each a distributed set and one of its ranges.
pub(super) type Turns = std::collections::BTreeSet<(Ulid, Ulid)>;

/// The turn the lanes of one job share, so their page transactions run one at a time.
pub(super) fn lane_turn(job_id: JobId) -> (Ulid, Ulid) {
    (job_id.as_ulid(), Ulid::nil())
}

/// The files of a page this node imports, and the turn of every range they fall in.
pub(super) fn own_files(
    binding: &DistributionBinding,
    files: Vec<StagedFile>,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<(Vec<StagedFile>, Turns), PageFailure> {
    let keys: Vec<&str> = files.iter().map(|file| file.target_key.as_str()).collect();
    let ranges = local_ranges(binding, &keys, record, fence).map_err(PageFailure::Retry)?;
    let mut turns = Turns::new();
    let files = (files.into_iter().zip(ranges))
        .filter_map(|(file, range)| {
            turns.insert((binding.set_id, range?));
            Some(file)
        })
        .collect();
    Ok((files, turns))
}

/// The key ranges of a distributed bucket this node coordinates, as lower and upper keys.
pub(super) fn own_ranges(
    binding: &DistributionBinding,
    record: Option<&[u8]>,
    fence: Option<&[u8]>,
) -> Result<Vec<(String, Option<String>)>, PageFailure> {
    let retry = |error: GuardError| PageFailure::Retry(error.to_string());
    let view = (record.map(DistributionView::from_bytes).transpose())
        .map_err(|error| retry(error.into()))?
        .ok_or_else(|| retry(GuardError::RecordMissing(binding.set_id)))?;
    // A range this node fenced for a transfer is left to its new coordinator.
    let mut coordinated = Vec::new();
    for range in &view.snapshot().config.ranges {
        match write_target(binding, &range.lower, record, fence) {
            Ok(WriteRoute::Local { .. }) => {
                coordinated.push((range.lower.clone(), range.upper.clone()))
            }
            Ok(WriteRoute::Forward { .. }) => {}
            Err(error) if error.moved_away() => {}
            Err(error) => return Err(retry(error)),
        }
    }
    Ok(coordinated)
}

#[derive(Debug, PartialEq)]
enum SegmentStep {
    Bucket,
    Authority(Box<BucketInfo>),
}

/// The bounds of a flat walk's next listing: the key ranges a distributed destination has this
/// node coordinate, or the whole bucket, cut at a lane's exclusive upper target key.
#[derive(Debug, PartialEq)]
struct SegmentOperation {
    run: StagingRun,
    walk: StagingWalk,
    upper: Option<String>,
    step: SegmentStep,
}

impl SegmentOperation {
    fn new(run: StagingRun, walk: StagingWalk, upper: Option<String>) -> Self {
        Self {
            run,
            walk,
            upper,
            step: SegmentStep::Bucket,
        }
    }
}

impl Operation for SegmentOperation {
    type Output = Bounds;
    type Error = PageFailure;

    fn first(&mut self) -> Result<Next<Bounds>, PageFailure> {
        Ok(Next::Call(self.run.bucket_call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Bounds>, PageFailure> {
        let upper = self.upper.as_deref();
        match std::mem::replace(&mut self.step, SegmentStep::Bucket) {
            SegmentStep::Bucket => {
                let bucket = pinned(&self.run.spec, result)?;
                let Some(binding) = bucket.distribution.as_ref() else {
                    if upper.is_none() && self.walk.from.is_none() {
                        return Ok(Next::Done(Bounds::Whole));
                    }
                    let whole = clamped(vec![(String::new(), None)], upper);
                    return Ok(Next::Done(next_segment(&self.walk, whole)));
                };
                let call = authority_call(binding);
                self.step = SegmentStep::Authority(Box::new(bucket));
                Ok(Next::Call(call))
            }
            SegmentStep::Authority(bucket) => {
                let Some(binding) = bucket.distribution.as_ref() else {
                    return Ok(Next::Done(Bounds::Whole));
                };
                let [record, fence] = values::<2>(result)?;
                let ranges = own_ranges(binding, record.as_deref(), fence.as_deref())?;
                Ok(Next::Done(next_segment(&self.walk, clamped(ranges, upper))))
            }
        }
    }
}

/// Where a target key falls among the source keys of a flat walk.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Mapped {
    /// Before every target key of the walk.
    Before,
    /// The source key that maps to it.
    At(String),
    /// After every target key of the walk.
    After,
}

/// The source key of a flat walk that target key `key` stands for: target keys are the walk's
/// target prefix plus the path below its source path, so the mapping keeps key order.
fn source_key(walk: &StagingWalk, key: &str) -> Mapped {
    let target = walk.target_prefix.trim_matches('/');
    let target = match target.is_empty() {
        true => String::new(),
        false => format!("{target}/"),
    };
    let relative = match key.strip_prefix(target.as_str()) {
        Some(relative) if !relative.is_empty() => relative,
        Some(_) => return Mapped::Before,
        None if key < target.as_str() => return Mapped::Before,
        None => return Mapped::After,
    };
    let base = walk.source_path.trim_matches('/');
    Mapped::At(match base.is_empty() {
        true => relative.to_string(),
        false => format!("{base}/{relative}"),
    })
}

/// The source keys of one coordinated key range: listed from `lower`, ending before `stop`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Segment {
    lower: Option<String>,
    stop: Option<String>,
    /// The range's exclusive upper target key, where the next range starts.
    upper: Option<String>,
}

/// What a flat walk lists next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Bounds {
    /// The whole prefix: the destination is an ordinary bucket.
    Whole,
    Segment(Segment),
    /// No coordinated range of the walk's prefix is left.
    Done,
}

/// The first coordinated range at or after `from` that holds target keys of the walk.
fn next_segment(
    walk: &StagingWalk,
    ranges: impl IntoIterator<Item = (String, Option<String>)>,
) -> Bounds {
    let from = walk.from.clone().unwrap_or_default();
    for (lower, upper) in ranges {
        if upper.as_deref().is_some_and(|upper| upper <= from.as_str()) {
            continue;
        }
        let stop = match upper.as_deref().map(|upper| source_key(walk, upper)) {
            Some(Mapped::Before) => continue,
            Some(Mapped::At(stop)) => Some(stop),
            Some(Mapped::After) | None => None,
        };
        let lower = match source_key(walk, lower.max(from.clone()).as_str()) {
            Mapped::Before => None,
            Mapped::At(lower) => Some(lower),
            Mapped::After => return Bounds::Done,
        };
        return Bounds::Segment(Segment { lower, stop, upper });
    }
    Bounds::Done
}

/// Key ranges cut at a lane's exclusive upper target key; ranges left empty are dropped.
fn clamped(
    ranges: Vec<(String, Option<String>)>,
    upper: Option<&str>,
) -> Vec<(String, Option<String>)> {
    let Some(upper) = upper else {
        return ranges;
    };
    (ranges.into_iter())
        .filter(|(lower, _)| lower.as_str() < upper)
        .map(|(lower, end)| {
            let end = match end {
                Some(end) if end.as_str() < upper => end,
                _ => upper.to_string(),
            };
            (lower, Some(end))
        })
        .collect()
}

/// The target key a source key below the walk's path maps to.
pub(super) fn target_of(walk: &StagingWalk, source_key: &str) -> Option<String> {
    let relative = relative_entry_path(&walk.source_path, source_key, true).ok()?;
    Some(join_target_prefix(&walk.target_prefix, &relative))
}

/// Adds shipped digests to an observation; they bind every complete read.
pub(super) fn with_manifest(
    mut observation: SourceMetadata,
    digests: Vec<ManifestDigest>,
) -> SourceMetadata {
    for (algorithm, digest) in digests {
        let origin = DigestOrigin::Manifest;
        let scope = DigestScope::FullObject;
        let digest = SourceDigest {
            digest,
            scope,
            origin,
        };
        observation.digests.insert(algorithm, digest);
    }
    observation
}

/// READ on the source file and WRITE on the destination key, decided as a single check would.
pub(super) fn file_access(
    rules: &PermissionRules,
    spec: &StagingJobSpec,
    source_path: &str,
    target_key: &str,
) -> Result<(), ItemFailure> {
    let source_path = source_permission_path(spec, source_path);
    let target_path = object_permission_path(
        spec.auth_context.realm_id,
        spec.group_id,
        spec.node_id,
        &spec.bucket,
        target_key,
    );
    let allowed = rules.allows(&source_path, &Permission::READ)
        && rules.allows(&target_path, &Permission::WRITE);
    match allowed {
        true => Ok(()),
        false => Err(ItemFailure::Denied("permission denied".to_string())),
    }
}

enum MappedEntry {
    File(StagedFile),
    Directory(StagingJobDirectory),
}

/// Maps a listed entry below the walk's path to its destination. A flat listing returns
/// files at any depth; a directory listing returns direct children only.
fn map_source_entry(
    walk: &StagingWalk,
    entry: SourceEntry,
    (flat, exact): (bool, bool),
) -> Result<MappedEntry, String> {
    let entry_path = canonical_walker_path(&entry.path)?;
    let relative = relative_entry_path(&walk.source_path, &entry_path, flat)?;
    let target = join_target_prefix(&walk.target_prefix, &relative);
    Ok(match entry.kind {
        SourceEntryKind::File => MappedEntry::File(StagedFile {
            observation: entry.observation().filter(|_| exact),
            manifest: Vec::new(),
            metadata: BTreeMap::new(),
            modified: None,
            source_path: entry_path,
            target_key: target,
            size: entry.size,
            mirrors: Vec::new(),
        }),
        SourceEntryKind::Directory => MappedEntry::Directory(StagingJobDirectory {
            source_path: directory_list_path(&entry_path),
            target_prefix: target,
        }),
    })
}

pub(super) fn directory_list_path(path: &str) -> String {
    if path.is_empty() || path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

fn relative_entry_path(base: &str, entry: &str, nested: bool) -> Result<String, String> {
    let base = base.trim_matches('/');
    let relative = if base.is_empty() {
        entry
    } else {
        entry
            .strip_prefix(base)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| "source listing returned an entry outside its directory".to_string())?
    };
    if relative.is_empty() || (!nested && relative.contains('/')) {
        return Err("source listing returned a non-child entry".to_string());
    }
    Ok(relative.to_string())
}

pub(super) fn canonical_walker_path(path: &str) -> Result<String, String> {
    let mut path = path.trim();
    while let Some(stripped) = path.strip_prefix("./") {
        path = stripped;
    }
    let path = path.trim_end_matches('/');
    if path.is_empty() {
        return Err("source listing returned an empty entry path".to_string());
    }
    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err("source listing returned an invalid entry path".to_string());
    }
    Ok(path.to_string())
}

fn join_target_prefix(prefix: &str, relative: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    }
}

pub(super) fn source_permission_path(spec: &StagingJobSpec, source_path: &str) -> String {
    connector_permission_path(spec, spec.connector_id, source_path)
}

/// The permission path of `source_path` below another connector of the job's group.
pub(super) fn connector_permission_path(
    spec: &StagingJobSpec,
    connector_id: Ulid,
    source_path: &str,
) -> String {
    let root = format!(
        "/{}/g/{}/data/{}/_sources/{}",
        spec.auth_context.realm_id, spec.group_id, spec.node_id, connector_id
    );
    if source_path.is_empty() {
        root
    } else {
        format!("{root}/{source_path}")
    }
}

#[cfg(test)]
#[path = "walk_tests.rs"]
mod state_machine_tests;

#[cfg(test)]
mod pure_tests {
    use super::*;
    use std::time::Duration;

    fn bounded(from: Option<&str>) -> StagingWalk {
        StagingWalk {
            source_path: "src/".to_string(),
            target_prefix: "copy".to_string(),
            cursor: None,
            queue_row: None,
            from: from.map(str::to_string),
        }
    }

    // Transient listing failures retry the page; only a refusing source skips its path.
    #[test]
    fn transient_listing_retries() {
        use aruna_core::errors::SourceResolutionError;
        let retried =
            |error: ListStagingError| matches!(list_failure(error), ItemFailure::System(_));
        let storage =
            SourceResolutionError::StorageError(aruna_core::errors::StorageError::CommitFailed);
        assert!(retried(ListStagingError::Resolve(storage)));
        assert!(retried(ListStagingError::Resolve(
            SourceResolutionError::ResolveFailed
        )));
        assert!(retried(
            StagingSourceError::ListError("reset".into()).into()
        ));
        assert!(!retried(ListStagingError::Resolve(
            SourceResolutionError::NotFound
        )));
        assert!(!retried(StagingSourceError::AccessDenied.into()));
    }

    #[test]
    fn keys_map_sources() {
        let walk = bounded(None);
        assert_eq!(source_key(&walk, "a"), Mapped::Before);
        assert_eq!(source_key(&walk, "copy/"), Mapped::Before);
        assert_eq!(
            source_key(&walk, "copy/x/1"),
            Mapped::At("src/x/1".to_string())
        );
        assert_eq!(source_key(&walk, "d"), Mapped::After);
    }

    #[test]
    fn segments_follow_ranges() {
        let ranges = || {
            vec![
                (String::new(), Some("copy/g".to_string())),
                ("copy/p".to_string(), Some("e".to_string())),
            ]
        };
        let first = Segment {
            lower: None,
            stop: Some("src/g".to_string()),
            upper: Some("copy/g".to_string()),
        };
        assert_eq!(
            next_segment(&bounded(None), ranges()),
            Bounds::Segment(first)
        );
        // The next walk starts at the end of the first range and skips to the second.
        let second = Segment {
            lower: Some("src/p".to_string()),
            stop: None,
            upper: Some("e".to_string()),
        };
        let after = next_segment(&bounded(Some("copy/g")), ranges());
        assert_eq!(after, Bounds::Segment(second));
        assert_eq!(next_segment(&bounded(Some("e")), ranges()), Bounds::Done);
        // A range wholly before the prefix is skipped.
        let early = vec![(String::new(), Some("b".to_string()))];
        assert_eq!(next_segment(&bounded(None), early), Bounds::Done);
    }

    fn walk(source_path: &str) -> StagingWalk {
        StagingWalk {
            source_path: source_path.to_string(),
            target_prefix: "copy".to_string(),
            cursor: None,
            queue_row: None,
            from: None,
        }
    }

    fn file(path: &str) -> SourceEntry {
        SourceEntry {
            name: path.rsplit('/').next().unwrap().to_string(),
            path: path.to_string(),
            kind: SourceEntryKind::File,
            size: Some(4),
            modified: None,
            etag: Some("\"9e107d9d372bb6826bd81d3542a419d6\"".to_string()),
            stat: None,
        }
    }

    #[test]
    fn directory_keeps_marker() {
        let entry = SourceEntry {
            name: "refseq".to_string(),
            kind: SourceEntryKind::Directory,
            size: None,
            etag: None,
            ..file("refseq/")
        };

        let MappedEntry::Directory(directory) =
            map_source_entry(&walk(""), entry, (false, true)).unwrap()
        else {
            panic!("expected directory")
        };
        assert_eq!(directory.source_path, "refseq/");
        assert_eq!(directory.target_prefix, "copy/refseq");
    }

    // An NCBI index page gives a rounded size and a local minute: never an observation.
    #[test]
    fn index_entries_headed() {
        let entry = SourceEntry {
            size: Some(928 << 20),
            modified: Some(std::time::UNIX_EPOCH + Duration::from_secs(1_763_401_080)),
            etag: None,
            ..file("all/GCF/fcs_report.txt")
        };
        let mapped = map_source_entry(&walk("all/GCF/"), entry.clone(), (false, false));
        let Ok(MappedEntry::File(mapped)) = mapped else {
            panic!("expected file")
        };
        assert_eq!(mapped.observation, None);
        assert_eq!(mapped.size, Some(928 << 20));
        let exact = map_source_entry(&walk("all/GCF/"), entry, (false, true));
        assert!(matches!(exact, Ok(MappedEntry::File(file)) if file.observation.is_some()));
    }

    #[test]
    fn flat_maps_nested() {
        let MappedEntry::File(mapped) =
            map_source_entry(&walk("data/"), file("data/a/b.txt"), (true, true)).unwrap()
        else {
            panic!("expected file")
        };
        assert_eq!(mapped.target_key, "copy/a/b.txt");
        let observation = mapped.observation.unwrap();
        assert_eq!(observation.content_length, 4);
        assert_eq!(observation.content_type, None);
        assert_eq!(observation.digests.len(), 1);

        assert!(map_source_entry(&walk("data/"), file("data/a/b.txt"), (false, true)).is_err());
        assert!(map_source_entry(&walk("data/"), file("other/b.txt"), (true, true)).is_err());
    }
}
