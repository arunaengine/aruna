//! Merges a distributed bucket's listings over its key ranges into one bounded page in key
//! order, each range from this member's records or from a member that keeps it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Debug;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::member_read::MemberMethod;
use aruna_core::operation::Operation;
use aruna_core::outbound::{xml_elements, xml_field};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BlobHeadKey;
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::upload::UploadTag;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionSnapshot, DistributionView, KeyRange,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::distribution::guard::GuardError;
use crate::distribution::routing::{bound_view, local_fence, upload_tags};
use crate::distribution::uploads::{
    InventoryUploadsError, InventoryUploadsInput, InventoryUploadsOperation,
};
use crate::distribution::versions::{
    InventoryVersionsError, InventoryVersionsInput, InventoryVersionsOperation,
};
use crate::s3::multipart::uploads::ListUploadsResult;
use crate::s3::object::link::{LinkCaller, LinkError, LinkNode};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListContinuationToken, ListedObject,
    decode_list_token, encode_list_token,
};
use crate::s3::object::member_read::{
    MemberCallOperation, MemberPayload, MemberRefusal, MemberRequest, MemberTarget,
    MemberTextOperation,
};
use crate::s3::object::range_list::DelegatedCaller;
use crate::s3::object::versions::{ListVersionsItem, ListVersionsResult};
use crate::s3::object::visible::{
    HiddenCursor, LinkShows, ListingKeys, NextPage, RecordSource, VisibleError, VisiblePage,
    VisiblePageOperation, is_sealed, open_cursor,
};

/// Ranges one merged page visits at most, so its fan-out stays bounded.
const MAX_PAGE_RANGES: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
/// The query parameters a forwarded listing names its key range with.
const LOWER_PARAM: &str = "aruna-lower";
const UPPER_PARAM: &str = "aruna-upper";
/// Asks a copy to list its records while the range's coordinator cannot.
const COPY_PARAM: &str = "aruna-copy";

type Ids = Option<(NodeId, RealmId)>;

/// Why a merged page could not be built.
#[derive(Debug, Error, PartialEq)]
pub enum MergeError {
    #[error(transparent)]
    Route(#[from] GuardError),
    /// A member refused or failed, and no other may answer.
    #[error("a member refused the listing")]
    Member(MemberRefusal),
    /// No holder of the range answered, so the listing cannot be complete.
    #[error("no holder of range {range_id} of distributed bucket {set_id} answered")]
    Incomplete { set_id: Ulid, range_id: Ulid },
    #[error("invalid continuation token")]
    Token,
    #[error("invalid version-id-marker")]
    VersionMarker,
    /// This node's own listing of a range failed.
    #[error("the object listing failed")]
    Listing(VisibleError<LinkError, ListBucketError>),
    #[error("{0}")]
    Failed(String),
}

impl MergeError {
    /// Whether another holder may answer instead.
    fn unreachable(&self) -> bool {
        match self {
            Self::Member(refusal) => refusal.unreachable(),
            Self::Incomplete { .. } => true,
            _ => false,
        }
    }
}

impl From<IoError> for MergeError {
    fn from(error: IoError) -> Self {
        Self::Failed(error.to_string())
    }
}

/// The query of a listing a holder answers for one range.
fn range_query(base: &[(&str, String)], range: &KeyRange) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (name, value) in base {
        query.append_pair(name, value);
    }
    query.append_pair(LOWER_PARAM, &range.lower);
    if let Some(upper) = &range.upper {
        query.append_pair(UPPER_PARAM, upper);
    }
    query.finish()
}

fn listing_request(operation: &'static str, bucket: &str, query: String) -> MemberRequest {
    MemberRequest {
        method: MemberMethod::Get,
        operation,
        bucket: bucket.to_string(),
        key: String::new(),
        query: Some(query),
        headers: Vec::new(),
        body: MemberPayload::Empty,
    }
}

/// Who a member listing is delegated for, and in which distributed bucket.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeCaller {
    pub ids: Ids,
    pub caller: DelegatedCaller,
    pub binding: DistributionBinding,
}

/// One range's listing, asked of the members named in turn; one that cannot answer gives way.
#[derive(Debug, PartialEq)]
struct AskMembersOperation {
    from: MergeCaller,
    range_id: Ulid,
    members: VecDeque<NodeId>,
    operation: &'static str,
    bucket: String,
    query: String,
    asked: Option<NodeId>,
}

type AskTurn = Result<Next<(String, NodeId)>, MergeError>;

impl AskMembersOperation {
    /// Asks the member that lists `range`, or with `copies` its ready copies; `only` narrows
    /// the members to one.
    fn new(
        from: &MergeCaller,
        snapshot: &DistributionSnapshot,
        (range, copies, only): (&KeyRange, bool, Option<NodeId>),
        (operation, bucket, query): (&'static str, &str, String),
    ) -> Self {
        let (holders, query) = match copies {
            true => (
                snapshot.copy_holders(range),
                format!("{query}&{COPY_PARAM}=1"),
            ),
            false => (snapshot.list_holder(range).into_iter().collect(), query),
        };
        let members = (holders.into_iter())
            .filter(|member| member.member_id != from.binding.member_id)
            .filter(|member| only.is_none_or(|node| member.node_id == node))
            .map(|member| member.node_id)
            .collect();
        Self {
            from: from.clone(),
            range_id: range.range_id,
            members,
            operation,
            bucket: bucket.to_string(),
            query,
            asked: None,
        }
    }

    fn ask_next(&mut self) -> AskTurn {
        let from = &self.from;
        let Some(node_id) = self.members.pop_front() else {
            return Err(MergeError::Incomplete {
                set_id: from.binding.set_id,
                range_id: self.range_id,
            });
        };
        let target = MemberTarget {
            set_id: from.binding.set_id,
            range_id: self.range_id,
            node_id,
        };
        let request = listing_request(self.operation, &self.bucket, self.query.clone());
        let call = MemberCallOperation::read(from.ids, from.caller.clone(), target, request);
        self.asked = Some(node_id);
        Ok(Next::Call(IoCall::child(MemberTextOperation::of_call(
            call,
        ))))
    }
}

impl Operation for AskMembersOperation {
    type Output = (String, NodeId);
    type Error = MergeError;

    fn first(&mut self) -> AskTurn {
        self.ask_next()
    }

    fn next(&mut self, result: IoResult) -> AskTurn {
        let node_id = self.asked.ok_or(IoError::Unfinished)?;
        match result.child::<String, MemberRefusal>()? {
            Ok(body) => Ok(Next::Done((body, node_id))),
            Err(refusal) if refusal.unreachable() => self.ask_next(),
            Err(refusal) => Err(MergeError::Member(refusal)),
        }
    }
}

/// Where a merged listing continues: after a listed entry, at the first key of a range when a
/// page visited its bound of ranges without an entry, or inside the range starting at `lower`
/// from the sealed cursor of the member that stopped there among hidden entries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MergeResume {
    After(ListContinuationToken),
    From(String),
    Sealed {
        lower: String,
        node: NodeId,
        copy: bool,
        cursor: String,
        group: Option<String>,
    },
}

/// An object another member listed, as its listing names it.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteObject {
    pub key: String,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub size: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MergedObject {
    Local(Box<ListedObject>),
    Remote(RemoteObject),
}

impl MergedObject {
    fn key(&self) -> &str {
        match self {
            Self::Local(object) => &object.head.key,
            Self::Remote(object) => &object.key,
        }
    }
}

/// One merged object page and the ranges a copy listed because their coordinator could not.
#[derive(Debug, Default, PartialEq)]
pub struct MergedObjects {
    pub objects: Vec<MergedObject>,
    pub prefixes: Vec<String>,
    pub token: Option<ListContinuationToken>,
    pub next: Option<MergeResume>,
    pub stale: Vec<Ulid>,
}

/// One range's page before it joins the merged one.
struct ObjectsPart {
    objects: Vec<MergedObject>,
    prefixes: Vec<String>,
    token: Option<ListContinuationToken>,
    sealed: Option<String>,
}

/// One range's objects from a member's listing body.
fn remote_objects(body: &str) -> Result<ObjectsPart, MergeError> {
    let objects = (xml_elements(body, "Contents").into_iter())
        .filter_map(|entry| {
            Some(MergedObject::Remote(RemoteObject {
                key: xml_field(entry, "Key")?,
                last_modified: xml_field(entry, "LastModified"),
                etag: xml_field(entry, "ETag"),
                size: xml_field(entry, "Size").and_then(|size| size.parse().ok()),
            }))
        })
        .collect();
    let prefixes = (xml_elements(body, "CommonPrefixes").into_iter())
        .filter_map(|entry| xml_field(entry, "Prefix"))
        .collect();
    // A member that stopped among hidden entries answers with its own sealed cursor.
    let (token, sealed) = match xml_field(body, "NextContinuationToken") {
        Some(token) if is_sealed(&token) => (None, Some(token)),
        Some(token) => (
            Some(decode_list_token(&token).ok_or(MergeError::Token)?),
            None,
        ),
        None => (None, None),
    };
    Ok(ObjectsPart {
        objects,
        prefixes,
        token,
        sealed,
    })
}

/// The position after a page's last key and common prefix, whichever sorts later.
pub fn merged_marker(
    bucket: &str,
    last_key: Option<&str>,
    last_prefix: Option<&str>,
) -> Result<Option<ListContinuationToken>, ConversionError> {
    let group = last_prefix.filter(|prefix| last_key.is_none_or(|key| *prefix > key));
    let Some(entry) = group.or(last_key) else {
        return Ok(None);
    };
    Ok(Some(ListContinuationToken {
        last_key: BlobHeadKey::object_prefix(bucket, entry)?,
        last_common_prefix: group.map(str::to_string),
    }))
}

/// The range being listed.
#[derive(Debug, PartialEq)]
struct ObjectsRange {
    range: KeyRange,
    remaining: usize,
    resumed: Option<(NodeId, bool, String)>,
    local: bool,
}

#[derive(Debug, PartialEq)]
enum ObjectsStep {
    View,
    Fence,
    Local,
    /// The range's lister; when it cannot answer, this member's copy or another lists.
    Primary(Option<(&'static str, String)>),
    Asked(bool),
}

type ObjectsTurn = Result<Next<MergedObjects>, MergeError>;

/// What decides which local entries a merged object listing shows.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeShows<K> {
    pub keys: K,
    pub caller: LinkCaller,
    pub realm_id: RealmId,
    pub node: LinkNode,
}

/// One merged object page over every range under the prefix, at most `max_keys` entries. With
/// `bounded` the page also stops at its bound of ranges and bytes.
#[derive(Debug, PartialEq)]
pub struct MergedObjectsOperation<K> {
    from: MergeCaller,
    shows: MergeShows<K>,
    input: ListBucketInput,
    resume: Option<MergeResume>,
    bounded: bool,
    view: Option<DistributionView>,
    fence: Option<LocalFence>,
    ranges: VecDeque<KeyRange>,
    resumed: Option<(NodeId, bool, String)>,
    page: MergedObjects,
    last_prefix: Option<String>,
    page_prefix: Option<String>,
    last_key: Option<String>,
    first: bool,
    bytes: usize,
    visited: usize,
    current: Option<ObjectsRange>,
    step: ObjectsStep,
}

impl<K> MergedObjectsOperation<K>
where
    K: ListingKeys + Clone + 'static,
{
    pub fn new(
        from: MergeCaller,
        shows: MergeShows<K>,
        input: ListBucketInput,
        (resume, bounded): (Option<MergeResume>, bool),
    ) -> Self {
        Self {
            from,
            shows,
            input,
            resume,
            bounded,
            view: None,
            fence: None,
            ranges: VecDeque::new(),
            resumed: None,
            page: MergedObjects::default(),
            last_prefix: None,
            page_prefix: None,
            last_key: None,
            first: true,
            bytes: 0,
            visited: 0,
            current: None,
            step: ObjectsStep::View,
        }
    }

    fn snapshot(&self) -> Result<&DistributionSnapshot, IoError> {
        Ok(self.view.as_ref().ok_or(IoError::Unfinished)?.snapshot())
    }

    /// The position after the last entry of this page.
    fn marker(&self) -> Result<Option<ListContinuationToken>, MergeError> {
        let (key, prefix) = (self.last_key.as_deref(), self.page_prefix.as_deref());
        merged_marker(&self.input.bucket, key, prefix)
            .map_err(|_| MergeError::Failed("invalid listing marker".to_string()))
    }

    fn plan(&mut self) -> ObjectsTurn {
        let input = &self.input;
        let resume = match &input.continuation_token {
            Some(token) => Some((
                (BlobHeadKey::from_bytes(&token.last_key).map_err(|_| MergeError::Token)?).key,
                token.last_common_prefix.clone(),
            )),
            None => (input.start_after.clone())
                .filter(|start| !start.is_empty())
                .map(|start| (start, None)),
        };
        self.last_prefix = resume.as_ref().and_then(|(_, group)| group.clone());
        // A sealed position resumes its range at the member that sealed it.
        let from = match self.resume.take() {
            Some(MergeResume::From(lower)) => Some(lower),
            Some(MergeResume::Sealed {
                lower,
                node,
                copy,
                cursor,
                group,
            }) => {
                self.last_prefix = group;
                self.resumed = Some((node, copy, cursor));
                Some(lower)
            }
            _ => None,
        };
        let prefix = self.input.prefix.clone().unwrap_or_default();
        let ranges = (self.snapshot()?.listed_ranges(&prefix).into_iter())
            .filter(|range| {
                let after = resume.as_ref().is_none_or(|(key, _)| {
                    (range.upper.as_deref()).is_none_or(|upper| key.as_str() < upper)
                });
                after
                    && from
                        .as_deref()
                        .is_none_or(|from| range.lower.as_str() >= from)
            })
            .cloned()
            .collect();
        self.ranges = ranges;
        self.next_range()
    }

    fn finish(&mut self, next: Option<MergeResume>) -> ObjectsTurn {
        self.page.next = next;
        Ok(Next::Done(std::mem::take(&mut self.page)))
    }

    fn next_range(&mut self) -> ObjectsTurn {
        let Some(range) = self.ranges.pop_front() else {
            return self.finish(None);
        };
        let visited = self.visited;
        self.visited += 1;
        let max = (self.input.max_keys).unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS);
        let remaining = max.saturating_sub(self.page.objects.len() + self.page.prefixes.len());
        let full = self.bounded && (visited == MAX_PAGE_RANGES || self.bytes >= MAX_PAGE_BYTES);
        if remaining == 0 || full {
            self.page.token = self.marker()?;
            // Nothing listed yet: the next page starts at this range.
            let next = (self.page.token.is_none()).then(|| MergeResume::From(range.lower.clone()));
            return self.finish(next);
        }
        let resumed = self.resumed.take();
        // A copy may be behind its coordinator, so a range resumed at one is marked stale.
        if resumed.as_ref().is_some_and(|(_, copy, _)| *copy) {
            self.page.stale.push(range.range_id);
        }
        let local = match &resumed {
            Some((node, ..)) => *node == self.shows.node.local,
            None => {
                let fence = self.fence.as_ref().ok_or(IoError::Unfinished)?;
                let member = self.from.binding.member_id;
                self.snapshot()?.lists_locally(member, &range, fence)
            }
        };
        let asked = resumed.clone();
        self.current = Some(ObjectsRange {
            range,
            remaining,
            resumed,
            local,
        });
        match (local, asked) {
            (true, _) => self.list_here(),
            (false, Some((node, copy, cursor))) => {
                let start = Some(("continuation-token", cursor));
                self.ask(start, (copy, Some(node)), ObjectsStep::Asked(copy))
            }
            (false, None) => {
                let start = self.remote_start()?;
                self.ask(start.clone(), (false, None), ObjectsStep::Primary(start))
            }
        }
    }

    /// Where a holder resumes the first listed range: at the full position of the request, so
    /// a common prefix is skipped as a whole and not listed again after its name.
    fn remote_start(&self) -> Result<Option<(&'static str, String)>, MergeError> {
        if !self.first {
            return Ok(None);
        }
        Ok(match &self.input.continuation_token {
            Some(token) => {
                let token = encode_list_token(token)
                    .map_err(|error| MergeError::Failed(error.to_string()))?;
                Some(("continuation-token", token))
            }
            None => (self.input.start_after.clone())
                .filter(|start| !start.is_empty())
                .map(|start| ("start-after", start)),
        })
    }

    fn ask(
        &mut self,
        start: Option<(&'static str, String)>,
        (copies, only): (bool, Option<NodeId>),
        step: ObjectsStep,
    ) -> ObjectsTurn {
        let input = &self.input;
        let current = self.current.as_ref().ok_or(IoError::Unfinished)?;
        let mut base = vec![
            ("list-type", "2".to_string()),
            ("max-keys", current.remaining.to_string()),
        ];
        base.extend(input.prefix.clone().map(|prefix| ("prefix", prefix)));
        base.extend(input.delimiter.clone().map(|group| ("delimiter", group)));
        base.extend(start);
        let query = range_query(&base, &current.range);
        let request = ("ListObjectsV2", input.bucket.as_str(), query);
        let asked = (&current.range, copies, only);
        let ask = AskMembersOperation::new(&self.from, self.snapshot()?, asked, request);
        self.step = step;
        Ok(Next::Call(IoCall::child(ask)))
    }

    fn list_here(&mut self) -> ObjectsTurn {
        let current = self.current.as_ref().ok_or(IoError::Unfinished)?;
        let mut sub = self.input.clone();
        sub.max_keys = Some(current.remaining);
        if !self.first || current.resumed.is_some() {
            (sub.continuation_token, sub.start_after) = (None, None);
        }
        let hidden = match &current.resumed {
            Some((_, _, cursor)) => {
                Some(open_cursor::<HiddenCursor>(cursor).ok_or(MergeError::Token)?)
            }
            None => None,
        };
        let range = &current.range;
        let source = RecordSource {
            bounds: Some((range.lower.clone(), range.upper.clone())),
        };
        let shows = &self.shows;
        let rule = LinkShows {
            keys: shows.keys.clone(),
            caller: shows.caller.clone(),
            realm_id: shows.realm_id,
            node: shows.node,
        };
        let list = VisiblePageOperation::new(source, rule, sub, hidden);
        self.step = ObjectsStep::Local;
        Ok(Next::Call(IoCall::child(list)))
    }

    fn local_listed(&mut self, page: VisiblePage<ListedObject>) -> ObjectsTurn {
        let (token, sealed) = match page.next {
            Some(NextPage::After(token)) => (Some(token), None),
            Some(NextPage::Sealed(sealed)) => (None, Some(sealed)),
            None => (None, None),
        };
        let objects = (page.objects.into_iter())
            .map(|object| MergedObject::Local(Box::new(object)))
            .collect();
        let part = ObjectsPart {
            objects,
            prefixes: page.prefixes,
            token,
            sealed,
        };
        let current = self.current.as_ref().ok_or(IoError::Unfinished)?;
        // Listing here instead of the coordinator lists this member's copy.
        let copy = (current.resumed.as_ref()).map_or(!current.local, |(_, copy, _)| *copy);
        self.apply(part, self.shows.node.local, copy)
    }

    /// Narrows a holder's page to what this node's own rules of the caller allow.
    fn remote_listed(&mut self, body: &str, lister: NodeId, copy: bool) -> ObjectsTurn {
        let mut part = remote_objects(body)?;
        let keys = &self.shows.keys;
        (part.objects).retain(|object| keys.allows(object.key()));
        (part.prefixes).retain(|prefix| keys.groups(prefix));
        self.apply(part, lister, copy)
    }

    fn apply(&mut self, part: ObjectsPart, lister: NodeId, copy: bool) -> ObjectsTurn {
        let current = self.current.take().ok_or(IoError::Unfinished)?;
        self.first = false;
        let keys = (part.objects.iter().map(MergedObject::key))
            .chain(part.prefixes.iter().map(String::as_str));
        self.bytes += keys.map(str::len).sum::<usize>();
        for prefix in part.prefixes {
            if self.last_prefix.as_deref() == Some(prefix.as_str()) {
                continue;
            }
            self.last_prefix = Some(prefix.clone());
            self.page_prefix = Some(prefix.clone());
            self.page.prefixes.push(prefix);
        }
        if let Some(object) = part.objects.last() {
            self.last_key = Some(object.key().to_string());
        }
        self.page.objects.extend(part.objects);
        // A range that stopped among hidden entries goes on from its member's cursor.
        if let Some(cursor) = part.sealed {
            return self.finish(Some(MergeResume::Sealed {
                lower: current.range.lower,
                node: lister,
                copy,
                cursor,
                group: self.last_prefix.clone(),
            }));
        }
        if part.token.is_some() {
            self.page.token = part.token;
            return self.finish(None);
        }
        self.next_range()
    }

    /// The range's lister cannot answer, so this member's copy or another lists.
    fn lister_failed(&mut self, start: Option<(&'static str, String)>) -> ObjectsTurn {
        let current = self.current.as_ref().ok_or(IoError::Unfinished)?;
        self.page.stale.push(current.range.range_id);
        let member = self.from.binding.member_id;
        let here = (self.snapshot()?.record_holders(&current.range).iter())
            .any(|holder| holder.member_id == member);
        match here {
            true => self.list_here(),
            false => self.ask(start, (true, None), ObjectsStep::Asked(true)),
        }
    }
}

impl<K> Operation for MergedObjectsOperation<K>
where
    K: ListingKeys + Clone + 'static,
{
    type Output = MergedObjects;
    type Error = MergeError;

    fn first(&mut self) -> ObjectsTurn {
        Ok(Next::Call(IoCall::child(bound_view(&self.from.binding))))
    }

    fn next(&mut self, result: IoResult) -> ObjectsTurn {
        match std::mem::replace(&mut self.step, ObjectsStep::Local) {
            ObjectsStep::View => {
                self.view = Some(result.child::<DistributionView, GuardError>()??);
                self.step = ObjectsStep::Fence;
                let fence = local_fence(self.from.binding.set_id);
                Ok(Next::Call(IoCall::child(fence)))
            }
            ObjectsStep::Fence => {
                self.fence = Some(result.child::<LocalFence, GuardError>()??);
                self.plan()
            }
            ObjectsStep::Local => {
                let page = result
                    .child::<VisiblePage<ListedObject>, VisibleError<LinkError, ListBucketError>>(
                    )?;
                self.local_listed(page.map_err(MergeError::Listing)?)
            }
            ObjectsStep::Primary(start) => match result.child::<(String, NodeId), MergeError>()? {
                Ok((body, lister)) => self.remote_listed(&body, lister, false),
                Err(error) if error.unreachable() => self.lister_failed(start),
                Err(error) => Err(error),
            },
            ObjectsStep::Asked(copy) => {
                let (body, lister) = result.child::<(String, NodeId), MergeError>()??;
                self.remote_listed(&body, lister, copy)
            }
        }
    }
}

/// One range's entries of a marker listing, with the keys the merge orders them by.
#[derive(Debug, PartialEq)]
pub struct MarkerPart<P> {
    pub part: P,
    keys: Vec<String>,
    prefixes: Vec<String>,
    next: Option<(String, Option<String>)>,
}

/// A merged marker listing: its parts in key order, its common prefixes and next markers.
#[derive(Debug, PartialEq)]
pub struct MergedMarkers<P> {
    pub parts: Vec<P>,
    pub prefixes: Vec<String>,
    pub next: Option<(String, Option<String>)>,
}

/// What one kind of marker listing does for one range.
pub trait MarkerSource: Send + Debug + PartialEq {
    type Part: Send + Debug + PartialEq + 'static;
    fn prefix(&self) -> Option<&str>;
    fn key_marker(&self) -> Option<&str>;
    fn id_marker(&self) -> Option<&str>;
    fn binding(&self) -> &DistributionBinding;
    fn needs_fence(&self) -> bool;
    fn is_local(
        &self,
        snapshot: &DistributionSnapshot,
        fence: Option<&LocalFence>,
        range: &KeyRange,
    ) -> bool;
    /// The call that lists `range` from `markers`, at most `remaining` entries.
    fn call(
        &self,
        scope: (&DistributionSnapshot, &KeyRange, bool),
        markers: (Option<String>, Option<String>),
        remaining: usize,
    ) -> Result<IoCall, MergeError>;
    fn take(&self, local: bool, result: IoResult) -> Result<MarkerPart<Self::Part>, MergeError>;
}

#[derive(Debug, PartialEq)]
enum MarkerStep {
    View,
    Fence,
    Part(bool),
}

type MarkerTurn<P> = Result<Next<MergedMarkers<P>>, MergeError>;

/// One merged marker listing page over every range under the prefix.
#[derive(Debug, PartialEq)]
pub struct MergedMarkersOperation<S: MarkerSource> {
    source: S,
    max: usize,
    view: Option<DistributionView>,
    fence: Option<LocalFence>,
    ranges: VecDeque<KeyRange>,
    parts: Vec<S::Part>,
    prefixes: Vec<String>,
    entries: usize,
    next_markers: Option<(String, Option<String>)>,
    last: Option<String>,
    first: bool,
    bytes: usize,
    visited: usize,
    step: MarkerStep,
}

impl<S: MarkerSource> MergedMarkersOperation<S> {
    pub fn new(source: S, max: usize) -> Self {
        Self {
            source,
            max,
            view: None,
            fence: None,
            ranges: VecDeque::new(),
            parts: Vec::new(),
            prefixes: Vec::new(),
            entries: 0,
            next_markers: None,
            last: None,
            first: true,
            bytes: 0,
            visited: 0,
            step: MarkerStep::View,
        }
    }

    fn snapshot(&self) -> Result<&DistributionSnapshot, IoError> {
        Ok(self.view.as_ref().ok_or(IoError::Unfinished)?.snapshot())
    }

    fn plan(&mut self) -> MarkerTurn<S::Part> {
        let prefix = self.source.prefix().unwrap_or_default().to_string();
        let marker = self.source.key_marker().filter(|marker| !marker.is_empty());
        let ranges = (self.snapshot()?.listed_ranges(&prefix).into_iter())
            .filter(|range| {
                marker.is_none_or(|key| range.upper.as_deref().is_none_or(|upper| key < upper))
            })
            .cloned()
            .collect();
        self.ranges = ranges;
        self.next_range()
    }

    fn finish(&mut self) -> MarkerTurn<S::Part> {
        Ok(Next::Done(MergedMarkers {
            parts: std::mem::take(&mut self.parts),
            prefixes: std::mem::take(&mut self.prefixes),
            next: self.next_markers.take(),
        }))
    }

    fn next_range(&mut self) -> MarkerTurn<S::Part> {
        let Some(range) = self.ranges.pop_front() else {
            return self.finish();
        };
        let visited = self.visited;
        self.visited += 1;
        let remaining = self.max.saturating_sub(self.entries + self.prefixes.len());
        if remaining == 0 || visited == MAX_PAGE_RANGES || self.bytes >= MAX_PAGE_BYTES {
            // Nothing listed yet: the nil id starts the next page at this range.
            let start = || (range.lower.clone(), Some(Ulid::nil().to_string()));
            self.next_markers = Some(self.last.clone().map_or_else(start, |key| (key, None)));
            return self.finish();
        }
        // The key marker skips a common prefix in every range; the id only in the first.
        let source = &self.source;
        let key = source.key_marker().filter(|key| !key.is_empty());
        let id = source.id_marker().filter(|_| self.first);
        let markers = (key.map(str::to_string), id.map(str::to_string));
        self.first = false;
        let snapshot = self.snapshot()?;
        let local = source.is_local(snapshot, self.fence.as_ref(), &range);
        let call = source.call((snapshot, &range, local), markers, remaining)?;
        self.step = MarkerStep::Part(local);
        Ok(Next::Call(call))
    }

    fn part_listed(&mut self, listed: MarkerPart<S::Part>) -> MarkerTurn<S::Part> {
        let MarkerPart {
            part,
            keys,
            prefixes,
            next,
        } = listed;
        let names = keys.iter().chain(prefixes.iter());
        self.bytes += names.map(String::len).sum::<usize>();
        for prefix in prefixes {
            if self.prefixes.last() != Some(&prefix) {
                self.prefixes.push(prefix);
            }
        }
        let greatest = keys.iter().chain(self.prefixes.last()).max().cloned();
        self.last = greatest.or(self.last.take());
        self.entries += keys.len();
        self.parts.push(part);
        if next.is_some() {
            self.next_markers = next;
            return self.finish();
        }
        self.next_range()
    }
}

impl<S: MarkerSource> Operation for MergedMarkersOperation<S> {
    type Output = MergedMarkers<S::Part>;
    type Error = MergeError;

    fn first(&mut self) -> MarkerTurn<S::Part> {
        Ok(Next::Call(IoCall::child(bound_view(self.source.binding()))))
    }

    fn next(&mut self, result: IoResult) -> MarkerTurn<S::Part> {
        match self.step {
            MarkerStep::View => {
                self.view = Some(result.child::<DistributionView, GuardError>()??);
                if !self.source.needs_fence() {
                    return self.plan();
                }
                self.step = MarkerStep::Fence;
                let fence = local_fence(self.source.binding().set_id);
                Ok(Next::Call(IoCall::child(fence)))
            }
            MarkerStep::Fence => {
                self.fence = Some(result.child::<LocalFence, GuardError>()??);
                self.plan()
            }
            MarkerStep::Part(local) => {
                let listed = self.source.take(local, result)?;
                self.part_listed(listed)
            }
        }
    }
}

/// What a marker listing filters by.
#[derive(Clone, Debug, PartialEq)]
pub struct MarkerQuery {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub key_marker: Option<String>,
    pub id_marker: Option<String>,
}

impl MarkerQuery {
    /// The query parameters a member's listing of this query takes.
    fn base(
        &self,
        (kind, limit_name, id_name): (&'static str, &'static str, &'static str),
        (key, id): (Option<String>, Option<String>),
        remaining: usize,
    ) -> Vec<(&'static str, String)> {
        let mut base = vec![(kind, String::new()), (limit_name, remaining.to_string())];
        base.extend(self.prefix.clone().map(|prefix| ("prefix", prefix)));
        base.extend(self.delimiter.clone().map(|group| ("delimiter", group)));
        base.extend(key.map(|key| ("key-marker", key)));
        base.extend(id.map(|id| (id_name, id)));
        base
    }
}

/// The common prefixes and next markers of a member's marker listing.
fn listed_rest(body: &str, id_tag: &str) -> (Vec<String>, Option<(String, Option<String>)>) {
    let prefixes = (xml_elements(body, "CommonPrefixes").into_iter())
        .filter_map(|entry| xml_field(entry, "Prefix"))
        .collect();
    let next = (xml_field(body, "IsTruncated").as_deref() == Some("true")).then(|| {
        let key = xml_field(body, "NextKeyMarker").unwrap_or_default();
        (key, xml_field(body, id_tag))
    });
    (prefixes, next)
}

/// A version entry another member listed.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteVersion {
    pub key: String,
    pub version_id: Option<String>,
    pub is_latest: Option<bool>,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub size: Option<i64>,
}

#[derive(Debug, PartialEq)]
pub enum VersionsPart {
    Local(Box<ListVersionsResult>),
    Remote {
        versions: Vec<RemoteVersion>,
        markers: Vec<RemoteVersion>,
    },
}

fn remote_version(entry: &str) -> Option<RemoteVersion> {
    Some(RemoteVersion {
        key: xml_field(entry, "Key")?,
        version_id: xml_field(entry, "VersionId"),
        is_latest: xml_field(entry, "IsLatest").map(|value| value == "true"),
        last_modified: xml_field(entry, "LastModified"),
        etag: xml_field(entry, "ETag"),
        size: xml_field(entry, "Size").and_then(|size| size.parse().ok()),
    })
}

/// The ranges of a version listing: this member's ordered inventory, or a holder.
#[derive(Debug, PartialEq)]
pub struct VersionSource {
    pub from: MergeCaller,
    pub query: MarkerQuery,
}

impl MarkerSource for VersionSource {
    type Part = VersionsPart;

    fn prefix(&self) -> Option<&str> {
        self.query.prefix.as_deref()
    }

    fn key_marker(&self) -> Option<&str> {
        self.query.key_marker.as_deref()
    }

    fn id_marker(&self) -> Option<&str> {
        self.query.id_marker.as_deref()
    }

    fn binding(&self) -> &DistributionBinding {
        &self.from.binding
    }

    fn needs_fence(&self) -> bool {
        true
    }

    fn is_local(
        &self,
        snapshot: &DistributionSnapshot,
        fence: Option<&LocalFence>,
        range: &KeyRange,
    ) -> bool {
        let member = self.from.binding.member_id;
        fence.is_some_and(|fence| snapshot.lists_locally(member, range, fence))
    }

    fn call(
        &self,
        (snapshot, range, local): (&DistributionSnapshot, &KeyRange, bool),
        (key, id): (Option<String>, Option<String>),
        remaining: usize,
    ) -> Result<IoCall, MergeError> {
        let query = &self.query;
        if local {
            let id = (id.as_deref().map(Ulid::from_string).transpose())
                .map_err(|_| MergeError::VersionMarker)?;
            let list = InventoryVersionsOperation::new(InventoryVersionsInput {
                set_id: self.from.binding.set_id,
                bucket: query.bucket.clone(),
                prefix: query.prefix.clone(),
                delimiter: query.delimiter.clone(),
                key_marker: key,
                version_id_marker: id,
                max_keys: remaining,
                lower: range.lower.clone(),
                upper: range.upper.clone(),
            });
            return Ok(IoCall::child(list));
        }
        let names = ("versions", "max-keys", "version-id-marker");
        let base = query.base(names, (key, id), remaining);
        let request = (
            "ListObjectVersions",
            query.bucket.as_str(),
            range_query(&base, range),
        );
        let asked = (range, false, None);
        let ask = AskMembersOperation::new(&self.from, snapshot, asked, request);
        Ok(IoCall::child(ask))
    }

    fn take(&self, local: bool, result: IoResult) -> Result<MarkerPart<VersionsPart>, MergeError> {
        if local {
            let listed = result.child::<ListVersionsResult, InventoryVersionsError>()?;
            let listed = listed.map_err(|error| {
                MergeError::Failed(format!("The version listing failed: {error}"))
            })?;
            let keys = (listed.items.iter())
                .map(|item| match item {
                    ListVersionsItem::Version { key, .. }
                    | ListVersionsItem::DeleteMarker { key, .. } => key.clone(),
                })
                .collect();
            let next = listed.is_truncated.then(|| {
                (
                    listed.next_key_marker.clone().unwrap_or_default(),
                    listed.next_version_marker.map(|id| id.to_string()),
                )
            });
            let prefixes = listed.common_prefixes.clone();
            return Ok(MarkerPart {
                part: VersionsPart::Local(Box::new(listed)),
                keys,
                prefixes,
                next,
            });
        }
        let (body, _) = result.child::<(String, NodeId), MergeError>()??;
        let entries = |tag: &str| {
            (xml_elements(&body, tag).into_iter())
                .filter_map(remote_version)
                .collect::<Vec<_>>()
        };
        let (versions, markers) = (entries("Version"), entries("DeleteMarker"));
        let keys = (versions.iter().chain(markers.iter()))
            .map(|entry| entry.key.clone())
            .collect();
        let (prefixes, next) = listed_rest(&body, "NextVersionIdMarker");
        Ok(MarkerPart {
            part: VersionsPart::Remote { versions, markers },
            keys,
            prefixes,
            next,
        })
    }
}

/// An open upload another member listed.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteUpload {
    pub key: String,
    pub upload_id: Option<String>,
    pub initiated: Option<String>,
    pub initiator: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum UploadsPart {
    /// This member's uploads with the ids clients see, in the listing's order.
    Local(Box<ListUploadsResult>, Vec<String>),
    Remote(Vec<RemoteUpload>),
}

#[derive(Debug, PartialEq)]
enum LocalUploadsStep {
    List,
    Tags(Box<ListUploadsResult>),
}

/// One range's uploads from this member's inventory, ids as clients see them.
#[derive(Debug, PartialEq)]
struct LocalUploadsOperation {
    input: Option<InventoryUploadsInput>,
    binding: DistributionBinding,
    step: LocalUploadsStep,
}

type LocalUploads = (Box<ListUploadsResult>, Vec<String>);

impl Operation for LocalUploadsOperation {
    type Output = LocalUploads;
    type Error = MergeError;

    fn first(&mut self) -> Result<Next<LocalUploads>, MergeError> {
        let input = self.input.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(InventoryUploadsOperation::new(
            input,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<LocalUploads>, MergeError> {
        match std::mem::replace(&mut self.step, LocalUploadsStep::List) {
            LocalUploadsStep::List => {
                let listed = result.child::<ListUploadsResult, InventoryUploadsError>()?;
                let listed = listed.map_err(|error| {
                    MergeError::Failed(format!("The upload listing failed: {error}"))
                })?;
                let uploads: Vec<(String, Ulid)> = (listed.uploads.iter())
                    .map(|upload| (upload.key.clone(), upload.upload_id))
                    .collect();
                self.step = LocalUploadsStep::Tags(Box::new(listed));
                let tags = upload_tags(&self.binding, &uploads);
                Ok(Next::Call(IoCall::child(tags)))
            }
            LocalUploadsStep::Tags(listed) => {
                let tags = result.child::<Vec<UploadTag>, GuardError>()??;
                let tags = tags.iter().map(UploadTag::encode).collect();
                Ok(Next::Done((listed, tags)))
            }
        }
    }
}

/// The ranges of an upload listing: uploads live only on the coordinator of their key's range.
#[derive(Debug, PartialEq)]
pub struct UploadSource {
    pub from: MergeCaller,
    pub query: MarkerQuery,
}

impl MarkerSource for UploadSource {
    type Part = UploadsPart;

    fn prefix(&self) -> Option<&str> {
        self.query.prefix.as_deref()
    }

    fn key_marker(&self) -> Option<&str> {
        self.query.key_marker.as_deref()
    }

    fn id_marker(&self) -> Option<&str> {
        self.query.id_marker.as_deref()
    }

    fn binding(&self) -> &DistributionBinding {
        &self.from.binding
    }

    fn needs_fence(&self) -> bool {
        false
    }

    fn is_local(&self, _: &DistributionSnapshot, _: Option<&LocalFence>, range: &KeyRange) -> bool {
        range.coordinator == self.from.binding.member_id
    }

    fn call(
        &self,
        (snapshot, range, local): (&DistributionSnapshot, &KeyRange, bool),
        (key, id): (Option<String>, Option<String>),
        remaining: usize,
    ) -> Result<IoCall, MergeError> {
        let (query, from) = (&self.query, &self.from);
        let set_id = from.binding.set_id;
        if local {
            let id = (id.as_deref().map(Ulid::from_string).transpose())
                .map_err(|_| MergeError::Token)?;
            return Ok(IoCall::child(LocalUploadsOperation {
                input: Some(InventoryUploadsInput {
                    set_id,
                    bucket: query.bucket.clone(),
                    prefix: query.prefix.clone(),
                    delimiter: query.delimiter.clone(),
                    key_marker: key,
                    upload_id_marker: id,
                    max_uploads: remaining,
                    lower: range.lower.clone(),
                    upper: range.upper.clone(),
                }),
                binding: from.binding.clone(),
                step: LocalUploadsStep::List,
            }));
        }
        let incomplete = || MergeError::Incomplete {
            set_id,
            range_id: range.range_id,
        };
        let coordinator = snapshot.member(range.coordinator).ok_or_else(incomplete)?;
        let target = MemberTarget {
            set_id,
            range_id: range.range_id,
            node_id: coordinator.node_id,
        };
        let names = ("uploads", "max-uploads", "upload-id-marker");
        let base = query.base(names, (key, id), remaining);
        let request = listing_request(
            "ListMultipartUploads",
            &query.bucket,
            range_query(&base, range),
        );
        let call = MemberCallOperation::write(from.ids, from.caller.clone(), target, request);
        let text = MemberTextOperation::of_call(call);
        Ok(IoCall::child(RemoteUploadsOperation {
            text: Some(text),
            set_id,
            range_id: range.range_id,
        }))
    }

    fn take(&self, local: bool, result: IoResult) -> Result<MarkerPart<UploadsPart>, MergeError> {
        if local {
            let (listed, tags) = result.child::<LocalUploads, MergeError>()??;
            let keys = (listed.uploads.iter())
                .map(|upload| upload.key.clone())
                .collect();
            let next = listed.is_truncated.then(|| {
                (
                    listed.next_key_marker.clone().unwrap_or_default(),
                    listed.next_upload_marker.map(|id| id.to_string()),
                )
            });
            let prefixes = listed.common_prefixes.clone();
            return Ok(MarkerPart {
                part: UploadsPart::Local(listed, tags),
                keys,
                prefixes,
                next,
            });
        }
        let body = result.child::<String, MergeError>()??;
        let uploads: Vec<RemoteUpload> = (xml_elements(&body, "Upload").into_iter())
            .filter_map(|entry| {
                Some(RemoteUpload {
                    key: xml_field(entry, "Key")?,
                    upload_id: xml_field(entry, "UploadId"),
                    initiated: xml_field(entry, "Initiated"),
                    initiator: xml_field(entry, "ID"),
                })
            })
            .collect();
        let keys = uploads.iter().map(|upload| upload.key.clone()).collect();
        let (prefixes, next) = listed_rest(&body, "NextUploadIdMarker");
        Ok(MarkerPart {
            part: UploadsPart::Remote(uploads),
            keys,
            prefixes,
            next,
        })
    }
}

/// A range's uploads at its coordinator; a silent coordinator leaves the listing incomplete.
#[derive(Debug, PartialEq)]
struct RemoteUploadsOperation {
    text: Option<MemberTextOperation>,
    set_id: Ulid,
    range_id: Ulid,
}

impl Operation for RemoteUploadsOperation {
    type Output = String;
    type Error = MergeError;

    fn first(&mut self) -> Result<Next<String>, MergeError> {
        let text = self.text.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(text)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<String>, MergeError> {
        match result.child::<String, MemberRefusal>()? {
            Ok(body) => Ok(Next::Done(body)),
            // A broken answer of a member that did answer is its own failure.
            Err(MemberRefusal::Broken(reason)) => {
                Err(MergeError::Member(MemberRefusal::Broken(reason)))
            }
            Err(refusal) if refusal.unreachable() => Err(MergeError::Incomplete {
                set_id: self.set_id,
                range_id: self.range_id,
            }),
            Err(refusal) => Err(MergeError::Member(refusal)),
        }
    }
}

#[cfg(test)]
#[path = "member_merge_tests.rs"]
mod tests;
