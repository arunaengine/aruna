//! Lists a bucket's objects a caller may see, page by page; a distributed bucket is merged across
//! its key ranges in key order, each range listed here or by a holder (D12).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::cursor;
use aruna_core::operation::Operation;
use aruna_core::outbound::{RangeEntry, RangePage, RangeRefusal};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::distribution::fence::LocalFence;
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, DistributionView, KeyRange,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::{BucketAccess, BucketAccessOperation};
use crate::distribution::guard::GuardError;
use crate::distribution::routing::{bound_view, local_fence};
use crate::s3::object::link::{LinkCaller, LinkError, LinkNode};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
    ListedObject,
};
use crate::s3::object::range_list::{
    MemberAccess, RangeListError, RangeListOperation, RangeQuery, RangeTarget,
};
use crate::s3::object::visible::{
    HiddenCursor, LinkShows, ListFailure, ListSource, Listed, ListingKeys, NextPage, RecordSource,
    ShowRule, Shown, VisibleError, VisiblePage, VisiblePageOperation, after_entry, is_sealed,
    open_cursor,
};

/// Ranges one merged page visits at most, so its fan-out stays bounded.
const MAX_PAGE_RANGES: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// One listing request as the merge needs it.
#[derive(Clone, Debug, PartialEq)]
pub struct ListRequest {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub cursor: Option<String>,
    pub limit: usize,
    /// A range no holder serves, unanswered or refused, is named in `unavailable_ranges` and
    /// skipped, instead of failing the page.
    pub partial: bool,
}

/// One page of shown objects in key order.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectPage<O> {
    pub objects: Vec<O>,
    pub prefixes: Vec<String>,
    pub next_cursor: Option<String>,
    /// Ranges a copy listed because their coordinator could not answer; it may be behind.
    pub stale_ranges: Vec<String>,
    /// Ranges a partial listing skipped because no holder served them.
    pub unavailable_ranges: Vec<String>,
}

impl<O> Default for ObjectPage<O> {
    fn default() -> Self {
        Self {
            objects: Vec::new(),
            prefixes: Vec::new(),
            next_cursor: None,
            stale_ranges: Vec::new(),
            unavailable_ranges: Vec::new(),
        }
    }
}

/// A listed object as the transport shows it.
pub trait PageObject:
    for<'a> From<&'a ListedObject> + From<RangeEntry> + Send + Debug + PartialEq + 'static
{
    fn key(&self) -> &str;
}

/// This node as a listing needs it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PageNode {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    /// The ids delegations are signed under.
    pub ids: Option<(NodeId, RealmId)>,
}

impl PageNode {
    fn local(&self) -> (NodeId, RealmId) {
        (self.node_id, self.realm_id)
    }
}

/// A caller and the expiry of its bearer, which bounds delegated reads on its behalf.
pub type Caller<'a> = (&'a AuthContext, u64);

/// A caller an operation keeps.
type OwnedCaller = (AuthContext, u64);

#[derive(Debug, Error, PartialEq)]
pub enum PageError {
    #[error("the cursor is not a next_cursor of this listing")]
    Cursor,
    #[error(
        "no holder of range {range_id} of distributed bucket {set_id} answered, so the listing \
         would be incomplete; retry later"
    )]
    Unanswered { range_id: Ulid, set_id: Ulid },
    #[error(transparent)]
    Guard(#[from] GuardError),
    #[error("{0}")]
    Internal(String),
    #[error("a holder refused the listing")]
    Relay(RangeRefusal),
}

impl PageError {
    fn internal(error: impl std::fmt::Display) -> Self {
        Self::Internal(error.to_string())
    }

    /// Whether a failed range listing is the holders' fault, so a copy may list instead.
    fn holder_fault(&self) -> bool {
        match self {
            Self::Unanswered { .. } => true,
            Self::Relay(refusal) => refusal.status >= 500,
            _ => false,
        }
    }
}

impl From<IoError> for PageError {
    fn from(error: IoError) -> Self {
        Self::internal(error)
    }
}

impl From<RangeListError> for PageError {
    fn from(error: RangeListError) -> Self {
        match error {
            RangeListError::Refused(refusal) => Self::Relay(refusal),
            RangeListError::Cursor => Self::Cursor,
            RangeListError::Failed(reason) => Self::Internal(reason),
        }
    }
}

type PageResult<O> = Result<ObjectPage<O>, PageError>;
type PageTurn<O> = Result<Next<ObjectPage<O>>, PageError>;

/// Where a merged page continues: after a listed key or common prefix, at the first key of a
/// range when a page visited its bound of ranges before any entry, or inside the range at
/// `lower`, from the sealed cursor of the member that stopped there among hidden entries.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Resume {
    After {
        key: String,
        group: Option<String>,
    },
    From(String),
    Hidden {
        lower: String,
        node: NodeId,
        copy: bool,
        cursor: String,
        group: Option<String>,
    },
}

pub fn encode_resume(resume: &Resume) -> Result<String, PageError> {
    cursor::encode_value(resume).map_err(PageError::internal)
}

pub fn decode_resume(cursor: &str) -> Result<Resume, PageError> {
    cursor::decode_value(cursor).map_err(|_| PageError::Cursor)
}

/// The token that resumes a local listing after `key`, or past the whole `group`.
pub fn resume_token(
    bucket: &str,
    key: &str,
    group: Option<&str>,
) -> Result<ListContinuationToken, PageError> {
    let last_key =
        BlobHeadKey::object_prefix(bucket, group.unwrap_or(key)).map_err(PageError::internal)?;
    Ok(ListContinuationToken {
        last_key,
        last_common_prefix: group.map(str::to_string),
    })
}

/// The path whose READ lets a caller list `prefix` of a bucket, as for the group's data paths.
pub fn listing_path(
    (realm_id, group_id, node_id): (RealmId, Ulid, NodeId),
    bucket: &str,
    prefix: Option<&str>,
) -> String {
    match prefix.filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => object_permission_path(realm_id, group_id, node_id, bucket, prefix),
        None => bucket_permission_path(realm_id, group_id, node_id, bucket),
    }
}

/// The local listing position a cursor of an unmerged listing names.
fn local_token(
    request: &ListRequest,
) -> Result<(Option<ListContinuationToken>, Option<HiddenCursor>), PageError> {
    position_of(&request.bucket, request.cursor.as_deref())
}

fn position_of(
    bucket: &str,
    cursor: Option<&str>,
) -> Result<(Option<ListContinuationToken>, Option<HiddenCursor>), PageError> {
    if let Some(cursor) = cursor.filter(|cursor| is_sealed(cursor)) {
        let hidden = open_cursor(cursor).ok_or(PageError::Cursor)?;
        return Ok((None, Some(hidden)));
    }
    match cursor.map(decode_resume).transpose()? {
        Some(Resume::After { key, group }) => {
            Ok((Some(resume_token(bucket, &key, group.as_deref())?), None))
        }
        Some(Resume::From(_) | Resume::Hidden { .. }) => Err(PageError::Cursor),
        None => Ok((None, None)),
    }
}

/// The cursor of a local listing that stopped after `token`.
pub fn after_cursor(token: ListContinuationToken) -> Result<String, PageError> {
    let key = BlobHeadKey::from_bytes(&token.last_key)
        .map_err(PageError::internal)?
        .key;
    let group = token.last_common_prefix;
    encode_resume(&Resume::After { key, group })
}

fn list_input(
    request: &ListRequest,
    group_id: Ulid,
    token: Option<ListContinuationToken>,
    start_after: Option<String>,
) -> ListBucketInput {
    ListBucketInput {
        bucket: request.bucket.clone(),
        group_id,
        continuation_token: token,
        max_keys: Some(request.limit),
        prefix: request.prefix.clone().filter(|prefix| !prefix.is_empty()),
        delimiter: (request.delimiter.clone()).filter(|delimiter| !delimiter.is_empty()),
        start_after,
    }
}

fn visible_failure<E: std::fmt::Display, L: std::fmt::Display>(
    error: VisibleError<E, L>,
) -> PageError {
    match error {
        VisibleError::List(error) => PageError::internal(error),
        VisibleError::Shows(error) => PageError::internal(error),
        VisibleError::Cursor => PageError::Internal("listing cursor not sealed".into()),
        VisibleError::Io(error) => PageError::internal(error),
    }
}

fn shown_page<O, T>(page: VisiblePage<T>, map: impl Fn(T) -> O) -> PageResult<O> {
    let next_cursor = match page.next {
        Some(NextPage::After(token)) => Some(after_cursor(token)?),
        Some(NextPage::Sealed(sealed)) => Some(sealed),
        None => None,
    };
    Ok(ObjectPage {
        objects: page.objects.into_iter().map(map).collect(),
        prefixes: page.prefixes,
        next_cursor,
        ..ObjectPage::default()
    })
}

/// The keys a caller's loaded rules allow.
#[derive(Debug, PartialEq)]
struct AccessKeys(Arc<BucketAccess>);

impl ListingKeys for AccessKeys {
    fn allows(&self, key: &str) -> bool {
        self.0.allows(Some(key), &Permission::READ)
    }

    fn groups(&self, prefix: &str) -> bool {
        self.allows(prefix)
    }
}

#[derive(Debug, PartialEq)]
enum LocalStep {
    Access,
    Listed,
    Visible,
}

/// One page of a bucket's objects from this node's records, inside bounds when given. A caller
/// sees only the keys and link targets it may read, and a prefix once an entry below it shows;
/// a page that stopped among hidden entries carries a sealed cursor.
#[derive(Debug, PartialEq)]
struct LocalPartOperation<O> {
    node: PageNode,
    group_id: Ulid,
    caller: Option<OwnedCaller>,
    input: Option<ListBucketInput>,
    hidden: Option<HiddenCursor>,
    bounds: Option<(String, Option<String>)>,
    step: LocalStep,
    object: PhantomData<O>,
}

impl<O: PageObject> LocalPartOperation<O> {
    fn new(
        node: PageNode,
        (group_id, caller): (Ulid, Option<OwnedCaller>),
        input: ListBucketInput,
        (hidden, bounds): (Option<HiddenCursor>, Option<(String, Option<String>)>),
    ) -> Self {
        Self {
            node,
            group_id,
            caller,
            input: Some(input),
            hidden,
            bounds,
            step: LocalStep::Listed,
            object: PhantomData,
        }
    }

    fn input(&mut self) -> Result<ListBucketInput, IoError> {
        self.input.take().ok_or(IoError::Unfinished)
    }

    fn list_all(&mut self) -> PageTurn<O> {
        let list = ListBucketOperation::new(self.input()?);
        let list = match self.bounds.take() {
            Some((lower, upper)) => list.with_bounds(lower, upper),
            None => list,
        };
        self.step = LocalStep::Listed;
        Ok(Next::Call(IoCall::child(list)))
    }

    fn list_visible(&mut self, access: BucketAccess) -> PageTurn<O> {
        let (auth, expiry) = self.caller.take().ok_or(IoError::Unfinished)?;
        let node = self.node;
        // The bearer's expiry bounds a delegated HEAD of a target on another node.
        let rule = LinkShows {
            keys: AccessKeys(Arc::new(access)),
            caller: LinkCaller {
                auth,
                credential: (String::new(), self.group_id, expiry),
            },
            realm_id: node.realm_id,
            node: LinkNode {
                local: node.node_id,
                ids: node.ids,
            },
        };
        let source = RecordSource {
            bounds: self.bounds.take(),
        };
        let page = VisiblePageOperation::new(source, rule, self.input()?, self.hidden.take());
        self.step = LocalStep::Visible;
        Ok(Next::Call(IoCall::child(page)))
    }
}

impl<O: PageObject> Operation for LocalPartOperation<O> {
    type Output = ObjectPage<O>;
    type Error = PageError;

    fn first(&mut self) -> PageTurn<O> {
        let Some((auth, _)) = &self.caller else {
            return self.list_all();
        };
        let node = self.node;
        let ids = (node.realm_id, self.group_id, node.node_id);
        let bucket = &self.input.as_ref().ok_or(IoError::Unfinished)?.bucket;
        let access = BucketAccessOperation::new(auth, ids, bucket);
        self.step = LocalStep::Access;
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> PageTurn<O> {
        match self.step {
            LocalStep::Access => {
                let access = result.child::<BucketAccess, AuthorizeError>()?;
                self.list_visible(access.map_err(PageError::internal)?)
            }
            LocalStep::Listed => {
                let listed = result.child::<ListBucketResult, ListBucketError>()?;
                let listed = listed.map_err(PageError::internal)?;
                let next_cursor = listed.continuation_token.map(after_cursor).transpose()?;
                Ok(Next::Done(ObjectPage {
                    objects: listed.objects.iter().map(O::from).collect(),
                    prefixes: listed.common_prefixes,
                    next_cursor,
                    ..ObjectPage::default()
                }))
            }
            LocalStep::Visible => {
                let page = result
                    .child::<VisiblePage<ListedObject>, VisibleError<LinkError, ListBucketError>>(
                    )?;
                let page = page.map_err(visible_failure)?;
                Ok(Next::Done(shown_page(page, |object| O::from(&object))?))
            }
        }
    }
}

fn range_page_of<O: PageObject>(page: RangePage) -> ObjectPage<O> {
    ObjectPage {
        objects: page.objects.into_iter().map(O::from).collect(),
        prefixes: page.prefixes,
        next_cursor: page.next,
        stale_ranges: page.stale_ranges,
        unavailable_ranges: Vec::new(),
    }
}

/// One range at one member, for a listing this node checks again.
#[derive(Clone, Debug, PartialEq)]
struct MemberCall {
    local: (NodeId, RealmId),
    target: RangeTarget,
    access: MemberAccess,
}

/// The position after the last entry of a page in key order.
fn last_entry<O: PageObject>(bucket: &str, page: &ObjectPage<O>) -> Option<ListContinuationToken> {
    let objects = page.objects.iter().map(|object| (object.key(), false));
    let groups = page.prefixes.iter().map(|group| (group.as_str(), true));
    let (name, group) = objects.chain(groups).max()?;
    Some(after_entry(bucket, name, group))
}

/// One page from one member as a listing source sees it. A member that hid its whole page goes
/// on from its own sealed cursor.
#[derive(Debug, PartialEq)]
struct MemberPageOperation<O> {
    member: MemberCall,
    request: ListRequest,
    position: Option<ListContinuationToken>,
    object: PhantomData<O>,
}

impl<O: PageObject> MemberPageOperation<O> {
    fn call(&self) -> IoCall {
        let request = &self.request;
        let query = RangeQuery {
            bucket: request.bucket.clone(),
            prefix: request.prefix.clone(),
            delimiter: request.delimiter.clone(),
            limit: request.limit,
            cursor: request.cursor.clone(),
        };
        let member = self.member.clone();
        IoCall::child(RangeListOperation::new(
            member.local,
            member.target,
            query,
            member.access,
        ))
    }
}

impl<O: PageObject> Operation for MemberPageOperation<O> {
    type Output = Listed<O>;
    type Error = PageError;

    fn first(&mut self) -> Result<Next<Listed<O>>, PageError> {
        if let Some(position) = self.position.take() {
            self.request.cursor = Some(after_cursor(position)?);
        }
        Ok(Next::Call(self.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Listed<O>>, PageError> {
        let target = &self.member.target;
        let unanswered = PageError::Unanswered {
            range_id: target.range.range_id,
            set_id: target.set_id,
        };
        let page = result.child::<Option<RangePage>, RangeListError>()??;
        let page = range_page_of::<O>(page.ok_or(unanswered)?);
        let shown = !page.objects.is_empty() || !page.prefixes.is_empty();
        let bucket = &self.request.bucket;
        let next = match page.next_cursor.clone() {
            Some(next) if is_sealed(&next) && !shown => {
                self.request.cursor = Some(next);
                return Ok(Next::Call(self.call()));
            }
            Some(next) if is_sealed(&next) => last_entry(bucket, &page),
            Some(next) => match decode_resume(&next)? {
                Resume::After { key, group } => Some(resume_token(bucket, &key, group.as_deref())?),
                Resume::From(_) | Resume::Hidden { .. } => return Err(PageError::Cursor),
            },
            None => None,
        };
        Ok(Next::Done(Listed {
            objects: page.objects,
            prefixes: page.prefixes,
            next,
        }))
    }
}

/// One member as a source of the pages this node checks again.
#[derive(Debug, PartialEq)]
struct MemberSource<O> {
    member: MemberCall,
    object: PhantomData<O>,
}

impl<O: PageObject> ListSource for MemberSource<O> {
    type Object = O;
    type Error = PageError;

    fn call(&self, input: &ListBucketInput) -> IoCall {
        let request = ListRequest {
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            cursor: None,
            limit: (input.max_keys).unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS),
            partial: false,
        };
        IoCall::child(MemberPageOperation::<O> {
            member: self.member.clone(),
            request,
            position: input.continuation_token.clone(),
            object: PhantomData,
        })
    }

    fn take(&self, result: IoResult) -> Result<Listed<O>, ListFailure<PageError>> {
        let page = result.child::<Listed<O>, PageError>()?;
        page.map_err(ListFailure::Source)
    }

    fn key(object: &O) -> &str {
        object.key()
    }
}

/// Shows the keys this node's own rules allow the caller, whatever a member listed.
#[derive(Debug, PartialEq)]
struct CheckedKeys<O> {
    keys: AccessKeys,
    object: PhantomData<O>,
}

impl<O: PageObject> ShowRule for CheckedKeys<O> {
    type Object = O;
    type Error = IoError;

    fn shows(&self, object: &O) -> Shown {
        Shown::Decided(self.keys.allows(object.key()))
    }

    fn answered(&self, _: IoResult) -> Result<Result<bool, IoError>, IoError> {
        Err(IoError::Unfinished)
    }

    fn groups(&self, prefix: &str) -> bool {
        self.keys.groups(prefix)
    }
}

/// One merged page over every range of a distributed bucket under the prefix.
async fn merged_page<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    (info, caller): (&BucketInfo, Option<Caller<'_>>),
    binding: &DistributionBinding,
    request: ListRequest,
) -> PageResult<R::Object, R::Error> {
    let (context, relay) = (scope.context, scope.relay);
    // A relay whose downloads this node authorizes has this node check every remote entry.
    let access = match caller.filter(|_| relay.checks_here()) {
        Some((auth, _)) => {
            let ids = (scope.realm_id, info.group_id, scope.node_id);
            let access = drive(
                BucketAccessOperation::new(auth, ids, &request.bucket),
                context,
            )
            .await;
            Some(access.map_err(PageError::internal)?)
        }
        None => None,
    };
    let checks = access.as_ref().map(|access| (access, info.group_id));
    let view = drive(bound_view(binding), context).await?;
    let snapshot = view.snapshot();
    let fence = drive(local_fence(binding.set_id), context).await?;
    let resume = request.cursor.as_deref().map(decode_resume).transpose()?;
    // A sealed position resumes its range at the member that sealed it.
    let (after, from, mut resumed, hidden_group) = match &resume {
        Some(Resume::After { key, group }) => {
            (Some((key.clone(), group.clone())), None, None, None)
        }
        Some(Resume::From(lower)) => (None, Some(lower.clone()), None, None),
        Some(Resume::Hidden {
            lower,
            node,
            copy,
            cursor,
            group,
        }) => (
            None,
            Some(lower.clone()),
            Some((*node, *copy, cursor.clone())),
            group.clone(),
        ),
        None => (None, None, None, None),
    };
    let prefix = request.prefix.clone().unwrap_or_default();
    let mut page = ObjectPage::default();
    let mut last_group = (after.as_ref().and_then(|(_, group)| group.clone())).or(hidden_group);
    let mut last_key = after.as_ref().map(|(key, _)| key.clone());
    let ranges = snapshot.listed_ranges(&prefix).into_iter().filter(|range| {
        let past = after.as_ref().is_none_or(|(key, group)| {
            let entry = group.as_deref().unwrap_or(key);
            range.upper.as_deref().is_none_or(|upper| entry < upper)
        });
        past && from
            .as_deref()
            .is_none_or(|from| range.lower.as_str() >= from)
    });
    let mut first = true;
    let mut bytes = 0usize;
    // After a skipped range the next page starts at the next range, never before the skip.
    let mut skipped = false;
    for (visited, range) in ranges.enumerate() {
        let listed = page.objects.len() + page.prefixes.len();
        let remaining = request.limit.saturating_sub(listed);
        if remaining == 0 || visited == MAX_PAGE_RANGES || bytes >= MAX_PAGE_BYTES {
            // A page that listed nothing continues at this range, never at its own start.
            let next = match last_key.as_ref().filter(|_| listed > 0 && !skipped) {
                Some(key) => Resume::After {
                    key: key.clone(),
                    group: last_group.clone().filter(|group| group >= key),
                },
                None => Resume::From(range.lower.clone()),
            };
            page.next_cursor = Some(encode_resume(&next)?);
            return Ok(page);
        }
        let start = match (first, &after) {
            (true, Some((key, group))) => Some((key.clone(), group.clone())),
            _ => None,
        };
        first = false;
        let sub = ListRequest {
            limit: remaining,
            cursor: None,
            ..request.clone()
        };
        let resumed = resumed.take();
        // A copy may be behind its coordinator, so a range resumed at one is marked stale.
        if resumed.as_ref().is_some_and(|(_, copy, _)| *copy) {
            page.stale_ranges.push(range.range_id.to_string());
        }
        let local = match &resumed {
            Some((node, ..)) => *node == scope.node_id,
            None => snapshot.lists_locally(binding.member_id, range, &fence),
        };
        let remote = match (local, &resumed) {
            (true, _) => None,
            (false, Some((node, copy, cursor))) => {
                let asked = (&sub, Some(cursor.clone()));
                let holder = (binding, vec![*node], checks);
                let part = remote_part(relay, holder, (range, *copy), asked).await;
                Some(part.map(|part| (part, *copy)))
            }
            (false, None) => {
                // The full position, so a common prefix is skipped as a whole.
                let cursor = (start.clone())
                    .map(|(key, group)| encode_resume(&Resume::After { key, group }))
                    .transpose()?;
                let others = |members: Vec<&DistributionMember>| {
                    (members.into_iter())
                        .filter(|member| member.member_id != binding.member_id)
                        .map(|member| member.node_id)
                        .collect()
                };
                let holders = others(snapshot.list_holder(range).into_iter().collect());
                let asked = (&sub, cursor.clone());
                match remote_part(relay, (binding, holders, checks), (range, false), asked).await {
                    Ok(part) => Some(Ok((part, false))),
                    // The coordinator cannot answer, so this member's copy or another lists.
                    Err(error) if holder_fault::<R>(&error) => {
                        page.stale_ranges.push(range.range_id.to_string());
                        let here = (snapshot.record_holders(range).iter())
                            .any(|member| member.member_id == binding.member_id);
                        match here {
                            true => None,
                            false => {
                                let copies = others(snapshot.copy_holders(range));
                                let asked = (&sub, cursor);
                                let part = remote_part(
                                    relay,
                                    (binding, copies, checks),
                                    (range, true),
                                    asked,
                                );
                                Some(part.await.map(|part| (part, true)))
                            }
                        }
                    }
                    Err(error) => Some(Err(error)),
                }
            }
        };
        // A partial listing keeps what it collected and goes on past a range no holder served.
        let remote = match remote {
            Some(Err(PageError::Unanswered { .. } | PageError::Relay(_))) if request.partial => {
                let range_id = range.range_id.to_string();
                page.stale_ranges.retain(|stale| *stale != range_id);
                page.unavailable_ranges.push(range_id);
                skipped = true;
                continue;
            }
            Some(part) => Some(part?),
            None => None,
        };
        let (part, lister, copy) = match remote {
            Some(((part, lister), copy)) => (part, lister, copy),
            None => {
                let token = (start.as_ref())
                    .map(|(key, group)| resume_token(&request.bucket, key, group.as_deref()))
                    .transpose()?;
                let hidden = (resumed.as_ref())
                    .map(|(_, _, cursor)| open_cursor(cursor).ok_or(PageError::Cursor))
                    .transpose()?;
                let bounds = Some((range.lower.clone(), range.upper.clone()));
                let position = (token, None, hidden);
                let part = local_part(scope, (info, caller), &sub, position, bounds).await?;
                // Listing here instead of the coordinator lists this member's copy.
                let copy = resumed.as_ref().map_or(!local, |(_, copy, _)| *copy);
                (part, scope.node_id, copy)
            }
        };
        skipped = false;
        // A range that stopped among hidden entries goes on from its member's cursor.
        if let Some(cursor) = part.next_cursor.clone().filter(|cursor| is_sealed(cursor)) {
            let next = Resume::Hidden {
                lower: range.lower.clone(),
                node: lister,
                copy,
                cursor,
                group: last_group.clone(),
            };
            page.next_cursor = Some(encode_resume(&next)?);
            return Ok(page);
        }
        let keys = (part.objects.iter().map(PageObject::key))
            .chain(part.prefixes.iter().map(String::as_str));
        bytes += keys.map(str::len).sum::<usize>();
        for group in part.prefixes {
            if last_group.as_deref() == Some(group.as_str()) {
                continue;
            }
            if last_key.as_deref().is_none_or(|key| group.as_str() > key) {
                last_key = Some(group.clone());
            }
            last_group = Some(group.clone());
            page.prefixes.push(group);
        }
        if let Some(object) = part.objects.last()
            && last_key.as_deref().is_none_or(|key| object.key() > key)
        {
            last_key = Some(object.key().to_string());
        }
        page.objects.extend(part.objects);
        // The range's own position, which also moves on when nothing in it was shown.
        if part.next_cursor.is_some() {
            page.next_cursor = part.next_cursor;
            break;
        }
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    type Error = PageError<()>;

    #[test]
    fn resume_round_trips() {
        let after = Resume::After {
            key: "a/b".to_string(),
            group: Some("a/".to_string()),
        };
        let encoded = encode_resume::<()>(&after).unwrap();
        assert_eq!(decode_resume::<()>(&encoded).unwrap(), after);
        let from = Resume::From("m".to_string());
        let encoded = encode_resume::<()>(&from).unwrap();
        assert_eq!(decode_resume::<()>(&encoded).unwrap(), from);
        assert!(matches!(
            decode_resume::<()>("not a cursor"),
            Err(Error::Cursor)
        ));
    }

    #[test]
    fn cursor_skips_group() {
        // A holder resumes past the whole common prefix, not just after its name.
        let request = ListRequest {
            bucket: "b".to_string(),
            prefix: None,
            delimiter: Some("/".to_string()),
            cursor: Some(
                encode_resume::<()>(&Resume::After {
                    key: "d/".to_string(),
                    group: Some("d/".to_string()),
                })
                .unwrap(),
            ),
            limit: 5,
            partial: false,
        };
        let token = local_token::<()>(&request).unwrap().0.unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("d/"));
        let from = ListRequest {
            cursor: Some(encode_resume::<()>(&Resume::From("m".to_string())).unwrap()),
            ..request
        };
        assert!(local_token::<()>(&from).is_err());
    }

    #[test]
    fn resumes_past_group() {
        let token = resume_token::<()>("bucket", "d/x", Some("d/")).unwrap();
        assert_eq!(token.last_common_prefix.as_deref(), Some("d/"));
        let head = BlobHeadKey::from_bytes(&token.last_key).unwrap();
        assert_eq!(head.key, "d/");
    }
}
