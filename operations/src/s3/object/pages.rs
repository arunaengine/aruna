//! Lists a bucket's objects a caller may see, page by page; a distributed bucket is merged across
//! its key ranges in key order, each range listed here or by a holder through a relay (D12).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionMember, KeyRange,
};
use aruna_core::structs::storage::link::LinkTarget;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use crate::auth::request_authorization::BucketAccess;
use crate::distribution::guard::{GuardError, bound_view, local_fence};
use crate::driver::{DriverContext, drive};
use crate::s3::object::link::{LinkCaller, target_visible};
use crate::s3::object::list::{
    ListBucketInput, ListBucketOperation, ListContinuationToken, ListedObject,
};
use crate::s3::object::visible::{
    HiddenCursor, Listed, Lister, NextPage, VisibleError, after_entry, is_sealed, open_cursor,
    visible_from, visible_page,
};

/// Ranges one merged page visits at most, so its fan-out stays bounded.
const MAX_PAGE_RANGES: usize = 64;
/// Bytes of keys and prefixes after which a merged page visits no further range.
const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// One listing request as the merge needs it.
#[derive(Clone, Debug)]
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
#[derive(Clone, Debug)]
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
pub trait PageObject: for<'a> From<&'a ListedObject> + Send {
    fn key(&self) -> &str;
}

/// The listing of one range by one holder.
pub struct RangeCall<'a> {
    pub set_id: Ulid,
    pub node_id: NodeId,
    pub range: &'a KeyRange,
    pub request: &'a ListRequest,
    pub cursor: Option<&'a str>,
    /// Lists that member's copy while the coordinator cannot.
    pub copy: bool,
}

/// Asks another member for one range's page with the caller's credential.
pub trait RangeRelay: Sync {
    type Object: PageObject;
    type Error: Send;
    /// The holder's page, or `None` when it did not answer, so another holder may be asked.
    fn list_range(
        &self,
        call: RangeCall<'_>,
    ) -> impl Future<Output = Result<Option<ObjectPage<Self::Object>>, Self::Error>> + Send;
    /// Whether a holder's refusal is its own fault, so a copy may list instead.
    fn is_fault(error: &Self::Error) -> bool;
    /// Whether this node also checks the caller on each entry other members list, as for a
    /// share visit, whose downloads this node authorizes on its own path.
    fn checks_here(&self) -> bool {
        false
    }
}

/// What a listing runs with: this node, its records and the relay to other members.
pub struct PageScope<'a, R> {
    pub context: &'a DriverContext,
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub relay: &'a R,
}

/// A caller and the expiry of its bearer, which bounds delegated reads on its behalf.
pub type Caller<'a> = (&'a AuthContext, u64);

#[derive(Debug, Error)]
pub enum PageError<E> {
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
    Relay(E),
}

impl<E> PageError<E> {
    fn internal(error: impl std::fmt::Display) -> Self {
        Self::Internal(error.to_string())
    }
}

type PageResult<O, E> = Result<ObjectPage<O>, PageError<E>>;

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

pub fn encode_resume<E>(resume: &Resume) -> Result<String, PageError<E>> {
    let bytes = postcard::to_allocvec(resume).map_err(PageError::internal)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn decode_resume<E>(cursor: &str) -> Result<Resume, PageError<E>> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| PageError::Cursor)?;
    postcard::from_bytes(&bytes).map_err(|_| PageError::Cursor)
}

/// The token that resumes a local listing after `key`, or past the whole `group`.
pub fn resume_token<E>(
    bucket: &str,
    key: &str,
    group: Option<&str>,
) -> Result<ListContinuationToken, PageError<E>> {
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

/// One page of `info`'s objects on this node: local for an ordinary bucket, merged across the
/// key ranges of a distributed one. With a caller, only what that caller may read shows.
pub async fn object_page<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    info: &BucketInfo,
    caller: Option<Caller<'_>>,
    request: ListRequest,
) -> PageResult<R::Object, R::Error> {
    match info.distribution.as_ref() {
        Some(binding) => merged_page(scope, (info, caller), binding, request).await,
        None => {
            let (token, hidden) = local_token(&request)?;
            local_part(scope, (info, caller), &request, (token, None, hidden), None).await
        }
    }
}

/// One page of the range `bounds` names, for a listing another member relays here.
pub async fn range_page<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    (info, caller): (&BucketInfo, Option<Caller<'_>>),
    request: &ListRequest,
    start_after: Option<String>,
    bounds: (String, Option<String>),
) -> PageResult<R::Object, R::Error> {
    let (token, hidden) = local_token(request)?;
    let position = (token, start_after, hidden);
    local_part(scope, (info, caller), request, position, Some(bounds)).await
}

/// The local listing position a cursor of an unmerged listing names.
fn local_token<E>(
    request: &ListRequest,
) -> Result<(Option<ListContinuationToken>, Option<HiddenCursor>), PageError<E>> {
    if let Some(cursor) = request.cursor.as_deref().filter(|cursor| is_sealed(cursor)) {
        let hidden = open_cursor(cursor).ok_or(PageError::Cursor)?;
        return Ok((None, Some(hidden)));
    }
    match request.cursor.as_deref().map(decode_resume).transpose()? {
        Some(Resume::After { key, group }) => Ok((
            Some(resume_token(&request.bucket, &key, group.as_deref())?),
            None,
        )),
        Some(Resume::From(_) | Resume::Hidden { .. }) => Err(PageError::Cursor),
        None => Ok((None, None)),
    }
}

/// The cursor of a local listing that stopped after `token`.
pub fn after_cursor<E>(token: ListContinuationToken) -> Result<String, PageError<E>> {
    let key = BlobHeadKey::from_bytes(&token.last_key)
        .map_err(PageError::internal)?
        .key;
    let group = token.last_common_prefix;
    encode_resume(&Resume::After { key, group })
}

/// One page of `info`'s objects from this node's records, inside `bounds` when given. For a
/// caller, a key or prefix shows only when the caller may read it, a link only with its
/// target, and a prefix only once an entry below it shows.
async fn local_part<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    (info, caller): (&BucketInfo, Option<Caller<'_>>),
    request: &ListRequest,
    (token, start_after, hidden): (
        Option<ListContinuationToken>,
        Option<String>,
        Option<HiddenCursor>,
    ),
    bounds: Option<(String, Option<String>)>,
) -> PageResult<R::Object, R::Error> {
    let input = ListBucketInput {
        bucket: request.bucket.clone(),
        group_id: info.group_id,
        continuation_token: token,
        max_keys: Some(request.limit),
        prefix: request.prefix.clone().filter(|prefix| !prefix.is_empty()),
        delimiter: request
            .delimiter
            .clone()
            .filter(|delimiter| !delimiter.is_empty()),
        start_after,
    };
    if let Some(caller) = caller {
        return shown_part(scope, (info, caller), (input, hidden), bounds).await;
    }
    let operation = match bounds {
        Some((lower, upper)) => ListBucketOperation::new(input).with_bounds(lower, upper),
        None => ListBucketOperation::new(input),
    };
    let result = drive(operation, scope.context)
        .await
        .map_err(PageError::internal)?;
    Ok(ObjectPage {
        objects: result.objects.iter().map(R::Object::from).collect(),
        prefixes: result.common_prefixes,
        next_cursor: result.continuation_token.map(after_cursor).transpose()?,
        ..ObjectPage::default()
    })
}

/// The page of a local listing a caller may see; a page that stopped among hidden entries
/// carries a sealed cursor.
async fn shown_part<R: RangeRelay>(
    scope: &PageScope<'_, R>,
    (info, (auth, expiry)): (&BucketInfo, Caller<'_>),
    (input, hidden): (ListBucketInput, Option<HiddenCursor>),
    bounds: Option<(String, Option<String>)>,
) -> PageResult<R::Object, R::Error> {
    let (realm_id, node_id, context) = (scope.realm_id, scope.node_id, scope.context);
    let ids = (realm_id, info.group_id, node_id);
    let access = BucketAccess::load(context, auth, ids, &input.bucket)
        .await
        .map_err(PageError::internal)?;
    // The bearer's expiry bounds a delegated HEAD of a target on another node.
    let caller = LinkCaller {
        auth: auth.clone(),
        credential: (String::new(), info.group_id, expiry),
    };
    let readable = |key: &str| access.allows(Some(key), &Permission::READ);
    let page = visible_page(
        context,
        input,
        (hidden, bounds),
        |object: &ListedObject| {
            let object = object.clone();
            let (access, caller) = (&access, &caller);
            Box::pin(async move {
                if !access.allows(Some(&object.head.key), &Permission::READ) {
                    return Ok(false);
                }
                match object
                    .link
                    .as_ref()
                    .map(|link| LinkTarget::of(link, realm_id))
                {
                    None => Ok(true),
                    Some(Some(target)) => target_visible(context, caller, &target, node_id).await,
                    Some(None) => Ok(false),
                }
            })
        },
        readable,
    );
    let page = page.await.map_err(|error| match error {
        VisibleError::List(error) => PageError::internal(error),
        VisibleError::Shows(error) => PageError::internal(error),
        VisibleError::Cursor => PageError::Internal("listing cursor not sealed".into()),
    })?;
    let next_cursor = match page.next {
        Some(NextPage::After(token)) => Some(after_cursor(token)?),
        Some(NextPage::Sealed(sealed)) => Some(sealed),
        None => None,
    };
    Ok(ObjectPage {
        objects: page.objects.iter().map(R::Object::from).collect(),
        prefixes: page.prefixes,
        next_cursor,
        ..ObjectPage::default()
    })
}

/// One range at one member, listed through the relay for a page this node checks again.
struct MemberRange<'a, R> {
    relay: &'a R,
    call: (Ulid, NodeId, bool),
    range: &'a KeyRange,
}

impl<R: RangeRelay> Lister for MemberRange<'_, R> {
    type Object = R::Object;
    type Error = PageError<R::Error>;

    async fn list(&self, input: &ListBucketInput) -> Result<Listed<R::Object>, Self::Error> {
        let (set_id, node_id, copy) = self.call;
        let request = ListRequest {
            bucket: input.bucket.clone(),
            prefix: input.prefix.clone(),
            delimiter: input.delimiter.clone(),
            cursor: None,
            limit: (input.max_keys).unwrap_or(ListBucketOperation::DEFAULT_MAX_KEYS),
            partial: false,
        };
        let mut cursor = input
            .continuation_token
            .clone()
            .map(after_cursor)
            .transpose()?;
        loop {
            let call = RangeCall {
                set_id,
                node_id,
                range: self.range,
                request: &request,
                cursor: cursor.as_deref(),
                copy,
            };
            let unanswered = PageError::Unanswered {
                range_id: self.range.range_id,
                set_id,
            };
            let page = self
                .relay
                .list_range(call)
                .await
                .map_err(PageError::Relay)?;
            let page = page.ok_or(unanswered)?;
            let shown = !page.objects.is_empty() || !page.prefixes.is_empty();
            // A member that hid its whole page goes on from its own sealed cursor.
            let next = match page.next_cursor.clone() {
                Some(next) if is_sealed(&next) && !shown => {
                    cursor = Some(next);
                    continue;
                }
                Some(next) if is_sealed(&next) => last_entry(&request.bucket, &page),
                Some(next) => match decode_resume(&next)? {
                    Resume::After { key, group } => {
                        Some(resume_token(&request.bucket, &key, group.as_deref())?)
                    }
                    Resume::From(_) | Resume::Hidden { .. } => return Err(PageError::Cursor),
                },
                None => None,
            };
            return Ok(Listed {
                objects: page.objects,
                prefixes: page.prefixes,
                next,
            });
        }
    }

    fn key(object: &R::Object) -> &str {
        object.key()
    }
}

/// The position after the last entry of `page` in key order.
fn last_entry<O: PageObject>(bucket: &str, page: &ObjectPage<O>) -> Option<ListContinuationToken> {
    let objects = page.objects.iter().map(|object| (object.key(), false));
    let groups = page.prefixes.iter().map(|group| (group.as_str(), true));
    let (name, group) = objects.chain(groups).max()?;
    Some(after_entry(bucket, name, group))
}

/// One range's page as this node's own rules show it to the caller, read from `node_id`:
/// a key shows when `access` allows it, a common prefix once a key below it does, and a page
/// that stops among hidden entries ends with this node's sealed cursor.
async fn checked_part<R: RangeRelay>(
    relay: &R,
    (access, group_id): (&BucketAccess, Ulid),
    (set_id, node_id, range, copy): (Ulid, NodeId, &KeyRange, bool),
    (request, cursor): (&ListRequest, Option<&str>),
) -> Result<ObjectPage<R::Object>, PageError<R::Error>> {
    let (token, hidden) = match cursor {
        Some(cursor) if is_sealed(cursor) => (
            None,
            Some(open_cursor::<HiddenCursor>(cursor).ok_or(PageError::Cursor)?),
        ),
        Some(cursor) => match decode_resume(cursor)? {
            Resume::After { key, group } => (
                Some(resume_token(&request.bucket, &key, group.as_deref())?),
                None,
            ),
            Resume::From(_) | Resume::Hidden { .. } => return Err(PageError::Cursor),
        },
        None => (None, None),
    };
    let input = ListBucketInput {
        bucket: request.bucket.clone(),
        group_id,
        continuation_token: token,
        max_keys: Some(request.limit),
        prefix: request.prefix.clone().filter(|prefix| !prefix.is_empty()),
        delimiter: (request.delimiter.clone()).filter(|delimiter| !delimiter.is_empty()),
        start_after: None,
    };
    let lister = MemberRange {
        relay,
        call: (set_id, node_id, copy),
        range,
    };
    let shows = |object: &R::Object| {
        let shown = access.allows(Some(object.key()), &Permission::READ);
        Box::pin(async move { Ok::<bool, std::convert::Infallible>(shown) })
            as futures_util::future::BoxFuture<'static, _>
    };
    let groups = |prefix: &str| access.allows(Some(prefix), &Permission::READ);
    let page = visible_from(&lister, input, hidden, shows, groups).await;
    let page = page.map_err(|error| match error {
        VisibleError::List(error) => error,
        VisibleError::Shows(never) => match never {},
        VisibleError::Cursor => PageError::Internal("listing cursor not sealed".into()),
    })?;
    let next_cursor = match page.next {
        Some(NextPage::After(token)) => Some(after_cursor(token)?),
        Some(NextPage::Sealed(sealed)) => Some(sealed),
        None => None,
    };
    Ok(ObjectPage {
        objects: page.objects,
        prefixes: page.prefixes,
        next_cursor,
        ..ObjectPage::default()
    })
}

/// One range's page from a holder of the range, with the holder that answered; only a range
/// nobody answers ends the page. With `checks`, this node checks the caller on each entry too.
async fn remote_part<R: RangeRelay>(
    relay: &R,
    (binding, holders, checks): (
        &DistributionBinding,
        Vec<NodeId>,
        Option<(&BucketAccess, Ulid)>,
    ),
    (range, copy): (&KeyRange, bool),
    (request, cursor): (&ListRequest, Option<String>),
) -> Result<(ObjectPage<R::Object>, NodeId), PageError<R::Error>> {
    for node_id in holders {
        if let Some(checks) = checks {
            let call = (binding.set_id, node_id, range, copy);
            match checked_part(relay, checks, call, (request, cursor.as_deref())).await {
                Ok(page) => return Ok((page, node_id)),
                Err(PageError::Unanswered { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        let call = RangeCall {
            set_id: binding.set_id,
            node_id,
            range,
            request,
            cursor: cursor.as_deref(),
            copy,
        };
        if let Some(page) = relay.list_range(call).await.map_err(PageError::Relay)? {
            return Ok((page, node_id));
        }
    }
    Err(PageError::Unanswered {
        range_id: range.range_id,
        set_id: binding.set_id,
    })
}

/// Whether a failed range listing is the holders' fault, so a copy may list instead.
fn holder_fault<R: RangeRelay>(error: &PageError<R::Error>) -> bool {
    match error {
        PageError::Unanswered { .. } => true,
        PageError::Relay(error) => R::is_fault(error),
        _ => false,
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
            let access = BucketAccess::load(context, auth, ids, &request.bucket).await;
            Some(access.map_err(PageError::internal)?)
        }
        None => None,
    };
    let checks = access.as_ref().map(|access| (access, info.group_id));
    let view = bound_view(context, binding).await?;
    let snapshot = view.snapshot();
    let fence = local_fence(context, binding.set_id).await?;
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
