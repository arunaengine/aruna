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

