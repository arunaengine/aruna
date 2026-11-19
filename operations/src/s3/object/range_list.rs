//! Lists one key range of a distributed bucket at a member: finds the member's published
//! address, chooses the access and turns the member's positions into listing cursors.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::compute::SharedSecret;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::Event;
use aruna_core::heartbeat::HeartbeatEffect;
use aruna_core::node_state::{NodeInfoError, NodeUrlsOperation};
use aruna_core::operation::Operation;
use aruna_core::outbound::{
    OutboundEffect, OutboundEvent, RangeAccess, RangeAnswer, RangeListing, RangePage, RangeRefusal,
};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::distribution::KeyRange;
use aruna_core::structs::storage::distribution::delegation::Payload;
use aruna_core::structs::storage::node_info::NodeUrls;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::distribution::delegate::{DelegationInput, unsigned_delegation};
use crate::s3::object::list::{decode_list_token, encode_list_token};
use crate::s3::object::pages::{Resume, after_cursor, decode_resume, resume_token};
use crate::s3::object::visible::is_sealed;

/// How long a delegation lets a caller without a credential act.
const ANONYMOUS_SECS: u64 = 60;

/// The verified caller an S3 listing is delegated for.
#[derive(Clone, PartialEq)]
pub struct DelegatedCaller {
    pub user_id: UserId,
    pub group_id: GroupId,
    pub credential_id: String,
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Unix seconds at which the caller's credential expires.
    pub credential_expiry: u64,
}

impl std::fmt::Debug for DelegatedCaller {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DelegatedCaller")
    }
}

/// How a listing proves its caller to the member. Never shown in a `Debug` print.
#[derive(Clone, PartialEq)]
pub enum MemberAccess {
    Bearer(Option<SharedSecret>),
    Delegated(DelegatedCaller),
}

impl std::fmt::Debug for MemberAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Bearer(_) => "MemberAccess::Bearer",
            Self::Delegated(_) => "MemberAccess::Delegated",
        })
    }
}

/// The range and member a listing asks.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeTarget {
    pub set_id: Ulid,
    pub node_id: NodeId,
    pub range: KeyRange,
    /// Lists that member's copy while the coordinator cannot.
    pub copy: bool,
}

/// What a range listing asks for; `cursor` is a cursor of `pages`.
#[derive(Clone, Debug, PartialEq)]
pub struct RangeQuery {
    pub bucket: String,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub limit: usize,
    pub cursor: Option<String>,
}

#[derive(Debug, Error, PartialEq)]
pub enum RangeListError {
    #[error("a member refused the listing: {}", .0.message)]
    Refused(RangeRefusal),
    #[error("the cursor is not a next_cursor of this listing")]
    Cursor,
    #[error("{0}")]
    Failed(String),
}

impl From<IoError> for RangeListError {
    fn from(error: IoError) -> Self {
        Self::Failed(error.to_string())
    }
}

/// The S3 continuation token a holder resumes a cursor at; a sealed one passes as is.
fn holder_token(bucket: &str, cursor: &str) -> Option<String> {
    if is_sealed(cursor) {
        return Some(cursor.to_string());
    }
    match decode_resume(cursor).ok()? {
        Resume::After { key, group } => {
            let token = resume_token(bucket, &key, group.as_deref()).ok()?;
            encode_list_token(&token).ok()
        }
        Resume::From(_) | Resume::Hidden { .. } => None,
    }
}

/// The cursor a holder's next token continues at; a sealed one passes as is.
fn page_cursor(token: String) -> Option<String> {
    if is_sealed(&token) {
        return Some(token);
    }
    after_cursor(decode_list_token(&token)?).ok()
}

#[derive(Debug, PartialEq)]
enum ListStep {
    Urls,
    Listing,
    Avoid,
}

/// One page of one range from one member; `None` when it did not answer, so another holder may
/// be asked. A member that failed an S3 listing is avoided for a while.
#[derive(Debug, PartialEq)]
pub struct RangeListOperation {
    local: (NodeId, RealmId),
    target: RangeTarget,
    query: RangeQuery,
    access: MemberAccess,
    now_ms: u64,
    step: ListStep,
}

type ListTurn = Result<Next<Option<RangePage>>, RangeListError>;

impl RangeListOperation {
    pub fn new(
        local: (NodeId, RealmId),
        target: RangeTarget,
        query: RangeQuery,
        access: MemberAccess,
    ) -> Self {
        Self {
            local,
            target,
            query,
            access,
            now_ms: 0,
            step: ListStep::Urls,
        }
    }

    fn delegated(&self) -> bool {
        matches!(self.access, MemberAccess::Delegated(_))
    }

    /// The member gave no answer: another holder may, and a delegated one is avoided for now.
    fn silent(&mut self) -> ListTurn {
        if !self.delegated() {
            return Ok(Next::Done(None));
        }
        self.step = ListStep::Avoid;
        let avoid = HeartbeatEffect::Avoid {
            holder: self.target.node_id,
            now_ms: self.now_ms,
        };
        Ok(Next::Call(
            Effect::Net(NetEffect::Heartbeat(Box::new(avoid))).into(),
        ))
    }

    fn range_access(&self) -> RangeAccess {
        match &self.access {
            MemberAccess::Bearer(token) => RangeAccess::Bearer(token.clone()),
            MemberAccess::Delegated(caller) => {
                let now_secs = self.now_ms / 1000;
                let credential_expiry = match caller.user_id.is_nil() {
                    true => now_secs.saturating_add(ANONYMOUS_SECS),
                    false => caller.credential_expiry,
                };
                let input = DelegationInput {
                    set_id: self.target.set_id,
                    range_id: self.target.range.range_id,
                    target: self.target.node_id,
                    user_id: caller.user_id,
                    group_id: caller.group_id,
                    credential_id: caller.credential_id.clone(),
                    restrictions: caller.restrictions.clone(),
                    credential_expiry,
                    operation: String::new(),
                    bucket: String::new(),
                    key: String::new(),
                    copy_source: None,
                    request_digest: [0; 32],
                    payload: Payload::Empty,
                };
                let delegation = unsigned_delegation(self.local, input, now_secs);
                RangeAccess::Delegated(Box::new(delegation))
            }
        }
    }

    fn list(&mut self, urls: Option<NodeUrls>) -> ListTurn {
        let base = urls.and_then(|urls| match self.delegated() {
            true => urls.s3,
            false => urls.api,
        });
        let Some(base_url) = base else {
            return self.silent();
        };
        let query = &self.query;
        let position = match (&query.cursor, self.delegated()) {
            (Some(cursor), true) => {
                Some(holder_token(&query.bucket, cursor).ok_or(RangeListError::Cursor)?)
            }
            (cursor, _) => cursor.clone(),
        };
        let listing = RangeListing {
            base_url,
            local_node: self.local.0,
            node_id: self.target.node_id,
            set_id: self.target.set_id,
            range: self.target.range.clone(),
            copy: self.target.copy,
            bucket: query.bucket.clone(),
            prefix: query.prefix.clone(),
            delimiter: query.delimiter.clone(),
            limit: query.limit,
            position,
            access: self.range_access(),
        };
        self.step = ListStep::Listing;
        let effect = OutboundEffect::ListRange(Box::new(listing));
        Ok(Next::Call(Effect::Outbound(effect).into()))
    }

    fn answered(&mut self, answer: RangeAnswer) -> ListTurn {
        match answer {
            RangeAnswer::Page(mut page) => {
                if self.delegated()
                    && let Some(token) = page.next.take()
                {
                    // A position this node cannot read leaves the page unusable.
                    let Some(next) = page_cursor(token) else {
                        return Ok(Next::Done(None));
                    };
                    page.next = Some(next);
                }
                Ok(Next::Done(Some(page)))
            }
            RangeAnswer::Silent => self.silent(),
            RangeAnswer::Refused(refusal) if self.delegated() && refusal.status >= 500 => {
                self.silent()
            }
            RangeAnswer::Refused(refusal) => Err(RangeListError::Refused(refusal)),
        }
    }
}

impl Operation for RangeListOperation {
    type Output = Option<RangePage>;
    type Error = RangeListError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ListTurn {
        let urls = NodeUrlsOperation::new(self.target.node_id);
        Ok(Next::Call(IoCall::child(urls)))
    }

    fn next(&mut self, result: IoResult) -> ListTurn {
        match self.step {
            ListStep::Urls => {
                let urls = result.child::<Option<NodeUrls>, NodeInfoError>()?;
                self.list(urls.ok().flatten())
            }
            ListStep::Listing => match result.event()? {
                Event::Outbound(OutboundEvent::RangeListed(answer)) => self.answered(*answer),
                other => Err(RangeListError::Failed(format!(
                    "unexpected listing answer: {}",
                    other.label()
                ))),
            },
            ListStep::Avoid => Ok(Next::Done(None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::compute::SecretBytes;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([3; 32])
    }

    fn operation(access: MemberAccess, cursor: Option<&str>) -> RangeListOperation {
        let target = RangeTarget {
            set_id: Ulid::from_bytes([4; 16]),
            node_id: node(2),
            range: KeyRange {
                range_id: Ulid::from_bytes([5; 16]),
                lower: "a".to_string(),
                upper: None,
                lineage: None,
                coordinator: Ulid::from_bytes([6; 16]),
                epoch: 1,
                replicas: Vec::new(),
            },
            copy: false,
        };
        let query = RangeQuery {
            bucket: "spire".to_string(),
            prefix: None,
            delimiter: None,
            limit: 5,
            cursor: cursor.map(str::to_string),
        };
        RangeListOperation::new((node(1), realm()), target, query, access)
    }

    fn bearer() -> MemberAccess {
        let token = SecretBytes::new(b"do-not-print".to_vec());
        MemberAccess::Bearer(Some(SharedSecret::new(token)))
    }

    fn delegated() -> MemberAccess {
        MemberAccess::Delegated(DelegatedCaller {
            user_id: UserId::nil(realm()),
            group_id: Ulid::from_bytes([7; 16]),
            credential_id: "share:do-not-print".to_string(),
            restrictions: None,
            credential_expiry: 0,
        })
    }

    fn urls(api: Option<&str>, s3: Option<&str>) -> IoResult {
        let urls = NodeUrls {
            api: api.map(str::to_string),
            s3: s3.map(str::to_string),
        };
        let read: Result<Option<NodeUrls>, NodeInfoError> = Ok(Some(urls));
        IoResult::Child(ChildOutcome::new(read))
    }

    fn listed(answer: RangeAnswer) -> IoResult {
        let event = OutboundEvent::RangeListed(Box::new(answer));
        IoResult::Event(Box::new(Event::Outbound(event)))
    }

    fn is_listing(turn: &ListTurn) -> Option<&RangeListing> {
        let Ok(Next::Call(IoCall::Effect(effect))) = turn else {
            return None;
        };
        match effect.as_ref() {
            Effect::Outbound(OutboundEffect::ListRange(listing)) => Some(listing),
            _ => None,
        }
    }

    #[test]
    fn credentials_stay_hidden() {
        for access in [bearer(), delegated()] {
            assert!(!format!("{:?}", operation(access, None)).contains("do-not-print"));
        }
    }

    #[test]
    fn bearer_uses_api() {
        let mut list = operation(bearer(), Some("cursor"));
        list.first().unwrap();
        let turn = list.next(urls(Some("http://api"), Some("http://s3")));
        let listing = is_listing(&turn).expect("the range is listed");
        assert_eq!(listing.base_url, "http://api");
        assert_eq!(listing.position.as_deref(), Some("cursor"));
    }

    #[test]
    fn missing_address_silent() {
        let mut list = operation(bearer(), None);
        list.first().unwrap();
        assert_eq!(
            list.next(urls(None, Some("http://s3"))),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn delegated_failure_avoids() {
        let mut list = operation(delegated(), None);
        list.stamp(9_000);
        list.first().unwrap();
        let turn = list.next(urls(None, Some("http://s3")));
        let listing = is_listing(&turn).expect("the range is listed");
        let RangeAccess::Delegated(delegation) = &listing.access else {
            panic!("the listing is delegated");
        };
        assert_eq!(delegation.credential_expiry, 9 + ANONYMOUS_SECS);
        let refusal = RangeRefusal {
            node_id: node(2),
            status: 503,
            code: None,
            message: String::new(),
        };
        let avoided = list.next(listed(RangeAnswer::Refused(refusal)));
        assert!(matches!(avoided, Ok(Next::Call(IoCall::Effect(_)))));
        let done = Event::Delayed;
        assert_eq!(
            list.next(IoResult::Event(Box::new(done))),
            Ok(Next::Done(None))
        );
    }

    #[test]
    fn denial_is_refusal() {
        let mut list = operation(bearer(), None);
        list.first().unwrap();
        list.next(urls(Some("http://api"), None)).unwrap();
        let refusal = RangeRefusal {
            node_id: node(2),
            status: 403,
            code: None,
            message: "no".to_string(),
        };
        let answer = list.next(listed(RangeAnswer::Refused(refusal.clone())));
        assert_eq!(answer, Err(RangeListError::Refused(refusal)));
    }

    #[test]
    fn foreign_cursor_refused() {
        let mut list = operation(delegated(), Some("not a cursor"));
        list.first().unwrap();
        let answer = list.next(urls(None, Some("http://s3")));
        assert_eq!(answer, Err(RangeListError::Cursor));
    }
}
