//! Reads one exact version from the node holding it, under the read grant that node issued.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::errors::StagingSourceError;
use aruna_core::events::{Event, NetEvent};
use aruna_core::native_reference::{GrantedBody, GrantedReadEffect, NativeRange};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::read_grant::{
    self, GrantScope, GrantedRequest, HEAD_OPERATION, READ_OPERATION, SignedGrant,
};
use aruna_core::time::unix_timestamp_millis;

/// Seconds a granted request this node signs stays valid.
const GRANTED_SECS: u64 = 15;

/// One exact version read from `holder` for the caller the grant names. The holder checks the
/// grant it issued, this node's signature and the caller's permissions, then serves the range.
#[derive(Debug, PartialEq)]
pub struct GrantedReadOperation {
    ids: Option<(NodeId, RealmId)>,
    holder: NodeId,
    grant: SignedGrant,
    range: Option<Range<u64>>,
    head: bool,
}

impl GrantedReadOperation {
    /// `ids` is this node and its realm, as `DriverContext::node_and_realm` gives them.
    pub fn new(
        ids: Option<(NodeId, RealmId)>,
        holder: NodeId,
        grant: &SignedGrant,
        range: Option<Range<u64>>,
        head: bool,
    ) -> Self {
        Self {
            ids,
            holder,
            grant: grant.clone(),
            range,
            head,
        }
    }

    fn request(&self, range: Option<NativeRange>) -> Result<GrantedRequest, StagingSourceError> {
        let (issuer, realm_id) = self.ids.ok_or(StagingSourceError::HandleMissing)?;
        let operation = match self.head {
            true => HEAD_OPERATION,
            false => READ_OPERATION,
        };
        let digest = read_grant::request_digest(
            operation,
            &self.grant.grant.scope,
            range.map(|range| (range.start, range.end)),
        )
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
        Ok(GrantedRequest {
            realm_id,
            issuer,
            target: self.holder,
            operation: operation.to_string(),
            request_digest: digest,
            nonce: rand::random(),
            issued_at: unix_timestamp_millis() / 1000,
            valid_secs: GRANTED_SECS,
            grant: self.grant.clone(),
        })
    }
}

impl Operation for GrantedReadOperation {
    type Output = GrantedBody;
    type Error = StagingSourceError;

    fn first(&mut self) -> Result<Next<GrantedBody>, StagingSourceError> {
        if self.ids.is_none() {
            return Err(StagingSourceError::HandleMissing);
        }
        let GrantScope::Version(arn) = &self.grant.grant.scope else {
            return Err(StagingSourceError::AccessDenied);
        };
        let range = NativeRange::checked(self.range.clone())?;
        let read = GrantedReadEffect {
            request: self.request(range)?,
            bucket: arn.bucket.clone(),
            key: arn.key.clone(),
            version_id: arn.version,
            head: self.head,
            range,
        };
        let effect = Effect::Net(NetEffect::GrantedRead(Box::new(read)));
        Ok(Next::Call(IoCall::from(effect)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<GrantedBody>, StagingSourceError> {
        match result.event()? {
            Event::Net(NetEvent::GrantedRead(answer)) => Ok(Next::Done(*answer?)),
            other => Err(IoError::Unexpected {
                expected: "Event::Net(NetEvent::GrantedRead)",
                got: other.label(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::stream::BackendStream;
    use aruna_core::structs::execution::job::SubmissionId;
    use aruna_core::structs::execution::source_access::SourceMetadata;
    use aruna_core::structs::storage::read_grant::ReadGrant;
    use aruna_core::structs::storage::replication::VersionedObjectArn;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn arn() -> VersionedObjectArn {
        VersionedObjectArn::new(realm(), node(2), "spire", "a/b", Ulid::from_bytes([3; 16]))
            .unwrap()
    }

    fn grant(scope: GrantScope) -> SignedGrant {
        SignedGrant {
            grant: ReadGrant {
                realm_id: realm(),
                issuer: node(2),
                submission_id: SubmissionId([2; 32]),
                user_id: UserId::nil(realm()),
                restrictions: None,
                scope,
                operations: vec![READ_OPERATION.to_string()],
                expires_at: 100,
            },
            signature: iroh::SecretKey::from_bytes(&[2; 32]).sign(b"grant"),
        }
    }

    fn read(range: Option<Range<u64>>, head: bool) -> GrantedReadOperation {
        let grant = grant(GrantScope::Version(arn()));
        GrantedReadOperation::new(Some((node(1), realm())), node(2), &grant, range, head)
    }

    fn answer(read: Result<Box<GrantedBody>, StagingSourceError>) -> IoResult {
        IoResult::Event(Box::new(Event::Net(NetEvent::GrantedRead(read))))
    }

    fn sent(next: Result<Next<GrantedBody>, StagingSourceError>) -> GrantedReadEffect {
        let Ok(Next::Call(IoCall::Effect(effect))) = next else {
            panic!("expected the read effect");
        };
        let Effect::Net(NetEffect::GrantedRead(read)) = *effect else {
            panic!("expected a granted read");
        };
        *read
    }

    // The request binds this node, the holder, the operation and the exact range.
    #[test]
    fn binds_exact_range() {
        let effect = sent(read(Some(2..5), false).first());
        let scope = GrantScope::Version(arn());
        let digest = read_grant::request_digest(READ_OPERATION, &scope, Some((2, 5))).unwrap();
        assert_eq!(effect.request.issuer, node(1));
        assert_eq!(effect.request.target, node(2));
        assert_eq!(effect.request.operation, READ_OPERATION);
        assert_eq!(effect.request.request_digest, digest);
        assert_eq!(effect.request.valid_secs, GRANTED_SECS);
        assert_eq!(
            (effect.bucket.as_str(), effect.key.as_str()),
            ("spire", "a/b")
        );
        assert_eq!(effect.version_id, arn().version);
        assert_eq!(effect.range, Some(NativeRange { start: 2, end: 5 }));
        assert!(!effect.head);
    }

    // A metadata read names its own operation, so a grant check tells the two apart.
    #[test]
    fn head_names_operation() {
        let effect = sent(read(None, true).first());
        assert_eq!(effect.request.operation, HEAD_OPERATION);
        assert!(effect.head);
        assert_eq!(effect.range, None);
    }

    // Reads that cannot be asked for end before anything is sent.
    #[test]
    fn refuses_before_sending() {
        let version = grant(GrantScope::Version(arn()));
        let mut unlinked = GrantedReadOperation::new(None, node(2), &version, None, false);
        assert_eq!(unlinked.first(), Err(StagingSourceError::HandleMissing));
        let bucket = grant(GrantScope::Bucket {
            node_id: node(2),
            bucket: "spire".to_string(),
        });
        let ids = Some((node(1), realm()));
        let mut listing = GrantedReadOperation::new(ids, node(2), &bucket, None, false);
        assert_eq!(listing.first(), Err(StagingSourceError::AccessDenied));
        let empty = read(Some(5..5), false).first();
        assert!(matches!(empty, Err(StagingSourceError::ReadError(_))));
    }

    // The holder's answer is the outcome: its bytes, or its refusal.
    #[test]
    fn answers_with_holder() {
        let metadata = SourceMetadata {
            content_length: 3,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        };
        let blob = BackendStream(Box::pin(futures_util::stream::empty()));
        let body = GrantedBody {
            metadata: metadata.clone(),
            blob,
        };
        let Ok(Next::Done(done)) = read(None, false).next(answer(Ok(Box::new(body)))) else {
            panic!("expected the holder's answer");
        };
        assert_eq!(done.metadata, metadata);
        let refused = read(None, false).next(answer(Err(StagingSourceError::NotFound)));
        assert_eq!(refused, Err(StagingSourceError::NotFound));
        assert!(read(None, false).next(IoResult::Done).is_err());
    }
}
