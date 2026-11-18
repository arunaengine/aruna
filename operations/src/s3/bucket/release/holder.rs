//! Reads release crates and asks a range's holder about releases. The effects below only stream
//! the bytes and carry the request; what is read and who is asked is decided here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect, MetadataRequestEffect, NetEffect};
use aruna_core::events::{BlobEvent, Event, MetadataAnswer, NetEvent};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::release::{
    CRATE_CHUNK, CrateBytes, CrateRead, CrateReadError, HolderAnswer, HolderAsk,
};
use thiserror::Error;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, decode_message, encode_message,
};
use crate::realm::peer_trust::is_sync_eligible;
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult, ObjectRangeRequest};
use crate::s3::object::read::GetRoutedOperation;

/// How long one question to a range's holder may take in all.
const HOLDER_DEADLINE: Duration = Duration::from_secs(60);

/// Reads one release crate version, or one chunk of it, with the caller's identity, through a
/// holder when this member keeps the record but not the bytes. Answers the version's whole size
/// with the bytes read.
#[derive(Debug, PartialEq)]
pub struct ReadCrateOperation {
    read: CrateRead,
    range: Option<ObjectRangeRequest>,
    collecting: Option<(HashMap<String, String>, u64)>,
}

type CrateTurn = Result<Next<(CrateBytes, u64)>, CrateReadError>;

impl ReadCrateOperation {
    pub fn new(read: CrateRead) -> Self {
        Self {
            read,
            range: None,
            collecting: None,
        }
    }

    /// Reads only the [`CRATE_CHUNK`] that starts at `offset`.
    pub fn chunk_at(mut self, offset: u64) -> Self {
        self.range = Some(ObjectRangeRequest::StartEnd {
            start: offset,
            end: offset.saturating_add(CRATE_CHUNK - 1),
        });
        self
    }

    fn input(&self) -> GetObjectInput {
        let read = &self.read;
        GetObjectInput {
            bucket: read.bucket.clone(),
            key: read.key.clone(),
            version_id: Some(read.version_id),
            range: self.range.clone(),
            group_id: read.group_id,
            user_identity: read.auth.user_id,
            realm_id: read.realm_id,
            node_id: read.node_id,
        }
    }
}

impl Operation for ReadCrateOperation {
    type Output = (CrateBytes, u64);
    type Error = CrateReadError;

    fn first(&mut self) -> CrateTurn {
        let restrictions = self.read.auth.path_restrictions.clone();
        let routed = GetRoutedOperation::new(self.input(), restrictions, None);
        Ok(Next::Call(IoCall::child(routed)))
    }

    fn next(&mut self, result: IoResult) -> CrateTurn {
        if let Some((metadata, size)) = self.collecting.take() {
            let Event::Blob(event) = result.event()? else {
                return Err(CrateReadError::Failed("no crate bytes".to_string()));
            };
            return match event {
                BlobEvent::StreamCollected { bytes: Ok(bytes) } => {
                    Ok(Next::Done((CrateBytes { metadata, bytes }, size)))
                }
                BlobEvent::StreamCollected { bytes: Err(error) } => {
                    Err(CrateReadError::Failed(error))
                }
                other => Err(CrateReadError::Failed(format!("{other:?}"))),
            };
        }
        let found = match result.child::<GetObjectResult, GetObjectError>()? {
            Ok(found) => found,
            Err(
                GetObjectError::NoSuchKey
                | GetObjectError::NoSuchVersion
                | GetObjectError::DeleteMarker,
            ) => return Err(CrateReadError::Missing),
            Err(error) => return Err(CrateReadError::Failed(error.to_string())),
        };
        self.collecting = Some((found.metadata, found.info.size));
        let collect = BlobEffect::CollectStream { blob: found.blob };
        Ok(Next::Call(Effect::Blob(collect).into()))
    }
}

/// Why a holder gave no answer.
#[derive(Debug, Error, PartialEq)]
pub enum AskHolderError {
    #[error("the holder refused the request: {0:?}")]
    Refused(MetadataReadError),
    #[error("{0}")]
    Failed(String),
}

impl From<IoError> for AskHolderError {
    fn from(error: IoError) -> Self {
        Self::Failed(error.to_string())
    }
}

/// Asks one holder under the caller's bearer, which the holder validates itself.
#[derive(Debug, PartialEq)]
pub struct AskHolderOperation {
    node_id: NodeId,
    token: Result<AuthToken, String>,
    bucket: String,
    ask: Option<HolderAsk>,
    realm_id: Option<RealmId>,
    asking: bool,
}

type AskTurn = Result<Next<HolderAnswer>, AskHolderError>;

impl AskHolderOperation {
    pub fn new(node_id: NodeId, bearer: &str, bucket: &str, ask: HolderAsk) -> Self {
        Self {
            node_id,
            token: AuthToken::bearer(bearer).map_err(|error| error.to_string()),
            bucket: bucket.to_string(),
            ask: Some(ask),
            realm_id: None,
            asking: false,
        }
    }

    /// Sends the bearer only when `realm_id` lists the node as infrastructure.
    pub fn infrastructure_only(mut self, realm_id: RealmId) -> Self {
        self.realm_id = Some(realm_id);
        self
    }

    fn send(&mut self) -> AskTurn {
        let auth_token = self.token.clone().map_err(AskHolderError::Failed)?;
        let ask = self.ask.take().ok_or(IoError::Unfinished)?;
        let frame = encode_message(&MetadataTransportMessage::ReleaseHolder {
            auth_token: Some(auth_token),
            bucket: self.bucket.clone(),
            ask,
        })
        .map_err(AskHolderError::Failed)?;
        let request = MetadataRequestEffect {
            peer: self.node_id,
            frame,
            timeout: HOLDER_DEADLINE,
        };
        self.asking = true;
        let effect = Effect::Net(NetEffect::MetadataRequest(Box::new(request)));
        Ok(Next::Call(effect.into()))
    }
}

/// A holder's reply; a transport failure reads like an unavailable holder.
fn holder_reply(result: IoResult) -> Result<HolderAnswer, AskHolderError> {
    let unavailable = AskHolderError::Refused(MetadataReadError::Unavailable);
    let Event::Net(NetEvent::MetadataAnswer(answer)) = result.event()? else {
        return Err(unavailable);
    };
    let MetadataAnswer { result, .. } = *answer;
    match result.ok().and_then(|bytes| decode_message(&bytes).ok()) {
        Some(MetadataTransportMessage::ReleaseHolderResults { result }) => {
            result.map_err(AskHolderError::Refused)
        }
        _ => Err(unavailable),
    }
}

impl Operation for AskHolderOperation {
    type Output = HolderAnswer;
    type Error = AskHolderError;

    fn first(&mut self) -> AskTurn {
        match self.realm_id {
            Some(realm_id) => Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id)))),
            None => self.send(),
        }
    }

    fn next(&mut self, result: IoResult) -> AskTurn {
        if self.asking {
            return Ok(Next::Done(holder_reply(result)?));
        }
        let config = result.child::<RealmConfigDocument, GetConfigError>()?;
        let config = config.map_err(|error| AskHolderError::Failed(error.to_string()))?;
        if !is_sync_eligible(&config, self.node_id) {
            let reason = format!("{} is no infrastructure node", self.node_id);
            return Err(AskHolderError::Failed(reason));
        }
        self.send()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::UserId;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::auth::AuthContext;
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[3; 32]).public()
    }

    fn crate_read() -> CrateRead {
        let realm_id = RealmId::from_bytes([1; 32]);
        CrateRead {
            bucket: "db".to_string(),
            key: "release.json".to_string(),
            version_id: Ulid::from_bytes([2; 16]),
            group_id: Ulid::from_bytes([4; 16]),
            auth: AuthContext {
                user_id: UserId::nil(realm_id),
                ..AuthContext::anonymous(realm_id)
            },
            realm_id,
            node_id: node(),
        }
    }

    fn failed_get(error: GetObjectError) -> IoResult {
        let read: Result<GetObjectResult, GetObjectError> = Err(error);
        IoResult::Child(ChildOutcome::new(read))
    }

    #[test]
    fn chunk_names_range() {
        let read = ReadCrateOperation::new(crate_read()).chunk_at(CRATE_CHUNK);
        let expected = ObjectRangeRequest::StartEnd {
            start: CRATE_CHUNK,
            end: 2 * CRATE_CHUNK - 1,
        };
        assert_eq!(read.input().range, Some(expected));
        assert_eq!(read.input().version_id, Some(crate_read().version_id));
    }

    #[test]
    fn gone_version_missing() {
        let mut read = ReadCrateOperation::new(crate_read());
        read.first().unwrap();
        let answer = read.next(failed_get(GetObjectError::DeleteMarker));
        assert_eq!(answer, Err(CrateReadError::Missing));
    }

    #[test]
    fn collected_bytes_answer() {
        let mut read = ReadCrateOperation::new(crate_read());
        read.collecting = Some((HashMap::new(), 9));
        let bytes = Ok(vec![1, 2]);
        let event = Event::Blob(BlobEvent::StreamCollected { bytes });
        let found = CrateBytes {
            metadata: HashMap::new(),
            bytes: vec![1, 2],
        };
        assert_eq!(
            read.next(IoResult::Event(Box::new(event))),
            Ok(Next::Done((found, 9)))
        );
    }

    fn ask() -> HolderAsk {
        HolderAsk::Members {
            range_ids: Vec::new(),
            queries: Vec::new(),
        }
    }

    #[test]
    fn bearer_stays_hidden() {
        let holder = AskHolderOperation::new(node(), "do-not-print", "db", ask());
        assert!(!format!("{holder:?}").contains("do-not-print"));
    }

    #[test]
    fn request_uses_transport() {
        let mut holder = AskHolderOperation::new(node(), "token", "db", ask());
        let Ok(Next::Call(IoCall::Effect(effect))) = holder.first() else {
            panic!("an unchecked holder is asked at once");
        };
        assert!(matches!(
            *effect,
            Effect::Net(NetEffect::MetadataRequest(_))
        ));
    }

    #[test]
    fn broken_reply_unavailable() {
        let answer = MetadataAnswer {
            peer: node(),
            elapsed: Duration::ZERO,
            result: Ok(vec![0xff]),
        };
        let event = Event::Net(NetEvent::MetadataAnswer(Box::new(answer)));
        assert_eq!(
            holder_reply(IoResult::Event(Box::new(event))),
            Err(AskHolderError::Refused(MetadataReadError::Unavailable))
        );
    }

    #[test]
    fn checked_holder_reads_config() {
        let realm_id = RealmId::from_bytes([1; 32]);
        let mut holder =
            AskHolderOperation::new(node(), "token", "db", ask()).infrastructure_only(realm_id);
        assert!(matches!(holder.first(), Ok(Next::Call(IoCall::Child(_)))));
        let failed: Result<RealmConfigDocument, GetConfigError> = Err(IoError::Unfinished.into());
        let answer = holder.next(IoResult::Child(ChildOutcome::new(failed)));
        assert!(matches!(answer, Err(AskHolderError::Failed(_))));
    }
}
