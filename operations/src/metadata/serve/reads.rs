//! Serves the reads a peer forwards for a caller: the caller is checked first, then the read
//! runs as that caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use ulid::Ulid;

use crate::forward::transport::read_error;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::links::local::{LinksLocalOperation, SetPreviewOperation};
use crate::metadata::api::links::types::{LinkNodePage, LinkNodeRequest, SetPreviewPage};
use crate::metadata::identifier_lookup::{IdentifierLookupOperation, IdentifierQuery};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::metadata::read_peer::ReadPeerOperation;
use crate::metadata::secondary_ids::IdentifierMatch;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// A read that runs as the checked caller.
#[derive(Debug, PartialEq)]
pub(crate) enum CallerRead {
    Identifier(IdentifierQuery),
    Links(Box<LinkNodeRequest>),
    /// The group owning a bucket; only a known caller may ask.
    Owner(String),
    Preview {
        set_id: Ulid,
        limit: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ReadReply {
    Identifier,
    Links,
    Owner,
    Preview,
}

impl ReadReply {
    fn of(read: &CallerRead) -> Self {
        match read {
            CallerRead::Identifier(_) => Self::Identifier,
            CallerRead::Links(_) => Self::Links,
            CallerRead::Owner(_) => Self::Owner,
            CallerRead::Preview { .. } => Self::Preview,
        }
    }

    fn refused(self, error: MetadataReadError) -> MetadataTransportMessage {
        let result = Err(error);
        match self {
            Self::Identifier => MetadataTransportMessage::IdentifierMatches { result },
            Self::Links => MetadataTransportMessage::DataLinkResults { result },
            Self::Owner => MetadataTransportMessage::BucketOwnerResult { result },
            Self::Preview => MetadataTransportMessage::SetPreviewResults { result },
        }
    }

    fn answer(self, result: IoResult) -> Result<MetadataTransportMessage, IoError> {
        Ok(match self {
            Self::Identifier => MetadataTransportMessage::IdentifierMatches {
                result: (result.child::<Vec<IdentifierMatch>, MetadataApiError>()?)
                    .map_err(|_| MetadataReadError::Unavailable),
            },
            Self::Links => MetadataTransportMessage::DataLinkResults {
                result: (result.child::<LinkNodePage, MetadataApiError>()?)
                    .map(Box::new)
                    .map_err(read_error),
            },
            Self::Owner => MetadataTransportMessage::BucketOwnerResult {
                result: (result.child::<BucketInfo, GetBucketError>()?)
                    .map(|info| info.group_id)
                    .map_err(|error| match error {
                        GetBucketError::NotFound => MetadataReadError::NotFound,
                        _ => MetadataReadError::Unavailable,
                    }),
            },
            Self::Preview => MetadataTransportMessage::SetPreviewResults {
                result: (result.child::<SetPreviewPage, MetadataApiError>()?)
                    .map(Box::new)
                    .map_err(read_error),
            },
        })
    }
}

/// Checks the caller `peer` forwarded, then answers its read; a node without a realm answers
/// unavailable.
#[derive(Debug, PartialEq)]
pub(crate) struct CallerReadOperation {
    peer: NodeId,
    realm_id: Option<RealmId>,
    token: Option<AuthToken>,
    read: Option<CallerRead>,
    reply: ReadReply,
}

type ReadTurn = Result<Next<MetadataTransportMessage>, IoError>;

impl CallerReadOperation {
    pub(crate) fn new(
        (peer, realm_id): (NodeId, Option<RealmId>),
        token: Option<AuthToken>,
        read: CallerRead,
    ) -> Self {
        Self {
            peer,
            realm_id,
            token,
            reply: ReadReply::of(&read),
            read: Some(read),
        }
    }

    fn run(&mut self, realm_id: RealmId, auth: Option<AuthContext>, read: CallerRead) -> ReadTurn {
        let call = match read {
            CallerRead::Identifier(query) => {
                IoCall::child(IdentifierLookupOperation::new(realm_id, auth, query))
            }
            CallerRead::Links(request) => {
                IoCall::child(LinksLocalOperation::new(realm_id, auth, *request))
            }
            CallerRead::Owner(_) if auth.is_none() => {
                return Ok(Next::Done(
                    self.reply.refused(MetadataReadError::Unauthorized),
                ));
            }
            CallerRead::Owner(bucket) => IoCall::child(GetBucketOperation::new(bucket)),
            CallerRead::Preview { set_id, limit } => IoCall::child(SetPreviewOperation::new(
                (realm_id, auth),
                set_id,
                None,
                limit,
            )),
        };
        Ok(Next::Call(call))
    }
}

impl Operation for CallerReadOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> ReadTurn {
        let Some(realm_id) = self.realm_id else {
            return Ok(Next::Done(
                self.reply.refused(MetadataReadError::Unavailable),
            ));
        };
        let check = ReadPeerOperation::new((self.peer, realm_id), self.token.take(), false);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        let (Some(realm_id), Some(read)) = (self.realm_id, self.read.take()) else {
            return self.reply.answer(result).map(Next::Done);
        };
        match result.child::<Option<AuthContext>, MetadataReadError>()? {
            Ok(auth) => self.run(realm_id, auth, read),
            Err(error) => Ok(Next::Done(self.reply.refused(error))),
        }
    }
}
