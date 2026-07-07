use aruna_core::structs::{
    Group, GroupAuthorizationDocument, MetadataRegistryRecord, NotificationRecord, RealmId, Role,
    User, WatchEventMask, WatchSubscription,
};
use aruna_core::types::{GroupId, RoleId, UserId};
use aruna_net::streams::BiStream;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use tokio::io::AsyncWriteExt;
use ulid::Ulid;

/// A proxied reply carries at most one document; 16 MiB bounds a large document
/// while still refusing a hostile oversized frame.
pub const HOLDER_PROXY_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
/// Forwarded bearer tokens are capped like the metadata bearer wrapper.
pub const MAX_PROXY_BEARER_TOKEN_LEN: usize = 4096;

/// Length-capped wrapper around a forwarded bearer token. Deserialization
/// enforces the cap so a hostile origin cannot smuggle an unbounded token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProxyBearerToken(String);

impl ProxyBearerToken {
    pub fn new(token: impl Into<String>) -> Result<Self, ProxyBearerTokenError> {
        let token = token.into();
        if token.len() > MAX_PROXY_BEARER_TOKEN_LEN {
            return Err(ProxyBearerTokenError {
                length: token.len(),
            });
        }
        Ok(Self(token))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ProxyBearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let token = String::deserialize(deserializer)?;
        Self::new(token).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyBearerTokenError {
    length: usize,
}

impl fmt::Display for ProxyBearerTokenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "proxy bearer token length {} exceeds maximum {}",
            self.length, MAX_PROXY_BEARER_TOKEN_LEN
        )
    }
}

impl std::error::Error for ProxyBearerTokenError {}

/// One-hop holder-routing request. The target validates `bearer` itself,
/// rebuilds the auth context, and serves only when it is a current holder of the
/// call's subject shard and `hop == 0`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HolderProxyRequest {
    pub realm_id: RealmId,
    pub bearer: Option<ProxyBearerToken>,
    pub hop: u8,
    pub call: ProxiedCall,
}

/// A user-authorized operation forwarded to a holder. The domain enums other
/// than [`GroupCall`] and [`UserCall`] are skeletons filled in by later commits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProxiedCall {
    Group(GroupCall),
    User(UserCall),
    Metadata(MetadataCall),
    Notification(NotificationCall),
}

impl ProxiedCall {
    /// Whether this call mutates holder state. The single write-vs-read
    /// classification the dispatch loop keys its failover policy on: a mutation
    /// must not be retried on another holder once the request may have reached
    /// the first, or two holders could apply the same write.
    pub fn is_mutation(&self) -> bool {
        match self {
            ProxiedCall::Group(call) => !matches!(call, GroupCall::Get { .. }),
            ProxiedCall::User(call) => match call {
                UserCall::Get { .. } | UserCall::ReadDocument { .. } => false,
                UserCall::Update { .. } | UserCall::EnsureCanonicalTokenSubject { .. } => true,
            },
            ProxiedCall::Metadata(_) => true,
            ProxiedCall::Notification(call) => match call {
                NotificationCall::List { .. }
                | NotificationCall::UnreadCount { .. }
                | NotificationCall::ListWatches { .. } => false,
                NotificationCall::MarkRead { .. }
                | NotificationCall::CreateWatch { .. }
                | NotificationCall::DeleteWatch { .. } => true,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupCall {
    Get {
        group_id: GroupId,
    },
    AddMember {
        group_id: GroupId,
        user_id: UserId,
        role_ids: HashSet<RoleId>,
    },
    RemoveMember {
        group_id: GroupId,
        user_id: UserId,
        role_ids: Option<HashSet<RoleId>>,
    },
    AddRole {
        group_id: GroupId,
        role: Box<Role>,
    },
    RemoveRole {
        group_id: GroupId,
        role_id: RoleId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserCall {
    Get {
        user_id: String,
    },
    Update {
        user_id: String,
        name: Option<String>,
        set_attributes: HashMap<String, String>,
        remove_attributes: Vec<String>,
    },
    /// Read the caller's own user document; `user_id` steers holder resolution
    /// only — the target reads the identity from the validated bearer.
    ReadDocument {
        user_id: UserId,
    },
    /// Ensure the caller's canonical token subject; `user_id` steers holder
    /// resolution only.
    EnsureCanonicalTokenSubject {
        user_id: UserId,
    },
}

/// Mutating metadata calls route to the document shard holder so a non-holder's
/// write never strands in the outbox. Visibility-checked reads (`GET`, RO-Crate
/// export) and SPARQL fan-out stay on their existing local/`Alpn::Metadata`
/// paths — under the default everywhere placement every node already holds the
/// registry record, so routing a read would be a no-op that only flattened its
/// error classification.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataCall {
    /// Create-with-path: holders resolve from `(group_id, document_path)`, then
    /// the new document lands at `document_id`.
    Create {
        group_id: GroupId,
        document_id: Ulid,
        document_path: String,
        public: bool,
        payload: MetadataCreatePayload,
    },
    Delete {
        document_id: Ulid,
    },
    Update {
        document_id: Ulid,
        /// `None` keeps the record's current visibility.
        public: Option<bool>,
        mutation: MetadataMutation,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataCreatePayload {
    Scaffold {
        name: String,
        description: String,
        date_published: String,
        license: String,
    },
    RoCrate {
        jsonld: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataMutation {
    ReplaceRoCrate { jsonld: String },
    UpsertDataEntity { jsonld: String },
    UpsertContextualEntity { jsonld: String },
}

/// User-facing inbox reads and watch CRUD. The `recipient` steers holder
/// resolution (the inbox's replica-1 holder); the target serves the identity
/// carried by the validated bearer and rejects a wire recipient claiming
/// anyone else as a bad request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotificationCall {
    List {
        recipient: UserId,
        cursor: Option<Vec<u8>>,
        limit: u32,
    },
    UnreadCount {
        recipient: UserId,
    },
    MarkRead {
        recipient: UserId,
        ids: Vec<Ulid>,
        up_to_ms: Option<u64>,
    },
    CreateWatch {
        recipient: UserId,
        path_prefix: String,
        event_mask: WatchEventMask,
    },
    ListWatches {
        recipient: UserId,
    },
    DeleteWatch {
        recipient: UserId,
        watch_id: Ulid,
    },
}

/// Why a holder refused to serve a call. The origin maps each kind onto an HTTP
/// status, so a domain error keeps its meaning across the proxy instead of
/// collapsing every refusal into one status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectKind {
    /// Authenticated but not permitted (→ 403).
    Forbidden,
    /// Conflicts with current state, e.g. the last admin or a per-user cap (→ 409).
    Conflict,
    /// Malformed or failed validation (→ 400).
    BadRequest,
    /// The holder cannot serve right now; the caller may retry (→ 503).
    Unavailable,
    /// A holder-side storage or internal failure (→ 500).
    Internal,
}

/// Holder response. `NotHolder` tells the origin to retry another holder;
/// `NotFound` from a reachable holder is authoritative; `Rejected` carries a
/// typed refusal `kind` plus a human-readable `reason`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HolderProxyResponse {
    Ok(ProxiedReply),
    NotFound,
    NotHolder,
    Rejected { kind: RejectKind, reason: String },
}

impl HolderProxyResponse {
    /// A 403 refusal: the caller is authenticated but not permitted.
    pub fn forbidden(reason: impl Into<String>) -> Self {
        Self::Rejected {
            kind: RejectKind::Forbidden,
            reason: reason.into(),
        }
    }

    /// A 409 refusal: the request conflicts with current state.
    pub fn conflict(reason: impl Into<String>) -> Self {
        Self::Rejected {
            kind: RejectKind::Conflict,
            reason: reason.into(),
        }
    }

    /// A 400 refusal: the request is malformed or fails validation.
    pub fn bad_request(reason: impl Into<String>) -> Self {
        Self::Rejected {
            kind: RejectKind::BadRequest,
            reason: reason.into(),
        }
    }

    /// A 503 refusal: the holder cannot serve right now.
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self::Rejected {
            kind: RejectKind::Unavailable,
            reason: reason.into(),
        }
    }

    /// A 500 refusal: a holder-side storage or internal failure.
    pub fn internal(reason: impl Into<String>) -> Self {
        Self::Rejected {
            kind: RejectKind::Internal,
            reason: reason.into(),
        }
    }
}

/// Typed reply payload, grown one domain per commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProxiedReply {
    Group(Box<GroupReply>),
    User(Box<UserReply>),
    Metadata(Box<MetadataReply>),
    Notification(Box<NotificationReply>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotificationReply {
    List {
        records: Vec<NotificationRecord>,
        next_cursor: Option<Vec<u8>>,
    },
    UnreadCount {
        count: u32,
        capped: bool,
    },
    MarkRead {
        marked: u32,
    },
    Watch(WatchSubscription),
    Watches(Vec<WatchSubscription>),
    Ack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataReply {
    Record(MetadataRegistryRecord),
    Ack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupReply {
    Document {
        group: Group,
        authorization: GroupAuthorizationDocument,
    },
    /// The group's authorization document after a membership or role mutation.
    Authorization(GroupAuthorizationDocument),
    /// A mutation that returns no body to the caller.
    Ack,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserReply {
    User(User),
    TokenSubjectEnsured,
}

pub async fn write_holder_proxy_request(
    stream: &mut BiStream,
    message: &HolderProxyRequest,
) -> Result<(), String> {
    write_frame(stream, message).await
}

pub async fn read_holder_proxy_request(
    stream: &mut BiStream,
) -> Result<HolderProxyRequest, String> {
    read_frame(stream).await
}

pub async fn write_holder_proxy_response(
    stream: &mut BiStream,
    message: &HolderProxyResponse,
) -> Result<(), String> {
    write_frame(stream, message).await
}

pub async fn read_holder_proxy_response(
    stream: &mut BiStream,
) -> Result<HolderProxyResponse, String> {
    read_frame(stream).await
}

async fn write_frame<T: Serialize>(stream: &mut BiStream, message: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(message).map_err(|err| err.to_string())?;
    if bytes.len() > HOLDER_PROXY_MAX_MESSAGE_SIZE {
        return Err("holder proxy message exceeds maximum size".to_string());
    }
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
}

async fn read_frame<T: DeserializeOwned>(stream: &mut BiStream) -> Result<T, String> {
    let mut len_buf = [0u8; 4];
    stream
        .1
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > HOLDER_PROXY_MAX_MESSAGE_SIZE {
        return Err("holder proxy frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; len];
    stream
        .1
        .read_exact(&mut bytes)
        .await
        .map_err(|err| err.to_string())?;
    postcard::from_bytes(&bytes).map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::RealmId;
    use aruna_core::types::UserId;
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    fn group_fixture() -> (Group, GroupAuthorizationDocument) {
        let realm_id = RealmId::from_bytes([3u8; 32]);
        let group_id = Ulid::from_bytes([5u8; 16]);
        let owner = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id);
        let group = Group {
            display_name: "team".to_string(),
            group_id,
            realm_id,
            roles: HashSet::new(),
            owner,
        };
        let authorization = GroupAuthorizationDocument {
            group_id,
            roles: HashMap::new(),
        };
        (group, authorization)
    }

    #[test]
    fn request_round_trips() {
        let message = HolderProxyRequest {
            realm_id: RealmId::from_bytes([2u8; 32]),
            bearer: Some(ProxyBearerToken::new("token").unwrap()),
            hop: 0,
            call: ProxiedCall::Group(GroupCall::Get {
                group_id: Ulid::from_bytes([9u8; 16]),
            }),
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        assert_eq!(
            postcard::from_bytes::<HolderProxyRequest>(&bytes).unwrap(),
            message
        );
    }

    #[test]
    fn response_round_trips() {
        let (group, authorization) = group_fixture();
        let response =
            HolderProxyResponse::Ok(ProxiedReply::Group(Box::new(GroupReply::Document {
                group,
                authorization,
            })));
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert_eq!(
            postcard::from_bytes::<HolderProxyResponse>(&bytes).unwrap(),
            response
        );

        for control in [
            HolderProxyResponse::NotFound,
            HolderProxyResponse::NotHolder,
            HolderProxyResponse::forbidden("nope"),
            HolderProxyResponse::conflict("last admin"),
            HolderProxyResponse::bad_request("invalid"),
        ] {
            let bytes = postcard::to_allocvec(&control).unwrap();
            assert_eq!(
                postcard::from_bytes::<HolderProxyResponse>(&bytes).unwrap(),
                control
            );
        }
    }

    // The one write-vs-read classification the dispatch failover policy keys on:
    // every wire variant is pinned here so a new call cannot silently default.
    #[test]
    fn proxied_call_mutation_classification() {
        let group_id = Ulid::nil();
        let user_id = UserId::local(Ulid::nil(), RealmId::from_bytes([1u8; 32]));
        let reads = [
            ProxiedCall::Group(GroupCall::Get { group_id }),
            ProxiedCall::User(UserCall::Get {
                user_id: user_id.to_string(),
            }),
            ProxiedCall::User(UserCall::ReadDocument { user_id }),
            ProxiedCall::Notification(NotificationCall::List {
                recipient: user_id,
                cursor: None,
                limit: 10,
            }),
            ProxiedCall::Notification(NotificationCall::UnreadCount { recipient: user_id }),
            ProxiedCall::Notification(NotificationCall::ListWatches { recipient: user_id }),
        ];
        let mutations = [
            ProxiedCall::Group(GroupCall::AddMember {
                group_id,
                user_id,
                role_ids: HashSet::new(),
            }),
            ProxiedCall::Group(GroupCall::RemoveMember {
                group_id,
                user_id,
                role_ids: None,
            }),
            ProxiedCall::Group(GroupCall::AddRole {
                group_id,
                role: Box::new(Role {
                    role_id: Ulid::nil(),
                    name: "role".to_string(),
                    permissions: HashMap::new(),
                    assigned_users: HashSet::new(),
                }),
            }),
            ProxiedCall::Group(GroupCall::RemoveRole {
                group_id,
                role_id: Ulid::nil(),
            }),
            ProxiedCall::User(UserCall::Update {
                user_id: user_id.to_string(),
                name: None,
                set_attributes: HashMap::new(),
                remove_attributes: Vec::new(),
            }),
            ProxiedCall::User(UserCall::EnsureCanonicalTokenSubject { user_id }),
            ProxiedCall::Metadata(MetadataCall::Delete {
                document_id: Ulid::nil(),
            }),
            ProxiedCall::Metadata(MetadataCall::Update {
                document_id: Ulid::nil(),
                public: None,
                mutation: MetadataMutation::ReplaceRoCrate {
                    jsonld: String::new(),
                },
            }),
            ProxiedCall::Metadata(MetadataCall::Create {
                group_id,
                document_id: Ulid::nil(),
                document_path: String::new(),
                public: false,
                payload: MetadataCreatePayload::RoCrate {
                    jsonld: String::new(),
                },
            }),
            ProxiedCall::Notification(NotificationCall::MarkRead {
                recipient: user_id,
                ids: Vec::new(),
                up_to_ms: None,
            }),
            ProxiedCall::Notification(NotificationCall::CreateWatch {
                recipient: user_id,
                path_prefix: String::new(),
                event_mask: WatchEventMask::default(),
            }),
            ProxiedCall::Notification(NotificationCall::DeleteWatch {
                recipient: user_id,
                watch_id: Ulid::nil(),
            }),
        ];
        for call in &reads {
            assert!(!call.is_mutation(), "misclassified as mutation: {call:?}");
        }
        for call in &mutations {
            assert!(call.is_mutation(), "misclassified as read: {call:?}");
        }
    }

    #[test]
    fn bearer_token_rejects_oversized() {
        let oversized = "x".repeat(MAX_PROXY_BEARER_TOKEN_LEN + 1);
        assert!(ProxyBearerToken::new(oversized.clone()).is_err());

        // A hostile frame carrying the oversized token must fail to decode.
        let raw = postcard::to_allocvec(&oversized).unwrap();
        assert!(postcard::from_bytes::<ProxyBearerToken>(&raw).is_err());
    }
}
