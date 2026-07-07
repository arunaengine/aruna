use aruna_core::structs::{Group, GroupAuthorizationDocument, RealmId, Role, User};
use aruna_core::types::{GroupId, RoleId, UserId};
use aruna_net::streams::BiStream;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt;
use tokio::io::AsyncWriteExt;

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetadataCall {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NotificationCall {}

/// Holder response. `NotHolder` tells the origin to retry another holder;
/// `NotFound` from a reachable holder is authoritative; `Rejected` carries an
/// authorization or validation failure reason.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HolderProxyResponse {
    Ok(ProxiedReply),
    NotFound,
    NotHolder,
    Rejected(String),
}

/// Typed reply payload, grown one domain per commit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProxiedReply {
    Group(Box<GroupReply>),
    User(Box<UserReply>),
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
            HolderProxyResponse::Rejected("nope".to_string()),
        ] {
            let bytes = postcard::to_allocvec(&control).unwrap();
            assert_eq!(
                postcard::from_bytes::<HolderProxyResponse>(&bytes).unwrap(),
                control
            );
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
