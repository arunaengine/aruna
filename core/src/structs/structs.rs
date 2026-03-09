use crate::errors::{BlobError, ConversionError};
use crate::structs::realm::RealmId;
use crate::types::autosurgeon_ulid;
use crate::types::{RoleId, UserId};
use crate::NodeId;
use autosurgeon::{Hydrate, Reconcile};
use byteview::ByteView;
use core::fmt;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::SystemTime;
use ulid::Ulid;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub enum Permission {
    READ,
    WRITE,
    DENY,
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Permission::READ => "Read",
                Permission::WRITE => "Write",
                Permission::DENY => "Deny",
            }
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hydrate, Reconcile)]
pub struct Role {
    #[autosurgeon(with = "autosurgeon_ulid")]
    pub role_id: RoleId,
    pub name: String,
    pub permissions: HashMap<String, Permission>,
    #[autosurgeon(with = "autosurgeon_ulid_set")]
    pub assigned_users: HashSet<UserId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Subject: user identity in format `{user_ulid}@{realm_pubkey_base64}`.
    pub sub: String,
    /// Issuer: realm public key (base64-encoded).
    pub iss: String,
    /// Issued at: Unix timestamp in seconds.
    pub iat: u64,
    /// Expiration: Unix timestamp in seconds.
    pub exp: u64,
    /// JWT ID: unique token identifier (ULID string).
    pub jti: String,
    /// Path restrictions: List of (path_pattern, permission) pairs acting as a whitelist
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restrictions: Option<Vec<PathRestriction>>,
    /// Issuer public key (base64-encoded).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_pubkey: Option<String>,
    /// Delegation signature: Realm signature over issuer_pubkey
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation_signature: Option<String>,
}

/// Path restriction for token scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PathRestriction {
    /// Path pattern (supports * and ** wildcards).
    pub pattern: String,
    /// Permission level for this pattern.
    pub permission: Permission,
}

#[derive(Clone, Debug, PartialEq)]
pub enum NodeCapabilities {
    // This may seem redundant, but because we need both representations both
    // and need both on (nearly) every request, this mitigates unneeded conversions
    Management {
        /// ed25519-dalek representation
        realm_signing_key: SigningKey,
        /// pkcs8_pem bytes representation
        realm_verifying_key: [u8; 113],
        realm_encoding_key: [u8; 168],
    },
    Server {
        /// ed25519-dalek representation
        issuer_signing_key: SigningKey,
        /// pkcs8_pem bytes representation
        issuer_verifying_key: [u8; 113],
        issuer_encoding_key: [u8; 168],

        /// pkcs8_pem bytes representation
        realm_verifying_key: [u8; 113],
        /// Realm signature over issuer_pubkey
        delegation_signature: String,
    },
    Local {
        //realm_pubkey: [u8; 32],
        realm_verifying_key: [u8; 113],
    },
}

impl NodeCapabilities {
    pub fn management_node(realm_signing_key: SigningKey) -> Result<Self, ConversionError> {
        let privkey = realm_signing_key.to_pkcs8_pem(LineEnding::default())?;
        let pubkey = realm_signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::default())?;
        let realm_verifying_key = pubkey.as_bytes().try_into()?;
        let realm_encoding_key = privkey.as_bytes().try_into()?;
        Ok(NodeCapabilities::Management {
            realm_signing_key,
            realm_verifying_key,
            realm_encoding_key,
        })
    }
    pub fn server_node(
        issuer_signing_key: SigningKey,
        realm_id: RealmId,
        delegation_signature: String,
    ) -> Result<Self, ConversionError> {
        let privkey = issuer_signing_key.to_pkcs8_pem(LineEnding::default())?;
        let pubkey = issuer_signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::default())?;
        let issuer_verifying_key = pubkey.as_bytes().try_into()?;
        let issuer_encoding_key = privkey.as_bytes().try_into()?;
        let realm_verifying_key = realm_id.to_pkcs8_pem_bytes()?;
        Ok(NodeCapabilities::Server {
            issuer_signing_key,
            issuer_verifying_key,
            issuer_encoding_key,
            delegation_signature,
            realm_verifying_key,
        })
    }
    pub fn local_node(realm_id: RealmId) -> Result<Self, ConversionError> {
        let realm_verifying_key = realm_id.to_pkcs8_pem_bytes()?;
        Ok(NodeCapabilities::Local {
            realm_verifying_key,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AuthContext {
    pub user_id: UserId,
    pub realm_id: RealmId,
    pub path_restrictions: Option<Vec<PathRestriction>>,
}

impl TryFrom<TokenClaims> for AuthContext {
    type Error = ConversionError;

    fn try_from(value: TokenClaims) -> Result<Self, Self::Error> {
        let (user, realm) = value
            .sub
            .split_once('@')
            .ok_or_else(|| ConversionError::InvalidUserId)?;
        let user_id = Ulid::from_string(&user)?;
        let realm_id = RealmId::from_base64(realm)?;
        let path_restrictions = value.restrictions;

        Ok(Self {
            user_id,
            realm_id,
            path_restrictions,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Actor {
    pub node_id: NodeId,
    pub user_id: UserId,
    pub realm_id: RealmId,
}

impl TryFrom<&[u8]> for Actor {
    type Error = ConversionError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(postcard::from_bytes(value)?)
    }
}

impl TryFrom<&Actor> for Vec<u8> {
    type Error = ConversionError;

    fn try_from(value: &Actor) -> Result<Self, Self::Error> {
        Ok(postcard::to_allocvec(value)?)
    }
}

pub mod autosurgeon_ulid_set {
    use std::collections::{HashMap, HashSet};

    use autosurgeon::reconcile::MapReconciler;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use ulid::Ulid;

    use crate::types::UserId;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<HashSet<UserId>, HydrateError> {
        let inner: HashMap<String, String> = HashMap::hydrate(doc, obj, prop)?;
        let role_set = inner
            .iter()
            .map(|(k, _)| Ulid::from_string(&k))
            .collect::<Result<HashSet<UserId>, ulid::DecodeError>>()
            .map_err(|e| {
                HydrateError::unexpected("valid Ulid string", format!("Invalid Ulid {}", e))
            })?;
        Ok(role_set)
    }
    pub fn reconcile<R: Reconciler>(
        ulid: &HashSet<UserId>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        let mut map = reconciler.map()?;
        for id in ulid.iter().map(|k| k.to_string()) {
            map.put(&id, "")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Backend {
    #[default]
    S3,
    HTTP,
    Postgres,
    FileSystem,
}

impl FromStr for Backend {
    type Err = ConversionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "s3" => Ok(Backend::S3),
            "http" => Ok(Backend::HTTP),
            "postgres" => Ok(Backend::Postgres),
            "filesystem" => Ok(Backend::FileSystem),
            _ => Err(ConversionError::FromStrError(format!(
                "unknown backend {}",
                s
            ))),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Backend::S3 => write!(f, "s3"),
            Backend::HTTP => write!(f, "http"),
            Backend::Postgres => write!(f, "postgres"),
            Backend::FileSystem => write!(f, "filesystem"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BackendConfig {
    pub backend_type: Backend,
    pub root: String,
    pub service_config: HashMap<String, String>,
    pub bucket_prefix: Option<String>,
    pub max_bucket_size: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct BackendBucket {
    pub name: String,
    pub load: u64,
}

impl TryFrom<(ByteView, ByteView)> for BackendBucket {
    type Error = ConversionError;

    fn try_from(value: (ByteView, ByteView)) -> Result<Self, Self::Error> {
        let (bucket, load) = value;

        Ok(BackendBucket {
            name: String::from_utf8(bucket.to_vec())?,
            load: u64::from_le_bytes(load.as_ref().try_into()?),
        })
    }
}

impl From<(String, u64)> for BackendBucket {
    fn from((name, size): (String, u64)) -> Self {
        Self { name, load: size }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BackendLocation {
    pub root: String,
    pub storage_bucket: String,
    pub object_bucket: String, // S3 bucket from request
    pub object_key: String,    // S3 key from request
    pub ulid: Ulid,
    pub compressed: bool,
    pub encrypted: bool,
}

impl Display for BackendLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid));
        write!(
            f,
            "{}",
            path.into_os_string()
                .into_string()
                .map_err(|_| fmt::Error)?
        )
    }
}

impl BackendLocation {
    pub fn get_full_path(&self) -> Result<String, ConversionError> {
        PathBuf::from(&self.root)
            .join(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid))
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)
    }

    pub fn get_storage_path(&self) -> Result<String, BlobError> {
        Ok(PathBuf::from(&self.storage_bucket)
            .join(&self.object_bucket)
            .join(format!("{}_{}", self.object_key, self.ulid))
            .into_os_string()
            .into_string()
            .map_err(|_| ConversionError::OsStringError)?)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlobInfo {
    pub location: BackendLocation,
    pub created_by: UserId,
    pub created_at: SystemTime,
    pub staging: bool,
    pub partial: bool,
    pub blob_size: u64,
    pub hashes: HashMap<String, Vec<u8>>, // Raw bytes that can be encoded as needed
}

impl BlobInfo {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn get_location(&self) -> &BackendLocation {
        &self.location
    }

    pub fn get_blake3(&self) -> Option<&Vec<u8>> {
        self.hashes.get("blake3")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserIdentity {
    pub user_id: UserId,
    pub realm_key: RealmId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAccess {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub secret: String,
    //pub expiry: SystemTime
    //filter: todo!()
}

impl UserAccess {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(&self)?)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[cfg(test)]
mod test {
    use crate::structs::{Permission, RealmId, Role};
    use autosurgeon::{hydrate, reconcile};
    use std::collections::{HashMap, HashSet};
    use ulid::Ulid;

    #[test]
    pub fn test_role_conversion() {
        let role = Role {
            role_id: Ulid::new(),
            name: "admin".to_string(),
            permissions: HashMap::from([(
                format!(
                    "/{}/g/{}/**",
                    RealmId([0u8; 32]).to_string(),
                    Ulid::new().to_string()
                ),
                Permission::WRITE,
            )]),
            assigned_users: HashSet::from([Ulid::new()]),
        };

        let mut automerge_doc = automerge::AutoCommit::new();
        reconcile(&mut automerge_doc, &role).unwrap();

        let bytes = automerge_doc.save();

        let stored_automerge_doc = automerge::AutoCommit::load(&bytes).unwrap();
        let hydrated_role: Role = hydrate(&stored_automerge_doc).unwrap();

        assert_eq!(role, hydrated_role);
    }
}
