use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::RealmId;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use ulid::Ulid;

pub const ARUNA_DATA_PREFIX: &str = "https://w3id.org/aruna/data/";

const ARN_KEY_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArunaArn {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub resource_type: ArunaArnType,
    pub path: String,
}

impl ArunaArn {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        resource_type: ArunaArnType,
        path: impl Into<String>,
    ) -> Result<Self, ConversionError> {
        let path = path.into();
        if path.is_empty() {
            return Err(ConversionError::FromStrError(
                "ARN path must not be empty".to_string(),
            ));
        }

        Ok(Self {
            realm_id,
            node_id,
            resource_type,
            path,
        })
    }

    pub fn s3_bucket(
        realm_id: RealmId,
        node_id: NodeId,
        bucket: impl Into<String>,
    ) -> Result<Self, ConversionError> {
        Self::new(realm_id, node_id, ArunaArnType::S3, bucket)
    }

    pub fn s3_object_prefix(
        realm_id: RealmId,
        node_id: NodeId,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, ConversionError> {
        Self::new(
            realm_id,
            node_id,
            ArunaArnType::S3,
            format!("{}/{}", bucket.into(), prefix.into()),
        )
    }

    pub fn bucket(&self) -> Option<&str> {
        if self.resource_type != ArunaArnType::S3 {
            return None;
        }
        Some(
            self.path
                .split_once('/')
                .map_or(self.path.as_str(), |(bucket, _)| bucket),
        )
    }

    pub fn key_prefix(&self) -> Option<&str> {
        if self.resource_type != ArunaArnType::S3 {
            return None;
        }
        self.path.split_once('/').map(|(_, prefix)| prefix)
    }

    pub fn parse(input: &str) -> Result<Self, ConversionError> {
        let prefix = "arn:aruna:";
        let remainder = input.strip_prefix(prefix).ok_or_else(|| {
            ConversionError::FromStrError("ARN must start with `arn:aruna:`".to_string())
        })?;

        let (realm_id, resource_remainder) = remainder
            .split_once(':')
            .ok_or_else(|| ConversionError::FromStrError("ARN missing node id".to_string()))?;
        let realm_id = RealmId::from_base64(realm_id)?;

        let (node_id, resource) = resource_remainder.split_once(':').ok_or_else(|| {
            ConversionError::FromStrError("ARN missing resource path".to_string())
        })?;

        let node_id = NodeId::from_str(node_id)
            .map_err(|err| ConversionError::FromStrError(err.to_string()))?;

        let mut resource_parts = resource.splitn(2, '/');
        let resource_type = resource_parts
            .next()
            .ok_or_else(|| ConversionError::FromStrError("ARN missing resource type".to_string()))
            .and_then(ArunaArnType::parse)?;
        let path = resource_parts
            .next()
            .ok_or_else(|| ConversionError::FromStrError("ARN missing path".to_string()))?
            .to_string();

        if path.is_empty() {
            return Err(ConversionError::FromStrError(
                "ARN path must not be empty".to_string(),
            ));
        }

        Self::new(realm_id, node_id, resource_type, path)
    }
}

impl fmt::Display for ArunaArn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "arn:aruna:{}:{}:{}/{}",
            self.realm_id, self.node_id, self.resource_type, self.path
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionedObjectArn {
    pub realm_id: RealmId,
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version: Ulid,
}

impl VersionedObjectArn {
    pub fn new(
        realm_id: RealmId,
        node_id: NodeId,
        bucket: impl Into<String>,
        key: impl Into<String>,
        version: Ulid,
    ) -> Result<Self, ConversionError> {
        let bucket = bucket.into();
        let key = key.into();
        if bucket.is_empty() || bucket.contains(['/', '@']) {
            return Err(ConversionError::FromStrError(
                "versioned ARN bucket is invalid".to_string(),
            ));
        }
        if key.is_empty() {
            return Err(ConversionError::FromStrError(
                "versioned ARN key must not be empty".to_string(),
            ));
        }
        Ok(Self {
            realm_id,
            node_id,
            bucket,
            key,
            version,
        })
    }

    pub fn parse(input: &str) -> Result<Self, ConversionError> {
        let arn = ArunaArn::parse(input)?;
        if arn.resource_type != ArunaArnType::S3 {
            return Err(ConversionError::FromStrError(
                "versioned object ARN must use the s3 resource type".to_string(),
            ));
        }
        let (bucket, object) = arn.path.split_once('/').ok_or_else(|| {
            ConversionError::FromStrError("versioned object ARN is missing a key".to_string())
        })?;
        let (encoded_key, version) = object.rsplit_once('@').ok_or_else(|| {
            ConversionError::FromStrError("versioned object ARN is missing a version".to_string())
        })?;
        let key = percent_decode_str(encoded_key)
            .decode_utf8()
            .map_err(|_| {
                ConversionError::FromStrError(
                    "versioned object ARN key is not valid UTF-8".to_string(),
                )
            })?
            .into_owned();
        if encode_object_key(&key) != encoded_key {
            return Err(ConversionError::FromStrError(
                "versioned object ARN key is not canonically encoded".to_string(),
            ));
        }
        if version.len() != 26
            || !version
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z'))
        {
            return Err(ConversionError::FromStrError(
                "versioned object ARN has an invalid ULID".to_string(),
            ));
        }
        let version = Ulid::from_string(version)?;
        Self::new(arn.realm_id, arn.node_id, bucket, key, version)
    }

    pub fn to_w3id(&self) -> String {
        format!("{ARUNA_DATA_PREFIX}{self}")
    }
}

impl FromStr for VersionedObjectArn {
    type Err = ConversionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl fmt::Display for VersionedObjectArn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "arn:aruna:{}:{}:s3/{}/{}@{}",
            self.realm_id,
            self.node_id,
            self.bucket,
            encode_object_key(&self.key),
            self.version
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum W3idDataIdentifier {
    ContentHash([u8; 32]),
    VersionedObject(VersionedObjectArn),
}

impl W3idDataIdentifier {
    pub fn parse(input: &str) -> Result<Self, ConversionError> {
        let suffix = input.strip_prefix(ARUNA_DATA_PREFIX).ok_or_else(|| {
            ConversionError::FromStrError("identifier is not an Aruna data W3ID".to_string())
        })?;
        if suffix.len() == 64
            && suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            let mut hash = [0; 32];
            hex::decode_to_slice(suffix, &mut hash)
                .map_err(|error| ConversionError::FromStrError(error.to_string()))?;
            return Ok(Self::ContentHash(hash));
        }
        if suffix.starts_with("arn:") {
            return Ok(Self::VersionedObject(VersionedObjectArn::parse(suffix)?));
        }
        Err(ConversionError::FromStrError(
            "Aruna data W3ID has an unsupported suffix".to_string(),
        ))
    }

    pub fn to_w3id(&self) -> String {
        match self {
            Self::ContentHash(hash) => format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash)),
            Self::VersionedObject(arn) => arn.to_w3id(),
        }
    }
}

fn encode_object_key(key: &str) -> String {
    utf8_percent_encode(key, ARN_KEY_ENCODE_SET).to_string()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ArunaArnType {
    S3,
    ContentHash,
}

impl ArunaArnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::ContentHash => "ch",
        }
    }

    fn parse(input: &str) -> Result<Self, ConversionError> {
        match input {
            "s3" => Ok(Self::S3),
            "ch" => Ok(Self::ContentHash),
            other => Err(ConversionError::FromStrError(format!(
                "Unsupported ARN resource type `{other}`"
            ))),
        }
    }
}

impl fmt::Display for ArunaArnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketReplicationConfig {
    pub targets: Vec<BucketReplicationTarget>,
}

impl BucketReplicationConfig {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BucketReplicationTarget {
    pub node_id: NodeId,
    pub realm_id: RealmId,
    pub bucket: String,
    pub arn: String,
    pub replicate_delete_markers: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationItemKind {
    Materialized,
    DeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationNegotiationResult {
    AlreadyReplicatedVersion,
    NeedVersionOnly,
    NeedBlobAndVersion,
    Rejected(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationSuboperationResult {
    Replicated,
    Skipped,
    ReplicatedBytes(u64),
}

#[cfg(test)]
mod tests {
    use super::{
        ARUNA_DATA_PREFIX, ArunaArn, ArunaArnType, VersionedObjectArn, W3idDataIdentifier,
    };
    use crate::errors::ConversionError;
    use crate::{NodeId, structs::RealmId};
    use proptest::prelude::*;
    use std::str::FromStr;
    use ulid::Ulid;

    fn test_node_id() -> NodeId {
        NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
            .unwrap()
    }

    #[test]
    fn formats_s3_arn_canonically() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let arn = ArunaArn::s3_bucket(realm_id, node_id, "mybucket").unwrap();

        assert_eq!(
            arn.to_string(),
            format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket")
        );
    }

    #[test]
    fn parses_canonical_s3_arn() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let arn = format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket");

        let parsed = ArunaArn::parse(&arn).unwrap();
        assert_eq!(parsed.realm_id, realm_id);
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.resource_type, ArunaArnType::S3);
        assert_eq!(parsed.path, "mybucket");
        assert_eq!(parsed.to_string(), arn);
    }

    #[test]
    fn roundtrips_s3_prefix() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let arn =
            ArunaArn::s3_object_prefix(realm_id, node_id, "mybucket", "nested/object-prefix/")
                .unwrap();

        let canonical = format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket/nested/object-prefix/");
        assert_eq!(arn.to_string(), canonical);

        let parsed = ArunaArn::parse(&canonical).unwrap();
        assert_eq!(parsed, arn);
        assert_eq!(parsed.bucket(), Some("mybucket"));
        assert_eq!(parsed.key_prefix(), Some("nested/object-prefix/"));
    }

    #[test]
    fn reads_s3_parts() {
        let arn = ArunaArn::s3_bucket(RealmId::from_bytes([1u8; 32]), test_node_id(), "mybucket")
            .unwrap();

        assert_eq!(arn.bucket(), Some("mybucket"));
        assert_eq!(arn.key_prefix(), None);
    }

    #[test]
    fn rejects_invalid_prefix() {
        let err = ArunaArn::parse("invalid").unwrap_err();
        assert_eq!(
            err,
            ConversionError::FromStrError("ARN must start with `arn:aruna:`".to_string())
        );
    }

    #[test]
    fn formats_versioned_arn() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let version = Ulid::from_bytes([2u8; 16]);
        let arn =
            VersionedObjectArn::new(realm_id, node_id, "bucket", "dir/a @ %.txt", version).unwrap();

        assert_eq!(
            arn.to_string(),
            format!("arn:aruna:{realm_id}:{node_id}:s3/bucket/dir/a%20%40%20%25.txt@{version}")
        );
        assert_eq!(VersionedObjectArn::parse(&arn.to_string()).unwrap(), arn);
    }

    #[test]
    fn classifies_w3id() {
        let hash = [3u8; 32];
        let hash_url = format!("{ARUNA_DATA_PREFIX}{}", hex::encode(hash));
        assert_eq!(
            W3idDataIdentifier::parse(&hash_url).unwrap(),
            W3idDataIdentifier::ContentHash(hash)
        );

        let arn = VersionedObjectArn::new(
            RealmId::from_bytes([1u8; 32]),
            test_node_id(),
            "bucket",
            "key",
            Ulid::from_bytes([2u8; 16]),
        )
        .unwrap();
        assert_eq!(
            W3idDataIdentifier::parse(&arn.to_w3id()).unwrap(),
            W3idDataIdentifier::VersionedObject(arn)
        );
    }

    #[test]
    fn rejects_w3id_suffix() {
        assert!(W3idDataIdentifier::parse(&format!("{ARUNA_DATA_PREFIX}ABC")).is_err());
        assert!(
            W3idDataIdentifier::parse(&format!("{ARUNA_DATA_PREFIX}{}", "A".repeat(64))).is_err()
        );
    }

    #[test]
    fn rejects_uncanonical_key() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let version = Ulid::from_bytes([2u8; 16]);
        assert!(
            VersionedObjectArn::parse(&format!(
                "arn:aruna:{realm_id}:{node_id}:s3/bucket/raw@key@{version}"
            ))
            .is_err()
        );
        assert!(
            VersionedObjectArn::parse(&format!(
                "arn:aruna:{realm_id}:{node_id}:s3/bucket/%6bey@{version}"
            ))
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn versioned_arn_roundtrips(
            chars in proptest::collection::vec(any::<char>(), 1..64),
            version in any::<[u8; 16]>(),
        ) {
            let key: String = chars.into_iter().collect();
            let arn = VersionedObjectArn::new(
                RealmId::from_bytes([1u8; 32]),
                test_node_id(),
                "bucket",
                key,
                Ulid::from_bytes(version),
            )
            .unwrap();

            prop_assert_eq!(VersionedObjectArn::parse(&arn.to_string()).unwrap(), arn);
        }
    }
}
