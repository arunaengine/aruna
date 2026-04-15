use crate::errors::ConversionError;
use crate::id::NodeId;
use crate::structs::RealmId;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

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

    pub fn parse(input: &str) -> Result<Self, ConversionError> {
        let prefix = "arn:aruna:";
        let remainder = input.strip_prefix(prefix).ok_or_else(|| {
            ConversionError::FromStrError("ARN must start with `arn:aruna:`".to_string())
        })?;

        let (realm_id, resource_remainder) = remainder
            .split_once(':')
            .ok_or_else(|| ConversionError::FromStrError("ARN missing node id".to_string()))?;
        let realm_id = RealmId::from_base64(realm_id)?;

        let (node_id, resource) =
            if let Some((node_id, resource)) = resource_remainder.split_once(':') {
                (node_id, resource)
            } else if let Some((node_id, resource)) = resource_remainder.split_once('/') {
                (node_id, resource)
            } else {
                return Err(ConversionError::FromStrError(
                    "ARN missing resource path".to_string(),
                ));
            };

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
}

#[cfg(test)]
mod tests {
    use super::{ArunaArn, ArunaArnType};
    use crate::errors::ConversionError;
    use crate::{NodeId, structs::RealmId};
    use std::str::FromStr;

    fn test_node_id() -> NodeId {
        NodeId::from_str("ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6")
            .unwrap()
    }

    #[test]
    fn formats_s3_arn_canonically() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let arn = ArunaArn::s3_bucket(realm_id.clone(), node_id, "mybucket").unwrap();

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
    fn parses_legacy_s3_arn_and_canonicalizes_output() {
        let realm_id = RealmId::from_bytes([1u8; 32]);
        let node_id = test_node_id();
        let legacy = format!("arn:aruna:{realm_id}:{node_id}/s3/mybucket");

        let parsed = ArunaArn::parse(&legacy).unwrap();
        assert_eq!(parsed.realm_id, realm_id);
        assert_eq!(parsed.node_id, node_id);
        assert_eq!(parsed.resource_type, ArunaArnType::S3);
        assert_eq!(parsed.path, "mybucket");
        assert_eq!(
            parsed.to_string(),
            format!("arn:aruna:{realm_id}:{node_id}:s3/mybucket")
        );
    }

    #[test]
    fn rejects_invalid_prefix() {
        let err = ArunaArn::parse("invalid").unwrap_err();
        assert_eq!(
            err,
            ConversionError::FromStrError("ARN must start with `arn:aruna:`".to_string())
        );
    }
}
