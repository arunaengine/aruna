//! Rewrites dataset entries that name an object of a local distributed member bucket by node
//! ARN or `s3://` location to the object's global id before the dataset is saved.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Entries of ordinary buckets and of other nodes stay as they are.

use std::collections::{BTreeMap, HashMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{DISTRIBUTION_KEYSPACE, VERSION_INDEX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::data::identity::{CONTENT_URL, ObjectLocation, text_values};
use aruna_core::structs::storage::distribution::inventory::{IndexEntry, Position, index_key};
use aruna_core::structs::storage::distribution::{DistributionView, SetHint};
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::replication::{ARUNA_DATA_PREFIX, VersionedObjectArn};
use aruna_core::types::{Key, Value as RowValue};
use serde_json::Value;
use thiserror::Error;
use ulid::Ulid;

use crate::metadata::object_version::{ObjectVersion, ObjectVersionError, ObjectVersionOperation};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// At most this many entities are looked at per save.
const MAX_ENTITIES: usize = 10_000;

/// The local object an identifier value names: bucket, key and version when known.
fn local_object(
    value: &str,
    realm_id: RealmId,
    node_id: NodeId,
) -> Option<(String, String, Option<Ulid>)> {
    let arn = value.strip_prefix(ARUNA_DATA_PREFIX).unwrap_or(value);
    if let Ok(exact) = VersionedObjectArn::parse(arn) {
        return (exact.realm_id == realm_id && exact.node_id == node_id).then_some((
            exact.bucket,
            exact.key,
            Some(exact.version),
        ));
    }
    let location = ObjectLocation::parse(value)?;
    match location.key.split_once("?versionId=") {
        Some((key, version)) => Some((
            location.bucket,
            key.to_string(),
            Some(Ulid::from_string(version).ok()?),
        )),
        None => Some((location.bucket, location.key, None)),
    }
}

/// Why a dataset's entries could not be normalized.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct NormalizeError(String);

impl From<IoError> for NormalizeError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, PartialEq)]
enum NormalizeStage {
    Bucket,
    Version,
}

/// Rewrites every entity of a document (a crate with `@graph`, or one entity) whose `@id` or
/// `contentUrl` names a version of a local distributed member bucket. The output is the
/// document and how many values were rewritten; a value that cannot be resolved stays.
#[derive(Debug, PartialEq)]
pub struct NormalizeEntriesOperation {
    realm_id: RealmId,
    node_id: NodeId,
    document: Value,
    candidates: VecDeque<String>,
    current: Option<(String, String, String, Option<Ulid>)>,
    buckets: HashMap<String, Option<BucketInfo>>,
    rewrites: BTreeMap<String, String>,
    stage: NormalizeStage,
}

impl NormalizeEntriesOperation {
    pub fn new(realm_id: RealmId, node_id: NodeId, document: Value) -> Self {
        let entities: Vec<&Value> = match document.get("@graph").and_then(Value::as_array) {
            Some(graph) => graph.iter().take(MAX_ENTITIES).collect(),
            None => vec![&document],
        };
        let mut candidates = VecDeque::new();
        for entity in entities {
            candidates.extend(
                entity
                    .get("@id")
                    .and_then(Value::as_str)
                    .map(str::to_string),
            );
            candidates.extend(text_values(entity.get(CONTENT_URL)));
        }
        Self {
            realm_id,
            node_id,
            document,
            candidates,
            current: None,
            buckets: HashMap::new(),
            rewrites: BTreeMap::new(),
            stage: NormalizeStage::Bucket,
        }
    }

    /// The next candidate that names a local object of a distributed member bucket.
    fn next_candidate(&mut self) -> Result<Next<(Value, usize)>, NormalizeError> {
        while let Some(value) = self.candidates.pop_front() {
            if self.rewrites.contains_key(&value) {
                continue;
            }
            let Some((bucket, key, version)) = local_object(&value, self.realm_id, self.node_id)
            else {
                continue;
            };
            let known = self.buckets.contains_key(&bucket);
            self.current = Some((value, bucket.clone(), key, version));
            if !known {
                self.stage = NormalizeStage::Bucket;
                return Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))));
            }
            if let Some(call) = self.version_call() {
                return Ok(Next::Call(call));
            }
        }
        let count = self.rewrites.len();
        let mut document = std::mem::take(&mut self.document);
        if count > 0 {
            rewrite(&mut document, &self.rewrites, false);
        }
        Ok(Next::Done((document, count)))
    }

    /// The version read of the current candidate, when its bucket is a distributed member.
    fn version_call(&mut self) -> Option<IoCall> {
        let (_, bucket, key, version) = self.current.as_ref()?;
        let bound = (self.buckets.get(bucket))
            .and_then(Option::as_ref)
            .is_some_and(|info| info.distribution.is_some());
        if !bound {
            return None;
        }
        self.stage = NormalizeStage::Version;
        Some(IoCall::child(ObjectVersionOperation::new(
            bucket, key, *version,
        )))
    }
}

impl Operation for NormalizeEntriesOperation {
    type Output = (Value, usize);
    type Error = NormalizeError;

    fn first(&mut self) -> Result<Next<(Value, usize)>, NormalizeError> {
        self.next_candidate()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<(Value, usize)>, NormalizeError> {
        match self.stage {
            NormalizeStage::Bucket => {
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => Some(info),
                    Err(GetBucketError::NotFound) => None,
                    Err(error) => return Err(NormalizeError(error.to_string())),
                };
                let bucket = (self.current.as_ref()).map(|(_, bucket, _, _)| bucket.clone());
                self.buckets.insert(bucket.unwrap_or_default(), info);
                match self.version_call() {
                    Some(call) => Ok(Next::Call(call)),
                    None => self.next_candidate(),
                }
            }
            NormalizeStage::Version => {
                let object = result.child::<ObjectVersion, ObjectVersionError>()?;
                let object = object.map_err(|error| NormalizeError(error.to_string()))?;
                let value = self.current.take().map(|(value, ..)| value);
                if let (Some(value), Some(version_id)) = (value, object.version_id)
                    && object.version.is_some_and(|version| !version.is_deleted())
                {
                    let global = GlobalId::Version {
                        realm_id: self.realm_id,
                        version_id,
                    };
                    self.rewrites.insert(value, global.to_url());
                }
                self.next_candidate()
            }
        }
    }
}

/// Replaces every identifier equal to a rewritten value: ids, references and `contentUrl`
/// values. Free text that happens to equal one keeps its wording.
fn rewrite(value: &mut Value, rewrites: &BTreeMap<String, String>, identifier: bool) {
    match value {
        Value::String(text) if identifier => {
            if let Some(replacement) = rewrites.get(text.as_str()) {
                *text = replacement.clone();
            }
        }
        Value::Array(items) => items
            .iter_mut()
            .for_each(|item| rewrite(item, rewrites, identifier)),
        Value::Object(fields) => {
            for (name, field) in fields.iter_mut() {
                rewrite(field, rewrites, name == "@id" || name == CONTENT_URL);
            }
        }
        _ => {}
    }
}

/// At most this many distribution records are read to find a version's candidate sets.
const MAX_SETS: usize = 4096;

#[derive(Debug, Error, PartialEq)]
pub enum LocalVersionError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Failed(String),
}

#[derive(Debug, PartialEq)]
enum VersionStep {
    Page,
    Bucket(Ulid, String),
    Entry(String),
}

/// The local member bucket and key of a distributed version: a set whose hint the id carries,
/// whose local member bucket is bound here, and whose version id index holds it. `None` on a
/// node that holds no member of such a set.
#[derive(Debug, PartialEq)]
pub struct LocalVersionOperation {
    version_id: Ulid,
    rows: VecDeque<RowValue>,
    after: Option<Key>,
    seen: usize,
    step: VersionStep,
}

type VersionTurn = Result<Next<Option<(String, String)>>, LocalVersionError>;

impl LocalVersionOperation {
    pub fn new(version_id: Ulid) -> Self {
        Self {
            version_id,
            rows: VecDeque::new(),
            after: None,
            seen: 0,
            step: VersionStep::Page,
        }
    }

    fn page(&mut self) -> VersionTurn {
        self.step = VersionStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: DISTRIBUTION_KEYSPACE.to_string(),
            prefix: Key::from(Vec::<u8>::new()),
            after: self.after.take(),
            limit: 256,
        }))
    }

    /// The next record of a set the id's hint names, or the next page while sets remain.
    fn advance(&mut self) -> VersionTurn {
        let hint = SetHint::of_version(self.version_id);
        while let Some(value) = self.rows.pop_front() {
            self.seen += 1;
            let Ok(view) = DistributionView::from_bytes(&value) else {
                continue;
            };
            let snapshot = view.snapshot();
            let set_id = snapshot.set_id();
            if SetHint::of_set(set_id) != hint {
                continue;
            }
            let bucket = snapshot.config.bucket.to_string();
            self.step = VersionStep::Bucket(set_id, bucket.clone());
            return Ok(Next::Call(IoCall::child(GetBucketOperation::new(bucket))));
        }
        match self.after.is_some() && self.seen < MAX_SETS {
            true => self.page(),
            false => Ok(Next::Done(None)),
        }
    }
}

impl Operation for LocalVersionOperation {
    type Output = Option<(String, String)>;
    type Error = LocalVersionError;

    fn first(&mut self) -> VersionTurn {
        self.page()
    }

    fn next(&mut self, result: IoResult) -> VersionTurn {
        match std::mem::replace(&mut self.step, VersionStep::Page) {
            VersionStep::Page => {
                let (rows, next) = result.page()?;
                self.rows = rows.into_iter().map(|(_, value)| value).collect();
                self.after = next;
                self.advance()
            }
            VersionStep::Bucket(set_id, bucket) => {
                let info = match result.child::<BucketInfo, GetBucketError>()? {
                    Ok(info) => info,
                    Err(GetBucketError::NotFound) => return self.advance(),
                    Err(error) => return Err(LocalVersionError::Failed(error.to_string())),
                };
                if info.distribution.as_ref().map(|binding| binding.set_id) != Some(set_id) {
                    return self.advance();
                }
                self.step = VersionStep::Entry(bucket);
                let key = index_key(set_id, self.version_id);
                Ok(Next::Call(read_row(VERSION_INDEX_KEYSPACE, key)))
            }
            VersionStep::Entry(bucket) => {
                let Some(entry) = result.value()? else {
                    return self.advance();
                };
                let failed = |error: String| LocalVersionError::Failed(error);
                let entry = IndexEntry::from_bytes(&entry).map_err(|e| failed(e.to_string()))?;
                let position =
                    Position::decode(&entry.position).map_err(|e| failed(e.to_string()))?;
                Ok(Next::Done(Some((bucket, position.key))))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{IoCall, IoResult, LocalVersionOperation, Next, Operation};
    use super::{local_object, rewrite};
    use aruna_core::structs::identity::realm::RealmId;
    use serde_json::json;
    use std::collections::BTreeMap;
    use ulid::Ulid;

    fn node(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn reads_local_only() {
        let realm = RealmId::from_bytes([1; 32]);
        let version = Ulid::from_bytes([2; 16]);
        let own = format!("arn:aruna:{realm}:{}:s3/lab/a.txt@{version}", node(3));
        assert_eq!(
            local_object(&own, realm, node(3)),
            Some(("lab".into(), "a.txt".into(), Some(version)))
        );
        // Another node's ARN is resolved there, never through a local bucket of the name.
        assert_eq!(local_object(&own, realm, node(4)), None);
        assert_eq!(
            local_object(
                &format!("s3://lab/a.txt?versionId={version}"),
                realm,
                node(3)
            ),
            Some(("lab".into(), "a.txt".into(), Some(version)))
        );
        assert_eq!(
            local_object("s3://lab/dir/b.txt", realm, node(3)),
            Some(("lab".into(), "dir/b.txt".into(), None))
        );
    }

    async fn seed(context: &crate::driver::DriverContext, bucket: &str, bound: bool) -> Ulid {
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::{
            BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
        };
        use aruna_core::structs::storage::blob::{
            BackendRef, BlobHeadKey, BlobVersion, BucketInfo, CurrentVersionPointer, VersionKey,
        };
        use aruna_core::structs::storage::distribution::DistributionBinding;
        use aruna_core::structs::storage::format::EncodingClass;
        let realm = RealmId::from_bytes([1; 32]);
        let info = BucketInfo {
            group_id: Ulid::from_bytes([3; 16]),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: aruna_core::UserId::local(Ulid::from_bytes([5; 16]), realm),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: bound.then(|| DistributionBinding {
                set_id: Ulid::from_bytes([6; 16]),
                member_id: Ulid::from_bytes([7; 16]),
                incarnation: 0,
                revision: Default::default(),
            }),
        };
        let version_id = Ulid::from_bytes([8; 16]);
        let version = BlobVersion::materialized(
            [9; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            std::time::SystemTime::UNIX_EPOCH,
            info.created_by,
            None,
        );
        let writes = vec![
            (
                S3_BUCKET_KEYSPACE.to_string(),
                bucket.as_bytes().to_vec().into(),
                info.to_bytes().unwrap().into(),
            ),
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                BlobHeadKey::new(bucket, "a.txt").to_bytes().unwrap().into(),
                CurrentVersionPointer::new(version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                VersionKey::new(bucket, "a.txt", version_id)
                    .to_bytes()
                    .unwrap()
                    .into(),
                version.to_bytes().unwrap().into(),
            ),
        ];
        context
            .storage_handle
            .send_storage_effect(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            })
            .await;
        version_id
    }

    #[tokio::test]
    async fn bound_entries_globalized() {
        let dir = tempfile::tempdir().unwrap();
        let context = crate::driver::DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let realm = RealmId::from_bytes([1; 32]);
        let version_id = seed(&context, "shared", true).await;
        seed(&context, "plain", false).await;
        let crate_value = json!({"@graph": [
            {"@id": "s3://shared/a.txt", "contentUrl": "s3://shared/a.txt"},
            {"@id": "s3://plain/a.txt", "contentUrl": "s3://plain/a.txt"}
        ]});
        let normalize = super::NormalizeEntriesOperation::new(realm, node(3), crate_value);
        let (crate_value, rewritten) = crate::driver::drive(normalize, &context).await.unwrap();
        assert_eq!(rewritten, 1);
        let global = aruna_core::structs::storage::global_id::GlobalId::Version {
            realm_id: realm,
            version_id,
        }
        .to_url();
        assert_eq!(crate_value["@graph"][0]["@id"], global.as_str());
        assert_eq!(crate_value["@graph"][0]["contentUrl"], global.as_str());
        // An ordinary bucket keeps its location; only member buckets have a global id.
        assert_eq!(crate_value["@graph"][1]["@id"], "s3://plain/a.txt");
    }

    #[test]
    fn rewrites_identifier_positions() {
        let old = "s3://lab/a.txt";
        let new = "https://w3id.org/aruna/data/x/y";
        let mut crate_value = json!({"@graph": [
            {"@id": "./", "hasPart": [{"@id": old}]},
            {"@id": old, "contentUrl": old, "description": old, "keywords": [old]}
        ]});
        rewrite(
            &mut crate_value,
            &BTreeMap::from([(old.to_string(), new.to_string())]),
            false,
        );
        assert_eq!(crate_value["@graph"][0]["hasPart"][0]["@id"], new);
        assert_eq!(crate_value["@graph"][1]["@id"], new);
        assert_eq!(crate_value["@graph"][1]["contentUrl"], new);
        // Free text that happens to equal the location keeps its wording.
        assert_eq!(crate_value["@graph"][1]["description"], old);
        assert_eq!(crate_value["@graph"][1]["keywords"][0], old);
    }

    #[test]
    fn no_sets_found() {
        let mut lookup = LocalVersionOperation::new(Ulid::from_bytes([1; 16]));
        assert!(matches!(
            lookup.first(),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
        let page = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        assert_eq!(lookup.next(page), Ok(Next::Done(None)));
    }

    #[test]
    fn broken_record_skipped() {
        let mut lookup = LocalVersionOperation::new(Ulid::from_bytes([1; 16]));
        lookup.first().unwrap();
        let page = IoResult::Page {
            rows: vec![(vec![1u8].into(), vec![0xffu8].into())],
            next: Some(vec![1u8].into()),
        };
        assert!(matches!(
            lookup.next(page),
            Ok(Next::Call(IoCall::Page { .. }))
        ));
    }

    #[test]
    fn foreign_values_untouched() {
        // A value that names no local object needs no read and the document stays as it is.
        let document = json!({"@id": "urn:example:x"});
        let realm = RealmId::from_bytes([1; 32]);
        let mut normalize = super::NormalizeEntriesOperation::new(realm, node(3), document.clone());
        assert_eq!(normalize.first(), Ok(Next::Done((document, 0))));
    }

    #[test]
    fn local_object_reads_bucket() {
        let realm = RealmId::from_bytes([1; 32]);
        let document = json!({"@id": "s3://shared/a.txt"});
        let mut normalize = super::NormalizeEntriesOperation::new(realm, node(3), document);
        assert!(matches!(
            normalize.first(),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        let missing = IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<
            aruna_core::structs::storage::blob::BucketInfo,
            _,
        >(
            crate::s3::bucket::get::GetBucketError::NotFound,
        )));
        let Ok(Next::Done((_, count))) = normalize.next(missing) else {
            panic!("expected the unchanged document");
        };
        assert_eq!(count, 0);
    }
}
