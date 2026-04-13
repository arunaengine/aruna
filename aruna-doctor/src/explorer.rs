use crate::error::CliError;
use aruna::config::PersistedNodeState;
use aruna_api::server_state::{
    INITIAL_REALM_ADMIN_CLAIMED_KEY, TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY,
};
use aruna_core::id::{DhtKeyId, TopicId};
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, AUTH_KEYSPACE, DHT_KEYSPACE, GOSSIP_SUBSCRIPTIONS_KEYSPACE, GROUP_KEYSPACE,
    NODE_STATE_KEYSPACE, ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE, REALM_KEYSPACE,
    S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE, USER_ACCESS_KEYSPACE,
};
use aruna_core::onboarding::OnboardingSecretRecord;
use aruna_core::structs::{
    BucketInfo, Group, GroupAuthorizationDocument, Location, LookupKey, Realm,
    RealmAuthorizationDocument, RealmConfigDocument, RealmId, UserAccess, VersionKey,
    VersionMetadata,
};
use aruna_net::dht::storage::StoredEntry;
use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase, Readable};
use serde::Serialize;
use serde::ser::{SerializeStruct, Serializer};
use std::collections::HashSet;
use std::path::Path;
use ulid::Ulid;

const CRAQLE_TERMS_KEYSPACE: &str = "terms";
const CRAQLE_QUADS_KEYSPACE: &str = "quads";
const CRAQLE_GRAPHS_KEYSPACE: &str = "graphs";
const CRAQLE_LOG_KEYSPACE: &str = "log";

#[derive(Debug, thiserror::Error)]
pub enum ExplorerError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error("keyspace not found: {0}")]
    KeyspaceNotFound(String),
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct KeyspacesOutput {
    database_path: String,
    keyspaces: Vec<KeyspaceEntry>,
    missing_keyspaces: Vec<KeyspaceEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct KeyspaceEntry {
    name: String,
}

#[derive(Debug, Serialize, PartialEq)]
struct EntriesOutput {
    database_path: String,
    keyspace: String,
    entries: Vec<EntryOutput>,
}

#[derive(Debug, Serialize, PartialEq)]
struct EntryOutput {
    key: DecodedField,
    value: DecodedValue,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "format")]
enum DecodedField {
    #[serde(rename = "ulid")]
    Ulid { value: String },
    #[serde(rename = "realm_id")]
    RealmId { value: String },
    #[serde(rename = "dht_key")]
    DhtKeyId { value: String },
    #[serde(rename = "utf8")]
    Utf8 { value: String },
    #[serde(rename = "lookup_key")]
    LookupKey { value: LookupKey },
    #[serde(rename = "version_key")]
    VersionKey { value: VersionKey },
    #[serde(rename = "raw")]
    Raw { hex: String },
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(tag = "type")]
enum DecodedValue {
    Group {
        data: JsonGroup,
    },
    GroupAuthorizationDocument {
        data: GroupAuthorizationDocument,
    },
    Realm {
        data: JsonRealm,
    },
    RealmAuthorizationDocument {
        data: JsonRealmAuthorizationDocument,
    },
    RealmConfigDocument {
        data: JsonRealmConfigDocument,
    },
    UserAccess {
        data: JsonUserAccess,
    },
    BucketInfo {
        data: BucketInfo,
    },
    Location {
        data: Location,
    },
    VersionMetadata {
        data: VersionMetadata,
    },
    ApiTokenRevocationList {
        data: HashSet<String>,
    },
    ApiTrustedRealmsList {
        data: Vec<String>,
    },
    ApiInitialRealmAdminClaimed {
        data: bool,
    },
    GossipSubscriptions {
        data: Vec<String>,
    },
    NodeState {
        data: JsonPersistedNodeState,
    },
    OnboardingSecretRecord {
        data: OnboardingSecretRecord,
    },
    DhtEntries {
        data: Vec<JsonStoredEntry>,
    },
    Raw {
        hex: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        decode_error: Option<String>,
    },
}

#[derive(Debug, PartialEq, Eq)]
struct JsonGroup(Group);

impl Serialize for JsonGroup {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Group", 4)?;
        state.serialize_field("display_name", &self.0.display_name)?;
        state.serialize_field("group_id", &self.0.group_id.to_string())?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("roles", &self.0.roles)?;
        state.end()
    }
}

#[derive(Debug, PartialEq)]
struct JsonRealm(Realm);

impl Serialize for JsonRealm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Realm", 2)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("description", &self.0.description)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonRealmAuthorizationDocument(RealmAuthorizationDocument);

impl Serialize for JsonRealmAuthorizationDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RealmAuthorizationDocument", 3)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("roles", &self.0.roles)?;
        state.serialize_field("operation_restrictions", &self.0.operation_restrictions)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonRealmConfigDocument(RealmConfigDocument);

impl Serialize for JsonRealmConfigDocument {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RealmConfigDocument", 2)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("metadata_replication", &self.0.metadata_replication)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonUserAccess(UserAccess);

impl Serialize for JsonUserAccess {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UserAccess", 8)?;
        state.serialize_field("access_key", &self.0.access_key)?;
        state.serialize_field("user_identity", &JsonUserIdentity(&self.0.user_identity))?;
        state.serialize_field("group_id", &self.0.group_id.to_string())?;
        state.serialize_field("secret", &self.0.secret)?;
        state.serialize_field("expiry", &self.0.expiry)?;
        state.serialize_field("path_restrictions", &self.0.path_restrictions)?;
        state.serialize_field("issued_by", &self.0.issued_by)?;
        state.serialize_field("revoked_at", &self.0.revoked_at)?;
        state.end()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct JsonPersistedNodeState(PersistedNodeState);

impl Serialize for JsonPersistedNodeState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PersistedNodeState", 7)?;
        state.serialize_field("boot_origin", &self.0.boot_origin)?;
        state.serialize_field("status", &self.0.status)?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("net_secret_key", &hex::encode(self.0.net_secret_key))?;
        state.serialize_field("bootstrap_endpoints", &self.0.bootstrap_endpoints)?;
        state.serialize_field("onboarding_phase", &self.0.onboarding_phase)?;
        state.serialize_field("onboarding_sync_ticket", &self.0.onboarding_sync_ticket)?;
        state.serialize_field("identity", &self.0.identity)?;
        state.end()
    }
}

#[derive(Debug)]
struct JsonStoredEntry(StoredEntry);

impl PartialEq for JsonStoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.publisher == other.0.publisher
            && self.0.realm_id == other.0.realm_id
            && self.0.value == other.0.value
            && self.0.expires_at == other.0.expires_at
            && self.0.signature == other.0.signature
    }
}

impl Serialize for JsonStoredEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("StoredEntry", 6)?;
        state.serialize_field("publisher", &self.0.publisher.to_string())?;
        state.serialize_field("realm_id", &self.0.realm_id.to_string())?;
        state.serialize_field("expires_at", &self.0.expires_at)?;
        state.serialize_field(
            "signature",
            &self
                .0
                .signature
                .as_ref()
                .map(std::string::ToString::to_string),
        )?;
        state.serialize_field("value_len", &self.0.value.len())?;
        state.serialize_field("value_hex", &hex::encode(&self.0.value))?;
        state.end()
    }
}

struct JsonUserIdentity<'a>(&'a aruna_core::structs::UserIdentity);

impl Serialize for JsonUserIdentity<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UserIdentity", 2)?;
        state.serialize_field("user_id", &self.0.user_id.to_string())?;
        state.serialize_field("realm_key", &self.0.realm_key.to_string())?;
        state.end()
    }
}

pub async fn explore_keyspaces(database_path: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        move || list_keyspaces(&database_path)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub async fn explore_entries(database_path: String, keyspace: String) -> Result<(), CliError> {
    let output = tokio::task::spawn_blocking({
        let database_path = database_path.clone();
        let keyspace = keyspace.clone();
        move || list_entries(&database_path, &keyspace)
    })
    .await
    .map_err(std::io::Error::other)??;

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn list_keyspaces(database_path: &str) -> Result<KeyspacesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let mut keyspaces = db.list_keyspace_names();
    keyspaces.sort();
    let existing = keyspaces
        .iter()
        .map(|name| name.as_ref())
        .collect::<HashSet<_>>();
    let mut missing_keyspaces = defined_keyspaces()
        .into_iter()
        .filter(|name| !existing.contains(name))
        .map(|name| KeyspaceEntry {
            name: name.to_string(),
        })
        .collect::<Vec<_>>();
    missing_keyspaces.sort_by(|left, right| left.name.cmp(&right.name));

    Ok(KeyspacesOutput {
        database_path: database_path.to_string(),
        keyspaces: keyspaces
            .into_iter()
            .map(|name| KeyspaceEntry {
                name: name.to_string(),
            })
            .collect(),
        missing_keyspaces,
    })
}

fn defined_keyspaces() -> [&'static str; 17] {
    [
        API_STATE_KEYSPACE,
        AUTH_KEYSPACE,
        CRAQLE_GRAPHS_KEYSPACE,
        CRAQLE_LOG_KEYSPACE,
        CRAQLE_QUADS_KEYSPACE,
        CRAQLE_TERMS_KEYSPACE,
        DHT_KEYSPACE,
        GOSSIP_SUBSCRIPTIONS_KEYSPACE,
        GROUP_KEYSPACE,
        NODE_STATE_KEYSPACE,
        ONBOARDING_KEYSPACE,
        REALM_CONFIG_KEYSPACE,
        REALM_KEYSPACE,
        S3_BUCKET_KEYSPACE,
        S3_LOOKUP_KEYSPACE,
        S3_VERSION_KEYSPACE,
        USER_ACCESS_KEYSPACE,
    ]
}

fn list_entries(database_path: &str, keyspace_name: &str) -> Result<EntriesOutput, ExplorerError> {
    let db = OptimisticTxDatabase::builder(Path::new(database_path)).open()?;
    let keyspace_names = db.list_keyspace_names();
    if !keyspace_names
        .iter()
        .any(|name| name.as_ref() == keyspace_name)
    {
        return Err(ExplorerError::KeyspaceNotFound(keyspace_name.to_string()));
    }

    let keyspace = db.keyspace(keyspace_name, KeyspaceCreateOptions::default)?;
    let snapshot = db.read_tx();
    let mut entries = Vec::new();

    for entry in snapshot.iter(&keyspace) {
        let (key, value) = entry.into_inner()?;
        entries.push(decode_entry(keyspace_name, key.as_ref(), value.as_ref()));
    }

    Ok(EntriesOutput {
        database_path: database_path.to_string(),
        keyspace: keyspace_name.to_string(),
        entries,
    })
}

fn decode_entry(keyspace_name: &str, key: &[u8], value: &[u8]) -> EntryOutput {
    EntryOutput {
        key: decode_key(keyspace_name, key),
        value: decode_value(keyspace_name, key, value),
    }
}

fn decode_key(keyspace_name: &str, key: &[u8]) -> DecodedField {
    match keyspace_name {
        GROUP_KEYSPACE | AUTH_KEYSPACE => decode_ulid_key(key),
        REALM_KEYSPACE | REALM_CONFIG_KEYSPACE => decode_realm_id_key(key),
        USER_ACCESS_KEYSPACE
        | S3_BUCKET_KEYSPACE
        | API_STATE_KEYSPACE
        | GOSSIP_SUBSCRIPTIONS_KEYSPACE
        | NODE_STATE_KEYSPACE
        | ONBOARDING_KEYSPACE => decode_utf8_key(key),
        DHT_KEYSPACE => decode_dht_key(key),
        S3_LOOKUP_KEYSPACE => LookupKey::from_bytes(key)
            .map(|value| DecodedField::LookupKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        S3_VERSION_KEYSPACE => VersionKey::from_bytes(key)
            .map(|value| DecodedField::VersionKey { value })
            .unwrap_or_else(|_| raw_field(key)),
        _ => raw_field(key),
    }
}

fn decode_value(keyspace_name: &str, key: &[u8], value: &[u8]) -> DecodedValue {
    match keyspace_name {
        GROUP_KEYSPACE => decode_value_with(value, Group::from_bytes, |data| DecodedValue::Group {
            data: JsonGroup(data),
        }),
        REALM_KEYSPACE => decode_value_with(value, Realm::from_bytes, |data| DecodedValue::Realm {
            data: JsonRealm(data),
        }),
        REALM_CONFIG_KEYSPACE => {
            decode_value_with(value, RealmConfigDocument::from_bytes, |data| {
                DecodedValue::RealmConfigDocument {
                    data: JsonRealmConfigDocument(data),
                }
            })
        }
        USER_ACCESS_KEYSPACE => decode_value_with(value, UserAccess::from_bytes, |data| {
            DecodedValue::UserAccess {
                data: JsonUserAccess(data),
            }
        }),
        S3_BUCKET_KEYSPACE => decode_value_with(value, BucketInfo::from_bytes, |data| {
            DecodedValue::BucketInfo { data }
        }),
        S3_LOOKUP_KEYSPACE => decode_value_with(value, Location::from_bytes, |data| {
            DecodedValue::Location { data }
        }),
        S3_VERSION_KEYSPACE => decode_value_with(value, VersionMetadata::from_bytes, |data| {
            DecodedValue::VersionMetadata { data }
        }),
        AUTH_KEYSPACE => decode_auth_value(value),
        API_STATE_KEYSPACE => decode_api_state_value(key, value),
        GOSSIP_SUBSCRIPTIONS_KEYSPACE => decode_gossip_subscriptions_value(value),
        NODE_STATE_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<PersistedNodeState>(bytes),
            |data| DecodedValue::NodeState {
                data: JsonPersistedNodeState(data),
            },
        ),
        ONBOARDING_KEYSPACE => decode_value_with(
            value,
            |bytes| postcard::from_bytes::<OnboardingSecretRecord>(bytes),
            |data| DecodedValue::OnboardingSecretRecord { data },
        ),
        DHT_KEYSPACE => decode_value_with(value, decode_dht_entries, |data| {
            DecodedValue::DhtEntries { data }
        }),
        _ => raw_value(value, None),
    }
}

fn decode_gossip_subscriptions_value(value: &[u8]) -> DecodedValue {
    decode_value_with(
        value,
        |bytes| postcard::from_bytes::<Vec<TopicId>>(bytes),
        |data| {
            let mut data = data
                .into_iter()
                .map(|topic| topic.to_string())
                .collect::<Vec<_>>();
            data.sort();
            DecodedValue::GossipSubscriptions { data }
        },
    )
}

fn decode_auth_value(value: &[u8]) -> DecodedValue {
    if let Ok(data) = GroupAuthorizationDocument::from_bytes(value) {
        return DecodedValue::GroupAuthorizationDocument { data };
    }
    if let Ok(data) = RealmAuthorizationDocument::from_bytes(value) {
        return DecodedValue::RealmAuthorizationDocument {
            data: JsonRealmAuthorizationDocument(data),
        };
    }

    raw_value(
        value,
        Some(
            "failed to decode as GroupAuthorizationDocument or RealmAuthorizationDocument"
                .to_string(),
        ),
    )
}

fn decode_api_state_value(key: &[u8], value: &[u8]) -> DecodedValue {
    match key {
        TOKEN_REVOCATION_LIST_KEY => postcard::from_bytes::<HashSet<String>>(value)
            .map(|data| DecodedValue::ApiTokenRevocationList { data })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        TRUSTED_REALMS_LIST_KEY => postcard::from_bytes::<HashSet<RealmId>>(value)
            .map(|data| {
                let mut data = data
                    .into_iter()
                    .map(|realm_id| realm_id.to_string())
                    .collect::<Vec<_>>();
                data.sort();
                DecodedValue::ApiTrustedRealmsList { data }
            })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        INITIAL_REALM_ADMIN_CLAIMED_KEY => postcard::from_bytes::<bool>(value)
            .map(|data| DecodedValue::ApiInitialRealmAdminClaimed { data })
            .unwrap_or_else(|error| raw_value(value, Some(error.to_string()))),
        _ => raw_value(value, Some("unsupported api_state key".to_string())),
    }
}

fn decode_value_with<T, E>(
    value: &[u8],
    decoder: impl Fn(&[u8]) -> Result<T, E>,
    mapper: impl Fn(T) -> DecodedValue,
) -> DecodedValue
where
    E: std::fmt::Display,
{
    match decoder(value) {
        Ok(data) => mapper(data),
        Err(error) => raw_value(value, Some(error.to_string())),
    }
}

fn decode_ulid_key(key: &[u8]) -> DecodedField {
    if key.len() == 16 {
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(key);
        DecodedField::Ulid {
            value: Ulid::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_realm_id_key(key: &[u8]) -> DecodedField {
    if key.len() == 32 {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(key);
        DecodedField::RealmId {
            value: RealmId::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_utf8_key(key: &[u8]) -> DecodedField {
    String::from_utf8(key.to_vec())
        .map(|value| DecodedField::Utf8 { value })
        .unwrap_or_else(|_| raw_field(key))
}

fn decode_dht_key(key: &[u8]) -> DecodedField {
    if key.len() == 32 {
        let mut bytes = [0_u8; 32];
        bytes.copy_from_slice(key);
        DecodedField::DhtKeyId {
            value: DhtKeyId::from_bytes(bytes).to_string(),
        }
    } else {
        raw_field(key)
    }
}

fn decode_dht_entries(value: &[u8]) -> Result<Vec<JsonStoredEntry>, postcard::Error> {
    postcard::from_bytes::<Vec<StoredEntry>>(value)
        .map(|entries| entries.into_iter().map(JsonStoredEntry).collect())
}

fn raw_field(bytes: &[u8]) -> DecodedField {
    DecodedField::Raw {
        hex: hex::encode(bytes),
    }
}

fn raw_value(value: &[u8], decode_error: Option<String>) -> DecodedValue {
    DecodedValue::Raw {
        hex: hex::encode(value),
        decode_error,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CRAQLE_GRAPHS_KEYSPACE, CRAQLE_LOG_KEYSPACE, CRAQLE_QUADS_KEYSPACE, CRAQLE_TERMS_KEYSPACE,
        DecodedField, DecodedValue, decode_entry, list_entries, list_keyspaces, raw_field,
    };
    use aruna::config::{
        BootOrigin, PersistedNodeIdentity, PersistedNodeState, PersistedNodeStatus,
    };
    use aruna_core::id::{DhtKeyId, TopicId};
    use aruna_core::keyspaces::{
        API_STATE_KEYSPACE, AUTH_KEYSPACE, DHT_KEYSPACE, GOSSIP_SUBSCRIPTIONS_KEYSPACE,
        GROUP_KEYSPACE, NODE_STATE_KEYSPACE, ONBOARDING_KEYSPACE, REALM_CONFIG_KEYSPACE,
        REALM_KEYSPACE, S3_BUCKET_KEYSPACE, S3_LOOKUP_KEYSPACE, S3_VERSION_KEYSPACE,
        USER_ACCESS_KEYSPACE,
    };
    use aruna_core::onboarding::{OnboardingMode, OnboardingSecretRecord};
    use aruna_core::structs::{Actor, Group, Realm, RealmId};
    use aruna_net::dht::storage::StoredEntry;
    use fjall::{KeyspaceCreateOptions, OptimisticTxDatabase};
    use tempfile::tempdir;
    use ulid::Ulid;

    #[test]
    fn lists_sorted_keyspaces() {
        let temp = tempdir().unwrap();
        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            db.keyspace("zeta", KeyspaceCreateOptions::default).unwrap();
            db.keyspace("alpha", KeyspaceCreateOptions::default)
                .unwrap();
            db.keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
        }

        let output = list_keyspaces(temp.path().to_str().unwrap()).unwrap();
        let names = output
            .keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "alpha".to_string(),
                GROUP_KEYSPACE.to_string(),
                "zeta".to_string()
            ]
        );

        let missing = output
            .missing_keyspaces
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>();
        let mut expected_missing = vec![
            API_STATE_KEYSPACE.to_string(),
            AUTH_KEYSPACE.to_string(),
            CRAQLE_GRAPHS_KEYSPACE.to_string(),
            CRAQLE_LOG_KEYSPACE.to_string(),
            CRAQLE_QUADS_KEYSPACE.to_string(),
            CRAQLE_TERMS_KEYSPACE.to_string(),
            DHT_KEYSPACE.to_string(),
            GOSSIP_SUBSCRIPTIONS_KEYSPACE.to_string(),
            NODE_STATE_KEYSPACE.to_string(),
            ONBOARDING_KEYSPACE.to_string(),
            REALM_CONFIG_KEYSPACE.to_string(),
            REALM_KEYSPACE.to_string(),
            S3_BUCKET_KEYSPACE.to_string(),
            S3_LOOKUP_KEYSPACE.to_string(),
            S3_VERSION_KEYSPACE.to_string(),
            USER_ACCESS_KEYSPACE.to_string(),
        ];
        expected_missing.sort();
        assert_eq!(missing, expected_missing);
    }

    #[test]
    fn decodes_typed_group_entries() {
        let temp = tempdir().unwrap();
        let group_id = Ulid::new();
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[9_u8; 32]).public(),
            user_id: Ulid::new(),
            realm_id: realm_id.clone(),
        };
        let group = Group {
            display_name: "Explorer Group".to_string(),
            group_id,
            realm_id,
            roles: Default::default(),
        };

        {
            let db = OptimisticTxDatabase::builder(temp.path()).open().unwrap();
            let keyspace = db
                .keyspace(GROUP_KEYSPACE, KeyspaceCreateOptions::default)
                .unwrap();
            let mut txn = db.write_tx().unwrap();
            txn.insert(
                keyspace,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            );
            let _ = txn.commit().unwrap();
        }

        let output = list_entries(temp.path().to_str().unwrap(), GROUP_KEYSPACE).unwrap();
        assert_eq!(output.entries.len(), 1);
        assert_eq!(
            output.entries[0].key,
            DecodedField::Ulid {
                value: group_id.to_string()
            }
        );
        match &output.entries[0].value {
            DecodedValue::Group { data } => assert_eq!(data.0.display_name, "Explorer Group"),
            other => panic!("expected group, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_for_unknown_keyspace() {
        let entry = decode_entry("unknown", b"\x01\x02", b"\x03\x04");
        assert_eq!(entry.key, raw_field(b"\x01\x02"));
        match entry.value {
            DecodedValue::Raw { hex, .. } => assert_eq!(hex, "0304"),
            other => panic!("expected raw fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_realm_key() {
        let realm_id = RealmId::from_bytes([5_u8; 32]);
        let entry = decode_entry(REALM_KEYSPACE, realm_id.as_bytes(), b"not-a-realm");
        assert_eq!(
            entry.key,
            DecodedField::RealmId {
                value: realm_id.to_string()
            }
        );
    }

    #[test]
    fn returns_missing_keyspace_error() {
        let temp = tempdir().unwrap();
        let error = list_entries(temp.path().to_str().unwrap(), "missing").unwrap_err();
        assert!(error.to_string().contains("keyspace not found"));
    }

    #[test]
    fn decodes_typed_realm_value_with_raw_error_fallback() {
        let realm_id = RealmId::from_bytes([1_u8; 32]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[3_u8; 32]).public(),
            user_id: Ulid::new(),
            realm_id: realm_id.clone(),
        };
        let realm = Realm {
            realm_id: realm_id.clone(),
            description: "Explorer Realm".to_string(),
        };

        let decoded = decode_entry(
            REALM_KEYSPACE,
            realm_id.as_bytes(),
            &realm.to_bytes(&actor).unwrap(),
        );
        match decoded.value {
            DecodedValue::Realm { data } => assert_eq!(data.0.description, "Explorer Realm"),
            other => panic!("expected realm, got {other:?}"),
        }

        let fallback = decode_entry(REALM_KEYSPACE, realm_id.as_bytes(), b"broken");
        match fallback.value {
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            } => {}
            other => panic!("expected raw decode fallback, got {other:?}"),
        }
    }

    #[test]
    fn decodes_gossip_subscriptions_value() {
        let realm_id = RealmId::from_bytes([8_u8; 32]);
        let group_id = Ulid::new();
        let value = postcard::to_allocvec(&vec![
            TopicId::group(group_id),
            TopicId::realm(realm_id.clone()),
        ])
        .unwrap();

        let decoded = decode_entry(GOSSIP_SUBSCRIPTIONS_KEYSPACE, b"topics", &value);
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "topics".to_string()
            }
        );
        match decoded.value {
            DecodedValue::GossipSubscriptions { data } => {
                assert_eq!(data, vec![format!("g:{group_id}"), format!("r:{realm_id}")]);
            }
            other => panic!("expected gossip subscriptions, got {other:?}"),
        }
    }

    #[test]
    fn decodes_onboarding_secret_record_value() {
        let record = OnboardingSecretRecord {
            enrollment_id: Ulid::new(),
            secret_hash: "hash123".to_string(),
            mode: OnboardingMode::Server,
            expires_at: 1234,
            consumed: false,
        };
        let value = postcard::to_allocvec(&record).unwrap();

        let decoded = decode_entry(ONBOARDING_KEYSPACE, b"secret:test", &value);
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "secret:test".to_string()
            }
        );
        match decoded.value {
            DecodedValue::OnboardingSecretRecord { data } => assert_eq!(data, record),
            other => panic!("expected onboarding secret record, got {other:?}"),
        }
    }

    #[test]
    fn decodes_node_state_value() {
        let realm_id = RealmId::from_bytes([4_u8; 32]);
        let state = PersistedNodeState {
            boot_origin: BootOrigin::Onboarded,
            status: PersistedNodeStatus::PendingOnboarding,
            realm_id: realm_id.clone(),
            net_secret_key: [11_u8; 32],
            bootstrap_endpoints: Vec::new(),
            onboarding_phase: None,
            onboarding_sync_ticket: Some("ticket".to_string()),
            identity: PersistedNodeIdentity::Local,
        };
        let value = postcard::to_allocvec(&state).unwrap();

        let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", &value);
        assert_eq!(
            decoded.key,
            DecodedField::Utf8 {
                value: "node_state".to_string()
            }
        );
        match decoded.value {
            DecodedValue::NodeState { data } => assert_eq!(data.0, state),
            other => panic!("expected node state, got {other:?}"),
        }
    }

    #[test]
    fn decodes_dht_entries_and_key() {
        let key = DhtKeyId::from_bytes([6_u8; 32]);
        let realm_id = RealmId::from_bytes([7_u8; 32]);
        let publisher = iroh::SecretKey::from_bytes(&[5_u8; 32]).public();
        let entries = vec![StoredEntry {
            publisher,
            realm_id: realm_id.clone(),
            value: vec![1, 2, 3, 4],
            expires_at: 42,
            signature: None,
        }];
        let value = postcard::to_allocvec(&entries).unwrap();

        let decoded = decode_entry(DHT_KEYSPACE, key.as_bytes(), &value);
        assert_eq!(
            decoded.key,
            DecodedField::DhtKeyId {
                value: key.to_string()
            }
        );
        match decoded.value {
            DecodedValue::DhtEntries { data } => {
                assert_eq!(data.len(), 1);
                assert_eq!(data[0].0.publisher, publisher);
                assert_eq!(data[0].0.realm_id, realm_id);
                assert_eq!(data[0].0.value, vec![1, 2, 3, 4]);
            }
            other => panic!("expected dht entries, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_for_invalid_node_state_value() {
        let decoded = decode_entry(NODE_STATE_KEYSPACE, b"node_state", b"broken");
        match decoded.value {
            DecodedValue::Raw {
                decode_error: Some(_),
                ..
            } => {}
            other => panic!("expected raw decode fallback, got {other:?}"),
        }
    }
}
