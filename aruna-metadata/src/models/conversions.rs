use aruna_permission::UserIdentity;
use automerge::ExpandedChange;
use serde_json::json;
use tracing::{error, trace};
use ulid::Ulid;

use crate::{
    error::ArunaMetadataError,
    models::structs::{Actor, Change},
};

use super::{
    requests::{GetUserRequest, GetUserRequestOuter},
    structs::{Group, Resource, TypedDoc, TypedSavedDoc, User},
};

impl TryFrom<&[u8]> for Resource {
    type Error = ArunaMetadataError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let res = postcard::from_bytes(value)?;
        Ok(res)
    }
}

impl TryFrom<Resource> for Vec<u8> {
    type Error = ArunaMetadataError;
    fn try_from(value: Resource) -> Result<Self, Self::Error> {
        let res = postcard::to_allocvec(&value)?;
        Ok(res)
    }
}

impl TryFrom<&[u8]> for User {
    type Error = ArunaMetadataError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let res = postcard::from_bytes(value)?;
        Ok(res)
    }
}

impl TryFrom<User> for Vec<u8> {
    type Error = ArunaMetadataError;
    fn try_from(value: User) -> Result<Self, Self::Error> {
        let res = postcard::to_allocvec(&value)?;
        Ok(res)
    }
}

impl TryFrom<&[u8]> for Group {
    type Error = ArunaMetadataError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let res = postcard::from_bytes(value)?;
        Ok(res)
    }
}

impl TryFrom<Group> for Vec<u8> {
    type Error = ArunaMetadataError;
    fn try_from(value: Group) -> Result<Self, Self::Error> {
        let bytes = postcard::to_allocvec(&value)?;
        Ok(bytes)
    }
}
impl TryFrom<GetUserRequestOuter> for GetUserRequest {
    type Error = ArunaMetadataError;
    fn try_from(value: GetUserRequestOuter) -> Result<Self, Self::Error> {
        trace!("{value:?}");
        Ok(GetUserRequest {
            id: aruna_permission::UserIdentity::from_string(value.id)?,
        })
    }
}

impl From<TypedDoc> for TypedSavedDoc {
    fn from(value: TypedDoc) -> Self {
        match value {
            TypedDoc::Resource(mut d) => TypedSavedDoc::Resource(d.save()),
            TypedDoc::Group(mut d) => TypedSavedDoc::Group(d.save()),
            TypedDoc::User(mut d) => TypedSavedDoc::User(d.save()),
        }
    }
}

impl TryFrom<TypedSavedDoc> for TypedDoc {
    type Error = ArunaMetadataError;
    fn try_from(value: TypedSavedDoc) -> Result<Self, Self::Error> {
        Ok(match value {
            TypedSavedDoc::Resource(d) => TypedDoc::Resource(automerge::AutoCommit::load(&d)?),
            TypedSavedDoc::Group(d) => TypedDoc::Group(automerge::AutoCommit::load(&d)?),
            TypedSavedDoc::User(d) => TypedDoc::User(automerge::AutoCommit::load(&d)?),
        })
    }
}

impl TryFrom<String> for Actor {
    type Error = ArunaMetadataError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        let (user_identity, rest) = value.split_at(96);
        let (realm_key, node_id) = rest.split_at(64);
        let unhexed_user = hex::decode(user_identity).map_err(|e| {
            error!("{}", e);
            ArunaMetadataError::ConversionError {
                from: "Hex".to_string(),
                to: "Vec<u8>".to_string(),
            }
        })?;
        let user_identity = UserIdentity::from_bytes(&unhexed_user)?;
        Ok(Actor {
            user_identity: user_identity.to_string(),
            realm_key: realm_key.to_string(),
            node_id: node_id.to_string(),
        })
    }
}

impl TryFrom<ExpandedChange> for Change {
    type Error = ArunaMetadataError;
    fn try_from(value: ExpandedChange) -> Result<Self, Self::Error> {
        Ok(Change {
            operations: value
                .operations
                .iter()
                .map(|o| {
                    let id = o.obj.clone();
                    let key = o.key.clone();
                    let predecessor = o.pred.clone();
                    let action = o.action.clone();
                    let insert = o.insert;

                    json!({
                            "id": id.to_string(),
                            "key": key,
                            "predecessor": predecessor,
                            "action": action,
                            "insert": insert,
                    })
                })
                .collect(),
            actor_id: value.actor_id.to_string().try_into()?,
            hash: value.hash.map(|h| h.to_string()),
            seq: value.seq,
            start_op: value.start_op.into(),
            time: value.time,
            message: value.message,
            deps: value.deps.iter().map(|d| d.to_string()).collect(),
            extra_bytes: value.extra_bytes,
        })
    }
}

pub trait ToBytes: Sized {
    fn to_bytes(self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, ArunaMetadataError>;
}

impl ToBytes for Ulid {
    fn to_bytes(self) -> Vec<u8> {
        Ulid::to_bytes(&self).to_vec()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, ArunaMetadataError> {
        Ok(Ulid::from_bytes(bytes.as_slice().try_into()?))
    }
}

pub mod autosurgeon_ulid {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use ulid::Ulid;
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<Ulid, HydrateError> {
        let inner = autosurgeon::bytes::ByteVec::hydrate(doc, obj, prop)?;
        Ok(Ulid::from_bytes(inner.as_slice().try_into().map_err(
            |_| HydrateError::unexpected("&[u8; 16]", "Invalid slice of bytes".to_string()),
        )?))
    }

    pub fn reconcile<R: Reconciler>(ulid: &Ulid, mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(ulid.to_bytes())
    }
}

pub mod autosurgeon_bytes {
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<[u8; 32], HydrateError> {
        let inner = autosurgeon::bytes::ByteVec::hydrate(doc, obj, prop)?;
        inner.as_slice().try_into().map_err(|_| {
            HydrateError::unexpected("&[u8; 16]", "Invalid slice of bytes".to_string())
        })
    }

    pub fn reconcile<R: Reconciler>(bytes: &[u8; 32], mut reconciler: R) -> Result<(), R::Error> {
        reconciler.bytes(bytes)
    }
}

pub mod autosurgeon_date_time {

    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    use chrono::{DateTime, Utc};
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<DateTime<Utc>, HydrateError> {
        let inner = <i64>::hydrate(doc, obj, prop)?;
        DateTime::from_timestamp_millis(inner).ok_or_else(|| {
            HydrateError::unexpected(
                "a valid (millisec) timestamp",
                "Invalid timestamp".to_string(),
            )
        })
    }

    pub fn reconcile<R: Reconciler>(
        time: &DateTime<Utc>,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.i64(time.timestamp_millis())
    }
}

pub mod autosurgeon_user_identity {
    use aruna_permission::UserIdentity;
    use autosurgeon::{Hydrate, HydrateError, Prop, ReadDoc, Reconciler};
    pub fn hydrate<'a, D: ReadDoc>(
        doc: &D,
        obj: &automerge::ObjId,
        prop: Prop<'a>,
    ) -> Result<UserIdentity, HydrateError> {
        let inner = autosurgeon::bytes::ByteVec::hydrate(doc, obj, prop)?;
        UserIdentity::from_bytes(inner.as_slice()).map_err(|_| {
            HydrateError::unexpected("&[u8; 16]", "Invalid slice of bytes".to_string())
        })
    }

    pub fn reconcile<R: Reconciler>(
        identity: &UserIdentity,
        mut reconciler: R,
    ) -> Result<(), R::Error> {
        reconciler.bytes(identity.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use aruna_permission::UserIdentity;
    use autosurgeon::{hydrate, reconcile};
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;
    use ulid::Ulid;

    use crate::models::structs::{
        Author, Data, KeyValue, Resource, ResourceVariant, User, VisibilityClass,
    };

    #[test]
    fn test_conversion() {
        let realm_key = SigningKey::generate(&mut OsRng).verifying_key().to_bytes();
        let id = UserIdentity::new(Ulid::new(), realm_key);
        let user = User {
            id,
            realm_key,
            name: "test_name".to_string(),
        };

        let time = chrono::Utc::now().timestamp_millis();
        let time = chrono::DateTime::from_timestamp_millis(time).unwrap();
        let resource = Resource {
            id: Ulid::new(),
            name: "test_name".to_string(),
            title: "test_title".to_string(),
            description: "test_description".to_string(),
            revision: 5,
            variant: ResourceVariant::Object,
            labels: vec![KeyValue {
                key: "test_key".to_string(),
                value: "test_value".to_string(),
            }],
            identifiers: vec!["ThisIsAnORCIDPlaceholder".to_string()],
            content_len: 12049814209,
            count: 0,
            visibility: VisibilityClass::Private,
            created_at: time,
            last_modified: time,
            authors: vec![Author {
                first: "Jane".to_string(),
                last: "Doe".to_string(),
                id: "ORCID".to_string(),
            }],
            license_id: Ulid::new(),
            locked: true,
            deleted: false,
            data: vec![Data::ContentHash {
                datahash: "716f6e863f744b9ac22c97ec7b76ea5f5908bc5b2f67c61510bfc4751384ea7a"
                    .to_string(),
            }],
            // location: vec!["proxy.data.org".to_string()],
            // hashes: vec![Hash {
            //     algorithm: HashAlgorithm::MD5,
            //     value: "ExampleHashString".to_string(),
            // }],
        };
        // Convert into bytes
        let user_bytes: Vec<u8> = user.clone().try_into().unwrap();
        let resource_bytes: Vec<u8> = resource.clone().try_into().unwrap();

        // Convert back
        let user_back_conversion: User = user_bytes.as_slice().try_into().unwrap();
        let resource_back_conversion: Resource = resource_bytes.as_slice().try_into().unwrap();

        assert_eq!(user, user_back_conversion);
        assert_eq!(resource, resource_back_conversion);
    }

    #[test]
    fn test_automerge() {
        let realm_key = SigningKey::generate(&mut OsRng).verifying_key().to_bytes();
        let id = UserIdentity::new(Ulid::new(), realm_key);
        let user1 = User {
            id,
            realm_key,
            name: "test_name".to_string(),
        };

        let mut doc = automerge::AutoCommit::new();
        reconcile(&mut doc, &user1).unwrap();
        let user2: User = hydrate(&doc).unwrap();
        assert_eq!(user1, user2);

        let time = chrono::Utc::now().timestamp_millis();
        let time = chrono::DateTime::from_timestamp_millis(time).unwrap();
        let resource1 = Resource {
            id: Ulid::new(),
            name: "test_name".to_string(),
            title: "test_title".to_string(),
            description: "test_description".to_string(),
            revision: 5,
            variant: ResourceVariant::Object,
            labels: vec![KeyValue {
                key: "test_key".to_string(),
                value: "test_value".to_string(),
            }],
            identifiers: vec!["ThisIsAnORCIDPlaceholder".to_string()],
            content_len: 12049814209,
            count: 0,
            visibility: VisibilityClass::Private,
            created_at: time,
            last_modified: time,
            authors: vec![Author {
                first: "Jane".to_string(),
                last: "Doe".to_string(),
                id: "ORCID".to_string(),
            }],
            license_id: Ulid::new(),
            locked: true,
            deleted: false,
            data: vec![Data::ContentHash {
                datahash: "716f6e863f744b9ac22c97ec7b76ea5f5908bc5b2f67c61510bfc4751384ea7a"
                    .to_string(),
            }],
            // location: vec!["proxy.data.org".to_string()],
            // hashes: vec![Hash {
            //     algorithm: HashAlgorithm::MD5,
            //     value: "ExampleHashString".to_string(),
            // }],
        };

        let mut doc = automerge::AutoCommit::new();
        reconcile(&mut doc, &resource1).unwrap();
        let resource2: Resource = hydrate(&doc).unwrap();
        assert_eq!(resource1, resource2);
    }
}
