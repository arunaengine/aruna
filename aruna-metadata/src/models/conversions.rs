use crate::error::ArunaMetadataError;

use super::models::{Group, Resource, User};

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

#[cfg(test)]
mod tests {
    use autosurgeon::{hydrate, reconcile};
    use ulid::Ulid;

    use crate::models::models::{
        Author, Hash, HashAlgorithm, KeyValue, Resource, ResourceVariant, User, VisibilityClass,
    };

    #[test]
    fn test_conversion() {
        let user = User {
            id: Ulid::new(),
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
            location: vec!["proxy.data.org".to_string()],
            hashes: vec![Hash {
                algorithm: HashAlgorithm::MD5,
                value: "ExampleHashString".to_string(),
            }],
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
        let user1 = User {
            id: Ulid::new(),
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
            location: vec!["proxy.data.org".to_string()],
            hashes: vec![Hash {
                algorithm: HashAlgorithm::MD5,
                value: "ExampleHashString".to_string(),
            }],
        };

        let mut doc = automerge::AutoCommit::new();
        reconcile(&mut doc, &resource1).unwrap();
        let resource2: Resource = hydrate(&doc).unwrap();
        assert_eq!(resource1, resource2);
    }
}
