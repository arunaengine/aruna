use crate::{
    error::ArunaMetadataError,
    models::models::{Resource, VisibilityClass},
    persistence::persistence::tables::{PUBLIC_MAPPINGS_DB_NAME, USER_MAPPINGS_DB_NAME},
};
use aruna_storage::storage::store::Store;
use automerge::ReadDoc;
use roaring::RoaringBitmap;
use std::borrow::Cow;
use ulid::Ulid;

pub(super) fn idx_from_cow<'a>(cow: Cow<'a, [u8]>) -> Result<u32, ArunaMetadataError> {
    Ok(u32::from_be_bytes(cow.as_ref().try_into().map_err(
        |_e| ArunaMetadataError::ConversionError {
            from: "&[u8]".to_string(),
            to: "&[u8; 4]".to_string(),
        },
    )?))
}

pub(super) fn visiblity_from_doc(
    doc: &automerge::AutoCommit,
) -> Result<VisibilityClass, ArunaMetadataError> {
    Ok(
        match doc.get(automerge::ROOT, "visibility")?.ok_or_else(|| {
            ArunaMetadataError::DeserializeError("visibility field not found".to_string())
        })? {
            (automerge::Value::Scalar(cow), _) => match cow.to_str() {
                Some("Private") => VisibilityClass::Private,
                Some("Public") => VisibilityClass::Public,
                Some("Invisible") => VisibilityClass::Invisible,
                _ => {
                    return Err(ArunaMetadataError::ConversionError {
                        from: "Cow".to_string(),
                        to: "VisibilityClass".to_string(),
                    });
                }
            },
            (_, _) => {
                return Err(ArunaMetadataError::ConversionError {
                    from: "Cow".to_string(),
                    to: "VisibilityClass".to_string(),
                });
            }
        },
    )
}

pub(super) fn update_mappings<'a, 'b, S>(
    store: &'a S,
    txn: &'b mut <S as Store<'a>>::Txn,
    resource: Resource,
    user_id: &Ulid,
    idx: u32,
) -> Result<(), ArunaMetadataError>
where
    'a: 'b,
    S: Store<'a> + Send + Sync,
{
    if !matches!(resource.visibility, VisibilityClass::Public) {
        // Add to private mappings
        let user_id = user_id.to_bytes();
        let mut map = match store.get(txn, USER_MAPPINGS_DB_NAME, &user_id)? {
            Some(map) => RoaringBitmap::deserialize_from(map.as_ref())?,
            None => RoaringBitmap::new(),
        };
        //.ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
        map.insert(idx);
        let mut bitmap = Vec::new();
        map.serialize_into(&mut bitmap)?;
        store.put(txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;

        // Remove from public mappings
        let public_id = Ulid::default().to_bytes();
        let res = store
            .get(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
            .ok_or_else(|| ArunaMetadataError::NotFound("No public mapping found".to_string()))?;
        let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
        mut_map.remove(idx);
        let mut bitmap = Vec::new();
        mut_map.serialize_into(&mut bitmap)?;
        store.put(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;
    } else {
        // Add to public mappings
        let public_id = Ulid::default().to_bytes();
        let res = store
            .get(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
            .ok_or_else(|| ArunaMetadataError::NotFound("No public mapping found".to_string()))?;
        let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
        mut_map.insert(idx);
        let mut bitmap = Vec::new();
        mut_map.serialize_into(&mut bitmap)?;
        store.put(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;

        // Remove from private mappings
        let user_id = user_id.to_bytes();
        let mut map = match store.get(txn, USER_MAPPINGS_DB_NAME, &user_id)? {
            Some(map) => RoaringBitmap::deserialize_from(map.as_ref())?,
            None => RoaringBitmap::new(),
        };
        map.remove(idx);
        let mut bitmap = Vec::new();
        map.serialize_into(&mut bitmap)?;
        store.put(txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;
    }
    Ok(())
}

pub(super) fn create_mappings<'a, 'b, S>(
    store: &'a S,
    txn: &'b mut <S as Store<'a>>::Txn,
    resource: Resource,
    user_id: &Ulid,
    idx: u32,
) -> Result<(), ArunaMetadataError>
where
    'a: 'b,
    S: Store<'a> + Send + Sync + Sized,
{
    if !matches!(resource.visibility, VisibilityClass::Public) {
        // Create private bitmap
        let user_id = user_id.to_bytes();
        let mut map = match store.get(txn, USER_MAPPINGS_DB_NAME, &user_id)? {
            Some(map) => RoaringBitmap::deserialize_from(map.as_ref())?,
            None => RoaringBitmap::new(),
        };
        map.insert(idx);
        let mut bitmap = Vec::new();
        map.serialize_into(&mut bitmap)?;
        store.put(txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;
    } else {
        // Update public bitmap
        let public_id = Ulid::default().to_bytes();
        let res = store
            .get(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
            .ok_or_else(|| ArunaMetadataError::NotFound("No user mapping found".to_string()))?;
        let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
        mut_map.insert(idx);
        let mut bitmap = Vec::new();
        mut_map.serialize_into(&mut bitmap)?;
        store.put(txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;
    }
    Ok(())
}
