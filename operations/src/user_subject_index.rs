use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::structs::User;
use aruna_core::types::{Effects, TxnId};
use aruna_core::USER_SUBJECT_INDEX_KEYSPACE;
use byteview::ByteView;
use smallvec::smallvec;

pub fn rewrite_subject_index_effects(
    previous: Option<&User>,
    current: &User,
    txn_id: TxnId,
) -> Result<Effects, ConversionError> {
    let mut deletes = Vec::new();
    let mut writes = Vec::new();

    if let Some(previous) = previous {
        for subject_id in &previous.subject_ids {
            if !current.subject_ids.contains(subject_id) {
                deletes.push((
                    USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                    ByteView::from(subject_id.as_bytes().to_vec()),
                ));
            }
        }
    }

    for subject_id in &current.subject_ids {
        // TODO: detect subject_id collisions across different users instead of overwriting.
        writes.push((
            USER_SUBJECT_INDEX_KEYSPACE.to_string(),
            ByteView::from(subject_id.as_bytes().to_vec()),
            ByteView::from(current.user_id.to_string().into_bytes()),
        ));
    }

    let mut effects = smallvec![];
    if !deletes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchDelete {
            deletes,
            txn_id: Some(txn_id),
        }));
    }
    if !writes.is_empty() {
        effects.push(Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(txn_id),
        }));
    }
    Ok(effects)
}
