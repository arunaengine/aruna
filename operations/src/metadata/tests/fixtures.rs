use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_storage::StorageHandle;
use byteview::ByteView;

pub(crate) async fn write_entries(
    storage: &StorageHandle,
    writes: Vec<(String, ByteView, ByteView)>,
) {
    match storage
        .send_storage_effect(StorageEffect::BatchWrite {
            writes,
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => {}
        other => panic!("unexpected storage event: {other:?}"),
    }
}

pub(crate) async fn storage_key_exists(
    storage: &StorageHandle,
    key_space: &str,
    key: Vec<u8>,
) -> bool {
    match storage
        .send_storage_effect(StorageEffect::Read {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
        other => panic!("unexpected storage event: {other:?}"),
    }
}
