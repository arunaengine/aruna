use aruna_core::structs::{MultipartUpload, MultipartUploadStatus};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatusCheck {
    Open,
    Takeover { now_ms: u64 },
    Skip,
}

#[derive(Debug, Error, PartialEq)]
pub(crate) enum UploadTargetError {
    #[error("The specified multipart upload does not match the target object.")]
    TargetMismatch,
    #[error("The multipart upload is no longer open.")]
    NotOpen,
    #[error("The upload is being completed, retry shortly.")]
    CompletionInProgress,
}

pub(crate) fn validate_upload(
    record: &MultipartUpload,
    bucket: &str,
    key: &str,
    status: StatusCheck,
) -> Result<(), UploadTargetError> {
    if record.bucket != bucket || record.key != key {
        return Err(UploadTargetError::TargetMismatch);
    }
    match (status, record.status) {
        (StatusCheck::Skip, _) | (StatusCheck::Open, MultipartUploadStatus::Open) => Ok(()),
        (StatusCheck::Takeover { .. }, MultipartUploadStatus::Open) => Ok(()),
        (StatusCheck::Takeover { now_ms }, MultipartUploadStatus::Completing)
            if record.completion_stale(now_ms) =>
        {
            Ok(())
        }
        (StatusCheck::Takeover { .. }, MultipartUploadStatus::Completing) => {
            Err(UploadTargetError::CompletionInProgress)
        }
        _ => Err(UploadTargetError::NotOpen),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::BackendRef;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn upload(status: MultipartUploadStatus, since: Option<u64>) -> MultipartUpload {
        MultipartUpload {
            upload_id: Ulid::from(1u128),
            backend: BackendRef::node_default(),
            storage_class: None,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            group_id: Ulid::from(2u128),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            status,
            checksum_hint: None,
            metadata: HashMap::new(),
            placement_policies: Vec::new(),
            subject_generation: 0,
            completing_since_ms: since,
        }
    }

    #[test]
    fn open_check_strict() {
        assert_eq!(
            validate_upload(
                &upload(MultipartUploadStatus::Completing, None),
                "bucket",
                "key",
                StatusCheck::Open,
            ),
            Err(UploadTargetError::NotOpen)
        );
    }

    #[test]
    fn takeover_uses_clock() {
        let now_ms = aruna_core::structs::COMPLETION_LEASE_MS + 10;
        let record = upload(MultipartUploadStatus::Completing, Some(10));

        assert_eq!(
            validate_upload(&record, "bucket", "key", StatusCheck::Takeover { now_ms }),
            Ok(())
        );
        assert_eq!(
            validate_upload(
                &record,
                "bucket",
                "key",
                StatusCheck::Takeover { now_ms: 11 },
            ),
            Err(UploadTargetError::CompletionInProgress)
        );
    }

    #[test]
    fn skip_keeps_target() {
        let record = upload(MultipartUploadStatus::Aborting, None);

        assert_eq!(
            validate_upload(&record, "bucket", "key", StatusCheck::Skip),
            Ok(())
        );
        assert_eq!(
            validate_upload(&record, "other", "key", StatusCheck::Skip),
            Err(UploadTargetError::TargetMismatch)
        );
    }
}
