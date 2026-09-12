//! Multipart upload helpers for the S3 adapter. The trait implementation stays
//! in `service`.

use super::ArunaS3Service;
use aruna_core::structs::AuthContext;
use aruna_operations::replication::queue::complete_put;
use s3s::{S3Result, s3_error};

pub(super) fn parse_upload_marker(
    key_marker: Option<&str>,
    upload_id_marker: Option<&str>,
) -> S3Result<Option<ulid::Ulid>> {
    let Some(_) = key_marker.filter(|marker| !marker.is_empty()) else {
        return Ok(None);
    };
    upload_id_marker
        .map(|marker| {
            ulid::Ulid::from_string(marker)
                .map_err(|_| s3_error!(InvalidArgument, "Invalid upload-id-marker"))
        })
        .transpose()
}

impl ArunaS3Service {
    pub(super) async fn complete_put(
        &self,
        auth: AuthContext,
        group_id: ulid::Ulid,
        bucket: String,
        key: String,
        version_id: ulid::Ulid,
        size_bytes: u64,
    ) {
        complete_put(
            &self.state,
            self.realm_id,
            self.node_id,
            auth,
            group_id,
            bucket,
            key,
            version_id,
            size_bytes,
        )
        .await;
    }
}
