//! Refuses S3 deletions of single versions that a release of the bucket pins.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_operations::driver::drive;
use aruna_operations::s3::bucket::get::GetBucketOperation;
use aruna_operations::s3::bucket::release::{BucketScope, MembershipQuery};
use aruna_operations::s3::object::delete::bulk::BulkDeleteEntry;
use aruna_operations::s3::object::delete::pinned::{PinnedError, refuse_pinned, version_pins};
use s3s::dto::Error as S3DeleteError;
use s3s::{S3Error, S3ErrorCode, S3Result};
use ulid::Ulid;

use super::ArunaS3Service;
use crate::s3::error::IntoS3Error;

const PINNED_CODE: &str = "VersionPinnedByRelease";

/// A pinned version answers 409; an unchecked one 503.
pub(super) fn pinned_error(error: &PinnedError) -> S3Error {
    let (code, status) = match error {
        PinnedError::Unchecked(_) => (
            S3ErrorCode::ServiceUnavailable,
            http::StatusCode::SERVICE_UNAVAILABLE,
        ),
        _ => (
            S3ErrorCode::Custom(PINNED_CODE.into()),
            http::StatusCode::CONFLICT,
        ),
    };
    let mut refused = S3Error::with_message(code, error.to_string());
    refused.set_status_code(status);
    refused
}

impl ArunaS3Service {
    /// The bucket's info when it needs a release check: a distributed bucket refuses single
    /// version deletes itself.
    async fn release_info(
        &self,
        bucket: &str,
        info: Option<&BucketInfo>,
    ) -> S3Result<Option<BucketInfo>> {
        let info = match info {
            Some(info) => info.clone(),
            None => drive(GetBucketOperation::new(bucket.to_string()), &self.state)
                .await
                .map_err(IntoS3Error::into_s3_error)?,
        };
        Ok(info.distribution.is_none().then_some(info))
    }

    fn scope<'a>(&self, bucket: &'a str, info: &'a BucketInfo) -> BucketScope<'a> {
        BucketScope {
            realm_id: self.realm_id,
            node_id: self.node_id,
            bucket,
            info,
        }
    }

    /// Refuses the permanent deletion of `version_id` of `key` while a release pins it.
    pub(super) async fn refuse_pinned(
        &self,
        (bucket, info): (&str, Option<&BucketInfo>),
        auth: &AuthContext,
        (key, version_id): (&str, Ulid),
    ) -> S3Result<()> {
        let Some(info) = self.release_info(bucket, info).await? else {
            return Ok(());
        };
        let scope = self.scope(bucket, &info);
        refuse_pinned(&self.state, scope, auth, (key, version_id))
            .await
            .map_err(|error| pinned_error(&error))
    }

    /// Keeps the entries no release pins; each pinned or unchecked version gets an error.
    pub(super) async fn drop_pinned(
        &self,
        (bucket, info): (&str, Option<&BucketInfo>),
        auth: &AuthContext,
        entries: Vec<BulkDeleteEntry>,
    ) -> S3Result<(Vec<BulkDeleteEntry>, Vec<S3DeleteError>)> {
        let versioned = entries.iter().any(|entry| entry.version_id.is_some());
        let info = match versioned {
            true => self.release_info(bucket, info).await?,
            false => None,
        };
        let Some(info) = info else {
            return Ok((entries, Vec::new()));
        };
        let queries: Vec<MembershipQuery> = (entries.iter())
            .filter_map(|entry| {
                Some(MembershipQuery {
                    key: entry.key.clone(),
                    version_id: entry.version_id?,
                })
            })
            .collect();
        let pins = version_pins(&self.state, self.scope(bucket, &info), auth, &queries).await;
        let mut pins = match pins {
            Ok(pins) => pins.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
            Err(error) => vec![Err(error); queries.len()].into_iter(),
        };
        let (mut kept, mut errors) = (Vec::new(), Vec::new());
        for entry in entries {
            let Some(version_id) = entry.version_id else {
                kept.push(entry);
                continue;
            };
            let refusal = match pins.next() {
                Some(Ok(false)) => None,
                Some(Ok(true)) => Some(PinnedError::Version {
                    key: entry.key.clone(),
                    version_id,
                }),
                Some(Err(error)) => Some(error),
                None => Some(PinnedError::Unchecked("no answer".to_string())),
            };
            match refusal {
                None => kept.push(entry),
                Some(refusal) => errors.push(S3DeleteError {
                    code: Some(match refusal {
                        PinnedError::Unchecked(_) => "ServiceUnavailable".to_string(),
                        _ => PINNED_CODE.to_string(),
                    }),
                    message: Some(refusal.to_string()),
                    key: Some(entry.key),
                    version_id: Some(version_id.to_string()),
                }),
            }
        }
        Ok((kept, errors))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_maps_conflict() {
        let pinned = PinnedError::Version {
            key: "db/a".to_string(),
            version_id: Ulid::from_bytes([1; 16]),
        };
        let refused = pinned_error(&pinned);
        assert_eq!(*refused.code(), S3ErrorCode::Custom(PINNED_CODE.into()));
        assert_eq!(refused.status_code(), Some(http::StatusCode::CONFLICT));
        let unchecked = pinned_error(&PinnedError::Unchecked("down".to_string()));
        assert_eq!(
            unchecked.status_code(),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );
    }
}
