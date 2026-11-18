//! Reduces the answers of the holders a routed metadata read asked to one outcome.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;

use crate::metadata::api::export_types::ExportMetadataResult;
use crate::metadata::protocol::MetadataReadError;

/// Any answer is the exact revision that was asked for, so a lagging holder's
/// not-found never outranks a holder that served that revision.
pub(crate) fn collect_profile_export(
    responses: Vec<(NodeId, Result<ExportMetadataResult, MetadataReadError>)>,
    holder_count: usize,
    timed_out: bool,
) -> Result<ExportMetadataResult, MetadataReadError> {
    let mut not_found = 0usize;
    let mut success = None;
    let mut auth_error = None;
    let mut unavailable = timed_out;
    for (_, response) in responses {
        match response {
            Ok(export) => {
                success.get_or_insert(export);
            }
            Err(error @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden)) => {
                auth_error.get_or_insert(error);
            }
            Err(MetadataReadError::NotFound) => not_found += 1,
            Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => unavailable = true,
        }
    }
    let all_not_found = holder_count > 0 && not_found == holder_count;
    match reduce_holder_reads(
        success,
        auth_error,
        all_not_found,
        false,
        unavailable,
        AuthFailure::Unavailable,
    ) {
        ReadDecision::Success(export) => Ok(export),
        ReadDecision::NotFound => Err(MetadataReadError::NotFound),
        ReadDecision::Auth(_) | ReadDecision::Unavailable => Err(MetadataReadError::Unavailable),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthFailure {
    Fatal,
    Unavailable,
}

pub(crate) enum ReadDecision<T> {
    Success(T),
    NotFound,
    Auth(MetadataReadError),
    Unavailable,
}

/// Reduces holder answers; `conflict` outranks success and `all_not_found`
/// is only `NotFound` while no holder was unavailable.
pub(crate) fn reduce_holder_reads<T>(
    success: Option<T>,
    auth_error: Option<MetadataReadError>,
    all_not_found: bool,
    conflict: bool,
    mut unavailable: bool,
    auth_failure: AuthFailure,
) -> ReadDecision<T> {
    if let Some(error) = auth_error {
        match auth_failure {
            AuthFailure::Fatal => return ReadDecision::Auth(error),
            AuthFailure::Unavailable => unavailable = true,
        }
    }
    if conflict {
        return ReadDecision::Unavailable;
    }
    if let Some(value) = success {
        return ReadDecision::Success(value);
    }
    if all_not_found && !unavailable {
        ReadDecision::NotFound
    } else {
        ReadDecision::Unavailable
    }
}
