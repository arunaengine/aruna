//! Object range and path restriction helpers for the S3 adapter. The trait
//! implementation stays in `service`.

use aruna_core::permission_path::permission_pattern_matches;
use aruna_core::structs::{PathRestriction, Permission};
use aruna_operations::s3::get_object::ObjectRangeRequest;

pub(super) fn object_range_request(range: s3s::dto::Range) -> ObjectRangeRequest {
    match range {
        s3s::dto::Range::Int { first, last } => match last {
            Some(end) => ObjectRangeRequest::StartEnd { start: first, end },
            None => ObjectRangeRequest::Start { start: first },
        },
        s3s::dto::Range::Suffix { length } => ObjectRangeRequest::Suffix { length },
    }
}

/// Whether a credential's restrictions leave any allowed scope at or below one
/// bucket: either a restriction covers the bucket node itself, or it is scoped
/// to a path inside that bucket. Unrestricted credentials reach every bucket.
pub(super) fn restrictions_reach(
    restrictions: Option<&[PathRestriction]>,
    bucket_path: &str,
) -> bool {
    let Some(restrictions) = restrictions else {
        return true;
    };
    let inside = format!("{bucket_path}/");
    restrictions.iter().any(|restriction| {
        restriction.permission != Permission::DENY
            && (restriction.pattern.starts_with(&inside)
                || permission_pattern_matches(&restriction.pattern, bucket_path))
    })
}
