//! Rules for share links: what a new link may name, and how a visit acts for the link's
//! creator narrowed to the link's scope and mode.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::id::NodeId;
use aruna_core::structs::identity::auth::{
    AuthContext, PathRestriction, Permission, SessionKind, SessionRef,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{bucket_permission_path, object_permission_path};
use aruna_core::structs::storage::share::{ShareLink, ShareMode, ShareTarget, VISIT_SECS};
use thiserror::Error;

/// Bytes of a label; it names the link in the owner's list only.
pub const MAX_LABEL: usize = 256;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ShareRuleError {
    #[error("a folder prefix is a relative path that ends with `/`")]
    Prefix,
    #[error("the object key or file name is not a valid relative path")]
    Key,
    #[error("upload links need a bucket or folder")]
    UploadTarget,
    #[error("the expiry must lie in the future")]
    Expiry,
    #[error("the label is longer than {MAX_LABEL} bytes")]
    Label,
}

/// Checks a new or changed link at `now` (Unix seconds).
pub fn validate_link(link: &ShareLink, now: u64) -> Result<(), ShareRuleError> {
    if link.label.len() > MAX_LABEL {
        return Err(ShareRuleError::Label);
    }
    if link.expires_at.is_some_and(|expiry| expiry <= now) {
        return Err(ShareRuleError::Expiry);
    }
    match &link.target {
        ShareTarget::Folder { prefix, .. } => {
            let folder = prefix.strip_suffix('/').ok_or(ShareRuleError::Prefix)?;
            relative_path(folder).map_err(|_| ShareRuleError::Prefix)?;
        }
        ShareTarget::Object { key, .. } => relative_path(key)?,
        ShareTarget::Bucket { .. } | ShareTarget::Dataset { .. } => {}
    }
    let uploads_allowed = matches!(
        link.target,
        ShareTarget::Bucket { .. } | ShareTarget::Folder { .. }
    );
    if link.mode.uploads() && !uploads_allowed {
        return Err(ShareRuleError::UploadTarget);
    }
    Ok(())
}

/// The permission the creator needs on the target: WRITE for links that upload.
pub fn required_permission(mode: ShareMode) -> Permission {
    match mode.uploads() {
        true => Permission::WRITE,
        false => Permission::READ,
    }
}

/// The path pattern a bucket, folder or object visit is confined to; `None` for a dataset,
/// whose files are confined one object at a time.
pub fn scope_pattern(link: &ShareLink, realm_id: RealmId, node_id: NodeId) -> Option<String> {
    let bucket_path = |bucket: &str| {
        globset::escape(&bucket_permission_path(
            realm_id,
            link.group_id,
            node_id,
            bucket,
        ))
    };
    match &link.target {
        ShareTarget::Bucket { bucket } => Some(format!("{}/**", bucket_path(bucket))),
        ShareTarget::Folder { bucket, prefix } => Some(format!(
            "{}/{}**",
            bucket_path(bucket),
            globset::escape(prefix)
        )),
        ShareTarget::Object { bucket, key } => Some(exact_pattern(&object_permission_path(
            realm_id,
            link.group_id,
            node_id,
            bucket,
            key,
        ))),
        ShareTarget::Dataset { .. } => None,
    }
}

/// A pattern that matches exactly `path`, whatever characters its key holds.
pub fn exact_pattern(path: &str) -> String {
    globset::escape(path)
}

/// The creator's context for one visit request, narrowed to `pattern` and the link's mode.
pub fn visit_context(link: &ShareLink, realm_id: RealmId, pattern: String) -> AuthContext {
    AuthContext {
        user_id: link.created_by,
        realm_id,
        path_restrictions: Some(vec![PathRestriction {
            pattern,
            permission: required_permission(link.mode),
        }]),
        session: Some(SessionRef {
            sid: link.id.to_string(),
            kind: SessionKind::Share,
            name: None,
            via: None,
        }),
    }
}

/// When a visit token issued at `now` ends: an hour later, never past the link's expiry.
pub fn visit_expiry(link: &ShareLink, now: u64) -> u64 {
    let end = now.saturating_add(VISIT_SECS);
    link.expires_at.map_or(end, |expiry| end.min(expiry))
}

/// The key an upload named `name` lands at: below the folder, or at the bucket root.
pub fn upload_key(target: &ShareTarget, name: &str) -> Result<String, ShareRuleError> {
    relative_path(name)?;
    let prefix = target.prefix().ok_or(ShareRuleError::UploadTarget)?;
    Ok(format!("{prefix}{name}"))
}

/// The `n`th alternative to a taken key: `a/report (2).pdf` for `a/report.pdf`.
pub fn numbered_key(key: &str, n: u32) -> String {
    let (folder, name) = key
        .rsplit_once('/')
        .map_or(("", key), |(folder, name)| (folder, name));
    let (stem, extension) = match name.rsplit_once('.') {
        Some((stem, extension)) if !stem.is_empty() => (stem, Some(extension)),
        _ => (name, None),
    };
    let numbered = match extension {
        Some(extension) => format!("{stem} ({n}).{extension}"),
        None => format!("{stem} ({n})"),
    };
    match folder.is_empty() && !key.contains('/') {
        true => numbered,
        false => format!("{folder}/{numbered}"),
    }
}

/// Checks a visitor-given path: relative, no empty, `.` or `..` segments, no control characters.
pub fn relative_path(path: &str) -> Result<(), ShareRuleError> {
    let valid = !path.is_empty()
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..");
    valid.then_some(()).ok_or(ShareRuleError::Key)
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod tests;
