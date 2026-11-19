//! Groups the shared test helper modules of the operations tests. The export, notification, S3,
//! shard and staging fixtures are public under `test-hooks` for the node crate's tests.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(test)]
pub(crate) mod calls;
#[cfg(test)]
pub(crate) mod device;
pub mod export;
#[cfg(test)]
pub(crate) mod import;
pub mod locations;
#[cfg(test)]
pub(crate) mod metadata;
#[cfg(test)]
pub(crate) mod metadata_api;
pub mod notifications;
#[cfg(test)]
pub(crate) mod peers;
#[cfg(test)]
pub(crate) mod policy;
#[cfg(test)]
pub(crate) mod records;
pub mod s3;
pub mod shard;
pub mod staging;
