//! Groups the storage routes: blobs, bucket usage, compression, connectors, backends, imports,
//! routing and deletion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod abe;
pub mod blobs;
pub mod bucket;
pub mod checks;
pub mod compression;
pub mod connectors;
pub mod deletion;
pub mod discovery;
pub mod distributed;
pub mod encryption;
pub mod group_backends;
pub mod imports;
pub mod moves;
pub mod organize;
pub mod relay;
pub mod rename;
pub mod routing;
