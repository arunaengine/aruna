//! Builds the identity, group and credential tests and their shared fixtures into one test binary.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "512"]

#[path = "operations/access_replication.rs"]
mod access_replication;
#[path = "operations/auth_validation.rs"]
mod auth_validation;
#[path = "operations/convergence/mod.rs"]
mod convergence;
mod credentials;
#[path = "operations/group_replication.rs"]
mod group_replication;
mod groups;
mod oidc_registration;
mod onboarding;
mod promotion;
mod self_service;
mod service_credentials;
mod shared;
#[path = "operations/token_revocation.rs"]
mod token_revocation;
#[path = "operations/topology/mod.rs"]
mod topology;
mod user_routes;
