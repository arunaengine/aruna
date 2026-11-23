//! Groups the identity, group and credential tests under one test regime.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[path = "operations/access_replication.rs"]
mod access_replication;
#[path = "operations/auth_validation.rs"]
mod auth_validation;
#[path = "credentials.rs"]
mod credentials;
#[path = "operations/group_replication.rs"]
mod group_replication;
#[path = "groups.rs"]
mod groups;
#[path = "oidc_registration.rs"]
mod oidc_registration;
#[path = "onboarding.rs"]
mod onboarding;
#[path = "promotion.rs"]
mod promotion;
#[path = "self_service.rs"]
mod self_service;
#[path = "service_credentials.rs"]
mod service_credentials;
#[path = "operations/token_revocation.rs"]
mod token_revocation;
#[path = "user_routes.rs"]
mod user_routes;
