//! Groups the document sync outbox records and the decisions which drained records publish now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod defer;
pub mod record;
pub mod rotation;
