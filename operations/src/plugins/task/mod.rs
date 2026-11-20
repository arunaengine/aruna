//! Groups the container tasks a plugin run starts: the job spec, the start, the task row
//! changes, sending, settling, cancelling and the maintenance pass.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod cancel;
pub mod change;
pub mod pass;
pub mod send;
pub mod settle;
pub mod spec;
pub mod start;
