//! Collects the lifecycle test modules so they compile as one test tree.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

mod admission;
mod admission_race;
mod cancel;
mod capacity;
mod family_reads;
mod report;
mod scheduling;
mod terminal;
mod uncertain_commit;
