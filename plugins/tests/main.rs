//! Integration tests for plugin packaging, runtime execution, languages, and examples.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#[cfg(all(feature = "runtime", feature = "build"))]
mod common;
#[cfg(all(feature = "runtime", feature = "build"))]
mod elabftw_python;
#[cfg(all(feature = "runtime", feature = "build"))]
mod examples;
#[cfg(all(feature = "runtime", feature = "build"))]
mod languages;
#[cfg(feature = "build")]
mod package;
#[cfg(feature = "runtime")]
mod runtime;
