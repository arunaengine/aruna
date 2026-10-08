//! Prints local timings and encoded sizes of parameters, ciphertexts and keys on request.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use alloc::vec::Vec;

use rand::{SeedableRng, rngs::StdRng};
use std::time::Instant;

use crate::encoding::encode_key;
use crate::*;

#[test]
#[ignore = "prints local timings and canonical encoded sizes"]
fn measurements() {
    let mut rng = StdRng::from_seed([3; 32]);
    let start = Instant::now();
    let (parameters, master) = setup_from_seed(&[7; 32], b"measurement bucket").unwrap();
    std::println!(
        "setup_us={} parameter_bytes={}",
        start.elapsed().as_micros(),
        parameters.to_bytes().len()
    );
    let (single, _) = encapsulate(&parameters, &[Attribute::Epoch(1)], &mut rng).unwrap();
    std::println!(
        "attributes=1 rows=unsupported ciphertext_bytes={} issue_us=unsupported decapsulate_us=unsupported",
        single.to_bytes().unwrap().len()
    );
    for rows in [2usize, 8, 32] {
        let scopes: Vec<_> = (0..rows - 2)
            .map(|index| Attribute::Write([index as u8; 16]))
            .collect();
        let policy = Policy::new(b"bucket", &[1], &scopes).unwrap();
        let attributes = policy.labels();
        let start = Instant::now();
        let key = issue(&parameters, &master, &policy, &mut rng).unwrap();
        let issue_us = start.elapsed().as_micros();
        let (ciphertext, shared) = encapsulate(&parameters, &attributes, &mut rng).unwrap();
        let start = Instant::now();
        let recovered = decapsulate(&parameters, &key, &ciphertext).unwrap();
        let decapsulate_us = start.elapsed().as_micros();
        assert_eq!(
            shared.derive(b"measurement write").unwrap().as_bytes(),
            recovered.derive(b"measurement write").unwrap().as_bytes()
        );
        std::println!(
            "attributes={} rows={} key_bytes={} ciphertext_bytes={} issue_us={} decapsulate_us={}",
            attributes.len(),
            rows,
            encode_key(&key).unwrap().len(),
            ciphertext.to_bytes().unwrap().len(),
            issue_us,
            decapsulate_us
        );
    }
}
