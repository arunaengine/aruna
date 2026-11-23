//! Draws orders from the entropy the runner gives an operation run, so an operation never reads
//! a random source.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Puts `items` in an order drawn from `entropy`; the same entropy always gives the same order.
pub fn shuffle<T>(items: &mut [T], entropy: u128) {
    let mut stream = blake3::Hasher::new()
        .update(&entropy.to_le_bytes())
        .finalize_xof();
    for last in (1..items.len()).rev() {
        let mut bytes = [0; 8];
        stream.fill(&mut bytes);
        let pick = u64::from_le_bytes(bytes) % (last as u64 + 1);
        items.swap(last, pick as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::shuffle;

    #[test]
    fn same_entropy_agrees() {
        let mut first: Vec<u32> = (0..16).collect();
        let mut second = first.clone();
        shuffle(&mut first, 7);
        shuffle(&mut second, 7);
        assert_eq!(first, second);
        let mut sorted = first.clone();
        sorted.sort();
        assert_eq!(sorted, (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn entropy_moves_first() {
        let firsts: std::collections::BTreeSet<u32> = (0..64u128)
            .map(|entropy| {
                let mut items: Vec<u32> = (0..4).collect();
                shuffle(&mut items, entropy << 16);
                items[0]
            })
            .collect();
        assert_eq!(firsts.len(), 4, "every item comes first for some entropy");
    }
}
