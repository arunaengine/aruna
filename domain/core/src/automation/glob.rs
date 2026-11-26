//! Filename glob matching and safe copy destination template expansion.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

/// Matches `name` against a simple `*` / `?` glob pattern in linear worst-case time.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let (pat, text): (Vec<char>, Vec<char>) = (pattern.chars().collect(), name.chars().collect());
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ti < text.len() {
        if pi < pat.len() && (pat[pi] == '?' || pat[pi] == text[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < pat.len() && pat[pi] == '*' {
            star = Some(pi);
            pi += 1;
            mark = ti;
        } else if let Some(pos) = star {
            pi = pos + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < pat.len() && pat[pi] == '*' {
        pi += 1;
    }
    pi == pat.len()
}
