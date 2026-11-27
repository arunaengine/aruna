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

/// Validates a copy destination template and expands only `{date}`, `{run_id}` and `{filename}`.
pub fn expand_copy(
    template: &str,
    date: &str,
    run_id: &str,
    filename: &str,
) -> Result<String, String> {
    if template.is_empty() || template.starts_with('/') || template.contains('\\') {
        return Err("copy destination template must be a relative key".into());
    }
    if template.contains('\0') {
        return Err("copy destination template contains a null byte".into());
    }
    let clean_name = filename.rsplit('/').next().unwrap_or_default();
    if clean_name.is_empty() || clean_name == "." || clean_name == ".." {
        return Err("copy filename must be a plain file name".into());
    }
    let mut out = String::with_capacity(template.len() + 32);
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err("unclosed placeholder in copy template".into());
        };
        let token = &after[..close];
        match token {
            "date" => out.push_str(date),
            "run_id" => out.push_str(run_id),
            "filename" => out.push_str(clean_name),
            other => {
                return Err(format!(
                    "unsupported copy placeholder `{{{other}}}`; allowed: date, run_id, filename"
                ));
            }
        }
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        return Err("unmatched closing brace in copy template".into());
    }
    out.push_str(rest);
    if out.is_empty()
        || out.starts_with('/')
        || out
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err("expanded copy key escapes or has empty path segments".into());
    }
    Ok(out)
}

/// Formats UTC calendar date `YYYY-MM-DD` from Unix epoch milliseconds.
pub fn format_date(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let mut year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    if month <= 2 {
        year += 1;
    }
    format!("{year:04}-{month:02}-{day:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_globs_safely() {
        assert!(glob_match("*.fastq.gz", "sample_1.fastq.gz"));
        assert!(!glob_match("*.fastq.gz", "sample_1.bam"));
        assert!(glob_match("run-?.csv", "run-7.csv"));
        assert!(!glob_match("run-?.csv", "run-77.csv"));
        assert!(glob_match("a*b*c", "axxxbyyyc"));
    }

    #[test]
    fn expands_copy_safely() {
        let key = expand_copy(
            "delivered/{date}/{run_id}/{filename}",
            "2026-11-26",
            "01K",
            "dir/out.tsv",
        )
        .unwrap();
        assert_eq!(key, "delivered/2026-11-26/01K/out.tsv");
        assert!(expand_copy("delivered/{unknown}/{filename}", "2026-11-26", "01K", "a").is_err());
        assert!(expand_copy("../escape/{filename}", "2026-11-26", "01K", "a").is_err());
        assert!(expand_copy("/abs/{filename}", "2026-11-26", "01K", "a").is_err());
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(1_764_115_200_000), "2025-11-26");
    }
}
