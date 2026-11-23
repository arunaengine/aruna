//! File name patterns of discovery rules: `*`, `?`, character sets, alternatives and counts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashSet;

/// Alternatives one `{...}` group may hold.
pub const MAX_OPTIONS: usize = 8;
/// Names the alternatives of one pattern may spell out together.
pub const MAX_EXPANSIONS: usize = 32;
/// Largest repeat count.
pub const MAX_COUNT: usize = 64;

/// One character a pattern accepts at a position.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Class {
    /// `?` or the characters of `*`: any character.
    Any,
    /// A literal or a `[...]` set; `negated` for `[!...]`.
    Set {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
}

impl Class {
    fn literal(c: char) -> Self {
        Self::Set {
            negated: false,
            ranges: vec![(c, c)],
        }
    }

    fn accepts(&self, c: char) -> bool {
        if c == '/' {
            return false;
        }
        match self {
            Self::Any => true,
            Self::Set { negated, ranges } => {
                ranges.iter().any(|(lo, hi)| (*lo..=*hi).contains(&c)) != *negated
            }
        }
    }

    /// Whether some character of a file name, never `/`, is accepted by both.
    fn meets(&self, other: &Self) -> bool {
        let named = |(lo, hi): (char, char)| (lo, hi) != ('/', '/');
        match (self, other) {
            (Self::Any, Self::Any) | (Self::Any, Self::Set { negated: true, .. }) => true,
            (Self::Set { negated: true, .. }, Self::Any | Self::Set { negated: true, .. }) => true,
            (Self::Any, Self::Set { ranges, .. }) | (Self::Set { ranges, .. }, Self::Any) => {
                ranges.iter().any(|range| named(*range))
            }
            (
                Self::Set {
                    negated: false,
                    ranges,
                },
                set @ Self::Set { negated: true, .. },
            )
            | (
                set @ Self::Set { negated: true, .. },
                Self::Set {
                    negated: false,
                    ranges,
                },
            ) => ranges.iter().any(|range| !covered(*range, set)),
            (Self::Set { ranges: left, .. }, Self::Set { ranges: right, .. }) => {
                left.iter().any(|(a, b)| {
                    let both = |(c, d): &(char, char)| (*a.max(c), *b.min(d));
                    right
                        .iter()
                        .map(both)
                        .any(|(lo, hi)| lo <= hi && named((lo, hi)))
                })
            }
        }
    }
}

/// Whether the negated `set` refuses every file name character of `range`; `/` never counts.
fn covered((lo, hi): (char, char), set: &Class) -> bool {
    let Class::Set { ranges, .. } = set else {
        return false;
    };
    let mut ranges = ranges.clone();
    ranges.push(('/', '/'));
    ranges.sort();
    let mut next = lo as u32;
    for (start, end) in ranges {
        if start as u32 > next {
            break;
        }
        next = next.max(end as u32 + 1);
    }
    next > hi as u32
}

/// A pattern as a graph of states: edges consume one character, jumps consume none.
#[derive(Debug, Default)]
struct Machine {
    edges: Vec<Vec<(Class, usize)>>,
    jumps: Vec<Vec<usize>>,
}

impl Machine {
    fn state(&mut self) -> usize {
        self.edges.push(Vec::new());
        self.jumps.push(Vec::new());
        self.edges.len() - 1
    }

    fn accept(&self) -> usize {
        self.edges.len() - 1
    }

    /// Every state reachable from `states` without consuming a character.
    fn close(&self, states: impl IntoIterator<Item = usize>) -> Vec<usize> {
        let mut seen = HashSet::new();
        let mut stack: Vec<usize> = states.into_iter().collect();
        while let Some(state) = stack.pop() {
            if seen.insert(state) {
                stack.extend(&self.jumps[state]);
            }
        }
        seen.into_iter().collect()
    }
}

/// Parses `pattern` into its state graph, or says why it is malformed.
fn compile(pattern: &str) -> Result<Machine, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut machine = Machine::default();
    let mut current = machine.state();
    let (mut expansions, mut at) = (1usize, 0);
    // The class just read by `?` or `[...]`, which a count may repeat.
    let mut last: Option<(Class, usize)> = None;
    while at < chars.len() {
        let c = chars[at];
        at += 1;
        match c {
            '*' => {
                let star = machine.state();
                machine.jumps[current].push(star);
                machine.edges[star].push((Class::Any, star));
                current = star;
                last = None;
            }
            '?' => {
                let next = machine.state();
                machine.edges[current].push((Class::Any, next));
                last = Some((Class::Any, current));
                current = next;
            }
            '[' => {
                let (class, end) = parse_set(&chars, at)?;
                at = end;
                let next = machine.state();
                machine.edges[current].push((class.clone(), next));
                last = Some((class, current));
                current = next;
            }
            '{' => {
                let end = (chars[at..].iter().position(|c| *c == '}'))
                    .map(|end| at + end)
                    .ok_or_else(|| format!("`{{` in `{pattern}` is not closed"))?;
                let body: String = chars[at..end].iter().collect();
                at = end + 1;
                if let Some((class, from)) = last.take().filter(|_| is_count(&body)) {
                    current = repeat(&mut machine, from, class, &body)?;
                    continue;
                }
                let options: Vec<&str> = body.split(',').collect();
                let special = |option: &&str| option.contains(['*', '?', '[', ']', '{', '}']);
                if options.len() < 2 || options.iter().any(|o| o.is_empty() || special(o)) {
                    return Err(format!(
                        "`{{{body}}}` needs two or more plain alternatives, or a count after `?` \
                         or `]`"
                    ));
                }
                expansions = expansions.saturating_mul(options.len());
                if options.len() > MAX_OPTIONS || expansions > MAX_EXPANSIONS {
                    return Err(format!(
                        "`{pattern}` may hold {MAX_OPTIONS} alternatives in a group that spell \
                         out at most {MAX_EXPANSIONS} names"
                    ));
                }
                let join = machine.state();
                for option in options {
                    let mut state = current;
                    for c in option.chars() {
                        let next = machine.state();
                        machine.edges[state].push((Class::literal(c), next));
                        state = next;
                    }
                    machine.jumps[state].push(join);
                }
                current = join;
            }
            ']' | '}' => return Err(format!("`{c}` in `{pattern}` has no opening bracket")),
            c => {
                let next = machine.state();
                machine.edges[current].push((Class::literal(c), next));
                current = next;
                last = None;
            }
        }
        if !matches!(c, '?' | '[') {
            last = None;
        }
    }
    let accept = machine.state();
    machine.jumps[current].push(accept);
    Ok(machine)
}

/// A `[...]` set starting after its `[`, and the position after its `]`.
fn parse_set(chars: &[char], mut at: usize) -> Result<(Class, usize), String> {
    let negated = chars.get(at) == Some(&'!');
    at += usize::from(negated);
    let start = at;
    let mut ranges = Vec::new();
    while let Some(&c) = chars.get(at) {
        if c == ']' && at > start {
            return Ok((Class::Set { negated, ranges }, at + 1));
        }
        match (chars.get(at + 1), chars.get(at + 2)) {
            (Some('-'), Some(&end)) if end != ']' => {
                if end < c {
                    return Err(format!("the range `{c}-{end}` runs backwards"));
                }
                ranges.push((c, end));
                at += 3;
            }
            _ => {
                ranges.push((c, c));
                at += 1;
            }
        }
    }
    Err("a `[` set is not closed with `]`".to_string())
}

/// Whether a brace body is a count: digits, or two numbers with one comma.
fn is_count(body: &str) -> bool {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
    match body.split_once(',') {
        Some((min, max)) => digits(min) && digits(max),
        None => digits(body),
    }
}

/// Replaces the single edge from `from` with `min` to `max` copies of `class`.
fn repeat(machine: &mut Machine, from: usize, class: Class, body: &str) -> Result<usize, String> {
    let (min, max) = match body.split_once(',') {
        Some((min, max)) => (min.parse::<usize>(), max.parse::<usize>()),
        None => (body.parse::<usize>(), body.parse::<usize>()),
    };
    let (Ok(min), Ok(max)) = (min, max) else {
        return Err(format!("the count `{{{body}}}` is not a number"));
    };
    if max == 0 || max > MAX_COUNT || min > max {
        return Err(format!(
            "the count `{{{body}}}` must be 1 to {MAX_COUNT} with the smaller number first"
        ));
    }
    machine.edges[from].pop();
    let end = machine.state();
    let mut current = from;
    for index in 0..max {
        if index >= min {
            machine.jumps[current].push(end);
        }
        let next = machine.state();
        machine.edges[current].push((class.clone(), next));
        current = next;
    }
    machine.jumps[current].push(end);
    Ok(end)
}

/// Checks that `pattern` is well formed and within the limits.
pub fn check_glob(pattern: &str) -> Result<(), String> {
    compile(pattern).map(|_| ())
}

/// Matches the whole name against the pattern; a malformed pattern matches nothing.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let Ok(machine) = compile(pattern) else {
        return false;
    };
    let mut states = machine.close([0]);
    for c in name.chars() {
        let next = states.iter().flat_map(|state| &machine.edges[*state]);
        let next = next
            .filter(|(class, _)| class.accepts(c))
            .map(|(_, to)| *to);
        states = machine.close(next.collect::<Vec<_>>());
        if states.is_empty() {
            return false;
        }
    }
    states.contains(&machine.accept())
}

/// Whether some name matches both patterns, by walking both graphs side by side.
pub fn globs_overlap(left: &str, right: &str) -> bool {
    let (Ok(a), Ok(b)) = (compile(left), compile(right)) else {
        return false;
    };
    let mut seen = HashSet::new();
    let mut stack = vec![(0, 0)];
    while let Some((i, j)) = stack.pop() {
        let pairs = a.close([i]).into_iter().flat_map(|i| {
            let b = &b;
            b.close([j]).into_iter().map(move |j| (i, j))
        });
        for (i, j) in pairs.collect::<Vec<_>>() {
            if !seen.insert((i, j)) {
                continue;
            }
            if i == a.accept() && j == b.accept() {
                return true;
            }
            for (x, to_a) in &a.edges[i] {
                for (y, to_b) in &b.edges[j] {
                    if x.meets(y) {
                        stack.push((*to_a, *to_b));
                    }
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_sets() {
        assert!(glob_match("sample[0-9].txt", "sample7.txt"));
        assert!(!glob_match("sample[0-9].txt", "samplex.txt"));
        assert!(glob_match("[abc]*", "beta"));
        assert!(!glob_match("[!abc]*", "beta"));
        assert!(glob_match("[!abc]*", "delta"));
        // A `-` first or last is literal, as is `]` right after the opening bracket.
        assert!(glob_match("[-a]x", "-x"));
        assert!(glob_match("[]]x", "]x"));
    }

    #[test]
    fn matches_alternatives() {
        assert!(glob_match("*.{fna,fa}.gz", "s.fa.gz"));
        assert!(glob_match("*.{fna,fa}.gz", "s.fna.gz"));
        assert!(!glob_match("*.{fna,fa}.gz", "s.faa.gz"));
    }

    #[test]
    fn matches_counts() {
        assert!(glob_match("run[0-9]{4}.csv", "run2024.csv"));
        assert!(!glob_match("run[0-9]{4}.csv", "run202.csv"));
        assert!(glob_match("?{2,3}.x", "ab.x"));
        assert!(glob_match("?{2,3}.x", "abc.x"));
        assert!(!glob_match("?{2,3}.x", "a.x"));
        assert!(!glob_match("?{2,3}.x", "abcd.x"));
        // A brace after a literal is alternatives, never a count.
        assert!(check_glob("a{4}").is_err());
    }

    #[test]
    fn rejects_malformed() {
        for pattern in [
            "[abc",
            "abc]",
            "{a,b",
            "a}",
            "[]",
            "[z-a]",
            "{fna}",
            "{a,}",
            "{a,*}",
            "?{0}",
            "?{65}",
            "?{3,2}",
            "{a,b}{a,b}{a,b}{a,b}{a,b}{a,b}",
            "{a,b,c,d,e,f,g,h,i}",
        ] {
            assert!(check_glob(pattern).is_err(), "{pattern}");
        }
        assert!(check_glob("{a,b}{a,b}{a,b}{a,b}{a,b}").is_ok());
        assert!(check_glob("{a,b,c,d,e,f,g,h}{x,y,z,w}").is_ok());
        assert!(check_glob("?{64}").is_ok());
    }

    #[test]
    fn checks_overlaps() {
        assert!(globs_overlap("*.{fna,fa}", "*.fa"));
        assert!(!globs_overlap("*.{fna,fa}", "*.faa"));
        assert!(globs_overlap("[a-f]*", "[d-z]*"));
        assert!(!globs_overlap("[a-c]*", "[d-z]*"));
        assert!(globs_overlap("[!a]*", "b*"));
        assert!(!globs_overlap("[!a-z]*", "[b-y]*"));
        assert!(globs_overlap("[!a]*", "[!b]*"));
        assert!(globs_overlap("?{2,4}", "abc"));
        assert!(!globs_overlap("?{2,4}", "abcde"));
        assert!(!globs_overlap("[0-9]{4}", "[0-9]{3}"));
        // A file name never holds `/`, also where a range or negation spans it.
        assert!(!globs_overlap("[.-0].txt", "[!.0].txt"));
        assert!(!globs_overlap("[.-0]", "[/]"));
        assert!(globs_overlap("[.-0].txt", "[!.].txt"));
    }
}
