//! Refuses to start when the environment still holds a published demonstration key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;

/// Opt-in that downgrades the refusal to a warning.
pub const OVERRIDE_FLAG: &str = "--dangerously-use-default-env";
pub const OVERRIDE_VAR: &str = "ARUNA_DANGEROUSLY_USE_DEFAULT_ENV";

/// PEM bodies of the demonstration keys the tracked `.env` published. The private
/// keys shipped until 2026-09-12, so a copied old profile may still carry them.
const PUBLISHED_KEYS: [(&str, &str); 4] = [
    (
        "REALM_PUBLIC_KEY",
        "MCowBQYDK2VwAyEArYMI2Y2/rCqHcjjWLPxFDmKW8aqk6P+y8TujKj+fT9Q=",
    ),
    (
        "NODE_PUBLIC_KEY",
        "MCowBQYDK2VwAyEAgdoLXqmUs0dxmMBKMwJProc3jJi4GJ1Hh9cbXtnpqB4=",
    ),
    (
        "REALM_PRIVATE_KEY",
        "MC4CAQAwBQYDK2VwBCIEIIMoiSLBgtFREb89XfBV/I0DcpCrGmMk9BmeAMyMPwRp",
    ),
    (
        "NODE_PRIVATE_KEY",
        "MC4CAQAwBQYDK2VwBCIEIFpQrCQTORNWj+EAGWWNXRdO8csaJgfzc8KzMU6GvHGx",
    ),
];

#[derive(Debug, Error)]
#[error(
    "refusing to start on the demonstration environment: {} still {} the published key. \
     Set your own keys, or pass {OVERRIDE_FLAG} (or {OVERRIDE_VAR}=1) to start anyway.",
    .keys.join(", "),
    if .keys.len() == 1 { "has" } else { "have" }
)]
pub struct DefaultEnvError {
    keys: Vec<&'static str>,
}

/// The published keys `lookup` still reports, in list order. Only the PEM body
/// counts, so a copy with other line breaks or indentation still matches.
pub fn keys_in_use(lookup: impl Fn(&str) -> Option<String>) -> Vec<&'static str> {
    PUBLISHED_KEYS
        .iter()
        .filter(|(key, body)| lookup(key).is_some_and(|value| pem_body(&value) == *body))
        .map(|(key, _)| *key)
        .collect()
}

fn pem_body(value: &str) -> String {
    value
        .lines()
        .filter(|line| !line.trim_start().starts_with("-----"))
        .flat_map(str::split_whitespace)
        .collect()
}

/// Whether the operator accepted the demonstration environment explicitly.
pub fn opted_in(
    mut args: impl Iterator<Item = String>,
    lookup: impl Fn(&str) -> Option<String>,
) -> bool {
    args.any(|arg| arg == OVERRIDE_FLAG)
        || lookup(OVERRIDE_VAR)
            .is_some_and(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

/// Returns the published keys in use, or an error when there is one and the
/// operator did not opt in. The caller warns for each admitted key.
pub fn guard(
    lookup: impl Fn(&str) -> Option<String> + Copy,
    args: impl Iterator<Item = String>,
) -> Result<Vec<&'static str>, DefaultEnvError> {
    let keys = keys_in_use(lookup);
    if !keys.is_empty() && !opted_in(args, lookup) {
        return Err(DefaultEnvError { keys });
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    const REALM_PEM: &str = "-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEArYMI2Y2/rCqHcjjWLPxFDmKW8aqk6P+y8TujKj+fT9Q=
-----END PUBLIC KEY-----";

    fn env_of(
        pairs: &'static [(&'static str, &'static str)],
    ) -> impl Fn(&str) -> Option<String> + Copy {
        move |key: &str| {
            pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).to_string())
        }
    }

    #[test]
    fn published_key_blocks() {
        let error = guard(
            env_of(&[("REALM_PUBLIC_KEY", REALM_PEM)]),
            std::iter::empty(),
        )
        .expect_err("a published key blocks startup");
        assert!(error.to_string().contains("REALM_PUBLIC_KEY"));
    }

    #[test]
    fn private_key_blocks() {
        // The formerly shipped private key matters most: it signs as the demo realm.
        let pem = "-----BEGIN PRIVATE KEY-----\n\
                   MC4CAQAwBQYDK2VwBCIEIIMoiSLBgtFREb89XfBV/I0DcpCrGmMk9BmeAMyMPwRp\n\
                   -----END PRIVATE KEY-----";
        let lookup = move |key: &str| (key == "REALM_PRIVATE_KEY").then(|| pem.to_string());
        let error = guard(lookup, std::iter::empty()).expect_err("a published key blocks");
        assert!(error.to_string().contains("REALM_PRIVATE_KEY"));
    }

    #[test]
    fn reformatted_key_matches() {
        let reformatted = format!("  {}  ", REALM_PEM.replace('\n', "\r\n    "));
        let lookup = move |key: &str| (key == "REALM_PUBLIC_KEY").then(|| reformatted.clone());
        assert_eq!(keys_in_use(lookup), vec!["REALM_PUBLIC_KEY"]);
    }

    #[test]
    fn flag_allows_start() {
        let keys = guard(
            env_of(&[("REALM_PUBLIC_KEY", REALM_PEM)]),
            [OVERRIDE_FLAG.to_string()].into_iter(),
        )
        .expect("the flag admits the demonstration keys");
        assert_eq!(keys, vec!["REALM_PUBLIC_KEY"]);
    }

    #[test]
    fn variable_allows_start() {
        guard(
            env_of(&[("REALM_PUBLIC_KEY", REALM_PEM), (OVERRIDE_VAR, "1")]),
            std::iter::empty(),
        )
        .expect("the opt-in variable admits them too");
    }

    #[test]
    fn own_values_pass() {
        // Other settings, such as an onboarding secret, never count as published.
        let keys = guard(
            env_of(&[
                ("REALM_PUBLIC_KEY", "operator-key"),
                ("ONBOARDING_SECRET", "own-secret"),
            ]),
            std::iter::empty(),
        )
        .expect("own values start silently");
        assert!(keys.is_empty());
    }
}
