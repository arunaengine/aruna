//! Refuses to start on the demonstration environment shipped in the repository.
//! The tracked `.env` is embedded at build time and compared against the live
//! environment, so a node never serves with published keys by accident.

use std::io::Cursor;
use thiserror::Error;

// The demonstration environment as tracked in the repository, embedded by the
// build script. Empty when the build context carries no `.env`.
include!(concat!(env!("OUT_DIR"), "/shipped_env.rs"));

/// Opt-in that downgrades the refusal to a warning.
pub const OVERRIDE_FLAG: &str = "--dangerously-use-default-env";
pub const OVERRIDE_VAR: &str = "ARUNA_DANGEROUSLY_USE_DEFAULT_ENV";

/// Key suffixes whose shipped value is published key material or a secret. A
/// match on one of these cannot be a coincidence, so it blocks startup.
const SECRET_SUFFIXES: [&str; 4] = ["_KEY", "_SECRET", "_TOKEN", "_PASSWORD"];

#[derive(Debug, Error)]
#[error(
    "refusing to start on the demonstration environment: {} still {} the shipped value. \
     Set your own values, or pass {OVERRIDE_FLAG} (or {OVERRIDE_VAR}=1) to start anyway.",
    .keys.join(", "),
    if .keys.len() == 1 { "has" } else { "have" }
)]
pub struct DefaultEnvError {
    keys: Vec<String>,
}

/// One shipped value still present in the live environment.
#[derive(Debug, PartialEq, Eq)]
pub struct DefaultInUse {
    pub key: String,
    /// Published key material or a secret, which never matches by chance.
    pub secret: bool,
}

/// The shipped key/value pairs, skipping empty values because matching those
/// says nothing about the deployment.
pub fn shipped_values() -> Vec<(String, String)> {
    dotenvy::from_read_iter(Cursor::new(SHIPPED_ENV))
        .flatten()
        .filter(|(_, value)| !value.is_empty())
        .collect()
}

/// The shipped values `lookup` still reports, in the order they are shipped.
pub fn defaults_in_use(
    shipped: &[(String, String)],
    lookup: impl Fn(&str) -> Option<String>,
) -> Vec<DefaultInUse> {
    shipped
        .iter()
        .filter(|(key, value)| lookup(key).as_ref() == Some(value))
        .map(|(key, _)| DefaultInUse {
            key: key.clone(),
            secret: SECRET_SUFFIXES.iter().any(|suffix| key.ends_with(suffix)),
        })
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

/// Reports the shipped values in use. Returns an error when one of them carries
/// key material and the operator did not opt in; otherwise the caller warns.
pub fn guard(
    shipped: &[(String, String)],
    lookup: impl Fn(&str) -> Option<String> + Copy,
    args: impl Iterator<Item = String>,
) -> Result<Vec<DefaultInUse>, DefaultEnvError> {
    let in_use = defaults_in_use(shipped, lookup);
    let secrets: Vec<String> = in_use
        .iter()
        .filter(|entry| entry.secret)
        .map(|entry| entry.key.clone())
        .collect();
    if !secrets.is_empty() && !opted_in(args, lookup) {
        return Err(DefaultEnvError { keys: secrets });
    }
    Ok(in_use)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shipped() -> Vec<(String, String)> {
        vec![
            ("REALM_PUBLIC_KEY".to_string(), "demo-key".to_string()),
            ("SOCKET_ADDRESS".to_string(), "0.0.0.0:3000".to_string()),
        ]
    }

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
    fn shipped_env_parses() {
        // The embedded file is the operator-facing demonstration profile, so a
        // malformed one must not reach a release.
        let shipped = shipped_values();
        assert!(shipped.iter().any(|(key, _)| key == "REALM_PUBLIC_KEY"));
        assert!(
            shipped.iter().all(|(_, value)| !value.is_empty()),
            "empty shipped values carry no signal"
        );
    }

    #[test]
    fn secret_match_blocks() {
        let error = guard(
            &shipped(),
            env_of(&[("REALM_PUBLIC_KEY", "demo-key")]),
            std::iter::empty(),
        )
        .expect_err("published key material blocks startup");
        assert!(error.to_string().contains("REALM_PUBLIC_KEY"));
    }

    #[test]
    fn flag_allows_start() {
        let in_use = guard(
            &shipped(),
            env_of(&[("REALM_PUBLIC_KEY", "demo-key")]),
            [OVERRIDE_FLAG.to_string()].into_iter(),
        )
        .expect("the flag admits the demonstration environment");
        assert_eq!(
            in_use,
            vec![DefaultInUse {
                key: "REALM_PUBLIC_KEY".to_string(),
                secret: true,
            }]
        );
    }

    #[test]
    fn variable_allows_start() {
        guard(
            &shipped(),
            env_of(&[("REALM_PUBLIC_KEY", "demo-key"), (OVERRIDE_VAR, "1")]),
            std::iter::empty(),
        )
        .expect("the opt-in variable admits it too");
    }

    #[test]
    fn plain_match_warns() {
        // A bind address is a plausible production value, so it is reported
        // without blocking the node.
        let in_use = guard(
            &shipped(),
            env_of(&[("SOCKET_ADDRESS", "0.0.0.0:3000")]),
            std::iter::empty(),
        )
        .expect("a non-secret match does not block");
        assert_eq!(in_use.len(), 1);
        assert!(!in_use[0].secret);
    }

    #[test]
    fn own_values_pass() {
        let in_use = guard(
            &shipped(),
            env_of(&[("REALM_PUBLIC_KEY", "operator-key")]),
            std::iter::empty(),
        )
        .expect("own values start silently");
        assert!(in_use.is_empty());
    }
}
