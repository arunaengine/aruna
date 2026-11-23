#!/usr/bin/env python3
"""Self-tests for check_layers.py. Plain Python on small fixture trees; no Rust toolchain or build."""
# Copyright (c) 2026 The Aruna Contributors
# SPDX-License-Identifier: MIT or Apache-2.0
import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import check_layers
import check_style

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKSPACE = '[workspace]\nmembers = ["domain/ops-work"]\n'
MANIFEST = '[package]\nname = "aruna-ops-work"\n\n[dependencies]\naruna-core = { workspace = true }\n'
PURE_OP = "pub struct Plan;\nimpl Operation for Plan {}\n"
SIX_LINES = "    let a = 1;\n    let b = 2;\n    let c = 3;\n    let d = 4;\n    let e = 5;\n    let f = a + b + c + d + e;\n"


def write_tree(root, files):
    for rel, text in files.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)


# The dispatch shells the trigger guard expects, each with one arm that only drives.
TRIGGER_SITES = {
    **{
        rel: f"fn serve(on: Kind) {{\n    match on {{\n        {prefix}One => {{}}\n    }}\n}}\n"
        for rel, prefix in check_layers.TRIGGER_MATCHES
    },
    "apps/node/src/plugin_host/broker.rs": "impl Broker for RunBroker {}\n",
}


def base_tree(extra=None):
    files = {
        **TRIGGER_SITES,
        "Cargo.toml": WORKSPACE,
        "domain/ops-work/Cargo.toml": MANIFEST,
        "domain/ops-work/src/lib.rs": "pub mod abe;\npub mod jobs;\n",
        "domain/ops-work/src/abe/mod.rs": PURE_OP,
        "domain/ops-work/src/jobs/mod.rs": "pub async fn run() {}\n",
    }
    files.update(extra or {})
    return files


def pure_tree(extra=None):
    """The base tree without its async module, as production operations must be."""
    files = base_tree({"domain/ops-work/src/jobs/mod.rs": "pub fn run() {}\n"})
    files.update(extra or {})
    return files


def zero_baseline():
    return {
        "crate_allow": {},
        "structural": {key: 0 for key, _ in check_layers.COUNTS},
    }


def run_report(files, baseline):
    with tempfile.TemporaryDirectory() as root:
        write_tree(root, files)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            errors = check_layers.report(root, baseline, False)
    return errors, output.getvalue()


def ops_for(files):
    with tempfile.TemporaryDirectory() as root:
        write_tree(root, files)
        sources = [check_layers.load_source(root, rel) for rel in check_layers.rust_files(root)]
    return check_layers.Operations(sources)


class MaskTest(unittest.TestCase):
    def test_comments(self):
        code, masked = check_layers.mask_rust('let a = "x // y"; // .await\n/* async fn */ b\n')
        self.assertNotIn(".await", code)
        self.assertNotIn("async", code)
        self.assertIn('"x // y"', code)
        self.assertNotIn("x // y", masked)
        self.assertEqual(code.count("\n"), 2)

    def test_raw_strings(self):
        _, masked = check_layers.mask_rust('let a = r#"say ".await" now"#; a.await\n')
        self.assertEqual(masked.count(".await"), 1)

    def test_char_lifetime(self):
        _, masked = check_layers.mask_rust("fn f<'a>(x: &'a str) -> char { '{' }\n")
        self.assertIn("'a", masked)
        self.assertEqual(masked.count("{"), 1)


class SpansTest(unittest.TestCase):
    def strip(self, text):
        _, masked = check_layers.mask_rust(text)
        return check_layers.blank(masked, check_layers.test_spans(masked))

    def test_cfg_module(self):
        text = "fn keep() {}\n#[cfg(test)]\nmod tests {\n    async fn probe() { x.await }\n}\n"
        stripped = self.strip(text)
        self.assertIn("fn keep", stripped)
        self.assertNotIn("await", stripped)
        self.assertEqual(stripped.count("\n"), text.count("\n"))

    def test_test_fns(self):
        text = "#[tokio::test(start_paused = true)]\n#[ignore]\nasync fn probe() { x.await }\nasync fn real() {}\n"
        stripped = self.strip(text)
        self.assertNotIn("probe", stripped)
        self.assertIn("async fn real", stripped)

    def test_file_attr(self):
        self.assertEqual(self.strip("#![cfg(test)]\nasync fn probe() {}\n").strip(), "")

    def test_test_paths(self):
        self.assertTrue(check_layers.is_test("domain/ops-work/tests/main.rs"))
        self.assertTrue(check_layers.is_test("domain/ops-work/src/realm_vault_tests.rs"))
        self.assertTrue(check_layers.is_test("domain/ops-work/src/jobs/tests_outbox.rs"))
        self.assertTrue(check_layers.is_test("domain/ops-work/src/metadata/store_tests/mod.rs"))
        self.assertFalse(check_layers.is_test("domain/ops-work/src/jobs/testing.rs"))


class ClassifyTest(unittest.TestCase):
    def test_kinds(self):
        ops = ops_for(
            base_tree(
                {
                    "domain/ops-work/src/abe/mixed.rs": PURE_OP + "async fn helper() {}\n",
                    "domain/ops-work/src/abe/plain.rs": "pub fn plain() {}\n// .await in a comment\n",
                    "domain/ops-work/src/abe/sub.rs": "impl<T: Send> SubOperation for Step<T> {}\n",
                    "domain/ops-work/src/abe/tight.rs": "fn f() { let a = async{}; let b = async move{}; }\n",
                }
            )
        )
        kinds = {rel: info["kind"] for rel, info in ops.files.items()}
        self.assertEqual(kinds["domain/ops-work/src/abe/mod.rs"], "op")
        self.assertEqual(kinds["domain/ops-work/src/abe/mixed.rs"], "mixed")
        self.assertEqual(kinds["domain/ops-work/src/abe/plain.rs"], "plain")
        self.assertEqual(kinds["domain/ops-work/src/abe/sub.rs"], "op")
        self.assertEqual(kinds["domain/ops-work/src/abe/tight.rs"], "async")
        self.assertEqual(kinds["domain/ops-work/src/jobs/mod.rs"], "async")
        self.assertEqual(check_layers.async_lines(ops), {"abe": 4, "jobs": 1})

    def test_use_paths(self):
        paths = check_layers.use_paths("crate::a::{b, c::{d as e, self}, f::*}")
        self.assertEqual(
            paths,
            [("crate", "a", "b"), ("crate", "a", "c", "d"), ("crate", "a", "c", "self"), ("crate", "a", "f", "*")],
        )


class ReferencesTest(unittest.TestCase):
    def counts(self, extra):
        ops = ops_for(base_tree(extra))
        return check_layers.layer_counts(ops)

    def test_pure_async(self):
        counts, details = self.counts({"domain/ops-work/src/abe/mod.rs": PURE_OP + "use crate::jobs::run;\n"})
        self.assertEqual(counts["pure_async_refs"], 1)
        self.assertEqual(details["pure_async_refs"], ["domain/ops-work/src/abe/mod.rs -> domain/ops-work/src/jobs/mod.rs"])
        self.assertEqual(counts["upward_refs"], 1)

    def test_super_paths(self):
        counts, _ = self.counts(
            {
                "domain/ops-work/src/abe/mod.rs": PURE_OP,
                "domain/ops-work/src/abe/inner.rs": "fn f() { super::super::jobs::run(); }\nuse super::Plan;\n",
            }
        )
        self.assertEqual(counts["upward_refs"], 0)
        self.assertEqual(counts["pure_async_refs"], 0)
        counts, _ = self.counts({"domain/ops-work/src/abe/inner.rs": "use super::super::jobs::run;\n"})
        self.assertEqual(counts["pure_async_refs"], 1)

    def test_split_crates(self):
        counts, details = self.counts(
            {
                "domain/ops-identity/src/auth/mod.rs": "pub struct A;\n",
                "domain/ops-data/src/s3/mod.rs": "use aruna_ops_identity::auth::A;\npub struct B;\n",
                "domain/ops-work/src/jobs/mod.rs": "use aruna_ops_data::s3::B;\n",
            }
        )
        self.assertEqual(counts["upward_refs"], 0, details["upward_refs"])
        counts, _ = self.counts(
            {
                "domain/ops-identity/src/auth/mod.rs": "use aruna_ops_data::s3::B;\n",
                "domain/ops-data/src/s3/mod.rs": "pub struct B;\n",
            }
        )
        self.assertEqual(counts["upward_refs"], 1)

    def test_group_cycles(self):
        counts, details = self.counts(
            {
                "domain/ops-work/src/abe/mod.rs": "use crate::auth::B;\npub struct A;\n",
                "domain/ops-work/src/auth/mod.rs": "use crate::realm::C;\npub struct B;\n",
                "domain/ops-work/src/realm/mod.rs": "use crate::abe::A;\npub struct C;\n",
                "domain/ops-work/src/users/mod.rs": "use crate::abe::A;\npub struct D;\n",
                "domain/ops-work/src/groups/mod.rs": "use crate::users::D;\npub struct E;\n",
            }
        )
        self.assertEqual(counts["group_cycle_modules"], 3)
        self.assertEqual(details["group_cycle_modules"], ["abe, auth, realm"])
        self.assertEqual(counts["group_mutual_pairs"], 0)


class TriggerTest(unittest.TestCase):
    def test_arms(self):
        text = (
            "fn work(key: TaskKey) {\n"
            "    match key {\n"
            "        TaskKey::Drain { id, .. } => Box::pin(async move { drain(id).await }),\n"
            "        TaskKey::Plain => start(Plan::new()),\n"
            "        TaskKey::Block if ready() => {\n"
            "            let x = match y { Some(v) => v, None => 0 };\n"
            "            go(x).await;\n"
            "        }\n"
            "        _ => {}\n"
            "    }\n"
            "}\n"
        )
        _, masked = check_layers.mask_rust(text)
        arms = [(pattern.strip(), "await" in body) for pattern, body in check_layers.match_arms(masked)]
        self.assertEqual(
            arms[:4],
            [("TaskKey::Drain { id, .. }", True), ("TaskKey::Plain", False), ("TaskKey::Block if ready()", True), ("_", False)],
        )

    def trigger_findings(self, files):
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            sources = {rel: check_layers.load_source(root, rel) for rel in files}
        return check_layers.trigger_arms(sources)

    def test_trigger_missing(self):
        found = self.trigger_findings({})
        missing = [f"{rel}: no {prefix} dispatch" for rel, prefix in check_layers.TRIGGER_MATCHES]
        broker = "apps/node/src/plugin_host/broker.rs: no Broker impl source"
        self.assertEqual(found, missing + [broker])

    def test_trigger_helpers(self):
        # An arm may return only the table helper's future, and that helper must look the
        # operation up once and drive it; a bespoke helper or a helper without a drive counts.
        (timers, _), _ = check_layers.TRIGGER_MATCHES
        timer_text = (
            "fn work(key: TaskKey) {\n    match key {\n"
            "        TaskKey::One => Box::pin(self.run_trigger(key)),\n"
            "        TaskKey::Two => Box::pin(self.drain_two()),\n"
            "    }\n}\n"
            "async fn run_trigger(&self, key: TaskKey) {\n"
            "    let operation = timer_operation(&key, &input);\n    self.later().await;\n}\n"
        )
        found = self.trigger_findings({**TRIGGER_SITES, timers: timer_text})
        self.assertEqual(
            found,
            [
                f"{timers}: TaskKey::Two hands off to drain_two",
                f"{timers}: run_trigger does not drive one timer_operation operation",
            ],
        )

    def test_trigger_shapes(self):
        (timers, timer_prefix), (peers, peer_prefix) = check_layers.TRIGGER_MATCHES
        timer_text = (
            "fn work(key: TaskKey) {\n    match key {\n"
            "        TaskKey::One => Box::pin(async move { drive(One::new(), c).await; }),\n"
            "        TaskKey::Two => Box::pin(async move { drive(A, c).await; b().await; }),\n"
            "    }\n}\n"
        )
        peer_text = "fn serve(alpn: Alpn) {\n    match alpn {\n        Alpn::Bao => {}\n    }\n}\n"
        found = self.trigger_findings({**TRIGGER_SITES, timers: timer_text, peers: peer_text})
        self.assertEqual(found, [f"{timers}: {timer_prefix}Two"])
        self.assertTrue(peer_prefix.startswith("Alpn"))

    def test_broker_impl(self):
        rel = "apps/node/src/plugin_host/broker.rs"
        text = (
            "impl Broker for RunBroker {\n    async fn get(&self) { self.a().await; drive(b).await }\n"
            "    async fn one(&self) { Ok(drive(op, c).await?) }\n    fn sync(&self) {}\n}\n"
            "async fn free() { x.await; y.await }\n"
        )
        found = self.trigger_findings({**TRIGGER_SITES, rel: text})
        self.assertEqual(found, [f"{rel}: Broker::get"])


class ClocksTest(unittest.TestCase):
    def test_core_hooks(self):
        hook = '#[cfg(any(test, feature = "test-hooks"))]\n'
        files = {
            "domain/core/src/lib.rs": f"{hook}pub mod time;\npub mod rows;\n",
            "domain/core/src/time.rs": "pub fn now() -> u64 { unix_timestamp_millis() }\n",
            "domain/core/src/rows.rs": (
                f"{hook}pub fn key() -> u8 {{ getrandom::fill(&mut b); 1 }}\n"
                "pub fn now_ms(&self) -> u64 { self.now_ms }\nfn at(rows: &R) -> u64 { rows.now_ms() }\n"
                "fn read() -> u64 { SystemTime::now(); now_ms() }\n"
            ),
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            sources = [check_layers.load_source(root, rel) for rel in check_layers.rust_files(root)]
        self.assertEqual(check_layers.core_clocks(sources), ["domain/core/src/rows.rs:5", "domain/core/src/rows.rs:5"])


class HabitsTest(unittest.TestCase):
    def habits(self, text, rel="runtime/p2p/tests/probe.rs"):
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, {rel: text})
            source = check_layers.load_source(root, rel)
        return check_layers.test_habits([source])

    def test_sleeps(self):
        text = (
            "#[tokio::test(start_paused = true)]\nasync fn paused() { tokio::time::sleep(D).await; }\n"
            "#[tokio::test]\nasync fn manual() { tokio::time::pause(); sleep(D).await; }\n"
            "#[tokio::test]\nasync fn real() {\n    std::thread::sleep(D);\n}\n"
            "fn sleep(d: u8) {}\nfn other() { clock.sleep(D); }\n"
        )
        sleeps, _ = self.habits(text)
        self.assertEqual(sleeps, ["runtime/p2p/tests/probe.rs:7"])

    def test_production_ignored(self):
        sleeps, _ = self.habits("async fn wait() { tokio::time::sleep(D).await; }\n", "runtime/p2p/src/lib.rs")
        self.assertEqual(sleeps, [])

    def test_silent_skip(self):
        text = (
            "fn docker() -> Option<u8> {\n    eprintln!(\"skipping: {e}\");\n    None\n}\n"
            "#[test]\nfn probe() {\n    if missing() {\n        println!(\"no tool\");\n        return;\n    }\n"
            "    eprintln!(\"logs\");\n    assert!(true);\n}\n"
        )
        _, skips = self.habits(text)
        self.assertEqual(skips, ["runtime/p2p/tests/probe.rs:2", "runtime/p2p/tests/probe.rs:8"])


class DuplicatesTest(unittest.TestCase):
    def groups(self, files):
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            sources = [check_layers.load_source(root, rel) for rel in check_layers.rust_files(root)]
        return check_layers.duplicate_groups(sources)

    def test_identical(self):
        spaced = SIX_LINES.replace("= 1;", "=   1; // note")
        groups = self.groups(
            {"a/src/one.rs": "fn first() {\n" + SIX_LINES + "}\n", "b/src/two.rs": "fn second()\n{\n" + spaced + "}\n"}
        )
        self.assertEqual(groups, [["a/src/one.rs:1 first", "b/src/two.rs:2 second"]])

    def test_short_differs(self):
        five = SIX_LINES.split("\n", 1)[1]
        changed = SIX_LINES.replace('1', '"1"')
        groups = self.groups(
            {
                "a/src/one.rs": "fn first() {\n" + five + "}\nfn third() {\n" + SIX_LINES + "}\n",
                "b/src/two.rs": "fn second() {\n" + five + "}\nfn fourth() {\n" + changed + "}\n",
            }
        )
        self.assertEqual(groups, [])


class CratesTest(unittest.TestCase):
    def test_outside_target(self):
        manifest = MANIFEST + "aruna-storage = { workspace = true }\n"
        files = {
            "Cargo.toml": '[workspace]\nmembers = ["domain/ops-work", "runtime/storage", "domain/core"]\n',
            "domain/ops-work/Cargo.toml": manifest,
            "runtime/storage/Cargo.toml": '[package]\nname = "aruna-storage"\n',
            "domain/core/Cargo.toml": '[package]\nname = "aruna-core"\n',
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            self.assertEqual(check_layers.crate_findings(root, {}), (["aruna-ops-work -> aruna-storage"], []))
            allow = {"aruna-ops-work": ["aruna-storage", "aruna-rdf-graph"]}
            self.assertEqual(check_layers.crate_findings(root, allow), ([], ["aruna-ops-work -> aruna-rdf-graph"]))

    def test_backend_dev(self):
        files = {
            "Cargo.toml": '[workspace]\nmembers = ["domain/ops-work", "runtime/storage", "runtime/p2p"]\n',
            "domain/ops-work/Cargo.toml": MANIFEST + "\n[dev-dependencies]\naruna-p2p = { workspace = true }\n",
            "runtime/storage/Cargo.toml": '[package]\nname = "aruna-storage"\n\n[dev-dependencies]\naruna-p2p = "1"\n',
            "runtime/p2p/Cargo.toml": '[package]\nname = "aruna-p2p"\n\n[dev-dependencies]\naruna-storage = "1"\n',
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            self.assertEqual(check_layers.crate_findings(root, {}), (["aruna-storage -> aruna-p2p"], []))
            files["runtime/p2p/Cargo.toml"] = '[package]\nname = "aruna-p2p"\n\n[dependencies]\naruna-storage = "1"\n'
            write_tree(root, files)
            self.assertEqual(
                check_layers.crate_findings(root, {}),
                (["aruna-p2p -> aruna-storage", "aruna-storage -> aruna-p2p"], []),
            )

    def test_build_deps(self):
        build = "\n[build-dependencies]\naruna-runner = { workspace = true }\n"
        target = "\n[target.'cfg(unix)'.build-dependencies]\naruna-runner = { workspace = true }\n"
        for extra in (build, target):
            files = {
                "Cargo.toml": '[workspace]\nmembers = ["domain/ops-data", "runtime/runner"]\n',
                "domain/ops-data/Cargo.toml": '[package]\nname = "aruna-ops-data"\n' + extra,
                "runtime/runner/Cargo.toml": '[package]\nname = "aruna-runner"\n',
            }
            with tempfile.TemporaryDirectory() as root:
                write_tree(root, files)
                self.assertEqual(check_layers.crate_findings(root, {}), (["aruna-ops-data -> aruna-runner"], []))
                self.assertEqual(
                    check_layers.layer_findings(root),
                    ["aruna-ops-data -> aruna-runner: domain crates depend only on domain crates"],
                )


def crate_toml(name, normal=(), dev=()):
    """A member manifest with internal normal and dev-dependencies; a trailing ? marks optional."""
    lines = [f'[package]\nname = "{name}"\n\n[dependencies]']
    for dep in normal:
        extra = ", optional = true" if dep.endswith("?") else ""
        lines.append(f"{dep.rstrip('?')} = {{ workspace = true{extra} }}")
    lines.append("\n[dev-dependencies]")
    lines.extend(f"{dep} = {{ workspace = true }}" for dep in dev)
    return "\n".join(lines) + "\n"


def layer_tree(crates):
    """A workspace of `crates`, each (member directory, name, normal deps, dev deps)."""
    members = ", ".join(f'"{member}"' for member, *_ in crates)
    files = {"Cargo.toml": f"[workspace]\nmembers = [{members}]\n"}
    for member, name, normal, dev in crates:
        files[f"{member}/Cargo.toml"] = crate_toml(name, normal, dev)
    return files


def layer_breaks(crates):
    with tempfile.TemporaryDirectory() as root:
        write_tree(root, layer_tree(crates))
        return check_layers.layer_findings(root)


class LayersTest(unittest.TestCase):
    def test_clean_order(self):
        crates = [
            ("domain/core", "aruna-core", (), ()),
            ("domain/ops-identity", "aruna-ops-identity", ["aruna-core"], ["aruna-storage"]),
            ("domain/ops-data", "aruna-ops-data", ["aruna-ops-identity"], ["aruna-test-harness"]),
            ("runtime/storage", "aruna-storage", ["aruna-core"], ()),
            ("frontends/api", "aruna-api", ["aruna-runner", "aruna-test-harness?"], ()),
            ("runtime/runner", "aruna-runner", ["aruna-storage"], ()),
            ("testing/harness", "aruna-test-harness", ["aruna-ops-data"], ()),
        ]
        self.assertEqual(layer_breaks(crates), [])

    def test_domain_runtime(self):
        crates = [
            ("domain/ops-work", "aruna-ops-work", ["aruna-storage"], ()),
            ("runtime/storage", "aruna-storage", (), ()),
        ]
        self.assertEqual(
            layer_breaks(crates),
            ["aruna-ops-work -> aruna-storage: domain crates depend only on domain crates"],
        )

    def test_ops_dev(self):
        crates = [
            ("domain/ops-identity", "aruna-ops-identity", (), ["aruna-ops-data"]),
            ("domain/ops-data", "aruna-ops-data", ["aruna-ops-identity"], ["aruna-ops-work"]),
            ("domain/ops-work", "aruna-ops-work", ["aruna-ops-data"], ["aruna-ops-work"]),
        ]
        self.assertEqual(
            layer_breaks(crates),
            [
                "aruna-ops-data -> aruna-ops-work: operations crates never reach a higher one",
                "aruna-ops-identity -> aruna-ops-data: operations crates never reach a higher one",
            ],
        )

    def test_unknown_crate(self):
        crates = [("domain/extra", "aruna-extra", (), ()), ("misc/tool", "aruna-plugin", (), ())]
        self.assertEqual(
            layer_breaks(crates),
            [
                "aruna-extra: missing from CRATE_TARGETS",
                "aruna-plugin: misc/tool lies outside the layer directories",
            ],
        )

    def test_frontend_reach(self):
        crates = [
            ("frontends/api", "aruna-api", ["aruna", "aruna-storage", "aruna-test-harness"], ()),
            ("apps/node", "aruna", ["aruna-test-harness"], ()),
            ("runtime/storage", "aruna-storage", ["aruna-ops-work"], ()),
            ("domain/ops-work", "aruna-ops-work", (), ()),
            ("testing/harness", "aruna-test-harness", (), ()),
        ]
        self.assertEqual(
            layer_breaks(crates),
            [
                "aruna -> aruna-test-harness: testing crates are never a required dependency here",
                "aruna-api -> aruna: frontends never depend on apps",
                "aruna-api -> aruna-storage: frontends reach the runtime only through the clock and the runner",
                "aruna-api -> aruna-test-harness: frontends name test helpers only as an optional dependency",
                "aruna-storage -> aruna-ops-work: runtime crates depend only on runtime crates and domain "
                "foundations",
            ],
        )

    def test_report_errors(self):
        files = pure_tree({"domain/ops-work/Cargo.toml": MANIFEST + "aruna-storage = { workspace = true }\n"})
        files["Cargo.toml"] = '[workspace]\nmembers = ["domain/ops-work", "runtime/storage"]\n'
        files["runtime/storage/Cargo.toml"] = '[package]\nname = "aruna-storage"\n'
        errors, output = run_report(files, zero_baseline())
        self.assertEqual(errors, 2, output)
        self.assertIn("error: aruna-ops-work -> aruna-storage: domain crates depend only on domain crates", output)


class EffectsTest(unittest.TestCase):
    def test_api_effects(self):
        text = (
            "fn handler() {\n    let read = StorageEffect::Read;\n}\n"
            "#[cfg(test)]\nmod tests {\n    fn fixture() {\n        let read = StorageEffect::Read;\n    }\n}\n"
        )
        files = {
            "frontends/api/src/routes.rs": text,
            "frontends/api/src/routes_tests.rs": "fn fixture() { BlobEffect::Write; }\n",
            "frontends/api/tests/flow.rs": "fn flow() { NetEffect::Send; }\n",
            "apps/node/src/serve.rs": "fn serve() { StorageEffect::Read; }\n",
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            sources = [check_layers.load_source(root, rel) for rel in sorted(files)]
        self.assertEqual(check_layers.raw_effects(sources), ["frontends/api/src/routes.rs:2"])
        errors, output = run_report(pure_tree({"frontends/api/src/routes.rs": text}), zero_baseline())
        self.assertEqual(errors, 1, output)
        self.assertIn("error: api_raw_effects rose above its baseline", output)

    def test_aliased_effects(self):
        text = (
            "use aruna_core::effects::StorageEffect as Store;\n"
            "use aruna_core::effects::{BlobEffect as _, NetEffect as Net};\n"
            "fn handler() {\n    let sync = Store::SyncAll;\n    let send = Net::Send;\n}\n"
            "#[cfg(test)]\nmod tests {\n    fn fixture() {\n        let sync = Store::SyncAll;\n    }\n}\n"
        )
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, {"frontends/api/src/routes.rs": text})
            sources = [check_layers.load_source(root, "frontends/api/src/routes.rs")]
        self.assertEqual(
            check_layers.raw_effects(sources),
            ["frontends/api/src/routes.rs:4", "frontends/api/src/routes.rs:5"],
        )

    def test_raw_alias(self):
        text = (
            "use aruna_core::effects::StorageEffect as r#type;\n"
            "fn handler() {\n    let sync = r#type::SyncAll;\n}\n"
        )
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, {"frontends/api/src/routes.rs": text})
            sources = [check_layers.load_source(root, "frontends/api/src/routes.rs")]
        self.assertEqual(check_layers.raw_effects(sources), ["frontends/api/src/routes.rs:3"])

    def test_scoped_alias(self):
        text = (
            "fn first() {\n"
            "    use aruna_core::effects::StorageEffect as Store;\n"
            "    let _ = Store::SyncAll;\n"
            "}\n"
            "fn second() {\n"
            "    let _ = Store::open();\n"
            "}\n"
        )
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, {"frontends/api/src/routes.rs": text})
            sources = [check_layers.load_source(root, "frontends/api/src/routes.rs")]
        self.assertEqual(check_layers.raw_effects(sources), ["frontends/api/src/routes.rs:3"])


class GuardTest(unittest.TestCase):
    def test_production_await(self):
        errors, output = run_report(pure_tree(), zero_baseline())
        self.assertEqual(errors, 0, output)
        added = pure_tree({"domain/ops-work/src/abe/plain.rs": "fn later() { ready.await; }\n"})
        errors, output = run_report(added, zero_baseline())
        self.assertEqual(errors, 1, output)
        self.assertIn("error: async code in operations production at domain/ops-work/src/abe/plain.rs:1", output)
        errors, output = run_report(base_tree(), zero_baseline())
        self.assertEqual(errors, 1, output)
        self.assertIn("at domain/ops-work/src/jobs/mod.rs:1", output)

    def test_count_rises(self):
        files = pure_tree({"domain/ops-work/src/abe/mod.rs": PURE_OP + "async fn later() {}\n"})
        errors, output = run_report(files, zero_baseline())
        self.assertEqual(errors, 2)
        self.assertIn("error: mixed_files rose above its baseline", output)
        self.assertIn("error: async code in operations production at domain/ops-work/src/abe/mod.rs:3", output)
        baseline = zero_baseline()
        baseline["structural"]["mixed_files"] = 2
        errors, output = run_report(files, baseline)
        self.assertEqual(errors, 1)
        self.assertIn("note: mixed_files fell below its baseline", output)

    def test_baseline_keys(self):
        path = os.path.join(REPO_ROOT, check_layers.BASELINE_PATH)
        baseline, missing = check_layers.load_baseline(path)
        self.assertEqual(missing, [])
        self.assertEqual(sorted(baseline["structural"]), sorted(key for key, _ in check_layers.COUNTS))
        with open(path, encoding="utf-8") as handle:
            self.assertEqual(json.load(handle), baseline)


class OwnFilesTest(unittest.TestCase):
    def test_own_clean(self):
        own = [
            finding
            for finding in list(check_style.python_findings(REPO_ROOT)) + list(check_style.check_headers(REPO_ROOT))
            if finding[1].endswith(("check_layers.py", "check_layers_tests.py"))
        ]
        self.assertEqual(own, [])


if __name__ == "__main__":
    unittest.main()
