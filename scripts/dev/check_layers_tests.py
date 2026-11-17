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
WORKSPACE = '[workspace]\nmembers = ["operations"]\n'
MANIFEST = '[package]\nname = "aruna-operations"\n\n[dependencies]\naruna-core = { workspace = true }\n'
PURE_OP = "pub struct Plan;\nimpl Operation for Plan {}\n"
SIX_LINES = "    let a = 1;\n    let b = 2;\n    let c = 3;\n    let d = 4;\n    let e = 5;\n    let f = a + b + c + d + e;\n"


def write_tree(root, files):
    for rel, text in files.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)


def base_tree(extra=None):
    files = {
        "Cargo.toml": WORKSPACE,
        "operations/Cargo.toml": MANIFEST,
        "operations/src/lib.rs": "pub mod abe;\npub mod jobs;\n",
        "operations/src/abe/mod.rs": PURE_OP,
        "operations/src/jobs/mod.rs": "pub async fn run() {}\n",
    }
    files.update(extra or {})
    return files


def zero_baseline(clean=()):
    return {
        "clean_domains": list(clean),
        "crate_allow": {},
        "structural": {key: 0 for key, _ in check_layers.COUNTS},
        "async_lines": {},
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
        self.assertTrue(check_layers.is_test("operations/tests/main.rs"))
        self.assertTrue(check_layers.is_test("operations/src/realm_vault_tests.rs"))
        self.assertTrue(check_layers.is_test("operations/src/jobs/tests_outbox.rs"))
        self.assertFalse(check_layers.is_test("operations/src/jobs/testing.rs"))


class ClassifyTest(unittest.TestCase):
    def test_kinds(self):
        ops = ops_for(
            base_tree(
                {
                    "operations/src/abe/mixed.rs": PURE_OP + "async fn helper() {}\n",
                    "operations/src/abe/plain.rs": "pub fn plain() {}\n// .await in a comment\n",
                    "operations/src/abe/sub.rs": "impl<T: Send> SubOperation for Step<T> {}\n",
                }
            )
        )
        kinds = {rel: info["kind"] for rel, info in ops.files.items()}
        self.assertEqual(kinds["operations/src/abe/mod.rs"], "op")
        self.assertEqual(kinds["operations/src/abe/mixed.rs"], "mixed")
        self.assertEqual(kinds["operations/src/abe/plain.rs"], "plain")
        self.assertEqual(kinds["operations/src/abe/sub.rs"], "op")
        self.assertEqual(kinds["operations/src/jobs/mod.rs"], "async")
        self.assertEqual(check_layers.async_lines(ops), {"abe": 3, "jobs": 1})

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
        counts, details = self.counts({"operations/src/abe/mod.rs": PURE_OP + "use crate::jobs::run;\n"})
        self.assertEqual(counts["pure_async_refs"], 1)
        self.assertEqual(details["pure_async_refs"], ["operations/src/abe/mod.rs -> operations/src/jobs/mod.rs"])
        self.assertEqual(counts["upward_refs"], 1)

    def test_super_paths(self):
        counts, _ = self.counts(
            {
                "operations/src/abe/mod.rs": PURE_OP,
                "operations/src/abe/inner.rs": "fn f() { super::super::jobs::run(); }\nuse super::Plan;\n",
            }
        )
        self.assertEqual(counts["upward_refs"], 0)
        self.assertEqual(counts["pure_async_refs"], 0)
        counts, _ = self.counts({"operations/src/abe/inner.rs": "use super::super::jobs::run;\n"})
        self.assertEqual(counts["pure_async_refs"], 1)

    def test_group_cycles(self):
        counts, details = self.counts(
            {
                "operations/src/abe/mod.rs": "use crate::auth::B;\npub struct A;\n",
                "operations/src/auth/mod.rs": "use crate::realm::C;\npub struct B;\n",
                "operations/src/realm/mod.rs": "use crate::abe::A;\npub struct C;\n",
                "operations/src/users/mod.rs": "use crate::abe::A;\npub struct D;\n",
                "operations/src/groups/mod.rs": "use crate::users::D;\npub struct E;\n",
            }
        )
        self.assertEqual(counts["group_cycle_modules"], 3)
        self.assertEqual(details["group_cycle_modules"], ["abe, auth, realm"])
        self.assertEqual(counts["group_mutual_pairs"], 0)

    def test_runner_refs(self):
        counts, details = self.counts(
            {"operations/src/driver.rs": "use crate::jobs::run;\nuse crate::storage_read::read;\n",
             "operations/src/storage_read.rs": "pub fn read() {}\n"}
        )
        self.assertEqual(counts["runner_refs"], 1)
        self.assertEqual(details["runner_refs"], ["driver -> jobs: 1"])


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

    def test_broker_impl(self):
        rel = "operations/src/plugins/broker.rs"
        text = "impl Broker for RunBroker {\n    async fn get(&self) {}\n    fn sync(&self) {}\n}\nasync fn free() {}\n"
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, {rel: text})
            source = check_layers.load_source(root, rel)
        self.assertEqual(check_layers.trigger_arms({rel: source}), [f"{rel}: Broker::get"])


class HabitsTest(unittest.TestCase):
    def habits(self, text, rel="net/tests/probe.rs"):
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
        self.assertEqual(sleeps, ["net/tests/probe.rs:7"])

    def test_production_ignored(self):
        sleeps, _ = self.habits("async fn wait() { tokio::time::sleep(D).await; }\n", "net/src/lib.rs")
        self.assertEqual(sleeps, [])

    def test_silent_skip(self):
        text = (
            "fn docker() -> Option<u8> {\n    eprintln!(\"skipping: {e}\");\n    None\n}\n"
            "#[test]\nfn probe() {\n    if missing() {\n        println!(\"no tool\");\n        return;\n    }\n"
            "    eprintln!(\"logs\");\n    assert!(true);\n}\n"
        )
        _, skips = self.habits(text)
        self.assertEqual(skips, ["net/tests/probe.rs:2", "net/tests/probe.rs:8"])


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
            "Cargo.toml": '[workspace]\nmembers = ["operations", "storage", "core"]\n',
            "operations/Cargo.toml": manifest,
            "storage/Cargo.toml": '[package]\nname = "aruna-storage"\n',
            "core/Cargo.toml": '[package]\nname = "aruna-core"\n',
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            self.assertEqual(check_layers.crate_findings(root, {}), (["aruna-operations -> aruna-storage"], []))
            allow = {"aruna-operations": ["aruna-storage", "aruna-graph"]}
            self.assertEqual(check_layers.crate_findings(root, allow), ([], ["aruna-operations -> aruna-graph"]))

    def test_backend_dev(self):
        files = {
            "Cargo.toml": '[workspace]\nmembers = ["operations", "storage", "net"]\n',
            "operations/Cargo.toml": MANIFEST + "\n[dev-dependencies]\naruna-net = { workspace = true }\n",
            "storage/Cargo.toml": '[package]\nname = "aruna-storage"\n\n[dev-dependencies]\naruna-net = "1"\n',
            "net/Cargo.toml": '[package]\nname = "aruna-net"\n\n[dependencies]\naruna-storage = "1"\n',
        }
        with tempfile.TemporaryDirectory() as root:
            write_tree(root, files)
            self.assertEqual(check_layers.crate_findings(root, {}), (["aruna-storage -> aruna-net"], []))


class GuardTest(unittest.TestCase):
    def test_clean_await(self):
        errors, output = run_report(base_tree(), zero_baseline(["abe"]))
        self.assertEqual(errors, 0, output)
        added = base_tree({"operations/src/abe/mod.rs": PURE_OP + "fn later() { ready.await; }\n"})
        errors, output = run_report(added, zero_baseline(["abe"]))
        self.assertGreater(errors, 0)
        self.assertIn("async code in a clean domain at operations/src/abe/mod.rs:3", output)

    def test_count_rises(self):
        files = base_tree({"operations/src/abe/mod.rs": PURE_OP + "async fn later() {}\n"})
        errors, output = run_report(files, zero_baseline())
        self.assertEqual(errors, 1)
        self.assertIn("error: mixed_files rose above its baseline", output)
        baseline = zero_baseline()
        baseline["structural"]["mixed_files"] = 2
        errors, output = run_report(files, baseline)
        self.assertEqual(errors, 0)
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
