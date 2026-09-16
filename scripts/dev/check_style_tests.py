#!/usr/bin/env python3
"""Self-tests for check_style.py. Plain Python; no Rust toolchain or build."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import check_style


def scan(text, name="probe.rs"):
    masked, comments, attrs, tokens = check_style.mask_source(text)
    return list(check_style.name_findings(name, name, text, masked, attrs, tokens))


def messages(findings):
    return [finding[3] for finding in findings]


class IdentifierKindsTest(unittest.TestCase):
    def test_function(self):
        findings = scan("fn load_bucket_config_now() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'load_bucket_config_now' has 4 terms")])

    def test_testfn(self):
        findings = scan("#[test]\nfn stop_between_phases_now() {}\n")
        self.assertEqual(findings, [("testfn", "probe.rs", 2, "fn 'stop_between_phases_now' has 4 terms")])

    def test_test_attrs(self):
        text = (
            "#[test]\nfn case_one_two_three() {}\n"
            "#[rstest]\nfn case_two_three_four() {}\n"
            "#[test]\n#[ignore]\nfn case_three_four_five() {}\n"
        )
        findings = scan(text)
        self.assertEqual([finding[0] for finding in findings], ["testfn", "testfn", "testfn"])

    def test_const_fn(self):
        findings = scan("const fn drive_effects_now_twice() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'drive_effects_now_twice' has 4 terms")])

    def test_types(self):
        text = (
            "struct FourTermStructName;\n"
            "enum FourTermEnumName { One }\n"
            "trait FourTermTraitName {}\n"
            "type FourTermAliasName = u8;\n"
            "union FourTermUnionName { value: u8 }\n"
        )
        findings = scan(text)
        self.assertEqual(
            messages(findings),
            [
                "struct 'FourTermStructName' has 4 terms",
                "enum 'FourTermEnumName' has 4 terms",
                "trait 'FourTermTraitName' has 4 terms",
                "type 'FourTermAliasName' has 4 terms",
                "union 'FourTermUnionName' has 4 terms",
            ],
        )
        self.assertTrue(all(finding[0] == "type" for finding in findings))

    def test_module(self):
        findings = scan("mod four_term_module_name {}\n")
        self.assertEqual(findings, [("mod", "probe.rs", 1, "mod 'four_term_module_name' has 4 terms")])

    def test_const_static(self):
        text = "const MAXIMUM_RETRY_COUNT_VALUE_LIMIT: u32 = 1;\nstatic SHARED_REGISTRY_CACHE_VALUE: u8 = 0;\n"
        self.assertEqual(
            messages(scan(text)),
            [
                "const 'MAXIMUM_RETRY_COUNT_VALUE_LIMIT' has 5 terms",
                "static 'SHARED_REGISTRY_CACHE_VALUE' has 4 terms",
            ],
        )

    def test_static_mut(self):
        findings = scan('extern "C" { static mut GLOBAL_SHARED_REGISTRY_VALUE: u8; }\n')
        self.assertEqual(findings, [("const", "probe.rs", 1, "static 'GLOBAL_SHARED_REGISTRY_VALUE' has 4 terms")])

    def test_field(self):
        findings = scan("struct Config { maximum_retry_count_value_limit: u32 }\n")
        self.assertEqual(findings, [("field", "probe.rs", 1, "field 'maximum_retry_count_value_limit' has 5 terms")])

    def test_attr_field(self):
        text = (
            "struct Config {\n"
            "    #[serde(default)]\n"
            "    pub(crate) maximum_thread_pool_size_limit: usize,\n"
            "}\n"
        )
        self.assertEqual(messages(scan(text)), ["field 'maximum_thread_pool_size_limit' has 5 terms"])

    def test_where_field(self):
        text = (
            "struct Holder<T>\nwhere\n    T: Iterator<Item = u8>,\n{\n    shared_state_value_limit: T,\n}\n"
        )
        self.assertEqual(messages(scan(text)), ["field 'shared_state_value_limit' has 4 terms"])

    def test_variant(self):
        findings = scan("enum Mode { MaximumRetryCountValueLimit }\n")
        self.assertEqual(findings, [("variant", "probe.rs", 1, "variant 'MaximumRetryCountValueLimit' has 5 terms")])

    def test_struct_variant(self):
        findings = scan("enum Mode { Payload { some_field_name_here_long: u32 } }\n")
        self.assertEqual(messages(findings), ["field 'some_field_name_here_long' has 5 terms"])

    def test_filename(self):
        masked, _comments, attrs, tokens = check_style.mask_source("")
        findings = list(
            check_style.name_findings(
                "rel/four_term_file_name.rs", "rel/four_term_file_name.rs", "", masked, attrs, tokens
            )
        )
        self.assertEqual(findings, [("file", "rel/four_term_file_name.rs", 0, "filename 'four_term_file_name' has 4 terms")])

    def test_local_binding(self):
        findings = scan("fn tiny() { let mut maximum_retry_count_value = 1; }\n")
        self.assertEqual(findings, [("let", "probe.rs", 1, "let 'maximum_retry_count_value' has 4 terms")])

    def test_local_destructure(self):
        findings = scan("fn tiny() { let (maximum_retry_count_value, other) = pair(); }\n")
        self.assertEqual(findings, [("let", "probe.rs", 1, "let 'maximum_retry_count_value' has 4 terms")])

    def test_param(self):
        findings = scan("fn tiny(value: SomeFourTermTypeName, maximum_retry_count_value: u32) {}\n")
        self.assertEqual(findings, [("param", "probe.rs", 1, "param 'maximum_retry_count_value' has 4 terms")])

    def test_param_pattern(self):
        findings = scan("fn tiny((maximum_retry_count_value, minimum_retry_count_value): (u32, u32)) {}\n")
        self.assertEqual(
            messages(findings),
            [
                "param 'maximum_retry_count_value' has 4 terms",
                "param 'minimum_retry_count_value' has 4 terms",
            ],
        )

    def test_macro_name(self):
        findings = scan("macro_rules! maximum_retry_count_value { () => {}; }\n")
        self.assertEqual(
            findings, [("macro", "probe.rs", 1, "macro_rules 'maximum_retry_count_value' has 4 terms")]
        )


class TermBoundaryTest(unittest.TestCase):
    def test_three_terms(self):
        self.assertEqual(scan("fn load_bucket_config() {}\n"), [])

    def test_four_terms(self):
        findings = scan("fn load_bucket_config_now() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'load_bucket_config_now' has 4 terms")])

    def test_missed_kinds(self):
        text = (
            "const MAXIMUM_RETRY_COUNT_VALUE_LIMIT: u32 = 3;\n"
            "struct Settings { maximum_retry_count_value_limit: u32 }\n"
            "enum Mode { MaximumRetryCountValueLimit }\n"
        )
        self.assertEqual(
            messages(scan(text)),
            [
                "const 'MAXIMUM_RETRY_COUNT_VALUE_LIMIT' has 5 terms",
                "field 'maximum_retry_count_value_limit' has 5 terms",
                "variant 'MaximumRetryCountValueLimit' has 5 terms",
            ],
        )


class AcronymTest(unittest.TestCase):
    def test_flat_acronyms(self):
        names = (
            "handle_http_response",
            "load_uuid_value",
            "fetch_s3_object",
            "check_sha256_digest",
            "validate_oidc_token",
            "open_ro_crate_manifest",
        )
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(scan(f"fn {name}() {{}}\n"), [])

    def test_camel_acronyms(self):
        for name in ("HttpUuid", "S3UuidKey", "RoCrateManifest"):
            with self.subTest(name=name):
                self.assertEqual(scan(f"struct {name};\n"), [])

    def test_extra_terms(self):
        findings = scan("fn fetch_http_uuid_data() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'fetch_http_uuid_data' has 4 terms")])


class StringTest(unittest.TestCase):
    def test_literal_kinds(self):
        text = (
            'const A: &str = "fn four_term_name_here() {}";\n'
            'const B: &str = r#"struct FourTermStructName;"#;\n'
            'const C: &str = r"enum FourTermEnumName {}";\n'
            'const D: &[u8] = b"mod four_term_module_name {}";\n'
            'const E: &[u8] = br#"fn four_term_name_here()"#;\n'
            'const F: &str = r##"a "# inside"##;\n'
            "const G: char = 'x';\n"
        )
        self.assertEqual(scan(text), [])

    def test_after_string(self):
        text = 'const A: &str = "noise";\nfn four_term_name_here() {}\n'
        self.assertEqual(scan(text), [("fn", "probe.rs", 2, "fn 'four_term_name_here' has 4 terms")])

    def test_lifetime(self):
        text = "fn borrowed() -> &'static str { \"\" }\n"
        self.assertEqual(scan(text), [])


class AttributeTest(unittest.TestCase):
    def test_attr_body(self):
        text = (
            '#[cfg(feature = "four_term_feature_name")]\n'
            '#[allow(dead_code, reason = "four_term_reason_name")]\n'
            "fn tiny() {}\n"
        )
        self.assertEqual(scan(text), [])

    def test_inner_attr(self):
        self.assertEqual(scan('#![allow(dead_code, reason = "four_term_reason_name")]\nfn tiny() {}\n'), [])


class CommentTest(unittest.TestCase):
    def comments(self, text):
        masked, comments, attrs, tokens = check_style.mask_source(text)
        return list(check_style.comment_findings("probe.rs", text, masked, comments, tokens))

    def test_block_span(self):
        text = "/* outer /* inner */ fn four_term_name_here() {} */\nfn tiny() {}\n"
        self.assertEqual(scan(text), [])
        masked, comments, _attrs, _tokens = check_style.mask_source(text)
        self.assertEqual(len(comments), 1)

    def test_three_lines(self):
        self.assertEqual(self.comments("// one\n// two\n// three\nfn tiny() {}\n"), [])

    def test_four_lines(self):
        self.assertEqual(
            self.comments("// one\n// two\n// three\n// four\nfn tiny() {}\n"),
            [("comment", "probe.rs", 1, "comment spans 4 lines")],
        )

    def test_block_lines(self):
        self.assertEqual(
            self.comments("/* one\n two\n three\n four */\nfn tiny() {}\n"),
            [("comment", "probe.rs", 1, "comment spans 4 lines")],
        )

    def test_license(self):
        self.assertEqual(self.comments("// Copyright 2026\n// two\n// three\n// four\nfn tiny() {}\n"), [])

    def test_restates(self):
        self.assertEqual(
            self.comments("// bucket name\nfn bucket_name() {}\n"),
            [("restates", "probe.rs", 1, "comment repeats 'bucket_name'")],
        )


class MacroTest(unittest.TestCase):
    def test_invocation(self):
        findings = scan("my_macro! {\n    static SHARED_REGISTRY_CACHE_VALUE: u8 = 1;\n}\n")
        self.assertEqual(findings, [("const", "probe.rs", 2, "static 'SHARED_REGISTRY_CACHE_VALUE' has 4 terms")])

    def test_static_ref(self):
        findings = scan("lazy_static! {\n    static ref SHARED_SESSION_REGISTRY_CACHE: u8 = 0;\n}\n")
        self.assertEqual(findings, [("const", "probe.rs", 2, "static 'SHARED_SESSION_REGISTRY_CACHE' has 4 terms")])

    def test_rules_skip(self):
        text = "macro_rules! helper {\n    () => { fn four_term_name_here() {} };\n}\n"
        self.assertEqual(scan(text), [])

    def test_reference(self):
        text = "let _ = matches!(value, Option::Some(SomeFourTermTypeName));\n"
        self.assertEqual(scan(text), [])


class TreeTest(unittest.TestCase):
    def write(self, root, relpath, text=""):
        path = os.path.join(root, relpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)


class FolderTest(TreeTest):
    def setUp(self):
        self.saved = dict(check_style.SMALL_DOMAINS)

    def tearDown(self):
        check_style.SMALL_DOMAINS.clear()
        check_style.SMALL_DOMAINS.update(self.saved)

    def folders(self, tree):
        with tempfile.TemporaryDirectory() as tmp:
            for relpath, text in tree:
                self.write(tmp, relpath, text)
            return sorted(check_style.check_folders(tmp))

    def test_five_files(self):
        tree = [("pkg/Cargo.toml", ""), ("pkg/src/lib.rs", "")] + [
            (f"pkg/src/good/{name}.rs", "") for name in ("one", "two", "three", "four", "five")
        ]
        self.assertEqual(self.folders(tree), [])

    def test_four_files(self):
        tree = [("pkg/Cargo.toml", ""), ("pkg/src/lib.rs", "")] + [
            (f"pkg/src/good/{name}.rs", "") for name in ("one", "two", "three", "four")
        ]
        self.assertEqual(self.folders(tree), [("folder", "pkg/src/good", 0, "4 of 5 direct files besides mod.rs")])

    def test_child_dirs(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/module/one.rs", ""),
            ("pkg/src/module/two.rs", ""),
            ("pkg/src/module/three.rs", ""),
            ("pkg/src/module/first/mod.rs", ""),
            ("pkg/src/module/second/mod.rs", ""),
        ]
        self.assertIn(("folder", "pkg/src/module", 0, "3 of 5 direct files besides mod.rs"), self.folders(tree))

    def test_generic_prefix(self):
        verb_tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/set_compute.rs", ""),
            ("pkg/src/set_policies.rs", ""),
            ("pkg/src/set_quota.rs", ""),
        ]
        self.assertEqual([finding for finding in self.folders(verb_tree) if finding[0] == "prefix"], [])
        domain_tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/loader_parse.rs", ""),
            ("pkg/src/loader_cache.rs", ""),
            ("pkg/src/loader_tests.rs", ""),
        ]
        self.assertIn(
            ("prefix", "pkg/src", 0, "3 siblings share prefix 'loader'; group them into 'loader/'"),
            self.folders(domain_tree),
        )

    def test_two_prefixed(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/small/loader_one.rs", ""),
            ("pkg/src/small/loader_two.rs", ""),
        ]
        self.assertEqual(self.folders(tree), [("folder", "pkg/src/small", 0, "2 of 5 direct files besides mod.rs")])

    def test_prefix_flat(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/loader_parse.rs", ""),
            ("pkg/src/loader_cache.rs", ""),
            ("pkg/src/loader_tests.rs", ""),
        ]
        self.assertIn(("prefix", "pkg/src", 0, "3 siblings share prefix 'loader'; group them into 'loader/'"), self.folders(tree))

    def test_prefix_repeat(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/compute/compute_config.rs", ""),
            ("pkg/src/compute/compute_backend.rs", ""),
            ("pkg/src/compute/compute_tests.rs", ""),
        ]
        self.assertIn(
            ("prefix", "pkg/src/compute", 0, "3 children repeat prefix 'compute'; drop it from their names"),
            self.folders(tree),
        )

    def test_prefix_grouped(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/compute/mod.rs", ""),
            ("pkg/src/compute/config.rs", ""),
            ("pkg/src/compute/tests.rs", ""),
        ]
        check_style.SMALL_DOMAINS["pkg/src/compute"] = "grouped three-file domain"
        self.assertEqual(self.folders(tree), [])

    def test_small_entry(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/scope/one.rs", ""),
            ("pkg/src/scope/two.rs", ""),
            ("pkg/src/scope/three.rs", ""),
        ]
        check_style.SMALL_DOMAINS["pkg/src/scope"] = "reviewed small scope"
        self.assertEqual(self.folders(tree), [])

    def test_structural(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/bin/tool.rs", ""),
            ("pkg/src/benches/bench.rs", ""),
            ("pkg/src/examples/demo.rs", ""),
            ("pkg/tests/single.rs", ""),
        ]
        self.assertEqual(self.folders(tree), [])

    def test_mod_file(self):
        tree = [("pkg/Cargo.toml", ""), ("pkg/src/lib.rs", ""), ("pkg/src/empty/mod.rs", "")]
        self.assertEqual(self.folders(tree), [("folder", "pkg/src/empty", 0, "0 of 5 direct files besides mod.rs")])

    def test_asset_only(self):
        tree = [("pkg/Cargo.toml", ""), ("pkg/src/lib.rs", ""), ("pkg/src/asset/one.toml", "")]
        self.assertEqual(self.folders(tree), [("folder", "pkg/src/asset", 0, "1 of 5 direct files besides mod.rs")])

    def test_fixtures(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/fixtures/one.bin", ""),
            ("pkg/tests/fixtures/two.bin", ""),
        ]
        self.assertEqual(self.folders(tree), [])

    def test_shared_tests(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/tests/mod.rs", ""),
            ("pkg/src/tests/one.rs", ""),
            ("pkg/src/tests/two.rs", ""),
            ("pkg/src/tests/three.rs", ""),
        ]
        self.assertEqual(self.folders(tree), [])

    def test_nomod(self):
        tree = [
            ("pkg/Cargo.toml", ""),
            ("pkg/src/lib.rs", ""),
            ("pkg/src/tests/one.rs", ""),
            ("pkg/src/tests/two.rs", ""),
        ]
        self.assertIn(("folder", "pkg/src/tests", 0, "2 of 5 direct files besides mod.rs"), self.folders(tree))


class ExternalNamesTest(unittest.TestCase):
    def setUp(self):
        self.saved = dict(check_style.EXTERNAL_NAMES)

    def tearDown(self):
        check_style.EXTERNAL_NAMES.clear()
        check_style.EXTERNAL_NAMES.update(self.saved)

    def test_reasoned(self):
        check_style.EXTERNAL_NAMES["GeneratedWireRecordName"] = "generated from the external schema"
        self.assertEqual(scan("struct GeneratedWireRecordName;\n"), [])

    def test_missing_reason(self):
        check_style.EXTERNAL_NAMES["MissingReasonName"] = ""
        self.assertEqual(check_style.external_without_reason(), ["MissingReasonName"])


class ReferenceTest(unittest.TestCase):
    def test_references(self):
        text = (
            "fn use_types() { let _: Vec<SomeFourTermTypeName> = Vec::new(); }\n"
            "struct Holder { value: std::collections::HashMap<SomeFourTermTypeName, u32> }\n"
            "fn borrowed() -> &'static str { \"\" }\n"
        )
        self.assertEqual(scan(text), [])

    def test_generic_param(self):
        self.assertEqual(scan("fn tiny(value: HashMap<u8, u8>) {}\n"), [])


class PythonToolTest(TreeTest):
    def python(self, tree):
        with tempfile.TemporaryDirectory() as tmp:
            for relpath, text in tree:
                self.write(tmp, relpath, text)
            return sorted(check_style.python_findings(tmp))

    def test_def_name(self):
        findings = self.python([("scripts/dev/tool.py", "def load_bucket_config_now():\n    pass\n")])
        self.assertEqual(
            findings, [("pydecl", "scripts/dev/tool.py", 1, "def 'load_bucket_config_now' has 4 terms")]
        )

    def test_class_name(self):
        findings = self.python([("scripts/dev/tool.py", "class MaximumRetryCountValue:\n    pass\n")])
        self.assertEqual(
            findings, [("pydecl", "scripts/dev/tool.py", 1, "class 'MaximumRetryCountValue' has 4 terms")]
        )

    def test_comment_span(self):
        findings = self.python([("scripts/dev/tool.py", "# one\n# two\n# three\n# four\nvalue = 1\n")])
        self.assertEqual(findings, [("pycomment", "scripts/dev/tool.py", 1, "comment spans 4 lines")])

    def test_string_skip(self):
        findings = self.python([("scripts/dev/tool.py", 'TEXT = "def four_term_name_here(): pass"\n')])
        self.assertEqual(findings, [])


class OwnFilesTest(unittest.TestCase):
    def test_own_clean(self):
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        findings = list(check_style.python_findings(root))
        own = [
            finding
            for finding in findings
            if finding[1].endswith(("check_style.py", "check_style_tests.py"))
        ]
        self.assertEqual(own, [])


if __name__ == "__main__":
    unittest.main()
