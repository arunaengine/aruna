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

    def test_test_function(self):
        findings = scan("#[test]\nfn stop_between_phases_now() {}\n")
        self.assertEqual(findings, [("testfn", "probe.rs", 2, "fn 'stop_between_phases_now' has 4 terms")])

    def test_test_attributes(self):
        text = (
            "#[test]\nfn case_one_two_three() {}\n"
            "#[rstest]\nfn case_two_three_four() {}\n"
            "#[test]\n#[ignore]\nfn case_three_four_five() {}\n"
        )
        findings = scan(text)
        self.assertEqual([finding[0] for finding in findings], ["testfn", "testfn", "testfn"])

    def test_const_fn_is_a_function(self):
        findings = scan("const fn drive_effects_now_twice() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'drive_effects_now_twice' has 4 terms")])

    def test_type_declarations(self):
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

    def test_constant_and_static(self):
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

    def test_public_and_attribute_field(self):
        text = (
            "struct Config {\n"
            "    #[serde(default)]\n"
            "    pub(crate) maximum_thread_pool_size_limit: usize,\n"
            "}\n"
        )
        self.assertEqual(messages(scan(text)), ["field 'maximum_thread_pool_size_limit' has 5 terms"])

    def test_where_clause_field(self):
        text = (
            "struct Holder<T>\nwhere\n    T: Iterator<Item = u8>,\n{\n    shared_state_value_limit: T,\n}\n"
        )
        self.assertEqual(messages(scan(text)), ["field 'shared_state_value_limit' has 4 terms"])

    def test_variant(self):
        findings = scan("enum Mode { MaximumRetryCountValueLimit }\n")
        self.assertEqual(findings, [("variant", "probe.rs", 1, "variant 'MaximumRetryCountValueLimit' has 5 terms")])

    def test_struct_variant_field(self):
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


class TermBoundaryTest(unittest.TestCase):
    def test_three_term_name_passes(self):
        self.assertEqual(scan("fn load_bucket_config() {}\n"), [])

    def test_four_term_repository_owned_declaration_fails(self):
        findings = scan("fn load_bucket_config_now() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'load_bucket_config_now' has 4 terms")])

    def test_missed_constant_field_and_variant_probe(self):
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
    def test_allowed_acronyms_count_once(self):
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

    def test_camel_case_acronyms_count_once(self):
        for name in ("HttpUuid", "S3UuidKey", "RoCrateManifest"):
            with self.subTest(name=name):
                self.assertEqual(scan(f"struct {name};\n"), [])

    def test_acronyms_do_not_hide_extra_terms(self):
        findings = scan("fn fetch_http_uuid_data() {}\n")
        self.assertEqual(findings, [("fn", "probe.rs", 1, "fn 'fetch_http_uuid_data' has 4 terms")])


class StringTest(unittest.TestCase):
    def test_quoted_raw_byte_and_char_literals_are_ignored(self):
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

    def test_strings_do_not_hide_following_declarations(self):
        text = 'const A: &str = "noise";\nfn four_term_name_here() {}\n'
        self.assertEqual(scan(text), [("fn", "probe.rs", 2, "fn 'four_term_name_here' has 4 terms")])

    def test_lifetimes_are_not_static_declarations(self):
        text = "fn borrowed() -> &'static str { \"\" }\n"
        self.assertEqual(scan(text), [])


class AttributeTest(unittest.TestCase):
    def test_attribute_bodies_are_not_scanned(self):
        text = (
            '#[cfg(feature = "four_term_feature_name")]\n'
            '#[allow(dead_code, reason = "four_term_reason_name")]\n'
            "fn tiny() {}\n"
        )
        self.assertEqual(scan(text), [])

    def test_inner_attribute_is_ignored(self):
        self.assertEqual(scan('#![allow(dead_code, reason = "four_term_reason_name")]\nfn tiny() {}\n'), [])


class CommentTest(unittest.TestCase):
    def comments(self, text):
        masked, comments, attrs, tokens = check_style.mask_source(text)
        return list(check_style.comment_findings("probe.rs", text, masked, comments, tokens))

    def test_nested_block_comment_is_one_span(self):
        text = "/* outer /* inner */ fn four_term_name_here() {} */\nfn tiny() {}\n"
        self.assertEqual(scan(text), [])
        masked, comments, _attrs, _tokens = check_style.mask_source(text)
        self.assertEqual(len(comments), 1)

    def test_three_line_comment_passes(self):
        self.assertEqual(self.comments("// one\n// two\n// three\nfn tiny() {}\n"), [])

    def test_four_line_comment_fails(self):
        self.assertEqual(
            self.comments("// one\n// two\n// three\n// four\nfn tiny() {}\n"),
            [("comment", "probe.rs", 1, "comment spans 4 lines")],
        )

    def test_four_line_block_comment_fails(self):
        self.assertEqual(
            self.comments("/* one\n two\n three\n four */\nfn tiny() {}\n"),
            [("comment", "probe.rs", 1, "comment spans 4 lines")],
        )

    def test_license_comment_is_exempt(self):
        self.assertEqual(self.comments("// Copyright 2026\n// two\n// three\n// four\nfn tiny() {}\n"), [])

    def test_restates_warning(self):
        self.assertEqual(
            self.comments("// bucket name\nfn bucket_name() {}\n"),
            [("restates", "probe.rs", 1, "comment repeats 'bucket_name'")],
        )


class MacroTest(unittest.TestCase):
    def test_macro_invocation_declarations_are_scanned(self):
        findings = scan("my_macro! {\n    static SHARED_REGISTRY_CACHE_VALUE: u8 = 1;\n}\n")
        self.assertEqual(findings, [("const", "probe.rs", 2, "static 'SHARED_REGISTRY_CACHE_VALUE' has 4 terms")])

    def test_macro_static_ref_declaration(self):
        findings = scan("lazy_static! {\n    static ref SHARED_SESSION_REGISTRY_CACHE: u8 = 0;\n}\n")
        self.assertEqual(findings, [("const", "probe.rs", 2, "static 'SHARED_SESSION_REGISTRY_CACHE' has 4 terms")])

    def test_macro_rules_definitions_are_skipped(self):
        text = "macro_rules! helper {\n    () => { fn four_term_name_here() {} };\n}\n"
        self.assertEqual(scan(text), [])

    def test_macro_invocation_references_are_ignored(self):
        text = "let _ = matches!(value, Option::Some(SomeFourTermTypeName));\n"
        self.assertEqual(scan(text), [])


class FolderTest(unittest.TestCase):
    def write(self, root, relpath):
        path = os.path.join(root, relpath)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("")

    def folders(self, tree):
        with tempfile.TemporaryDirectory() as tmp:
            for relpath in tree:
                self.write(tmp, relpath)
            return sorted(check_style.check_folders(tmp))

    def test_four_files_pass_and_three_fail(self):
        tree = [
            "pkg/Cargo.toml",
            "pkg/src/lib.rs",
            "pkg/src/good/one.rs",
            "pkg/src/good/two.rs",
            "pkg/src/good/three.rs",
            "pkg/src/good/four.rs",
            "pkg/src/small/one.rs",
            "pkg/src/small/two.rs",
            "pkg/src/small/three.rs",
        ]
        self.assertEqual(self.folders(tree), [("pkg/src/small", 3)])

    def test_structural_roots_are_exempt(self):
        tree = [
            "pkg/Cargo.toml",
            "pkg/src/lib.rs",
            "pkg/src/bin/tool.rs",
            "pkg/src/benches/bench.rs",
            "pkg/src/examples/demo.rs",
            "pkg/tests/single.rs",
        ]
        self.assertEqual(self.folders(tree), [])

    def test_mod_rs_does_not_count(self):
        tree = ["pkg/Cargo.toml", "pkg/src/lib.rs", "pkg/src/empty/mod.rs"]
        self.assertEqual(self.folders(tree), [("pkg/src/empty", 0)])

    def test_asset_only_directory_is_counted(self):
        tree = ["pkg/Cargo.toml", "pkg/src/lib.rs", "pkg/src/asset/one.toml"]
        self.assertEqual(self.folders(tree), [("pkg/src/asset", 1)])

    def test_fixture_asset_directories_are_exempt(self):
        tree = ["pkg/Cargo.toml", "pkg/src/lib.rs", "pkg/src/fixtures/one.bin", "pkg/tests/fixtures/two.bin"]
        self.assertEqual(self.folders(tree), [])


class ExternalNamesTest(unittest.TestCase):
    def setUp(self):
        self.saved = dict(check_style.EXTERNAL_NAMES)

    def tearDown(self):
        check_style.EXTERNAL_NAMES.clear()
        check_style.EXTERNAL_NAMES.update(self.saved)

    def test_reasoned_external_name_is_exempt(self):
        check_style.EXTERNAL_NAMES["GeneratedWireRecordName"] = "generated from the external schema"
        self.assertEqual(scan("struct GeneratedWireRecordName;\n"), [])

    def test_every_entry_needs_a_reason(self):
        check_style.EXTERNAL_NAMES["MissingReasonName"] = ""
        self.assertEqual(check_style.external_without_reason(), ["MissingReasonName"])


class ReferenceTest(unittest.TestCase):
    def test_references_are_not_declarations(self):
        text = (
            "fn use_types() { let _: Vec<SomeFourTermTypeName> = Vec::new(); }\n"
            "struct Holder { value: std::collections::HashMap<SomeFourTermTypeName, u32> }\n"
            "fn borrowed() -> &'static str { \"\" }\n"
        )
        self.assertEqual(scan(text), [])


if __name__ == "__main__":
    unittest.main()
