import importlib.util
import pathlib
import unittest

SCRIPT = pathlib.Path(__file__).parents[1] / "backend_log.py"
SPEC = importlib.util.spec_from_file_location("backend_log", SCRIPT)
assert SPEC and SPEC.loader
BACKEND_LOG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BACKEND_LOG)


class BackendLogTests(unittest.TestCase):
    expected = ["suite::first", "suite::second"]

    def test_success(self):
        text = "test suite::first ... ok\ntest suite::second ... ok\n"
        self.assertEqual(BACKEND_LOG.verify_log(text, self.expected), [])

    def test_missing(self):
        errors = BACKEND_LOG.verify_log("test suite::first ... ok\n", self.expected)
        self.assertIn("tests did not succeed: suite::second", errors)

    def test_ignored(self):
        text = "test suite::first ... ignored\ntest suite::second ... ignored\n"
        self.assertTrue(BACKEND_LOG.verify_log(text, self.expected))

    def test_unexpected(self):
        text = "test suite::first ... ok\ntest suite::other ... ok\n"
        errors = BACKEND_LOG.verify_log(text, ["suite::first"])
        self.assertIn("unexpected successful tests: suite::other", errors)

    def test_duplicate(self):
        text = "test suite::first ... ok\ntest suite::first ... ok\n"
        errors = BACKEND_LOG.verify_log(text, ["suite::first"])
        self.assertIn("tests reported success more than once: suite::first", errors)

    def test_bare_name(self):
        text = "test first ... ok\ntest suite::second ... ok\n"
        errors = BACKEND_LOG.verify_log(text, self.expected)
        self.assertIn("tests did not succeed: suite::first", errors)

    def test_empty_expected(self):
        self.assertEqual(BACKEND_LOG.verify_log("", []), ["expected test list is empty"])


if __name__ == "__main__":
    unittest.main()
