import tempfile
import unittest
from pathlib import Path
import subprocess

from source_inventory import collect, declarations


class SourceInventoryTests(unittest.TestCase):
    def test_slt_identity_keeps_record_position_and_source_line(self):
        text = "skipif xtdb\nstatement ok\nCREATE TABLE foo(a)\n\nquery I nosort\nSELECT a FROM foo\n----\n1\n"
        records = list(declarations("sqlite-test/fixture.test", text))
        self.assertEqual([(line, name) for line, name, _, _ in records], [(2, "record:1"), (5, "record:2")])

    def test_junit_template_name_retains_backtick_text(self):
        text = "package xtdb.example\n@Test\nfun `same name with spaces`() {}\n@ParameterizedTest\nfun parameter(value: Int) {}\n"
        records = list(declarations("core/src/test/kotlin/ExampleTest.kt", text))
        self.assertEqual([record[1] for record in records], ["xtdb.example.ExampleTest/same name with spaces", "xtdb.example.ExampleTest/parameter"])

    def test_clojure_var_retains_namespace(self):
        text = "(ns xtdb.example-test)\n(t/deftest ^:integration a-case (t/is true))"
        self.assertEqual(list(declarations("src/test/clojure/example_test.clj", text))[0][1], "xtdb.example-test/a-case")

    def test_collection_reads_the_revision_and_leaves_unrecognized_suites_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.DEVNULL).decode().strip()

            git("init")
            git("config", "user.name", "inventory fixture")
            git("config", "user.email", "fixture@example.invalid")
            test = root / "core/src/test/kotlin/Disabled.kt"
            test.parent.mkdir(parents=True)
            test.write_text("@Disabled\nclass Disabled {}\n")
            git("add", ".")
            git("commit", "-m", "test fixture")
            revision = git("rev-parse", "HEAD")
            test.write_text("@Test\nfun newer() {}\n")
            inventory = collect(root, revision)
            self.assertEqual(inventory["reference"], revision)
            self.assertEqual(inventory["files"][0]["declarations"], 0)
            self.assertEqual(inventory["runtime_discovery"], "pending")
            self.assertIsNone(inventory["assertion_denominator"])
            self.assertEqual(inventory["candidates"], [])


if __name__ == "__main__":
    unittest.main()
