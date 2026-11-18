import pathlib
import tempfile
import unittest

from unsupported_inventory import classify, production_sites


def entry(code, classification="native-gap"):
    return {"code": code, "classification": classification, "sites": [f"rust/crates/x/src/lib.rs:{len(code)}"]}


def reason(category="reference-failure"):
    return {"category": category, "rationale": "why", "reference": "core/src/main/clojure/xtdb/x.clj:1"}


class ClassificationTests(unittest.TestCase):
    def test_native_gaps_carry_their_entry_and_unclassified_gaps_stay_bare(self):
        codes = [entry("a/classified"), entry("a/unclassified"), entry("xtdb/parity", "reference-parity")]
        merged = classify(codes, {"a/classified": reason()})
        self.assertEqual(merged[0], {**codes[0], **reason()})
        self.assertEqual(merged[1:], codes[1:])
        self.assertNotIn("category", codes[0])

    def test_entries_must_name_a_current_native_gap_and_a_known_category(self):
        codes = [entry("a/gap"), entry("xtdb/parity", "reference-parity")]
        for classification in [{"a/removed": reason()}, {"xtdb/parity": reason()}, {"a/gap": reason("unknown")}]:
            with self.subTest(classification=classification), self.assertRaises(ValueError):
                classify(codes, classification)


class SiteTests(unittest.TestCase):
    def test_a_test_only_item_hides_only_itself_and_a_test_module_hides_the_rest(self):
        source = "\n".join([
            '#[cfg(test)]',
            'fn helper() -> Anomaly { Anomaly::unsupported("a/test-helper", "") }',
            'fn production() -> Anomaly { Anomaly::unsupported("a/production", "") }',
            'match outcome {',
            '    #[cfg(test)]',
            '    Outcome::Test => return Err(Anomaly::unsupported(',
            '        "a/test-arm", "")),',
            '}',
            '#[cfg(test)]',
            'mod tests { fn t() { Anomaly::unsupported("a/in-tests", ""); } }',
        ])
        with tempfile.TemporaryDirectory() as root:
            crates = pathlib.Path(root) / "rust" / "crates"
            (crates / "x" / "src").mkdir(parents=True)
            (crates / "x" / "src" / "lib.rs").write_text(source, encoding="utf-8")
            self.assertEqual(list(production_sites(crates)), [("a/production", "rust/crates/x/src/lib.rs:3")])


if __name__ == "__main__":
    unittest.main()
