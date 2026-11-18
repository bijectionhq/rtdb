import unittest

from unsupported_inventory import classify


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


if __name__ == "__main__":
    unittest.main()
