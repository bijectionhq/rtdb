import io
import unittest

from sql_logic_records import parse_record, records


class OriginalRecords(unittest.TestCase):
    def test_directives_keep_physical_identity_and_assertion_ordinals(self):
        actual = list(records(io.StringIO("# ignored\n\nhash-threshold 32\n\nskipif mysql\nquery I\nSELECT 1 # stripped\n----\n1\n\nhalt\n\nstatement ok\nCREATE TABLE t(a INTEGER)\n")))
        self.assertEqual([3, 5, 11, 13], [item["line"] for item in actual])
        self.assertEqual([None, 1, None, 2], [item["assertion_ordinal"] for item in actual])
        self.assertEqual("SELECT 1", actual[1]["record"]["sql"])
        self.assertEqual(["mysql"], actual[1]["record"]["skipif"])

    def test_nested_conditions_keep_pinned_override_and_conj_order(self):
        record = parse_record(["onlyif xtdb", "skipif mysql", "onlyif sqlite", "skipif postgresql", "query I", "SELECT 1"])
        self.assertEqual("xtdb", record["onlyif"])
        self.assertEqual(["postgresql", "mysql"], record["skipif"])
        self.assertEqual({"count": 0, "text": "\n"}, record["expected"])

    def test_hash_case_count_and_unused_label_are_preserved(self):
        record = parse_record(["query IR valuesort repeated", "SELECT 1, 2.0", "----", "99 values hashing to ABCDEF0123456789abcdef0123456789"])
        self.assertEqual(99, record["expected"]["count"])
        self.assertTrue(record["expected"]["md5"].startswith("ABCDEF"))
        self.assertEqual("repeated", record["label"])

    def test_comment_regex_also_strips_inside_string_but_not_bare_hash(self):
        actual = list(records(io.StringIO("statement ok\nSELECT '#text'\n\nstatement ok\nSELECT '#'\n")))
        self.assertEqual("SELECT '", actual[0]["record"]["sql"])
        self.assertEqual("SELECT '", actual[1]["record"]["sql"])
        self.assertEqual("SELECT #", list(records(io.StringIO("statement ok\nSELECT #\n")))[0]["record"]["sql"])

    def test_parse_error_cannot_remove_record_or_next_identity(self):
        actual = list(records(io.StringIO("query Z\nSELECT 1\n\nquery I\nSELECT 2\n----\n2\n")))
        self.assertIsNotNone(actual[0]["parse_error"])
        self.assertEqual((2, 4), (actual[1]["ordinal"], actual[1]["line"]))

    def test_carriage_return_newlines_preserve_original_lines(self):
        actual = list(records(io.StringIO("statement ok\r\nCREATE TABLE a(b INTEGER)\r\n\r\nquery I\r\nSELECT 1\r\n")))
        self.assertEqual([1, 4], [item["line"] for item in actual])

    def test_trailing_header_spaces_and_signed_long_boundaries_follow_pinned_parser(self):
        self.assertEqual("nosort", parse_record(["query I   ", "SELECT 1"])["sort"])
        with self.assertRaises(ValueError):
            parse_record(["hash-threshold 9223372036854775808"])
        with self.assertRaises(ValueError):
            parse_record(["hash-threshold 1_000"])
        self.assertEqual(-1, parse_record(["hash-threshold -1"])["threshold"])


if __name__ == "__main__":
    unittest.main()
