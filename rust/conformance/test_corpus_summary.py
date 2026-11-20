import unittest

from corpus_summary import outcome


def detail(expected_size, actual_size, actual_rows=6):
    block = lambda size, rows: "{:row-count %d, :partitions [{:tries [{:trie-key \"l00-rc-b00\", :data-file-size %d}]}]}" % (rows, size)
    return ("fail [objects/v06/tables/public$xt_docs/blocks/b00.binpb.edn]\n  expected: (= ...)\n  actual: (not (= "
            + block(expected_size, 6) + " " + block(actual_size, actual_rows) + "))")


class DataFileLayoutTest(unittest.TestCase):
    def test_a_block_catalog_differing_only_in_data_file_size_is_a_layout_difference(self):
        self.assertEqual(outcome({"var": "xtdb.indexer-test/test-multi-block-metadata", "result": "error", "detail": detail(5638, 5598)}), "jvm-layout")

    def test_any_other_block_catalog_difference_still_fails(self):
        self.assertEqual(outcome({"var": "xtdb.indexer-test/x", "result": "fail", "detail": detail(5638, 5598, actual_rows=7)}), "fail")


if __name__ == "__main__":
    unittest.main()
