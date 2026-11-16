import threading
import unittest
from unittest.mock import patch

from clients.performance import Execution


class CompactionObservation(unittest.TestCase):
    def test_observer_closes_its_client_when_the_load_has_already_finished(self):
        selected = {"id": "compaction", "node": "leader", "protocol": "pgwire"}
        execution = Execution({"case": selected, "max_receipt_bytes": 4096})
        operation = {"id": "adopted", "sql": "catalog", "minimum_system_time": "2100-01-03T00:00:00Z", "timeout_seconds": 1}
        with patch("clients.performance.Client") as factory:
            factory.return_value.query.return_value = {"schema": [{"name": "trie_key"}, {"name": "max_system_from"}], "rows": []}
            with self.assertRaisesRegex(ValueError, "during measured ingestion"):
                execution.observe_overlap(operation, threading.Barrier(1))
            factory.return_value.query.assert_called_once()
            factory.return_value.close.assert_called_once()
        self.assertFalse(execution.clients)

    def test_observation_retains_query_and_commit_boundary_inside_the_owner(self):
        execution = Execution({"case": {"node": "leader", "protocol": "pgwire"}, "max_receipt_bytes": 4096})
        execution.active_workers, execution.commits = 1, 2
        operation = {"id": "adopted", "sql": "catalog", "minimum_system_time": "2100-01-03T00:00:00Z", "timeout_seconds": 1}
        empty = {"schema": [{"name": "trie_key"}, {"name": "max_system_from"}], "rows": []}
        catalog = {**empty, "rows": [[{"kind": "text", "value": "new"}, {"kind": "timestamp", "value": "2100-01-03T00:00:00+00:00"}]]}
        with patch("clients.performance.Client") as factory, patch("clients.performance.time.perf_counter_ns", side_effect=[10, 50]):
            factory.return_value.query.side_effect = [empty, catalog]
            observation = execution.observe_overlap(operation, threading.Barrier(1))
            self.assertEqual((50, 2, "adopted"), (observation["observed_ns"], observation["commits_before"], observation["operation"]))
            self.assertEqual(["new"], observation["new_trie_keys"])
            factory.return_value.close.assert_called_once()
        self.assertFalse(execution.clients)


if __name__ == "__main__":
    unittest.main()
