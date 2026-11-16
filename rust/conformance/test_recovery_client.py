import unittest
from unittest.mock import MagicMock, Mock, patch

from clients.performance import cell
from clients.recovery import perform


class RecoveryClientTest(unittest.TestCase):
    def test_flush_uses_bounded_inspection_and_waits_for_adoption_and_empty_live_tables(self):
        client = Mock()
        blocks = iter(["00", "00", "01", "01"])
        live = iter([0, 1, 0])
        def query(sql):
            if "xt.block_files" in sql:
                self.assertIn("WHERE block_idx BETWEEN '00' AND '0f'", sql)
                return {"rows":[[cell(next(blocks))]]}
            self.assertEqual("SELECT count(*) AS n FROM xt.live_tables WHERE row_count > 0", sql)
            return {"rows":[[cell(next(live))]]}
        client.query.side_effect = query
        response = MagicMock()
        response.__enter__.return_value.status = 200
        response.__enter__.return_value.read.return_value = b""
        request = {"action":"flush", "node":"origin", "timeout_seconds":5,
                   "endpoints":{"origin":{"health_port":8080, "database":"xtdb"}}}
        with patch("clients.recovery.urllib.request.urlopen", return_value=response) as post, patch("clients.recovery.time.sleep"):
            result = perform(request, client)
        self.assertEqual({"before":{"rows":[[cell("00")]]}, "after":{"rows":[[cell("01")]]}, "live":{"rows":[[cell(0)]]}}, result)
        self.assertEqual(7, client.query.call_count)
        self.assertEqual("POST", post.call_args.args[0].method)
        self.assertEqual("http://origin:8080/system/finish-block?db=xtdb", post.call_args.args[0].full_url)
