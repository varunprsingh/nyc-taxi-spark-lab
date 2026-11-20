"""HTTP integration verifies aggregate payload and stale reporting detection."""
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest
import urllib.request


class DashboardTests(unittest.TestCase):
    def test_payload_and_freshness(self):
        spec = importlib.util.spec_from_file_location("dashboard_server", Path(__file__).parents[1] / "dashboard/server.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            module.ROOT = root
            (root / "catalog").mkdir()
            (root / "reports" / "v1").mkdir(parents=True)
            (root / "reports/v1/dashboard.json").write_text(json.dumps({"trips": 3, "total_collected_amount": "19.00"}))
            (root / "catalog/reporting.json").write_text(json.dumps({"report_path": "reports/v1", "partition_versions": {"2024-01": "v1"}}))
            partitions = root / "catalog/partitions.json"
            partitions.write_text(json.dumps({"2024-01": {"version": "v1"}}))
            server = module.ThreadingHTTPServer(("127.0.0.1", 0), module.Handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}"
                with urllib.request.urlopen(url+"/api/dashboard") as response:
                    payload = json.load(response)
                self.assertFalse(payload["stale"])
                self.assertEqual(payload["data"]["trips"], 3)
                partitions.write_text(json.dumps({"2024-01": {"version": "v2"}}))
                with urllib.request.urlopen(url+"/api/dashboard") as response:
                    self.assertTrue(json.load(response)["stale"])
                with urllib.request.urlopen(url+"/") as response:
                    self.assertIn(b"Taxi Demand Lab", response.read())
            finally:
                server.shutdown()
                server.server_close()
                thread.join()


if __name__ == "__main__":
    unittest.main()
