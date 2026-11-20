"""Read-only aggregate dashboard. No third-party Python or JavaScript dependencies."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path

ROOT = Path("/data")
SITE = Path(__file__).parent


def read(path):
    return json.loads(path.read_text())


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            if self.path == "/":
                content = (SITE / "index.html").read_bytes()
                content_type = "text/html; charset=utf-8"
            elif self.path == "/api/dashboard":
                manifest = read(ROOT / "catalog" / "reporting.json")
                partitions = read(ROOT / "catalog" / "partitions.json")
                relative = Path(manifest["report_path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError("Invalid report path")
                content = json.dumps({"manifest": manifest, "data": read(ROOT / relative / "dashboard.json"),
                                      "stale": manifest["partition_versions"] != {
                                          k:v["version"] for k,v in partitions.items()
                                      }}).encode()
                content_type = "application/json"
            elif self.path == "/api/benchmark":
                files = sorted((ROOT / "benchmarks").glob("*-verified.json"))
                content = files[-1].read_bytes() if files else b'{"status":"not run"}'
                content_type = "application/json"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(503, "Run ingestion and reporting first")


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8502), Handler).serve_forever()
